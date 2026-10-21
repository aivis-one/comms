# =============================================================================
# COMMS Service -- Notification Channel Formatters
# =============================================================================
#
# Canonical merge of the cbshome (base) and velo formatter layers.
#
# ARCHITECTURE (cbshome base):
#   ChannelFormatter  -- Protocol: deliver(notification, delivery,
#                        recipient) -> bool; raises PermanentDeliveryError
#                        for non-retryable failures; transient failures
#                        raise ordinary exceptions (service retries).
#   InAppFormatter    -- in_app delivery: the delivery row IS the
#                        inbox entry, nothing leaves the process.
#   EmailFormatter    -- plain-text email through the provider's HTTP
#                        API (one path, no SMTP fallback).
#   UnavailableChannelFormatter
#                     -- a channel this deploy does not have; every
#                        delivery fails PERMANENTLY and loudly.
#   TelegramFormatter -- aiogram Bot.send_message.
#
# TELEGRAM (merged):
#   - Message building, inline deep-link button, channel_options
#     handling and the injected-Bot constructor come from velo
#     (the richer donor for the send path).
#   - format_deep_link enforces the ONE-PARAMETER encoding rule
#     (Phase 3a item 3, arch doc §2.6): the whole ?startapp= value is
#     validated (charset [A-Za-z0-9_-], 64 chars) AT LINK BUILD;
#     violations are config errors -> loud PermanentDeliveryError with
#     the greppable "deep link:" prefix, never a silently broken
#     button. The velo heritage of joining values with "_" is gone --
#     it could not be parsed back.
#   - Permanent-error detection merges velo's _PERMANENT_ERRORS
#     substrings (richer) with cbshome's exception-based contract:
#     the formatter RAISES PermanentDeliveryError instead of returning
#     velo's DeliveryResult, because the canonical service layer
#     (cbshome) speaks exceptions.
#   - Credentials come from recipient columns (telegram_id, locale),
#     not from a product User (de-domainization).
#
# REGISTRY (app/core/channels.py holds the rule):
#   build_formatters() maps EVERY DeliveryChannel to a formatter: live
#   channels to their implementation, every other channel to
#   UnavailableChannelFormatter. There is no fallback to a succeeding
#   stub anywhere: a requested channel that the deploy does not have is
#   a lost notification, and it must be a FAILED delivery, not a quiet
#   success. The refusal is raised by the formatter's deliver() -- the
#   service catches PermanentDeliveryError per delivery, so neighbours
#   in the same pass are unaffected (get_formatter itself never raises
#   for a known channel).
#   The worker builds the registry AT STARTUP (init_formatters), so a
#   failure to build kills the worker before its loop, never on the
#   first delivery.
#
# LLM (P5-1, spec §14):
#   LLMFormatter -- ONE adapter for every OpenAI-compatible provider: the
#   address is the key set's (app/core/channels.py LLM_KEY_SETS). It is
#   ASKED, not delivered: it takes the product's letter and the job's
#   ordered key sets and brings back the provider's answer raw
#   (LLMReply), so it lives in the registry beside the formatters as the
#   one ModelChannel (get_model_channel), not among them. It knows HOW to
#   talk to a provider -- the request shape, the routing block of a
#   gateway, where a provider hides a transport failure inside a 200 --
#   and never WHAT is asked: it reads the response's envelope (error,
#   choices, model, provider), never the content of the answer.
#
# PUSH:
#   Not implemented -> absent on every deploy (not_implemented in the
#   startup channel map).
# =============================================================================

import asyncio
import base64
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from html import escape
from typing import TYPE_CHECKING, Any, NoReturn, Protocol

import structlog

from app.audience.models import Recipient
from app.core.channels import (
    LLM_BODY_CLIENT_TIMEOUTS,
    LLM_BODY_DOWNLOAD_TIMEOUT,
    ChannelState,
    LLMKeySet,
    body_endpoint,
    channel_env_keys,
    channel_spec,
    evaluate_channels,
)
from app.core.config import Settings, settings
from app.core.sanitize import sanitize_error, sanitize_text, sanitized_traceback
from app.engine.constants import DeliveryChannel
from app.engine.models import Notification, NotificationDelivery
from app.engine.template_engine import render, resolve_flag

if TYPE_CHECKING:
    from aiogram import Bot

logger = structlog.get_logger()


class PermanentDeliveryError(Exception):
    """A delivery failed permanently and must not be retried.

    ABSTRACT (F1.3): a channel raises one of the three subclasses below,
    never this class -- the subclass IS the failure class (spec §5.6),
    and app/engine/service.py _deliver_single maps it to a FailureClass
    in one place. Raising the base directly is refused at construction,
    so a new raise site cannot silently drop the class.
    """

    def __init__(self, *args: object) -> None:
        if type(self) is PermanentDeliveryError:
            raise TypeError(
                "PermanentDeliveryError is abstract: raise "
                "ConfigurationError, MessageRejectedError or NoAddressError"
            )
        super().__init__(*args)


class ConfigurationError(PermanentDeliveryError):
    """The channel is dead on this deploy: every message will die the
    same way until the deploy changes (bad credentials, a channel that
    is not configured, an account without rights). Logged LOUDLY."""


class MessageRejectedError(PermanentDeliveryError):
    """This message will not arrive; the channel is alive and other
    messages go (the recipient closed the channel, the letter is
    malformed for this channel, the provider refused this message)."""


class NoAddressError(PermanentDeliveryError):
    """The recipient has no usable address in this channel -- the
    product's sync, not the channel, is what to fix."""


class RateLimitedError(Exception):
    """Raised when the channel says "come back later" (HTTP 429).

    Not a message failure: the channel is healthy, it just asks to
    slow down -- and tells exactly when to retry. The service layer
    defers via next_retry_at WITHOUT burning an attempt (same pattern
    as the delivery schedule), up to a deferral budget (Phase 2.2).
    """

    def __init__(self, retry_after: float, reason_tail: str = "") -> None:
        """reason_tail is appended verbatim: a channel that read the
        provider's own words passes them already sanitized and cut (see
        _reason_tail); a channel that has none passes nothing."""
        super().__init__(
            f"rate limited by channel: retry after {retry_after}s"
            f"{reason_tail}"
        )
        self.retry_after = retry_after


class EmailTransientError(Exception):
    """The email provider failed in a way a retry can fix.

    A named type rather than a bare Exception: the service layer treats
    any unexpected exception as transient too, but that path logs
    delivery_error with a traceback. A provider 500 is expected weather,
    not a bug in this service.
    """


class LLMTransientError(Exception):
    """Every key set of an llm call failed, and at least one of them in a
    way a retry can fix (a 5xx, a timeout, a broken answer, a rate limit
    without a named wait). Named, like EmailTransientError: the provider's
    weather, not a defect of comms -- and its text is the provider's
    answer, set by set, as the journal records it."""


class ChannelFormatter(Protocol):
    """Protocol for channel-specific notification delivery."""

    async def deliver(
        self,
        notification: Notification,
        delivery: NotificationDelivery,
        recipient: Recipient,
    ) -> bool:
        """Attempt to deliver a notification via this channel.

        Args:
            notification: The parent notification (title, body, action_data).
            delivery: The delivery record (channel, channel_options).
            recipient: The recipient (telegram_id, email, locale).

        Returns:
            True if delivery succeeded, False otherwise.

        Raises:
            PermanentDeliveryError: If delivery failed permanently.
        """
        ...


class InAppFormatter:
    """in_app delivery -- the delivery row IS the inbox entry.

    The inbox reads in_app deliveries with status sent (app/api/
    inbox.py), so succeeding here is the whole delivery: nothing leaves
    the process and there is nothing to configure (in_app declares no
    keys, app/core/channels.py).
    """

    async def deliver(
        self,
        notification: Notification,
        delivery: NotificationDelivery,
        recipient: Recipient,
    ) -> bool:
        """Log the delivery and return True -- the row is the inbox."""
        # NEVER the letter in the line (spec §6.4, §0 of the line): the
        # title is the letter -- it was here until P5-1.
        logger.info(
            "in_app_delivery",
            notification_id=str(notification.id),
            delivery_id=str(delivery.id),
            channel=delivery.channel,
            recipient_id=str(delivery.recipient_id),
        )
        return True


class UnavailableChannelFormatter:
    """A channel this deploy does not have -- every delivery FAILS.

    Not configured (its key set is empty) or not implemented: either
    way the channel will not appear between attempts, so the failure is
    permanent (PermanentDeliveryError -> immediate FAILED, no attempt
    increment, no retry).
    """

    def __init__(self, channel: str, reason: str) -> None:
        """Remember which channel and why it is unavailable."""
        self._channel = channel
        self._reason = reason

    async def deliver(
        self,
        notification: Notification,
        delivery: NotificationDelivery,
        recipient: Recipient,
    ) -> bool:
        """Refuse loudly: log at error level, raise permanent failure."""
        logger.error(
            "delivery_channel_unavailable",
            notification_id=str(notification.id),
            delivery_id=str(delivery.id),
            channel=self._channel,
            reason=self._reason,
        )
        raise ConfigurationError(
            f"channel '{self._channel}' is not available on this "
            f"deploy: {self._reason}"
        )

    async def ask(
        self,
        letter: Mapping[str, Any],
        set_names: Sequence[str],
        audio: Mapping[str, Any] | None = None,
    ) -> "LLMReply":
        """The llm channel absent on this deploy: refused like deliver."""
        logger.error(
            "delivery_channel_unavailable",
            channel=self._channel,
            reason=self._reason,
        )
        raise ConfigurationError(
            f"channel '{self._channel}' is not available on this "
            f"deploy: {self._reason}"
        )


# ===================================================================
# TelegramFormatter
# ===================================================================

# Telegram API error substrings that indicate permanent failure
# (ported from velo -- richer than cbshome's two substrings).
# No point retrying these -- user must unblock or start the bot.
_PERMANENT_ERRORS = frozenset({
    "bot was blocked by the user",
    "user is deactivated",
    "chat not found",
    "bot can't initiate conversation",
    "have no rights to send a message",
    "forbidden",
    # Review 1.1: with variables escaped, a parse failure can only come
    # from broken HTML in the template itself -- a config error that a
    # retry cannot fix.
    "can't parse entities",
    # Review 1.2: the button URL is built from immutable action_data --
    # an invalid URL is deterministic, retrying cannot fix it.
    "button_url_invalid",
})


# Deep-link encoding limits (Phase 3a item 3, arch doc §2.6).
# Telegram's ?startapp= payload is fragile: charset [A-Za-z0-9_-] and
# at most 64 characters -- for the WHOLE value, action and parameter
# together. Validated at link build; a violation is a CONFIG error
# (action_data is immutable -> deterministic, same logic as
# button_url_invalid) and must fail LOUDLY, not ship a dead button.
_STARTAPP_ALLOWED_RE = re.compile(r"[A-Za-z0-9_-]+")
_STARTAPP_MAX_LEN = 64


class TelegramFormatter:
    """Deliver notifications via Telegram Bot API (aiogram 3.x).

    Uses Bot.send_message() only -- no Dispatcher, no polling, no
    event-loop conflict with uvicorn. The Bot instance is injected
    (velo pattern) so tests can pass a fake.
    """

    def __init__(self, bot: "Bot", bot_url: str) -> None:
        """Initialize with aiogram Bot instance and bot URL.

        Args:
            bot: Aiogram Bot instance (already configured with token).
            bot_url: Base URL for deep links (e.g. "https://t.me/<product_bot>").
        """
        self._bot = bot
        self._bot_url = bot_url.rstrip("/")

    def format_deep_link(
        self, action_data: dict[str, Any] | None,
    ) -> str | None:
        """Convert action_data to a Telegram WebApp deep link.

        ENCODING RULE (Phase 3a item 3, arch doc §2.6): at most ONE
        parameter. The velo heritage of joining several values with
        "_" cannot be unpacked -- the separator is legal inside the
        values -- so multi-parameter targets are forbidden outright.
        Composite targets belong behind an OPAQUE TOKEN minted by the
        product, never packed field-by-field into the string.

        The ASSEMBLED value ({action} or {action}__{param}) is
        validated here, at link build: charset [A-Za-z0-9_-] and the
        64-char limit apply to the whole thing. A violation is a
        CONFIG error -- action_data is immutable, so the failure is
        deterministic and a retry cannot fix it (same logic as
        button_url_invalid, review 1.2) -- and raises
        PermanentDeliveryError whose message starts with the STABLE
        "deep link:" prefix: encoding failures are greppable in
        NotificationDelivery.error_message.

        The domain comes from env (settings.telegram_bot_url via the
        constructor) -- no domain literals in code (item 4,
        decision 13).

        Args:
            action_data: {"action": "open_thread",
                          "params": {"thread_id": "<uuid>"}}

        Returns:
            Deep link URL, or None when there is nothing to link
            (no action_data / no action).

        Raises:
            PermanentDeliveryError: More than one parameter, or the
                assembled startapp value violates charset/length.
        """
        if not action_data:
            return None

        action = action_data.get("action")
        if not action:
            return None

        params = action_data.get("params") or {}
        if len(params) > 1:
            # The refusal names counts and limits, never a value of the
            # letter: its text is kept as the answer of this call in the
            # delivery and the journal (H8, tests/test_delivery_error_text.py).
            raise MessageRejectedError(
                f"deep link: the action carries {len(params)} parameters; "
                f"the startapp encoding fits at most ONE. Put composite "
                f"targets behind an opaque token on the product side."
            )

        if params:
            (value,) = params.values()
            startapp = f"{action}__{value}"
        else:
            startapp = str(action)

        if len(startapp) > _STARTAPP_MAX_LEN:
            raise MessageRejectedError(
                f"deep link: startapp value is {len(startapp)} chars, "
                f"the Telegram limit is {_STARTAPP_MAX_LEN}"
            )
        if not _STARTAPP_ALLOWED_RE.fullmatch(startapp):
            raise MessageRejectedError(
                "deep link: startapp value contains characters outside "
                "[A-Za-z0-9_-]"
            )

        return f"{self._bot_url}?startapp={startapp}"

    async def deliver(
        self,
        notification: Notification,
        delivery: NotificationDelivery,
        recipient: Recipient,
    ) -> bool:
        """Send a Telegram message to the recipient.

        COMPOSITION RULE (Phase 3a item 2): the markup source follows
        the CONTENT source, per field. A template-rendered field is
        trusted and goes into the message VERBATIM -- the sheet owns
        its own markup, nothing is injected on top. A stored-fallback
        field is untrusted content (escaped), so the SERVICE supplies
        its presentation: the stored title keeps the historical <b>
        wrap, the stored body stays plain. Mixed cases fall out of the
        same per-field rule: a template body next to a stored title
        bolds ONLY the title, and vice versa. The "\n\n" join is
        structure, not markup -- a per-field template cannot express
        the seam between two fields.

        PRESENTATION (Phase 3a item 1): button_text / disable_preview
        / silent resolve template sheet (localizable default) <
        channel_options (per-delivery override); the deep-link button
        appears when action_data carries an action.

        Raises:
            PermanentDeliveryError: If the recipient has no telegram_id,
                the deep link cannot be encoded (item 3), or the
                Telegram API reports a permanent failure.
        """
        # NULL is "no chat" (F1.4); a 0 can no longer arrive
        # (ck_recipients_telegram_id_not_zero).
        if recipient.telegram_id is None:
            raise NoAddressError("Recipient has no telegram_id")

        # Lazy import: aiogram types only needed on the real send path.
        from aiogram.enums import ParseMode
        from aiogram.exceptions import (
            TelegramAPIError,
            TelegramRetryAfter,
            TelegramUnauthorizedError,
        )
        from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

        # Trust boundary (review 1.1): the TEMPLATE is trusted and may
        # carry HTML; VARIABLE VALUES and the stored title/body are not
        # and are escaped before entering ParseMode.HTML. The RAW
        # values are kept for the button label -- a plain-text surface
        # (see the button block below).
        raw_variables = build_variables(notification)
        variables = _escape_html_variables(raw_variables)
        # NULL is "no language" (F1.4) -- the one way to say it; a blank
        # string can no longer arrive (ck_recipients_locale_not_blank).
        locale = (
            recipient.locale
            if recipient.locale is not None
            else settings.default_locale
        )

        rendered_title = render(
            notification_type=notification.type,
            channel=DeliveryChannel.TELEGRAM,
            field="title",
            locale=locale,
            variables=variables,
        )
        # Markup follows the content source (see docstring): only the
        # stored-fallback title gets the service-supplied <b> wrap.
        title = (
            rendered_title
            if rendered_title is not None
            else f"<b>{escape(notification.letter_text()[0])}</b>"
        )
        rendered_body = render(
            notification_type=notification.type,
            channel=DeliveryChannel.TELEGRAM,
            field="body",
            locale=locale,
            variables=variables,
        )
        body = (
            rendered_body
            if rendered_body is not None
            else escape(notification.letter_text()[1])
        )

        text = f"{title}\n\n{body}"

        # Build inline keyboard if a deep link is available.
        deep_link = self.format_deep_link(notification.action_data)
        channel_options = delivery.channel_options
        keyboard = None
        if deep_link:
            # BUTTON TEXT (Phase 3a item 1 / fix A), priority low ->
            # high: hardcoded "Open" < template sheet field
            # "button_text" (localizable default, same locale chain as
            # title/body) < channel_options["button_text"]. Same key
            # on both sides on purpose: the override is literal --
            # same key, different source, channel_options win.
            #
            # THIRD RENDERING MODE (fix F) -- do NOT "fix" this by
            # escaping: title/body enter ParseMode.HTML and take
            # ESCAPED variables; the button label is PLAIN TEXT
            # (Telegram does not parse HTML inside inline-button
            # labels), so its template renders with RAW variables.
            # Escaping here would show a literal "&amp;" to the user;
            # there is no injection surface -- markup is not
            # interpreted, and the URL is validated separately.
            # Asymmetry, on purpose: the SHEET value is a TEMPLATE
            # (rendered per locale, dry-run checked at startup); the
            # channel_options value is a per-delivery LITERAL from the
            # producer and is not format_map'ed.
            button_text = "Open"
            rendered_button = render(
                notification_type=notification.type,
                channel=DeliveryChannel.TELEGRAM,
                field="button_text",
                locale=locale,
                variables=raw_variables,
            )
            if rendered_button is not None:
                button_text = rendered_button
            if channel_options and "button_text" in channel_options:
                button_text = channel_options["button_text"]
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=button_text, url=deep_link)]
                ]
            )

        # PRESENTATION FLAGS (Phase 3a item 1), priority low -> high:
        # channel default < template sheet flag (bool leaf, resolved
        # through the same per-field locale chain as the texts) <
        # channel_options override.
        sheet_preview = resolve_flag(
            notification.type, DeliveryChannel.TELEGRAM,
            "disable_preview", locale,
        )
        disable_preview = True if sheet_preview is None else sheet_preview
        sheet_silent = resolve_flag(
            notification.type, DeliveryChannel.TELEGRAM, "silent", locale,
        )
        silent = False if sheet_silent is None else sheet_silent
        if channel_options:
            disable_preview = channel_options.get(
                "disable_preview", disable_preview,
            )
            silent = channel_options.get("silent", silent)

        try:
            await self._bot.send_message(
                chat_id=recipient.telegram_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=disable_preview,
                disable_notification=silent,
            )
            return True

        except TelegramAPIError as exc:
            # 429 first: TelegramRetryAfter IS-A TelegramAPIError, and
            # the server names the exact wait -- surface it typed so
            # the service defers via next_retry_at instead of burning
            # an attempt (Phase 2.2).
            if isinstance(exc, TelegramRetryAfter):
                # exc.message is aiogram's own frame ("Flood control
                # exceeded ... Retry in N seconds.") followed by
                # Telegram's description, so it is never empty: the
                # tail always carries words, never the empty marker.
                raise RateLimitedError(
                    float(exc.retry_after),
                    _reason_tail(_clean_reason(exc.message), 200),
                ) from exc
            # A rejected bot token: the channel is dead for EVERY message
            # until the deploy changes. Before F1.3 this fell through to
            # the transient path and burned the whole attempt budget,
            # ending as "transient attempts exhausted" -- a dead channel
            # disguised as weather.
            if isinstance(exc, TelegramUnauthorizedError):
                raise ConfigurationError(
                    f"Telegram rejected the bot token: {exc}"
                ) from exc
            error_msg = str(exc).lower()
            for perm_error in _PERMANENT_ERRORS:
                if perm_error in error_msg:
                    raise MessageRejectedError(
                        f"Telegram permanent failure: {exc}"
                    ) from exc
            # Transient error -- service layer retries.
            raise


# ===================================================================
# EmailFormatter
# ===================================================================

# Response statuses that mean the provider ACCEPTED the message. Accepted
# is not delivered: what happens after -- the provider's own queue, the
# receiving MX, greylisting -- is outside this service and, without
# inbound status webhooks, not measurable here. The provider's message
# id is logged for exactly that reason (see below).
_EMAIL_ACCEPTED_STATUSES = frozenset({200, 202})

# Statuses that mean the CHANNEL is wrong, not the message: a bad API
# key, an account out of credit, a forbidden operation. Every message
# will die the same way until a human changes the configuration.
_EMAIL_CONFIG_STATUSES = frozenset({401, 402, 403})

# Substrings in the provider's own error text that place a 400 on the
# SENDER rather than on the recipient -- an unverified domain or a from
# address outside it kills the channel, not one message, and the status
# code alone cannot tell the two apart. Lower-cased before matching.
_EMAIL_SENDER_FAULT_MARKERS = (
    "domain not found",
    "domain is not verified",
    "not allowed to send",
    "sandbox",
    "free accounts are for test purposes",
    "from address",
    "invalid domain",
)

# The characters a provider splits a RECIPIENT LIST on. The `to` field
# below carries one address, and the provider parses that field as a
# list -- so a separator inside a single snapshot value does not make
# the address invalid, it makes the message go to somebody else as
# well. See _usable_email_address for why this, and only this, is ours
# to judge rather than the provider's.
_EMAIL_LIST_SEPARATORS = (",", ";")

# NO TRACKING, IN EVERY MESSAGE (H3). Mailgun's tracking REWRITES the
# message -- links become redirects through the provider, an open pixel
# is added -- and comms delivers the message as the product wrote it.
# The domain's own tracking settings may be switched on in the
# provider's panel; these per-message options switch clicks and opens
# off whatever the domain says (Mailgun API, POST
# /v3/{domain}/messages: o:tracking toggles both, o:tracking-clicks and
# o:tracking-opens each one -- both spelled out, so no precedence
# between them is left to guess). The same for every type and every
# message: there is no knob, because there is no case for changing a
# message. What these do NOT close: the unsubscribe footer is a DOMAIN
# setting with no per-message option -- deploy/INTEGRATION.md tells
# the product to keep it off.
_NO_TRACKING: dict[str, str] = {
    "o:tracking": "no",
    "o:tracking-clicks": "no",
    "o:tracking-opens": "no",
}


class EmailFormatter:
    """Deliver notifications as plain-text email through the provider.

    ONE PATH, on purpose: an SMTP fallback would be code no deploy
    executes and only a mock ever exercises. Delivery is retried, and
    the products that send codes re-send them on request.

    TEXT ONLY: an HTML body carrying a link, with no text alternative,
    is a classic reason to be filed as spam -- and deliverability is the
    single reason this provider is used at all.

    SUBJECT AND BODY come from the profile like every other channel:
    {type: {channel: {field}}}, so email reads its OWN `subject` and
    `body` fields. Nothing new enters the profile contract.
    """

    def __init__(
        self,
        *,
        client: Any,
        api_base_url: str,
        api_key: str,
        domain: str,
        from_address: str,
    ) -> None:
        """Hold the HTTP client and this deploy's provider settings."""
        self._client = client
        self._url = f"{api_base_url.rstrip('/')}/v3/{domain}/messages"
        self._api_key = api_key
        self._from_address = from_address
        # LOUDNESS IS PER STATE, NOT PER MESSAGE (see _fail_configured):
        # a dead channel during a thousand registrations is a thousand
        # identical lines, which is noise, not a signal. This flag ONLY
        # controls log volume -- it never gates a delivery and never
        # marks the channel as broken.
        self._configuration_reported = False

    async def deliver(
        self,
        notification: Notification,
        delivery: NotificationDelivery,
        recipient: Recipient,
    ) -> bool:
        """Send one email; True when the provider accepted it."""
        address = _usable_email_address(recipient.email)
        if address is None:
            # The recipient snapshot arrives over the bus and an address
            # MAY appear between attempts -- retrying is still wrong: the
            # notification would live to its attempt budget, eating the
            # delivery-time budget of the recipients who do have one. An
            # address that appears later is a reason for a NEW
            # notification from the product, not for reviving this one.
            raise NoAddressError(
                "recipient has no usable email address "
                f"({_address_defect(recipient.email)})"
            )

        subject, body = self._compose(notification, recipient)

        try:
            response = await self._client.post(
                self._url,
                auth=("api", self._api_key),
                data={
                    "from": self._from_address,
                    "to": [address],
                    "subject": subject,
                    "text": body,
                    **_NO_TRACKING,
                },
            )
        except Exception as exc:
            # Transient by construction: connect failures and read
            # timeouts alike. The two are logged apart because only one
            # of them can have produced a message on the provider's side
            # -- see the duplicate note in the phase report.
            logger.warning(
                "email_request_failed",
                notification_id=str(notification.id),
                delivery_id=str(delivery.id),
                recipient_id=str(delivery.recipient_id),
                sent_unknown=_request_may_have_arrived(exc),
                error=sanitize_error(exc),
            )
            raise

        return self._interpret(response, notification, delivery)

    # -- composition --------------------------------------------------

    def _compose(
        self, notification: Notification, recipient: Recipient,
    ) -> tuple[str, str]:
        """Subject and body for this notification, in the recipient's
        locale, with the fallbacks the channel needs."""
        variables = build_variables(notification)
        # NULL is "no language" (F1.4) -- the one way to say it; a blank
        # string can no longer arrive (ck_recipients_locale_not_blank).
        locale = (
            recipient.locale
            if recipient.locale is not None
            else settings.default_locale
        )

        rendered_subject = render(
            notification_type=notification.type,
            channel=DeliveryChannel.EMAIL,
            field="subject",
            locale=locale,
            variables=variables,
        )
        # SUBJECT FALLBACK, three steps, and each step is load-bearing:
        #   1. the channel's own subject template;
        #   2. the stored title -- NOT anything derived from the body:
        #      a confirmation body opens with the code, and the subject
        #      shows up in the lock-screen preview;
        #   3. the type key, when the title is blank. A blank title IS
        #      reachable (the ingest validator rejects "" but accepts
        #      whitespace), and an empty subject both looks like spam
        #      and hurts the deliverability this channel exists for.
        #      The type key is ugly and honest: never blank, never
        #      derived from the body, not invented copy in one language
        #      on a multi-locale service, and traceable to the producer.
        stored_title, stored_body = notification.letter_text()
        subject = _clean_subject(rendered_subject or stored_title)
        if not subject:
            subject = notification.type

        rendered_body = render(
            notification_type=notification.type,
            channel=DeliveryChannel.EMAIL,
            field="body",
            locale=locale,
            variables=variables,
        )
        # BODY FALLBACK goes to the STORED body and never to another
        # channel's template: the telegram body carries HTML markup,
        # which would arrive as raw characters in a text email.
        body = rendered_body if rendered_body is not None else stored_body
        return subject, body

    # -- response handling --------------------------------------------

    def _interpret(
        self,
        response: Any,
        notification: Notification,
        delivery: NotificationDelivery,
    ) -> bool:
        """Turn the provider's response into a delivery outcome.

        Three classes, decided here and NOT in the service layer (which
        owns the shared retry policy and is not touched by this change):
          transient  -> raise, the attempts budget retries;
          permanent, one message -> PermanentDeliveryError;
          permanent, the channel -> PermanentDeliveryError, and the
          FIRST one says so in its own line.
        """
        status = int(response.status_code)
        payload = _response_payload(response)

        if status in _EMAIL_ACCEPTED_STATUSES:
            # THE FORENSIC HANDLE. A message accepted here and never
            # seen in a mailbox is invisible in every log we own; the
            # provider's id is the only thing that can be looked up in
            # its dashboard afterwards. Logged on BOTH paths -- an id
            # that only appears on success is missing exactly when
            # something went wrong. The recipient address is
            # deliberately absent: recipient_id already leads to it
            # through the database, and comms logs carry no personal
            # data today.
            logger.info(
                "email_accepted",
                notification_id=str(notification.id),
                delivery_id=str(delivery.id),
                recipient_id=str(delivery.recipient_id),
                provider_message_id=payload.get("id"),
            )
            return True

        # TWO TEXTS, ON PURPOSE. `message` CLASSIFIES and is read from
        # the JSON body only, exactly as before: the sender-fault
        # markers are substrings, and a proxy's HTML page that happens
        # to say "sandbox" must not declare a live channel dead.
        # `reason` REPORTS: the provider's own words on ANY body shape,
        # sanitized where it enters -- the service layer's logs print
        # the exception text without sanitizing it -- or None when the
        # provider sent nothing to read.
        message = str(payload.get("message") or payload.get("text") or "")
        reason = _provider_reason(response, payload)

        if status == 429:
            # The provider's own wait, honored through the existing
            # deferral mechanism (a second cap of our own would be a
            # second way to express one thing). Without a named wait it
            # degrades to an ordinary transient failure.
            retry_after = _retry_after_seconds(response)
            if retry_after is not None:
                raise RateLimitedError(
                    retry_after, _reason_tail(reason, 200),
                )
            raise EmailTransientError(
                f"provider rate limit (429){_reason_tail(reason, 200)}"
            )

        if status in _EMAIL_CONFIG_STATUSES or (
            status == 400 and _looks_like_sender_fault(message)
        ):
            self._fail_configured(status, reason, notification, delivery)

        if status >= 500:
            raise EmailTransientError(
                f"provider error ({status}){_reason_tail(reason, 200)}"
            )

        # Everything else -- a 400 on the recipient, and any 4xx whose
        # body did not name the sender -- is ONE message failing.
        # Deliberately conservative: mistaking a channel fault for a
        # single message costs one email, the opposite declares a live
        # channel dead on an unparsed reply. The body is logged so the
        # case stays visible.
        logger.warning(
            "email_rejected",
            notification_id=str(notification.id),
            delivery_id=str(delivery.id),
            recipient_id=str(delivery.recipient_id),
            status=status,
            provider_message_id=payload.get("id"),
            provider_message=_reason_for_log(reason),
        )
        raise MessageRejectedError(
            f"provider rejected the message ({status})"
            f"{_reason_tail(reason, 200)}"
        )

    def _fail_configured(
        self,
        status: int,
        reason: str | None,
        notification: Notification,
        delivery: NotificationDelivery,
    ) -> NoReturn:
        """Raise the channel-fault class, loud once and then plain.

        NoReturn, not None: this never gives control back, and a reader
        of the caller must not have to walk the body to learn that the
        branch below it is the not-a-configuration-fault branch.
        """
        if not self._configuration_reported:
            self._configuration_reported = True
            logger.error(
                "email_channel_not_viable",
                channel=DeliveryChannel.EMAIL,
                status=status,
                provider_message=_reason_for_log(reason),
                detail=(
                    "the provider refused on configuration, not on this "
                    "message: every email will fail the same way until "
                    "the deploy's email settings are corrected"
                ),
            )
        logger.warning(
            "email_rejected",
            notification_id=str(notification.id),
            delivery_id=str(delivery.id),
            recipient_id=str(delivery.recipient_id),
            status=status,
            provider_message=_reason_for_log(reason),
        )
        raise ConfigurationError(
            f"provider refused on configuration ({status})"
            f"{_reason_tail(reason, 200)}"
        )


# ===================================================================
# LLMFormatter (P5-1)
# ===================================================================


@dataclass(frozen=True)
class LLMReply:
    """A model's answer as the provider gave it, and who gave it.

    `body` is the provider's HTTP response body, decoded, untouched --
    the product parses it (spec §14.8). `provider` and `model` are what
    the RESPONSE names (spec §12.1, condition 4), None when it names
    nothing -- never the requested model.
    """

    body: str = field(repr=False)
    set_name: str
    provider: str | None
    model: str | None
    # SHA-256 hex of the audio body sent with the letter (P5-2), or None.
    audio_hash: str | None = None


class ModelChannel(Protocol):
    """The llm channel as the service calls it: asked with a letter and
    the job's ordered key sets; an answer or a channel exception."""

    async def ask(
        self,
        letter: Mapping[str, Any],
        set_names: Sequence[str],
        audio: Mapping[str, Any] | None = None,
    ) -> LLMReply:
        """Ask the model, going down the key sets on transport failures.
        With `audio` (P5-2), the body is fetched first, once, and sent to
        every set the call reaches.

        Raises:
            RateLimitedError: every set named a wait (the shortest).
            LLMTransientError: some set failed in a retryable way.
            MessageRejectedError: no set took this request.
            ConfigurationError: every set refused on configuration.
        """
        ...


# The routing block of a GATEWAY set (spec §12.1, conditions 1 and 2):
# no silent fallback of the gateway to another provider, and only
# providers that do not collect the data. In EVERY request of a gateway
# set and in none of a direct set -- a vendor refuses a parameter it
# does not know ("Unrecognized request argument", 400).
LLM_GATEWAY_ROUTING: dict[str, Any] = {
    "allow_fallbacks": False,
    "data_collection": "deny",
}

# The path of the chat completions protocol under a set's endpoint.
_LLM_PATH = "/chat/completions"

# HTTP statuses that mean the SET is wrong, not this request: the key
# (401, 403), the credit on the key (402 -- spec §14.12 item 6: "a
# configuration failure, the channel is dead, fix the deploy"), the
# model the set names (404). A redirect means the endpoint is wrong.
_LLM_CONFIG_STATUSES = frozenset({401, 402, 403, 404})
# Statuses a retry can fix besides 5xx.
_LLM_TRANSIENT_STATUSES = frozenset({408, 409})


class _SetOutcome(StrEnum):
    """What one key set's call came to, short of an answer."""

    RATE_LIMITED = "rate_limited"
    TRANSIENT = "transient"
    REJECTED = "rejected"
    CONFIGURATION = "configuration"


@dataclass(frozen=True)
class _SetFailure:
    set_name: str
    outcome: _SetOutcome
    # What the provider said, as the journal may keep it: the status and
    # the provider's error code and type -- never its error message,
    # which may quote the request, and the request is the letter.
    text: str
    retry_after: float | None = None


def build_llm_request(
    letter: Mapping[str, Any],
    key_set: LLMKeySet,
    audio: tuple[str, str] | None = None,
) -> dict[str, Any]:
    """The chat completions request for one key set.

    The letter's parameters first; then what the TRANSPORT writes, over
    them: the set's model, the messages and the schema as they came, and
    the gateway's routing block on a gateway set. The parameters cannot
    hold those names anyway -- intake refused them
    (app/transport/events.py LLM_RESERVED_PARAMS). No `stream`: the
    answer comes whole.
    """
    request: dict[str, Any] = dict(letter.get("params") or {})
    request["model"] = key_set.model
    request["messages"] = list(letter["messages"])
    if audio is not None:
        # THE BODY GOES AS ONE MORE MESSAGE, AT THE END (P5-2): nothing in
        # the letter is searched or changed -- the list is appended to,
        # as `model` is written. The product writes its prompt knowing
        # the audio comes last. The data is base64 of the raw bytes, not
        # a data URI (the input_audio part of the protocol).
        data, audio_format = audio
        request["messages"].append({
            "role": "user",
            "content": [{
                "type": "input_audio",
                "input_audio": {"data": data, "format": audio_format},
            }],
        })
    if "response_format" in letter:
        request["response_format"] = letter["response_format"]
    if key_set.gateway:
        request["provider"] = dict(LLM_GATEWAY_ROUTING)
    return request


def _short(value: Any) -> str | None:
    """A provider's code, type, model or provider name as a short
    string, or None. Sanitized and cut: it is the provider's text."""
    if isinstance(value, bool) or value is None:
        return None
    if not isinstance(value, (str, int)):
        return None
    text = " ".join(sanitize_text(str(value)).split())[:200]
    return text or None


def _error_text(status: int, error: Any) -> str:
    """The text "status N", with the provider's error code and type when it gave
    them -- the classification's evidence, nothing of the request."""
    parts = [f"status {status}"]
    if isinstance(error, dict):
        code = _short(error.get("code"))
        if code is not None:
            parts.append(f"error code {code}")
        kind = _short(error.get("type"))
        if kind is None and isinstance(error.get("metadata"), dict):
            kind = _short(error["metadata"].get("error_type"))
        if kind is not None:
            parts.append(f"error type {kind}")
    return "; ".join(parts)


def _error_code(error: Any) -> int | None:
    """The HTTP-like code a provider put in an error object (a gateway
    reports a failure inside a 200 so -- its documented error shape is
    {"error": {"code": <number>, "message": ...}})."""
    if not isinstance(error, dict):
        return None
    code = error.get("code")
    if isinstance(code, bool):
        return None
    if isinstance(code, int):
        return code
    if isinstance(code, str) and code.isdigit():
        return int(code)
    return None


def interpret_llm_response(
    set_name: str, response: Any,
) -> LLMReply | _SetFailure:
    """One set's HTTP response -> an answer or a set failure.

    THE BOUNDARY (spec §14.4, §14.5): ANY content the model gave is an
    answer -- a refusal in words, a filtered or truncated answer, an
    answer that does not fit the schema the product asked for (comms
    does not read it). A failure is the transport's only: the status,
    an error object inside a 200, an answer that is not a chat
    completion at all.
    """
    status = int(response.status_code)
    try:
        payload = response.json()
    except Exception:
        payload = None
    error = payload.get("error") if isinstance(payload, dict) else None

    if 200 <= status < 300:
        if not isinstance(payload, dict):
            return _SetFailure(
                set_name, _SetOutcome.TRANSIENT,
                f"status {status}; the answer is not a JSON object",
            )
        if error is not None:
            # A transport failure inside a 200 (§14.5): classified by
            # the code the provider put there.
            code = _error_code(error)
            text = f"{_error_text(status, error)} (inside the answer)"
            if code in _LLM_CONFIG_STATUSES:
                return _SetFailure(set_name, _SetOutcome.CONFIGURATION, text)
            if code == 429:
                return _SetFailure(
                    set_name, _SetOutcome.RATE_LIMITED, text,
                    retry_after=_retry_after_seconds(response),
                )
            return _SetFailure(set_name, _SetOutcome.TRANSIENT, text)
        choices = payload.get("choices")
        if (
            not isinstance(choices, list)
            or not choices
            or not isinstance(choices[0], dict)
            or not isinstance(choices[0].get("message"), dict)
        ):
            return _SetFailure(
                set_name, _SetOutcome.TRANSIENT,
                f"status {status}; the answer is not a chat completion "
                f"(no choices[0].message)",
            )
        return LLMReply(
            body=response.text,
            set_name=set_name,
            provider=_short(payload.get("provider")),
            model=_short(payload.get("model")),
        )

    text = _error_text(status, error)
    if status in _LLM_CONFIG_STATUSES or 300 <= status < 400:
        return _SetFailure(set_name, _SetOutcome.CONFIGURATION, text)
    if status == 429:
        return _SetFailure(
            set_name, _SetOutcome.RATE_LIMITED, text,
            retry_after=_retry_after_seconds(response),
        )
    if status >= 500 or status in _LLM_TRANSIENT_STATUSES:
        return _SetFailure(set_name, _SetOutcome.TRANSIENT, text)
    return _SetFailure(set_name, _SetOutcome.REJECTED, text)


def fold_llm_failures(failures: Sequence[_SetFailure]) -> Exception:
    """The channel exception of a call whose every set failed.

    Every set named a wait -> RateLimitedError with the SHORTEST (the
    soonest any set takes a request again); any retryable failure, a
    wait or not -> LLMTransientError; else any refusal of the request ->
    MessageRejectedError; else every set refused on configuration ->
    ConfigurationError. The text lists every set, in order.
    """
    summary = "; ".join(
        f"set {failure.set_name!r} {failure.outcome.value}: {failure.text}"
        for failure in failures
    )
    waits = [failure.retry_after for failure in failures]
    if waits and all(
        failure.outcome is _SetOutcome.RATE_LIMITED for failure in failures
    ) and all(wait is not None for wait in waits):
        return RateLimitedError(
            min(wait for wait in waits if wait is not None), f": {summary}",
        )
    outcomes = {failure.outcome for failure in failures}
    if outcomes & {_SetOutcome.TRANSIENT, _SetOutcome.RATE_LIMITED}:
        return LLMTransientError(f"every key set failed: {summary}")
    if _SetOutcome.REJECTED in outcomes:
        return MessageRejectedError(f"no key set took the request: {summary}")
    return ConfigurationError(
        f"every key set refused on configuration: {summary}"
    )


def _body_status_failure(status: int) -> Exception:
    """The store's answer to a body link that is not 200 (P5-2). Object
    gone, link expired or refused (403 -- an S3 store's answer to an
    expired signature), any other 4xx, a redirect (not followed): the
    message's -- this body will not be fetched. 408, 429, 5xx: the
    store's weather, transient."""
    text = f"body download: status {status}"
    if status >= 500 or status in (408, 429):
        return LLMTransientError(text)
    return MessageRejectedError(text)


def _body_too_large(size: int, limit: int) -> MessageRejectedError:
    return MessageRejectedError(
        f"body download: larger than max_audio_bytes ({size} > {limit}; "
        f"not read further)"
    )


class LLMFormatter:
    """The llm channel: one OpenAI-compatible adapter (spec §14.12 item 2).

    TRANSPORT FALLBACK IS HERE, MEANING FALLBACK IS NOT (§14.12 item 1):
    the job's sets are tried in order, and the next one only when the
    previous one FAILED as a transport -- a model that answered, whatever
    it answered, ends the call. A bad answer is the product's to judge,
    with a new letter.
    """

    def __init__(
        self,
        *,
        client: Any,
        sets: Mapping[str, LLMKeySet],
        body_client: Any = None,
        body_hosts: frozenset[tuple[str, int]] = frozenset(),
    ) -> None:
        """Hold the HTTP clients, this deploy's key sets and the hosts a
        body may be fetched from (LLM_BODY_HOSTS)."""
        self._client = client
        self._sets = dict(sets)
        self._body_client = body_client
        self._body_hosts = body_hosts

    async def ask(
        self,
        letter: Mapping[str, Any],
        set_names: Sequence[str],
        audio: Mapping[str, Any] | None = None,
    ) -> LLMReply:
        """Ask through the sets in order; see ModelChannel.ask."""
        body: tuple[str, str] | None = None
        audio_hash: str | None = None
        if audio is not None:
            raw = await self._fetch_body(audio)
            audio_hash = hashlib.sha256(raw).hexdigest()
            body = (base64.b64encode(raw).decode("ascii"), audio["format"])
            del raw
        failures: list[_SetFailure] = []
        for name in set_names:
            key_set = self._sets.get(name)
            if key_set is None:
                # The job's snapshot names a set this deploy no longer
                # has (startup refuses a PROFILE that does; a job
                # accepted before a restart keeps its list).
                failures.append(_SetFailure(
                    name, _SetOutcome.CONFIGURATION,
                    "not defined in LLM_KEY_SETS on this deploy",
                ))
                continue
            outcome = await self._ask_one(letter, key_set, body)
            if isinstance(outcome, LLMReply):
                return replace(outcome, audio_hash=audio_hash)
            logger.warning(
                "llm_set_failed",
                key_set=name,
                outcome=outcome.outcome.value,
                detail=outcome.text,
            )
            failures.append(outcome)
        raise fold_llm_failures(failures)

    async def _fetch_body(self, audio: Mapping[str, Any]) -> bytes:
        """The audio body behind its signed link, whole, in memory (P5-2,
        spec §5.10). Fetched on EVERY attempt -- never stored.

        THE LINK IS USED AS IT CAME and named NOWHERE: not in an
        exception, not in a log line (the HTTP client's own request
        lines are held at WARNING, app/core/logging.py). Every failure
        reads by status or by exception class only.

        A failure of the BODY (refused, gone, expired, too large) is the
        message's: no set is asked -- every one would get the same bad
        body. A failure of the STORE is transient.
        """
        url: str = audio["url"]
        limit: int = audio["max_bytes"]
        if body_endpoint(url) not in self._body_hosts:
            # Intake checked the host; the list changed since (a restart
            # with another .env). The deploy's, not the message's.
            raise ConfigurationError(
                "body download: the link's host is not in LLM_BODY_HOSTS "
                "on this deploy"
            )
        try:
            async with asyncio.timeout(LLM_BODY_DOWNLOAD_TIMEOUT):
                async with self._body_client.stream("GET", url) as response:
                    status = int(response.status_code)
                    if status != 200:
                        raise _body_status_failure(status)
                    declared = response.headers.get("content-length", "")
                    if declared.isdigit() and int(declared) > limit:
                        raise _body_too_large(int(declared), limit)
                    received = bytearray()
                    async for chunk in response.aiter_bytes():
                        received.extend(chunk)
                        if len(received) > limit:
                            raise _body_too_large(len(received), limit)
        except TimeoutError:
            logger.warning("llm_body_download_failed", outcome="timeout")
            raise LLMTransientError(
                "body download: the store did not answer in time"
            ) from None
        except PermanentDeliveryError as exc:
            logger.warning(
                "llm_body_download_failed",
                outcome="refused",
                detail=str(exc),
            )
            raise
        except LLMTransientError as exc:
            logger.warning(
                "llm_body_download_failed", outcome="transient", detail=str(exc),
            )
            raise
        except Exception as exc:
            # Class only, never the text: an HTTP client's exception may
            # carry the request, and the request is the signed link.
            logger.warning(
                "llm_body_download_failed",
                outcome="transient",
                error=type(exc).__name__,
            )
            raise LLMTransientError(
                f"body download failed ({type(exc).__name__})"
            ) from None
        logger.info("llm_body_downloaded", size=len(received))
        return bytes(received)

    async def _ask_one(
        self,
        letter: Mapping[str, Any],
        key_set: LLMKeySet,
        body: tuple[str, str] | None,
    ) -> LLMReply | _SetFailure:
        content = json.dumps(
            build_llm_request(letter, key_set, body), ensure_ascii=False,
        ).encode("utf-8")
        try:
            response = await self._client.post(
                f"{key_set.endpoint}{_LLM_PATH}",
                content=content,
                headers={
                    "Authorization": f"Bearer {key_set.key}",
                    "Content-Type": "application/json",
                },
            )
        except Exception as exc:
            # Transient by construction. Class only, never the text: an
            # exception of the client may carry the request.
            arrived = _request_may_have_arrived(exc)
            return _SetFailure(
                key_set.name, _SetOutcome.TRANSIENT,
                f"request failed ({type(exc).__name__}"
                f"{'; it may have reached the provider' if arrived else ''})",
            )
        return interpret_llm_response(key_set.name, response)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# -- email helpers ----------------------------------------------------------


def _usable_email_address(value: str | None) -> str | None:
    """The recipient's address, or None when it cannot be used.

    THE TWO GATES ANSWER DIFFERENT QUESTIONS, and the asymmetry with
    _address_shape (app/core/channels.py) is deliberate -- a review
    read the two as copies of one question. The SENDER is ours: we know
    it completely, it is one value per deploy, and its full form is
    judged at startup. The RECIPIENT arrives inside a product's
    snapshot over the bus, and its SYNTAX is the provider's judgement,
    not ours: reusing _address_shape here would demand a dot in the
    domain and refuse nodot@localhost -- legal in a private deploy --
    and would take from the provider what it judges better than we do.

    So this gate keeps exactly what the provider cannot judge for us:

      - no address at all: null (the snapshot contract carries an
        explicit null for "no value") or blank (the ingest validator
        lets an untrimmed empty value through);
      - CR/LF or an inner space: header-injection shapes;
      - a LIST SEPARATOR (_EMAIL_LIST_SEPARATORS): the provider parses
        the `to` field as a list, so one snapshot value carrying a
        comma becomes TWO addressees -- and the body of a confirmation
        message opens with a one-time code. These change the NUMBER of
        recipients, not the validity of an address, which is why they
        are ours and not the provider's.

    A merely MALFORMED address passes on purpose and is judged where it
    should be: a double @ or a dotless domain reaches the provider and
    lands in its 400 class, costing one message and nobody's privacy.
    """
    if value is None:
        return None
    # A blank address cannot arrive (F1.4: ck_recipients_email_not_blank,
    # refused on both write paths); the strip only trims the edges.
    address = value.strip()
    if "@" not in address or address.startswith("@") or address.endswith("@"):
        return None
    if any(ch in address for ch in ("\r", "\n")) or " " in address:
        return None
    if any(ch in address for ch in _EMAIL_LIST_SEPARATORS):
        return None
    return address


def _address_defect(value: str | None) -> str:
    """Name the defect WITHOUT echoing the address (no PII in logs)."""
    if value is None:
        return "no address in the recipient snapshot"
    return "the address is not a usable mailbox"


def _clean_subject(value: str) -> str:
    """Collapse whitespace and drop line breaks from a subject line.

    The subject is rendered from a template whose variables come from
    the producer -- the same trust boundary that HTML escaping guards on
    the telegram side. A line break in a subject is a header-injection
    shape wherever headers are built from it, and a folded subject is
    wrong even where they are not.
    """
    return " ".join(value.split())


def _response_payload(response: Any) -> dict[str, Any]:
    """The provider's JSON body, or an empty mapping.

    A non-JSON body is normal on an infrastructure error page in front
    of the API -- it must not turn a classifiable failure into a
    traceback.
    """
    try:
        payload = response.json()
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _provider_reason(response: Any, payload: dict[str, Any]) -> str | None:
    """What the provider said about a failure, on any body shape.

    The JSON `message`, else the JSON `text`, else the RAW body -- so a
    non-JSON page (Mailgun answers 401/403 with a plain "Forbidden"), a
    JSON value that is not a mapping, and a mapping with neither key all
    still name their reason. A key that holds only whitespace counts as
    absent. Sanitized over the WHOLE text before anything cuts it, then
    whitespace-collapsed so an HTML page stays one readable line.
    Collapsing never joins two tokens, so it cannot assemble a secret
    the patterns did not see. None: the provider sent nothing readable.
    """
    for key in ("message", "text"):
        value = payload.get(key)
        if value and str(value).strip():
            text = str(value)
            break
    else:
        text = response.text
    return _clean_reason(text)


def _clean_reason(text: str) -> str | None:
    """A provider's words made safe to record, or None if there are none.

    Sanitized over the WHOLE text first, then whitespace-collapsed so a
    multi-line page or message stays one readable line. Collapsing
    never joins two tokens, so it cannot assemble a secret the patterns
    did not see. Shared by every channel that reads a provider's words,
    so they are cleaned by one piece of code.
    """
    collapsed = " ".join(sanitize_text(text).split())
    return collapsed or None


def _reason_tail(reason: str | None, limit: int) -> str:
    """The tail a failure text carries after its status.

    ": <provider's words>" or ", empty body". The empty case is told by
    its SEPARATOR, not by its words: a provider whose body literally
    reads "empty body" still arrives after a colon, so no body can pass
    for the marker. The marker never reaches classification, which
    reads the JSON message only.
    """
    if reason is None:
        return ", empty body"
    return f": {reason[:limit]}"


def _reason_for_log(reason: str | None) -> str | None:
    """provider_message for a log line: None when the body was empty,
    which no provider text can be mistaken for."""
    return None if reason is None else reason[:300]


def _retry_after_seconds(response: Any) -> float | None:
    """The wait the provider named, in seconds, if it named one."""
    raw = None
    headers = getattr(response, "headers", None)
    if headers is not None:
        raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        seconds = float(raw)
    except (TypeError, ValueError):
        # An HTTP-date form is legal here; we do not parse it. Treating
        # it as "no named wait" degrades to the ordinary transient path,
        # which is finite -- guessing a duration would not be.
        return None
    return seconds if seconds > 0 else None


def _looks_like_sender_fault(message: str) -> bool:
    """Whether a 400 blames the SENDER (the channel), not the recipient.

    Read from the provider's own text, which says what it objects to.
    A "several 400s in a row" counter was deliberately not built: it is
    state, a threshold and a flaky test for a signal the reply already
    carries.
    """
    lowered = message.lower()
    return any(marker in lowered for marker in _EMAIL_SENDER_FAULT_MARKERS)


def _request_may_have_arrived(exc: Exception) -> bool:
    """True when the request could already be at the provider.

    A connect failure never produced a message; a read timeout may
    have. The provider offers no idempotency on this endpoint, so the
    distinction cannot PREVENT a duplicate -- it only makes it
    attributable afterwards. Named by class rather than by an httpx
    import: this module must stay importable on a deploy without email.
    """
    name = type(exc).__name__
    return "Read" in name or "Pool" in name or "Remote" in name


def _escape_html_variables(variables: dict[str, Any]) -> dict[str, Any]:
    """Escape variable VALUES for ParseMode.HTML (review 1.1).

    Strings are HTML-escaped; numbers/bools/None pass through so
    numeric format specs ("{amount:,.2f}") keep working; anything else
    is stringified and escaped -- an unexpected repr must not be able
    to break entity parsing.
    """
    escaped: dict[str, Any] = {}
    for key, value in variables.items():
        if isinstance(value, str):
            escaped[key] = escape(value)
        elif isinstance(value, (int, float)) or value is None:
            escaped[key] = value
        else:
            escaped[key] = escape(str(value))
    return escaped


def build_variables(notification: Notification) -> dict[str, Any]:
    """Build template variables from notification fields and action_data.

    action_data keys are merged first, then title/body override on top
    to prevent action_data from overwriting core notification fields.
    """
    variables: dict[str, Any] = {}
    if notification.action_data:
        # Every key is a template variable: the letter carries no
        # internal keys since F1.2 (the channel left action_data), and
        # the underscore reservation that guarded them is gone (F1.3).
        variables.update(notification.action_data)
    # Core fields always win over action_data.
    variables["title"], variables["body"] = notification.letter_text()
    return variables


# ---------------------------------------------------------------------------
# Formatter registry (app/core/channels.py decides; built at startup)
# ---------------------------------------------------------------------------


# What a channel hands back for shutdown: an awaitable close with no
# arguments (aiogram's session.close, httpx's aclose).
Closer = Callable[[], Awaitable[None]]


@dataclass
class ChannelRegistry:
    """One formatter per DeliveryChannel, plus what must be closed.

    R-1: this used to hold a single `bot`, because telegram was the only
    channel owning a network resource. Email owns a second one (an HTTP
    client whose connection is REUSED across sends -- a fresh TLS
    handshake per message would eat the delivery-time budget), so the
    field became a list of closers. Each entry names what it closes, so
    a failure to close one is attributable in the log.
    """

    formatters: dict[str, ChannelFormatter]
    # The llm channel (P5-1): asked, not delivered, so it is not one of
    # the formatters -- every other DeliveryChannel is. Live: the
    # adapter; absent: UnavailableChannelFormatter, refusing like any
    # absent channel.
    model: ModelChannel
    closers: list[tuple[str, Closer]] = field(default_factory=list)


BotFactory = Callable[..., "Bot"]
HttpClientFactory = Callable[..., Any]


def channel_map(source: Settings) -> dict[str, str]:
    """State of EVERY DeliveryChannel on this deploy (the startup log).

    live / not_configured (implemented, key set empty) /
    not_implemented. Only ever called on validated settings, so a
    broken key set cannot reach here -- startup was refused before.
    """
    states, _ = evaluate_channels({
        key: getattr(source, key.lower()) for key in channel_env_keys()
    })
    return {
        channel.value: states.get(
            channel.value, ChannelState.NOT_IMPLEMENTED,
        ).value
        for channel in DeliveryChannel
    }


@dataclass
class _Built:
    """A live channel: its formatter (or, for llm, its model channel)
    and anything it must close."""

    formatter: ChannelFormatter | None = None
    model: ModelChannel | None = None
    closers: list[tuple[str, Closer]] = field(default_factory=list)


def _build_llm(source: Settings, factories: "_Factories") -> _Built:
    client = factories.llm_http_client()

    async def _close_client() -> None:
        await client.aclose()

    body_client = factories.llm_body_client()

    async def _close_body_client() -> None:
        await body_client.aclose()

    return _Built(
        model=LLMFormatter(
            client=client,
            sets=source.llm_sets,
            body_client=body_client,
            body_hosts=source.llm_body_host_set,
        ),
        closers=[
            ("llm_http_client", _close_client),
            ("llm_body_client", _close_body_client),
        ],
    )


def _build_in_app(source: Settings, factories: "_Factories") -> _Built:
    return _Built(formatter=InAppFormatter())


def _build_telegram(source: Settings, factories: "_Factories") -> _Built:
    bot = factories.bot(token=source.telegram_bot_token)

    async def _close_bot() -> None:
        # Resolved at CLOSE time, not at build time: aiogram creates the
        # aiohttp session lazily, and reaching for it here would open one
        # at worker startup on a deploy that never sends a message.
        await bot.session.close()

    return _Built(
        formatter=TelegramFormatter(
            bot=bot, bot_url=source.telegram_bot_url,
        ),
        closers=[("telegram_bot_session", _close_bot)],
    )


def _build_email(source: Settings, factories: "_Factories") -> _Built:
    client = factories.http_client()

    async def _close_client() -> None:
        await client.aclose()

    return _Built(
        formatter=EmailFormatter(
            client=client,
            api_base_url=source.email_api_base_url,
            api_key=source.email_mailgun_api_key,
            domain=source.email_mailgun_domain,
            from_address=source.email_from_address,
        ),
        closers=[("email_http_client", _close_client)],
    )


@dataclass
class _Factories:
    """Network objects a live channel needs, injectable for tests."""

    bot: BotFactory
    http_client: HttpClientFactory
    llm_http_client: HttpClientFactory
    llm_body_client: HttpClientFactory


# One builder per implemented channel (app/core/channels.py
# CHANNEL_SPECS) -- pinned by a test in both directions.
_BUILDERS: dict[str, Callable[[Settings, _Factories], _Built]] = {
    DeliveryChannel.IN_APP: _build_in_app,
    DeliveryChannel.TELEGRAM: _build_telegram,
    DeliveryChannel.EMAIL: _build_email,
    DeliveryChannel.LLM: _build_llm,
}

_UNAVAILABLE_REASONS = {
    ChannelState.NOT_CONFIGURED.value: (
        "not configured (every key of the channel is empty)"
    ),
    ChannelState.NOT_IMPLEMENTED.value: "not implemented in this service",
}


def build_formatters(
    source: Settings,
    bot_factory: BotFactory,
    http_client_factory: HttpClientFactory | None = None,
    llm_http_client_factory: HttpClientFactory | None = None,
    llm_body_client_factory: HttpClientFactory | None = None,
) -> ChannelRegistry:
    """Build the formatter of every channel from validated settings.

    Pure apart from constructing the network objects: no logging, no
    globals. Tests pass a full key set plus fake factories to get a live
    channel without the network.
    """
    factories = _Factories(
        bot=bot_factory,
        http_client=http_client_factory or _real_http_client_factory,
        llm_http_client=(
            llm_http_client_factory or _real_llm_http_client_factory
        ),
        llm_body_client=(
            llm_body_client_factory or _real_llm_body_client_factory
        ),
    )
    formatters: dict[str, ChannelFormatter] = {}
    model: ModelChannel | None = None
    closers: list[tuple[str, Closer]] = []
    for channel, state in channel_map(source).items():
        if state == ChannelState.LIVE:
            built = _BUILDERS[channel](source, factories)
            if built.model is not None:
                model = built.model
            else:
                assert built.formatter is not None
                formatters[channel] = built.formatter
            closers.extend(built.closers)
        else:
            unavailable = UnavailableChannelFormatter(
                channel, _UNAVAILABLE_REASONS[state],
            )
            if channel == DeliveryChannel.LLM:
                model = unavailable
            else:
                formatters[channel] = unavailable
    assert model is not None  # DeliveryChannel.LLM is in every map
    return ChannelRegistry(formatters=formatters, model=model, closers=closers)


_registry: ChannelRegistry | None = None


def _real_bot_factory(**kwargs: Any) -> "Bot":
    """The telegram channel's Bot, its session timed by the channel's
    spec (app/core/channels.py client_timeouts) -- see the email client
    below for why the timeout is explicit."""
    from aiogram import Bot
    from aiogram.client.session.aiohttp import AiohttpSession

    timeouts = channel_spec(DeliveryChannel.TELEGRAM).client_timeouts
    return Bot(session=AiohttpSession(timeout=timeouts["timeout"]), **kwargs)


def _real_http_client_factory() -> Any:
    """The email channel's HTTP client, one per process.

    Lazy import, same hygiene as the Bot above: one image serves every
    product, and a deploy without email must not pay for the client.

    TIMEOUTS ARE EXPLICIT, taken from the channel's spec
    (app/core/channels.py client_timeouts), and startup refuses a lane
    whose deliver timeout is not above their sum (app/core/config.py
    deliver_timeout_problems): a timeout that surfaces INSIDE the
    formatter can still be told apart -- connect failed (nothing was
    sent) versus read timed out (the message may be on its way). A
    timeout swallowed by the lane's wait_for (app/engine/service.py)
    arrives as a bare TimeoutError with that distinction lost.
    """
    import httpx

    timeouts = channel_spec(DeliveryChannel.EMAIL).client_timeouts
    return httpx.AsyncClient(timeout=httpx.Timeout(**timeouts))


def _real_llm_http_client_factory() -> Any:
    """The llm channel's HTTP client, one per process, timed by the
    channel's spec (app/core/channels.py client_timeouts): a call times
    out inside the client, below LLM_DELIVER_TIMEOUT_SECONDS, where a
    connect failure (never reached) and a read timeout (may have been
    answered and billed) stay apart."""
    import httpx

    timeouts = channel_spec(DeliveryChannel.LLM).client_timeouts
    return httpx.AsyncClient(timeout=httpx.Timeout(**timeouts))


def _real_llm_body_client_factory() -> Any:
    """The client that fetches a letter's body by its link (P5-2).

    trust_env=False: the link is fetched AS IT IS -- the same host and
    port, over the internal network, never through a proxy the
    environment names (HTTP(S)_PROXY), and with no credentials from a
    .netrc. Redirects are not followed: a link that redirects is not the
    object (app/core/channels.py LLM_BODY_HOSTS bounds where it may
    point; a redirect would leave that bound). Its numbers are
    LLM_BODY_CLIENT_TIMEOUTS.
    """
    import httpx

    return httpx.AsyncClient(
        timeout=httpx.Timeout(**LLM_BODY_CLIENT_TIMEOUTS),
        trust_env=False,
        follow_redirects=False,
    )


def llm_set_map(source: Settings) -> dict[str, dict[str, Any]]:
    """The deploy's llm key sets for the startup log: the name, the
    model and whether it is a gateway -- never the key or the address."""
    return {
        name: {"model": key_set.model, "gateway": key_set.gateway}
        for name, key_set in source.llm_sets.items()
    }


def init_formatters() -> ChannelRegistry:
    """Build this process's registry once (worker startup).

    Idempotent: a second call returns the registry already built. No
    exception is caught here -- a registry that cannot be built must
    stop the process that needs it, never degrade a channel.
    """
    global _registry
    if _registry is None:
        _registry = build_formatters(settings, _real_bot_factory)
        logger.info(
            "channel_formatters_built",
            channels=channel_map(settings),
            llm_sets=llm_set_map(settings),
        )
    return _registry


def get_formatter(channel: str) -> ChannelFormatter:
    """Get the formatter for a delivery channel.

    Builds the registry on first use when the process has not built it
    yet (the worker builds it at startup). Every DeliveryChannel but llm
    has an entry -- live or unavailable; there is no fallback. llm is
    asked through get_model_channel.
    """
    return init_formatters().formatters[channel]


def get_model_channel() -> ModelChannel:
    """The llm channel: the adapter when live, a refusal otherwise."""
    return init_formatters().model


def reset_formatters() -> None:
    """Forget the built registry (tests); the next use rebuilds it.

    Does NOT close anything the registry holds -- use
    close_formatters() on a real shutdown path.
    """
    global _registry
    _registry = None


async def close_formatters() -> None:
    """Close network resources held by the registry, then forget it.

    Called on worker/API shutdown. Every closer is attempted even when
    an earlier one fails -- one stuck resource must not strand the
    others. Safe when nothing was built, and safe when the registry has
    no closers at all (a deploy with in_app only): the loop is empty.
    """
    if _registry is not None:
        for name, close in _registry.closers:
            try:
                await close()
                logger.info("channel_resource_closed", resource=name)
            except Exception as exc:
                logger.error(
                    "channel_resource_close_failed",
                    resource=name,
                    exception=sanitized_traceback(exc),
                )
    reset_formatters()
