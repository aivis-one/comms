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
#   StubFormatter     -- logs and succeeds; used for unconfigured
#                        channels and in CHANNELS_MODE=stub.
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
# CHANNELS_MODE:
#   settings.channels_mode == "stub" -> get_formatter returns the stub
#   for EVERY channel regardless of configured credentials. Tests and
#   CI run in this mode; nothing leaves the process.
#
# EMAIL / PUSH / IN_APP:
#   Stubs in Phase 1. cbshome's EmailFormatter (SMTP+Mailgun) was NOT
#   ported -- it drags core/email.py and per-product mail config; it
#   returns with the profile work in later phases.
# =============================================================================

import re
from html import escape
from typing import TYPE_CHECKING, Any, Protocol

import structlog

from app.audience.models import Recipient
from app.core.config import settings
from app.engine.constants import DeliveryChannel
from app.engine.models import Notification, NotificationDelivery
from app.engine.template_engine import render, resolve_flag

if TYPE_CHECKING:
    from aiogram import Bot

logger = structlog.get_logger()


class PermanentDeliveryError(Exception):
    """Raised when delivery fails permanently and should not be retried.

    Examples: bot blocked by user, chat not found, recipient has no
    telegram_id.
    """


class RateLimitedError(Exception):
    """Raised when the channel says "come back later" (HTTP 429).

    Not a message failure: the channel is healthy, it just asks to
    slow down -- and tells exactly when to retry. The service layer
    defers via next_retry_at WITHOUT burning an attempt (same pattern
    as quiet hours), up to a deferral budget (Phase 2.2).
    """

    def __init__(self, retry_after: float) -> None:
        super().__init__(
            f"rate limited by channel: retry after {retry_after}s"
        )
        self.retry_after = retry_after


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


class StubFormatter:
    """Stub formatter -- logs delivery and always succeeds.

    Used in CHANNELS_MODE=stub and for channels without real
    implementations or credentials.
    """

    async def deliver(
        self,
        notification: Notification,
        delivery: NotificationDelivery,
        recipient: Recipient,
    ) -> bool:
        """Log the delivery attempt and return True."""
        logger.info(
            "stub_delivery",
            notification_id=str(notification.id),
            delivery_id=str(delivery.id),
            channel=delivery.channel,
            recipient_id=str(delivery.recipient_id),
            title=notification.title,
        )
        return True


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
            bot_url: Base URL for deep links (e.g. "https://t.me/velo_testbot").
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
            action_data: {"action": "open_practice",
                          "params": {"practice_id": "<uuid>"}}

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
            raise PermanentDeliveryError(
                f"deep link: action {action!r} carries {len(params)} "
                f"parameters ({sorted(params)}); the startapp encoding "
                f"fits at most ONE. Put composite targets behind an "
                f"opaque token on the product side."
            )

        if params:
            (value,) = params.values()
            startapp = f"{action}__{value}"
        else:
            startapp = str(action)

        if len(startapp) > _STARTAPP_MAX_LEN:
            raise PermanentDeliveryError(
                f"deep link: startapp value is {len(startapp)} chars, "
                f"the Telegram limit is {_STARTAPP_MAX_LEN}: "
                f"{startapp[:80]!r}"
            )
        if not _STARTAPP_ALLOWED_RE.fullmatch(startapp):
            raise PermanentDeliveryError(
                f"deep link: startapp value contains characters outside "
                f"[A-Za-z0-9_-]: {startapp!r}"
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
        if not recipient.telegram_id:
            raise PermanentDeliveryError("Recipient has no telegram_id")

        # Lazy import: aiogram types only needed on the real send path.
        from aiogram.enums import ParseMode
        from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
        from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

        # Trust boundary (review 1.1): the TEMPLATE is trusted and may
        # carry HTML; VARIABLE VALUES and the stored title/body are not
        # and are escaped before entering ParseMode.HTML. The RAW
        # values are kept for the button label -- a plain-text surface
        # (see the button block below).
        raw_variables = build_variables(notification)
        variables = _escape_html_variables(raw_variables)
        locale = recipient.locale or settings.default_locale

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
            else f"<b>{escape(notification.title)}</b>"
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
            else escape(notification.body)
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
                raise RateLimitedError(float(exc.retry_after)) from exc
            error_msg = str(exc).lower()
            for perm_error in _PERMANENT_ERRORS:
                if perm_error in error_msg:
                    raise PermanentDeliveryError(
                        f"Telegram permanent failure: {exc}"
                    ) from exc
            # Transient error -- service layer retries.
            raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# Secret-redaction patterns (review 1.1). The Phase 1 version truncated
# AFTER a keyword, leaking secrets that precede it, and missed DSNs
# (no "password" substring in postgresql://user:pass@host).
# Order matters: bearer first, so "Authorization: Bearer x" is not
# half-eaten by the key=value pattern.
_BEARER_RE = re.compile(r"(?i)\bbearer\s+\S+")
_URL_USERINFO_RE = re.compile(r"(://[^/\s:@]+:)[^@/\s]+(@)")
_KEYVAL_RE = re.compile(
    r"(?i)\b(password|passwd|pwd|token|secret|api[_-]?key|apikey"
    r"|authorization)\b\s*[=:]\s*\S+"
)


def sanitize_error(exc: Exception) -> str:
    """Sanitize exception message to remove potential secrets.

    Covers: bearer tokens, userinfo in URLs/DSNs, key=value / key: value
    shapes for common credential keywords. Plain messages pass through.
    """
    msg = str(exc)
    msg = _BEARER_RE.sub("bearer [redacted]", msg)
    msg = _URL_USERINFO_RE.sub(r"\1[redacted]\2", msg)
    msg = _KEYVAL_RE.sub(lambda m: f"{m.group(1)}=[redacted]", msg)
    return msg[:2000]


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
        for key, value in notification.action_data.items():
            # Skip internal keys (prefixed with underscore).
            if not key.startswith("_"):
                variables[key] = value
    # Core fields always win over action_data.
    variables["title"] = notification.title
    variables["body"] = notification.body
    return variables


# ---------------------------------------------------------------------------
# Formatter registry + lazy init (cbshome pattern + CHANNELS_MODE gate)
# ---------------------------------------------------------------------------

_stub = StubFormatter()
_initialized = False

# Real aiogram Bot created by _init_formatters -- kept so its aiohttp
# session can be closed on shutdown (review 1.1: unclosed session).
_bot: "Bot | None" = None

_FORMATTERS: dict[str, ChannelFormatter] = {
    DeliveryChannel.TELEGRAM: _stub,
    DeliveryChannel.EMAIL: _stub,
    DeliveryChannel.PUSH: _stub,
    DeliveryChannel.IN_APP: _stub,
}


def _init_formatters() -> None:
    """Initialize real formatters based on config values.

    Called once on first get_formatter() call in CHANNELS_MODE=real.
    Uses StubFormatter when credentials are not configured.
    """
    global _bot, _initialized

    # -- Telegram --
    token = settings.telegram_bot_token
    if token:
        try:
            from aiogram import Bot

            bot = Bot(token=token)
            _bot = bot
            _FORMATTERS[DeliveryChannel.TELEGRAM] = TelegramFormatter(
                bot=bot,
                bot_url=settings.telegram_bot_url,
            )
            logger.info("telegram_formatter_initialized")
        except Exception:
            logger.exception("telegram_formatter_init_failed")
    else:
        logger.info("telegram_formatter_stub", reason="no bot token")

    # -- Email / Push / In-app: stubs in Phase 1 --

    _initialized = True


def get_formatter(channel: str) -> ChannelFormatter:
    """Get the formatter for a delivery channel.

    In CHANNELS_MODE=stub every channel resolves to the stub -- nothing
    is ever sent (tests / CI). In "real" mode, formatters are lazily
    initialized on first call; unknown channels fall back to the stub.
    """
    if settings.channels_mode == "stub":
        return _stub

    global _initialized
    if not _initialized:
        _init_formatters()
    return _FORMATTERS.get(channel, _stub)


def reset_formatters() -> None:
    """Reset lazy-initialized formatters to stubs (tests).

    Does NOT close a live Bot session -- use close_formatters() on a
    real shutdown path.
    """
    global _initialized
    _initialized = False
    for channel in _FORMATTERS:
        _FORMATTERS[channel] = _stub


async def close_formatters() -> None:
    """Close network resources held by real formatters, then reset.

    Called on worker/API shutdown. Safe to call when nothing was
    initialized (stub mode): no-op apart from the reset.
    """
    global _bot
    if _bot is not None:
        try:
            await _bot.session.close()
            logger.info("telegram_bot_session_closed")
        except Exception:
            logger.exception("telegram_bot_session_close_failed")
        _bot = None
    reset_formatters()
