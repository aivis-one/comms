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
#   - format_deep_link is PORTED AS-IS from velo (handoff item 4);
#     only the bot URL source moved to comms config
#     (settings.telegram_bot_url). Everything else around deep links
#     is Phase 3 scope and deliberately untouched.
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
from app.engine.template_engine import render

if TYPE_CHECKING:
    from aiogram import Bot

logger = structlog.get_logger()


class PermanentDeliveryError(Exception):
    """Raised when delivery fails permanently and should not be retried.

    Examples: bot blocked by user, chat not found, recipient has no
    telegram_id.
    """


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

        PORTED AS-IS from velo (Phase 7.3). Deep-link work beyond this
        transfer is Phase 3 scope.

        Format: {bot_url}?startapp={action}__{param1}_{param2}

        Args:
            action_data: {"action": "open_practice",
                          "params": {"practice_id": "uuid"}}

        Returns:
            Deep link URL or None if no action_data.
        """
        if not action_data:
            return None

        action = action_data.get("action")
        if not action:
            return None

        params = action_data.get("params", {})
        if params:
            param_str = "_".join(str(v) for v in params.values())
            return f"{self._bot_url}?startapp={action}__{param_str}"

        return f"{self._bot_url}?startapp={action}"

    async def deliver(
        self,
        notification: Notification,
        delivery: NotificationDelivery,
        recipient: Recipient,
    ) -> bool:
        """Send a Telegram message to the recipient.

        Renders title/body templates for the recipient's locale
        (falling back to the stored values), builds an HTML message,
        and attaches an inline deep-link button when action_data
        carries an action.

        Raises:
            PermanentDeliveryError: If the recipient has no telegram_id
                or the Telegram API reports a permanent failure.
        """
        if not recipient.telegram_id:
            raise PermanentDeliveryError("Recipient has no telegram_id")

        # Lazy import: aiogram types only needed on the real send path.
        from aiogram.enums import ParseMode
        from aiogram.exceptions import TelegramAPIError
        from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

        # Trust boundary (review 1.1): the TEMPLATE is trusted and may
        # carry HTML; VARIABLE VALUES and the stored title/body are not
        # and are escaped before entering ParseMode.HTML.
        variables = _escape_html_variables(build_variables(notification))
        locale = recipient.locale or settings.default_locale

        rendered_title = render(
            notification_type=notification.type,
            channel=DeliveryChannel.TELEGRAM,
            field="title",
            locale=locale,
            variables=variables,
        )
        title = (
            rendered_title
            if rendered_title is not None
            else escape(notification.title)
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

        text = f"<b>{title}</b>\n\n{body}"

        # Build inline keyboard if a deep link is available.
        deep_link = self.format_deep_link(notification.action_data)
        channel_options = delivery.channel_options
        keyboard = None
        if deep_link:
            button_text = "Open"
            if channel_options and "button_text" in channel_options:
                button_text = channel_options["button_text"]
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=button_text, url=deep_link)]
                ]
            )

        # Apply channel_options overrides.
        disable_preview = True
        silent = False
        if channel_options:
            disable_preview = channel_options.get("disable_preview", True)
            silent = channel_options.get("silent", False)

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
