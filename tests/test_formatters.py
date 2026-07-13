# =============================================================================
# COMMS Service -- Formatter tests
# =============================================================================
# Handoff item 4:
#   - CHANNELS_MODE=stub keeps every channel local
#   - format_deep_link ported as-is from velo (same three cases as
#     velo's TestTelegramFormatter)
#   - telegram deliver: HTML message, deep-link button, permanent
#     failure mapping to PermanentDeliveryError
# Review 1.1: HTML escaping (trust boundary), render-error fallback,
#   secret sanitizer, bot session close
# =============================================================================

from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from aiogram.exceptions import TelegramAPIError

from app.audience.models import Recipient
from app.engine.constants import DeliveryChannel
from app.engine.formatters import (
    PermanentDeliveryError,
    StubFormatter,
    TelegramFormatter,
    close_formatters,
    get_formatter,
    sanitize_error,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.registry import registry
from tests.helpers import next_telegram_id

BOT_URL = "https://t.me/comms_testbot"


class _FakeBot:
    """Minimal aiogram Bot stand-in recording send_message kwargs."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[dict[str, Any]] = []

    async def send_message(self, **kwargs: Any) -> Any:
        if self.error is not None:
            raise self.error
        self.calls.append(kwargs)
        return SimpleNamespace(message_id=1)


def _notification(**overrides: Any) -> Notification:
    """Transient Notification for formatter unit tests (no DB)."""
    defaults: dict[str, Any] = {
        "type": "unit_event",
        "title": "Hello",
        "body": "World",
        "target_type": "user",
        "target_value": "*",
        "action_data": None,
    }
    defaults.update(overrides)
    return Notification(**defaults)


def _delivery(**overrides: Any) -> NotificationDelivery:
    """Transient delivery row for formatter unit tests (no DB)."""
    defaults: dict[str, Any] = {"channel": "telegram", "channel_options": None}
    defaults.update(overrides)
    return NotificationDelivery(**defaults)


def _recipient(**overrides: Any) -> Recipient:
    """Transient recipient for formatter unit tests (no DB)."""
    defaults: dict[str, Any] = {
        "telegram_id": next_telegram_id(),
        "locale": "en",
        "active": True,
    }
    defaults.update(overrides)
    return Recipient(**defaults)


class TestChannelsMode:
    """CHANNELS_MODE=stub keeps everything local (handoff item 4)."""

    def test_stub_mode_returns_stub_for_all_channels(self) -> None:
        """Default test mode: every channel is the stub."""
        for channel in DeliveryChannel:
            assert isinstance(get_formatter(channel), StubFormatter)

    def test_stub_mode_ignores_real_credentials(self) -> None:
        """Even with a token configured, stub mode stays local."""
        with patch(
            "app.engine.formatters.settings"
        ) as mock_settings:
            mock_settings.channels_mode = "stub"
            mock_settings.telegram_bot_token = "123456:real-looking-token"
            assert isinstance(
                get_formatter(DeliveryChannel.TELEGRAM), StubFormatter,
            )


class TestFormatDeepLink:
    """format_deep_link ported as-is from velo -- velo's own cases."""

    def test_with_params(self) -> None:
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        link = fmt.format_deep_link({
            "action": "open_practice",
            "params": {"practice_id": "abc-123"},
        })
        assert link == f"{BOT_URL}?startapp=open_practice__abc-123"

    def test_action_only(self) -> None:
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        link = fmt.format_deep_link({"action": "dashboard"})
        assert link == f"{BOT_URL}?startapp=dashboard"

    def test_none_action_data(self) -> None:
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        assert fmt.format_deep_link(None) is None

    def test_no_action_key(self) -> None:
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        assert fmt.format_deep_link({"params": {"x": 1}}) is None


class TestTelegramDeliver:
    """Send path merged from velo (message building) + cbshome contract."""

    async def test_success_builds_html_message(self) -> None:
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        recipient = _recipient()

        ok = await fmt.deliver(_notification(), _delivery(), recipient)

        assert ok is True
        (call,) = bot.calls
        assert call["chat_id"] == recipient.telegram_id
        assert "<b>Hello</b>" in call["text"]
        assert "World" in call["text"]
        assert call["disable_web_page_preview"] is True
        assert call["disable_notification"] is False
        assert call["reply_markup"] is None

    async def test_deep_link_becomes_inline_button(self) -> None:
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = _notification(
            action_data={
                "action": "open_practice",
                "params": {"practice_id": "p1"},
            },
        )
        delivery = _delivery(channel_options={"button_text": "View"})

        await fmt.deliver(notification, delivery, _recipient())

        (call,) = bot.calls
        keyboard = call["reply_markup"]
        assert keyboard is not None
        button = keyboard.inline_keyboard[0][0]
        assert button.text == "View"
        assert button.url == f"{BOT_URL}?startapp=open_practice__p1"

    async def test_channel_options_overrides(self) -> None:
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        delivery = _delivery(
            channel_options={"disable_preview": False, "silent": True},
        )

        await fmt.deliver(_notification(), delivery, _recipient())

        (call,) = bot.calls
        assert call["disable_web_page_preview"] is False
        assert call["disable_notification"] is True

    async def test_template_rendered_for_locale(self) -> None:
        """ru recipient gets the ru template (registry-driven)."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        recipient = _recipient(locale="ru")

        await fmt.deliver(_notification(), _delivery(), recipient)

        (call,) = bot.calls
        # ru template: "RU: {body}" -- body variable is the stored body.
        assert "RU: World" in call["text"]

    async def test_missing_template_falls_back_to_stored(self) -> None:
        """Type without templates sends stored title/body."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = _notification(type="unit_rem_24h", title="R", body="!")

        await fmt.deliver(notification, _delivery(), _recipient())

        (call,) = bot.calls
        assert call["text"] == "<b>R</b>\n\n!"

    async def test_no_telegram_id_is_permanent(self) -> None:
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        with pytest.raises(PermanentDeliveryError):
            await fmt.deliver(
                _notification(), _delivery(), _recipient(telegram_id=None),
            )

    async def test_blocked_bot_is_permanent(self) -> None:
        """velo _PERMANENT_ERRORS substring -> PermanentDeliveryError."""
        error = TelegramAPIError(
            method=SimpleNamespace(),  # type: ignore[arg-type]
            message="Forbidden: bot was blocked by the user",
        )
        fmt = TelegramFormatter(bot=_FakeBot(error=error), bot_url=BOT_URL)
        with pytest.raises(PermanentDeliveryError):
            await fmt.deliver(_notification(), _delivery(), _recipient())

    async def test_transient_error_reraised(self) -> None:
        """Non-permanent Telegram errors bubble up for retry."""
        error = TelegramAPIError(
            method=SimpleNamespace(),  # type: ignore[arg-type]
            message="Too Many Requests: retry after 5",
        )
        fmt = TelegramFormatter(bot=_FakeBot(error=error), bot_url=BOT_URL)
        with pytest.raises(TelegramAPIError):
            await fmt.deliver(_notification(), _delivery(), _recipient())


class TestHtmlEscaping:
    """Review 1.1 critical: legit < & > must not kill delivery;
    templates stay trusted, variables and stored values do not."""

    async def test_stored_fallback_is_escaped(self) -> None:
        """No template -> stored title/body escaped into HTML shell."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = _notification(
            type="unit_rem_24h",  # no template registered
            title="Баланс < 0 & просрочен",
            body="a > b & c",
        )

        ok = await fmt.deliver(notification, _delivery(), _recipient())

        assert ok is True
        (call,) = bot.calls
        assert call["text"] == (
            "<b>Баланс &lt; 0 &amp; просрочен</b>\n\na &gt; b &amp; c"
        )

    async def test_variable_injection_neutralized(self) -> None:
        """Markup smuggled through action_data renders inert."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = _notification(
            action_data={"extra": "<a href='//evil'>x</a>"},
        )

        await fmt.deliver(notification, _delivery(), _recipient())

        (call,) = bot.calls
        assert "<a href" not in call["text"]
        assert "&lt;a href=&#x27;//evil&#x27;&gt;x&lt;/a&gt;" in call["text"]

    async def test_template_html_preserved(self) -> None:
        """Trusted template markup survives; its variables are escaped."""
        registry.register_templates(
            "en",
            {"unit_event": {"telegram": {"body": "<i>{body}</i>"}}},
        )
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = _notification(body="5 < 7")

        await fmt.deliver(notification, _delivery(), _recipient())

        (call,) = bot.calls
        assert "<i>5 &lt; 7</i>" in call["text"]

    async def test_numeric_variables_keep_format_specs(self) -> None:
        """Numbers pass escaping untouched so {x:,.2f} keeps working."""
        registry.register_templates(
            "en",
            {"unit_event": {"telegram": {"body": "sum: {amount:,.2f}"}}},
        )
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = _notification(action_data={"amount": 1234.5})

        await fmt.deliver(notification, _delivery(), _recipient())

        (call,) = bot.calls
        assert "sum: 1,234.50" in call["text"]

    async def test_parse_entities_error_is_permanent(self) -> None:
        """Broken HTML in the template itself = config error, no retry."""
        error = TelegramAPIError(
            method=SimpleNamespace(),  # type: ignore[arg-type]
            message="Bad Request: can't parse entities: unsupported tag",
        )
        fmt = TelegramFormatter(bot=_FakeBot(error=error), bot_url=BOT_URL)
        with pytest.raises(PermanentDeliveryError):
            await fmt.deliver(_notification(), _delivery(), _recipient())


class TestRenderErrorFallback:
    """Review 1.1: broken format spec -> stored fallback, not retries."""

    async def test_bad_format_spec_falls_back_to_stored(self) -> None:
        registry.register_templates(
            "en",
            {"unit_event": {"telegram": {"body": "{extra:,.2f}"}}},
        )
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = _notification(action_data={"extra": "not-a-number"})

        ok = await fmt.deliver(notification, _delivery(), _recipient())

        assert ok is True
        (call,) = bot.calls
        # Stored body ("World") delivered instead of the broken template.
        assert "World" in call["text"]


class TestCloseFormatters:
    """Review 1.1: the aiogram session is closed on shutdown."""

    async def test_closes_tracked_bot_session(self) -> None:
        from app.engine import formatters as formatters_module

        class _FakeSession:
            def __init__(self) -> None:
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        fake_bot = SimpleNamespace(session=_FakeSession())
        formatters_module._bot = fake_bot  # simulate real-mode init
        try:
            await close_formatters()
        finally:
            formatters_module._bot = None

        assert fake_bot.session.closed is True
        assert formatters_module._bot is None

    async def test_noop_when_nothing_initialized(self) -> None:
        """Stub mode: close is a harmless reset."""
        await close_formatters()


class TestSanitizeError:
    """Review 1.1: sanitizer covers DSNs, key=value and bearer shapes."""

    def test_dsn_userinfo_redacted(self) -> None:
        msg = sanitize_error(
            Exception("connect postgresql+asyncpg://comms:s3cret@db/comms")
        )
        assert "s3cret" not in msg
        assert "://comms:[redacted]@db/comms" in msg

    def test_key_value_redacted(self) -> None:
        msg = sanitize_error(Exception("failed: token=abc123 rest"))
        assert "abc123" not in msg
        assert "token=[redacted]" in msg

    def test_bearer_before_keyword_redacted(self) -> None:
        """Secret precedes the keyword -- the Phase 1 gap (review §4)."""
        msg = sanitize_error(
            Exception("invalid Authorization: Bearer eyJhbGci.payload x")
        )
        assert "eyJhbGci" not in msg

    def test_plain_message_untouched(self) -> None:
        assert sanitize_error(Exception("plain failure")) == "plain failure"
