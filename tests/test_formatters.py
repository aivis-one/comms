# =============================================================================
# COMMS Service -- Formatter tests
# =============================================================================
# Handoff item 4:
#   - an empty channel key set keeps every channel local (the refusal,
#     not a quiet success -- R-0; was the global switch's stub mode)
#   - format_deep_link ported as-is from velo (same three cases as
#     velo's TestTelegramFormatter)
#   - telegram deliver: HTML message, deep-link button, permanent
#     failure mapping to PermanentDeliveryError
# Review 1.1: HTML escaping (trust boundary), render-error fallback,
#   secret sanitizer, bot session close
# Phase 3a:
#   item 1 (+fixes A/F) -- presentation keys on the template sheet
#     (button_text / disable_preview / silent), priority sheet <
#     channel_options, per-field locale fallback, plain-text button
#   item 2 (+fix G) -- composition rule: markup source follows the
#     content source, per field (both paths + both mixed cases)
#   item 3 -- deep-link encoding: one-parameter rule, charset/64
#     validated at link build, loud PermanentDeliveryError
# =============================================================================

import base64
from types import SimpleNamespace
from typing import Any

import pytest
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter

from app.audience.models import Recipient
from app.core.config import Settings
from app.engine.constants import DeliveryChannel
from app.engine.formatters import (
    ChannelRegistry,
    InAppFormatter,
    PermanentDeliveryError,
    RateLimitedError,
    TelegramFormatter,
    UnavailableChannelFormatter,
    build_formatters,
    close_formatters,
    get_formatter,
    sanitize_error,
    sanitize_text,
)
from app.engine.models import Notification, NotificationDelivery
from app.profile.registry import registry
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


class TestLocalByDefault:
    """Nothing leaves the process unless a channel's key set is full.

    R-0: these two tests asserted the global switch's stub mode --
    "every channel is the stub" and "even a configured token stays
    local". Both were right for a switch that no longer exists. What
    replaced them: the key set decides per channel (app/core/
    channels.py), and an absent channel REFUSES instead of succeeding.
    """

    def test_empty_key_sets_keep_every_channel_local(self) -> None:
        """The suite's own configuration (conftest channel fence): only
        in_app -- zero declared keys, live by definition -- succeeds;
        every other channel resolves to the refusal, never a stub."""
        assert isinstance(get_formatter(DeliveryChannel.IN_APP), InAppFormatter)
        for channel in DeliveryChannel:
            if channel is DeliveryChannel.IN_APP:
                continue
            assert isinstance(
                get_formatter(channel), UnavailableChannelFormatter,
            )

    def test_full_key_set_makes_telegram_live_through_the_factory(
        self,
    ) -> None:
        """The successor of "stub mode ignores real credentials": a full
        key set DOES make the channel live -- and the Bot comes from the
        injected factory, so a test never builds a real one."""
        built: list[dict[str, Any]] = []

        def _factory(**kwargs: Any) -> _FakeBot:
            built.append(kwargs)
            return _FakeBot()

        full = Settings(
            _env_file=None,  # type: ignore[call-arg]
            telegram_bot_token="123456:unit-test-token",
            telegram_bot_url=BOT_URL,
        )
        registry_ = build_formatters(full, _factory)
        assert isinstance(
            registry_.formatters[DeliveryChannel.TELEGRAM], TelegramFormatter,
        )
        assert built == [{"token": "123456:unit-test-token"}]


class TestFormatDeepLink:
    """Phase 3a item 3: one-parameter rule; charset/length validated
    on the ASSEMBLED startapp value at link build; violations are
    LOUD (PermanentDeliveryError, "deep link:" prefix)."""

    def test_single_param(self) -> None:
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        link = fmt.format_deep_link({
            "action": "open_practice",
            "params": {"practice_id": "abc-123"},
        })
        assert link == f"{BOT_URL}?startapp=open_practice__abc-123"

    def test_single_uuid_param_fits(self) -> None:
        """The canonical payload -- action + one UUID -- is legal:
        36-char uuid + "__" + a short action stays under 64."""
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        uuid = "01890a5c-90bd-7e2a-b7c8-4a6f2d3e5f60"
        link = fmt.format_deep_link({
            "action": "open_practice",
            "params": {"practice_id": uuid},
        })
        assert link == f"{BOT_URL}?startapp=open_practice__{uuid}"

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

    def test_multi_param_is_loud(self) -> None:
        """The velo "_"-join is gone: two params cannot be encoded
        reversibly -> config error, not a silently broken button."""
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        with pytest.raises(PermanentDeliveryError, match=r"^deep link:"):
            fmt.format_deep_link({
                "action": "open_practice",
                "params": {"practice_id": "p1", "slot_id": "s1"},
            })

    def test_over_64_chars_is_loud(self) -> None:
        """Limit applies to the WHOLE assembled value."""
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        with pytest.raises(PermanentDeliveryError, match=r"^deep link:"):
            fmt.format_deep_link({
                "action": "a" * 40,
                "params": {"id": "b" * 30},
            })

    def test_exactly_64_chars_passes(self) -> None:
        """Boundary, not off-by-one: exactly 64 is legal."""
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        action = "a" * 30
        value = "b" * 32  # 30 + 2 ("__") + 32 = 64
        link = fmt.format_deep_link({
            "action": action, "params": {"id": value},
        })
        assert link == f"{BOT_URL}?startapp={action}__{value}"

    def test_charset_violation_is_loud(self) -> None:
        """Characters outside [A-Za-z0-9_-] anywhere in the value."""
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        with pytest.raises(PermanentDeliveryError, match=r"^deep link:"):
            fmt.format_deep_link({
                "action": "open",
                "params": {"id": "p/1?x=y"},
            })

    def test_bad_action_charset_is_loud(self) -> None:
        """The action is part of the value -- same rules apply."""
        fmt = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        with pytest.raises(PermanentDeliveryError, match=r"^deep link:"):
            fmt.format_deep_link({"action": "open practice"})


class TestTelegramDeliver:
    """Send path merged from velo (message building) + cbshome contract."""

    async def test_success_builds_html_message(self) -> None:
        """Template path (Phase 3a item 2): both fields rendered from
        the sheet -> VERBATIM, no injected <b>; the sheet owns its
        markup. Flags fall to channel defaults (no sheet flags for
        unit_event, no channel_options)."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        recipient = _recipient()

        ok = await fmt.deliver(_notification(), _delivery(), recipient)

        assert ok is True
        (call,) = bot.calls
        assert call["chat_id"] == recipient.telegram_id
        # en fixture sheet: title "{title}", body "{body} [{extra}]";
        # missing {extra} stays literal (SafeDict).
        assert call["text"] == "Hello\n\nWorld [{extra}]"
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

    async def test_invalid_button_url_is_permanent(self) -> None:
        """Review 1.2: button URL derives from immutable action_data --
        deterministic failure, a retry cannot fix it."""
        error = TelegramAPIError(
            method=SimpleNamespace(),  # type: ignore[arg-type]
            message="Bad Request: BUTTON_URL_INVALID",
        )
        fmt = TelegramFormatter(bot=_FakeBot(error=error), bot_url=BOT_URL)
        with pytest.raises(PermanentDeliveryError):
            await fmt.deliver(_notification(), _delivery(), _recipient())

    async def test_transient_error_reraised(self) -> None:
        """Non-permanent Telegram errors bubble up for retry."""
        error = TelegramAPIError(
            method=SimpleNamespace(),  # type: ignore[arg-type]
            message="Bad Gateway",
        )
        fmt = TelegramFormatter(bot=_FakeBot(error=error), bot_url=BOT_URL)
        with pytest.raises(TelegramAPIError):
            await fmt.deliver(_notification(), _delivery(), _recipient())

    async def test_rate_limit_maps_to_rate_limited_error(self) -> None:
        """Phase 2.2: a real 429 arrives as TelegramRetryAfter and
        surfaces TYPED (with the server-named wait) -- it must not
        fall through to the permanent/transient text matching."""
        error = TelegramRetryAfter(
            method=SimpleNamespace(),  # type: ignore[arg-type]
            message="Too Many Requests: retry after 42",
            retry_after=42,
        )
        fmt = TelegramFormatter(bot=_FakeBot(error=error), bot_url=BOT_URL)
        with pytest.raises(RateLimitedError) as exc_info:
            await fmt.deliver(_notification(), _delivery(), _recipient())
        assert exc_info.value.retry_after == 42.0


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

    async def test_closes_every_registered_resource(self) -> None:
        """R-0 put the Bot in the registry instead of a module global;
        R-1 replaced the single `bot` field with a list of named
        closers, because email owns a second network object. What holds
        across all three: whatever the registry knows about gets closed
        on shutdown, and the registry is forgotten afterwards."""
        from app.engine import formatters as formatters_module

        closed: list[str] = []

        def _closer(name: str) -> Any:
            async def _close() -> None:
                closed.append(name)

            return _close

        formatters_module._registry = ChannelRegistry(
            formatters={},
            closers=[
                ("telegram_bot_session", _closer("telegram")),
                ("email_http_client", _closer("email")),
            ],
        )
        try:
            await close_formatters()
        finally:
            formatters_module._registry = None

        assert closed == ["telegram", "email"]
        assert formatters_module._registry is None

    async def test_one_failing_closer_does_not_strand_the_others(
        self,
    ) -> None:
        """A stuck resource must not keep the rest open."""
        from app.engine import formatters as formatters_module

        closed: list[str] = []

        async def _boom() -> None:
            raise RuntimeError("session already gone")

        async def _ok() -> None:
            closed.append("second")

        formatters_module._registry = ChannelRegistry(
            formatters={},
            closers=[("first", _boom), ("second", _ok)],
        )
        try:
            await close_formatters()
        finally:
            formatters_module._registry = None

        assert closed == ["second"]

    async def test_noop_when_nothing_initialized(self) -> None:
        """Nothing built at all: close is a harmless reset."""
        await close_formatters()

    async def test_registry_without_closers_closes_cleanly(self) -> None:
        """The pair to the assertions above: a deploy whose only channel
        is in_app has NOTHING to close, and shutdown must not trip over
        the empty list."""
        from app.engine import formatters as formatters_module

        formatters_module._registry = ChannelRegistry(formatters={})
        await close_formatters()
        assert formatters_module._registry is None


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



# Sentinel secrets for the form tests. Each is the SHAPE the real secret
# has (comms-deploy.sh mints hex 24/32 bytes; a telegram token is
# <id>:<35 chars>; a Mailgun key is <32hex>-<8hex>-<8hex> or
# key-<32hex>) and none is a substring of any text a test surrounds it
# with, so "the sentinel is absent" can only mean "it was redacted".
_PG_PASS = "5e17" * 12  # 48 hex, openssl rand -hex 24
_REDIS_PASS = "4d0c" * 12
_SERVICE_TOKEN = "7a9b" * 16  # 64 hex, openssl rand -hex 32
# BUILT AT RUNTIME, NEVER WRITTEN WHOLE: a literal in the provider's
# shape is exactly what repository secret scanning looks for, and a
# fake key that blocks a push is a test that cannot ship. The shape the
# sanitizer sees at run time is unchanged.
_TG_SECRET = "AAH" + "sentinel" * 4  # 35 chars, the bot token's secret part
_TG_TOKEN = "8123456789:" + _TG_SECRET
_MG_KEY = "c0ffee00" * 4 + "-" + "1a2b3c4d" + "-" + "5e6f7a8b"
_MG_LEGACY = "key" + "-" + "beef0000" * 4
_BASIC = base64.b64encode(f"api:{_MG_KEY}".encode()).decode()

# (name, text as it appears in a real error, sentinel, a piece of the
# ordinary text around it that must SURVIVE). The texts are the shapes
# the error actually takes: the DSN and the redis URL exactly as
# comms-deploy.sh writes them, the telegram token inside the URL an
# aiohttp error prints (aiogram wraps it as "<class>: <error>").
_SECRET_FORMS = [
    (
        "database_url",
        f"connect failed postgresql+asyncpg://comms:{_PG_PASS}"
        "@comms-postgres:5432/comms",
        _PG_PASS,
        "@comms-postgres:5432/comms",
    ),
    (
        "redis_url_empty_user",
        f"Error connecting to redis://:{_REDIS_PASS}@comms-redis:6379/0",
        _REDIS_PASS,
        "@comms-redis:6379/0",
    ),
    (
        "service_token_bearer",
        f"invalid Authorization: Bearer {_SERVICE_TOKEN} rejected",
        _SERVICE_TOKEN,
        "rejected",
    ),
    (
        "service_token_bare",
        f"token mismatch {_SERVICE_TOKEN} rejected",
        _SERVICE_TOKEN,
        "rejected",
    ),
    (
        "password_bare",
        f"AUTH {_REDIS_PASS} called without any password configured",
        _REDIS_PASS,
        "called without any password configured",
    ),
    (
        "telegram_token_in_url",
        "ClientResponseError: 502, message='Bad Gateway', "
        f"url='https://api.telegram.org/bot{_TG_TOKEN}/sendMessage'",
        _TG_SECRET,
        "/sendMessage",
    ),
    (
        "telegram_token_bare",
        f"token {_TG_TOKEN} is invalid",
        _TG_SECRET,
        "is invalid",
    ),
    (
        "mailgun_key_bare",
        f"request with {_MG_KEY} refused",
        _MG_KEY,
        "refused",
    ),
    (
        "mailgun_key_legacy_bare",
        f"request with {_MG_LEGACY} refused",
        _MG_LEGACY,
        "refused",
    ),
    (
        "mailgun_key_basic_header",
        f"sent Authorization: Basic {_BASIC} upstream",
        _BASIC,
        "upstream",
    ),
]


class TestSanitizeForms:
    """F0-comms item 2: every form in which a configured secret exists
    is redacted -- checked on the form as it appears in an error text,
    with the ordinary text around it kept (a sanitizer that blanks the
    whole line would pass every "secret absent" check and report
    nothing)."""

    @pytest.mark.parametrize(
        ("text", "secret", "kept"),
        [form[1:] for form in _SECRET_FORMS],
        ids=[form[0] for form in _SECRET_FORMS],
    )
    def test_the_secret_goes_and_the_text_stays(
        self, text: str, secret: str, kept: str,
    ) -> None:
        cleaned = sanitize_text(text)
        assert secret not in cleaned
        assert "[redacted]" in cleaned
        assert kept in cleaned

    @pytest.mark.parametrize(
        "text",
        [form[1] for form in _SECRET_FORMS],
        ids=[form[0] for form in _SECRET_FORMS],
    )
    def test_idempotent_on_every_form(self, text: str) -> None:
        """The email path sanitizes where the provider's text enters
        and the service layer sanitizes the exception again: a second
        pass must change nothing."""
        once = sanitize_text(text)
        assert sanitize_text(once) == once

    @pytest.mark.parametrize(
        "text",
        [
            f"Authorization: Basic {_BASIC}",
            f"Authorization: Bearer {_SERVICE_TOKEN}",
            f"password=postgresql://comms:{_PG_PASS}@db/comms",
            f"url=redis://:{_REDIS_PASS}@comms-redis:6379/0",
            f"api_key={_MG_KEY}",
            f"token={_TG_TOKEN}",
        ],
    )
    def test_idempotent_where_patterns_overlap(self, text: str) -> None:
        """Two patterns reach the same span here; the second pass must
        find nothing either of them would change."""
        once = sanitize_text(text)
        assert sanitize_text(once) == once
        for secret in (_BASIC, _SERVICE_TOKEN, _PG_PASS, _REDIS_PASS,
                       _MG_KEY, _TG_SECRET):
            assert secret not in once

    def test_repeated_secret_goes_everywhere(self) -> None:
        text = f"first {_MG_KEY} then {_MG_KEY} end"
        cleaned = sanitize_text(text)
        assert _MG_KEY not in cleaned
        assert cleaned == "first [redacted] then [redacted] end"

    def test_several_forms_in_one_text(self) -> None:
        text = " ".join(form[1] for form in _SECRET_FORMS)
        cleaned = sanitize_text(text)
        for _, _, secret, kept in _SECRET_FORMS:
            assert secret not in cleaned
            assert kept in cleaned

    def test_a_raw_at_sign_in_a_password_leaves_no_tail(self) -> None:
        """The password runs to the LAST @ of the authority."""
        cleaned = sanitize_text("postgresql://comms:hun@ter2@db/comms")
        assert "ter2" not in cleaned
        assert "://comms:[redacted]@db/comms" in cleaned

    def test_an_empty_password_stays_a_well_formed_line(self) -> None:
        cleaned = sanitize_text("redis://:@comms-redis:6379/0 down")
        assert cleaned == "redis://:[redacted]@comms-redis:6379/0 down"
        assert sanitize_text(cleaned) == cleaned

    def test_empty_text_stays_empty(self) -> None:
        assert sanitize_text("") == ""

    def test_the_record_cut_never_splits_a_secret(self) -> None:
        """sanitize_error cuts at 2000 AFTER redacting: a secret that
        straddles the cut would otherwise leave its prefix behind,
        which no pattern recognises once the @ is gone."""
        text = "x" * 1985 + f" redis://:{_REDIS_PASS}@comms-redis:6379/0"
        cleaned = sanitize_error(Exception(text))
        assert len(cleaned) == 2000
        assert _REDIS_PASS[:8] not in cleaned

    @pytest.mark.parametrize(
        "text",
        [
            "basic auth failed",
            "retry at 12:30:00",
            "<20260912.1@mg.test>",
            "request 1b4e28ba-2fa1-11d2-883f-0016d3cca427 failed",
            "commit da39a3ee5e6b4b0d3255bfef95601890afd80709 built",
            "provider refused on configuration (401): Forbidden",
        ],
    )
    def test_ordinary_text_is_not_mistaken_for_a_secret(
        self, text: str,
    ) -> None:
        """The shape patterns stop short of message ids, uuids and a
        40-hex commit id -- the long-hex pattern starts at 48."""
        assert sanitize_text(text) == text


class TestComposition:
    """Phase 3a item 2 / fix G: the markup source follows the content
    source, PER FIELD. Both pure paths and both mixed cases."""

    async def test_both_from_template_verbatim(self) -> None:
        """Template path: no injected markup at all -- the sheet's own
        markup (here: none) is the whole story."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)

        await fmt.deliver(_notification(), _delivery(), _recipient())

        (call,) = bot.calls
        assert call["text"] == "Hello\n\nWorld [{extra}]"
        assert "<b>" not in call["text"]

    async def test_both_stored_keeps_service_presentation(self) -> None:
        """Stored path: untrusted content, escaped -- the SERVICE
        supplies the presentation (<b> around the title)."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = _notification(type="unit_rem_24h", title="R", body="!")

        await fmt.deliver(notification, _delivery(), _recipient())

        (call,) = bot.calls
        assert call["text"] == "<b>R</b>\n\n!"

    async def test_template_body_stored_title_bolds_only_title(self) -> None:
        """Mixed: template body + stored title -> ONLY the stored
        title gets the service <b>; the rendered body is verbatim."""
        registry.register_templates(
            "en",
            {"unit_rem_1h": {"telegram": {"body": "<i>{body}</i>"}}},
        )
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = _notification(
            type="unit_rem_1h", title="T & Co", body="soon",
        )

        await fmt.deliver(notification, _delivery(), _recipient())

        (call,) = bot.calls
        assert call["text"] == "<b>T &amp; Co</b>\n\n<i>soon</i>"

    async def test_template_title_stored_body_no_bold(self) -> None:
        """Mixed: template title + stored body -> the rendered title
        is verbatim (NOT bolded), the stored body is escaped plain."""
        registry.register_templates(
            "en",
            {"unit_rem_10m": {"telegram": {"title": "now: {title}"}}},
        )
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = _notification(
            type="unit_rem_10m", title="T", body="a & b",
        )

        await fmt.deliver(notification, _delivery(), _recipient())

        (call,) = bot.calls
        assert call["text"] == "now: T\n\na &amp; b"


class TestPresentationKeys:
    """Phase 3a item 1 / fixes A+F: the sheet owns presentation;
    channel_options is the per-delivery override (same keys)."""

    @staticmethod
    def _presented(**overrides: Any) -> Notification:
        defaults: dict[str, Any] = {
            "type": "unit_presented",
            "action_data": {"action": "dashboard"},
        }
        defaults.update(overrides)
        return _notification(**defaults)

    async def test_sheet_supplies_button_and_flags(self) -> None:
        """No channel_options: button label and both flags come from
        the en fixture sheet (button_text "Open {title}",
        disable_preview false, silent true)."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)

        await fmt.deliver(self._presented(), _delivery(), _recipient())

        (call,) = bot.calls
        button = call["reply_markup"].inline_keyboard[0][0]
        assert button.text == "Open Hello"
        assert call["disable_web_page_preview"] is False
        assert call["disable_notification"] is True

    async def test_channel_options_override_sheet(self) -> None:
        """fix A: same keys, different source -- channel_options win
        over the sheet for all three presentation keys."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        delivery = _delivery(
            channel_options={
                "button_text": "View",
                "disable_preview": True,
                "silent": False,
            },
        )

        await fmt.deliver(self._presented(), delivery, _recipient())

        (call,) = bot.calls
        assert call["reply_markup"].inline_keyboard[0][0].text == "View"
        assert call["disable_web_page_preview"] is True
        assert call["disable_notification"] is False

    async def test_locale_chain_per_field(self) -> None:
        """ru recipient: button_text comes from the ru sheet, the
        flags (absent in ru) fall back per field to en."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)

        await fmt.deliver(
            self._presented(), _delivery(), _recipient(locale="ru"),
        )

        (call,) = bot.calls
        button = call["reply_markup"].inline_keyboard[0][0]
        assert button.text == "Открыть Hello"
        assert call["disable_web_page_preview"] is False
        assert call["disable_notification"] is True

    async def test_button_renders_plain_text_unescaped(self) -> None:
        """fix F: third rendering mode -- the button label is plain
        text (RAW variables), while the same variable is escaped
        inside the ParseMode.HTML message body."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)
        notification = self._presented(title="A & B")

        await fmt.deliver(notification, _delivery(), _recipient())

        (call,) = bot.calls
        button = call["reply_markup"].inline_keyboard[0][0]
        assert button.text == "Open A & B"          # plain, no &amp;
        assert "A &amp; B" in call["text"]          # escaped in HTML

    async def test_flags_absent_fall_to_channel_defaults(self) -> None:
        """A sheet without flags (unit_event) keeps the channel
        defaults: preview disabled, not silent."""
        bot = _FakeBot()
        fmt = TelegramFormatter(bot=bot, bot_url=BOT_URL)

        await fmt.deliver(_notification(), _delivery(), _recipient())

        (call,) = bot.calls
        assert call["disable_web_page_preview"] is True
        assert call["disable_notification"] is False

    def test_registry_type_discipline(self) -> None:
        """get_template never leaks a flag as "True"; get_flag never
        reads a string as a flag (Phase 3a item 1)."""
        registry.register_templates(
            "en",
            {"unit_plain": {"telegram": {
                "silent": True, "button_text": "Go",
            }}},
        )
        assert registry.get_template(
            "en", "unit_plain", "telegram", "silent",
        ) is None
        assert registry.get_flag(
            "en", "unit_plain", "telegram", "silent",
        ) is True
        assert registry.get_flag(
            "en", "unit_plain", "telegram", "button_text",
        ) is None
        assert registry.get_template(
            "en", "unit_plain", "telegram", "button_text",
        ) == "Go"
