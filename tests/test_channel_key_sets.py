# =============================================================================
# COMMS Service -- The channel key-set rule (R-0)
# =============================================================================
#
# A channel is decided by its own set of settings keys and by nothing
# else (app/core/channels.py):
#   every key empty -> the deploy has no such channel; the service
#                      starts, and a REQUEST for the channel fails
#                      permanently and loudly
#   every key set   -> the channel is live
#   partial set or a malformed value -> startup is REFUSED, naming the
#                      channel and the keys
#
# WHAT THIS FILE COVERS, beyond the table above:
#   - the full state enumeration for the telegram key set, including
#     whitespace-only values and both empty (a legal deploy);
#   - THE THREE DOUBLE AXES on both inputs this change touches, the key
#     set (parse) and the channel lookup (formatter choice): REPEAT,
#     EMPTY, SHORTFALL;
#   - the refusal TEXT, checked the way an integrator meets it: a
#     subprocess importing the settings, no traceback, keys named;
#   - one delivery FAILED while its neighbour in the same pass is
#     delivered -- the refusal is raised by the formatter, so the pass
#     is not broken (app/engine/service.py is untouched by this change);
#   - the consumer slice: a deploy whose env has a FULL telegram set
#     plus the removed switch as a leftover variable keeps exactly the
#     channel it had before.
#
# NOTHING HERE GOES TO THE NETWORK: a live channel is only ever built
# with an injected fake Bot factory, and conftest's tripwire fails any
# test that reaches the real Telegram transport.
# =============================================================================

import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.channels import (
    CHANNEL_SPECS,
    ChannelState,
    channel_env_keys,
    evaluate_channels,
)
from app.core.config import Settings
from app.engine.constants import (
    DeliveryChannel,
    DeliveryStatus,
    NotificationStatus,
    TargetType,
)
from app.engine.formatters import (
    InAppFormatter,
    PermanentDeliveryError,
    UnavailableChannelFormatter,
    build_formatters,
    channel_map,
    get_formatter,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import process_pending_notifications
from app.engine.service import create_notification
from tests.helpers import create_recipient, intake_fields

REPO_ROOT = Path(__file__).resolve().parents[1]

_GOOD_TOKEN = "123456:unit-test-bot-token"
_GOOD_URL = "https://t.me/unit_test_bot"

# A production deploy that has telegram. Tests knock single keys out.
_WITH_TELEGRAM: dict[str, Any] = {
    "app_env": "production",
    "database_url": "postgresql+asyncpg://u:p@db/comms_unit",
    "comms_service_token": "unit-test-service-token",
    "telegram_bot_token": _GOOD_TOKEN,
    "telegram_bot_url": _GOOD_URL,
}


def _settings(**overrides: str) -> Settings:
    """Settings built from explicit kwargs -- never from a stray .env."""
    kwargs: dict[str, Any] = {
        "_env_file": None, **_WITH_TELEGRAM, **overrides,
    }
    return Settings(**kwargs)


class _FakeBot:
    """aiogram Bot stand-in: has a token, reaches no network."""

    def __init__(self, token: str = _GOOD_TOKEN) -> None:
        self.token = token


def _fake_factory(**kwargs: Any) -> _FakeBot:
    return _FakeBot(**kwargs)


# ---------------------------------------------------------------------------
# The rule itself: every state of the telegram key set
# ---------------------------------------------------------------------------


class TestKeySetStates:
    """The enumeration, state by state, on the parse function."""

    def test_both_empty_is_a_deploy_without_the_channel(self) -> None:
        states, problems = evaluate_channels({
            "TELEGRAM_BOT_TOKEN": "", "TELEGRAM_BOT_URL": "",
        })
        assert problems == []
        assert states["telegram"] == ChannelState.NOT_CONFIGURED

    def test_both_set_is_live(self) -> None:
        states, problems = evaluate_channels({
            "TELEGRAM_BOT_TOKEN": _GOOD_TOKEN,
            "TELEGRAM_BOT_URL": _GOOD_URL,
        })
        assert problems == []
        assert states["telegram"] == ChannelState.LIVE

    @pytest.mark.parametrize(
        ("values", "named", "not_named"),
        [
            (
                {"TELEGRAM_BOT_TOKEN": _GOOD_TOKEN, "TELEGRAM_BOT_URL": ""},
                "TELEGRAM_BOT_URL",
                "TELEGRAM_BOT_TOKEN",
            ),
            (
                {"TELEGRAM_BOT_TOKEN": "", "TELEGRAM_BOT_URL": _GOOD_URL},
                "TELEGRAM_BOT_TOKEN",
                "TELEGRAM_BOT_URL",
            ),
        ],
    )
    def test_partial_set_names_exactly_the_missing_key(
        self, values: dict[str, str], named: str, not_named: str,
    ) -> None:
        """SHORTFALL on the parse: one key short, and the message names
        THAT key -- naming both would send an integrator to a key that
        is already correct."""
        states, problems = evaluate_channels(values)
        assert "telegram" not in states
        (problem,) = problems
        assert f"missing: {named}" in problem
        assert f"missing: {not_named}" not in problem

    @pytest.mark.parametrize(
        "token",
        [
            "not-a-telegram-token",  # no ':' at all
            "abc:secret",            # bot id is not digits
            ":secret",               # empty bot id
        ],
    )
    def test_malformed_token_is_a_refusal(self, token: str) -> None:
        _, problems = evaluate_channels({
            "TELEGRAM_BOT_TOKEN": token, "TELEGRAM_BOT_URL": _GOOD_URL,
        })
        (problem,) = problems
        assert "TELEGRAM_BOT_TOKEN is malformed" in problem

    @pytest.mark.parametrize(
        "url",
        [
            "http://t.me/unit_test_bot",     # not https
            "https://t.me",                  # no bot segment
            "https://t.me/a/b",              # two segments
            "unit_test_bot",                 # not a URL
        ],
    )
    def test_malformed_url_is_a_refusal(self, url: str) -> None:
        _, problems = evaluate_channels({
            "TELEGRAM_BOT_TOKEN": _GOOD_TOKEN, "TELEGRAM_BOT_URL": url,
        })
        (problem,) = problems
        assert "TELEGRAM_BOT_URL is malformed" in problem

    @pytest.mark.parametrize(
        "value", [" ", "   ", "\t", "\n"],
    )
    def test_whitespace_only_is_garbage_not_empty(
        self, value: str,
    ) -> None:
        """A space in a value is a typo, not a design decision: reading
        it as "empty" would silently drop a channel the integrator was
        trying to configure."""
        states, problems = evaluate_channels({
            "TELEGRAM_BOT_TOKEN": value, "TELEGRAM_BOT_URL": value,
        })
        assert "telegram" not in states
        (problem,) = problems
        assert "TELEGRAM_BOT_TOKEN consists only of whitespace" in problem
        assert "TELEGRAM_BOT_URL consists only of whitespace" in problem

    def test_no_key_carries_its_value_into_the_message(self) -> None:
        """Keys carry secrets: a complaint names the KEY, and a value is
        only ever echoed where it cannot be a secret (the URL shape)."""
        secret = "0000000000:absolutely-secret-value"
        _, problems = evaluate_channels({
            "TELEGRAM_BOT_TOKEN": secret, "TELEGRAM_BOT_URL": "",
        })
        assert secret not in "\n".join(problems)

    def test_every_problem_is_collected_not_just_the_first(self) -> None:
        """An integrator fixes everything in one pass, not one restart
        per typo."""
        _, problems = evaluate_channels({
            "TELEGRAM_BOT_TOKEN": "not-a-token",
            "TELEGRAM_BOT_URL": "also-not-a-url",
        })
        (problem,) = problems
        assert "TELEGRAM_BOT_TOKEN is malformed" in problem
        assert "TELEGRAM_BOT_URL is malformed" in problem


class TestChannelsWithoutKeys:
    def test_a_channel_declaring_no_keys_is_live_by_definition(
        self,
    ) -> None:
        """in_app is not an exception to the rule -- it is the rule
        applied to zero keys: the empty set is trivially complete."""
        (in_app,) = [s for s in CHANNEL_SPECS if s.name == "in_app"]
        assert in_app.keys == ()
        states, problems = evaluate_channels({})
        assert problems == []
        assert states["in_app"] == ChannelState.LIVE

    def test_an_unimplemented_channel_is_absent_everywhere(self) -> None:
        implemented = {spec.name for spec in CHANNEL_SPECS}
        absent = [c for c in DeliveryChannel if c.value not in implemented]
        assert absent, "the enum has a channel with no implementation"
        mapped = channel_map(_settings())
        for channel in absent:
            assert mapped[channel] == ChannelState.NOT_IMPLEMENTED


class TestSpecAgreesWithTheCode:
    """The spec is the single source; these pin it to both its users."""

    def test_every_declared_key_is_a_settings_field(self) -> None:
        for key in channel_env_keys():
            assert key.lower() in Settings.model_fields, key

    def test_every_channel_name_is_a_delivery_channel(self) -> None:
        values = {c.value for c in DeliveryChannel}
        for spec in CHANNEL_SPECS:
            assert spec.name in values, spec.name

    def test_every_implemented_channel_has_a_builder(self) -> None:
        from app.engine.formatters import _BUILDERS

        assert {s.name for s in CHANNEL_SPECS} == set(_BUILDERS)


# ---------------------------------------------------------------------------
# Startup: the refusal, and its text
# ---------------------------------------------------------------------------


class TestStartupRefusal:
    def test_empty_set_boots(self) -> None:
        """A product without telegram exists, and it starts."""
        settings = _settings(telegram_bot_token="", telegram_bot_url="")
        assert channel_map(settings)["telegram"] == (
            ChannelState.NOT_CONFIGURED
        )

    def test_partial_set_refuses(self) -> None:
        with pytest.raises(ValueError, match="TELEGRAM_BOT_URL"):
            _settings(telegram_bot_url="")

    def _import_settings(self, **env: str) -> subprocess.CompletedProcess[str]:
        """Import the settings in a FRESH process, as a deploy does.

        The message is only ever seen this way: settings are built at
        import, before logging exists, and the process dies there.
        """
        return subprocess.run(
            [sys.executable, "-c", "import app.core.config"],
            cwd=REPO_ROOT,
            env={
                "PATH": "/usr/bin:/bin",
                "APP_ENV": "production",
                "DATABASE_URL": "postgresql+asyncpg://u:p@db/comms_unit",
                "COMMS_SERVICE_TOKEN": "unit-test-service-token",
                **env,
            },
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )

    def test_refusal_text_is_a_message_not_a_traceback(self) -> None:
        result = self._import_settings(
            TELEGRAM_BOT_TOKEN=_GOOD_TOKEN, TELEGRAM_BOT_URL="",
        )
        assert result.returncode == 1
        output = result.stderr
        # Names the channel, names the missing key, says what to do.
        assert "telegram" in output
        assert "TELEGRAM_BOT_URL" in output
        assert "leave all of" in output
        # Not a traceback, and not pydantic's own wrapping.
        assert "Traceback" not in output
        assert "ValidationError" not in output
        assert "pydantic" not in output

    def test_refusal_text_names_the_missing_service_token(self) -> None:
        result = self._import_settings(
            COMMS_SERVICE_TOKEN="",
            TELEGRAM_BOT_TOKEN="",
            TELEGRAM_BOT_URL="",
        )
        assert result.returncode == 1
        assert "COMMS_SERVICE_TOKEN" in result.stderr
        assert "Traceback" not in result.stderr

    def test_empty_set_import_succeeds(self) -> None:
        """The pair to the assertions above: the refusal fires on a
        BROKEN set, not on every start -- an empty set imports fine."""
        result = self._import_settings(
            TELEGRAM_BOT_TOKEN="", TELEGRAM_BOT_URL="",
        )
        assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Formatter choice: the three axes on the lookup
# ---------------------------------------------------------------------------


class TestFormatterChoice:
    def test_empty_set_channel_resolves_to_the_refusal(self) -> None:
        """EMPTY on the lookup: an absent channel is never a formatter
        that succeeds."""
        registry = build_formatters(
            _settings(telegram_bot_token="", telegram_bot_url=""),
            _fake_factory,
        )
        formatter = registry.formatters[DeliveryChannel.TELEGRAM]
        assert isinstance(formatter, UnavailableChannelFormatter)

    def test_unimplemented_channel_resolves_to_the_refusal(self) -> None:
        """SHORTFALL on the lookup: no implementation, so no deploy has
        the channel -- and the lookup still answers, for every value of
        the enum."""
        registry = build_formatters(_settings(), _fake_factory)
        assert set(registry.formatters) == {c.value for c in DeliveryChannel}
        implemented = {spec.name for spec in CHANNEL_SPECS}
        for channel in DeliveryChannel:
            if channel.value in implemented:
                continue
            assert isinstance(
                registry.formatters[channel], UnavailableChannelFormatter,
            )

    def test_full_set_is_live_and_the_bot_comes_from_the_factory(
        self,
    ) -> None:
        built: list[dict[str, Any]] = []

        def _factory(**kwargs: Any) -> _FakeBot:
            built.append(kwargs)
            return _FakeBot(**kwargs)

        registry = build_formatters(_settings(), _factory)
        assert built == [{"token": _GOOD_TOKEN}]
        # R-1: this asserted `registry.bot is not None` -- the registry
        # held one network object because telegram was the only channel
        # that owned one. Email owns a second, so the field became a
        # list of named closers. The property is the same: a live
        # channel leaves the registry knowing what to close.
        assert [name for name, _ in registry.closers] == [
            "telegram_bot_session",
        ]

    def test_in_app_is_live_even_with_nothing_configured(self) -> None:
        registry = build_formatters(
            _settings(telegram_bot_token="", telegram_bot_url=""),
            _fake_factory,
        )
        assert isinstance(
            registry.formatters[DeliveryChannel.IN_APP], InAppFormatter,
        )

    def test_repeated_builds_agree(self) -> None:
        """REPEAT on the lookup: the same settings give the same map
        every time -- the answer comes from the keys, not from the order
        in which things were built."""
        settings = _settings()
        first = build_formatters(settings, _fake_factory)
        second = build_formatters(settings, _fake_factory)
        assert channel_map(settings) == channel_map(settings)
        assert {
            channel: type(formatter)
            for channel, formatter in first.formatters.items()
        } == {
            channel: type(formatter)
            for channel, formatter in second.formatters.items()
        }

    def test_get_formatter_is_stable_across_calls(self) -> None:
        """REPEAT through the process-wide registry: one build, one
        formatter instance per channel."""
        first = get_formatter(DeliveryChannel.IN_APP)
        assert get_formatter(DeliveryChannel.IN_APP) is first


class TestRefusalIsPermanent:
    async def test_deliver_raises_permanent(self) -> None:
        formatter = UnavailableChannelFormatter("telegram", "not configured")
        with pytest.raises(PermanentDeliveryError, match="telegram"):
            await formatter.deliver(
                Notification(
                    type="unit_event", title="T", body="B",
                    target_type="user", target_value="*",
                ),
                NotificationDelivery(channel="telegram"),
                None,  # type: ignore[arg-type]
            )


# ---------------------------------------------------------------------------
# The delivery path: FAILED, and only the one delivery
# ---------------------------------------------------------------------------


async def _deliveries(
    notification_id: UUID,
) -> list[NotificationDelivery]:
    from app.core.database import get_session_factory

    factory = get_session_factory()
    async with factory() as session:
        rows = await session.execute(
            select(NotificationDelivery)
            .where(NotificationDelivery.notification_id == notification_id)
            .order_by(NotificationDelivery.channel)
        )
        return list(rows.scalars().all())


async def _notification(notification_id: UUID) -> Notification:
    from app.core.database import get_session_factory

    factory = get_session_factory()
    async with factory() as session:
        row = await session.execute(
            select(Notification).where(Notification.id == notification_id)
        )
        return row.scalar_one()


class TestRequestedButUnconfigured:
    async def test_failed_immediately_without_attempts(
        self, db_session: AsyncSession,
    ) -> None:
        """The lost-notification case: the product asked for a channel
        it does not have. Not a success, not a retry -- FAILED, with no
        attempt spent, because the channel will not appear between
        attempts."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        await db_session.commit()

        assert await process_pending_notifications() == 1

        (delivery,) = await _deliveries(notification.id)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.attempts == 0
        assert delivery.sent_at is None
        assert delivery.error_message is not None
        assert "telegram" in delivery.error_message
        fresh = await _notification(notification.id)
        assert fresh.status == NotificationStatus.FAILED

    async def test_neighbour_in_the_same_pass_is_delivered(
        self, db_session: AsyncSession,
    ) -> None:
        """The refusal is raised by the FORMATTER, inside the per-
        delivery try -- so it closes one delivery and leaves the pass
        alone. (get_formatter is called outside that try: a refusal
        raised there would have broken the whole pass.) The rollup of
        the mixed result is the existing PARTIAL_SENT."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event_telegram_in_app",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        await db_session.commit()

        assert await process_pending_notifications() == 1

        by_channel = {d.channel: d for d in await _deliveries(notification.id)}
        assert by_channel[DeliveryChannel.IN_APP].status == (
            DeliveryStatus.SENT
        )
        assert by_channel[DeliveryChannel.TELEGRAM].status == (
            DeliveryStatus.FAILED
        )
        fresh = await _notification(notification.id)
        assert fresh.status == NotificationStatus.PARTIAL_SENT

    async def test_second_pass_does_not_retry_the_refusal(
        self, db_session: AsyncSession,
    ) -> None:
        """REPEAT on the delivery path: a refused delivery is terminal,
        so a second worker pass neither picks it up nor spends an
        attempt on it."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        await db_session.commit()

        await process_pending_notifications()
        assert await process_pending_notifications() == 0

        (delivery,) = await _deliveries(notification.id)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.attempts == 0

    async def test_all_channels_absent_fails_the_notification(
        self, db_session: AsyncSession,
    ) -> None:
        """EMPTY on the delivery path: every requested channel absent ->
        every delivery FAILED, and the notification with them."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event_telegram_email",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        await db_session.commit()

        await process_pending_notifications()

        deliveries = await _deliveries(notification.id)
        assert len(deliveries) == 2
        assert {d.status for d in deliveries} == {DeliveryStatus.FAILED}
        fresh = await _notification(notification.id)
        assert fresh.status == NotificationStatus.FAILED

    async def test_live_channel_delivers_through_the_same_path(
        self, db_session: AsyncSession,
    ) -> None:
        """The pair to every assertion above: the refusal is a property
        of the ABSENT channel, not of the path. A live telegram -- built
        from a full key set with a fake Bot -- delivers."""
        from app.engine.formatters import TelegramFormatter
        from tests.helpers import next_telegram_id

        recipient = await create_recipient(
            db_session, telegram_id=next_telegram_id(),
        )
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        await db_session.commit()

        class _SendingBot:
            def __init__(self) -> None:
                self.calls: list[dict[str, Any]] = []

            async def send_message(self, **kwargs: Any) -> Any:
                self.calls.append(kwargs)
                return object()

        bot = _SendingBot()
        registry = build_formatters(
            _settings(), lambda **kwargs: bot,  # type: ignore[arg-type,return-value]
        )
        assert isinstance(
            registry.formatters[DeliveryChannel.TELEGRAM], TelegramFormatter,
        )
        with patch(
            "app.engine.service.get_formatter",
            return_value=registry.formatters[DeliveryChannel.TELEGRAM],
        ):
            await process_pending_notifications()

        (delivery,) = await _deliveries(notification.id)
        assert delivery.status == DeliveryStatus.SENT
        assert len(bot.calls) == 1


# ---------------------------------------------------------------------------
# The consumer slice: a deploy that HAS telegram is untouched
# ---------------------------------------------------------------------------


class TestExistingDeployIsUnchanged:
    def test_full_set_plus_a_leftover_variable_still_lives(self) -> None:
        """Slice 2, in unit form: a deploy whose installer writes a full
        telegram key set AND the now-removed global switch as a leftover
        variable gets exactly what it had -- a live telegram on the same
        bot URL. extra="ignore" is what makes the leftover harmless;
        removing it would refuse the start of a working deploy (see the
        KNOWN CEILING in app/core/channels.py)."""
        from app.engine.formatters import TelegramFormatter

        settings = Settings(
            _env_file=None,  # type: ignore[call-arg]
            app_env="production",
            database_url="postgresql+asyncpg://u:p@db/comms_unit",
            comms_service_token="unit-test-service-token",
            telegram_bot_token=_GOOD_TOKEN,
            telegram_bot_url=_GOOD_URL,
            channels_mode="real",  # type: ignore[call-arg]
        )
        assert channel_map(settings)["telegram"] == ChannelState.LIVE
        registry = build_formatters(settings, _fake_factory)
        formatter = registry.formatters[DeliveryChannel.TELEGRAM]
        assert isinstance(formatter, TelegramFormatter)
        assert formatter._bot_url == _GOOD_URL


# ---------------------------------------------------------------------------
# The installer must not mint a state the service refuses
# ---------------------------------------------------------------------------


class TestGeneratedEnv:
    """`comms-deploy.sh install` writes an EMPTY telegram key set.

    R-0: it used to mint a placeholder token next to an empty bot URL --
    a partial set, which the new rule refuses. The product installer
    delivers both keys later, so the generated file must carry the legal
    empty state, not a half-set.
    """

    def _generate_env_body(self) -> str:
        script = (REPO_ROOT / "deploy" / "comms-deploy.sh").read_text()
        start = script.index("generate_env() {")
        return script[start : script.index("\n}\n", start)]

    def test_no_placeholder_and_the_keys_are_there_empty(self) -> None:
        """The absence assertion with its pair: "no placeholder" alone
        would also pass on a file that lost the keys entirely."""
        body = self._generate_env_body()
        # Absence.
        assert "replace-with-real" not in body
        assert "tg_placeholder" not in body
        # Its pair: every channel key present, and present EMPTY.
        # R-1: email joined the list -- an installer that mints a
        # PARTIAL set of any channel writes a state the service itself
        # refuses, and the fresh install of a product dies on it.
        from app.core.channels import channel_env_keys

        for key in channel_env_keys():
            assert f"\n{key}=\n" in body, key

    def test_service_token_is_present_and_not_empty(self) -> None:
        """The generated env declares APP_ENV=production, where the
        service token is required -- so the installer must mint one."""
        body = self._generate_env_body()
        assert "\nAPP_ENV=production\n" in body
        assert "\nCOMMS_SERVICE_TOKEN=$service_token\n" in body
        assert "service_token=$(openssl rand -hex 32)" in body


class TestWorkerBuildsAtStartup:
    def test_main_builds_the_registry_before_the_loop(self) -> None:
        """The worker is the only process that delivers, so it builds
        the registry at startup: a registry that cannot be built stops
        the worker there, instead of surfacing on the first delivery.
        """
        from app.engine import formatters as formatters_module
        from app.engine.formatters import reset_formatters

        reset_formatters()
        assert formatters_module._registry is None

        built_before_loop: list[bool] = []

        def _fake_run(coro: Any) -> None:
            coro.close()
            built_before_loop.append(
                formatters_module._registry is not None
            )

        with (
            patch("app.worker.setup_logging"),
            patch("app.worker.install_profile_from_settings"),
            patch("app.worker.asyncio.run", _fake_run),
        ):
            from app.worker import main

            main()

        assert built_before_loop == [True]
