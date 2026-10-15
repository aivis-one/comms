# =============================================================================
# P4-3 -- a channel call is timed by its job's lane, and every lane's
# timeout is held above the client timeout of every live channel
# =============================================================================
#
# Through the real pipeline (process_lane: resolve -> deliver -> rollup)
# with every channel answering through a slow spy; the rule on real
# Settings; the clients built by the real factories; aiogram's session
# against a slow local server.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  normal's timeout for every lane (the one timeout before lanes)
#                                                   -> TestTheLaneTimesTheCall
#   M2  one fixed lane's timeout for every job      -> TestTheLaneTimesTheCall
#   M3  the lane's timeout journaled as transient    -> TestTheOutcome
#   M4  normal's default moved off 30                -> TestTheDefaults
#   M5  the rule removed from Settings               -> TestTheRule
#   M6  the rule compared with >= (equal passes)     -> TestTheRule (edge)
#   M7  the email client's timeouts written as literals
#                                                   -> TestOnePlaceForTheNumbers
#   M8  the Bot built without the spec's session     -> TestOnePlaceForTheNumbers
#   M9  the rule over every implemented channel, not the live ones
#                                                   -> TestTheRule (in_app only)
#   M10 the wait for a slot inside the timeout -- unkillable since H7: a
#       portion is one round of the slots, no call waits for one
#       (TestTheSlotIsNotTimed holds what stays)
#   M11 a key out of .env.example / the sentence out of INTEGRATION.md
#                                                   -> TestTheContract
# NOT BUILT: a call that ends exactly AT the lane's timeout -- which of
# wait_for and the call wins there is the loop's, not the code's.
# =============================================================================

import ast
import asyncio
import importlib.util
import re
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import UUID

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import select
from structlog.testing import capture_logs

import app.engine.service as service
from app.core.channels import CHANNEL_SPECS, channel_spec
from app.core.config import (
    NUMERIC_BOUNDS,
    Settings,
    format_startup_error,
    lane_deliver_timeout_variable,
    settings,
)
from app.core.database import get_session_factory
from app.core.lanes import Lane
from app.engine import journal
from app.engine.constants import (
    ChannelAnswer,
    DeliveryChannel,
    DeliveryStatus,
    JournalSubject,
    TargetType,
    WaitReason,
)
from app.engine.formatters import _real_bot_factory, _real_http_client_factory
from app.engine.models import NotificationDelivery, NotificationTransition
from app.engine.processor import process_lane
from app.engine.service import create_notification
from app.profile.registry import Decided, Layer, TypeRecord, registry
from tests.helpers import create_recipient, intake_fields

_REPO = Path(__file__).resolve().parents[1]
_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")
_ENV_EXAMPLE = (_REPO / ".env.example").read_text(encoding="utf-8")
_FORMATTERS = _REPO / "app" / "engine" / "formatters.py"

# in_app types of the fixture profile. The second has two channels
# (in_app, email) and therefore two deliveries per recipient: the slot
# test needs two calls of one job.
_ONE_CALL = "unit_event_in_app"
_TWO_CALLS = "unit_event_in_app_email"

# The margins, explicit: a call of _SLOW seconds against a short lane
# well below it and a long lane well above it.
_SLOW = 0.6
_SHORT = 0.2
_LONG = 2.0

# Assembled at run time: a literal in the shape of a bot token blocks
# the push (§0 §10).
_BOT_TOKEN = "123456" + ":" + "unit-test-lane-timeouts"

# A deploy that starts: production, so nothing is filled in for it.
_BASE = {
    "app_env": "production",
    "database_url": "postgresql+asyncpg://u:p@db/comms_unit",
    "comms_service_token": "unit-test-service-token",
}
_TELEGRAM = {
    "telegram_bot_token": _BOT_TOKEN,
    "telegram_bot_url": "https://t.me/unit_test_bot",
}
_EMAIL = {
    "email_mailgun_api_key": "unit-test-provider-key",
    "email_mailgun_domain": "mg.example.org",
    "email_from_address": "Unit <unit@example.org>",
}


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _declare(type_key: str, lane: Lane) -> None:
    """The type's record with `lane` declared (the profile layer)."""
    record = registry.record_of(type_key)
    assert record is not None
    fields = dict(record.fields)
    fields["lane"] = Decided(lane.value, Layer.PROFILE, "test")
    registry.register_type(
        type_key, category=registry.category_of(type_key),
        record=TypeRecord(fields=fields),
    )


class _Slow:
    """Every channel: answers yes after `seconds`, recording each call's
    start and end."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.calls: list[tuple[float, float]] = []

    async def deliver(self, *args: Any) -> bool:
        started = time.monotonic()
        await asyncio.sleep(self.seconds)
        self.calls.append((started, time.monotonic()))
        return True


@contextmanager
def _channel(spy: Any) -> Iterator[Any]:
    with patch("app.engine.service.get_formatter", return_value=spy):
        yield spy


def _lane_timeout(monkeypatch: pytest.MonkeyPatch, lane: Lane, seconds: float) -> None:
    monkeypatch.setattr(
        settings, f"notification_lane_{lane.value}_deliver_timeout_seconds", seconds,
    )


async def _job_in(lane: Lane, type_key: str = _ONE_CALL) -> UUID:
    """One job of `type_key`, its type declaring `lane`, for one recipient."""
    _declare(type_key, lane)
    async with get_session_factory()() as session:
        recipient = await create_recipient(session)
        job = await create_notification(
            session, **intake_fields(), type=type_key, title="T", body="B",
            target_type=TargetType.USER, target_value=str(recipient.id),
        )
        await session.commit()
        assert job.lane == lane
        return job.id


async def _deliveries(nid: UUID) -> list[NotificationDelivery]:
    async with get_session_factory()() as session:
        return list((await session.execute(
            select(NotificationDelivery)
            .where(NotificationDelivery.notification_id == nid)
            .order_by(NotificationDelivery.channel)
        )).scalars())


async def _channel_answers(nid: UUID) -> list[NotificationTransition]:
    async with get_session_factory()() as session:
        return list((await session.execute(
            select(NotificationTransition)
            .where(
                NotificationTransition.notification_id == nid,
                NotificationTransition.subject == JournalSubject.CHANNEL,
            )
            .order_by(NotificationTransition.id)
        )).scalars())


def _record_timeouts(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """The timeout of every wait_for the delivery module makes, in order
    -- the number a call is actually given."""
    seen: list[float] = []
    real = asyncio.wait_for

    async def wait_for(aw: Any, **kwargs: float) -> Any:
        seen.append(kwargs["timeout"])
        return await real(aw, **kwargs)

    monkeypatch.setattr(
        service, "asyncio",
        SimpleNamespace(**{**vars(asyncio), "wait_for": wait_for}),
    )
    return seen


def _settings(**overrides: Any) -> Settings:
    kwargs: dict[str, Any] = {"_env_file": None, **_BASE, **overrides}
    return Settings(**kwargs)


def _refusal(**overrides: Any) -> str:
    """The startup refusal a deploy with `overrides` gets, as printed."""
    with pytest.raises(ValidationError) as caught:
        _settings(**overrides)
    return format_startup_error(caught.value)


def _at(lane: Lane, seconds: int) -> dict[str, int]:
    return {f"notification_lane_{lane.value}_deliver_timeout_seconds": seconds}


# -----------------------------------------------------------------------------
# item 1 -- the lane times the call
# -----------------------------------------------------------------------------


class TestTheLaneTimesTheCall:
    """done-when 1 (1): a slow channel, through the real pipeline -- the
    interactive job is cut at its short timeout, the bulk job waits its
    long one. M1, M2."""

    async def test_short_lane_cut_long_lane_waits(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _lane_timeout(monkeypatch, Lane.INTERACTIVE, _SHORT)
        _lane_timeout(monkeypatch, Lane.BULK, _LONG)
        interactive = await _job_in(Lane.INTERACTIVE, _ONE_CALL)
        bulk = await _job_in(Lane.BULK, "unit_bare")

        with _channel(_Slow(_SLOW)) as spy:
            started = time.monotonic()
            await process_lane(Lane.INTERACTIVE)
            interactive_took = time.monotonic() - started
            await process_lane(Lane.BULK)

        (cut,) = await _deliveries(interactive)
        assert cut.status == DeliveryStatus.PENDING
        assert cut.error_message == f"Timeout after {_SHORT}s"
        # Cut at the lane's timeout, not at the call's end.
        assert interactive_took < _SLOW
        (waited,) = await _deliveries(bulk)
        assert waited.status == DeliveryStatus.SENT
        # The pair: the bulk call did run its whole length -- the slow
        # spy was really slow, and the bulk lane waited for it.
        assert [end - start >= _SLOW for start, end in spy.calls] == [True]

    async def test_each_lane_reads_its_own_timeout(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """REPEAT over the lanes: every lane's call gets that lane's
        number and no other's."""
        seen: dict[Lane, list[float]] = {}
        for number, lane in enumerate(Lane, start=31):
            _lane_timeout(monkeypatch, lane, number)
        for lane in Lane:
            nid = await _job_in(lane)
            seen[lane] = _record_timeouts(monkeypatch)
            with _channel(_Slow(0)):
                await process_lane(lane)
            assert [d.status for d in await _deliveries(nid)] == [
                DeliveryStatus.SENT,
            ]
        assert seen == {
            lane: [number] for number, lane in enumerate(Lane, start=31)
        }


class TestTheSlotIsNotTimed:
    """REPEAT: two calls of one bulk job. Each is shorter than the lane's
    timeout, the two together longer -- both go: the timeout is the
    call's, never the job's.

    Until H7 the two calls shared one attempt and queued for the lane's
    one slot, and this test caught the wait for the slot inside the
    timeout (M10). Since H7 an attempt takes one portion -- one round of
    the slots (app/engine/service.py delivery_portion) -- so within an
    attempt no call waits for a slot at all and M10 has nothing left to
    break. What stays true, and is held here: the second call goes in
    the lane's next pass, with its own full timeout."""

    async def test_two_calls_two_passes_each_its_own_timeout(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        assert service.LANE_CONCURRENCY[Lane.BULK] == 1
        assert service.delivery_portion(Lane.BULK) == 1
        call, timeout = 0.7, 1.0
        assert call < timeout < 2 * call
        _lane_timeout(monkeypatch, Lane.BULK, timeout)
        nid = await _job_in(Lane.BULK, _TWO_CALLS)
        more: list[bool] = []
        with _channel(_Slow(call)) as spy:
            await process_lane(Lane.BULK, on_more=lambda: more.append(True))
            assert sorted(d.status for d in await _deliveries(nid)) == sorted([
                DeliveryStatus.SENT, DeliveryStatus.PENDING,
            ])
            await process_lane(Lane.BULK)
        assert [d.status for d in await _deliveries(nid)] == [
            DeliveryStatus.SENT, DeliveryStatus.SENT,
        ]
        assert more == [True]  # the first pass's portion was full
        (_, first_end), (second_start, _) = sorted(spy.calls)
        assert second_start >= first_end


class TestTheOutcome:
    """done-when 1 (2): the lane's timeout ends a call exactly as the one
    timeout did before lanes -- a transient failure that burns an
    attempt and waits its backoff, journaled `timeout` with nothing of
    the provider's, logged with the timeout that fired. M3."""

    async def test_same_class_journal_and_retry(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _lane_timeout(monkeypatch, Lane.INTERACTIVE, _SHORT)
        nid = await _job_in(Lane.INTERACTIVE)
        with _channel(_Slow(_SLOW)), capture_logs() as logs:
            await process_lane(Lane.INTERACTIVE)

        (delivery,) = await _deliveries(nid)
        assert (
            delivery.status, delivery.attempts, delivery.failure_class,
            delivery.wait_reason, delivery.error_message,
        ) == (
            DeliveryStatus.PENDING, 1, None,
            WaitReason.TRANSIENT_BACKOFF, f"Timeout after {_SHORT}s",
        )
        assert delivery.next_retry_at is not None
        (answer,) = await _channel_answers(nid)
        assert (answer.outcome, answer.provider_text, answer.error) == (
            ChannelAnswer.TIMEOUT, None, None,
        )
        (line,) = [log for log in logs if log["event"] == "delivery_timeout"]
        assert (line["timeout"], line["channel"]) == (_SHORT, DeliveryChannel.IN_APP)


class TestTheDefaults:
    """done-when 1 (3): normal is 30 s -- the one timeout every call had
    before lanes -- and so is every other lane (P4-3 gate: a shorter lane
    would time out nothing earlier than its clients, a longer one would
    wait for nothing). M4."""

    def test_every_lane_defaults_to_thirty(self) -> None:
        defaults = _settings()
        assert {lane: defaults.lane_deliver_timeout(lane) for lane in Lane} == {
            Lane.INTERACTIVE: 30, Lane.CRITICAL: 30, Lane.NORMAL: 30, Lane.BULK: 30,
        }

    async def test_a_normal_call_is_given_thirty_seconds(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Through the pipeline, on the process's own settings: the
        number the call actually gets, not only the field's default."""
        assert settings.notification_lane_normal_deliver_timeout_seconds == 30
        seen = _record_timeouts(monkeypatch)
        nid = await _job_in(Lane.NORMAL)
        with _channel(_Slow(0)):
            await process_lane(Lane.NORMAL)
        assert seen == [30]
        assert [d.status for d in await _deliveries(nid)] == [DeliveryStatus.SENT]

    def test_every_lane_has_its_knob_and_bound(self) -> None:
        """SHORTAGE: no lane without a timeout -- a lane added to the
        enum without one fails here, not as an AttributeError in the
        delivery."""
        for lane in Lane:
            name = f"notification_lane_{lane.value}_deliver_timeout_seconds"
            assert name in Settings.model_fields
            assert (NUMERIC_BOUNDS[name].lo, NUMERIC_BOUNDS[name].hi) == (1, 600)


# -----------------------------------------------------------------------------
# item 2 -- the rule: a lane above the clients of the live channels
# -----------------------------------------------------------------------------


class TestTheRule:
    """done-when 2 (1): a lane at the client timeout of a live channel
    refuses startup naming the variable and the channel; one second
    above it starts. M5, M6, M9."""

    @pytest.mark.parametrize("lane", list(Lane))
    def test_at_the_client_refuses_one_above_starts(self, lane: Lane) -> None:
        total = channel_spec(DeliveryChannel.TELEGRAM).client_timeout_total
        assert total == 29.0
        refusal = _refusal(**_TELEGRAM, **_at(lane, 29))
        assert (
            f"{lane_deliver_timeout_variable(lane)}=29 is not above the client "
            f"timeout of the live channel telegram (29 s): a call must time "
            f"out inside its client first, where 'never reached' and "
            f"'reached, no answer' stay apart. Set it to 30 or more."
        ) in refusal
        # The pair: one second above the client, the deploy starts.
        started = _settings(**_TELEGRAM, **_at(lane, 30))
        assert started.lane_deliver_timeout(lane) == 30

    def test_every_live_client_is_named(self) -> None:
        """REPEAT: two live channels below the lane -- both named, in one
        line, so one fix covers both."""
        refusal = _refusal(**_TELEGRAM, **_EMAIL, **_at(Lane.CRITICAL, 20))
        assert (
            "NOTIFICATION_LANE_CRITICAL_DELIVER_TIMEOUT_SECONDS=20 is not above "
            "the client timeout of the live channel telegram (29 s), email (29 s)"
        ) in refusal

    def test_email_alone_is_held_to_its_own_sum(self) -> None:
        """SHORTAGE: one external channel live -- the rule reads that
        one; the sum of its phases is the number."""
        assert channel_spec(DeliveryChannel.EMAIL).client_timeout_total == 29.0
        refusal = _refusal(**_EMAIL, **_at(Lane.INTERACTIVE, 29))
        assert "live channel email (29 s)" in refusal
        assert "telegram" not in refusal
        assert _settings(**_EMAIL, **_at(Lane.INTERACTIVE, 30))

    def test_no_network_channel_no_client_floor(self) -> None:
        """EMPTY: only in_app is live -- it has no client, so the lane's
        own bound is the only floor. M9: the rule over every implemented
        channel would refuse this deploy for telegram's number."""
        started = _settings(**_at(Lane.INTERACTIVE, 1))
        assert started.lane_deliver_timeout(Lane.INTERACTIVE) == 1

    def test_every_lane_wrong_every_lane_named(self) -> None:
        refusal = _refusal(
            **_TELEGRAM, **{k: v for lane in Lane for k, v in _at(lane, 10).items()},
        )
        for lane in Lane:
            assert f"{lane_deliver_timeout_variable(lane)}=10 is not above" in refusal

    def test_outside_its_bound_named_once(self) -> None:
        """0 is out of the bound AND below the client: one line, the
        bound's -- not two for one fault."""
        refusal = _refusal(**_TELEGRAM, **_at(Lane.BULK, 0))
        variable = lane_deliver_timeout_variable(Lane.BULK)
        assert refusal.count(f"{variable}=0") == 1
        assert f"{variable}=0 is outside 1..600" in refusal

    def test_written_empty_refuses_naming_the_variable(self) -> None:
        """EMPTY: `VAR=` is not the default -- like every int knob, an
        empty value refuses startup, naming the variable."""
        refusal = _refusal(
            **{"notification_lane_normal_deliver_timeout_seconds": ""},
        )
        assert "NOTIFICATION_LANE_NORMAL_DELIVER_TIMEOUT_SECONDS" in refusal


class TestOnePlaceForTheNumbers:
    """done-when 2 (2): the clients' timeouts are written once, in the
    channel's spec, and both the client and the rule read them there.
    M7, M8."""

    def test_the_published_numbers(self) -> None:
        """Pinned HERE, not read from the spec, so a changed number is a
        visible change (P4-3 gate: telegram 29, email 5/15/5/4)."""
        assert {spec.name: dict(spec.client_timeouts) for spec in CHANNEL_SPECS} == {
            "in_app": {},
            "telegram": {"timeout": 29.0},
            "email": {"connect": 5.0, "read": 15.0, "write": 5.0, "pool": 4.0},
            # P5-1 gate: the model writes for up to 100 s; 114 in all,
            # held below LLM_DELIVER_TIMEOUT_SECONDS, not below a lane.
            "llm": {"connect": 5.0, "read": 100.0, "write": 5.0, "pool": 4.0},
        }

    async def test_the_email_client_carries_the_spec(self) -> None:
        client = _real_http_client_factory()
        try:
            assert client.timeout == httpx.Timeout(
                **channel_spec(DeliveryChannel.EMAIL).client_timeouts,
            )
        finally:
            await client.aclose()

    async def test_the_bot_session_carries_the_spec(self) -> None:
        bot = _real_bot_factory(token=_BOT_TOKEN)
        try:
            assert bot.session.timeout == (
                channel_spec(DeliveryChannel.TELEGRAM).client_timeout_total
            )
        finally:
            await bot.session.close()

    @pytest.mark.parametrize(
        "factory", ["_real_bot_factory", "_real_http_client_factory"],
    )
    def test_no_number_in_the_factory(self, factory: str) -> None:
        """No copy: the factory's body holds no numeric literal -- and it
        does read the spec (the pair)."""
        tree = ast.parse(_FORMATTERS.read_text(encoding="utf-8"))
        (function,) = [
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == factory
        ]
        body = function.body[1:]  # past the docstring
        numbers = [
            node.value for statement in body for node in ast.walk(statement)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, int | float)
            and not isinstance(node.value, bool)
        ]
        assert numbers == []
        names = {
            node.id for statement in body for node in ast.walk(statement)
            if isinstance(node, ast.Name)
        }
        assert "channel_spec" in names


def _unpatched_make_request() -> Any:
    """aiogram's own AiohttpSession.make_request.

    The suite's network tripwire (tests/conftest.py) replaces it on the
    class, for every test, so that no test reaches the real Telegram
    API. This test talks to a server on 127.0.0.1 only, so it puts the
    library's function back for itself -- read from a fresh copy of the
    module, the code the class had before the tripwire.
    """
    found = importlib.util.find_spec("aiogram.client.session.aiohttp")
    assert found is not None and found.loader is not None
    module = importlib.util.module_from_spec(found)
    found.loader.exec_module(module)
    return module.AiohttpSession.make_request


@asynccontextmanager
async def _dripping_server(gap: float, pieces: int) -> AsyncIterator[str]:
    """A local HTTP server that answers at once with headers, then one
    byte of body every `gap` seconds, `pieces` times."""

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        await reader.read(65536)
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
        )
        try:
            for _ in range(pieces):
                writer.write(b"1\r\n \r\n")
                await writer.drain()
                await asyncio.sleep(gap)
            writer.write(b"0\r\n\r\n")
            await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.close()
        await server.wait_closed()


class TestTheTelegramTimeoutIsTotal:
    """The P4-3 gate's open claim, closed: aiogram hands its session's
    one timeout to aiohttp, which counts it over the WHOLE request --
    the answer included -- and not per read. Each byte here arrives well
    inside the timeout, the whole answer long after it: a per-read
    timeout would never fire, a total one fires at the timeout. And it
    reaches the attempt as the client's own error, journaled transient
    with its words -- not as the lane's bare timeout."""

    async def test_a_dripping_answer_is_cut_at_the_total(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from aiogram import Bot
        from aiogram.client.session.aiohttp import AiohttpSession
        from aiogram.client.telegram import TelegramAPIServer
        from aiogram.exceptions import TelegramNetworkError

        monkeypatch.setattr(AiohttpSession, "make_request", _unpatched_make_request())
        total, gap, pieces = 1.0, 0.25, 12
        assert gap < total < gap * pieces
        async with _dripping_server(gap, pieces) as base:
            session = AiohttpSession(
                api=TelegramAPIServer.from_base(base), timeout=total,
            )
            bot = Bot(token=_BOT_TOKEN, session=session)
            started = time.monotonic()
            try:
                with pytest.raises(TelegramNetworkError) as caught:
                    await bot.send_message(chat_id=1, text="x")
            finally:
                await session.close()
            took = time.monotonic() - started
        assert total <= took < gap * pieces
        assert "timeout" in str(caught.value).lower()
        assert not isinstance(caught.value, TimeoutError)
        assert journal.answer_of(caught.value).outcome == ChannelAnswer.TRANSIENT


# -----------------------------------------------------------------------------
# item 3 -- the operator's contract
# -----------------------------------------------------------------------------


class TestTheContract:
    """done-when 3: the four settings in .env.example with their
    defaults, and the rule said once in INTEGRATION.md. M11."""

    def test_the_variables_are_the_settings_both_ways(self) -> None:
        """The lanes' deliver timeouts are the settings named by lane --
        and, since P5-1, the one other deliver timeout is the llm
        channel's own (not a lane's): exactly five, named."""
        fields = {
            name for name in Settings.model_fields
            if name.endswith("_deliver_timeout_seconds")
        }
        lanes = {lane_deliver_timeout_variable(lane).lower() for lane in Lane}
        assert fields == lanes | {"llm_deliver_timeout_seconds"}
        assert len(lanes) == 4

    @pytest.mark.parametrize("lane", list(Lane))
    def test_env_example_has_the_default(self, lane: Lane) -> None:
        variable = lane_deliver_timeout_variable(lane)
        written = re.search(rf"^{variable}=(\d+)$", _ENV_EXAMPLE, re.M)
        assert written is not None, variable
        default = Settings.model_fields[variable.lower()].default
        assert int(written.group(1)) == default == 30

    def test_integration_says_the_rule(self) -> None:
        start = _DOC.index("### `lane`")
        section = _DOC[start:_DOC.index("\n### ", start)]
        flat = " ".join(section.split())
        assert "`NOTIFICATION_LANE_<LANE>_DELIVER_TIMEOUT_SECONDS`" in flat
        assert "in `lane_budgets` as `deliver_timeout`" in flat
        assert "above the client timeout of every channel live on the deploy" in flat
        assert "refuses startup, naming the variable and the channel" in flat

