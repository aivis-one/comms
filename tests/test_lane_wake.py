# =============================================================================
# P4-2 -- intake wakes its lane's loop
# =============================================================================
#
# Section 1 holds the Postgres facts the wake stands on, measured on a
# real database before the code that relies on them: a NOTIFY reaches
# a listener only after its transaction commits, never after a
# rollback, and identical ones in one transaction arrive once; a
# listener on an idle connection is called without a query of its own;
# a terminated connection says so to its termination listener.
#
# The rest drives the real worker loops (app/worker.py run_worker_loop)
# with the real listener, the jobs accepted through the stream intake
# (parse_event -> handle_event -> commit, as the consumer does). The
# loops are held at a 30 s interval -- poll and backoff alike -- so
# "delivered at once" and "delivered at the interval" are seconds apart;
# the boundary of "at once" is the default BASE poll interval, 5 s.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the signal before the intake commits           -> TestTheCommit (held)
#   M2  every event of the process set on any wake     -> TestTheLanes
#   M3  one pass per signal                            -> TestTheStorm
#   M4  no signal at all                               -> TestAtOnce
#   M5  the event cleared after the pass, not before   -> TestDuringAPass
#   M6  the signal on DUPLICATE and CONFLICT too       -> TestTheReplay
#   M7  the listener not connecting again              -> TestALostWake (dropped)
#   M8  no pass of every lane when LISTEN is set up    -> TestALostWake (dropped)
# =============================================================================

import asyncio
import json
import time
from collections import Counter
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

import app.engine.service as service
import app.worker as worker
from app.core.config import Settings, settings
from app.core.database import get_session_factory
from app.engine.constants import NotificationStatus
from app.engine.models import Notification
from app.engine.wake import WAKE_APPLICATION_NAME
from app.profile.registry import Decided, Lane, Layer, TypeRecord, registry
from app.transport.events import parse_event
from app.transport.handlers import HandleResult, handle_event
from tests.helpers import create_recipient

# A channel of these tests alone: nothing in comms listens on it.
_PROBE = "comms_test_probe"
# How long a notification that is NOT coming is waited for. Delivery to
# a listening backend on the same host takes milliseconds.
_QUIET = 0.5


@pytest.fixture
async def listener_engine() -> AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    yield engine
    await engine.dispose()


class _Heard:
    """What a listener on the probe channel received, in order."""

    def __init__(self) -> None:
        self.payloads: list[str] = []
        self.changed = asyncio.Event()

    def __call__(self, conn: Any, pid: int, channel: str, payload: str) -> None:
        self.payloads.append(payload)
        self.changed.set()

    async def until(self, count: int, seconds: float) -> bool:
        deadline = asyncio.get_running_loop().time() + seconds
        while len(self.payloads) < count:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            self.changed.clear()
            try:
                async with asyncio.timeout(remaining):
                    await self.changed.wait()
            except TimeoutError:
                return False
        return True


async def _notify(session: Any, payload: str) -> None:
    await session.execute(select(func.pg_notify(_PROBE, payload)))


class TestPostgresFacts:
    async def test_a_notify_arrives_after_commit_and_not_before(
        self, listener_engine: AsyncEngine,
    ) -> None:
        heard = _Heard()
        async with listener_engine.connect() as connection:
            driver = (await connection.get_raw_connection()).driver_connection
            assert driver is not None
            await driver.add_listener(_PROBE, heard)
            async with get_session_factory()() as session:
                await _notify(session, "interactive")
                await asyncio.sleep(_QUIET)
                assert heard.payloads == []
                await session.commit()
            assert await heard.until(1, 5.0)
            assert heard.payloads == ["interactive"]

    async def test_a_rolled_back_notify_never_arrives(
        self, listener_engine: AsyncEngine,
    ) -> None:
        heard = _Heard()
        async with listener_engine.connect() as connection:
            driver = (await connection.get_raw_connection()).driver_connection
            assert driver is not None
            await driver.add_listener(_PROBE, heard)
            async with get_session_factory()() as session:
                await _notify(session, "rolled-back")
                await session.rollback()
            # The pair: a committed one after it does arrive, alone.
            async with get_session_factory()() as session:
                await _notify(session, "committed")
                await session.commit()
            assert await heard.until(1, 5.0)
            await asyncio.sleep(_QUIET)
            assert heard.payloads == ["committed"]

    async def test_identical_notifies_of_one_transaction_arrive_once(
        self, listener_engine: AsyncEngine,
    ) -> None:
        heard = _Heard()
        async with listener_engine.connect() as connection:
            driver = (await connection.get_raw_connection()).driver_connection
            assert driver is not None
            await driver.add_listener(_PROBE, heard)
            async with get_session_factory()() as session:
                for _ in range(50):
                    await _notify(session, "normal")
                await _notify(session, "bulk")
                await session.commit()
            assert await heard.until(2, 5.0)
            await asyncio.sleep(_QUIET)
            assert sorted(heard.payloads) == ["bulk", "normal"]
            # Two transactions are two notifications: the folding is
            # per transaction, not per channel.
            for _ in range(2):
                async with get_session_factory()() as session:
                    await _notify(session, "normal")
                    await session.commit()
            assert await heard.until(4, 5.0)

    async def test_an_idle_listener_is_called_without_a_query_of_its_own(
        self, listener_engine: AsyncEngine,
    ) -> None:
        """The listening connection runs nothing after LISTEN and is not
        inside a transaction (a LISTEN in one would take effect only at
        its commit)."""
        heard = _Heard()
        async with listener_engine.connect() as connection:
            driver = (await connection.get_raw_connection()).driver_connection
            assert driver is not None
            assert not driver.is_in_transaction()
            await driver.add_listener(_PROBE, heard)
            assert not driver.is_in_transaction()
            await asyncio.sleep(_QUIET)
            async with get_session_factory()() as session:
                await _notify(session, "idle")
                await session.commit()
            assert await heard.until(1, 5.0)

    async def test_a_terminated_connection_calls_its_termination_listener(
        self, listener_engine: AsyncEngine,
    ) -> None:
        lost = asyncio.Event()
        async with listener_engine.connect() as connection:
            driver = (await connection.get_raw_connection()).driver_connection
            assert driver is not None
            driver.add_termination_listener(lambda conn: lost.set())
            await asyncio.sleep(_QUIET)
            assert not lost.is_set()
            pid = driver.get_server_pid()
            async with get_session_factory()() as session:
                terminated = (await session.execute(
                    text("SELECT pg_terminate_backend(:pid)"), {"pid": pid},
                )).scalar_one()
            assert terminated is True
            async with asyncio.timeout(5.0):
                await lost.wait()
            assert driver.is_closed()


# -----------------------------------------------------------------------------
# The worker, its loops held at a long interval
# -----------------------------------------------------------------------------

_BASE = 5.0  # the default poll interval: "at once" is below it
_LONG = 30  # the loops' interval in these tests, poll and backoff alike
# unit_routed declares lane: interactive in the fixture profile; the
# other in_app types are moved to the lane a test needs.
_INTERACTIVE = "unit_routed"
_OTHER = "unit_event_in_app"


def test_the_boundary_is_the_default_base_interval() -> None:
    """The bound these tests call "at once" is the poll interval a
    deploy runs with when it sets none."""
    assert Settings(_env_file=None).notification_poll_interval_seconds == _BASE


def _declare(type_key: str, lane: Lane) -> None:
    record = registry.record_of(type_key)
    assert record is not None
    fields = dict(record.fields)
    fields["lane"] = Decided(lane.value, Layer.PROFILE, "test")
    registry.register_type(
        type_key, category=registry.category_of(type_key),
        record=TypeRecord(fields=fields),
    )


class _Spy:
    """Every channel answers yes -- after `release` while `hold` is on."""

    def __init__(self) -> None:
        self.hold = False
        self.release = asyncio.Event()
        self.started = asyncio.Event()

    async def deliver(self, notification: Notification, *args: Any) -> bool:
        if self.hold:
            self.started.set()
            await self.release.wait()
        return True


class _Worker:
    """The running worker: passes per lane, the listener's backend."""

    def __init__(self) -> None:
        self.passes: Counter[str] = Counter()
        # A lane named here waits for `release` before its pass runs.
        self.held: str | None = None
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def settle(self) -> None:
        """Until the listener listens and no loop has passed for 0.5 s."""
        deadline = time.monotonic() + 15
        while await listener_pid() is None:
            assert time.monotonic() < deadline, "the listener never listened"
            await asyncio.sleep(0.05)
        before = Counter(self.passes)
        while True:
            await asyncio.sleep(0.5)
            if self.passes == before:
                return
            assert time.monotonic() < deadline, "the loops never settled"
            before = Counter(self.passes)


async def listener_pid() -> int | None:
    async with get_session_factory()() as session:
        pids = (await session.execute(text(
            "SELECT pid FROM pg_stat_activity WHERE application_name = :name"
        ), {"name": WAKE_APPLICATION_NAME})).scalars().all()
    assert len(pids) <= 1, pids
    return pids[0] if pids else None


@asynccontextmanager
async def _running(
    monkeypatch: pytest.MonkeyPatch,
    *,
    lanes: str | None = None,
    interval: int = _LONG,
    spy: _Spy | None = None,
    listen: Callable[..., Awaitable[None]] | None = None,
) -> AsyncIterator[_Worker]:
    monkeypatch.setattr(settings, "notification_poll_interval_seconds", interval)
    monkeypatch.setattr(settings, "notification_max_backoff_seconds", interval)
    monkeypatch.setattr(settings, "comms_worker_lanes", lanes)
    running = _Worker()
    real_process_lane = worker.process_lane

    async def counted(lane: Lane) -> int:
        running.passes[lane.value] += 1
        if running.held == lane.value:
            running.entered.set()
            await running.release.wait()
        return await real_process_lane(lane)

    async def idle() -> int:
        return 0

    monkeypatch.setattr(worker, "process_lane", counted)
    monkeypatch.setattr(worker, "run_maintenance_pass", idle)
    if listen is not None:
        monkeypatch.setattr(worker, "listen_for_wakes", listen)
    spy = spy or _Spy()
    with patch("app.engine.service.get_formatter", return_value=spy):
        task = asyncio.ensure_future(worker.run_worker_loop())
        try:
            if listen is None:
                await running.settle()
            yield running
        finally:
            running.release.set()
            spy.release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    # The listener's connection goes with the worker.
    deadline = time.monotonic() + 2
    while await listener_pid() is not None:
        assert time.monotonic() < deadline, "the listener outlived the worker"
        await asyncio.sleep(0.05)


async def _recipient() -> UUID:
    async with get_session_factory()() as session:
        recipient = await create_recipient(session)
        await session.commit()
        return recipient.id


def _request(type_key: str, recipient: UUID, **extra: Any) -> dict[str, Any]:
    return {
        "v": 1,
        "idempotency_key": f"wake-{uuid4().hex}",
        "type": type_key,
        "target_type": "user",
        "target_value": str(recipient),
        "title": "T",
        "body": "B",
        **extra,
    }


async def _handle(session: AsyncSession, data: dict[str, Any]) -> HandleResult:
    """One stream entry, parsed and handled as the consumer does."""
    entry = {"event": "notification_request", "data": json.dumps(data)}
    return await handle_event(session, parse_event(entry))


async def _accept(data: dict[str, Any]) -> HandleResult:
    async with get_session_factory()() as session:
        result = await _handle(session, data)
        await session.commit()
    return result


async def _job(key: str) -> Notification | None:
    async with get_session_factory()() as session:
        return (await session.execute(
            select(Notification).where(Notification.idempotency_key == key)
        )).scalar_one_or_none()


async def _sent_within(key: str, seconds: float) -> float | None:
    """Seconds until the job is SENT, or None if not within `seconds`."""
    started = time.monotonic()
    while time.monotonic() - started < seconds:
        job = await _job(key)
        if job is not None and job.status == NotificationStatus.SENT:
            return time.monotonic() - started
        await asyncio.sleep(0.02)
    return None


async def _quiet() -> None:
    """Long enough for a wake that is coming to have come and passed."""
    await asyncio.sleep(1.0)


# -----------------------------------------------------------------------------
# (1) at once, and the pair without the signal
# -----------------------------------------------------------------------------


class TestAtOnce:
    async def test_a_job_accepted_while_its_lane_sleeps_goes_at_once(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recipient = await _recipient()
        async with _running(monkeypatch):
            data = _request(_INTERACTIVE, recipient)
            started = time.monotonic()
            assert await _accept(data) == HandleResult.PROCESSED
            took = await _sent_within(data["idempotency_key"], _BASE)
            assert took is not None, "not delivered within the base interval"
            print(f"\nintake -> delivered, woken: {time.monotonic() - started:.3f}s")

    async def test_without_the_signal_the_job_waits_for_the_interval(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The pair: what makes it go at once is the signal, not a pass
        that happened to come."""
        recipient = await _recipient()

        async def silent(session: AsyncSession, lane: str) -> None:
            return None

        monkeypatch.setattr(service, "signal_lane", silent)
        async with _running(monkeypatch):
            data = _request(_INTERACTIVE, recipient)
            started = time.monotonic()
            await _accept(data)
            assert await _sent_within(data["idempotency_key"], _BASE) is None
            job = await _job(data["idempotency_key"])
            assert job is not None and job.status == NotificationStatus.PENDING
            took = await _sent_within(data["idempotency_key"], _LONG + 5)
            assert took is not None
            print(f"\nintake -> delivered, unwoken: {time.monotonic() - started:.3f}s")


# -----------------------------------------------------------------------------
# (2) after the commit
# -----------------------------------------------------------------------------


class TestTheCommit:
    async def test_a_rolled_back_intake_wakes_nothing_and_leaves_nothing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recipient = await _recipient()
        async with _running(monkeypatch) as running:
            before = Counter(running.passes)
            data = _request(_INTERACTIVE, recipient)
            async with get_session_factory()() as session:
                assert await _handle(session, data) == HandleResult.PROCESSED
                await session.rollback()
            await _quiet()
            assert running.passes == before
            assert await _job(data["idempotency_key"]) is None
            # The pair: the same request committed wakes and goes.
            assert await _accept(data) == HandleResult.PROCESSED
            assert await _sent_within(data["idempotency_key"], _BASE) is not None
            lane = Lane.INTERACTIVE.value
            assert running.passes[lane] > before[lane]

    async def test_a_commit_held_after_intake_is_woken_only_at_the_commit(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The intake has run, its transaction is still open: no pass.
        A signal sent before the commit would wake a pass that cannot
        see the job, and the job would then wait for the interval (M1)."""
        recipient = await _recipient()
        async with _running(monkeypatch) as running:
            before = Counter(running.passes)
            data = _request(_INTERACTIVE, recipient)
            async with get_session_factory()() as session:
                assert await _handle(session, data) == HandleResult.PROCESSED
                await _quiet()
                assert running.passes == before
                await session.commit()
            assert await _sent_within(data["idempotency_key"], _BASE) is not None


# -----------------------------------------------------------------------------
# (3) a replay creates no work
# -----------------------------------------------------------------------------


class TestTheReplay:
    async def test_duplicate_and_conflict_wake_nothing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recipient = await _recipient()
        async with _running(monkeypatch) as running:
            data = _request(_INTERACTIVE, recipient)
            first = Counter(running.passes)
            assert await _accept(data) == HandleResult.PROCESSED
            assert await _sent_within(data["idempotency_key"], _BASE) is not None
            await running.settle()
            # The pair: the accepted one did wake its lane.
            lane = Lane.INTERACTIVE.value
            assert running.passes[lane] > first[lane]

            before = Counter(running.passes)
            assert await _accept(data) == HandleResult.DUPLICATE
            assert await _accept({**data, "body": "other"}) == HandleResult.CONFLICT
            await _quiet()
            assert running.passes == before


# -----------------------------------------------------------------------------
# (4) a storm is not a pass per signal
# -----------------------------------------------------------------------------


_STORM = 200


class TestTheStorm:
    async def test_two_hundred_wakes_during_a_pass_are_one_more_pass(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recipient = await _recipient()
        lane = Lane.INTERACTIVE.value
        async with _running(monkeypatch) as running:
            running.held = lane
            first = _request(_INTERACTIVE, recipient)
            await _accept(first)
            async with asyncio.timeout(_BASE):
                await running.entered.wait()
            before = running.passes[lane]
            keys = []
            for _ in range(_STORM):
                data = _request(_INTERACTIVE, recipient)
                keys.append(data["idempotency_key"])
                assert await _accept(data) == HandleResult.PROCESSED
            await _quiet()
            assert running.passes[lane] == before
            running.held = None
            running.release.set()
            assert await _sent_within(first["idempotency_key"], _BASE) is not None
            await _quiet()
            # The held pass and exactly one after it -- not 200.
            assert running.passes[lane] == before + 1
            sent = 0
            for key in keys:
                job = await _job(key)
                assert job is not None
                sent += job.status == NotificationStatus.SENT
            # The pair: those passes did the work (each takes a batch).
            batch = settings.lane_batch_size(Lane.INTERACTIVE)
            assert sent == min(_STORM, 2 * batch - 1)


# -----------------------------------------------------------------------------
# a wake during a pass is one more pass after it
# -----------------------------------------------------------------------------


class TestDuringAPass:
    async def test_a_job_accepted_after_the_pass_selected_goes_right_after_it(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recipient = await _recipient()
        spy = _Spy()
        async with _running(monkeypatch, spy=spy):
            spy.hold = True
            first = _request(_INTERACTIVE, recipient)
            await _accept(first)
            async with asyncio.timeout(_BASE):
                await spy.started.wait()
            # The pass has selected `first` and is inside its channel
            # call: `second` is not in that pass.
            second = _request(_INTERACTIVE, recipient)
            await _accept(second)
            spy.hold = False
            spy.release.set()
            assert await _sent_within(first["idempotency_key"], _BASE) is not None
            assert await _sent_within(second["idempotency_key"], _BASE) is not None


# -----------------------------------------------------------------------------
# (5) a lane wakes only its own loops
# -----------------------------------------------------------------------------


class TestTheLanes:
    @pytest.mark.parametrize("lane", list(Lane))
    async def test_each_lane_wakes_its_loop_and_no_other(
        self, monkeypatch: pytest.MonkeyPatch, lane: Lane,
    ) -> None:
        _declare(_OTHER, lane)
        recipient = await _recipient()
        async with _running(monkeypatch) as running:
            before = Counter(running.passes)
            data = _request(_OTHER, recipient)
            await _accept(data)
            assert await _sent_within(data["idempotency_key"], _BASE) is not None
            await _quiet()
            woke = running.passes - before
            assert woke == Counter({lane.value: woke[lane.value]})
            assert woke[lane.value] >= 1

    async def test_a_process_without_the_lane_does_not_stir(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _declare(_OTHER, Lane.BULK)
        recipient = await _recipient()
        async with _running(monkeypatch, lanes="interactive,normal") as running:
            before = Counter(running.passes)
            data = _request(_OTHER, recipient)
            assert await _accept(data) == HandleResult.PROCESSED
            await _quiet()
            assert running.passes == before
            job = await _job(data["idempotency_key"])
            assert job is not None and job.status == NotificationStatus.PENDING
            # The pair: a lane it does take wakes in the same process.
            woken = _request(_INTERACTIVE, recipient)
            await _accept(woken)
            assert await _sent_within(woken["idempotency_key"], _BASE) is not None
            await _quiet()
            assert running.passes - before == Counter(
                {Lane.INTERACTIVE.value: running.passes[Lane.INTERACTIVE.value]
                 - before[Lane.INTERACTIVE.value]},
            )


# -----------------------------------------------------------------------------
# a job scheduled for later wakes an empty pass (the KNOWN CEILING in
# app/engine/wake.py: it is picked up by the poll once due)
# -----------------------------------------------------------------------------


class TestScheduledLater:
    async def test_it_wakes_the_lane_and_stays_pending(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recipient = await _recipient()
        async with _running(monkeypatch) as running:
            before = Counter(running.passes)
            later = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
            data = _request(_INTERACTIVE, recipient, scheduled_at=later)
            await _accept(data)
            await _quiet()
            assert running.passes - before == Counter({Lane.INTERACTIVE.value: 1})
            job = await _job(data["idempotency_key"])
            assert job is not None and job.status == NotificationStatus.PENDING


# -----------------------------------------------------------------------------
# (6) a lost wake costs the interval, never the job
# -----------------------------------------------------------------------------


class TestALostWake:
    async def test_with_no_listener_the_poll_still_delivers(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recipient = await _recipient()

        async def deaf(events: Any) -> None:
            await asyncio.Event().wait()

        async with _running(monkeypatch, interval=2, listen=deaf):
            data = _request(_INTERACTIVE, recipient)
            await _accept(data)
            assert await _sent_within(data["idempotency_key"], 2 + _BASE) is not None

    async def test_a_dropped_listener_comes_back_and_catches_up(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The listener's backend is terminated; a job accepted then has
        no one to hear it. The listener connects again (after the
        backoff, read when it retries -- 1 s here, while the loops keep
        the 30 s they started with) and its first pass of every lane
        takes the job; a job after that is woken as usual."""
        recipient = await _recipient()
        async with _running(monkeypatch):
            monkeypatch.setattr(settings, "notification_max_backoff_seconds", 1)
            dropped = await listener_pid()
            assert dropped is not None
            async with get_session_factory()() as session:
                assert (await session.execute(
                    text("SELECT pg_terminate_backend(:pid)"), {"pid": dropped},
                )).scalar_one() is True
            lost = _request(_INTERACTIVE, recipient)
            await _accept(lost)
            assert await _sent_within(lost["idempotency_key"], _BASE) is not None
            deadline = time.monotonic() + _BASE
            while (pid := await listener_pid()) in (None, dropped):
                assert time.monotonic() < deadline, "the listener never came back"
                await asyncio.sleep(0.05)
            after = _request(_INTERACTIVE, recipient)
            await _accept(after)
            assert await _sent_within(after["idempotency_key"], _BASE) is not None
            assert await listener_pid() == pid


# -----------------------------------------------------------------------------
# item 2 -- the contract says it
# -----------------------------------------------------------------------------


_DOC = (
    Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md"
).read_text(encoding="utf-8")


class TestTheContract:
    def _section(self) -> str:
        start = _DOC.index("### `lane`")
        return _DOC[start:_DOC.index("\n### ", start)]

    def test_every_lane_goes_at_once_and_a_lost_wake_falls_back_to_the_poll(
        self,
    ) -> None:
        section = " ".join(self._section().split())
        assert "picked up as soon as it is accepted, in every lane" in section
        assert "falls back to the poll" in section
        assert "never loses the job" in section
        assert "`NOTIFICATION_MAX_BACKOFF_SECONDS`" in section
        # The name it gives is a setting the worker reads.
        assert "notification_max_backoff_seconds" in Settings.model_fields
        assert "becomes due later" in section
