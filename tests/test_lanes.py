# =============================================================================
# P4-1 -- isolation lanes: the job keeps its type's lane, each lane is
# selected with its own budget by its own loop, the process takes the
# lanes its environment names
# =============================================================================
#
# Through the real pipeline (resolve -> deliver -> rollup) with the
# in_app channel answering through a spy; the race at the end on real
# sessions.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the lane filter dropped from the selection    -> TestIsolation
#   M2  one loop for every lane                       -> TestTheLoops (bulk)
#   M3  the selection limited by one lane's budget     -> TestIsolation (count)
#   M4  the job not given its type's lane              -> TestSnapshot
#   M5  the process's lanes ignored                    -> TestProcessLanes
#   M6  the setting accepting empty / unknown / twice  -> TestTheSetting
#   M7  the expiry sweep over every lane               -> TestTheSweep
#   M8  the index without the lane                     -> TestThePlan
#   M9  a failing lane cancelling the others           -> TestIsolation
#   M10 the startup log without lanes or budgets       -> TestTheLoops (log)
#   M11 migration 0024 filling another lane            -> test_migration_0024
#   M12 maintenance run inside the lane loop           -> TestTheLoops
#   M13 the expiry filter dropped from the selection   -> TestTheSweep
#   M14 the race harness's attempt FOR UPDATE          -> TestAttemptAgainstExpiry
#   M15 the lanes' slots past the pool                 -> TestTheBudgets
#   M16 one number of slots for every lane             -> TestTheSlots
# =============================================================================

import asyncio
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import AsyncGenerator, Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.pool import NullPool
from structlog.testing import capture_logs

import app.engine.processor as processor
import app.worker as worker
from app.core.config import NUMERIC_BOUNDS, Settings, settings
from app.core.database import (
    POOL_MAX_OVERFLOW,
    POOL_SIZE,
    dispose_engine,
    get_engine,
    get_session_factory,
)
from app.engine import journal
from app.engine.constants import (
    ChannelAnswer,
    DeliveryStatus,
    JournalSubject,
    NotificationStatus,
    TargetType,
)
from app.engine.models import (
    Notification,
    NotificationDelivery,
    NotificationTransition,
)
from app.engine.processor import (
    expire_overdue_notifications,
    process_lane,
    process_pending_notifications,
)
from app.engine.service import (
    LANE_CONCURRENCY,
    close_notifications,
    create_notification,
    resolve_notification,
)
from app.engine.wake import wake_engine
from app.profile.loader import _FIELDS, LANES
from app.profile.registry import Decided, Lane, Layer, TypeRecord, registry
from tests.helpers import create_recipient, intake_fields, notification_row_fields

_REPO = Path(__file__).resolve().parents[1]
_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")

# in_app types of the fixture profile, one per lane in the tests that
# need four; unit_routed DECLARES lane: interactive in the fixture.
_TYPES = (
    "unit_event_in_app",
    "unit_routed",
    "unit_event_telegram_in_app",
    "unit_event_in_app_email",
)
_DECLARED = "unit_routed"
_BARE = "unit_bare"


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


class _Spy:
    """Every channel, answering yes -- after `hold` for a held lane."""

    def __init__(self, held: str | None = None) -> None:
        self.held = held
        self.release = asyncio.Event()
        self.started = asyncio.Event()

    async def deliver(self, notification: Notification, *args: Any) -> bool:
        if notification.lane == self.held:
            self.started.set()
            await self.release.wait()
        return True


@contextmanager
def _channel(spy: Any = None) -> Iterator[Any]:
    """Every channel answers through `spy` (a fresh _Spy if none)."""
    spy = spy or _Spy()
    with patch("app.engine.service.get_formatter", return_value=spy):
        yield spy


async def _intake(type_key: str, recipient_id: UUID, **extra: Any) -> UUID:
    async with get_session_factory()() as session:
        job = await create_notification(
            session, **intake_fields(), type=type_key, title="T", body="B",
            target_type=TargetType.USER, target_value=str(recipient_id),
            **extra,
        )
        await session.commit()
        return job.id


async def _recipient() -> UUID:
    async with get_session_factory()() as session:
        recipient = await create_recipient(session)
        await session.commit()
        return recipient.id


async def _job(nid: UUID) -> Notification:
    async with get_session_factory()() as session:
        job = await session.get(Notification, nid)
        assert job is not None
        return job


async def _status(nid: UUID) -> str:
    return (await _job(nid)).status


def _lanes(monkeypatch: pytest.MonkeyPatch, value: str | None) -> None:
    monkeypatch.setattr(settings, "comms_worker_lanes", value)


# -----------------------------------------------------------------------------
# Б1 -- the job keeps its type's lane
# -----------------------------------------------------------------------------


class TestSnapshot:
    """REPEAT: two types, two lanes, each job its own. EMPTY: a type
    without the field -> normal, said as the default. SHORTAGE: the
    profile changes after intake -> the job keeps what it was given."""

    async def test_each_type_gives_its_own_lane(self) -> None:
        recipient = await _recipient()
        routed = await _intake(_DECLARED, recipient)
        plain = await _intake("unit_event_in_app", recipient)
        assert (await _job(routed)).lane == Lane.INTERACTIVE
        assert (await _job(plain)).lane == Lane.NORMAL

    async def test_a_type_without_a_lane_goes_normal_and_says_why(self) -> None:
        recipient = await _recipient()
        nid = await _intake(_BARE, recipient)
        assert (await _job(nid)).lane == Lane.NORMAL
        decided = registry.explain(_BARE, "lane")
        assert (decided.value, decided.layer) == ("normal", Layer.DEFAULT)
        assert decided.source == "comms default: lane not declared"
        declared = registry.explain(_DECLARED, "lane")
        assert (declared.value, declared.layer) == ("interactive", Layer.PROFILE)

    async def test_a_profile_changed_after_intake_does_not_move_the_job(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Б1 (2): accepted interactive, the profile then says bulk. A
        process that takes only bulk does not take it; one that takes
        interactive does. M4."""
        recipient = await _recipient()
        nid = await _intake(_DECLARED, recipient)
        _declare(_DECLARED, Lane.BULK)
        with _channel():
            _lanes(monkeypatch, "bulk")
            await process_pending_notifications()
            assert await _status(nid) == NotificationStatus.PENDING
            _lanes(monkeypatch, "interactive")
            await process_pending_notifications()
        job = await _job(nid)
        assert (job.lane, job.status) == (Lane.INTERACTIVE, NotificationStatus.SENT)


class TestTheCheck:
    """The CHECK over a nullable-at-birth column: NULL and an unknown
    lane refused; the pair, a lane of the four, flushes."""

    @pytest.mark.parametrize("lane", [None, "urgent", "Normal", ""])
    async def test_the_column_refuses(
        self, db_session: AsyncSession, lane: str | None,
    ) -> None:
        fields = {**notification_row_fields(), "lane": lane}
        db_session.add(Notification(
            type=_BARE, title="T", body="B", target_type="all",
            target_value="*", **fields,
        ))
        with pytest.raises(IntegrityError, match="lane"):
            await db_session.flush()

    @pytest.mark.parametrize("lane", list(Lane))
    async def test_the_column_takes_each_lane(
        self, db_session: AsyncSession, lane: Lane,
    ) -> None:
        fields = {**notification_row_fields(), "lane": lane.value}
        db_session.add(Notification(
            type=_BARE, title="T", body="B", target_type="all",
            target_value="*", **fields,
        ))
        await db_session.flush()


_BEFORE = "0023_expired_cleanup_index"
_SUBJECT = "0024_job_lane"


def _alembic(*args: str) -> None:
    done = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=_REPO, capture_output=True, text=True, env=dict(os.environ),
    )
    assert done.returncode == 0, done.stderr


async def _migrate(*args: str) -> None:
    await dispose_engine()
    _alembic(*args)
    await dispose_engine()


async def _sql(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        result = await session.execute(text(statement), params)
        await session.commit()
        return result


async def _indexes() -> set[str]:
    rows = await _sql(
        "SELECT indexname FROM pg_indexes WHERE tablename = 'notifications'",
    )
    return {row[0] for row in rows}


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    yield
    await _sql("DELETE FROM notifications")
    await _migrate("upgrade", "head")


async def test_migration_0024(at_head_afterwards: None) -> None:
    """An existing job takes 'normal' -- the selection every job had
    -- then NOT NULL and the CHECK; the pick index leads with the lane
    and the lane-less one is gone; down restores it; up again. M11."""
    await _migrate("downgrade", _BEFORE)
    id_ = uuid4()
    await _sql(
        "INSERT INTO notifications (id, type, title, body, target_type, "
        "target_value, idempotency_key, fingerprint, channels, "
        "expiry_layer, push_on, retry_max_attempts, retry_backoff_seconds, "
        "recipient_schedule, status) VALUES (:id, 'unit_event', 'T', 'B', "
        "'all', '*', :key, :fp, '[\"in_app\"]'::jsonb, 'default', 'none', "
        "3, 30, 'respect', 'processing')",
        id=id_, key=f"m24:{id_}", fp="a" * 64,
    )
    await _migrate("upgrade", _SUBJECT)
    assert (await _sql(
        "SELECT lane FROM notifications WHERE id = :id", id=id_,
    )).scalar_one() == "normal"
    assert (await _sql(
        "SELECT is_nullable FROM information_schema.columns WHERE "
        "table_name = 'notifications' AND column_name = 'lane'",
    )).scalar_one() == "NO"
    assert (await _sql(
        "SELECT count(*) FROM pg_constraint "
        "WHERE conname = 'ck_notifications_lane'",
    )).scalar_one() == 1
    indexes = await _indexes()
    assert "ix_notifications_lane_status_scheduled" in indexes
    assert "ix_notifications_status_scheduled" not in indexes
    await _migrate("downgrade", _BEFORE)
    assert (await _sql(
        "SELECT count(*) FROM information_schema.columns WHERE "
        "table_name = 'notifications' AND column_name = 'lane'",
    )).scalar_one() == 0
    indexes = await _indexes()
    assert "ix_notifications_status_scheduled" in indexes
    assert "ix_notifications_lane_status_scheduled" not in indexes
    await _migrate("upgrade", _SUBJECT)
    assert (await _sql(
        "SELECT lane FROM notifications WHERE id = :id", id=id_,
    )).scalar_one() == "normal"


# -----------------------------------------------------------------------------
# Б2 -- one lane's backlog does not delay another
# -----------------------------------------------------------------------------


class TestIsolation:
    async def _backlog(self, type_key: str, recipient: UUID, n: int) -> list[UUID]:
        """`n` jobs of `type_key`, each older than whatever comes next
        (minutes, not hours: unit_routed expires after 15m)."""
        old = datetime.now(UTC) - timedelta(minutes=5)
        return [
            await _intake(
                type_key, recipient, scheduled_at=old + timedelta(seconds=i),
            )
            for i in range(n)
        ]

    @pytest.mark.parametrize(
        ("backlog_lane", "urgent_lane"),
        [(Lane.BULK, Lane.INTERACTIVE), (Lane.NORMAL, Lane.CRITICAL)],
    )
    async def test_the_urgent_job_goes_on_the_first_pass(
        self, monkeypatch: pytest.MonkeyPatch,
        backlog_lane: Lane, urgent_lane: Lane,
    ) -> None:
        """Б2 (1), (2): a backlog larger than its own batch AND than the
        urgent lane's, all of it older; the newest job, of the urgent
        lane, is delivered on the first pass. M1: without the lane
        filter the urgent lane's pass takes the oldest of the backlog
        and the urgent job waits."""
        monkeypatch.setattr(
            settings, f"notification_lane_{backlog_lane}_batch_size", 3,
        )
        _declare("unit_event_in_app", backlog_lane)
        _declare(_DECLARED, urgent_lane)
        recipient = await _recipient()
        urgent_batch = settings.lane_batch_size(urgent_lane)
        backlog = await self._backlog(
            "unit_event_in_app", recipient, urgent_batch + 5,
        )
        urgent = await _intake(_DECLARED, recipient)
        with _channel():
            await process_pending_notifications()
        assert await _status(urgent) == NotificationStatus.SENT
        statuses = [await _status(nid) for nid in backlog]
        assert statuses.count(NotificationStatus.SENT) == 3
        assert statuses.count(NotificationStatus.PENDING) == urgent_batch + 2

    async def test_each_lane_selects_at_most_its_budget(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Б2 (3): four lanes, budgets 1..4, six due jobs each: one pass
        delivers exactly the budget of each lane, and the rest waits
        (REPEAT, and the pair: something is left). M3."""
        recipient = await _recipient()
        jobs: dict[Lane, list[UUID]] = {}
        for budget, (lane, type_key) in enumerate(
            zip(Lane, _TYPES, strict=True), start=1,
        ):
            monkeypatch.setattr(
                settings, f"notification_lane_{lane}_batch_size", budget,
            )
            _declare(type_key, lane)
            jobs[lane] = await self._backlog(type_key, recipient, 6)
        with _channel():
            await process_pending_notifications()
        for budget, lane in enumerate(Lane, start=1):
            statuses = [await _status(nid) for nid in jobs[lane]]
            assert statuses.count(NotificationStatus.SENT) == budget, lane
            assert statuses.count(NotificationStatus.PENDING) == 6 - budget

    async def test_an_empty_lane_takes_nothing_and_holds_nothing(self) -> None:
        """EMPTY: three lanes with nothing due; the fourth delivers."""
        recipient = await _recipient()
        nid = await _intake("unit_event_in_app", recipient)
        with _channel():
            for lane in (Lane.INTERACTIVE, Lane.CRITICAL, Lane.BULK):
                assert await process_lane(lane) == 0
            assert await process_lane(Lane.NORMAL) == 1
        assert await _status(nid) == NotificationStatus.SENT

    async def test_a_failing_lane_does_not_stop_the_others(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """M9: bulk's pass raises; interactive's runs to its end, the
        failure is logged with its lane, then raised."""
        recipient = await _recipient()
        nid = await _intake(_DECLARED, recipient)
        real = processor.process_lane

        async def bulk_breaks(lane: Lane) -> int:
            if lane is Lane.BULK:
                raise RuntimeError("bulk selection failed")
            await asyncio.sleep(0.2)  # finishes after bulk has raised
            return await real(lane)

        monkeypatch.setattr(processor, "process_lane", bulk_breaks)
        with _channel(), capture_logs() as logs, pytest.raises(
            RuntimeError, match="bulk selection failed",
        ):
            await process_pending_notifications()
        assert await _status(nid) == NotificationStatus.SENT
        errors = [log for log in logs if log["event"] == "lane_pass_error"]
        assert [log["lane"] for log in errors] == ["bulk"]


class _Counting:
    """Every channel, answering yes after a moment; counts the calls in
    flight at once."""

    def __init__(self) -> None:
        self.now = 0
        self.most = 0

    async def deliver(self, *args: Any) -> bool:
        self.now += 1
        self.most = max(self.most, self.now)
        await asyncio.sleep(0.05)
        self.now -= 1
        return True


class TestTheSlots:
    @pytest.mark.parametrize("lane", [Lane.BULK, Lane.NORMAL])
    async def test_a_lane_never_holds_more_than_its_slots(
        self, lane: Lane,
    ) -> None:
        """One job to four recipients: bulk calls one at a time, normal
        all four at once (its 20 slots) -- the pair that shows the
        counter counts. M16."""
        _declare("unit_event_in_app", lane)
        for _ in range(4):
            await _recipient()
        await _intake_all("unit_event_in_app")
        spy = _Counting()
        with _channel(spy):
            await process_lane(lane)
        assert spy.most == min(4, LANE_CONCURRENCY[lane])


async def _intake_all(type_key: str) -> UUID:
    async with get_session_factory()() as session:
        job = await create_notification(
            session, **intake_fields(), type=type_key, title="T", body="B",
            target_type=TargetType.ALL, target_value="*",
        )
        await session.commit()
        return job.id


class TestProcessLanes:
    async def test_a_process_takes_only_its_lanes(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Б3 (2): one database, two runs of the selection with two
        sets. `critical` takes the critical job and not the normal one;
        a set without critical takes the normal job and not a critical
        one. M5."""
        _declare(_DECLARED, Lane.CRITICAL)
        recipient = await _recipient()
        critical = await _intake(_DECLARED, recipient)
        normal = await _intake("unit_event_in_app", recipient)
        with _channel():
            _lanes(monkeypatch, "critical")
            await process_pending_notifications()
            assert await _status(critical) == NotificationStatus.SENT
            assert await _status(normal) == NotificationStatus.PENDING

            later = await _intake(_DECLARED, recipient)
            _lanes(monkeypatch, "interactive,normal,bulk")
            await process_pending_notifications()
            assert await _status(normal) == NotificationStatus.SENT
            assert await _status(later) == NotificationStatus.PENDING


class TestTheSweep:
    async def _overdue(self, type_key: str, recipient: UUID) -> UUID:
        nid = await _intake(
            type_key, recipient,
            expiry_at=datetime.now(UTC) + timedelta(hours=1),
        )
        async with get_session_factory()() as session:
            await session.execute(
                update(Notification)
                .where(Notification.id == nid)
                .values(expiry_at=datetime.now(UTC) - timedelta(seconds=1)),
            )
            await session.commit()
        return nid

    async def test_an_overdue_job_the_sweep_has_not_reached_is_not_attempted(
        self,
    ) -> None:
        """The sweep runs in its own loop: between its passes the lane
        does not take a job past its expiry. The pair: its neighbour
        without one is delivered. Then the sweep expires it. M13."""
        recipient = await _recipient()
        overdue = await self._overdue("unit_event_in_app", recipient)
        fresh = await _intake("unit_event_in_app", recipient)
        with _channel():
            await process_lane(Lane.NORMAL)
        assert await _status(fresh) == NotificationStatus.SENT
        assert await _status(overdue) == NotificationStatus.PENDING
        assert await expire_overdue_notifications((Lane.NORMAL,)) == 1
        assert await _status(overdue) == NotificationStatus.EXPIRED

    async def test_the_sweep_takes_only_the_process_lanes(self) -> None:
        """A critical-only process expires its own and leaves bulk's to
        the process that takes bulk. M7."""
        _declare(_DECLARED, Lane.CRITICAL)
        _declare("unit_event_in_app", Lane.BULK)
        recipient = await _recipient()
        critical = await self._overdue(_DECLARED, recipient)
        bulk = await self._overdue("unit_event_in_app", recipient)
        assert await expire_overdue_notifications((Lane.CRITICAL,)) == 1
        assert await _status(critical) == NotificationStatus.EXPIRED
        assert await _status(bulk) == NotificationStatus.PENDING
        assert await expire_overdue_notifications((Lane.BULK,)) == 1
        assert await _status(bulk) == NotificationStatus.EXPIRED


# -----------------------------------------------------------------------------
# Б2 (4) -- the lane's selection runs on the index
# -----------------------------------------------------------------------------


@contextmanager
def _captured() -> Iterator[list[tuple[str, Any]]]:
    seen: list[tuple[str, Any]] = []
    sync_engine = get_engine().sync_engine

    def record(conn: Any, cursor: Any, statement: str, parameters: Any,
               context: Any, executemany: bool) -> None:
        seen.append((statement, parameters))

    event.listen(sync_engine, "before_cursor_execute", record)
    try:
        yield seen
    finally:
        event.remove(sync_engine, "before_cursor_execute", record)


def _nodes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    out = [plan]
    for child in plan.get("Plans", []):
        out.extend(_nodes(child))
    return out


class TestThePlan:
    async def test_the_lane_selection_runs_on_the_lane_index(self) -> None:
        """50 000 jobs over four lanes, most of them sent, 2 000 active:
        the statement process_lane sends, captured off the connection and
        explained with its own parameters, reads notifications through
        ix_notifications_lane_status_scheduled by the lane and never
        scans it. M8."""
        await _sql(
            "INSERT INTO notifications (id, type, title, body, target_type, "
            "target_value, idempotency_key, fingerprint, channels, "
            "expiry_layer, push_on, retry_max_attempts, "
            "retry_backoff_seconds, recipient_schedule, lane, status, "
            "scheduled_at) "
            "SELECT gen_random_uuid(), 'unit_bare', 'T', 'B', 'all', '*', "
            "'p41-seed-' || g, repeat('a', 64), '[\"in_app\"]'::jsonb, "
            "'default', 'none', 3, 30, 'respect', "
            "(ARRAY['interactive','critical','normal','bulk'])[g % 4 + 1], "
            "CASE WHEN g % 25 <> 0 THEN 'sent' "
            "     WHEN g % 50 = 0 THEN 'processing' ELSE 'pending' END, "
            "now() + interval '1 day' - (g % 5000) * interval '1 minute' "
            "FROM generate_series(1, 50000) g",
        )
        await _sql("ANALYZE notifications")
        with _captured() as seen, _channel():
            await process_lane(Lane.BULK)
        (statement, parameters), = [
            (s, p) for s, p in seen
            if "notifications.lane =" in s and "ORDER BY" in s
        ]
        async with get_session_factory()() as session:
            raw = await session.connection()
            plan = (await raw.exec_driver_sql(
                "EXPLAIN (FORMAT JSON) " + statement, parameters,
            )).scalar_one()
        root = (json.loads(plan) if isinstance(plan, str) else plan)[0]["Plan"]
        nodes = _nodes(root)
        scans = [
            n for n in nodes
            if n["Node Type"] == "Seq Scan"
            and n.get("Relation Name") == "notifications"
        ]
        assert scans == []
        # The pair: the index is read BY THE LANE -- its condition names
        # the lane, not only the status (a same-named index without the
        # lane passes a check by name).
        on_index = [
            n for n in nodes
            if n.get("Index Name") == "ix_notifications_lane_status_scheduled"
        ]
        assert on_index
        assert all("lane" in n["Index Cond"] for n in on_index)


# -----------------------------------------------------------------------------
# Б2 (6), правка 2 -- the numbers
# -----------------------------------------------------------------------------


class TestTheBudgets:
    def test_every_lane_has_a_batch_bound_and_a_concurrency(self) -> None:
        for lane in Lane:
            assert f"notification_lane_{lane}_batch_size" in NUMERIC_BOUNDS
            assert LANE_CONCURRENCY[lane] >= 1
        assert set(LANE_CONCURRENCY) == set(Lane)

    def test_the_lanes_fit_the_pool(self) -> None:
        """Every lane's slots each hold a journal session while they
        call, every lane's attempt holds one, the maintenance loop holds
        one: all of it at once fits the worker's pool, so no journal
        write waits for a connection.

        P4-2: the worker's wake listener holds a connection for the
        process's whole life, and it is NOT in this sum: the sum already
        fills the pool exactly, so the listener has its own engine
        outside it (app/engine/wake.py) -- asserted beside this sum, not
        added to it.
        """
        held = (
            sum(LANE_CONCURRENCY.values())
            + len(Lane)
            + worker.MAINTENANCE_SESSIONS
        )
        assert held <= POOL_SIZE + POOL_MAX_OVERFLOW
        assert worker.MAINTENANCE_SESSIONS >= 1
        listener = wake_engine()
        assert isinstance(listener.pool, NullPool)
        assert listener is not get_engine()

    def test_zero_is_not_off(self) -> None:
        with pytest.raises(ValueError) as caught:
            Settings(_env_file=None, notification_lane_bulk_batch_size=0)
        message = str(caught.value)
        assert "NOTIFICATION_LANE_BULK_BATCH_SIZE=0 is outside" in message
        assert "leaving the lane out of COMMS_WORKER_LANES" in message


# -----------------------------------------------------------------------------
# Б3 -- the process's lanes come from its environment
# -----------------------------------------------------------------------------


class TestTheSetting:
    def test_unset_takes_every_lane_in_lane_order(self) -> None:
        assert Settings(_env_file=None).worker_lanes == tuple(Lane)

    @pytest.mark.parametrize(("value", "lanes"), [
        ("critical", (Lane.CRITICAL,)),
        (" bulk , interactive ", (Lane.INTERACTIVE, Lane.BULK)),
        ("bulk,normal,critical,interactive", tuple(Lane)),
    ])
    def test_a_set_takes_exactly_its_lanes(
        self, value: str, lanes: tuple[Lane, ...],
    ) -> None:
        assert Settings(_env_file=None, comms_worker_lanes=value).worker_lanes == lanes

    @pytest.mark.parametrize(("value", "says"), [
        ("", "names no lane"),
        (" , ", "names no lane"),
        ("critical,,bulk", "an empty item between commas"),
        ("urgent", "'urgent' is not a lane"),
        ("Critical", "'Critical' is not a lane"),
        ("bulk,bulk", "'bulk' is listed twice"),
    ])
    def test_a_bad_set_refuses_startup_naming_the_variable(
        self, value: str, says: str,
    ) -> None:
        with pytest.raises(ValueError) as caught:
            Settings(_env_file=None, comms_worker_lanes=value)
        message = str(caught.value)
        assert f"COMMS_WORKER_LANES={value!r}" in message
        assert says in message

    def test_written_empty_in_the_environment_is_not_unset(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The variable as the process gets it: present and empty
        refuses; the pair, absent, takes all four."""
        monkeypatch.setenv("COMMS_WORKER_LANES", "")
        with pytest.raises(ValueError, match="COMMS_WORKER_LANES=''"):
            Settings(_env_file=None)
        monkeypatch.delenv("COMMS_WORKER_LANES")
        assert Settings(_env_file=None).worker_lanes == tuple(Lane)


# -----------------------------------------------------------------------------
# правка 1, 2 -- one loop per lane, maintenance beside them
# -----------------------------------------------------------------------------


_INTERVAL = 1  # seconds: the poll interval AND the backoff cap in the
# loop tests, so a lane's interval is the base interval throughout


async def _sent_within(nid: UUID, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if await _status(nid) == NotificationStatus.SENT:
            return True
        await asyncio.sleep(0.05)
    return False


@contextmanager
def _fast_loops(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(settings, "notification_poll_interval_seconds", _INTERVAL)
    monkeypatch.setattr(settings, "notification_max_backoff_seconds", _INTERVAL)
    yield


async def _run(task_body: Callable[[], Any]) -> asyncio.Task[None]:
    task = asyncio.ensure_future(task_body())
    await asyncio.sleep(0)
    return task


async def _stop(task: asyncio.Task[None]) -> None:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


class TestTheLoops:
    async def test_interactive_goes_while_a_bulk_pass_is_held(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """правка 1: bulk's pass is held inside a channel call; a job of
        interactive accepted meanwhile is delivered within one poll
        interval of its lane, the bulk call still held. M2: with one
        loop for every lane it waits for the bulk call."""
        _declare("unit_event_in_app", Lane.BULK)
        recipient = await _recipient()
        bulk = await _intake("unit_event_in_app", recipient)
        spy = _Spy(held=Lane.BULK.value)

        async def idle() -> int:
            return 0

        monkeypatch.setattr(worker, "run_maintenance_pass", idle)
        with _fast_loops(monkeypatch), _channel(spy):
            task = await _run(worker.run_worker_loop)
            try:
                await asyncio.wait_for(spy.started.wait(), 5 * _INTERVAL)
                urgent = await _intake(_DECLARED, recipient)
                assert await _sent_within(urgent, _INTERVAL + 1.0)
                assert not spy.release.is_set()
                assert await _status(bulk) != NotificationStatus.SENT
                spy.release.set()
                # The pair: the held lane was alive all along.
                assert await _sent_within(bulk, _INTERVAL + 1.0)
            finally:
                spy.release.set()
                await _stop(task)

    async def test_a_held_maintenance_pass_holds_no_lane(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """правка 2: the maintenance pass waits (as the expiry sweep
        waits FOR UPDATE for an attempt); a normal job is delivered
        within one poll interval all the same. M12."""
        recipient = await _recipient()
        held = asyncio.Event()
        entered = asyncio.Event()

        async def waiting() -> int:
            entered.set()
            await held.wait()
            return 0

        monkeypatch.setattr(worker, "run_maintenance_pass", waiting)
        with _fast_loops(monkeypatch), _channel():
            task = await _run(worker.run_worker_loop)
            try:
                await asyncio.wait_for(entered.wait(), 5 * _INTERVAL)
                nid = await _intake("unit_event_in_app", recipient)
                assert await _sent_within(nid, _INTERVAL + 1.0)
                assert not held.is_set()
            finally:
                held.set()
                await _stop(task)

    async def test_one_loop_per_lane_and_one_for_maintenance(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Б3 (4): the startup line names the process's lanes and their
        budgets; a loop is started for each lane and one for
        maintenance, none for a lane the process does not take. M10.

        P4-2: each loop is now started with its wake event, and the
        process's one wake listener gets the lane loops' events by lane
        -- the maintenance loop's is not among them. The loops' log
        fields are asserted exactly as before.
        """
        started: list[dict[str, str]] = []
        wakes: dict[str, asyncio.Event] = {}
        listened: list[dict[str, asyncio.Event]] = []

        async def record(run_pass: Any, wake: asyncio.Event, **fields: str) -> None:
            started.append(fields)
            wakes[fields.get("lane", "maintenance")] = wake

        async def listen(events: dict[str, asyncio.Event]) -> None:
            listened.append(dict(events))

        monkeypatch.setattr(worker, "_poll_loop", record)
        monkeypatch.setattr(worker, "listen_for_wakes", listen)
        _lanes(monkeypatch, "critical,bulk")
        with capture_logs() as logs:
            await worker.run_worker_loop()
        assert started == [
            {"lane": "critical"}, {"lane": "bulk"}, {"loop": "maintenance"},
        ]
        assert listened == [
            {"critical": wakes["critical"], "bulk": wakes["bulk"]},
        ]
        assert wakes["maintenance"] not in listened[0].values()
        assert len({id(wake) for wake in wakes.values()}) == 3
        (line,) = [
            log for log in logs if log["event"] == "notification_worker_started"
        ]
        assert line["lanes"] == ["critical", "bulk"]
        assert line["lane_budgets"] == {
            "critical": {
                "batch_size": settings.notification_lane_critical_batch_size,
                "concurrency": LANE_CONCURRENCY[Lane.CRITICAL],
            },
            "bulk": {
                "batch_size": settings.notification_lane_bulk_batch_size,
                "concurrency": LANE_CONCURRENCY[Lane.BULK],
            },
        }


# -----------------------------------------------------------------------------
# правка 3 (gate) -- the race of attempt and expiry, on real sessions
# -----------------------------------------------------------------------------


async def _lock_waiters() -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(text(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE wait_event_type = 'Lock' AND datname = current_database()"
        ))).scalar_one())


class TestAttemptAgainstExpiry:
    async def test_a_journal_write_does_not_queue_behind_a_waiting_expiry(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The order that would hang the worker with no 40P01: the
        attempt holds the job FOR NO KEY UPDATE; the expiry is QUEUED on
        the row with FOR UPDATE (seen in pg_stat_activity); only then
        the channel's answer is journaled in its own session -- a
        KEY SHARE on the job through the foreign key. Every checkout
        carries lock_timeout, so a journal write that queued would fail
        (logged, the row missing), not wait: it commits, and the expiry, once the
        attempt commits, closes the accepted delivery SENT, not EXPIRED
        (P2-1). The expiry's own session waits longer than the test."""
        recipient = await _recipient()
        nid = await _intake(
            "unit_event_in_app", recipient,
            expiry_at=datetime.now(UTC) + timedelta(hours=1),
        )
        async with get_session_factory()() as session:
            job = await session.get(Notification, nid)
            assert job is not None
            await resolve_notification(session, job)
            job.expiry_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        await dispose_engine()

        def short_lock_timeout(
            dbapi_connection: Any, record: Any, proxy: Any,
        ) -> None:
            # At every checkout, not at connect: a SET on connect runs in
            # a transaction the pool's reset rolls back.
            cursor = dbapi_connection.cursor()
            cursor.execute("SET lock_timeout = '2s'")
            cursor.close()

        engine = get_engine()
        event.listen(engine.sync_engine, "checkout", short_lock_timeout)
        factory = get_session_factory()
        try:
            # The pair: the bound is really in force on a fresh session.
            async with factory() as probe:
                assert (await probe.execute(
                    text("SHOW lock_timeout"),
                )).scalar_one() == "2s"
            async with factory() as attempt:
                job = (await attempt.execute(
                    select(Notification).where(Notification.id == nid)
                    .with_for_update(skip_locked=True, key_share=True)
                )).scalar_one()
                delivery = (await attempt.execute(
                    select(NotificationDelivery)
                    .where(NotificationDelivery.notification_id == nid)
                )).scalar_one()

                async def expire() -> int:
                    async with factory() as session:
                        await session.execute(text(
                            "SET LOCAL lock_timeout = '60s'"
                        ))
                        closed = await close_notifications(
                            session,
                            Notification.id == nid,
                            NotificationStatus.EXPIRED,
                        )
                        await session.commit()
                        return closed

                waiting = asyncio.ensure_future(expire())
                for _ in range(100):
                    if await _lock_waiters() == 1:
                        break
                    await asyncio.sleep(0.05)
                assert await _lock_waiters() == 1, "the expiry is not queued"

                with capture_logs() as logs:
                    await journal.record_channel_answer(
                        delivery, journal.ACCEPTED,
                    )
                assert [
                    log for log in logs if log.get("log_level") == "error"
                ] == []
                assert not waiting.done()
                assert await _lock_waiters() == 1

                delivery.status = DeliveryStatus.SENT
                delivery.sent_at = datetime.now(UTC)
                job.status = NotificationStatus.SENT
                await attempt.commit()
            assert await asyncio.wait_for(waiting, 10) == 0
        finally:
            event.remove(engine.sync_engine, "checkout", short_lock_timeout)
            await dispose_engine()

        async with get_session_factory()() as session:
            answers = (await session.execute(
                select(NotificationTransition.outcome).where(
                    NotificationTransition.notification_id == nid,
                    NotificationTransition.subject == JournalSubject.CHANNEL,
                )
            )).scalars().all()
        assert answers == [ChannelAnswer.ACCEPTED]
        assert await _status(nid) == NotificationStatus.SENT


# -----------------------------------------------------------------------------
# Б4 -- the contract
# -----------------------------------------------------------------------------


class TestTheContract:
    def _section(self) -> str:
        start = _DOC.index("### `lane`")
        end = _DOC.index("\n### ", start)
        return _DOC[start:end]

    def _table(self) -> dict[str, str]:
        rows = {}
        for line in self._section().splitlines():
            match = re.match(r"\| `([a-z_]+)` \| (.*) \|$", line)
            if match:
                rows[match.group(1)] = match.group(2)
        assert rows, "the value table is empty"
        return rows

    def test_the_values_are_the_loader_s_both_ways(self) -> None:
        assert set(self._table()) == set(LANES) == {lane.value for lane in Lane}

    def test_the_default_is_the_loader_s(self) -> None:
        defaults = [v for v, row in self._table().items() if "the default" in row]
        assert defaults == [_FIELDS["lane"].default()[0]]

    def test_the_declaration_is_written_as_the_loader_reads_it(self) -> None:
        assert "    lane: interactive\n" in self._section()

    def test_what_it_does_not_do_is_said(self) -> None:
        section = self._section()
        assert "It is not a priority" in section
        assert "It passes no gate" in section

    def test_the_deploy_covers_every_lane(self) -> None:
        """правка 3: the union of the workers' lanes is the deploy's
        duty, the cost of missing one is said, and the startup log
        fields named are the ones the worker writes (TestTheLoops)."""
        section = self._section()
        assert "`COMMS_WORKER_LANES`" in section
        assert "together must take all four lanes" in section
        assert "neither delivered nor expired" in section
        for field in ("`notification_worker_started`", "`lanes`", "`lane_budgets`"):
            assert field in section
