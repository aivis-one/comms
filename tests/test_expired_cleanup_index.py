# =============================================================================
# H5 -- the expiry cleanup by index, a ceiling per pass; the schedule
# gate errs toward the recipient
# =============================================================================
#
# Б1: the batch select of the expiry cleanup runs on
# ix_notifications_expired_deletable (migration 0023) and both batch
# deletes -- the cleanup's and retention's -- go through the primary key
# (`id = ANY(ARRAY(...))`), never a scan of notifications outside the
# batch select. Plans are read with EXPLAIN on the very statement the
# code executes (captured on the connection), over a seeded table of
# realistic shape. One pass deletes at most
# _EXPIRED_MAX_BATCHES_PER_PASS batches.
#
# Б2: the schedule gate asks the recipient's schedule for every value
# but bypass.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the index not created           -> TestThePlan (expiry select)
#   M2  the cleanup's delete back on IN (subquery)
#                                       -> TestThePlan (expiry outer)
#   M2r retention's delete back on IN (subquery)
#                                       -> TestThePlan (retention outer)
#   M3  the index predicate without the statuses
#                                       -> TestTheIndex
#   M4  no ceiling (drain to the end)   -> TestTheCeiling
#   M5  the ceiling counted in rows, not batches
#                                       -> TestTheCeiling
#   M6  the gate back on == respect     -> TestTheGate (unknown value)
#   M7  the gate asking always          -> TestTheGate (bypass)
#   M8  no commit between the cleanup's batches
#                                       -> TestConcurrentDeleters: 40P01
# =============================================================================

import asyncio
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import event, func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

import app.engine.processor as processor
from app.core.database import get_engine, get_session_factory
from app.engine import service
from app.engine.constants import DeliveryStatus, NotificationStatus
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import cleanup_expired_notifications
from app.engine.service import (
    _EXPIRED_DELETABLE_STATUSES,
    delete_expired_notifications_batch,
    delete_terminal_notifications_batch,
)
from tests.helpers import notification_row_fields
from tests.test_recipient_schedule_field import (
    _channel,
    _declare,
    _intake,
    _outside_its_schedule,
)

_INDEX = "ix_notifications_expired_deletable"
_BOUND = 10.0
_POLL = 0.01


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


async def _job(
    *,
    status: str = NotificationStatus.SENT,
    expiry_at: datetime | None,
    created_at: datetime | None = None,
) -> UUID:
    async with get_session_factory()() as session:
        job = Notification(
            type="unit_event_in_app", title="T", body="B",
            target_type="all", target_value="*", status=status,
            expiry_at=expiry_at, **notification_row_fields(),
        )
        session.add(job)
        await session.flush()
        if created_at is not None:
            await session.execute(
                text("UPDATE notifications SET created_at = :c WHERE id = :i"),
                {"c": created_at, "i": job.id},
            )
        await session.commit()
        return job.id


async def _expired(n: int) -> list[UUID]:
    now = datetime.now(UTC)
    return [
        await _job(expiry_at=now - timedelta(minutes=n - i))
        for i in range(n)
    ]


async def _left() -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(
            select(func.count()).select_from(Notification)
        )).scalar_one())


async def _seed(total: int, every: int) -> None:
    """A table of realistic shape: `total` jobs, a third of them with an
    expiry spread over `every` hours around now -- the first twenty of
    those hours already past, so a small share is expired; failed and
    active rows besides, created over the last day or so."""
    async with get_session_factory()() as session:
        await session.execute(text(
            "INSERT INTO notifications (id, type, title, body, target_type, "
            "target_value, idempotency_key, fingerprint, channels, "
            "expiry_layer, push_on, retry_max_attempts, "
            "retry_backoff_seconds, recipient_schedule, lane, status, "
            "expiry_at, created_at) "
            "SELECT gen_random_uuid(), 'unit_event', 'T', 'B', 'all', '*', "
            "'h5-seed-' || g, repeat('a', 64), '[\"in_app\"]'::jsonb, "
            "'default', 'none', 3, 30, 'respect', 'normal', "
            "CASE WHEN g % 50 = 0 THEN 'failed' "
            "     WHEN g % 20 = 0 THEN 'pending' ELSE 'sent' END, "
            "CASE WHEN g % 3 = 0 "
            "     THEN now() + ((g % :every) - 20) * interval '1 hour' END, "
            "now() - (g % 1000) * interval '1 minute' "
            "FROM generate_series(1, :total) g"
        ), {"total": total, "every": every})
        await session.commit()
        await session.execute(text("ANALYZE notifications"))
        await session.commit()


@contextmanager
def _captured() -> Iterator[list[tuple[str, Any]]]:
    """Every statement the engine sends, with its parameters."""
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


async def _plan_of(batch: Any, **kwargs: Any) -> dict[str, Any]:
    """The EXPLAIN plan of the DELETE `batch` executes -- the statement
    captured off the connection, explained with its own parameters, in a
    transaction rolled back."""
    async with get_session_factory()() as session:
        with _captured() as seen:
            await batch(session, **kwargs)
        await session.rollback()
        (statement, parameters), = [
            (s, p) for s, p in seen if s.lstrip().upper().startswith("DELETE")
        ]
        raw = await session.connection()
        rows = await raw.exec_driver_sql(
            "EXPLAIN (FORMAT JSON) " + statement, parameters,
        )
        plan = rows.scalar_one()
        await session.rollback()
    return (json.loads(plan) if isinstance(plan, str) else plan)[0]["Plan"]


def _nodes(plan: dict[str, Any], *, skip_init_plans: bool = False) -> list[dict]:
    out = [plan]
    for child in plan.get("Plans", []):
        if skip_init_plans and child.get("Parent Relationship") == "InitPlan":
            continue
        out.extend(_nodes(child, skip_init_plans=skip_init_plans))
    return out


def _seq_scans_of_notifications(nodes: list[dict]) -> list[dict]:
    return [
        n for n in nodes
        if n["Node Type"] == "Seq Scan" and n.get("Relation Name") == "notifications"
    ]


def _index_names(nodes: list[dict]) -> set[str]:
    return {n["Index Name"] for n in nodes if "Index Name" in n}


# -----------------------------------------------------------------------------
# Б1 (1) -- the index, as the schema holds it
# -----------------------------------------------------------------------------


class TestTheIndex:
    async def test_its_predicate_is_the_cleanup_s_status_list(
        self, db_session: AsyncSession,
    ) -> None:
        """Read back from pg_indexes: the statuses of the predicate are
        exactly _EXPIRED_DELETABLE_STATUSES, both ways, and the key is
        expiry_at."""
        definition = (await db_session.execute(text(
            "SELECT indexdef FROM pg_indexes WHERE indexname = :name"
        ), {"name": _INDEX})).scalar_one()
        statuses = set(re.findall(r"'([a-z_]+)'::character varying", definition))
        assert statuses == {str(s) for s in _EXPIRED_DELETABLE_STATUSES}
        assert "btree (expiry_at)" in definition
        assert "expiry_at IS NOT NULL" in definition
        assert NotificationStatus.FAILED not in statuses


# -----------------------------------------------------------------------------
# Б1 (2) and gate item 1 -- the plans
# -----------------------------------------------------------------------------


@pytest.mark.usefixtures("pristine_tables")
class TestThePlan:
    @pytest.fixture(autouse=True)
    async def _realistic(self, pristine_tables: None) -> None:
        # After pristine_tables, never before: it would truncate the seed.
        await _seed(total=50000, every=200)

    async def test_the_cleanup_batch_runs_on_the_index_and_the_pkey(self) -> None:
        """No scan of notifications anywhere in the plan: the batch is
        selected on the partial index, the rows deleted through the
        primary key. The pair: both indexes are named in the plan."""
        plan = await _plan_of(
            delete_expired_notifications_batch,
            now=datetime.now(UTC), limit=1000,
        )
        nodes = _nodes(plan)
        assert _seq_scans_of_notifications(nodes) == []
        assert {_INDEX, "notifications_pkey"} <= _index_names(nodes)
        # The batch is selected ONCE, as an InitPlan, and the delete
        # walks the primary key -- the shape `id = ANY(ARRAY(...))`
        # gives whatever the estimates. `id IN (...)` leaves the join
        # to the planner: a nested loop on the pkey here, a hash
        # semi-join over a scan of the whole table on other estimates.
        assert any(
            child.get("Parent Relationship") == "InitPlan"
            for child in plan.get("Plans", [])
        )
        assert "notifications_pkey" in _index_names(
            _nodes(plan, skip_init_plans=True),
        )

    async def test_the_retention_delete_scans_nothing_outside_its_select(
        self,
    ) -> None:
        """Retention's delete goes through the primary key too. Its
        batch SELECT may still scan -- that is BL-3, its own marker -- so
        only the plan outside the InitPlan is asserted."""
        plan = await _plan_of(
            delete_terminal_notifications_batch,
            cutoff=datetime.now(UTC), limit=1000,
        )
        outer = _nodes(plan, skip_init_plans=True)
        assert _seq_scans_of_notifications(outer) == []
        assert "notifications_pkey" in _index_names(outer)
        assert any(
            child.get("Parent Relationship") == "InitPlan"
            for child in plan.get("Plans", [])
        )


# -----------------------------------------------------------------------------
# Б1 (3), (4) -- the ceiling per pass
# -----------------------------------------------------------------------------


class TestTheCeiling:
    @pytest.fixture(autouse=True)
    def _small(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(processor, "_RETENTION_BATCH_SIZE", 2)
        monkeypatch.setattr(processor, "_EXPIRED_MAX_BATCHES_PER_PASS", 3)

    async def test_a_backlog_past_the_ceiling_goes_over_several_ticks(
        self,
    ) -> None:
        """Ten expired, batches of two, three batches a pass: 6, then 4,
        then nothing -- the backlog continues where the last pass
        stopped."""
        await _expired(10)
        assert await cleanup_expired_notifications() == 6
        assert await _left() == 4
        assert await cleanup_expired_notifications() == 4
        assert await cleanup_expired_notifications() == 0
        assert await _left() == 0

    async def test_exactly_the_ceiling(self) -> None:
        await _expired(6)
        assert await cleanup_expired_notifications() == 6
        assert await cleanup_expired_notifications() == 0

    async def test_under_the_ceiling_drains_in_one_call(self) -> None:
        await _expired(5)
        assert await cleanup_expired_notifications() == 5
        assert await _left() == 0



def test_the_production_ceiling() -> None:
    """Ten batches a pass (outside the class that shrinks it)."""
    assert processor._EXPIRED_MAX_BATCHES_PER_PASS == 10
    assert processor._RETENTION_BATCH_SIZE == 1000


# -----------------------------------------------------------------------------
# Gate item 2 -- deleters against each other
# -----------------------------------------------------------------------------


async def _pid(session: AsyncSession) -> int:
    return int((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())


async def _waiting_deleters(exclude: int | None = None) -> list[int]:
    async with get_session_factory()() as observer:
        rows = (await observer.execute(text(
            "SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
            "AND query ILIKE 'DELETE FROM notifications%'"
        ))).scalars().all()
    return [pid for pid in rows if pid != exclude]


def _sqlstate(exc: BaseException | None) -> str | None:
    orig = getattr(exc, "orig", None)
    return getattr(orig, "sqlstate", None) or getattr(
        getattr(orig, "__cause__", None), "sqlstate", None,
    )


class TestConcurrentDeleters:
    @pytest.mark.parametrize("second", ["expiry", "retention"])
    async def test_one_waits_both_finish_each_row_once(self, second: str) -> None:
        """A holds a batch of the expiry cleanup; B -- another worker's
        expiry cleanup, or retention -- deletes the same rows: B waits
        for A (observed, not timed), A commits, B finishes. Deleted
        exactly n, once."""
        await _expired(4)
        now = datetime.now(UTC)
        factory = get_session_factory()
        async with factory() as a, factory() as b:
            assert await delete_expired_notifications_batch(
                a, now=now, limit=100,
            ) == 4
            b_pid = await _pid(b)
            if second == "expiry":
                b_task = asyncio.ensure_future(
                    delete_expired_notifications_batch(b, now=now, limit=100),
                )
            else:
                b_task = asyncio.ensure_future(
                    delete_terminal_notifications_batch(
                        b, cutoff=now + timedelta(seconds=1), limit=100,
                    ),
                )
            waited = False
            for _ in range(int(_BOUND / _POLL)):
                if b_pid in await _waiting_deleters():
                    waited = True
                    break
                if b_task.done():
                    break
                await asyncio.sleep(_POLL)
            await a.commit()
            b_deleted = await b_task
            await b.commit()
        assert waited
        assert b_deleted == 0
        assert await _left() == 0

    async def test_the_cleanup_against_retention_with_crossing_keys(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Keys that cross: X expires first but was created last, Y the
        other way round. Retention (B, batches of one) takes Y and holds
        it; the expiry cleanup (A, the real pass, batches of one) takes
        X, commits, then wants Y and waits for B -- B, for its next
        batch, does not find X (gone, committed) and wants nothing of
        A. Both reach the end; each row deleted once. Without the commit
        between the cleanup's batches (M8) A would hold X while waiting
        for Y, and B would want X: both wait, 40P01."""
        monkeypatch.setattr(processor, "_RETENTION_BATCH_SIZE", 1)
        monkeypatch.setattr(processor, "_EXPIRED_MAX_BATCHES_PER_PASS", 3)
        now = datetime.now(UTC)
        x = await _job(
            expiry_at=now - timedelta(hours=2),
            created_at=now - timedelta(days=1),
        )
        y = await _job(
            expiry_at=now - timedelta(hours=1),
            created_at=now - timedelta(days=2),
        )
        cutoff = now + timedelta(seconds=1)
        factory = get_session_factory()
        async with factory() as b:
            await b.execute(text("SET LOCAL deadlock_timeout = '3s'"))
            b_pid = await _pid(b)
            assert await delete_terminal_notifications_batch(
                b, cutoff=cutoff, limit=1,
            ) == 1  # Y, the oldest created
            a_task = asyncio.ensure_future(cleanup_expired_notifications())
            a_waits = False
            for _ in range(int(_BOUND / _POLL)):
                if await _waiting_deleters(exclude=b_pid):
                    a_waits = True
                    break
                if a_task.done():
                    break
                await asyncio.sleep(_POLL)
            assert a_waits
            b_error: BaseException | None = None
            try:
                b_second = await delete_terminal_notifications_batch(
                    b, cutoff=cutoff, limit=1,
                )
                await b.commit()
            except DBAPIError as exc:
                b_error, b_second = exc, -1
                await b.rollback()
        a_deleted = await a_task
        assert _sqlstate(b_error) is None, b_error
        assert b_second == 0
        assert a_deleted == 1  # X; Y was B's
        async with factory() as session:
            assert (await session.execute(
                select(func.count()).select_from(Notification)
                .where(Notification.id.in_([x, y]))
            )).scalar_one() == 0


# -----------------------------------------------------------------------------
# Б2 -- the polarity of the schedule gate
# -----------------------------------------------------------------------------


class TestTheGate:
    """The gate is read through the names it compares against
    (app/engine/service.py RecipientSchedule). To meet it with a value
    it does not know -- the CHECK keeps the column to two values -- the
    test swaps those names for a set where the stored 'respect' is no
    member (as when the enum grows or is renamed): the gate's known
    values and the stored one differ, nothing is written around the
    CHECK."""

    async def _through_the_pipeline(
        self, db: AsyncSession, declared: str,
    ) -> tuple[str, str | None]:
        _declare(declared)
        nid = await _intake(db, await _outside_its_schedule(db))
        with _channel():
            await processor.process_pending_notifications()
        async with get_session_factory()() as session:
            (delivery,) = (await session.execute(
                select(NotificationDelivery)
                .where(NotificationDelivery.notification_id == nid)
            )).scalars().all()
            return delivery.status, delivery.wait_reason

    async def test_an_unknown_value_waits_for_the_recipient(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The gate knows {respect_v2, bypass}; the job carries
        'respect' -- a value it does not know. It waits."""
        monkeypatch.setattr(service, "RecipientSchedule", SimpleNamespace(
            RESPECT="respect_v2", BYPASS="bypass",
        ))
        assert await self._through_the_pipeline(db_session, "respect") == (
            DeliveryStatus.PENDING, "recipient_schedule",
        )

    async def test_bypass_goes(self, db_session: AsyncSession) -> None:
        """The pair: bypass is sent at once, outside the period."""
        assert await self._through_the_pipeline(db_session, "bypass") == (
            DeliveryStatus.SENT, None,
        )

    async def test_respect_waits(self, db_session: AsyncSession) -> None:
        assert await self._through_the_pipeline(db_session, "respect") == (
            DeliveryStatus.PENDING, "recipient_schedule",
        )
