# =============================================================================
# H5 -- retention's delete through the primary key; the ceiling of a
# maintenance pass; the schedule gate errs toward the recipient
# =============================================================================
#
# Since R-1 there is no expiry cleanup: a finished job past its
# expiry_at is kept, with its key, until retention removes it
# (tests/test_kept_until_retention.py). Its index,
# ix_notifications_expired_deletable (migration 0023), is read by no
# query and stays until the next major -- the KNOWN CEILING on its entry
# in app/core/schema_objects.py; tests/test_schema_objects.py holds its
# definition against pg_indexes. What this file held of the cleanup and
# of the index went with it (R-1 report); what stands:
#
# Б1: retention's batch delete goes through the primary key
# (`id = ANY(ARRAY(...))`), never a scan of notifications outside the
# batch select. Plans are read with EXPLAIN on the very statement the
# code executes (captured on the connection), over a seeded table of
# realistic shape. One pass of the purge of the model answers past their
# term clears at most _EXPIRED_MAX_BATCHES_PER_PASS batches -- the
# ceiling the expiry cleanup shared with it until R-1. Two retention
# passes (two workers) over the same rows: one waits, each row deleted
# once.
#
# Б2: the schedule gate asks the recipient's schedule for every value
# but bypass.
#
# MUTATIONS these tests were written against (each turns one red):
#   M2r retention's delete back on IN (subquery)
#                                       -> TestThePlan (retention outer)
#   M4  no ceiling in the purge (drain to the end)
#                                       -> TestTheCeiling
#   M5  the ceiling counted in rows, not batches
#                                       -> TestTheCeiling
#   M6  the gate back on == respect     -> TestTheGate (unknown value)
#   M7  the gate asking always          -> TestTheGate (bypass)
# =============================================================================

import asyncio
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import event, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

import app.engine.processor as processor
from app.core.database import get_engine, get_session_factory
from app.engine import service
from app.engine.constants import DeliveryStatus, NotificationStatus
from app.engine.models import LLMAnswer, Notification, NotificationDelivery
from app.engine.processor import purge_expired_llm_answers
from app.engine.service import delete_terminal_notifications_batch
from tests.helpers import notification_row_fields, row_waiters, sql_shape
from tests.test_recipient_schedule_field import (
    _channel,
    _declare,
    _intake,
    _outside_its_schedule,
)

_BOUND = 10.0
_POLL = 0.01


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


async def _job() -> UUID:
    """A finished job (sent): what retention deletes."""
    async with get_session_factory()() as session:
        job = Notification(
            type="unit_event_in_app", title="T", body="B",
            target_type="all", target_value="*",
            status=NotificationStatus.SENT, **notification_row_fields(),
        )
        session.add(job)
        await session.commit()
        return job.id


async def _finished(n: int) -> list[UUID]:
    return [await _job() for _ in range(n)]


async def _answers_past_their_term(n: int) -> None:
    """`n` jobs, each with a model's answer whose body is still kept
    and whose keep_until has passed."""
    now = datetime.now(UTC)
    async with get_session_factory()() as session:
        for job in await _finished(n):
            session.add(LLMAnswer(
                notification_id=job, body="the answer", answered_set="s",
                answered_at=now - timedelta(hours=2),
                keep_until=now - timedelta(hours=1),
            ))
        await session.commit()


async def _kept_answers() -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(
            select(func.count()).select_from(LLMAnswer)
            .where(LLMAnswer.body.is_not(None))
        )).scalar_one())


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
            (s, p) for s, p in seen if sql_shape(s).verb == "DELETE FROM"
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
# Б1 (2) and gate item 1 -- the plans
# -----------------------------------------------------------------------------


@pytest.mark.usefixtures("pristine_tables")
class TestThePlan:
    @pytest.fixture(autouse=True)
    async def _realistic(self, pristine_tables: None) -> None:
        # After pristine_tables, never before: it would truncate the seed.
        await _seed(total=50000, every=200)

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
    """The ceiling per pass, on the purge of the model answers past their
    term -- its one user since R-1 removed the expiry cleanup, whose
    ceiling these tests held until then (same loop shape, same
    constant)."""

    @pytest.fixture(autouse=True)
    def _small(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(processor, "_RETENTION_BATCH_SIZE", 2)
        monkeypatch.setattr(processor, "_EXPIRED_MAX_BATCHES_PER_PASS", 3)

    async def test_a_backlog_past_the_ceiling_goes_over_several_ticks(
        self,
    ) -> None:
        """Ten past their term, batches of two, three batches a pass: 6,
        then 4, then nothing -- the backlog continues where the last
        pass stopped."""
        await _answers_past_their_term(10)
        assert await purge_expired_llm_answers() == 6
        assert await _kept_answers() == 4
        assert await purge_expired_llm_answers() == 4
        assert await purge_expired_llm_answers() == 0
        assert await _kept_answers() == 0

    async def test_exactly_the_ceiling(self) -> None:
        await _answers_past_their_term(6)
        assert await purge_expired_llm_answers() == 6
        assert await purge_expired_llm_answers() == 0

    async def test_under_the_ceiling_drains_in_one_call(self) -> None:
        await _answers_past_their_term(5)
        assert await purge_expired_llm_answers() == 5
        assert await _kept_answers() == 0


def test_the_production_ceiling() -> None:
    """Ten batches a pass (outside the class that shrinks it)."""
    assert processor._EXPIRED_MAX_BATCHES_PER_PASS == 10
    assert processor._RETENTION_BATCH_SIZE == 1000


# -----------------------------------------------------------------------------
# Gate item 2 -- deleters against each other
# -----------------------------------------------------------------------------


async def _pid(session: AsyncSession) -> int:
    return int((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())


async def _waiting_deleters() -> list[int]:
    """The backends waiting to DELETE a job row -- by their LOCKS
    (tests/helpers.py row_waiters): a tuple lock FOR UPDATE under a
    RowExclusiveLock of notifications. Until TX-3 by the text's prefix
    (query ILIKE 'DELETE FROM notifications%'), right while the text was
    the identity; a waiter is identified by what it waits with now."""
    return [
        pid for pid, wait in await row_waiters("notifications")
        if wait.is_delete
    ]


class TestConcurrentDeleters:
    async def test_one_waits_both_finish_each_row_once(self) -> None:
        """A holds a batch of retention; B -- another worker's retention
        (the cadence gate is per process, app/engine/processor.py
        cleanup_terminal_notifications) -- deletes the same rows: B
        waits for A (observed, not timed), A commits, B finishes.
        Deleted exactly n, once. Until R-1 the pair was the expiry
        cleanup against itself and against retention, and a test of
        their crossing orders (expiry_at against created_at); since
        R-1 retention is the one deleter, and both of its batches take
        their rows in one order."""
        await _finished(4)
        cutoff = datetime.now(UTC) + timedelta(seconds=1)
        factory = get_session_factory()
        async with factory() as a, factory() as b:
            assert await delete_terminal_notifications_batch(
                a, cutoff=cutoff, limit=100,
            ) == 4
            b_pid = await _pid(b)
            b_task = asyncio.ensure_future(
                delete_terminal_notifications_batch(b, cutoff=cutoff, limit=100),
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
