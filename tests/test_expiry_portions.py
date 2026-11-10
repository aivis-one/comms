# =============================================================================
# PERF-1 -- the expiry sweep closes the overdue jobs in portions
# =============================================================================
#
# What is held here, on real sessions:
#   1 a backlog of 3 portions + 1 is closed `expired`, every job, in four
#     transactions, none of them holding more than a portion -- counted
#     per call of close_notifications, each with its own txid_current();
#   2 the pair: fewer than a portion -> one transaction, as before;
#   3 an attempt holding a job of a later portion (the job row FOR NO KEY
#     UPDATE, as the attempt takes it) -> the sweep WAITS for it
#     (pg_blocking_pids, never timing) and closes the job after it: the
#     order of app/engine/processor.py THE LOCK RULE, no SKIP LOCKED.
#
# The portion is shrunk to 3 (processor._EXPIRY_PORTION, module-level for
# this). Other tests may leave overdue jobs behind: each test first
# drains the lane, then counts only from there.
#
# MUTATIONS (each turns one red):
#   M1 the sweep without its limit -> test_a_backlog_in_portions (one
#      call closes all ten)
#   M2 one transaction for every portion (the commit after the loop) ->
#      test_a_backlog_in_portions (one txid for four calls)
#   M3 SKIP LOCKED in the sweep's lock -> test_the_sweep_waits_for_the_attempt
# =============================================================================

import asyncio
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select, text, update

from app.core.database import get_session_factory
from app.core.lanes import Lane
from app.engine import processor
from app.engine.constants import NotificationStatus, TargetType
from app.engine.models import Notification
from app.engine.service import create_notification
from tests.helpers import create_recipient, intake_fields, next_api1_telegram_id

_PORTION = 3
_BOUND = 10.0
_POLL = 0.02


@pytest.fixture
def small_portion(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(processor, "_EXPIRY_PORTION", _PORTION)


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[tuple[int, int]]]:
    """Every call of close_notifications the sweep makes: (its
    transaction's txid, the jobs it closed)."""
    seen: list[tuple[int, int]] = []
    real = processor.close_notifications

    async def spy(session: Any, *args: Any, **kwargs: Any) -> int:
        txid = await session.scalar(text("SELECT txid_current()"))
        closed = await real(session, *args, **kwargs)
        seen.append((int(txid), closed))
        return closed

    monkeypatch.setattr(processor, "close_notifications", spy)
    yield seen


async def _overdue(count: int) -> tuple[list[UUID], Lane]:
    async with get_session_factory()() as s:
        person = await create_recipient(s, telegram_id=next_api1_telegram_id())
        ids = []
        for _ in range(count):
            job = await create_notification(
                s, **intake_fields(), type="unit_event_in_app", title="T",
                body="B", target_type=TargetType.USER,
                target_value=str(person.id),
                expiry_at=datetime.now(UTC) + timedelta(hours=1),
            )
            ids.append(job.id)
        lane = Lane(job.lane)
        await s.execute(
            update(Notification)
            .where(Notification.id.in_(ids))
            .values(expiry_at=datetime.now(UTC) - timedelta(seconds=1))
        )
        await s.commit()
    return sorted(ids), lane


async def _statuses(ids: list[UUID]) -> set[str]:
    async with get_session_factory()() as s:
        rows = await s.scalars(
            select(Notification.status).where(Notification.id.in_(ids))
        )
        return set(rows)


async def _drain(lane: Lane) -> None:
    """Close what other tests left overdue in the lane, uncounted."""
    await processor.expire_overdue_notifications([lane])


async def _pid(session: Any) -> int:
    return int(await session.scalar(text("SELECT pg_backend_pid()")))


async def _until_waiting(holder: int, task: asyncio.Task[Any]) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _BOUND
    while True:
        async with get_session_factory()() as s:
            waiting = await s.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() "
                    "AND CAST(:holder AS integer) = ANY(pg_blocking_pids(pid))"
                ),
                {"holder": holder},
            )
        if waiting:
            return
        assert not task.done(), f"the sweep ended without waiting: {task.result()!r}"
        assert loop.time() < deadline, "the sweep never waited"
        await asyncio.sleep(_POLL)


class TestTheSweepInPortions:
    async def test_a_backlog_in_portions(
        self, stub_profile: None, small_portion: None,
        calls: list[tuple[int, int]],
    ) -> None:
        ids, lane = await _overdue(3 * _PORTION + 1)
        await _drain(lane)  # closes ours too: count from a fresh backlog
        calls.clear()
        ids, lane = await _overdue(3 * _PORTION + 1)
        assert await processor.expire_overdue_notifications([lane]) == len(ids)
        assert await _statuses(ids) == {NotificationStatus.EXPIRED}
        assert [closed for _, closed in calls] == [3, 3, 3, 1]
        txids = [txid for txid, _ in calls]
        assert len(set(txids)) == len(txids), "one transaction per portion"

    async def test_fewer_than_a_portion_is_one_transaction(
        self, stub_profile: None, small_portion: None,
        calls: list[tuple[int, int]],
    ) -> None:
        ids, lane = await _overdue(_PORTION - 1)
        await _drain(lane)
        calls.clear()
        ids, lane = await _overdue(_PORTION - 1)
        assert await processor.expire_overdue_notifications([lane]) == len(ids)
        assert await _statuses(ids) == {NotificationStatus.EXPIRED}
        assert [closed for _, closed in calls] == [_PORTION - 1]

    async def test_the_sweep_waits_for_the_attempt(
        self, stub_profile: None, small_portion: None,
    ) -> None:
        ids, lane = await _overdue(3 * _PORTION + 1)
        await _drain(lane)
        ids, lane = await _overdue(3 * _PORTION + 1)
        held = ids[_PORTION + 1]  # a job of the second portion
        async with get_session_factory()() as attempt:
            await attempt.execute(
                select(Notification)
                .where(Notification.id == held)
                .with_for_update(key_share=True)
            )
            holder = await _pid(attempt)
            sweep = asyncio.create_task(
                processor.expire_overdue_notifications([lane])
            )
            await _until_waiting(holder, sweep)
            assert await _statuses([held]) == {NotificationStatus.PENDING}
            await attempt.rollback()
        assert await asyncio.wait_for(sweep, _BOUND) == len(ids)
        assert await _statuses(ids) == {NotificationStatus.EXPIRED}
