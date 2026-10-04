# =============================================================================
# H1 Б2 -- the relay's delete against the cascades that delete jobs:
# no 40P01, both sides reach the end, no outbox row is lost
# =============================================================================
#
# Real sessions on real connections; the order is built with locks and
# observed in pg_stat_activity, never with sleeps.
#
#   R  the relay's delete (app/transport/push_relay.py delete_published)
#      of the published rows a (job X) and b (job Y);
#   T  retention (app/engine/service.py delete_terminal_notifications_batch,
#      the statement cleanup_terminal_notifications commits batch by
#      batch), one job per batch: Y, the older, first.
#
# The crossing: T has deleted Y, so its cascade holds b; R deletes [a, b]
# -- it locks a, and then b is T's; T deletes X, and its cascade needs
# a. A relay that WAITS for b closes the cycle: both wait on Lock, and
# Postgres kills one with 40P01. With FOR UPDATE SKIP LOCKED R never
# waits: it deletes a, skips b, and T goes on once R commits.
#
# MUTATIONS these tests were written against:
#   M5  the relay's delete without SKIP LOCKED -> TestTheCrossing sees
#       both sessions waiting on Lock, then 40P01
#   M6  NOWAIT instead of SKIP LOCKED -> R fails 55P03, a not deleted
# =============================================================================

import asyncio
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from fakeredis import aioredis as fakeaioredis
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

import app.transport.push_relay as relay
from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.constants import NotificationStatus, TargetType
from app.engine.models import Notification, PushOutbox
from app.engine.processor import cleanup_expired_notifications
from app.engine.service import delete_terminal_notifications_batch
from tests.helpers import notification_row_fields

_BOUND = 10.0
_POLL = 0.01


# -----------------------------------------------------------------------------
# Setup and observation
# -----------------------------------------------------------------------------


async def _jobs() -> tuple[UUID, UUID, int, int]:
    """Two terminal jobs -- Y older than X -- with one owed row each:
    a for X, inserted first, then b for Y. Returns (x, y, a, b)."""
    now = datetime.now(UTC)
    async with get_session_factory()() as session:
        y = Notification(
            type="unit_event_in_app", title="T", body="B",
            target_type=TargetType.ALL, target_value="*",
            status=NotificationStatus.SENT,
            expiry_at=now - timedelta(minutes=1),
            **notification_row_fields(),
        )
        x = Notification(
            type="unit_event_in_app", title="T", body="B",
            target_type=TargetType.ALL, target_value="*",
            status=NotificationStatus.SENT,
            expiry_at=now - timedelta(minutes=1),
            **notification_row_fields(),
        )
        session.add_all([y, x])
        await session.flush()
        await session.execute(
            update(Notification).where(Notification.id == y.id)
            .values(created_at=now - timedelta(days=2))
        )
        await session.execute(
            update(Notification).where(Notification.id == x.id)
            .values(created_at=now - timedelta(days=1))
        )
        row_a = PushOutbox(notification_id=x.id)
        session.add(row_a)
        await session.flush()
        row_b = PushOutbox(notification_id=y.id)
        session.add(row_b)
        await session.flush()
        ids = (x.id, y.id, row_a.id, row_b.id)
        await session.commit()
    return ids


async def _pid(session: AsyncSession) -> int:
    return int((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())


async def _waiting(pid: int) -> bool:
    async with get_session_factory()() as observer:
        kind = (await observer.execute(
            text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :pid"),
            {"pid": pid},
        )).scalar_one_or_none()
    return kind == "Lock"


async def _done_or_waiting(task: asyncio.Task[Any], pid: int) -> bool:
    """Wait until `task` finished (False) or its session waits on a lock
    (True) -- whichever comes first, observed, not timed."""
    for _ in range(int(_BOUND / _POLL)):
        if task.done():
            return False
        if await _waiting(pid):
            return True
        await asyncio.sleep(_POLL)
    raise AssertionError("neither done nor waiting within the bound")


async def _both_waiting(first: int, second: int) -> bool:
    for _ in range(int(_BOUND / _POLL)):
        if await _waiting(first) and await _waiting(second):
            return True
        await asyncio.sleep(_POLL)
    return False


def _sqlstate(exc: BaseException | None) -> str | None:
    orig = getattr(exc, "orig", None)
    return getattr(orig, "sqlstate", None) or getattr(
        getattr(orig, "__cause__", None), "sqlstate", None,
    )


async def _count(model: Any, *where: Any) -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(
            select(func.count()).select_from(model).where(*where)
        )).scalar_one())


async def _outcome(task: asyncio.Task[Any]) -> tuple[Any, BaseException | None]:
    try:
        return await task, None
    except DBAPIError as exc:
        return None, exc


def _later(work: Awaitable[Any]) -> asyncio.Task[Any]:
    return asyncio.ensure_future(work)


# -----------------------------------------------------------------------------
# The crossing
# -----------------------------------------------------------------------------


class TestTheCrossing:
    async def _cross(self, *, t_commits: bool) -> dict[str, Any]:
        """Build the crossing; return what each side did."""
        x, y, a, b = await _jobs()
        cutoff = datetime.now(UTC)
        factory = get_session_factory()
        seen: dict[str, Any] = {"x": x, "y": y, "a": a, "b": b}
        async with factory() as t, factory() as r:
            t_pid, r_pid = await _pid(t), await _pid(r)
            # Long enough for the observer to see a cycle before the
            # detector breaks it; the cycle itself is built by locks.
            for session in (t, r):
                await session.execute(text("SET LOCAL deadlock_timeout = '3s'"))
            # Precondition of the crossing: R meets a before b.
            order = (await t.execute(
                text("SELECT id FROM push_outbox ORDER BY ctid"),
            )).scalars().all()
            assert list(order) == [a, b]
            # T: retention's first batch -- Y; its cascade holds b.
            assert await delete_terminal_notifications_batch(
                t, cutoff=cutoff, limit=1,
            ) == 1
            # R: the relay's delete of both published rows.
            r_task = _later(relay.delete_published(r, [a, b]))
            seen["r_waited"] = await _done_or_waiting(r_task, r_pid)
            # T: retention's second batch -- X; its cascade needs a.
            t_task = _later(delete_terminal_notifications_batch(
                t, cutoff=cutoff, limit=1,
            ))
            seen["both_waited"] = (
                seen["r_waited"] and await _both_waiting(t_pid, r_pid)
            )
            if not seen["r_waited"]:
                seen["t_waited_on_r"] = await _done_or_waiting(t_task, t_pid)
            seen["r_deleted"], seen["r_error"] = await _outcome(r_task)
            if seen["r_error"] is None:
                await r.commit()
            else:
                await r.rollback()
            seen["t_deleted"], seen["t_error"] = await _outcome(t_task)
            if seen["t_error"] is None and t_commits:
                await t.commit()
            else:
                await t.rollback()
        return seen

    async def test_the_relay_never_waits_and_both_sides_finish(self) -> None:
        seen = await self._cross(t_commits=True)
        assert _sqlstate(seen["r_error"]) is None, seen["r_error"]
        assert _sqlstate(seen["t_error"]) is None, seen["t_error"]
        assert seen["r_waited"] is False
        assert seen["both_waited"] is False
        # R deleted a and skipped b, which T's cascade held.
        assert seen["r_deleted"] == 1
        # T waited for R's a -- a wait with nobody waiting back.
        assert seen["t_waited_on_r"] is True
        assert seen["t_deleted"] == 1
        # Nothing is left and nothing is lost: a was published and
        # deleted by R; b went with Y; both jobs are gone.
        assert await _count(PushOutbox) == 0
        assert await _count(
            Notification, Notification.id.in_([seen["x"], seen["y"]]),
        ) == 0

    async def test_a_cascade_that_rolls_back_leaves_the_skipped_row_owed(
        self,
    ) -> None:
        """T rolls back: Y and X are back, b -- skipped by R -- is still
        owed, and the next tick publishes Y's key again (a duplicate,
        harmless). a, published and deleted by R, stays deleted."""
        seen = await self._cross(t_commits=False)
        assert seen["r_deleted"] == 1
        remaining = await _count(PushOutbox)
        assert remaining == 1
        assert await _count(PushOutbox, PushOutbox.id == seen["b"]) == 1
        redis = fakeaioredis.FakeRedis()
        try:
            assert await relay.relay_once(redis) == 1
            entries = await redis.xrange(settings.changes_stream)
        finally:
            await redis.aclose()
        async with get_session_factory()() as session:
            job = await session.get(Notification, seen["y"])
            assert job is not None
        assert [e[b"idempotency_key"].decode() for _, e in entries] == [
            job.idempotency_key,
        ]
        assert await _count(PushOutbox) == 0


class TestTwoRelays:
    async def test_a_second_relay_skips_what_the_first_holds(self) -> None:
        """Two relays (a deploy rolling over) on the same rows: the one
        that came second deletes nothing and does not wait."""
        _, _, a, b = await _jobs()
        factory = get_session_factory()
        async with factory() as first:
            assert await relay.delete_published(first, [a, b]) == 2
            async with factory() as second:
                second_pid = await _pid(second)
                task = _later(relay.delete_published(second, [a, b]))
                try:
                    waited = await _done_or_waiting(task, second_pid)
                finally:
                    # Released either way, so a relay that does wait
                    # fails this test instead of hanging it.
                    await first.commit()
                assert waited is False
                assert await task == 0
                await second.commit()
        assert await _count(PushOutbox) == 0

    async def test_nothing_to_delete_is_zero(self) -> None:
        async with get_session_factory()() as session:
            assert await relay.delete_published(session, []) == 0
            assert await relay.delete_published(session, [10**12]) == 0


class TestTheExpiryCleanup:
    """The second deleter: cleanup_expired_notifications, one statement
    in its own transaction. R holds a (deleted, not yet committed); the
    cleanup's cascade waits for it -- and R, which never waits, commits;
    both reach the end, and no row is lost."""

    @pytest.mark.parametrize("r_commits", [True, False])
    async def test_both_sides_reach_the_end(self, r_commits: bool) -> None:
        x, y, a, _b = await _jobs()
        factory = get_session_factory()
        async with factory() as r:
            assert await relay.delete_published(r, [a]) == 1
            cleanup = _later(cleanup_expired_notifications())
            # The cleanup runs in a session of its own; it is seen
            # waiting by its statement, not by its pid.
            for _ in range(int(_BOUND / _POLL)):
                async with factory() as observer:
                    waiting = (await observer.execute(text(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE wait_event_type = 'Lock' "
                        "AND query ILIKE 'DELETE FROM notifications%'"
                    ))).scalar_one()
                if waiting or cleanup.done():
                    break
                await asyncio.sleep(_POLL)
            assert not cleanup.done()
            assert waiting == 1
            if r_commits:
                await r.commit()
            else:
                await r.rollback()
        assert await cleanup == 2
        # Both jobs gone; their rows with them -- a deleted by R or, R
        # rolled back, by the cascade; b by the cascade. None left.
        assert await _count(
            Notification, Notification.id.in_([x, y]),
        ) == 0
        assert await _count(PushOutbox) == 0
