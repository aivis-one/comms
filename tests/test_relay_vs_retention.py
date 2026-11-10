# =============================================================================
# H1 Б2 -- the relay's delete against the cascades that delete jobs:
# no 40P01, both sides reach the end, no outbox row is lost
# =============================================================================
#
# Since H7 (the expiry cleanup) and REV-2 (retention) neither deleter
# takes a job whose push is still owed, so neither cascade reaches an
# outbox row: the crossing described below (as it stood until REV-2)
# can no longer be built, and
# the tests hold that the deleters never meet the relay
# (TestTheRetentionNeverMeetsTheRelay, TestTheExpiryCleanup). The relay's
# SKIP LOCKED stays, held against a second relay (TestTwoRelays).
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
# The crossing, until REV-2: T has deleted Y, so its cascade held b; R deletes [a, b]
# -- it locks a, and then b is T's; T deletes X, and its cascade needs
# a. A relay that WAITS for b closes the cycle: both wait on Lock, and
# Postgres kills one with 40P01. With FOR UPDATE SKIP LOCKED R never
# waits: it deletes a, skips b, and T goes on once R commits.
#
# MUTATIONS these tests were written against:
#   M5  the relay's delete without SKIP LOCKED -> TestTwoRelays: the
#       second relay waits for the first
#   M6  NOWAIT instead of SKIP LOCKED -> TestTwoRelays: the second
#       relay fails 55P03
#   T3  retention without the push_outbox anti-join (REV-2)
#       -> TestTheRetentionNeverMeetsTheRelay: retention waits for R
# =============================================================================

import asyncio
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

import app.transport.push_relay as relay
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
            text(
                "SELECT wait_event_type FROM pg_stat_activity "
                "WHERE pid = :pid AND datname = current_database()"
            ),
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




async def _count(model: Any, *where: Any) -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(
            select(func.count()).select_from(model).where(*where)
        )).scalar_one())



def _later(work: Awaitable[Any]) -> asyncio.Task[Any]:
    return asyncio.ensure_future(work)


# -----------------------------------------------------------------------------
# Retention against the relay
# -----------------------------------------------------------------------------


class TestTheRetentionNeverMeetsTheRelay:
    """Retention against the relay's delete.

    Until REV-2 retention deleted X and Y whatever they owed, its cascade
    took b while the relay deleted [a, b], and the crossing was held
    here: the relay never waits (SKIP LOCKED), retention waits for the
    relay's a, both finish, no row lost -- true then. REV-2 made
    retention skip a job whose push is still owed
    (delete_terminal_notifications_batch, an anti-join on push_outbox),
    as H7 made the expiry cleanup (TestTheExpiryCleanup below): its
    cascade never reaches an outbox row, so it never meets the relay --
    and with no lock of each other's held, no cycle of the two can be
    built at all. Held now, more precisely: with R open (a deleted,
    uncommitted) retention deletes nothing and does not wait; R
    committed, the next pass deletes X (its push published) and keeps Y
    (b still owed); R rolled back, both stay, rows and all. The relay's
    SKIP LOCKED is held by TestTwoRelays."""

    @pytest.mark.parametrize("r_commits", [True, False])
    async def test_retention_skips_what_is_owed_and_never_waits(
        self, r_commits: bool,
    ) -> None:
        x, y, a, _b = await _jobs()
        cutoff = datetime.now(UTC) + timedelta(seconds=1)
        factory = get_session_factory()
        async with factory() as r, factory() as t:
            assert await relay.delete_published(r, [a]) == 1
            t_pid = await _pid(t)
            t_task = _later(delete_terminal_notifications_batch(
                t, cutoff=cutoff, limit=100,
            ))
            # Observed, not timed: retention finished, it did not wait
            # for the relay's lock on a.
            waited = await _done_or_waiting(t_task, t_pid)
            if waited:
                # Let retention go, so a regression fails this test
                # instead of hanging it on the relay's open transaction.
                await r.rollback()
                await t_task
                await t.rollback()
            assert waited is False
            assert await t_task == 0
            await t.commit()
            if r_commits:
                await r.commit()
            else:
                await r.rollback()
        async with factory() as t:
            assert await delete_terminal_notifications_batch(
                t, cutoff=cutoff, limit=100,
            ) == (1 if r_commits else 0)
            await t.commit()
        assert await _count(Notification, Notification.id == x) == (
            0 if r_commits else 1
        )
        assert await _count(Notification, Notification.id == y) == 1
        assert await _count(PushOutbox) == (1 if r_commits else 2)


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
    """The second deleter: cleanup_expired_notifications.

    Until H7 the cleanup deleted X and Y whatever they owed, its cascade
    WAITED for R on a, and the test held that both reached the end with
    no row lost -- true then. H7 made the cleanup skip a job whose push
    is still owed (delete_expired_notifications_batch, an anti-join on
    push_outbox): the cascade never reaches an outbox row, so it never
    meets R. Held now, more precisely: with R open the cleanup deletes
    nothing and does not wait; R committed, the next pass deletes X
    (its push published) and keeps Y (b still owed); R rolled back,
    both stay, rows and all."""

    @pytest.mark.parametrize("r_commits", [True, False])
    async def test_the_cleanup_never_meets_the_relay(self, r_commits: bool) -> None:
        x, y, a, _b = await _jobs()
        factory = get_session_factory()
        async with factory() as r:
            assert await relay.delete_published(r, [a]) == 1
            assert await asyncio.wait_for(
                cleanup_expired_notifications(), _BOUND,
            ) == 0
            if r_commits:
                await r.commit()
            else:
                await r.rollback()
        assert await cleanup_expired_notifications() == (1 if r_commits else 0)
        assert await _count(Notification, Notification.id == x) == (
            0 if r_commits else 1
        )
        assert await _count(Notification, Notification.id == y) == 1
        assert await _count(PushOutbox) == (1 if r_commits else 2)
