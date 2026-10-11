# =============================================================================
# H2 Б2 -- the expiry cleanup deletes in batches, a commit per batch
# =============================================================================
#
# cleanup_expired_notifications (app/engine/processor.py) drains expired
# terminal jobs batch by batch (_RETENTION_BATCH_SIZE), like the
# retention pass -- so its cascade never holds push_outbox rows for one
# unbounded transaction.
#
# MUTATIONS these tests were written against (each turns one red):
#   M16 one batch, no drain loop        -> test_more_than_a_batch_...
#   M17 the last batch's count returned,
#       not the sum                     -> test_more_than_a_batch_...
# =============================================================================

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

import app.engine.processor as processor
import app.transport.push_relay as relay
from app.core.database import get_session_factory
from app.engine.constants import NotificationStatus, TargetType
from app.engine.models import Notification, PushOutbox
from app.engine.processor import cleanup_expired_notifications
from tests.helpers import notification_row_fields

_BOUND = 10.0
_POLL = 0.01


async def _job(
    status: str = NotificationStatus.SENT,
    *,
    expired: bool = True,
    owed: bool = False,
) -> UUID:
    now = datetime.now(UTC)
    async with get_session_factory()() as session:
        job = Notification(
            type="unit_event_in_app", title="T", body="B",
            target_type=TargetType.ALL, target_value="*", status=status,
            expiry_at=now + (timedelta(minutes=-1) if expired else timedelta(hours=1)),
            **notification_row_fields(),
        )
        session.add(job)
        await session.flush()
        if owed:
            session.add(PushOutbox(notification_id=job.id))
        await session.commit()
        return job.id


async def _left(*ids: UUID) -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(
            select(func.count()).select_from(Notification)
            .where(Notification.id.in_(ids))
        )).scalar_one())


@pytest.fixture
def batch_of_two(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(processor, "_RETENTION_BATCH_SIZE", 2)


class TestBatches:
    async def test_more_than_a_batch_is_drained_in_one_call(
        self, batch_of_two: None,
    ) -> None:
        """Five expired jobs, batches of two: one call deletes all five,
        returns five, in three commits."""
        ids = [await _job() for _ in range(5)]
        commits = 0
        real_commit = AsyncSession.commit

        async def counting(self: AsyncSession) -> None:
            nonlocal commits
            commits += 1
            await real_commit(self)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(AsyncSession, "commit", counting)
            assert await cleanup_expired_notifications() == 5
        assert await _left(*ids) == 0
        assert commits == 3

    @pytest.mark.parametrize("count", [0, 1, 2])
    async def test_up_to_a_batch(self, batch_of_two: None, count: int) -> None:
        ids = [await _job() for _ in range(count)]
        assert await cleanup_expired_notifications() == count
        if ids:
            assert await _left(*ids) == 0

    async def test_only_expired_outcomes_other_than_failed_go(
        self, batch_of_two: None,
    ) -> None:
        """The filter is unchanged: failed, still active and not yet
        expired stay; the pair -- the expired outcomes beside them go."""
        stay = [
            await _job(NotificationStatus.FAILED),
            await _job(NotificationStatus.PROCESSING),
            await _job(NotificationStatus.PENDING),
            await _job(expired=False),
        ]
        go = [
            await _job(status) for status in (
                NotificationStatus.SENT, NotificationStatus.PARTIAL_SENT,
                NotificationStatus.EXPIRED, NotificationStatus.CANCELLED,
                NotificationStatus.SUPPRESSED, NotificationStatus.NO_RECIPIENTS,
            )
        ]
        assert await cleanup_expired_notifications() == len(go)
        assert await _left(*go) == 0
        assert await _left(*stay) == len(stay)

    async def test_a_failing_batch_keeps_what_came_before(
        self, batch_of_two: None, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        ids = [await _job() for _ in range(4)]
        real = processor.delete_expired_notifications_batch
        calls = 0

        async def second_fails(session: AsyncSession, **kw: Any) -> int:
            nonlocal calls
            calls += 1
            if calls == 2:
                await session.execute(text("SELECT * FROM no_such_table_h2"))
            return await real(session, **kw)

        monkeypatch.setattr(
            processor, "delete_expired_notifications_batch", second_fails,
        )
        with capture_logs() as logs:
            assert await cleanup_expired_notifications() == 2
        assert await _left(*ids) == 2
        (error,) = [e for e in logs if e["event"] == "notification_cleanup_error"]
        assert error["deleted"] == 2


class TestAgainstTheRelay:
    async def test_a_held_row_in_a_later_batch_waits_for_the_relay_only(
        self, batch_of_two: None,
    ) -> None:
        """Batches of two; the relay has deleted (and not committed) the
        outbox row of one expired job. The cleanup's cascade waits for
        the relay -- which waits for nobody -- the relay commits, the
        cleanup goes on to the end. Every job gone, no outbox row left,
        none lost (the relay's row was published and deleted)."""
        ids = [await _job(owed=True) for _ in range(5)]
        async with get_session_factory()() as session:
            rows = list((await session.execute(
                select(PushOutbox.id).order_by(PushOutbox.id)
            )).scalars())
        factory = get_session_factory()
        async with factory() as r:
            assert await relay.delete_published(r, [rows[-1]]) == 1
            cleanup = asyncio.ensure_future(cleanup_expired_notifications())
            waiting = 0
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
            assert waiting == 1 and not cleanup.done()
            await r.commit()
        assert await cleanup == 5
        assert await _left(*ids) == 0
        async with factory() as session:
            assert (await session.execute(
                select(func.count()).select_from(PushOutbox)
            )).scalar_one() == 0
