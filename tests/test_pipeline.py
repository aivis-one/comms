# =============================================================================
# COMMS Service -- Pipeline tests (resolve -> deliver -> rollup)
# =============================================================================
# Handoff item 6 core coverage:
#   - resolve: deliveries per recipient x channel, idempotency, no-target
#   - deliver: success / transient failure + retries / permanent failure
#   - rollup: sent / failed / partial_sent
#   - expire + cleanup, scheduled_at gating (reminder mechanism)
#
# The worker/processor commits in its own sessions -- assertions
# re-read state through a fresh session.
# =============================================================================

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.engine.constants import (
    DeliveryStatus,
    NotificationStatus,
    TargetType,
)
from app.engine.formatters import PermanentDeliveryError
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import (
    cleanup_expired_notifications,
    process_pending_notifications,
)
from app.engine.service import create_notification, resolve_notification
from tests.helpers import create_recipient


async def _fetch_notification(notification_id: UUID) -> Notification:
    """Re-read a notification through a fresh session."""
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            select(Notification).where(Notification.id == notification_id)
        )
        return result.scalar_one()


async def _fetch_deliveries(
    notification_id: UUID,
) -> list[NotificationDelivery]:
    """Re-read deliveries through a fresh session."""
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            select(NotificationDelivery).where(
                NotificationDelivery.notification_id == notification_id
            )
        )
        return list(result.scalars().all())


class _FailingFormatter:
    """Formatter that always fails transiently."""

    async def deliver(self, *args: Any, **kwargs: Any) -> bool:
        raise RuntimeError("boom transient")


class _PermanentFormatter:
    """Formatter that always fails permanently."""

    async def deliver(self, *args: Any, **kwargs: Any) -> bool:
        raise PermanentDeliveryError("bot was blocked by the user")


class TestResolveStage:
    """Target expansion into deliveries."""

    async def test_deliveries_per_recipient_and_channel(
        self, db_session: AsyncSession,
    ) -> None:
        """recipients x channels delivery rows are created."""
        alpha = await create_recipient(db_session)
        bravo = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
            channels=["telegram", "in_app"],
        )
        deliveries = await resolve_notification(db_session, notification)

        assert len(deliveries) == 4
        assert {d.recipient_id for d in deliveries} == {alpha.id, bravo.id}
        assert {d.channel for d in deliveries} == {"telegram", "in_app"}
        assert notification.status == NotificationStatus.PROCESSING

    async def test_resolve_is_idempotent(
        self, db_session: AsyncSession,
    ) -> None:
        """Second resolve returns existing deliveries, creates nothing."""
        await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
            channels=["telegram"],
        )
        first = await resolve_notification(db_session, notification)
        second = await resolve_notification(db_session, notification)

        assert len(first) == 1
        assert len(second) == 1
        assert first[0].id == second[0].id

    async def test_no_targets_marks_failed(
        self, db_session: AsyncSession,
    ) -> None:
        """Empty audience -> FAILED (cbshome base; velo said SENT)."""
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.GROUP,
            target_value="empty_group",
            channels=["telegram"],
        )
        deliveries = await resolve_notification(db_session, notification)

        assert deliveries == []
        assert notification.status == NotificationStatus.FAILED

    async def test_default_channel_is_in_app(
        self, db_session: AsyncSession,
    ) -> None:
        """channels=None defaults to a single in_app delivery."""
        await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
        )
        deliveries = await resolve_notification(db_session, notification)
        assert [d.channel for d in deliveries] == ["in_app"]


class TestDeliverAndRollup:
    """End-to-end batch processing with the stub channel."""

    async def test_happy_path_sent(self, db_session: AsyncSession) -> None:
        """create -> process -> delivery SENT, notification SENT."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="Hello",
            body="World",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
        )
        await db_session.commit()

        processed = await process_pending_notifications()
        assert processed == 1

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.SENT

        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.status == DeliveryStatus.SENT
        assert delivery.attempts == 1
        assert delivery.sent_at is not None

    async def test_transient_failure_retries_until_max(
        self, db_session: AsyncSession,
    ) -> None:
        """Transient errors increment attempts; FAILED at max attempts."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
        )
        await db_session.commit()

        with patch(
            "app.engine.service.get_formatter",
            return_value=_FailingFormatter(),
        ):
            # Attempt 1: stays pending, error recorded.
            await process_pending_notifications()
            (delivery,) = await _fetch_deliveries(notification.id)
            assert delivery.status == DeliveryStatus.PENDING
            assert delivery.attempts == 1
            assert delivery.error_message is not None
            assert "boom transient" in delivery.error_message

            fresh = await _fetch_notification(notification.id)
            assert fresh.status == NotificationStatus.PROCESSING

            # Attempts 2..3: FAILED at max (default 3).
            await process_pending_notifications()
            await process_pending_notifications()

        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.attempts == 3

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.FAILED

    async def test_permanent_failure_no_attempt_increment(
        self, db_session: AsyncSession,
    ) -> None:
        """PermanentDeliveryError -> FAILED immediately, attempts stay 0."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
        )
        await db_session.commit()

        with patch(
            "app.engine.service.get_formatter",
            return_value=_PermanentFormatter(),
        ):
            await process_pending_notifications()

        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.attempts == 0
        assert delivery.error_message is not None
        assert "blocked" in delivery.error_message

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.FAILED

    async def test_partial_sent_rollup(
        self, db_session: AsyncSession,
    ) -> None:
        """Mixed sent+failed deliveries -> PARTIAL_SENT."""
        blocked = await create_recipient(db_session)
        healthy = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
            channels=["telegram"],
        )
        await db_session.commit()

        class _SelectiveFormatter:
            """Fail permanently for one recipient, succeed for the other."""

            async def deliver(
                self,
                notification: Notification,
                delivery: NotificationDelivery,
                recipient: Any,
            ) -> bool:
                if recipient.id == blocked.id:
                    raise PermanentDeliveryError("chat not found")
                return True

        with patch(
            "app.engine.service.get_formatter",
            return_value=_SelectiveFormatter(),
        ):
            await process_pending_notifications()

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.PARTIAL_SENT

        deliveries = await _fetch_deliveries(notification.id)
        by_recipient = {d.recipient_id: d for d in deliveries}
        assert by_recipient[healthy.id].status == DeliveryStatus.SENT
        assert by_recipient[blocked.id].status == DeliveryStatus.FAILED


class TestSchedulingAndExpiry:
    """scheduled_at gating (reminder mechanism) + expiry + cleanup."""

    async def test_future_notification_waits(
        self, db_session: AsyncSession,
    ) -> None:
        """scheduled_at in the future -> untouched by the batch."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
            scheduled_at=datetime.now(UTC) + timedelta(hours=1),
        )
        await db_session.commit()

        processed = await process_pending_notifications()
        assert processed == 0

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.PENDING
        assert await _fetch_deliveries(notification.id) == []

    async def test_overdue_notification_expires(
        self, db_session: AsyncSession,
    ) -> None:
        """expiry_at in the past -> EXPIRED before any delivery."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
            scheduled_at=datetime.now(UTC) - timedelta(hours=2),
            expiry_at=datetime.now(UTC) - timedelta(hours=1),
        )
        await db_session.commit()

        await process_pending_notifications()

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.EXPIRED
        assert await _fetch_deliveries(notification.id) == []

    async def test_cleanup_deletes_expired_delivered(
        self, db_session: AsyncSession,
    ) -> None:
        """cleanup removes terminal notifications past expiry_at."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
            scheduled_at=datetime.now(UTC) - timedelta(hours=2),
            expiry_at=datetime.now(UTC) - timedelta(hours=1),
        )
        await db_session.commit()

        # First batch expires it; cleanup then deletes it.
        await process_pending_notifications()
        deleted = await cleanup_expired_notifications()
        assert deleted == 1

        factory = get_session_factory()
        async with factory() as session:
            result = await session.execute(
                select(Notification).where(
                    Notification.id == notification.id
                )
            )
            assert result.scalar_one_or_none() is None
