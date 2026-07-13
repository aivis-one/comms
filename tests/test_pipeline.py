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

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
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


async def _force_retry_due(delivery_id: UUID) -> None:
    """Backdate the retry gate -- simulates the backoff window passing."""
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            select(NotificationDelivery).where(
                NotificationDelivery.id == delivery_id
            )
        )
        delivery = result.scalar_one()
        delivery.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()


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
            # Attempt 1: stays pending, error recorded, retry gated.
            await process_pending_notifications()
            (delivery,) = await _fetch_deliveries(notification.id)
            assert delivery.status == DeliveryStatus.PENDING
            assert delivery.attempts == 1
            assert delivery.error_message is not None
            assert "boom transient" in delivery.error_message
            assert delivery.next_retry_at is not None

            fresh = await _fetch_notification(notification.id)
            assert fresh.status == NotificationStatus.PROCESSING

            # Attempts 2..3 -- each after its backoff window "passes"
            # (backdate the gate, review 1.1): FAILED at max (3).
            await _force_retry_due(delivery.id)
            await process_pending_notifications()
            await _force_retry_due(delivery.id)
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
        assert delivery.next_retry_at is None
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


class TestRetryBackoff:
    """Review 1.1: transient retries are gated by next_retry_at."""

    async def test_gate_blocks_early_retry_and_backs_off(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Attempts don't burn within one poll window; gate grows 2x."""
        monkeypatch.setattr(
            settings, "notification_retry_backoff_base_seconds", 100,
        )
        monkeypatch.setattr(
            settings, "notification_retry_backoff_max_seconds", 10_000,
        )
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
            # Attempt 1 -> gate ~= now + base.
            before = datetime.now(UTC)
            await process_pending_notifications()
            (delivery,) = await _fetch_deliveries(notification.id)
            assert delivery.attempts == 1
            assert delivery.next_retry_at is not None
            first_gate = delivery.next_retry_at
            delta = (first_gate - before).total_seconds()
            assert 90 <= delta <= 115

            # Immediate re-poll: gated, attempts unchanged.
            await process_pending_notifications()
            (delivery,) = await _fetch_deliveries(notification.id)
            assert delivery.attempts == 1
            assert delivery.next_retry_at == first_gate

            # Window passes -> attempt 2, gate doubles (base * 2).
            await _force_retry_due(delivery.id)
            before = datetime.now(UTC)
            await process_pending_notifications()
            (delivery,) = await _fetch_deliveries(notification.id)
            assert delivery.attempts == 2
            assert delivery.next_retry_at is not None
            delta = (delivery.next_retry_at - before).total_seconds()
            assert 190 <= delta <= 215

    async def test_gated_notification_invisible_to_batch(
        self, db_session: AsyncSession,
    ) -> None:
        """Review 1.2: while every delivery is gated, the batch skips
        the notification entirely -- no idle locks, processed == 0
        (the worker loop can back off) -- while fresh work still flows."""
        recipient = await create_recipient(db_session)
        gated = await create_notification(
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
            # Attempt 1 -> delivery gated into the future.
            assert await process_pending_notifications() == 1
            (delivery,) = await _fetch_deliveries(gated.id)
            assert delivery.attempts == 1
            assert delivery.next_retry_at is not None

            # Gate closed -> the batch sees NOTHING (processed == 0,
            # so the worker loop's backoff engages).
            assert await process_pending_notifications() == 0
            (delivery,) = await _fetch_deliveries(gated.id)
            assert delivery.attempts == 1

            # A fresh notification still flows while the first is gated.
            fresh = await create_notification(
                db_session,
                type="unit_event",
                title="F",
                body="B",
                target_type=TargetType.USER,
                target_value=str(recipient.id),
                channels=["telegram"],
            )
            await db_session.commit()
            assert await process_pending_notifications() == 1
            (fresh_delivery,) = await _fetch_deliveries(fresh.id)
            assert fresh_delivery.attempts == 1

            # Both gated now -> batch empty again.
            assert await process_pending_notifications() == 0

            # Window passes -> the gated one is visible again.
            await _force_retry_due(delivery.id)
            assert await process_pending_notifications() == 1

        (delivery,) = await _fetch_deliveries(gated.id)
        assert delivery.attempts == 2

    async def test_backoff_capped(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The gate never exceeds the configured cap."""
        monkeypatch.setattr(
            settings, "notification_retry_backoff_base_seconds", 100,
        )
        monkeypatch.setattr(
            settings, "notification_retry_backoff_max_seconds", 120,
        )
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
            await process_pending_notifications()
            (delivery,) = await _fetch_deliveries(notification.id)
            await _force_retry_due(delivery.id)
            before = datetime.now(UTC)
            await process_pending_notifications()

        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.attempts == 2
        assert delivery.next_retry_at is not None
        # base * 2**(2-1) = 200 -> capped at 120.
        delta = (delivery.next_retry_at - before).total_seconds()
        assert 110 <= delta <= 135


class TestBatchLimit:
    """Review 1.1: the poll batch is capped; tail rides the next tick."""

    async def test_limit_and_next_tick_pickup(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "notification_batch_size", 2)
        recipient = await create_recipient(db_session)
        for index in range(3):
            await create_notification(
                db_session,
                type="unit_event",
                title=f"T{index}",
                body="B",
                target_type=TargetType.USER,
                target_value=str(recipient.id),
                channels=["telegram"],
            )
        await db_session.commit()

        assert await process_pending_notifications() == 2
        assert await process_pending_notifications() == 1
        assert await process_pending_notifications() == 0


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
