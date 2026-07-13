# =============================================================================
# COMMS Service -- Reminder scheduling tests
# =============================================================================
# Handoff item 6: the reminders scheduler (velo donor, de-domainized).
# A reminder is a Notification with a future scheduled_at picked up by
# the regular worker -- no broker.
# =============================================================================

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.constants import (
    DeliveryStatus,
    NotificationStatus,
    TargetType,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import process_pending_notifications
from app.engine.reminders import (
    DEFAULT_REMINDER_PRIORITY,
    ReminderSpec,
    cancel_reminders,
    schedule_reminders,
)
from tests.helpers import create_recipient

SPECS = [
    ReminderSpec(type="unit_rem_24h", lead=timedelta(hours=24)),
    ReminderSpec(type="unit_rem_1h", lead=timedelta(hours=1)),
    ReminderSpec(type="unit_rem_10m", lead=timedelta(minutes=10)),
]

REMINDER_TYPES = {spec.type for spec in SPECS}


class TestScheduleReminders:
    """Series creation at lead offsets before the anchor."""

    async def test_full_series_scheduled(
        self, db_session: AsyncSession,
    ) -> None:
        """Far anchor -> all specs land, offsets/expiry/priority correct."""
        recipient = await create_recipient(db_session)
        anchor = datetime.now(UTC) + timedelta(hours=25)

        created = await schedule_reminders(
            db_session,
            specs=SPECS,
            anchor_at=anchor,
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
            correlation_key="event_id",
            correlation_value="ev-1",
        )

        assert len(created) == 3
        by_type = {n.type: n for n in created}
        for spec in SPECS:
            reminder = by_type[spec.type]
            assert reminder.scheduled_at == anchor - spec.lead
            assert reminder.expiry_at == anchor
            assert reminder.priority == DEFAULT_REMINDER_PRIORITY
            assert reminder.status == NotificationStatus.PENDING
            assert reminder.action_data is not None
            assert reminder.action_data["event_id"] == "ev-1"

    async def test_past_offsets_skipped(
        self, db_session: AsyncSession,
    ) -> None:
        """Near anchor -> only the offsets still in the future land."""
        recipient = await create_recipient(db_session)
        anchor = datetime.now(UTC) + timedelta(minutes=30)

        created = await schedule_reminders(
            db_session,
            specs=SPECS,
            anchor_at=anchor,
            target_type=TargetType.USER,
            target_value=str(recipient.id),
        )

        # 24h and 1h offsets are already past; 10min (= now+20min) fits.
        assert [n.type for n in created] == ["unit_rem_10m"]

    async def test_min_lead_cutoff(self, db_session: AsyncSession) -> None:
        """A send time closer than min_lead is skipped (velo 5min buffer)."""
        recipient = await create_recipient(db_session)
        anchor = datetime.now(UTC) + timedelta(minutes=12)

        created = await schedule_reminders(
            db_session,
            specs=[ReminderSpec(type="unit_rem_10m", lead=timedelta(minutes=10))],
            anchor_at=anchor,
            target_type=TargetType.USER,
            target_value=str(recipient.id),
        )

        # send_at = now+2min < cutoff now+5min -> skipped.
        assert created == []


class TestCancelReminders:
    """Correlation-based cancellation (velo mechanism, generic key)."""

    async def test_cancel_by_correlation(
        self, db_session: AsyncSession,
    ) -> None:
        """Matching correlation -> EXPIRED; other correlation untouched."""
        recipient = await create_recipient(db_session)
        anchor = datetime.now(UTC) + timedelta(hours=25)

        await schedule_reminders(
            db_session,
            specs=SPECS,
            anchor_at=anchor,
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            correlation_key="event_id",
            correlation_value="ev-1",
        )
        await schedule_reminders(
            db_session,
            specs=[SPECS[0]],
            anchor_at=anchor,
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            correlation_key="event_id",
            correlation_value="ev-2",
        )
        await db_session.commit()

        cancelled = await cancel_reminders(
            db_session,
            types=REMINDER_TYPES,
            correlation_key="event_id",
            correlation_value="ev-1",
        )
        await db_session.commit()

        assert cancelled == 3

        result = await db_session.execute(select(Notification))
        by_correlation: dict[str, list[str]] = {}
        for notification in result.scalars().all():
            key = (notification.action_data or {})["event_id"]
            by_correlation.setdefault(key, []).append(notification.status)

        assert set(by_correlation["ev-1"]) == {NotificationStatus.EXPIRED}
        assert set(by_correlation["ev-2"]) == {NotificationStatus.PENDING}

    async def test_cancel_respects_target_filter(
        self, db_session: AsyncSession,
    ) -> None:
        """Target filter cancels one participant's series only."""
        alice = await create_recipient(db_session)
        bob = await create_recipient(db_session)
        anchor = datetime.now(UTC) + timedelta(hours=25)

        for recipient in (alice, bob):
            await schedule_reminders(
                db_session,
                specs=SPECS,
                anchor_at=anchor,
                target_type=TargetType.USER,
                target_value=str(recipient.id),
                correlation_key="event_id",
                correlation_value="ev-1",
            )
        await db_session.commit()

        cancelled = await cancel_reminders(
            db_session,
            types=REMINDER_TYPES,
            correlation_key="event_id",
            correlation_value="ev-1",
            target_type=TargetType.USER,
            target_value=str(alice.id),
        )
        await db_session.commit()

        assert cancelled == 3

        result = await db_session.execute(select(Notification))
        for notification in result.scalars().all():
            expected = (
                NotificationStatus.EXPIRED
                if notification.target_value == str(alice.id)
                else NotificationStatus.PENDING
            )
            assert notification.status == expected

    async def test_cancelled_reminder_never_delivers(
        self, db_session: AsyncSession,
    ) -> None:
        """A cancelled (EXPIRED) reminder is invisible to the worker."""
        recipient = await create_recipient(db_session)
        anchor = datetime.now(UTC) + timedelta(hours=2)

        created = await schedule_reminders(
            db_session,
            specs=[ReminderSpec(type="unit_rem_1h", lead=timedelta(hours=1))],
            anchor_at=anchor,
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
            correlation_key="event_id",
            correlation_value="ev-9",
        )
        assert len(created) == 1
        await db_session.commit()

        await cancel_reminders(
            db_session,
            types=REMINDER_TYPES,
            correlation_key="event_id",
            correlation_value="ev-9",
        )
        # Simulate time passing: the reminder is now due (scheduled_at
        # is pipeline-mutable by design; only title/body are locked).
        created[0].scheduled_at = datetime.now(UTC) - timedelta(seconds=1)
        await db_session.commit()

        processed = await process_pending_notifications()
        assert processed == 0

        result = await db_session.execute(select(NotificationDelivery))
        assert list(result.scalars().all()) == []


class TestWorkerPickup:
    """Due reminders flow through the regular pipeline -- no broker."""

    async def test_due_reminder_delivered_future_waits(
        self, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)

        # Scheduled normally, then backdated to "due" (time passing).
        due = await schedule_reminders(
            db_session,
            specs=[ReminderSpec(type="unit_rem_1h", lead=timedelta(hours=1))],
            anchor_at=datetime.now(UTC) + timedelta(hours=2),
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
        )
        # Still in the future.
        future = await schedule_reminders(
            db_session,
            specs=[ReminderSpec(type="unit_rem_24h", lead=timedelta(hours=24))],
            anchor_at=datetime.now(UTC) + timedelta(hours=48),
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
        )
        due[0].scheduled_at = datetime.now(UTC) - timedelta(seconds=1)
        await db_session.commit()
        due_id, future_id = due[0].id, future[0].id

        processed = await process_pending_notifications()
        assert processed == 1

        # The processor committed in its own sessions -- drop cached
        # attribute state before re-reading.
        db_session.expire_all()

        fresh = await db_session.execute(
            select(Notification).where(Notification.id == due_id)
        )
        assert fresh.scalar_one().status == NotificationStatus.SENT

        fresh = await db_session.execute(
            select(Notification).where(Notification.id == future_id)
        )
        assert fresh.scalar_one().status == NotificationStatus.PENDING

        result = await db_session.execute(select(NotificationDelivery))
        deliveries = list(result.scalars().all())
        assert len(deliveries) == 1
        assert deliveries[0].status == DeliveryStatus.SENT
