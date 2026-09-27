# =============================================================================
# COMMS Service -- Reminder tests
# =============================================================================
# A reminder is a Notification with a future scheduled_at (the
# envelope's "not before", F1.2) picked up by the regular worker -- no
# broker. What comms still owns is CANCELLATION by correlation
# (reminder_cancel). The series scheduler and its TestScheduleReminders
# were removed in F1.2 together: nothing in the service called it, and
# a series helper would have had to invent a key per request.
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
from app.engine.reminders import cancel_reminders
from app.engine.service import create_notification
from tests.helpers import create_recipient, intake_fields

REMINDER_TYPES = {"unit_rem_24h", "unit_rem_1h", "unit_rem_10m"}
_LEADS = {
    "unit_rem_24h": timedelta(hours=24),
    "unit_rem_1h": timedelta(hours=1),
    "unit_rem_10m": timedelta(minutes=10),
}


async def _series(
    session: AsyncSession,
    *,
    types: list[str],
    anchor_at: datetime,
    target_value: str,
    correlation_value: str | None = None,
) -> list[Notification]:
    """What a product emits for a series: one request per reminder,
    each "not before" anchor - lead, expiring at the anchor."""
    created = []
    for type_key in types:
        created.append(await create_notification(
            session,
            **intake_fields(),
            type=type_key,
            title="Reminder",
            body="",
            target_type=TargetType.USER,
            target_value=target_value,
            correlation=correlation_value,
            scheduled_at=anchor_at - _LEADS[type_key],
            expiry_at=anchor_at,
        ))
    return created


class TestCancelReminders:
    """Correlation-based cancellation.

    F1.3 changed two things these tests pinned: the match is the
    ENVELOPE correlation (before: a key read out of action_data -- comms
    reading the letter to decide), and a cancelled job ends CANCELLED
    (before: EXPIRED, indistinguishable from a missed deadline)."""

    async def test_cancel_by_correlation(
        self, db_session: AsyncSession,
    ) -> None:
        """Matching correlation -> CANCELLED; other correlation untouched."""
        recipient = await create_recipient(db_session)
        anchor = datetime.now(UTC) + timedelta(hours=25)

        await _series(
            db_session,
            types=sorted(REMINDER_TYPES),
            anchor_at=anchor,
            target_value=str(recipient.id),
            correlation_value="ev-1",
        )
        await _series(
            db_session,
            types=["unit_rem_24h"],
            anchor_at=anchor,
            target_value=str(recipient.id),
            correlation_value="ev-2",
        )
        await db_session.commit()

        cancelled = await cancel_reminders(
            db_session,
            types=REMINDER_TYPES,
            correlation="ev-1",
        )
        await db_session.commit()

        assert cancelled == 3

        result = await db_session.execute(select(Notification))
        by_correlation: dict[str, list[str]] = {}
        for notification in result.scalars().all():
            key = notification.correlation or ""
            by_correlation.setdefault(key, []).append(notification.status)

        assert set(by_correlation["ev-1"]) == {NotificationStatus.CANCELLED}
        assert set(by_correlation["ev-2"]) == {NotificationStatus.PENDING}

    async def test_cancel_respects_target_filter(
        self, db_session: AsyncSession,
    ) -> None:
        """Target filter cancels one participant's series only."""
        alice = await create_recipient(db_session)
        bob = await create_recipient(db_session)
        anchor = datetime.now(UTC) + timedelta(hours=25)

        for recipient in (alice, bob):
            await _series(
                db_session,
                types=sorted(REMINDER_TYPES),
                anchor_at=anchor,
                target_value=str(recipient.id),
                correlation_value="ev-1",
            )
        await db_session.commit()

        cancelled = await cancel_reminders(
            db_session,
            types=REMINDER_TYPES,
            correlation="ev-1",
            target_type=TargetType.USER,
            target_value=str(alice.id),
        )
        await db_session.commit()

        assert cancelled == 3

        result = await db_session.execute(select(Notification))
        for notification in result.scalars().all():
            expected = (
                NotificationStatus.CANCELLED
                if notification.target_value == str(alice.id)
                else NotificationStatus.PENDING
            )
            assert notification.status == expected

    async def test_cancelled_reminder_never_delivers(
        self, db_session: AsyncSession,
    ) -> None:
        """A cancelled reminder is invisible to the worker."""
        recipient = await create_recipient(db_session)
        anchor = datetime.now(UTC) + timedelta(hours=2)

        created = await _series(
            db_session,
            types=["unit_rem_1h"],
            anchor_at=anchor,
            target_value=str(recipient.id),
            correlation_value="ev-9",
        )
        assert len(created) == 1
        await db_session.commit()

        await cancel_reminders(
            db_session,
            types=REMINDER_TYPES,
            correlation="ev-9",
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
        """A due reminder is delivered; a future one waits.

        R-0: this test requested telegram and relied on the old stub,
        which made an UNCONFIGURED channel "succeed". Its subject is
        reminder pickup, not the channel, so it uses in_app, the channel
        that is live on every deploy by definition (zero declared keys).
        Since F1.2 the channel is the profile's, per type: the test uses
        unit_bare, a type whose record takes the default channels
        (in_app), instead of passing in_app in the call.
        """
        recipient = await create_recipient(db_session)
        now = datetime.now(UTC)

        # Scheduled normally, then backdated to "due" (time passing).
        due = [await create_notification(
            db_session, **intake_fields(), type="unit_bare",
            title="Reminder", body="", target_type=TargetType.USER,
            target_value=str(recipient.id),
            scheduled_at=now + timedelta(hours=1),
            expiry_at=now + timedelta(hours=2),
        )]
        # Still in the future.
        future = [await create_notification(
            db_session, **intake_fields(), type="unit_bare",
            title="Reminder", body="", target_type=TargetType.USER,
            target_value=str(recipient.id),
            scheduled_at=now + timedelta(hours=24),
            expiry_at=now + timedelta(hours=48),
        )]
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
