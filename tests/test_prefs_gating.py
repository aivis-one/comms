# =============================================================================
# COMMS Service -- Preferences + gating tests (Phase 2 items 5-7)
# =============================================================================
# Item 5: preference API -- category mutes (idempotent, validated
#   against the profile), quiet-hours window (all-or-nothing, day
#   normalization). Timezone is NOT settable here since Phase 2.1
#   (sync-owned; read-only in RecipientPreferences).
# Item 6: gating -- a muted recipient gets NO deliveries (gated at
#   resolve, family granularity via the type dictionary); quiet hours
#   DEFER delivery via next_retry_at (never suppress), including
#   backoff retries that land inside a window.
# Item 7: SKIPPED -- empty-after-mute audiences end SKIPPED; the
#   status is terminal (invisible to the poll, immune to rollup).
# Phase 2.1 item 3: LATE MUTES -- a mute set while a delivery sits
#   gated (backoff / quiet hours) closes it out with
#   DeliveryStatus.SKIPPED at deliver time; rollup treats skips as
#   non-events (matrix covered below).
#
# Recipients here draw telegram_ids from the Phase 2 band 81000-81999.
# =============================================================================

from datetime import UTC, datetime, time, timedelta
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import Recipient
from app.audience.prefs import (
    get_preferences,
    muted_recipient_ids,
    set_category_muted,
    set_quiet_hours,
)
from app.audience.quiet_hours import recipient_quiet_until
from app.core.database import get_session_factory
from app.core.exceptions import NotFoundError, ValidationError
from app.engine.constants import (
    DeliveryStatus,
    NotificationStatus,
    TargetType,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import process_pending_notifications
from app.engine.service import (
    create_notification,
    resolve_notification,
    rollup_notification,
)
from tests.helpers import create_recipient, next_phase2_telegram_id


async def _phase2_recipient(
    session: AsyncSession, **overrides: Any,
) -> Recipient:
    """Recipient with a telegram_id from the Phase 2 band."""
    overrides.setdefault("telegram_id", next_phase2_telegram_id())
    return await create_recipient(session, **overrides)


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


async def _fetch_recipient(recipient_id: UUID) -> Recipient:
    """Re-read a recipient through a fresh session."""
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            select(Recipient).where(Recipient.id == recipient_id)
        )
        return result.scalar_one()


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


def _window_around_now() -> tuple[time, time, list[int]]:
    """A quiet window straddling the current UTC moment (+-2h).

    Day = ISO weekday of the window START; the overnight from>=to
    convention makes this correct at any hour, including just after
    midnight (start lands on yesterday, from > to).
    """
    start = datetime.now(UTC) - timedelta(hours=2)
    end = datetime.now(UTC) + timedelta(hours=2)
    return (
        start.time().replace(second=0, microsecond=0),
        end.time().replace(second=0, microsecond=0),
        [start.date().isoweekday()],
    )


def _window_missing_now() -> tuple[time, time, list[int]]:
    """A quiet window strictly in the future (now+3h .. now+4h)."""
    start = datetime.now(UTC) + timedelta(hours=3)
    end = datetime.now(UTC) + timedelta(hours=4)
    return (
        start.time().replace(second=0, microsecond=0),
        end.time().replace(second=0, microsecond=0),
        [start.date().isoweekday()],
    )


class TestPreferenceApi:
    """Item 5: mutes, quiet hours and timezone read/write."""

    async def test_mute_roundtrip_is_idempotent(
        self, db_session: AsyncSession,
    ) -> None:
        """Double mute -> one mute; double unmute -> clean state."""
        recipient = await _phase2_recipient(db_session)
        for _ in range(2):
            await set_category_muted(
                db_session, recipient.id, "unit_updates", True,
            )
        prefs = await get_preferences(db_session, recipient.id)
        assert prefs.muted_categories == {"unit_updates"}

        for _ in range(2):
            await set_category_muted(
                db_session, recipient.id, "unit_updates", False,
            )
        prefs = await get_preferences(db_session, recipient.id)
        assert prefs.muted_categories == frozenset()

    async def test_unknown_category_rejected(
        self, db_session: AsyncSession,
    ) -> None:
        """Categories are profile vocabulary -- unknown ones bounce."""
        recipient = await _phase2_recipient(db_session)
        with pytest.raises(ValidationError, match="Unknown preference"):
            await set_category_muted(
                db_session, recipient.id, "ghost_category", True,
            )

    async def test_unknown_recipient_rejected(
        self, db_session: AsyncSession,
    ) -> None:
        """Muting for a recipient that was never synced is an error."""
        with pytest.raises(NotFoundError):
            await set_category_muted(
                db_session, uuid4(), "unit_updates", True,
            )

    async def test_quiet_hours_roundtrip_and_clear(
        self, db_session: AsyncSession,
    ) -> None:
        """Set normalizes days (dedupe + sort); all-None clears."""
        recipient = await _phase2_recipient(db_session)
        await set_quiet_hours(
            db_session,
            recipient.id,
            quiet_from=time(22, 0),
            quiet_to=time(8, 0),
            days=[7, 1, 1, 5],
        )
        prefs = await get_preferences(db_session, recipient.id)
        assert prefs.quiet_from == time(22, 0)
        assert prefs.quiet_to == time(8, 0)
        assert prefs.quiet_days == (1, 5, 7)

        await set_quiet_hours(
            db_session, recipient.id,
            quiet_from=None, quiet_to=None, days=None,
        )
        prefs = await get_preferences(db_session, recipient.id)
        assert prefs.quiet_from is None
        assert prefs.quiet_to is None
        assert prefs.quiet_days is None

    async def test_quiet_hours_invalid_inputs_rejected(
        self, db_session: AsyncSession,
    ) -> None:
        """Partial config, zero-length window and bad days bounce."""
        recipient = await _phase2_recipient(db_session)
        with pytest.raises(ValidationError, match="all-or-nothing"):
            await set_quiet_hours(
                db_session, recipient.id,
                quiet_from=time(22, 0), quiet_to=None, days=None,
            )
        with pytest.raises(ValidationError, match="must differ"):
            await set_quiet_hours(
                db_session, recipient.id,
                quiet_from=time(8, 0), quiet_to=time(8, 0), days=[1],
            )
        with pytest.raises(ValidationError, match="non-empty"):
            await set_quiet_hours(
                db_session, recipient.id,
                quiet_from=time(22, 0), quiet_to=time(8, 0), days=[],
            )
        with pytest.raises(ValidationError, match="ISO weekdays"):
            await set_quiet_hours(
                db_session, recipient.id,
                quiet_from=time(22, 0), quiet_to=time(8, 0), days=[0, 8],
            )

    async def test_preferences_expose_synced_timezone_readonly(
        self, db_session: AsyncSession,
    ) -> None:
        """Timezone is sync-owned (Phase 2.1): prefs only display it.

        There is deliberately no set_timezone -- a re-sync would
        silently clobber it. The snapshot mirrors whatever sync wrote.
        """
        recipient = await _phase2_recipient(db_session)
        recipient.timezone = "Europe/Berlin"
        await db_session.flush()

        prefs = await get_preferences(db_session, recipient.id)
        assert prefs.timezone == "Europe/Berlin"


class TestMuteGating:
    """Item 6 (mutes) + item 7 (SKIPPED): resolve-time gating."""

    async def test_family_mute_gates_all_family_types(
        self, db_session: AsyncSession,
    ) -> None:
        """One category mute gates every type in the family."""
        recipient = await _phase2_recipient(db_session)
        await set_category_muted(
            db_session, recipient.id, "unit_reminder", True,
        )
        for family_type in ("unit_rem_24h", "unit_rem_1h"):
            notification = await create_notification(
                db_session,
                type=family_type,
                title="T",
                body="B",
                target_type=TargetType.USER,
                target_value=str(recipient.id),
                channels=["telegram"],
            )
            deliveries = await resolve_notification(db_session, notification)
            assert deliveries == []
            assert notification.status == NotificationStatus.SKIPPED

    async def test_all_muted_audience_marks_skipped(
        self, db_session: AsyncSession,
    ) -> None:
        """Everyone muted -> zero deliveries + SKIPPED, not FAILED."""
        recipient = await _phase2_recipient(db_session)
        await set_category_muted(
            db_session, recipient.id, "unit_updates", True,
        )
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
            channels=["telegram"],
        )
        deliveries = await resolve_notification(db_session, notification)
        assert deliveries == []
        assert notification.status == NotificationStatus.SKIPPED
        assert await _fetch_deliveries(notification.id) == []

    async def test_mixed_audience_delivers_to_unmuted_only(
        self, db_session: AsyncSession,
    ) -> None:
        """Muted recipients are dropped; the rest deliver -> SENT."""
        muted = await _phase2_recipient(db_session)
        listening = await _phase2_recipient(db_session)
        await set_category_muted(db_session, muted.id, "unit_updates", True)
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

        assert await process_pending_notifications() == 1

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.SENT
        deliveries = await _fetch_deliveries(notification.id)
        assert [d.recipient_id for d in deliveries] == [listening.id]
        assert deliveries[0].status == DeliveryStatus.SENT

    async def test_type_without_category_ignores_mutes(
        self, db_session: AsyncSession,
    ) -> None:
        """unit_plain has no category -> mute gating does not apply."""
        recipient = await _phase2_recipient(db_session)
        for category in ("unit_updates", "unit_reminder"):
            await set_category_muted(db_session, recipient.id, category, True)
        notification = await create_notification(
            db_session,
            type="unit_plain",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
        )
        deliveries = await resolve_notification(db_session, notification)
        assert len(deliveries) == 1
        assert notification.status == NotificationStatus.PROCESSING

    async def test_muted_recipient_ids_probe(
        self, db_session: AsyncSession,
    ) -> None:
        """The resolve-side probe returns exactly the muted subset."""
        muted = await _phase2_recipient(db_session)
        listening = await _phase2_recipient(db_session)
        await set_category_muted(db_session, muted.id, "unit_updates", True)
        result = await muted_recipient_ids(
            db_session, "unit_updates", [muted.id, listening.id],
        )
        assert result == {muted.id}
        assert await muted_recipient_ids(db_session, "unit_updates", []) == (
            set()
        )

    async def test_skipped_is_terminal(
        self, db_session: AsyncSession,
    ) -> None:
        """Item 7: SKIPPED is invisible to the poll and rollup-proof."""
        recipient = await _phase2_recipient(db_session)
        await set_category_muted(
            db_session, recipient.id, "unit_updates", True,
        )
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["telegram"],
        )
        await resolve_notification(db_session, notification)
        assert notification.status == NotificationStatus.SKIPPED

        # Rollup must not reinterpret "zero deliveries" as FAILED.
        await rollup_notification(db_session, notification)
        assert notification.status == NotificationStatus.SKIPPED
        await db_session.commit()

        # The worker poll only sees PENDING/PROCESSING.
        assert await process_pending_notifications() == 0
        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.SKIPPED


class TestQuietHoursGating:
    """Item 6 (quiet hours): defer via next_retry_at, never suppress."""

    async def test_delivery_deferred_inside_window(
        self, db_session: AsyncSession,
    ) -> None:
        """Inside the window: no send, no attempt burned, gate set to
        the window end; the gated row hides from the next poll."""
        recipient = await _phase2_recipient(db_session)
        quiet_from, quiet_to, days = _window_around_now()
        await set_quiet_hours(
            db_session, recipient.id,
            quiet_from=quiet_from, quiet_to=quiet_to, days=days,
        )
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

        assert await process_pending_notifications() == 1

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.PROCESSING
        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.attempts == 0
        assert delivery.error_message is None

        expected = recipient_quiet_until(
            await _fetch_recipient(recipient.id), datetime.now(UTC),
        )
        assert expected is not None
        assert delivery.next_retry_at == expected

        # Fully gated -> the notification is invisible to the poll.
        assert await process_pending_notifications() == 0

    async def test_delivery_sends_outside_window(
        self, db_session: AsyncSession,
    ) -> None:
        """A window elsewhere in the day does not block delivery."""
        recipient = await _phase2_recipient(db_session)
        quiet_from, quiet_to, days = _window_missing_now()
        await set_quiet_hours(
            db_session, recipient.id,
            quiet_from=quiet_from, quiet_to=quiet_to, days=days,
        )
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

        assert await process_pending_notifications() == 1
        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.SENT

    async def test_transient_retry_landing_in_window_is_deferred(
        self, db_session: AsyncSession,
    ) -> None:
        """A backoff retry due inside a quiet window is re-deferred to
        the window end without burning an attempt."""
        recipient = await _phase2_recipient(db_session)
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

        # Attempt 1 fails transiently -> backoff gate, attempts == 1.
        with patch(
            "app.engine.service.get_formatter",
            return_value=_FailingFormatter(),
        ):
            await process_pending_notifications()
        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.attempts == 1
        assert delivery.status == DeliveryStatus.PENDING

        # The recipient's quiet window opens before the retry runs.
        quiet_from, quiet_to, days = _window_around_now()
        await set_quiet_hours(
            db_session, recipient.id,
            quiet_from=quiet_from, quiet_to=quiet_to, days=days,
        )
        await db_session.commit()
        await _force_retry_due(delivery.id)

        # No patch: an ungated retry WOULD send via the stub formatter.
        await process_pending_notifications()

        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.attempts == 1  # deferral is not an attempt
        expected = recipient_quiet_until(
            await _fetch_recipient(recipient.id), datetime.now(UTC),
        )
        assert expected is not None
        assert delivery.next_retry_at == expected


class TestLateMuteAtDeliver:
    """Phase 2.1 item 3: mutes set after resolve close gated
    deliveries out with DeliveryStatus.SKIPPED at deliver time."""

    async def test_late_mute_closes_deferred_delivery(
        self, db_session: AsyncSession,
    ) -> None:
        """Done-when scenario: delivery created -> mute -> gate opens
        -> NO send; delivery and notification end SKIPPED."""
        recipient = await _phase2_recipient(db_session)
        quiet_from, quiet_to, days = _window_around_now()
        await set_quiet_hours(
            db_session, recipient.id,
            quiet_from=quiet_from, quiet_to=quiet_to, days=days,
        )
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

        # Pass 1: quiet-deferred (delivery exists, nothing sent yet).
        assert await process_pending_notifications() == 1
        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.status == DeliveryStatus.PENDING

        # The recipient mutes the category while the delivery waits.
        await set_category_muted(
            db_session, recipient.id, "unit_updates", True,
        )
        await db_session.commit()
        await _force_retry_due(delivery.id)

        # Gate opens: an unmuted delivery WOULD send via the stub.
        await process_pending_notifications()

        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.status == DeliveryStatus.SKIPPED
        assert delivery.attempts == 0  # a skip is not an attempt
        assert delivery.sent_at is None
        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.SKIPPED

    async def test_late_mute_mixed_audience_keeps_history(
        self, db_session: AsyncSession,
    ) -> None:
        """One of two mutes while backoff-gated: the muted delivery
        closes SKIPPED with its transient history intact, the other
        sends, the notification rolls up SENT."""
        muted = await _phase2_recipient(db_session)
        listening = await _phase2_recipient(db_session)
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

        # Pass 1: both fail transiently -> attempts 1, backoff-gated.
        with patch(
            "app.engine.service.get_formatter",
            return_value=_FailingFormatter(),
        ):
            await process_pending_notifications()

        deliveries = await _fetch_deliveries(notification.id)
        assert all(d.attempts == 1 for d in deliveries)

        # Late mute for one of the two, then the gates open.
        await set_category_muted(db_session, muted.id, "unit_updates", True)
        await db_session.commit()
        for delivery in deliveries:
            await _force_retry_due(delivery.id)

        await process_pending_notifications()

        by_recipient = {
            d.recipient_id: d
            for d in await _fetch_deliveries(notification.id)
        }
        skipped = by_recipient[muted.id]
        sent = by_recipient[listening.id]
        assert skipped.status == DeliveryStatus.SKIPPED
        assert skipped.attempts == 1  # prior transient history kept
        assert skipped.error_message is not None
        assert sent.status == DeliveryStatus.SENT
        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.SENT

    async def test_rollup_matrix_with_skipped(
        self, db_session: AsyncSession,
    ) -> None:
        """Skips are non-events: the verdict comes from the rest."""
        cases: list[tuple[str, str, str]] = [
            (DeliveryStatus.SKIPPED, DeliveryStatus.SENT,
             NotificationStatus.SENT),
            (DeliveryStatus.SKIPPED, DeliveryStatus.FAILED,
             NotificationStatus.FAILED),
            (DeliveryStatus.SKIPPED, DeliveryStatus.PENDING,
             NotificationStatus.PROCESSING),
            (DeliveryStatus.SKIPPED, DeliveryStatus.SKIPPED,
             NotificationStatus.SKIPPED),
        ]
        await _phase2_recipient(db_session)
        await _phase2_recipient(db_session)

        for status_a, status_b, expected in cases:
            notification = await create_notification(
                db_session,
                type="unit_event",
                title="T",
                body="B",
                target_type=TargetType.ALL,
                target_value="*",
                channels=["telegram"],
            )
            deliveries = await resolve_notification(db_session, notification)
            assert len(deliveries) == 2
            deliveries[0].status = status_a
            deliveries[1].status = status_b
            await db_session.flush()

            await rollup_notification(db_session, notification)
            assert notification.status == expected, (
                f"{status_a}+{status_b} -> expected {expected}, "
                f"got {notification.status}"
            )
