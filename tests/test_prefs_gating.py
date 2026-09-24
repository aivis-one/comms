# =============================================================================
# COMMS Service -- Preferences + gating tests (Phase 2 items 5-7)
# =============================================================================
# Item 5: preference API -- category mutes (idempotent, validated
#   against the profile), the delivery schedule (allowed periods,
#   normalization). Timezone is NOT settable here since Phase 2.1
#   (sync-owned; read-only in RecipientPreferences).
# Item 6: gating -- a muted recipient gets NO deliveries (gated at
#   resolve, family granularity via the type dictionary); the schedule
#   DEFER delivery via next_retry_at (never suppress), including
#   backoff retries that land inside a window.
# Item 7: SUPPRESSED (F1.3; SKIPPED before) -- empty-after-mute
# audiences end SUPPRESSED; the
#   status is terminal (invisible to the poll, immune to rollup).
# Phase 2.1 item 3: LATE MUTES -- a mute set while a delivery sits
#   gated (backoff / schedule) closes it out with
#   DeliveryStatus.SUPPRESSED at deliver time; rollup treats skips as
#   non-events (matrix covered below).
#
# Recipients here draw telegram_ids from the Phase 2 band 81000-81999.
# =============================================================================

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.audience.prefs import (
    get_preferences,
    muted_recipient_ids,
    set_category_muted,
    set_schedule,
)
from app.audience.schedule import recipient_deferred_until
from app.core.database import get_session_factory
from app.core.exceptions import NotFoundError, ValidationError
from app.engine.constants import (
    DeliveryStatus,
    FailureClass,
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
from tests.helpers import create_recipient, intake_fields, next_phase2_telegram_id


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


def _allowed_now() -> list[dict[str, int]]:
    """A schedule whose allowed period straddles the current moment.

    Periods never cross midnight now, so a stretch around "now" may
    fall in two days -- near midnight this yields one period ending at
    24:00 and another starting at 00:00 the next day. That IS the
    replacement for the old overnight window, expressed rather than
    implied.
    """
    start = datetime.now(UTC) - timedelta(hours=2)
    end = datetime.now(UTC) + timedelta(hours=2)
    return _periods_between(start, end)


def _allowed_later() -> list[dict[str, int]]:
    """A schedule allowing delivery only in 3..4 hours from now."""
    start = datetime.now(UTC) + timedelta(hours=3)
    end = datetime.now(UTC) + timedelta(hours=4)
    return _periods_between(start, end)


def _periods_between(
    start: datetime, end: datetime
) -> list[dict[str, int]]:
    """Stored periods covering [start, end), split at local midnight."""

    def minutes(moment: datetime) -> int:
        return moment.hour * 60 + moment.minute

    if start.date() == end.date():
        return [
            {
                "day": start.date().isoweekday(),
                "from": minutes(start),
                "to": max(minutes(end), minutes(start) + 1),
            }
        ]
    return [
        {
            "day": start.date().isoweekday(),
            "from": minutes(start),
            "to": 1440,
        },
        {
            "day": end.date().isoweekday(),
            "from": 0,
            "to": max(minutes(end), 1),
        },
    ]


class TestPreferenceApi:
    """Item 5: mutes, the delivery schedule and timezone."""

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

    async def test_schedule_roundtrip_and_clear(
        self, db_session: AsyncSession,
    ) -> None:
        """Set normalizes (sorted by day then start); None clears.

        The old form of this test asserted that the DAY LIST was
        deduplicated and sorted, and it was right about the model it
        tested: one window with a set of start days. There is no set
        of days any more -- each period carries its own day -- so what
        is left to pin is the ordering of the periods themselves.
        """
        recipient = await _phase2_recipient(db_session)
        await set_schedule(
            db_session,
            recipient.id,
            windows=[
                {"day": 5, "from": 540, "to": 720},
                {"day": 1, "from": 1320, "to": 1440},
                {"day": 1, "from": 540, "to": 720},
            ],
        )
        prefs = await get_preferences(db_session, recipient.id)
        assert prefs.allowed_windows == (
            {"day": 1, "from": 540, "to": 720},
            {"day": 1, "from": 1320, "to": 1440},
            {"day": 5, "from": 540, "to": 720},
        )

        await set_schedule(db_session, recipient.id, windows=None)
        prefs = await get_preferences(db_session, recipient.id)
        assert prefs.allowed_windows is None

    async def test_schedule_invalid_inputs_rejected(
        self, db_session: AsyncSession,
    ) -> None:
        """Every state from the release's own table that must bounce.

        The old test covered a partial window, a zero-length one and
        bad days. Partial state is gone with the three columns; the
        rest carried over, and two refusals are NEW because the model
        is: an empty list (which would mean "never deliver", a black
        hole rather than a schedule) and periods that overlap or touch
        (one stretch, one spelling -- accepting both would let two
        stored values mean one thing, which is the defect class this
        release removes).
        """
        recipient = await _phase2_recipient(db_session)
        with pytest.raises(ValidationError, match="at least one period"):
            await set_schedule(db_session, recipient.id, windows=[])
        with pytest.raises(ValidationError, match="end after it starts"):
            await set_schedule(
                db_session, recipient.id,
                windows=[{"day": 1, "from": 540, "to": 540}],
            )
        with pytest.raises(ValidationError, match="ISO weekday"):
            await set_schedule(
                db_session, recipient.id,
                windows=[{"day": 8, "from": 540, "to": 600}],
            )
        with pytest.raises(ValidationError, match="minutes"):
            await set_schedule(
                db_session, recipient.id,
                windows=[{"day": 1, "from": 540, "to": 1441}],
            )
        with pytest.raises(ValidationError, match="overlap or touch"):
            await set_schedule(
                db_session, recipient.id,
                windows=[
                    {"day": 1, "from": 540, "to": 720},
                    {"day": 1, "from": 700, "to": 780},
                ],
            )
        with pytest.raises(ValidationError, match="overlap or touch"):
            await set_schedule(
                db_session, recipient.id,
                windows=[
                    {"day": 1, "from": 540, "to": 720},
                    {"day": 1, "from": 720, "to": 780},
                ],
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
    """Item 6 (mutes) + item 7 (SUPPRESSED, SKIPPED before F1.3):
    resolve-time gating."""

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
                **intake_fields(),
                type=family_type,
                title="T",
                body="B",
                target_type=TargetType.USER,
                target_value=str(recipient.id),
            )
            deliveries = await resolve_notification(db_session, notification)
            assert deliveries == []
            assert notification.status == NotificationStatus.SUPPRESSED

    async def test_all_muted_audience_marks_suppressed(
        self, db_session: AsyncSession,
    ) -> None:
        """Everyone muted -> zero deliveries + SUPPRESSED, not FAILED.

        F1.3: was SKIPPED, which also meant "empty audience"; the
        recipients' decision is its own outcome now."""
        recipient = await _phase2_recipient(db_session)
        await set_category_muted(
            db_session, recipient.id, "unit_updates", True,
        )
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
        )
        deliveries = await resolve_notification(db_session, notification)
        assert deliveries == []
        assert notification.status == NotificationStatus.SUPPRESSED
        assert await _fetch_deliveries(notification.id) == []

    async def test_mixed_audience_delivers_to_unmuted_only(
        self, db_session: AsyncSession,
    ) -> None:
        """Muted recipients are dropped; the rest deliver -> SENT.

        R-0: this test requested telegram and relied on the old stub,
        which made an UNCONFIGURED channel "succeed". Its subject is the
        mute gate, not the channel, and delivery does not branch on the
        channel before the formatter (app/engine/service.py
        deliver_notification). An unconfigured telegram now FAILS
        permanently -- so the test asks for in_app, the channel that is
        live on every deploy by definition (zero declared keys).
        """
        muted = await _phase2_recipient(db_session)
        listening = await _phase2_recipient(db_session)
        await set_category_muted(db_session, muted.id, "unit_updates", True)
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event_in_app",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
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
            **intake_fields(),
            type="unit_plain",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
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

    async def test_suppressed_is_terminal(
        self, db_session: AsyncSession,
    ) -> None:
        """Item 7: SUPPRESSED (SKIPPED before F1.3) is invisible to the
        poll and rollup-proof."""
        recipient = await _phase2_recipient(db_session)
        await set_category_muted(
            db_session, recipient.id, "unit_updates", True,
        )
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        await resolve_notification(db_session, notification)
        assert notification.status == NotificationStatus.SUPPRESSED

        # Rollup must not reinterpret "zero deliveries" as FAILED.
        await rollup_notification(db_session, notification)
        assert notification.status == NotificationStatus.SUPPRESSED
        await db_session.commit()

        # The worker poll only sees PENDING/PROCESSING.
        assert await process_pending_notifications() == 0
        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.SUPPRESSED


class TestQuietHoursGating:
    """Item 6 (quiet hours): defer via next_retry_at, never suppress."""

    async def test_delivery_deferred_outside_the_allowed_periods(
        self, db_session: AsyncSession,
    ) -> None:
        """Outside the allowed periods: no send, no attempt burned,
        gate set to the next opening; the gated row hides from the
        next poll.

        THE POLARITY FLIPPED, the assertion did not. This test used to
        put the recipient INSIDE a quiet window to get the same
        deferral, and it was right about that model. What it pins is
        unchanged: a deferral is not a failure and not an attempt.
        """
        recipient = await _phase2_recipient(db_session)
        await set_schedule(
            db_session, recipient.id, windows=_allowed_later(),
        )
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        await db_session.commit()

        assert await process_pending_notifications() == 1

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.PROCESSING
        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.attempts == 0
        assert delivery.error_message is None

        expected = recipient_deferred_until(
            await _fetch_recipient(recipient.id), datetime.now(UTC),
        )
        assert expected is not None
        assert delivery.next_retry_at == expected

        # Fully gated -> the notification is invisible to the poll.
        assert await process_pending_notifications() == 0

    async def test_delivery_sends_inside_an_allowed_period(
        self, db_session: AsyncSession,
    ) -> None:
        """A window elsewhere in the day does not block delivery.

        R-0: this test requested telegram and relied on the old stub,
        which made an UNCONFIGURED channel "succeed". Its subject is the
        quiet-hours gate, not the channel, and delivery does not branch
        on the channel before the formatter (app/engine/service.py
        deliver_notification). An unconfigured telegram now FAILS
        permanently -- so the test asks for in_app, the channel that is
        live on every deploy by definition (zero declared keys).
        """
        recipient = await _phase2_recipient(db_session)
        await set_schedule(
            db_session, recipient.id, windows=_allowed_now(),
        )
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event_in_app",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        await db_session.commit()

        assert await process_pending_notifications() == 1
        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.SENT

    async def test_transient_retry_landing_outside_the_periods_is_deferred(
        self, db_session: AsyncSession,
    ) -> None:
        """A backoff retry due inside a quiet window is re-deferred to
        the window end without burning an attempt."""
        recipient = await _phase2_recipient(db_session)
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
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
        await set_schedule(
            db_session, recipient.id, windows=_allowed_later(),
        )
        await db_session.commit()
        await _force_retry_due(delivery.id)

        # No patch: an ungated retry WOULD send via the stub formatter.
        await process_pending_notifications()

        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.attempts == 1  # deferral is not an attempt
        expected = recipient_deferred_until(
            await _fetch_recipient(recipient.id), datetime.now(UTC),
        )
        assert expected is not None
        assert delivery.next_retry_at == expected


class TestLateMuteAtDeliver:
    """Phase 2.1 item 3: mutes set after resolve close gated
    deliveries out with DeliveryStatus.SUPPRESSED (SKIPPED before F1.3)
    at deliver time."""

    async def test_late_mute_closes_deferred_delivery(
        self, db_session: AsyncSession,
    ) -> None:
        """Done-when scenario: delivery created -> mute -> gate opens
        -> NO send; delivery and notification end SUPPRESSED."""
        recipient = await _phase2_recipient(db_session)
        await set_schedule(
            db_session, recipient.id, windows=_allowed_later(),
        )
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
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
        assert delivery.status == DeliveryStatus.SUPPRESSED
        assert delivery.attempts == 0  # a skip is not an attempt
        assert delivery.sent_at is None
        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.SUPPRESSED

    async def test_late_mute_mixed_audience_keeps_history(
        self, db_session: AsyncSession,
    ) -> None:
        """One of two mutes while backoff-gated: the muted delivery
        closes SUPPRESSED with its transient history intact, the other
        sends, the notification rolls up SENT.

        R-0: this test requested telegram and relied on the old stub,
        which made an UNCONFIGURED channel "succeed". Its subject is the
        late-mute gate, not the channel, and delivery does not branch on
        the channel before the formatter (app/engine/service.py
        deliver_notification). An unconfigured telegram now FAILS
        permanently -- so the test asks for in_app, the channel that is
        live on every deploy by definition (zero declared keys).
        """
        muted = await _phase2_recipient(db_session)
        listening = await _phase2_recipient(db_session)
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event_in_app",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
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
        suppressed = by_recipient[muted.id]
        sent = by_recipient[listening.id]
        assert suppressed.status == DeliveryStatus.SUPPRESSED
        assert suppressed.attempts == 1  # prior transient history kept
        assert suppressed.error_message is not None
        assert sent.status == DeliveryStatus.SENT
        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.SENT

    async def test_rollup_matrix_with_suppressed(
        self, db_session: AsyncSession,
    ) -> None:
        """Suppressions are non-events: the verdict comes from the rest.

        F1.3: SKIPPED became SUPPRESSED at both levels (the "everyone
        muted" half of the old SKIPPED); the verdicts are unchanged --
        which is THE FOLD's rule 1, now written next to the code."""
        cases: list[tuple[str, str, str]] = [
            (DeliveryStatus.SUPPRESSED, DeliveryStatus.SENT,
             NotificationStatus.SENT),
            (DeliveryStatus.SUPPRESSED, DeliveryStatus.FAILED,
             NotificationStatus.FAILED),
            (DeliveryStatus.SUPPRESSED, DeliveryStatus.PENDING,
             NotificationStatus.PROCESSING),
            (DeliveryStatus.SUPPRESSED, DeliveryStatus.SUPPRESSED,
             NotificationStatus.SUPPRESSED),
        ]
        await _phase2_recipient(db_session)
        await _phase2_recipient(db_session)

        for status_a, status_b, expected in cases:
            notification = await create_notification(
                db_session,
                **intake_fields(),
                type="unit_event",
                title="T",
                body="B",
                target_type=TargetType.ALL,
                target_value="*",
            )
            deliveries = await resolve_notification(db_session, notification)
            assert len(deliveries) == 2
            for delivery, status in zip(
                deliveries, (status_a, status_b), strict=True,
            ):
                delivery.status = status
                # A failed delivery carries its class (CHECK
                # ck_deliveries_failure_class, F1.3).
                if status == DeliveryStatus.FAILED:
                    delivery.failure_class = FailureClass.MESSAGE_REJECTED
            await db_session.flush()

            await rollup_notification(db_session, notification)
            assert notification.status == expected, (
                f"{status_a}+{status_b} -> expected {expected}, "
                f"got {notification.status}"
            )

    async def test_rollup_triple_mixed_is_partial_sent(
        self, db_session: AsyncSession,
    ) -> None:
        """Phase 2.2 gap: the triple suppressed+sent+failed -- after
        subtracting the suppression, {sent, failed} remains -> PARTIAL_SENT."""
        for _ in range(3):
            await _phase2_recipient(db_session)
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
        )
        deliveries = await resolve_notification(db_session, notification)
        assert len(deliveries) == 3
        deliveries[0].status = DeliveryStatus.SUPPRESSED
        deliveries[1].status = DeliveryStatus.SENT
        deliveries[2].status = DeliveryStatus.FAILED
        deliveries[2].failure_class = FailureClass.MESSAGE_REJECTED
        await db_session.flush()

        await rollup_notification(db_session, notification)
        assert notification.status == NotificationStatus.PARTIAL_SENT


async def _force_expiry(notification_id: UUID) -> None:
    """Backdate expiry_at -- simulates the deadline passing."""
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            select(Notification).where(Notification.id == notification_id)
        )
        notification = result.scalar_one()
        notification.expiry_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()


class TestScheduleDeferralVsExpiry:
    """Phase 2.2 items 3+5: a schedule deferral that pushes past
    expiry_at is a DELIBERATE expiry, not a late send -- and the
    deferral log names the causality (beyond_expiry=true)."""

    async def test_expiry_outside_the_periods_expires(
        self, db_session: AsyncSession,
    ) -> None:
        """Deferred past the deadline -> step-0 EXPIRED, nothing sent;
        the deferral log flags beyond_expiry."""
        recipient = await _phase2_recipient(db_session)
        await set_schedule(
            db_session, recipient.id, windows=_allowed_later(),
        )
        # The deadline falls before the gate reopens: the next
        # allowed period starts ~now+3h, the notification dies at
        # now+1h.
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            expiry_at=datetime.now(UTC) + timedelta(hours=1),
        )
        await db_session.commit()

        with capture_logs() as logs:
            assert await process_pending_notifications() == 1
        deferred = [
            log for log in logs
            if log["event"] == "delivery_schedule_deferred"
        ]
        assert len(deferred) == 1
        assert deferred[0]["beyond_expiry"] is True

        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.status == DeliveryStatus.PENDING

        # The deadline passes while the gate is still closed.
        await _force_expiry(notification.id)
        await process_pending_notifications()

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.EXPIRED
        (delivery,) = await _fetch_deliveries(notification.id)
        assert delivery.sent_at is None
