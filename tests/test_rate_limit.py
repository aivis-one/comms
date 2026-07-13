# =============================================================================
# COMMS Service -- Channel rate-limit (429) handling tests (Phase 2.2)
# =============================================================================
# A 429 is "come back later", not a message failure:
#   - the delivery is deferred via next_retry_at using the
#     SERVER-NAMED retry_after (+1-2s jitter), attempts untouched;
#   - a per-delivery budget (rate_limit_deferrals) bounds the loop:
#     past settings.notification_max_rate_limit_deferrals a 429
#     degrades to a regular transient failure, so termination is
#     guaranteed by the finite attempts budget;
#   - everything that is NOT a typed RateLimitedError (timeouts,
#     generic transients) behaves exactly as before and never touches
#     the deferral counter.
#
# Recipients draw telegram_ids from the Phase 2 band 81000-81999.
# =============================================================================

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.constants import (
    DeliveryStatus,
    NotificationStatus,
    TargetType,
)
from app.engine.formatters import RateLimitedError
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import process_pending_notifications
from app.engine.service import create_notification
from tests.helpers import create_recipient, next_phase2_telegram_id


async def _fetch_delivery(notification_id: UUID) -> NotificationDelivery:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            select(NotificationDelivery).where(
                NotificationDelivery.notification_id == notification_id
            )
        )
        return result.scalar_one()


async def _fetch_notification(notification_id: UUID) -> Notification:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            select(Notification).where(Notification.id == notification_id)
        )
        return result.scalar_one()


async def _force_retry_due(delivery_id: UUID) -> None:
    """Backdate the retry gate -- simulates the wait passing."""
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


class _RateLimitedFormatter:
    """Formatter that always answers 429 with a fixed retry_after."""

    def __init__(self, retry_after: float = 42.0) -> None:
        self.retry_after = retry_after

    async def deliver(self, *args: Any, **kwargs: Any) -> bool:
        raise RateLimitedError(self.retry_after)


class _FailingFormatter:
    """Formatter that always fails transiently."""

    async def deliver(self, *args: Any, **kwargs: Any) -> bool:
        raise RuntimeError("boom transient")


class _SlowFormatter:
    """Formatter that outlives the per-call delivery timeout."""

    async def deliver(self, *args: Any, **kwargs: Any) -> bool:
        import asyncio

        await asyncio.sleep(1.0)
        return True


async def _rate_limited_setup(
    db_session: AsyncSession,
    expiry_at: datetime | None = None,
) -> tuple[Recipient, Notification]:
    """One recipient + one pending telegram notification, committed."""
    recipient = await create_recipient(
        db_session, telegram_id=next_phase2_telegram_id(),
    )
    notification = await create_notification(
        db_session,
        type="unit_event",
        title="T",
        body="B",
        target_type=TargetType.USER,
        target_value=str(recipient.id),
        channels=["telegram"],
        expiry_at=expiry_at,
    )
    await db_session.commit()
    return recipient, notification


class TestRateLimitDeferral:
    """429 defers via the server-named wait, without burning attempts."""

    async def test_429_defers_without_burning_attempt(
        self, db_session: AsyncSession,
    ) -> None:
        """Done-when core: attempts stay 0, next_retry_at comes from
        retry_after (+ jitter), the counter tracks the deferral."""
        _, notification = await _rate_limited_setup(db_session)

        before = datetime.now(UTC)
        with (
            patch(
                "app.engine.service.get_formatter",
                return_value=_RateLimitedFormatter(retry_after=42.0),
            ),
            capture_logs() as logs,
        ):
            assert await process_pending_notifications() == 1

        deferred = [
            log for log in logs
            if log["event"] == "delivery_rate_limit_deferred"
        ]
        assert len(deferred) == 1
        # No expiry on this notification -> the causality flag is off.
        assert deferred[0]["beyond_expiry"] is False
        # 42s is well under the trust ceiling -> honored as-is.
        assert deferred[0]["capped"] is False

        delivery = await _fetch_delivery(notification.id)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.attempts == 0
        assert delivery.rate_limit_deferrals == 1
        assert delivery.next_retry_at is not None
        # Server-named wait + 1-2s jitter (slack for test runtime).
        low = before + timedelta(seconds=42)
        high = datetime.now(UTC) + timedelta(seconds=42 + 2)
        assert low <= delivery.next_retry_at <= high

        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.PROCESSING
        # Gated -> invisible to the next poll until the wait passes.
        assert await process_pending_notifications() == 0

    async def test_budget_exhausted_degrades_to_transient(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Past the budget a 429 burns attempts like any transient
        error and terminates in FAILED -- no infinite deferral."""
        monkeypatch.setattr(
            settings, "notification_max_rate_limit_deferrals", 2,
        )
        _, notification = await _rate_limited_setup(db_session)

        with patch(
            "app.engine.service.get_formatter",
            return_value=_RateLimitedFormatter(),
        ):
            # Deferrals 1 and 2: within budget, attempts untouched.
            for expected_deferrals in (1, 2):
                await process_pending_notifications()
                delivery = await _fetch_delivery(notification.id)
                assert delivery.rate_limit_deferrals == expected_deferrals
                assert delivery.attempts == 0
                await _force_retry_due(delivery.id)

            # Budget exhausted: every further 429 is a transient
            # failure. attempts budget (3) drives it to FAILED.
            for expected_attempts in (1, 2, 3):
                await process_pending_notifications()
                delivery = await _fetch_delivery(notification.id)
                assert delivery.attempts == expected_attempts
                assert delivery.rate_limit_deferrals == 2  # frozen
                if expected_attempts < 3:
                    assert delivery.status == DeliveryStatus.PENDING
                    assert delivery.error_message is not None
                    assert "rate limited" in delivery.error_message
                    await _force_retry_due(delivery.id)

        delivery = await _fetch_delivery(notification.id)
        assert delivery.status == DeliveryStatus.FAILED
        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.FAILED

    async def test_timeout_stays_transient(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A delivery timeout must burn an attempt (transient path)
        and never touch the 429 counter -- guards the except order."""
        monkeypatch.setattr(
            "app.engine.service._DELIVER_TIMEOUT_SECONDS", 0.05,
        )
        _, notification = await _rate_limited_setup(db_session)

        with patch(
            "app.engine.service.get_formatter",
            return_value=_SlowFormatter(),
        ):
            await process_pending_notifications()

        delivery = await _fetch_delivery(notification.id)
        assert delivery.attempts == 1
        assert delivery.rate_limit_deferrals == 0
        assert delivery.error_message is not None
        assert "Timeout" in delivery.error_message

    async def test_regular_transient_untouched(
        self, db_session: AsyncSession,
    ) -> None:
        """Plain transient errors behave exactly as before Phase 2.2
        and never increment the deferral counter."""
        _, notification = await _rate_limited_setup(db_session)

        with patch(
            "app.engine.service.get_formatter",
            return_value=_FailingFormatter(),
        ):
            await process_pending_notifications()

        delivery = await _fetch_delivery(notification.id)
        assert delivery.attempts == 1
        assert delivery.rate_limit_deferrals == 0
        assert delivery.status == DeliveryStatus.PENDING

    async def test_retry_after_is_capped(
        self, db_session: AsyncSession,
    ) -> None:
        """Phase 2.3: retry_after is UNTRUSTED channel output -- a
        pathological value must not park the delivery for hours. The
        honored wait is capped at the DEDICATED trust knob
        (notification_max_retry_after_seconds, generous by design);
        jitter rides on top of the CAPPED value; the override is
        named in the log (capped=true is an alarm signal)."""
        _, notification = await _rate_limited_setup(db_session)
        cap = settings.notification_max_retry_after_seconds

        before = datetime.now(UTC)
        with (
            patch(
                "app.engine.service.get_formatter",
                return_value=_RateLimitedFormatter(
                    retry_after=float(cap * 2),
                ),
            ),
            capture_logs() as logs,
        ):
            assert await process_pending_notifications() == 1

        deferred = [
            log for log in logs
            if log["event"] == "delivery_rate_limit_deferred"
        ]
        assert len(deferred) == 1
        assert deferred[0]["capped"] is True

        delivery = await _fetch_delivery(notification.id)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.attempts == 0
        assert delivery.rate_limit_deferrals == 1
        assert delivery.next_retry_at is not None
        # cap + jitter(1..2), NOT the server-named double.
        low = before + timedelta(seconds=cap)
        high = datetime.now(UTC) + timedelta(seconds=cap + 2)
        assert low <= delivery.next_retry_at <= high

    async def test_429_deferral_flags_beyond_expiry(
        self, db_session: AsyncSession,
    ) -> None:
        """Phase 2.3: same causality flag as the quiet gate -- a 429
        deferral pushing past expiry_at is named in the log, and the
        step-0 sweep then expires the notification, deliberately."""
        _, notification = await _rate_limited_setup(
            db_session,
            expiry_at=datetime.now(UTC) + timedelta(seconds=10),
        )

        with (
            patch(
                "app.engine.service.get_formatter",
                return_value=_RateLimitedFormatter(retry_after=42.0),
            ),
            capture_logs() as logs,
        ):
            assert await process_pending_notifications() == 1

        deferred = [
            log for log in logs
            if log["event"] == "delivery_rate_limit_deferred"
        ]
        assert len(deferred) == 1
        assert deferred[0]["beyond_expiry"] is True

        delivery = await _fetch_delivery(notification.id)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.attempts == 0

        # The deadline passes while the gate is closed -> EXPIRED.
        await _force_expiry_now(notification.id)
        await process_pending_notifications()
        fresh = await _fetch_notification(notification.id)
        assert fresh.status == NotificationStatus.EXPIRED
        delivery = await _fetch_delivery(notification.id)
        assert delivery.sent_at is None


async def _force_expiry_now(notification_id: UUID) -> None:
    """Backdate expiry_at -- simulates the deadline passing."""
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            select(Notification).where(Notification.id == notification_id)
        )
        notification = result.scalar_one()
        notification.expiry_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()
