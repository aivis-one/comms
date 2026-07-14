# =============================================================================
# COMMS Service -- Retention tests (Phase 3a item 5, fixes C/H/I)
# =============================================================================
# Test band: 82000-82999 (comms Phase 3a -- dispatch plan §5).
#
# Layering under test (fix C / fix H):
#   service   -- delete_terminal_notifications_batch: ONE commit-free
#                batch (exercised through the processor loop here);
#   processor -- cleanup_terminal_notifications: the drain loop, one
#                commit per batch; tests call it DIRECTLY, bypassing
#                the worker's cadence gate (by design);
#   worker    -- run_notification_batch: the per-process cadence gate
#                (monotonic timestamp + reset helper), tested with the
#                processor functions monkeypatched out.
# =============================================================================

import time
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

import app.engine.processor as processor_module
import app.engine.worker as worker_module
from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.constants import (
    DeliveryStatus,
    NotificationStatus,
    TargetType,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import cleanup_terminal_notifications
from app.engine.worker import reset_retention_gate, run_notification_batch
from tests.helpers import create_recipient, next_phase3a_telegram_id


async def _make_notification(
    session: AsyncSession,
    *,
    status: NotificationStatus,
    age_days: int,
    **overrides: Any,
) -> Notification:
    """Persist a notification with an explicit age.

    created_at is set explicitly in the constructor -- SQLAlchemy
    sends the value, overriding the column's server_default.
    """
    defaults: dict[str, Any] = {
        "type": "unit_event",
        "title": "T",
        "body": "B",
        "target_type": TargetType.USER,
        "target_value": "*",
        "status": status,
        "created_at": datetime.now(UTC) - timedelta(days=age_days),
    }
    defaults.update(overrides)
    notification = Notification(**defaults)
    session.add(notification)
    await session.flush()
    return notification


async def _remaining_ids() -> set[UUID]:
    factory = get_session_factory()
    async with factory() as session:
        rows = (await session.execute(select(Notification.id))).scalars()
        return set(rows.all())


class TestRetentionPass:
    """cleanup_terminal_notifications: what gets deleted, what stays."""

    async def test_old_terminal_deleted_fresh_and_active_kept(
        self, db_session: AsyncSession,
    ) -> None:
        """All four SPECCED terminal statuses past retention_days go;
        a fresh terminal row and an old ACTIVE row stay."""
        old_terminal = [
            await _make_notification(
                db_session, status=status, age_days=100,
            )
            for status in (
                NotificationStatus.SENT,
                NotificationStatus.FAILED,
                NotificationStatus.SKIPPED,
                NotificationStatus.EXPIRED,
            )
        ]
        fresh = await _make_notification(
            db_session, status=NotificationStatus.SENT, age_days=10,
        )
        active = await _make_notification(
            db_session, status=NotificationStatus.PENDING, age_days=100,
        )
        await db_session.commit()

        deleted = await cleanup_terminal_notifications()

        assert deleted == len(old_terminal)
        assert await _remaining_ids() == {fresh.id, active.id}

    async def test_boundary_inside_retention_is_kept(
        self, db_session: AsyncSession,
    ) -> None:
        """A terminal row YOUNGER than the cutoff by a margin stays --
        the comparison is strict created_at < cutoff."""
        kept = await _make_notification(
            db_session,
            status=NotificationStatus.SENT,
            age_days=settings.notification_retention_days - 1,
        )
        await db_session.commit()

        assert await cleanup_terminal_notifications() == 0
        assert await _remaining_ids() == {kept.id}

    async def test_partial_sent_is_kept_per_spec(
        self, db_session: AsyncSession,
    ) -> None:
        """PARTIAL_SENT is terminal but deliberately NOT in the spec's
        retention list (SENT/FAILED/SKIPPED/EXPIRED) -- this test PINS
        the specced behavior. Flagged in the phase report: old
        partial_sent rows accumulate until Master-chat decides."""
        kept = await _make_notification(
            db_session,
            status=NotificationStatus.PARTIAL_SENT,
            age_days=365,
        )
        await db_session.commit()

        assert await cleanup_terminal_notifications() == 0
        assert await _remaining_ids() == {kept.id}

    async def test_deliveries_cascade(
        self, db_session: AsyncSession,
    ) -> None:
        """Deliveries of a retained-away notification go with it (FK
        ondelete CASCADE) -- no orphan inbox rows."""
        recipient = await create_recipient(
            db_session, telegram_id=next_phase3a_telegram_id(),
        )
        notification = await _make_notification(
            db_session, status=NotificationStatus.SENT, age_days=100,
        )
        db_session.add(
            NotificationDelivery(
                notification_id=notification.id,
                recipient_id=recipient.id,
                channel="telegram",
                status=DeliveryStatus.SENT,
            )
        )
        await db_session.commit()

        assert await cleanup_terminal_notifications() == 1

        factory = get_session_factory()
        async with factory() as session:
            count = (
                await session.execute(
                    select(func.count()).select_from(NotificationDelivery),
                )
            ).scalar_one()
        assert count == 0

    async def test_drains_past_one_batch(
        self,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fix C: the processor loop drains until a batch comes back
        short -- 5 rows through batches of 2 all go in one pass."""
        monkeypatch.setattr(processor_module, "_RETENTION_BATCH_SIZE", 2)
        for _ in range(5):
            await _make_notification(
                db_session, status=NotificationStatus.SENT, age_days=100,
            )
        await db_session.commit()

        assert await cleanup_terminal_notifications() == 5
        assert await _remaining_ids() == set()

    async def test_retention_days_zero_deletes_nothing(
        self,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fix I done-when: RETENTION_DAYS = 0 means DISABLED -- never
        "cutoff = now, delete everything". Nothing is touched."""
        monkeypatch.setattr(settings, "notification_retention_days", 0)
        ancient = await _make_notification(
            db_session, status=NotificationStatus.SENT, age_days=3650,
        )
        await db_session.commit()

        assert await cleanup_terminal_notifications() == 0
        assert await _remaining_ids() == {ancient.id}

    async def test_retention_days_negative_deletes_nothing(
        self,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fix I: any <= 0 value is the same OFF switch."""
        monkeypatch.setattr(settings, "notification_retention_days", -5)
        ancient = await _make_notification(
            db_session, status=NotificationStatus.FAILED, age_days=3650,
        )
        await db_session.commit()

        assert await cleanup_terminal_notifications() == 0
        assert await _remaining_ids() == {ancient.id}


class TestRetentionCadenceGate:
    """Fix H: the worker runs the retention pass on its own slow
    cadence -- per-process monotonic gate in run_notification_batch.
    The processor functions are monkeypatched out: these tests are
    about SCHEDULING, not deletion."""

    @pytest.fixture(autouse=True)
    def _stub_tick(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> Generator[dict[str, int], None, None]:
        """Reset the gate and replace the tick's processor calls with
        counters (worker imports them into its own namespace)."""
        reset_retention_gate()
        calls = {"pending": 0, "expired": 0, "retention": 0}
        order: list[str] = []

        async def _pending() -> int:
            calls["pending"] += 1
            order.append("pending")
            return 0

        async def _expired() -> int:
            calls["expired"] += 1
            order.append("expired")
            return 0

        async def _retention() -> int:
            calls["retention"] += 1
            order.append("retention")
            return 0

        monkeypatch.setattr(
            worker_module, "process_pending_notifications", _pending,
        )
        monkeypatch.setattr(
            worker_module, "cleanup_expired_notifications", _expired,
        )
        monkeypatch.setattr(
            worker_module, "cleanup_terminal_notifications", _retention,
        )
        self.calls = calls
        self.order = order
        yield calls
        reset_retention_gate()

    async def test_first_tick_runs_retention_immediately(self) -> None:
        """None-state gate: restart = immediate first pass (a feature
        -- deploys restart comms, staleness is bounded above)."""
        await run_notification_batch()
        assert self.calls["retention"] == 1

    async def test_tick_order_is_pending_expiry_retention(self) -> None:
        """Fix H: deliveries first -- retention never delays them."""
        await run_notification_batch()
        assert self.order == ["pending", "expired", "retention"]

    async def test_within_interval_is_gated(self) -> None:
        """Second tick inside the interval: pending + expiry run every
        tick, retention does not."""
        await run_notification_batch()
        await run_notification_batch()
        assert self.calls["pending"] == 2
        assert self.calls["expired"] == 2
        assert self.calls["retention"] == 1

    async def test_after_interval_runs_again(self) -> None:
        """Move the stamp back past the interval -> the gate opens."""
        await run_notification_batch()
        interval = settings.notification_retention_interval_seconds
        worker_module._last_retention_at = (
            time.monotonic() - interval - 1
        )
        await run_notification_batch()
        assert self.calls["retention"] == 2

    async def test_disabled_never_enters_the_gate(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fix I: retention_days <= 0 -> the tick never schedules the
        pass at all (the processor guard is the second line)."""
        monkeypatch.setattr(settings, "notification_retention_days", 0)
        await run_notification_batch()
        await run_notification_batch()
        assert self.calls["retention"] == 0
