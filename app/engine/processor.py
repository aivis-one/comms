# =============================================================================
# COMMS Service -- Notification Processor
# =============================================================================
#
# Ported from the cbshome processor (canonical base).
#
# RESPONSIBILITY:
#   Three-stage pipeline for pending notifications:
#     1. resolve  -- expand targets into deliveries
#     2. deliver  -- call channel formatters
#     3. rollup   -- update notification status from delivery statuses
#
#   Plus: expire overdue notifications, delete expired delivered ones.
#
# CALLED BY:
#   app/engine/worker.py (run_notification_batch) -- which the separate
#   worker process loops over.
#
# SESSION MANAGEMENT:
#   Each notification is processed in its own session/transaction.
#   Failure on one notification does not roll back others.
#
# CONCURRENCY:
#   SELECT ... FOR UPDATE SKIP LOCKED prevents double-processing when
#   multiple worker instances run concurrently.
#
# RETRY:
#   Selects both PENDING and PROCESSING notifications. PROCESSING
#   notifications have deliveries that may still be PENDING (failed
#   delivery with attempts < max). resolve_notification is idempotent --
#   skips resolve if deliveries already exist.
#
# SCHEDULING (this is what makes velo-style reminders work with no
# broker): only notifications with scheduled_at <= now() are picked up;
# a future scheduled_at simply waits its turn.
#
# EXPIRE LOGIC:
#   Expires both PENDING and PROCESSING notifications past expiry_at.
# =============================================================================

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select, update

from app.core.database import get_session_factory
from app.engine.constants import NotificationStatus
from app.engine.models import Notification
from app.engine.service import (
    deliver_notification,
    resolve_notification,
    rollup_notification,
)

logger = structlog.get_logger()


async def process_pending_notifications() -> int:
    """Process all pending/processing notifications that are ready.

    Each notification is processed in its own session/transaction.
    Pipeline per notification: resolve -> deliver -> rollup.

    Returns:
        Number of notifications processed.
    """
    factory = get_session_factory()
    now = datetime.now(UTC)

    # -- Step 0: Expire overdue notifications (own session) --
    async with factory() as session:
        try:
            expire_stmt = (
                update(Notification)
                .where(
                    Notification.status.in_([
                        NotificationStatus.PENDING,
                        NotificationStatus.PROCESSING,
                    ]),
                    Notification.expiry_at.isnot(None),
                    Notification.expiry_at < now,
                )
                .values(status=NotificationStatus.EXPIRED)
            )
            expire_result = await session.execute(expire_stmt)
            expired_count: int = expire_result.rowcount  # type: ignore[attr-defined]
            if expired_count:
                logger.info("notifications_expired", count=expired_count)
            await session.commit()
        except Exception:
            await session.rollback()
            logger.exception("notification_expire_error")

    # -- Step 1: Collect IDs of notifications to process --
    async with factory() as session:
        stmt = (
            select(Notification.id)
            .where(
                Notification.status.in_([
                    NotificationStatus.PENDING,
                    NotificationStatus.PROCESSING,
                ]),
                Notification.scheduled_at <= now,
            )
            .order_by(Notification.priority, Notification.scheduled_at)
        )
        result = await session.execute(stmt)
        notification_ids = [row[0] for row in result.all()]

    if not notification_ids:
        return 0

    # -- Step 2: Process each notification in its own session --
    processed = 0
    for notif_id in notification_ids:
        async with factory() as session:
            try:
                # Lock the notification row (skip if another worker has it).
                lock_stmt = (
                    select(Notification)
                    .where(Notification.id == notif_id)
                    .with_for_update(skip_locked=True)
                )
                lock_result = await session.execute(lock_stmt)
                notification = lock_result.scalar_one_or_none()

                if notification is None:
                    # Another worker is processing this one.
                    continue

                # Skip if status changed since our initial query.
                if notification.status not in (
                    NotificationStatus.PENDING,
                    NotificationStatus.PROCESSING,
                ):
                    continue

                # resolve (idempotent -- skips if deliveries exist)
                await resolve_notification(session, notification)

                # deliver
                await deliver_notification(session, notification)

                # rollup
                await rollup_notification(session, notification)

                await session.commit()
                processed += 1

            except Exception:
                await session.rollback()
                logger.exception(
                    "notification_pipeline_error",
                    notification_id=str(notif_id),
                )

    logger.info(
        "notifications_processed",
        total=len(notification_ids),
        processed=processed,
    )
    return processed


async def cleanup_expired_notifications() -> int:
    """Delete expired notifications that have been fully delivered.

    Removes notifications where:
      - expiry_at < now()
      - status in (sent, partial_sent, expired)

    Deliveries are CASCADE-deleted automatically.

    Returns:
        Number of notifications deleted.
    """
    factory = get_session_factory()
    now = datetime.now(UTC)

    async with factory() as session:
        try:
            stmt = (
                delete(Notification)
                .where(
                    Notification.expiry_at.isnot(None),
                    Notification.expiry_at < now,
                    Notification.status.in_([
                        NotificationStatus.SENT,
                        NotificationStatus.PARTIAL_SENT,
                        NotificationStatus.EXPIRED,
                    ]),
                )
            )
            result = await session.execute(stmt)
            deleted: int = result.rowcount  # type: ignore[attr-defined]

            if deleted:
                logger.info("notifications_cleaned_up", count=deleted)

            await session.commit()
            return deleted

        except Exception:
            await session.rollback()
            logger.exception("notification_cleanup_error")
            return 0
