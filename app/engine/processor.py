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
#   Plus: expire overdue notifications, delete expired delivered
#   ones, and (on its own slow cadence -- see app/engine/worker.py)
#   drain terminal notifications past retention (Phase 3a item 5).
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

import time
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import and_, delete, or_, select, update

from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.constants import DeliveryStatus, NotificationStatus
from app.engine.models import Notification, NotificationDelivery
from app.engine.service import (
    delete_terminal_notifications_batch,
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
    # Review 1.2: PENDING rows are always ready (not yet resolved).
    # PROCESSING rows are picked only when at least one delivery is
    # actually attemptable (pending + retry gate open) -- otherwise a
    # gated notification would be locked and no-op'ed every tick, and
    # the no-op "processed" count would keep the worker loop from
    # backing off for the whole backoff window.
    async with factory() as session:
        ready_delivery = (
            select(NotificationDelivery.id)
            .where(
                NotificationDelivery.notification_id == Notification.id,
                NotificationDelivery.status == DeliveryStatus.PENDING,
                or_(
                    NotificationDelivery.next_retry_at.is_(None),
                    NotificationDelivery.next_retry_at <= now,
                ),
            )
            .exists()
        )
        stmt = (
            select(Notification.id)
            .where(
                or_(
                    Notification.status == NotificationStatus.PENDING,
                    and_(
                        Notification.status == NotificationStatus.PROCESSING,
                        ready_delivery,
                    ),
                ),
                Notification.scheduled_at <= now,
            )
            .order_by(Notification.priority, Notification.scheduled_at)
            # Review 1.1: cap the batch; the tail is picked up on the
            # next tick (the worker loop is eternal anyway).
            .limit(settings.notification_batch_size)
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
      - status in (sent, partial_sent, expired, skipped)

    SKIPPED is terminal (Phase 2): an expired skipped notification is
    as dead as an expired sent one. General retention of terminal
    notifications (retention_days) is Phase 3 -- this cleanup only
    covers rows that carry an explicit expiry_at.

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
                        NotificationStatus.SKIPPED,
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


# Phase 3a item 5: rows deleted per retention batch. One batch = one
# transaction (bounded FK cascade); the drain loop below commits
# between batches. Module-level so tests can shrink it to force a
# multi-batch drain.
_RETENTION_BATCH_SIZE = 1000


async def cleanup_terminal_notifications() -> int:
    """Retention pass: drain terminal notifications older than
    NOTIFICATION_RETENTION_DAYS, in batches (Phase 3a item 5).

    Orchestration only (fix C): the service supplies ONE commit-free
    batch (delete_terminal_notifications_batch); this loop owns the
    commits -- one per batch, so a 90-day backlog never becomes one
    long transaction -- and drains until a batch comes back short.

    DISABLED (fix I): settings.notification_retention_days <= 0 means
    retention is OFF -- return 0 without touching anything ("delete
    everything" must never fall out of the cutoff arithmetic; the
    worker startup log names the disabled state loudly). The guard
    lives HERE, not only behind the worker's cadence gate, because
    tests call this function directly.

    Scheduling lives in app/engine/worker.py (fix H): the pass runs on
    its own slow cadence (NOTIFICATION_RETENTION_INTERVAL_SECONDS),
    not on the worker tick. Every pass logs its duration -- that is
    the observable value of the BL-3 promotion trigger.

    KNOWN CEILING (acknowledged by design -- dispatch plan BL-3):
      1. Mechanics: the batch select filters on (status, created_at)
         with no matching index -> each pass seq-scans the
         notifications table as it grows.
      2. Status: acknowledged by design.
      3. Backlog ref: BL-3 (dispatch plan §6a).
      4. Promotion trigger (observable, via the retention_pass log):
         a pass stably longer than ~1 second OR terminal rows on the
         order of a million.
      5. Agreed fix: ONE migration -- a partial index on created_at
         with a predicate over the terminal statuses.
      6. Rejected: an index NOW (write amplification on a hot table
         for a query that is cheap at current scale and rare by
         cadence); an unbounded DELETE (one long transaction + its FK
         cascade). Also rejected: DB coordination of the cadence gate
         -- it is PER-PROCESS on purpose (two workers -> two cheap
         scans per interval, idempotent and harmless); do not "fix"
         it with a distributed lock.

    Returns:
        Total number of notifications deleted this pass.
    """
    retention_days = settings.notification_retention_days
    if retention_days <= 0:
        return 0

    factory = get_session_factory()
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    started = time.monotonic()
    total = 0

    async with factory() as session:
        try:
            while True:
                deleted = await delete_terminal_notifications_batch(
                    session,
                    cutoff=cutoff,
                    limit=_RETENTION_BATCH_SIZE,
                )
                await session.commit()
                total += deleted
                if deleted < _RETENTION_BATCH_SIZE:
                    break
        except Exception:
            await session.rollback()
            logger.exception("retention_pass_error", deleted=total)
            return total

    # Logged EVERY pass, empty ones included: a slow empty scan is
    # exactly the BL-3 trigger signal.
    logger.info(
        "retention_pass",
        deleted=total,
        duration_ms=round((time.monotonic() - started) * 1000, 1),
        retention_days=retention_days,
    )
    return total
