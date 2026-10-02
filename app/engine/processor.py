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
#   ones, and (on its own slow cadence -- scheduled by app/worker.py)
#   drain terminal notifications past retention (Phase 3a item 5).
#
# CALLED BY:
#   app/engine/worker.py (run_notification_batch) for the delivery
#   pipeline + expiry; app/worker.py for the retention pass on its own
#   cadence. The worker LOOP and all maintenance scheduling live in
#   app/worker.py (the neutral layer above engine and messaging), which
#   keeps `engine` messaging-free (Phase 4b, edit 3).
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
#
# A PIPELINE DEFECT IS AN OUTCOME (T12):
#   An exception of comms' own in an attempt (channel answers never get
#   here -- _deliver_single turns every one of them into an outcome)
#   rolls the attempt back, and then, in a SEPARATE transaction, the
#   failure is written onto the job: attempts + 1, the step, the
#   exception's class and place (never its text). Below the ceiling
#   (settings.notification_max_pipeline_attempts) the job waits behind
#   a gate, pipeline_retry_at, and the selection skips it until then:
#   oldest-first stays the order, the gate is what keeps a failing job
#   from starving the healthy ones and keeps the healthy ones from
#   starving it. At the ceiling the job is closed: waiting deliveries
#   FAILED with class `pipeline`, the job folded (or FAILED outright
#   when it has no deliveries -- resolve was the step that tore).
# =============================================================================

import time
import traceback
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import structlog
from sqlalchemy import and_, delete, or_, select, update

from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.constants import (
    DeliveryStatus,
    FailureClass,
    NotificationStatus,
    PipelineStep,
)
from app.engine.formatters import sanitized_traceback
from app.engine.models import Notification, NotificationDelivery
from app.engine.service import (
    close_notifications,
    delete_intake_outcomes_before,
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
    # Through the deliveries (F1.3): the waiting ones expire, the job is
    # folded -- a job half of which went out is PARTIAL_SENT, and its
    # waiting deliveries no longer stay PENDING forever.
    async with factory() as session:
        try:
            expired_count = await close_notifications(
                session,
                and_(
                    Notification.expiry_at.isnot(None),
                    Notification.expiry_at < now,
                ),
                NotificationStatus.EXPIRED,
            )
            if expired_count:
                logger.info("notifications_expired", count=expired_count)
            await session.commit()
        except Exception as exc:
            await session.rollback()
            logger.error(
                "notification_expire_error",
                exception=sanitized_traceback(exc),
            )

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
                # T12: a job whose pipeline failed waits behind its gate.
                or_(
                    Notification.pipeline_retry_at.is_(None),
                    Notification.pipeline_retry_at <= now,
                ),
            )
            # Oldest due first. Priority left the ordering with the
            # column (F1.4); isolation is the job of lanes (phase 4).
            .order_by(Notification.scheduled_at)
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
        step = PipelineStep.LOCK
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
                step = PipelineStep.RESOLVE
                await resolve_notification(session, notification)

                # deliver
                step = PipelineStep.DELIVER
                await deliver_notification(session, notification)

                # rollup
                step = PipelineStep.ROLLUP
                await rollup_notification(session, notification)

                # A gate that let this attempt through has done its job;
                # the failure record (attempts, step, error) stays.
                notification.pipeline_retry_at = None

                step = PipelineStep.COMMIT
                await session.commit()
                processed += 1

            except Exception as exc:
                await session.rollback()
                logger.error(
                    "notification_pipeline_error",
                    notification_id=str(notif_id),
                    step=step.value,
                    exception=sanitized_traceback(exc),
                )
                await _record_pipeline_failure(notif_id, step, exc)

    logger.info(
        "notifications_processed",
        total=len(notification_ids),
        processed=processed,
    )
    return processed


# The package whose frames name the place of a pipeline failure: the
# innermost frame inside comms' own code is where comms broke, a frame
# inside a library is where the library was called from comms.
_APP_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = _APP_ROOT.parent
# notifications.pipeline_error is varchar(300) (migration 0015).
_PIPELINE_ERROR_MAX = 300


def pipeline_error_of(exc: BaseException) -> str:
    """What notifications.pipeline_error records: the exception's CLASS
    and the PLACE in comms where it was raised (module:line).

    NEVER the exception's text, sanitized or not. Sanitizing removes
    secrets, not content: a text raised from a template or a formatter
    can carry the letter's variables, and a record of comms holds no
    letter content (spec §6.4). The class and the place say where it
    tore; the redacted traceback in the log says the rest.
    """
    name = f"{type(exc).__module__}.{type(exc).__qualname__}"
    frames = traceback.extract_tb(exc.__traceback__)
    place = "no traceback"
    for frame in reversed(frames):
        path = Path(frame.filename).resolve()
        if path.is_relative_to(_APP_ROOT):
            place = f"{path.relative_to(_REPO_ROOT).as_posix()}:{frame.lineno}"
            break
    else:
        if frames:
            place = f"{Path(frames[-1].filename).name}:{frames[-1].lineno}"
    return f"{name} at {place}"[:_PIPELINE_ERROR_MAX]


def _pipeline_backoff(attempts: int) -> timedelta:
    """The gate after the attempts-th failure: the delivery backoff
    (base * 2**(attempts-1), capped) -- comms' one transient policy."""
    seconds = min(
        settings.notification_retry_backoff_base_seconds
        * 2 ** (attempts - 1),
        settings.notification_retry_backoff_max_seconds,
    )
    return timedelta(seconds=seconds)


async def _record_pipeline_failure(
    notif_id: UUID, step: PipelineStep, exc: BaseException,
) -> None:
    """Write one pipeline failure onto the job, in its own transaction
    (the attempt's own was rolled back). Below the ceiling: gate the
    job. At the ceiling: close it (see the module header).

    KNOWN CEILING (acknowledged by design -- T12, D1 gate):
      1. Mechanics: when deliver has already handed letters to the
         channel and the attempt then raises (the flush, the rollup,
         the commit), the rollback erases the SENT marks; the next
         attempt sends those letters again. Bounded by
         NOTIFICATION_MAX_PIPELINE_ATTEMPTS -- before T12 it repeated
         on every tick, forever.
      2. Status: acknowledged by design.
      3. Backlog ref: none -- no task is opened; this marker is the
         record until the trigger below is observed.
      4. Promotion trigger (observable): a job with pipeline_step
         deliver / rollup / commit whose deliveries show sent_at, or
         a recipient reporting the same letter twice.
      5. Agreed fix: none agreed yet -- the shape is decided when the
         trigger fires.
      6. Rejected: committing each delivery's outcome right after its
         channel call (splits the attempt across transactions and
         breaks the one-lock-per-attempt rule the expiry and the
         cancellation rely on -- app/engine/service.py, IN FLIGHT);
         dropping the retry of the pipeline altogether (a transient
         database hiccup would then close jobs that would have gone
         out).
    """
    factory = get_session_factory()
    async with factory() as session:
        try:
            notification = (
                await session.execute(
                    select(Notification)
                    .where(Notification.id == notif_id)
                    .with_for_update(skip_locked=True)
                )
            ).scalar_one_or_none()
            if notification is None or notification.status not in (
                NotificationStatus.PENDING,
                NotificationStatus.PROCESSING,
            ):
                # Taken by another worker, or closed meanwhile (expiry,
                # cancellation): nothing to record on.
                return
            notification.pipeline_attempts += 1
            notification.pipeline_step = step.value
            notification.pipeline_error = pipeline_error_of(exc)
            attempts = notification.pipeline_attempts
            ceiling = settings.notification_max_pipeline_attempts
            if attempts < ceiling:
                notification.pipeline_retry_at = (
                    datetime.now(UTC) + _pipeline_backoff(attempts)
                )
                await session.commit()
                logger.warning(
                    "notification_pipeline_retry",
                    notification_id=str(notif_id),
                    step=step.value,
                    attempts=attempts,
                    ceiling=ceiling,
                )
                return
            notification.pipeline_retry_at = None
            closed = (
                await session.execute(
                    update(NotificationDelivery)
                    .where(
                        NotificationDelivery.notification_id == notif_id,
                        NotificationDelivery.status == DeliveryStatus.PENDING,
                    )
                    .values(
                        status=DeliveryStatus.FAILED,
                        failure_class=FailureClass.PIPELINE,
                        next_retry_at=None,
                        wait_reason=None,
                    )
                )
            ).rowcount  # type: ignore[attr-defined]
            if notification.status == NotificationStatus.PROCESSING:
                await rollup_notification(session, notification)
            else:
                # PENDING: resolve never committed, there are no
                # deliveries to fold.
                notification.status = NotificationStatus.FAILED
            await session.commit()
            logger.error(
                "notification_pipeline_failed",
                notification_id=str(notif_id),
                step=step.value,
                attempts=attempts,
                deliveries_closed=closed,
                status=notification.status,
            )
        except Exception as record_exc:
            await session.rollback()
            logger.error(
                "notification_pipeline_record_error",
                notification_id=str(notif_id),
                exception=sanitized_traceback(record_exc),
            )


async def cleanup_expired_notifications() -> int:
    """Delete expired notifications that have been fully delivered.

    Removes notifications where:
      - expiry_at < now()
      - status is an outcome other than failed (a failure is kept for
        inspection until general retention takes it)

    Suppressed, no-recipients and cancelled are outcomes like sent: an
    expired one is as dead as an expired sent one. General retention of terminal
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
                        NotificationStatus.CANCELLED,
                        NotificationStatus.SUPPRESSED,
                        NotificationStatus.NO_RECIPIENTS,
                    ]),
                )
            )
            result = await session.execute(stmt)
            deleted: int = result.rowcount  # type: ignore[attr-defined]

            if deleted:
                logger.info("notifications_cleaned_up", count=deleted)

            await session.commit()
            return deleted

        except Exception as exc:
            await session.rollback()
            logger.error(
                "notification_cleanup_error",
                exception=sanitized_traceback(exc),
            )
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

    DISABLED (fix I): settings.notification_retention_days == 0 means
    retention is OFF (a negative value refuses startup -- D1 / R3) --
    return 0 without touching anything ("delete everything" must never
    fall out of the cutoff arithmetic; the worker startup log names the
    disabled state loudly). The guard lives HERE, not only behind the
    worker's cadence gate, because tests call this function directly.

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
    intake_deleted = 0

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
            # The records of requests that were NOT accepted follow the
            # same horizon (F1.2): one statement -- the table holds only
            # refusals, it is small by nature.
            intake_deleted = await delete_intake_outcomes_before(
                session, cutoff=cutoff,
            )
            await session.commit()
        except Exception as exc:
            await session.rollback()
            logger.error(
                "retention_pass_error",
                deleted=total,
                exception=sanitized_traceback(exc),
            )
            return total

    # Logged EVERY pass, empty ones included: a slow empty scan is
    # exactly the BL-3 trigger signal.
    logger.info(
        "retention_pass",
        deleted=total,
        intake_outcomes_deleted=intake_deleted,
        duration_ms=round((time.monotonic() - started) * 1000, 1),
        retention_days=retention_days,
    )
    return total
