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
#   app/worker.py: one loop per lane of the process calls process_lane;
#   the maintenance loop calls app/engine/worker.py
#   (run_engine_maintenance: the expiry sweep + the expired cleanup)
#   and the retention pass on its own cadence. The worker LOOPS and all
#   maintenance scheduling live in app/worker.py (the neutral layer
#   above engine and messaging), which keeps `engine` messaging-free
#   (Phase 4b, edit 3). process_pending_notifications is the same steps
#   in one call.
#
# LANES (P4-1, spec §8): a job carries the lane of its type
# (notifications.lane, snapshotted at intake). Each lane is selected by
# its own query with its own batch size, its jobs are attempted in its
# own places and deliver in its own slots (app/engine/service.py
# LANE_JOBS, LANE_CONCURRENCY) -- a backlog or a slow channel in one
# lane takes nothing from another.
#
# JOBS OF A LANE SIDE BY SIDE (P5-L): a lane attempts up to its places
# of jobs at once (LaneJobs), so a long call -- a model writing for a
# minute -- holds one place, not the lane. In the worker a pass does not
# wait for the attempts it starts: they run in the lane loop's task
# group (app/worker.py _lane_loop), and the next pass selects beside
# them, dropping the ones still in work.
#
# SESSION MANAGEMENT:
#   Each notification is processed in its own session/transaction.
#   Failure on one notification does not roll back others.
#
# CONCURRENCY:
#   SELECT ... FOR NO KEY UPDATE SKIP LOCKED prevents double-processing
#   when multiple worker instances -- or several lane loops of one
#   instance -- run concurrently (the lock rule is written at the lock
#   itself, in _attempt_job). A job is in exactly one lane, so two lane
#   loops of one process never select the same row; a lane does not
#   attempt again a job it has in work (LaneJobs.in_work). The attempts
#   of one lane run side by side, each on its own job row, as the
#   attempts of two lanes or of two worker processes already did: the
#   pair they form is the one those formed.
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
#   Expires both PENDING and PROCESSING notifications past expiry_at,
#   of the process's lanes only (expire_overdue_notifications); the lane
#   selection skips a job past expiry_at that the sweep has not reached.
#
# A PIPELINE DEFECT IS AN OUTCOME (T12):
#   An exception of comms' own in an attempt (channel answers never get
#   here -- _deliver_single turns every one of them into an outcome)
#   rolls the attempt back, and then, in a SEPARATE transaction, the
#   failure is written onto the job: attempts + 1, the step, the
#   exception's class and place (never its text). The count is of the
#   failures IN A ROW (REV-2): an attempt that commits ends the run --
#   count, step and error back to 0 / null in its own transaction
#   (_attempt_job); every failure stays in the journal. Below the
#   ceiling (settings.notification_max_pipeline_attempts) the job waits behind
#   a gate, pipeline_retry_at, and the selection skips it until then:
#   oldest-first stays the order, the gate is what keeps a failing job
#   from starving the healthy ones and keeps the healthy ones from
#   starving it. At the ceiling the job is closed (_close_at_ceiling):
#   a job whose deliveries were never committed is resolved first, in
#   the closing transaction; then waiting deliveries are FAILED with
#   class `pipeline` (SENT, where the channel already accepted the
#   letter) and the job is folded. FAILED outright only when that
#   resolve itself fails. The attempt takes the row only while it is
#   still ready by the selection's own condition (ready_now, in the
#   lock's WHERE): a gate another worker set between the selection and
#   the lock is kept, never wiped (WK-2).
#
# A TRANSIENT FAILURE OF THE DATABASE (WK-2, the consumer's rule, spec
#   16.3): while the database does not answer (app/core/database.py
#   database_answers), nothing is written -- the job rests in the
#   process, uncounted; while it answers, the failure is counted like
#   any other, the ceiling included: the budget is
#   NOTIFICATION_MAX_PIPELINE_ATTEMPTS failures in a row, no other.
#
# A LETTER THE CHANNEL TOOK IS NOT SENT TWICE (the T12 resend, closed
# in P2-1): every channel answer is journaled in its own transaction
# the moment it comes (app/engine/journal.py), so a rollback of the
# attempt erases the SENT mark but not the record that the channel
# accepted the letter. The next attempt closes such a delivery SENT
# without calling the channel; expiry, cancellation, forgetting and the
# ceiling close it SENT too. What remains is the window between the
# channel's answer and that record's commit -- the KNOWN CEILING on
# journal.record_channel_answer.
# =============================================================================

import asyncio
import enum
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import structlog
from sqlalchemy import ColumnElement, Select, and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import (
    database_answers,
    get_session_factory,
    is_transient_db_error,
)
from app.core.lanes import Lane
from app.core.sanitize import pipeline_error_of, sanitized_traceback
from app.engine import journal
from app.engine.constants import (
    DeliveryStatus,
    FailureClass,
    JournalStep,
    NotificationStatus,
    PipelineStep,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.service import (
    LANE_CONCURRENCY,
    LANE_JOBS,
    close_notifications,
    close_waiting_deliveries,
    delete_expired_notifications_batch,
    delete_intake_outcomes_before,
    delete_terminal_notifications_batch,
    deliver_notification,
    first_waiting_delivery,
    forget_llm_letter,
    purge_llm_answers,
    resolve_notification,
    rollup_notification,
)

logger = structlog.get_logger()


async def process_pending_notifications() -> int:
    """One pass over this process's lanes: the expiry sweep, then one
    pass of every lane (process_lane), the lanes concurrently.

    The worker does not call this: it runs the same steps as separate
    loops -- one per lane and one for maintenance (app/worker.py) -- so
    that no lane waits for another's pass. This is those steps in one
    call, for a caller that wants everything due done once.

    A lane that fails does not stop the others: every lane's pass runs
    to its end, each failure is logged with its lane, and then the
    first one is raised.

    Returns:
        Number of notifications processed, over the lanes.
    """
    lanes = settings.worker_lanes
    await expire_overdue_notifications(lanes)
    results = await asyncio.gather(
        *(process_lane(lane) for lane in lanes), return_exceptions=True,
    )
    processed = 0
    failure: BaseException | None = None
    for lane, result in zip(lanes, results, strict=True):
        if isinstance(result, BaseException):
            logger.error(
                "lane_pass_error",
                lane=lane.value,
                exception=sanitized_traceback(result),
            )
            failure = failure or result
        else:
            processed += result
    if failure is not None:
        raise failure
    return processed


async def expire_overdue_notifications(lanes: Sequence[Lane]) -> int:
    """Expire the overdue jobs of `lanes` (own session). Returns the
    number of jobs closed.

    Through the deliveries (F1.3): the waiting ones expire, the job is
    folded -- a job half of which went out is PARTIAL_SENT, and its
    waiting deliveries no longer stay PENDING forever.

    ONLY THE LANES OF THIS PROCESS (P4-1): close_notifications locks
    FOR UPDATE and WAITS for an attempt in flight. A process that takes
    only `critical` must not wait for a `bulk` attempt of another
    process; the process that takes `bulk` expires its jobs. A lane no
    process takes is neither delivered nor expired -- the deploy
    contract is that the processes' lanes cover all four
    (deploy/INTEGRATION.md).

    IN PORTIONS (PERF-1): at most _EXPIRY_PORTION jobs per transaction,
    committed one portion after the other, in id order, until a portion
    comes back short -- where THIS pass ends, not a proof that nothing
    is left: under READ COMMITTED a row the portion's lock re-checks and
    drops (closed or changed meanwhile) shortens it, and the tail goes
    on the next pass. A backlog after a worker's downtime no longer sits
    in one transaction -- its row locks, its objects, the attempts of
    these lanes queued behind it. Each portion WAITS for an attempt that
    holds one of its jobs (no SKIP LOCKED, the rule above): the job is
    closed after the attempt, in that portion. A failure rolls back the
    portion it happened in; the portions before stay committed, and the
    next pass closes the rest -- every job's outcome is the one it would
    have had in one transaction.

    The worker runs this in its maintenance loop, beside the lane loops:
    the sweep waits for an attempt of this process too, while the
    attempt's channel answers are journaled in their own sessions -- a
    KEY SHARE on the job row that does not queue behind the waiting
    FOR UPDATE (tests/test_lanes.py, the race of attempt and expiry).
    """
    factory = get_session_factory()
    now = datetime.now(UTC)
    overdue = and_(
        Notification.lane.in_([lane.value for lane in lanes]),
        Notification.expiry_at.isnot(None),
        Notification.expiry_at < now,
    )
    expired_count = 0
    async with factory() as session:
        try:
            while True:
                portion = await close_notifications(
                    session, overdue, NotificationStatus.EXPIRED,
                    limit=_EXPIRY_PORTION,
                )
                await session.commit()
                expired_count += portion
                if portion < _EXPIRY_PORTION:
                    break
        except Exception as exc:
            await session.rollback()
            logger.error(
                "notification_expire_error",
                exception=sanitized_traceback(exc),
                committed=expired_count,
            )
            return expired_count
    if expired_count:
        logger.info("notifications_expired", count=expired_count)
    return expired_count


def not_expired(now: datetime) -> ColumnElement[bool]:
    """A job not yet past its expiry. P4-1: the expiry sweep runs in its
    own loop, not just before a lane's selection; a job past its expiry
    that the sweep has not reached yet is not attempted -- and not
    waiting either: the sweep will close it, not a delivery."""
    return or_(Notification.expiry_at.is_(None), Notification.expiry_at > now)


def ready_now(now: datetime) -> ColumnElement[bool]:
    """THE condition of a job a lane's pass may attempt now -- one place,
    read by the pass (process_lane), by the attempt's lock (_attempt_job,
    WK-2: a job no longer ready when its row is taken is not attempted)
    and by the lane depth (app/engine/depth.py, H8), so the depth's
    "ready" is exactly what the next pass would select, its batch limit
    aside: pending, or
    processing with a delivery due; scheduled; past its pipeline gate
    (T12); not expired; not stuck at the ceiling (not_stuck, TX-3)."""
    # The job's first due waiting delivery, correlated to the job row:
    # a scalar subquery of first_waiting_delivery, never an EXISTS -- an
    # EXISTS under an OR is planned as a hashed subplan that reads every
    # waiting delivery of every job (H14; the form's reasons are on
    # first_waiting_delivery).
    ready_delivery = first_waiting_delivery(
        Notification.id,
        or_(
            NotificationDelivery.next_retry_at.is_(None),
            NotificationDelivery.next_retry_at <= now,
        ),
    ).scalar_subquery().is_not(None)
    return and_(
        or_(
            Notification.status == NotificationStatus.PENDING,
            and_(
                Notification.status == NotificationStatus.PROCESSING,
                ready_delivery,
            ),
        ),
        Notification.scheduled_at <= now,
        or_(
            Notification.pipeline_retry_at.is_(None),
            Notification.pipeline_retry_at <= now,
        ),
        not_expired(now),
        not_stuck(),
    )


def not_stuck() -> ColumnElement[bool]:
    """A job below the ceiling of attempts (TX-3). One that reached it
    and is not closed is STUCK: comms could not close it
    (_record_stuck), and no lane takes it again -- the state is the
    database's, so a restart or a second worker process does not take
    it either. A ceiling raised in the settings makes it selectable
    again, on purpose (deploy/INTEGRATION.md)."""
    return Notification.pipeline_attempts < settings.notification_max_pipeline_attempts


class LaneJobs:
    """The jobs one lane has in work in this process (P5-L): its places
    (app/engine/service.py LANE_JOBS), its slots (LANE_CONCURRENCY), the
    ids of its jobs in work, and the task group their attempts run in.

    Whoever owns `attempts` owns this: the worker's lane loop keeps one
    for the process's life (app/worker.py _lane_loop), so an attempt
    outlives the pass that started it and is cancelled with the loop;
    process_lane given none builds its own and waits for its attempts.
    """

    def __init__(self, lane: Lane, attempts: asyncio.TaskGroup) -> None:
        self.lane = lane
        self.attempts = attempts
        self.places = asyncio.Semaphore(LANE_JOBS[lane])
        self.slots = asyncio.Semaphore(LANE_CONCURRENCY[lane])
        self.in_work: set[UUID] = set()
        # THE REST IN THE PROCESS (TX-3): a job whose failure was not
        # written onto it -- the database silent on the step or on the
        # record (WK-2) -- is not selected again before its backoff, so
        # the lane does not spin on it every poll. job -> monotonic time
        # it may be taken.
        # Kept only in memory: a restart forgets it, and the job is
        # taken once more -- harmless. Purged at every pass (expired
        # entries go), and a job that ends gives its entry back, so the
        # map holds only the lane's recently failed jobs.
        self.resting: dict[UUID, float] = {}
        # Failures of a job that could not be written for a reason no
        # rest cures, counted in this process (TX-3, unrecordable).
        self.unrecorded: dict[UUID, int] = {}

    def unrecordable(self, notif_id: UUID) -> None:
        """A failure that could not be written for a reason no rest will
        cure (TX-3): not a quiet loop. The job rests for the longest
        backoff; at the ceiling of attempts it is PARKED in this process
        -- not taken again until a restart -- and said loudly. A flag on
        the job cannot be written: every write to it just failed."""
        count = self.unrecorded.get(notif_id, 0) + 1
        self.unrecorded[notif_id] = count
        ceiling = settings.notification_max_pipeline_attempts
        logger.error(
            "notification_pipeline_unrecordable",
            notification_id=str(notif_id),
            count=count,
            ceiling=ceiling,
        )
        if count >= ceiling:
            self.resting[notif_id] = float("inf")
            logger.error(
                "notification_pipeline_parked",
                notification_id=str(notif_id),
                lane=self.lane.value,
            )
            return
        self.resting[notif_id] = time.monotonic() + float(
            settings.notification_max_backoff_seconds
        )

    def rest(self, notif_id: UUID) -> None:
        """Keep `notif_id` out of the lane's selection for the backoff."""
        self.resting[notif_id] = time.monotonic() + float(
            settings.notification_retry_backoff_base_seconds
        )

    def purge_rest(self) -> None:
        """Drop every rest that has run out."""
        now = time.monotonic()
        for notif_id, until in list(self.resting.items()):
            if until <= now:
                del self.resting[notif_id]


@dataclass
class _PassTally:
    """What one pass started, counted as its attempts end; the last one
    to end logs the pass."""

    lane: Lane
    total: int
    started: float
    processed: int = 0
    ended: int = 0

    def end(self, processed: bool) -> None:
        self.processed += processed
        self.ended += 1
        if self.ended < self.total:
            return
        # Every pass with work logs its lane and duration -- from its
        # selection to the end of its last attempt: a pass far longer
        # than the poll interval is a lane whose channel is slow --
        # visible per lane, and no longer anyone else's wait.
        logger.info(
            "notifications_processed",
            lane=self.lane.value,
            total=self.total,
            processed=self.processed,
            duration_ms=round((time.monotonic() - self.started) * 1000, 1),
        )


async def process_lane(
    lane: Lane,
    on_more: Callable[[], None] | None = None,
    jobs: LaneJobs | None = None,
) -> int:
    """One pass of one lane: select its due jobs, at most the lane's
    batch size, and attempt them, up to the lane's places at once
    (LaneJobs), each in its own session/transaction. Pipeline per
    notification: resolve -> deliver -> rollup.

    THE PLACES (P5-L): the selection itself takes a place first -- its
    session is one of the connections a place stands for -- and that
    place goes to the first job selected; each next job waits for a
    place to come free. An attempt keeps its place to its end, so a long
    call holds one place and the lane's other jobs go on in the rest.
    The pass selects its batch PLUS the jobs the lane has in work as it
    starts the selection, and drops those: it takes a batch of jobs not
    in work, and never attempts a job twice at once. The selection's statement is the
    same; only its LIMIT grows, by at most the places less one.

    TWO CALLERS, ONE PASS. With `jobs` -- the worker's lane loop
    (app/worker.py _lane_loop) -- the pass does NOT wait for its
    attempts: it returns once the last job it selected has started,
    and the attempts run on in the loop's task group; the next pass
    selects beside them. Without -- process_pending_notifications, a
    caller that wants everything due done once -- the pass builds its
    own places and waits for every attempt it started.

    ONE PORTION PER JOB PER ATTEMPT (H7): an attempt delivers at most
    one round of the lane's slots (app/engine/service.py
    delivery_portion) and commits, so it never holds the job's row, a
    connection and the change feed's xmin for a whole audience. A job
    with more due goes on in a NEXT pass, selected afresh -- expiry,
    cancellation and forgetting get the row between two portions, and a
    job accepted meanwhile is selected beside it. When an attempt ends
    with its portion full, `on_more` is called -- after the job has left
    the lane's work and given its place back, so the pass it starts
    selects it again: the worker passes the lane's wake (app/worker.py),
    so the next pass follows at once instead of after the interval.

    THE LANE'S BUDGET (P4-1, spec §8.2): the selection names the lane,
    so another lane's backlog never takes its places; the attempts run
    in the lane's places and their deliveries in the lane's slots
    (app/engine/service.py LANE_JOBS, LANE_CONCURRENCY), so one lane
    never holds more than its own. Isolation, not priority: no lane is
    picked before another -- each has its own loop.

    Returns:
        With `jobs`: the attempts this pass started (the loop's backoff
        reads it; the attempts' outcome is in the pass's log line).
        Without: the notifications processed.
    """
    if jobs is not None:
        tally = await _pass(lane, jobs, on_more)
        return 0 if tally is None else tally.total
    async with asyncio.TaskGroup() as attempts:
        tally = await _pass(lane, LaneJobs(lane, attempts), on_more)
    return 0 if tally is None else tally.processed


async def _pass(
    lane: Lane, jobs: LaneJobs, on_more: Callable[[], None] | None,
) -> _PassTally | None:
    """Select and start; None when there was nothing to start. See
    process_lane."""
    factory = get_session_factory()
    started = time.monotonic()
    batch = settings.lane_batch_size(lane)

    # -- Step 1: Collect IDs of the lane's notifications to process --
    # Review 1.2: PENDING rows are always ready (not yet resolved).
    # PROCESSING rows are picked only when at least one delivery is
    # actually attemptable (pending + retry gate open) -- otherwise a
    # gated notification would be locked and no-op'ed every pass, and
    # the no-op "processed" count would keep the lane's loop from
    # backing off for the whole backoff window.
    await jobs.places.acquire()
    try:
        # The jobs in work as the selection sees them, taken before it:
        # one that ends while the selection runs is still in its snapshot,
        # due, and has left the live set by the time the rows are back --
        # dropped by THIS set, it is not counted as one of the batch's new
        # (a SENT job attempted again, a new one left behind). Only this
        # pass starts jobs of the lane, so no job enters it meanwhile.
        # A job at rest in the process (LaneJobs.rest) is left out the
        # same way, until its rest runs out (TX-3).
        jobs.purge_rest()
        in_work = set(jobs.in_work) | set(jobs.resting)
        now = datetime.now(UTC)
        async with factory() as session:
            stmt = (
                select(Notification.id)
                .where(Notification.lane == lane.value, ready_now(now))
                # Oldest due first within the lane. Priority left the
                # ordering with the column (F1.4); isolation is the lane's.
                .order_by(Notification.scheduled_at)
                # Review 1.1: cap the batch -- the lane's own (P4-1); the
                # tail is picked up on the lane's next pass. Larger by
                # the jobs in work, which are dropped below (P5-L).
                .limit(batch + len(in_work))
            )
            result = await session.execute(stmt)
            selected = [row[0] for row in result.all()]
    except BaseException:
        jobs.places.release()
        raise
    fresh = [nid for nid in selected if nid not in in_work][:batch]
    if not fresh:
        jobs.places.release()
        return None

    # -- Step 2: Attempt each in its own task, session and place --
    tally = _PassTally(lane=lane, total=len(fresh), started=started)
    for n, notif_id in enumerate(fresh):
        if n:
            await jobs.places.acquire()
        jobs.in_work.add(notif_id)
        jobs.attempts.create_task(_attempt(notif_id, jobs, tally, on_more))
    return tally


async def _attempt(
    notif_id: UUID,
    jobs: LaneJobs,
    tally: _PassTally,
    on_more: Callable[[], None] | None,
) -> None:
    """One job's attempt in its place: the place and the job's mark of
    work are given back when it ends, however it ends; then the pass is
    told, and a full portion wakes the lane (process_lane)."""
    unrecorded: _Recorded | None = None

    def rest(kind: _Recorded) -> None:
        nonlocal unrecorded
        unrecorded = kind

    processed = portion_full = False
    try:
        processed, portion_full = await _attempt_job(
            notif_id, jobs.slots, rest=rest,
        )
    except Exception as exc:
        # THE LAST LINE (TX-3): nothing an attempt raises reaches the
        # lane's task group -- an Exception there cancels the group,
        # ends the lane's loop and takes the worker process down with
        # the other loops (app/worker.py run_worker_loop). The job is
        # left as it was and rests; the log gets the class and the place.
        logger.error(
            "notification_attempt_error",
            notification_id=str(notif_id),
            error=pipeline_error_of(exc),
        )
        unrecorded = _Recorded.NO
    finally:
        jobs.in_work.discard(notif_id)
        jobs.places.release()
    if unrecorded is _Recorded.TRANSIENT:
        jobs.rest(notif_id)
    elif unrecorded is _Recorded.NO:
        jobs.unrecordable(notif_id)
    else:
        jobs.resting.pop(notif_id, None)
        jobs.unrecorded.pop(notif_id, None)
    tally.end(processed)
    if portion_full and on_more is not None:
        on_more()


async def _attempt_job(
    notif_id: UUID,
    slots: asyncio.Semaphore,
    rest: Callable[["_Recorded"], None] | None = None,
) -> tuple[bool, bool]:
    """Lock, resolve, deliver one portion in the lane's `slots`, roll up
    and commit one job. Returns (processed, portion full).

    A FAILURE OF THE ATTEMPT is recorded by _record_failure: a TRANSIENT
    failure of the database (app/core/database.py is_transient_db_error)
    while the database does not answer writes nothing and the job rests
    in the process; every other failure -- a transient one while the
    database answers included (WK-2) -- spends an attempt, up to the
    ceiling (_record_pipeline_failure). The rule reads only what
    reaches here: a provider's error -- aiohttp, httpx, the body's download -- is
    caught inside _deliver_single and becomes the delivery's outcome,
    never this step's exception (the TX-3 report). The rollback is
    guarded: on a connection that died while the session held a
    transaction it raises too. When nothing could be recorded, `rest`
    is told why (_Recorded): the lane rests or parks the job.
    """
    factory = get_session_factory()
    step = PipelineStep.LOCK
    async with factory() as session:
        try:
            # THE LOCK RULE. The attempt holds the job's row FOR NO KEY
            # UPDATE for its whole transaction (resolve -> deliver ->
            # rollup -> commit), skipping a row another worker holds.
            # NOT FOR UPDATE: every channel answer is journaled from a
            # SECOND session while this lock is held, and that
            # insert's foreign key takes FOR KEY SHARE on this row.
            # FOR UPDATE conflicts with KEY SHARE -- the journal
            # session would wait for the attempt and the attempt, in
            # its await, for the journal session; Postgres sees no
            # deadlock across the two connections and the worker
            # hangs. FOR NO KEY UPDATE does not conflict with KEY
            # SHARE and still conflicts with FOR UPDATE, which is
            # what expiry, cancellation and forgetting take
            # (app/engine/service.py, IN FLIGHT): they stay mutually
            # exclusive with the attempt.
            #
            # STILL READY (WK-2): the row is taken only if the job is
            # still ready by THE selection's condition (ready_now) -- one
            # rule in one place, not a second text of it here. Between
            # the selection and this lock another worker may have gated
            # the job (its failure recorded), closed it, or put it at the
            # ceiling; time may have expired it, and another attempt may
            # have put all its due deliveries behind a wait. Such a row
            # is not taken: nothing is written, its gate is not wiped.
            # The session is new, so no stale copy of the row is in its
            # identity map (no populate_existing needed). OF notifications:
            # the correlated subquery's deliveries are read, not locked.
            lock_result = await session.execute(
                _lock_of(notif_id, datetime.now(UTC)),
            )
            notification = lock_result.scalar_one_or_none()

            if notification is None:
                # Another worker holds it, or it is no longer ready.
                return False, False

            # A gate that let this attempt through has done its job: it
            # goes HERE, at the start, in the attempt's own transaction --
            # an attempt that fails rolls this back with the rest, and
            # its failure record sets the next gate. The failure record
            # (attempts, step, error) stays.
            notification.pipeline_retry_at = None

            # resolve (idempotent -- skips if deliveries exist)
            step = PipelineStep.RESOLVE
            await resolve_notification(session, notification)

            # deliver -- one portion, in the lane's slots
            step = PipelineStep.DELIVER
            portion_full = await deliver_notification(
                session, notification, slots,
            )

            # rollup
            step = PipelineStep.ROLLUP
            await rollup_notification(session, notification)

            # A SUCCESS ENDS THE RUN OF FAILURES (REV-2): the budget is
            # the failures IN A ROW. A job in portions takes thousands of
            # attempts, and three transient failures scattered over its
            # life must not close it at the ceiling. The record goes as
            # one -- count, step, error (ck_notifications_pipeline: 0
            # exactly with no step and no error); every failure stays in
            # the journal, its history. Written in this attempt's own
            # transaction, so only a commit that lands ends the run. A
            # failure of comms that is deterministic tears every attempt
            # that reaches it, and a torn attempt rolls its portion back:
            # the next takes the same due deliveries in id order
            # (deliver_notification), so it fails again, in a row, up to
            # the ceiling -- at worst after the other deliveries are done.
            if notification.pipeline_attempts > 0:
                notification.pipeline_attempts = 0
                notification.pipeline_step = None
                notification.pipeline_error = None

            step = PipelineStep.COMMIT
            await session.commit()
            return True, portion_full

        except Exception as exc:
            await _guarded_rollback(session, notif_id)
            logger.error(
                "notification_pipeline_error",
                notification_id=str(notif_id),
                step=step.value,
                exception=sanitized_traceback(exc),
            )
            recorded = await _record_failure(notif_id, step, exc)
            if recorded is not _Recorded.YES and rest is not None:
                rest(recorded)
            return False, False


def _lock_of(notif_id: UUID, now: datetime) -> Select[tuple[Notification]]:
    """The attempt's lock of its job (THE LOCK RULE in _attempt_job): the
    row FOR NO KEY UPDATE OF notifications SKIP LOCKED, only while the job
    is ready by ready_now at `now` (WK-2)."""
    return (
        select(Notification)
        .where(Notification.id == notif_id, ready_now(now))
        .with_for_update(skip_locked=True, key_share=True, of=Notification)
    )


class _Recorded(enum.Enum):
    """Whether a failure of an attempt was written onto its job."""

    YES = "yes"
    # Nothing written, on a TRANSIENT failure of the database while it
    # does not answer (WK-2) -- the step's or the record's own: the job
    # rests in the process for the backoff, uncounted, and is taken
    # again.
    TRANSIENT = "transient"
    # The record failed on anything else -- a constraint, a bug, or a
    # transient failure while the database answers: not a quiet loop,
    # counted in the process (_attempt: LaneJobs.unrecordable, parked
    # at the ceiling).
    NO = "no"


async def _record_failure(
    notif_id: UUID, step: PipelineStep, exc: BaseException,
) -> "_Recorded":
    """Record a failure of an attempt -- THE CONSUMER'S RULE (WK-2, spec
    16.3): a transient failure of the database while the database does
    not answer (database_answers) is waited for, uncounted, and nothing
    is written -- there is nothing to write it to; the lane rests the
    job (_Recorded.TRANSIENT). Every other failure, a transient one
    while the database answers included -- a deadlock that keeps coming
    back is the job's -- is counted (_record_pipeline_failure), up to
    the ceiling: the one budget."""
    if await _database_silent(exc):
        logger.warning(
            "notification_pipeline_database_silent",
            notification_id=str(notif_id),
            step=step.value,
            error=pipeline_error_of(exc),
        )
        return _Recorded.TRANSIENT
    return await _record_pipeline_failure(notif_id, step, exc)


async def _database_silent(exc: BaseException) -> bool:
    """A transient failure of the database, and the database does not
    answer: the one case that is waited for, uncounted (WK-2)."""
    return is_transient_db_error(exc) and not await database_answers()


async def _kind_of(record_exc: BaseException) -> "_Recorded":
    """Why a record could not be written, by the same rule (WK-2): the
    database silent -> TRANSIENT, a rest uncounted; anything else -- a
    transient failure while it answers included -> NO, counted in the
    process up to the ceiling (LaneJobs.unrecordable)."""
    if await _database_silent(record_exc):
        return _Recorded.TRANSIENT
    return _Recorded.NO


async def _guarded_rollback(
    session: AsyncSession, notif_id: UUID | None,
) -> None:
    """Roll back, and survive a rollback that cannot be done (TX-3):
    measured on a real session, a connection that died while the session
    held a transaction raises on ROLLBACK too (one that died under a
    failed statement does not). The transaction is gone either way."""
    try:
        await session.rollback()
    except Exception as exc:
        logger.error(
            "notification_rollback_error",
            notification_id=str(notif_id) if notif_id else None,
            error=pipeline_error_of(exc),
        )


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
) -> "_Recorded":
    """Write one pipeline failure onto the job, in its own transaction
    (the attempt's own was rolled back). Below the ceiling: gate the
    job. At the ceiling: close it (see the module header).

    The failure is a journal row of the job under the step that tore,
    with the exception's class and place; below the ceiling it carries
    the gate. The rollback has already taken the attempt's own rows
    with it; the channel's answers, journaled in their own
    transactions, stay -- so a letter the channel accepted before the
    attempt tore is closed SENT here at the ceiling, and SENT without
    a second send by the next attempt below it.

    Returns whether the failure was written (_Recorded); its own
    rollback is guarded (TX-3), so a database down for the record too is
    an answer, not an exception into the lane's task group.
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
                return _Recorded.YES
            notification.pipeline_attempts += 1
            notification.pipeline_step = step.value
            notification.pipeline_error = pipeline_error_of(exc)
            attempts = notification.pipeline_attempts
            ceiling = settings.notification_max_pipeline_attempts
            torn = JournalStep(step.value)
            if attempts < ceiling:
                notification.pipeline_retry_at = (
                    datetime.now(UTC) + _pipeline_backoff(attempts)
                )
                journal.record_job(
                    session, notification, torn,
                    error=notification.pipeline_error,
                    wait_until=notification.pipeline_retry_at,
                )
                await session.commit()
                logger.warning(
                    "notification_pipeline_retry",
                    notification_id=str(notif_id),
                    step=step.value,
                    attempts=attempts,
                    ceiling=ceiling,
                )
                return _Recorded.YES
            notification.pipeline_retry_at = None
            # The last failure itself, under the step that tore, then
            # the closing rows under the ceiling.
            journal.record_job(
                session, notification, torn,
                error=notification.pipeline_error,
            )
            try:
                closed = await _close_at_ceiling(session, notification)
                await session.commit()
            except Exception as close_exc:
                # THE CLOSE ITSELF FAILED (TX-3): it goes through the same
                # fold the attempts failed in, and its failure took the
                # whole record back with it -- the count never reached the
                # ceiling, the job was never closed and was taken again
                # forever. By the consumer's rule (WK-2): the database
                # silent -> nothing written, the job rests and comes back
                # to the ceiling once it answers; anything else, a
                # transient failure while it answers included, leaves the
                # job STUCK, recorded so (_record_stuck) and no longer
                # selected. Not a retry of the close on the next pass: the
                # rollback put the count back below the ceiling, so every
                # attempt would count up to it and roll back again --
                # TX-3's loop.
                await _guarded_rollback(session, notif_id)
                if await _database_silent(close_exc):
                    return _Recorded.TRANSIENT
                return await _record_stuck(notif_id, close_exc)
            logger.error(
                "notification_pipeline_failed",
                notification_id=str(notif_id),
                step=step.value,
                attempts=attempts,
                deliveries_closed=closed,
                status=notification.status,
            )
            return _Recorded.YES
        except Exception as record_exc:
            await _guarded_rollback(session, notif_id)
            logger.error(
                "notification_pipeline_record_error",
                notification_id=str(notif_id),
                exception=sanitized_traceback(record_exc),
            )
            return await _kind_of(record_exc)


async def _record_stuck(notif_id: UUID, exc: BaseException) -> "_Recorded":
    """A job comms could not close at the ceiling (TX-3): its count set
    to the ceiling, its last failure the close's (class and place), no
    gate; a journal row under the ceiling. The lane's
    selection leaves a job at the ceiling that is not closed
    (ready_now, not_stuck) -- so it is said once, here, and not taken
    again; the job's answer shows it (attempts at the ceiling, a status
    not closed). A defect of comms, not of the product."""
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
                return _Recorded.YES
            error = pipeline_error_of(exc)
            notification.pipeline_attempts = settings.notification_max_pipeline_attempts
            notification.pipeline_step = PipelineStep.ROLLUP.value
            notification.pipeline_error = error
            notification.pipeline_retry_at = None
            journal.record_job(session, notification, JournalStep.CEILING, error=error)
            await session.commit()
            logger.error(
                "notification_pipeline_stuck",
                notification_id=str(notif_id),
                error=error,
                attempts=notification.pipeline_attempts,
            )
            return _Recorded.YES
        except Exception as record_exc:
            await _guarded_rollback(session, notif_id)
            logger.error(
                "notification_pipeline_record_error",
                notification_id=str(notif_id),
                error=pipeline_error_of(record_exc),
            )
            return await _kind_of(record_exc)


async def _close_at_ceiling(
    session: AsyncSession, notification: Notification,
) -> int:
    """Close a job whose pipeline exhausted its attempts. Returns the
    number of deliveries closed.

    The job's status is never set here except in the one branch below
    where there is nothing to fold: every other outcome is THE FOLD's
    (app/engine/service.py), the rule every closing path uses.

    A job still PENDING has no committed deliveries -- each attempt's
    resolve went with its rollback -- yet the channel may have accepted
    letters in those attempts (journal: CHANNEL rows, `accepted`). So
    the deliveries are materialized first, by the same resolve the
    attempt runs, in a SAVEPOINT of this closing transaction; then the
    waiting ones close like any job's at the ceiling: SENT where the
    channel accepted, FAILED / pipeline otherwise; then the fold. The
    channel is never called here.

    The savepoint is what keeps a resolve that fails again from taking
    the failure record with it: the record (pipeline_* and the job row)
    is flushed before the savepoint opens, and only the savepoint rolls
    back -- without it the closing transaction would roll back whole,
    the job would stay active with its count unchanged, and come back
    to the ceiling forever.

    KNOWN CEILING (acknowledged by design -- P2-1, ceiling without
    deliveries):
      1. Mechanics: a letter the channel accepted is not reflected in
         the job's status when the deliveries cannot carry it at the
         ceiling: (a) the resolve in the savepoint fails -- the job
         closes FAILED with no deliveries; (b) the resolve finds nobody
         or everyone muted -- the job is NO_RECIPIENTS / SUPPRESSED,
         the status resolve gives; (c) a recipient whose letter was
         accepted fell out of the audience since (deactivated, muted,
         left the group) -- no delivery is made for them and the fold
         does not count their letter. The journal keeps the accepted
         answer in all three: the path is true, the status is not.
      2. Status: acknowledged by design.
      3. Backlog ref: none -- the branches need a failing resolve or a
         changed audience between a torn attempt and the ceiling;
         nothing has been observed to file.
      4. Promotion trigger (observable): a CHANNEL row `accepted` in the
         journal of a job whose status does not reflect a send (FAILED,
         NO_RECIPIENTS or SUPPRESSED, or a fold without that triple's
         SENT delivery).
      5. Agreed fix: materialize, for every accepted triple without a
         delivery after the resolve, a SENT delivery from the journal
         row itself, and hand all of them to THE FOLD.
      6. Rejected: closing the job from the journal past the fold
         (setting SENT / PARTIAL_SENT directly) -- the fold is the one
         rule of a job's outcome, and a second one would decide
         differently on the same deliveries.
    """
    if notification.status == NotificationStatus.PENDING:
        resolved = await _resolve_at_ceiling(session, notification)
        if not resolved:
            # Nothing to fold: the resolve failed (branch (a) above).
            notification.status = NotificationStatus.FAILED
            journal.record_job(session, notification, JournalStep.CEILING)
            await forget_llm_letter(session, notification)
            return 0
    closed = await close_waiting_deliveries(
        session,
        notification,
        DeliveryStatus.FAILED,
        JournalStep.CEILING,
        failure_class=FailureClass.PIPELINE,
    )
    await rollup_notification(session, notification, JournalStep.CEILING)
    return closed


async def _resolve_at_ceiling(
    session: AsyncSession, notification: Notification,
) -> bool:
    """Resolve inside a savepoint; False when it fails (rolled back to
    the savepoint, the job re-read). See _close_at_ceiling."""
    try:
        async with session.begin_nested():
            await resolve_notification(session, notification)
    except Exception as exc:
        logger.error(
            "notification_ceiling_resolve_error",
            notification_id=str(notification.id),
            exception=sanitized_traceback(exc),
        )
        await session.refresh(notification)
        return False
    return True


async def cleanup_expired_notifications() -> int:
    """Delete expired notifications that have been fully delivered.

    Removes notifications where:
      - expiry_at < now()
      - status is an outcome other than failed (a failure is kept for
        inspection until general retention takes it)
      - no push is owed for them (H7): a job whose push_outbox row the
        relay has not published yet waits for it -- the cascade would
        take the push with the job (delete_expired_notifications_batch)

    Suppressed, no-recipients and cancelled are outcomes like sent: an
    expired one is as dead as an expired sent one. General retention of
    terminal notifications by age (NOTIFICATION_RETENTION_DAYS) is the
    other pass, cleanup_terminal_notifications -- this cleanup only
    covers rows that carry an explicit expiry_at.

    Deliveries are CASCADE-deleted automatically.

    IN BATCHES (H2), like cleanup_terminal_notifications: the service
    supplies one commit-free batch of _RETENTION_BATCH_SIZE jobs
    (delete_expired_notifications_batch); this loop commits each. One
    unbounded DELETE held the cascade's push_outbox rows for its whole
    transaction -- rows the relay (app/transport/push_relay.py) then
    skipped, tick after tick.

    AT MOST _EXPIRED_MAX_BATCHES_PER_PASS BATCHES (H5): this runs on
    EVERY pass of the worker's maintenance loop (app/worker.py), so one
    pass stops at the ceiling and leaves the rest of a backlog to the
    next pass -- which comes at the poll interval, since a pass that
    deleted something keeps the loop from backing off. A pass also
    stops on a short batch -- where the pass ends, not a proof that
    nothing more is expired: under READ COMMITTED a row another deleter
    took meanwhile shortens a batch, and the tail goes on the next
    pass. (Since P4-1 the
    loop runs beside the lane loops, not on the delivery path.)

    Returns:
        Number of notifications deleted in this pass, over its batches.
        A failing batch is rolled back and logged; the batches before
        it stay deleted and are counted.
    """
    factory = get_session_factory()
    now = datetime.now(UTC)
    total = 0

    async with factory() as session:
        try:
            for _ in range(_EXPIRED_MAX_BATCHES_PER_PASS):
                deleted = await delete_expired_notifications_batch(
                    session, now=now, limit=_RETENTION_BATCH_SIZE,
                )
                await session.commit()
                total += deleted
                if deleted < _RETENTION_BATCH_SIZE:
                    break

            if total:
                logger.info("notifications_cleaned_up", count=total)
            return total

        except Exception as exc:
            await session.rollback()
            logger.error(
                "notification_cleanup_error",
                deleted=total,
                exception=sanitized_traceback(exc),
            )
            return total


async def purge_expired_llm_answers() -> int:
    """Clear the model answers past their term (P5-1), in batches of
    _RETENTION_BATCH_SIZE, at most _EXPIRED_MAX_BATCHES_PER_PASS a pass
    -- the expiry cleanup's shape, on the same maintenance loop. A
    failing batch is rolled back and logged; the batches before it stay.

    Returns:
        Number of answers cleared in this pass.
    """
    factory = get_session_factory()
    now = datetime.now(UTC)
    total = 0
    async with factory() as session:
        try:
            for _ in range(_EXPIRED_MAX_BATCHES_PER_PASS):
                cleared = await purge_llm_answers(
                    session, now=now, limit=_RETENTION_BATCH_SIZE,
                )
                await session.commit()
                total += cleared
                if cleared < _RETENTION_BATCH_SIZE:
                    break
        except Exception as exc:
            await session.rollback()
            logger.error(
                "llm_answer_purge_error",
                cleared=total,
                exception=sanitized_traceback(exc),
            )
            return total
    if total:
        logger.info("llm_answers_purged", count=total)
    return total


# PERF-1: jobs the expiry sweep closes per transaction
# (expire_overdue_notifications). Jobs, not deliveries: one job's waiting
# deliveries close in one UPDATE whatever their number. Module-level so
# tests can shrink it to force a multi-portion sweep.
_EXPIRY_PORTION = 500

# Phase 3a item 5: rows deleted per retention batch. One batch = one
# transaction (bounded FK cascade); the drain loops commit between
# batches -- the retention pass below and, since H2, the expiry cleanup
# above. Module-level so tests can shrink it to force a multi-batch
# drain.
_RETENTION_BATCH_SIZE = 1000

# H5: batches one expiry-cleanup pass deletes at most -- a backlog of
# _EXPIRED_MAX_BATCHES_PER_PASS x _RETENTION_BATCH_SIZE jobs a pass,
# the rest on the next pass. Module-level so tests can shrink it.
_EXPIRED_MAX_BATCHES_PER_PASS = 10


async def cleanup_terminal_notifications() -> int:
    """Retention pass: drain terminal notifications older than
    NOTIFICATION_RETENTION_DAYS, in batches (Phase 3a item 5).

    Orchestration only (fix C): the service supplies ONE commit-free
    batch (delete_terminal_notifications_batch); this loop owns the
    commits -- one per batch, so a 90-day backlog never becomes one
    long transaction -- and drains until a batch comes back short:
    where the pass ends, not a proof that nothing is left -- under READ
    COMMITTED a row another deleter took meanwhile shortens a batch, and
    the tail goes on the next pass.

    DISABLED (fix I): settings.notification_retention_days == 0 means
    retention is OFF (a negative value refuses startup -- D1 / R3) --
    return 0 without touching anything ("delete everything" must never
    fall out of the cutoff arithmetic; the worker startup log names the
    disabled state loudly). The guard lives HERE, not only behind the
    worker's cadence gate, because tests call this function directly.

    Scheduling lives in app/worker.py (fix H): the pass runs on
    its own slow cadence (NOTIFICATION_RETENTION_INTERVAL_SECONDS),
    not on the worker tick. Every pass logs its duration -- that is
    the observable value of the BL-3 promotion trigger.

    KNOWN CEILING (acknowledged by design -- dispatch plan BL-3):
      1. Mechanics: the batch select filters on status and on
         greatest(created_at, scheduled_at) (REV-2), with an anti-join
         on push_outbox, and no index serves the filter -> each pass
         seq-scans the notifications table as it grows (REV-2 measured
         it at 200k rows: its report).
      2. Status: acknowledged by design.
      3. Backlog ref: BL-3 (dispatch plan §6a).
      4. Promotion trigger (observable, via the retention_pass log):
         a pass stably longer than ~1 second OR terminal rows on the
         order of a million.
      5. Agreed fix: ONE migration -- a partial index on the expression
         greatest(created_at, scheduled_at) with a predicate over the
         terminal statuses (an index on created_at alone no longer
         serves the filter since REV-2).
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
            await _guarded_rollback(session, None)
            logger.error(
                "retention_pass_error",
                deleted=total,
                exception=sanitized_traceback(exc),
            )
            # Raised on (TX-3): a failed retention must not look like a
            # done one -- the worker moves its cadence mark only after a
            # pass that succeeded (app/worker.py run_maintenance_pass).
            raise

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
