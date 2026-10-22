# =============================================================================
# COMMS Service -- Worker Entrypoint + Loops (neutral layer)
# =============================================================================
#
# Separate worker process: same Docker image as the API, different
# command (handoff item 1):
#
#   API:    uvicorn app.main:app --host 0.0.0.0 --port 8000
#   Worker: python -m app.worker
#
# This module is the NEUTRAL layer above both `engine` and `messaging`.
# It owns the eternal poll loops and ALL maintenance SCHEDULING:
#   - one delivery loop per lane of this process
#     (engine.processor.process_lane, P4-1), woken by intake through
#     one listener (engine.wake.listen_for_wakes, P4-2);
#   - one maintenance loop: the engine's expiry sweep and expired
#     cleanup (engine.worker.run_engine_maintenance), the close-notify
#     consumer, and -- each behind its own cadence gate -- the
#     notification retention pass (engine.processor) and the thread
#     auto-close pass (messaging.processor).
# Keeping the loops and both cadence gates here is what lets `engine`
# stay MESSAGING-FREE (Phase 4b, edit 3 / the DAG hygiene of 3b): the
# only place that imports both engine and messaging is this entrypoint.
#
# ONE LOOP PER LANE (P4-1, spec §8.2): a lane's pass waits for nothing
# but its own jobs. A bulk pass held by slow channel calls, or a
# maintenance pass waiting FOR UPDATE for an attempt in flight, delays
# only its own loop; every other lane goes on at its own interval. The
# lanes of the process come from COMMS_WORKER_LANES (all four when
# unset): one image, any set of lanes (spec §8.6).
#
# A LANE'S JOBS SIDE BY SIDE (P5-L): each lane loop owns a task group
# and the lane's places in it (_lane_loop, engine.processor.LaneJobs).
# A pass starts its jobs' attempts there and returns without waiting
# for them, so a job in a long call holds one of the lane's places, not
# its loop: the next pass -- woken by intake, by a full portion, or at
# the interval -- selects beside it.
#
# BACKOFF, per loop: a pass with no work -> that loop's interval doubles
# (up to NOTIFICATION_MAX_BACKOFF_SECONDS); work found -> back to
# NOTIFICATION_POLL_INTERVAL_SECONDS; a pass that raised -> the maximum.
# The settings are shared, the state is each loop's own.
#
# WAKE (P4-2, spec §8.4): the backoff decides how long an idle loop
# sleeps, not how soon it notices work. Intake wakes the loop of the
# job's lane (app/engine/wake.py): one listener per process sets the
# lane's event, the loop's sleep ends, the pass runs at once. The event
# is cleared just before each pass, so wakes that come during a pass --
# one or a thousand -- cost one more pass after it, never one each. A
# lost wake costs the interval, never the job. The maintenance loop has
# no producer of wakes: its event is never set and its sleep is its
# interval. A job that becomes due later is noticed by the poll (the
# KNOWN CEILING in app/engine/wake.py).
#
# SHUTDOWN: SIGTERM/SIGINT cancel the loops. Cancelling a lane loop
# cancels its task group, and with it every attempt the lane has in
# work: each is cancelled at its current await and rolled back with its
# session; what its channel calls already journaled stays
# (app/engine/journal.py). A loop -- or an attempt in its group -- that
# ends any other way ends the process: the task groups cancel the rest
# and the error propagates -- the container restarts with every lane,
# instead of running on with some of them.
# =============================================================================

import asyncio
import contextlib
import signal
import time
from collections.abc import Awaitable, Callable

import structlog

from app.core.config import settings
from app.core.database import ProcessRole, dispose_engine, set_process_role
from app.core.lanes import Lane
from app.core.logging import setup_logging
from app.core.sanitize import sanitized_traceback
from app.engine.formatters import close_formatters, init_formatters
from app.engine.processor import (
    LaneJobs,
    cleanup_terminal_notifications,
    process_lane,
)
from app.engine.service import LANE_CONCURRENCY, LANE_JOBS
from app.engine.wake import listen_for_wakes
from app.engine.worker import run_engine_maintenance
from app.messaging.processor import AutoClosePassError, auto_close_idle_threads
from app.notifier import consume_close_notifications
from app.profile.loader import install_profile_from_settings, lanes_mixing_llm

logger = structlog.get_logger()

# -- Maintenance cadence gates (PER-PROCESS, Phase 3a fix H). None ->
# never ran in this process: the first maintenance pass runs it at once
# (restart = bounded catch-up). Two workers -> two cheap idempotent
# scans per interval; NO DB coordination (see the KNOWN CEILING markers
# in the two processors -- do not "fix" this with a distributed lock).
# Each pass keeps its OWN gate: retention and auto-close are unrelated
# frequencies and must not share a timestamp. --
_last_retention_at: float | None = None
_last_auto_close_at: float | None = None


def reset_retention_gate() -> None:
    """Reset the retention cadence gate (tests)."""
    global _last_retention_at
    _last_retention_at = None


def reset_auto_close_gate() -> None:
    """Reset the auto-close cadence gate (tests)."""
    global _last_auto_close_at
    _last_auto_close_at = None


# Sessions the maintenance loop holds at once: its passes run one after
# another, each in one session at a time (the expiry sweep, the expired
# cleanup, the close-notify consumer, retention, auto-close). Counted
# with the lanes' against the pool (app/engine/service.py
# LANE_CONCURRENCY, LANE_JOBS; tests/test_lanes.py holds the sum).
MAINTENANCE_SESSIONS = 1


async def run_maintenance_pass() -> int:
    """One pass of the maintenance loop: the engine's expiry sweep and
    expired cleanup, the close-notify consumer, then the two slow
    passes -- each gated to its OWN cadence.

    It runs beside the lane loops, never on the delivery path: the
    expiry sweep waits FOR UPDATE for an attempt in flight, and that
    wait is this loop's alone. Each slow pass is skipped when disabled
    (days <= 0; the startup log names it) and otherwise gated to its
    interval. The passes commit per batch, so transactions stay short
    even when a pass drains a backlog.

    Returns:
        Jobs expired and deleted plus close notices emitted (drives the
        maintenance loop's backoff). The slow passes do not count: they
        keep their own cadence whatever the loop's interval.
    """
    global _last_retention_at, _last_auto_close_at

    done = await run_engine_maintenance()

    # Close-notify consumer (Phase 4c item 2): EVERY pass -- a partial
    # index over a normally-empty flag set makes it cheap and prompt,
    # unlike the two growing-table maintenance scans below (own gates).
    done += await consume_close_notifications()

    if settings.notification_retention_days > 0:
        now = time.monotonic()
        interval = settings.notification_retention_interval_seconds
        if (
            _last_retention_at is None
            or now - _last_retention_at >= interval
        ):
            _last_retention_at = now
            await cleanup_terminal_notifications()

    if settings.thread_auto_close_days > 0:
        now = time.monotonic()
        interval = settings.thread_auto_close_interval_seconds
        if (
            _last_auto_close_at is None
            or now - _last_auto_close_at >= interval
        ):
            _last_auto_close_at = now
            try:
                await auto_close_idle_threads()
            except AutoClosePassError as exc:
                # Logged here, not in the pass: messaging cannot import
                # the redactor (app/messaging/processor.py,
                # AutoClosePassError). A failed pass does not stop the
                # loop -- it never did.
                logger.error(
                    "thread_auto_close_pass_error",
                    closed=exc.closed,
                    exception=sanitized_traceback(exc),
                )

    return done


async def _poll_loop(
    run_pass: Callable[[], Awaitable[int]],
    wake: asyncio.Event,
    **log_fields: str,
) -> None:
    """Run `run_pass` forever with this loop's own interval: back to the
    base after work, doubled after an empty pass (up to the maximum),
    the maximum after a pass that raised. Cancellation ends it.

    The sleep between passes ends early when `wake` is set; the event
    is cleared right before the pass, so a wake during the pass is kept
    for one pass after it, and any number of wakes is one.

    `log_fields` name the loop in its error log (lane=..., or
    loop=maintenance).
    """
    base_interval = settings.notification_poll_interval_seconds
    max_backoff = settings.notification_max_backoff_seconds
    interval = base_interval
    while True:
        wake.clear()
        try:
            done = await run_pass()
            interval = (
                base_interval if done > 0
                else min(interval * 2, max_backoff)
            )
        except Exception as exc:
            logger.error(
                "notification_worker_error",
                **log_fields,
                exception=sanitized_traceback(exc),
            )
            interval = max_backoff
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(interval):
                await wake.wait()


def _lane_pass(
    lane: Lane, wake: asyncio.Event, jobs: LaneJobs,
) -> Callable[[], Awaitable[int]]:
    """One pass of `lane` over the lane's jobs in work (P5-L); an
    attempt that ended with a full portion sets the lane's own wake
    (H7), so the next portion goes at once -- the pass clears the event
    before it starts (_poll_loop), so a wake set while it runs or after
    it ends is kept for the sleep that follows."""

    async def run_pass() -> int:
        return await process_lane(lane, on_more=wake.set, jobs=jobs)

    return run_pass


async def _lane_loop(lane: Lane, wake: asyncio.Event) -> None:
    """The loop of `lane`: its passes (_poll_loop), and the task group
    the attempts they start run in -- with the lane's places and slots
    (engine.processor.LaneJobs), one set for the process's life.
    Cancelling the loop cancels its attempts and waits for their
    rollback; an attempt that raises past its own handling (only a
    BaseException does) ends the loop."""
    async with asyncio.TaskGroup() as attempts:
        await _poll_loop(
            _lane_pass(lane, wake, LaneJobs(lane, attempts)),
            wake,
            lane=lane.value,
        )


async def run_worker_loop() -> None:
    """The worker's loops: one per lane of this process and one for
    maintenance, each with its own interval, and the listener that
    wakes the lane loops (app/engine/wake.py). Runs until cancelled.

    In a task group: cancelling this cancels every loop; a loop that
    ends any other way (each catches Exception, so only a
    BaseException gets out) cancels the others and propagates.
    """
    lanes = settings.worker_lanes
    logger.info(
        "notification_worker_started",
        poll_interval=settings.notification_poll_interval_seconds,
        max_backoff=settings.notification_max_backoff_seconds,
        max_attempts=settings.notification_max_delivery_attempts,
        lanes=[lane.value for lane in lanes],
        lane_budgets={
            lane.value: {
                "batch_size": settings.lane_batch_size(lane),
                "jobs": LANE_JOBS[lane],
                "concurrency": LANE_CONCURRENCY[lane],
                "deliver_timeout": settings.lane_deliver_timeout(lane),
            }
            for lane in lanes
        },
        retention_days=settings.notification_retention_days,
        retention_interval_seconds=(
            settings.notification_retention_interval_seconds
        ),
        thread_auto_close_days=settings.thread_auto_close_days,
        thread_auto_close_interval_seconds=(
            settings.thread_auto_close_interval_seconds
        ),
    )
    if settings.notification_retention_days <= 0:
        # Fix I: disabling retention must be LOUD -- terminal rows now
        # accumulate forever.
        logger.warning(
            "retention_disabled",
            retention_days=settings.notification_retention_days,
        )
    if settings.thread_auto_close_days <= 0:
        # Same rule for auto-close: a stray THREAD_AUTO_CLOSE_DAYS=0
        # means threads never auto-close -- surface it at startup.
        logger.warning(
            "thread_auto_close_disabled",
            thread_auto_close_days=settings.thread_auto_close_days,
        )
    untaken = [lane.value for lane in Lane if lane not in lanes]
    if untaken:
        # H9: a valid subset is lawful -- another worker process of the
        # deploy may take the rest (spec §8.6) -- so it starts; but this
        # process neither delivers nor expires those lanes, and if no
        # other one does, their jobs wait for ever. A knob that stops
        # work is loud, like the two above: one line, naming them.
        logger.warning(
            "worker_lanes_partial",
            lanes_not_taken=untaken,
            hint=(
                "this process does not deliver or expire these lanes; "
                "another worker process of the deploy must take them "
                "(COMMS_WORKER_LANES), or their jobs stay pending -- "
                "GET /api/v1/lanes/depth shows them"
            ),
        )

    for mix in lanes_mixing_llm():
        # P5-1, P5-L: a model writes for up to LLM_DELIVER_TIMEOUT_SECONDS
        # per key set and holds one of its lane's places all that time;
        # as many models as the lane has places hold the lane, and the
        # types for people of the same lane wait behind them. A lawful
        # profile, so it starts; loud, one line per lane. KNOWN CEILING
        # (the places of a lane) on app/engine/service.py _deliver_single.
        jobs = LANE_JOBS[Lane(mix.lane)]
        logger.warning(
            "lane_mixes_llm",
            lane=mix.lane,
            jobs=jobs,
            llm_types=list(mix.llm_types),
            other_types=list(mix.other_types),
            hint=(
                f"this lane attempts {jobs} job(s) at a time: a model "
                "holds one of them while it writes (up to "
                "LLM_DELIVER_TIMEOUT_SECONDS per key set), and while "
                f"{jobs} model(s) write, every job of these other types "
                "waits behind them -- give the llm types a lane without "
                "types for people and with more than one job at a time "
                "(deploy/INTEGRATION.md)"
            ),
        )

    wakes = {lane.value: asyncio.Event() for lane in lanes}
    try:
        async with asyncio.TaskGroup() as group:
            for lane in lanes:
                group.create_task(_lane_loop(lane, wakes[lane.value]))
            group.create_task(
                _poll_loop(
                    run_maintenance_pass, asyncio.Event(), loop="maintenance",
                ),
            )
            group.create_task(listen_for_wakes(wakes))
    except asyncio.CancelledError:
        logger.info("notification_worker_stopped")
        return


async def _main() -> None:
    """Run the worker loop with graceful shutdown on signals."""
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(run_worker_loop())

    def _request_shutdown() -> None:
        logger.info("worker_shutdown_requested")
        task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown)

    with contextlib.suppress(asyncio.CancelledError):
        await task

    # Review 1.1: close the aiogram session before dropping the engine.
    await close_formatters()
    await dispose_engine()


def main() -> None:
    """Console entrypoint: `python -m app.worker`."""
    setup_logging()
    set_process_role(ProcessRole.WORKER)
    # The worker renders templates -> it MUST have the profile. A
    # broken profile kills the process at startup (ProfileError).
    install_profile_from_settings()
    # The worker is the only process that delivers -> it builds the
    # channel registry BEFORE its loop. A registry that cannot be built
    # kills the worker here, at startup, instead of surfacing on the
    # first delivery (the build logs the channel map).
    init_formatters()
    asyncio.run(_main())


if __name__ == "__main__":
    main()
