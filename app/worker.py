# =============================================================================
# COMMS Service -- Worker Entrypoint + Loop (neutral layer)
# =============================================================================
#
# Separate worker process: same Docker image as the API, different
# command (handoff item 1):
#
#   API:    uvicorn app.main:app --host 0.0.0.0 --port 8000
#   Worker: python -m app.worker
#
# This module is the NEUTRAL layer above both `engine` and `messaging`.
# It owns the eternal poll loop and ALL maintenance SCHEDULING:
#   - engine delivery batch (engine.worker.run_notification_batch),
#   - notification retention pass (engine.processor), own cadence,
#   - thread auto-close pass (messaging.processor), own cadence.
# Keeping the loop and both cadence gates here is what lets `engine`
# stay MESSAGING-FREE (Phase 4b, edit 3 / the DAG hygiene of 3b): the
# only place that imports both engine and messaging is this entrypoint.
#
# BACKOFF: empty batch -> interval doubles (up to max_backoff); work
# found -> interval resets to base. SIGTERM/SIGINT cancel the loop task
# so the current batch finishes its per-item transaction cleanly.
# =============================================================================

import asyncio
import contextlib
import signal
import time

import structlog

from app.core.config import settings
from app.core.database import dispose_engine
from app.core.logging import setup_logging
from app.engine.formatters import close_formatters
from app.engine.processor import cleanup_terminal_notifications
from app.engine.worker import run_notification_batch
from app.messaging.processor import auto_close_idle_threads
from app.profile.loader import install_profile_from_settings

logger = structlog.get_logger()

# -- Maintenance cadence gates (PER-PROCESS, Phase 3a fix H). None ->
# never ran in this process: the first tick runs the pass immediately
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


async def run_worker_batch() -> int:
    """One full worker iteration: the engine delivery batch, then the
    two maintenance passes -- each gated to its OWN slow cadence.

    ORDER: deliveries first (maintenance never delays them); retention
    next; auto-close last. Each maintenance pass is skipped when
    disabled (days <= 0; the startup log names it) and otherwise gated
    to its interval. The passes commit per batch, so transactions stay
    short even when a pass drains a backlog.

    Returns:
        Number of notifications processed (drives the loop backoff).
    """
    global _last_retention_at, _last_auto_close_at

    processed = await run_notification_batch()

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
            await auto_close_idle_threads()

    return processed


async def run_worker_loop() -> None:
    """Main worker loop. Runs until cancelled.

    Catches all exceptions to prevent the loop from dying; a failing
    batch backs off to max interval and tries again.
    """
    base_interval = settings.notification_poll_interval_seconds
    max_backoff = settings.notification_max_backoff_seconds
    interval = base_interval

    logger.info(
        "notification_worker_started",
        poll_interval=base_interval,
        max_backoff=max_backoff,
        max_attempts=settings.notification_max_delivery_attempts,
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

    while True:
        try:
            processed = await run_worker_batch()

            if processed > 0:
                interval = base_interval
            else:
                interval = min(interval * 2, max_backoff)

        except asyncio.CancelledError:
            logger.info("notification_worker_stopped")
            return
        except Exception:
            logger.exception("notification_worker_error")
            interval = max_backoff

        try:
            await asyncio.sleep(interval)
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
    # The worker renders templates -> it MUST have the profile. A
    # broken profile kills the process at startup (ProfileError).
    install_profile_from_settings()
    asyncio.run(_main())


if __name__ == "__main__":
    main()
