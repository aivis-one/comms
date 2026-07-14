# =============================================================================
# COMMS Service -- Notification Worker Loop
# =============================================================================
#
# Canonical merge of the two donors' run models:
#   - cbshome: run_notification_batch() -- one batch = process pending
#     + cleanup expired. In cbshome it was ticked by an asyncio daemon
#     inside the API process.
#   - velo: a long-running poll loop with exponential backoff and clean
#     cancellation (its processor ran the loop itself).
#
# In comms the loop runs in a SEPARATE PROCESS (same image, different
# command: `python -m app.worker`), never inside the API process --
# see the Dockerfile and app/worker.py.
#
# BACKOFF (velo):
#   Empty batch -> interval doubles (up to max_backoff).
#   Work found  -> interval resets to base poll interval.
# =============================================================================

import asyncio
import time

import structlog

from app.core.config import settings
from app.engine.processor import (
    cleanup_expired_notifications,
    cleanup_terminal_notifications,
    process_pending_notifications,
)

logger = structlog.get_logger()

# Phase 3a fix H: per-process monotonic timestamp of the last
# retention pass. Cadence is SCHEDULING and lives here in the worker
# (the processor owns the drain loop, the service owns one batch).
# None -> never ran in this process: the first tick runs the pass
# immediately (restart = immediate catch-up, a feature -- deploys
# restart comms anyway, so staleness is bounded above).
# PER-PROCESS on purpose: two workers -> two cheap scans per interval,
# idempotent and harmless; no DB coordination (see the KNOWN CEILING
# marker in processor.cleanup_terminal_notifications -- do not "fix"
# this with a distributed lock).
_last_retention_at: float | None = None


def reset_retention_gate() -> None:
    """Reset the retention cadence gate (tests -- same pattern as
    formatters.reset/close helpers)."""
    global _last_retention_at
    _last_retention_at = None


async def run_notification_batch() -> int:
    """Run one worker batch: process pending + cleanup expired +
    (on its own cadence) the retention pass.

    ORDER (fix H): deliveries first -- retention never delays them;
    expiry cleanup second (small, every tick); retention last, gated
    to NOTIFICATION_RETENTION_INTERVAL_SECONDS. The first pass after
    a long idle may be long (draining the backlog), but it runs after
    the tick's deliveries and commits per batch, so transactions stay
    short. Disabled retention (retention_days <= 0) never enters the
    gate; the startup log names it (fix I).

    Returns:
        Number of notifications processed (drives loop backoff).
    """
    global _last_retention_at

    processed = await process_pending_notifications()
    await cleanup_expired_notifications()

    if settings.notification_retention_days > 0:
        now = time.monotonic()
        interval = settings.notification_retention_interval_seconds
        if (
            _last_retention_at is None
            or now - _last_retention_at >= interval
        ):
            # Stamp BEFORE the pass: even if a future edit lets the
            # pass raise, a failing scan retries on the slow cadence,
            # not on every 5s tick.
            _last_retention_at = now
            await cleanup_terminal_notifications()

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
    )
    if settings.notification_retention_days <= 0:
        # Fix I: disabling retention must be LOUD -- terminal rows now
        # accumulate forever. A stray RETENTION_DAYS=0 in env should
        # surface on the first startup, not be discovered on disk.
        logger.warning(
            "retention_disabled",
            retention_days=settings.notification_retention_days,
        )

    while True:
        try:
            processed = await run_notification_batch()

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
