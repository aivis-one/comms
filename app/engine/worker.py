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

import structlog

from app.core.config import settings
from app.engine.processor import (
    cleanup_expired_notifications,
    process_pending_notifications,
)

logger = structlog.get_logger()


async def run_notification_batch() -> int:
    """Run one worker batch: process pending + cleanup expired.

    Returns:
        Number of notifications processed (drives loop backoff).
    """
    processed = await process_pending_notifications()
    await cleanup_expired_notifications()
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
