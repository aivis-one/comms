# =============================================================================
# COMMS Service -- Notification delivery batch (engine tick)
# =============================================================================
#
# ONE delivery batch = process pending + cleanup expired. Ported from
# cbshome's run_notification_batch (there it was ticked by an asyncio
# daemon inside the API process).
#
# The eternal poll LOOP and all maintenance SCHEDULING (retention +
# auto-close cadence gates) live one layer up in app/worker.py -- the
# neutral process entrypoint above BOTH engine and messaging. This
# keeps the engine MESSAGING-FREE: it must not (and does not) import
# app.messaging. run_notification_batch is the engine's cohesive tick
# unit; app/worker.py loops over it and adds the gated passes.
# =============================================================================

import structlog

from app.engine.processor import (
    cleanup_expired_notifications,
    process_pending_notifications,
)

logger = structlog.get_logger()


async def run_notification_batch() -> int:
    """Run one engine delivery batch: process pending + cleanup expired.

    Deliveries first, then the small per-tick expiry cleanup. Owns no
    scheduling: the retention and auto-close passes are gated and driven
    by app/worker.py on their own slow cadences.

    Returns:
        Number of notifications processed (drives the loop backoff in
        app/worker.py).
    """
    processed = await process_pending_notifications()
    await cleanup_expired_notifications()
    return processed
