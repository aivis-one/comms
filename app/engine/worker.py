# =============================================================================
# COMMS Service -- Engine maintenance (one pass of the maintenance loop)
# =============================================================================
#
# The engine's share of the worker's maintenance loop: expire the
# overdue jobs of the process's lanes, then delete the expired terminal
# ones. Ported from cbshome's run_notification_batch, whose delivery
# half left in P4-1: deliveries are the lane loops' (one per lane,
# app/engine/processor.py process_lane), and maintenance runs beside
# them in its own loop so that neither waits for the other -- the
# expiry sweep waits FOR UPDATE for an attempt in flight, which no lane
# loop may do.
#
# The eternal LOOPS and all maintenance SCHEDULING (retention +
# auto-close cadence gates) live one layer up in app/worker.py -- the
# neutral process entrypoint above BOTH engine and messaging. This
# keeps the engine MESSAGING-FREE: it must not (and does not) import
# app.messaging.
# =============================================================================

import structlog

from app.core.config import settings
from app.engine.processor import (
    cleanup_expired_notifications,
    expire_overdue_notifications,
    purge_expired_llm_answers,
)

logger = structlog.get_logger()


async def run_engine_maintenance() -> int:
    """Run the engine's maintenance pass: expire the overdue jobs of
    this process's lanes, clear the model answers past their term, then
    delete expired terminal jobs.

    Owns no scheduling: the maintenance loop in app/worker.py calls it
    every pass, and the retention and auto-close passes are gated there
    on their own slow cadences.

    Returns:
        Number of jobs expired and deleted, and of answers cleared
        (drives the maintenance loop's backoff in app/worker.py).
    """
    expired = await expire_overdue_notifications(settings.worker_lanes)
    # The answers past their term first (P5-1): a job whose answer is
    # cleared now may be deleted by the cleanup in the same pass.
    purged = await purge_expired_llm_answers()
    deleted = await cleanup_expired_notifications()
    return expired + purged + deleted
