# =============================================================================
# COMMS Service -- Messaging Processor (Phase 4b, item 6)
# =============================================================================
#
# The messaging-side maintenance pass: auto-close of idle threads. The
# sibling of the notification retention pass (app/engine/processor.py,
# Phase 3a) -- same shape (disabled guard, batched drain with a commit
# per batch, one loud per-pass log), different table.
#
# SCHEDULING lives in app/worker.py (the neutral layer above engine and
# messaging), on its OWN slow cadence (THREAD_AUTO_CLOSE_INTERVAL_
# SECONDS) -- NOT the worker tick, NOT the retention interval. This
# module is engine-free (messaging must not import engine, and does
# not need to); the batch UPDATE lives in app/messaging/status.py.
# Callers of the batch commit; this pass owns its own sessions.
# =============================================================================

import time
from datetime import UTC, datetime, timedelta

import structlog

from app.core.config import settings
from app.core.database import get_session_factory
from app.messaging.status import auto_close_idle_threads_batch

logger = structlog.get_logger()

_AUTO_CLOSE_BATCH_SIZE = 1000


async def auto_close_idle_threads(
    *,
    days: int | None = None,
    when: datetime | None = None,
) -> int:
    """Auto-close pass: close threads idle for THREAD_AUTO_CLOSE_DAYS,
    in batches (Phase 4b item 6).

    Orchestration only: status.auto_close_idle_threads_batch supplies
    ONE commit-free batch; this loop owns the commits -- one per batch,
    so a large backlog never becomes one long transaction -- and drains
    until a batch comes back short.

    DISABLED: settings.thread_auto_close_days <= 0 means auto-close is
    OFF -- return 0 without touching anything ("close everything" must
    never fall out of the cutoff arithmetic; the worker startup log
    names the disabled state loudly). The guard lives HERE, not only
    behind the worker's cadence gate, because tests call this directly.
    `days` / `when` are injectable for deterministic tests; both default
    to settings / now.

    Idle is measured on COALESCE(last_message_at, created_at) (D8): an
    empty thread ages from created_at. Every pass logs its duration --
    the observable promotion trigger.

    KNOWN CEILING (acknowledged by design -- messaging-side idle scan,
    the BL-3 family):
      1. Mechanics: the batch select filters on COALESCE(
         last_message_at, created_at) with no matching index -> each
         pass seq-scans the threads table as it grows. COALESCE over
         two columns makes it doubly unindexed by a plain column index.
      2. Status: acknowledged by design.
      3. Backlog ref: BL-3 family (dispatch plan §6a) -- the messaging
         idle scan alongside the notification retention scan.
      4. Promotion trigger (observable, via the thread_auto_close_pass
         log): a pass stably longer than ~1 second OR threads on the
         order of ~100k+.
      5. Agreed fix: ONE migration -- an expression / partial index on
         COALESCE(last_message_at, created_at) with a predicate
         excluding closed threads, OR a maintained last_activity_at
         column carrying that value with its own index.
      6. Rejected: an index NOW (write amplification on thread activity
         for a query cheap at current scale and rare by cadence); an
         unbounded UPDATE (one long transaction); DB coordination of
         the cadence gate -- it is PER-PROCESS on purpose (two workers
         -> two cheap idempotent scans per interval), no distributed
         lock.

    Returns:
        Total number of threads closed this pass.
    """
    close_days = days if days is not None else settings.thread_auto_close_days
    if close_days <= 0:
        return 0

    evaluated_at = when if when is not None else datetime.now(UTC)
    factory = get_session_factory()
    cutoff = evaluated_at - timedelta(days=close_days)
    started = time.monotonic()
    total = 0

    async with factory() as session:
        try:
            while True:
                closed = await auto_close_idle_threads_batch(
                    session,
                    cutoff=cutoff,
                    when=evaluated_at,
                    limit=_AUTO_CLOSE_BATCH_SIZE,
                )
                await session.commit()
                total += closed
                if closed < _AUTO_CLOSE_BATCH_SIZE:
                    break
        except Exception:
            await session.rollback()
            logger.exception("thread_auto_close_pass_error", closed=total)
            return total

    # Logged EVERY pass, empty ones included: a slow empty scan is
    # exactly the promotion-trigger signal.
    logger.info(
        "thread_auto_close_pass",
        closed=total,
        duration_ms=round((time.monotonic() - started) * 1000, 1),
        thread_auto_close_days=close_days,
    )
    return total
