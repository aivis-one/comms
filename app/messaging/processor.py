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

    Idle-scan index (Phase 4c -- resolved, was a KNOWN CEILING): the
    batch select filters and orders on COALESCE(last_message_at,
    created_at); migration 0008 adds the expression index
    ix_threads_activity over exactly that value. EXPLAIN (ANALYZE) on 5k
    threads confirms the planner uses it here -- "Index Scan Backward
    using ix_threads_activity" with the cutoff carried as an Index Cond
    (not a post-scan filter) -- so a pass is no longer a seq-scan that
    grows with the table. The SAME index backs the list_visible_threads
    keyset (one index, two readers; both plans in the Phase 4c report).
    Two design choices are deliberately KEPT: the cadence gate stays
    PER-PROCESS (two workers -> two cheap idempotent scans per interval,
    no distributed lock), and batches stay bounded (no unbounded UPDATE,
    one commit per batch).

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
