# =============================================================================
# COMMS Service -- Messaging: Status transitions (Phase 4b, item 5)
# =============================================================================
#
# Thread lifecycle transitions over the 4a status field (values LOCKED
# to open/resolved/closed). Two entry points:
#   - set_status          -- MANUAL operator transitions (a matrix);
#   - apply_client_message_reopen -- the single AUTO transition, called
#                            from post_message when the CLIENT writes.
#
# TRANSITION MATRIX (D5):
#   manual:  open -> resolved, resolved -> closed, open -> closed;
#            X -> X is a no-op success.
#   auto:    resolved -> open, closed -> open  (client message only).
#   rejected: everything else -- a MANUAL reopen (an operator moving
#            closed/resolved back to open), closed -> resolved, any
#            other backward move. Only a client message reopens.
#
# NOTIFIABLE CLOSE (D6): reaching `closed` on a SECTION thread flags it
# notifiable (close_notify_pending_at) on BOTH paths -- manual close and
# auto-close -- because "loud vs quiet" is a property of the operator
# FORM (section vs user/DM), not the trigger. user/DM: never flagged.
# 4b only MARKS; 4c sends and clears. A client auto-reopen CLEARS the
# mark (the close was voided before 4c sent).
#
# AUTHZ: set_status validates the TRANSITION only, never WHO may make
# it -- write-authz is Phase 4c (the same seam as the sender write-authz
# and the supervisor read-only marker). Callers commit.
# =============================================================================

from datetime import UTC, datetime
from uuid import UUID

import structlog
from sqlalchemy import case, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundError, ValidationError
from app.messaging.constants import OperatorKind, ThreadStatus
from app.messaging.models import Thread

logger = structlog.get_logger()

# Allowed MANUAL forward transitions. closed has none (reopen is
# auto-only). A target equal to the current status is a no-op success,
# handled before this table is consulted.
_MANUAL_TRANSITIONS: dict[ThreadStatus, frozenset[ThreadStatus]] = {
    ThreadStatus.OPEN: frozenset({ThreadStatus.RESOLVED, ThreadStatus.CLOSED}),
    ThreadStatus.RESOLVED: frozenset({ThreadStatus.CLOSED}),
    ThreadStatus.CLOSED: frozenset(),
}

# The only states a client message reopens FROM.
_REOPEN_FROM: frozenset[ThreadStatus] = frozenset(
    {ThreadStatus.RESOLVED, ThreadStatus.CLOSED}
)


def mark_close_notify_if_section(thread: Thread, when: datetime) -> None:
    """Flag a SECTION thread notifiable when it reaches `closed`.

    Shared by set_status (manual) and the auto-close batch. user/DM
    threads are never flagged. 4c consumes and clears the mark.
    """
    if thread.operator_kind == OperatorKind.SECTION:
        thread.close_notify_pending_at = when


async def set_status(
    session: AsyncSession,
    *,
    thread_id: UUID,
    target: ThreadStatus,
    when: datetime | None = None,
) -> Thread:
    """Apply a manual operator status transition (see the D5 matrix).

    Raises NotFoundError if the thread does not exist, ValidationError
    on an invalid transition. X -> X is a no-op success. Reaching
    `closed` flags a section thread notifiable (D6).
    """
    thread = await session.get(Thread, thread_id)
    if thread is None:
        raise NotFoundError(f"thread {thread_id} does not exist")

    current = ThreadStatus(thread.status)
    if target == current:
        return thread  # no-op success

    if target not in _MANUAL_TRANSITIONS[current]:
        raise ValidationError(
            f"invalid status transition {current.value} -> {target.value}"
        )

    thread.status = target
    if target is ThreadStatus.CLOSED:
        mark_close_notify_if_section(
            thread, when if when is not None else datetime.now(UTC)
        )
    await session.flush()
    logger.info(
        "thread_status_changed",
        thread_id=str(thread_id),
        old=current.value,
        new=target.value,
    )
    return thread


def apply_client_message_reopen(thread: Thread) -> bool:
    """Revive a resolved/closed thread on a CLIENT message.

    Called from post_message ONLY when the sender is the thread's
    client. A resolved/closed thread goes back to `open`; any pending
    close-notify is CLEARED (the close was voided before 4c sent). Open
    threads are untouched. Mutates the passed object; the caller
    (post_message) flushes. Returns True iff it reopened.
    """
    current = ThreadStatus(thread.status)
    if current not in _REOPEN_FROM:
        return False
    thread.status = ThreadStatus.OPEN
    thread.close_notify_pending_at = None
    logger.info(
        "thread_reopened_by_client",
        thread_id=str(thread.id),
        old=current.value,
    )
    return True


async def auto_close_idle_threads_batch(
    session: AsyncSession,
    *,
    cutoff: datetime,
    when: datetime,
    limit: int,
) -> int:
    """Close ONE batch of idle threads (COMMIT-FREE).

    Idle = status != closed AND COALESCE(last_message_at, created_at) <
    cutoff. D8: an empty thread (never messaged) ages from created_at,
    so it is eligible -- closed is not deleted, and it revives on the
    next client message. Section threads that reach `closed` are flagged
    notifiable in the SAME statement via a CASE (D6, both paths);
    user/DM are left unflagged.

    Oldest-first via an IN subquery (ORDER BY the activity expression
    LIMIT n): DELETE/UPDATE ... LIMIT is not portable, and the subquery
    bounds each transaction to `limit` rows -- an unbounded UPDATE over
    a backlog was rejected. The caller (auto_close_idle_threads in
    app/messaging/processor.py) owns the drain loop and per-batch commit.

    Returns the number of threads closed in this batch.
    """
    activity = func.coalesce(Thread.last_message_at, Thread.created_at)
    batch_ids = (
        select(Thread.id)
        .where(Thread.status != ThreadStatus.CLOSED, activity < cutoff)
        .order_by(activity)
        .limit(limit)
        .scalar_subquery()
    )
    result = await session.execute(
        update(Thread)
        .where(Thread.id.in_(batch_ids))
        .values(
            status=ThreadStatus.CLOSED,
            # D6: mark section threads notifiable at the close instant;
            # user/DM keep their (NULL) marker.
            close_notify_pending_at=case(
                (Thread.operator_kind == OperatorKind.SECTION, when),
                else_=Thread.close_notify_pending_at,
            ),
        )
    )
    closed: int = result.rowcount  # type: ignore[attr-defined]
    return closed
