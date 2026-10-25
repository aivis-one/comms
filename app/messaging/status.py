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
# TRANSITION MATRIX (D5; the manual reopen is TK-1):
#   manual:  open -> resolved, open -> closed, resolved -> closed,
#            resolved -> open, closed -> open;
#            X -> X is a no-op success.
#   auto:    resolved -> open, closed -> open  (client message only).
#   rejected: closed -> resolved -- the one backward move that does not
#            land on `open`.
# Reopening is one rule for both forms (section and user/DM) and for
# both paths (manual and client): reopen() below, the only place that
# writes it. Until TK-1 only a client message reopened; the lifecycle
# of a ticket is the product's, so the product may now return a
# conversation to work itself.
#
# NOTIFIABLE CLOSE (D6): reaching `closed` on a SECTION thread flags it
# notifiable (close_notify_pending_at) on BOTH paths -- manual close and
# auto-close -- because "loud vs quiet" is a property of the operator
# FORM (section vs user/DM), not the trigger. user/DM: never flagged.
# 4b only MARKS; 4c sends and clears (app/notifier.py,
# consume_close_notifications). A reopen -- manual or by the client --
# CLEARS the mark: the close was voided before 4c sent.
#
# THE ORDER OF ROW LOCKS (threads). Every writer of a threads row from a
# request takes exactly ONE threads row: set_status here (FOR NO KEY
# UPDATE -- it writes non-key columns only), the UPDATE of claim /
# assign / release / retag, and post_message's activity bump
# (app/messaging/operators.py, app/messaging/threads.py). Beside it a
# writer takes KEY SHARE on recipients rows through the FKs of what it
# writes (assignee, sender), before or after the thread row -- it does
# not matter, because no path holds a recipients row and then asks for a
# threads row: the recipient writers (app/audience/sync.py,
# app/forgetting.py) take recipients FOR UPDATE and never write threads.
# The two passes that take MANY threads rows take
# disjoint sets -- auto-close below takes threads that are not closed,
# the close-notice pass takes closed threads that carry the mark (the
# mark is set only together with `closed` and cleared only together
# with leaving it, in the same statement) -- and the
# close-notice pass walks its set in (close_notify_pending_at, id)
# order, so two of its passes queue on the first row they share.
#
# AUTHZ: set_status validates the TRANSITION only, never WHO may make
# it -- write-authz lives one layer up, in app/api/messaging.py (4c),
# the same seam as the sender write-authz and the supervisor read-only
# rule. Callers commit.
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

# Allowed MANUAL transitions. A target equal to the current status is a
# no-op success, handled before this table is consulted. The contract
# carries this table cell by cell (deploy/INTEGRATION.md section 14).
_MANUAL_TRANSITIONS: dict[ThreadStatus, frozenset[ThreadStatus]] = {
    ThreadStatus.OPEN: frozenset({ThreadStatus.RESOLVED, ThreadStatus.CLOSED}),
    ThreadStatus.RESOLVED: frozenset({ThreadStatus.CLOSED, ThreadStatus.OPEN}),
    ThreadStatus.CLOSED: frozenset({ThreadStatus.OPEN}),
}

# The states a thread is reopened FROM -- by an operator or by a client
# message alike. Read off the matrix, not written a second time.
_REOPEN_FROM: frozenset[ThreadStatus] = frozenset(
    current
    for current, targets in _MANUAL_TRANSITIONS.items()
    if ThreadStatus.OPEN in targets
)


def reopen(thread: Thread) -> None:
    """Return a thread to `open` and void any pending close notice.

    THE ONE COPY of the reopen rule (TK-1): set_status (manual) and
    apply_client_message_reopen (a client message) both land here, so a
    close notice cannot survive one path and not the other. The mark is
    cleared whatever the form: a user/DM thread never carries one, and
    a section thread reopened after the notice went out has it NULL
    already. Mutates the passed object; the caller flushes.
    """
    thread.status = ThreadStatus.OPEN
    thread.close_notify_pending_at = None


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

    THE ROW IS TAKEN, NOT READ (TK-1): FOR NO KEY UPDATE, refreshed, and
    the transition is checked against the row as it is under the lock.
    Two crossing calls queue; the second decides on the state the first
    left -- a no-op, a transition, or a 422 -- and never writes over it.
    Before, the check ran on a plain read and the write was blind:
    `open -> resolved` against a concurrent `open -> closed` could leave
    a resolved thread carrying a close notice.

    Raises NotFoundError if the thread does not exist, ValidationError
    on an invalid transition. X -> X is a no-op success. Reaching
    `closed` flags a section thread notifiable (D6); reaching `open`
    goes through reopen(), which voids the flag.

    A MANUAL REOPEN DOES NOT TOUCH last_message_at: no message was
    written, and that column is the time of the last message (the
    thread list's order, the auto-close socket).

    # KNOWN CEILING (auto-close undoes a manual reopen of an idle thread;
    # acknowledged by design):
    #   1. Mechanics: auto-close picks threads by COALESCE(last_message_at,
    #      created_at) < now - THREAD_AUTO_CLOSE_DAYS, and a manual reopen
    #      leaves that value where it was. A thread idle longer than the
    #      setting and reopened by hand is closed again by the next pass
    #      (THREAD_AUTO_CLOSE_INTERVAL_SECONDS) -- a section thread with a
    #      fresh close notice to its client. Measured, with the race
    #      against a pass already running: tests/test_thread_handoff.py.
    #   2. Status: acknowledged by design.
    #   3. Backlog ref: none -- no product reaches it today: velo runs
    #      THREAD_AUTO_CLOSE_DAYS=0 and never moves a ticket to closed,
    #      aivis never sends `open` (its SetStatusIn takes only resolved
    #      and closed).
    #   4. Promotion trigger: a product with THREAD_AUTO_CLOSE_DAYS > 0
    #      calls POST /threads/{id}/status with `open`.
    #   5. Agreed fix: an activity column of the thread's own that a
    #      manual reopen moves and auto-close reads -- a migration (C3).
    #   6. Rejected: moving last_message_at on a reopen (a lie about a
    #      message that was never written -- the list order and every
    #      reader of the column would believe it); a switch of
    #      auto-close per thread kind (the product turns auto-close off
    #      with THREAD_AUTO_CLOSE_DAYS=0, as velo does).
    #   The contract says the same to the product: deploy/INTEGRATION.md
    #   section 14.
    """
    thread = await session.get(
        Thread,
        thread_id,
        with_for_update={"key_share": True},
        populate_existing=True,
    )
    if thread is None:
        raise NotFoundError(f"thread {thread_id} does not exist")

    current = ThreadStatus(thread.status)
    if target == current:
        return thread  # no-op success

    if target not in _MANUAL_TRANSITIONS[current]:
        raise ValidationError(
            f"invalid status transition {current.value} -> {target.value}"
        )

    if target is ThreadStatus.OPEN:
        reopen(thread)
    else:
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
    client. A resolved/closed thread goes back to `open` through
    reopen() -- the same rule as a manual reopen, the pending close
    notice voided with it. Open threads are untouched. Mutates the
    passed object; the caller (post_message) flushes. Returns True iff
    it reopened.
    """
    current = ThreadStatus(thread.status)
    if current not in _REOPEN_FROM:
        return False
    reopen(thread)
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

    A manual reopen does not reset the idle clock -- the KNOWN CEILING
    on set_status.

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
