# =============================================================================
# COMMS Service -- Messaging: Read State (Phase 4a, item 4)
# =============================================================================
#
# Per-participant read pointer (arch doc §2.4 / D5). "Unread in a
# thread" is DERIVED, never stored: count_unread compares message times
# to the pointer.
#
# ONE SOURCE OF THE UNREAD SEMANTICS (T-51). The condition itself lives
# in exactly one place -- unread_condition() -- and every reader builds
# on it: count_unread (one thread), unread_counts_select (a batch of
# threads) and unread_count_lateral (a correlated per-thread count for
# an aggregate over a participant's threads). The semantics are frozen;
# the EXPRESSION of them is not, and expressing them twice is how the
# two copies drift.
#
# WHAT THIS BUILDER DELIBERATELY DOES NOT KNOW: whether the reader is a
# PARTICIPANT of the thread. That predicate lives in
# operators.participation_clause and is applied by the three T-51
# aggregate endpoints ONLY. count_unread answers a narrower question --
# "how many messages in this thread has this pair not read" -- and
# answers it for a non-participant too; folding participation in here
# would silently turn GET /threads/{id}/unread-count into a zero for
# non-participants and make an additive release a breaking one.
#
# mark_read is RACE-SAFE and MONOTONIC (mandatory edit 1). A
# SELECT-then-UPDATE cannot hold monotonicity under concurrency: two
# markers with T1 < T2 can both read the old value and the later write
# can lose to the earlier one (a regressed pointer -- the exact failure
# the pointer is meant to prevent). The fix is a single atomic
# statement: INSERT ... ON CONFLICT DO UPDATE with
# last_read_at = greatest(existing, proposed). The database serializes
# the two upserts on the PK and each applies greatest(), so the final
# value is max(T1, T2) regardless of arrival order. on_conflict is a
# blessed Postgres-only construct here (D5 answered).
#
# Callers commit (repo session rule).
# =============================================================================

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import ColumnElement, Select, and_, func, or_, select, true
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import LateralFromClause

from app.messaging.models import Message, Thread, ThreadReadState
from app.messaging.operators import participation_clause

logger = structlog.get_logger()


async def mark_read(
    session: AsyncSession,
    *,
    thread_id: UUID,
    participant: UUID,
    last_read_at: datetime,
) -> None:
    """Advance a participant's read pointer to `last_read_at`.

    Race-safe and monotonic: the pointer only ever moves forward. See
    the module header. (thread_id / participant referents are guarded
    by the table's FKs -- a bad id surfaces as an IntegrityError.)
    """
    insert_stmt = pg_insert(ThreadReadState).values(
        thread_id=thread_id,
        participant=participant,
        last_read_at=last_read_at,
    )
    # greatest(existing row's value, the value we are proposing) -> the
    # pointer never regresses, even if an older mark_read lands later.
    stmt = insert_stmt.on_conflict_do_update(
        index_elements=["thread_id", "participant"],
        set_={
            "last_read_at": func.greatest(
                ThreadReadState.last_read_at,
                insert_stmt.excluded.last_read_at,
            )
        },
    )
    await session.execute(stmt)
    logger.info(
        "read_state_marked",
        thread_id=str(thread_id),
        participant=str(participant),
    )


def unread_condition(participant: UUID) -> ColumnElement[bool]:
    """The frozen "this message is unread by `participant`" condition.

    unread = a message NOT sent by the participant (you have no unread
    of what you sent) that is newer than the participant's read
    pointer. Written set-wise, over a LEFT JOIN of thread_read_states,
    so that one thread and a thousand threads are the same expression.

    ABSENT POINTER (was: a clean Python branch on a scalar fetch). The
    outer join yields NULL for last_read_at exactly when the pointer
    row does not exist, and `IS NULL OR created_at > ...` maps that to
    "every message not sent by the participant is unread" -- the same
    rule the scalar branch encoded. The NULL is unambiguous here
    because ThreadReadState.last_read_at is NOT NULL in the schema: a
    pointer row with no timestamp cannot exist, so a NULL can only mean
    an absent row, never an empty pointer.

    Callers MUST have joined ThreadReadState on
    (thread_id == Message.thread_id AND participant == participant) --
    see _outer_join_pointer, which is the only intended way to do it.
    """
    return and_(
        Message.sender != participant,
        or_(
            ThreadReadState.last_read_at.is_(None),
            Message.created_at > ThreadReadState.last_read_at,
        ),
    )


def _outer_join_pointer(stmt: Select[Any], participant: UUID) -> Select[Any]:
    """LEFT JOIN this participant's read pointer onto a Message select.

    The join is on the read-state PK (thread_id, participant), so the
    planner takes it as an index lookup and the row is either the
    pointer or nothing -- the two states unread_condition distinguishes.
    """
    return stmt.outerjoin(
        ThreadReadState,
        and_(
            ThreadReadState.thread_id == Message.thread_id,
            ThreadReadState.participant == participant,
        ),
    )


def unread_counts_select(
    participant: UUID,
    thread_ids: Sequence[UUID] | None = None,
) -> Select[Any]:
    """Per-thread unread counts for `participant`, one row per thread.

    Returns a select of (thread_id, unread). Threads with nothing
    unread produce NO ROW (GROUP BY over the matching messages) -- the
    caller decides whether that means zero (count_unread) or an absent
    key (the batch endpoint counts only participant threads and gets
    its absence rule from participation, not from this).

    `thread_ids` narrows the scan to a batch; None means every thread
    the participant has unread in.
    """
    stmt = select(
        Message.thread_id.label("thread_id"),
        func.count().label("unread"),
    ).select_from(Message)
    stmt = _outer_join_pointer(stmt, participant)
    if thread_ids is not None:
        stmt = stmt.where(Message.thread_id.in_(thread_ids))
    return stmt.where(unread_condition(participant)).group_by(
        Message.thread_id
    )


def unread_count_lateral(participant: UUID) -> LateralFromClause:
    """A correlated per-thread unread count, to LATERAL-join onto Thread.

    Used by the participant summary, where the driving set is "the
    threads this participant takes part in" rather than a known list of
    ids: the outer query walks threads by the participation clause and
    this subquery counts each one through ix_messages_thread_created.
    Unlike unread_counts_select it yields a row for EVERY driving
    thread (count() over an empty set is 0), which is what makes the
    outer aggregate honest about threads with nothing unread.
    """
    stmt = select(func.count().label("unread")).select_from(Message)
    stmt = _outer_join_pointer(stmt, participant)
    return (
        stmt.where(Message.thread_id == Thread.id)
        .where(unread_condition(participant))
        .correlate(Thread)
        .lateral("unread_lat")
    )


# ---------------------------------------------------------------------------
# Participant aggregates (T-51)
# ---------------------------------------------------------------------------
# TWO PREDICATES MEET HERE, AND ONLY HERE. "Unread" comes from
# unread_condition above; "participant" comes from
# operators.participation_clause. Neither builder contains the other --
# the composition is written at these two call sites, in the open. That
# is deliberate: a single builder holding both would be the obvious
# "one source of truth" refactor, and it would quietly reroute
# count_unread (which must answer for non-participants too) through a
# participation filter, turning GET /threads/{id}/unread-count into a
# zero for the very callers it was built for.
#
# ABSENCE, NOT ZERO: both aggregates DRIVE off the participation clause,
# so a thread the participant has no role in produces no row at all.
# A participant thread with nothing unread produces a row with 0 (the
# LATERAL count over an empty set), which is the honest answer.


async def participant_unread_summary(
    session: AsyncSession,
    *,
    participant: UUID,
) -> tuple[int, int]:
    """(threads_with_unread, unread_messages) across a participant's threads.

    One statement: walk the participant's threads by the three-role
    clause, LATERAL-count each through ix_messages_thread_created,
    aggregate outside. A participant with no threads aggregates to
    (0, 0) -- an empty driving set, not an error.
    """
    lateral = unread_count_lateral(participant)
    stmt = (
        select(
            func.count().filter(lateral.c.unread > 0),
            func.coalesce(func.sum(lateral.c.unread), 0),
        )
        .select_from(Thread)
        .join(lateral, true())
        .where(participation_clause(participant))
    )
    threads_with_unread, unread_messages = (await session.execute(stmt)).one()
    return int(threads_with_unread), int(unread_messages)


async def unread_counts_for_participant(
    session: AsyncSession,
    *,
    participant: UUID,
    thread_ids: Sequence[UUID],
) -> dict[UUID, int]:
    """Per-thread unread counts, for the threads of `thread_ids` this
    participant actually takes part in.

    One statement for the whole batch. Threads the participant has no
    role in -- and ids that match no thread at all -- are simply not in
    the result: one rule, applied by the join, not two branches.
    """
    if not thread_ids:
        return {}
    lateral = unread_count_lateral(participant)
    stmt = (
        select(Thread.id, lateral.c.unread)
        .select_from(Thread)
        .join(lateral, true())
        .where(
            Thread.id.in_(thread_ids),
            participation_clause(participant),
        )
    )
    rows = await session.execute(stmt)
    return {thread_id: int(unread) for thread_id, unread in rows}


async def count_unread(
    session: AsyncSession,
    *,
    thread_id: UUID,
    participant: UUID,
) -> int:
    """Count messages this participant has not read, in ONE thread.

    The single-thread special case of unread_counts_select -- the
    primitive, unchanged in contract: it asks only about the (thread,
    participant) pair and does NOT check participation, so a
    non-participant gets a real number here, not a zero. See the module
    header for why that stays true.

    No matching messages -> no row -> 0.
    """
    row = (
        await session.execute(
            unread_counts_select(participant, thread_ids=[thread_id])
        )
    ).first()
    return int(row.unread) if row is not None else 0
