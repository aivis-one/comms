# =============================================================================
# COMMS Service -- Messaging: Read State (Phase 4a, item 4)
# =============================================================================
#
# Per-participant read pointer (arch doc §2.4 / D5). "Unread in a
# thread" is DERIVED, never stored: count_unread compares message times
# to the pointer.
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

from datetime import datetime
from uuid import UUID

import structlog
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.models import Message, ThreadReadState

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


async def count_unread(
    session: AsyncSession,
    *,
    thread_id: UUID,
    participant: UUID,
) -> int:
    """Count messages this participant has not read.

    unread = messages in the thread created after the pointer, EXCLUDING
    the participant's own messages (you have no unread of what you
    sent). No read-state row -> every message not sent by the
    participant counts. The pointer is fetched first (a plain scalar),
    then applied in Python, so an absent pointer is a clean branch
    rather than a NULL-comparison edge case in SQL.
    """
    last_read_at = await session.scalar(
        select(ThreadReadState.last_read_at).where(
            ThreadReadState.thread_id == thread_id,
            ThreadReadState.participant == participant,
        )
    )

    stmt = (
        select(func.count())
        .select_from(Message)
        .where(
            Message.thread_id == thread_id,
            Message.sender != participant,
        )
    )
    if last_read_at is not None:
        stmt = stmt.where(Message.created_at > last_read_at)

    return await session.scalar(stmt) or 0
