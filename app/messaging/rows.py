# =============================================================================
# COMMS Service -- Messaging: taking a thread's row (TK-2)
# =============================================================================
#
# A thread can be deleted (the product's DELETE /threads/{id}, TK-2), so
# "the thread is there" is answered by TAKING its row, never by reading
# it: the read says what was, the lock says what is and keeps it so
# until the caller commits. Every write under a thread takes the
# thread's row FIRST, before it writes the thread or any child of it --
# the order of row locks of app/messaging/status.py (THE ORDER OF ROW
# LOCKS), where the strength of each writer and the reason for it are
# written once.
#
# Two strengths, by what the caller writes:
#   - FOR NO KEY UPDATE -- the caller writes the thread row itself
#     (a message bumps its activity, a retag moves it, a status moves
#     it);
#   - FOR KEY SHARE -- the caller only inserts children under it (a
#     read pointer, a variant request, a variant). It conflicts with
#     the delete and with nothing else that writes the thread, so a
#     read pointer never queues a status call or a claim.
# =============================================================================

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundError
from app.messaging.models import Thread


async def take_thread(
    session: AsyncSession, thread_id: UUID, *, children_only: bool = False
) -> Thread:
    """The thread's row, locked and refreshed -- or a 404.

    `children_only` -- the caller inserts children and does not write
    the thread: FOR KEY SHARE; otherwise FOR NO KEY UPDATE. A thread
    deleted before the lock was granted is absent here, which is the
    answer a thread that never existed gets.
    """
    thread = await session.get(
        Thread,
        thread_id,
        with_for_update=(
            {"read": True, "key_share": True}
            if children_only
            else {"key_share": True}
        ),
        populate_existing=True,
    )
    if thread is None:
        raise NotFoundError(f"thread {thread_id} does not exist")
    return thread
