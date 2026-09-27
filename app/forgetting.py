# =============================================================================
# COMMS Service -- Forgetting a recipient (F1.4, spec §10.4)
# =============================================================================
#
# The product deleted a person; comms forgets how to reach them. Both
# write paths -- DELETE /api/v1/recipients/{id} and the user_deleted
# event -- call forget_recipient, and the deletion is ordered against
# snapshots by the same version rule (app/audience/sync.py).
#
# WHAT IS FORGOTTEN (every field by which the person can be reached):
#   - the recipient row becomes a TOMBSTONE: telegram_id, email,
#     locale, timezone and the delivery schedule NULL, active false,
#     deleted_at set -- CHECK ck_recipients_tombstone holds it;
#   - group memberships, category mutes, section roles, thread read
#     pointers -- deleted;
#   - deliveries still waiting -> RECIPIENT_INACTIVE, their jobs
#     folded; error_message cleared on every delivery of theirs.
#
# WHAT STAYS, and why: the tombstone row itself (threads and messages
# reference it with RESTRICT; deleting it would erase the other party's
# conversation or fail), threads and messages (a shared conversation --
# what happens to message BODIES is the product's decision, spec §10.6),
# and finished delivery rows (the delivery history; they carry the
# product's id, no way to reach anyone).
#
# A NEUTRAL MODULE, like app/notifier.py: it may import audience,
# messaging and engine, which never import each other in this direction.
# Callers commit.
# =============================================================================

from uuid import UUID

import structlog
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import CategoryMute, GroupMembership
from app.audience.sync import tombstone
from app.engine.service import withdraw_recipient
from app.messaging.models import SectionMember, ThreadReadState

logger = structlog.get_logger()


async def forget_recipient(
    session: AsyncSession, *, recipient_id: UUID, version: int,
) -> bool:
    """Forget one recipient; True if forgotten by THIS call.

    A repeated deletion returns False and changes nothing. A deletion
    older than the stored snapshot raises StaleSnapshotError, one at the
    stored version SnapshotConflictError (sync.tombstone).
    """
    _, forgotten = await tombstone(
        session, recipient_id=recipient_id, version=version,
    )
    if not forgotten:
        return False
    for statement in (
        delete(GroupMembership).where(GroupMembership.recipient_id == recipient_id),
        delete(CategoryMute).where(CategoryMute.recipient_id == recipient_id),
        delete(SectionMember).where(SectionMember.operator_id == recipient_id),
        delete(ThreadReadState).where(ThreadReadState.participant == recipient_id),
    ):
        await session.execute(statement)
    closed = await withdraw_recipient(session, recipient_id)
    await session.flush()
    logger.info(
        "recipient_forgotten",
        recipient_id=str(recipient_id),
        version=version,
        deliveries_closed=closed,
    )
    return True
