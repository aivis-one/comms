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
#   - deliveries still waiting -> RECIPIENT_INACTIVE (SENT when the
#     channel already accepted them), their jobs folded; error_message
#     cleared on every delivery of theirs, and provider_text on every
#     journal row of theirs (the journal's one edit).
#
# WHAT STAYS, and why: the tombstone row itself (threads and messages
# reference it with RESTRICT; deleting it would erase the other party's
# conversation or fail), threads and messages (a shared conversation --
# what happens to message BODIES is the product's decision, spec §10.6),
# and finished delivery rows (the delivery history; they carry the
# product's id, no way to reach anyone).
#
# WHAT HOLDS THE WINDOW against an attempt in flight (G1 item 1, tests/
# test_g1_forgetting_in_flight.py). An attempt resolves, delivers and
# commits in one transaction; a forgetting that lands while it stands in
# a channel call is ordered after it, never beside it:
#   - the FIRST attempt of a job inserts its deliveries, and their
#     foreign key takes FOR KEY SHARE on the recipient row until the
#     attempt commits. The tombstone takes that row FOR UPDATE
#     (sync._tombstone_once), which conflicts with KEY SHARE: forgetting
#     waits, the letter goes while the person is still known, and
#     withdraw_recipient then sees the committed deliveries and clears
#     what they carry. withdraw_recipient on its own would not wait --
#     it sees no committed delivery and returns 0; the tombstone is what
#     waits, so the tombstone comes first.
#   - a tombstone committed between the attempt's choice of ids and that
#     insert is seen by the late activity gate of deliver_notification:
#     it reads the recipient fresh, because resolve selects ids only and
#     no Recipient of an earlier read sits in the attempt's identity map.
#     A read of Recipient rows put into the attempt before that gate,
#     with the objects still referenced (the identity map holds them
#     weakly), would hand it a stale `active` and reopen the window.
#   - a LATER attempt inserts no delivery and takes no lock on the
#     recipient: the tombstone passes, and withdraw_recipient waits on
#     the job row (FOR UPDATE against the attempt's FOR NO KEY UPDATE).
#   - WHAT THIS STANDS ON: the journal's recipient_id carries no foreign
#     key (app/engine/models.py NotificationTransition). The channel's
#     answer is journaled from a second session while the attempt waits
#     for it; with such a key that session would take KEY SHARE on the
#     recipient, queue behind a tombstone that waits for the attempt, and
#     the attempt would wait for it -- a cycle across two connections
#     Postgres does not see. A foreign key there reopens this.
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
