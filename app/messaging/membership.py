# =============================================================================
# COMMS Service -- Messaging: Section membership (T-67, API-1)
# =============================================================================
#
# THE ONE RULE, WRITTEN ONCE:
#
#   A SECTION WHOSE ROSTER WAS NEVER DECLARED IS SERVED BY ANY OPERATOR.
#   A SECTION WHOSE ROSTER WAS DECLARED IS SERVED BY ITS MEMBERS -- AND,
#   ONCE THE PRODUCT HAS REMOVED THEM ALL, BY NOBODY.
#
# "Declared" is sections.roster_declared (migration 0039): set by the
# first section_membership_changed with `member: true`, never cleared
# (set_membership below). Before API-1 the rule read "no members ->
# anybody", and "the product never kept a roster" and "the product
# removed the last operator" were one state: the second opened the
# section to everybody. A product that declares no roster still keeps
# exactly the old behaviour, for as long as it declares none.
#
# The rule is consumed in three places and lives in NONE of them:
#   - visibility  (operators.list_visible_threads)   -- what a pool shows;
#   - operate/claim authz (operators.can_operate / can_claim, and the new
#     assignee of operators.assign_thread);
#   - the pool push (notifier.notify_new_message).
# Three copies of the rule would be three chances to disagree, and the
# one that disagreed would either hide a queue or leak one. Hence
# serves_section() below, and hence the SQL form of the same rule
# (section_serves_clause) rather than a second hand-written EXISTS.
#
# THE RIGHT IS TAKEN, NOT READ (API-1). serves_section() is the form the
# WRITERS ask, and it answers on rows it LOCKS until the caller commits:
# a removal from the roster cannot land between the check and the verb's
# write. A removal that came first is seen (a locking read in READ
# COMMITTED reads the latest committed row); one that comes later waits
# for the verb. Visibility reads the SQL form without locks -- a list is
# a picture, not a write.
#
# THE ORDER OF ROW LOCKS (sections, section_members). The operator
# verbs (app/api/messaging.py claim / assign / release / status /
# retag) take, in this order:
#   1. the thread's row, FOR NO KEY UPDATE (app/messaging/rows.py
#      take_thread; app/messaging/status.py THE ORDER OF ROW LOCKS);
#   2. the operator's recipients row, FOR KEY SHARE -- the actor's, and
#      for assign the new assignee's too (KEY SHARE never conflicts with
#      KEY SHARE, so the two are taken in either order);
#   3. the section's row, FOR SHARE -- against the one write of it, the
#      declaration (set_membership: UPDATE ... SET roster_declared, a
#      NO KEY UPDATE), so an undeclared section stays undeclared until
#      the verb commits;
#   4. the operator's section_members row, FOR KEY SHARE -- against its
#      DELETE (FOR UPDATE strength), by set_membership and by forgetting.
# Step 2 comes before step 4 because of forgetting (app/forgetting.py):
# it takes the recipient FOR UPDATE (the tombstone) and then deletes
# their seats. A verb holding the seat and asking for the recipient --
# the claim's assignee foreign key does exactly that -- would wait on
# forgetting while forgetting waits on the seat: 40P01. With the
# recipient taken first, forgetting waits at its first lock
# (tests/test_operator_rights.py TestForgetting). set_membership takes
# the section, then the member row, and never a thread; forgetting takes
# the recipient, then the seats, and never a thread or a section. No
# writer here holds a sections or section_members row and then asks for
# a threads row, so the verbs' thread-first order closes no cycle.
#
# NO MATERIALIZED AUDIENCE (BL-1). member_ids() returns the DECLARED
# roster of ONE section -- a bounded list the product wrote down -- and
# never "every agent" or "every recipient". The pool push fans out over
# those rows and stops there; broadcasting to all recipients in the
# absence of a roster stays rejected, exactly as the deferred marker
# said before this module existed. An undeclared section and an emptied
# one both yield an empty roster, so both push to nobody.
# =============================================================================

from collections.abc import Sequence
from uuid import UUID

import structlog
from sqlalchemy import ColumnElement, column, or_, select, table, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app.core.database import once_more_on_insert_race
from app.core.exceptions import NotFoundError
from app.messaging.models import Section, SectionMember
from app.messaging.sections import get_or_create_section
from app.messaging.threads import recipient_exists

logger = structlog.get_logger()

# Lightweight, by-name reference to the audience-owned recipients table.
# Lets serves_section take the operator's recipient row (step 2 of THE
# ORDER OF ROW LOCKS) without importing an app.audience model (package
# DAG: core <- messaging; the same reference as app/messaging/threads.py).
_recipients = table("recipients", column("id"))


def section_serves_clause(
    section_column: ColumnElement[UUID] | InstrumentedAttribute[UUID],
    operator: UUID,
) -> ColumnElement[bool]:
    """The rule as a SQL predicate over a query's section column.

    Same rule as serves_section(), expressed for a query that has not
    loaded the rows yet: EITHER the section's roster was never declared,
    OR this operator is on it. A declared roster with nobody on it
    satisfies neither half. Written here, next to the boolean form, so
    the two cannot drift apart.

    `section_column` is the column carrying the section id in the outer
    query (threads.operator_value), so the EXISTS correlates with it.
    The union in the signature is not decoration: a mapped attribute
    (Thread.operator_value) is an InstrumentedAttribute, not a plain
    ColumnElement, and strict typing rejects the narrower annotation --
    which is exactly how this was found.
    """
    undeclared = select(Section.id).where(
        Section.id == section_column, Section.roster_declared.is_(False)
    )
    mine = select(SectionMember.section_id).where(
        SectionMember.section_id == section_column,
        SectionMember.operator_id == operator,
    )
    return or_(undeclared.exists(), mine.exists())


async def serves_section(
    session: AsyncSession, section_id: UUID, operator: UUID
) -> bool:
    """Does this operator serve this section? (The rule, in booleans.)

    THE ANSWER IS TAKEN, for the caller's transaction (the module
    header, THE RIGHT IS TAKEN and THE ORDER OF ROW LOCKS): the
    operator's recipient row FOR KEY SHARE, the section's row FOR
    SHARE, and -- for a declared roster -- the operator's member row
    FOR KEY SHARE. Every caller is a writer deciding who may act.

    A section row that is absent answers False: a section thread's
    section is never deleted (no deletion path exists -- the KNOWN
    CEILING P-1 on operators.retag_thread), so this is not a state the
    verbs meet.
    """
    await session.execute(
        select(_recipients.c.id)
        .where(_recipients.c.id == operator)
        .with_for_update(read=True, key_share=True)
    )
    declared = await session.scalar(
        select(Section.roster_declared)
        .where(Section.id == section_id)
        .with_for_update(read=True)
    )
    if declared is None:
        return False
    if not declared:
        return True
    seat = await session.scalar(
        select(SectionMember.operator_id)
        .where(
            SectionMember.section_id == section_id,
            SectionMember.operator_id == operator,
        )
        .with_for_update(read=True, key_share=True)
    )
    return seat is not None


async def member_ids(
    session: AsyncSession, section_id: UUID
) -> Sequence[UUID]:
    """The declared roster of ONE section, in id order.

    Empty for an undeclared section and for an emptied one -- and the
    caller must read that as "nobody was named", NOT as "everybody": a
    push to an unnamed audience is the broadcast this service does not
    do.
    """
    rows = await session.scalars(
        select(SectionMember.operator_id)
        .where(SectionMember.section_id == section_id)
        .order_by(SectionMember.operator_id)
    )
    return list(rows)


async def set_membership(
    session: AsyncSession,
    *,
    section_key: str,
    section_label: str,
    operator_id: UUID,
    member: bool,
) -> None:
    """Apply one membership sync event. Idempotent in both directions.

    `member: true` DECLARES THE ROSTER (sections.roster_declared), before
    the member row is added, and nothing clears it: from then on the
    section is served by its members, and by nobody once they are all
    removed (module header). `member: false` declares nothing -- removing
    somebody who was never there is no roster, and a removal that does
    remove a row is on a section a `member: true` already declared.

    THE SECTION IS CREATED IF ABSENT, unlike group_changed's stance on
    an unknown recipient (which raises and is retried). The orders are
    genuinely different: a recipient is created by an event that is
    already on its way, so waiting works; a section is created by the
    first product call that opens a thread in it, which may be days
    after the operators were hired. Waiting there is not a retry, it is
    a deadlock with a timeout. get_or_create_section is idempotent by
    key and arbitrates its own race in the database.

    The OPERATOR is not created, and its absence is reported the way
    group_changed reports it: NotFoundError, which the consumer
    classifies RETRYABLE and retries with backoff until the identity
    sync catches up. Letting the recipient FK raise instead would look
    the same in the database and behave the opposite way in the queue --
    an IntegrityError is not in the retryable set and would hold a
    perfectly recoverable ordering lag as a defect of comms
    (app/transport/consumer.py, HELD BY SUBJECT).
    """
    section = await get_or_create_section(
        session, key=section_key, label=section_label
    )

    existing = await session.scalar(
        select(SectionMember).where(
            SectionMember.section_id == section.id,
            SectionMember.operator_id == operator_id,
        )
    )

    if member:
        # The section's row before the member row (THE ORDER OF ROW
        # LOCKS); a no-op on a declared section takes no lock.
        await session.execute(
            update(Section)
            .where(Section.id == section.id, Section.roster_declared.is_(False))
            .values(roster_declared=True)
            .execution_options(synchronize_session=False)
        )
        if existing is not None:
            return
        added = await once_more_on_insert_race(
            session,
            lambda: _add_member_once(
                session,
                section_id=section.id,
                section_key=section_key,
                operator_id=operator_id,
            ),
            event="section_member_add_raced",
            section_key=section_key,
            operator_id=str(operator_id),
        )
        if added:
            logger.info(
                "section_member_added",
                section_key=section_key,
                operator_id=str(operator_id),
            )
        return

    if existing is None:
        return
    await session.delete(existing)
    await session.flush()
    logger.info(
        "section_member_removed",
        section_key=section_key,
        operator_id=str(operator_id),
    )


async def _add_member_once(
    session: AsyncSession,
    *,
    section_id: UUID,
    section_key: str,
    operator_id: UUID,
) -> bool:
    """Add the operator to the section's roster; whether THIS call did.

    Run by the insert race's one rule (app/core/database.py
    once_more_on_insert_race): a concurrent identical event that loses
    the insert on the composite key runs this once more and finds the
    winner's row; any other integrity error is raised at once. The
    operator is not created: an unknown one is a NotFoundError, which
    the consumer retries until the identity sync catches up (see
    set_membership).
    """
    existing = await session.scalar(
        select(SectionMember).where(
            SectionMember.section_id == section_id,
            SectionMember.operator_id == operator_id,
        )
    )
    if existing is not None:
        return False
    if not await recipient_exists(session, operator_id):
        raise NotFoundError(
            f"Cannot add unknown recipient {operator_id} to section "
            f"{section_key!r}: the identity sync must precede the "
            f"membership sync"
        )
    session.add(SectionMember(section_id=section_id, operator_id=operator_id))
    await session.flush()
    return True
