# =============================================================================
# COMMS Service -- Messaging: Section membership (T-67)
# =============================================================================
#
# THE ONE RULE, WRITTEN ONCE:
#
#   A SECTION WITH NO DECLARED MEMBERS IS SERVED BY ANY OPERATOR.
#   A SECTION WITH MEMBERS IS SERVED BY ITS MEMBERS.
#
# This is a DEFINITION, not a migration window. Before this module,
# "section" meant "everybody" everywhere -- trivial membership, stated in
# operators.OperatorScope. A product that declares no roster keeps
# exactly that behaviour, byte for byte, for as long as it declares
# none; nothing has to be undone the day it declares one.
#
# The rule is consumed in three places and lives in NONE of them:
#   - visibility  (operators.list_visible_threads)   -- what a pool shows;
#   - operate/claim authz (operators.can_operate / can_claim);
#   - the pool push (notifier.notify_new_message).
# Three copies of "or the section is empty" would be three chances to
# disagree, and the one that disagreed would either hide a queue or leak
# one. Hence serves_section() below, and hence the SQL form of the same
# rule (section_serves_clause) rather than a second hand-written EXISTS.
#
# NO MATERIALIZED AUDIENCE (BL-1). member_ids() returns the DECLARED
# roster of ONE section -- a bounded list the product wrote down -- and
# never "every agent" or "every recipient". The pool push fans out over
# those rows and stops there; broadcasting to all recipients in the
# absence of a roster stays rejected, exactly as the deferred marker
# said before this module existed.
# =============================================================================

from collections.abc import Sequence
from uuid import UUID

import structlog
from sqlalchemy import ColumnElement, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app.core.database import once_more_on_insert_race
from app.core.exceptions import NotFoundError
from app.messaging.models import SectionMember
from app.messaging.sections import get_or_create_section
from app.messaging.threads import recipient_exists

logger = structlog.get_logger()


def section_serves_clause(
    section_column: ColumnElement[UUID] | InstrumentedAttribute[UUID],
    operator: UUID,
) -> ColumnElement[bool]:
    """The rule as a SQL predicate over a query's section column.

    Same rule as serves_section(), expressed for a query that has not
    loaded the rows yet: EITHER the section has no roster at all, OR
    this operator is on it. Written here, next to the boolean form, so
    the two cannot drift apart.

    `section_column` is the column carrying the section id in the outer
    query (threads.operator_value), so the EXISTS correlates with it.
    The union in the signature is not decoration: a mapped attribute
    (Thread.operator_value) is an InstrumentedAttribute, not a plain
    ColumnElement, and strict typing rejects the narrower annotation --
    which is exactly how this was found.
    """
    roster = select(SectionMember.section_id).where(
        SectionMember.section_id == section_column
    )
    return or_(
        ~roster.exists(),
        roster.where(SectionMember.operator_id == operator).exists(),
    )


async def has_members(session: AsyncSession, section_id: UUID) -> bool:
    """True if anyone at all is declared for this section."""
    found = await session.scalar(
        select(SectionMember.operator_id)
        .where(SectionMember.section_id == section_id)
        .limit(1)
    )
    return found is not None


async def serves_section(
    session: AsyncSession, section_id: UUID, operator: UUID
) -> bool:
    """Does this operator serve this section? (The rule, in booleans.)"""
    mine = await session.scalar(
        select(SectionMember.operator_id).where(
            SectionMember.section_id == section_id,
            SectionMember.operator_id == operator,
        )
    )
    if mine is not None:
        return True
    return not await has_members(session, section_id)


async def member_ids(
    session: AsyncSession, section_id: UUID
) -> Sequence[UUID]:
    """The declared roster of ONE section, in id order.

    Empty for an undeclared section -- and the caller must read that as
    "nobody was named", NOT as "everybody": a push to an unnamed
    audience is the broadcast this service does not do.
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
    an IntegrityError is not in the retryable set and would send a
    perfectly recoverable ordering lag straight to the dead-letter
    queue.
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
