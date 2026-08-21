# =============================================================================
# COMMS Service -- Section membership (T-67)
# =============================================================================
#
# THE LOAD-BEARING TESTS HERE ARE THE EMPTY ONES. This service is
# deployed twice from one tree, and the other deployment has section
# threads in production and no roster at all. So "an empty roster
# behaves exactly as before membership existed" is not a courtesy to a
# hypothetical configuration -- it is the contract that keeps a live
# system working, and it is asserted BY NAME in all three places the
# rule is read:
#
#   1. visibility          -- the unclaimed pool is still visible to
#                             every operator, named thread by thread;
#   2. operate / claim     -- an operator with no roster seat still
#                             passes both, as they did before;
#   3. the pool push       -- an unclaimed thread still notifies NOBODY,
#                             which is what the deferred marker
#                             promised for as long as no roster exists.
#
# "Did not raise" would satisfy none of those. Each test states the
# result it expects and would fail if the answer merely changed shape.
#
# The non-empty half then shows the narrowing: declared members serve,
# outsiders do not, and the push reaches the roster and nobody else.
#
# Band 92200-92259 (see tests/helpers.next_t67_telegram_id).
# =============================================================================

from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundError
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.membership import (
    member_ids,
    serves_section,
    set_membership,
)
from app.messaging.models import SectionMember
from app.messaging.sections import get_or_create_section
from app.messaging.operators import (
    can_claim,
    can_operate,
    list_visible_threads,
)
from app.messaging.threads import create_or_get_thread, post_message
from app.notifier import notify_new_message
from tests.helpers import (
    create_recipient,
    create_section,
    next_t67_telegram_id,
)


async def _rid(session: AsyncSession) -> UUID:
    recipient = await create_recipient(
        session, telegram_id=next_t67_telegram_id()
    )
    return recipient.id


async def _section_thread(
    session: AsyncSession, *, client: UUID, section_id: UUID
):
    return await create_or_get_thread(
        session,
        client=client,
        operator_kind=OperatorKind.SECTION,
        operator_value=section_id,
        kind=ThreadKind.TICKET,
    )


async def _visible_ids(
    session: AsyncSession, operator: UUID
) -> set[UUID]:
    threads, _ = await list_visible_threads(session, operator=operator)
    return {t.id for t in threads}


# ---------------------------------------------------------------------------
# 1. An empty roster is not a gap -- it is the old behaviour, exactly
# ---------------------------------------------------------------------------


class TestEmptyMembershipIsTodaysBehaviour:
    async def test_an_undeclared_pool_is_still_visible_to_everyone(
        self, db_session: AsyncSession
    ) -> None:
        """Place 1 of 3. Named, not incidental: the thread the operator
        must see is asserted by id, so a change that returned an empty
        page would fail here rather than pass quietly."""
        client = await _rid(db_session)
        stranger = await _rid(db_session)
        section = await create_section(db_session, key="t67-empty-vis")
        thread = await _section_thread(
            db_session, client=client, section_id=section.id
        )

        assert thread.id in await _visible_ids(db_session, stranger)

    async def test_an_undeclared_section_is_operated_by_anyone(
        self, db_session: AsyncSession
    ) -> None:
        """Place 2 of 3, both verbs. Before membership, `section` meant
        `anybody` for status, retag and claim alike."""
        client = await _rid(db_session)
        stranger = await _rid(db_session)
        section = await create_section(db_session, key="t67-empty-op")
        thread = await _section_thread(
            db_session, client=client, section_id=section.id
        )

        assert await can_operate(db_session, thread, stranger) is True
        assert await can_claim(db_session, thread, stranger) is True
        assert await serves_section(db_session, section.id, stranger) is True

    async def test_an_undeclared_pool_still_notifies_nobody(
        self, db_session: AsyncSession, stub_profile: None
    ) -> None:
        """Place 3 of 3. Zero, not 'some smaller number': an unclaimed
        thread with no roster is exactly as silent as it was when the
        pool push did not exist."""
        client = await _rid(db_session)
        section = await create_section(db_session, key="t67-empty-push")
        thread = await _section_thread(
            db_session, client=client, section_id=section.id
        )
        message = await post_message(
            db_session, thread_id=thread.id, sender=client, body="hello?"
        )

        created = await notify_new_message(
            db_session, thread=thread, message=message
        )

        assert created == []


# ---------------------------------------------------------------------------
# 2. A declared roster narrows all three
# ---------------------------------------------------------------------------


class TestDeclaredMembership:
    async def test_a_declared_pool_is_hidden_from_outsiders(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        member = await _rid(db_session)
        outsider = await _rid(db_session)
        section = await create_section(db_session, key="t67-vis")
        thread = await _section_thread(
            db_session, client=client, section_id=section.id
        )
        db_session.add(
            SectionMember(section_id=section.id, operator_id=member)
        )
        await db_session.flush()

        assert thread.id in await _visible_ids(db_session, member)
        assert thread.id not in await _visible_ids(db_session, outsider)

    async def test_an_outsider_neither_operates_nor_claims(
        self, db_session: AsyncSession
    ) -> None:
        """The window and the door together: narrowing status/retag
        while leaving claim open would let an outsider become the
        assignee and inherit both."""
        client = await _rid(db_session)
        member = await _rid(db_session)
        outsider = await _rid(db_session)
        section = await create_section(db_session, key="t67-authz")
        thread = await _section_thread(
            db_session, client=client, section_id=section.id
        )
        db_session.add(
            SectionMember(section_id=section.id, operator_id=member)
        )
        await db_session.flush()

        assert await can_operate(db_session, thread, member) is True
        assert await can_claim(db_session, thread, member) is True
        assert await can_operate(db_session, thread, outsider) is False
        assert await can_claim(db_session, thread, outsider) is False

    async def test_the_assignee_operates_even_after_losing_the_seat(
        self, db_session: AsyncSession
    ) -> None:
        """Whoever took the conversation finishes it. Losing a roster
        seat mid-thread must not strand a conversation someone is
        already answering."""
        client = await _rid(db_session)
        former = await _rid(db_session)
        other = await _rid(db_session)
        section = await create_section(db_session, key="t67-assignee")
        thread = await _section_thread(
            db_session, client=client, section_id=section.id
        )
        thread.assignee = former
        db_session.add(
            SectionMember(section_id=section.id, operator_id=other)
        )
        await db_session.flush()

        assert await can_operate(db_session, thread, former) is True

    async def test_the_pool_push_reaches_the_roster_and_nobody_else(
        self, db_session: AsyncSession, stub_profile: None
    ) -> None:
        client = await _rid(db_session)
        first = await _rid(db_session)
        second = await _rid(db_session)
        outsider = await _rid(db_session)
        section = await create_section(db_session, key="t67-push")
        thread = await _section_thread(
            db_session, client=client, section_id=section.id
        )
        for operator in (first, second):
            db_session.add(
                SectionMember(section_id=section.id, operator_id=operator)
            )
        await db_session.flush()
        message = await post_message(
            db_session, thread_id=thread.id, sender=client, body="help"
        )

        created = await notify_new_message(
            db_session, thread=thread, message=message
        )

        pinged = {UUID(n.target_value) for n in created}
        assert pinged == {first, second}
        assert outsider not in pinged

    async def test_a_member_who_wrote_does_not_ping_themselves(
        self, db_session: AsyncSession, stub_profile: None
    ) -> None:
        """Reachable, not theoretical: staff opening a request of their
        own are the client AND on the roster."""
        staff = await _rid(db_session)
        colleague = await _rid(db_session)
        section = await create_section(db_session, key="t67-self")
        thread = await _section_thread(
            db_session, client=staff, section_id=section.id
        )
        for operator in (staff, colleague):
            db_session.add(
                SectionMember(section_id=section.id, operator_id=operator)
            )
        await db_session.flush()
        message = await post_message(
            db_session, thread_id=thread.id, sender=staff, body="mine"
        )

        created = await notify_new_message(
            db_session, thread=thread, message=message
        )

        assert {UUID(n.target_value) for n in created} == {colleague}

    async def test_a_claimed_thread_pings_the_assignee_not_the_roster(
        self, db_session: AsyncSession, stub_profile: None
    ) -> None:
        """The branches are exclusive: a claimed thread keeps the
        behaviour it always had, and the roster does not get a second
        copy of it."""
        client = await _rid(db_session)
        assignee = await _rid(db_session)
        bystander = await _rid(db_session)
        section = await create_section(db_session, key="t67-claimed")
        thread = await _section_thread(
            db_session, client=client, section_id=section.id
        )
        thread.assignee = assignee
        for operator in (assignee, bystander):
            db_session.add(
                SectionMember(section_id=section.id, operator_id=operator)
            )
        await db_session.flush()
        message = await post_message(
            db_session, thread_id=thread.id, sender=client, body="again"
        )

        created = await notify_new_message(
            db_session, thread=thread, message=message
        )

        assert {UUID(n.target_value) for n in created} == {assignee}


# ---------------------------------------------------------------------------
# 3. The sync itself
# ---------------------------------------------------------------------------


class TestMembershipSync:
    async def test_adding_twice_leaves_one_row(
        self, db_session: AsyncSession
    ) -> None:
        operator = await _rid(db_session)
        for _ in range(2):
            await set_membership(
                db_session,
                section_key="t67-sync",
                section_label="Sync",
                operator_id=operator,
                member=True,
            )

        section_ids = await db_session.scalars(
            SectionMember.__table__.select().where(
                SectionMember.operator_id == operator
            )
        )
        assert len(list(section_ids)) == 1

    async def test_removing_is_idempotent_in_both_directions(
        self, db_session: AsyncSession
    ) -> None:
        operator = await _rid(db_session)
        await set_membership(
            db_session,
            section_key="t67-remove",
            section_label="Remove",
            operator_id=operator,
            member=True,
        )
        for _ in range(2):
            await set_membership(
                db_session,
                section_key="t67-remove",
                section_label="Remove",
                operator_id=operator,
                member=False,
            )

        section = await create_section(db_session, key="t67-remove-probe")
        assert await member_ids(db_session, section.id) == []

    async def test_an_unknown_section_is_created_not_awaited(
        self, db_session: AsyncSession
    ) -> None:
        """Operators are appointed before anybody writes in, so waiting
        for the section to appear would be a retry with no end.

        The section is read back BY KEY through the service's own
        lookup, not created a second time: the first version of this
        test called the create helper again and was asserting against a
        duplicate row rather than the one the sync made.
        """
        operator = await _rid(db_session)

        await set_membership(
            db_session,
            section_key="t67-brand-new",
            section_label="Brand new",
            operator_id=operator,
            member=True,
        )

        section = await get_or_create_section(
            db_session, key="t67-brand-new", label="Brand new"
        )
        assert await member_ids(db_session, section.id) == [operator]
        assert await serves_section(db_session, section.id, operator) is True

    async def test_an_unsynced_operator_is_retryable_not_fatal(
        self, db_session: AsyncSession
    ) -> None:
        """An identity lag must come back as the error the consumer
        retries -- a raw integrity failure would go to the dead-letter
        queue and the roster would silently lose a person."""
        from uuid import uuid4

        with pytest.raises(NotFoundError):
            await set_membership(
                db_session,
                section_key="t67-unsynced",
                section_label="Unsynced",
                operator_id=uuid4(),
                member=True,
            )
