# =============================================================================
# COMMS Service -- Retag / mutability tests (Phase 4b, item 4)
# =============================================================================
# section threads retag (subject_ref / section) -> re-resolve + reset
# assignee, thread_id + history preserved, dedup key NOT recomputed
# (a colliding retag is rejected -- one thread per entity, never
# merged). user threads are frozen. Section deletion has NO code path.
# =============================================================================

from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.core.exceptions import NotFoundError, ValidationError
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Message, Thread
from app.messaging.operators import SubjectRef, claim_thread, retag_thread
from app.messaging.threads import create_or_get_thread, post_message
from tests.helpers import (
    create_recipient,
    create_section,
    intake_fields,
    next_phase4b_telegram_id,
)


async def _rid(session: AsyncSession) -> UUID:
    r = await create_recipient(session, telegram_id=next_phase4b_telegram_id())
    return r.id


async def _make_section_thread(
    session: AsyncSession,
    *,
    section_id: UUID,
    subject_id: str,
    client_id: UUID,
) -> Thread:
    return await create_or_get_thread(
        session, **intake_fields(),
        client=client_id,
        operator_kind=OperatorKind.SECTION,
        operator_value=section_id,
        kind=ThreadKind.TICKET,
        subject_type="practice",
        subject_id=subject_id,
    )


class TestRetagSection:
    async def test_retag_subject_keeps_thread(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        section = await create_section(db_session, key="rt-sec")
        thread = await _make_section_thread(
            db_session, section_id=section.id, subject_id="old", client_id=client
        )
        original_id = thread.id
        retagged = await retag_thread(
            db_session,
            thread_id=thread.id,
            section=section.id,
            subject=SubjectRef("practice", "new"),
        )
        assert retagged.id == original_id
        assert retagged.subject_id == "new"
        assert retagged.operator_value == section.id

    async def test_retag_to_different_section(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        sec_a = await create_section(db_session, key="rt-a")
        sec_b = await create_section(db_session, key="rt-b")
        thread = await _make_section_thread(
            db_session, section_id=sec_a.id, subject_id="x", client_id=client
        )
        retagged = await retag_thread(
            db_session,
            thread_id=thread.id,
            section=sec_b.id,
            subject=SubjectRef("practice", "x"),
        )
        assert retagged.operator_value == sec_b.id

    async def test_retag_resets_assignee(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        agent = await _rid(db_session)
        section = await create_section(db_session, key="rt-reset")
        thread = await _make_section_thread(
            db_session, section_id=section.id, subject_id="s", client_id=client
        )
        await claim_thread(db_session, thread_id=thread.id, operator=agent)
        assert await db_session.scalar(
            select(Thread.assignee).where(Thread.id == thread.id)
        ) == agent
        await retag_thread(
            db_session,
            thread_id=thread.id,
            section=section.id,
            subject=SubjectRef("practice", "s2"),
        )
        assert await db_session.scalar(
            select(Thread.assignee).where(Thread.id == thread.id)
        ) is None

    async def test_retag_preserves_history(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        section = await create_section(db_session, key="rt-hist")
        thread = await _make_section_thread(
            db_session, section_id=section.id, subject_id="h", client_id=client
        )
        await post_message(
            db_session,
            **intake_fields(),
            thread_id=thread.id,
            sender=client,
            body="before retag",
        )
        await retag_thread(
            db_session,
            thread_id=thread.id,
            section=section.id,
            subject=SubjectRef("practice", "h2"),
        )
        msg_count = await db_session.scalar(
            select(func.count())
            .select_from(Message)
            .where(Message.thread_id == thread.id)
        )
        assert msg_count == 1  # history survived the retag


class TestRetagFrozen:
    async def test_user_thread_frozen(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        master = await _rid(db_session)
        section = await create_section(db_session, key="rt-frozen")
        user_thread = await create_or_get_thread(
            db_session, **intake_fields(), client=client,
            operator_kind=OperatorKind.USER, operator_value=master,
            kind=ThreadKind.DM,
        )
        with pytest.raises(ValidationError):
            await retag_thread(
                db_session,
                thread_id=user_thread.id,
                section=section.id,
                subject=SubjectRef("practice", "nope"),
            )


class TestRetagValidation:
    def test_half_subject_rejected(self) -> None:
        """A half subject_ref cannot reach the operation any more.

        This test used to hand retag_thread subject_type without
        subject_id and expect its both-or-neither refusal. That was
        right about the property -- no half reference is ever stored --
        and D1 / R1 moved where it is held: retag_thread takes ONE
        subject value (SubjectRef | KEEP | None), so a half reference
        cannot be passed at all, and the refusal of a half one sent over
        HTTP is the route's 422 (tests/test_retag_subject.py). What is
        asserted now is the type itself: a SubjectRef without both
        halves cannot be constructed.
        """
        with pytest.raises(TypeError):
            SubjectRef("practice")  # type: ignore[call-arg]

    async def test_unknown_section_rejected(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        section = await create_section(db_session, key="rt-unk")
        thread = await _make_section_thread(
            db_session, section_id=section.id, subject_id="s", client_id=client
        )
        with pytest.raises(NotFoundError):
            await retag_thread(
                db_session,
                thread_id=thread.id,
                section=uuid4(),
                subject=SubjectRef("practice", "s"),
            )

    async def test_unknown_thread_rejected(
        self, db_session: AsyncSession
    ) -> None:
        section = await create_section(db_session, key="rt-unk2")
        with pytest.raises(NotFoundError):
            await retag_thread(
                db_session,
                thread_id=uuid4(),
                section=section.id,
            )


class TestRetagCollision:
    async def test_colliding_retag_rejected(self) -> None:
        """Retagging onto an entity that already has a thread is
        rejected (one thread per entity, never merged). Committed setup
        + a separate session so the failed retag is discarded cleanly."""
        factory = get_session_factory()
        client_id = uuid4()
        async with factory() as session:
            await create_recipient(
                session, recipient_id=client_id,
                telegram_id=next_phase4b_telegram_id(),
            )
            section = await create_section(session, key="rt-collide")
            section_id = section.id
            a = await _make_section_thread(
                session, section_id=section_id, subject_id="X",
                client_id=client_id,
            )
            b = await _make_section_thread(
                session, section_id=section_id, subject_id="Y",
                client_id=client_id,
            )
            a_id, b_id = a.id, b.id
            await session.commit()

        async with factory() as session:
            with pytest.raises(ValidationError):
                await retag_thread(
                    session,
                    thread_id=b_id,
                    section=section_id,
                    subject=SubjectRef("practice", "X"),  # collides with thread A
                )

        # B is unchanged (still Y), A still X -> two distinct threads
        async with factory() as session:
            assert await session.scalar(
                select(Thread.subject_id).where(Thread.id == b_id)
            ) == "Y"
            assert await session.scalar(
                select(Thread.subject_id).where(Thread.id == a_id)
            ) == "X"
