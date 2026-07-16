# =============================================================================
# COMMS Service -- Thread dedup / creation tests (Phase 4a, item 2)
# =============================================================================
# The thread_id invariant (arch doc §2.4):
#   - subjectless dm     -> one eternal thread per (client, operator)
#   - subjectless ticket -> a fresh thread every time
#   - subject present    -> one thread per (client, operator, subject),
#                           KIND-AGNOSTIC (dm and ticket collapse)
# Operator referent validation, both forms (mandatory edit 2). Race:
# two concurrent creators of the same dm -> one row.
# =============================================================================

import asyncio
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.core.exceptions import NotFoundError, ValidationError
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Thread
from app.messaging.threads import create_or_get_thread
from tests.helpers import (
    create_recipient,
    create_section,
    next_phase4a_telegram_id,
)


async def _client_and_operator(session: AsyncSession) -> tuple[UUID, UUID]:
    client = await create_recipient(
        session, telegram_id=next_phase4a_telegram_id()
    )
    operator = await create_recipient(
        session, telegram_id=next_phase4a_telegram_id()
    )
    return client.id, operator.id


async def _thread_count(session: AsyncSession) -> int:
    return await session.scalar(select(func.count()).select_from(Thread)) or 0


class TestSubjectlessDedup:
    async def test_dm_dedups_to_one(
        self, db_session: AsyncSession
    ) -> None:
        client_id, op_id = await _client_and_operator(db_session)
        first = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.DM,
        )
        second = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.DM,
        )
        assert first.id == second.id
        assert await _thread_count(db_session) == 1

    async def test_ticket_is_always_new(
        self, db_session: AsyncSession
    ) -> None:
        client_id, op_id = await _client_and_operator(db_session)
        first = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.TICKET,
        )
        second = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.TICKET,
        )
        assert first.id != second.id
        assert await _thread_count(db_session) == 2


class TestSubjectDedup:
    async def test_subject_dedups_across_kind(
        self, db_session: AsyncSession
    ) -> None:
        """Same subject -> same thread even if kind differs (the subject
        key excludes kind)."""
        client_id, op_id = await _client_and_operator(db_session)
        as_ticket = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.TICKET,
            subject_type="practice",
            subject_id="42",
        )
        as_dm = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.DM,
            subject_type="practice",
            subject_id="42",
        )
        assert as_ticket.id == as_dm.id
        assert await _thread_count(db_session) == 1

    async def test_different_subject_distinct_threads(
        self, db_session: AsyncSession
    ) -> None:
        client_id, op_id = await _client_and_operator(db_session)
        a = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.TICKET,
            subject_type="practice",
            subject_id="1",
        )
        b = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.TICKET,
            subject_type="practice",
            subject_id="2",
        )
        assert a.id != b.id
        assert await _thread_count(db_session) == 2


class TestOperatorReferentValidation:
    """Mandatory edit 2: both operator forms are validated before
    insert; a dangling operator is refused and no thread row is left."""

    async def test_user_operator_must_exist(
        self, db_session: AsyncSession
    ) -> None:
        client = await create_recipient(
            db_session, telegram_id=next_phase4a_telegram_id()
        )
        with pytest.raises(NotFoundError):
            await create_or_get_thread(
                db_session,
                client=client.id,
                operator_kind=OperatorKind.USER,
                operator_value=uuid4(),  # no such recipient
                kind=ThreadKind.DM,
            )
        assert await _thread_count(db_session) == 0

    async def test_section_operator_must_exist(
        self, db_session: AsyncSession
    ) -> None:
        client = await create_recipient(
            db_session, telegram_id=next_phase4a_telegram_id()
        )
        with pytest.raises(NotFoundError):
            await create_or_get_thread(
                db_session,
                client=client.id,
                operator_kind=OperatorKind.SECTION,
                operator_value=uuid4(),  # no such section
                kind=ThreadKind.DM,
            )
        assert await _thread_count(db_session) == 0

    async def test_user_operator_ok_when_present(
        self, db_session: AsyncSession
    ) -> None:
        client_id, op_id = await _client_and_operator(db_session)
        thread = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.DM,
        )
        assert thread.operator_value == op_id

    async def test_section_operator_ok_when_present(
        self, db_session: AsyncSession
    ) -> None:
        client = await create_recipient(
            db_session, telegram_id=next_phase4a_telegram_id()
        )
        section = await create_section(db_session, key="support")
        thread = await create_or_get_thread(
            db_session,
            client=client.id,
            operator_kind=OperatorKind.SECTION,
            operator_value=section.id,
            kind=ThreadKind.DM,
        )
        assert thread.operator_kind == OperatorKind.SECTION
        assert thread.operator_value == section.id


class TestSubjectRefValidation:
    async def test_half_subject_ref_rejected(
        self, db_session: AsyncSession
    ) -> None:
        client_id, op_id = await _client_and_operator(db_session)
        with pytest.raises(ValidationError):
            await create_or_get_thread(
                db_session,
                client=client_id,
                operator_kind=OperatorKind.USER,
                operator_value=op_id,
                kind=ThreadKind.TICKET,
                subject_type="practice",
                subject_id=None,
            )
        assert await _thread_count(db_session) == 0


class TestConcurrentCreate:
    async def test_two_creators_one_dm(self) -> None:
        """Two concurrent create-or-get for the same dm -> one row, both
        callers see the same thread (DB-arbitrated race)."""
        factory = get_session_factory()
        client_id = uuid4()
        op_id = uuid4()
        async with factory() as session:
            await create_recipient(
                session,
                recipient_id=client_id,
                telegram_id=next_phase4a_telegram_id(),
            )
            await create_recipient(
                session,
                recipient_id=op_id,
                telegram_id=next_phase4a_telegram_id(),
            )
            await session.commit()

        async def worker() -> str:
            async with factory() as session:
                thread = await create_or_get_thread(
                    session,
                    client=client_id,
                    operator_kind=OperatorKind.USER,
                    operator_value=op_id,
                    kind=ThreadKind.DM,
                )
                await session.commit()
                return str(thread.id)

        id_a, id_b = await asyncio.gather(worker(), worker())
        assert id_a == id_b

        async with factory() as session:
            count = await _thread_count(session)
        assert count == 1
