# =============================================================================
# COMMS Service -- Operator resolve + claim tests (Phase 4b, items 1-2)
# =============================================================================
# resolve_operator -> OperatorScope (descriptor, no recipient list);
# D1(i) pre-assign of user threads; atomic claim (conditional UPDATE +
# rowcount) with a concurrent single-winner race. The claim guard
# `assignee IS NULL` rejects user threads for free (no special-case),
# because D1(i) pre-assigns them.
# =============================================================================

import asyncio
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.core.exceptions import NotFoundError
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Thread
from app.messaging.operators import claim_thread, resolve_operator
from app.messaging.threads import create_or_get_thread
from tests.helpers import (
    create_recipient,
    create_section,
    next_phase4b_telegram_id,
)


async def _recipient(session: AsyncSession) -> UUID:
    r = await create_recipient(session, telegram_id=next_phase4b_telegram_id())
    return r.id


async def _section_thread(session: AsyncSession) -> UUID:
    """An unassigned section thread (claimable)."""
    client_id = await _recipient(session)
    section = await create_section(session, key=f"sec-{uuid4().hex[:8]}")
    thread = await create_or_get_thread(
        session,
        client=client_id,
        operator_kind=OperatorKind.SECTION,
        operator_value=section.id,
        kind=ThreadKind.TICKET,
        subject_type="practice",
        subject_id=uuid4().hex,
    )
    return thread.id


async def _assignee_of(session: AsyncSession, thread_id: UUID) -> UUID | None:
    return await session.scalar(
        select(Thread.assignee).where(Thread.id == thread_id)
    )


class TestResolveOperator:
    def test_user_scope_not_claimable(self) -> None:
        value = uuid4()
        scope = resolve_operator(OperatorKind.USER, value)
        assert scope.kind is OperatorKind.USER
        assert scope.value == value
        assert scope.is_claimable is False

    def test_section_scope_claimable(self) -> None:
        scope = resolve_operator(OperatorKind.SECTION, uuid4())
        assert scope.is_claimable is True


class TestUserThreadPreAssign:
    """D1(i): user threads are created with assignee = operator_value."""

    async def test_user_thread_preassigned(
        self, db_session: AsyncSession
    ) -> None:
        client_id = await _recipient(db_session)
        master_id = await _recipient(db_session)
        thread = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=master_id,
            kind=ThreadKind.DM,
        )
        assert thread.assignee == master_id
        assert thread.assigned_at is not None

    async def test_section_thread_unassigned(
        self, db_session: AsyncSession
    ) -> None:
        thread_id = await _section_thread(db_session)
        assert await _assignee_of(db_session, thread_id) is None


class TestClaim:
    async def test_claim_unassigned_succeeds(
        self, db_session: AsyncSession
    ) -> None:
        thread_id = await _section_thread(db_session)
        agent_id = await _recipient(db_session)
        claimed = await claim_thread(
            db_session, thread_id=thread_id, operator=agent_id
        )
        assert claimed is True
        assert await _assignee_of(db_session, thread_id) == agent_id

    async def test_second_claim_rejected(
        self, db_session: AsyncSession
    ) -> None:
        thread_id = await _section_thread(db_session)
        first = await _recipient(db_session)
        second = await _recipient(db_session)
        assert await claim_thread(
            db_session, thread_id=thread_id, operator=first
        ) is True
        assert await claim_thread(
            db_session, thread_id=thread_id, operator=second
        ) is False
        # ownership stays with the first claimer
        assert await _assignee_of(db_session, thread_id) == first

    async def test_user_thread_not_claimable(
        self, db_session: AsyncSession
    ) -> None:
        """No special-case: the pre-assigned assignee makes `assignee
        IS NULL` miss, so claim returns False and the master stays."""
        client_id = await _recipient(db_session)
        master_id = await _recipient(db_session)
        thread = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=master_id,
            kind=ThreadKind.DM,
        )
        other = await _recipient(db_session)
        assert await claim_thread(
            db_session, thread_id=thread.id, operator=other
        ) is False
        assert await _assignee_of(db_session, thread.id) == master_id

    async def test_claim_unknown_thread_raises(
        self, db_session: AsyncSession
    ) -> None:
        agent_id = await _recipient(db_session)
        with pytest.raises(NotFoundError):
            await claim_thread(
                db_session, thread_id=uuid4(), operator=agent_id
            )


class TestConcurrentClaim:
    async def test_single_winner(self) -> None:
        """Two agents claim the same section thread concurrently -> one
        True, one False; final assignee is the winner."""
        factory = get_session_factory()
        client_id = uuid4()
        op_a = uuid4()
        op_b = uuid4()
        async with factory() as session:
            for rid in (client_id, op_a, op_b):
                await create_recipient(
                    session,
                    recipient_id=rid,
                    telegram_id=next_phase4b_telegram_id(),
                )
            section = await create_section(session, key="race-sec")
            thread = await create_or_get_thread(
                session,
                client=client_id,
                operator_kind=OperatorKind.SECTION,
                operator_value=section.id,
                kind=ThreadKind.TICKET,
                subject_type="practice",
                subject_id="race",
            )
            thread_id = thread.id
            await session.commit()

        async def worker(agent: UUID) -> bool:
            async with factory() as session:
                claimed = await claim_thread(
                    session, thread_id=thread_id, operator=agent
                )
                await session.commit()
                return claimed

        results = await asyncio.gather(worker(op_a), worker(op_b))
        assert results.count(True) == 1
        assert results.count(False) == 1

        winner = op_a if results[0] else op_b
        async with factory() as session:
            assert await _assignee_of(session, thread_id) == winner
