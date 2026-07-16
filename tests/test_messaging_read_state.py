# =============================================================================
# COMMS Service -- Read-state tests (Phase 4a, item 4 + mandatory edit 1)
# =============================================================================
# mark_read is monotonic and race-safe: the pointer only moves forward,
# even under two concurrent markers (edit 1). count_unread derives
# unread from message times vs the pointer, excluding the participant's
# own messages; an absent pointer means everything-not-yours counts.
# =============================================================================

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import ThreadReadState
from app.messaging.read_state import count_unread, mark_read
from app.messaging.threads import create_or_get_thread, post_message
from tests.helpers import create_recipient, next_phase4a_telegram_id

_T0 = datetime(2026, 7, 16, 12, 0, 0, tzinfo=UTC)
_T1 = _T0 + timedelta(minutes=5)
_T2 = _T0 + timedelta(minutes=10)


async def _thread_client_operator(
    session: AsyncSession,
) -> tuple[UUID, UUID, UUID]:
    client = await create_recipient(
        session, telegram_id=next_phase4a_telegram_id()
    )
    operator = await create_recipient(
        session, telegram_id=next_phase4a_telegram_id()
    )
    thread = await create_or_get_thread(
        session,
        client=client.id,
        operator_kind=OperatorKind.USER,
        operator_value=operator.id,
        kind=ThreadKind.DM,
    )
    return thread.id, client.id, operator.id


async def _read_pointer(
    session: AsyncSession, thread_id: UUID, participant: UUID
) -> datetime | None:
    return await session.scalar(
        select(ThreadReadState.last_read_at).where(
            ThreadReadState.thread_id == thread_id,
            ThreadReadState.participant == participant,
        )
    )


class TestMarkReadMonotonic:
    async def test_creates_then_advances_forward(
        self, db_session: AsyncSession
    ) -> None:
        thread_id, client_id, _ = await _thread_client_operator(db_session)
        await mark_read(
            db_session,
            thread_id=thread_id,
            participant=client_id,
            last_read_at=_T0,
        )
        assert await _read_pointer(db_session, thread_id, client_id) == _T0

        await mark_read(
            db_session,
            thread_id=thread_id,
            participant=client_id,
            last_read_at=_T2,
        )
        assert await _read_pointer(db_session, thread_id, client_id) == _T2

    async def test_does_not_regress_on_older_mark(
        self, db_session: AsyncSession
    ) -> None:
        thread_id, client_id, _ = await _thread_client_operator(db_session)
        await mark_read(
            db_session,
            thread_id=thread_id,
            participant=client_id,
            last_read_at=_T2,
        )
        # an older timestamp must NOT pull the pointer back
        await mark_read(
            db_session,
            thread_id=thread_id,
            participant=client_id,
            last_read_at=_T0,
        )
        assert await _read_pointer(db_session, thread_id, client_id) == _T2


class TestMarkReadConcurrent:
    async def test_two_concurrent_marks_converge_on_latest(self) -> None:
        """Mandatory edit 1: two concurrent mark_read (T1 < T2) in any
        order -> final pointer == T2 (no lost update / regression)."""
        factory = get_session_factory()
        async with factory() as session:
            thread_id, client_id, _ = await _thread_client_operator(session)
            await session.commit()

        async def worker(when: datetime) -> None:
            async with factory() as session:
                await mark_read(
                    session,
                    thread_id=thread_id,
                    participant=client_id,
                    last_read_at=when,
                )
                await session.commit()

        await asyncio.gather(worker(_T1), worker(_T2))

        async with factory() as session:
            pointer = await _read_pointer(session, thread_id, client_id)
        assert pointer == _T2


class TestCountUnread:
    async def test_excludes_own_and_counts_others_without_pointer(
        self, db_session: AsyncSession
    ) -> None:
        thread_id, client_id, operator_id = await _thread_client_operator(
            db_session
        )
        # two from the operator, one from the client
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=operator_id,
            body="op-1",
            created_at=_T0,
        )
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=operator_id,
            body="op-2",
            created_at=_T1,
        )
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=client_id,
            body="me",
            created_at=_T2,
        )
        # no pointer yet: the client's own message is excluded
        unread = await count_unread(
            db_session, thread_id=thread_id, participant=client_id
        )
        assert unread == 2

    async def test_pointer_limits_to_newer_messages(
        self, db_session: AsyncSession
    ) -> None:
        thread_id, client_id, operator_id = await _thread_client_operator(
            db_session
        )
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=operator_id,
            body="op-1",
            created_at=_T0,
        )
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=operator_id,
            body="op-2",
            created_at=_T2,
        )
        # read up to _T1: only the _T2 operator message remains unread
        await mark_read(
            db_session,
            thread_id=thread_id,
            participant=client_id,
            last_read_at=_T1,
        )
        unread = await count_unread(
            db_session, thread_id=thread_id, participant=client_id
        )
        assert unread == 1

    async def test_zero_when_all_read(
        self, db_session: AsyncSession
    ) -> None:
        thread_id, client_id, operator_id = await _thread_client_operator(
            db_session
        )
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=operator_id,
            body="op-1",
            created_at=_T0,
        )
        await mark_read(
            db_session,
            thread_id=thread_id,
            participant=client_id,
            last_read_at=_T2,
        )
        unread = await count_unread(
            db_session, thread_id=thread_id, participant=client_id
        )
        assert unread == 0
