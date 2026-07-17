# =============================================================================
# COMMS Service -- Status transition tests (Phase 4b, item 5)
# =============================================================================
# D5 matrix (manual): open->resolved, resolved->closed, open->closed;
# X->X no-op; manual reopen / closed->resolved / backward rejected.
# D6: reaching `closed` marks a SECTION thread notifiable (manual path
# here); user/DM never. Auto-reopen: a CLIENT message revives a
# resolved/closed thread and CLEARS the close-notify marker.
# =============================================================================

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundError, ValidationError
from app.messaging.constants import OperatorKind, ThreadKind, ThreadStatus
from app.messaging.models import Thread
from app.messaging.status import set_status
from app.messaging.threads import create_or_get_thread, post_message
from tests.helpers import (
    create_recipient,
    create_section,
    next_phase4b_telegram_id,
)


async def _rid(session: AsyncSession) -> UUID:
    r = await create_recipient(session, telegram_id=next_phase4b_telegram_id())
    return r.id


async def _dm_thread(session: AsyncSession) -> tuple[Thread, UUID]:
    """A user/DM thread; returns (thread, client_id)."""
    client = await _rid(session)
    master = await _rid(session)
    thread = await create_or_get_thread(
        session, client=client,
        operator_kind=OperatorKind.USER, operator_value=master,
        kind=ThreadKind.DM,
    )
    return thread, client


async def _section_thread(session: AsyncSession) -> tuple[Thread, UUID]:
    """A section/ticket thread; returns (thread, client_id)."""
    client = await _rid(session)
    section = await create_section(session, key=f"st-{uuid4().hex[:8]}")
    thread = await create_or_get_thread(
        session, client=client,
        operator_kind=OperatorKind.SECTION, operator_value=section.id,
        kind=ThreadKind.TICKET, subject_type="practice", subject_id="s",
    )
    return thread, client


async def _status_of(session: AsyncSession, tid: UUID) -> str | None:
    return await session.scalar(select(Thread.status).where(Thread.id == tid))


async def _marker_of(
    session: AsyncSession, tid: UUID
) -> datetime | None:
    return await session.scalar(
        select(Thread.close_notify_pending_at).where(Thread.id == tid)
    )


class TestManualMatrix:
    async def test_open_to_resolved(self, db_session: AsyncSession) -> None:
        thread, _ = await _dm_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.RESOLVED
        )
        assert await _status_of(db_session, thread.id) == "resolved"

    async def test_resolved_to_closed(self, db_session: AsyncSession) -> None:
        thread, _ = await _dm_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.RESOLVED
        )
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.CLOSED
        )
        assert await _status_of(db_session, thread.id) == "closed"

    async def test_open_to_closed_direct(
        self, db_session: AsyncSession
    ) -> None:
        thread, _ = await _dm_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.CLOSED
        )
        assert await _status_of(db_session, thread.id) == "closed"

    async def test_same_status_noop(self, db_session: AsyncSession) -> None:
        thread, _ = await _dm_thread(db_session)
        result = await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.OPEN
        )
        assert result.status == ThreadStatus.OPEN
        assert await _status_of(db_session, thread.id) == "open"

    async def test_manual_reopen_rejected(
        self, db_session: AsyncSession
    ) -> None:
        thread, _ = await _dm_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.RESOLVED
        )
        with pytest.raises(ValidationError):
            await set_status(
                db_session, thread_id=thread.id, target=ThreadStatus.OPEN
            )

    async def test_closed_to_open_rejected(
        self, db_session: AsyncSession
    ) -> None:
        thread, _ = await _dm_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.CLOSED
        )
        with pytest.raises(ValidationError):
            await set_status(
                db_session, thread_id=thread.id, target=ThreadStatus.OPEN
            )

    async def test_closed_to_resolved_rejected(
        self, db_session: AsyncSession
    ) -> None:
        thread, _ = await _dm_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.CLOSED
        )
        with pytest.raises(ValidationError):
            await set_status(
                db_session, thread_id=thread.id, target=ThreadStatus.RESOLVED
            )

    async def test_unknown_thread_rejected(
        self, db_session: AsyncSession
    ) -> None:
        with pytest.raises(NotFoundError):
            await set_status(
                db_session, thread_id=uuid4(), target=ThreadStatus.RESOLVED
            )


class TestCloseNotifyMarker:
    async def test_section_close_marks_notify(
        self, db_session: AsyncSession
    ) -> None:
        thread, _ = await _section_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.CLOSED
        )
        assert await _marker_of(db_session, thread.id) is not None

    async def test_user_close_does_not_mark(
        self, db_session: AsyncSession
    ) -> None:
        thread, _ = await _dm_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.CLOSED
        )
        assert await _marker_of(db_session, thread.id) is None

    async def test_section_resolve_does_not_mark(
        self, db_session: AsyncSession
    ) -> None:
        """Only reaching `closed` marks -- resolved does not."""
        thread, _ = await _section_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.RESOLVED
        )
        assert await _marker_of(db_session, thread.id) is None


class TestClientAutoReopen:
    async def test_client_message_reopens_resolved(
        self, db_session: AsyncSession
    ) -> None:
        thread, client = await _dm_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.RESOLVED
        )
        await post_message(
            db_session, thread_id=thread.id, sender=client, body="hi again"
        )
        assert await _status_of(db_session, thread.id) == "open"

    async def test_client_message_reopens_and_clears_marker(
        self, db_session: AsyncSession
    ) -> None:
        thread, client = await _section_thread(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.CLOSED
        )
        assert await _marker_of(db_session, thread.id) is not None
        await post_message(
            db_session, thread_id=thread.id, sender=client, body="reopen pls"
        )
        assert await _status_of(db_session, thread.id) == "open"
        assert await _marker_of(db_session, thread.id) is None

    async def test_operator_message_does_not_reopen(
        self, db_session: AsyncSession
    ) -> None:
        thread, _ = await _section_thread(db_session)
        agent = await _rid(db_session)
        await set_status(
            db_session, thread_id=thread.id, target=ThreadStatus.RESOLVED
        )
        await post_message(
            db_session, thread_id=thread.id, sender=agent, body="agent note"
        )
        assert await _status_of(db_session, thread.id) == "resolved"

    async def test_client_message_open_stays_open(
        self, db_session: AsyncSession
    ) -> None:
        thread, client = await _dm_thread(db_session)
        await post_message(
            db_session, thread_id=thread.id, sender=client, body="hello",
            created_at=datetime.now(UTC),
        )
        assert await _status_of(db_session, thread.id) == "open"
