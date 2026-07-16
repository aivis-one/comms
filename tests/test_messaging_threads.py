# =============================================================================
# COMMS Service -- Message posting tests (Phase 4a, item 3)
# =============================================================================
# post_message appends to an existing thread, advances last_message_at
# monotonically (the 4b auto-close socket), and messages read back in
# created_at order. Posting to an unknown thread raises NotFoundError.
# =============================================================================

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundError
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Message, Thread
from app.messaging.threads import create_or_get_thread, post_message
from tests.helpers import create_recipient, next_phase4a_telegram_id

_T0 = datetime(2026, 7, 16, 12, 0, 0, tzinfo=UTC)


async def _dm_thread(session: AsyncSession) -> tuple[UUID, UUID]:
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
    return thread.id, client.id


class TestPostMessage:
    async def test_appends_message(
        self, db_session: AsyncSession
    ) -> None:
        thread_id, client_id = await _dm_thread(db_session)
        message = await post_message(
            db_session,
            thread_id=thread_id,
            sender=client_id,
            body="hello",
            created_at=_T0,
        )
        assert message.thread_id == thread_id
        assert message.sender == client_id
        assert message.body == "hello"

    async def test_unknown_thread_raises(
        self, db_session: AsyncSession
    ) -> None:
        sender = await create_recipient(
            db_session, telegram_id=next_phase4a_telegram_id()
        )
        with pytest.raises(NotFoundError):
            await post_message(
                db_session,
                thread_id=uuid4(),
                sender=sender.id,
                body="nowhere",
            )


class TestLastMessageAt:
    async def test_bumps_forward_then_holds_on_backfill(
        self, db_session: AsyncSession
    ) -> None:
        thread_id, client_id = await _dm_thread(db_session)
        thread = await db_session.get(Thread, thread_id)
        assert thread is not None
        assert thread.last_message_at is None

        await post_message(
            db_session,
            thread_id=thread_id,
            sender=client_id,
            body="first",
            created_at=_T0,
        )
        await db_session.refresh(thread)
        assert thread.last_message_at == _T0

        # a later message advances the marker
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=client_id,
            body="later",
            created_at=_T0 + timedelta(minutes=5),
        )
        await db_session.refresh(thread)
        assert thread.last_message_at == _T0 + timedelta(minutes=5)

        # a backfilled earlier message must NOT pull the marker back
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=client_id,
            body="backfill",
            created_at=_T0 - timedelta(minutes=5),
        )
        await db_session.refresh(thread)
        assert thread.last_message_at == _T0 + timedelta(minutes=5)


class TestOrderedRead:
    async def test_messages_read_in_created_at_order(
        self, db_session: AsyncSession
    ) -> None:
        thread_id, client_id = await _dm_thread(db_session)
        # insert out of chronological order
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=client_id,
            body="second",
            created_at=_T0 + timedelta(minutes=1),
        )
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=client_id,
            body="first",
            created_at=_T0,
        )
        await post_message(
            db_session,
            thread_id=thread_id,
            sender=client_id,
            body="third",
            created_at=_T0 + timedelta(minutes=2),
        )
        bodies = (
            await db_session.scalars(
                select(Message.body)
                .where(Message.thread_id == thread_id)
                .order_by(Message.created_at)
            )
        ).all()
        assert list(bodies) == ["first", "second", "third"]
