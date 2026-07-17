# =============================================================================
# COMMS Service -- Auto-close tests (Phase 4b, item 6)
# =============================================================================
# Idle = COALESCE(last_message_at, created_at) < cutoff AND not closed.
# D8: empty threads age from created_at; a recent message resets the
# clock (COALESCE picks last_message_at). Section closes are marked
# notifiable (D6), user/DM are silent. Disabled (days<=0) is a no-op.
# The orchestrator uses its own sessions -> committed setup + separate
# assertion sessions (mirrors the retention tests); the commit-free
# batch is exercised directly on db_session.
# =============================================================================

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.database import get_session_factory
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Thread
from app.messaging.processor import auto_close_idle_threads
from app.messaging.status import auto_close_idle_threads_batch
from app.messaging.threads import create_or_get_thread, post_message
from tests.helpers import (
    create_recipient,
    create_section,
    next_phase4b_telegram_id,
)

Factory = async_sessionmaker[AsyncSession]


async def _new_client(factory: Factory) -> UUID:
    cid = uuid4()
    async with factory() as s:
        await create_recipient(
            s, recipient_id=cid, telegram_id=next_phase4b_telegram_id()
        )
        await s.commit()
    return cid


async def _backdate(
    factory: Factory,
    thread_id: UUID,
    *,
    created_days_ago: int,
    last_msg_days_ago: int | None = None,
) -> None:
    now = datetime.now(UTC)
    last = (
        None if last_msg_days_ago is None
        else now - timedelta(days=last_msg_days_ago)
    )
    async with factory() as s:
        await s.execute(
            update(Thread)
            .where(Thread.id == thread_id)
            .values(
                created_at=now - timedelta(days=created_days_ago),
                last_message_at=last,
            )
        )
        await s.commit()


async def _status(factory: Factory, tid: UUID) -> str | None:
    async with factory() as s:
        return await s.scalar(select(Thread.status).where(Thread.id == tid))


async def _marker(factory: Factory, tid: UUID) -> datetime | None:
    async with factory() as s:
        return await s.scalar(
            select(Thread.close_notify_pending_at).where(Thread.id == tid)
        )


class TestAutoClosePass:
    async def test_idle_section_thread_closes_and_marks(self) -> None:
        factory = get_session_factory()
        client = await _new_client(factory)
        async with factory() as s:
            section = await create_section(s, key="ac-sec")
            thread = await create_or_get_thread(
                s, client=client,
                operator_kind=OperatorKind.SECTION, operator_value=section.id,
                kind=ThreadKind.TICKET, subject_type="practice", subject_id="i",
            )
            tid = thread.id
            await s.commit()
        await _backdate(factory, tid, created_days_ago=40)  # empty, no message

        closed = await auto_close_idle_threads(days=30)
        assert closed >= 1
        assert await _status(factory, tid) == "closed"
        assert await _marker(factory, tid) is not None

    async def test_disabled_is_noop(self) -> None:
        factory = get_session_factory()
        client = await _new_client(factory)
        async with factory() as s:
            section = await create_section(s, key="ac-off")
            thread = await create_or_get_thread(
                s, client=client,
                operator_kind=OperatorKind.SECTION, operator_value=section.id,
                kind=ThreadKind.TICKET, subject_type="practice", subject_id="o",
            )
            tid = thread.id
            await s.commit()
        await _backdate(factory, tid, created_days_ago=40)

        assert await auto_close_idle_threads(days=0) == 0
        assert await _status(factory, tid) == "open"

    async def test_user_dm_closed_silently(self) -> None:
        factory = get_session_factory()
        client = await _new_client(factory)
        master = await _new_client(factory)
        async with factory() as s:
            thread = await create_or_get_thread(
                s, client=client,
                operator_kind=OperatorKind.USER, operator_value=master,
                kind=ThreadKind.DM,
            )
            tid = thread.id
            await s.commit()
        await _backdate(factory, tid, created_days_ago=40)

        assert await auto_close_idle_threads(days=30) >= 1
        assert await _status(factory, tid) == "closed"
        assert await _marker(factory, tid) is None  # user/DM: quiet

    async def test_recent_message_keeps_open(self) -> None:
        """D8: COALESCE picks last_message_at when present -- an old
        thread with a recent message is NOT idle."""
        factory = get_session_factory()
        client = await _new_client(factory)
        async with factory() as s:
            section = await create_section(s, key="ac-active")
            thread = await create_or_get_thread(
                s, client=client,
                operator_kind=OperatorKind.SECTION, operator_value=section.id,
                kind=ThreadKind.TICKET, subject_type="practice", subject_id="a",
            )
            tid = thread.id
            await s.commit()
        # created 40d ago, but messaged 5d ago -> activity is recent
        await _backdate(factory, tid, created_days_ago=40, last_msg_days_ago=5)

        assert await auto_close_idle_threads(days=30) == 0
        assert await _status(factory, tid) == "open"

    async def test_autoclosed_thread_revives_on_client_message(self) -> None:
        factory = get_session_factory()
        client = await _new_client(factory)
        async with factory() as s:
            section = await create_section(s, key="ac-revive")
            thread = await create_or_get_thread(
                s, client=client,
                operator_kind=OperatorKind.SECTION, operator_value=section.id,
                kind=ThreadKind.TICKET, subject_type="practice", subject_id="r",
            )
            tid = thread.id
            await s.commit()
        await _backdate(factory, tid, created_days_ago=40)
        await auto_close_idle_threads(days=30)
        assert await _status(factory, tid) == "closed"

        async with factory() as s:
            await post_message(s, thread_id=tid, sender=client, body="back")
            await s.commit()
        assert await _status(factory, tid) == "open"


class TestAutoCloseBatch:
    async def test_batch_respects_limit_oldest_first(
        self, db_session: AsyncSession
    ) -> None:
        """The commit-free batch closes at most `limit`, oldest-first."""
        client = await create_recipient(
            db_session, telegram_id=next_phase4b_telegram_id()
        )
        section = await create_section(db_session, key="ac-batch")
        older = await create_or_get_thread(
            db_session, client=client.id,
            operator_kind=OperatorKind.SECTION, operator_value=section.id,
            kind=ThreadKind.TICKET, subject_type="practice", subject_id="old",
        )
        newer = await create_or_get_thread(
            db_session, client=client.id,
            operator_kind=OperatorKind.SECTION, operator_value=section.id,
            kind=ThreadKind.TICKET, subject_type="practice", subject_id="new",
        )
        now = datetime.now(UTC)
        await db_session.execute(
            update(Thread).where(Thread.id == older.id).values(
                created_at=now - timedelta(days=50), last_message_at=None
            )
        )
        await db_session.execute(
            update(Thread).where(Thread.id == newer.id).values(
                created_at=now - timedelta(days=40), last_message_at=None
            )
        )
        cutoff = now - timedelta(days=30)

        first = await auto_close_idle_threads_batch(
            db_session, cutoff=cutoff, when=now, limit=1
        )
        assert first == 1
        assert await db_session.scalar(
            select(Thread.status).where(Thread.id == older.id)
        ) == "closed"
        assert await db_session.scalar(
            select(Thread.status).where(Thread.id == newer.id)
        ) == "open"

        second = await auto_close_idle_threads_batch(
            db_session, cutoff=cutoff, when=now, limit=1
        )
        assert second == 1
        assert await db_session.scalar(
            select(Thread.status).where(Thread.id == newer.id)
        ) == "closed"
