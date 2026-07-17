# =============================================================================
# COMMS Service -- close-notify consumer tests (Phase 4c item 2)
# =============================================================================
# A section thread reaching `closed` (manual OR auto) is flagged; the
# consumer emits ONE "conversation closed" notice to the client and
# clears the flag. Idempotent (flag clear + dedup key); a reopen before
# the pass voids it; user/DM are never flagged -> never notified.
# The consumer owns its sessions -> committed setup + fresh-session
# assertions.
# =============================================================================

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.database import get_session_factory
from app.engine.models import Notification
from app.messaging.constants import OperatorKind, ThreadKind, ThreadStatus
from app.messaging.models import Thread
from app.messaging.processor import auto_close_idle_threads
from app.messaging.status import set_status
from app.messaging.threads import create_or_get_thread, post_message
from app.notifier import TYPE_THREAD_CLOSED, consume_close_notifications
from tests.helpers import (
    create_recipient,
    create_section,
    next_phase4c_telegram_id,
)

Factory = async_sessionmaker[AsyncSession]


async def _new_recipient(factory: Factory) -> UUID:
    rid = uuid4()
    async with factory() as s:
        await create_recipient(
            s, recipient_id=rid, telegram_id=next_phase4c_telegram_id()
        )
        await s.commit()
    return rid


async def _closed_notice_count(factory: Factory, client: UUID) -> int:
    async with factory() as s:
        return await s.scalar(
            select(func.count())
            .select_from(Notification)
            .where(
                Notification.type == TYPE_THREAD_CLOSED,
                Notification.target_value == str(client),
            )
        ) or 0


async def _flag(factory: Factory, tid: UUID) -> datetime | None:
    async with factory() as s:
        return await s.scalar(
            select(Thread.close_notify_pending_at).where(Thread.id == tid)
        )


async def _make_section(
    factory: Factory, client: UUID
) -> tuple[UUID, UUID]:
    """Committed section thread; returns (thread_id, section_id)."""
    async with factory() as s:
        section = await create_section(s, key=f"cn-{uuid4().hex[:8]}")
        thread = await create_or_get_thread(
            s, client=client,
            operator_kind=OperatorKind.SECTION, operator_value=section.id,
            kind=ThreadKind.TICKET, subject_type="practice", subject_id="s",
        )
        tid, sid = thread.id, section.id
        await s.commit()
    return tid, sid


class TestCloseNotifyConsumer:
    async def test_manual_close_emits_and_clears(self) -> None:
        factory = get_session_factory()
        client = await _new_recipient(factory)
        tid, _ = await _make_section(factory, client)
        async with factory() as s:
            await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)
            await s.commit()

        assert await consume_close_notifications() == 1
        assert await _closed_notice_count(factory, client) == 1
        assert await _flag(factory, tid) is None  # flag cleared

    async def test_rerun_no_duplicate(self) -> None:
        factory = get_session_factory()
        client = await _new_recipient(factory)
        tid, _ = await _make_section(factory, client)
        async with factory() as s:
            await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)
            await s.commit()

        await consume_close_notifications()
        assert await consume_close_notifications() == 0  # nothing left
        assert await _closed_notice_count(factory, client) == 1

    async def test_reopen_before_consume_no_notice(self) -> None:
        factory = get_session_factory()
        client = await _new_recipient(factory)
        tid, _ = await _make_section(factory, client)
        async with factory() as s:
            await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)
            await s.commit()
        # client reopens (4b clears the flag) BEFORE the consumer runs
        async with factory() as s:
            await post_message(s, thread_id=tid, sender=client, body="back")
            await s.commit()

        assert await consume_close_notifications() == 0
        assert await _closed_notice_count(factory, client) == 0

    async def test_user_dm_never_notified(self) -> None:
        factory = get_session_factory()
        client = await _new_recipient(factory)
        master = await _new_recipient(factory)
        async with factory() as s:
            thread = await create_or_get_thread(
                s, client=client,
                operator_kind=OperatorKind.USER, operator_value=master,
                kind=ThreadKind.DM,
            )
            tid = thread.id
            await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)
            await s.commit()

        assert await consume_close_notifications() == 0
        assert await _closed_notice_count(factory, client) == 0
        assert await _flag(factory, tid) is None  # user/DM never flagged

    async def test_auto_close_path_also_notifies(self) -> None:
        """The consumer covers the AUTO path too: auto-close flags a
        section thread, the consumer notifies + clears."""
        factory = get_session_factory()
        client = await _new_recipient(factory)
        tid, _ = await _make_section(factory, client)
        now = datetime.now(UTC)
        async with factory() as s:
            await s.execute(
                update(Thread).where(Thread.id == tid).values(
                    created_at=now - timedelta(days=40), last_message_at=None
                )
            )
            await s.commit()
        await auto_close_idle_threads(days=30)  # flags the section thread

        assert await consume_close_notifications() == 1
        assert await _closed_notice_count(factory, client) == 1
        assert await _flag(factory, tid) is None
