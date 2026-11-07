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

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import UUID, uuid4

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from app import notifier
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
    intake_fields,
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
            s, **intake_fields(), client=client,
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
            await post_message(s,
                **intake_fields(), thread_id=tid, sender=client, body="back")
            await s.commit()

        assert await consume_close_notifications() == 0
        assert await _closed_notice_count(factory, client) == 0

    async def test_user_dm_never_notified(self) -> None:
        factory = get_session_factory()
        client = await _new_recipient(factory)
        master = await _new_recipient(factory)
        async with factory() as s:
            thread = await create_or_get_thread(
                s, **intake_fields(), client=client,
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


# -----------------------------------------------------------------------------
# REV-2 item 3 -- one broken thread does not hold the others' notices
# -----------------------------------------------------------------------------
#
# MUTATIONS these tests were written against (each turns one red):
#   C1 one rollback per batch (no savepoint per thread)
#      -> test_one_broken_thread_does_not_hold_the_rest
#   C2 no keyset inside the pass (each batch selected from the start)
#      -> test_a_batch_of_broken_threads_ends_the_pass

_REAL_EMIT = notifier._emit_close_notification


def _broken_for(broken: set[UUID]):  # type: ignore[no-untyped-def]
    """The notice, real for every thread but `broken` -- for those a
    DATABASE error, which aborts the transaction: what the savepoint
    must survive, not a Python raise the session never sees."""

    async def emit(session: AsyncSession, thread: Thread, when: datetime) -> None:
        if thread.id in broken:
            await session.execute(text("SELECT * FROM no_such_table_rev2"))
        await _REAL_EMIT(session, thread, when)

    return emit


async def _closed_threads(factory: Factory, count: int) -> list[tuple[UUID, UUID]]:
    """`count` section threads closed one after the other (so their flags
    are in this order); (thread_id, client) each."""
    made: list[tuple[UUID, UUID]] = []
    for _ in range(count):
        client = await _new_recipient(factory)
        tid, _ = await _make_section(factory, client)
        async with factory() as s:
            await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)
            await s.commit()
        made.append((tid, client))
    return made


class TestOneThreadAtATime:
    async def test_one_broken_thread_does_not_hold_the_rest(self) -> None:
        """A batch of four, the second breaks: the other three are
        notified and cleared, the broken one is said once in the log --
        its id, the class and the place, never the error's text -- and
        keeps its flag; the next pass notifies it. C1."""
        factory = get_session_factory()
        threads = await _closed_threads(factory, 4)
        broken_tid, broken_client = threads[1]

        with (
            patch.object(
                notifier, "_emit_close_notification", _broken_for({broken_tid}),
            ),
            capture_logs() as logs,
        ):
            assert await consume_close_notifications(limit=4) == 3

        for tid, client in threads:
            if tid == broken_tid:
                continue
            assert await _flag(factory, tid) is None
            assert await _closed_notice_count(factory, client) == 1
        assert await _flag(factory, broken_tid) is not None
        assert await _closed_notice_count(factory, broken_client) == 0
        said = [e for e in logs if e["event"] == "close_notification_error"]
        assert [e["thread_id"] for e in said] == [str(broken_tid)]
        assert said[0]["error"].startswith("sqlalchemy.exc.ProgrammingError at ")
        assert "no_such_table" not in str(said[0])

        # The next pass: the broken thread, mended, is notified.
        assert await consume_close_notifications(limit=4) == 1
        assert await _flag(factory, broken_tid) is None
        assert await _closed_notice_count(factory, broken_client) == 1

    async def test_a_batch_of_broken_threads_ends_the_pass(self) -> None:
        """More broken threads than a batch holds, a sound one behind
        them: the pass walks past the broken ones and ends -- it does
        not select the same broken batch again and again -- and the
        sound one is notified in it. C2."""
        factory = get_session_factory()
        threads = await _closed_threads(factory, 4)
        broken = {tid for tid, _ in threads[:3]}
        sound_tid, sound_client = threads[3]

        with patch.object(notifier, "_emit_close_notification", _broken_for(broken)):
            notified = await asyncio.wait_for(
                consume_close_notifications(limit=2), timeout=10,
            )
        assert notified == 1
        assert await _flag(factory, sound_tid) is None
        assert await _closed_notice_count(factory, sound_client) == 1
        for tid in broken:
            assert await _flag(factory, tid) is not None
