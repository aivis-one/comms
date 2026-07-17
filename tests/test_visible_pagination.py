# =============================================================================
# COMMS Service -- list_visible_threads keyset pagination (Phase 4c item 5)
# =============================================================================
# Ordered COALESCE(last_message_at, created_at) DESC, id DESC (the
# ix_threads_activity order). Pages are stable across a cursor walk,
# limit clamps to 1..100, next_cursor is None on the last page. The
# opaque wire codec + malformed-cursor 422 live at the API layer (B6).
# =============================================================================

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Thread
from app.messaging.operators import claim_thread, list_visible_threads
from app.messaging.threads import create_or_get_thread
from tests.helpers import (
    create_recipient,
    create_section,
    next_phase4c_telegram_id,
)

_BASE = datetime(2026, 1, 1, tzinfo=UTC)


async def _rid(session: AsyncSession) -> UUID:
    r = await create_recipient(session, telegram_id=next_phase4c_telegram_id())
    return r.id


async def _section_thread(session: AsyncSession, client: UUID, i: int) -> UUID:
    section = await create_section(session, key=f"pg-{uuid4().hex[:8]}")
    thread = await create_or_get_thread(
        session, client=client,
        operator_kind=OperatorKind.SECTION, operator_value=section.id,
        kind=ThreadKind.TICKET, subject_type="practice", subject_id=str(i),
    )
    return thread.id


async def _set_activity(
    session: AsyncSession, tid: UUID, when: datetime
) -> None:
    await session.execute(
        update(Thread).where(Thread.id == tid).values(last_message_at=when)
    )


async def _seed(session: AsyncSession, client: UUID, n: int) -> list[UUID]:
    """n section threads, activity ascending by index (i later = newer)."""
    ids: list[UUID] = []
    for i in range(n):
        tid = await _section_thread(session, client, i)
        await _set_activity(session, tid, _BASE + timedelta(minutes=i))
        ids.append(tid)
    await session.flush()
    return ids


async def _walk(
    session: AsyncSession, operator: UUID, *, limit: int, supervisor: bool
) -> list[UUID]:
    collected: list[UUID] = []
    cursor: tuple[datetime, UUID] | None = None
    while True:
        page, cursor = await list_visible_threads(
            session, operator=operator, is_supervisor=supervisor,
            limit=limit, cursor=cursor,
        )
        collected.extend(t.id for t in page)
        if cursor is None:
            break
    return collected


class TestKeysetOrder:
    async def test_pages_cover_full_set_in_order(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        ids = await _seed(db_session, client, 7)
        expected = list(reversed(ids))  # activity DESC -> newest first
        collected = await _walk(
            db_session, uuid4(), limit=3, supervisor=True
        )
        assert collected == expected  # stable, no gaps, no duplicates

    async def test_first_page_capped_with_cursor(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        await _seed(db_session, client, 7)
        page, cursor = await list_visible_threads(
            db_session, operator=uuid4(), is_supervisor=True, limit=3
        )
        assert len(page) == 3
        assert cursor is not None

    async def test_last_page_has_null_cursor(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        await _seed(db_session, client, 7)
        page, cursor = await list_visible_threads(
            db_session, operator=uuid4(), is_supervisor=True, limit=50
        )
        assert len(page) == 7
        assert cursor is None

    async def test_cursor_round_trip(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        ids = await _seed(db_session, client, 5)
        expected = list(reversed(ids))
        page1, cursor = await list_visible_threads(
            db_session, operator=uuid4(), is_supervisor=True, limit=2
        )
        assert [t.id for t in page1] == expected[:2]
        page2, _ = await list_visible_threads(
            db_session, operator=uuid4(), is_supervisor=True,
            limit=2, cursor=cursor,
        )
        assert [t.id for t in page2] == expected[2:4]


class TestClampAndTiebreak:
    async def test_limit_clamped_to_at_least_one(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        await _seed(db_session, client, 3)
        page, cursor = await list_visible_threads(
            db_session, operator=uuid4(), is_supervisor=True, limit=0
        )
        assert len(page) == 1  # 0 -> clamped to 1
        assert cursor is not None

    async def test_equal_activity_ordered_by_id_desc(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        t1 = await _section_thread(db_session, client, 100)
        t2 = await _section_thread(db_session, client, 101)
        await _set_activity(db_session, t1, _BASE)
        await _set_activity(db_session, t2, _BASE)  # identical activity
        await db_session.flush()
        page, _ = await list_visible_threads(
            db_session, operator=uuid4(), is_supervisor=True, limit=50
        )
        pair = [t.id for t in page if t.id in {t1, t2}]
        assert pair == sorted([t1, t2], reverse=True)  # id DESC tiebreak


class TestOperatorScopePagination:
    async def test_operator_paginates_only_visible(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        operator = await _rid(db_session)
        mine: list[UUID] = []
        for i in range(5):
            tid = await _section_thread(db_session, client, 200 + i)
            await claim_thread(db_session, thread_id=tid, operator=operator)
            await _set_activity(db_session, tid, _BASE + timedelta(minutes=i))
            mine.append(tid)
        # a foreign user thread (mastered by someone else) stays invisible
        other = await _rid(db_session)
        await create_or_get_thread(
            db_session, client=client,
            operator_kind=OperatorKind.USER, operator_value=other,
            kind=ThreadKind.DM,
        )
        await db_session.flush()
        collected = await _walk(
            db_session, operator, limit=2, supervisor=False
        )
        assert set(collected) == set(mine)
