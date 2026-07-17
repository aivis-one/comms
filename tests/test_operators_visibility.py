# =============================================================================
# COMMS Service -- Operator visibility tests (Phase 4b, item 3)
# =============================================================================
# visible(me) = assignee==me OR (unassigned section thread). A foreign
# user thread (mastered by someone else) is invisible; a supervisor
# sees everything (read-only NOT enforced here -- P-2 marker / 4c).
# =============================================================================

from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.operators import claim_thread, list_visible_threads
from app.messaging.threads import create_or_get_thread
from tests.helpers import (
    create_recipient,
    create_section,
    next_phase4b_telegram_id,
)


async def _rid(session: AsyncSession) -> UUID:
    r = await create_recipient(session, telegram_id=next_phase4b_telegram_id())
    return r.id


async def _visible_ids(
    session: AsyncSession, operator: UUID, *, is_supervisor: bool = False
) -> set[UUID]:
    threads = await list_visible_threads(
        session, operator=operator, is_supervisor=is_supervisor
    )
    return {t.id for t in threads}


class TestVisibility:
    async def test_visibility_partition(
        self, db_session: AsyncSession
    ) -> None:
        client = await _rid(db_session)
        a = await _rid(db_session)
        b = await _rid(db_session)
        section = await create_section(db_session, key="vis-sec")

        # user threads -- pre-assigned to their master
        t_user_a = await create_or_get_thread(
            db_session, client=client,
            operator_kind=OperatorKind.USER, operator_value=a,
            kind=ThreadKind.DM,
        )
        t_user_b = await create_or_get_thread(
            db_session, client=client,
            operator_kind=OperatorKind.USER, operator_value=b,
            kind=ThreadKind.DM,
        )

        # section threads
        t_sec_free = await create_or_get_thread(
            db_session, client=client,
            operator_kind=OperatorKind.SECTION, operator_value=section.id,
            kind=ThreadKind.TICKET, subject_type="practice", subject_id="free",
        )
        t_sec_a = await create_or_get_thread(
            db_session, client=client,
            operator_kind=OperatorKind.SECTION, operator_value=section.id,
            kind=ThreadKind.TICKET, subject_type="practice", subject_id="a",
        )
        await claim_thread(db_session, thread_id=t_sec_a.id, operator=a)
        t_sec_b = await create_or_get_thread(
            db_session, client=client,
            operator_kind=OperatorKind.SECTION, operator_value=section.id,
            kind=ThreadKind.TICKET, subject_type="practice", subject_id="b",
        )
        await claim_thread(db_session, thread_id=t_sec_b.id, operator=b)

        # A sees: own user thread, the free section thread, own claim
        assert await _visible_ids(db_session, a) == {
            t_user_a.id, t_sec_free.id, t_sec_a.id,
        }
        # B symmetric
        assert await _visible_ids(db_session, b) == {
            t_user_b.id, t_sec_free.id, t_sec_b.id,
        }
        # a foreign user thread is invisible to the other operator
        assert t_user_b.id not in await _visible_ids(db_session, a)
        assert t_user_a.id not in await _visible_ids(db_session, b)

        # supervisor sees everything
        all_ids = {
            t_user_a.id, t_user_b.id, t_sec_free.id, t_sec_a.id, t_sec_b.id,
        }
        assert await _visible_ids(
            db_session, uuid4(), is_supervisor=True
        ) == all_ids

    async def test_stranger_sees_only_free_section(
        self, db_session: AsyncSession
    ) -> None:
        """An operator with no claims and no user threads still sees
        unassigned section threads (v1: serves every section)."""
        client = await _rid(db_session)
        master = await _rid(db_session)
        stranger = await _rid(db_session)
        section = await create_section(db_session, key="vis-sec2")

        await create_or_get_thread(
            db_session, client=client,
            operator_kind=OperatorKind.USER, operator_value=master,
            kind=ThreadKind.DM,
        )
        t_free = await create_or_get_thread(
            db_session, client=client,
            operator_kind=OperatorKind.SECTION, operator_value=section.id,
            kind=ThreadKind.TICKET, subject_type="practice", subject_id="f2",
        )
        assert await _visible_ids(db_session, stranger) == {t_free.id}
