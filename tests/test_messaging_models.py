# =============================================================================
# COMMS Service -- Messaging model / schema tests (Phase 4a)
# =============================================================================
# Schema-level invariants (migration 0006):
#   - status defaults to "open"
#   - subject_ref both-or-neither CHECK
#   - RESTRICT blocks deleting a recipient a thread/message/read-state
#     references (mandatory edit 3)
#   - model column widths match messaging/constants.py
# =============================================================================

from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import Recipient
from app.messaging.constants import (
    MAX_MESSAGE_BODY_LEN,
    MAX_SECTION_KEY_LEN,
    MAX_SECTION_LABEL_LEN,
    MAX_SUBJECT_ID_LEN,
    MAX_SUBJECT_TYPE_LEN,
    MAX_THREAD_TITLE_LEN,
    OperatorKind,
    ThreadKind,
    ThreadStatus,
)
from app.messaging.models import Message, Section, Thread, ThreadReadState
from app.messaging.threads import create_or_get_thread, post_message
from tests.helpers import create_recipient, next_phase4a_telegram_id


async def _two_recipients(session: AsyncSession) -> tuple[UUID, UUID]:
    client = await create_recipient(
        session, telegram_id=next_phase4a_telegram_id()
    )
    operator = await create_recipient(
        session, telegram_id=next_phase4a_telegram_id()
    )
    return client.id, operator.id


class TestStatusDefault:
    async def test_new_thread_is_open(
        self, db_session: AsyncSession
    ) -> None:
        client_id, op_id = await _two_recipients(db_session)
        thread = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.DM,
        )
        assert thread.status == ThreadStatus.OPEN
        assert thread.status == "open"


class TestSubjectRefCheck:
    async def test_half_subject_ref_violates_check(
        self, db_session: AsyncSession
    ) -> None:
        """A thread with one subject column set and the other NULL is
        rejected by the DB CHECK (constructed directly to bypass the
        service's earlier ValidationError)."""
        client_id, op_id = await _two_recipients(db_session)
        bad = Thread(
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.TICKET,
            subject_type="practice",
            subject_id=None,
        )
        db_session.add(bad)
        with pytest.raises(IntegrityError):
            await db_session.flush()


class TestRestrictOnRecipients:
    async def test_delete_client_blocked_by_thread(
        self, db_session: AsyncSession
    ) -> None:
        """Deleting a recipient a thread references is refused at the
        DB level (ondelete=RESTRICT, edit 3)."""
        client_id, op_id = await _two_recipients(db_session)
        await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.DM,
        )
        with pytest.raises(IntegrityError):
            await db_session.execute(
                delete(Recipient).where(Recipient.id == client_id)
            )
            await db_session.flush()

    async def test_delete_sender_blocked_by_message(
        self, db_session: AsyncSession
    ) -> None:
        client_id, op_id = await _two_recipients(db_session)
        thread = await create_or_get_thread(
            db_session,
            client=client_id,
            operator_kind=OperatorKind.USER,
            operator_value=op_id,
            kind=ThreadKind.DM,
        )
        await post_message(
            db_session,
            thread_id=thread.id,
            sender=client_id,
            body="hi",
            created_at=datetime.now(UTC),
        )
        with pytest.raises(IntegrityError):
            await db_session.execute(
                delete(Recipient).where(Recipient.id == client_id)
            )
            await db_session.flush()


class TestColumnWidths:
    def test_model_widths_match_constants(self) -> None:
        assert Section.__table__.c.key.type.length == MAX_SECTION_KEY_LEN
        assert Section.__table__.c.label.type.length == MAX_SECTION_LABEL_LEN
        cols = Thread.__table__.c
        assert cols.subject_type.type.length == MAX_SUBJECT_TYPE_LEN
        assert cols.subject_id.type.length == MAX_SUBJECT_ID_LEN
        assert cols.title.type.length == MAX_THREAD_TITLE_LEN
        assert Message.__table__.c.body.type.length == MAX_MESSAGE_BODY_LEN


class TestReadStateShape:
    async def test_composite_primary_key(self) -> None:
        pk_cols = {c.name for c in ThreadReadState.__table__.primary_key}
        assert pk_cols == {"thread_id", "participant"}


class TestEmptyCounts:
    async def test_tables_start_empty(
        self, db_session: AsyncSession
    ) -> None:
        for model in (Thread, Message, ThreadReadState, Section):
            count = await db_session.scalar(
                select(func.count()).select_from(model)
            )
            assert count == 0
