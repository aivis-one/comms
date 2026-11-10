# =============================================================================
# PERF-3 -- a feed page reads the LATEST translation request per message
# =============================================================================
#
# What is held here (app/variants.py reader_states): a page of _PAGE
# messages, each with N requests in the reader's language -- the product
# repeats a translation automatically twice (velo and aivis
# support/translation.py MAX_ATTEMPTS = 2) and an admin may repeat it
# without bound -- at N = 2 and N = 20:
#   - the requests read RETURNS one row per message, whatever N (counted
#     on the server: EXPLAIN ANALYZE of the very statement reader_states
#     sent, its top node's actual rows);
#   - the answer is the latest request's: the latest of each message
#     points at a pending job (PREPARING), every older one at no job (an
#     older one chosen would say IMPOSSIBLE).
# Before PERF-3 the read returned every request of the page in the
# language -- _PAGE x N rows -- and Python kept the first per message.
#
# MUTATION: M6 the DISTINCT ON taken out (the read returns them all) ->
# test_one_row_per_message[20].
# =============================================================================

from typing import Any
from uuid import UUID

import pytest
from httpx import AsyncClient
from sqlalchemy import event, select, text

from app.core.database import get_engine, get_session_factory
from app.engine.constants import NotificationStatus
from app.messaging.constants import VariantState
from app.messaging.models import Message
from app.variants import reader_states
from tests.test_message_variants import _conversation, _job

_PAGE = 100


async def _seed(client: AsyncClient, n: int) -> list[UUID]:
    thread, _, sender = await _conversation(client)
    pending = await _job(NotificationStatus.PENDING, answered=False)
    async with get_session_factory()() as session:
        await session.execute(text(
            "INSERT INTO messages (id, thread_id, sender, body, "
            "idempotency_key, fingerprint, created_at) "
            "SELECT gen_random_uuid(), :thread, :sender, 'm' || g, "
            "'p3:m:' || gen_random_uuid(), repeat('f', 64), "
            "now() - g * interval '1 s' "
            "FROM generate_series(1, :n) AS g"
        ), {"thread": UUID(thread), "sender": sender, "n": _PAGE})
        # The older requests: no job behind them.
        await session.execute(text(
            "INSERT INTO message_variant_requests (id, message_id, locale, "
            "job_key, job_id, idempotency_key, fingerprint, created_at) "
            "SELECT gen_random_uuid(), m.id, 'ar', 'p3:old', "
            "gen_random_uuid(), 'p3:r:' || gen_random_uuid(), "
            "repeat('f', 64), now() - (k + 1) * interval '1 min' "
            "FROM messages m, generate_series(1, :older) AS k "
            "WHERE m.thread_id = :thread"
        ), {"thread": UUID(thread), "older": n - 1})
        # The latest: the pending job.
        await session.execute(text(
            "INSERT INTO message_variant_requests (id, message_id, locale, "
            "job_key, job_id, idempotency_key, fingerprint, created_at) "
            "SELECT gen_random_uuid(), m.id, 'ar', n.idempotency_key, n.id, "
            "'p3:r:' || gen_random_uuid(), repeat('f', 64), now() "
            "FROM messages m, notifications n "
            "WHERE m.thread_id = :thread AND n.idempotency_key = :key"
        ), {"thread": UUID(thread), "key": pending})
        await session.commit()
    async with get_engine().connect() as connection:
        autocommit = await connection.execution_options(
            isolation_level="AUTOCOMMIT",
        )
        await autocommit.execute(text("ANALYZE"))
    async with get_session_factory()() as session:
        return list((await session.scalars(
            select(Message.id).where(
                Message.thread_id == UUID(thread),
                Message.body.like("m%"),
            )
        )).all())


@pytest.mark.usefixtures("pristine_tables")
class TestTheLatestRequest:
    @pytest.mark.parametrize("n", [2, 20])
    async def test_one_row_per_message(self, client: AsyncClient, n: int) -> None:
        page = await _seed(client, n)
        assert len(page) == _PAGE
        seen: list[tuple[str, Any]] = []

        def record(conn: Any, cursor: Any, statement: str, parameters: Any,
                   *rest: Any) -> None:
            if "message_variant_requests" in statement:
                seen.append((statement, parameters))

        engine = get_engine().sync_engine
        event.listen(engine, "before_cursor_execute", record)
        try:
            async with get_session_factory()() as session:
                states = await reader_states(session, page, "ar")
        finally:
            event.remove(engine, "before_cursor_execute", record)
        assert {state for state, _ in states.values()} == {VariantState.PREPARING}
        ((statement, parameters),) = seen
        async with get_session_factory()() as session:
            raw = await session.connection()
            plan = (await raw.exec_driver_sql(
                "EXPLAIN (ANALYZE, FORMAT JSON) " + statement, parameters,
            )).scalar_one()
            await session.rollback()
        top = plan[0]
        assert top["Plan"]["Actual Rows"] == _PAGE
