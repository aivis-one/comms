# =============================================================================
# TK-2 -- the product deletes a thread, with everything under it
# =============================================================================
#
# What is held here, against the code and deploy/INTEGRATION.md section
# 14 ("Deleting a thread"):
#   1 the deletion -- every row of the thread goes, the neighbour stays
#     to the row; a repeat and an absent thread are the same 204; every
#     route that names the thread is a 404 after it, as for a thread
#     that never was; a new thread of the same subject is a new id;
#   2 races on real sessions, the wait observed through pg_blocking_pids
#     of the pair in this database: a deletion against a message, claim,
#     status, retag, a read pointer, a variant request, the close-notice
#     pass, auto-close, another deletion, forgetting the participant;
#   3 what the thread gave birth to -- a ping and a close notice already
#     issued are delivered, one not issued never is, a model's job runs
#     on;
#   4 the price of the cascade -- generic plans, and the blocks one
#     deletion reads;
#   5 the contract.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  one cascading FK recreated without ON DELETE CASCADE (each of
#       the four) -> TestDelete test_everything_goes (a 500)
#   M2  the deletion answering 404 for an absent thread -> TestDelete
#       test_a_repeat_and_an_absent_thread
#   M3a the message route reading the thread instead of taking it ->
#       TestRaces test_a_message_is_authorized_on_the_taken_row
#   M3b post_message reading the thread instead of taking it ->
#       TestRaces test_delete_against_a_message[delete_first]
#   M4  the variant writes reading the message without taking the thread
#       -> TestRaces test_delete_against_a_variant_request[delete_first]
#   M5  mark_read without taking the thread -> TestRaces
#       test_delete_against_a_read_pointer[delete_first]
#   M6  retag_thread reading instead of taking -> TestRaces
#       test_delete_against_retag[delete_first]
#   M7  the feed without its 404 -> TestAbsent
#   M8  unread-count without its 404 -> TestAbsent
#   M9  the close-notice pass clearing through the ORM, blindly (before
#       TK-1) -> TestRaces test_delete_against_the_close_pass
#   M10 the DELETE row out of the contract -> TestTheContract
# NOT A MUTATION: auto-close against a deletion -- a deleted row is
# skipped by Postgres' recheck, no line of comms decides it; built as a
# test of the outcome.
# =============================================================================

import asyncio
import json
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_engine, get_session_factory
from app.core.exceptions import NotFoundError
from app.engine import service
from app.engine.constants import NotificationStatus
from app.engine.models import LLMAnswer, Notification
from app.forgetting import forget_recipient
from app.messaging.constants import OperatorKind, ThreadKind, ThreadStatus
from app.messaging.models import (
    Message,
    MessageVariant,
    MessageVariantRequest,
    SectionMember,
    Thread,
    ThreadReadState,
)
from app.messaging.operators import assign_thread, claim_thread, retag_thread
from app.messaging.read_state import mark_read
from app.messaging.status import auto_close_idle_threads_batch, set_status
from app.messaging.threads import create_or_get_thread, delete_thread, post_message
from app.notifier import TYPE_THREAD_CLOSED, consume_close_notifications
from app.variants import write_request
from tests.helpers import (
    create_recipient,
    create_section,
    intake_fields,
    next_tk2_telegram_id,
)
from tests.test_message_variants import _job

_REPO = Path(__file__).resolve().parent.parent
_BOUND = 10.0
_POLL = 0.02


# ---------------------------------------------------------------------------
# Building blocks -- every row committed, so real sessions see it
# ---------------------------------------------------------------------------


async def _recipient() -> UUID:
    rid = uuid4()
    async with get_session_factory()() as s:
        await create_recipient(
            s, recipient_id=rid, telegram_id=next_tk2_telegram_id()
        )
        await s.commit()
    return rid


async def _section(*members: UUID) -> UUID:
    async with get_session_factory()() as s:
        section = await create_section(s, key=f"tk2-{uuid4().hex[:8]}")
        for member in members:
            s.add(SectionMember(section_id=section.id, operator_id=member))
        await s.commit()
        return section.id


async def _ticket(
    section: UUID, customer: UUID, *, holder: UUID | None = None
) -> UUID:
    async with get_session_factory()() as s:
        thread = await create_or_get_thread(
            s, **intake_fields(), client=customer,
            operator_kind=OperatorKind.SECTION, operator_value=section,
            kind=ThreadKind.TICKET, subject_type="practice",
            subject_id=uuid4().hex,
        )
        if holder is not None:
            assert await claim_thread(s, thread_id=thread.id, operator=holder)
        await s.commit()
        return thread.id


async def _dm(customer: UUID, master: UUID) -> UUID:
    async with get_session_factory()() as s:
        thread = await create_or_get_thread(
            s, **intake_fields(), client=customer,
            operator_kind=OperatorKind.USER, operator_value=master,
            kind=ThreadKind.DM,
        )
        await s.commit()
        return thread.id


async def _message(tid: UUID, sender: UUID) -> UUID:
    async with get_session_factory()() as s:
        message = await post_message(
            s, thread_id=tid, sender=sender, body="hello",
            idempotency_key=f"tk2:{uuid4()}", fingerprint="f" * 64,
        )
        await s.commit()
        return message.id


async def _furnish(tid: UUID, customer: UUID, operator: UUID) -> list[UUID]:
    """Messages from both sides, both read pointers, a variant and a
    variant request on the first message -- every child a thread has."""
    messages = [await _message(tid, customer), await _message(tid, operator)]
    async with get_session_factory()() as s:
        for who in (customer, operator):
            await mark_read(
                s, thread_id=tid, participant=who,
                last_read_at=datetime.now(UTC) - timedelta(days=1),
            )
        s.add(MessageVariantRequest(
            message_id=messages[0], locale="en", job_key=f"job:{uuid4()}",
            job_id=uuid4(), idempotency_key=f"tk2:{uuid4()}",
            fingerprint="f" * 64,
        ))
        s.add(MessageVariant(
            message_id=messages[0], locale="en", job_key=f"job:{uuid4()}",
            source_locale="de", body="hi", declined=False,
            idempotency_key=f"tk2:{uuid4()}", fingerprint="f" * 64,
        ))
        await s.commit()
    return messages


async def _rows(tid: UUID, messages: list[UUID]) -> dict[str, int]:
    """Every row a thread owns, table by table."""
    async with get_session_factory()() as s:
        async def count(model: Any, *where: Any) -> int:
            return await s.scalar(
                select(func.count()).select_from(model).where(*where)
            ) or 0
        return {
            "threads": await count(Thread, Thread.id == tid),
            "messages": await count(Message, Message.thread_id == tid),
            "read": await count(
                ThreadReadState, ThreadReadState.thread_id == tid
            ),
            "variants": await count(
                MessageVariant, MessageVariant.message_id.in_(messages)
            ),
            "requests": await count(
                MessageVariantRequest,
                MessageVariantRequest.message_id.in_(messages),
            ),
        }


async def _close_notices(tid: UUID) -> int:
    async with get_session_factory()() as s:
        return await s.scalar(
            select(func.count()).select_from(Notification).where(
                Notification.type == TYPE_THREAD_CLOSED,
                Notification.idempotency_key.like(f"close:{tid}:%"),
            )
        ) or 0


async def _gone(tid: UUID) -> bool:
    async with get_session_factory()() as s:
        return await s.get(Thread, tid) is None


# ---------------------------------------------------------------------------
# 1 -- the deletion
# ---------------------------------------------------------------------------


class TestDelete:
    @pytest.mark.parametrize("state", ["pool-open", "taken-closed", "dm-resolved"])
    async def test_everything_goes_the_neighbour_stays(
        self, client: AsyncClient, state: str
    ) -> None:
        customer, operator = await _recipient(), await _recipient()
        if state == "dm-resolved":
            gone = await _dm(customer, operator)
            kept = await _dm(customer, await _recipient())
        else:
            section = await _section()
            holder = operator if state == "taken-closed" else None
            gone = await _ticket(section, customer, holder=holder)
            kept = await _ticket(section, customer, holder=holder)
        gone_messages = await _furnish(gone, customer, operator)
        kept_messages = await _furnish(kept, customer, operator)
        target = {"pool-open": None, "taken-closed": ThreadStatus.CLOSED,
                  "dm-resolved": ThreadStatus.RESOLVED}[state]
        if target is not None:
            async with get_session_factory()() as s:
                for tid in (gone, kept):
                    await set_status(s, thread_id=tid, target=target)
                await s.commit()
        kept_before = await _rows(kept, kept_messages)

        resp = await client.delete(f"/api/v1/threads/{gone}")
        assert resp.status_code == 204, resp.text
        assert resp.content == b""
        assert await _rows(gone, gone_messages) == {
            "threads": 0, "messages": 0, "read": 0, "variants": 0,
            "requests": 0,
        }
        # the pair: the same client's other thread, to the row
        assert await _rows(kept, kept_messages) == kept_before
        assert all(kept_before.values())

    async def test_an_empty_thread(self, client: AsyncClient) -> None:
        tid = await _ticket(await _section(), await _recipient())
        assert (await client.delete(f"/api/v1/threads/{tid}")).status_code == 204
        assert await _gone(tid)

    async def test_a_repeat_and_an_absent_thread(
        self, client: AsyncClient
    ) -> None:
        tid = await _ticket(await _section(), await _recipient())
        first = await client.delete(f"/api/v1/threads/{tid}")
        second = await client.delete(f"/api/v1/threads/{tid}")
        never = await client.delete(f"/api/v1/threads/{uuid4()}")
        assert first.status_code == second.status_code == never.status_code == 204
        assert await _gone(tid)
        malformed = await client.delete("/api/v1/threads/not-a-uuid")
        assert malformed.status_code == 422

    async def test_a_new_thread_of_the_same_subject(
        self, bare_client: AsyncClient
    ) -> None:
        """The same create request, the same Idempotency-Key: after the
        deletion it creates -- a new thread, a new id."""
        customer, section = await _recipient(), await _section()
        body = {
            "client": str(customer), "operator_kind": "section",
            "operator_value": str(section), "kind": "ticket",
            "subject_type": "practice", "subject_id": "p-1",
        }
        key = {"Idempotency-Key": f"tk2:{uuid4()}"}
        first = await bare_client.post("/api/v1/threads", json=body, headers=key)
        assert first.status_code == 200, first.text
        old = first.json()["id"]
        repeat = await bare_client.post("/api/v1/threads", json=body, headers=key)
        assert repeat.json()["id"] == old  # the pair: a replay, while it lives
        assert (await bare_client.delete(f"/api/v1/threads/{old}")).status_code == 204
        again = await bare_client.post("/api/v1/threads", json=body, headers=key)
        assert again.status_code == 200, again.text
        assert again.json()["id"] != old
        assert again.json()["created"] is True


# The routes that name a thread: (method, path after /threads/{id}, body
# or query). Held to the app's own routes by test_every_route_is_here.
def _calls(
    message: UUID, customer: UUID, operator: UUID, section: UUID
) -> list[tuple[str, str, dict[str, Any]]]:
    return [
        ("GET", "/messages", {}),
        ("POST", "/messages", {"sender": str(customer), "body": "x"}),
        ("POST", f"/messages/{message}/variant-requests",
         {"locale": "en", "job_key": "job:none"}),
        ("POST", f"/messages/{message}/variants",
         {"locale": "en", "job_key": "job:none", "source_locale": "de",
          "body": "hi"}),
        ("POST", "/read", {"participant": str(customer)}),
        ("GET", "/unread-count", {"participant": str(customer)}),
        ("POST", "/claim", {"operator": str(operator)}),
        ("POST", "/assign",
         {"operator": str(operator), "assignee": str(operator)}),
        ("POST", "/release", {"operator": str(operator)}),
        ("POST", "/status", {"operator": str(operator), "status": "resolved"}),
        ("POST", "/retag", {"operator": str(operator), "section": str(section)}),
    ]


async def _call(
    client: AsyncClient, tid: UUID, method: str, path: str,
    payload: dict[str, Any],
) -> Any:
    url = f"/api/v1/threads/{tid}{path}"
    if method == "GET":
        return await client.get(url, params=payload)
    return await client.post(url, json=payload)


class TestAfterIt:
    def test_every_route_is_here(self) -> None:
        """Both ways: every route of the app that names a thread is
        called below -- a new one turns this red until it is."""
        from app.main import app

        named = {
            (method.upper(), path.removeprefix("/api/v1/threads/{thread_id}"))
            for path, item in app.openapi()["paths"].items()
            if path.startswith("/api/v1/threads/{thread_id}")
            for method in item
        }
        named = {
            (m, re.sub(r"\{message_id\}", "{m}", p)) for m, p in named
        }
        mine = {
            (m, p.replace(str(_M), "{m}"))
            for m, p, _ in _calls(_M, _M, _M, _M)
        }
        assert named == mine | {("DELETE", "")}

    async def test_a_deleted_thread_is_404_everywhere(
        self, client: AsyncClient
    ) -> None:
        customer, operator = await _recipient(), await _recipient()
        section, other = await _section(), await _section()
        gone = await _ticket(section, customer)
        kept = await _ticket(section, customer)
        gone_message = await _message(gone, customer)
        kept_message = await _message(kept, customer)
        assert (await client.delete(f"/api/v1/threads/{gone}")).status_code == 204
        for method, path, payload in _calls(
            gone_message, customer, operator, other
        ):
            resp = await _call(client, gone, method, path, payload)
            assert resp.status_code == 404, (method, path, resp.text)
            assert resp.json()["error"]["class"] == "not_found"
        # the pair: the same calls on a living thread are not 404
        for method, path, payload in _calls(
            kept_message, customer, operator, other
        ):
            resp = await _call(client, kept, method, path, payload)
            assert resp.status_code != 404, (method, path, resp.text)

    async def test_lists_and_counts_do_not_contain_it(
        self, client: AsyncClient
    ) -> None:
        customer, operator = await _recipient(), await _recipient()
        gone = await _dm(customer, operator)
        kept = await _dm(customer, await _recipient())
        await _message(gone, operator)
        await _message(kept, await _row_assignee(kept))
        assert (await client.delete(f"/api/v1/threads/{gone}")).status_code == 204

        listed = await client.get(
            "/api/v1/threads", params={"client": str(customer)}
        )
        assert [row["id"] for row in listed.json()["items"]] == [str(kept)]
        mine = await client.get(
            "/api/v1/threads", params={"operator": str(operator)}
        )
        assert mine.json()["items"] == []
        counts = await client.post(
            "/api/v1/threads/unread-counts",
            json={"participant": str(customer),
                  "thread_ids": [str(gone), str(kept)]},
        )
        assert set(counts.json()["counts"]) == {str(kept)}
        summary = await client.get(
            f"/api/v1/participants/{customer}/unread-summary"
        )
        assert summary.json()["threads_with_unread"] == 1


async def _row_assignee(tid: UUID) -> UUID:
    async with get_session_factory()() as s:
        thread = await s.get(Thread, tid)
        assert thread is not None and thread.assignee is not None
        return thread.assignee


_M = UUID(int=1)


class TestAbsent:
    """A thread that never was answers as a deleted one: 404 -- after the
    input's own 422."""

    async def test_the_feed(self, client: AsyncClient) -> None:
        resp = await client.get(f"/api/v1/threads/{uuid4()}/messages")
        assert resp.status_code == 404
        assert resp.json()["error"]["class"] == "not_found"
        bad_cursor = await client.get(
            f"/api/v1/threads/{uuid4()}/messages", params={"cursor": "!!"}
        )
        assert bad_cursor.status_code == 422
        bad_locale = await client.get(
            f"/api/v1/threads/{uuid4()}/messages", params={"locale": "!!"}
        )
        assert bad_locale.status_code == 422
        # the pair: a living thread's empty feed is a 200 with no items
        tid = await _ticket(await _section(), await _recipient())
        empty = await client.get(f"/api/v1/threads/{tid}/messages")
        assert empty.status_code == 200
        assert empty.json()["items"] == []

    async def test_the_unread_count(self, client: AsyncClient) -> None:
        reader = await _recipient()
        resp = await client.get(
            f"/api/v1/threads/{uuid4()}/unread-count",
            params={"participant": str(reader)},
        )
        assert resp.status_code == 404
        tid = await _ticket(await _section(), reader)
        living = await client.get(
            f"/api/v1/threads/{tid}/unread-count",
            params={"participant": str(reader)},
        )
        assert living.status_code == 200
        assert living.json() == {"unread": 0}


# ---------------------------------------------------------------------------
# 2 -- races on real sessions
# ---------------------------------------------------------------------------


async def _pid(session: AsyncSession) -> int:
    pid = await session.scalar(text("SELECT pg_backend_pid()"))
    assert isinstance(pid, int)
    return pid


async def _waiting_on(holder: int) -> bool:
    async with get_session_factory()() as s:
        waiting = await s.scalar(
            text(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname = current_database() "
                "AND CAST(:holder AS integer) = ANY(pg_blocking_pids(pid))"
            ),
            {"holder": holder},
        )
    return bool(waiting)


async def _until_waiting(holder: int, task: asyncio.Task[Any]) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _BOUND
    while not await _waiting_on(holder):
        assert not task.done(), (
            f"the second call ended without waiting: {task.result()!r}"
        )
        assert loop.time() < deadline, "the second call never waited"
        await asyncio.sleep(_POLL)


async def _outcome(work: Awaitable[Any], session: AsyncSession) -> Any:
    """Run `work` and commit; ANY exception is the outcome -- a 500 is
    exactly what these tests must see if it happens."""
    try:
        result = await work
    except Exception as exc:  # the outcome under test, a 500 included
        await session.rollback()
        return exc
    await session.commit()
    return result


async def _cross(
    first: Callable[[AsyncSession], Awaitable[Any]],
    second: Callable[[AsyncSession], Awaitable[Any]],
) -> tuple[Any, Any]:
    """`first` writes and holds; `second` must wait for it; both end."""
    factory = get_session_factory()
    async with factory() as late, factory() as early:
        first_result = await first(early)
        holder = await _pid(early)
        task = asyncio.create_task(_outcome(second(late), late))
        await _until_waiting(holder, task)
        await early.commit()
        second_result = await asyncio.wait_for(task, _BOUND)
    return first_result, second_result


def _deleting(tid: UUID) -> Callable[[AsyncSession], Awaitable[Any]]:
    async def run(s: AsyncSession) -> Any:
        return await delete_thread(s, thread_id=tid)
    return run


_ORDERS = ["delete_first", "other_first"]


async def _race(
    order: str, tid: UUID, other: Callable[[AsyncSession], Awaitable[Any]]
) -> Any:
    """The other call's outcome; the deletion always ends, the thread is
    always gone."""
    if order == "delete_first":
        deleted, result = await _cross(_deleting(tid), other)
    else:
        result, deleted = await _cross(other, _deleting(tid))
    assert not isinstance(deleted, Exception), deleted
    assert await _gone(tid)
    return result


class TestRaces:
    @pytest.mark.parametrize("order", _ORDERS)
    async def test_delete_against_a_message(self, order: str) -> None:
        customer = await _recipient()
        tid = await _ticket(await _section(), customer)

        async def write(s: AsyncSession) -> Any:
            return await post_message(
                s, thread_id=tid, sender=customer, body="x",
                idempotency_key=f"tk2:{uuid4()}", fingerprint="f" * 64,
            )

        result = await _race(order, tid, write)
        if order == "delete_first":
            assert isinstance(result, NotFoundError), result
        else:
            assert isinstance(result, Message)
            assert await _rows(tid, [result.id]) == {
                "threads": 0, "messages": 0, "read": 0, "variants": 0,
                "requests": 0,
            }

    @pytest.mark.parametrize("order", _ORDERS)
    async def test_delete_against_claim(self, order: str) -> None:
        operator = await _recipient()
        tid = await _ticket(await _section(), await _recipient())

        async def claim(s: AsyncSession) -> Any:
            return await claim_thread(s, thread_id=tid, operator=operator)

        result = await _race(order, tid, claim)
        expected = NotFoundError if order == "delete_first" else bool
        assert isinstance(result, expected), result

    @pytest.mark.parametrize("order", _ORDERS)
    async def test_delete_against_status(self, order: str) -> None:
        tid = await _ticket(await _section(), await _recipient())

        async def close(s: AsyncSession) -> Any:
            return await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)

        result = await _race(order, tid, close)
        expected = NotFoundError if order == "delete_first" else Thread
        assert isinstance(result, expected), result

    @pytest.mark.parametrize("order", _ORDERS)
    async def test_delete_against_retag(self, order: str) -> None:
        tid = await _ticket(await _section(), await _recipient())
        moved_to = await _section()

        async def retag(s: AsyncSession) -> Any:
            return await retag_thread(s, thread_id=tid, section=moved_to)

        result = await _race(order, tid, retag)
        expected = NotFoundError if order == "delete_first" else Thread
        assert isinstance(result, expected), result

    @pytest.mark.parametrize("order", _ORDERS)
    async def test_delete_against_a_read_pointer(self, order: str) -> None:
        reader = await _recipient()
        tid = await _ticket(await _section(), reader)

        async def read(s: AsyncSession) -> Any:
            return await mark_read(
                s, thread_id=tid, participant=reader,
                last_read_at=datetime.now(UTC),
            )

        result = await _race(order, tid, read)
        if order == "delete_first":
            assert isinstance(result, NotFoundError), result
        else:
            assert result is None
            assert (await _rows(tid, []))["read"] == 0

    @pytest.mark.parametrize("order", _ORDERS)
    async def test_delete_against_a_variant_request(self, order: str) -> None:
        customer = await _recipient()
        tid = await _ticket(await _section(), customer)
        message = await _message(tid, customer)
        job = await _job()

        async def ask(s: AsyncSession) -> Any:
            return await write_request(
                s, thread_id=tid, message_id=message, locale="en",
                job_key=job, key=f"tk2:{uuid4()}", fingerprint="f" * 64,
            )

        result = await _race(order, tid, ask)
        if order == "delete_first":
            assert isinstance(result, NotFoundError), result
        else:
            assert isinstance(result, MessageVariantRequest)
            assert (await _rows(tid, [message]))["requests"] == 0

    async def test_delete_against_delete(self) -> None:
        tid = await _ticket(await _section(), await _recipient())
        first, second = await _cross(_deleting(tid), _deleting(tid))
        assert first is None
        assert second is None
        assert await _gone(tid)

    async def test_delete_against_the_close_pass(self) -> None:
        """A flagged thread deleted while the pass holds its read of it:
        the pass skips it and announces the next flagged thread."""
        section = await _section()
        gone = await _ticket(section, await _recipient())
        kept = await _ticket(section, await _recipient())
        async with get_session_factory()() as s:
            for tid in (gone, kept):
                await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)
            await s.commit()

        async def the_pass(_: AsyncSession) -> Any:
            return await consume_close_notifications()

        _, notified = await _cross(_deleting(gone), the_pass)
        assert notified == 1
        assert await _gone(gone)
        assert await _close_notices(gone) == 0
        assert await _close_notices(kept) == 1

    @pytest.mark.parametrize("order", _ORDERS)
    async def test_delete_against_auto_close(self, order: str) -> None:
        now = datetime.now(UTC)
        section = await _section()
        gone = await _ticket(section, await _recipient())
        kept = await _ticket(section, await _recipient())
        async with get_session_factory()() as s:
            await s.execute(
                update(Thread)
                .where(Thread.id.in_([gone, kept]))
                .values(created_at=now - timedelta(days=40))
            )
            await s.commit()

        async def auto_close(s: AsyncSession) -> Any:
            return await auto_close_idle_threads_batch(
                s, cutoff=now - timedelta(days=30), when=now, limit=1000
            )

        closed = await _race(order, gone, auto_close)
        assert closed == (1 if order == "delete_first" else 2)
        async with get_session_factory()() as s:
            row = await s.get(Thread, kept)
            assert row is not None
            assert row.status == ThreadStatus.CLOSED

    @pytest.mark.parametrize("order", _ORDERS)
    async def test_delete_against_forgetting(self, order: str) -> None:
        """Both delete the read pointer (thread, person): one waits for
        the other, in either order; no 40P01, no 500."""
        person, operator = await _recipient(), await _recipient()
        tid = await _dm(person, operator)
        messages = await _furnish(tid, person, operator)

        async def forget(s: AsyncSession) -> Any:
            return await forget_recipient(s, recipient_id=person, version=2)

        result = await _race(order, tid, forget)
        assert result is True, result
        assert await _rows(tid, messages) == {
            "threads": 0, "messages": 0, "read": 0, "variants": 0,
            "requests": 0,
        }
        async with get_session_factory()() as s:
            pointers = await s.scalar(
                select(func.count()).select_from(ThreadReadState)
                .where(ThreadReadState.participant == person)
            )
        assert pointers == 0

    async def test_a_message_is_authorized_on_the_taken_row(
        self, client: AsyncClient
    ) -> None:
        """TK-1's observation 2, closed: the previous holder -- off the
        roster -- writes while the thread is being handed to a colleague.
        The route takes the row before it decides who may write: it
        waits for the hand-over and answers 403."""
        holder, colleague = await _recipient(), await _recipient()
        section = await _section(holder, colleague)
        tid = await _ticket(section, await _recipient(), holder=holder)
        async with get_session_factory()() as s:
            await s.execute(
                delete(SectionMember).where(SectionMember.operator_id == holder)
            )
            await s.commit()
        async with get_session_factory()() as early:
            await assign_thread(
                early, thread=await early.get(Thread, tid), assignee=colleague
            )
            pid = await _pid(early)
            task = asyncio.create_task(client.post(
                f"/api/v1/threads/{tid}/messages",
                json={"sender": str(holder), "body": "late"},
            ))
            await _until_waiting(pid, task)
            await early.commit()
        resp = await asyncio.wait_for(task, _BOUND)
        assert resp.status_code == 403, resp.text
        assert (await _rows(tid, []))["messages"] == 0


# ---------------------------------------------------------------------------
# 3 -- what the thread gave birth to
# ---------------------------------------------------------------------------


async def _deliver(notification_id: UUID) -> Notification:
    async with get_session_factory()() as s:
        job = await s.get(Notification, notification_id)
        assert job is not None
        await service.resolve_notification(s, job)
        await service.deliver_notification(s, job)
        await service.rollup_notification(s, job)
        await s.commit()
    async with get_session_factory()() as s:
        done = await s.get(Notification, notification_id)
        assert done is not None
        return done


class TestBorn:
    async def test_an_issued_ping_is_delivered(self, client: AsyncClient) -> None:
        customer, master = await _recipient(), await _recipient()
        tid = await _dm(customer, master)
        posted = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(customer), "body": "hello"},
        )
        assert posted.status_code == 200
        async with get_session_factory()() as s:
            ping = await s.scalar(
                select(Notification.id).where(
                    Notification.target_value == str(master)
                )
            )
        assert ping is not None
        assert (await client.delete(f"/api/v1/threads/{tid}")).status_code == 204
        done = await _deliver(ping)
        assert done.status == NotificationStatus.SENT
        link = done.action_data or {}
        assert link["params"]["thread_id"] == str(tid)
        feed = await client.get(f"/api/v1/threads/{tid}/messages")
        assert feed.status_code == 404  # where the link now leads

    async def test_an_issued_close_notice_is_delivered(
        self, client: AsyncClient
    ) -> None:
        tid = await _ticket(await _section(), await _recipient())
        async with get_session_factory()() as s:
            await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)
            await s.commit()
        assert await consume_close_notifications() == 1
        async with get_session_factory()() as s:
            notice = await s.scalar(
                select(Notification.id).where(
                    Notification.idempotency_key.like(f"close:{tid}:%")
                )
            )
        assert notice is not None
        assert (await client.delete(f"/api/v1/threads/{tid}")).status_code == 204
        assert (await _deliver(notice)).status == NotificationStatus.SENT

    async def test_a_notice_not_issued_never_is(
        self, client: AsyncClient
    ) -> None:
        section = await _section()
        gone = await _ticket(section, await _recipient())
        kept = await _ticket(section, await _recipient())
        async with get_session_factory()() as s:
            for tid in (gone, kept):
                await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)
            await s.commit()
        assert (await client.delete(f"/api/v1/threads/{gone}")).status_code == 204
        assert await consume_close_notifications() == 1
        assert await _close_notices(gone) == 0
        assert await _close_notices(kept) == 1

    async def test_a_model_job_runs_on(self, client: AsyncClient) -> None:
        customer = await _recipient()
        tid = await _ticket(await _section(), customer)
        message = await _message(tid, customer)
        job = await _job(NotificationStatus.PROCESSING, answered=False)
        asked = await client.post(
            f"/api/v1/threads/{tid}/messages/{message}/variant-requests",
            json={"locale": "en", "job_key": job},
        )
        assert asked.status_code == 200, asked.text
        assert (await client.delete(f"/api/v1/threads/{tid}")).status_code == 204
        async with get_session_factory()() as s:
            row = await s.scalar(
                select(Notification).where(Notification.idempotency_key == job)
            )
            assert row is not None
            assert row.status == NotificationStatus.PROCESSING
            # the job answers after the deletion -- comms keeps the answer
            now = datetime.now(UTC)
            s.add(LLMAnswer(
                notification_id=row.id, body="{}", answered_set="llm.gw",
                answered_provider="p", answered_model="m", answered_at=now,
                keep_until=now + timedelta(days=1),
            ))
            await s.commit()
        written = await client.post(
            f"/api/v1/threads/{tid}/messages/{message}/variants",
            json={"locale": "en", "job_key": job, "source_locale": "de",
                  "body": "hi"},
        )
        assert written.status_code == 404


# ---------------------------------------------------------------------------
# 4 -- the price of the cascade
# ---------------------------------------------------------------------------

# THE RULE, written before the measurement (TK-2 gate, item 1):
#   (a) none of the four foreign-key cascades -- the statements the RI
#       triggers run, in their own text, planned GENERIC as the triggers'
#       cached plans are -- has a Seq Scan anywhere in its plan;
#   (b) deleting one thread of 50 messages, each with a variant and a
#       variant request, and two read pointers, among 50 000 messages in
#       1 000 threads, reads at most _DELETE_BLOCKS blocks (hit + read,
#       every relation of the schema, the whole transaction).
# WHY 1 200: by the indexes the cascade is per row of the thread -- the
#   thread, its 50 messages through ix_messages_thread_created, then for
#   each message one probe of uq_message_variants_message_locale and one
#   of uq_message_variant_requests_job (about three blocks each) and the
#   rows themselves, two read pointers through their primary key: about
#   50 x 9 + 30 = 480. 1 200 leaves 2.5 times that, and stays BELOW one
#   sequential read of `messages` (asserted: the heap is larger than the
#   limit), so a single Seq Scan anywhere on the hot table breaks it.
# EXPLAIN (ANALYZE, BUFFERS) of the DELETE counts only its own nodes:
#   the cascades run in triggers, outside the plan tree. So the blocks
#   are counted from the transaction's own statistics
#   (pg_stat_get_xact_blocks_fetched over every relation of the schema),
#   and the EXPLAIN is kept to show where the time of the triggers goes.
_DELETE_BLOCKS = 1200
_THREADS = 1000
_PER_THREAD = 50
_CASCADES = {
    "messages": 'DELETE FROM ONLY "public"."messages" '
                'WHERE $1 OPERATOR(pg_catalog.=) "thread_id"',
    "thread_read_states": 'DELETE FROM ONLY "public"."thread_read_states" '
                          'WHERE $1 OPERATOR(pg_catalog.=) "thread_id"',
    "message_variants": 'DELETE FROM ONLY "public"."message_variants" '
                        'WHERE $1 OPERATOR(pg_catalog.=) "message_id"',
    "message_variant_requests":
        'DELETE FROM ONLY "public"."message_variant_requests" '
        'WHERE $1 OPERATOR(pg_catalog.=) "message_id"',
}
_BLOCKS = (
    "SELECT COALESCE(sum(pg_stat_get_xact_blocks_fetched(c.oid)), 0) "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = 'public'"
)


@pytest.mark.usefixtures("pristine_tables")
class TestThePrice:
    async def _seed(self) -> UUID:
        """1 000 tickets of 50 messages; every fifth message with a
        variant and a request; two pointers per thread. The thread to
        delete is the heaviest: every one of its messages has both."""
        people = [await _recipient() for _ in range(20)]
        section = await _section()
        async with get_session_factory()() as s:
            await s.execute(text(
                "INSERT INTO threads (id, client, operator_kind, "
                "operator_value, kind, status, created_at, idempotency_key, "
                "fingerprint) SELECT gen_random_uuid(), "
                "(CAST(:people AS uuid[]))[1 + g % 20], 'section', "
                ":section, 'ticket', 'open', now() - interval '1 day', "
                "'seed:t:' || g, repeat('f', 64) "
                "FROM generate_series(1, :n) AS g"
            ), {"people": people, "section": section, "n": _THREADS})
            await s.execute(text(
                "INSERT INTO messages (id, thread_id, sender, body, "
                "idempotency_key, fingerprint, created_at) "
                "SELECT gen_random_uuid(), t.id, t.client, repeat('x', 300), "
                "'seed:m:' || t.id || ':' || g, repeat('f', 64), "
                "now() - g * interval '1 s' "
                "FROM threads t, generate_series(1, :n) AS g"
            ), {"n": _PER_THREAD})
            target = await s.scalar(select(Thread.id).limit(1))
            assert target is not None
            for table, extra in (
                ("message_variant_requests", "job_id"),
                ("message_variants", "body, declined, source_locale"),
            ):
                values = (
                    "gen_random_uuid()" if extra == "job_id"
                    else "'hi', false, 'de'"
                )
                await s.execute(text(
                    f"INSERT INTO {table} (id, message_id, locale, job_key, "
                    f"idempotency_key, fingerprint, {extra}) "
                    "SELECT gen_random_uuid(), m.id, 'en', 'job:' || m.id, "
                    f"'seed:{table}:' || m.id, repeat('f', 64), {values} "
                    "FROM (SELECT id, thread_id, row_number() OVER () AS r "
                    "      FROM messages) m "
                    "WHERE m.thread_id = :target OR m.r % 5 = 0"
                ), {"target": target})
            await s.execute(text(
                "INSERT INTO thread_read_states (thread_id, participant, "
                "last_read_at) SELECT t.id, p, now() FROM threads t, "
                "unnest(CAST(:pair AS uuid[])) AS p"
            ), {"pair": people[:2]})
            await s.commit()
        async with get_engine().connect() as connection:
            auto = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await auto.execute(text("SET default_statistics_target = 1000"))
            await auto.execute(text("ANALYZE"))
        return target

    async def test_the_cascades_and_one_deletion(self) -> None:
        target = await self._seed()
        async with get_session_factory()() as s:
            await s.execute(text(
                "CREATE FUNCTION pg_temp.generic_plan(q text) RETURNS SETOF "
                "text LANGUAGE plpgsql AS $f$ BEGIN RETURN QUERY EXECUTE "
                "'EXPLAIN (GENERIC_PLAN) ' || q; END $f$"
            ))
            for table, statement in _CASCADES.items():
                plan = "\n".join(await s.scalars(text(
                    "SELECT * FROM pg_temp.generic_plan(:q)"
                ), {"q": statement}))
                print(f"\nGENERIC {table}\n{plan}")
                assert "$1" in plan, plan  # generic: the value never bound
                assert "Seq Scan" not in plan, plan
                assert "Index" in plan, plan
            await s.rollback()

        async with get_session_factory()() as s:
            pages = await s.scalar(text(
                "SELECT relpages FROM pg_class WHERE relname = 'messages'"
            ))
            assert pages is not None and pages > _DELETE_BLOCKS, pages
            before = await s.scalar(text(_BLOCKS))
            await delete_thread(s, thread_id=target)
            after = await s.scalar(text(_BLOCKS))
            gone = await s.scalar(select(func.count()).select_from(Message)
                                  .where(Message.thread_id == target))
            await s.rollback()
        blocks = int(after) - int(before)
        print(f"\nDELETE one thread: {blocks} blocks; messages heap {pages}")
        assert gone == 0
        # the instrument's pair: the counter saw the cascade at all --
        # at least a block per message deleted
        assert blocks >= _PER_THREAD, blocks
        assert blocks <= _DELETE_BLOCKS, blocks

        async with get_session_factory()() as s:
            raw = await s.connection()
            plan: Any = (await raw.exec_driver_sql(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
                "DELETE FROM threads WHERE id = $1", (target,),
            )).scalar_one()
            await s.rollback()
        triggers = plan[0].get("Triggers", [])
        print("TRIGGERS", json.dumps(triggers))
        assert {t["Relation"] for t in triggers} >= {"threads", "messages"}


# ---------------------------------------------------------------------------
# 5 -- the contract
# ---------------------------------------------------------------------------

_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


def _section_14() -> str:
    start = _DOC.index("\n## 14. ")
    return _DOC[start:_DOC.index("\n## ", start + 1)]


class TestTheContract:
    def test_the_route_is_the_documented_one(self) -> None:
        from app.main import app

        item = app.openapi()["paths"]["/api/v1/threads/{thread_id}"]
        assert set(item) == {"delete"}
        assert set(item["delete"]["responses"]) >= {"204"}
        assert "requestBody" not in item["delete"]
        # Matched as patterns of the document, not as SQL text: the
        # route's verb is the HTTP one (tests/test_g2_hygiene.py reads a
        # literal `in` test of it as a statement search).
        deleting = _section_14().split("### Deleting a thread", 1)[1]
        assert re.search(r"`DELETE /api/v1/threads/\{id\}`", deleting)
        assert "**204**" in deleting
        repeats = _DOC.split("| call | why a repeat is safe |", 1)[1]
        table = repeats.split("\n\n", 1)[0]
        assert re.search(r"^\| `DELETE /threads/\{id\}` \|", table, re.M)

    def test_the_separate_lines(self) -> None:
        """The two things velo builds on are said as lines of their own."""
        deleting = _section_14().split("### Deleting a thread", 1)[1]
        assert "**A message ping or a close notice already issued is " \
               "delivered.**" in deleting
        assert "**The feed of a thread that does not exist is 404" in deleting
        assert "never was one alike" in deleting

    def test_no_sentence_says_comms_deletes_nothing(self) -> None:
        """The premise this change ended, in every form it was written."""
        forms = ("deletes no message", "comms deletes none", "immortal",
                 "eternal")
        found = [f for f in forms if f in _DOC.lower()]
        assert found == []
        assert "### Deleting a thread" in _DOC  # the pair
