# =============================================================================
# MSG-1 -- the client's thread list; the last message (and its variant)
# in a row of either list
# =============================================================================
#
#   1 the client list: only the client's threads; the operator's order,
#     keyset and cursor; unread counted for the client; the refusals
#   2 the last message: opt-in in both lists, byte for byte without it;
#     null for a thread with none; the feed's order, ties by id
#   3 the preview in the reader's language: variant and variant_state
#     from the feed's own reader_states -- every state, both directions,
#     and the same state the feed gives the same message
#   4 the cost: the client's page and the page's last messages on
#     pristine tables with fresh statistics (the decision rule of the
#     gate, fixed before the measurement)
#   5 the contract (deploy/INTEGRATION.md section 13) against the code
#
# MUTATIONS (one run, every one red here):
#   M1 the client list without WHERE client          -> test_only_the_
#                                                       clients_threads
#   M2 the client read as an operator (visible)       -> the same
#   M3 the client's unread counts their own message   -> test_unread_is_
#                                                       the_clients
#   M4 "last" by created_at alone, no id              -> test_ties_are_
#                                                       decided_by_id
#   M5 the preview decides its own state (row 4 gone) -> TestThePreview
#   M6 last_message without the parameter             -> TestByteForByte
#   M7 locale without with_last_message ignored       -> test_refusals
#   M8 a thread with no message left out, not null    -> test_no_message_
#                                                       is_null
# =============================================================================

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import event, text

from app.api.messaging import (
    FEED_VARIANT_FIELDS,
    LAST_MESSAGE_FIELDS,
)
from app.core.database import get_engine, get_session_factory
from app.messaging.constants import VariantState
from app.messaging.operators import list_client_threads
from app.messaging.threads import last_messages
from tests.helpers import create_recipient, next_phase4c_telegram_id, sql_shape
from tests.test_message_variants import _job, _request, _text, _variant

_REPO = Path(__file__).resolve().parents[1]


async def _person() -> UUID:
    async with get_session_factory()() as session:
        rid = uuid4()
        await create_recipient(
            session, recipient_id=rid, telegram_id=next_phase4c_telegram_id(),
        )
        await session.commit()
    return rid


async def _dm(client: AsyncClient, person: UUID, operator: UUID) -> str:
    created = await client.post("/api/v1/threads", json={
        "client": str(person), "operator_kind": "user",
        "operator_value": str(operator), "kind": "dm",
    })
    assert created.status_code == 200, created.text
    thread: str = created.json()["id"]
    return thread


async def _say(client: AsyncClient, thread: str, sender: UUID, body: str) -> str:
    posted = await client.post(f"/api/v1/threads/{thread}/messages",
                               json={"sender": str(sender), "body": body})
    assert posted.status_code == 200, posted.text
    message: str = posted.json()["id"]
    return message


async def _list(http: AsyncClient, **params: Any) -> Any:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return await http.get(f"/api/v1/threads?{query}")


def _ids(response: Any) -> list[str]:
    return [row["id"] for row in response.json()["items"]]


# ---------------------------------------------------------------------------
# 1 -- the client list
# ---------------------------------------------------------------------------


class TestTheClientList:
    async def test_only_the_clients_threads(self, client: AsyncClient) -> None:
        """Mine as a client: in. Another client's: out. One where I am
        the operator: out -- the pairs that make "only" mean something."""
        me, master, other = await _person(), await _person(), await _person()
        mine = await _dm(client, me, master)
        theirs = await _dm(client, other, master)
        i_operate = await _dm(client, other, me)
        listed = _ids(await _list(client, client=me))
        assert listed == [mine]
        assert theirs not in listed and i_operate not in listed
        # The pair of the last one: as an operator I do see it.
        assert i_operate in _ids(await _list(client, operator=me))

    async def test_order_pages_and_cursor(self, client: AsyncClient) -> None:
        """The operator's order: most recent activity first, a message
        moves a thread up; pages of 2 by the cursor, to the end."""
        me = await _person()
        threads = [await _dm(client, me, await _person()) for _ in range(5)]
        await _say(client, threads[0], me, "bump")  # oldest becomes newest
        expected = [threads[0], *reversed(threads[1:])]
        seen: list[str] = []
        cursor = None
        while True:
            params: dict[str, Any] = {"client": me, "limit": 2}
            if cursor:
                params["cursor"] = cursor
            page = (await _list(client, **params)).json()
            seen += [row["id"] for row in page["items"]]
            cursor = page["next_cursor"]
            if cursor is None:
                break
        assert seen == expected

    async def test_unread_is_the_clients(self, client: AsyncClient) -> None:
        me, master = await _person(), await _person()
        thread = await _dm(client, me, master)

        async def unread() -> int:
            response = await _list(client, client=me, with_unread="true")
            (row,) = response.json()["items"]
            count: int = row["unread"]
            return count

        await _say(client, thread, me, "mine")
        assert await unread() == 0          # one's own message is not unread
        await _say(client, thread, master, "theirs")
        assert await unread() == 1
        read = await client.post(f"/api/v1/threads/{thread}/read",
                                 json={"participant": str(me)})
        assert read.status_code == 200
        assert await unread() == 0

    @pytest.mark.parametrize(
        "query",
        [
            "",                                      # no reader
            "operator={a}&client={b}",               # both
            "client={a}&is_supervisor=true",         # widening a client
            "client=not-a-uuid",
            "client={a}&client={b}",                 # twice (G1)
            "client={a}&locale=en",                  # locale, no preview
            "operator={a}&locale=en",
            "client={a}&with_last_message=true&locale=toolong12",
            "client={a}&with_last_message=true&locale=en&locale=de",
        ],
    )
    async def test_refusals(self, client: AsyncClient, query: str) -> None:
        response = await client.get(
            "/api/v1/threads?" + query.format(a=uuid4(), b=uuid4()),
        )
        assert response.status_code == 422, response.text
        assert response.json()["error"]["class"] == "validation"


# ---------------------------------------------------------------------------
# 2 -- the last message
# ---------------------------------------------------------------------------


class TestTheLastMessage:
    async def test_in_both_lists(self, client: AsyncClient) -> None:
        me, master = await _person(), await _person()
        thread = await _dm(client, me, master)
        await _say(client, thread, me, "first")
        last = await _say(client, thread, master, "second -- emu-3301")
        for params in ({"client": me}, {"operator": master}):
            (row,) = (await _list(
                client, with_last_message="true", **params)).json()["items"]
            preview = row["last_message"]
            assert set(preview) == set(LAST_MESSAGE_FIELDS)
            assert (preview["id"], preview["sender"], preview["body"]) == (
                last, str(master), "second -- emu-3301",
            )

    async def test_no_message_is_null(self, client: AsyncClient) -> None:
        """The key is there and null -- not missing. The pair: a thread
        with a message on the same page has it."""
        me = await _person()
        empty = await _dm(client, me, await _person())
        spoken = await _dm(client, me, await _person())
        await _say(client, spoken, me, "hello")
        rows = {row["id"]: row for row in (await _list(
            client, client=me, with_last_message="true")).json()["items"]}
        assert "last_message" in rows[empty] and rows[empty]["last_message"] is None
        assert rows[spoken]["last_message"]["body"] == "hello"

    async def test_ties_are_decided_by_id(self, client: AsyncClient) -> None:
        """Two messages of one instant: the last is the one the feed
        gives first -- the greater id."""
        me, master = await _person(), await _person()
        thread = await _dm(client, me, master)
        a = await _say(client, thread, me, "a")
        b = await _say(client, thread, master, "b")
        async with get_session_factory()() as session:
            await session.execute(
                text("UPDATE messages SET created_at = :at WHERE thread_id = :t"),
                {"at": datetime(2026, 1, 1, tzinfo=UTC), "t": UUID(thread)},
            )
            await session.commit()
        feed_first = (await client.get(
            f"/api/v1/threads/{thread}/messages")).json()["items"][0]["id"]
        assert feed_first == max(a, b, key=UUID)
        (row,) = (await _list(client, client=me, with_last_message="true")
                  ).json()["items"]
        assert row["last_message"]["id"] == feed_first


class TestByteForByte:
    @pytest.mark.parametrize("who", ["operator", "client"])
    async def test_without_the_new_parameters(
        self, client: AsyncClient, who: str,
    ) -> None:
        """Without with_last_message: no key on any row; and for the
        operator, the bytes are the list's as they were (the same
        response with variants and requests written)."""
        me, master = await _person(), await _person()
        thread = await _dm(client, me, master)
        message = await _say(client, thread, me, "hello")
        params = {who: me if who == "client" else master, "with_unread": "true"}
        before = await _list(client, **params)
        assert all("last_message" not in row for row in before.json()["items"])
        assert all("variant_state" not in json.dumps(row)
                   for row in before.json()["items"])
        job = await _job()
        await _request(client, thread, message, "en", job)
        await _variant(client, thread, message, _text("en", job))
        after = await _list(client, **params)
        assert after.content == before.content
        # The pair: with the parameter the key is there.
        assert "last_message" in (await _list(
            client, with_last_message="true", **params)).json()["items"][0]


# ---------------------------------------------------------------------------
# 3 -- the preview in the reader's language
# ---------------------------------------------------------------------------

_DIRECTIONS = ["operator_to_client", "client_to_operator"]


async def _preview(http: AsyncClient, reader: dict[str, Any],
                   locale: str) -> dict[str, Any]:
    (row,) = (await _list(http, with_last_message="true", locale=locale,
                          **reader)).json()["items"]
    preview: dict[str, Any] = row["last_message"]
    return preview


async def _feed_item(client: AsyncClient, thread: str, locale: str) -> Any:
    return (await client.get(
        f"/api/v1/threads/{thread}/messages?locale={locale}")).json()["items"][0]


@pytest.mark.parametrize("direction", _DIRECTIONS)
class TestThePreview:
    """Every state, in a row of the list -- and the very state the feed
    gives the same message with the same locale (the pair of "one code
    decides")."""

    async def _setup(self, client: AsyncClient, direction: str
                     ) -> tuple[str, str, dict[str, Any]]:
        me, master = await _person(), await _person()
        thread = await _dm(client, me, master)
        writer, reader = ((master, {"client": me})
                          if direction == "operator_to_client"
                          else (me, {"operator": master}))
        message = await _say(client, thread, writer, "Original -- tapir-8812")
        return thread, message, reader

    async def _same_as_feed(self, client: AsyncClient, thread: str,
                            reader: dict[str, Any], locale: str,
                            expected: VariantState) -> dict[str, Any]:
        preview = await _preview(client, reader, locale)
        feed = await _feed_item(client, thread, locale)
        assert preview["variant_state"] == feed["variant_state"] == expected
        assert preview["variant"] == feed["variant"]
        assert preview["body"] == "Original -- tapir-8812"
        return preview

    async def test_ready(self, client: AsyncClient, direction: str) -> None:
        thread, message, reader = await self._setup(client, direction)
        await _variant(client, thread, message, _text("de", await _job(), "en"))
        preview = await self._same_as_feed(client, thread, reader, "de",
                                           VariantState.READY)
        assert set(preview["variant"]) == set(FEED_VARIANT_FIELDS)

    async def test_not_needed(self, client: AsyncClient, direction: str) -> None:
        """Through another variant whose source is the reader's language
        (row 4 of the order -- the one a second copy would forget)."""
        thread, message, reader = await self._setup(client, direction)
        await _variant(client, thread, message, _text("de", await _job(), "en"))
        await self._same_as_feed(client, thread, reader, "en",
                                 VariantState.NOT_NEEDED)

    async def test_preparing(self, client: AsyncClient, direction: str) -> None:
        thread, message, reader = await self._setup(client, direction)
        await _request(client, thread, message, "de",
                       await _job("pending", answered=False))
        await self._same_as_feed(client, thread, reader, "de",
                                 VariantState.PREPARING)

    async def test_impossible(self, client: AsyncClient, direction: str) -> None:
        thread, message, reader = await self._setup(client, direction)
        await _request(client, thread, message, "de",
                       await _job("failed", answered=False))
        await self._same_as_feed(client, thread, reader, "de",
                                 VariantState.IMPOSSIBLE)

    async def test_not_requested(self, client: AsyncClient, direction: str) -> None:
        thread, _, reader = await self._setup(client, direction)
        preview = await self._same_as_feed(client, thread, reader, "de",
                                           VariantState.NOT_REQUESTED)
        assert preview["variant"] is None


# ---------------------------------------------------------------------------
# 4 -- the cost (pristine tables, fresh statistics)
# ---------------------------------------------------------------------------

# The gate's rule, fixed before the measurement:
#   client page: a client with 2000 threads -> at most 300 buffers and
#     no Sort of all their threads, or the index
#     (client, activity DESC, id DESC) is needed;
#   last messages: a Limit over the index per thread, never a read of
#     every message of the page's threads, or the index
#     (thread_id, created_at DESC, id DESC) is needed.
_HEAVY_THREADS = 2000
_OTHER_THREADS = 40_000
_MESSAGES_PER_THREAD = 300
_PAGE = 100
_CLIENT_PAGE_BUFFERS = 300
# A top-1 per thread through the index: a few buffers per thread.
_LAST_BUFFERS = 600


async def _plan(statement: str, parameters: Any) -> dict[str, Any]:
    async with get_session_factory()() as session:
        raw = await session.connection()
        plan: Any = (await raw.exec_driver_sql(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement, parameters,
        )).scalar_one()
        await session.rollback()
    return dict(plan[0])


def _buffers(plan: dict[str, Any]) -> int:
    top = plan["Plan"]
    return int(top["Shared Hit Blocks"]) + int(top.get("Shared Read Blocks", 0))


def _nodes(plan: dict[str, Any]) -> list[str]:
    return re.findall(r'"Node Type": "([^"]+)"', json.dumps(plan))


async def _captured(run: Any) -> list[tuple[str, Any]]:
    seen: list[tuple[str, Any]] = []

    def record(conn: Any, cursor: Any, statement: str, parameters: Any,
               *rest: Any) -> None:
        seen.append((statement, parameters))

    engine = get_engine().sync_engine
    event.listen(engine, "before_cursor_execute", record)
    try:
        await run()
    finally:
        event.remove(engine, "before_cursor_execute", record)
    return seen


@pytest.mark.usefixtures("pristine_tables")
class TestTheCost:
    async def _seed(self) -> tuple[UUID, list[UUID]]:
        heavy, others = await _person(), [await _person() for _ in range(20)]
        async with get_session_factory()() as session:
            insert = (
                "INSERT INTO threads (id, client, operator_kind, operator_value,"
                " kind, status, last_message_at, created_at, idempotency_key,"
                " fingerprint) SELECT gen_random_uuid(), :client, 'user',"
                " gen_random_uuid(), 'dm', 'open', now() - g * interval '1 min',"
                " now() - interval '1 year', :tag || 'k' || g, repeat('f', 64)"
                " FROM generate_series(1, :n) AS g"
            )
            await session.execute(text(insert), {
                "client": heavy, "tag": "h", "n": _HEAVY_THREADS})
            for n, other in enumerate(others):
                await session.execute(text(insert), {
                    "client": other, "tag": f"o{n}:",
                    "n": _OTHER_THREADS // len(others)})
            # The heavy client's newest 100 threads talk a lot: 300
            # messages each, the last two of one instant.
            await session.execute(text(
                "INSERT INTO messages (id, thread_id, sender, body,"
                " idempotency_key, fingerprint, created_at)"
                " SELECT gen_random_uuid(), t.id, :client, 'm',"
                " 'seed:' || t.id || ':' || g, repeat('f', 64),"
                " now() - least(g, :n - 1) * interval '1 s'"
                " FROM (SELECT id FROM threads WHERE client = :client"
                "       ORDER BY last_message_at DESC LIMIT :page) t,"
                " generate_series(1, :n) AS g"
            ), {"client": heavy, "n": _MESSAGES_PER_THREAD, "page": _PAGE})
            await session.commit()
        async with get_engine().connect() as connection:
            auto = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await auto.execute(text("SET default_statistics_target = 1000"))
            await auto.execute(text("ANALYZE"))
        async with get_session_factory()() as session:
            page, _ = await list_client_threads(session, client=heavy, limit=_PAGE)
        return heavy, [thread.id for thread in page]

    async def test_the_client_page_and_its_last_messages(self) -> None:
        heavy, page = await self._seed()
        assert len(page) == _PAGE

        async def client_page() -> None:
            async with get_session_factory()() as session:
                await list_client_threads(session, client=heavy, limit=_PAGE)

        async def last_of_page() -> None:
            async with get_session_factory()() as session:
                last = await last_messages(session, page)
                assert len(last) == _PAGE

        (listing,) = [
            (s, p) for s, p in await _captured(client_page)
            if sql_shape(s).table == "threads"
        ]
        (lateral,) = [
            (s, p) for s, p in await _captured(last_of_page)
            if "messages" in sql_shape(s).sources
        ]
        listing_plan = await _plan(*listing)
        lateral_plan = await _plan(*lateral)
        print("\nCOST client page", _buffers(listing_plan), _nodes(listing_plan))
        print("COST last messages", _buffers(lateral_plan), _nodes(lateral_plan))
        assert "Seq Scan" not in _nodes(listing_plan)
        assert "Seq Scan" not in _nodes(lateral_plan)
        assert "Sort" not in _nodes(listing_plan), listing_plan
        assert _buffers(listing_plan) <= _CLIENT_PAGE_BUFFERS, listing_plan
        assert _buffers(lateral_plan) <= _LAST_BUFFERS, lateral_plan
        assert "Limit" in _nodes(lateral_plan)


# ---------------------------------------------------------------------------
# 5 -- the contract
# ---------------------------------------------------------------------------

_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


def _section_13() -> str:
    start = _DOC.index("\n## 13. ")
    return _DOC[start:_DOC.index("\n## ", start + 1)]


class TestTheContract:
    def test_the_parameters_are_the_routes(self) -> None:
        from app.main import app

        params = [
            p["name"] for p in
            app.openapi()["paths"]["/api/v1/threads"]["get"]["parameters"]
            if p["in"] == "query"
        ]
        section = _section_13()
        table = section.split("| parameter |", 1)[1].split("\n\n", 1)[0]
        documented = re.findall(r"^\| `([a-z_]+)` \|", table, re.M)
        assert documented == params

    def test_the_last_message_fields_and_the_states(self) -> None:
        section = _section_13()
        row = next(line for line in section.splitlines()
                   if line.startswith("| `last_message` |"))
        assert re.findall(r"`([a-z_]+)`", row.split("|")[3].split(";")[0]) == list(
            LAST_MESSAGE_FIELDS,
        )
        table = section.split("### The preview in the reader's language", 1)[1]
        body = table.split("|---|---|", 1)[1]
        states = re.findall(r"^\| `([a-z_]+)` \| ", body, re.M)
        assert set(states) == {state.value for state in VariantState}
        not_requested = next(line for line in section.splitlines()
                             if line.startswith("| `not_requested` |"))
        assert "do not show the original" in not_requested

