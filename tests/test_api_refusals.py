# =============================================================================
# API-2 -- a client's error is a refusal of its class; a 500 is comms' own
# =============================================================================
#
# What is held here, against the code and against deploy/INTEGRATION.md
# (section 5's class table, section 6 Preferences):
#   1 a body string the database cannot store (a NUL, a lone surrogate --
#     a value or a key, at any depth) is a 422 `validation` naming the
#     top-level field, never the value, on EVERY route that takes a body
#     -- the list here is the application's list (a watchman); the same
#     rule as the stream's (app/core/storable.py);
#   2 a preference write for a forgotten recipient is a 409
#     `recipient_deleted` and writes nothing; against forgetting on real
#     sessions, both orders: the invariant "a forgotten person has no
#     schedule and no mute";
#   3 a 4xx the framework raises that no class names is `bad_request`,
#     never `internal`;
#   4 /ready's 503 says why in the log, without the database's text;
#   5 claim's two refusals say which one it is.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 refuse_unstorable_body taken out of the app's dependencies ->
#      TestUnstorableBody (every route)
#   M2 the rule letting NUL through (app/core/storable.py) ->
#      TestUnstorableBody, TestTheOneRule
#   M3 the forgotten check taken out of _take_live_recipient ->
#      TestForgottenPreferences
#   M4 the recipient row read, not taken -> TestTheForgettingRace
#      test_forgetting_first (a mute of a forgotten person)
#   M5 INTERNAL as the default of an unmapped 4xx -> TestUnmapped4xx
#   M6 /ready's log line taken out -> TestReadySaysWhy
#   M7 one text for both claim refusals -> TestClaimRefusals
#
# Band 94260-94659 (tests/helpers.next_api1_telegram_id), shared with
# tests/test_operator_rights.py -- one counter per process.
# =============================================================================

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.routing import APIRoute
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request
from structlog.testing import capture_logs

import app.main as main_module
from app.audience.models import CategoryMute, Recipient
from app.audience.prefs import set_category_muted, set_schedule
from app.core.config import settings
from app.core.database import get_session_factory
from app.core.exceptions import RecipientDeletedError, ValidationError
from app.forgetting import forget_recipient
from app.main import app
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Section
from app.messaging.threads import create_or_get_thread
from app.profile.registry import registry
from app.transport.events import parse_event
from tests.helpers import (
    create_recipient,
    create_section,
    declare_member,
    intake_fields,
    next_api1_telegram_id,
)

_BOUND = 10.0
_POLL = 0.02
_UNSTORABLE_SAID = "is a string the database cannot store"
_BAD = {"nul": "\x00", "surrogate": "\ud800"}


def _raw(body: Any) -> dict[str, Any]:
    """The body as JSON bytes: httpx will not encode a lone surrogate,
    json.dumps writes it as the escape a product's JSON would carry."""
    return {
        "content": json.dumps(body).encode("ascii"),
        "headers": {"content-type": "application/json"},
    }


async def _recipient() -> UUID:
    rid = uuid4()
    async with get_session_factory()() as s:
        await create_recipient(
            s, recipient_id=rid, telegram_id=next_api1_telegram_id()
        )
        await s.commit()
    return rid


async def _forgotten() -> UUID:
    rid = await _recipient()
    async with get_session_factory()() as s:
        assert await forget_recipient(s, recipient_id=rid, version=2)
        await s.commit()
    return rid


def _a_category() -> str:
    return sorted(registry.registered_categories())[0]


# ---------------------------------------------------------------------------
# 1 -- a string the database cannot store
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Body:
    method: str
    path: str
    field: str
    body: Callable[[str], Any]


_T = "00000000-0000-4000-8000-000000000001"
_M = "00000000-0000-4000-8000-000000000002"
_R = "00000000-0000-4000-8000-000000000003"
_U = str(uuid4())

# Every route with a body: (method, the route's path), the field to spoil,
# and a body around it. The other fields are well-formed, so only the
# spoiled string can be what is refused.
_BODIES = [
    _Body("PATCH", "/api/v1/recipients/{recipient_id}/preferences", "categories",
          lambda b: {"categories": {f"c{b}": True}}),
    _Body("PATCH", "/api/v1/recipients/{recipient_id}/preferences", "schedule",
          lambda b: {"schedule": [{"day": f"mon{b}", "from": "09:00", "to": "10:00"}]}),
    _Body("POST", "/api/v1/threads", "title",
          lambda b: {"client": _U, "operator_kind": "section",
                     "operator_value": _U, "kind": "ticket", "title": f"t{b}"}),
    _Body("POST", "/api/v1/threads/unread-counts", "participant",
          lambda b: {"participant": f"{_U}{b}", "thread_ids": []}),
    _Body("POST", "/api/v1/threads/{thread_id}/messages", "body",
          lambda b: {"sender": _U, "body": f"hello{b}"}),
    _Body("POST", "/api/v1/threads/{thread_id}/messages/{message_id}/variant-requests",
          "job_key", lambda b: {"locale": "de", "job_key": f"k{b}"}),
    _Body("POST", "/api/v1/threads/{thread_id}/messages/{message_id}/variants",
          "body", lambda b: {"locale": "de", "job_key": "k", "body": f"x{b}"}),
    _Body("POST", "/api/v1/threads/{thread_id}/read", "participant",
          lambda b: {"participant": f"{_U}{b}"}),
    _Body("POST", "/api/v1/threads/{thread_id}/claim", "operator",
          lambda b: {"operator": f"{_U}{b}"}),
    _Body("POST", "/api/v1/threads/{thread_id}/assign", "assignee",
          lambda b: {"operator": _U, "assignee": f"{_U}{b}"}),
    _Body("POST", "/api/v1/threads/{thread_id}/release", "operator",
          lambda b: {"operator": f"{_U}{b}"}),
    _Body("POST", "/api/v1/threads/{thread_id}/status", "status",
          lambda b: {"operator": _U, "status": f"resolved{b}"}),
    _Body("POST", "/api/v1/threads/{thread_id}/retag", "subject_id",
          lambda b: {"operator": _U, "section": _U, "subject_type": "practice",
                     "subject_id": f"p{b}"}),
    _Body("POST", "/api/v1/sections", "key",
          lambda b: {"key": f"k{b}", "label": "L"}),
    _Body("POST", "/api/v1/sections", "label",
          lambda b: {"key": "k", "label": f"L{b}"}),
    _Body("PUT", "/api/v1/recipients/{recipient_id}", "locale",
          lambda b: {"version": 1, "telegram_id": None, "email": None,
                     "locale": f"e{b}n", "timezone": None, "active": True}),
    _Body("DELETE", "/api/v1/recipients/{recipient_id}", "version",
          lambda b: {"version": f"1{b}"}),
]


def _url(path: str) -> str:
    return path.format(thread_id=_T, message_id=_M, recipient_id=_R)


def _body_routes() -> set[tuple[str, str]]:
    """Every (method, path) of the application whose route takes a body."""
    found: set[tuple[str, str]] = set()
    for included in app.router.routes:
        router = getattr(included, "original_router", None)
        for route in getattr(router, "routes", ()):
            if isinstance(route, APIRoute) and route.body_field is not None:
                found |= {(m, route.path) for m in route.methods}
    return found


class TestUnstorableBody:
    def test_every_route_with_a_body_is_here(self) -> None:
        """The watchman: a route added with a body is added here."""
        routes = _body_routes()
        assert len(routes) > 10, "the pair: the walk finds the routes"
        assert {(b.method, b.path) for b in _BODIES} == routes

    @pytest.mark.parametrize("bad", sorted(_BAD))
    @pytest.mark.parametrize(
        "case", _BODIES, ids=[f"{b.method}-{b.path}-{b.field}" for b in _BODIES]
    )
    async def test_refused_naming_the_field(
        self, client: AsyncClient, case: _Body, bad: str
    ) -> None:
        resp = await client.request(
            case.method, _url(case.path), **_raw(case.body(_BAD[bad]))
        )
        assert resp.status_code == 422, resp.text
        error = resp.json()["error"]
        assert error["class"] == "validation"
        assert f"field {case.field!r} {_UNSTORABLE_SAID}" in error["message"]
        assert "\x00" not in resp.text and "\\u0000" not in resp.text
        assert "\\ud800" not in resp.text

    @pytest.mark.parametrize("bad", sorted(_BAD))
    async def test_a_field_name(self, client: AsyncClient, bad: str) -> None:
        resp = await client.post(
            "/api/v1/sections", **_raw({"key": "k", "label": "L", f"x{_BAD[bad]}": 1})
        )
        assert resp.status_code == 422, resp.text
        assert f"a field name {_UNSTORABLE_SAID}" in resp.json()["error"]["message"]

    @pytest.mark.parametrize("bad", sorted(_BAD))
    async def test_nothing_is_stored(self, client: AsyncClient, bad: str) -> None:
        """The two routes that would write the string on their own: the
        section and the recipient snapshot -- no row appears."""
        key = f"api2-{uuid4().hex[:8]}"
        resp = await client.post(
            "/api/v1/sections", **_raw({"key": key, "label": f"L{_BAD[bad]}"})
        )
        assert resp.status_code == 422
        rid = uuid4()
        resp = await client.put(
            f"/api/v1/recipients/{rid}",
            **_raw({"version": 1, "telegram_id": None, "email": None,
                    "locale": f"e{_BAD[bad]}n", "timezone": None,
                    "active": True}),
        )
        assert resp.status_code == 422
        async with get_session_factory()() as s:
            assert await s.scalar(
                select(func.count()).select_from(Section).where(Section.key == key)
            ) == 0
            assert await s.get(Recipient, rid) is None

    async def test_an_ordinary_string_goes_through(
        self, client: AsyncClient
    ) -> None:
        """The pair: the same routes with a plain string answer as before."""
        key = f"api2-{uuid4().hex[:8]}"
        resp = await client.post(
            "/api/v1/sections", **_raw({"key": key, "label": "L"})
        )
        assert resp.status_code in (200, 201), resp.text
        assert resp.json()["key"] == key
        rid = uuid4()
        resp = await client.put(
            f"/api/v1/recipients/{rid}",
            **_raw({"version": 1, "telegram_id": None, "email": None,
                    "locale": "en", "timezone": None, "active": True}),
        )
        assert resp.status_code in (200, 201), resp.text

    async def test_the_check_runs_before_the_model(
        self, client: AsyncClient
    ) -> None:
        """`label` of the wrong type would be pydantic's 422 -- the answer
        names `key`: the check ran first, on the same body."""
        resp = await client.post(
            "/api/v1/sections", **_raw({"key": "k\x00", "label": 5})
        )
        assert resp.status_code == 422
        assert f"field 'key' {_UNSTORABLE_SAID}" in resp.json()["error"]["message"]


class TestTheOneRule:
    @pytest.mark.parametrize("bad", sorted(_BAD))
    def test_the_stream_says_the_same(self, bad: str) -> None:
        """The stream's refusal is the same rule, the same words."""
        data = {"v": 1, "recipient_id": str(uuid4()), "version": 1,
                "telegram_id": None, "email": None,
                "locale": f"e{_BAD[bad]}n", "timezone": None, "active": True}
        with pytest.raises(ValidationError) as raised:
            parse_event({"event": "user_upserted", "data": json.dumps(data)})
        assert f"field 'locale' {_UNSTORABLE_SAID}" in str(raised.value)


# ---------------------------------------------------------------------------
# 2 -- a forgotten recipient has no preferences
# ---------------------------------------------------------------------------


def _prefs(rid: UUID) -> str:
    return f"/api/v1/recipients/{rid}/preferences"


async def _mutes(rid: UUID) -> int:
    async with get_session_factory()() as s:
        return int(await s.scalar(
            select(func.count()).select_from(CategoryMute)
            .where(CategoryMute.recipient_id == rid)
        ) or 0)


async def _windows(rid: UUID) -> Any:
    async with get_session_factory()() as s:
        row = await s.get(Recipient, rid)
        assert row is not None
        return row.allowed_windows


_SCHEDULE = [{"day": "mon", "from": "09:00", "to": "10:00"}]


class TestForgottenPreferences:
    @pytest.mark.parametrize(
        "patch",
        [
            lambda c: {"categories": {c: False}},
            lambda c: {"categories": {c: True}},
            lambda c: {"schedule": _SCHEDULE},
            lambda c: {"schedule": None},
        ],
        ids=["mute", "unmute", "schedule", "clear"],
    )
    async def test_refused_and_nothing_written(
        self, client: AsyncClient, stub_profile: None, patch: Any
    ) -> None:
        rid = await _forgotten()
        resp = await client.patch(_prefs(rid), json=patch(_a_category()))
        assert resp.status_code == 409, resp.text
        assert resp.json()["error"]["class"] == "recipient_deleted"
        assert await _mutes(rid) == 0
        assert await _windows(rid) is None

    async def test_a_live_one_is_written(
        self, client: AsyncClient, stub_profile: None
    ) -> None:
        """The pair: the same PATCH on a live recipient is stored."""
        rid = await _recipient()
        resp = await client.patch(
            _prefs(rid),
            json={"categories": {_a_category(): False}, "schedule": _SCHEDULE},
        )
        assert resp.status_code == 200, resp.text
        assert await _mutes(rid) == 1
        assert await _windows(rid) is not None

    async def test_an_unknown_one_is_still_404(
        self, client: AsyncClient, stub_profile: None
    ) -> None:
        resp = await client.patch(_prefs(uuid4()), json={"schedule": None})
        assert resp.status_code == 404


async def _pid(session: AsyncSession) -> int:
    pid = await session.scalar(text("SELECT pg_backend_pid()"))
    assert isinstance(pid, int)
    return pid


async def _until_waiting(holder: int, task: asyncio.Task[Any]) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _BOUND
    while True:
        async with get_session_factory()() as s:
            waiting = await s.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() "
                    "AND CAST(:holder AS integer) = ANY(pg_blocking_pids(pid))"
                ),
                {"holder": holder},
            )
        if waiting:
            return
        assert not task.done(), f"ended without waiting: {task.result()!r}"
        assert loop.time() < deadline, "never waited"
        await asyncio.sleep(_POLL)


async def _write(session: AsyncSession, rid: UUID, what: str) -> None:
    if what == "mute":
        await set_category_muted(session, rid, _a_category(), muted=True)
    else:
        await set_schedule(
            session, rid, windows=[{"day": 1, "from": 540, "to": 600}]
        )


@pytest.mark.parametrize("what", ["mute", "schedule"])
class TestTheForgettingRace:
    """Two real sessions on one recipient row. Whatever the order, a
    forgotten person ends with no schedule and no mute."""

    async def test_the_write_first(self, stub_profile: None, what: str) -> None:
        rid = await _recipient()
        factory = get_session_factory()
        async with factory() as writer, factory() as eraser:
            await _write(writer, rid, what)
            holder = await _pid(writer)

            async def forget() -> bool:
                done = await forget_recipient(eraser, recipient_id=rid, version=2)
                await eraser.commit()
                return done

            task = asyncio.create_task(forget())
            await _until_waiting(holder, task)
            await writer.commit()
            assert await asyncio.wait_for(task, _BOUND) is True
        assert await _mutes(rid) == 0
        assert await _windows(rid) is None

    async def test_forgetting_first(self, stub_profile: None, what: str) -> None:
        rid = await _recipient()
        factory = get_session_factory()
        async with factory() as eraser, factory() as writer:
            assert await forget_recipient(eraser, recipient_id=rid, version=2)
            holder = await _pid(eraser)

            async def write() -> Exception | None:
                try:
                    await _write(writer, rid, what)
                    await writer.commit()
                except RecipientDeletedError as exc:
                    await writer.rollback()
                    return exc
                return None

            task = asyncio.create_task(write())
            await _until_waiting(holder, task)
            await eraser.commit()
            assert isinstance(
                await asyncio.wait_for(task, _BOUND), RecipientDeletedError
            )
        assert await _mutes(rid) == 0
        assert await _windows(rid) is None


# ---------------------------------------------------------------------------
# 3 -- a client's status is never `internal`
# ---------------------------------------------------------------------------


def _request() -> Request:
    return Request({"type": "http", "method": "GET", "path": "/x",
                    "headers": [], "query_string": b""})


async def _answer(status: int) -> dict[str, Any]:
    handler = app.exception_handlers[StarletteHTTPException]
    response = await handler(_request(), StarletteHTTPException(status_code=status))
    assert response.status_code == status
    body: dict[str, Any] = json.loads(bytes(response.body))
    return body["error"]


class TestUnmapped4xx:
    async def test_a_body_that_is_not_utf8(self, client: AsyncClient) -> None:
        """The 4xx comms really answers outside its table: the framework's
        400 for a body it cannot decode."""
        rid = await _recipient()
        resp = await client.patch(
            _prefs(rid),
            content=b'{"categories":{"\xff":true}}',
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 400
        assert resp.json()["error"]["class"] == "bad_request"

    @pytest.mark.parametrize("status", [400, 413, 415, 418, 429])
    async def test_any_unmapped_client_status(self, status: int) -> None:
        assert (await _answer(status))["class"] == "bad_request"

    @pytest.mark.parametrize(
        ("status", "cls"),
        [(401, "unauthorized"), (404, "not_found"), (405, "method_not_allowed"),
         (500, "internal"), (502, "internal")],
    )
    async def test_the_rest_as_before(self, status: int, cls: str) -> None:
        assert (await _answer(status))["class"] == cls


# ---------------------------------------------------------------------------
# 4 -- /ready says why
# ---------------------------------------------------------------------------


def _engine_at(url: str) -> Any:
    engine = create_async_engine(url, pool_pre_ping=False)
    return lambda: engine


class TestReadySaysWhy:
    async def test_a_refused_password(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A database error: its summary (class, place, SQLSTATE), never
        the database's words."""
        bad = settings.database_url.replace("comms:comms@", "comms:not-it@")
        assert bad != settings.database_url
        monkeypatch.setattr(main_module, "get_engine", _engine_at(bad))
        with capture_logs() as logs:
            resp = await client.get("/ready")
        assert resp.status_code == 503
        (line,) = [e for e in logs if e["event"] == "ready_db_unreachable"]
        assert "SQLSTATE 28P01" in line["error"]
        # The database's own words, not the class name (InvalidPassword-
        # Error), are what must not be there.
        assert "authentication failed" not in line["error"]
        assert "not-it" not in str(line)

    async def test_nobody_listening(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        bad = settings.database_url.replace("localhost:5432", "127.0.0.1:1")
        assert bad != settings.database_url
        monkeypatch.setattr(main_module, "get_engine", _engine_at(bad))
        with capture_logs() as logs:
            resp = await client.get("/ready")
        assert resp.status_code == 503
        (line,) = [e for e in logs if e["event"] == "ready_db_unreachable"]
        # A refused connection is no database error: its class and place,
        # and not the OS's text, which quotes the address.
        assert " at " in line["error"]
        assert "Error" in line["error"]
        assert "127.0.0.1" not in str(line) and "Connect call" not in str(line)

    async def test_a_database_that_answers(self, client: AsyncClient) -> None:
        with capture_logs() as logs:
            resp = await client.get("/ready")
        assert resp.status_code == 200
        assert [e for e in logs if e["event"] == "ready_db_unreachable"] == []


# ---------------------------------------------------------------------------
# 5 -- claim's two refusals
# ---------------------------------------------------------------------------


class TestClaimRefusals:
    async def test_not_on_the_roster(self, client: AsyncClient) -> None:
        member, outsider, customer = [await _recipient() for _ in range(3)]
        async with get_session_factory()() as s:
            section = await create_section(s, key=f"api2-{uuid4().hex[:8]}")
            await declare_member(s, section, member)
            thread = await create_or_get_thread(
                s, **intake_fields(), client=customer,
                operator_kind=OperatorKind.SECTION, operator_value=section.id,
                kind=ThreadKind.TICKET, subject_type="practice",
                subject_id=uuid4().hex,
            )
            await s.commit()
        resp = await client.post(
            f"/api/v1/threads/{thread.id}/claim", json={"operator": str(outsider)}
        )
        assert resp.status_code == 403
        error = resp.json()["error"]
        assert error["class"] == "forbidden"
        assert error["message"] == "the operator does not serve the thread's section"

    async def test_a_user_thread(self, client: AsyncClient) -> None:
        master, customer = await _recipient(), await _recipient()
        async with get_session_factory()() as s:
            thread = await create_or_get_thread(
                s, **intake_fields(), client=customer,
                operator_kind=OperatorKind.USER, operator_value=master,
                kind=ThreadKind.DM,
            )
            await s.commit()
        resp = await client.post(
            f"/api/v1/threads/{thread.id}/claim", json={"operator": str(master)}
        )
        assert resp.status_code == 403
        error = resp.json()["error"]
        assert error["class"] == "forbidden"
        assert "only a section thread is claimed" in error["message"]
