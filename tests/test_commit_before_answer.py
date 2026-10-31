# =============================================================================
# TX-1 -- the commit before the answer; a transient database failure is
# retried, not lost
# =============================================================================
#
# What is held here:
#   1 the rule, app/core/database.py is_transient_db_error, on REAL
#     failures of Postgres and the driver (tests/helpers.py, the real_
#     builders -- real_serialization_failure and its siblings): a
#     serialization failure, a deadlock, this test's own backend
#     terminated idle and mid-statement, a refused connection, the pool's
#     timeout -- transient; a unique violation, a CHECK, a foreign key --
#     not. What cannot be raised here is held by its class, said why;
#   2 every route that writes (17, read off the app's own routes): the
#     commit fails -> the answer is the error (503 / 500), the write is
#     not there; the pair: the commit succeeds -> the answer as before,
#     the write there. And what a route answers is complete, the values
#     the database computes (revision) after the write;
#   3 the guard: no route takes the write session but through DbSession;
#   4 anything no handler takes -> 500 in the one form, no text;
#   5 the consumer: a transient failure once -> retried and applied;
#     every time -> "retries exhausted"; a misuse of a connection (a bug
#     the rule cannot tell from a lost connection) -> the same, bounded;
#   6 the contract.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  scope="function" off DbSession -> TestTheBoundary (the answer
#       went out before the commit)
#   M2  one route on a bare Depends(get_db_session) -> TestTheGuard
#   M3  the DBAPIError handler not calling the rule -> the 503 rows
#   M4  the consumer not calling the rule -> TestTheConsumer
#   M5  class 40 out of the rule -> TestTheRule (deadlock, 40001)
#   M6  class 08 and the driver's InterfaceError out -> TestTheRule
#       (the own backend terminated)
#   M7  the 503 row out of the contract -> TestTheContract
#   TX-1 FIX:
#   M8  the rule = the whole of OSError -> test_twins_of_the_wrong_form
#   M9  the `errno is None` check off the combined form -> the twins of
#       a plain OSError with an errno (EIO, EINVAL)
#   M10 the combined form out of the rule -> the combined rows (rule,
#       route, consumer)
# =============================================================================

import ast
import errno
import json
import re
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.api.errors import ErrorClass
from app.audience.models import CategoryMute, Recipient
from app.core.config import settings
from app.core.database import get_db_session, get_session_factory, is_transient_db_error
from app.engine.constants import DeliveryStatus, TargetType
from app.engine.models import NotificationDelivery
from app.engine.service import create_notification, resolve_notification
from app.main import app
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import (
    Message,
    MessageVariant,
    MessageVariantRequest,
    Section,
    Thread,
    ThreadReadState,
)
from app.messaging.operators import claim_thread
from app.messaging.threads import create_or_get_thread, post_message
from app.transport import consumer as consumer_module
from app.transport.consumer import StreamConsumer
from tests.helpers import (
    create_recipient,
    create_section,
    intake_fields,
    next_tx1_telegram_id,
    real_combined_connect_failure,
    real_connection_refused,
    real_constraint_violations,
    real_deadlock,
    real_misuse_of_a_connection,
    real_own_backend_terminated,
    real_pool_timeout,
    real_serialization_failure,
)
from tests.test_message_variants import _job
from tests.test_transport_consumer import (
    _no_pending,
    _run_until,
    _user_upserted_data,
    _xadd,
)

_REPO = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# 1 -- the rule, on real failures
# ---------------------------------------------------------------------------


class _OrigError(Exception):
    """A driver error carrying a SQLSTATE, as the asyncpg adapter's does
    (`pgcode`) -- for the codes no test can raise for real."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.pgcode = code


def _by_code(code: str) -> DBAPIError:
    return DBAPIError("SELECT 1", {}, _OrigError(code))


class TestTheRule:
    async def test_a_serialization_failure(self) -> None:
        failure = await real_serialization_failure()
        assert getattr(getattr(failure, "orig", None), "pgcode", None) == "40001"
        assert is_transient_db_error(failure)

    async def test_a_deadlock(self) -> None:
        failure = await real_deadlock()
        assert getattr(getattr(failure, "orig", None), "pgcode", None) == "40P01"
        assert is_transient_db_error(failure)

    @pytest.mark.parametrize("mid_statement", [False, True], ids=["idle", "mid"])
    async def test_the_own_backend_terminated(self, mid_statement: bool) -> None:
        """Idle: the next statement meets a closed connection -- the
        driver's InterfaceError, no SQLSTATE. Mid-statement: 08003."""
        failure = await real_own_backend_terminated(mid_statement=mid_statement)
        assert is_transient_db_error(failure)

    async def test_a_connection_refused(self) -> None:
        """Whatever form the environment gives a refused connection, it is
        transient. Until the TX-1 FIX this asserted ConnectionRefusedError
        -- the sandbox's form, `localhost` resolving to 127.0.0.1 alone. On
        the stand it resolves to ::1 and 127.0.0.1, both refuse, and
        asyncio combines them into a bare OSError without errno; the
        assertion failed there, and so did the rule. The combined form is
        built on purpose below (test_the_combined_failure_of_two_addresses)."""
        failure = await real_connection_refused()
        assert isinstance(failure, OSError)  # unwrapped by SQLAlchemy
        assert type(failure) in (ConnectionRefusedError, OSError)
        assert is_transient_db_error(failure)

    async def test_the_combined_failure_of_two_addresses(self) -> None:
        """asyncio's own combined OSError, through SQLAlchemy and asyncpg:
        exactly OSError, no errno -- the stand's form."""
        failure = await real_combined_connect_failure()
        assert type(failure) is OSError
        assert failure.errno is None
        assert is_transient_db_error(failure)

    @pytest.mark.parametrize("code", [
        errno.ECONNREFUSED, errno.ECONNRESET, errno.ECONNABORTED,
        errno.EHOSTUNREACH, errno.EHOSTDOWN, errno.ENETUNREACH, errno.ENETDOWN,
        errno.ETIMEDOUT, errno.EADDRNOTAVAIL,
    ], ids=errno.errorcode.get)
    def test_a_network_errno(self, code: int) -> None:
        """By class and errno: a network that cannot be broken here on
        purpose without taking it from every other test."""
        assert is_transient_db_error(OSError(code, "network"))

    def test_a_name_that_did_not_resolve(self) -> None:
        assert is_transient_db_error(socket.gaierror(socket.EAI_AGAIN, "dns"))

    @pytest.mark.parametrize("failure", [
        FileNotFoundError(errno.ENOENT, "no file"),
        PermissionError(errno.EACCES, "no right"),
        IsADirectoryError(errno.EISDIR, "a directory"),
        BlockingIOError(errno.EAGAIN, "would block"),
        OSError(errno.ENOENT, "mapped to FileNotFoundError"),
        OSError(errno.EACCES, "mapped to PermissionError"),
        OSError(errno.EIO, "plain OSError, an errno outside the set"),
        OSError(errno.EINVAL, "plain OSError, an errno outside the set"),
    ], ids=lambda e: f"{type(e).__name__}-{errno.errorcode.get(e.errno, e.errno)}")
    def test_twins_of_the_wrong_form(self, failure: OSError) -> None:
        """An OSError that is no network failure is not transient: a file,
        a right, a directory, a would-block, a plain OSError with an
        errno outside the network set. The rule takes the network's
        features, never the whole of OSError."""
        assert not is_transient_db_error(failure)

    async def test_the_pools_timeout(self) -> None:
        assert is_transient_db_error(await real_pool_timeout())

    async def test_a_misuse_of_a_connection_reads_as_transient(self) -> None:
        """A bug of ours the rule cannot tell from a lost connection
        without reading the message: transient, bounded by the retries
        (TestTheConsumer: "retries exhausted")."""
        assert is_transient_db_error(await real_misuse_of_a_connection())

    async def test_the_pair_constraints_are_not(self) -> None:
        found = await real_constraint_violations()
        assert set(found) == {"unique", "check", "foreign key"}
        for name, failure in found.items():
            assert not is_transient_db_error(failure), name

    @pytest.mark.parametrize("code", ["57P01", "57P02", "57P03", "53300"])
    def test_by_code_what_cannot_be_raised_here(self, code: str) -> None:
        """A restart (57P0x) is the server's own, not this test's; too
        many connections (53300) would take the database from every other
        test (rule 3.7.1). Held by the code the driver reports."""
        assert is_transient_db_error(_by_code(code))

    @pytest.mark.parametrize("code", ["22001", "42703", "57014", "55P03", "23505"])
    def test_by_code_not_transient(self, code: str) -> None:
        """Bad data, a statement, a cancelled one, a lock not available
        (unreachable in comms: no NOWAIT, no lock_timeout), a key."""
        assert not is_transient_db_error(_by_code(code))

    def test_operational_error_by_its_definition(self) -> None:
        assert is_transient_db_error(OperationalError("SELECT 1", {}, Exception()))

    def test_anything_else_is_not(self) -> None:
        assert not is_transient_db_error(RuntimeError("x"))
        assert not is_transient_db_error(DBAPIError("SELECT 1", {}, Exception()))


# ---------------------------------------------------------------------------
# 2 -- every route that writes: the commit before the answer
# ---------------------------------------------------------------------------


@dataclass
class _Case:
    method: str
    path: str
    body: dict[str, Any] | None
    written: Callable[[], Awaitable[bool]]


async def _recipient(email: str | None = None) -> UUID:
    rid = uuid4()
    async with get_session_factory()() as s:
        recipient = await create_recipient(
            s, recipient_id=rid, telegram_id=next_tx1_telegram_id(),
        )
        recipient.email = email
        await s.commit()
    return rid


async def _scalar(statement: Any) -> Any:
    async with get_session_factory()() as s:
        return await s.scalar(statement)


async def _section(key: str | None = None) -> UUID:
    async with get_session_factory()() as s:
        section = await create_section(s, key=key or f"tx1-{uuid4().hex[:8]}")
        await s.commit()
        return section.id


async def _ticket(*, holder: UUID | None = None) -> tuple[UUID, UUID]:
    customer = await _recipient()
    async with get_session_factory()() as s:
        thread = await create_or_get_thread(
            s, **intake_fields(), client=customer,
            operator_kind=OperatorKind.SECTION, operator_value=await _section(),
            kind=ThreadKind.TICKET, subject_type="practice",
            subject_id=uuid4().hex,
        )
        if holder is not None:
            assert await claim_thread(s, thread_id=thread.id, operator=holder)
        await s.commit()
        return thread.id, customer


async def _delivery(recipient: UUID) -> UUID:
    async with get_session_factory()() as s:
        job = await create_notification(
            s, **intake_fields(), type="unit_event_in_app", title="T", body="B",
            target_type=TargetType.USER, target_value=str(recipient),
        )
        (delivery,) = await resolve_notification(s, job)
        delivery.status = DeliveryStatus.SENT
        await s.commit()
        return delivery.id


async def _a_message(thread: UUID, sender: UUID) -> UUID:
    async with get_session_factory()() as s:
        message = await post_message(
            s, thread_id=thread, sender=sender, body="hi",
            idempotency_key=f"tx1:{uuid4()}", fingerprint="f" * 64,
        )
        await s.commit()
        return message.id


def _read_at(delivery: UUID) -> Callable[[], Awaitable[bool]]:
    async def written() -> bool:
        return await _scalar(
            select(NotificationDelivery.read_at)
            .where(NotificationDelivery.id == delivery)
        ) is not None
    return written


def _thread_column(
    thread: UUID, check: Callable[[Thread], bool],
) -> Callable[[], Awaitable[bool]]:
    async def written() -> bool:
        async with get_session_factory()() as s:
            row = await s.get(Thread, thread)
            return row is not None and check(row)
    return written


async def _case(name: str) -> _Case:
    """The setup of one write route, its request, and how to see its
    write in the database."""
    if name in ("read_all", "read_one"):
        person = await _recipient()
        delivery = await _delivery(person)
        path = (f"/api/v1/recipients/{person}/inbox/read-all" if name == "read_all"
                else f"/api/v1/recipients/{person}/inbox/{delivery}/read")
        return _Case("POST", path, None, _read_at(delivery))
    if name == "patch_preferences_form":
        person = await _recipient()

        async def muted() -> bool:
            return bool(await _scalar(select(func.count()).select_from(CategoryMute)
                                      .where(CategoryMute.recipient_id == person)))
        return _Case("PATCH", f"/api/v1/recipients/{person}/preferences",
                     {"categories": {"unit_updates": False}}, muted)
    if name == "upsert_recipient":
        person = uuid4()

        async def exists() -> bool:
            return await _scalar(select(Recipient.id).where(Recipient.id == person)) \
                is not None
        return _Case("PUT", f"/api/v1/recipients/{person}", {
            "version": 1, "telegram_id": next_tx1_telegram_id(), "email": None,
            "locale": "en", "timezone": "Europe/Berlin", "active": True,
        }, exists)
    if name == "delete_recipient":
        person = await _recipient()

        async def forgotten() -> bool:
            return await _scalar(select(Recipient.deleted_at)
                                 .where(Recipient.id == person)) is not None
        return _Case("DELETE", f"/api/v1/recipients/{person}", {"version": 2},
                     forgotten)
    if name == "create_section":
        key = f"tx1-{uuid4().hex[:8]}"

        async def found() -> bool:
            return await _scalar(select(Section.id).where(Section.key == key)) \
                is not None
        return _Case("POST", "/api/v1/sections", {"key": key, "label": "L"}, found)
    if name == "create_thread":
        customer, master = await _recipient(), await _recipient()

        async def created() -> bool:
            return await _scalar(select(Thread.id).where(Thread.client == customer)) \
                is not None
        return _Case("POST", "/api/v1/threads", {
            "client": str(customer), "operator_kind": "user",
            "operator_value": str(master), "kind": "dm",
        }, created)
    if name == "delete_thread_route":
        thread, _ = await _ticket()
        return _Case("DELETE", f"/api/v1/threads/{thread}", None,
                     _gone(thread))
    if name == "post_thread_message":
        thread, customer = await _ticket()

        async def posted() -> bool:
            return bool(await _scalar(select(func.count()).select_from(Message)
                                      .where(Message.thread_id == thread)))
        return _Case("POST", f"/api/v1/threads/{thread}/messages",
                     {"sender": str(customer), "body": "x"}, posted)
    if name in ("post_variant_request", "post_variant"):
        thread, customer = await _ticket()
        message = await _a_message(thread, customer)
        job = await _job()
        model = MessageVariantRequest if name == "post_variant_request" else (
            MessageVariant
        )

        async def kept() -> bool:
            return bool(await _scalar(select(func.count()).select_from(model)
                                      .where(model.message_id == message)))
        body: dict[str, Any] = {"locale": "en", "job_key": job}
        suffix = "variant-requests"
        if name == "post_variant":
            body |= {"source_locale": "de", "body": "hi"}
            suffix = "variants"
        return _Case("POST", f"/api/v1/threads/{thread}/messages/{message}/{suffix}",
                     body, kept)
    if name == "mark_thread_read":
        thread, customer = await _ticket()

        async def pointer() -> bool:
            return bool(await _scalar(select(func.count()).select_from(ThreadReadState)
                                      .where(ThreadReadState.thread_id == thread)))
        return _Case("POST", f"/api/v1/threads/{thread}/read",
                     {"participant": str(customer)}, pointer)
    operator = await _recipient()
    if name == "claim":
        thread, _ = await _ticket()
        return _Case("POST", f"/api/v1/threads/{thread}/claim",
                     {"operator": str(operator)},
                     _thread_column(thread, lambda t: t.assignee == operator))
    if name == "assign":
        thread, _ = await _ticket()
        return _Case("POST", f"/api/v1/threads/{thread}/assign",
                     {"operator": str(operator), "assignee": str(operator)},
                     _thread_column(thread, lambda t: t.assignee == operator))
    if name == "release":
        thread, _ = await _ticket(holder=operator)
        return _Case("POST", f"/api/v1/threads/{thread}/release",
                     {"operator": str(operator)},
                     _thread_column(thread, lambda t: t.assignee is None))
    if name == "change_status":
        thread, _ = await _ticket()
        return _Case("POST", f"/api/v1/threads/{thread}/status",
                     {"operator": str(operator), "status": "resolved"},
                     _thread_column(thread, lambda t: t.status == "resolved"))
    assert name == "retag", name
    thread, _ = await _ticket()
    moved = await _section()
    return _Case("POST", f"/api/v1/threads/{thread}/retag",
                 {"operator": str(operator), "section": str(moved)},
                 _thread_column(thread, lambda t: t.operator_value == moved))


def _gone(thread: UUID) -> Callable[[], Awaitable[bool]]:
    async def written() -> bool:
        return await _scalar(select(Thread.id).where(Thread.id == thread)) is None
    return written


def _write_routes() -> dict[str, tuple[str, str]]:
    """Every route whose dependencies take get_db_session, by its
    endpoint's name -- read off the app, not listed by hand."""
    found: dict[str, tuple[str, str]] = {}
    for route, calls in _routes_and_calls(app):
        if any(dep.call is get_db_session for dep in calls):
            found[route.endpoint.__name__] = (
                ",".join(sorted(route.methods)), route.path,
            )
    return found


def _routes_and_calls(application: FastAPI) -> list[tuple[Any, list[Any]]]:
    def walk(dependant: Any) -> list[Any]:
        out = []
        for dep in dependant.dependencies:
            out.append(dep)
            out.extend(walk(dep))
        return out

    def flat(routes: Any) -> list[Any]:
        found = []
        for route in routes:
            if hasattr(route, "original_router"):
                found.extend(flat(route.original_router.routes))
            elif hasattr(route, "dependant"):
                found.append(route)
        return found

    return [(route, walk(route.dependant)) for route in flat(application.routes)]


_WRITE_ROUTES = sorted([
    "read_all", "read_one", "patch_preferences_form", "upsert_recipient",
    "delete_recipient", "create_section", "create_thread",
    "delete_thread_route", "post_thread_message", "post_variant_request",
    "post_variant", "mark_thread_read", "claim", "assign", "release",
    "change_status", "retag",
])


async def _send(client: AsyncClient, case: _Case) -> Any:
    return await client.request(
        case.method, case.path,
        content=json.dumps(case.body) if case.body is not None else None,
        headers={"Content-Type": "application/json"} if case.body is not None
        else {},
    )


def _failing_commit(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException,
) -> None:
    async def commit(self: AsyncSession) -> None:
        raise failure

    monkeypatch.setattr(AsyncSession, "commit", commit)


class TestTheBoundary:
    def test_the_list_is_the_apps(self) -> None:
        """Both ways: the 17 below are exactly the app's write routes."""
        assert sorted(_write_routes()) == _WRITE_ROUTES

    @pytest.mark.parametrize("name", _WRITE_ROUTES)
    async def test_the_commit_succeeds(self, client: AsyncClient, name: str) -> None:
        """The pair: the answer as before, the write there."""
        case = await _case(name)
        resp = await _send(client, case)
        assert resp.status_code < 300, resp.text
        assert await case.written()

    @pytest.mark.parametrize("name", _WRITE_ROUTES)
    async def test_a_transient_failure_of_the_commit(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch, name: str,
    ) -> None:
        case = await _case(name)
        failure = await real_serialization_failure()
        _failing_commit(monkeypatch, failure)
        resp = await _send(client, case)
        assert resp.status_code == 503, resp.text
        assert resp.json()["error"]["class"] == "unavailable"
        assert resp.headers["Retry-After"] == "1"
        monkeypatch.undo()
        assert not await case.written()

    @pytest.mark.parametrize("name", _WRITE_ROUTES)
    async def test_another_failure_of_the_commit(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch, name: str,
    ) -> None:
        case = await _case(name)
        failure = (await real_constraint_violations())["check"]
        _failing_commit(monkeypatch, failure)
        resp = await _send(client, case)
        assert resp.status_code == 500, resp.text
        assert resp.json()["error"]["class"] == "internal"
        monkeypatch.undo()
        assert not await case.written()


class TestTheAnswerIsComplete:
    """The body is serialized inside the endpoint's call, before the
    function stack -- the commit -- closes (fastapi/routing.py,
    get_request_handler): complete, nothing lazy left for after the
    session is gone, and the values the database computes are those of
    the write."""

    @pytest.mark.parametrize(
        "name", ["create_thread", "claim", "assign", "release", "change_status",
                 "retag"],
    )
    async def test_a_thread_with_its_revision(
        self, client: AsyncClient, name: str,
    ) -> None:
        case = await _case(name)
        resp = await _send(client, case)
        assert resp.status_code == 200, resp.text
        answer = resp.json()
        answer = answer.get("thread", answer)
        async with get_session_factory()() as s:
            row = await s.get(Thread, UUID(answer["id"]))
            assert row is not None
            assert answer["revision"] == row.revision
            assert answer["status"] == row.status
            assert answer["assignee"] == (str(row.assignee) if row.assignee else None)

    async def test_a_message(self, client: AsyncClient) -> None:
        case = await _case("post_thread_message")
        resp = await _send(client, case)
        answer = resp.json()
        async with get_session_factory()() as s:
            row = await s.get(Message, UUID(answer["id"]))
            assert row is not None
            assert (answer["body"], answer["sender"]) == (row.body, str(row.sender))

    async def test_a_recipient_and_a_section(self, client: AsyncClient) -> None:
        recipient = await _send(client, await _case("upsert_recipient"))
        section = await _send(client, await _case("create_section"))
        assert recipient.json()["version"] == 1
        async with get_session_factory()() as s:
            row = await s.get(Section, UUID(section.json()["id"]))
            assert row is not None
            assert section.json()["key"] == row.key


# ---------------------------------------------------------------------------
# 3 -- the guard
# ---------------------------------------------------------------------------

_SESSION_FORMS = ("Depends", "Security")


def _bare_session_dependencies(source: str) -> list[int]:
    """Lines where get_db_session is taken as a dependency -- in every
    form: Depends(get_db_session), Depends(dependency=get_db_session),
    Security(get_db_session), with or without scope."""
    lines = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        name = node.func.id if isinstance(node.func, ast.Name) else (
            node.func.attr if isinstance(node.func, ast.Attribute) else ""
        )
        if name not in _SESSION_FORMS:
            continue
        args = list(node.args) + [k.value for k in node.keywords
                                  if k.arg == "dependency"]
        if any(isinstance(a, ast.Name) and a.id == "get_db_session" for a in args):
            lines.append(node.lineno)
    return lines


class TestTheGuard:
    def test_every_write_route_commits_before_answering(self) -> None:
        """The graph of the live app: every route that takes
        get_db_session takes it with scope "function"."""
        for route, calls in _routes_and_calls(app):
            for dep in calls:
                if dep.call is get_db_session:
                    assert dep.scope == "function", route.path

    def test_the_graph_check_sees_a_bare_dependency(self) -> None:
        """The pair: a route on a bare Depends(get_db_session) is seen."""
        from fastapi import Depends

        stray = FastAPI()

        @stray.post("/stray")
        async def write(session: AsyncSession = Depends(get_db_session)) -> None:
            return None

        scopes = [dep.scope for _, calls in _routes_and_calls(stray)
                  for dep in calls if dep.call is get_db_session]
        assert scopes and scopes[0] != "function"

    def test_one_place_in_the_source(self) -> None:
        """AST over app/: get_db_session is taken as a dependency in
        exactly one place -- DbSession, app/api/deps.py."""
        places = {
            path.relative_to(_REPO).as_posix(): lines
            for path in sorted((_REPO / "app").rglob("*.py"))
            if (lines := _bare_session_dependencies(path.read_text("utf-8")))
        }
        assert list(places) == ["app/api/deps.py"]
        assert len(places["app/api/deps.py"]) == 1

    def test_the_scanner_sees_every_form(self) -> None:
        source = (
            "a = Depends(get_db_session)\n"
            "b = Depends(dependency=get_db_session)\n"
            "c = Security(get_db_session)\n"
            "d = fastapi.Depends(get_db_session, scope='function')\n"
            "e = Depends(get_db_reader)\n"
        )
        assert _bare_session_dependencies(source) == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# 4 -- anything no handler takes: 500 in the one form
# ---------------------------------------------------------------------------

_S = "SENTINEL-TX1"


class TestUnhandled:
    async def test_a_runtime_error(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def broken(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError(f"broken {_S}")

        monkeypatch.setattr("app.api.messaging.get_or_create_section", broken)
        with capture_logs() as logs:
            resp = await client.post(
                "/api/v1/sections", json={"key": "k-tx1", "label": "L"},
            )
        assert resp.status_code == 500
        assert resp.json()["error"]["class"] == "internal"
        assert _S not in resp.text
        (line,) = [e for e in logs if e["event"] == "unhandled_error_at_api_edge"]
        assert line["error"].startswith("builtins.RuntimeError at app/")
        assert _S not in json.dumps(logs, default=str)

    @pytest.mark.parametrize("make", [real_connection_refused,
                                      real_combined_connect_failure],
                             ids=["refused", "combined"])
    async def test_a_refused_connection_is_unavailable(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
        make: Callable[[], Awaitable[Exception]],
    ) -> None:
        """Both forms of a refused connection -- the environment's, and
        asyncio's combined one of the stand -- are a 503."""
        refused = await make()

        async def broken(*_args: Any, **_kwargs: Any) -> Any:
            raise refused

        monkeypatch.setattr("app.api.messaging.get_or_create_section", broken)
        resp = await client.post("/api/v1/sections", json={"key": "k", "label": "L"})
        assert resp.status_code == 503
        assert resp.json()["error"]["class"] == "unavailable"
        assert resp.headers["Retry-After"] == "1"


# ---------------------------------------------------------------------------
# 5 -- the consumer, on real failures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_redis() -> fakeaioredis.FakeRedis:
    return fakeaioredis.FakeRedis()


@pytest.fixture
def a_stream(monkeypatch: pytest.MonkeyPatch) -> str:
    """A stream of its own, the loop fast, the backoff short."""
    name = f"comms:test:{uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", name)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    monkeypatch.setattr(consumer_module, "_BACKOFF_DELAYS", (0.05, 0.1, 0.2, 0.4))
    return name


async def _consume_until(
    redis_: fakeaioredis.FakeRedis, stream_: str,
    condition: Callable[[], Awaitable[bool]],
) -> list[dict[str, Any]]:
    with capture_logs() as logs:
        await _run_until(StreamConsumer(redis_), condition)
    return logs


class TestTheConsumer:
    async def test_transient_once_then_applied(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        failure = await real_deadlock()
        real = consumer_module.handle_event
        calls = 0

        async def once(*args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise failure
            return await real(*args, **kwargs)

        monkeypatch.setattr(consumer_module, "handle_event", once)
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def applied() -> bool:
            return await _scalar(select(Recipient.id).where(Recipient.id == person)) \
                is not None and await _no_pending(fake_redis, a_stream)

        logs = await _consume_until(fake_redis, a_stream, applied)
        assert await fake_redis.xlen(settings.dlq_stream) == 0
        assert calls == 2
        assert [e["event"] for e in logs].count("event_retry_scheduled") == 1

    async def test_a_combined_connect_failure_once_then_applied(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The stand's form of a refused connection: retried, not
        dead-lettered (TX-1 FIX)."""
        failure = await real_combined_connect_failure()
        real = consumer_module.handle_event
        calls = 0

        async def once(*args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise failure
            return await real(*args, **kwargs)

        monkeypatch.setattr(consumer_module, "handle_event", once)
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(person))

        async def applied() -> bool:
            found = await _scalar(select(Recipient.id).where(Recipient.id == person))
            return found is not None and await _no_pending(fake_redis, a_stream)

        await _consume_until(fake_redis, a_stream, applied)
        assert await fake_redis.xlen(settings.dlq_stream) == 0
        assert calls == 2

    @pytest.mark.parametrize("make", [real_serialization_failure,
                                      real_misuse_of_a_connection],
                             ids=["40001", "misuse"])
    async def test_every_time_then_exhausted(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
        make: Callable[[], Awaitable[Exception]],
    ) -> None:
        """A transient failure that never passes -- and a misuse of a
        connection, the bug the rule reads as transient: a bounded series
        of retries, then the dead letter, its class and place."""
        failure = await make()

        async def always(*_args: Any, **_kwargs: Any) -> Any:
            raise failure

        monkeypatch.setattr(consumer_module, "handle_event", always)
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(uuid4()))

        async def dead() -> bool:
            return await fake_redis.xlen(settings.dlq_stream) == 1

        await _consume_until(fake_redis, a_stream, dead)
        (entry,) = await fake_redis.xrange(settings.dlq_stream)
        fields = {k.decode(): v.decode() for k, v in entry[1].items()}
        assert fields["_dlq_error"].startswith(
            f"retries exhausted: {type(failure).__module__}."
            f"{type(failure).__qualname__} at app/"
        )

    async def test_not_transient_at_once(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        failure = (await real_constraint_violations())["unique"]

        async def always(*_args: Any, **_kwargs: Any) -> Any:
            raise failure

        monkeypatch.setattr(consumer_module, "handle_event", always)
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(uuid4()))

        async def dead() -> bool:
            return await fake_redis.xlen(settings.dlq_stream) == 1

        logs = await _consume_until(fake_redis, a_stream, dead)
        (entry,) = await fake_redis.xrange(settings.dlq_stream)
        fields = {k.decode(): v.decode() for k, v in entry[1].items()}
        assert fields["_dlq_error"].startswith("unexpected: ")
        assert fields["_dlq_attempts"] == "1"
        assert "event_retry_scheduled" not in [e["event"] for e in logs]


# ---------------------------------------------------------------------------
# 6 -- the contract
# ---------------------------------------------------------------------------

_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


class TestTheContract:
    def test_the_classes_are_the_codes(self) -> None:
        """Both ways: the classes of section 5's table are ErrorClass."""
        table = _DOC.split("| status | class | when |", 1)[1].split("\n\n", 1)[0]
        rows = re.findall(r"^\| (\d{3}) \| `([a-z_]+)` \|", table, re.M)
        assert {cls for _, cls in rows} == {c.value for c in ErrorClass}
        assert ("503", "unavailable") in rows

    def test_the_commit_and_the_unknown_outcome(self) -> None:
        assert "**A write is answered after its commit.**" in _DOC
        part = _DOC.split("**503 `unavailable`: the outcome is unknown", 1)[1]
        part = part.split("\n\n", 1)[0]
        for said in ("repeat the same", "`Idempotency-Key`",
                     "safe to repeat by construction",
                     "misuse of a connection"):
            assert said in part.replace("\n", " "), said
        assert "`Retry-After: 1`" in _DOC

