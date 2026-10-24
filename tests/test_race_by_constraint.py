# =============================================================================
# G5 -- the insert race is recognized by its SQLSTATE and the violated
# constraint's NAME as the driver reports it, never by the error's text.
# =============================================================================
#
# Before G5 four places looked for their index name in str(exc.orig): the
# intake (app/engine/service.py accept_notification), POST /threads and
# POST /threads/{id}/messages (app/api/messaging.py), and the thread dedup
# (app/messaging/threads.py is_dedup_violation, which retag also calls).
# The text carries the DETAIL, and the DETAIL carries VALUES -- a key, a
# subject id, the whole failing row of a CHECK or a NOT NULL. Now all of
# them call app/core/database.py is_unique_violation_of.
#
# Three parts:
#   TestTheRule       the rule on REAL errors of the real tables, every
#                     form; each input whose text spells one of our names
#                     asserts that it does (the instrument), so the old
#                     text rule would have read it as our race;
#   TestEachWritersOwnRace  every writer's own race on two real sessions:
#                     the loser waits on the winner (pg_blocking_pids, in
#                     this database), then gets the winner -- as before.
#                     These are also the sentinel of the rule's ADAPTER
#                     dependency: the name is on the asyncpg exception the
#                     SQLAlchemy adapter chains as __cause__, a behaviour
#                     and not an API; an upgrade that stops chaining it
#                     turns every one of these red;
#   TestTheWatchman   no IntegrityError in app/ is read as text, in every
#                     form, paired with "every handler calls the rule".
#
# MUTATIONS:
#   M1 the rule checks the SQLSTATE only     -> TestTheRule (pkey, the
#      other unique index of threads)
#   M2 the rule reads the name from the text -> TestTheRule (CHECK, NOT
#      NULL, the other unique index spelling our name)
#   M3 one place back to `in str(exc.orig)`  -> TestTheWatchman
#   M4 the rule reads exc.orig.constraint_name (not the cause) ->
#      TestEachWritersOwnRace, every writer
#   M10 the watchman without one form        -> its planted twin
#   Unkillable, not built: the rule without its SQLSTATE check -- no
#   CHECK or FK of the schema shares a name with one of these indexes.
# =============================================================================

import ast
import asyncio
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request
from structlog.testing import capture_logs

from app.api.messaging import (
    _MESSAGES_KEY_INDEX,
    _THREADS_KEY_INDEX,
    MessageIn,
    ThreadCreateIn,
    create_thread,
    post_thread_message,
)
from app.core.database import (
    get_session_factory,
    is_unique_violation,
    is_unique_violation_of,
)
from app.core.exceptions import ValidationError
from app.engine.models import Notification
from app.engine.service import (
    IDEMPOTENCY_INDEX_NAME,
    Intake,
    accept_notification,
)
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Message, Thread
from app.messaging.operators import SubjectRef, retag_thread
from app.messaging.threads import (
    _DEDUP_INDEX_NAMES,
    create_or_get_thread,
    create_or_get_thread_detailed,
)
from tests.helpers import (
    TEST_FINGERPRINT,
    create_recipient,
    create_section,
    intake_fields,
    notification_row_fields,
)

_BOUND = 10.0
_APP = Path(__file__).resolve().parent.parent / "app"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


async def _violation(session: AsyncSession, row: Any) -> IntegrityError:
    """The real IntegrityError the database raises for this row."""
    with pytest.raises(IntegrityError) as caught:
        async with session.begin_nested():
            session.add(row)
            await session.flush()
    return caught.value


def _job(**overrides: Any) -> Notification:
    fields: dict[str, Any] = {
        **notification_row_fields(),
        "type": "unit_event_in_app", "title": "T", "body": "B",
        "target_type": "all", "target_value": "*",
    }
    fields.update(overrides)
    return Notification(**fields)


def _thread(client: UUID, operator: UUID, **overrides: Any) -> Thread:
    fields: dict[str, Any] = {
        **intake_fields(),
        "client": client, "operator_kind": OperatorKind.USER,
        "operator_value": operator, "kind": ThreadKind.TICKET,
    }
    fields.update(overrides)
    return Thread(**fields)


async def _pair(session: AsyncSession) -> tuple[UUID, UUID]:
    client = await create_recipient(session)
    operator = await create_recipient(session)
    return client.id, operator.id


async def _pid(session: AsyncSession) -> int:
    pid: int = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
    return pid


async def _until_blocked(loser: list[int], winner: int) -> None:
    """Until THE losing session waits on THE winning one -- in this
    database, by pg_blocking_pids of the loser, never by how many
    sessions wait anywhere."""
    for _ in range(int(_BOUND / 0.02)):
        if loser:
            async with get_session_factory()() as session:
                blocked = (await session.execute(text(
                    "SELECT :winner = ANY(pg_blocking_pids(pid)) "
                    "FROM pg_stat_activity "
                    "WHERE pid = :loser AND datname = current_database()"
                ), {"winner": winner, "loser": loser[0]})).scalar_one_or_none()
            if blocked:
                return
        await asyncio.sleep(0.02)
    raise AssertionError("the losing session never waited on the winner")


def _request(path: str, payload: BaseModel) -> Request:
    """A POST as the route receives it: the fingerprint reads its path
    and body (app/api/idempotency.py request_fingerprint)."""
    body = payload.model_dump_json().encode()

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    return Request({
        "type": "http", "method": "POST", "scheme": "http",
        "server": ("test", 80), "path": path, "query_string": b"",
        "headers": [],
    }, receive)


# ---------------------------------------------------------------------------
# The rule, on the real errors of the real tables
# ---------------------------------------------------------------------------


class TestTheRule:
    async def test_own_index_of_the_intake(
        self, db_session: AsyncSession,
    ) -> None:
        holder = _job()
        db_session.add(holder)
        await db_session.flush()
        exc = await _violation(
            db_session, _job(idempotency_key=holder.idempotency_key),
        )
        assert is_unique_violation_of(exc, IDEMPOTENCY_INDEX_NAME)

    async def test_the_primary_key_is_another_unique_index(
        self, db_session: AsyncSession,
    ) -> None:
        holder = _job()
        db_session.add(holder)
        await db_session.flush()
        # Out of the identity map: the row stays, the session forgets it,
        # so the second instance with its id is the database's to refuse.
        db_session.expunge(holder)
        exc = await _violation(db_session, _job(id=holder.id))
        assert is_unique_violation(exc)
        assert not is_unique_violation_of(exc, IDEMPOTENCY_INDEX_NAME)

    async def test_a_check_whose_failing_row_spells_our_name(
        self, db_session: AsyncSession,
    ) -> None:
        """The DETAIL of a CHECK is the whole failing row, the key
        included: a key that spells our index name puts it in the text."""
        exc = await _violation(db_session, _job(
            idempotency_key=f"{IDEMPOTENCY_INDEX_NAME}:{uuid4().hex}",
            lane="no-such-lane",
        ))
        assert getattr(exc.orig, "pgcode", None) == "23514"
        assert IDEMPOTENCY_INDEX_NAME in str(exc.orig)
        assert not is_unique_violation_of(exc, IDEMPOTENCY_INDEX_NAME)

    async def test_a_not_null_whose_failing_row_spells_our_name(
        self, db_session: AsyncSession,
    ) -> None:
        exc = await _violation(db_session, _job(
            idempotency_key=f"{IDEMPOTENCY_INDEX_NAME}:{uuid4().hex}",
            fingerprint=None,
        ))
        assert getattr(exc.orig, "pgcode", None) == "23502"
        assert IDEMPOTENCY_INDEX_NAME in str(exc.orig)
        assert not is_unique_violation_of(exc, IDEMPOTENCY_INDEX_NAME)

    async def test_a_dedup_index_whose_value_spells_the_key_index(
        self, db_session: AsyncSession,
    ) -> None:
        """Another unique index of the same table: the subject id is the
        producer's text and lands in the dedup index's DETAIL."""
        client, operator = await _pair(db_session)
        subject = {"subject_type": "s", "subject_id": _THREADS_KEY_INDEX}
        db_session.add(_thread(client, operator, **subject))
        await db_session.flush()
        exc = await _violation(db_session, _thread(client, operator, **subject))
        assert _THREADS_KEY_INDEX in str(exc.orig)
        assert not is_unique_violation_of(exc, _THREADS_KEY_INDEX)
        assert is_unique_violation_of(exc, *_DEDUP_INDEX_NAMES)

    async def test_the_key_index_whose_value_spells_a_dedup_index(
        self, db_session: AsyncSession,
    ) -> None:
        client, operator = await _pair(db_session)
        key = f"{_DEDUP_INDEX_NAMES[0]}:{uuid4().hex}"
        db_session.add(_thread(client, operator, idempotency_key=key))
        await db_session.flush()
        exc = await _violation(
            db_session, _thread(client, operator, idempotency_key=key),
        )
        assert _DEDUP_INDEX_NAMES[0] in str(exc.orig)
        assert not is_unique_violation_of(exc, *_DEDUP_INDEX_NAMES)
        assert is_unique_violation_of(exc, _THREADS_KEY_INDEX)

    async def test_own_index_of_a_message_and_its_other_violations(
        self, db_session: AsyncSession,
    ) -> None:
        client, operator = await _pair(db_session)
        thread = _thread(client, operator)
        db_session.add(thread)
        await db_session.flush()

        def message(**overrides: Any) -> Message:
            fields: dict[str, Any] = {
                **intake_fields(), "thread_id": thread.id,
                "sender": client, "body": "hi",
            }
            fields.update(overrides)
            return Message(**fields)

        holder = message()
        db_session.add(holder)
        await db_session.flush()
        own = await _violation(
            db_session, message(idempotency_key=holder.idempotency_key),
        )
        assert is_unique_violation_of(own, _MESSAGES_KEY_INDEX)

        spelled = f"{_MESSAGES_KEY_INDEX}:{uuid4().hex}"
        not_null = await _violation(
            db_session, message(idempotency_key=spelled, body=None),
        )
        assert _MESSAGES_KEY_INDEX in str(not_null.orig)
        assert not is_unique_violation_of(not_null, _MESSAGES_KEY_INDEX)

        foreign = await _violation(db_session, message(thread_id=uuid4()))
        assert getattr(foreign.orig, "pgcode", None) == "23503"
        assert not is_unique_violation_of(foreign, _MESSAGES_KEY_INDEX)

    def test_an_error_without_the_drivers_name_is_not_a_race(self) -> None:
        """No cause, or a cause without the field: the name is unknown,
        and an unknown name is not our race."""

        class _OrigError(Exception):
            pgcode = "23505"

        bare = _OrigError(f"violates unique constraint \"{IDEMPOTENCY_INDEX_NAME}\"")
        assert not is_unique_violation_of(
            IntegrityError("INSERT", {}, bare), IDEMPOTENCY_INDEX_NAME,
        )
        caused = _OrigError(IDEMPOTENCY_INDEX_NAME)
        caused.__cause__ = Exception(IDEMPOTENCY_INDEX_NAME)
        assert not is_unique_violation_of(
            IntegrityError("INSERT", {}, caused), IDEMPOTENCY_INDEX_NAME,
        )

    def test_no_name_is_a_call_that_can_never_match(self) -> None:
        with pytest.raises(ValueError, match="at least one name"):
            is_unique_violation_of(IntegrityError("INSERT", {}, Exception()))

    async def test_every_name_is_a_unique_index_of_its_table(
        self, db_session: AsyncSession,
    ) -> None:
        """The names the code passes are the schema's -- read from
        pg_indexes, not from the models. A renamed index would turn the
        writer's race into a raised error."""
        names = {
            ("notifications", IDEMPOTENCY_INDEX_NAME),
            ("threads", _THREADS_KEY_INDEX),
            ("messages", _MESSAGES_KEY_INDEX),
            *(("threads", name) for name in _DEDUP_INDEX_NAMES),
        }
        assert len(names) == 5
        rows = set((await db_session.execute(text(
            "SELECT tablename, indexname FROM pg_indexes "
            "WHERE schemaname = current_schema() "
            "AND indexdef LIKE 'CREATE UNIQUE INDEX %'"
        ))).tuples().all())
        assert names <= rows


# ---------------------------------------------------------------------------
# Every writer's own race, on two real sessions
# ---------------------------------------------------------------------------


async def _race(first_call: Any, second_call: Any) -> tuple[Any, Any]:
    """`first_call(session)` runs and stays uncommitted; the second runs
    in its own session, must wait on the first, then finishes after the
    first commits. Returns both answers."""
    factory = get_session_factory()
    async with factory() as first:
        won = await first_call(first)
        winner = await _pid(first)
        loser: list[int] = []

        async def second() -> Any:
            async with factory() as session:
                loser.append(await _pid(session))
                answer = await second_call(session)
                await session.commit()
                return answer

        task = asyncio.create_task(second())
        await _until_blocked(loser, winner)
        await first.commit()
        lost = await asyncio.wait_for(task, _BOUND)
    return won, lost


async def _committed_pair() -> tuple[UUID, UUID]:
    async with get_session_factory()() as session:
        pair = await _pair(session)
        await session.commit()
    return pair


async def _count(model: Any, key: str) -> int:
    async with get_session_factory()() as session:
        count: int = (await session.execute(
            select(func.count()).select_from(model)
            .where(model.idempotency_key == key)
        )).scalar_one()
    return count


class TestEachWritersOwnRace:
    """Each writer's own race, recognized by the driver's constraint name
    -- and, being on a real error, the sentinel of where that name comes
    from (M4; app/core/database.py is_unique_violation_of)."""

    async def test_the_intake(self) -> None:
        key = f"g5-intake-{uuid4().hex}"

        async def accept(session: AsyncSession) -> Any:
            return await accept_notification(
                session, idempotency_key=key, fingerprint=TEST_FINGERPRINT,
                type="unit_event_in_app", title="T", body="B",
                target_type="all", target_value="*",
            )

        won, lost = await _race(accept, accept)
        assert won.outcome is Intake.ACCEPTED
        assert lost.outcome is Intake.DUPLICATE
        assert lost.notification.id == won.notification.id
        assert await _count(Notification, key) == 1

    async def test_post_threads(self) -> None:
        client, operator = await _committed_pair()
        key = f"g5-thread-{uuid4().hex}"
        payload = ThreadCreateIn(
            client=client, operator_kind=OperatorKind.USER,
            operator_value=operator, kind=ThreadKind.TICKET,
        )

        async def post(session: AsyncSession) -> dict[str, Any]:
            return await create_thread(
                request=_request("/api/v1/threads", payload),
                payload=payload, idempotency_key=key, session=session,
            )

        won, lost = await _race(post, post)
        assert lost["id"] == won["id"]
        assert lost["created"] is True
        assert await _count(Thread, key) == 1

    async def test_post_messages(self) -> None:
        client, operator = await _committed_pair()
        async with get_session_factory()() as session:
            thread = await create_or_get_thread(
                session, **intake_fields(), client=client,
                operator_kind=OperatorKind.USER, operator_value=operator,
                kind=ThreadKind.TICKET,
            )
            await session.commit()
        key = f"g5-message-{uuid4().hex}"
        payload = MessageIn(sender=client, body="hi")
        path = f"/api/v1/threads/{thread.id}/messages"

        async def post(session: AsyncSession) -> dict[str, Any]:
            return await post_thread_message(
                request=_request(path, payload), thread_id=thread.id,
                payload=payload, idempotency_key=key, session=session,
            )

        won, lost = await _race(post, post)
        assert lost["id"] == won["id"]
        assert await _count(Message, key) == 1

    async def test_the_thread_dedup(self) -> None:
        client, operator = await _committed_pair()

        async def create(session: AsyncSession) -> tuple[Thread, bool]:
            return await create_or_get_thread_detailed(
                session, **intake_fields(), client=client,
                operator_kind=OperatorKind.USER, operator_value=operator,
                kind=ThreadKind.DM,
            )

        with capture_logs() as logs:
            (won, created), (lost, lost_created) = await _race(create, create)
        assert lost.id == won.id
        assert (created, lost_created) == (True, False)
        events = [e["event"] for e in logs]
        assert events.count("thread_create_lost_race") == 1

    async def test_the_retag_onto_a_taken_subject(
        self, db_session: AsyncSession,
    ) -> None:
        """retag is an UPDATE meeting an existing thread, not a race: its
        own violation is the dedup index, answered as a refusal."""
        client = (await create_recipient(db_session)).id
        sections = [
            await create_section(db_session, key=f"g5-{n}-{uuid4().hex[:8]}")
            for n in (1, 2)
        ]
        taken, moving = [
            await create_or_get_thread(
                db_session, **intake_fields(), client=client,
                operator_kind=OperatorKind.SECTION, operator_value=section.id,
                kind=ThreadKind.TICKET, subject_type="practice",
                subject_id=subject,
            )
            for section, subject in zip(sections, ("p1", "p2"), strict=True)
        ]
        with pytest.raises(ValidationError, match="duplicate"):
            await retag_thread(
                db_session, thread_id=moving.id, section=sections[0].id,
                subject=SubjectRef("practice", "p1"),
            )
        assert taken.id != moving.id

    async def test_the_key_spelling_a_dedup_name_on_post_threads(
        self, db_session: AsyncSession,
    ) -> None:
        """THE CROSS CASE ON threads -- held as behaviour, not a mutation
        test: green on the pre-G5 code as well, in both directions. The
        dedup indexes come before the key index (lower OIDs), so two
        requests of one dedup key collide on dedup first; a key spelling
        a dedup name collides only on the key index, and the text rule
        then reached the same 409 by another way. What it holds: the
        same key with another request is a conflict, the dm is not
        returned in its place."""
        client, operator = await _pair(db_session)
        other = (await create_recipient(db_session)).id
        key = f"{_DEDUP_INDEX_NAMES[1]}:{uuid4().hex}"
        first = ThreadCreateIn(
            client=client, operator_kind=OperatorKind.USER,
            operator_value=operator, kind=ThreadKind.DM,
        )
        await create_thread(
            request=_request("/api/v1/threads", first), payload=first,
            idempotency_key=key, session=db_session,
        )
        second = ThreadCreateIn(
            client=client, operator_kind=OperatorKind.USER,
            operator_value=other, kind=ThreadKind.DM,
        )
        with pytest.raises(Exception, match="another request"):
            await create_thread(
                request=_request("/api/v1/threads", second), payload=second,
                idempotency_key=key, session=db_session,
            )


# ---------------------------------------------------------------------------
# The watchman: no IntegrityError in app/ is read as text
# ---------------------------------------------------------------------------

# The forms an error's text can be reached by, declared before the scan:
# str / repr / ascii / format of it, an f-string or a %-format with it,
# .format(...) / .join(...) over it, and its text-bearing attributes --
# .args, the DBAPI's .pgerror / .diag, the driver's .detail / .hint /
# .message, SQLAlchemy's .statement / .params, __str__ / __repr__.
_TEXT_CALLS = frozenset({"str", "repr", "ascii", "format"})
_TEXT_METHODS = frozenset({"format", "join"})
_TEXT_ATTRS = frozenset({
    "args", "pgerror", "diag", "detail", "hint", "message",
    "statement", "params", "__str__", "__repr__",
})
# The rule's family: how a handler classifies an IntegrityError.
_RULES = frozenset({
    "is_unique_violation", "is_unique_violation_of", "is_dedup_violation",
})


def _is_integrity(node: ast.expr | None) -> bool:
    if node is None:
        return False
    if isinstance(node, ast.Tuple):
        return any(_is_integrity(e) for e in node.elts)
    return (isinstance(node, ast.Name) and node.id == "IntegrityError") or (
        isinstance(node, ast.Attribute) and node.attr == "IntegrityError"
    )


def _holders(tree: ast.AST) -> list[tuple[str, list[ast.stmt], int]]:
    """(the error's variable, the code that holds it, line): every
    `except IntegrityError as X` in any form, and every function with a
    parameter annotated IntegrityError."""
    found: list[tuple[str, list[ast.stmt], int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler) and _is_integrity(node.type):
            found.append((node.name or "", node.body, node.lineno))
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            for arg in node.args.args + node.args.kwonlyargs:
                if _is_integrity(arg.annotation):
                    found.append((arg.arg, node.body, node.lineno))
    return found


def _mentions(node: ast.AST, var: str) -> bool:
    return any(isinstance(n, ast.Name) and n.id == var for n in ast.walk(node))


def _text_reads(source: str) -> list[str]:
    """Every form in which code holding an IntegrityError reads its
    text, as "line:form"."""
    reads: list[str] = []
    for var, body, _ in _holders(ast.parse(source)):
        if not var:
            continue
        for stmt in body:
            for node in ast.walk(stmt):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id in _TEXT_CALLS
                    and node.args and _mentions(node.args[0], var)
                ):
                    reads.append(f"{node.lineno}:{node.func.id}()")
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in _TEXT_METHODS
                    and any(_mentions(a, var) for a in node.args)
                ):
                    reads.append(f"{node.lineno}:.{node.func.attr}()")
                if isinstance(node, ast.JoinedStr) and _mentions(node, var):
                    reads.append(f"{node.lineno}:f-string")
                if (
                    isinstance(node, ast.BinOp)
                    and isinstance(node.op, ast.Mod)
                    and _mentions(node.right, var)
                ):
                    reads.append(f"{node.lineno}:%")
                if (
                    isinstance(node, ast.Attribute)
                    and node.attr in _TEXT_ATTRS
                    and _mentions(node.value, var)
                ):
                    reads.append(f"{node.lineno}:.{node.attr}")
    return reads


def _sources() -> dict[str, str]:
    files = sorted(_APP.rglob("*.py"))
    assert len(files) > 50
    return {str(p.relative_to(_APP.parent)): p.read_text() for p in files}


def _calls(body: list[ast.stmt]) -> set[str]:
    return {
        node.func.id if isinstance(node.func, ast.Name) else
        node.func.attr if isinstance(node.func, ast.Attribute) else ""
        for stmt in body for node in ast.walk(stmt)
        if isinstance(node, ast.Call)
    }


def _function(
    source: str, name: str,
) -> ast.FunctionDef | ast.AsyncFunctionDef:
    (found,) = [
        node for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and node.name == name
    ]
    return found


_PLANTED = {
    "str()": "if NAME in str(exc.orig): pass",
    "repr()": "if NAME in repr(exc): pass",
    "ascii()": "x = ascii(exc.orig)",
    "format()": "x = format(exc)",
    "f-string": 'x = f"{exc.orig}"',
    "%": 'x = "%s" % exc',
    ".format()": 'x = "{}".format(exc.orig)',
    ".join()": 'x = "".join(exc.orig.args)',
    ".args": "x = exc.orig.args[0]",
    ".pgerror": "x = exc.orig.pgerror",
    ".diag": "x = exc.orig.diag.message_detail",
    ".detail": "x = exc.orig.__cause__.detail",
    ".hint": "x = exc.orig.__cause__.hint",
    ".message": "x = exc.orig.message",
    ".statement": "x = exc.statement",
    ".params": "x = exc.params",
    ".__str__": "x = exc.orig.__str__()",
    ".__repr__": "x = exc.__repr__()",
}


class TestTheWatchman:
    def test_no_integrity_error_in_app_is_read_as_text(self) -> None:
        reads = {
            path: found for path, source in _sources().items()
            if (found := _text_reads(source))
        }
        assert reads == {}

    def test_the_pair_every_handler_calls_the_rule(self) -> None:
        """"Nothing reads the text" is only worth something next to
        "everything classifies by the rule": the handlers exist, and
        each of them calls it."""
        handlers = [
            (path, line, _calls(body))
            for path, source in _sources().items()
            for _, body, line in _holders(ast.parse(source))
        ]
        assert len(handlers) >= 10
        unclassified = [
            (path, line) for path, line, calls in handlers
            if not calls & _RULES
        ]
        # The one holder that calls none of the family IS its base: the
        # SQLSTATE test the others are built on.
        base = _function(
            _sources()["app/core/database.py"], "is_unique_violation",
        )
        assert unclassified == [("app/core/database.py", base.lineno)]

    @pytest.mark.parametrize(
        ("path", "function", "rule"),
        [
            ("app/engine/service.py", "accept_notification",
             "is_unique_violation_of"),
            ("app/api/messaging.py", "create_thread", "is_unique_violation_of"),
            ("app/api/messaging.py", "post_thread_message",
             "is_unique_violation_of"),
            ("app/messaging/threads.py", "is_dedup_violation",
             "is_unique_violation_of"),
            ("app/messaging/threads.py", "create_or_get_thread_detailed",
             "is_dedup_violation"),
            ("app/messaging/operators.py", "retag_thread",
             "is_dedup_violation"),
        ],
    )
    def test_each_place_calls_the_rule(
        self, path: str, function: str, rule: str,
    ) -> None:
        node = _function(_sources()[path], function)
        assert rule in _calls([node])

    @pytest.mark.parametrize("form", sorted(_PLANTED))
    @pytest.mark.parametrize(
        "holder",
        [
            "try:\n    pass\nexcept IntegrityError as exc:\n    {code}\n",
            "try:\n    pass\nexcept (ValueError, IntegrityError) as exc:\n"
            "    {code}\n",
            "try:\n    pass\n"
            "except sqlalchemy.exc.IntegrityError as exc:\n    {code}\n",
            "def check(exc: IntegrityError) -> None:\n    {code}\n",
        ],
    )
    def test_the_scan_sees_every_form(self, form: str, holder: str) -> None:
        """The instrument's twin: each form, planted in each kind of
        holder, is found -- an empty scan proves nothing on its own."""
        source = holder.replace("{code}", _PLANTED[form])
        assert any(read.endswith(form) for read in _text_reads(source))

    def test_the_rule_itself_is_not_a_text_read(self) -> None:
        source = (
            "def check(exc: IntegrityError) -> bool:\n"
            "    return is_unique_violation_of(exc, NAME)\n"
            "try:\n    pass\nexcept IntegrityError as exc:\n"
            "    cause = getattr(exc.orig, '__cause__', None)\n"
            "    name = getattr(cause, 'constraint_name', None)\n"
        )
        assert _text_reads(source) == []
