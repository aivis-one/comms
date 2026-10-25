# =============================================================================
# TK-1 -- a taken ticket changes hands; an operator returns a thread to open
# =============================================================================
#
# What is held here, against the code and against deploy/INTEGRATION.md
# section 14:
#   1 assign / release -- the grid of states and refusals, the pair "the
#     new assignee writes, the previous one cannot", a repeat answering
#     the same, nobody pinged;
#   2 the manual matrix, every cell for both forms, with the close notice;
#     ONE reopen rule (a watchman over app/ and over its own scanner);
#   3 races on real sessions -- the wait is observed through
#     pg_blocking_pids of the pair in this database, never by timing;
#   4 the contract, both ways.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  assign without the condition on assignee/section -> TestRaces
#       test_assign_against_claim (two winners)
#   M2  assign without the roster check on the assignee -> TestAssign
#       test_assignee_outside_the_roster
#   M3  release without the condition on the assignee -> TestRaces
#       test_release_against_assign
#   M4  assign not writing assigned_at -> TestAssign test_from_the_pool
#   M5  assign without the "already theirs" shortcut -> TestAssign
#       test_a_repeat_is_the_same_outcome (assigned_at moves)
#   M6  a cell of _MANUAL_TRANSITIONS added or dropped -> TestStatusMatrix,
#       TestTheContract test_the_matrix_is_the_code
#   M7  set_status reopening without reopen() -> TestTheReopenRule
#   M8  set_status reading the row instead of taking it -> TestRaces
#       test_status_against_status
#   M9  the close pass emitting first and clearing blindly -> TestRaces
#       test_reopen_against_the_close_pass, test_reclose_against_the_close_pass
#   M10 a verb, a body field or a matrix cell out of step with section 14
#       -> TestTheContract
#   M11 the status route deciding WHO on a plain read (_take_thread without
#       the lock) -> TestRaces test_status_is_authorized_on_the_taken_row
# =============================================================================

import ast
import asyncio
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

from app.core.database import get_session_factory
from app.core.exceptions import ClaimTakenError, ValidationError
from app.engine.models import Notification
from app.forgetting import forget_recipient
from app.messaging.constants import OperatorKind, ThreadKind, ThreadStatus
from app.messaging.models import SectionMember, Thread
from app.messaging.operators import assign_thread, claim_thread, release_thread
from app.messaging.status import (
    _MANUAL_TRANSITIONS,
    auto_close_idle_threads_batch,
    set_status,
)
from app.messaging.threads import create_or_get_thread
from app.notifier import TYPE_THREAD_CLOSED, consume_close_notifications
from tests.helpers import (
    create_recipient,
    create_section,
    intake_fields,
    next_tk1_telegram_id,
)

_REPO = Path(__file__).resolve().parent.parent
_APP = _REPO / "app"
_BOUND = 10.0
_POLL = 0.02


# ---------------------------------------------------------------------------
# Building blocks -- every row committed, so real sessions see it
# ---------------------------------------------------------------------------


async def _recipient() -> UUID:
    rid = uuid4()
    async with get_session_factory()() as s:
        await create_recipient(
            s, recipient_id=rid, telegram_id=next_tk1_telegram_id()
        )
        await s.commit()
    return rid


async def _section(*members: UUID) -> UUID:
    """A section; with members, a DECLARED roster of exactly them."""
    async with get_session_factory()() as s:
        section = await create_section(s, key=f"tk1-{uuid4().hex[:8]}")
        for member in members:
            s.add(SectionMember(section_id=section.id, operator_id=member))
        await s.commit()
        return section.id


async def _ticket(section: UUID, *, holder: UUID | None = None) -> UUID:
    """A section thread of a fresh client; claimed by `holder` if given."""
    customer = await _recipient()
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


async def _dm() -> tuple[UUID, UUID]:
    """A DM of a fresh client with a fresh master: (thread, master)."""
    customer, master = await _recipient(), await _recipient()
    async with get_session_factory()() as s:
        thread = await create_or_get_thread(
            s, **intake_fields(), client=customer,
            operator_kind=OperatorKind.USER, operator_value=master,
            kind=ThreadKind.DM,
        )
        await s.commit()
        return thread.id, master


async def _row(tid: UUID) -> Thread:
    async with get_session_factory()() as s:
        thread = await s.get(Thread, tid)
        assert thread is not None
        return thread


async def _close_notices(tid: UUID) -> list[str]:
    """Idempotency keys of the close notices issued for one thread."""
    async with get_session_factory()() as s:
        keys = await s.scalars(
            select(Notification.idempotency_key).where(
                Notification.type == TYPE_THREAD_CLOSED,
                Notification.idempotency_key.like(f"close:{tid}:%"),
            )
        )
        return list(keys)


async def _notifications_to(recipient: UUID) -> int:
    async with get_session_factory()() as s:
        return await s.scalar(
            select(func.count())
            .select_from(Notification)
            .where(Notification.target_value == str(recipient))
        ) or 0


def _error_class(resp: Any) -> str:
    return str(resp.json()["error"]["class"])


async def _assign(
    client: AsyncClient, tid: UUID | str, actor: UUID, assignee: UUID
) -> Any:
    return await client.post(
        f"/api/v1/threads/{tid}/assign",
        json={"operator": str(actor), "assignee": str(assignee)},
    )


async def _release(client: AsyncClient, tid: UUID | str, actor: UUID) -> Any:
    return await client.post(
        f"/api/v1/threads/{tid}/release", json={"operator": str(actor)}
    )


async def _post(client: AsyncClient, tid: UUID, sender: UUID) -> Any:
    return await client.post(
        f"/api/v1/threads/{tid}/messages",
        json={"sender": str(sender), "body": "hello"},
    )


# ---------------------------------------------------------------------------
# 1 -- assign
# ---------------------------------------------------------------------------


class TestAssign:
    async def test_no_such_thread(self, client: AsyncClient) -> None:
        actor, other = await _recipient(), await _recipient()
        resp = await _assign(client, uuid4(), actor, other)
        assert resp.status_code == 404
        assert _error_class(resp) == "not_found"

    async def test_a_dm_by_a_stranger(self, client: AsyncClient) -> None:
        tid, _ = await _dm()
        stranger = await _recipient()
        resp = await _assign(client, tid, stranger, stranger)
        assert resp.status_code == 403
        assert _error_class(resp) == "forbidden"

    async def test_a_dm_by_its_master(self, client: AsyncClient) -> None:
        """The master may operate the DM -- and still cannot hand it over:
        its operator is fixed by the form."""
        tid, master = await _dm()
        other = await _recipient()
        resp = await _assign(client, tid, master, other)
        assert resp.status_code == 422
        assert _error_class(resp) == "validation"
        assert (await _row(tid)).assignee == master

    async def test_an_actor_off_the_roster(self, client: AsyncClient) -> None:
        member, outsider = await _recipient(), await _recipient()
        tid = await _ticket(await _section(member))
        resp = await _assign(client, tid, outsider, member)
        assert resp.status_code == 403
        assert (await _row(tid)).assignee is None

    async def test_no_such_assignee(self, client: AsyncClient) -> None:
        actor = await _recipient()
        tid = await _ticket(await _section())
        resp = await _assign(client, tid, actor, uuid4())
        assert resp.status_code == 404
        assert (await _row(tid)).assignee is None

    async def test_a_deleted_assignee(self, client: AsyncClient) -> None:
        actor, gone = await _recipient(), await _recipient()
        async with get_session_factory()() as s:
            assert await forget_recipient(s, recipient_id=gone, version=2)
            await s.commit()
        tid = await _ticket(await _section())
        resp = await _assign(client, tid, actor, gone)
        assert resp.status_code == 409
        assert _error_class(resp) == "recipient_deleted"
        assert (await _row(tid)).assignee is None

    async def test_assignee_outside_the_roster(
        self, client: AsyncClient
    ) -> None:
        member, outsider = await _recipient(), await _recipient()
        tid = await _ticket(await _section(member))
        resp = await _assign(client, tid, member, outsider)
        assert resp.status_code == 422
        assert _error_class(resp) == "validation"
        assert (await _row(tid)).assignee is None

    async def test_from_the_pool(self, client: AsyncClient) -> None:
        """No roster declared: any operator hands to any operator."""
        actor, assignee = await _recipient(), await _recipient()
        tid = await _ticket(await _section())
        resp = await _assign(client, tid, actor, assignee)
        assert resp.status_code == 200, resp.text
        assert resp.json()["assignee"] == str(assignee)
        row = await _row(tid)
        assert row.assignee == assignee
        assert row.assigned_at is not None

    async def test_over_the_holder(self, client: AsyncClient) -> None:
        holder, admin, new = [await _recipient() for _ in range(3)]
        tid = await _ticket(await _section(holder, admin, new), holder=holder)
        resp = await _assign(client, tid, admin, new)
        assert resp.status_code == 200, resp.text
        assert (await _row(tid)).assignee == new

    async def test_to_oneself(self, client: AsyncClient) -> None:
        holder, admin = await _recipient(), await _recipient()
        tid = await _ticket(await _section(holder, admin), holder=holder)
        resp = await _assign(client, tid, admin, admin)
        assert resp.status_code == 200
        assert (await _row(tid)).assignee == admin

    async def test_a_holder_off_the_roster_hands_it_on(
        self, client: AsyncClient
    ) -> None:
        holder, colleague = await _recipient(), await _recipient()
        section = await _section(holder, colleague)
        tid = await _ticket(section, holder=holder)
        async with get_session_factory()() as s:
            await s.execute(
                delete(SectionMember).where(SectionMember.operator_id == holder)
            )
            await s.commit()
        resp = await _assign(client, tid, holder, colleague)
        assert resp.status_code == 200, resp.text
        assert (await _row(tid)).assignee == colleague

    async def test_a_repeat_is_the_same_outcome(
        self, client: AsyncClient
    ) -> None:
        actor, assignee = await _recipient(), await _recipient()
        tid = await _ticket(await _section())
        first = await _assign(client, tid, actor, assignee)
        at = (await _row(tid)).assigned_at
        second = await _assign(client, tid, actor, assignee)
        assert first.status_code == second.status_code == 200
        assert first.json() == second.json()
        assert at is not None
        assert (await _row(tid)).assigned_at == at

    async def test_status_and_notice_are_untouched(
        self, client: AsyncClient
    ) -> None:
        actor, assignee = await _recipient(), await _recipient()
        tid = await _ticket(await _section())
        closed = await client.post(
            f"/api/v1/threads/{tid}/status",
            json={"operator": str(actor), "status": "closed"},
        )
        assert closed.status_code == 200
        flag = (await _row(tid)).close_notify_pending_at
        assert flag is not None
        assert (await _assign(client, tid, actor, assignee)).status_code == 200
        row = await _row(tid)
        assert row.status == ThreadStatus.CLOSED
        assert row.close_notify_pending_at == flag

    async def test_the_new_assignee_writes_the_previous_cannot(
        self, client: AsyncClient
    ) -> None:
        holder, admin, new = [await _recipient() for _ in range(3)]
        tid = await _ticket(await _section(holder, admin, new), holder=holder)
        assert (await _post(client, tid, holder)).status_code == 200
        assert (await _assign(client, tid, admin, new)).status_code == 200
        accepted = await _post(client, tid, new)
        assert accepted.status_code == 200, accepted.text
        assert accepted.json()["sender"] == str(new)
        refused = await _post(client, tid, holder)
        assert refused.status_code == 403

    @pytest.mark.parametrize(
        "body",
        [
            {},
            {"operator": "ACTOR"},
            {"assignee": "ASSIGNEE"},
            {"operator": "ACTOR", "assignee": None},
            {"operator": None, "assignee": "ASSIGNEE"},
            {"operator": "ACTOR", "assignee": "ASSIGNEE", "section": "ACTOR"},
        ],
    )
    async def test_the_body(
        self, client: AsyncClient, body: dict[str, str | None]
    ) -> None:
        actor, assignee = await _recipient(), await _recipient()
        tid = await _ticket(await _section())
        names = {"ACTOR": str(actor), "ASSIGNEE": str(assignee)}
        sent = {k: names.get(v, v) if v else v for k, v in body.items()}
        resp = await client.post(f"/api/v1/threads/{tid}/assign", json=sent)
        assert resp.status_code == 422
        assert _error_class(resp) == "validation"
        assert (await _row(tid)).assignee is None
        # the pair: the full body passes
        assert (await _assign(client, tid, actor, assignee)).status_code == 200

    async def test_pings_nobody(self, client: AsyncClient) -> None:
        a, b = await _recipient(), await _recipient()
        tid = await _ticket(await _section(a, b))
        assert (await _assign(client, tid, a, b)).status_code == 200
        assert await _notifications_to(a) == 0
        assert await _notifications_to(b) == 0
        # the pair: a message now pings the assignee
        customer = (await _row(tid)).client
        assert (await _post(client, tid, customer)).status_code == 200
        assert await _notifications_to(b) == 1


# ---------------------------------------------------------------------------
# 1 -- release
# ---------------------------------------------------------------------------


class TestRelease:
    async def test_no_such_thread(self, client: AsyncClient) -> None:
        resp = await _release(client, uuid4(), await _recipient())
        assert resp.status_code == 404

    async def test_a_dm_by_a_stranger(self, client: AsyncClient) -> None:
        tid, _ = await _dm()
        resp = await _release(client, tid, await _recipient())
        assert resp.status_code == 403

    async def test_a_dm_by_its_master(self, client: AsyncClient) -> None:
        tid, master = await _dm()
        resp = await _release(client, tid, master)
        assert resp.status_code == 422
        assert (await _row(tid)).assignee == master

    async def test_an_actor_off_the_roster(self, client: AsyncClient) -> None:
        holder, outsider = await _recipient(), await _recipient()
        tid = await _ticket(await _section(holder), holder=holder)
        resp = await _release(client, tid, outsider)
        assert resp.status_code == 403
        assert (await _row(tid)).assignee == holder

    async def test_back_to_the_pool(self, client: AsyncClient) -> None:
        holder, admin = await _recipient(), await _recipient()
        tid = await _ticket(await _section(holder, admin), holder=holder)
        resp = await _release(client, tid, admin)
        assert resp.status_code == 200, resp.text
        assert resp.json()["assignee"] is None
        row = await _row(tid)
        assert row.assignee is None
        assert row.assigned_at is None
        pool = await client.get(
            "/api/v1/threads", params={"operator": str(holder)}
        )
        assert str(tid) in [item["id"] for item in pool.json()["items"]]

    async def test_a_holder_off_the_roster_releases(
        self, client: AsyncClient
    ) -> None:
        holder, colleague = await _recipient(), await _recipient()
        tid = await _ticket(await _section(holder, colleague), holder=holder)
        async with get_session_factory()() as s:
            await s.execute(
                delete(SectionMember).where(SectionMember.operator_id == holder)
            )
            await s.commit()
        assert (await _release(client, tid, holder)).status_code == 200
        assert (await _row(tid)).assignee is None

    async def test_a_repeat_is_the_same_outcome(
        self, client: AsyncClient
    ) -> None:
        holder = await _recipient()
        tid = await _ticket(await _section(), holder=holder)
        first = await _release(client, tid, holder)
        second = await _release(client, tid, holder)
        assert first.status_code == second.status_code == 200
        assert first.json() == second.json()
        assert second.json()["assignee"] is None

    @pytest.mark.parametrize(
        "body",
        [{}, {"operator": None}, {"operator": "ACTOR", "assignee": None}],
    )
    async def test_the_body(
        self, client: AsyncClient, body: dict[str, str | None]
    ) -> None:
        """`assignee: null` is not a release -- the verb is."""
        holder = await _recipient()
        tid = await _ticket(await _section(), holder=holder)
        sent = {k: str(holder) if v == "ACTOR" else v for k, v in body.items()}
        resp = await client.post(f"/api/v1/threads/{tid}/release", json=sent)
        assert resp.status_code == 422
        assert (await _row(tid)).assignee == holder
        assert (await _release(client, tid, holder)).status_code == 200

    async def test_calls_nobody(self, client: AsyncClient) -> None:
        holder, colleague = await _recipient(), await _recipient()
        tid = await _ticket(await _section(holder, colleague), holder=holder)
        assert (await _release(client, tid, holder)).status_code == 200
        assert await _notifications_to(holder) == 0
        assert await _notifications_to(colleague) == 0
        # the pair: the next message in the pool pings the roster
        customer = (await _row(tid)).client
        assert (await _post(client, tid, customer)).status_code == 200
        assert await _notifications_to(holder) == 1
        assert await _notifications_to(colleague) == 1


# ---------------------------------------------------------------------------
# 2 -- the manual matrix, both forms
# ---------------------------------------------------------------------------

_OPEN, _RESOLVED, _CLOSED = "open", "resolved", "closed"
# How each starting status is reached through the API, from `open`.
_REACH = {_OPEN: [], _RESOLVED: [_RESOLVED], _CLOSED: [_CLOSED]}
# The cells: 200 or 422.
_CELLS = {
    (_OPEN, _OPEN): 200, (_OPEN, _RESOLVED): 200, (_OPEN, _CLOSED): 200,
    (_RESOLVED, _OPEN): 200, (_RESOLVED, _RESOLVED): 200,
    (_RESOLVED, _CLOSED): 200,
    (_CLOSED, _OPEN): 200, (_CLOSED, _RESOLVED): 422, (_CLOSED, _CLOSED): 200,
}


class TestStatusMatrix:
    @pytest.mark.parametrize("form", ["section", "dm"])
    @pytest.mark.parametrize(("start", "target"), sorted(_CELLS))
    async def test_the_cell(
        self, client: AsyncClient, form: str, start: str, target: str
    ) -> None:
        if form == "dm":
            tid, actor = await _dm()
        else:
            actor = await _recipient()
            tid = await _ticket(await _section())
        for step in _REACH[start]:
            moved = await client.post(
                f"/api/v1/threads/{tid}/status",
                json={"operator": str(actor), "status": step},
            )
            assert moved.status_code == 200
        before = await _row(tid)

        resp = await client.post(
            f"/api/v1/threads/{tid}/status",
            json={"operator": str(actor), "status": target},
        )
        assert resp.status_code == _CELLS[(start, target)], resp.text
        after = await _row(tid)
        expected = target if resp.status_code == 200 else start
        assert after.status == expected

        flag = after.close_notify_pending_at
        if form == "dm" or expected != _CLOSED:
            assert flag is None
        elif start == _CLOSED:
            assert flag == before.close_notify_pending_at  # kept
            assert flag is not None
        else:
            assert flag is not None  # set by this close


# ---------------------------------------------------------------------------
# 2 -- one reopen rule
# ---------------------------------------------------------------------------

_FLAG = "close_notify_pending_at"


def _writes(source: str) -> tuple[set[str], set[str]]:
    """Functions that clear the close flag, and functions that write
    `status` to OPEN -- in every form a write can take: an attribute
    assignment, a keyword of `.values(...)`, `setattr(x, name, value)`."""
    clears: set[str] = set()
    opens: set[str] = set()

    def is_none(node: ast.AST) -> bool:
        return isinstance(node, ast.Constant) and node.value is None

    def is_open(node: ast.AST) -> bool:
        return isinstance(node, ast.Attribute) and node.attr == "OPEN"

    def walk(node: ast.AST, where: str) -> None:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            where = node.name
        pairs: list[tuple[str, ast.AST]] = []
        if isinstance(node, ast.Assign):
            pairs += [
                (t.attr, node.value)
                for t in node.targets if isinstance(t, ast.Attribute)
            ]
        if isinstance(node, ast.Call):
            func_ = node.func
            name = func_.attr if isinstance(func_, ast.Attribute) else (
                func_.id if isinstance(func_, ast.Name) else ""
            )
            if name == "values":
                pairs += [(k.arg, k.value) for k in node.keywords if k.arg]
            if name == "setattr" and len(node.args) == 3:
                attr = node.args[1]
                if isinstance(attr, ast.Constant) and isinstance(attr.value, str):
                    pairs.append((attr.value, node.args[2]))
        for attr_name, value in pairs:
            if attr_name == _FLAG and is_none(value):
                clears.add(where)
            if attr_name == "status" and is_open(value):
                opens.add(where)
        for child in ast.iter_child_nodes(node):
            walk(child, where)

    walk(ast.parse(source), "<module>")
    return clears, opens


class TestTheReopenRule:
    def test_the_scanner_sees_every_form(self) -> None:
        """The watchman below is only as good as this: each form, found."""
        source = (
            "def a(t):\n    t.close_notify_pending_at = None\n"
            "def b(q):\n    q.values(close_notify_pending_at=None)\n"
            "def c(t):\n    setattr(t, 'close_notify_pending_at', None)\n"
            "def d(t):\n    t.status = ThreadStatus.OPEN\n"
            "def e(q):\n    q.values(status=ThreadStatus.OPEN)\n"
            "def f(t):\n    t.close_notify_pending_at = when\n"
        )
        assert _writes(source) == ({"a", "b", "c"}, {"d", "e"})

    def test_one_copy_in_app(self) -> None:
        clears: set[tuple[str, str]] = set()
        opens: set[tuple[str, str]] = set()
        for path in sorted(_APP.rglob("*.py")):
            rel = path.relative_to(_REPO).as_posix()
            c, o = _writes(path.read_text(encoding="utf-8"))
            clears |= {(rel, f) for f in c}
            opens |= {(rel, f) for f in o}
        assert opens == {("app/messaging/status.py", "reopen")}
        # The close pass clears the flag it has CONSUMED -- the notice's
        # own bookkeeping, not a reopen.
        assert clears == {
            ("app/messaging/status.py", "reopen"),
            ("app/notifier.py", "consume_close_notifications"),
        }

    async def test_a_manual_reopen_cancels_the_notice(
        self, client: AsyncClient
    ) -> None:
        actor = await _recipient()
        reopened = await _ticket(await _section())
        kept = await _ticket(await _section())
        for tid in (reopened, kept):
            resp = await client.post(
                f"/api/v1/threads/{tid}/status",
                json={"operator": str(actor), "status": "closed"},
            )
            assert resp.status_code == 200
        resp = await client.post(
            f"/api/v1/threads/{reopened}/status",
            json={"operator": str(actor), "status": "open"},
        )
        assert resp.status_code == 200
        assert (await _row(reopened)).close_notify_pending_at is None
        assert await consume_close_notifications() == 1
        assert await _close_notices(reopened) == []
        # the pair: a close left alone is announced, once
        assert len(await _close_notices(kept)) == 1

    async def test_a_client_reopen_goes_the_same_way(
        self, client: AsyncClient
    ) -> None:
        actor = await _recipient()
        tid = await _ticket(await _section())
        await client.post(
            f"/api/v1/threads/{tid}/status",
            json={"operator": str(actor), "status": "closed"},
        )
        customer = (await _row(tid)).client
        assert (await _post(client, tid, customer)).status_code == 200
        row = await _row(tid)
        assert row.status == ThreadStatus.OPEN
        assert row.close_notify_pending_at is None
        assert await consume_close_notifications() == 0


# ---------------------------------------------------------------------------
# 3 -- races on real sessions
# ---------------------------------------------------------------------------


async def _pid(session: AsyncSession) -> int:
    pid = await session.scalar(text("SELECT pg_backend_pid()"))
    assert isinstance(pid, int)
    return pid


async def _waiting_on(holder: int) -> bool:
    """Is a backend of this database waiting on `holder` right now?"""
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


async def _settled(work: Awaitable[Any], session: AsyncSession) -> Any:
    """Run `work`, commit its session; the exception is the outcome."""
    try:
        result = await work
    except (ClaimTakenError, ValidationError) as exc:
        await session.rollback()
        return exc
    await session.commit()
    return result


async def _cross(
    first: Callable[[AsyncSession], Awaitable[Any]],
    second: Callable[[AsyncSession], Awaitable[Any]],
    *,
    prepare: Callable[[AsyncSession], Awaitable[Any]] | None = None,
) -> tuple[Any, Any]:
    """`first` writes and holds; `second` must wait for it; then both end.

    `prepare` runs in the second session BEFORE the first writes: what it
    returns is what the second call decided on (a read that is stale by
    the time it writes) and is passed to `second`.
    """
    factory = get_session_factory()
    async with factory() as late, factory() as early:
        seen = await prepare(late) if prepare is not None else None
        first_result = await first(early)
        holder = await _pid(early)
        task = asyncio.create_task(
            _settled(second(late) if seen is None else second(seen), late)
        )
        await _until_waiting(holder, task)
        await early.commit()
        second_result = await asyncio.wait_for(task, _BOUND)
    return first_result, second_result


class TestRaces:
    @pytest.mark.parametrize("assign_first", [False, True])
    async def test_assign_against_claim(self, assign_first: bool) -> None:
        """From the pool: exactly one of the two takes the thread."""
        claimer, assignee = await _recipient(), await _recipient()
        tid = await _ticket(await _section())

        async def claim(s: AsyncSession) -> Any:
            return await claim_thread(s, thread_id=tid, operator=claimer)

        async def assign_now(s: AsyncSession) -> Any:
            return await assign_thread(
                s, thread=await s.get(Thread, tid), assignee=assignee
            )

        async def loaded(s: AsyncSession) -> Any:
            thread = await s.get(Thread, tid)

            async def run() -> Any:
                return await assign_thread(s, thread=thread, assignee=assignee)
            return run

        if assign_first:
            assigned, claimed = await _cross(assign_now, claim)
        else:
            claimed, assigned = await _cross(
                claim, lambda run: run(), prepare=loaded,
            )
        won_claim = claimed is True
        won_assign = isinstance(assigned, Thread)
        assert won_claim != won_assign, (claimed, assigned)
        if not won_assign:
            assert isinstance(assigned, ClaimTakenError)
        winner = claimer if won_claim else assignee
        assert (await _row(tid)).assignee == winner

    async def test_assign_against_assign(self) -> None:
        a, b, c = [await _recipient() for _ in range(3)]
        tid = await _ticket(await _section(), holder=a)

        async def to_b(s: AsyncSession) -> Any:
            return await assign_thread(
                s, thread=await s.get(Thread, tid), assignee=b
            )

        async def loaded(s: AsyncSession) -> Any:
            thread = await s.get(Thread, tid)

            async def run(target: UUID) -> Any:
                return await assign_thread(s, thread=thread, assignee=target)
            return run

        first, second = await _cross(to_b, lambda run: run(c), prepare=loaded)
        assert isinstance(first, Thread)
        assert isinstance(second, ClaimTakenError)
        assert (await _row(tid)).assignee == b

        # The same call twice is not a crossing: both find their outcome.
        tid = await _ticket(await _section(), holder=a)
        first, second = await _cross(to_b, lambda run: run(b), prepare=loaded)
        assert isinstance(first, Thread)
        assert isinstance(second, Thread)
        assert second.assignee == b

    @pytest.mark.parametrize("release_first", [False, True])
    async def test_release_against_assign(self, release_first: bool) -> None:
        """Held by A: of a release and an assign that cross, exactly one
        changes the thread; the other answers 409, it overwrites nothing."""
        holder, new = await _recipient(), await _recipient()
        tid = await _ticket(await _section(), holder=holder)

        async def assign_now(s: AsyncSession) -> Any:
            return await assign_thread(
                s, thread=await s.get(Thread, tid), assignee=new
            )

        async def release_now(s: AsyncSession) -> Any:
            return await release_thread(s, thread=await s.get(Thread, tid))

        async def loaded(s: AsyncSession) -> Any:
            thread = await s.get(Thread, tid)

            async def run(verb: str) -> Any:
                if verb == "release":
                    return await release_thread(s, thread=thread)
                return await assign_thread(s, thread=thread, assignee=new)
            return run

        if release_first:
            released, assigned = await _cross(
                release_now, lambda run: run("assign"), prepare=loaded,
            )
        else:
            assigned, released = await _cross(
                assign_now, lambda run: run("release"), prepare=loaded,
            )
        outcomes = [isinstance(r, Thread) for r in (released, assigned)]
        assert outcomes.count(True) == 1, (released, assigned)
        loser = assigned if release_first else released
        assert isinstance(loser, ClaimTakenError)
        expected = None if release_first else new
        assert (await _row(tid)).assignee == expected

    async def test_release_against_claim(self) -> None:
        """NO WAIT CAN BE BUILT for this pair, in either order, and the
        test says so rather than pretend: a claim only writes a thread
        whose assignee IS NULL, so against a held thread it matches
        nothing and does not queue on the release; and a release of a
        thread it read in the pool writes nothing to queue with. Each
        answers the state it saw; nothing is overwritten."""
        factory = get_session_factory()
        holder, claimer = await _recipient(), await _recipient()
        tid = await _ticket(await _section(), holder=holder)
        async with factory() as early, factory() as late:
            await release_thread(early, thread=await early.get(Thread, tid))
            claimed = await asyncio.wait_for(
                claim_thread(late, thread_id=tid, operator=claimer), _BOUND
            )
            assert claimed is False  # held, as far as the claim can see
            await late.rollback()
            await early.commit()
        assert (await _row(tid)).assignee is None

        tid = await _ticket(await _section())
        async with factory() as early, factory() as late:
            pooled = await late.get(Thread, tid)
            assert await claim_thread(early, thread_id=tid, operator=claimer)
            result = await asyncio.wait_for(
                release_thread(late, thread=pooled), _BOUND
            )
            assert result.assignee is None  # in the pool, as it read it
            await late.commit()
            await early.commit()
        assert (await _row(tid)).assignee == claimer

    async def test_status_against_status(self) -> None:
        """open -> closed holds the row; open -> resolved waits and is
        decided on `closed`: 422, never a resolved thread with a notice."""
        tid = await _ticket(await _section())

        async def close(s: AsyncSession) -> Any:
            return await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)

        async def resolve(s: AsyncSession) -> Any:
            return await set_status(
                s, thread_id=tid, target=ThreadStatus.RESOLVED
            )

        closed, resolved = await _cross(close, resolve)
        assert isinstance(closed, Thread)
        assert isinstance(resolved, ValidationError)
        row = await _row(tid)
        assert row.status == ThreadStatus.CLOSED
        assert row.close_notify_pending_at is not None

    async def test_status_is_authorized_on_the_taken_row(
        self, client: AsyncClient
    ) -> None:
        """The previous holder -- off the roster, so only the assignee
        branch lets them operate -- asks for `resolved` while the thread
        is being handed to a colleague. The route takes the row before
        it decides who may act: it waits for the hand-over and answers
        403, instead of resolving a thread that is no longer theirs."""
        holder, colleague = await _recipient(), await _recipient()
        tid = await _ticket(await _section(holder, colleague), holder=holder)
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
                f"/api/v1/threads/{tid}/status",
                json={"operator": str(holder), "status": "resolved"},
            ))
            await _until_waiting(pid, task)
            await early.commit()
        resp = await asyncio.wait_for(task, _BOUND)
        assert resp.status_code == 403, resp.text
        row = await _row(tid)
        assert row.assignee == colleague
        assert row.status == ThreadStatus.OPEN

    async def test_reopen_against_the_close_pass(self) -> None:
        """A reopen committed between the pass's read and its commit:
        nobody is told the conversation closed."""
        tid = await _ticket(await _section())
        async with get_session_factory()() as s:
            await set_status(s, thread_id=tid, target=ThreadStatus.CLOSED)
            await s.commit()

        async def reopen(s: AsyncSession) -> Any:
            return await set_status(s, thread_id=tid, target=ThreadStatus.OPEN)

        async def the_pass(_: AsyncSession) -> Any:
            return await consume_close_notifications()

        _, notified = await _cross(reopen, the_pass)
        assert notified == 0
        assert await _close_notices(tid) == []
        row = await _row(tid)
        assert row.status == ThreadStatus.OPEN
        assert row.close_notify_pending_at is None

    async def test_reclose_against_the_close_pass(self) -> None:
        """Reopened and closed again (a NEW flag) between the pass's read
        and its commit: the new close is not lost -- the next pass
        announces it, and the voided one is never announced."""
        tid = await _ticket(await _section())
        first_close = datetime.now(UTC) - timedelta(minutes=5)
        async with get_session_factory()() as s:
            await set_status(
                s, thread_id=tid, target=ThreadStatus.CLOSED, when=first_close
            )
            await s.commit()
        voided = (await _row(tid)).close_notify_pending_at
        assert voided is not None

        async def reopen_and_close(s: AsyncSession) -> Any:
            await set_status(s, thread_id=tid, target=ThreadStatus.OPEN)
            return await set_status(
                s, thread_id=tid, target=ThreadStatus.CLOSED,
                when=datetime.now(UTC),
            )

        async def the_pass(_: AsyncSession) -> Any:
            return await consume_close_notifications()

        _, notified = await _cross(reopen_and_close, the_pass)
        assert notified == 0
        fresh = (await _row(tid)).close_notify_pending_at
        assert fresh is not None
        assert fresh != voided
        assert await consume_close_notifications() == 1
        assert await _close_notices(tid) == [
            f"close:{tid}:{fresh.isoformat()}"
        ]
        assert (await _row(tid)).close_notify_pending_at is None

    async def test_reopen_against_auto_close(self) -> None:
        """MEASURED, pinned with the KNOWN CEILING on set_status: a
        manual reopen of an idle thread does not survive auto-close --
        neither a pass already running nor the next one."""
        now = datetime.now(UTC)
        tid = await _ticket(await _section())
        async with get_session_factory()() as s:
            await set_status(s, thread_id=tid, target=ThreadStatus.RESOLVED)
            await s.execute(
                update(Thread)
                .where(Thread.id == tid)
                .values(created_at=now - timedelta(days=40))
            )
            await s.commit()

        async def reopen(s: AsyncSession) -> Any:
            return await set_status(s, thread_id=tid, target=ThreadStatus.OPEN)

        async def auto_close(s: AsyncSession) -> Any:
            return await auto_close_idle_threads_batch(
                s, cutoff=now - timedelta(days=30), when=now, limit=1000
            )

        reopened, closed = await _cross(reopen, auto_close)
        assert reopened.status == ThreadStatus.OPEN  # answered open ...
        assert closed == 1  # ... and closed right after, the same pass
        row = await _row(tid)
        assert row.status == ThreadStatus.CLOSED
        assert row.close_notify_pending_at is not None

        # Reopened by hand once more, with no pass running: the next
        # pass closes it again, with a fresh notice to the client.
        async with get_session_factory()() as s:
            await set_status(s, thread_id=tid, target=ThreadStatus.OPEN)
            await s.commit()
        assert (await _row(tid)).close_notify_pending_at is None
        async with get_session_factory()() as s:
            assert await auto_close(s) == 1
            await s.commit()
        row = await _row(tid)
        assert row.status == ThreadStatus.CLOSED
        assert row.close_notify_pending_at is not None


# ---------------------------------------------------------------------------
# 4 -- the contract, deploy/INTEGRATION.md section 14, both ways
# ---------------------------------------------------------------------------

_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


def _section_14() -> str:
    start = _DOC.index("\n## 14. ")
    return _DOC[start:_DOC.index("\n## ", start + 1)]


def _table_after(heading: str, text_: str) -> list[list[str]]:
    """The first pipe table after `heading`: cells of its body rows."""
    rest = text_.split(heading, 1)[1]
    lines = rest.split("|---", 1)[1].split("\n")[1:]
    rows: list[list[str]] = []
    for line in lines:
        if not line.startswith("|"):
            break
        rows.append([cell.strip() for cell in line.strip("|").split("|")])
    return rows


def _operator_verbs() -> dict[str, list[str]]:
    """Every POST /threads/{id}/<verb> whose body names an `operator`,
    with its body fields in order -- read off the app's own schema."""
    from app.main import app

    spec = app.openapi()
    verbs: dict[str, list[str]] = {}
    for path, item in spec["paths"].items():
        match = re.fullmatch(r"/api/v1/threads/\{thread_id\}/([a-z]+)", path)
        if match is None or "post" not in item:
            continue
        body = item["post"].get("requestBody")
        if body is None:
            continue
        ref = body["content"]["application/json"]["schema"]["$ref"]
        schema = spec["components"]["schemas"][ref.rsplit("/", 1)[1]]
        fields = list(schema["properties"])
        if "operator" in fields:
            verbs[match.group(1)] = fields
    return verbs


class TestTheContract:
    def test_the_verbs_are_the_routes(self) -> None:
        rows = _table_after("| call | body |", _section_14())
        documented: dict[str, list[str]] = {}
        for row in rows:
            call = re.fullmatch(r"`POST /threads/\{id\}/([a-z]+)`", row[0])
            assert call is not None, row[0]
            documented[call.group(1)] = re.findall(r"`([a-z_]+)`", row[1])
        code = _operator_verbs()
        assert {"claim", "assign", "release", "status", "retag"} <= set(code)
        assert documented == code

    def test_the_matrix_is_the_code(self) -> None:
        rows = _table_after("| from \\ to |", _section_14())
        order = [ThreadStatus.OPEN, ThreadStatus.RESOLVED, ThreadStatus.CLOSED]
        documented = {
            (ThreadStatus(row[0].strip("`")), target): cell
            for row in rows
            for target, cell in zip(order, row[1:], strict=True)
        }
        code = {
            (start, target): (
                "no-op" if start == target
                else "yes" if target in _MANUAL_TRANSITIONS[start]
                else "422"
            )
            for start in order
            for target in order
        }
        assert len(documented) == 9
        assert documented == code

    def test_the_refusals_speak_section_5(self) -> None:
        known = {
            (row[0], row[1].strip("`"))
            for row in _table_after("| status | class | when |", _DOC)
        }
        refusals = {
            (row[0], row[1].strip("`"))
            for row in _table_after("| status | class | when |", _section_14())
        }
        assert refusals
        assert refusals <= known
