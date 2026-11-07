# =============================================================================
# API-1 -- an operator acts only while they have the right; an emptied
# roster serves nobody
# =============================================================================
#
# What is held here, against the code and against deploy/INTEGRATION.md
# ("Serves the section"):
#   1 THE RIGHT AND THE ACTION ARE ONE. Each of claim, assign, release,
#     status and retag decides WHO on rows it has TAKEN and keeps them
#     until it commits (membership.serves_section). On real sessions, in
#     both orders against a removal from the roster:
#       - the verb first: the removal WAITS for it (pg_blocking_pids,
#         never timing) and lands after it -- the verb went through while
#         the actor had the right;
#       - the removal first, committed: the verb answers 403 and the
#         thread is not changed.
#     The invariant is asserted, not a winner: "the removal never lands
#     between the check and the write". The assignee keeps acting after
#     leaving the roster (the branch that comes first in can_operate).
#     Forgetting is the second writer of the roster (app/forgetting.py):
#     the same two orders, and the crossing that deadlocked without the
#     recipient lock taken first.
#   2 AN EMPTIED ROSTER SERVES NOBODY; AN UNDECLARED ONE SERVES EVERYBODY.
#     Pool, rights and the pool push agree on both, the boolean and the
#     SQL form of the rule alike.
#   3 the contract, against the code.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 serves_section reading without its locks (the check apart from the
#      write, as before API-1) -> TestTheRightAtTheWrite
#      test_the_verb_first[*] (the removal does not wait)
#   M2 the boolean form by the old rule ("no members -> anybody") ->
#      TestAnEmptiedRoster test_nobody_claims_nor_operates
#   M3 the SQL form by the old rule -> TestAnEmptiedRoster
#      test_the_pool_shows_it_to_nobody
#   M4 set_membership not declaring on `member: true` -> TestAnEmptiedRoster
#      test_the_event_declares_and_removal_keeps_it
#   M5 set_membership declaring on `member: false` too -> TestAnUndeclared
#      test_a_removal_alone_does_not_declare
#   M7 serves_section without the recipient lock first -> TestForgetting
#      test_forgetting_against_a_claim_in_flight (40P01)
#
# Band 94260-94659 (tests/helpers.next_api1_telegram_id).
# =============================================================================

import asyncio
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api import messaging as api
from app.core.database import get_session_factory
from app.forgetting import forget_recipient
from app.messaging.constants import OperatorKind, ThreadKind, ThreadStatus
from app.messaging.membership import serves_section, set_membership
from app.messaging.models import Section, SectionMember, Thread
from app.messaging.operators import (
    can_claim,
    can_operate,
    claim_thread,
    list_visible_threads,
)
from app.messaging.rows import take_thread
from app.messaging.threads import create_or_get_thread, post_message
from app.notifier import notify_new_message
from tests.helpers import (
    create_recipient,
    intake_fields,
    next_api1_telegram_id,
)

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
            s, recipient_id=rid, telegram_id=next_api1_telegram_id()
        )
        await s.commit()
    return rid


@dataclass(frozen=True)
class _Section:
    id: UUID
    key: str


async def _section(*members: UUID) -> _Section:
    """A section; with members, declared by the product's own event."""
    key = f"api1-{uuid4().hex[:8]}"
    async with get_session_factory()() as s:
        if not members:
            section = Section(key=key, label=key)
            s.add(section)
            await s.flush()
            await s.commit()
            return _Section(section.id, key)
        for member in members:
            await set_membership(
                s, section_key=key, section_label=key,
                operator_id=member, member=True,
            )
        await s.commit()
        found = await s.scalar(select(Section.id).where(Section.key == key))
        assert found is not None
        return _Section(found, key)


async def _remove(section: _Section, operator: UUID, s: AsyncSession) -> None:
    await set_membership(
        s, section_key=section.key, section_label=section.key,
        operator_id=operator, member=False,
    )
    await s.commit()


async def _ticket(section: _Section, *, holder: UUID | None = None) -> UUID:
    customer = await _recipient()
    async with get_session_factory()() as s:
        thread = await create_or_get_thread(
            s, **intake_fields(), client=customer,
            operator_kind=OperatorKind.SECTION, operator_value=section.id,
            kind=ThreadKind.TICKET, subject_type="practice",
            subject_id=uuid4().hex,
        )
        if holder is not None:
            assert await claim_thread(s, thread_id=thread.id, operator=holder)
        await s.commit()
        return thread.id


async def _row(tid: UUID) -> Thread:
    async with get_session_factory()() as s:
        thread = await s.get(Thread, tid)
        assert thread is not None
        return thread


def _shape(thread: Thread) -> tuple[Any, ...]:
    return (
        thread.assignee, thread.assigned_at, thread.status,
        thread.operator_value, thread.subject_type, thread.subject_id,
    )


async def _is_member(section: _Section, operator: UUID) -> bool:
    async with get_session_factory()() as s:
        found = await s.scalar(
            select(SectionMember.operator_id).where(
                SectionMember.section_id == section.id,
                SectionMember.operator_id == operator,
            )
        )
        return found is not None


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


# ---------------------------------------------------------------------------
# The five verbs: who acts, on what, and what "it went through" looks like
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Case:
    tid: UUID
    actor: UUID
    section: _Section
    call: Callable[[AsyncSession], Awaitable[Any]]
    url: str
    body: dict[str, Any]
    applied: Callable[[Thread], bool]


async def _case(verb: str, *, actor_holds: bool = False) -> _Case:
    """A section thread and an actor ON the roster who may run `verb`.

    `actor_holds` -- the actor is the thread's assignee (the branch of
    can_operate that ignores the roster). Claim's own such case -- the
    assignee's repeat, answered before the roster -- is held in
    tests/test_answer_as_first.py.
    """
    actor, holder, colleague = [await _recipient() for _ in range(3)]
    section = await _section(actor, holder, colleague)
    route: Callable[[AsyncSession, UUID, Any], Awaitable[Any]]
    payload: Any
    applied: Callable[[Thread], bool]
    if verb == "claim":
        tid = await _ticket(section)
        body: dict[str, Any] = {"operator": str(actor)}
        route, payload = api.claim, api.ClaimIn(operator=actor)

        def applied(t: Thread) -> bool:
            return t.assignee == actor
    else:
        tid = await _ticket(section, holder=actor if actor_holds else holder)
    if verb == "assign":
        body = {"operator": str(actor), "assignee": str(colleague)}
        route = api.assign
        payload = api.AssignIn(operator=actor, assignee=colleague)

        def applied(t: Thread) -> bool:
            return t.assignee == colleague
    elif verb == "release":
        body = {"operator": str(actor)}
        route, payload = api.release, api.ReleaseIn(operator=actor)

        def applied(t: Thread) -> bool:
            return t.assignee is None
    elif verb == "status":
        body = {"operator": str(actor), "status": "resolved"}
        route = api.change_status
        payload = api.StatusIn(operator=actor, status=ThreadStatus.RESOLVED)

        def applied(t: Thread) -> bool:
            return t.status == ThreadStatus.RESOLVED
    elif verb == "retag":
        target = await _section()
        body = {"operator": str(actor), "section": str(target.id)}
        route = api.retag
        payload = api.RetagIn(operator=actor, section=target.id)

        def applied(t: Thread) -> bool:
            return t.operator_value == target.id

    async def call(s: AsyncSession) -> Any:
        return await route(s, tid, payload)

    return _Case(
        tid, actor, section, call, f"/api/v1/threads/{tid}/{verb}", body,
        applied,
    )


_VERBS = ("claim", "assign", "release", "status", "retag")
_HELD_VERBS = ("assign", "release", "status", "retag")


# ---------------------------------------------------------------------------
# 1 -- the right is decided at the write
# ---------------------------------------------------------------------------


class TestTheRightAtTheWrite:
    @pytest.mark.parametrize("verb", _VERBS)
    async def test_the_verb_first(self, verb: str) -> None:
        """The verb has decided WHO and written, uncommitted; the
        product removes the actor from the roster meanwhile. The removal
        waits for the verb, and lands after it: the verb went through
        while the actor had the right -- never between the check and the
        write."""
        case = await _case(verb)
        factory = get_session_factory()
        async with factory() as verb_session, factory() as product:
            await case.call(verb_session)
            holder = await _pid(verb_session)
            task = asyncio.create_task(_remove(case.section, case.actor, product))
            await _until_waiting(holder, task)
            await verb_session.commit()
            await asyncio.wait_for(task, _BOUND)
        assert case.applied(await _row(case.tid))
        assert not await _is_member(case.section, case.actor)

    @pytest.mark.parametrize("verb", _VERBS)
    async def test_the_removal_first(
        self, verb: str, client: AsyncClient
    ) -> None:
        """Removed and committed before the verb: a refusal by right, the
        thread exactly as it was."""
        case = await _case(verb)
        async with get_session_factory()() as product:
            await _remove(case.section, case.actor, product)
        before = _shape(await _row(case.tid))
        resp = await client.post(case.url, json=case.body)
        assert resp.status_code == 403, resp.text
        assert resp.json()["error"]["class"] == "forbidden"
        assert _shape(await _row(case.tid)) == before

    @pytest.mark.parametrize("verb", _VERBS)
    async def test_no_removal_goes_through(
        self, verb: str, client: AsyncClient
    ) -> None:
        """The pair: nobody intervenes -> the verb goes through."""
        case = await _case(verb)
        resp = await client.post(case.url, json=case.body)
        assert resp.status_code == 200, resp.text
        assert case.applied(await _row(case.tid))

    @pytest.mark.parametrize("verb", _HELD_VERBS)
    async def test_the_assignee_acts_after_leaving(
        self, verb: str, client: AsyncClient
    ) -> None:
        """Whoever took the conversation finishes or hands it on, off the
        roster (can_operate's first branch)."""
        case = await _case(verb, actor_holds=True)
        async with get_session_factory()() as product:
            await _remove(case.section, case.actor, product)
        resp = await client.post(case.url, json=case.body)
        assert resp.status_code == 200, resp.text
        assert case.applied(await _row(case.tid))


class TestForgetting:
    """The second writer of the roster: forgetting deletes a person's
    seats (app/forgetting.py) under their recipient row, taken FOR
    UPDATE by the tombstone."""

    async def test_forgetting_against_a_claim_in_flight(self) -> None:
        """The claim has decided WHO (its locks held) and not written yet;
        forgetting the claimer starts. Forgetting waits on the recipient
        row the check took first, the claim writes and commits, and
        forgetting then removes the seat. Without the recipient lock
        first, forgetting would take the recipient, wait on the seat, and
        the claim's foreign key would wait on the recipient: 40P01."""
        actor, colleague = await _recipient(), await _recipient()
        section = await _section(actor, colleague)
        tid = await _ticket(section)
        factory = get_session_factory()
        async with factory() as claimer, factory() as eraser:
            thread = await take_thread(claimer, tid)
            assert await can_claim(claimer, thread, actor)
            holder = await _pid(claimer)

            async def forget() -> bool:
                done = await forget_recipient(
                    eraser, recipient_id=actor, version=2
                )
                await eraser.commit()
                return done

            task = asyncio.create_task(forget())
            await _until_waiting(holder, task)
            assert await claim_thread(claimer, thread_id=tid, operator=actor)
            await claimer.commit()
            assert await asyncio.wait_for(task, _BOUND) is True
        assert (await _row(tid)).assignee == actor
        assert not await _is_member(section, actor)

    async def test_forgotten_first(self, client: AsyncClient) -> None:
        actor, colleague = await _recipient(), await _recipient()
        section = await _section(actor, colleague)
        tid = await _ticket(section)
        async with get_session_factory()() as s:
            assert await forget_recipient(s, recipient_id=actor, version=2)
            await s.commit()
        resp = await client.post(
            f"/api/v1/threads/{tid}/claim", json={"operator": str(actor)}
        )
        assert resp.status_code == 403, resp.text
        assert (await _row(tid)).assignee is None


# ---------------------------------------------------------------------------
# 2 -- an emptied roster serves nobody; an undeclared one, everybody
# ---------------------------------------------------------------------------


async def _push_targets(tid: UUID, sender: UUID) -> list[UUID]:
    async with get_session_factory()() as s:
        thread = await s.get(Thread, tid)
        assert thread is not None
        message = await post_message(
            s, **intake_fields(), thread_id=tid, sender=sender, body="hello?"
        )
        created = await notify_new_message(s, thread=thread, message=message)
        await s.rollback()
        return [UUID(n.target_value) for n in created]


async def _pool_of(operator: UUID) -> set[UUID]:
    async with get_session_factory()() as s:
        threads, _ = await list_visible_threads(s, operator=operator)
        return {t.id for t in threads}


async def _declared(section: _Section) -> bool:
    async with get_session_factory()() as s:
        value = await s.scalar(
            select(Section.roster_declared).where(Section.id == section.id)
        )
        assert value is not None
        return bool(value)


class TestAnEmptiedRoster:
    async def _emptied(self) -> tuple[_Section, UUID, UUID, UUID]:
        """A declared section whose last member was removed, with a pool
        ticket in it; (section, ticket, former member, stranger)."""
        former, stranger = await _recipient(), await _recipient()
        section = await _section(former)
        tid = await _ticket(section)
        async with get_session_factory()() as product:
            await _remove(section, former, product)
        return section, tid, former, stranger

    async def test_the_event_declares_and_removal_keeps_it(self) -> None:
        member = await _recipient()
        section = await _section(member)
        assert await _declared(section) is True
        async with get_session_factory()() as product:
            await _remove(section, member, product)
        assert await _declared(section) is True

    async def test_nobody_claims_nor_operates(
        self, client: AsyncClient
    ) -> None:
        section, tid, former, stranger = await self._emptied()
        for actor in (former, stranger):
            async with get_session_factory()() as s:
                thread = await s.get(Thread, tid)
                assert thread is not None
                assert await can_claim(s, thread, actor) is False
                assert await can_operate(s, thread, actor) is False
                assert await serves_section(s, section.id, actor) is False
            resp = await client.post(
                f"/api/v1/threads/{tid}/claim", json={"operator": str(actor)}
            )
            assert resp.status_code == 403, resp.text
        assert (await _row(tid)).assignee is None

    async def test_the_pool_shows_it_to_nobody(self) -> None:
        """Hidden from both -- and the same operator still sees a thread
        that IS theirs, so the empty answer is not an empty query."""
        _, tid, former, stranger = await self._emptied()
        open_section = await _section()
        theirs = await _ticket(open_section)
        assert tid not in await _pool_of(former)
        assert tid not in await _pool_of(stranger)
        assert theirs in await _pool_of(stranger)

    async def test_the_pool_push_reaches_nobody(
        self, stub_profile: None
    ) -> None:
        _, tid, _, _ = await self._emptied()
        customer = (await _row(tid)).client
        assert await _push_targets(tid, customer) == []

    async def test_the_assignee_carries_on(self, client: AsyncClient) -> None:
        holder = await _recipient()
        section = await _section(holder)
        tid = await _ticket(section, holder=holder)
        async with get_session_factory()() as product:
            await _remove(section, holder, product)
        assert tid in await _pool_of(holder)
        resp = await client.post(
            f"/api/v1/threads/{tid}/status",
            json={"operator": str(holder), "status": "resolved"},
        )
        assert resp.status_code == 200, resp.text
        assert (await _row(tid)).status == ThreadStatus.RESOLVED


class TestAnUndeclared:
    async def test_any_operator_serves_it(self, client: AsyncClient) -> None:
        """Opened by a thread, no roster event ever: as before API-1."""
        stranger = await _recipient()
        section = await _section()
        tid = await _ticket(section)
        assert await _declared(section) is False
        assert tid in await _pool_of(stranger)
        resp = await client.post(
            f"/api/v1/threads/{tid}/claim", json={"operator": str(stranger)}
        )
        assert resp.status_code == 200, resp.text
        assert (await _row(tid)).assignee == stranger

    async def test_a_removal_alone_does_not_declare(self) -> None:
        """`member: false` for somebody who was never there removes
        nobody: the product kept no roster, and the section stays open."""
        stranger = await _recipient()
        section = await _section()
        async with get_session_factory()() as product:
            await _remove(section, stranger, product)
        assert await _declared(section) is False
        async with get_session_factory()() as s:
            assert await serves_section(s, section.id, stranger) is True


# ---------------------------------------------------------------------------
# 3 -- the contract, against the code
# ---------------------------------------------------------------------------


def _integration() -> str:
    return re.sub(
        r"\s+", " ", (_REPO / "deploy" / "INTEGRATION.md").read_text()
    )


class TestTheContract:
    def test_serves_the_section_states_the_rule(self) -> None:
        doc = _integration()
        for sentence in (
            "a section whose roster was never declared is served by every "
            "operator",
            "a section whose operators were all removed is served by nobody",
            "The thread's assignee may act on it even after leaving the "
            "roster",
            "A section emptied before comms 3.14.0 has no record of its "
            "roster and is served by every operator",
        ):
            assert sentence in doc, sentence

    def test_the_event_states_the_declaration(self) -> None:
        doc = _integration()
        assert (
            "The first `member: true` declares the section's roster, for "
            "good" in doc
        )

    @pytest.mark.parametrize(
        ("members", "removed", "served"),
        [((), False, True), (("me",), False, True), (("me",), True, False)],
        ids=["undeclared", "declared-member", "declared-emptied"],
    )
    async def test_the_three_states_are_the_code(
        self, members: tuple[str, ...], removed: bool, served: bool
    ) -> None:
        """The states the contract names, in the code's own answer."""
        me = await _recipient()
        section = await _section(*(me for _ in members))
        if removed:
            async with get_session_factory()() as product:
                await _remove(section, me, product)
        async with get_session_factory()() as s:
            assert await serves_section(s, section.id, me) is served
