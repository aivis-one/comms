# =============================================================================
# D1 / R1 -- retag reads the subject by presence.
# =============================================================================
#
# Before: a retag that named only the section wrote NULL into both
# subject columns and detached the thread from its entity.
#
# MUTATION these tests were written against:
#   M11 "absent" read as null (RetagIn.subject returns None instead of
#       KEEP) -> test_section_only_keeps_the_subject
# =============================================================================

from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.core.database import get_session_factory
from app.messaging.membership import set_membership
from app.messaging.models import Thread
from tests.helpers import create_recipient, create_section, next_phase4c_telegram_id


async def _recipient() -> UUID:
    rid = uuid4()
    async with get_session_factory()() as s:
        await create_recipient(
            s, recipient_id=rid, telegram_id=next_phase4c_telegram_id()
        )
        await s.commit()
    return rid


async def _section_with_agent(agent: UUID) -> tuple[UUID, str]:
    key = f"rt-{uuid4().hex[:8]}"
    async with get_session_factory()() as s:
        section = await create_section(s, key=key)
        sid = section.id
        await s.commit()
    async with get_session_factory()() as s:
        await set_membership(
            s, section_key=key, section_label=key, operator_id=agent,
            member=True,
        )
        await s.commit()
    return sid, key


async def _ticket(client: AsyncClient, client_id: UUID, section: UUID) -> str:
    resp = await client.post(
        "/api/v1/threads",
        json={
            "client": str(client_id),
            "operator_kind": "section",
            "operator_value": str(section),
            "kind": "ticket",
            "subject_type": "practice",
            "subject_id": "p1",
        },
    )
    assert resp.status_code == 200, resp.text
    return str(resp.json()["id"])


async def _thread(tid: str) -> Thread:
    async with get_session_factory()() as s:
        return (
            await s.execute(select(Thread).where(Thread.id == UUID(tid)))
        ).scalar_one()


@pytest.fixture
async def ticket(client: AsyncClient) -> dict[str, Any]:
    """A ticket in section A (subject practice/p1), an agent serving A
    and B, and section B to move it to."""
    agent, client_id = await _recipient(), await _recipient()
    section_a, _ = await _section_with_agent(agent)
    section_b, _ = await _section_with_agent(agent)
    tid = await _ticket(client, client_id, section_a)
    return {"tid": tid, "agent": agent, "to": section_b}


async def _retag(client: AsyncClient, t: dict[str, Any], **subject: Any) -> Any:
    return await client.post(
        f"/api/v1/threads/{t['tid']}/retag",
        json={"operator": str(t["agent"]), "section": str(t["to"]), **subject},
    )


class TestThreeForms:
    async def test_section_only_keeps_the_subject(
        self, client: AsyncClient, ticket: dict[str, Any],
    ) -> None:
        """done-when (1). M11. The pair: the section DID move -- the
        retag ran and only the subject was left alone."""
        resp = await _retag(client, ticket)
        assert resp.status_code == 200, resp.text
        thread = await _thread(ticket["tid"])
        assert (thread.subject_type, thread.subject_id) == ("practice", "p1")
        assert thread.operator_value == ticket["to"]

    async def test_both_null_clears(
        self, client: AsyncClient, ticket: dict[str, Any],
    ) -> None:
        """done-when (2)."""
        resp = await _retag(client, ticket, subject_type=None, subject_id=None)
        assert resp.status_code == 200, resp.text
        thread = await _thread(ticket["tid"])
        assert (thread.subject_type, thread.subject_id) == (None, None)

    async def test_both_set_sets(
        self, client: AsyncClient, ticket: dict[str, Any],
    ) -> None:
        resp = await _retag(client, ticket, subject_type="lesson", subject_id="l9")
        assert resp.status_code == 200, resp.text
        thread = await _thread(ticket["tid"])
        assert (thread.subject_type, thread.subject_id) == ("lesson", "l9")

    @pytest.mark.parametrize(
        "subject",
        [
            {"subject_type": "practice"},
            {"subject_id": "p1"},
            {"subject_type": None},
            {"subject_type": "practice", "subject_id": None},
            {"subject_type": None, "subject_id": "p1"},
        ],
    )
    async def test_a_mix_is_refused_and_changes_nothing(
        self, client: AsyncClient, ticket: dict[str, Any],
        subject: dict[str, Any],
    ) -> None:
        """done-when (3): every half form is a 422; the thread is left
        exactly as it was, section included."""
        before = await _thread(ticket["tid"])
        resp = await _retag(client, ticket, **subject)
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["class"] == "validation"
        after = await _thread(ticket["tid"])
        assert (after.operator_value, after.subject_type, after.subject_id) == (
            before.operator_value, before.subject_type, before.subject_id,
        )

    @pytest.mark.parametrize(
        "subject",
        [
            {},
            {"subject_type": None, "subject_id": None},
            {"subject_type": "lesson", "subject_id": "l9"},
        ],
    )
    async def test_a_repeat_leaves_the_same_state(
        self, client: AsyncClient, ticket: dict[str, Any],
        subject: dict[str, Any],
    ) -> None:
        """done-when (4): the same retag twice -- the same thread state
        as after the first, in every form."""
        first = await _retag(client, ticket, **subject)
        assert first.status_code == 200, first.text
        once = await _thread(ticket["tid"])
        second = await _retag(client, ticket, **subject)
        assert second.status_code == 200, second.text
        twice = await _thread(ticket["tid"])
        fields = ("operator_value", "subject_type", "subject_id", "assignee")
        assert [getattr(twice, f) for f in fields] == [getattr(once, f) for f in fields]

