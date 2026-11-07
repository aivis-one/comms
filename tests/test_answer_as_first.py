# =============================================================================
# A repeated request is answered as the first one was (REV-3)
# =============================================================================
#
# What is held here:
#   1 claim -- "is the thread yours now": the operator it is assigned to
#     gets 200 `claimed` again, decided on the taken row before the
#     roster is asked -- so a claim repeated after a lost answer stays
#     200 even when the roster has dropped the operator meanwhile
#     (app/api/messaging.py claim). The form is asked first: a user
#     thread is never claimed, not even by its master;
#   2 create a thread -- the request that lost the insert race to a
#     request with the SAME key and the SAME body is that request again:
#     `created: true` and the same thread. Postgres may report either
#     unique index first; on the dedup index the API applies the key's
#     rule itself: the same body is the replay, another body with the
#     key is a 409 (app/api/messaging.py create_thread). Two requests
#     with DIFFERENT keys are two requests: the loser gets
#     `created: false`, as before.
# Races on two real sessions, the loser waiting on the winner by
# pg_blocking_pids (tests/test_race_by_constraint.py _race).
#
# MUTATIONS these tests were written against (each turns one red):
#   M7 no answer for the assignee before the roster -> TestClaim
#      test_repeated_after_the_roster_dropped_the_operator
#   M8 that answer asked before the form -> test_a_user_thread_is_not_claimed
#   M9 no key rule on the dedup race -> TestCreateRace same body
#   M10 the key rule without the fingerprint -> TestCreateRace another body
# =============================================================================

from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.messaging import ThreadCreateIn, create_thread
from app.core.database import get_session_factory
from app.core.exceptions import IdempotencyConflictError
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.membership import set_membership
from app.messaging.models import Thread
from app.messaging.threads import create_or_get_thread
from tests.helpers import (
    create_recipient,
    create_section,
    declare_member,
    intake_fields,
)
from tests.test_race_by_constraint import _committed_pair, _count, _race, _request

# -----------------------------------------------------------------------------
# 1 -- claim
# -----------------------------------------------------------------------------


async def _section_thread() -> tuple[UUID, str, UUID]:
    """A section thread and one operator on the section's roster:
    (thread, section key, operator)."""
    key = f"rev3-{uuid4().hex[:8]}"
    async with get_session_factory()() as session:
        client = await create_recipient(session)
        operator = await create_recipient(session)
        section = await create_section(session, key=key)
        await declare_member(session, section, operator.id)
        await session.commit()
        thread = await create_or_get_thread(
            session,
            **intake_fields(),
            client=client.id,
            operator_kind=OperatorKind.SECTION,
            operator_value=section.id,
            kind=ThreadKind.TICKET,
        )
        await session.commit()
        return thread.id, key, operator.id


async def _claim(client: AsyncClient, thread: UUID, operator: UUID) -> Any:
    return await client.post(
        f"/api/v1/threads/{thread}/claim",
        json={"operator": str(operator)},
    )


class TestClaim:
    async def test_repeated_after_the_roster_dropped_the_operator(
        self,
        client: AsyncClient,
    ) -> None:
        thread, key, operator = await _section_thread()
        first = await _claim(client, thread, operator)
        assert first.status_code == 200, first.text
        async with get_session_factory()() as session:
            await set_membership(
                session,
                section_key=key,
                section_label=key,
                operator_id=operator,
                member=False,
            )
            await session.commit()
        again = await _claim(client, thread, operator)
        assert again.status_code == 200, again.text
        assert again.json()["claimed"] is True
        assert again.json()["thread"] == first.json()["thread"]

    async def test_another_operator_is_still_asked_the_roster(
        self,
        client: AsyncClient,
    ) -> None:
        """The pair: the shortcut is the assignee's alone."""
        thread, _, operator = await _section_thread()
        assert (await _claim(client, thread, operator)).status_code == 200
        async with get_session_factory()() as session:
            stranger = (await create_recipient(session)).id
            await session.commit()
        response = await _claim(client, thread, stranger)
        assert response.status_code == 403, response.text

    async def test_a_user_thread_is_not_claimed(
        self,
        client: AsyncClient,
    ) -> None:
        """Its master is its assignee from the start, and still a claim
        is refused by the form -- no first claim was ever answered 200."""
        async with get_session_factory()() as session:
            customer = await create_recipient(session)
            master = await create_recipient(session)
            await session.commit()
            thread = await create_or_get_thread(
                session,
                **intake_fields(),
                client=customer.id,
                operator_kind=OperatorKind.USER,
                operator_value=master.id,
                kind=ThreadKind.DM,
            )
            await session.commit()
            assert thread.assignee == master.id
        response = await _claim(client, thread.id, master.id)
        assert response.status_code == 403, response.text


# -----------------------------------------------------------------------------
# 2 -- the creation race
# -----------------------------------------------------------------------------

_CONFLICT = "conflict"


def _payload(
    client: UUID,
    operator: UUID,
    kind: ThreadKind,
    title: str | None = None,
) -> ThreadCreateIn:
    subject = (
        {"subject_type": "rev3", "subject_id": uuid4().hex}
        if kind is ThreadKind.TICKET
        else {}
    )
    return ThreadCreateIn(
        client=client,
        operator_kind=OperatorKind.USER,
        operator_value=operator,
        kind=kind,
        title=title,
        **subject,
    )


def _poster(key: str, payload: ThreadCreateIn) -> Any:
    async def post(session: AsyncSession) -> Any:
        try:
            return await create_thread(
                request=_request("/api/v1/threads", payload),
                payload=payload,
                idempotency_key=key,
                session=session,
            )
        except IdempotencyConflictError:
            return _CONFLICT

    return post


@pytest.mark.parametrize("kind", [ThreadKind.DM, ThreadKind.TICKET])
class TestCreateRace:
    async def test_the_same_request_twice(self, kind: ThreadKind) -> None:
        client, operator = await _committed_pair()
        key = f"rev3-thread-{uuid4().hex}"
        payload = _payload(client, operator, kind)
        won, lost = await _race(_poster(key, payload), _poster(key, payload))
        assert won["created"] is True
        assert lost["created"] is True
        assert lost["id"] == won["id"]
        assert await _count(Thread, key) == 1

    async def test_another_body_with_the_key(self, kind: ThreadKind) -> None:
        client, operator = await _committed_pair()
        key = f"rev3-thread-{uuid4().hex}"
        first = _payload(client, operator, kind)
        other = first.model_copy(update={"title": "another"})
        won, lost = await _race(_poster(key, first), _poster(key, other))
        assert won["created"] is True
        assert lost == _CONFLICT
        assert await _count(Thread, key) == 1

    async def test_two_keys_are_two_requests(self, kind: ThreadKind) -> None:
        """The pair: the dedup alone, without the key, is a get."""
        client, operator = await _committed_pair()
        payload = _payload(client, operator, kind)
        keys = [f"rev3-thread-{uuid4().hex}" for _ in range(2)]
        won, lost = await _race(
            _poster(keys[0], payload),
            _poster(keys[1], payload),
        )
        assert (won["created"], lost["created"]) == (True, False)
        assert lost["id"] == won["id"]
