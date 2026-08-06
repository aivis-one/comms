# =============================================================================
# COMMS Service -- additive `created` on POST /threads (seam T2, ID-10)
# =============================================================================
# Band 92100-92139. The product detects "a conversation started" from
# its OWN create call instead of a pushed callback, so the flag has to
# mean exactly one thing: THIS call inserted the row.
#
# Dedup is keyed on the SUBJECT REF, not on `kind` (threads.py: dedup
# applies when subject_ref is present OR the thread is a DM; arch §2.4:
# "subjectless ticket -> many"). Hence: a subjectless TICKET is a new
# thread every time, while a subject-bearing TICKET dedups exactly like
# a DM. Both branches are tested here.
#
# The response shape is frozen (3b): the key rides ONLY on this
# endpoint, and every pre-existing field stays byte-for-byte.
# =============================================================================

from typing import Any
from uuid import UUID, uuid4

from httpx import AsyncClient

from app.core.database import get_session_factory
from tests.helpers import (
    create_recipient,
    create_section,
    next_seam_t2_telegram_id,
)

# The frozen 3b thread shape -- the fields the product proxy already
# consumes. `created` is the ONLY addition this delivery makes.
FROZEN_THREAD_KEYS = {
    "id",
    "client",
    "operator_kind",
    "operator_value",
    "assignee",
    "kind",
    "status",
    "subject_type",
    "subject_id",
    "title",
    "priority",
    "last_message_at",
    "created_at",
}


async def _recipient() -> UUID:
    factory = get_session_factory()
    rid = uuid4()
    async with factory() as s:
        await create_recipient(
            s, recipient_id=rid, telegram_id=next_seam_t2_telegram_id()
        )
        await s.commit()
    return rid


async def _section() -> UUID:
    factory = get_session_factory()
    async with factory() as s:
        section = await create_section(s, key=f"cr-{uuid4().hex[:8]}")
        sid = section.id
        await s.commit()
    return sid


async def _create(api: AsyncClient, body: dict[str, Any]) -> dict[str, Any]:
    """`body` is passed as a dict, not **kwargs: the payload itself has
    a `client` key and would collide with the AsyncClient argument."""
    resp = await api.post("/api/v1/threads", json=body)
    assert resp.status_code == 200
    payload: dict[str, Any] = resp.json()
    return payload


async def _dm_body(client_id: UUID, operator: UUID) -> dict[str, Any]:
    return {
        "client": str(client_id),
        "operator_kind": "user",
        "operator_value": str(operator),
        "kind": "dm",
    }


# ---------------------------------------------------------------------------
# The six dedup cases
# ---------------------------------------------------------------------------


class TestCreatedFlag:
    async def test_first_dm_is_created(self, client: AsyncClient) -> None:
        body = await _dm_body(await _recipient(), await _recipient())
        assert (await _create(client, body))["created"] is True

    async def test_repeat_dm_is_not_created(self, client: AsyncClient) -> None:
        """The dedup hit: same thread back, flag down. This is what
        keeps the product from logging a second "conversation started"
        for the eternal DM."""
        body = await _dm_body(await _recipient(), await _recipient())
        first = await _create(client, body)
        second = await _create(client, body)

        assert first["created"] is True
        assert second["created"] is False
        assert second["id"] == first["id"]

    async def test_first_subject_thread_is_created(
        self, client: AsyncClient
    ) -> None:
        body = {
            "client": str(await _recipient()),
            "operator_kind": "section",
            "operator_value": str(await _section()),
            "kind": "ticket",
            "subject_type": "topic",
            "subject_id": "s-1",
        }
        assert (await _create(client, body))["created"] is True

    async def test_repeat_subject_thread_is_not_created(
        self, client: AsyncClient
    ) -> None:
        """A subject-bearing TICKET dedups exactly like a DM -- the key
        is the subject ref, not the kind."""
        body = {
            "client": str(await _recipient()),
            "operator_kind": "section",
            "operator_value": str(await _section()),
            "kind": "ticket",
            "subject_type": "topic",
            "subject_id": "s-2",
        }
        first = await _create(client, body)
        second = await _create(client, body)

        assert first["created"] is True
        assert second["created"] is False
        assert second["id"] == first["id"]

    async def test_subjectless_ticket_is_created_every_time(
        self, client: AsyncClient
    ) -> None:
        """No dedup key -> a genuinely new row per call, so the flag is
        True twice and the ids DIFFER."""
        body = {
            "client": str(await _recipient()),
            "operator_kind": "section",
            "operator_value": str(await _section()),
            "kind": "ticket",
        }
        first = await _create(client, body)
        second = await _create(client, body)

        assert first["created"] is True
        assert second["created"] is True
        assert first["id"] != second["id"]

    async def test_user_form_subject_thread_dedups_too(
        self, client: AsyncClient
    ) -> None:
        """The operator FORM does not enter the dedup key either: a
        user-form ticket on the same subject is the same thread."""
        body = {
            "client": str(await _recipient()),
            "operator_kind": "user",
            "operator_value": str(await _recipient()),
            "kind": "ticket",
            "subject_type": "topic",
            "subject_id": "s-3",
        }
        first = await _create(client, body)
        second = await _create(client, body)

        assert first["created"] is True
        assert second["created"] is False
        assert second["id"] == first["id"]


# ---------------------------------------------------------------------------
# Shape doubles: additive means ADDITIVE
# ---------------------------------------------------------------------------


class TestResponseShape:
    async def test_create_response_minus_created_is_the_frozen_shape(
        self, client: AsyncClient
    ) -> None:
        client_id = await _recipient()
        operator = await _recipient()
        body = await _dm_body(client_id, operator)
        payload = await _create(client, body)

        assert set(payload) == FROZEN_THREAD_KEYS | {"created"}
        # Field-for-field, the pre-existing part is untouched.
        assert payload["client"] == str(client_id)
        assert payload["operator_kind"] == "user"
        assert payload["operator_value"] == str(operator)
        assert payload["assignee"] == str(operator)  # D1 pre-assign
        assert payload["kind"] == "dm"
        assert payload["status"] == "open"
        assert payload["subject_type"] is None
        assert payload["subject_id"] is None
        assert payload["title"] is None
        assert payload["priority"] is None
        assert payload["last_message_at"] is None
        assert payload["created_at"]

    async def test_no_other_endpoint_gained_the_key(
        self, client: AsyncClient
    ) -> None:
        """The serializer is shared with list / claim / status / retag.
        The key must NOT have leaked into their frozen shapes."""
        client_id = await _recipient()
        operator = await _recipient()
        section = await _section()
        thread_id = (
            await _create(client, await _dm_body(client_id, operator))
        )["id"]

        listed = await client.get(
            "/api/v1/threads", params={"operator": str(operator)}
        )
        assert listed.status_code == 200
        for entry in listed.json()["threads"]:
            assert set(entry) == FROZEN_THREAD_KEYS

        status = await client.post(
            f"/api/v1/threads/{thread_id}/status",
            json={"operator": str(operator), "status": "resolved"},
        )
        assert status.status_code == 200
        assert set(status.json()) == FROZEN_THREAD_KEYS

        # claim + retag on a section thread (a user-form thread is
        # pre-assigned and its axes are frozen).
        ticket_id = (
            await _create(
                client,
                {
                    "client": str(client_id),
                    "operator_kind": "section",
                    "operator_value": str(section),
                    "kind": "ticket",
                },
            )
        )["id"]
        agent = await _recipient()
        claimed = await client.post(
            f"/api/v1/threads/{ticket_id}/claim", json={"operator": str(agent)}
        )
        assert claimed.status_code == 200
        assert set(claimed.json()) == {"claimed", "thread"}
        assert set(claimed.json()["thread"]) == FROZEN_THREAD_KEYS

        retagged = await client.post(
            f"/api/v1/threads/{ticket_id}/retag",
            json={"operator": str(agent), "section": str(await _section())},
        )
        assert retagged.status_code == 200
        assert set(retagged.json()) == FROZEN_THREAD_KEYS
