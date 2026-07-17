# =============================================================================
# COMMS Service -- messaging API tests (Phase 4c item 3)
# =============================================================================
# End-to-end through the ASGI transport (auth is no-op in stub mode).
# Actor ids (client/sender/operator/participant) are seeded recipients
# because the domain FKs are real; the API itself trusts them.
# =============================================================================

from uuid import UUID, uuid4

from httpx import AsyncClient
from sqlalchemy import func, select

from app.core.database import get_session_factory
from app.engine.models import Notification
from tests.helpers import (
    create_recipient,
    create_section,
    next_phase4c_telegram_id,
)


async def _recipient() -> UUID:
    factory = get_session_factory()
    rid = uuid4()
    async with factory() as s:
        await create_recipient(
            s, recipient_id=rid, telegram_id=next_phase4c_telegram_id()
        )
        await s.commit()
    return rid


async def _section() -> UUID:
    factory = get_session_factory()
    async with factory() as s:
        section = await create_section(s, key=f"api-{uuid4().hex[:8]}")
        sid = section.id
        await s.commit()
    return sid


async def _notif_count(target: UUID) -> int:
    factory = get_session_factory()
    async with factory() as s:
        return await s.scalar(
            select(func.count())
            .select_from(Notification)
            .where(Notification.target_value == str(target))
        ) or 0


async def _dm(client: AsyncClient, client_id: UUID, master: UUID) -> str:
    resp = await client.post(
        "/api/v1/threads",
        json={
            "client": str(client_id),
            "operator_kind": "user",
            "operator_value": str(master),
            "kind": "dm",
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["id"]


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
    return resp.json()["id"]


class TestThreadCreate:
    async def test_create_dm_preassigns_master(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        resp = await client.post(
            "/api/v1/threads",
            json={
                "client": str(client_id), "operator_kind": "user",
                "operator_value": str(master), "kind": "dm",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["client"] == str(client_id)
        assert body["assignee"] == str(master)  # D1 pre-assign

    async def test_create_is_idempotent(self, client: AsyncClient) -> None:
        client_id, master = await _recipient(), await _recipient()
        first = await _dm(client, client_id, master)
        second = await _dm(client, client_id, master)
        assert first == second  # dedup -> same thread


class TestPostMessage:
    async def test_post_message_pings_other_side(
        self, client: AsyncClient
    ) -> None:
        """Fork 3: message + support ping commit in one transaction."""
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(client_id), "body": "hello"},
        )
        assert resp.status_code == 200
        assert resp.json()["body"] == "hello"
        assert await _notif_count(master) == 1  # persisted alongside
        assert await _notif_count(client_id) == 0  # sender not pinged

    async def test_post_to_absent_thread_404(
        self, client: AsyncClient
    ) -> None:
        sender = await _recipient()
        resp = await client.post(
            f"/api/v1/threads/{uuid4()}/messages",
            json={"sender": str(sender), "body": "x"},
        )
        assert resp.status_code == 404


class TestThreadFeed:
    async def test_feed_paginates_newest_first(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        for i in range(3):
            await client.post(
                f"/api/v1/threads/{tid}/messages",
                json={"sender": str(client_id), "body": f"m{i}"},
            )
        page1 = await client.get(
            f"/api/v1/threads/{tid}/messages", params={"limit": 2}
        )
        assert page1.status_code == 200
        body1 = page1.json()
        assert [m["body"] for m in body1["messages"]] == ["m2", "m1"]
        assert body1["next_cursor"] is not None
        page2 = await client.get(
            f"/api/v1/threads/{tid}/messages",
            params={"limit": 2, "cursor": body1["next_cursor"]},
        )
        body2 = page2.json()
        assert [m["body"] for m in body2["messages"]] == ["m0"]
        assert body2["next_cursor"] is None

    async def test_malformed_cursor_is_422(
        self, client: AsyncClient
    ) -> None:
        resp = await client.get(
            f"/api/v1/threads/{uuid4()}/messages",
            params={"cursor": "!!!not-base64!!!"},
        )
        assert resp.status_code == 422


class TestReadState:
    async def test_read_pointer_clears_unread(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        for i in range(2):
            await client.post(
                f"/api/v1/threads/{tid}/messages",
                json={"sender": str(client_id), "body": f"m{i}"},
            )
        before = await client.get(
            f"/api/v1/threads/{tid}/unread-count",
            params={"participant": str(master)},
        )
        assert before.json()["unread"] == 2
        marked = await client.post(
            f"/api/v1/threads/{tid}/read",
            json={"participant": str(master)},
        )
        assert marked.status_code == 200
        assert marked.json()["unread"] == 0


class TestOperatorVerbs:
    async def test_claim_then_second_claim_loses(
        self, client: AsyncClient
    ) -> None:
        client_id, section = await _recipient(), await _section()
        tid = await _ticket(client, client_id, section)
        op1, op2 = await _recipient(), await _recipient()
        first = await client.post(
            f"/api/v1/threads/{tid}/claim", json={"operator": str(op1)}
        )
        assert first.status_code == 200
        assert first.json()["claimed"] is True
        assert first.json()["thread"]["assignee"] == str(op1)
        second = await client.post(
            f"/api/v1/threads/{tid}/claim", json={"operator": str(op2)}
        )
        assert second.json()["claimed"] is False

    async def test_set_status_closes(self, client: AsyncClient) -> None:
        client_id, section = await _recipient(), await _section()
        tid = await _ticket(client, client_id, section)
        operator = await _recipient()
        resp = await client.post(
            f"/api/v1/threads/{tid}/status",
            json={"operator": str(operator), "status": "closed"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "closed"

    async def test_set_status_invalid_transition_422(
        self, client: AsyncClient
    ) -> None:
        client_id, section = await _recipient(), await _section()
        tid = await _ticket(client, client_id, section)
        operator = await _recipient()
        # closed -> pending is not a legal manual transition
        await client.post(
            f"/api/v1/threads/{tid}/status",
            json={"operator": str(operator), "status": "closed"},
        )
        resp = await client.post(
            f"/api/v1/threads/{tid}/status",
            json={"operator": str(operator), "status": "pending"},
        )
        assert resp.status_code == 422

    async def test_retag_moves_section_and_unassigns(
        self, client: AsyncClient
    ) -> None:
        client_id, section = await _recipient(), await _section()
        tid = await _ticket(client, client_id, section)
        op = await _recipient()
        await client.post(
            f"/api/v1/threads/{tid}/claim", json={"operator": str(op)}
        )
        new_section = await _section()
        resp = await client.post(
            f"/api/v1/threads/{tid}/retag",
            json={"operator": str(op), "section": str(new_section)},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["operator_value"] == str(new_section)
        assert body["assignee"] is None  # retag returns it to the pool


class TestListVisible:
    async def test_operator_sees_claimed_threads(
        self, client: AsyncClient
    ) -> None:
        client_id = await _recipient()
        operator = await _recipient()
        ids = set()
        for _ in range(2):
            section = await _section()
            tid = await _ticket(client, client_id, section)
            await client.post(
                f"/api/v1/threads/{tid}/claim",
                json={"operator": str(operator)},
            )
            ids.add(tid)
        resp = await client.get(
            "/api/v1/threads",
            params={"operator": str(operator), "limit": 50},
        )
        assert resp.status_code == 200
        seen = {t["id"] for t in resp.json()["threads"]}
        assert ids <= seen


class TestReferentValidation:
    """4c.1-B: a bad trusted-actor FK id is a clean 404, not a 500."""

    async def test_create_unknown_client_is_404(
        self, client: AsyncClient
    ) -> None:
        master = await _recipient()  # valid operator referent
        resp = await client.post(
            "/api/v1/threads",
            json={
                "client": str(uuid4()), "operator_kind": "user",
                "operator_value": str(master), "kind": "dm",
            },
        )
        assert resp.status_code == 404

    async def test_claim_unknown_operator_is_404(
        self, client: AsyncClient
    ) -> None:
        client_id, section = await _recipient(), await _section()
        tid = await _ticket(client, client_id, section)
        resp = await client.post(
            f"/api/v1/threads/{tid}/claim", json={"operator": str(uuid4())}
        )
        assert resp.status_code == 404


class TestReadClamp:
    """4c.1-D: a future last_read_at is clamped to now, so it cannot
    pre-clear the participant's own badge for messages not yet seen."""

    async def test_future_last_read_at_is_clamped(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(client_id), "body": "m0"},
        )
        marked = await client.post(
            f"/api/v1/threads/{tid}/read",
            json={
                "participant": str(master),
                "last_read_at": "2099-01-01T00:00:00Z",
            },
        )
        assert marked.status_code == 200
        assert marked.json()["unread"] == 0  # m0 read at (clamped) now
        # a message posted AFTER the clamped pointer is still unread;
        # an unclamped 2099 pointer would have hidden it.
        await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(client_id), "body": "m1"},
        )
        after = await client.get(
            f"/api/v1/threads/{tid}/unread-count",
            params={"participant": str(master)},
        )
        assert after.json()["unread"] == 1
