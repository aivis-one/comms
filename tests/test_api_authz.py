# =============================================================================
# COMMS Service -- write-authz tests (Phase 4c item 4)
# =============================================================================
# Authorization by ROLE IN THE THREAD, not identity:
#   - post: participant (client) or serving operator (assignee);
#   - claim: section threads only (user threads are not claimable);
#   - status/retag: serving operator, or any agent on a section thread
#     (v1 trivial membership); a user thread is served only by its
#     master, so a foreign actor -- including a read-all "supervisor" --
#     is 403. Authz (403) is a distinct layer from domain validation
#     (a frozen user-thread retag by the master still 422s).
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
        section = await create_section(s, key=f"az-{uuid4().hex[:8]}")
        sid = section.id
        await s.commit()
    return sid


async def _dm(client: AsyncClient, client_id: UUID, master: UUID) -> str:
    resp = await client.post(
        "/api/v1/threads",
        json={
            "client": str(client_id), "operator_kind": "user",
            "operator_value": str(master), "kind": "dm",
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["id"]


async def _ticket(client: AsyncClient, client_id: UUID, section: UUID) -> str:
    resp = await client.post(
        "/api/v1/threads",
        json={
            "client": str(client_id), "operator_kind": "section",
            "operator_value": str(section), "kind": "ticket",
            "subject_type": "practice", "subject_id": "p1",
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["id"]


async def _notif_count(target: UUID) -> int:
    factory = get_session_factory()
    async with factory() as s:
        return await s.scalar(
            select(func.count())
            .select_from(Notification)
            .where(Notification.target_value == str(target))
        ) or 0


class TestPostAuthz:
    async def test_participant_may_post(self, client: AsyncClient) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(client_id), "body": "hi"},
        )
        assert resp.status_code == 200

    async def test_serving_operator_may_post(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(master), "body": "hi"},
        )
        assert resp.status_code == 200

    async def test_non_participant_post_is_403(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        stranger = await _recipient()
        resp = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(stranger), "body": "intrude"},
        )
        assert resp.status_code == 403
        # rejected write emits nothing
        assert await _notif_count(client_id) == 0
        assert await _notif_count(master) == 0


class TestClaimAuthz:
    async def test_claim_section_ok(self, client: AsyncClient) -> None:
        client_id, section = await _recipient(), await _section()
        tid = await _ticket(client, client_id, section)
        operator = await _recipient()
        resp = await client.post(
            f"/api/v1/threads/{tid}/claim", json={"operator": str(operator)}
        )
        assert resp.status_code == 200
        assert resp.json()["claimed"] is True

    async def test_claim_user_thread_is_403(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        stranger = await _recipient()
        resp = await client.post(
            f"/api/v1/threads/{tid}/claim", json={"operator": str(stranger)}
        )
        assert resp.status_code == 403  # user thread not claimable


class TestOperateAuthz:
    async def test_serving_operator_may_set_status(
        self, client: AsyncClient
    ) -> None:
        client_id, section = await _recipient(), await _section()
        tid = await _ticket(client, client_id, section)
        operator = await _recipient()
        await client.post(
            f"/api/v1/threads/{tid}/claim", json={"operator": str(operator)}
        )
        resp = await client.post(
            f"/api/v1/threads/{tid}/status",
            json={"operator": str(operator), "status": "closed"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "closed"

    async def test_supervisor_cannot_write_foreign_user_thread(
        self, client: AsyncClient
    ) -> None:
        """Read-all access (a supervisor) does NOT grant write: a user
        thread is served only by its master, so a non-master actor is
        403 on status."""
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        supervisor = await _recipient()
        resp = await client.post(
            f"/api/v1/threads/{tid}/status",
            json={"operator": str(supervisor), "status": "closed"},
        )
        assert resp.status_code == 403

    async def test_foreign_retag_user_thread_is_403(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        stranger = await _recipient()
        new_section = await _section()
        resp = await client.post(
            f"/api/v1/threads/{tid}/retag",
            json={"operator": str(stranger), "section": str(new_section)},
        )
        assert resp.status_code == 403  # authz before the frozen-422

    async def test_master_retag_user_thread_is_422_not_403(
        self, client: AsyncClient
    ) -> None:
        """Authz vs domain validation are different layers: the master
        PASSES authz (serving operator) but a user thread's axes are
        frozen, so retag_thread rejects it with 422."""
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        new_section = await _section()
        resp = await client.post(
            f"/api/v1/threads/{tid}/retag",
            json={"operator": str(master), "section": str(new_section)},
        )
        assert resp.status_code == 422
