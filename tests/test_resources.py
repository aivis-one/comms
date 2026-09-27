# =============================================================================
# COMMS Service -- Resources on the common language (F1.4)
# =============================================================================
# The address book has a version and can forget; a recipient deactivated
# after resolve is not sent to; the calls that create are idempotent
# under a key; every refusal has one body with a class; every listing
# pages one way. The mutation list these tests answer was written
# before them (see the report).
#
# THREE DOUBLE AXES per input -- in each class docstring.
# =============================================================================

import asyncio
import json
from typing import Any
from uuid import UUID, uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import CategoryMute, GroupMembership, Recipient
from app.audience.prefs import set_category_muted
from app.audience.sync import apply_snapshot
from app.core.config import settings
from app.core.database import get_session_factory
from app.core.exceptions import (
    RecipientDeletedError,
    StaleSnapshotError,
    ValidationError,
)
from app.engine.constants import DeliveryStatus, NotificationStatus
from app.engine.models import Notification, NotificationDelivery
from app.engine.service import (
    create_notification,
    deliver_notification,
    resolve_notification,
    rollup_notification,
)
from app.forgetting import forget_recipient
from app.messaging.models import Message, Thread
from app.transport.consumer import StreamConsumer
from app.transport.events import parse_event
from app.transport.handlers import handle_event
from tests.helpers import create_section, intake_fields

_WAIT = 5.0


def _fields(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "telegram_id": None,
        "email": "a@example.test",
        "locale": "en",
        "timezone": "Europe/Berlin",
        "active": True,
    }
    base.update(overrides)
    return base


async def _person(session: AsyncSession, version: int = 1, **fields: Any) -> UUID:
    rid = uuid4()
    await apply_snapshot(
        session, recipient_id=rid, version=version, **_fields(**fields)
    )
    return rid


async def _commit_person(version: int = 1, **fields: Any) -> UUID:
    async with get_session_factory()() as session:
        rid = await _person(session, version, **fields)
        await session.commit()
    return rid


def _put(rid: UUID) -> str:
    return f"/api/v1/recipients/{rid}"


async def _event(session: AsyncSession, name: str, data: dict[str, Any]) -> Any:
    return await handle_event(
        session,
        parse_event(
            {
                "event": name,
                "data": json.dumps({"v": 1, **data}),
            }
        ),
    )


# -----------------------------------------------------------------------------
# Item 1 -- the snapshot version, one rule on both paths
# -----------------------------------------------------------------------------


class TestSnapshotVersion:
    """REPEAT: the equal version with the same / other bytes. EMPTY: no
    version. SHORTFALL: an older version, by either path, in either
    order."""

    async def test_put_then_older_event_does_not_roll_back(
        self,
        client: AsyncClient,
        db_session: AsyncSession,
    ) -> None:
        rid = uuid4()
        assert (
            await client.put(
                _put(rid),
                json={
                    "version": 2,
                    **_fields(locale="de"),
                },
            )
        ).status_code == 200
        with pytest.raises(StaleSnapshotError):
            await _event(
                db_session,
                "user_upserted",
                {
                    "recipient_id": str(rid),
                    "version": 1,
                    **_fields(locale="en"),
                },
            )
        await db_session.rollback()
        row = await db_session.get(Recipient, rid)
        assert row is not None and (row.locale, row.version) == ("de", 2)

    async def test_event_then_older_put_is_409(
        self,
        client: AsyncClient,
        db_session: AsyncSession,
    ) -> None:
        rid = uuid4()
        await _event(
            db_session,
            "user_upserted",
            {
                "recipient_id": str(rid),
                "version": 5,
                **_fields(locale="de"),
            },
        )
        await db_session.commit()
        response = await client.put(_put(rid), json={"version": 4, **_fields()})
        assert response.status_code == 409
        assert response.json()["error"]["class"] == "stale_snapshot"
        db_session.expire_all()
        row = await db_session.get(Recipient, rid)
        assert row is not None and row.locale == "de"

    async def test_equal_version_same_bytes_is_a_replay(
        self, db_session: AsyncSession,
    ) -> None:
        """Nothing is WRITTEN: the row, read back from the database after
        a flush, is the stored one field for field and was not touched
        (updated_at). An in-memory check alone would miss a replay that
        modifies the row -- the mutation list caught exactly that."""
        rid = await _person(db_session, 3)
        await db_session.commit()
        before = dict((await db_session.execute(
            text("SELECT * FROM recipients WHERE id = :id"), {"id": rid}
        )).mappings().one())
        await apply_snapshot(db_session, recipient_id=rid, version=3, **_fields())
        await db_session.commit()
        after = dict((await db_session.execute(
            text("SELECT * FROM recipients WHERE id = :id"), {"id": rid}
        )).mappings().one())
        assert after == before

    async def test_equal_version_other_bytes_is_a_conflict(
        self,
        client: AsyncClient,
    ) -> None:
        rid = await _commit_person(3)
        response = await client.put(
            _put(rid),
            json={
                "version": 3,
                **_fields(locale="fr"),
            },
        )
        assert response.status_code == 409
        assert response.json()["error"]["class"] == "conflict"

    async def test_newer_version_applies(self, client: AsyncClient) -> None:
        rid = await _commit_person(3)
        response = await client.put(
            _put(rid),
            json={
                "version": 4,
                **_fields(locale="fr"),
            },
        )
        assert response.status_code == 200
        assert (response.json()["locale"], response.json()["version"]) == ("fr", 4)

    @pytest.mark.parametrize("version", [0, -1, True, "2", 2.0, None])
    async def test_version_forms_are_refused(
        self,
        client: AsyncClient,
        version: Any,
    ) -> None:
        response = await client.put(
            _put(uuid4()),
            json={
                "version": version,
                **_fields(),
            },
        )
        assert response.status_code == 422
        assert response.json()["error"]["class"] == "validation"

    async def test_no_version_is_refused(self, client: AsyncClient) -> None:
        response = await client.put(_put(uuid4()), json=_fields())
        assert response.status_code == 422
        assert response.json()["error"]["fields"][0]["loc"][-1] == "version"

    async def test_the_rule_refuses_version_zero_itself(
        self,
        db_session: AsyncSession,
    ) -> None:
        with pytest.raises(ValidationError, match="version"):
            await apply_snapshot(
                db_session,
                recipient_id=uuid4(),
                version=0,
                **_fields(),
            )


class TestExplicitNull:
    """ "No value" is null on both paths; a blank or a zero is refused.
    EMPTY: null passes (the twin). SHORTFALL: "", "  ", 0."""

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("locale", ""),
            ("locale", "  "),
            ("email", ""),
            ("email", " \t"),
            ("timezone", ""),
            ("telegram_id", 0),
        ],
    )
    async def test_refused_on_the_put(
        self,
        client: AsyncClient,
        field: str,
        value: Any,
    ) -> None:
        response = await client.put(
            _put(uuid4()),
            json={
                "version": 1,
                **_fields(**{field: value}),
            },
        )
        assert response.status_code == 422
        assert "send null" in response.json()["error"]["message"]

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("locale", ""),
            ("email", ""),
            ("timezone", " "),
            ("telegram_id", 0),
        ],
    )
    async def test_refused_on_the_event(
        self,
        db_session: AsyncSession,
        field: str,
        value: Any,
    ) -> None:
        with pytest.raises(ValidationError, match="send null"):
            await _event(
                db_session,
                "user_upserted",
                {
                    "recipient_id": str(uuid4()),
                    "version": 1,
                    **_fields(**{field: value}),
                },
            )

    async def test_null_passes_on_both_paths(
        self,
        client: AsyncClient,
        db_session: AsyncSession,
    ) -> None:
        nulls = _fields(email=None, locale=None, timezone=None)
        assert (
            await client.put(
                _put(uuid4()),
                json={
                    "version": 1,
                    **nulls,
                },
            )
        ).status_code == 200
        await _event(
            db_session,
            "user_upserted",
            {
                "recipient_id": str(uuid4()),
                "version": 1,
                **nulls,
            },
        )

    async def test_the_database_holds_the_rule_too(
        self,
        db_session: AsyncSession,
    ) -> None:
        rid = await _person(db_session)
        await db_session.commit()
        for column, bad in (
            ("locale", "''"),
            ("locale", "' '"),
            ("email", "''"),
            ("timezone", "''"),
            ("telegram_id", "0"),
        ):
            with pytest.raises(IntegrityError, match=f"ck_recipients_{column}"):
                await db_session.execute(
                    text(f"UPDATE recipients SET {column} = {bad} WHERE id = :id"),
                    {"id": rid},
                )
            await db_session.rollback()
        # The twin: NULL is what "no value" is, and passes every CHECK.
        await db_session.execute(
            text(
                "UPDATE recipients SET locale = NULL, email = NULL, "
                "timezone = NULL, telegram_id = NULL WHERE id = :id"
            ),
            {"id": rid},
        )


# -----------------------------------------------------------------------------
# Item 2 -- forgetting
# -----------------------------------------------------------------------------


class TestForgetting:
    """REPEAT: a second deletion. EMPTY: an id comms never heard of.
    SHORTFALL: a deletion older than the snapshot; a snapshot after."""

    async def test_everything_that_reaches_the_person_is_gone(
        self,
        client: AsyncClient,
        db_session: AsyncSession,
    ) -> None:
        rid = await _person(db_session, telegram_id=91234, timezone="Asia/Tokyo")
        db_session.add(GroupMembership(group_key="g-forget", recipient_id=rid))
        await set_category_muted(db_session, rid, "unit_updates", True)
        await db_session.commit()

        response = await client.request("DELETE", _put(rid), json={"version": 2})
        assert response.status_code == 200
        assert response.json()["deleted"] is True
        db_session.expire_all()
        row = await db_session.get(Recipient, rid)
        assert row is not None
        assert (
            row.telegram_id,
            row.email,
            row.locale,
            row.timezone,
            row.allowed_windows,
            row.active,
        ) == (None,) * 5 + (False,)
        assert row.deleted_at is not None
        for model in (GroupMembership, CategoryMute):
            count = await db_session.scalar(
                select(func.count())
                .select_from(model)
                .where(
                    model.recipient_id == rid,
                )
            )
            assert count == 0, model

    async def test_waiting_deliveries_are_closed_and_error_text_cleared(
        self,
        db_session: AsyncSession,
    ) -> None:
        stayer = await _person(db_session)
        leaver = await _person(db_session)
        job = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event_in_app",
            title="T",
            body="B",
            target_type="all",
            target_value="*",
        )
        deliveries = await resolve_notification(db_session, job)
        mine = [d for d in deliveries if d.recipient_id == leaver]
        other = [d for d in deliveries if d.recipient_id == stayer]
        mine[0].error_message = "mailbox a@example.test is full"
        other[0].status = DeliveryStatus.SENT
        await db_session.flush()

        await forget_recipient(db_session, recipient_id=leaver, version=2)
        await db_session.refresh(mine[0])
        assert mine[0].status == DeliveryStatus.RECIPIENT_INACTIVE
        assert mine[0].error_message is None
        await db_session.refresh(job)
        assert job.status == NotificationStatus.SENT

    async def test_an_unknown_id_becomes_a_tombstone(
        self,
        client: AsyncClient,
    ) -> None:
        rid = uuid4()
        response = await client.request("DELETE", _put(rid), json={"version": 7})
        assert response.status_code == 200
        late = await client.put(_put(rid), json={"version": 3, **_fields()})
        assert late.status_code == 409
        assert late.json()["error"]["class"] == "recipient_deleted"

    async def test_the_tombstone_is_terminal_at_any_version(
        self,
        client: AsyncClient,
        db_session: AsyncSession,
    ) -> None:
        rid = await _commit_person(1)
        await client.request("DELETE", _put(rid), json={"version": 2})
        newer = await client.put(_put(rid), json={"version": 99, **_fields()})
        assert newer.json()["error"]["class"] == "recipient_deleted"
        with pytest.raises(RecipientDeletedError):
            await _event(
                db_session,
                "user_upserted",
                {
                    "recipient_id": str(rid),
                    "version": 100,
                    **_fields(),
                },
            )

    async def test_a_second_deletion_is_not_an_error(
        self,
        client: AsyncClient,
    ) -> None:
        rid = await _commit_person(1)
        for _ in range(2):
            response = await client.request(
                "DELETE",
                _put(rid),
                json={"version": 2},
            )
            assert response.status_code == 200

    async def test_an_older_deletion_is_stale_an_equal_one_a_conflict(
        self,
        client: AsyncClient,
    ) -> None:
        rid = await _commit_person(5)
        older = await client.request("DELETE", _put(rid), json={"version": 4})
        assert older.json()["error"]["class"] == "stale_snapshot"
        equal = await client.request("DELETE", _put(rid), json={"version": 5})
        assert equal.json()["error"]["class"] == "conflict"

    async def test_nothing_re_attaches_to_a_tombstone(
        self,
        client: AsyncClient,
        db_session: AsyncSession,
    ) -> None:
        """A late membership, a thread, a message from the forgotten."""
        other = await _commit_person()
        rid = await _commit_person()
        section = await create_section(db_session, key=f"f-{uuid4().hex[:8]}")
        await db_session.commit()
        thread = (
            await client.post(
                "/api/v1/threads",
                json={
                    "client": str(rid),
                    "operator_kind": "section",
                    "operator_value": str(section.id),
                    "kind": "ticket",
                },
            )
        ).json()
        await client.request("DELETE", _put(rid), json={"version": 2})
        with pytest.raises(RecipientDeletedError):
            await _event(
                db_session,
                "group_changed",
                {
                    "group_key": "g",
                    "recipient_id": str(rid),
                    "member": True,
                },
            )
        await db_session.rollback()
        posted = await client.post(
            f"/api/v1/threads/{thread['id']}/messages",
            json={"sender": str(rid), "body": "hi"},
        )
        assert posted.json()["error"]["class"] == "recipient_deleted"
        created = await client.post(
            "/api/v1/threads",
            json={
                "client": str(rid),
                "operator_kind": "user",
                "operator_value": str(other),
                "kind": "dm",
            },
        )
        assert created.json()["error"]["class"] == "recipient_deleted"

    async def test_the_database_holds_the_tombstone(
        self,
        db_session: AsyncSession,
    ) -> None:
        rid = await _person(db_session)
        await forget_recipient(db_session, recipient_id=rid, version=2)
        await db_session.commit()
        with pytest.raises(IntegrityError, match="ck_recipients_tombstone"):
            await db_session.execute(
                text(
                    "UPDATE recipients SET email = 'back@example.test' WHERE id = :id"
                ),
                {"id": rid},
            )
        await db_session.rollback()


# -----------------------------------------------------------------------------
# Item 3 -- active is re-checked before the send
# -----------------------------------------------------------------------------


class TestLateActivity:
    """REPEAT: re-activated before the send -> delivered. EMPTY: --.
    SHORTFALL: deactivated between resolve and send."""

    async def _resolved(
        self, session: AsyncSession, people: int
    ) -> tuple[Notification, list[NotificationDelivery]]:
        for _ in range(people):
            await _person(session)
        job = await create_notification(
            session,
            **intake_fields(),
            type="unit_event_in_app",
            title="T",
            body="B",
            target_type="all",
            target_value="*",
        )
        return job, await resolve_notification(session, job)

    async def test_deactivated_after_resolve_is_not_sent_to(
        self,
        db_session: AsyncSession,
    ) -> None:
        job, (delivery,) = await self._resolved(db_session, 1)
        recipient = await db_session.get(Recipient, delivery.recipient_id)
        assert recipient is not None
        recipient.active = False
        await db_session.flush()
        await deliver_notification(db_session, job)
        assert delivery.status == DeliveryStatus.RECIPIENT_INACTIVE
        assert delivery.status not in (DeliveryStatus.SUPPRESSED, DeliveryStatus.FAILED)
        await rollup_notification(db_session, job)
        assert job.status == NotificationStatus.NO_RECIPIENTS

    async def test_re_activated_before_the_send_is_delivered(
        self,
        db_session: AsyncSession,
    ) -> None:
        job, (delivery,) = await self._resolved(db_session, 1)
        await deliver_notification(db_session, job)
        assert delivery.status == DeliveryStatus.SENT

    async def test_inactive_and_muted_together_fold_to_suppressed(
        self,
        db_session: AsyncSession,
    ) -> None:
        job, deliveries = await self._resolved(db_session, 2)
        deliveries[0].status = DeliveryStatus.RECIPIENT_INACTIVE
        deliveries[1].status = DeliveryStatus.SUPPRESSED
        await db_session.flush()
        await rollup_notification(db_session, job)
        assert job.status == NotificationStatus.SUPPRESSED


# -----------------------------------------------------------------------------
# Item 4 -- idempotency of the calls that create
# -----------------------------------------------------------------------------


class TestResourceKeys:
    """REPEAT: the same key with the same / other request; the same key
    into another thread. EMPTY: no key, a blank key. SHORTFALL: a key
    longer than its column."""

    async def _thread(self, bare_client: AsyncClient, key: str) -> Any:
        client_id = await _commit_person()
        async with get_session_factory()() as s:
            section = await create_section(s, key=f"k-{uuid4().hex[:8]}")
            await s.commit()
        return await bare_client.post(
            "/api/v1/threads",
            headers={
                "Idempotency-Key": key,
            },
            json={
                "client": str(client_id),
                "operator_kind": "section",
                "operator_value": str(section.id),
                "kind": "ticket",
            },
        ), client_id

    async def test_a_repeated_message_is_one_message_and_one_ping(
        self,
        bare_client: AsyncClient,
        db_session: AsyncSession,
    ) -> None:
        thread, client_id = await self._thread(bare_client, "t-1")
        tid = thread.json()["id"]
        url = f"/api/v1/threads/{tid}/messages"
        body = {"sender": str(client_id), "body": "ok"}
        first = await bare_client.post(
            url, headers={"Idempotency-Key": "m-1"}, json=body
        )
        second = await bare_client.post(
            url, headers={"Idempotency-Key": "m-1"}, json=body
        )
        assert first.status_code == second.status_code == 200
        assert first.json()["id"] == second.json()["id"]
        messages = await db_session.scalar(
            select(func.count())
            .select_from(Message)
            .where(Message.thread_id == UUID(tid))
        )
        assert messages == 1
        pings = await db_session.scalar(
            select(func.count())
            .select_from(Notification)
            .where(
                Notification.idempotency_key.like(f"msg:{first.json()['id']}:%"),
            )
        )
        assert pings <= 1

    async def test_the_same_key_into_another_thread_is_a_conflict(
        self,
        bare_client: AsyncClient,
    ) -> None:
        """Amendment 1: the fingerprint covers the path, not the body alone."""
        a, ca = await self._thread(bare_client, "t-a")
        b, _ = await self._thread(bare_client, "t-b")
        await bare_client.post(
            f"/api/v1/threads/{a.json()['id']}/messages",
            headers={"Idempotency-Key": "same"},
            json={"sender": str(ca), "body": "ok"},
        )
        other = await bare_client.post(
            f"/api/v1/threads/{b.json()['id']}/messages",
            headers={"Idempotency-Key": "same"},
            json={"sender": str(ca), "body": "ok"},
        )
        assert other.status_code == 409
        assert other.json()["error"]["class"] == "conflict"

    async def test_the_same_key_other_body_is_a_conflict(
        self,
        bare_client: AsyncClient,
    ) -> None:
        thread, client_id = await self._thread(bare_client, "t-2")
        url = f"/api/v1/threads/{thread.json()['id']}/messages"
        await bare_client.post(
            url,
            headers={"Idempotency-Key": "m-2"},
            json={"sender": str(client_id), "body": "a"},
        )
        other = await bare_client.post(
            url,
            headers={"Idempotency-Key": "m-2"},
            json={"sender": str(client_id), "body": "b"},
        )
        assert other.status_code == 409

    @pytest.mark.parametrize(
        "headers", [{}, {"Idempotency-Key": " "}, {"Idempotency-Key": "k" * 201}]
    )
    async def test_a_missing_or_bad_key_is_refused(
        self,
        bare_client: AsyncClient,
        headers: dict[str, str],
    ) -> None:
        response = await bare_client.post(
            "/api/v1/threads",
            headers=headers,
            json={
                "client": str(uuid4()),
                "operator_kind": "section",
                "operator_value": str(uuid4()),
                "kind": "ticket",
            },
        )
        assert response.status_code == 422
        assert "Idempotency-Key" in response.json()["error"]["message"]

    async def test_a_repeated_ticket_is_one_thread(
        self,
        bare_client: AsyncClient,
        db_session: AsyncSession,
    ) -> None:
        client_id = await _commit_person()
        section = await create_section(db_session, key=f"k-{uuid4().hex[:8]}")
        await db_session.commit()
        body = {
            "client": str(client_id),
            "operator_kind": "section",
            "operator_value": str(section.id),
            "kind": "ticket",
        }
        first = await bare_client.post(
            "/api/v1/threads", headers={"Idempotency-Key": "tk"}, json=body
        )
        second = await bare_client.post(
            "/api/v1/threads", headers={"Idempotency-Key": "tk"}, json=body
        )
        assert first.json()["id"] == second.json()["id"]
        assert second.json()["created"] is True
        count = await db_session.scalar(
            select(func.count()).select_from(Thread).where(Thread.client == client_id)
        )
        assert count == 1

    async def test_a_repeated_claim_is_still_yours(
        self,
        client: AsyncClient,
    ) -> None:
        client_id = await _commit_person()
        operator = await _commit_person()
        async with get_session_factory()() as s:
            section = await create_section(s, key=f"c-{uuid4().hex[:8]}")
            await s.commit()
        tid = (
            await client.post(
                "/api/v1/threads",
                json={
                    "client": str(client_id),
                    "operator_kind": "section",
                    "operator_value": str(section.id),
                    "kind": "ticket",
                },
            )
        ).json()["id"]
        for _ in range(2):
            response = await client.post(
                f"/api/v1/threads/{tid}/claim",
                json={"operator": str(operator)},
            )
            assert response.status_code == 200
            assert response.json()["claimed"] is True


# -----------------------------------------------------------------------------
# Items 5, 6 -- one error body, one way to page
# -----------------------------------------------------------------------------


class TestOneErrorBody:
    def _assert_form(self, response: Any, status: int, error_class: str) -> None:
        assert response.status_code == status
        body = response.json()
        assert set(body) == {"error"}
        assert body["error"]["class"] == error_class
        assert body["error"]["message"]

    async def test_every_source_has_the_one_form(
        self,
        client: AsyncClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._assert_form(await client.get("/api/v1/nowhere"), 404, "not_found")
        self._assert_form(
            await client.patch(f"/api/v1/threads/{uuid4()}/messages"),
            405,
            "method_not_allowed",
        )
        self._assert_form(
            await client.post(
                f"/api/v1/threads/{uuid4()}/messages",
                json={"sender": str(uuid4()), "body": "x"},
            ),
            404,
            "not_found",
        )
        invalid = await client.put(_put(uuid4()), json={"version": 1})
        self._assert_form(invalid, 422, "validation")
        assert invalid.json()["error"]["fields"]
        monkeypatch.setattr(settings, "comms_service_token", "secret-token-x")
        unauthorized = await client.get(f"/api/v1/threads?operator={uuid4()}")
        self._assert_form(unauthorized, 401, "unauthorized")
        assert "secret-token-x" not in unauthorized.text

    @pytest.mark.parametrize(
        "path",
        [
            "/api/v1/recipients/{rid}/inbox",
            "/api/v1/threads?operator={rid}",
            "/api/v1/threads/{tid}/messages",
        ],
    )
    async def test_every_listing_pages_one_way(
        self,
        client: AsyncClient,
        path: str,
    ) -> None:
        rid = await _commit_person()
        async with get_session_factory()() as s:
            section = await create_section(s, key=f"p-{uuid4().hex[:8]}")
            await s.commit()
        tid = (
            await client.post(
                "/api/v1/threads",
                json={
                    "client": str(rid),
                    "operator_kind": "section",
                    "operator_value": str(section.id),
                    "kind": "ticket",
                },
            )
        ).json()["id"]
        url = path.format(rid=rid, tid=tid)
        sep = "&" if "?" in url else "?"
        ok = await client.get(url)
        assert ok.status_code == 200
        assert {"items", "next_cursor"} <= set(ok.json())
        for bad in ("limit=0", "limit=101", "cursor=%%%"):
            refused = await client.get(f"{url}{sep}{bad}")
            assert refused.status_code == 422, bad
            assert refused.json()["error"]["class"] == "validation"


# -----------------------------------------------------------------------------
# The stream: a stale snapshot is acknowledged, its class in the log
# -----------------------------------------------------------------------------


async def test_a_stale_event_is_acked_with_its_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Amendment 3: no record under a key exists for a sync event -- the
    log's refusal_class is the one programmatic trace; no dead letter."""
    rid = await _commit_person(5)
    redis = fakeaioredis.FakeRedis()
    stream = f"comms:test:{uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", stream)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    await redis.xadd(
        stream,
        {
            "event": "user_upserted",
            "data": json.dumps(
                {
                    "v": 1,
                    "recipient_id": str(rid),
                    "version": 4,
                    **_fields(),
                }
            ),
        },
    )
    with capture_logs() as logs:
        task = asyncio.ensure_future(StreamConsumer(redis).run())
        try:
            deadline = asyncio.get_event_loop().time() + _WAIT
            while not any(e["event"] == "event_refused" for e in logs):
                assert asyncio.get_event_loop().time() < deadline
                await asyncio.sleep(0.02)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    (entry,) = [e for e in logs if e["event"] == "event_refused"]
    assert entry["refusal_class"] == "stale_snapshot"
    assert await redis.xlen(settings.dlq_stream) == 0
