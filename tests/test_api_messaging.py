# =============================================================================
# COMMS Service -- messaging API tests (Phase 4c item 3)
# =============================================================================
# End-to-end through the ASGI transport (auth is off in the suite --
# the autouse api_auth_disabled fixture in conftest).
# Actor ids (client/sender/operator/participant) are seeded recipients
# because the domain FKs are real; the API itself trusts them.
# =============================================================================

from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError
from structlog.testing import capture_logs

from app.core.database import get_session_factory
from app.engine.models import Notification
from app.messaging.constants import (
    MAX_MESSAGE_BODY_LEN,
    MAX_SUBJECT_ID_LEN,
    MAX_SUBJECT_TYPE_LEN,
    MAX_THREAD_PRIORITY,
    MAX_THREAD_TITLE_LEN,
    MIN_THREAD_PRIORITY,
)
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


# ---------------------------------------------------------------------------
# R-2: what the service says on ordinary input at the edges
# ---------------------------------------------------------------------------


class TestInputBounds:
    """Every bounded body field, one past the bound and exactly at it.

    THE PAIR IS THE POINT. "Too long is a 422" passes on a model that
    refuses everything; "exactly the column width is accepted" is what
    says the bound sits where the column sits. Before R-2 each of these
    values passed the model and died on the INSERT, and the caller got
    a 500 -- the service reporting its own failure for input only the
    caller could fix.
    """

    async def test_body_at_the_column_width_is_accepted(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(client_id), "body": "x" * MAX_MESSAGE_BODY_LEN},
        )
        assert resp.status_code == 200, resp.text
        assert len(resp.json()["body"]) == MAX_MESSAGE_BODY_LEN

    async def test_body_one_past_the_width_is_422(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={
                "sender": str(client_id),
                "body": "x" * (MAX_MESSAGE_BODY_LEN + 1),
            },
        )
        assert resp.status_code == 422, resp.text

    async def test_title_at_and_past_the_width(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        base = {
            "client": str(client_id), "operator_kind": "user",
            "operator_value": str(master), "kind": "ticket",
        }
        ok = await client.post(
            "/api/v1/threads",
            json={**base, "title": "t" * MAX_THREAD_TITLE_LEN},
        )
        assert ok.status_code == 200, ok.text
        over = await client.post(
            "/api/v1/threads",
            json={**base, "title": "t" * (MAX_THREAD_TITLE_LEN + 1)},
        )
        assert over.status_code == 422, over.text

    @pytest.mark.parametrize(
        ("field", "limit"),
        [
            ("subject_type", MAX_SUBJECT_TYPE_LEN),
            ("subject_id", MAX_SUBJECT_ID_LEN),
        ],
    )
    async def test_subject_ref_widths(
        self, client: AsyncClient, field: str, limit: int
    ) -> None:
        """Both halves are sent every time: a half-populated subject_ref
        is refused by a CHECK constraint, so a test that sent one would
        be measuring the wrong refusal."""
        client_id, master = await _recipient(), await _recipient()
        subject = {"subject_type": "practice", "subject_id": "s-1"}
        base = {
            "client": str(client_id), "operator_kind": "user",
            "operator_value": str(master), "kind": "dm",
        }
        ok = await client.post(
            "/api/v1/threads",
            json={**base, **subject, field: "s" * limit},
        )
        assert ok.status_code == 200, ok.text
        over = await client.post(
            "/api/v1/threads",
            json={**base, **subject, field: "s" * (limit + 1)},
        )
        assert over.status_code == 422, over.text

    async def test_priority_at_and_past_the_integer_range(
        self, client: AsyncClient
    ) -> None:
        """The bound is the column's range, not a scale of ours: comms
        does not own the meaning of a product's priority."""
        client_id, master = await _recipient(), await _recipient()
        base = {
            "client": str(client_id), "operator_kind": "user",
            "operator_value": str(master), "kind": "ticket",
        }
        ok = await client.post(
            "/api/v1/threads",
            json={**base, "priority": MAX_THREAD_PRIORITY},
        )
        assert ok.status_code == 200, ok.text
        for bad in (MAX_THREAD_PRIORITY + 1, MIN_THREAD_PRIORITY - 1):
            over = await client.post(
                "/api/v1/threads", json={**base, "priority": bad},
            )
            assert over.status_code == 422, over.text


class TestEmptyMessageBody:
    """A message nobody can read is not created (R-2 item 3)."""

    @pytest.mark.parametrize("body", ["", " ", "   \t\n  "])
    async def test_empty_and_whitespace_bodies_are_422(
        self, client: AsyncClient, body: str
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(client_id), "body": body},
        )
        assert resp.status_code == 422, resp.text

    async def test_surrounding_whitespace_is_kept_not_trimmed(
        self, client: AsyncClient
    ) -> None:
        """The pair to the refusals above, and a decision in its own
        right: the gate rejects a body with nothing IN it, it does not
        edit a body that has something. Trimming would quietly rewrite
        a person's message -- an indented snippet is meant.
        """
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(client_id), "body": "   indented\n"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["body"] == "   indented\n"


class TestUnknownFieldsAreRefused:
    """A misspelled field used to be dropped in silence and answered
    with 200: the product believed it had sent a value that went
    nowhere. The same reason already written into RecipientSnapshot.
    """

    async def test_unknown_field_on_a_message_is_422(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={
                "sender": str(client_id), "body": "hi", "bodyy": "typo",
            },
        )
        assert resp.status_code == 422, resp.text

    async def test_unknown_field_on_a_thread_is_422(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        resp = await client.post(
            "/api/v1/threads",
            json={
                "client": str(client_id), "operator_kind": "user",
                "operator_value": str(master), "kind": "dm",
                "titel": "typo",
            },
        )
        assert resp.status_code == 422, resp.text

    async def test_unknown_field_on_a_read_is_422(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/read",
            json={"participant": str(master), "last_read": "2026-01-01"},
        )
        assert resp.status_code == 422, resp.text

    async def test_the_declared_fields_still_pass(
        self, client: AsyncClient
    ) -> None:
        """The pair: forbidding the undeclared must not forbid the
        declared -- including the optional ones, which is where a
        careless 'forbid' bites.
        """
        client_id, master = await _recipient(), await _recipient()
        resp = await client.post(
            "/api/v1/threads",
            json={
                "client": str(client_id), "operator_kind": "user",
                "operator_value": str(master), "kind": "dm",
                "subject_type": "practice", "subject_id": "s-1",
                "title": "T", "priority": 3,
            },
        )
        assert resp.status_code == 200, resp.text

    async def test_a_missing_required_field_is_still_422(
        self, client: AsyncClient
    ) -> None:
        """NEHVATKA: forbidding extras must not soften absence."""
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(client_id)},
        )
        assert resp.status_code == 422, resp.text


class TestReadStateReferents:
    """Both halves of one endpoint (R-2 item 4).

    The read pointer writes a row with two foreign keys, and both used
    to surface as a 500: the product could not tell "no such thread"
    from "comms is broken".
    """

    async def test_read_on_an_absent_thread_is_404(
        self, client: AsyncClient
    ) -> None:
        participant = await _recipient()
        resp = await client.post(
            f"/api/v1/threads/{uuid4()}/read",
            json={"participant": str(participant)},
        )
        assert resp.status_code == 404, resp.text

    async def test_read_by_an_unknown_participant_is_404(
        self, client: AsyncClient
    ) -> None:
        """The half the thread check does not cover, and not an exotic
        one: a product whose synchronous recipient upsert failed and
        fell back to its outbox has users that exist for it and not yet
        for comms.
        """
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        resp = await client.post(
            f"/api/v1/threads/{tid}/read",
            json={"participant": str(uuid4())},
        )
        assert resp.status_code == 404, resp.text

    async def test_a_real_thread_and_participant_still_succeed(
        self, client: AsyncClient
    ) -> None:
        """The pair to both 404s: the check must not have replaced the
        behaviour it guards. Twice, because the pointer is idempotent
        and a referent check is exactly the kind of thing that breaks
        a repeat.
        """
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        await client.post(
            f"/api/v1/threads/{tid}/messages",
            json={"sender": str(client_id), "body": "hello"},
        )
        first = await client.post(
            f"/api/v1/threads/{tid}/read", json={"participant": str(master)},
        )
        second = await client.post(
            f"/api/v1/threads/{tid}/read", json={"participant": str(master)},
        )
        assert first.status_code == 200, first.text
        assert first.json() == {"unread": 0}
        assert second.json() == first.json()


class TestDatabaseFailureNet:
    """The safety net for what no bound foresaw (R-2 item 5)."""

    @staticmethod
    def _db_error() -> DBAPIError:
        """A database error shaped like the ones this net exists for:
        its string carries the statement and the bound parameters."""
        return DBAPIError(
            statement=(
                "INSERT INTO messages (thread_id, sender, body) "
                "VALUES (%(thread_id)s, %(sender)s, %(body)s)"
            ),
            params={"body": "one-time-code-424242"},
            orig=Exception(
                "value too long for type character varying(5000)"
            ),
        )

    async def test_nothing_of_the_database_reaches_the_caller(
        self, client: AsyncClient
    ) -> None:
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        with patch(
            "app.api.messaging.post_message", side_effect=self._db_error(),
        ):
            resp = await client.post(
                f"/api/v1/threads/{tid}/messages",
                json={"sender": str(client_id), "body": "hi"},
            )
        assert resp.status_code == 500
        text = resp.text
        for leak in (
            "INSERT", "messages", "thread_id", "character varying",
            "one-time-code-424242",
        ):
            assert leak, "an empty needle would make this check vacuous"
            assert leak not in text, f"{leak!r} reached the caller"

    async def test_the_log_keeps_what_the_response_dropped(
        self, client: AsyncClient
    ) -> None:
        """THE PAIR, and the whole reason the net is allowed to exist.
        A net that swallowed quietly would leave a clean response, a
        healthy-looking service and an invisible defect -- worse than
        no net at all.
        """
        client_id, master = await _recipient(), await _recipient()
        tid = await _dm(client, client_id, master)
        with capture_logs() as logs, patch(
            "app.api.messaging.post_message", side_effect=self._db_error(),
        ):
            await client.post(
                f"/api/v1/threads/{tid}/messages",
                json={"sender": str(client_id), "body": "hi"},
            )
        entries = [
            log for log in logs
            if log.get("event") == "database_error_at_api_edge"
        ]
        assert len(entries) == 1
        entry = entries[0]
        assert entry["log_level"] == "error"
        assert entry["error_type"] == "DBAPIError"
        assert entry["path"].endswith("/messages")
        # The traceback itself: without exc_info the entry would name
        # the failure and lose every means of finding it.
        assert isinstance(entry["exc_info"], DBAPIError)
