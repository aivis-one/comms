# =============================================================================
# COMMS Service -- a dead letter of the address book keeps who, never the
# person (ST-1)
# =============================================================================
#
# Forgetting a person (user_deleted) clears the book and does not reach
# the dead-letter stream, which has no expiry -- so the stream must never
# hold what describes the person (spec §10.4, §16.3 of 1 November 2026).
# Held here, against the code and against the contract:
#
#   1  the real consumer, for every address-book event: an event with a
#      sentinel in every field that is not who it is about, malformed so
#      it is dead-lettered -- the sentinel is in no name and no value of
#      the dead letter; the pair: the identity it was sent with is there,
#      exactly the kept set (TestTheRealLoop);
#   2  the table: every event of EVENT_FIELDS is named, each kept set is a
#      part of its event's own set, and the address book keeps a person's
#      id (TestTheTable);
#   3  the contract: the `_dlq_data` row of "How long comms keeps things"
#      names each address-book event with the code's set, both ways
#      (TestTheContract).
# The request's dead letter is G4's, unchanged: tests/test_dlq_no_letter.py.
#
# Mutations (built once, by hand):
#   M1  the address book keeps EVENT_FIELDS again   -> TestTheRealLoop
#       (user_upserted, group_changed, section_membership_changed; for
#       user_deleted the two sets are equal -- not buildable, not built)
#   M2  the identity dropped (kept = {"v"})          -> TestTheRealLoop, pair
#   M10 the contract row says the old text           -> TestTheContract
# =============================================================================

import json
import re
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fakeredis import aioredis as fakeaioredis
from sqlalchemy.ext.asyncio import AsyncSession

from app.transport.consumer import DLQ_KEPT_DATA_FIELDS
from app.transport.events import (
    EVENT_FIELDS,
    EVENT_GROUP_CHANGED,
    EVENT_NOTIFICATION_REQUEST,
    EVENT_REMINDER_CANCEL,
    EVENT_SECTION_MEMBERSHIP_CHANGED,
    EVENT_USER_DELETED,
    EVENT_USER_UPSERTED,
)
from tests.test_dlq_no_letter import (  # noqa: F401 -- fixtures
    _consume,
    redis,
    stream,
)

_DOC = (Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md").read_text(
    encoding="utf-8",
)

_S = "sentinel" + uuid4().hex

ADDRESS_BOOK = (
    EVENT_USER_UPSERTED,
    EVENT_USER_DELETED,
    EVENT_GROUP_CHANGED,
    EVENT_SECTION_MEMBERSHIP_CHANGED,
)

# Who each event is about -- the fields a dead letter keeps (the decision,
# written here as the test's own expectation, not read from the code).
_IDENTITY = {
    EVENT_USER_UPSERTED: {"v", "recipient_id", "version"},
    EVENT_USER_DELETED: {"v", "recipient_id", "version"},
    EVENT_GROUP_CHANGED: {"v", "recipient_id"},
    EVENT_SECTION_MEMBERSHIP_CHANGED: {"v", "operator_id"},
}


def _event(event: str) -> dict[str, Any]:
    """The event with the sentinel in every field that is not who it is
    about; malformed by form, so the parse dead-letters it at once."""
    if event == EVENT_USER_UPSERTED:
        return {
            "v": 1, "recipient_id": str(uuid4()), "version": 7,
            # A string where a number is due: the parse refuses here.
            "telegram_id": f"{_S}", "email": f"{_S}@example.test",
            "locale": f"{_S}", "timezone": f"{_S}", "active": f"{_S}",
        }
    if event == EVENT_USER_DELETED:
        # Every field of it is who; the sentinel rides in a field it does
        # not have, which refuses the event.
        return {"v": 1, "recipient_id": str(uuid4()), "version": 8, "note": f"{_S}"}
    if event == EVENT_GROUP_CHANGED:
        return {
            "v": 1, "recipient_id": str(uuid4()),
            "group_key": f"{_S}", "member": f"{_S}",
        }
    assert event == EVENT_SECTION_MEMBERSHIP_CHANGED
    return {
        "v": 1, "operator_id": str(uuid4()),
        "section_key": f"{_S}", "section_label": f"{_S}", "member": f"{_S}",
    }


class TestTheRealLoop:
    @pytest.mark.parametrize("event", ADDRESS_BOOK)
    async def test_who_stays_the_person_goes(
        self,
        redis: fakeaioredis.FakeRedis,  # noqa: F811
        stream: str,  # noqa: F811
        db_session: AsyncSession,
        event: str,
    ) -> None:
        data = _event(event)
        await redis.xadd(stream, {"event": event, "data": json.dumps(data)})
        (dead,) = await _consume(redis, stream, 1)
        for name, value in dead.items():
            assert _S not in name
            assert _S not in value, name
        # The pair: who it was about, exactly, and the rest named.
        kept = json.loads(dead["_dlq_data"])
        assert set(kept) == _IDENTITY[event]
        for key in _IDENTITY[event]:
            assert kept[key] == data[key]
            assert kept[key] not in (None, "")
        assert json.loads(dead["_dlq_dropped"]) == sorted(
            f"data.{key}" for key in data if key not in _IDENTITY[event]
        )
        assert dead["event"] == event
        assert dead["_dlq_attempts"] == "0"
        assert dead["_dlq_data_fingerprint"]


class TestTheTable:
    def test_every_event_is_named(self) -> None:
        """No event is kept whole by default: each is in the table."""
        assert set(DLQ_KEPT_DATA_FIELDS) == set(EVENT_FIELDS)

    @pytest.mark.parametrize("event", sorted(EVENT_FIELDS))
    def test_each_set_is_a_part_of_its_events(self, event: str) -> None:
        assert DLQ_KEPT_DATA_FIELDS[event] <= EVENT_FIELDS[event]
        assert DLQ_KEPT_DATA_FIELDS[event]

    @pytest.mark.parametrize("event", ADDRESS_BOOK)
    def test_the_address_book_keeps_who(self, event: str) -> None:
        assert DLQ_KEPT_DATA_FIELDS[event] == _IDENTITY[event]

    def test_a_contact_is_in_no_set(self) -> None:
        """The pair of the above, by name: no contact anywhere."""
        for kept in DLQ_KEPT_DATA_FIELDS.values():
            assert not kept & {"email", "telegram_id", "locale", "timezone",
                               "active", "group_key", "section_key",
                               "section_label", "member"}

    def test_the_two_others_keep_their_envelope(self) -> None:
        """reminder_cancel is no address book: its own fields; the request
        is G4's (tests/test_dlq_no_letter.py holds its set)."""
        assert DLQ_KEPT_DATA_FIELDS[EVENT_REMINDER_CANCEL] == (
            EVENT_FIELDS[EVENT_REMINDER_CANCEL]
        )
        assert "llm_audio" not in DLQ_KEPT_DATA_FIELDS[EVENT_NOTIFICATION_REQUEST]


class TestTheContract:
    def test_the_row_names_the_code_s_sets(self) -> None:
        """The `_dlq_data` row of "How long comms keeps things": each
        address-book event with its kept set, both ways."""
        start = _DOC.index("## How long comms keeps things")
        row = re.search(r"^\| `_dlq_data` \|(.*)$", _DOC[start:], re.M)
        assert row is not None
        documented: dict[str, set[str]] = {}
        for segment in row.group(1).split(";"):
            match = re.fullmatch(
                r"\s*((?:`[a-z_]+`(?:, )?)+) -- ((?:`[a-z_]+`(?:, )?)+)\s*",
                segment,
            )
            if match is None:
                continue
            fields = set(re.findall(r"`([a-z_]+)`", match.group(2)))
            for event in re.findall(r"`([a-z_]+)`", match.group(1)):
                documented[event] = fields
        assert documented == {
            event: set(DLQ_KEPT_DATA_FIELDS[event]) for event in ADDRESS_BOOK
        }

    def test_the_contacts_are_said_not_kept(self) -> None:
        section = " ".join(_DOC[_DOC.index("## How long comms keeps things"):].split())
        assert "never the person's contacts" in section
        assert ("Contacts, `active`, a group or section key and a section label "
                "are not kept") in section
