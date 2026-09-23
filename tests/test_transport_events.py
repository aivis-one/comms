# =============================================================================
# COMMS Service -- Event wire-contract tests (Phase 3c items 5, 7 + A)
# =============================================================================
#
# Pure parsing/validation: no DB, no Redis. Every rejection path of
# the FROZEN contract is pinned here -- these tests are the contract's
# executable spec. Amendment A (version barrier) is covered explicitly.
# =============================================================================

import json
from typing import Any
from uuid import uuid4

import pytest

from app.core.constants import (
    MAX_GROUP_KEY_LEN,
    MAX_TELEGRAM_ID,
    MAX_TYPE_KEY_LEN,
    MIN_TELEGRAM_ID,
)
from app.core.exceptions import ValidationError
from app.transport.events import (
    GroupChanged,
    NotificationRequest,
    RejectedNotificationRequest,
    ReminderCancel,
    UserUpserted,
    parse_event,
    validate_action_data,
)


def _envelope(event: str, data: dict[str, Any]) -> dict[str, str]:
    return {"event": event, "data": json.dumps(data)}


def _rejected(data: dict[str, Any]) -> RejectedNotificationRequest:
    """Parse a notification_request that must be REJECTED under its key.

    Since F1.2 a request whose key is readable is never an exception:
    the refusal comes back as data, to be recorded under the key.
    """
    event = parse_event(_envelope("notification_request", data))
    assert isinstance(event, RejectedNotificationRequest), event
    assert event.idempotency_key == data["idempotency_key"]
    assert len(event.fingerprint) == 64
    return event


def _request_data(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "v": 1,
        "idempotency_key": "outbox-row-1",
        "type": "unit_event",
        "target_type": "user",
        "target_value": str(uuid4()),
        "title": "T",
        "body": "B",
    }
    base.update(overrides)
    return base


class TestEnvelope:
    def test_bytes_fields_decoded(self) -> None:
        """Raw redis returns bytes; the parser must not care."""
        data = _request_data()
        fields = {
            b"event": b"notification_request",
            b"data": json.dumps(data).encode(),
        }
        event = parse_event(fields)
        assert isinstance(event, NotificationRequest)
        assert event.idempotency_key == "outbox-row-1"

    def test_missing_event_field(self) -> None:
        with pytest.raises(ValidationError, match="'event' is missing"):
            parse_event({"data": "{}"})

    def test_missing_data_field(self) -> None:
        with pytest.raises(ValidationError, match="'data' is missing"):
            parse_event({"event": "user_upserted"})

    def test_unknown_event_name(self) -> None:
        with pytest.raises(ValidationError, match="unknown event"):
            parse_event(_envelope("user_deleted", {"v": 1}))

    def test_broken_json(self) -> None:
        with pytest.raises(ValidationError, match="not valid JSON"):
            parse_event({"event": "user_upserted", "data": "{oops"})

    def test_non_object_json(self) -> None:
        with pytest.raises(ValidationError, match="JSON object"):
            parse_event({"event": "user_upserted", "data": "[1, 2]"})


class TestVersionBarrier:
    """Amendment A: 'v' is validated on EVERY event.

    For notification_request the barrier still refuses -- but since
    F1.2 a request whose key is readable is refused UNDER that key (a
    RejectedNotificationRequest), not dead-lettered: the product can
    find the refusal. These tests asserted a terminal ValidationError;
    the refusal is the same, only its address changed. The other events
    still raise (test_version_barrier_applies below).
    """

    def test_missing_v_is_refused(self) -> None:
        data = _request_data()
        del data["v"]
        assert "unsupported schema" in _rejected(data).reason

    def test_unsupported_v_is_refused(self) -> None:
        assert "unsupported schema" in _rejected(_request_data(v=2)).reason

    def test_bool_and_float_v_rejected(self) -> None:
        """Python's True == 1 and 1.0 == 1 must not open the barrier:
        'v' is strictly a JSON integer (review 3c.1)."""
        for bad_v in (True, 1.0):
            assert "unsupported schema" in _rejected(
                _request_data(v=bad_v),
            ).reason

    def test_v1_passes_all_events(self) -> None:
        rid = str(uuid4())
        assert isinstance(
            parse_event(
                _envelope("notification_request", _request_data())
            ),
            NotificationRequest,
        )
        assert isinstance(
            parse_event(_envelope("user_upserted", {
                "v": 1, "recipient_id": rid, "telegram_id": None,
                "email": None, "locale": "en", "timezone": None,
                "active": True,
            })),
            UserUpserted,
        )
        assert isinstance(
            parse_event(_envelope("group_changed", {
                "v": 1, "group_key": "g", "recipient_id": rid,
                "member": True,
            })),
            GroupChanged,
        )


class TestNotificationRequestSchema:
    def test_full_round(self) -> None:
        """Every envelope field and the letter, parsed. Before F1.2 this
        round also carried `channels` and `priority`; both left the
        request (the channel is the profile's, priority is not an
        envelope field) -- see test_channels_and_priority_are_refused."""
        data = _request_data(
            action_data={"action": "open_unit",
                         "params": {"unit_id": "42"}, "amount": 100},
            scheduled_at="2026-07-15T10:00:00+00:00",
            expiry_at="2026-07-16T10:00:00+00:00",
            correlation="order-17",
        )
        event = parse_event(_envelope("notification_request", data))
        assert isinstance(event, NotificationRequest)
        assert event.correlation == "order-17"
        assert event.scheduled_at is not None
        assert event.scheduled_at.tzinfo is not None
        assert event.action_data == data["action_data"]

    def test_defaults(self) -> None:
        event = parse_event(
            _envelope("notification_request", _request_data())
        )
        assert isinstance(event, NotificationRequest)
        assert event.action_data is None
        assert event.correlation is None
        assert event.scheduled_at is None
        assert event.expiry_at is None

    def test_missing_key_is_terminal(self) -> None:
        """No key, no address: the one required field whose absence is
        still a DLQ case (KNOWN CEILING in app/transport/events.py)."""
        data = _request_data()
        del data["idempotency_key"]
        with pytest.raises(ValidationError, match="idempotency_key"):
            parse_event(_envelope("notification_request", data))

    @pytest.mark.parametrize("field", [
        "type", "target_type", "target_value", "title", "body",
    ])
    def test_required_fields(self, field: str) -> None:
        """Was: every required field missing -> ValidationError. With a
        readable key the refusal is recorded under it instead (F1.2)."""
        data = _request_data()
        del data[field]
        assert f"required field {field!r} is missing" in _rejected(data).reason

    def test_user_target_must_be_uuid(self) -> None:
        assert "not a valid uuid" in _rejected(
            _request_data(target_value="not-a-uuid"),
        ).reason

    def test_all_target_must_be_star(self) -> None:
        assert 'must be "*"' in _rejected(
            _request_data(target_type="all", target_value="everyone"),
        ).reason

    def test_naive_datetime_rejected(self) -> None:
        assert "timezone offset" in _rejected(
            _request_data(scheduled_at="2026-07-15T10:00:00"),
        ).reason

    def test_overlong_idempotency_key(self) -> None:
        """A key longer than its column cannot become an address: still
        terminal, the DLQ."""
        with pytest.raises(ValidationError, match="exceeds 200"):
            parse_event(_envelope(
                "notification_request",
                _request_data(idempotency_key="x" * 201),
            ))

    @pytest.mark.parametrize("field,value", [
        ("channels", ["in_app"]),
        ("priority", 1),
        ("priority", True),
        ("priority", 2**31),
    ])
    def test_channels_and_priority_are_refused(
        self, field: str, value: Any,
    ) -> None:
        """Replaces five tests that pinned how `channels` and
        `priority` parsed (the priority ones: bool refused, the column
        range at both ends, the constant tied to the Integer column).
        F1.2 removed both fields from the request -- the channel is the
        profile's, and priority is not an envelope field comms may read
        -- so ANY value of either is refused, by name, under the key."""
        reason = _rejected(_request_data(**{field: value})).reason
        assert f"unknown field(s) {field!r}" in reason
        assert "chosen by the profile" in reason

    def test_type_longer_than_the_column_is_refused_here(self) -> None:
        """NO DEFECT HID BEHIND the 200 that used to stand here: a type
        longer than the column cannot be registered (the profile loader
        caps keys at the same constant), so the registry refused it
        before any INSERT. Only the WORDING moves -- the field is named
        at the boundary instead of the profile printing its whole
        declared list."""
        assert "'type' exceeds" in _rejected(
            _request_data(type="t" * (MAX_TYPE_KEY_LEN + 1)),
        ).reason

    def test_a_type_of_exactly_the_column_width_still_parses(self) -> None:
        """The pair. Parsing judges the string form only; whether the
        type is REGISTERED is the handler's question, and it is asked
        against the live registry."""
        key = "t" * MAX_TYPE_KEY_LEN
        event = parse_event(_envelope(
            "notification_request", _request_data(type=key),
        ))
        assert isinstance(event, NotificationRequest)
        assert event.type == key


class TestActionDataRules:
    """Item 5: the early line of defense."""

    def test_underscore_key_rejected(self) -> None:
        with pytest.raises(ValidationError, match="reserved"):
            validate_action_data({"_channels": ["email"]})

    def test_template_variable_must_be_scalar(self) -> None:
        with pytest.raises(ValidationError, match="template variable"):
            validate_action_data({"items": ["a", "b"]})
        with pytest.raises(ValidationError, match="template variable"):
            validate_action_data({"nested": {"a": 1}})

    def test_scalar_variables_pass(self) -> None:
        data = {"amount": 100, "rate": 1.5, "name": "x",
                "flag": True, "gone": None}
        assert validate_action_data(data) == data

    def test_action_must_be_nonempty_string(self) -> None:
        with pytest.raises(ValidationError, match="action"):
            validate_action_data({"action": ""})
        with pytest.raises(ValidationError, match="action"):
            validate_action_data({"action": 42})

    def test_params_values_must_be_scalar(self) -> None:
        with pytest.raises(ValidationError, match="params"):
            validate_action_data(
                {"action": "a", "params": {"ids": [1, 2]}}
            )

    def test_non_dict_rejected(self) -> None:
        with pytest.raises(ValidationError, match="JSON object"):
            validate_action_data(["not", "a", "dict"])


class TestSyncSchemas:
    def test_user_upserted_requires_every_key(self) -> None:
        """Snapshot discipline: 'no value' is an explicit null, an
        absent key is a producer bug."""
        base: dict[str, Any] = {
            "v": 1, "recipient_id": str(uuid4()), "telegram_id": 85000,
            "email": "a@b.c", "locale": "en",
            "timezone": "Europe/Berlin", "active": True,
        }
        for field in ("recipient_id", "telegram_id", "email", "locale",
                      "timezone", "active"):
            data = dict(base)
            del data[field]
            with pytest.raises(ValidationError, match="missing"):
                parse_event(_envelope("user_upserted", data))

        event = parse_event(_envelope("user_upserted", base))
        assert isinstance(event, UserUpserted)
        assert event.telegram_id == 85000

    def test_user_upserted_explicit_nulls(self) -> None:
        event = parse_event(_envelope("user_upserted", {
            "v": 1, "recipient_id": str(uuid4()), "telegram_id": None,
            "email": None, "locale": "en", "timezone": None,
            "active": False,
        }))
        assert isinstance(event, UserUpserted)
        assert event.telegram_id is None
        assert event.timezone is None
        assert event.active is False

    def test_group_changed(self) -> None:
        rid = str(uuid4())
        event = parse_event(_envelope("group_changed", {
            "v": 1, "group_key": "practice_42", "recipient_id": rid,
            "member": False,
        }))
        assert isinstance(event, GroupChanged)
        assert event.member is False

    def test_group_changed_member_must_be_bool(self) -> None:
        with pytest.raises(ValidationError, match="boolean"):
            parse_event(_envelope("group_changed", {
                "v": 1, "group_key": "g",
                "recipient_id": str(uuid4()), "member": "yes",
            }))

    @pytest.mark.parametrize("length", [MAX_GROUP_KEY_LEN, 1])
    def test_group_key_up_to_the_column_width_parses(
        self, length: int,
    ) -> None:
        """The pair to the refusal below: the boundary sits where the
        column sits, not wherever is convenient."""
        event = parse_event(_envelope("group_changed", {
            "v": 1, "group_key": "g" * length,
            "recipient_id": str(uuid4()), "member": True,
        }))
        assert isinstance(event, GroupChanged)
        assert len(event.group_key) == length

    def test_group_key_past_the_column_width_is_terminal(self) -> None:
        """This parser used to accept 200 characters against a column
        of MAX_GROUP_KEY_LEN, with a comment asserting the column was
        String(200). Anything in between passed here and died on the
        INSERT -- which the consumer treats as a possibly-transient
        failure, so it retried six times before the DLQ. Nothing that
        WORKED changed; the shape of the refusal did.
        """
        with pytest.raises(ValidationError, match="group_key"):
            parse_event(_envelope("group_changed", {
                "v": 1, "group_key": "g" * (MAX_GROUP_KEY_LEN + 1),
                "recipient_id": str(uuid4()), "member": True,
            }))

    @pytest.mark.parametrize(
        "telegram_id", [MAX_TELEGRAM_ID + 1, MIN_TELEGRAM_ID - 1],
    )
    def test_telegram_id_outside_the_column_range_is_terminal(
        self, telegram_id: int,
    ) -> None:
        """Out of the BigInteger range the value is not a large id, it
        is a broken one -- and unbounded it reached the INSERT."""
        with pytest.raises(ValidationError, match="telegram_id"):
            parse_event(_envelope("user_upserted", {
                "v": 1, "recipient_id": str(uuid4()),
                "telegram_id": telegram_id, "email": None,
                "locale": "en", "timezone": None, "active": True,
            }))

    def test_telegram_id_at_the_edge_of_the_range_parses(self) -> None:
        """The pair. Parsing only -- nothing is stored, so this costs
        the shared id band nothing (tests/helpers.py)."""
        event = parse_event(_envelope("user_upserted", {
            "v": 1, "recipient_id": str(uuid4()),
            "telegram_id": MAX_TELEGRAM_ID, "email": None,
            "locale": "en", "timezone": None, "active": True,
        }))
        assert isinstance(event, UserUpserted)
        assert event.telegram_id == MAX_TELEGRAM_ID


class TestReminderCancelSchema:
    """Phase 6/T1 additive event (Master-chat approved 2026-07-28):
    the wire mirror of engine/reminders.cancel_reminders."""

    @staticmethod
    def _cancel_data(**overrides: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "v": 1,
            "types": ["rem_24h", "rem_1h"],
            "correlation_key": "booking_id",
            "correlation_value": str(uuid4()),
        }
        base.update(overrides)
        return base

    def test_happy_path_without_target(self) -> None:
        data = self._cancel_data()
        event = parse_event(_envelope("reminder_cancel", data))
        assert isinstance(event, ReminderCancel)
        assert event.types == ["rem_24h", "rem_1h"]
        assert event.correlation_key == "booking_id"
        assert event.target_type is None
        assert event.target_value is None

    def test_happy_path_with_user_target(self) -> None:
        uid = str(uuid4())
        event = parse_event(_envelope("reminder_cancel", self._cancel_data(
            target_type="user", target_value=uid,
        )))
        assert isinstance(event, ReminderCancel)
        assert event.target_type == "user"
        assert event.target_value == uid

    def test_required_fields(self) -> None:
        for field in ("types", "correlation_key", "correlation_value"):
            data = self._cancel_data()
            del data[field]
            with pytest.raises(ValidationError, match="missing"):
                parse_event(_envelope("reminder_cancel", data))

    def test_types_must_be_nonempty_list(self) -> None:
        with pytest.raises(ValidationError, match="non-empty list"):
            parse_event(_envelope(
                "reminder_cancel", self._cancel_data(types=[]),
            ))
        with pytest.raises(ValidationError, match="non-empty list"):
            parse_event(_envelope(
                "reminder_cancel", self._cancel_data(types="rem_1h"),
            ))

    def test_types_entries_must_be_strings(self) -> None:
        with pytest.raises(ValidationError, match="types"):
            parse_event(_envelope(
                "reminder_cancel", self._cancel_data(types=["ok", 5]),
            ))

    def test_underscore_correlation_key_rejected(self) -> None:
        """An underscore key cannot exist in action_data (reserved),
        so the cancel could never match -- loud producer bug."""
        with pytest.raises(ValidationError, match="reserved"):
            parse_event(_envelope(
                "reminder_cancel",
                self._cancel_data(correlation_key="_channels"),
            ))

    def test_half_target_rejected(self) -> None:
        with pytest.raises(ValidationError, match="both target"):
            parse_event(_envelope(
                "reminder_cancel",
                self._cancel_data(target_value=str(uuid4())),
            ))
        with pytest.raises(ValidationError, match="both target"):
            parse_event(_envelope(
                "reminder_cancel", self._cancel_data(target_type="user"),
            ))

    def test_target_form_checked_per_type(self) -> None:
        with pytest.raises(ValidationError, match="uuid"):
            parse_event(_envelope("reminder_cancel", self._cancel_data(
                target_type="user", target_value="not-a-uuid",
            )))

    def test_version_barrier_applies(self) -> None:
        """The new event sits behind the same v-gate as the frozen
        three -- an unsupported version dead-letters, never parses."""
        with pytest.raises(ValidationError, match="unsupported schema"):
            parse_event(_envelope(
                "reminder_cancel", self._cancel_data(v=2),
            ))
