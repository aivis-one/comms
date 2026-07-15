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

from app.core.exceptions import ValidationError
from app.transport.events import (
    GroupChanged,
    NotificationRequest,
    UserUpserted,
    parse_event,
    validate_action_data,
)


def _envelope(event: str, data: dict[str, Any]) -> dict[str, str]:
    return {"event": event, "data": json.dumps(data)}


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
    """Amendment A: 'v' is validated on EVERY event."""

    def test_missing_v_is_terminal(self) -> None:
        data = _request_data()
        del data["v"]
        with pytest.raises(ValidationError, match="unsupported schema"):
            parse_event(_envelope("notification_request", data))

    def test_unsupported_v_is_terminal(self) -> None:
        with pytest.raises(ValidationError, match="unsupported schema"):
            parse_event(
                _envelope("notification_request", _request_data(v=2))
            )

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
        data = _request_data(
            channels=["in_app", "telegram"],
            action_data={"action": "open_unit",
                         "params": {"unit_id": "42"}, "amount": 100},
            priority=1,
            scheduled_at="2026-07-15T10:00:00+00:00",
            expiry_at="2026-07-16T10:00:00+00:00",
        )
        event = parse_event(_envelope("notification_request", data))
        assert isinstance(event, NotificationRequest)
        assert event.channels == ["in_app", "telegram"]
        assert event.priority == 1
        assert event.scheduled_at is not None
        assert event.scheduled_at.tzinfo is not None

    def test_defaults(self) -> None:
        event = parse_event(
            _envelope("notification_request", _request_data())
        )
        assert isinstance(event, NotificationRequest)
        assert event.channels is None
        assert event.action_data is None
        assert event.priority == 5
        assert event.scheduled_at is None
        assert event.expiry_at is None

    @pytest.mark.parametrize("field", [
        "idempotency_key", "type", "target_type", "target_value",
        "title", "body",
    ])
    def test_required_fields(self, field: str) -> None:
        data = _request_data()
        del data[field]
        with pytest.raises(ValidationError):
            parse_event(_envelope("notification_request", data))

    def test_user_target_must_be_uuid(self) -> None:
        with pytest.raises(ValidationError, match="not a valid uuid"):
            parse_event(_envelope(
                "notification_request",
                _request_data(target_value="not-a-uuid"),
            ))

    def test_all_target_must_be_star(self) -> None:
        with pytest.raises(ValidationError, match='must be "\\*"'):
            parse_event(_envelope(
                "notification_request",
                _request_data(target_type="all", target_value="everyone"),
            ))

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone offset"):
            parse_event(_envelope(
                "notification_request",
                _request_data(scheduled_at="2026-07-15T10:00:00"),
            ))

    def test_overlong_idempotency_key(self) -> None:
        with pytest.raises(ValidationError, match="exceeds 200"):
            parse_event(_envelope(
                "notification_request",
                _request_data(idempotency_key="x" * 201),
            ))

    def test_bool_priority_rejected(self) -> None:
        """bool is an int subclass -- 'priority': true is a bug."""
        with pytest.raises(ValidationError, match="integer"):
            parse_event(_envelope(
                "notification_request", _request_data(priority=True),
            ))


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
