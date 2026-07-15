# =============================================================================
# COMMS Service -- Event Wire Contract (Phase 3c) -- FROZEN
# =============================================================================
#
# The contract the product's outbox relay (Phase 6) writes against.
#
# ENVELOPE (one Redis Stream entry, XADD <stream> * ...):
#
#   event: "notification_request" | "user_upserted" | "group_changed"
#   data:  <UTF-8 JSON document>
#
# VERSIONING (frozen decision, review 3c part B): the ENVELOPE
# {event, data} is frozen FOREVER -- all evolution happens INSIDE
# data, gated by its required integer field "v". The consumer
# validates "v" on every event (review 3c amendment A): a missing or
# unsupported version is a TERMINAL error -> DLQ, never a silent parse
# under old semantics. Currently supported: v=1.
#
# SCHEMAS (data), all fields validated here:
#
#   notification_request (deduplicated by idempotency_key):
#     v                1                      -- required
#     idempotency_key  str 1..200             -- required, unique per
#                                                logical request (e.g.
#                                                the outbox row id)
#     type             str                    -- required; must be
#                                                registered by profile
#                                                (checked in handler)
#     target_type      "user"|"group"|"all"   -- required
#     target_value     "<uuid>"|"<group_key>"|"*"  -- required; form
#                                                checked per target_type
#     title            str 1..500             -- required (stored
#                                                fallback; templates
#                                                override at delivery)
#     body             str 1..5000            -- required
#     channels         [str]                  -- optional, default
#                                                ["in_app"]; values
#                                                validated by
#                                                create_notification
#     action_data      {..}                   -- optional, see below
#     priority         int                    -- optional, default 5
#     scheduled_at     iso8601 WITH tz        -- optional
#     expiry_at        iso8601 WITH tz        -- optional
#
#   user_upserted (naturally idempotent -- upsert; snapshot
#   discipline: ALL fields required, "no value" is an explicit null):
#     v            1           -- required
#     recipient_id "<uuid>"    -- required (product user id)
#     telegram_id  int | null  -- required key
#     email        str | null  -- required key
#     locale       str         -- required, non-empty
#     timezone     str | null  -- required key (IANA name)
#     active       bool        -- required
#
#   group_changed (naturally idempotent both ways):
#     v            1           -- required
#     group_key    str 1..200  -- required, opaque to comms
#     recipient_id "<uuid>"    -- required
#     member       bool        -- required (true=ensure, false=remove)
#
# action_data rules (Phase 3c item 5, early line of defense; the
# per-channel checks at delivery -- deep-link charset/64 from 3a --
# remain the second line):
#   - a JSON object;
#   - keys are non-empty strings; keys starting with "_" are REJECTED
#     (reserved for internal pipeline use, e.g. "_channels");
#   - "action" (optional): non-empty string -- the deep-link intent;
#   - "params" (optional): object of SCALAR values -- deep-link params;
#   - every OTHER key is a template variable and must be a SCALAR
#     (str / int / float / bool / null): lists and nested objects do
#     not survive str.format_map rendering meaningfully and are
#     rejected here, not at delivery time.
#
# ORDERING / DELIVERY EXPECTATIONS (for the producer):
#   - at-least-once; consumers ack after processing;
#   - user_upserted precedes group_changed for a new user; a momentary
#     inversion is retried by comms (bounded backoff), a persistent
#     one lands in the DLQ;
#   - sync events are safe to replay any number of times;
#   - notification_request replays are collapsed by idempotency_key.
#
# All validation failures raise ValidationError -- classified TERMINAL
# by the consumer (log + DLQ + XACK), per the poison-pill rule.
# =============================================================================

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from app.core.constants import (
    MAX_BODY_LEN,
    MAX_IDEMPOTENCY_KEY_LEN,
    MAX_TITLE_LEN,
)
from app.core.exceptions import ValidationError
from app.engine.constants import TargetType

SUPPORTED_SCHEMA_VERSIONS = frozenset({1})

# Envelope field names (frozen).
ENVELOPE_EVENT_FIELD = "event"
ENVELOPE_DATA_FIELD = "data"

EVENT_NOTIFICATION_REQUEST = "notification_request"
EVENT_USER_UPSERTED = "user_upserted"
EVENT_GROUP_CHANGED = "group_changed"

KNOWN_EVENTS = frozenset({
    EVENT_NOTIFICATION_REQUEST,
    EVENT_USER_UPSERTED,
    EVENT_GROUP_CHANGED,
})

_SCALAR_TYPES = (str, int, float, bool, type(None))

# group_memberships.group_key is String(200); mirror at the boundary.
_MAX_GROUP_KEY_LEN = 200


# ---------------------------------------------------------------------------
# Parsed event dataclasses (what handlers.py consumes)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NotificationRequest:
    idempotency_key: str
    type: str
    target_type: str
    target_value: str
    title: str
    body: str
    channels: list[str] | None
    action_data: dict[str, Any] | None
    priority: int
    scheduled_at: datetime | None
    expiry_at: datetime | None


@dataclass(frozen=True)
class UserUpserted:
    recipient_id: UUID
    telegram_id: int | None
    email: str | None
    locale: str
    timezone: str | None
    active: bool


@dataclass(frozen=True)
class GroupChanged:
    group_key: str
    recipient_id: UUID
    member: bool


ParsedEvent = NotificationRequest | UserUpserted | GroupChanged


# ---------------------------------------------------------------------------
# Field-level validators
# ---------------------------------------------------------------------------


def _require(data: dict[str, Any], key: str, event: str) -> Any:
    """Presence check: the KEY must exist (its value may be null where
    the schema says so -- snapshot discipline needs explicit nulls)."""
    if key not in data:
        raise ValidationError(
            f"{event}: required field {key!r} is missing"
        )
    return data[key]


def _string(value: Any, field: str, event: str, *, max_len: int) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError(
            f"{event}: field {field!r} must be a non-empty string"
        )
    if len(value) > max_len:
        raise ValidationError(
            f"{event}: field {field!r} exceeds {max_len} characters"
        )
    return value


def _optional_string(value: Any, field: str, event: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValidationError(
            f"{event}: field {field!r} must be a string or null"
        )
    return value


def _bool(value: Any, field: str, event: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationError(
            f"{event}: field {field!r} must be a boolean"
        )
    return value


def _int(value: Any, field: str, event: str) -> int:
    # bool is an int subclass -- reject it explicitly: "priority":
    # true is a producer bug, not priority 1.
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError(
            f"{event}: field {field!r} must be an integer"
        )
    return value


def _uuid(value: Any, field: str, event: str) -> UUID:
    if not isinstance(value, str):
        raise ValidationError(
            f"{event}: field {field!r} must be a uuid string"
        )
    try:
        return UUID(value)
    except ValueError as exc:
        raise ValidationError(
            f"{event}: field {field!r} is not a valid uuid: {value!r}"
        ) from exc


def _datetime(value: Any, field: str, event: str) -> datetime:
    """ISO-8601 WITH timezone. A naive timestamp is ambiguous across
    the product/comms boundary and is rejected outright."""
    if not isinstance(value, str):
        raise ValidationError(
            f"{event}: field {field!r} must be an iso8601 string"
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(
            f"{event}: field {field!r} is not valid iso8601: {value!r}"
        ) from exc
    if parsed.tzinfo is None:
        raise ValidationError(
            f"{event}: field {field!r} must carry a timezone offset "
            f"(naive timestamps are ambiguous on the wire)"
        )
    return parsed


def _is_scalar(value: Any) -> bool:
    return isinstance(value, _SCALAR_TYPES)


def validate_action_data(
    action_data: Any, *, event: str = EVENT_NOTIFICATION_REQUEST
) -> dict[str, Any]:
    """Enforce the action_data rules (see module header).

    The early line of defense (item 5): everything here would
    otherwise fail LATER and WORSE -- an underscore key would collide
    with the pipeline's "_channels", a list variable would render as
    "['a', 'b']" in a user-facing message, a non-scalar param would
    blow up deep-link encoding at delivery after burning a resolve.
    """
    if not isinstance(action_data, dict):
        raise ValidationError(
            f"{event}: action_data must be a JSON object"
        )
    for key, value in action_data.items():
        if not isinstance(key, str) or not key:
            raise ValidationError(
                f"{event}: action_data keys must be non-empty strings"
            )
        if key.startswith("_"):
            raise ValidationError(
                f"{event}: action_data key {key!r} is reserved "
                f"(underscore prefix is internal, e.g. _channels)"
            )
        if key == "action":
            if not isinstance(value, str) or not value:
                raise ValidationError(
                    f"{event}: action_data.action must be a non-empty "
                    f"string"
                )
        elif key == "params":
            if not isinstance(value, dict):
                raise ValidationError(
                    f"{event}: action_data.params must be a JSON object"
                )
            for p_key, p_value in value.items():
                if not isinstance(p_key, str) or not p_key:
                    raise ValidationError(
                        f"{event}: action_data.params keys must be "
                        f"non-empty strings"
                    )
                if not _is_scalar(p_value):
                    raise ValidationError(
                        f"{event}: action_data.params[{p_key!r}] must "
                        f"be a scalar (str/int/float/bool/null)"
                    )
        else:
            # A template variable: must survive str.format_map into
            # user-facing text -- scalars only.
            if not _is_scalar(value):
                raise ValidationError(
                    f"{event}: action_data[{key!r}] is a template "
                    f"variable and must be a scalar "
                    f"(str/int/float/bool/null), got "
                    f"{type(value).__name__}"
                )
    return action_data


def _validate_target(
    target_type: Any, target_value: Any, event: str
) -> tuple[str, str]:
    """Per-target_type form check for target_value (early signal; the
    membership/type checks happen downstream)."""
    if target_type not in (
        TargetType.USER,
        TargetType.GROUP,
        TargetType.ALL,
    ):
        raise ValidationError(
            f"{event}: target_type must be one of user/group/all, "
            f"got {target_type!r}"
        )
    if target_type == TargetType.USER:
        # Must be a uuid string (bare product user id).
        _uuid(target_value, "target_value", event)
        return str(target_type), str(target_value)
    if target_type == TargetType.GROUP:
        value = _string(
            target_value, "target_value", event,
            max_len=_MAX_GROUP_KEY_LEN,
        )
        return str(target_type), value
    # ALL: "*" by convention -- anything else is a producer bug worth
    # a loud rejection, not a silent ignore.
    if target_value != "*":
        raise ValidationError(
            f'{event}: target_value must be "*" when target_type is '
            f"all, got {target_value!r}"
        )
    return str(target_type), "*"


# ---------------------------------------------------------------------------
# Envelope + per-event parsing
# ---------------------------------------------------------------------------


def _decode(value: str | bytes) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else value


def parse_event(fields: dict[Any, Any]) -> ParsedEvent:
    """Parse one stream entry's field map into a typed event.

    Raises ValidationError (terminal -> DLQ) on ANY malformation:
    missing envelope fields, unknown event name, broken JSON, missing
    or unsupported schema version, schema violations.
    """
    decoded = {_decode(k): _decode(v) for k, v in fields.items()}

    if ENVELOPE_EVENT_FIELD not in decoded:
        raise ValidationError(
            f"envelope: field {ENVELOPE_EVENT_FIELD!r} is missing"
        )
    if ENVELOPE_DATA_FIELD not in decoded:
        raise ValidationError(
            f"envelope: field {ENVELOPE_DATA_FIELD!r} is missing"
        )

    event = decoded[ENVELOPE_EVENT_FIELD]
    if event not in KNOWN_EVENTS:
        raise ValidationError(f"envelope: unknown event {event!r}")

    try:
        data = json.loads(decoded[ENVELOPE_DATA_FIELD])
    except json.JSONDecodeError as exc:
        raise ValidationError(
            f"{event}: data is not valid JSON: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ValidationError(f"{event}: data must be a JSON object")

    # Version barrier (review 3c amendment A): validated on EVERY
    # event, from day one with a single version -- a v2 payload must
    # land in the DLQ, never be silently parsed under v1 semantics.
    version = data.get("v")
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ValidationError(
            f"{event}: unsupported schema version {version!r} "
            f"(supported: "
            f"{sorted(SUPPORTED_SCHEMA_VERSIONS)}); "
            f"a missing 'v' is a producer bug"
        )

    if event == EVENT_NOTIFICATION_REQUEST:
        return _parse_notification_request(data)
    if event == EVENT_USER_UPSERTED:
        return _parse_user_upserted(data)
    return _parse_group_changed(data)


def _parse_notification_request(data: dict[str, Any]) -> NotificationRequest:
    event = EVENT_NOTIFICATION_REQUEST

    idempotency_key = _string(
        _require(data, "idempotency_key", event),
        "idempotency_key", event, max_len=MAX_IDEMPOTENCY_KEY_LEN,
    )
    # Type registration is checked in the handler against the live
    # registry (create_notification); here only the string form.
    type_ = _string(
        _require(data, "type", event), "type", event, max_len=200,
    )
    target_type, target_value = _validate_target(
        _require(data, "target_type", event),
        _require(data, "target_value", event),
        event,
    )
    title = _string(
        _require(data, "title", event), "title", event,
        max_len=MAX_TITLE_LEN,
    )
    body = _string(
        _require(data, "body", event), "body", event,
        max_len=MAX_BODY_LEN,
    )

    channels: list[str] | None = None
    if data.get("channels") is not None:
        raw_channels = data["channels"]
        if not isinstance(raw_channels, list) or not raw_channels:
            raise ValidationError(
                f"{event}: channels must be a non-empty list of "
                f"channel names"
            )
        channels = [
            _string(c, "channels[]", event, max_len=20)
            for c in raw_channels
        ]
        # Channel VALUES are validated by create_notification against
        # the single source of truth (_VALID_CHANNELS) -- not
        # duplicated here.

    action_data: dict[str, Any] | None = None
    if data.get("action_data") is not None:
        action_data = validate_action_data(data["action_data"])

    priority = 5
    if data.get("priority") is not None:
        priority = _int(data["priority"], "priority", event)

    scheduled_at: datetime | None = None
    if data.get("scheduled_at") is not None:
        scheduled_at = _datetime(
            data["scheduled_at"], "scheduled_at", event,
        )
    expiry_at: datetime | None = None
    if data.get("expiry_at") is not None:
        expiry_at = _datetime(data["expiry_at"], "expiry_at", event)

    return NotificationRequest(
        idempotency_key=idempotency_key,
        type=type_,
        target_type=target_type,
        target_value=target_value,
        title=title,
        body=body,
        channels=channels,
        action_data=action_data,
        priority=priority,
        scheduled_at=scheduled_at,
        expiry_at=expiry_at,
    )


def _parse_user_upserted(data: dict[str, Any]) -> UserUpserted:
    event = EVENT_USER_UPSERTED

    recipient_id = _uuid(
        _require(data, "recipient_id", event), "recipient_id", event,
    )
    telegram_id_raw = _require(data, "telegram_id", event)
    telegram_id = (
        None
        if telegram_id_raw is None
        else _int(telegram_id_raw, "telegram_id", event)
    )
    email = _optional_string(
        _require(data, "email", event), "email", event,
    )
    locale = _string(
        _require(data, "locale", event), "locale", event, max_len=20,
    )
    timezone = _optional_string(
        _require(data, "timezone", event), "timezone", event,
    )
    active = _bool(_require(data, "active", event), "active", event)

    return UserUpserted(
        recipient_id=recipient_id,
        telegram_id=telegram_id,
        email=email,
        locale=locale,
        timezone=timezone,
        active=active,
    )


def _parse_group_changed(data: dict[str, Any]) -> GroupChanged:
    event = EVENT_GROUP_CHANGED

    group_key = _string(
        _require(data, "group_key", event), "group_key", event,
        max_len=_MAX_GROUP_KEY_LEN,
    )
    recipient_id = _uuid(
        _require(data, "recipient_id", event), "recipient_id", event,
    )
    member = _bool(_require(data, "member", event), "member", event)

    return GroupChanged(
        group_key=group_key,
        recipient_id=recipient_id,
        member=member,
    )
