# =============================================================================
# COMMS Service -- Event Wire Contract (Phase 3c) -- FROZEN
# =============================================================================
#
# The contract the product's outbox relay (Phase 6) writes against.
#
# ENVELOPE (one Redis Stream entry, XADD <stream> * ...):
#
#   event: "notification_request" | "user_upserted" | "group_changed"
#          | "reminder_cancel"
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
#   notification_request -- an ENVELOPE plus an opaque LETTER (F1.2).
#   The field set is CLOSED: any other field refuses the request, so a
#   producer still sending `channels` or `priority` learns it is not
#   choosing anything (the channel is the profile's, spec §5.2).
#
#     v                1                      -- required (protocol
#                                                framing, not the job)
#   envelope -- what comms reads, because without it comms cannot
#   deliver:
#     idempotency_key  str 1..200             -- required; the same key
#                                                is the same job
#     type             str                    -- required; must be
#                                                declared by the
#                                                profile, which routes
#                                                it to its channels
#     target_type      "user"|"group"|"all"   -- required (the address)
#     target_value     "<uuid>"|"<group_key>"|"*"  -- required; form
#                                                checked per target_type
#     scheduled_at     iso8601 WITH tz        -- optional, "not before"
#     expiry_at        iso8601 WITH tz        -- optional; must lie in
#                                                the future and after
#                                                scheduled_at; wins over
#                                                the profile's
#                                                expires_after
#     correlation      str 1..200             -- optional; the
#                                                product's reference,
#                                                stored untouched
#   letter -- carried, never interpreted:
#     title            str 1..500             -- required (stored
#                                                fallback; templates
#                                                override at delivery)
#     body             str 1..5000            -- required
#     action_data      {..}                   -- optional, see below
#
#   INTAKE (handlers.py): same key + same bytes -> the existing job;
#   same key + other bytes -> conflict, recorded under the key. A
#   request whose KEY is readable but which cannot be accepted (any
#   rule below, an undeclared type, an expiry already passed) is
#   REJECTED AT INTAKE and recorded under its key -- never the DLQ.
#   Only a request whose key cannot be read goes to the DLQ: there is
#   nothing to attach the rejection to.
#
#   user_upserted (a VERSIONED snapshot, F1.4; snapshot discipline:
#   ALL fields required, "no value" is an explicit null -- never a
#   blank string, never a telegram id of 0; the field set is CLOSED):
#     v            1           -- required
#     recipient_id "<uuid>"    -- required (product user id)
#     version      int >= 1    -- required; the product's monotonic
#                                 snapshot version. Older than stored ->
#                                 refused and acknowledged, its class
#                                 in the log (audience/sync.py)
#     telegram_id  int | null  -- required key, non-zero
#     email        str | null  -- required key, non-blank
#     locale       str | null  -- required key, non-blank
#     timezone     str | null  -- required key (IANA name), non-blank
#     active       bool        -- required
#
#   user_deleted (F1.4, spec §10.4): the product deleted the person;
#   comms forgets how to reach them (app/forgetting.py). Ordered against
#   snapshots by the same version rule; a repeat is a no-op:
#     v            1           -- required
#     recipient_id "<uuid>"    -- required
#     version      int >= 1    -- required
#
#   group_changed (naturally idempotent both ways):
#     v            1           -- required
#     group_key    str 1..200  -- required, opaque to comms
#     recipient_id "<uuid>"    -- required
#     member       bool        -- required (true=ensure, false=remove)
#
#   reminder_cancel -- cancels jobs by their ENVELOPE correlation
#   (F1.3). Naturally idempotent: an already finished or never
#   scheduled match set is a zero-row update. The field set is CLOSED.
#   Cancelled jobs take the outcome CANCELLED (their waiting deliveries
#   too), never EXPIRED:
#     v              1          -- required
#     types          [str]      -- required, non-empty list of type
#                                  keys to cancel
#     correlation    str 1..200 -- required; equal to the
#                                  `correlation` the jobs were sent
#                                  with (an equality test on an opaque
#                                  string -- comms never reads the
#                                  letter to find them)
#     target_type    str | null -- optional filter; both target
#     target_value   str | null    fields together or neither (a bare
#                                  target_value is ambiguous); form
#                                  checked per target_type as in
#                                  notification_request
#
# action_data rules (Phase 3c item 5, early line of defense; the
# per-channel checks at delivery -- deep-link charset/64 from 3a --
# remain the second line):
#   - a JSON object;
#   - keys are non-empty strings (no prefix is reserved: F1.3 removed
#     the underscore reservation with its last consumer);
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
# Validation failures raise ValidationError -- classified TERMINAL by the
# consumer (log + DLQ + XACK), per the poison-pill rule. The one
# exception is a notification_request whose key is readable: it is
# returned as RejectedNotificationRequest and recorded under the key.
#
# KNOWN CEILING -- a request whose key cannot be read is invisible to
# the product.
#   1. Mechanics: without a readable idempotency_key (data is not a
#      JSON object, the key is absent, not a string, empty or longer
#      than the column) there is no address to record the rejection
#      under; the entry goes only to the DLQ, which the product does
#      not read.
#   2. Status: acknowledged by design (spec §6.3 names this boundary).
#   3. Backlog ref: none -- no key, no address; nothing to build.
#   4. Promotion trigger (observable): an event_dead_lettered log line
#      for a notification_request (the DLQ entry's `event` field).
#   5. Agreed fix shape: none on comms' side; the producer's outbox
#      must never emit a request without a well-formed key.
#   6. Rejected: recording under a DERIVED key (the entry id, a hash
#      of the bytes) -- the product never knew that key, so it could
#      never look the rejection up; a record nobody can find is not a
#      receipt.
# =============================================================================

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from app.core.constants import (
    MAX_BODY_LEN,
    MAX_CORRELATION_LEN,
    MAX_EMAIL_LEN,
    MAX_GROUP_KEY_LEN,
    MAX_IDEMPOTENCY_KEY_LEN,
    MAX_LOCALE_LEN,
    MAX_SNAPSHOT_VERSION,
    MAX_TELEGRAM_ID,
    MAX_TIMEZONE_LEN,
    MAX_TITLE_LEN,
    MAX_TYPE_KEY_LEN,
    MIN_TELEGRAM_ID,
)
from app.core.exceptions import ValidationError
from app.engine.constants import TargetType
from app.engine.service import stream_fingerprint
from app.messaging.constants import MAX_SECTION_KEY_LEN, MAX_SECTION_LABEL_LEN

SUPPORTED_SCHEMA_VERSIONS = frozenset({1})

# Envelope field names (frozen).
ENVELOPE_EVENT_FIELD = "event"
ENVELOPE_DATA_FIELD = "data"

EVENT_NOTIFICATION_REQUEST = "notification_request"
EVENT_USER_UPSERTED = "user_upserted"
EVENT_USER_DELETED = "user_deleted"
EVENT_GROUP_CHANGED = "group_changed"
EVENT_REMINDER_CANCEL = "reminder_cancel"
EVENT_SECTION_MEMBERSHIP_CHANGED = "section_membership_changed"

KNOWN_EVENTS = frozenset({
    EVENT_NOTIFICATION_REQUEST,
    EVENT_USER_UPSERTED,
    EVENT_USER_DELETED,
    EVENT_GROUP_CHANGED,
    EVENT_REMINDER_CANCEL,
    EVENT_SECTION_MEMBERSHIP_CHANGED,
})

_SCALAR_TYPES = (str, int, float, bool, type(None))

# The boundary mirrors the COLUMN, and does so by reading the column's
# own constant rather than by repeating its number (R-2 item 1). The
# previous local constant here said 200 and its comment claimed the
# column was String(200); the column is MAX_GROUP_KEY_LEN, and a key
# between the two passed this parser and died on the INSERT -- six
# retries and a DLQ entry for what is a naming error in one field. A
# number copied by hand is a number that drifts; the comment that
# justified the copy outlived the value it justified.
_MAX_GROUP_KEY_LEN = MAX_GROUP_KEY_LEN

# sections.key is String(MAX_SECTION_KEY_LEN); mirror at the boundary so
# an over-long key is a terminal validation error here rather than a
# database error mid-transaction.
_MAX_SECTION_KEY_LEN = MAX_SECTION_KEY_LEN
_MAX_SECTION_LABEL_LEN = MAX_SECTION_LABEL_LEN


# ---------------------------------------------------------------------------
# Parsed event dataclasses (what handlers.py consumes)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NotificationRequest:
    # -- envelope --
    idempotency_key: str
    fingerprint: str
    type: str
    target_type: str
    target_value: str
    scheduled_at: datetime | None
    expiry_at: datetime | None
    correlation: str | None
    # -- letter --
    title: str
    body: str
    action_data: dict[str, Any] | None


@dataclass(frozen=True)
class RejectedNotificationRequest:
    """A request whose key is readable but which cannot be parsed.

    Not an exception: it is recorded under its key (handlers.py), the
    product's side of the responsibility line (spec §5.4).
    """

    idempotency_key: str
    fingerprint: str
    reason: str


@dataclass(frozen=True)
class UserUpserted:
    recipient_id: UUID
    version: int
    telegram_id: int | None
    email: str | None
    locale: str | None
    timezone: str | None
    active: bool


@dataclass(frozen=True)
class UserDeleted:
    recipient_id: UUID
    version: int


@dataclass(frozen=True)
class GroupChanged:
    group_key: str
    recipient_id: UUID
    member: bool


@dataclass(frozen=True)
class SectionMembershipChanged:
    """One operator declared (or undeclared) as serving one section.

    The section travels as a KEY, never as an id. Section ids live in
    this service's database and do not survive its teardown, so a
    product that stored one would be pointing at nothing after a
    rebuild -- the key is the only stable name the two sides share.
    """

    section_key: str
    section_label: str
    operator_id: UUID
    member: bool


@dataclass(frozen=True)
class ReminderCancel:
    types: list[str]
    # The envelope correlation of the jobs to cancel (F1.3): matched by
    # equality against notifications.correlation, never inside the
    # letter.
    correlation: str
    target_type: str | None
    target_value: str | None


ParsedEvent = (
    NotificationRequest
    | RejectedNotificationRequest
    | UserUpserted
    | UserDeleted
    | GroupChanged
    | SectionMembershipChanged
    | ReminderCancel
)


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


def _optional_string(
    value: Any, field: str, event: str, *, max_len: int,
) -> str | None:
    """A nullable string, bounded like its column.

    max_len is REQUIRED, not defaulted: a nullable field is still a
    field with a column behind it, and the two values that used to
    arrive here unbounded (email, timezone) reached the INSERT and came
    back as a database error. An optional value is optional, not
    unmeasured.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValidationError(
            f"{event}: field {field!r} must be a string or null"
        )
    if len(value) > max_len:
        raise ValidationError(
            f"{event}: field {field!r} exceeds {max_len} characters"
        )
    return value


def _bool(value: Any, field: str, event: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationError(
            f"{event}: field {field!r} must be a boolean"
        )
    return value


def _int(
    value: Any,
    field: str,
    event: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    # bool is an int subclass -- reject it explicitly: "telegram_id":
    # true is a producer bug, not id 1.
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError(
            f"{event}: field {field!r} must be an integer"
        )
    # The range, where given, is the COLUMN's range: an integer outside
    # it is not a large value, it is a broken one, and unbounded it
    # reaches the INSERT.
    if minimum is not None and value < minimum:
        raise ValidationError(
            f"{event}: field {field!r} is below {minimum}"
        )
    if maximum is not None and value > maximum:
        raise ValidationError(
            f"{event}: field {field!r} exceeds {maximum}"
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
    otherwise fail LATER and WORSE -- an underscore key would take a
    name reserved for comms, a list variable would render as
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
    or unsupported schema version, schema violations -- EXCEPT for a
    notification_request whose key is readable, which comes back as a
    RejectedNotificationRequest (see _parse_notification_intake).
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

    data = _json_object(decoded[ENVELOPE_DATA_FIELD], event)
    if event == EVENT_NOTIFICATION_REQUEST:
        return _parse_notification_intake(data, _raw_data(fields))
    _check_version(data, event)

    if event == EVENT_USER_UPSERTED:
        return _parse_user_upserted(data)
    if event == EVENT_USER_DELETED:
        return _parse_user_deleted(data)
    if event == EVENT_GROUP_CHANGED:
        return _parse_group_changed(data)
    if event == EVENT_SECTION_MEMBERSHIP_CHANGED:
        return _parse_section_membership_changed(data)
    return _parse_reminder_cancel(data)


def _raw_data(fields: dict[Any, Any]) -> bytes:
    """The `data` field's bytes exactly as the stream delivered them."""
    for key, value in fields.items():
        if _decode(key) == ENVELOPE_DATA_FIELD:
            return value if isinstance(value, bytes) else value.encode("utf-8")
    raise AssertionError("parse_event checked the data field first")


def _json_object(text: str, event: str) -> dict[str, Any]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValidationError(
            f"{event}: data is not valid JSON: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ValidationError(f"{event}: data must be a JSON object")
    return data


def _check_version(data: dict[str, Any], event: str) -> None:
    # Version barrier (review 3c amendment A): validated on EVERY
    # event, from day one with a single version -- a v2 payload must
    # never be silently parsed under v1 semantics. STRICTLY an int
    # (review 3c.1): Python's `True == 1` and `1.0 == 1` would
    # otherwise let "v": true / "v": 1.0 slip through a bare
    # membership test.
    version = data.get("v")
    if (
        not isinstance(version, int)
        or isinstance(version, bool)
        or version not in SUPPORTED_SCHEMA_VERSIONS
    ):
        raise ValidationError(
            f"{event}: unsupported schema version {version!r} "
            f"(supported: "
            f"{sorted(SUPPORTED_SCHEMA_VERSIONS)}, strictly an "
            f"integer); a missing 'v' is a producer bug"
        )


# The closed field set of notification_request (F1.2): envelope,
# letter and the protocol version. Anything else refuses the request.
_NOTIFICATION_REQUEST_FIELDS = frozenset({
    "v",
    # envelope
    "idempotency_key",
    "type",
    "target_type",
    "target_value",
    "scheduled_at",
    "expiry_at",
    "correlation",
    # letter
    "title",
    "body",
    "action_data",
})


def _parse_notification_intake(
    data: dict[str, Any], raw: bytes,
) -> NotificationRequest | RejectedNotificationRequest:
    """The key first, then everything else.

    The key is read BEFORE any other rule, so that a malformed title
    does not take a readable key down to the DLQ with it: once the key
    is known, every refusal has an address (spec §6.3). An unreadable
    key raises -- see the KNOWN CEILING in the module header.
    """
    event = EVENT_NOTIFICATION_REQUEST
    idempotency_key = _string(
        _require(data, "idempotency_key", event),
        "idempotency_key", event, max_len=MAX_IDEMPOTENCY_KEY_LEN,
    )
    fingerprint = stream_fingerprint(raw)
    try:
        _check_version(data, event)
        return _parse_notification_request(data, idempotency_key, fingerprint)
    except ValidationError as exc:
        return RejectedNotificationRequest(
            idempotency_key=idempotency_key,
            fingerprint=fingerprint,
            reason=str(exc),
        )


def _parse_notification_request(
    data: dict[str, Any], idempotency_key: str, fingerprint: str,
) -> NotificationRequest:
    event = EVENT_NOTIFICATION_REQUEST

    unknown = sorted(set(data) - _NOTIFICATION_REQUEST_FIELDS)
    if unknown:
        raise ValidationError(
            f"{event}: unknown field(s) {', '.join(map(repr, unknown))}; "
            f"allowed: {', '.join(sorted(_NOTIFICATION_REQUEST_FIELDS))}. "
            f"The channel is chosen by the profile, per type -- a request "
            f"never names it"
        )

    # Type declaration is checked in the handler against the live
    # registry (create_notification); here only the string form.
    type_ = _string(
        # MAX_TYPE_KEY_LEN: a type longer than the column cannot be
        # declared (the profile loader caps keys at the same constant).
        _require(data, "type", event), "type", event,
        max_len=MAX_TYPE_KEY_LEN,
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

    action_data: dict[str, Any] | None = None
    if data.get("action_data") is not None:
        action_data = validate_action_data(data["action_data"])

    scheduled_at: datetime | None = None
    if data.get("scheduled_at") is not None:
        scheduled_at = _datetime(
            data["scheduled_at"], "scheduled_at", event,
        )
    expiry_at: datetime | None = None
    if data.get("expiry_at") is not None:
        expiry_at = _datetime(data["expiry_at"], "expiry_at", event)

    correlation: str | None = None
    if data.get("correlation") is not None:
        # Non-empty: an empty reference is a producer bug, "no
        # reference" is the absence of the field.
        correlation = _string(
            data["correlation"], "correlation", event,
            max_len=MAX_CORRELATION_LEN,
        )

    return NotificationRequest(
        idempotency_key=idempotency_key,
        fingerprint=fingerprint,
        type=type_,
        target_type=target_type,
        target_value=target_value,
        scheduled_at=scheduled_at,
        expiry_at=expiry_at,
        correlation=correlation,
        title=title,
        body=body,
        action_data=action_data,
    )


_USER_UPSERTED_FIELDS = frozenset({
    "v", "recipient_id", "version", "telegram_id", "email", "locale",
    "timezone", "active",
})
_USER_DELETED_FIELDS = frozenset({"v", "recipient_id", "version"})


def _closed(data: dict[str, Any], allowed: frozenset[str], event: str) -> None:
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ValidationError(
            f"{event}: unknown field(s) {', '.join(map(repr, unknown))}; "
            f"allowed: {', '.join(sorted(allowed))}"
        )


def _version(data: dict[str, Any], event: str) -> int:
    return _int(
        _require(data, "version", event), "version", event,
        minimum=1, maximum=MAX_SNAPSHOT_VERSION,
    )


def _parse_user_upserted(data: dict[str, Any]) -> UserUpserted:
    """The wire form only; the value rules -- blank strings and a zero
    telegram id refused -- live in ONE place for both write paths,
    audience/sync.py apply_snapshot, so the PUT route and this event
    cannot disagree again (before F1.4 this parser refused an empty
    locale and the route accepted it)."""
    event = EVENT_USER_UPSERTED
    _closed(data, _USER_UPSERTED_FIELDS, event)

    recipient_id = _uuid(
        _require(data, "recipient_id", event), "recipient_id", event,
    )
    version = _version(data, event)
    telegram_id_raw = _require(data, "telegram_id", event)
    telegram_id = (
        None
        if telegram_id_raw is None
        else _int(
            telegram_id_raw, "telegram_id", event,
            minimum=MIN_TELEGRAM_ID, maximum=MAX_TELEGRAM_ID,
        )
    )
    email = _optional_string(
        _require(data, "email", event), "email", event,
        max_len=MAX_EMAIL_LEN,
    )
    locale = _optional_string(
        _require(data, "locale", event), "locale", event,
        max_len=MAX_LOCALE_LEN,
    )
    timezone = _optional_string(
        _require(data, "timezone", event), "timezone", event,
        max_len=MAX_TIMEZONE_LEN,
    )
    active = _bool(_require(data, "active", event), "active", event)

    return UserUpserted(
        recipient_id=recipient_id,
        version=version,
        telegram_id=telegram_id,
        email=email,
        locale=locale,
        timezone=timezone,
        active=active,
    )


def _parse_user_deleted(data: dict[str, Any]) -> UserDeleted:
    event = EVENT_USER_DELETED
    _closed(data, _USER_DELETED_FIELDS, event)
    return UserDeleted(
        recipient_id=_uuid(
            _require(data, "recipient_id", event), "recipient_id", event,
        ),
        version=_version(data, event),
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


def _parse_section_membership_changed(
    data: dict[str, Any],
) -> SectionMembershipChanged:
    event = EVENT_SECTION_MEMBERSHIP_CHANGED

    section_key = _string(
        _require(data, "section_key", event), "section_key", event,
        max_len=_MAX_SECTION_KEY_LEN,
    )
    # The label is carried because this event may be the FIRST mention
    # of a section: operators are hired before anybody writes in. The
    # handler create-or-finds by key, and an existing section keeps the
    # label it already has (create-or-find, not upsert), so a label sent
    # later never renames anything.
    section_label = _string(
        _require(data, "section_label", event), "section_label", event,
        max_len=_MAX_SECTION_LABEL_LEN,
    )
    operator_id = _uuid(
        _require(data, "operator_id", event), "operator_id", event,
    )
    member = _bool(_require(data, "member", event), "member", event)

    return SectionMembershipChanged(
        section_key=section_key,
        section_label=section_label,
        operator_id=operator_id,
        member=member,
    )



# The closed field set of reminder_cancel (F1.3). The two fields it
# had before -- correlation_key / correlation_value, a key INSIDE the
# letter and its value -- are refused by name: a producer still sending
# them must learn that the cancel reads the envelope now.
_REMINDER_CANCEL_FIELDS = frozenset({
    "v", "types", "correlation", "target_type", "target_value",
})


def _parse_reminder_cancel(data: dict[str, Any]) -> ReminderCancel:
    event = EVENT_REMINDER_CANCEL

    unknown = sorted(set(data) - _REMINDER_CANCEL_FIELDS)
    if unknown:
        raise ValidationError(
            f"{event}: unknown field(s) {', '.join(map(repr, unknown))}; "
            f"allowed: {', '.join(sorted(_REMINDER_CANCEL_FIELDS))}. "
            f"The cancel matches the envelope `correlation` of the jobs, "
            f"never a key of their action_data"
        )

    raw_types = _require(data, "types", event)
    if not isinstance(raw_types, list) or not raw_types:
        raise ValidationError(
            f"{event}: types must be a non-empty list of reminder "
            f"type keys"
        )
    types = [
        _string(t, "types[]", event, max_len=200) for t in raw_types
    ]

    correlation = _string(
        _require(data, "correlation", event),
        "correlation", event, max_len=MAX_CORRELATION_LEN,
    )

    target_type_raw = data.get("target_type")
    target_value_raw = data.get("target_value")
    target_type: str | None = None
    target_value: str | None = None
    if target_type_raw is None and target_value_raw is not None:
        raise ValidationError(
            f"{event}: target_value without target_type is ambiguous "
            f"-- send both target fields or neither"
        )
    if target_type_raw is not None and target_value_raw is None:
        raise ValidationError(
            f"{event}: target_type without target_value is ambiguous "
            f"-- send both target fields or neither"
        )
    if target_type_raw is not None:
        target_type, target_value = _validate_target(
            target_type_raw, target_value_raw, event,
        )

    return ReminderCancel(
        types=types,
        correlation=correlation,
        target_type=target_type,
        target_value=target_value,
    )
