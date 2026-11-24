# =============================================================================
# R-2 -- every bound of an event field is in the contract, by the same number
# =============================================================================
#
# deploy/INTEGRATION.md section 7 writes each event's fields in a table;
# tests/test_event_contract.py holds the FIELD NAMES to the parsers. This
# file holds the NUMBERS:
#
#   1 every range `a..b` and every "at most N" in a rule cell of section
#     7 is exactly the set BOUNDS names for that field, both ways -- a
#     constant changed without the document is red, and a number in the
#     document that no constant stands behind is red too
#     (test_every_cell_is_the_codes);
#   2 the parser stops at the number: a value AT the bound is taken, one
#     past it refused naming the field -- for every bound the parse
#     checks (TestTheParseStopsThere);
#   3 PUT /api/v1/recipients takes the bounds of the user_upserted table,
#     as section 5 says (test_put_takes_the_events_bounds);
#   4 every call in section 5's table of repeats is a route of the app,
#     at that path (test_the_repeat_table_names_real_routes).
#
# Before R-2 the document said group_key 1..200 against a column of 100,
# and named no bound for type, email, locale, timezone, telegram_id, the
# top of version, the length of a target or the list of a cancel; its
# repeat table named /inbox and /preferences, which are no routes.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 MAX_GROUP_KEY_LEN changed without the document ->
#      test_every_cell_is_the_codes[group_changed]
#   M2 the parse checks a literal instead of the constant (locale 9) ->
#      TestTheParseStopsThere (user_upserted.locale, past)
#   M3 a bound's number removed from the document -> the reverse
#      direction of test_every_cell_is_the_codes
#   M7 the length of types checked with >= (1000 refused) ->
#      TestTheParseStopsThere (reminder_cancel.types, at)
# =============================================================================

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from app.api.recipients import RecipientSnapshot
from app.core.constants import (
    MAX_ACTION_DATA_BYTES,
    MAX_BODY_LEN,
    MAX_CANCEL_TYPE_KEY_LEN,
    MAX_CANCEL_TYPES,
    MAX_CORRELATION_LEN,
    MAX_EMAIL_HTML_LEN,
    MAX_EMAIL_LEN,
    MAX_EMAIL_SUBJECT_LEN,
    MAX_EMAIL_TEXT_LEN,
    MAX_GROUP_KEY_LEN,
    MAX_IDEMPOTENCY_KEY_LEN,
    MAX_LOCALE_LEN,
    MAX_SNAPSHOT_VERSION,
    MAX_TARGET_VALUE_LEN,
    MAX_TELEGRAM_ID,
    MAX_TIMEZONE_LEN,
    MAX_TITLE_LEN,
    MAX_TYPE_KEY_LEN,
    MIN_TELEGRAM_ID,
)
from app.core.exceptions import ValidationError
from app.messaging.constants import MAX_SECTION_KEY_LEN, MAX_SECTION_LABEL_LEN
from app.profile.loader import MAX_DURATION_DAYS
from app.transport.events import (
    KNOWN_EVENTS,
    NotificationRequest,
    RejectedNotificationRequest,
    parse_event,
)

_DOC = (Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md").read_text(
    encoding="utf-8",
)
_CELL_NAME = re.compile(r"`([^`]+)`")
_RANGE = re.compile(r"(?<![\w.])(-?\d+)\.\.(-?\d+)(?![\w.])")
_AT_MOST = re.compile(r"\bat most (\d+)\b")

Bound = tuple[str, int, int]


def _range(low: int, high: int) -> Bound:
    return ("range", low, high)


def _at_most(high: int) -> Bound:
    return ("at most", 0, high)


# The bounds of section 7, field by field -- each the constant the code
# checks. A field absent here has no number in its row.
BOUNDS: dict[tuple[str, str], set[Bound]] = {
    ("notification_request", "idempotency_key"): {
        _range(1, MAX_IDEMPOTENCY_KEY_LEN)},
    ("notification_request", "type"): {_range(1, MAX_TYPE_KEY_LEN)},
    ("notification_request", "target_value"): {
        _range(1, MAX_GROUP_KEY_LEN), _at_most(MAX_TARGET_VALUE_LEN)},
    ("notification_request", "scheduled_at"): {_at_most(MAX_DURATION_DAYS)},
    ("notification_request", "correlation"): {_range(1, MAX_CORRELATION_LEN)},
    ("notification_request", "title"): {_range(1, MAX_TITLE_LEN)},
    ("notification_request", "body"): {_range(1, MAX_BODY_LEN)},
    ("notification_request", "action_data"): {_at_most(MAX_ACTION_DATA_BYTES)},
    ("notification_request", "email"): {
        _range(1, MAX_EMAIL_SUBJECT_LEN), _range(1, MAX_EMAIL_TEXT_LEN),
        _range(1, MAX_EMAIL_HTML_LEN)},
    ("reminder_cancel", "types"): {
        _range(1, MAX_CANCEL_TYPES), _range(1, MAX_CANCEL_TYPE_KEY_LEN)},
    ("reminder_cancel", "correlation"): {_range(1, MAX_CORRELATION_LEN)},
    ("user_upserted", "version"): {_range(1, MAX_SNAPSHOT_VERSION)},
    ("user_upserted", "telegram_id"): {_range(MIN_TELEGRAM_ID, MAX_TELEGRAM_ID)},
    ("user_upserted", "email"): {_range(1, MAX_EMAIL_LEN)},
    ("user_upserted", "locale"): {_range(1, MAX_LOCALE_LEN)},
    ("user_upserted", "timezone"): {_range(1, MAX_TIMEZONE_LEN)},
    ("user_deleted", "version"): {_range(1, MAX_SNAPSHOT_VERSION)},
    ("group_changed", "group_key"): {_range(1, MAX_GROUP_KEY_LEN)},
    ("section_membership_changed", "section_key"): {
        _range(1, MAX_SECTION_KEY_LEN)},
    ("section_membership_changed", "section_label"): {
        _range(1, MAX_SECTION_LABEL_LEN)},
}


def _section(number: str) -> str:
    start = _DOC.index(f"\n## {number}. ")
    end = _DOC.find("\n## ", start + 1)
    return _DOC[start:] if end == -1 else _DOC[start:end]


def rule_cells(text: str) -> dict[str, dict[str, str]]:
    """{event: {field: its rule cell}} from section 7: the first table
    under each event's heading, its first column and its last."""
    found: dict[str, dict[str, str]] = {}
    for block in re.split(r"\n### ", text)[1:]:
        heading, _, body = block.partition("\n")
        named = _CELL_NAME.findall(heading.split(" -- ", 1)[0])
        if len(named) != 1 or named[0] not in KNOWN_EVENTS:
            continue
        lines = body.splitlines()
        first = next(i for i, line in enumerate(lines) if line.startswith("|"))
        rows: list[str] = []
        for line in lines[first:]:
            if not line.startswith("|"):
                break
            rows.append(line)
        cells: dict[str, str] = {}
        for row in rows[2:]:
            columns = row.split("|")[1:-1]
            (field,) = _CELL_NAME.findall(columns[0])
            cells[field] = columns[-1]
        found[named[0]] = cells
    return found


def bounds_of(cell: str) -> set[Bound]:
    """The ranges and the "at most" numbers a rule cell names."""
    found = {_range(int(a), int(b)) for a, b in _RANGE.findall(cell)}
    found |= {_at_most(int(n)) for n in _AT_MOST.findall(cell)}
    return found


_CELLS = rule_cells(_section("7"))


class TestTheReaderItself:
    """The same gate on the instrument: a reader that found nothing
    would make every comparison below an empty one."""

    def test_it_reads_every_event_and_every_bound(self) -> None:
        assert set(_CELLS) == set(KNOWN_EVENTS)
        assert {(e, f) for e, cells in _CELLS.items() for f in cells} >= set(BOUNDS)
        assert bounds_of("string, 1..200 -- names") == {_range(1, 200)}
        assert bounds_of("-5..7, at most 9 bytes") == {
            _range(-5, 7), _at_most(9)}
        assert bounds_of("ISO 8601 with a zone (section 3a)") == set()


@pytest.mark.parametrize("event", sorted(KNOWN_EVENTS))
def test_every_cell_is_the_codes(event: str) -> None:
    """Both ways: each row names exactly the bounds of its constants, and
    a row with no constant behind it names none."""
    for field, cell in _CELLS[event].items():
        assert bounds_of(cell) == BOUNDS.get((event, field), set()), (
            f"{event}.{field}: the document says {sorted(bounds_of(cell))}"
        )


# ---------------------------------------------------------------------------
# The parse stops at the number
# ---------------------------------------------------------------------------


def _fields(event: str, data: dict[str, Any]) -> dict[str, str]:
    return {"event": event, "data": json.dumps(data)}


def _request(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "v": 1, "idempotency_key": f"key-{uuid4().hex}", "type": "unit_event",
        "target_type": "user", "target_value": str(uuid4()),
        "title": "T", "body": "B",
    }
    data.update(overrides)
    return data


def _snapshot(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "v": 1, "recipient_id": str(uuid4()), "version": 1,
        "telegram_id": None, "email": None, "locale": "en",
        "timezone": "Europe/Berlin", "active": True,
    }
    data.update(overrides)
    return data


def _padded_uuid(length: int) -> str:
    text = str(uuid4())
    return "-" * (length - len(text)) + text


def _parses(event: str, data: dict[str, Any]) -> None:
    parsed = parse_event(_fields(event, data))
    if event == "notification_request":
        assert isinstance(parsed, NotificationRequest), parsed


def _refused(event: str, data: dict[str, Any], field: str) -> None:
    if event == "notification_request" and field != "idempotency_key":
        parsed = parse_event(_fields(event, data))
        assert isinstance(parsed, RejectedNotificationRequest), parsed
        assert field in parsed.reason, parsed.reason
        return
    with pytest.raises(ValidationError) as caught:
        parse_event(_fields(event, data))
    assert field in str(caught.value)


# (event, field named by the refusal, the data with the value of a size)
_EDGES: list[tuple[str, str, int, Callable[[int], dict[str, Any]]]] = [
    ("notification_request", "idempotency_key", MAX_IDEMPOTENCY_KEY_LEN,
     lambda n: _request(idempotency_key="k" * n)),
    ("notification_request", "type", MAX_TYPE_KEY_LEN,
     lambda n: _request(type="t" * n)),
    ("notification_request", "target_value", MAX_TARGET_VALUE_LEN,
     lambda n: _request(target_value=_padded_uuid(n))),
    ("notification_request", "target_value", MAX_GROUP_KEY_LEN,
     lambda n: _request(target_type="group", target_value="g" * n)),
    ("notification_request", "correlation", MAX_CORRELATION_LEN,
     lambda n: _request(correlation="c" * n)),
    ("notification_request", "title", MAX_TITLE_LEN,
     lambda n: _request(title="t" * n)),
    ("notification_request", "body", MAX_BODY_LEN,
     lambda n: _request(body="b" * n)),
    ("notification_request", "email.subject", MAX_EMAIL_SUBJECT_LEN,
     lambda n: _request(email={"subject": "s" * n, "text": "x"})),
    ("notification_request", "email.text", MAX_EMAIL_TEXT_LEN,
     lambda n: _request(email={"subject": "s", "text": "x" * n})),
    ("notification_request", "email.html", MAX_EMAIL_HTML_LEN,
     lambda n: _request(email={"subject": "s", "text": "x", "html": "h" * n})),
    ("reminder_cancel", "types", MAX_CANCEL_TYPES,
     lambda n: {"v": 1, "correlation": "c",
                "types": [f"t{i}" for i in range(n)]}),
    ("reminder_cancel", "types[]", MAX_CANCEL_TYPE_KEY_LEN,
     lambda n: {"v": 1, "correlation": "c", "types": ["t" * n]}),
    ("reminder_cancel", "target_value", MAX_TARGET_VALUE_LEN,
     lambda n: {"v": 1, "correlation": "c", "types": ["t"],
                "target_type": "user", "target_value": _padded_uuid(n)}),
    ("reminder_cancel", "correlation", MAX_CORRELATION_LEN,
     lambda n: {"v": 1, "correlation": "c" * n, "types": ["t"]}),
    ("user_upserted", "version", MAX_SNAPSHOT_VERSION,
     lambda n: _snapshot(version=n)),
    ("user_upserted", "telegram_id", MAX_TELEGRAM_ID,
     lambda n: _snapshot(telegram_id=n)),
    ("user_upserted", "email", MAX_EMAIL_LEN,
     lambda n: _snapshot(email="e" * n)),
    ("user_upserted", "locale", MAX_LOCALE_LEN,
     lambda n: _snapshot(locale="l" * n)),
    ("user_upserted", "timezone", MAX_TIMEZONE_LEN,
     lambda n: _snapshot(timezone="z" * n)),
    ("user_deleted", "version", MAX_SNAPSHOT_VERSION,
     lambda n: {"v": 1, "recipient_id": str(uuid4()), "version": n}),
    ("group_changed", "group_key", MAX_GROUP_KEY_LEN,
     lambda n: {"v": 1, "group_key": "g" * n, "recipient_id": str(uuid4()),
                "member": True}),
    ("section_membership_changed", "section_key", MAX_SECTION_KEY_LEN,
     lambda n: {"v": 1, "section_key": "k" * n, "section_label": "L",
                "operator_id": str(uuid4()), "member": True}),
    ("section_membership_changed", "section_label", MAX_SECTION_LABEL_LEN,
     lambda n: {"v": 1, "section_key": "k", "section_label": "L" * n,
                "operator_id": str(uuid4()), "member": True}),
]


class TestTheParseStopsThere:
    @pytest.mark.parametrize(("event", "field", "bound", "build"), _EDGES,
                             ids=[f"{e}.{f}-{b}" for e, f, b, _ in _EDGES])
    def test_at_the_bound_and_past_it(
        self, event: str, field: str, bound: int,
        build: Callable[[int], dict[str, Any]],
    ) -> None:
        _parses(event, build(bound))
        _refused(event, build(bound + 1), field)

    def test_the_lower_end_of_telegram_id(self) -> None:
        """The one range whose low end is not 1."""
        _parses("user_upserted", _snapshot(telegram_id=MIN_TELEGRAM_ID))
        _refused("user_upserted", _snapshot(telegram_id=MIN_TELEGRAM_ID - 1),
                 "telegram_id")

    def test_repeats_count_in_the_list_of_types(self) -> None:
        """The list's length as sent: one key MAX_CANCEL_TYPES + 1 times
        is refused, though it names one type."""
        _refused("reminder_cancel", {
            "v": 1, "correlation": "c", "types": ["t"] * (MAX_CANCEL_TYPES + 1),
        }, "types")

    def test_every_bound_of_the_document_has_its_edge(self) -> None:
        """No bound of a string or a count left without its pair above:
        each (event, field) of BOUNDS that the parse checks is in
        _EDGES (scheduled_at, action_data: checked at intake and in
        validate_action_data, each with its own tests -- tests/
        test_size_limits.py, the REV-8 schedule ceiling)."""
        edged = {(e, f.split(".")[0].removesuffix("[]")) for e, f, _b, _ in _EDGES}
        elsewhere = {("notification_request", "scheduled_at"),
                     ("notification_request", "action_data")}
        assert set(BOUNDS) - elsewhere <= edged


# ---------------------------------------------------------------------------
# The synchronous path and the repeat table (section 5)
# ---------------------------------------------------------------------------


def _limits(name: str) -> set[Bound]:
    """The bounds the PUT body's field declares, read off its model: the
    low end its `ge`, or 1 for a string -- a blank one is refused on both
    paths alike (app/audience/sync.py apply_snapshot)."""
    found: dict[str, int] = {}
    for item in RecipientSnapshot.model_fields[name].metadata:
        for key in ("ge", "le", "max_length"):
            if getattr(item, key, None) is not None:
                found[key] = getattr(item, key)
    high = found.get("le", found.get("max_length"))
    assert high is not None, name
    return {_range(found.get("ge", 1), high)}


@pytest.mark.parametrize("name", ["version", "telegram_id", "email", "locale",
                                  "timezone"])
def test_put_takes_the_events_bounds(name: str) -> None:
    """Section 5: both write paths take the bounds of the user_upserted
    table -- the PUT body's constraints are those numbers."""
    said = " ".join(_section("5").split())
    assert "Both paths take the bounds of the `user_upserted` table" in said
    assert _limits(name) == bounds_of(_CELLS["user_upserted"][name])


def _shape(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "{}", path)


def test_the_repeat_table_names_real_routes() -> None:
    """Every call in the table of repeats is a route of the app at that
    path under /api/v1, with that method."""
    from app.main import app

    routes = {
        (method.upper(), _shape(path))
        for path, item in app.openapi()["paths"].items()
        for method in item
    }
    table = _DOC.split("| call | why a repeat is safe |", 1)[1].split("\n\n", 1)[0]
    calls = re.findall(r"`([A-Z]+) (/[^`]+)`", table)
    assert len(calls) >= 13
    missing = [f"{verb} {path}" for verb, path in calls
               if (verb, _shape(f"/api/v1{path}")) not in routes]
    assert missing == []
