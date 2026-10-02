# =============================================================================
# D1 / R5 -- the event contract is written once and held to the code.
# =============================================================================
#
# deploy/INTEGRATION.md section 7 carries one heading and one table per
# event; the first column names the fields, one per row. This file
# holds every table to the parser's closed field set (EVENT_FIELDS),
# both ways, and holds the set of tables to the set of events.
#
# MUTATIONS:
#   M14 a field added to a parser's set, not to the document
#       -> test_every_table_equals_its_parser
#   M15 a row added to a table, not to the parser -> the same test
# =============================================================================

import re
from pathlib import Path

import pytest

from app.transport.events import EVENT_FIELDS, KNOWN_EVENTS

_DOC = Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md"
_CELL_NAME = re.compile(r"`([^`]+)`")


def _section_7(text: str) -> str:
    start = text.index("\n## 7. ")
    end = text.find("\n## ", start + 1)
    return text[start:] if end == -1 else text[start:end]


def tables_by_event(text: str) -> dict[str, list[str]]:
    """{event: [field, ...]} from section 7: a `### `<event>`` heading,
    then the first table under it, its first column. A cell must name
    exactly one field; a field named twice in one table is kept twice,
    so the comparison below sees it."""
    found: dict[str, list[str]] = {}
    blocks = re.split(r"\n### ", _section_7(text))[1:]
    for block in blocks:
        heading, _, body = block.partition("\n")
        named = _CELL_NAME.findall(heading.split(" -- ", 1)[0])
        if len(named) != 1 or named[0] not in KNOWN_EVENTS:
            continue
        event = named[0]
        assert event not in found, f"{event}: two headings in section 7"
        rows = [line for line in body.splitlines() if line.startswith("|")]
        assert len(rows) >= 3, f"{event}: no table under its heading"
        header, separator, *data = rows
        assert header.split("|")[1].strip() == "field", f"{event}: first column"
        assert set(separator.replace("|", "").strip()) <= {"-", " "}
        fields: list[str] = []
        for row in data:
            cell = row.split("|")[1]
            names = _CELL_NAME.findall(cell)
            assert len(names) == 1, f"{event}: one field per row, got {cell!r}"
            fields.append(names[0])
        found[event] = fields
    return found


def test_every_event_has_its_table_and_only_those() -> None:
    """done-when (4): the test sees all six events, not zero."""
    tables = tables_by_event(_DOC.read_text(encoding="utf-8"))
    assert len(KNOWN_EVENTS) == 6
    assert set(tables) == set(KNOWN_EVENTS) == set(EVENT_FIELDS)


@pytest.mark.parametrize("event", sorted(KNOWN_EVENTS))
def test_every_table_equals_its_parser(event: str) -> None:
    """done-when (2): the table names exactly the parser's set -- a
    field in the code without the document fails, and the reverse; a
    field named twice fails too."""
    fields = tables_by_event(_DOC.read_text(encoding="utf-8"))[event]
    assert len(fields) == len(set(fields)), f"{event}: a field named twice"
    assert set(fields) == EVENT_FIELDS[event]


class TestTheReaderItself:
    """The same gate on the tool: a reader that found nothing, or that
    accepted a doubled cell, would keep the tests above green."""

    _TEXT = (
        "\n## 7. The event protocol\n\n### `group_changed` -- membership\n\n"
        "| field | rule |\n|---|---|\n| `v` | `1` |\n| `member` | bool |\n"
        "\n## 8. Next\n\n### `user_deleted` -- outside section 7\n\n"
        "| field | rule |\n|---|---|\n| `v` | `1` |\n"
    )

    def test_it_reads_a_table(self) -> None:
        assert tables_by_event(self._TEXT) == {"group_changed": ["v", "member"]}

    def test_it_stops_at_the_next_section(self) -> None:
        assert "user_deleted" not in tables_by_event(self._TEXT)

    def test_two_fields_in_one_cell_fail(self) -> None:
        text = self._TEXT.replace("| `member` | bool |", "| `member`, `v` | bool |")
        with pytest.raises(AssertionError, match="one field per row"):
            tables_by_event(text)


def test_the_header_lists_no_field() -> None:
    """done-when (1): the module header of events.py carries no field
    list of any event -- no schema row in the form the old copy had
    ("#     <field>  <type> -- required"), no SCHEMAS block. A field named
    in prose about mechanics (the KNOWN CEILING names idempotency_key)
    is not a list. The pair: the reference to section 7 is there, and
    the row pattern does match the old form."""
    source = (
        Path(__file__).resolve().parents[1] / "app" / "transport" / "events.py"
    ).read_text(encoding="utf-8")
    header = source.split("\n# " + "=" * 77 + "\n", 2)[1]
    assert "deploy/INTEGRATION.md" in header and "section 7" in header
    assert "SCHEMAS" not in header
    fields = {field for fields in EVENT_FIELDS.values() for field in fields}
    row = re.compile(r"^#\s{3,}([a-z_]+)\s{2,}\S", re.MULTILINE)
    old_form = "#     group_key    str 1..200  -- required, opaque to comms"
    assert row.search(old_form), "the pattern must see the old form"
    listed = {m.group(1) for m in row.finditer(header)} & fields
    assert listed == set()
