# =============================================================================
# P3-1 Б3 -- the push stream's contract, held to the code both ways
# =============================================================================
#
# deploy/INTEGRATION.md section 10 is the one full copy of the push
# contract: the stream's name and cap, the fields of an entry, what each
# push_on value pushes, and the listener a product builds from it. Every
# fact here is read from the document and compared with the code -- a
# field, a value or a name on one side only turns this red.
#
# MUTATIONS these tests were written against:
#   M8  a field in the entry the document does not name (or the reverse)
#   M13 a push_on value in the profile schema without its row (or the
#       reverse)
# =============================================================================

import re
from pathlib import Path

from app.core.config import NUMERIC_BOUNDS, Settings
from app.engine.constants import NotificationStatus
from app.engine.journal import OUTCOME_STATUSES
from app.profile.loader import PUSH_ON
from app.transport.push_relay import PUSH_FIELDS, PUSH_FORMAT_VERSION, push_entry

_DOC = (
    Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md"
).read_text(encoding="utf-8")


def _section(heading: str) -> str:
    start = _DOC.index(f"{heading}\n")
    end = re.compile(r"^## ", re.M).search(_DOC, start + len(heading))
    assert end is not None
    return _DOC[start:end.start()]


_PUSH = _section("## 10. The push stream")


def _table(first_header: str) -> dict[str, str]:
    """The rows of the section's table whose first header is
    `first_header`: first cell (unquoted) -> second cell."""
    lines = _PUSH.splitlines()
    start = next(
        i for i, line in enumerate(lines)
        if line.startswith(f"| {first_header} |")
    )
    rows: dict[str, str] = {}
    for line in lines[start + 2:]:
        if not line.startswith("|"):
            break
        cells = [c.strip() for c in line.strip("|").split("|")]
        rows[cells[0].strip("`")] = cells[1]
    assert rows, f"the table under {first_header!r} is empty"
    return rows


def _defaults() -> dict[str, object]:
    return {
        name: field.default for name, field in Settings.model_fields.items()
    }


class TestTheEntry:
    def test_the_fields_are_the_code_s_both_ways(self) -> None:
        documented = set(_table("field"))
        assert documented == set(PUSH_FIELDS)
        assert set(push_entry("k")) == documented

    def test_the_version_is_the_code_s(self) -> None:
        assert f"`{PUSH_FORMAT_VERSION}`" in _table("field")["v"]

    def test_no_status_no_channel_never_the_letter(self) -> None:
        """The pair to the field set: the section says what an entry
        does NOT carry, and the code carries none of it."""
        assert "no status, no channel, never the letter" in _PUSH
        for absent in ("status", "channel", "title", "body", "action_data"):
            assert absent not in PUSH_FIELDS


class TestWhatPushes:
    def test_the_values_are_the_profile_s_both_ways(self) -> None:
        assert set(_table("`push_on`")) == set(PUSH_ON)

    def test_the_default_is_none(self) -> None:
        assert "the default" in _table("`push_on`")["none"]

    def test_an_outcome_is_every_status_but_the_two_named(self) -> None:
        row = _table("`push_on`")["outcome"]
        named = set(re.findall(r"`([a-z_]+)`", row))
        assert named == set(NotificationStatus) - OUTCOME_STATUSES
        assert named  # the pair: the row names them


class TestTheStream:
    def test_the_name_is_the_derived_one(self) -> None:
        defaults = _defaults()
        assert "`<COMMS_EVENTS_STREAM>:changes`" in _PUSH
        assert f"`{defaults['comms_events_stream']}:changes`" in _PUSH

    def test_the_cap_is_the_setting_and_its_default(self) -> None:
        assert "changes_stream_maxlen" in NUMERIC_BOUNDS
        assert "`CHANGES_STREAM_MAXLEN`" in _PUSH
        default = _defaults()["changes_stream_maxlen"]
        assert f"default {default}" in _PUSH
        assert default == 100_000


class TestTheListener:
    def _recipe(self) -> str:
        (block,) = re.findall(r"```python\n(.*?)```", _PUSH, re.S)
        return str(block)

    def test_the_recipe_is_tens_of_lines(self) -> None:
        """Spec §7.8: a listener that fits in tens of lines."""
        lines = [line for line in self._recipe().splitlines() if line.strip()]
        assert 10 <= len(lines) <= 40

    def test_the_recipe_reads_the_documented_fields_and_stream(self) -> None:
        recipe = self._recipe()
        for field in PUSH_FIELDS:
            assert f'b"{field}"' in recipe
        assert ':changes"' in recipe
        assert f'b"{PUSH_FORMAT_VERSION}"' in recipe

    def test_the_recipe_reads_before_it_acknowledges(self) -> None:
        recipe = self._recipe()
        assert recipe.index("await read_job(") < recipe.index("xack(")

    def test_the_recipe_reconciles_before_it_follows(self) -> None:
        recipe = self._recipe()
        assert recipe.index("await reconcile()") < recipe.index("xreadgroup(")
