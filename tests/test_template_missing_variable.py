# =============================================================================
# TPL-1 -- a variable the job does not carry is printed as its placeholder
# =============================================================================
#
# History, because every test here was rewritten: before G5 the engine
# rendered with SafeDict, whose __missing__ printed a missing TOP-LEVEL
# variable back as "{name}" -- and a path into a dict without the key
# raised KeyError out of render, burning the delivery's attempts. G5
# (3f76ca6) made every unresolved path fall back to the stored text, as
# with no template; these tests asserted that, and were right while it
# was the rule. The owner withdrew G5 (4 November 2026, TPL-1): louder is
# better than quieter -- the field prints "{path}" where the value would
# stand, a JSON null included, the rest of the field renders, and the
# job's journal names the paths (tests/test_template_placeholder.py
# holds the journal). What G5 got right stays: no KeyError leaves
# render, no value of the letter reaches a log line; a type with no
# template still takes the stored text.
#
# Every template here goes through the REAL loader (load_profile /
# load_overlay -- the startup dry run), so each row of the grid is a
# template a profile can actually carry. Attribute access, a field
# inside a spec and an oversized width are not rows: the loader refuses
# them (tests/test_template_placeholder.py, TestTheLoader).
#
# MUTATIONS (the TPL-1 list lives in tests/test_template_placeholder.py):
#   T1 render back to None on an unresolved path -> TestTheGrid, callers
#   T2 null printed as None                      -> test_null_is_a_placeholder
#   T7 a value of the letter in the log line     -> test_no_value_of_*
# =============================================================================

import shutil
from collections.abc import MutableMapping, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import httpx
import pytest
import yaml
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.engine.constants import DeliveryChannel
from app.engine.formatters import EmailFormatter, TelegramFormatter
from app.engine.models import Notification, NotificationDelivery
from app.engine.template_engine import render, render_field
from app.profile.loader import (
    FileOverlaySource,
    FileProfileSource,
    install_overlay,
    install_profile,
    load_overlay,
    load_profile,
)
from app.profile.registry import registry
from tests.conftest import FIXTURE_PROFILE_DIR
from tests.helpers import next_telegram_id

_TYPE = "unit_event_telegram_email"
_ROOT = Path(__file__).resolve().parent.parent
_DOC = (_ROOT / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")

# A value of the letter: it must never reach the warning (H8).
_SENTINEL = "letter-value-7f3a"


def _install(
    tmp_path: Path,
    sheet: dict[str, dict[str, str]],
    overlay: dict[str, dict[str, str]] | None = None,
) -> None:
    """The fixture's types with `sheet` as _TYPE's en templates (and an
    overlay over it), through the startup loader, into the registry."""
    base_dir = tmp_path / "profile"
    (base_dir / "templates").mkdir(parents=True)
    shutil.copy(FIXTURE_PROFILE_DIR / "types.yaml", base_dir / "types.yaml")
    (base_dir / "templates" / "en.yaml").write_text(
        yaml.safe_dump({_TYPE: sheet}), encoding="utf-8",
    )
    base = load_profile(FileProfileSource(base_dir))
    loaded = None
    if overlay is not None:
        overlay_dir = tmp_path / "overlay"
        overlay_dir.mkdir()
        (overlay_dir / "en.yaml").write_text(
            yaml.safe_dump({_TYPE: overlay}), encoding="utf-8",
        )
        loaded = load_overlay(FileOverlaySource(overlay_dir), base)
    registry.reset()
    install_profile(base)
    if loaded is not None:
        install_overlay(loaded)


def _render(field: str, variables: dict[str, Any]) -> str | None:
    channel = "email" if field in ("subject", "email_body") else "telegram"
    return render(
        _TYPE, channel, "body" if field == "email_body" else field,
        locale="en", variables=variables,
    )


def _warnings(
    logs: Sequence[MutableMapping[str, Any]],
    event: str = "template_render_error",
) -> list[MutableMapping[str, Any]]:
    return [e for e in logs if e["event"] == event]


def _unresolved(field: str, variables: dict[str, Any]) -> tuple[str, ...]:
    rendered = render_field(
        _TYPE, "telegram", field, locale="en", variables=variables,
    )
    assert rendered is not None
    return rendered.unresolved


# ---------------------------------------------------------------------------
# The grid: every reachable state of a field path, on render itself
# ---------------------------------------------------------------------------


# (template, variables, what the field prints, the unresolved paths)
_MISSING = [
    ("{x}", {}, "{x}", ("x",)),
    ("{x!r}", {}, "{x}", ("x",)),
    ("{x:>9}", {}, "{x}", ("x",)),
    ("{x:,.2f}", {}, "{x}", ("x",)),
    ("{x[y]}", {}, "{x[y]}", ("x[y]",)),
    ("{x[0]}", {}, "{x[0]}", ("x[0]",)),
    ("{x[y]}", {"x": {}}, "{x[y]}", ("x[y]",)),
    ("{x[y]}", {"x": {"z": _SENTINEL}}, "{x[y]}", ("x[y]",)),
    ("{x[y]}", {"x": _SENTINEL}, "{x[y]}", ("x[y]",)),
    ("{x[y]}", {"x": 7}, "{x[y]}", ("x[y]",)),
    ("{x[y]}", {"x": [_SENTINEL]}, "{x[y]}", ("x[y]",)),
    ("{x[y]}", {"x": None}, "{x[y]}", ("x[y]",)),
    ("{x[y]}", {"x": {"y": None}}, "{x[y]}", ("x[y]",)),
    ("{p} {q}", {}, "{p} {q}", ("p", "q")),
    ("{x} {x}", {}, "{x} {x}", ("x",)),
    ("{a} {x}", {"a": "A"}, "A {x}", ("x",)),
]


class TestTheGrid:
    @pytest.mark.parametrize(("template", "variables", "printed", "paths"), _MISSING)
    def test_a_path_that_does_not_resolve_prints_its_placeholder(
        self,
        tmp_path: Path,
        template: str,
        variables: dict[str, Any],
        printed: str,
        paths: tuple[str, ...],
    ) -> None:
        """The path in braces where the value would stand -- never a
        fragment such as "{" (what "{x[0]}" printed with SafeDict), never
        a conversion or a spec -- the rest rendered, and the paths named,
        once each, in the template's order, by the template's text."""
        _install(tmp_path, {"telegram": {"body": template}})
        with capture_logs() as logs:
            assert _render("body", variables) == printed
        assert _unresolved("body", variables) == paths
        assert _warnings(logs) == []
        (noted,) = _warnings(logs, "template_field_unresolved")
        assert noted["variables"] == list(paths)
        assert (noted["type"], noted["channel"], noted["field"], noted["locale"]) \
            == (_TYPE, "telegram", "body", "en")
        assert _SENTINEL not in repr(logs)

    @pytest.mark.parametrize(
        ("template", "variables", "rendered"),
        [
            ("{x}", {"x": "v"}, "v"),
            ("{x!r}", {"x": "v"}, "'v'"),
            ("{x:>3}", {"x": "v"}, "  v"),
            ("{x:,.2f}", {"x": 1234.5}, "1,234.50"),
            ("{x[y]}", {"x": {"y": "v"}}, "v"),
            ("{x[0]}", {"x": ["v"]}, "v"),
            ("{p} {q}", {"p": "1", "q": "2"}, "1 2"),
            ("{x}", {"x": ""}, ""),
            ("{x}", {"x": 0}, "0"),
            ("{x}", {"x": False}, "False"),
        ],
    )
    def test_the_pair_every_path_resolves_and_renders(
        self,
        tmp_path: Path,
        template: str,
        variables: dict[str, Any],
        rendered: str,
    ) -> None:
        """The pair: the same templates with every variable present
        render as they always did, nothing noted. An empty string, a 0
        and a false are values, not a missing variable -- only null is."""
        _install(tmp_path, {"telegram": {"body": template}})
        with capture_logs() as logs:
            assert _render("body", variables) == rendered
        assert _unresolved("body", variables) == ()
        assert _warnings(logs) == []
        assert _warnings(logs, "template_field_unresolved") == []

    def test_a_broken_spec_over_present_variables_has_no_name(
        self, tmp_path: Path,
    ) -> None:
        """Unchanged: a format spec the value refuses is None with its
        class, and no `variable` -- every path resolves."""
        _install(tmp_path, {"telegram": {"body": "{x:,.2f}"}})
        with capture_logs() as logs:
            assert _render("body", {"x": _SENTINEL}) is None
        (warning,) = _warnings(logs)
        assert warning["error_class"] == "ValueError"
        assert "variable" not in warning
        assert _SENTINEL not in repr(logs)

    def test_null_is_a_placeholder(self, tmp_path: Path) -> None:
        """TPL-1: a JSON null is a variable the job does not carry. It
        printed "[None]" until now -- str.format's word for Python's None,
        which no letter means (test_null_is_a_present_variable asserted
        that; the owner's decision withdrew it)."""
        _install(tmp_path, {"telegram": {"body": "[{x}]"}})
        assert _render("body", {"x": None}) == "[{x}]"
        assert _unresolved("body", {"x": None}) == ("x",)

    def test_no_template_is_still_quiet(self, tmp_path: Path) -> None:
        """Unchanged: no template at all is None without a warning --
        the caller takes the stored text."""
        _install(tmp_path, {"telegram": {"title": "{title}"}})
        with capture_logs() as logs:
            assert _render("body", {}) is None
        assert _warnings(logs) == []
        assert _warnings(logs, "template_field_unresolved") == []

    def test_no_keyerror_leaves_render(self, tmp_path: Path) -> None:
        """A path into a dict without the key raised KeyError out of
        render before G5; it prints its placeholder now."""
        _install(tmp_path, {"telegram": {"body": "{user[name]}"}})
        assert _render("body", {"user": {"other": "v"}}) == "{user[name]}"

    def test_no_value_of_the_letter_reaches_the_log(
        self, tmp_path: Path,
    ) -> None:
        """H8 sentinel: the letter's values -- of the variables present,
        and of the dict the missing key was looked up in -- appear in no
        log line and in no unresolved path; only the template's text
        does."""
        _install(tmp_path, {"telegram": {"body": "{title} {user[name]}"}})
        variables = {"title": _SENTINEL, "user": {_SENTINEL: _SENTINEL}}
        with capture_logs() as logs:
            assert _render("body", variables) == f"{_SENTINEL} {{user[name]}}"
        assert _unresolved("body", variables) == ("user[name]",)
        (noted,) = _warnings(logs, "template_field_unresolved")
        assert noted["variables"] == ["user[name]"]
        assert _SENTINEL not in repr(logs)


# ---------------------------------------------------------------------------
# The overlay (PO-1): an overlay leaf behaves as a base leaf
# ---------------------------------------------------------------------------


class TestTheOverlay:
    def test_an_overlay_leaf_with_a_missing_variable_prints_it(
        self, tmp_path: Path,
    ) -> None:
        """The overlay may use only the variables the base already has
        for the type (PO-1); one of them missing from the job prints its
        placeholder and is named, exactly as in the base (it fell back
        to the stored text under G5)."""
        _install(
            tmp_path,
            {"telegram": {"body": "{body} [{extra}]"}},
            overlay={"telegram": {"body": "{extra}!"}},
        )
        assert _render("body", {"body": "B"}) == "{extra}!"
        assert _unresolved("body", {"body": "B"}) == ("extra",)

    def test_the_pair_the_overlay_leaf_renders(self, tmp_path: Path) -> None:
        _install(
            tmp_path,
            {"telegram": {"body": "{body} [{extra}]"}},
            overlay={"telegram": {"body": "{extra}!"}},
        )
        assert _render("body", {"body": "B", "extra": "E"}) == "E!"


# ---------------------------------------------------------------------------
# The callers: each field takes what it takes without a template
# ---------------------------------------------------------------------------


class _Bot:
    """An aiogram Bot stand-in recording send_message kwargs."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def send_message(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return SimpleNamespace(message_id=1)


def _job(**action_data: Any) -> Notification:
    return Notification(
        id=UUID(int=1), type=_TYPE, lane="normal", title="Hello",
        body="World", target_type="user", target_value="*",
        action_data=action_data or None,
    )


_LINK = {"action": "open_practice", "params": {"practice_id": "p1"}}


async def _telegram(job: Notification) -> dict[str, Any]:
    bot = _Bot()
    formatter = TelegramFormatter(bot=bot, bot_url="https://t.me/comms_testbot")
    recipient = Recipient(telegram_id=next_telegram_id(), locale="en", active=True)
    assert await formatter.deliver(
        job, NotificationDelivery(channel=DeliveryChannel.TELEGRAM), recipient,
    )
    (call,) = bot.calls
    return call


async def _email(job: Notification) -> dict[str, str]:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"id": "<g5.1@mg.test>"})

    formatter = EmailFormatter(
        client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        api_base_url="https://api.mailgun.test",
        api_key="key-" + "g5-not-a-secret",
        domain="mg.test",
        from_address="comms@mg.test",
    )
    recipient = Recipient(email="user@example.test", locale="en", active=True)
    assert await formatter.deliver(
        job,
        NotificationDelivery(
            id=UUID(int=2), recipient_id=UUID(int=3),
            channel=DeliveryChannel.EMAIL,
        ),
        recipient,
    )
    (request,) = requests
    return dict(httpx.QueryParams(request.content.decode()))


class TestTheCallers:
    """By the ASSEMBLED value the person receives (§10 of the line):
    every field a template renders prints its placeholders -- the
    telegram title, body and button, the email subject and body. Under
    G5 each fell back to its stored text or default label (the button to
    "Open"); TPL-1 decided they print the hole."""

    async def test_telegram_title_and_body_print_the_placeholders(
        self, tmp_path: Path,
    ) -> None:
        _install(tmp_path, {"telegram": {"title": "{titel}", "body": "{x}"}})
        call = await _telegram(_job())
        assert call["text"] == "{titel}\n\n{x}"

    async def test_the_pair_telegram_renders_its_sheet(
        self, tmp_path: Path,
    ) -> None:
        _install(tmp_path, {"telegram": {"title": "{titel}", "body": "{x}"}})
        call = await _telegram(_job(titel="T", x="X"))
        assert call["text"] == "T\n\nX"

    async def test_the_button_prints_the_placeholder(
        self, tmp_path: Path,
    ) -> None:
        """The button renders with the RAW variables (a plain-text
        label): the placeholder goes as it is."""
        _install(tmp_path, {"telegram": {"button_text": "{user[name]}"}})
        call = await _telegram(_job(**_LINK, user={"other": "v"}))
        (row,) = call["reply_markup"].inline_keyboard
        assert row[0].text == "{user[name]}"

    async def test_the_pair_the_button_renders_its_label(
        self, tmp_path: Path,
    ) -> None:
        _install(tmp_path, {"telegram": {"button_text": "{user[name]}"}})
        call = await _telegram(_job(**_LINK, user={"name": "Zo"}))
        (row,) = call["reply_markup"].inline_keyboard
        assert row[0].text == "Zo"

    async def test_email_subject_and_body_print_the_placeholders(
        self, tmp_path: Path,
    ) -> None:
        _install(
            tmp_path,
            {"email": {"subject": "{user[name]}", "body": "{titel}"}},
        )
        fields = await _email(_job(user={}))
        assert fields["subject"] == "{user[name]}"
        assert fields["text"] == "{titel}"

    async def test_the_pair_email_renders_its_sheet(
        self, tmp_path: Path,
    ) -> None:
        _install(
            tmp_path,
            {"email": {"subject": "{user[name]}", "body": "{titel}"}},
        )
        fields = await _email(_job(user={"name": "Zo"}, titel="T"))
        assert fields["subject"] == "Zo"
        assert fields["text"] == "T"


# ---------------------------------------------------------------------------
# The contract: deploy/INTEGRATION.md, section 3
# ---------------------------------------------------------------------------


def _section(heading: str) -> str:
    start = _DOC.index(heading)
    return _DOC[start:_DOC.index("\n## ", start + 1)]


class TestTheContract:
    """The product learns from INTEGRATION.md what is printed without a
    variable; both ends are held here: the sentence in the document, the
    behaviour in the code. The G5 sentence ("... is not printed", the
    field falling back as with no template) is withdrawn -- its tests
    were right while it was the rule."""

    _SENTENCE = (
        "**A variable the job does not carry is printed as its placeholder.**"
    )

    def test_the_sentence_is_in_the_profile_section(self) -> None:
        section = _section("## 3. The product profile")
        assert self._SENTENCE in section
        assert "**A variable the job does not carry is not printed.**" not in _DOC

    def test_the_references_resolve(self) -> None:
        """The places the paragraph sends the reader to exist: the
        stored-text fallback of a type with no template, the journal's
        field in section 8."""
        paragraph = _section("## 3. The product profile")
        paragraph = paragraph[paragraph.index(self._SENTENCE):]
        paragraph = paragraph[:paragraph.index("\n\n")]
        assert '"What it can change"' in paragraph
        assert '"What email reads from the profile" in §3a' in paragraph
        assert "`template_unresolved`, section 8" in paragraph
        assert "**What it can change:" in _section("## 3. The product profile")
        assert "the profile has no template for\na field" in _DOC
        email = _section("## 3a. How a product declares its channels")
        assert "**What email reads from the profile.**" in email
        assert "falls back to the stored body" in email
        assert "| `template_unresolved` |" in _section("## 8. Reading a job by its key")

    async def test_the_code_does_what_the_sentence_says(
        self, tmp_path: Path,
    ) -> None:
        """The other end: a field with a missing variable prints the
        placeholder; the same field with no template is the stored text."""
        _install(tmp_path, {"telegram": {"body": "{absent}"}})
        missing = await _telegram(_job())
        _install(tmp_path / "none", {"telegram": {"title": "{title}"}})
        without = await _telegram(_job())
        assert missing["text"].split("\n\n")[1] == "{absent}"
        assert without["text"].split("\n\n")[1] == "World"
