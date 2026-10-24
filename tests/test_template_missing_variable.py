# =============================================================================
# G5 -- a variable the job does not carry is never printed.
# =============================================================================
#
# Before G5 the template engine rendered with SafeDict, whose __missing__
# printed a missing variable back as "{name}": a typo in a template
# ({titel}) or a variable the product did not send reached the person as
# braces. A path into a dict that lacks the key ({user[name]}, `user`
# without `name`) raised KeyError out of render -- uncaught, it burned the
# delivery's attempts. Now every field path that does not resolve takes
# the broken-format-spec branch: render returns None with a warning that
# names the path, and the caller falls back exactly as with no template.
#
# Every template here goes through the REAL loader (load_profile /
# load_overlay -- the startup dry run), so each row of the grid is a
# template a profile can actually carry. Attribute access ({x.attr}) is
# not a row: the loader refuses it at startup for the base and for the
# overlay alike (app/profile/loader.py _leaf_complaint).
#
# MUTATIONS (tests/test_race_by_constraint.py has M1-M4, M10):
#   M5 KeyError not caught in render -> TestTheCallers button / email
#   M6 SafeDict restored             -> TestTheGrid top-level rows
#   M7 no name for the warning       -> every `variable` assertion
#   M8 a value in the warning        -> test_no_value_of_the_letter_*
#   M9 the contract sentence removed -> TestTheContract
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
from app.engine.template_engine import render
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
) -> list[MutableMapping[str, Any]]:
    return [e for e in logs if e["event"] == "template_render_error"]


# ---------------------------------------------------------------------------
# The grid: every reachable state of a field path, on render itself
# ---------------------------------------------------------------------------


# (template, variables, the name of the path that does not resolve)
_MISSING = [
    ("{x}", {}, "x"),
    ("{x!r}", {}, "x"),
    ("{x:>9}", {}, "x"),
    ("{x:,.2f}", {}, "x"),
    ("{x[y]}", {}, "x[y]"),
    ("{x[0]}", {}, "x[0]"),
    ("{x[y]}", {"x": {}}, "x[y]"),
    ("{x[y]}", {"x": {"z": _SENTINEL}}, "x[y]"),
    ("{x[y]}", {"x": _SENTINEL}, "x[y]"),
    ("{x[y]}", {"x": 7}, "x[y]"),
    ("{x[y]}", {"x": [_SENTINEL]}, "x[y]"),
    ("{a:{w}}", {"a": _SENTINEL}, "w"),
    ("{p} {q}", {}, "p"),
    ("{x} {x}", {}, "x"),
    ("{a} {x}", {"a": _SENTINEL}, "x"),
]


class TestTheGrid:
    @pytest.mark.parametrize(("template", "variables", "name"), _MISSING)
    def test_a_path_that_does_not_resolve_is_none_and_named(
        self,
        tmp_path: Path,
        template: str,
        variables: dict[str, Any],
        name: str,
    ) -> None:
        """None -- never braces, never a fragment such as "{" (what
        "{x[0]}" printed with SafeDict) -- and a warning naming the
        first path that does not resolve, by the template's text."""
        _install(tmp_path, {"telegram": {"body": template}})
        with capture_logs() as logs:
            assert _render("body", variables) is None
        (warning,) = _warnings(logs)
        assert warning["variable"] == name
        assert warning["type"] == _TYPE
        assert warning["channel"] == "telegram"
        assert warning["field"] == "body"
        assert warning["locale"] == "en"
        assert warning["error_class"]
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
            ("{a:{w}}", {"a": "v", "w": ">2"}, " v"),
            ("{p} {q}", {"p": "1", "q": "2"}, "1 2"),
            ("{x}", {"x": ""}, ""),
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
        render as they always did, without a warning. An empty string is
        a present variable, not a missing one."""
        _install(tmp_path, {"telegram": {"body": template}})
        with capture_logs() as logs:
            assert _render("body", variables) == rendered
        assert _warnings(logs) == []

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

    def test_null_is_a_present_variable(self, tmp_path: Path) -> None:
        """Unchanged, and not this delivery's business: a JSON null is a
        value, printed by str.format as "None" (comms does not interpret
        the letter's values -- an observation in the G5 report)."""
        _install(tmp_path, {"telegram": {"body": "[{x}]"}})
        assert _render("body", {"x": None}) == "[None]"

    def test_no_template_is_still_quiet(self, tmp_path: Path) -> None:
        """Unchanged: no template at all is None without a warning."""
        _install(tmp_path, {"telegram": {"title": "{title}"}})
        with capture_logs() as logs:
            assert _render("body", {}) is None
        assert _warnings(logs) == []

    def test_no_keyerror_leaves_render(self, tmp_path: Path) -> None:
        """A path into a dict without the key raised KeyError out of
        render before G5 (SafeDict covered the top level only)."""
        _install(tmp_path, {"telegram": {"body": "{user[name]}"}})
        assert _render("body", {"user": {"other": "v"}}) is None

    def test_no_value_of_the_letter_reaches_the_warning(
        self, tmp_path: Path,
    ) -> None:
        """H8 sentinel: the letter's values -- of the variables present,
        and of the dict the missing key was looked up in -- appear
        nowhere in the warning; only the template's own text does."""
        _install(tmp_path, {"telegram": {"body": "{title} {user[name]}"}})
        with capture_logs() as logs:
            assert _render(
                "body", {"title": _SENTINEL, "user": {_SENTINEL: _SENTINEL}},
            ) is None
        (warning,) = _warnings(logs)
        assert warning["variable"] == "user[name]"
        assert _SENTINEL not in repr(logs)


# ---------------------------------------------------------------------------
# The overlay (PO-1): an overlay leaf behaves as a base leaf
# ---------------------------------------------------------------------------


class TestTheOverlay:
    def test_an_overlay_leaf_with_a_missing_variable_falls_back(
        self, tmp_path: Path,
    ) -> None:
        """The overlay may use only the variables the base already has
        for the type (PO-1); one of them missing from the job gives None
        and its name, exactly as in the base."""
        _install(
            tmp_path,
            {"telegram": {"body": "{body} [{extra}]"}},
            overlay={"telegram": {"body": "{extra}!"}},
        )
        with capture_logs() as logs:
            assert _render("body", {"body": "B"}) is None
        (warning,) = _warnings(logs)
        assert warning["variable"] == "extra"

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
    """By the ASSEMBLED value the person receives (§10 of the line)."""

    async def test_telegram_title_and_body_take_the_stored_texts(
        self, tmp_path: Path,
    ) -> None:
        _install(tmp_path, {"telegram": {"title": "{titel}", "body": "{x}"}})
        call = await _telegram(_job())
        assert call["text"] == "<b>Hello</b>\n\nWorld"

    async def test_the_pair_telegram_renders_its_sheet(
        self, tmp_path: Path,
    ) -> None:
        _install(tmp_path, {"telegram": {"title": "{titel}", "body": "{x}"}})
        call = await _telegram(_job(titel="T", x="X"))
        assert call["text"] == "T\n\nX"

    async def test_the_button_takes_its_default_label(
        self, tmp_path: Path,
    ) -> None:
        """The button renders with the RAW variables, so `user` stays a
        dict and the missing key was a KeyError out of render -- out of
        deliver -- before G5."""
        _install(tmp_path, {"telegram": {"button_text": "{user[name]}"}})
        call = await _telegram(_job(**_LINK, user={"other": "v"}))
        (row,) = call["reply_markup"].inline_keyboard
        assert row[0].text == "Open"
        assert "{" not in call["text"]

    async def test_the_pair_the_button_renders_its_label(
        self, tmp_path: Path,
    ) -> None:
        _install(tmp_path, {"telegram": {"button_text": "{user[name]}"}})
        call = await _telegram(_job(**_LINK, user={"name": "Zo"}))
        (row,) = call["reply_markup"].inline_keyboard
        assert row[0].text == "Zo"

    async def test_email_subject_and_body_take_the_stored_texts(
        self, tmp_path: Path,
    ) -> None:
        _install(
            tmp_path,
            {"email": {"subject": "{user[name]}", "body": "{titel}"}},
        )
        fields = await _email(_job(user={}))
        assert fields["subject"] == "Hello"
        assert fields["text"] == "World"

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
    variable; the sentence points at the fallbacks rather than copying
    them, and both ends are held here: the sentence and its references
    in the document, the behaviour in the code (the grid above)."""

    _SENTENCE = "**A variable the job does not carry is not printed.**"

    def test_the_sentence_is_in_the_profile_section(self) -> None:
        assert self._SENTENCE in _section("## 3. The product profile")

    def test_the_references_resolve(self) -> None:
        """The two places the sentence sends the reader to exist, and
        they describe the stored-text fallback."""
        paragraph = _section("## 3. The product profile")
        paragraph = paragraph[paragraph.index(self._SENTENCE):]
        paragraph = paragraph[:paragraph.index("\n\n")]
        assert '"What it can change"' in paragraph
        assert '"What email reads from the profile" in §3a' in paragraph
        assert "**What it can change:" in _section("## 3. The product profile")
        assert "the profile has no template for\na field" in _DOC
        email = _section("## 3a. How a product declares its channels")
        assert "**What email reads from the profile.**" in email
        assert "falls back to the stored body" in email

    async def test_the_code_does_what_the_sentence_says(
        self, tmp_path: Path,
    ) -> None:
        """The other end: a field with a missing variable comes out as
        the same field with no template -- here, the telegram body."""
        _install(tmp_path, {"telegram": {"body": "{absent}"}})
        missing = await _telegram(_job())
        _install(tmp_path / "none", {"telegram": {"title": "{title}"}})
        without = await _telegram(_job())
        assert missing["text"].split("\n\n")[1] == "World"
        assert without["text"].split("\n\n")[1] == "World"
