# =============================================================================
# TPL-2 -- what dies on every letter of a type is refused when the profile
# loads: a blank base text, telegram HTML Telegram will not parse, an
# entry of templates/ that is not a <locale>.yaml
# =============================================================================
#
# Every profile here is the suite's fixture profile (tests/fixtures/
# profile) copied and changed in one place, loaded by the real loader --
# the start's own path. The telegram markup is checked by a port of
# TDLib's parse_html (app/profile/telegram_html.py); the grid of what it
# refuses and what it lets through is TestTheParser.
#
# MUTATIONS these tests were written against (each turns one red):
#   P1  the base blank check off                    -> TestBlankBaseText
#   P2  blank as "" only, no strip                  -> TestBlankBaseText
#   P3  the markup check off                        -> TestTelegramMarkup
#   P4  title and body checked apart                -> TestTelegramMarkup
#   P5  an unsupported tag let through              -> TestTheParser
#   P6  an end tag of another tag let through       -> TestTheParser
#   P7  the overlay's text not checked              -> TestTelegramMarkup
#   P8  no warning for an unknown directory         -> TestTheProfileDirectory
#   P9  a .yml in templates/ let through            -> TestTheProfileDirectory
#   P10 a file in the profile's root refused        -> TestTheProfileDirectory
#   P11 hidden entries not skipped                  -> TestTheProfileDirectory
# =============================================================================

import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml
from structlog.testing import capture_logs

from app.core.exceptions import ProfileError
from app.profile.loader import (
    FileOverlaySource,
    FileProfileSource,
    load_overlay,
    load_profile,
)
from app.profile.telegram_html import telegram_html_problem

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "profile"


def _profile(tmp_path: Path, **changes: Any) -> Path:
    """The fixture profile under tmp_path, en.yaml's leaves changed:
    changes are "type.channel.field" (dots as "__") -> text."""
    root = tmp_path / "profile"
    shutil.copytree(_FIXTURE, root)
    path = root / "templates" / "en.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    for dotted, text in changes.items():
        type_key, channel, field = dotted.split("__")
        doc.setdefault(type_key, {}).setdefault(channel, {})[field] = text
    path.write_text(yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8")
    return root


def _refusal(root: Path) -> str:
    with pytest.raises(ProfileError) as caught:
        load_profile(FileProfileSource(root))
    return str(caught.value)


class TestBlankBaseText:
    """Item 1: a blank text of the base -- any text field -- is refused,
    naming the file (the locale), the type, the channel and the field.
    P1, P2."""

    @pytest.mark.parametrize("field", [
        "unit_event__telegram__title",
        "unit_event__telegram__body",
        "unit_event__telegram__button_text",
        "unit_event__email__subject",
    ])
    @pytest.mark.parametrize("text", ["", "   \n "])
    def test_refused(self, tmp_path: Path, field: str, text: str) -> None:
        refusal = _refusal(_profile(tmp_path, **{field: text}))
        type_key, channel, name = field.split("__")
        assert "templates/en.yaml" in refusal
        assert f"{type_key}.{channel}.{name} is blank" in refusal

    def test_the_pair_a_text_loads(self, tmp_path: Path) -> None:
        load_profile(FileProfileSource(
            _profile(tmp_path, unit_event__telegram__title=" T ")))

    def test_the_pair_the_overlay_keeps_its_own_words(self, tmp_path: Path) -> None:
        """The overlay's blank text is refused as before, by its own
        sentence."""
        base = load_profile(FileProfileSource(_profile(tmp_path)))
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "en.yaml").write_text(
            'unit_event: {telegram: {title: ""}}', encoding="utf-8")
        with pytest.raises(ProfileError) as caught:
            load_overlay(FileOverlaySource(overlay), base)
        assert "is blank; an overlay text replaces the base text" in str(caught.value)


class TestTelegramMarkup:
    """Item 2: the telegram title and body, as they are sent together,
    in HTML Telegram parses -- or the profile is refused. P3, P4, P7."""

    @pytest.mark.parametrize(("changes", "words"), [
        ({"unit_event__telegram__body": "<b>{body}"}, 'start tag "b"'),
        ({"unit_event__telegram__body": "{body} <div>x</div>"}, '"div" is not one'),
        ({"unit_event__telegram__body": "<b><i>{body}</b></i>"}, 'end tag "b" closes'),
        ({"unit_event__telegram__body": "1 < 2 {body}"}, "is not one Telegram"),
        ({"unit_event__telegram__body": "<span>{body}</span>"}, "tg-spoiler"),
    ])
    def test_refused(
        self, tmp_path: Path, changes: dict[str, str], words: str,
    ) -> None:
        refusal = _refusal(_profile(tmp_path, **changes))
        assert "unit_event.telegram.title+body" in refusal
        assert "Telegram would not parse the HTML" in refusal
        assert words in refusal

    def test_a_tag_across_title_and_body_loads(self, tmp_path: Path) -> None:
        """Telegram parses the title and the body as one text."""
        load_profile(FileProfileSource(_profile(
            tmp_path,
            unit_event__telegram__title="<i>{title}",
            unit_event__telegram__body="{body}</i>",
        )))

    @pytest.mark.parametrize("body", [
        "<b>{body}</b> [{extra}]",
        '<a href="{extra}?a=1&amp;b=2">{body}</a>',
        "<blockquote expandable>{body}</blockquote> &lt;3 &foo;",
        '<span class="tg-spoiler">{body}</span> <tg-spoiler>{extra}</tg-spoiler>',
        '<pre><code class="language-py">{body}</code></pre>',
        '<tg-emoji emoji-id="{extra}">*</tg-emoji> {body}',
        "{{literal braces}} {body}",
    ])
    def test_the_pair_markup_telegram_takes_loads(
        self, tmp_path: Path, body: str,
    ) -> None:
        load_profile(FileProfileSource(_profile(
            tmp_path, unit_event__telegram__body=body)))

    def test_the_overlay_text_is_checked_over_the_base(
        self, tmp_path: Path,
    ) -> None:
        base = load_profile(FileProfileSource(_profile(
            tmp_path, unit_event__telegram__title="<i>{title}",
            unit_event__telegram__body="{body}</i>",
        )))
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "en.yaml").write_text(
            'unit_event: {telegram: {body: "{body}!"}}', encoding="utf-8")
        with pytest.raises(ProfileError) as caught:
            load_overlay(FileOverlaySource(overlay), base)
        assert "unit_event.telegram.title+body" in str(caught.value)
        assert 'no end tag for the start tag "i"' in str(caught.value)


class TestTheParser:
    """The port of parse_html's syntax refusals, one row each, and what
    it lets through. P5, P6."""

    @pytest.mark.parametrize(("text", "words"), [
        ("<b", "not closed"),
        ("<div>x</div>", '"div" is not one'),
        ("<br/>", '"br/" is not one'),
        ("a < b", '"" is not one'),
        ('<a ="x">t</a>', "empty attribute name"),
        ("<a href=x/y>t</a>", "name token"),
        ('<a href="x>t</a>', "not closed"),
        ("<span>t</span>", "tg-spoiler"),
        ('<span class="x">t</span>', "tg-spoiler"),
        ("t</b>", "closes no open tag"),
        ("<b>t</b", "not closed"),
        ("<b><i>t</b></i>", 'end tag "b" closes the start tag "i"'),
        ("<b>t", 'no end tag for the start tag "b"'),
    ])
    def test_refused(self, text: str, words: str) -> None:
        problem = telegram_html_problem(text)
        assert problem is not None and words in problem, problem

    @pytest.mark.parametrize("text", [
        "", "plain", "a > b", "&lt;b&gt; &amp; &foo; &#60;",
        "<B>t</B>", "<b>t</>", "<b >t</b >", "<i><b>t</b></i>",
        '<a href="x">t</a>', "<a href=https>t</a>", "<a href='x'>t</a>",
        '<span class="tg-spoiler">t</span>', "<span class=tg-spoiler>t</span>",
        "<blockquote expandable>t</blockquote>", "<tg-spoiler>t</tg-spoiler>",
        "<s>a</s><strike>b</strike><del>c</del><u>d</u><ins>e</ins>",
        "<strong>a</strong><em>b</em><code>c</code><pre>d</pre>",
    ])
    def test_taken(self, text: str) -> None:
        assert telegram_html_problem(text) is None


class TestTheProfileDirectory:
    """Item 3: no templates/ is lawful; an entry of templates/ that is not
    a <locale>.yaml is refused; a directory beside templates/ is only
    said. P8-P10."""

    def test_no_templates_loads_quietly(self, tmp_path: Path) -> None:
        root = _profile(tmp_path)
        shutil.rmtree(root / "templates")
        with capture_logs() as logs:
            profile = load_profile(FileProfileSource(root))
        assert profile.templates == {}
        assert [e for e in logs if e["log_level"] == "warning"] == []

    @pytest.mark.parametrize("entry", ["en.yml", "notes.txt", "ru"])
    def test_an_entry_that_is_not_a_locale_file(
        self, tmp_path: Path, entry: str,
    ) -> None:
        root = _profile(tmp_path)
        target = root / "templates" / entry
        if "." in entry:
            target.write_text("unit_event: {}\n", encoding="utf-8")
        else:
            target.mkdir()
        refusal = _refusal(root)
        assert f"templates/{entry}" in refusal
        assert "<locale>.yaml files only" in refusal

    def test_a_misspelt_templates_directory_is_said(self, tmp_path: Path) -> None:
        root = _profile(tmp_path)
        (root / "templates").rename(root / "template")
        with capture_logs() as logs:
            profile = load_profile(FileProfileSource(root))
        assert profile.templates == {}
        (said,) = [e for e in logs if e["event"] == "profile_unknown_directory"]
        assert said["directory"] == "template"
        assert said["log_level"] == "warning"

    def test_the_pair_a_file_beside_types_is_not_said(self, tmp_path: Path) -> None:
        """velo keeps a README.md in its profile's root."""
        root = _profile(tmp_path)
        (root / "README.md").write_text("# profile\n", encoding="utf-8")
        with capture_logs() as logs:
            load_profile(FileProfileSource(root))
        assert [e for e in logs if e["event"] == "profile_unknown_directory"] == []

    @pytest.mark.parametrize("entry", [".gitkeep", ".DS_Store"])
    def test_a_hidden_entry_of_templates_is_skipped(
        self, tmp_path: Path, entry: str,
    ) -> None:
        """A tool's file (the dispatcher's ruling on TPL-2): no refusal,
        no line; notes.txt beside it is still refused (the pair,
        test_an_entry_that_is_not_a_locale_file)."""
        root = _profile(tmp_path)
        (root / "templates" / entry).write_text("", encoding="utf-8")
        with capture_logs() as logs:
            profile = load_profile(FileProfileSource(root))
        assert set(profile.templates) == {"en", "ru"}
        assert [e for e in logs if e["log_level"] == "warning"] == []

    def test_a_hidden_directory_beside_types_is_not_said(
        self, tmp_path: Path,
    ) -> None:
        """.idea is a tool's; template/ beside it is still said (the
        pair, test_a_misspelt_templates_directory_is_said)."""
        root = _profile(tmp_path)
        (root / ".idea").mkdir()
        with capture_logs() as logs:
            load_profile(FileProfileSource(root))
        assert [e for e in logs if e["event"] == "profile_unknown_directory"] == []

