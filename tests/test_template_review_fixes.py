# =============================================================================
# The telegram markup check sees what goes out; a key of a variable does
# not make markup; a profile's durations are bounded
# =============================================================================
#
# Every profile here is the suite's fixture profile copied under tmp_path
# and changed in one place, loaded by the real loader (the start's path).
#
#   - The telegram title and body are checked for a recipient of every
#     locale, each field by the chain the send walks (the locale, then
#     DEFAULT_LOCALE -- "en" in the suite), a field standing as a value
#     with a link's characters ("x:/&amp;"): an unquoted attribute that
#     would not take the letter's link is refused, as Telegram refuses it.
#   - A JSON object's keys are escaped as its values are: a template that
#     prints the object whole ("{params}") prints them.
#   - A duration is a whole literal (fullmatch) of at most 3650 days,
#     refused as a ProfileError however long its count -- never an
#     OverflowError at startup or at an intake.
#
# MUTATIONS these tests were written against (each turns one red):
#   R1  a field stands as "1" again                 -> TestTheMarkupAsSent
#   R2  title and body from one locale              -> TestTheMarkupAsSent
#   R3  the overlay checked without the chain       -> TestTheMarkupAsSent
#   R4  keys not escaped                            -> TestKeysAreEscaped
#   R5  no ceiling                                  -> TestDurations
#   R6  the ceiling compared with >=                -> TestDurations
#   R7  .match with "$" again                       -> TestDurations
#   R8  the ceiling compared through a timedelta    -> TestDurations
# =============================================================================

import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
import yaml

from app.core.config import settings
from app.core.exceptions import ProfileError
from app.engine.formatters import TelegramFormatter
from app.engine.models import Notification
from app.profile.loader import (
    FileOverlaySource,
    FileProfileSource,
    _check_answer_keep,
    load_overlay,
    load_profile,
)
from app.profile.registry import registry

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "profile"


def _profile(
    tmp_path: Path,
    templates: dict[str, dict[str, Any]] | None = None,
    types: dict[str, dict[str, Any]] | None = None,
) -> Path:
    """The fixture profile under tmp_path; `templates` replace whole
    type sheets per locale ({"ru": {"unit_event": {...}}}), `types`
    update type records."""
    root = tmp_path / "profile"
    shutil.copytree(_FIXTURE, root)
    for locale, sheets in (templates or {}).items():
        path = root / "templates" / f"{locale}.yaml"
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        doc.update(sheets)
        path.write_text(yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8")
    if types:
        path = root / "types.yaml"
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        for key, fields in types.items():
            doc["types"].setdefault(key, {}).update(fields)
        path.write_text(yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8")
    return root


def _refusal(root: Path) -> str:
    with pytest.raises(ProfileError) as caught:
        load_profile(FileProfileSource(root))
    return str(caught.value)


def _telegram(title: str | None = None, body: str | None = None) -> dict[str, Any]:
    sheet: dict[str, Any] = {}
    if title is not None:
        sheet["title"] = title
    if body is not None:
        sheet["body"] = body
    return {"unit_event": {"telegram": sheet}}


class TestTheMarkupAsSent:
    """Item 1. R1-R3."""

    def test_a_field_in_an_unquoted_attribute_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """A link has ":" and "/": an unquoted value is a name token to
        Telegram, so every letter would be refused."""
        refusal = _refusal(_profile(
            tmp_path, {"en": _telegram(body='<a href={body}>open</a>')}))
        assert "unit_event.telegram.title+body for a en recipient" in refusal
        assert "name token" in refusal

    def test_the_pair_a_quoted_attribute_loads(self, tmp_path: Path) -> None:
        load_profile(FileProfileSource(_profile(
            tmp_path, {"en": _telegram(body='<a href="{body}">open</a>')})))

    def test_a_ru_title_over_the_en_body_is_refused_for_ru(
        self, tmp_path: Path,
    ) -> None:
        """en closes in its body what its title opens; ru has its own
        title, which opens nothing -- a ru recipient gets the ru title
        over the en body: "</b>" with nothing open. en itself is whole."""
        refusal = _refusal(_profile(tmp_path, {
            "en": _telegram(title="<b>{title}", body="{body}</b>"),
            "ru": _telegram(title="{title}"),
        }))
        assert "templates/ru.yaml" in refusal
        assert "for a ru recipient" in refusal
        assert "closes no open tag" in refusal
        assert "for a en recipient" not in refusal

    def test_the_pair_the_chain_makes_a_whole_text(self, tmp_path: Path) -> None:
        """The handoff's example: ru with only its title "<b>{x}" takes
        the en body "{y}</b>" -- "<b>…</b>", which Telegram takes."""
        load_profile(FileProfileSource(_profile(tmp_path, {
            "en": _telegram(title="<b>{title}", body="{body}</b>"),
            "ru": _telegram(title="<b>{title}"),
        })))

    def test_the_pair_a_tag_across_one_locale_loads(self, tmp_path: Path) -> None:
        load_profile(FileProfileSource(_profile(tmp_path, {
            "en": _telegram(title="<i>{title}", body="{body}</i>"),
            "ru": _telegram(title="<i>{title}", body="{body}</i>"),
        })))

    def test_an_en_overlay_breaks_ru_through_the_chain(
        self, tmp_path: Path,
    ) -> None:
        """The overlay replaces the en body; ru takes it by the chain over
        its own title -- checked over the base, for every locale."""
        base = load_profile(FileProfileSource(_profile(tmp_path, {
            "en": _telegram(title="<b>{title}", body="{body}</b>"),
            "ru": _telegram(title="<b>{title}"),
        })))
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "en.yaml").write_text(
            'unit_event: {telegram: {title: "{title}", body: "{body}"}}',
            encoding="utf-8",
        )
        with pytest.raises(ProfileError) as caught:
            load_overlay(FileOverlaySource(overlay), base)
        refusal = str(caught.value)
        assert "overlay en.yaml" in refusal
        assert "for a ru recipient" in refusal
        assert 'no end tag for the start tag "b"' in refusal


class TestKeysAreEscaped:
    """Item 2: through the real telegram formatter. R4."""

    async def _text(self, body: str, params: dict[str, Any]) -> str:
        registry.register_templates(
            "en", {"unit_event": {"telegram": {"title": "T", "body": body}}})
        sent: list[dict[str, Any]] = []

        class _Bot:
            async def send_message(self, **kwargs: Any) -> Any:
                sent.append(kwargs)
                return SimpleNamespace(message_id=1)

        formatter = TelegramFormatter(
            bot=_Bot(), bot_url="https://t.me/unit_bot")  # type: ignore[arg-type]
        job = Notification(
            id=uuid4(), type="unit_event", lane="normal", title="Hello",
            body="World", target_type="user", target_value="*",
            action_data={"params": params},
        )
        await formatter.deliver(
            job, SimpleNamespace(),  # type: ignore[arg-type]
            SimpleNamespace(telegram_id=80998, locale="en"),  # type: ignore[arg-type]
        )
        text: str = sent[0]["text"]
        return text

    async def test_a_key_with_markup_is_escaped(self) -> None:
        key = '<a href="https://evil.example">Confirm</a>'
        text = await self._text("{params}", {key: "v"})
        assert "<a " not in text
        assert (
            "&lt;a href=&quot;https://evil.example&quot;&gt;Confirm&lt;/a&gt;"
            in text
        )

    async def test_the_pair_a_value_as_before(self) -> None:
        text = await self._text("{params[name]}", {"name": "<b>x</b>"})
        assert text == "T\n\n&lt;b&gt;x&lt;/b&gt;"


class TestDurations:
    """Item 3. R5-R8."""

    @pytest.mark.parametrize("literal", [
        "3651d", "3000000d", "1000000000d", "87601h", "5256001m",
        "315360001s", "9" * 400 + "d",
    ])
    def test_above_the_ceiling(self, tmp_path: Path, literal: str) -> None:
        refusal = _refusal(_profile(
            tmp_path, types={"unit_expiring": {"expires_after": literal}}))
        assert "unit_expiring" in refusal and "expires_after" in refusal
        assert "longer than 3650d" in refusal

    @pytest.mark.parametrize("literal", ["5m\n", " 5m", "5m ", "5 m", "05m"])
    def test_not_a_whole_literal(self, tmp_path: Path, literal: str) -> None:
        refusal = _refusal(_profile(
            tmp_path, types={"unit_expiring": {"expires_after": literal}}))
        assert "expected a duration literal" in refusal

    @pytest.mark.parametrize("literal", ["90d", "1h", "3650d", "87600h", "15m"])
    def test_the_pair_loads(self, tmp_path: Path, literal: str) -> None:
        load_profile(FileProfileSource(_profile(
            tmp_path, types={"unit_expiring": {"expires_after": literal}})))

    def test_answer_keep_above_int32_with_retention_forever(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """NOTIFICATION_RETENTION_DAYS=0 keeps jobs forever and left
        answer_keep unbounded: 25000d is past an int32 of seconds."""
        monkeypatch.setattr(settings, "notification_retention_days", 0)
        problem = _check_answer_keep("25000d")
        assert problem is not None and "longer than 3650d" in problem
        assert _check_answer_keep("3650d") is None
