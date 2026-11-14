# =============================================================================
# The instance's languages, and the language of a letter (W4-1)
# =============================================================================
#
# LANGUAGES and DEFAULT_LANGUAGE come from the passport (primer.yaml
# `env`): both required, no default in code -- a start without them, or
# with one comms cannot read, is refused naming it. The old
# default-locale setting is gone. A text is looked up in the recipient's
# language as the product wrote it, compared byte for byte with the
# profile's (owner's decision of 7.11), then in DEFAULT_LANGUAGE, then the
# stored title/body.
#
# MUTATIONS these tests were written against (each turns one red):
#   L1  DEFAULT_LANGUAGE outside LANGUAGES accepted -> TestTheStart
#   L2  the language matched loosely (case, region) -> TestTheLetter
#   L3  the default a literal "en", not the setting  -> TestTheLetter
#   L4  no LANGUAGES given a default in code         -> TestTheStart
# =============================================================================

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.core.config import Settings, format_startup_error, language_problems, settings
from app.engine.formatters import TelegramFormatter
from app.engine.models import Notification
from app.engine.template_engine import render

_REPO = Path(__file__).resolve().parents[1]


class TestTheStart:
    """L1, L4."""

    @pytest.mark.parametrize(("languages", "default", "said"), [
        ("", "en", "LANGUAGES is not set"),
        ("en,ru", "", "DEFAULT_LANGUAGE is not set"),
        ("en,en-GB", "en", "'en-GB' is not a language code"),
        ("EN", "en", "'EN' is not a language code"),
        ("en, ru", "en", "' ru' is not a language code"),
        ("en,,ru", "en", "'' is not a language code"),
        ("en,ru,en", "en", "en is named twice"),
        ("en,ru", "uk", "DEFAULT_LANGUAGE='uk' is not among LANGUAGES='en,ru'"),
        ("en,ru", "ru-RU", "DEFAULT_LANGUAGE='ru-RU' is not a language code"),
    ])
    def test_refused(self, languages: str, default: str, said: str) -> None:
        problems = language_problems(languages, default)
        assert len(problems) == 1, problems
        assert said in problems[0]

    @pytest.mark.parametrize(("languages", "default"), [
        ("en", "en"), ("en,ru,uk", "uk"), ("haw,en", "haw"),
    ])
    def test_the_pair_the_passports_forms(self, languages: str, default: str) -> None:
        assert language_problems(languages, default) == []

    def test_a_start_without_them_is_refused_naming_both(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("LANGUAGES", raising=False)
        monkeypatch.delenv("DEFAULT_LANGUAGE", raising=False)
        with pytest.raises(ValidationError) as caught:
            Settings()
        message = format_startup_error(caught.value)
        assert "LANGUAGES is not set" in message
        assert "DEFAULT_LANGUAGE is not set" in message

    def test_the_pair_with_them_it_starts(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("LANGUAGES", "en,ru,uk")
        monkeypatch.setenv("DEFAULT_LANGUAGE", "uk")
        built = Settings()
        assert (built.languages, built.default_language) == ("en,ru,uk", "uk")

    def test_no_default_in_code(self) -> None:
        for name in ("languages", "default_language"):
            assert Settings.model_fields[name].default == "", name
        assert "_".join(("default", "locale")) not in Settings.model_fields


def _body(locale: str | None) -> str | None:
    """The fixture's unit_event telegram body for a recipient of `locale`:
    en "{body} [{extra}]", ru "RU: {body}"."""
    return render(
        "unit_event", "telegram", "body", locale, {"body": "B", "extra": "x"},
    )


class TestTheLetter:
    """L2, L3: through the registry the fixture profile is installed in."""

    @pytest.mark.parametrize(("locale", "text"), [
        ("ru", "RU: B"),
        ("en", "B [x]"),
        (None, "B [x]"),
        ("uk", "B [x]"),
        ("RU", "B [x]"),
        ("ru-RU", "B [x]"),
        ("ru_RU", "B [x]"),
        ("rus", "B [x]"),
    ])
    def test_exactly_the_language_or_the_default(
        self, locale: str | None, text: str,
    ) -> None:
        assert _body(locale) == text

    def test_the_default_is_the_instances(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "default_language", "ru")
        assert _body("de") == "RU: B"
        assert _body(None) == "RU: B"
        assert _body("en") == "B [x]"

    async def test_a_recipient_with_no_language_gets_the_default(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Through the telegram formatter, a NULL recipients.locale."""
        monkeypatch.setattr(settings, "default_language", "ru")
        sent: list[dict[str, Any]] = []

        class _Bot:
            async def send_message(self, **kwargs: Any) -> Any:
                sent.append(kwargs)
                return SimpleNamespace(message_id=1)

        formatter = TelegramFormatter(
            bot=_Bot(), bot_url="https://t.me/unit_bot")  # type: ignore[arg-type]
        job = Notification(
            id=uuid4(), type="unit_event", lane="normal", title="Hello",
            body="World", target_type="user", target_value="*", action_data={},
        )
        await formatter.deliver(
            job, SimpleNamespace(),  # type: ignore[arg-type]
            SimpleNamespace(telegram_id=80997, locale=None),  # type: ignore[arg-type]
        )
        assert sent[0]["text"].endswith("RU: World")


class TestDefaultLocaleIsGone:
    """done-when 3: no form of the old setting in the tree but history.
    The name is assembled here, so this file is not its own finding."""

    def test_no_form_anywhere(self) -> None:
        old = "_".join(("default", "locale"))
        roots = [_REPO / name for name in ("app", "deploy", "scripts", "tests", "bin")]
        files = [
            path for root in roots for path in root.rglob("*")
            if path.is_file() and path.suffix in {".py", ".md", ".yml", ".yaml", ""}
        ]
        files += [_REPO / "primer.yaml", _REPO / ".github" / "workflows" / "ci.yml"]
        assert len(files) > 100  # the pair: the walk read the tree
        found = [
            str(path.relative_to(_REPO)) for path in files
            if old in path.read_text(encoding="utf-8", errors="replace").lower()
        ]
        assert found == []
