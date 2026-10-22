# =============================================================================
# PO-1 -- the text overlay: texts of the base profile, replaced leaf by leaf
# =============================================================================
#
# What the overlay is and what it is checked for: app/profile/loader.py,
# THE TEXT OVERLAY; the contract with the products: deploy/INTEGRATION.md
# section 3, `PROFILE_OVERLAY_DIR`.
#
# Items:
#   1 the overlay loads: empty variable -> byte for byte as without it;
#     one field from the overlay, the others from the base, through the
#     real formatters (telegram, email; in_app renders no template, so
#     its test is the inbox showing the stored text); no record -> base;
#     an EMPTY directory -> start, zero overrides, said with the path
#   2 a broken overlay refuses the start, naming the file and the place
#   3 explain_texts names the layer of every field
#   4 the deploy forms (compose, comms-deploy.sh, the env templates,
#     INTEGRATION.md) held against the code
#
# MUTATIONS (one run, every one red here):
#   M1  install_overlay registers nothing   -> TestOneFieldFromTheOverlay,
#                                              TestNoRecord, TestExplain
#   M2  the overlay replaces the whole sheet -> the one-field tests
#   M3a the field coordinate unchecked      -> test_a_coordinate_the_base_
#   M3b the channel coordinate unchecked       does_not_have
#   M3c the locale coordinate unchecked     -> test_a_locale_the_base_
#                                              does_not_have
#   M4a CORE_VARIABLES left out of the set  -> test_core_variables_are_
#                                              accepted
#   M4b the variable check removed          -> test_a_variable_the_type_
#                                              does_not_have
#   M5  a blank text accepted               -> test_blank_text
#   M6  a missing directory read as empty   -> test_missing_directory
#   M7  every text explained as "profile"   -> TestExplain
#   M8  plain yaml.safe_load for the overlay -> test_a_key_declared_twice
#   M9  a flag accepted as a text           -> test_a_flag_is_not_a_text
#   M10 only the first violation reported   -> test_every_problem_at_once
#   M11 compose env without `:+`            -> TestTheDeployForms
#   M12 an empty directory refused          -> test_an_empty_directory_
#                                              is_zero_overrides
#   M13 check_overlay_dir passes a missing  -> TestTheDeployGuard
#       path
#   M14 explain_texts walks its own chain   -> test_the_chain_is_the_one_
#                                              render_walks
# NOT BUILT: "the overlay is checked although the base is broken" --
# parse_overlay needs the PARSED base, and parse_profile raises before
# there is one; the code cannot reach that state.
# =============================================================================

import re
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import yaml
from httpx import AsyncClient
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.core.config import Settings, settings
from app.core.database import get_session_factory
from app.core.exceptions import ProfileError
from app.engine.constants import DeliveryChannel, DeliveryStatus, TargetType
from app.engine.formatters import EmailFormatter, TelegramFormatter, build_variables
from app.engine.models import Notification, NotificationDelivery
from app.engine.service import create_notification, resolve_notification
from app.engine.template_engine import explain_texts, render
from app.profile.loader import (
    CORE_VARIABLES,
    FileOverlaySource,
    FileProfileSource,
    install_overlay,
    install_profile,
    install_profile_from_settings,
    load_overlay,
    load_profile,
)
from app.profile.registry import TextLayer, registry
from tests.conftest import FIXTURE_PROFILE_DIR
from tests.helpers import (
    configure_every_channel,
    create_recipient,
    intake_fields,
    next_telegram_id,
)

_REPO = Path(__file__).resolve().parents[1]
_DEPLOY = _REPO / "deploy"
BOT_URL = "https://t.me/comms_testbot"

# Sentinels: not a substring of anything the base or a channel writes.
_OVL = "OVL-zebra-7731"
_OVL_RU = "OVL-kiwi-5520"

# The base the tests stand on: the fixture's types.yaml, with sheets
# written here so every field the cases need exists exactly once.
_BASE_EN = """
unit_event:
  telegram:
    title: "{title}"
    body: "{body} [{extra}]"
    button_text: "Open {title}"
    silent: true
  email:
    subject: "COMMS: {title}"
    body: "{body} -- {extra}"
  in_app:
    title: "{title}"
unit_presented:
  telegram:
    title: "{title}"
    body: "{body}"
unit_event_in_app:
  telegram:
    title: "{title}"
unit_event_email:
  email:
    subject: "Static subject"
"""
_BASE_RU = """
unit_event:
  telegram:
    body: "RU: {body}"
"""


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    root = tmp_path / "profile"
    (root / "templates").mkdir(parents=True)
    shutil.copy(FIXTURE_PROFILE_DIR / "types.yaml", root / "types.yaml")
    (root / "templates" / "en.yaml").write_text(_BASE_EN, encoding="utf-8")
    (root / "templates" / "ru.yaml").write_text(_BASE_RU, encoding="utf-8")
    return root


@pytest.fixture
def overlay_dir(tmp_path: Path) -> Path:
    root = tmp_path / "overlay"
    root.mkdir()
    return root


def _write(root: Path, name: str, text: str) -> None:
    (root / name).write_text(text, encoding="utf-8")


def _install(base_dir: Path, overlay_dir: Path | None) -> None:
    """Base, then the overlay, into the shared registry -- the startup
    order (install_profile_from_settings)."""
    base = load_profile(FileProfileSource(base_dir))
    overlay = (
        load_overlay(FileOverlaySource(overlay_dir), base)
        if overlay_dir is not None
        else None
    )
    registry.reset()
    install_profile(base)
    if overlay is not None:
        install_overlay(overlay)


def _refusal(base_dir: Path, overlay_dir: Path) -> str:
    base = load_profile(FileProfileSource(base_dir))
    with pytest.raises(ProfileError) as caught:
        load_overlay(FileOverlaySource(overlay_dir), base)
    return str(caught.value)


def _snapshot() -> dict[str, Any]:
    """Every leaf the registry holds, with the value read through the
    typed accessors -- what a channel could ever see."""
    leaves: dict[str, Any] = {}
    for locale in ("en", "ru"):
        for type_key in sorted(registry.registered_types()):
            for channel in ("telegram", "email", "in_app"):
                for field in (
                    "title", "body", "subject", "button_text", "silent",
                ):
                    key = f"{locale}.{type_key}.{channel}.{field}"
                    leaves[key] = (
                        registry.get_template(locale, type_key, channel, field),
                        registry.get_flag(locale, type_key, channel, field),
                    )
    return leaves


class _FakeBot:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def send_message(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return SimpleNamespace(message_id=1)


def _notification(**overrides: Any) -> Notification:
    fields: dict[str, Any] = {
        "type": "unit_event",
        "lane": "normal",
        "title": "Hello",
        "body": "World",
        "target_type": "user",
        "target_value": "*",
        "action_data": {"extra": "x"},
    }
    fields.update(overrides)
    return Notification(**fields)


async def _telegram(locale: str = "en", **overrides: Any) -> dict[str, Any]:
    bot = _FakeBot()
    formatter = TelegramFormatter(bot=bot, bot_url=BOT_URL)
    await formatter.deliver(
        _notification(**overrides),
        NotificationDelivery(channel="telegram", channel_options=None),
        Recipient(telegram_id=next_telegram_id(), locale=locale, active=True),
    )
    (call,) = bot.calls
    return call


async def _email(locale: str = "en") -> dict[str, str]:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"id": "<1@mg.test>"})

    formatter = EmailFormatter(
        client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        api_base_url="https://api.mailgun.net",
        api_key="key-unit",
        domain="mail.example.test",
        from_address="noreply@mail.example.test",
    )
    await formatter.deliver(
        _notification(),
        NotificationDelivery(channel=DeliveryChannel.EMAIL, channel_options=None),
        Recipient(email="user@example.test", locale=locale, active=True),
    )
    (request,) = requests
    from urllib.parse import parse_qs

    return {
        key: values[0]
        for key, values in parse_qs(request.content.decode()).items()
    }


# ---------------------------------------------------------------------------
# Item 1 -- the overlay loads
# ---------------------------------------------------------------------------


class TestNoOverlay:
    """done-when 1(1): the variable empty -> as now, byte for byte."""

    def test_an_empty_variable_installs_exactly_the_base(
        self,
        base_dir: Path,
        overlay_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The startup path with TEMPLATES_OVERLAY_DIR empty against the
        base installed alone. An overlay file sits in a directory nobody
        points at -- it must not be read. The pair: the base text is
        there and non-empty."""
        _write(overlay_dir, "en.yaml", f'unit_event: {{telegram: {{title: "{_OVL}"}}}}')
        _install(base_dir, None)
        alone = _snapshot()
        configure_every_channel(monkeypatch)
        monkeypatch.setattr(settings, "templates_dir", str(base_dir))
        monkeypatch.setattr(settings, "templates_overlay_dir", "")
        registry.reset()
        with capture_logs() as logs:
            assert install_profile_from_settings() is True
        assert _snapshot() == alone
        assert alone["en.unit_event.telegram.title"] == ("{title}", None)
        assert not [e for e in logs if e["event"] == "profile_overlay_installed"]

    async def test_rendered_text_is_the_base(self, base_dir: Path) -> None:
        _install(base_dir, None)
        call = await _telegram()
        assert call["text"] == "Hello\n\nWorld [x]"
        assert _OVL not in call["text"]

    def test_an_empty_directory_is_zero_overrides(
        self,
        base_dir: Path,
        overlay_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Gate answer 3 / correction item 1: an existing empty directory
        is legal -- the registry is the base's byte for byte, and the
        info line says so with the path and the count. M12."""
        _install(base_dir, None)
        alone = _snapshot()
        configure_every_channel(monkeypatch)
        monkeypatch.setattr(settings, "templates_dir", str(base_dir))
        monkeypatch.setattr(settings, "templates_overlay_dir", str(overlay_dir))
        registry.reset()
        with capture_logs() as logs:
            assert install_profile_from_settings() is True
        assert _snapshot() == alone
        (line,) = [e for e in logs if e["event"] == "profile_overlay_installed"]
        assert line["log_level"] == "info"
        assert line["path"] == str(overlay_dir)
        assert line["overrides"] == 0


class TestOneFieldFromTheOverlay:
    """done-when 1(2): one field from the overlay, the others from the
    base, rendered by the real formatters."""

    async def test_title_from_the_overlay_body_from_the_base(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        """telegram. M1, M2."""
        _write(
            overlay_dir, "en.yaml",
            f'unit_event: {{telegram: {{title: "{_OVL} {{title}}"}}}}',
        )
        _install(base_dir, overlay_dir)
        call = await _telegram()
        assert call["text"] == f"{_OVL} Hello\n\nWorld [x]"
        # The flag of the same sheet stays the base's.
        assert call["disable_notification"] is True

    async def test_the_button_text_alone(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        _write(
            overlay_dir, "en.yaml",
            f'unit_event: {{telegram: {{button_text: "{_OVL}"}}}}',
        )
        _install(base_dir, overlay_dir)
        call = await _telegram(
            action_data={"action": "open_unit", "params": {"unit_id": "u1"},
                         "extra": "x"},
        )
        assert call["reply_markup"].inline_keyboard[0][0].text == _OVL
        assert call["text"] == "Hello\n\nWorld [x]"

    async def test_email_subject_from_the_overlay_body_from_the_base(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        _write(
            overlay_dir, "en.yaml",
            f'unit_event: {{email: {{subject: "{_OVL}: {{title}}"}}}}',
        )
        _install(base_dir, overlay_dir)
        fields = await _email()
        assert fields["subject"] == f"{_OVL}: Hello"
        assert fields["text"] == "World -- x"

    async def test_in_app_shows_the_stored_text(
        self, base_dir: Path, overlay_dir: Path, client: AsyncClient,
    ) -> None:
        """in_app renders no template (gate correction 0.1): the inbox
        item is the stored title/body byte for byte while an overlay is
        live for the same type. The pair: the overlay IS installed for
        that type -- explain_texts names it."""
        _write(
            overlay_dir, "en.yaml",
            f'unit_event_in_app: {{telegram: {{title: "{_OVL}"}}}}',
        )
        _install(base_dir, overlay_dir)
        assert explain_texts("unit_event_in_app", "telegram")["title"].layer \
            is TextLayer.OVERLAY
        factory = get_session_factory()
        async with factory() as session:
            recipient = await create_recipient(
                session, telegram_id=next_telegram_id(),
            )
            notification = await create_notification(
                session, **intake_fields(), type="unit_event_in_app",
                title="Stored title", body="Stored body",
                target_type=TargetType.USER, target_value=str(recipient.id),
                action_data=None,
            )
            for delivery in await resolve_notification(session, notification):
                delivery.status = DeliveryStatus.SENT
                delivery.sent_at = datetime.now(UTC)
            recipient_id = recipient.id
            await session.commit()
        response = await client.get(f"/api/v1/recipients/{recipient_id}/inbox")
        (item,) = response.json()["items"]
        assert (item["title"], item["body"]) == ("Stored title", "Stored body")
        assert _OVL not in response.text


class TestNoRecord:
    """done-when 1(3): no record for the type, channel or locale -> base."""

    async def test_other_type_channel_and_locale_stay_the_base(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        _write(
            overlay_dir, "en.yaml",
            f'unit_event: {{telegram: {{title: "{_OVL}"}}}}',
        )
        _install(base_dir, overlay_dir)
        # another type
        assert render("unit_presented", "telegram", "title", "en",
                      {"title": "T"}) == "T"
        # another channel of the same type
        assert (await _email())["subject"] == "COMMS: Hello"
        # a locale the overlay has no file for: ru has its own body and
        # falls back to en for the title -- the overlay's en title.
        call = await _telegram(locale="ru")
        assert call["text"] == f"{_OVL}\n\nRU: World"

    async def test_a_locale_file_replaces_that_locale_only(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        _write(
            overlay_dir, "ru.yaml",
            f'unit_event: {{telegram: {{body: "{_OVL_RU} {{body}}"}}}}',
        )
        _install(base_dir, overlay_dir)
        assert (await _telegram(locale="ru"))["text"] == \
            f"Hello\n\n{_OVL_RU} World"
        assert (await _telegram(locale="en"))["text"] == "Hello\n\nWorld [x]"


# ---------------------------------------------------------------------------
# Item 2 -- a broken overlay does not start
# ---------------------------------------------------------------------------


class TestRefusals:
    """Every place the overlay can be wrong: the refusal names the file
    and the place. The pair of the class: a correct overlay starts."""

    def test_a_correct_overlay_starts(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        _write(overlay_dir, "en.yaml", """
unit_event:
  telegram: {title: "{title}!", body: "{body} {extra}", button_text: "Go"}
  email: {subject: "{title}", body: "{body}"}
""")
        _write(overlay_dir, "ru.yaml", 'unit_event: {telegram: {body: "{body}"}}')
        base = load_profile(FileProfileSource(base_dir))
        assert load_overlay(FileOverlaySource(overlay_dir), base).overrides() == 6

    @pytest.mark.parametrize(
        ("document", "place", "said"),
        [
            ('unit_ghost: {telegram: {title: "x"}}', "unit_ghost",
             "not declared in types.yaml"),
            ('unit_plain: {telegram: {title: "x"}}', "unit_plain",
             "has no templates in templates/en.yaml"),
            ('unit_presented: {email: {subject: "x"}}', "unit_presented.email",
             "base has no 'email' sheet"),
            ('unit_event: {llm: {body: "x"}}', "unit_event.llm",
             "base has no 'llm' sheet"),
            ('unit_event: {email: {button_text: "x"}}',
             "unit_event.email.button_text", "not a field of the base sheet"),
            ('unit_event: {telegram: {tilte: "x"}}',
             "unit_event.telegram.tilte", "not a field of the base sheet"),
        ],
    )
    def test_a_coordinate_the_base_does_not_have(
        self, base_dir: Path, overlay_dir: Path,
        document: str, place: str, said: str,
    ) -> None:
        """Type, channel, field outside the base. M3."""
        _write(overlay_dir, "en.yaml", document)
        refusal = _refusal(base_dir, overlay_dir)
        assert "overlay en.yaml" in refusal
        assert place in refusal and said in refusal

    def test_a_locale_the_base_does_not_have(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        _write(overlay_dir, "uk.yaml", 'unit_event: {telegram: {title: "x"}}')
        refusal = _refusal(base_dir, overlay_dir)
        assert "overlay uk.yaml" in refusal
        assert "locale 'uk' has no templates in the base profile" in refusal

    def test_in_app_is_refused_by_name(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        """The base HAS an in_app sheet for unit_event -- refused anyway,
        it is never rendered."""
        _write(overlay_dir, "en.yaml", 'unit_event: {in_app: {title: "x"}}')
        refusal = _refusal(base_dir, overlay_dir)
        assert "unit_event.in_app: in_app renders no template" in refusal

    def test_a_flag_is_not_a_text(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        """M9."""
        _write(overlay_dir, "en.yaml", "unit_event: {telegram: {silent: false}}")
        refusal = _refusal(base_dir, overlay_dir)
        assert "unit_event.telegram.silent is a presentation flag" in refusal

    def test_a_variable_the_type_does_not_have(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        """{amount} is in no base template of unit_event. The nested
        spec is read too. M4b."""
        _write(overlay_dir, "en.yaml",
               'unit_event: {telegram: {title: "{title} {amount} {x:{width}}"}}')
        refusal = _refusal(base_dir, overlay_dir)
        assert "uses {amount}, {width}, {x}, which the type does not have" \
            in refusal
        assert "its variables: body, extra, title" in refusal

    def test_core_variables_are_accepted(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        """unit_event_email's base subject uses no variable at all: the
        overlay may still use {title} and {body}, which every job has --
        and item access on a variable another field of the base uses.
        M4a."""
        _write(overlay_dir, "en.yaml", """
unit_event_email: {email: {subject: "{title} {body}"}}
unit_event: {email: {body: "{extra[k]} {body}"}}
""")
        base = load_profile(FileProfileSource(base_dir))
        assert load_overlay(FileOverlaySource(overlay_dir), base).overrides() == 2

    def test_the_core_variables_are_what_the_engine_supplies(self) -> None:
        """CORE_VARIABLES is written in the loader because the profile
        package sits below the engine; this holds the two in step."""
        assert set(build_variables(_notification(action_data=None))) \
            == set(CORE_VARIABLES)

    @pytest.mark.parametrize("text", ['""', '"   "', '"\\t"'])
    def test_blank_text(
        self, base_dir: Path, overlay_dir: Path, text: str,
    ) -> None:
        """M5."""
        _write(overlay_dir, "en.yaml", f"unit_event: {{telegram: {{title: {text}}}}}")
        assert "unit_event.telegram.title is blank" in _refusal(base_dir, overlay_dir)

    @pytest.mark.parametrize(
        ("value", "said"),
        [
            ("{title}", "is a mapping, not a string"),
            ("12", "must be a string, got int"),
            ("true", "must be a string, got bool"),
            ('"{a:zzz}"', "broken format spec"),
            ('"{user.name}"', "attribute access"),
            ('"see https://example.com/x"', "external domain literal"),
        ],
    )
    def test_the_same_leaf_checks_as_the_base(
        self, base_dir: Path, overlay_dir: Path, value: str, said: str,
    ) -> None:
        _write(overlay_dir, "en.yaml", f"unit_event: {{telegram: {{title: {value}}}}}")
        refusal = _refusal(base_dir, overlay_dir)
        assert "unit_event.telegram.title" in refusal and said in refusal

    @pytest.mark.parametrize(
        ("document", "said"),
        [
            ("", "is empty"),
            ("{}", "got an empty mapping"),
            ("[a, b]", "got list"),
            ("unit_event: {}", "unit_event must be a non-empty mapping"),
            ("unit_event: {telegram: {}}",
             "unit_event.telegram must be a non-empty mapping"),
            ("unit_event: {telegram: x}",
             "unit_event.telegram must be a non-empty mapping"),
        ],
    )
    def test_not_the_shape_of_a_file(
        self, base_dir: Path, overlay_dir: Path, document: str, said: str,
    ) -> None:
        _write(overlay_dir, "en.yaml", document)
        refusal = _refusal(base_dir, overlay_dir)
        assert "overlay en.yaml" in refusal and said in refusal

    def test_a_key_declared_twice(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        """M8."""
        _write(overlay_dir, "en.yaml", """
unit_event:
  telegram:
    title: "a"
    title: "b"
""")
        refusal = _refusal(base_dir, overlay_dir)
        assert "overlay en.yaml" in refusal
        assert "'title' at line 5 is declared a second time" in refusal

    def test_unparseable_yaml(self, base_dir: Path, overlay_dir: Path) -> None:
        _write(overlay_dir, "en.yaml", "unit_event: {telegram: [")
        refusal = _refusal(base_dir, overlay_dir)
        assert "Invalid YAML" in refusal and "en.yaml" in refusal

    @pytest.mark.parametrize(
        "entry", ["templates", "en.yml", "README", ".gitkeep", "types.yaml"],
    )
    def test_an_entry_that_is_not_a_locale_file(
        self, base_dir: Path, overlay_dir: Path, entry: str,
    ) -> None:
        """A file beside the overlay would otherwise be silently skipped;
        types.yaml reads as the locale `types`, which the base has not."""
        if entry == "templates":
            (overlay_dir / entry).mkdir()
        else:
            _write(overlay_dir, entry, 'unit_event: {telegram: {title: "x"}}')
        refusal = _refusal(base_dir, overlay_dir)
        assert f"overlay {entry}" in refusal

    def test_missing_directory(self, base_dir: Path, tmp_path: Path) -> None:
        """M6. The pair: the base on its own loads."""
        assert load_profile(FileProfileSource(base_dir)).templates
        refusal = _refusal(base_dir, tmp_path / "nowhere")
        assert f"Text overlay directory does not exist: {tmp_path / 'nowhere'}" \
            in refusal

    def test_a_file_instead_of_a_directory(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        _write(overlay_dir, "en.yaml", 'unit_event: {telegram: {title: "x"}}')
        refusal = _refusal(base_dir, overlay_dir / "en.yaml")
        assert "Text overlay path is not a directory" in refusal

    def test_an_overlay_without_a_base(
        self, overlay_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "templates_dir", "")
        monkeypatch.setattr(settings, "templates_overlay_dir", str(overlay_dir))
        with pytest.raises(ProfileError, match="TEMPLATES_OVERLAY_DIR is set"):
            install_profile_from_settings()

    def test_every_problem_at_once(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        """Two files, three problems: one refusal, ordered by file. M10."""
        _write(overlay_dir, "en.yaml",
               'unit_event: {telegram: {title: "", tilte: "x"}}')
        _write(overlay_dir, "ru.yaml", 'unit_ghost: {telegram: {body: "x"}}')
        refusal = _refusal(base_dir, overlay_dir)
        assert "The text overlay at" in refusal and "(3 problems)" in refusal
        assert refusal.index("overlay en.yaml") < refusal.index("overlay ru.yaml")

    def test_the_startup_path_refuses_and_installs_nothing(
        self,
        base_dir: Path,
        overlay_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A broken overlay is not dropped in favour of the base: the
        start refuses, and not even the base got installed."""
        _write(overlay_dir, "en.yaml", 'unit_event: {telegram: {title: ""}}')
        configure_every_channel(monkeypatch)
        monkeypatch.setattr(settings, "templates_dir", str(base_dir))
        monkeypatch.setattr(settings, "templates_overlay_dir", str(overlay_dir))
        registry.reset()
        with pytest.raises(ProfileError, match="is blank"):
            install_profile_from_settings()
        assert registry.registered_types() == frozenset()


# ---------------------------------------------------------------------------
# Item 3 -- explain names the layer of every field
# ---------------------------------------------------------------------------


class TestExplain:
    def test_every_field_with_its_layer(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        """M7."""
        _write(overlay_dir, "en.yaml",
               f'unit_event: {{telegram: {{title: "{_OVL}"}}}}')
        _install(base_dir, overlay_dir)
        texts = explain_texts("unit_event", "telegram", "en")
        assert sorted(texts) == ["body", "button_text", "title"]
        assert (texts["title"].template, texts["title"].layer,
                texts["title"].source) == (_OVL, TextLayer.OVERLAY,
                                           "overlay en.yaml")
        assert (texts["body"].layer, texts["body"].source) == (
            TextLayer.PROFILE, "templates/en.yaml",
        )

    def test_the_chain_is_the_one_render_walks(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        """ru: the body is ru's own base text, the title falls back to
        en -- where the overlay replaced it."""
        _write(overlay_dir, "en.yaml",
               f'unit_event: {{telegram: {{title: "{_OVL}"}}}}')
        _install(base_dir, overlay_dir)
        texts = explain_texts("unit_event", "telegram", "ru")
        assert (texts["body"].locale, texts["body"].layer) == (
            "ru", TextLayer.PROFILE,
        )
        assert (texts["title"].locale, texts["title"].layer) == (
            "en", TextLayer.OVERLAY,
        )
        assert render("unit_event", "telegram", "title", "ru") == \
            texts["title"].template

    def test_without_an_overlay_everything_is_the_profile(
        self, base_dir: Path,
    ) -> None:
        _install(base_dir, None)
        texts = explain_texts("unit_event", "email")
        assert texts and {d.layer for d in texts.values()} == {TextLayer.PROFILE}

    def test_no_template_is_an_absent_field(self, base_dir: Path) -> None:
        """unit_presented has no email sheet: the channel will use the
        stored title/body, and the answer says so by being empty."""
        _install(base_dir, None)
        assert explain_texts("unit_presented", "email") == {}
        assert explain_texts("unit_presented", "telegram")

    def test_an_unknown_type_is_not_no_templates(self, base_dir: Path) -> None:
        _install(base_dir, None)
        with pytest.raises(LookupError, match="unit_ghost"):
            explain_texts("unit_ghost", "telegram")


# ---------------------------------------------------------------------------
# Item 4 -- the deploy forms, held against the code
# ---------------------------------------------------------------------------

_APP_SERVICES = ("comms-app", "comms-worker", "comms-consumer")
_SCRIPT = _DEPLOY / "comms-deploy.sh"


class TestTheDeployForms:
    def test_compose_hands_the_overlay_to_every_process(self) -> None:
        """M11. The in-container name is the setting's own."""
        compose = yaml.safe_load((_DEPLOY / "docker-compose.yml").read_text())
        name = "templates_overlay_dir".upper()
        assert "templates_overlay_dir" in Settings.model_fields
        for service in _APP_SERVICES:
            spec = compose["services"][service]
            assert spec["environment"][name] == \
                "${PROFILE_OVERLAY_DIR:+/profile-overlay}", service
            (mount,) = [
                v for v in spec["volumes"]
                if isinstance(v, dict) and v.get("target") == "/profile-overlay"
            ]
            assert mount == {
                "type": "bind",
                "source": "${PROFILE_OVERLAY_DIR:-${PROFILE_DIR}}",
                "target": "/profile-overlay",
                "read_only": True,
                "bind": {"create_host_path": False},
            }, service

    def test_the_env_name_reaches_the_setting(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("TEMPLATES_OVERLAY_DIR", "/profile-overlay")
        assert Settings(_env_file=None).templates_overlay_dir == "/profile-overlay"

    def test_the_templates_carry_the_variable_empty(self) -> None:
        deploy_env = (_DEPLOY / ".env.example").read_text()
        assert re.search(r"^PROFILE_OVERLAY_DIR=$", deploy_env, re.M)
        assert re.search(r"^PROFILE_DIR=\S+$", deploy_env, re.M)
        root_env = (_REPO / ".env.example").read_text()
        assert re.search(r"^TEMPLATES_OVERLAY_DIR=$", root_env, re.M)
        generated = _function("generate_env")
        assert re.search(r"^PROFILE_OVERLAY_DIR=$", generated, re.M)
        assert re.search(r"^PROFILE_DIR=\$PROFILE_DIR_DEFAULT$", generated, re.M)

    def test_integration_names_what_the_code_does(
        self, base_dir: Path, overlay_dir: Path,
    ) -> None:
        """The contract in INTEGRATION.md against the code: the names,
        the mount, the info line, the core variables, the in_app line,
        and the refusal a reader is told to expect."""
        text = (_DEPLOY / "INTEGRATION.md").read_text()
        section = text[text.index("### `PROFILE_OVERLAY_DIR`"):
                       text.index("### `lane`")]
        for name in ("PROFILE_OVERLAY_DIR", "TEMPLATES_OVERLAY_DIR=/profile-overlay",
                     "in_app", "overrides=0", "`{locale}.yaml`"):
            assert name in section, name
        for variable in CORE_VARIABLES:
            assert f"`{{{variable}}}`" in section, variable
        with capture_logs() as logs:
            base = load_profile(FileProfileSource(base_dir))
            install_overlay(load_overlay(FileOverlaySource(overlay_dir), base))
        (line,) = [e for e in logs if e["event"] == "profile_overlay_installed"]
        assert f"`{line['event']}`" in section


def _function(name: str) -> str:
    """One top-level function of comms-deploy.sh, verbatim."""
    text = _SCRIPT.read_text(encoding="utf-8")
    match = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text, re.S | re.M)
    assert match, f"{name} not found at column zero"
    return match.group(0)


class TestTheDeployGuard:
    """comms-deploy.sh check_overlay_dir: some compose releases create a
    missing bind source despite create_host_path: false, and an empty
    directory is a legal overlay -- so the path is checked on the host
    before every `up`."""

    def _run(self, overlay: str) -> subprocess.CompletedProcess[str]:
        program = "\n".join([
            "set -u",
            "RED= GREEN= YELLOW= NC= ENV_FILE=/dev/null",
            f"load_env() {{ PROFILE_OVERLAY_DIR='{overlay}'; }}",
            _function("check_overlay_dir"),
            "check_overlay_dir",
            "echo reached-up",
        ])
        return subprocess.run(
            ["bash", "-c", program], capture_output=True, text=True, timeout=30,
        )

    def test_a_missing_path_stops_before_up(self, tmp_path: Path) -> None:
        """M13."""
        missing = tmp_path / "nowhere"
        done = self._run(str(missing))
        assert done.returncode == 1
        assert f"PROFILE_OVERLAY_DIR={missing} is not an existing directory" \
            in done.stdout
        assert "reached-up" not in done.stdout

    def test_a_file_stops_before_up(self, tmp_path: Path) -> None:
        file = tmp_path / "en.yaml"
        file.write_text("x")
        assert self._run(str(file)).returncode == 1

    @pytest.mark.parametrize("kind", ["empty", "directory"])
    def test_empty_or_a_directory_passes(self, tmp_path: Path, kind: str) -> None:
        done = self._run("" if kind == "empty" else str(tmp_path))
        assert done.returncode == 0
        assert "reached-up" in done.stdout

    @pytest.mark.parametrize(
        "verb", ["cmd_install", "cmd_update", "cmd_restart", "cmd_start"],
    )
    def test_every_verb_that_starts_checks_first(self, verb: str) -> None:
        body = _function(verb)
        assert "check_overlay_dir" in body
        assert body.index("check_overlay_dir") < body.index("$COMPOSE_CMD up")
