# =============================================================================
# COMMS Service -- Profile loader / validator tests (Phase 2 items 1-3)
# =============================================================================
# Item 1: the fixture profile on disk loads through the REAL loader
#   into the registry (types, categories, templates); a type without
#   templates is legal; startup entry point honors TEMPLATES_DIR.
# Item 2: a broken profile explodes AT LOAD TIME with a pointed
#   message -- tree shape, the YAML flow-mapping trap, format-spec dry
#   run, category shape; templates for unregistered types only warn.
# Item 3: locale fallback chain (recipient locale -> default locale ->
#   stored title/body) -- all three steps.
#
# The autouse `stub_profile` fixture already installs the fixture
# profile through load_profile/install_profile, so "the loader works"
# is exercised by every test in the suite; tests here assert the
# RESULT of that load explicitly.
# =============================================================================

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.core.config import settings
from app.core.exceptions import ProfileError
from app.engine.constants import TargetType
from app.engine.formatters import TelegramFormatter
from app.engine.models import Notification, NotificationDelivery
from app.engine.profile import (
    FileProfileSource,
    install_profile,
    install_profile_from_settings,
    load_profile,
)
from app.engine.registry import registry
from app.engine.service import create_notification
from app.engine.template_engine import render
from tests.conftest import FIXTURE_PROFILE_DIR
from tests.helpers import next_phase2_telegram_id

BOT_URL = "https://t.me/comms_testbot"


def _write_profile(
    root: Path,
    types_yaml: str,
    templates: dict[str, str] | None = None,
) -> Path:
    """Materialize a throwaway profile directory under tmp_path."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "types.yaml").write_text(types_yaml, encoding="utf-8")
    templates_dir = root / "templates"
    templates_dir.mkdir(exist_ok=True)
    for locale, content in (templates or {}).items():
        (templates_dir / f"{locale}.yaml").write_text(
            content, encoding="utf-8",
        )
    return root


class TestFixtureProfileLoads:
    """Item 1: the on-disk fixture profile lands in the registry."""

    def test_types_and_categories_registered(self) -> None:
        """Types, per-type categories and the category set are loaded."""
        assert registry.is_registered("unit_event")
        assert registry.is_registered("unit_plain")
        assert registry.category_of("unit_event") == "unit_updates"
        # Family granularity: three reminder types, one category.
        assert registry.category_of("unit_rem_24h") == "unit_reminder"
        assert registry.category_of("unit_rem_1h") == "unit_reminder"
        assert registry.category_of("unit_rem_10m") == "unit_reminder"
        # A type with an empty spec has no category.
        assert registry.category_of("unit_plain") is None
        assert registry.registered_categories() == {
            "unit_updates",
            "unit_reminder",
        }

    def test_templates_registered(self) -> None:
        """Template trees for both fixture locales are queryable."""
        assert registry.get_template(
            "en", "unit_event", "telegram", "title",
        ) == "{title}"
        assert registry.get_template(
            "ru", "unit_event", "telegram", "body",
        ) == "RU: {body}"

    async def test_type_without_templates_is_legal(
        self, db_session: AsyncSession,
    ) -> None:
        """Item 1 done-when: a dictionary type with no templates works."""
        notification = await create_notification(
            db_session,
            type="unit_plain",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
            channels=["telegram"],
        )
        assert notification.type == "unit_plain"


class TestStartupEntryPoint:
    """install_profile_from_settings honors TEMPLATES_DIR."""

    def test_installs_from_configured_dir(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A configured TEMPLATES_DIR loads the profile at startup."""
        monkeypatch.setattr(
            settings, "templates_dir", str(FIXTURE_PROFILE_DIR),
        )
        registry.reset()
        assert install_profile_from_settings() is True
        assert registry.is_registered("unit_event")
        assert registry.category_of("unit_rem_1h") == "unit_reminder"

    def test_empty_dir_tolerated_in_development(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Dev without TEMPLATES_DIR starts with an empty registry.

        app_env is pinned explicitly: CI exports APP_ENV=ci, and this
        test asserts DEVELOPMENT behavior regardless of the runner env
        (mirrors the sibling test pinning app_env="ci").
        """
        monkeypatch.setattr(settings, "app_env", "development")
        monkeypatch.setattr(settings, "templates_dir", "")
        registry.reset()
        with capture_logs() as logs:
            assert install_profile_from_settings() is False
        assert any(
            log["event"] == "profile_not_configured" for log in logs
        )

    def test_empty_dir_fatal_outside_development(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Anywhere but dev a missing profile is a deploy error."""
        monkeypatch.setattr(settings, "templates_dir", "")
        monkeypatch.setattr(settings, "app_env", "ci")
        with pytest.raises(ProfileError, match="TEMPLATES_DIR"):
            install_profile_from_settings()

    def test_missing_directory_is_fatal(self, tmp_path: Path) -> None:
        """A TEMPLATES_DIR that does not exist fails loudly."""
        with pytest.raises(ProfileError, match="does not exist"):
            load_profile(FileProfileSource(tmp_path / "nope"))

    def test_missing_types_file_is_fatal(self, tmp_path: Path) -> None:
        """A profile directory without types.yaml fails loudly."""
        (tmp_path / "templates").mkdir()
        with pytest.raises(ProfileError, match=r"types\.yaml"):
            load_profile(FileProfileSource(tmp_path))


class TestValidator:
    """Item 2: broken profiles explode at load time, with hints."""

    def test_types_as_list_rejected_with_hint(self, tmp_path: Path) -> None:
        """The type dictionary must be a mapping, not a list."""
        root = _write_profile(tmp_path, "- unit_event\n- unit_plain\n")
        with pytest.raises(ProfileError, match="not a list"):
            load_profile(FileProfileSource(root))

    def test_flow_mapping_trap_gets_pointed_hint(
        self, tmp_path: Path,
    ) -> None:
        """Unquoted `body: {title}` is caught with a flow-mapping hint."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {"en": "unit_event:\n  telegram:\n    body: {title}\n"},
        )
        with pytest.raises(ProfileError, match="flow mapping"):
            load_profile(FileProfileSource(root))

    def test_broken_format_spec_fails_dry_run(self, tmp_path: Path) -> None:
        """An unbalanced brace dies at startup, not on delivery."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {"en": 'unit_event:\n  telegram:\n    body: "{title"\n'},
        )
        with pytest.raises(ProfileError, match="format spec"):
            load_profile(FileProfileSource(root))

    def test_non_string_leaf_rejected(self, tmp_path: Path) -> None:
        """A numeric template leaf is rejected."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {"en": "unit_event:\n  telegram:\n    body: 42\n"},
        )
        with pytest.raises(ProfileError, match="must be a string"):
            load_profile(FileProfileSource(root))

    def test_type_level_must_map_channels(self, tmp_path: Path) -> None:
        """A template type mapping straight to a string is rejected."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {"en": 'unit_event: "hello"\n'},
        )
        with pytest.raises(ProfileError, match="must map channels"):
            load_profile(FileProfileSource(root))

    def test_channel_level_must_map_fields(self, tmp_path: Path) -> None:
        """A channel mapping straight to a string is rejected."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {"en": 'unit_event:\n  telegram: "hello"\n'},
        )
        with pytest.raises(ProfileError, match="must map fields"):
            load_profile(FileProfileSource(root))

    def test_bad_category_rejected(self, tmp_path: Path) -> None:
        """A non-string category in the type dictionary is rejected."""
        root = _write_profile(tmp_path, "unit_event:\n  category: 5\n")
        with pytest.raises(ProfileError, match="category"):
            load_profile(FileProfileSource(root))

    def test_invalid_yaml_syntax_is_fatal(self, tmp_path: Path) -> None:
        """Unparseable YAML surfaces as ProfileError with the path."""
        root = _write_profile(tmp_path, "unit_event: [unclosed\n")
        with pytest.raises(ProfileError, match="Invalid YAML"):
            load_profile(FileProfileSource(root))

    def test_template_for_unregistered_type_warns(
        self, tmp_path: Path,
    ) -> None:
        """A template for a type missing from the dictionary is a
        loud warning, not a startup failure (additive contract)."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {"en": 'ghost_type:\n  telegram:\n    body: "{body}"\n'},
        )
        with capture_logs() as logs:
            profile = load_profile(FileProfileSource(root))
        assert any(
            log["event"] == "template_for_unregistered_type"
            and log["type"] == "ghost_type"
            for log in logs
        )
        # The template is still carried (the type may be enabled later).
        assert "ghost_type" in profile.templates["en"]

    def test_empty_types_document_warns(self, tmp_path: Path) -> None:
        """An empty types.yaml loads as zero types, with a warning."""
        root = _write_profile(tmp_path, "# nothing yet\n")
        with capture_logs() as logs:
            profile = load_profile(FileProfileSource(root))
        assert profile.types == {}
        assert any(log["event"] == "profile_types_empty" for log in logs)

    def test_unknown_spec_keys_tolerated(self, tmp_path: Path) -> None:
        """Unknown per-type fields pass through silently (additive)."""
        root = _write_profile(
            tmp_path,
            "unit_event:\n  category: cat_a\n  future_field: whatever\n",
        )
        profile = load_profile(FileProfileSource(root))
        registry.reset()
        install_profile(profile)
        assert registry.category_of("unit_event") == "cat_a"


class TestLocaleFallbackChain:
    """Item 3: recipient locale -> default locale -> stored fallback."""

    def test_step1_recipient_locale_template(self) -> None:
        """A template in the recipient's locale wins."""
        assert render(
            "unit_event", "telegram", "body",
            locale="ru", variables={"body": "B"},
        ) == "RU: B"

    def test_step2_default_locale_fallback(self) -> None:
        """A field missing in `ru` falls back to the `en` template."""
        assert render(
            "unit_event", "telegram", "title",
            locale="ru", variables={"title": "T"},
        ) == "T"

    def test_step3_no_template_anywhere_returns_none(self) -> None:
        """No template in any locale -> None (caller uses stored)."""
        assert render(
            "unit_plain", "telegram", "body",
            locale="ru", variables={"body": "B"},
        ) is None

    async def test_step3_formatter_uses_stored_title_body(self) -> None:
        """End to end: unit_plain delivers the stored title/body."""
        calls: list[dict[str, Any]] = []

        class _FakeBot:
            async def send_message(self, **kwargs: Any) -> Any:
                calls.append(kwargs)
                return SimpleNamespace(message_id=1)

        formatter = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        notification = Notification(
            type="unit_plain",
            title="Stored title",
            body="Stored body",
            target_type="user",
            target_value="*",
            action_data=None,
        )
        delivery = NotificationDelivery(
            channel="telegram", channel_options=None,
        )
        recipient = Recipient(
            telegram_id=next_phase2_telegram_id(), locale="ru", active=True,
        )

        assert await formatter.deliver(notification, delivery, recipient)
        (call,) = calls
        assert "Stored title" in call["text"]
        assert "Stored body" in call["text"]

    def test_missing_placeholder_does_not_break_send(self) -> None:
        """SafeDict keeps unknown placeholders literal, render survives."""
        assert render(
            "unit_event", "telegram", "body",
            locale="en", variables={"body": "B"},
        ) == "B [{extra}]"


class TestFormatSpecProbing:
    """Phase 2.1 item 1: the dry-run probe accepts a spec iff at
    least one JSON scalar type (str, int, float) accepts it."""

    def test_numeric_specs_pass_validation_and_render(
        self, tmp_path: Path,
    ) -> None:
        """Money/number/percent/padding specs load AND render.

        This is the exact false positive of the original dry run:
        {amount:,.2f} is valid at runtime (numbers pass through
        unescaped) but the string-placeholder dry run rejected it.
        """
        root = _write_profile(
            tmp_path,
            "money_event: {}\n",
            {
                "en": (
                    "money_event:\n"
                    "  telegram:\n"
                    '    body: "Total: {amount:,.2f} / {n:03d}'
                    ' / {x:.1%} / {s:>4}"\n'
                ),
            },
        )
        profile = load_profile(FileProfileSource(root))
        registry.reset()
        install_profile(profile)
        assert render(
            "money_event", "telegram", "body",
            locale="en",
            variables={"amount": 1234.5, "n": 7, "x": 0.256, "s": "hi"},
        ) == "Total: 1,234.50 / 007 / 25.6% /   hi"

    def test_garbage_spec_still_fatal(self, tmp_path: Path) -> None:
        """A spec no JSON scalar accepts kills startup (true positive)."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {"en": 'unit_event:\n  telegram:\n    body: "{a:zzz}"\n'},
        )
        with pytest.raises(ProfileError, match="format spec"):
            load_profile(FileProfileSource(root))

    def test_strftime_spec_is_fatal(self, tmp_path: Path) -> None:
        """Date-like specs are true positives: variables travel via
        JSONB, a datetime can never reach render(), so {when:%d.%m}
        is a guaranteed runtime failure. Dates arrive pre-formatted."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {
                "en": (
                    "unit_event:\n"
                    "  telegram:\n"
                    '    body: "{when:%d.%m %H:%M}"\n'
                ),
            },
        )
        with pytest.raises(ProfileError, match="format spec"):
            load_profile(FileProfileSource(root))

    def test_attribute_access_rejected_with_hint(
        self, tmp_path: Path,
    ) -> None:
        """{user.name} dies at validation: JSON objects arrive as
        dicts, getattr fails at runtime -- the hint points at item
        access instead."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {"en": 'unit_event:\n  telegram:\n    body: "{user.name}"\n'},
        )
        with pytest.raises(ProfileError, match="item access"):
            load_profile(FileProfileSource(root))

    def test_item_access_passes_and_renders(self, tmp_path: Path) -> None:
        """{user[name]} works on the JSON dicts that actually arrive."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {"en": 'unit_event:\n  telegram:\n    body: "Hi {user[name]}"\n'},
        )
        profile = load_profile(FileProfileSource(root))
        registry.reset()
        install_profile(profile)
        assert render(
            "unit_event", "telegram", "body",
            locale="en",
            variables={"user": {"name": "Zo"}},
        ) == "Hi Zo"

    def test_nested_spec_passes(self, tmp_path: Path) -> None:
        """Nested specs ({x:{width}.2f}) expand and validate."""
        root = _write_profile(
            tmp_path,
            "unit_event: {}\n",
            {"en": 'unit_event:\n  telegram:\n    body: "{x:{width}.2f}"\n'},
        )
        load_profile(FileProfileSource(root))

    def test_render_survives_attribute_access_at_runtime(self) -> None:
        """Second line of defense: a template registered PAST the
        validator with attribute access falls back (None + warning)
        instead of burning delivery attempts."""
        registry.register_templates(
            "en",
            {"unit_event": {"telegram": {"note": "{user.name}"}}},
        )
        assert render(
            "unit_event", "telegram", "note",
            locale="en",
            variables={"user": {"name": "Zo"}},
        ) is None


class TestKeyLengthValidation:
    """Phase 2.2 item 2: profile keys are length-checked at startup
    against the ACTUAL widths of the DB columns they land in -- a
    too-long key would otherwise DataError at create/mute time."""

    def test_type_key_too_long_is_fatal(self, tmp_path: Path) -> None:
        """A 51-char type key exceeds notifications.type (50)."""
        root = _write_profile(tmp_path, f"{'k' * 51}: {{}}\n")
        with pytest.raises(ProfileError, match=r"notifications\.type"):
            load_profile(FileProfileSource(root))

    def test_category_too_long_is_fatal(self, tmp_path: Path) -> None:
        """A 51-char category exceeds category_mutes.category (50)."""
        root = _write_profile(
            tmp_path, f"unit_event:\n  category: {'c' * 51}\n",
        )
        with pytest.raises(ProfileError, match=r"category_mutes\.category"):
            load_profile(FileProfileSource(root))

    def test_max_length_key_passes(self, tmp_path: Path) -> None:
        """Exactly at the limit is legal (boundary, not off-by-one)."""
        root = _write_profile(
            tmp_path, f"{'k' * 50}:\n  category: {'c' * 50}\n",
        )
        profile = load_profile(FileProfileSource(root))
        assert "k" * 50 in profile.types
