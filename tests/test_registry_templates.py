# =============================================================================
# COMMS Service -- Profile registry + template engine tests
# =============================================================================
# Handoff item 3: types and templates come from the registry; the
# engine must reject anything the profile did not register.
# =============================================================================

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ValidationError
from app.engine.constants import TargetType
from app.engine.service import create_notification
from app.engine.template_engine import render
from app.profile.registry import registry
from tests.helpers import intake_fields


class TestRegistry:
    """Type registration gates notification creation."""

    async def test_unregistered_type_rejected(
        self, db_session: AsyncSession,
    ) -> None:
        """create_notification refuses a type the profile did not register."""
        with pytest.raises(ValidationError):
            await create_notification(
                db_session,
                **intake_fields(),
                type="not_registered_anywhere",
                title="T",
                body="B",
                target_type=TargetType.ALL,
                target_value="*",
            )

    async def test_registered_type_accepted(
        self, db_session: AsyncSession,
    ) -> None:
        """A stub-profile type passes validation."""
        notification = await create_notification(
            db_session,
            **intake_fields(),
            type="unit_event_in_app",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
        )
        assert notification.type == "unit_event_in_app"

    def test_registered_types_snapshot(self) -> None:
        """Registry reports the stub profile's dictionary."""
        assert "unit_event" in registry.registered_types()
        assert registry.is_registered("unit_rem_24h")
        assert not registry.is_registered("bogus")


class TestTemplateEngine:
    """Rendering over the registry with locale fallback."""

    def test_render_en(self) -> None:
        """Registered template renders with variables."""
        text = render(
            notification_type="unit_event",
            channel="telegram",
            field="body",
            locale="en",
            variables={"body": "hello", "extra": "x1"},
        )
        assert text == "hello [x1]"

    def test_render_locale_specific(self) -> None:
        """A ru template wins for ru locale."""
        text = render(
            notification_type="unit_event",
            channel="telegram",
            field="body",
            locale="ru",
            variables={"body": "privet"},
        )
        assert text == "RU: privet"

    def test_render_falls_back_to_default_locale(self) -> None:
        """Locale without templates falls back to the deploy default (en)."""
        text = render(
            notification_type="unit_event",
            channel="telegram",
            field="body",
            locale="de",
            variables={"body": "hallo", "extra": "x2"},
        )
        assert text == "hallo [x2]"

    def test_render_missing_returns_none(self) -> None:
        """No template anywhere -> None (caller uses stored title/body)."""
        assert (
            render(
                notification_type="unit_rem_24h",
                channel="telegram",
                field="body",
                locale="en",
            )
            is None
        )

    def test_missing_variable_prints_its_placeholder(self) -> None:
        """A missing variable: no KeyError out of render, and its
        placeholder where the value would stand (TPL-1).

        It asserted None -- the stored-text fallback G5 made the rule,
        right while it was. Before G5 it asserted "hello [{extra}]",
        right while SafeDict printed a missing top-level variable back.
        The owner's decision of 4 November withdrew G5: the hole is
        printed. The pair: with `extra` the sheet renders."""
        text = render(
            notification_type="unit_event",
            channel="telegram",
            field="body",
            locale="en",
            variables={"body": "hello"},
        )
        assert text == "hello [{extra}]"
        assert render(
            notification_type="unit_event",
            channel="telegram",
            field="body",
            locale="en",
            variables={"body": "hello", "extra": "e"},
        ) == "hello [e]"

    def test_broken_format_spec_returns_none(self) -> None:
        """Review 1.1: a config error in the template must not raise --
        None routes the caller to the stored-value fallback."""
        registry.register_templates(
            "en",
            {"unit_event": {"telegram": {"body": "{extra:,.2f}"}}},
        )
        assert (
            render(
                notification_type="unit_event",
                channel="telegram",
                field="body",
                locale="en",
                variables={"extra": "not-a-number"},
            )
            is None
        )

    def test_a_missing_variable_is_printed_back(self) -> None:
        """"{missing}" with no `missing` renders to "{missing}" (TPL-1).

        It asserted None under G5, and replaced test_safedict_direct,
        which asserted SafeDict's "{key}" for a missing key. The value
        is the same as SafeDict's for a top-level name, by another
        mechanism -- render's own formatter, which also covers a path
        into a dict and a null, where SafeDict raised or printed None."""
        registry.register_templates(
            "en", {"unit_event": {"telegram": {"body": "{missing}"}}},
        )
        assert render(
            notification_type="unit_event",
            channel="telegram",
            field="body",
            locale="en",
            variables={},
        ) == "{missing}"

    def test_field_granularity(self) -> None:
        """{type}.{channel}.{field} addressing (email subject)."""
        text = render(
            notification_type="unit_event",
            channel="email",
            field="subject",
            locale="en",
            variables={"title": "Hi"},
        )
        assert text == "COMMS: Hi"
