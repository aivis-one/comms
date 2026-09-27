# =============================================================================
# COMMS Service -- Profile Registry (notification types + templates)
# =============================================================================
#
# The de-domainization seam. Both donors hardcoded a NotificationType
# enum and shipped template YAML inside the package -- pure domain
# vocabulary that cannot live in a product-agnostic core.
#
# Instead, the per-deploy PRODUCT PROFILE registers:
#   - the dictionary of notification type keys it emits,
#   - the templates for rendering them, per locale.
#
# In Phase 1 the profile was a stub that tests configured directly.
# Phase 2 adds real profile loading from disk (app/profile/loader.py)
# and per-type preference categories for mute gating.
#
# F1.1 adds the TYPE RECORD: the closed, typed set of fields a profile
# may declare per notification type (app/profile/loader.py owns the
# schema). The registry stores, for every field of every type, the
# VALUE and the LAYER that decided it -- the profile, or the comms
# default -- so "why did this type go there" has an answer that code
# can ask for (explain), not only a line in the startup log.
#
# TEMPLATE STRUCTURE (same shape as cbshome YAML, held in memory):
#   {locale: {type: {channel: {field: leaf}}}}
#   e.g. templates["en"]["unit_event"]["telegram"]["body"]
#
#   Since Phase 3a (item 1) the sheet owns PRESENTATION, not only
#   text: a leaf is either a template STRING (title / body / subject /
#   button_text / ...) or a presentation FLAG (bool: disable_preview /
#   silent). The two kinds are read through separate, type-disciplined
#   accessors (get_template / get_flag) so a bool can never leak into
#   a rendered message as "True".
#
# THREADING:
#   Registration happens at startup / test setup, before the worker or
#   API serve traffic; reads are lock-free dict lookups.
# =============================================================================

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import structlog

logger = structlog.get_logger()

# Nested template mapping: {type: {channel: {field: leaf}}}; a leaf
# is a template string or a presentation flag (Phase 3a item 1).
TemplateTree = dict[str, dict[str, dict[str, str | bool]]]


class Layer(StrEnum):
    """Which layer decided a value.

    For a type record: the profile, or the comms default. For a value
    of an accepted request that the envelope may set over the profile
    (the expiry, F1.2): the envelope as well.
    """

    # The request's own envelope carried the value (F1.2).
    ENVELOPE = "envelope"
    # The product profile declared the field for this type.
    PROFILE = "profile"
    # The profile left the field out; comms supplied the value.
    DEFAULT = "default"


@dataclass(frozen=True)
class Decided:
    """One field of one type: the value and who decided it.

    `source` says WHERE the value came from, in words an operator can
    follow: the profile file for the profile layer; for the default
    layer, either "comms default" or the setting the default is read
    from (the retry defaults are a reference to settings, not a copy).
    """

    value: Any
    layer: Layer
    source: str


@dataclass(frozen=True)
class TypeRecord:
    """Every field of one notification type, each with its layer.

    Built by the profile loader, which guarantees that EVERY field of
    the schema is present -- a field is either declared or defaulted,
    never missing.
    """

    fields: Mapping[str, Decided]

    def value(self, field: str) -> Any:
        """The effective value of a field (KeyError if not a field)."""
        return self.fields[field].value


class ProfileRegistry:
    """Holds the product profile's notification types and templates."""

    def __init__(self) -> None:
        self._types: set[str] = set()
        self._templates: dict[str, TemplateTree] = {}
        # type_key -> preference category (Phase 2). Categories are
        # domain vocabulary too: the profile declares them per type
        # (family granularity -- e.g. reminder_24h/1h/10min all map
        # to "reminder"). A type without a category is exempt from
        # mute gating.
        self._categories: dict[str, str] = {}
        # type_key -> the full type record with layers (F1.1). Only a
        # type installed from a profile has one: tests that register a
        # bare key directly get no record, and explain() says so.
        self._records: dict[str, TypeRecord] = {}

    # -- Types --

    def register_type(
        self,
        key: str,
        *,
        category: str | None = None,
        record: TypeRecord | None = None,
    ) -> None:
        """Register a single notification type key.

        `category` (optional) links the type to a preference category
        for mute gating; the profile's type dictionary supplies it.
        `record` (optional) is the type's full record with layers, as
        the profile loader built it.
        """
        if not key:
            raise ValueError("Notification type key must be non-empty")
        self._types.add(key)
        if record is not None:
            self._records[key] = record
        if category is not None:
            if not category:
                raise ValueError(
                    f"Category for type {key!r} must be non-empty"
                )
            self._categories[key] = category

    def register_types(self, keys: Iterable[str]) -> None:
        """Register multiple notification type keys (no categories)."""
        for key in keys:
            self.register_type(key)

    def is_registered(self, key: str) -> bool:
        """True if the type key was registered by the profile."""
        return key in self._types

    def registered_types(self) -> frozenset[str]:
        """Snapshot of all registered type keys."""
        return frozenset(self._types)

    # -- Categories --

    def category_of(self, type_key: str) -> str | None:
        """Preference category for a type; None if the type has none."""
        return self._categories.get(type_key)

    def registered_categories(self) -> frozenset[str]:
        """Snapshot of all categories declared by the profile.

        The source of truth for preference validation: a mute may only
        be set for a category the profile actually declares.
        """
        return frozenset(self._categories.values())

    # -- Type records (F1.1) --

    def record_of(self, type_key: str) -> TypeRecord | None:
        """The type's record; None if it was not installed from a
        profile (unknown type, or a bare key registered directly)."""
        return self._records.get(type_key)

    def explain(self, type_key: str, field: str) -> Decided:
        """Why a field of a type has its value: value, layer, source.

        Raises LookupError naming what is missing -- an unknown type or
        field must never read as "decided by default".
        """
        record = self._records.get(type_key)
        if record is None:
            raise LookupError(
                f"type {type_key!r} has no profile record: it is not "
                f"declared in the installed profile"
            )
        if field not in record.fields:
            raise LookupError(
                f"{field!r} is not a field of the type record; fields: "
                f"{', '.join(sorted(record.fields))}"
            )
        return record.fields[field]

    # -- Templates --

    def register_templates(self, locale: str, tree: TemplateTree) -> None:
        """Merge a template tree for a locale into the registry.

        Later registrations override earlier ones at the field level,
        so a profile can be assembled from several fragments.
        """
        bucket = self._templates.setdefault(locale, {})
        for type_key, channels in tree.items():
            type_bucket = bucket.setdefault(type_key, {})
            for channel, fields in channels.items():
                channel_bucket = type_bucket.setdefault(channel, {})
                channel_bucket.update(fields)
        logger.info(
            "templates_registered",
            locale=locale,
            types=sorted(tree.keys()),
        )

    def get_template(
        self,
        locale: str,
        type_key: str,
        channel: str,
        field: str,
    ) -> str | None:
        """Look up a template string; None if missing or not a string.

        Presentation FLAGS (bool leaves, Phase 3a item 1) are
        deliberately invisible here: the old str() cast would turn
        True into the rendered text "True". Flags go through
        get_flag().
        """
        value = self._get_leaf(locale, type_key, channel, field)
        return value if isinstance(value, str) else None

    def get_flag(
        self,
        locale: str,
        type_key: str,
        channel: str,
        field: str,
    ) -> bool | None:
        """Look up a presentation flag; None if missing or not a bool.

        Mirror of get_template's type discipline (Phase 3a item 1).
        The profile validator enforces bool for flag fields at load
        time; anything registered past it that is not a bool resolves
        to None here, and the channel default applies.
        """
        value = self._get_leaf(locale, type_key, channel, field)
        return value if isinstance(value, bool) else None

    def _get_leaf(
        self,
        locale: str,
        type_key: str,
        channel: str,
        field: str,
    ) -> str | bool | None:
        """Raw leaf lookup shared by the typed accessors."""
        return (
            self._templates.get(locale, {})
            .get(type_key, {})
            .get(channel, {})
            .get(field)
        )

    # -- Lifecycle --

    def reset(self) -> None:
        """Clear all registrations (tests / profile reload)."""
        self._types.clear()
        self._templates.clear()
        self._categories.clear()
        self._records.clear()
        logger.info("profile_registry_reset")


# Module-level singleton -- the per-deploy profile registers into it.
registry = ProfileRegistry()
