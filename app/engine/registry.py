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
# Phase 2 adds real profile loading from disk (app/engine/profile.py)
# and per-type preference categories for mute gating.
#
# TEMPLATE STRUCTURE (same shape as cbshome YAML, held in memory):
#   {locale: {type: {channel: {field: template_str}}}}
#   e.g. templates["en"]["booking_confirmed"]["telegram"]["body"]
#
# THREADING:
#   Registration happens at startup / test setup, before the worker or
#   API serve traffic; reads are lock-free dict lookups.
# =============================================================================

from collections.abc import Iterable

import structlog

logger = structlog.get_logger()

# Nested template mapping: {type: {channel: {field: template_str}}}.
TemplateTree = dict[str, dict[str, dict[str, str]]]


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

    # -- Types --

    def register_type(self, key: str, *, category: str | None = None) -> None:
        """Register a single notification type key.

        `category` (optional) links the type to a preference category
        for mute gating; the profile's type dictionary supplies it.
        """
        if not key:
            raise ValueError("Notification type key must be non-empty")
        self._types.add(key)
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
        """Look up a template string; None if any level is missing."""
        value = (
            self._templates.get(locale, {})
            .get(type_key, {})
            .get(channel, {})
            .get(field)
        )
        return None if value is None else str(value)

    # -- Lifecycle --

    def reset(self) -> None:
        """Clear all registrations (tests / profile reload)."""
        self._types.clear()
        self._templates.clear()
        self._categories.clear()
        logger.info("profile_registry_reset")


# Module-level singleton -- the per-deploy profile registers into it.
registry = ProfileRegistry()
