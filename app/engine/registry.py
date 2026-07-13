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
# In Phase 1 the profile is a stub that tests configure directly.
# Phase 2 adds real profile loading (per-deploy data, not code).
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

    # -- Types --

    def register_type(self, key: str) -> None:
        """Register a single notification type key."""
        if not key:
            raise ValueError("Notification type key must be non-empty")
        self._types.add(key)

    def register_types(self, keys: Iterable[str]) -> None:
        """Register multiple notification type keys."""
        for key in keys:
            self.register_type(key)

    def is_registered(self, key: str) -> bool:
        """True if the type key was registered by the profile."""
        return key in self._types

    def registered_types(self) -> frozenset[str]:
        """Snapshot of all registered type keys."""
        return frozenset(self._types)

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
        logger.info("profile_registry_reset")


# Module-level singleton -- the per-deploy profile registers into it.
registry = ProfileRegistry()
