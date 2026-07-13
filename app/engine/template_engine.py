# =============================================================================
# COMMS Service -- Notification Template Engine
# =============================================================================
#
# Canonical merge of the cbshome (base) and velo template engines.
#
# WHAT CHANGED vs the donors:
#   - Templates come from the profile registry (in-memory, registered
#     per deploy), NOT from package-local YAML files. File loading of
#     per-deploy template data is Phase 2 profile work.
#   - render() returns None when no template exists anywhere (velo
#     semantics) so callers fall back to the notification's stored
#     title/body. cbshome returned a raw "{type}.{channel}.{field}"
#     string, which only made sense when templates shipped with the
#     code and a miss was a programming error.
#
# WHAT STAYED:
#   - SafeDict + str.format_map rendering (identical in both donors).
#   - {type}.{channel}.{field} addressing granularity (cbshome base --
#     richer than velo's per-type title/body, supports email subject).
#   - Locale fallback chain: requested -> deploy default locale.
# =============================================================================

import structlog

from app.core.config import settings
from app.engine.registry import registry

logger = structlog.get_logger()


class SafeDict(dict):  # type: ignore[type-arg]
    """Dict that returns '{key}' for missing keys instead of raising.

    Used with str.format_map() to safely render templates when some
    variables are not provided.
    """

    def __missing__(self, key: str) -> str:
        """Return the key wrapped in braces for missing values."""
        return "{" + key + "}"


def render(
    notification_type: str,
    channel: str,
    field: str,
    locale: str | None = None,
    variables: dict | None = None,  # type: ignore[type-arg]
) -> str | None:
    """Render a notification template with variable substitution.

    Lookup order:
      1. Requested locale
      2. Fallback to the deploy default locale
      3. None -- caller falls back to the stored title/body

    Args:
        notification_type: Registered type key (e.g. "booking_confirmed").
        channel: DeliveryChannel value (e.g. "telegram", "email").
        field: Template field (e.g. "body", "title", "subject").
        locale: Recipient locale; None means deploy default.
        variables: Dict of variables for substitution.

    Returns:
        Rendered template string, or None when no template is registered.
    """
    requested = locale or settings.default_locale

    template_str = registry.get_template(
        requested, notification_type, channel, field,
    )

    if template_str is None and requested != settings.default_locale:
        template_str = registry.get_template(
            settings.default_locale, notification_type, channel, field,
        )

    if template_str is None:
        logger.debug(
            "template_not_found",
            type=notification_type,
            channel=channel,
            field=field,
            locale=requested,
        )
        return None

    safe_vars = SafeDict(variables or {})
    return template_str.format_map(safe_vars)
