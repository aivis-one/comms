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

from typing import Any

import structlog

from app.core.config import settings
from app.profile.registry import registry

logger = structlog.get_logger()


class SafeDict(dict[str, Any]):
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
    variables: dict[str, Any] | None = None,
) -> str | None:
    """Render a notification template with variable substitution.

    Lookup order:
      1. Requested locale
      2. Fallback to the deploy default locale
      3. None -- caller falls back to the stored title/body

    A broken format spec in the template (e.g. "{amount:,.2f}" against
    a string variable) also returns None with a warning, as does
    attribute access on a JSON variable ("{user.name}") -- a template
    config error must not burn delivery retries (review 1.1); the
    caller's stored-value fallback covers it. Second line of defense:
    the profile validator (app/profile/loader.py) rejects these at
    startup, this guard covers templates registered past it.

    Args:
        notification_type: Registered type key (e.g. "unit_event").
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
    try:
        return template_str.format_map(safe_vars)
    except (ValueError, TypeError, IndexError, AttributeError) as exc:
        logger.warning(
            "template_render_error",
            type=notification_type,
            channel=channel,
            field=field,
            locale=requested,
            error=str(exc)[:200],
        )
        return None


def resolve_flag(
    notification_type: str,
    channel: str,
    field: str,
    locale: str | None = None,
) -> bool | None:
    """Resolve a presentation flag (Phase 3a item 1).

    Same per-field locale fallback as render(): requested locale ->
    deploy default locale -> None (the caller applies the channel
    default). Per-field on purpose: a locale file may override the
    texts and leave the flags to the default locale -- each field
    walks the chain independently, exactly like the text fields do.

    No third step: flags have no "stored" counterpart on the
    notification, the channel default is the end of the chain.
    """
    requested = locale or settings.default_locale

    value = registry.get_flag(requested, notification_type, channel, field)

    if value is None and requested != settings.default_locale:
        value = registry.get_flag(
            settings.default_locale, notification_type, channel, field,
        )

    return value
