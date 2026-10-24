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
#   - str.format_map rendering (identical in both donors) -- but NOT the
#     donors' SafeDict, which printed a missing variable back as its own
#     "{name}": a typo in a template, or a variable the product did not
#     send, reached the person as braces (G5). A variable the job does
#     not carry now makes the field fall back like a broken format spec
#     does (render below; deploy/INTEGRATION.md section 3).
#   - {type}.{channel}.{field} addressing granularity (cbshome base --
#     richer than velo's per-type title/body, supports email subject).
#   - Locale fallback chain: requested -> deploy default locale.
#
# TEXT OVERLAY (PO-1): the registry may hold texts of the base profile
# and texts of the overlay that replaced single leaves of it. render()
# does not tell them apart -- the overlay is merged leaf by leaf at
# startup (app/profile/loader.py install_overlay); explain_texts() is
# where the difference is visible, walking the SAME chain as render().
# =============================================================================

from string import Formatter
from typing import Any

import structlog

from app.core.config import settings
from app.profile.registry import TextDecided, registry

logger = structlog.get_logger()


# What a field path that does not resolve raises inside format_map --
# and what a broken format spec raises: one family, one fallback.
_RENDER_ERRORS = (KeyError, ValueError, TypeError, IndexError, AttributeError)


def _template_fields(template: str) -> list[str]:
    """Every field path of a template, in the order format_map meets
    them: a field, then the fields of its own format spec
    ("{amount:{width}}" -> amount, width). The stdlib parser is the one
    format_map uses (_string.formatter_parser)."""
    fields: list[str] = []
    for _, field_name, spec, _ in Formatter().parse(template):
        if field_name is None:
            continue
        fields.append(field_name)
        if spec:
            fields.extend(_template_fields(spec))
    return fields


def _unresolved_field(
    template: str, variables: dict[str, Any],
) -> str | None:
    """The first field path of the template that does not resolve
    against the variables -- "titel", "user[name]" -- or None.

    The name is the TEMPLATE's text (the profile's), never a value of
    the letter (H8): the resolved value is looked up and dropped. It
    names the first path that fails, whichever failure format_map met
    first; a broken format spec over variables that all resolve has no
    name, as before.
    """
    try:
        fields = _template_fields(template)
    except ValueError:
        return None
    for field_name in fields:
        try:
            Formatter().get_field(field_name, (), variables)
        except _RENDER_ERRORS:
            return field_name
    return None


def _locale_chain(locale: str | None) -> tuple[str, ...]:
    """The locales a field is looked up in, in order: the requested
    one, then the deploy default. One copy for render, resolve_flag and
    explain_texts, so the explanation cannot walk another chain than
    the delivery does."""
    requested = locale or settings.default_locale
    if requested == settings.default_locale:
        return (requested,)
    return (requested, settings.default_locale)


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

    So does a variable the job does not carry (G5): "{titel}", a key
    the product did not send, a path into one ("{user[name]}" with no
    `user`, or a `user` without `name`, or a `user` that is not an
    object). The field is not printed with braces in it; it falls back
    exactly as when no template exists. The warning names the first
    field path that does not resolve (`variable`) -- the template's
    text, never a value of the letter.

    Args:
        notification_type: Registered type key (e.g. "unit_event").
        channel: DeliveryChannel value (e.g. "telegram", "email").
        field: Template field (e.g. "body", "title", "subject").
        locale: Recipient locale; None means deploy default.
        variables: Dict of variables for substitution.

    Returns:
        Rendered template string, or None when no template is
        registered or it does not render with these variables.
    """
    requested = locale or settings.default_locale

    template_str = None
    for step in _locale_chain(locale):
        template_str = registry.get_template(
            step, notification_type, channel, field,
        )
        if template_str is not None:
            break

    if template_str is None:
        logger.debug(
            "template_not_found",
            type=notification_type,
            channel=channel,
            field=field,
            locale=requested,
        )
        return None

    values = variables or {}
    try:
        return template_str.format_map(values)
    except _RENDER_ERRORS as exc:
        # The exception's CLASS, never its text (H8): a nested spec
        # ({amount:{width}}) puts a value of the letter into the text
        # ("Invalid format specifier '<value>' ..."). Its place is the
        # coordinates below -- str.format_map raises from C, so the
        # traceback's place would be this line for every template.
        # KeyError is a variable the job does not carry (G5): the same
        # configuration error, the same fallback, named by its path.
        unresolved = _unresolved_field(template_str, values)
        logger.warning(
            "template_render_error",
            type=notification_type,
            channel=channel,
            field=field,
            locale=requested,
            error_class=type(exc).__name__,
            **({} if unresolved is None else {"variable": unresolved}),
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
    for step in _locale_chain(locale):
        value = registry.get_flag(step, notification_type, channel, field)
        if value is not None:
            return value
    return None


def explain_texts(
    notification_type: str,
    channel: str,
    locale: str | None = None,
) -> dict[str, TextDecided]:
    """Where every text a channel would read for a type comes from.

    One entry per text field the locale chain resolves -- the same
    chain render() walks, field by field: the text, its layer (base
    profile or text overlay, PO-1), its file and the locale it was
    found in. A field absent from the result has no template anywhere
    in the chain, so the channel falls back to the stored title/body.

    Raises LookupError for a type the installed profile does not
    declare -- an unknown type must never read as "no templates".
    """
    if not registry.is_registered(notification_type):
        raise LookupError(
            f"type {notification_type!r} is not declared in the installed "
            f"profile"
        )
    chain = _locale_chain(locale)
    names: set[str] = set()
    for step in chain:
        names |= registry.text_fields(step, notification_type, channel)
    result: dict[str, TextDecided] = {}
    for name in sorted(names):
        for step in chain:
            decided = registry.text_origin(
                step, notification_type, channel, name,
            )
            if decided is not None:
                result[name] = decided
                break
    return result
