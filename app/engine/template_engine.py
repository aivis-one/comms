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
#   - str.format rendering (identical in both donors), through
#     _LetterFormatter -- the same parser and the same lookups as
#     str.format_map (string.Formatter, _string).
#
# A VARIABLE THE JOB DOES NOT CARRY IS PRINTED AS ITS PLACEHOLDER (TPL-1,
# the owner's decision of 4 November 2026; G5's fallback is withdrawn):
# "{paid_amount}", "{user[name]}" stands in the letter where the field
# did not resolve -- a key the product did not send, a path into one
# that does not go through, a JSON null. Louder is better than quieter:
# the person sees that something is missing, and the job's journal names
# it (template_unresolved on the channel's `accepted` row,
# app/engine/journal.py). Every other field renders as usual. A TYPE
# WITHOUT A TEMPLATE for the field still takes the stored title/body
# (render -> None): that is the product's own text, not a hole in one.
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

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from string import Formatter
from typing import Any

import structlog

from app.core.config import settings
from app.profile.registry import TextDecided, registry
from app.profile.template_fields import path_has_attribute, spec_too_wide

logger = structlog.get_logger()


# What a broken format spec raises over a value that is there, and an
# attribute in a path -- the field then renders as if it had no template
# (render_field -> None).
_RENDER_ERRORS = (KeyError, ValueError, TypeError, IndexError, AttributeError)
# A path that does not resolve: the key or index is not there, or the
# value on the way is not a mapping or a list.
_LOOKUP_ERRORS = (KeyError, IndexError, TypeError)


class _AttributeInPathError(AttributeError):
    """A field path that takes an attribute: refused at startup
    (app/profile/template_fields.py); here it is not looked up at all --
    the field renders as if it had no template."""


@dataclass(frozen=True)
class _Unresolved:
    """The stand-in for a field that did not resolve: its path."""

    path: str


@dataclass(frozen=True)
class RenderedField:
    """A template field as rendered, and the paths that did not resolve
    in it -- the template's text, never a value of the letter."""

    text: str
    unresolved: tuple[str, ...]


class _LetterFormatter(Formatter):
    """str.format with the letter's variables, where a field that does
    not resolve -- or resolves to null -- prints its placeholder
    "{path}" (escaped by `escape` for the channel's markup) instead of
    raising, and is noted. A width or precision above
    MAX_TEMPLATE_SPEC_WIDTH, from the template or from the letter
    ({a:{w}}), is a broken format spec."""

    def __init__(self, escape: Callable[[str], str]) -> None:
        super().__init__()
        self._escape = escape
        self.unresolved: list[str] = []

    def get_field(
        self, field_name: str, args: Sequence[Any], kwargs: Mapping[str, Any],
    ) -> Any:
        if path_has_attribute(field_name):
            raise _AttributeInPathError(field_name)
        try:
            obj, first = super().get_field(field_name, args, kwargs)
        except _LOOKUP_ERRORS:
            obj, first = None, field_name
        if obj is None:
            if field_name not in self.unresolved:
                self.unresolved.append(field_name)
            return _Unresolved(field_name), first
        return obj, first

    def convert_field(self, value: Any, conversion: str | None) -> Any:
        if isinstance(value, _Unresolved):
            return value
        return super().convert_field(value, conversion)

    def format_field(self, value: Any, format_spec: str) -> str:
        if isinstance(value, _Unresolved):
            return self._escape("{" + value.path + "}")
        if spec_too_wide(format_spec):
            raise ValueError("a width or precision above the template limit")
        return str(super().format_field(value, format_spec))


def _locale_chain(locale: str | None) -> tuple[str, ...]:
    """The locales a field is looked up in, in order: the requested
    one, then the deploy default. One copy for render, resolve_flag and
    explain_texts, so the explanation cannot walk another chain than
    the delivery does."""
    requested = locale or settings.default_locale
    if requested == settings.default_locale:
        return (requested,)
    return (requested, settings.default_locale)


def render_field(
    notification_type: str,
    channel: str,
    field: str,
    locale: str | None = None,
    variables: dict[str, Any] | None = None,
    *,
    escape: Callable[[str], str] = str,
) -> RenderedField | None:
    """Render a notification template field with variable substitution.

    Lookup order:
      1. Requested locale
      2. Fallback to the deploy default locale
      3. None -- caller falls back to the stored title/body

    A FIELD THAT DOES NOT RESOLVE PRINTS ITS PLACEHOLDER (TPL-1):
    "{titel}", a key the product did not send, a path into one
    ("{user[name]}" with no `user`, or a `user` without `name`, or a
    `user` that is not an object), a JSON null anywhere on the path. The
    placeholder is the path as the template writes it, in braces, passed
    through `escape` (the telegram text is HTML; the button and the email
    are plain text) -- and the path is in `unresolved`, in the order the
    template meets it, once. The rest of the field renders as usual.

    A broken format spec over a value that is there ("{amount:,.2f}"
    against a string), a width or precision above the template limit,
    and an attribute in a path return None with a warning, as before --
    the caller's stored-value fallback covers them; the profile
    validator (app/profile/loader.py) refuses all three at startup, this
    covers templates registered past it. The warning names the class,
    never the text (H8): a nested spec puts a value of the letter in the
    exception's text.

    Args:
        notification_type: Registered type key (e.g. "unit_event").
        channel: DeliveryChannel value (e.g. "telegram", "email").
        field: Template field (e.g. "body", "title", "subject").
        locale: Recipient locale; None means deploy default.
        variables: Dict of variables for substitution.
        escape: What a placeholder goes through for the channel's markup.

    Returns:
        The rendered field and its unresolved paths, or None when no
        template is registered or it does not render.
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

    formatter = _LetterFormatter(escape)
    try:
        text = formatter.vformat(template_str, (), variables or {})
    except _RENDER_ERRORS as exc:
        # The exception's CLASS, never its text (H8): a nested spec
        # ({amount:{width}}) puts a value of the letter into the text
        # ("Invalid format specifier '<value>' ..."). Its place is the
        # coordinates below -- the traceback's place would be this line
        # for every template.
        logger.warning(
            "template_render_error",
            type=notification_type,
            channel=channel,
            field=field,
            locale=requested,
            error_class=type(exc).__name__,
            **(
                {"variable": formatter.unresolved[0]}
                if formatter.unresolved else {}
            ),
        )
        return None
    if formatter.unresolved:
        logger.warning(
            "template_field_unresolved",
            type=notification_type,
            channel=channel,
            field=field,
            locale=requested,
            variables=list(formatter.unresolved),
        )
    return RenderedField(text=text, unresolved=tuple(formatter.unresolved))


def render(
    notification_type: str,
    channel: str,
    field: str,
    locale: str | None = None,
    variables: dict[str, Any] | None = None,
) -> str | None:
    """render_field's text alone, placeholders unescaped -- for a reader
    that needs no unresolved paths (see render_field)."""
    rendered = render_field(
        notification_type, channel, field, locale=locale, variables=variables,
    )
    return None if rendered is None else rendered.text


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
