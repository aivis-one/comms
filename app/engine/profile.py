# =============================================================================
# COMMS Service -- Product Profile Loader (Phase 2)
# =============================================================================
#
# Loads the per-deploy PRODUCT PROFILE (notification type dictionary +
# templates) from disk into the ProfileRegistry at startup.
#
# LAYOUT (root = TEMPLATES_DIR, bind-mounted from the product repo's
# comms-profile/ on the VPS; the fixture directory in tests):
#
#   types.yaml            -- {type_key: {category: str, ...}} mapping.
#                            A type spec may be empty ({} or bare key);
#                            unknown spec keys are tolerated (additive
#                            profile contract, arch doc §4.3).
#   templates/{locale}.yaml -- {type: {channel: {field: template_str}}},
#                            1:1 with registry.register_templates.
#
# DESIGN: the loader is a SOCKET. The source of raw profile data is
# abstracted behind ProfileSource (files today; a DB or an editor UI
# later plug in without touching parsing/validation or the registry).
#
# VALIDATION happens here, at startup, source-independently:
#   - tree shape (mappings at every level, strings at the leaves);
#   - the YAML flow-mapping trap: a bare `body: {title}` parses as a
#     dict, not a string -- caught with an explicit hint;
#   - presentation FLAG fields (Phase 3a item 1: disable_preview /
#     silent) must be YAML booleans; every other leaf (title / body /
#     subject / button_text / unknown-but-tolerated fields) must be a
#     template string and is dry-run like any template;
#   - a dry run of every template through format_map with a PROBE
#     value whose __format__ accepts a spec iff at least one JSON
#     scalar type (str, int, float) accepts it. Money/number specs
#     ({amount:,.2f}) pass; garbage ({a:zzz}) and strftime specs
#     ({when:%d.%m}) die at startup -- variables travel through JSONB,
#     so a datetime can never reach render() and date-like specs are
#     true positives. Attribute access ({user.name}) is rejected too:
#     JSON objects arrive as dicts, use item access ({user[name]}).
#   - type keys and categories are length-checked against the widths
#     of the DB columns they land in (notifications.type,
#     category_mutes.category) -- a too-long key would otherwise
#     DataError at create/mute time instead of at startup. Widths come
#     from app/core/constants.py, SHARED with the column declarations
#     (Phase 3a item 6: the Phase 2 introspection made engine/profile
#     import audience/models while audience/prefs already imports
#     engine/registry -- a package cycle; a consistency test now keeps
#     the constants honest instead);
#   - templates for types missing from the dictionary -> loud warning
#     (not fatal: additive contract, profile may ship templates ahead
#     of enabling a type).
#
# A broken profile raises ProfileError -> the service does not start.
# The runtime fallback chain in template_engine stays as the second
# line of defense.
# =============================================================================

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import structlog
import yaml

from app.core.config import settings
from app.core.constants import MAX_CATEGORY_LEN, MAX_TYPE_KEY_LEN
from app.core.exceptions import ProfileError
from app.engine.registry import ProfileRegistry, TemplateTree, registry

logger = structlog.get_logger()

# File names inside the profile root.
_TYPES_FILE = "types.yaml"
_TEMPLATES_SUBDIR = "templates"

# Presentation FLAG fields of a template sheet (Phase 3a item 1):
# bool leaves, read via registry.get_flag. Everything else on a sheet
# is a template STRING (dry-run validated). Field-name based on
# purpose -- the rule holds for any channel, so a future channel
# reusing "silent" inherits the bool discipline for free.
_FLAG_FIELDS = frozenset({"disable_preview", "silent"})


# Type keys are stored in notifications.type; categories in
# category_mutes.category. A key longer than the column would
# DataError at create_notification / mute time -- fail at startup
# instead. The limits are the SHARED constants the column
# declarations use (app/core/constants.py); a unit test pins them to
# the actual mapped column widths.
_TYPE_KEY_MAX_LENGTH = MAX_TYPE_KEY_LEN
_CATEGORY_MAX_LENGTH = MAX_CATEGORY_LEN


# -----------------------------------------------------------------------------
# Raw profile + sources (the socket)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class RawProfile:
    """Unvalidated profile data as it came from a source.

    `types` is whatever the types document parsed to; `templates_by_locale`
    maps locale -> whatever that locale's template document parsed to.
    Validation of the actual shapes happens in parse_profile().
    """

    types: Any
    templates_by_locale: dict[str, Any] = field(default_factory=dict)


class ProfileSource(Protocol):
    """A source of raw profile data (files, DB, editor -- pluggable)."""

    def load(self) -> RawProfile:
        """Return the raw profile; raise ProfileError if unreadable."""
        ...


class FileProfileSource:
    """Reads the profile from a directory (types.yaml + templates/)."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def load(self) -> RawProfile:
        """Read and YAML-parse the profile files; no shape validation."""
        root = self._root
        if not root.is_dir():
            raise ProfileError(
                f"Profile directory does not exist: {root}. "
                f"Check TEMPLATES_DIR."
            )

        types_path = root / _TYPES_FILE
        if not types_path.is_file():
            raise ProfileError(
                f"Profile is missing {_TYPES_FILE}: expected at {types_path}"
            )
        types_doc = self._read_yaml(types_path)

        templates_by_locale: dict[str, Any] = {}
        templates_dir = root / _TEMPLATES_SUBDIR
        if templates_dir.is_dir():
            for path in sorted(templates_dir.glob("*.yaml")):
                locale = path.stem
                templates_by_locale[locale] = self._read_yaml(path)

        return RawProfile(
            types=types_doc,
            templates_by_locale=templates_by_locale,
        )

    @staticmethod
    def _read_yaml(path: Path) -> Any:
        """yaml.safe_load a file, wrapping errors into ProfileError."""
        try:
            with path.open(encoding="utf-8") as fh:
                return yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise ProfileError(f"Invalid YAML in {path}: {exc}") from exc
        except OSError as exc:
            raise ProfileError(f"Cannot read {path}: {exc}") from exc


# -----------------------------------------------------------------------------
# Parsed profile
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class Profile:
    """Validated product profile, ready to install into the registry.

    `types` maps type_key -> spec dict (category etc.); `templates`
    maps locale -> TemplateTree.
    """

    types: dict[str, dict[str, Any]]
    templates: dict[str, TemplateTree]


def parse_profile(raw: RawProfile) -> Profile:
    """Validate a raw profile and return the parsed form.

    Source-independent: everything loaded by any ProfileSource goes
    through the same checks. Raises ProfileError on the first fatal
    problem; templates for unregistered types only log a warning.
    """
    types = _parse_types(raw.types)

    templates: dict[str, TemplateTree] = {}
    for locale, doc in raw.templates_by_locale.items():
        templates[locale] = _parse_template_tree(locale, doc, types)

    return Profile(types=types, templates=templates)


def _parse_types(doc: Any) -> dict[str, dict[str, Any]]:
    """Validate the type dictionary document.

    Expected: {type_key: {category: str, ...} | None}. A None spec
    (bare `type_key:` line) means an empty spec. Unknown spec keys are
    tolerated silently (additive contract). The document must be a
    MAPPING, not a list -- a list would freeze the format against
    adding per-type fields like `category`.
    """
    if doc is None:
        logger.warning("profile_types_empty")
        return {}
    if isinstance(doc, list):
        raise ProfileError(
            "types.yaml must be a mapping {type_key: {...}}, not a list. "
            "A list cannot carry per-type fields such as `category`."
        )
    if not isinstance(doc, dict):
        raise ProfileError(
            f"types.yaml must be a mapping {{type_key: {{...}}}}, "
            f"got {type(doc).__name__}"
        )

    types: dict[str, dict[str, Any]] = {}
    for key, spec in doc.items():
        if not isinstance(key, str) or not key:
            raise ProfileError(
                f"types.yaml: type key must be a non-empty string, "
                f"got {key!r}"
            )
        if len(key) > _TYPE_KEY_MAX_LENGTH:
            raise ProfileError(
                f"types.yaml: type key {key[:30]!r}... is "
                f"{len(key)} characters; the limit is "
                f"{_TYPE_KEY_MAX_LENGTH} (width of the "
                f"notifications.type column)."
            )
        if spec is None:
            spec = {}
        if not isinstance(spec, dict):
            raise ProfileError(
                f"types.yaml: spec for type {key!r} must be a mapping "
                f"(or empty), got {type(spec).__name__}"
            )
        category = spec.get("category")
        if category is not None and (
            not isinstance(category, str) or not category
        ):
            raise ProfileError(
                f"types.yaml: `category` for type {key!r} must be a "
                f"non-empty string, got {category!r}"
            )
        if category is not None and len(category) > _CATEGORY_MAX_LENGTH:
            raise ProfileError(
                f"types.yaml: category {category[:30]!r}... for type "
                f"{key!r} is {len(category)} characters; the limit is "
                f"{_CATEGORY_MAX_LENGTH} (width of the "
                f"category_mutes.category column)."
            )
        types[key] = spec
    return types


def _parse_template_tree(
    locale: str,
    doc: Any,
    types: dict[str, dict[str, Any]],
) -> TemplateTree:
    """Validate one locale's template document.

    Expected: {type: {channel: {field: template_str}}}. Every leaf is
    dry-run through format_map(SafeDict()) so a broken format spec is
    caught at startup instead of on the delivery path.
    """
    if doc is None:
        logger.warning("profile_templates_empty", locale=locale)
        return {}
    if not isinstance(doc, dict):
        raise ProfileError(
            f"templates/{locale}.yaml must be a mapping "
            f"{{type: {{channel: {{field: str}}}}}}, "
            f"got {type(doc).__name__}"
        )

    tree: TemplateTree = {}
    for type_key, channels in doc.items():
        if not isinstance(type_key, str) or not type_key:
            raise ProfileError(
                f"templates/{locale}.yaml: type key must be a non-empty "
                f"string, got {type_key!r}"
            )
        if type_key not in types:
            # Not fatal: the profile contract is additive -- templates
            # may ship ahead of the type being enabled in the
            # dictionary. But it is loud: most likely a typo.
            logger.warning(
                "template_for_unregistered_type",
                locale=locale,
                type=type_key,
            )
        if not isinstance(channels, dict):
            raise ProfileError(
                f"templates/{locale}.yaml: {type_key} must map channels "
                f"to fields, got {type(channels).__name__}"
            )
        tree_channels: dict[str, dict[str, str | bool]] = {}
        for channel, fields in channels.items():
            if not isinstance(channel, str) or not channel:
                raise ProfileError(
                    f"templates/{locale}.yaml: channel key under "
                    f"{type_key} must be a non-empty string, "
                    f"got {channel!r}"
                )
            if not isinstance(fields, dict):
                raise ProfileError(
                    f"templates/{locale}.yaml: {type_key}.{channel} must "
                    f"map fields to template strings, "
                    f"got {type(fields).__name__}"
                )
            tree_fields: dict[str, str | bool] = {}
            for fname, template in fields.items():
                if not isinstance(fname, str) or not fname:
                    raise ProfileError(
                        f"templates/{locale}.yaml: field key under "
                        f"{type_key}.{channel} must be a non-empty "
                        f"string, got {fname!r}"
                    )
                _validate_leaf(locale, type_key, channel, fname, template)
                tree_fields[fname] = template
            tree_channels[channel] = tree_fields
        tree[type_key] = tree_channels
    return tree


class _FormatProbe:
    """Dry-run stand-in for one template variable.

    __format__ accepts a spec iff at least one JSON scalar runtime
    type (str, int, float) accepts it -- i.e. there EXISTS a value the
    template would render with. datetime is deliberately NOT probed:
    (a) datetime.__format__ delegates to strftime, which swallows any
    garbage literally -- the probe would accept everything and stop
    detecting anything; (b) template variables travel through the
    action_data JSONB column, so a datetime can never reach render()
    -- date-like specs are guaranteed runtime failures, rejecting them
    here is a true positive. Contract: the product sends dates as
    pre-formatted strings (same principle as pre-rendered digests).

    Item access returns another probe ({user[name]} works on the JSON
    dicts that actually arrive). Attribute access is deliberately NOT
    provided: getattr on a dict fails at runtime, so {user.name} must
    die at validation, not on the delivery path.
    """

    def __format__(self, spec: str) -> str:
        if not spec:
            return ""
        for probe_value in ("", 0, 0.0):
            try:
                format(probe_value, spec)
            except (ValueError, TypeError):
                continue
            return ""
        raise ValueError(
            f"format spec {spec!r} is not valid for any JSON scalar "
            f"type (str, int, float)"
        )

    def __getitem__(self, key: object) -> "_FormatProbe":
        return self

    def __str__(self) -> str:
        # For the !s conversion: the result is a str, so the spec is
        # then checked against str semantics -- same as at runtime.
        return ""


class _ProbeDict(dict[str, Any]):
    """format_map mapping for the dry run: every name is a probe."""

    def __missing__(self, key: str) -> _FormatProbe:
        return _FormatProbe()


def _validate_leaf(
    locale: str,
    type_key: str,
    channel: str,
    fname: str,
    template: Any,
) -> None:
    """Validate a single template leaf.

    Flag fields (_FLAG_FIELDS) must be booleans; every other field
    must be a template string that survives a dry run.

    The dict case gets a dedicated message: in YAML a bare
    `body: {title}` is a FLOW MAPPING (a dict {"title": None}), not
    the string "{title}" -- the single most likely authoring mistake.
    """
    where = f"templates/{locale}.yaml: {type_key}.{channel}.{fname}"
    if fname in _FLAG_FIELDS:
        # Presentation flags (Phase 3a item 1) are booleans, not
        # templates: no rendering, no dry run. Strict bool -- a quoted
        # "true" is a string and a 1 is an int; both are authoring
        # mistakes that must die at startup, not resolve to a
        # surprising default on the delivery path.
        if not isinstance(template, bool):
            raise ProfileError(
                f"{where} is a presentation flag and must be a YAML "
                f"boolean (true / false), got "
                f"{type(template).__name__}. Unquoted true/false only "
                f'-- "true" in quotes is a string.'
            )
        return
    if isinstance(template, dict):
        raise ProfileError(
            f"{where} is a mapping, not a string. In YAML a bare "
            f"`{fname}: {{title}}` parses as a flow mapping; quote the "
            f'value ("{{title}}") or use a block scalar (|-).'
        )
    if not isinstance(template, str):
        raise ProfileError(
            f"{where} must be a string, got {type(template).__name__}"
        )
    try:
        # Dry run against probes: parser errors (unbalanced braces,
        # bad conversions) and specs no JSON scalar accepts surface
        # here -- exactly what must kill startup instead of the
        # delivery path. Specs valid for SOME runtime value pass.
        template.format_map(_ProbeDict())
    except AttributeError as exc:
        raise ProfileError(
            f"{where} uses attribute access on a template variable "
            f"({exc}). Variables are JSON values, so `{{user.name}}` "
            f"fails at runtime -- use item access (`{{user[name]}}`)."
        ) from exc
    except (ValueError, TypeError, IndexError, KeyError) as exc:
        raise ProfileError(
            f"{where} has a broken format spec or field path: {exc}. "
            f"Template language is str.format_map -- check braces "
            f"and format specs."
        ) from exc


# -----------------------------------------------------------------------------
# Installation
# -----------------------------------------------------------------------------


def load_profile(source: ProfileSource) -> Profile:
    """Load and validate a profile from a source."""
    return parse_profile(source.load())


def install_profile(
    profile: Profile,
    target: ProfileRegistry = registry,
) -> None:
    """Install a validated profile into a registry.

    Types are registered with their categories; template trees are
    merged per locale via the registry's normal merge semantics.
    """
    for type_key, spec in profile.types.items():
        target.register_type(type_key, category=spec.get("category"))
    for locale, tree in profile.templates.items():
        target.register_templates(locale, tree)
    logger.info(
        "profile_installed",
        types=len(profile.types),
        locales=sorted(profile.templates.keys()),
    )


def install_profile_from_settings() -> bool:
    """Startup entry point: load the profile from TEMPLATES_DIR.

    Returns True when a profile was installed. An empty TEMPLATES_DIR
    is tolerated ONLY in development (mirrors the DATABASE_URL
    policy): the service starts with an empty registry and tests /
    local experiments register what they need. Anywhere else a
    missing profile is a deploy error -> ProfileError.
    """
    if not settings.templates_dir:
        if settings.is_dev:
            logger.warning(
                "profile_not_configured",
                hint="TEMPLATES_DIR is empty; registry stays empty",
            )
            return False
        raise ProfileError(
            "TEMPLATES_DIR is required outside development. "
            "Point it at the product profile directory."
        )
    profile = load_profile(FileProfileSource(Path(settings.templates_dir)))
    install_profile(profile)
    return True
