# =============================================================================
# COMMS Service -- Product Profile Loader (Phase 2; strict schema since F1.1)
# =============================================================================
#
# Loads the per-deploy PRODUCT PROFILE (notification type records +
# templates) from disk into the ProfileRegistry at startup.
#
# LAYOUT (root = TEMPLATES_DIR, bind-mounted from the product repo's
# comms-profile/ on the VPS; the fixture directory in tests):
#
#   types.yaml  -- {version: 2, types: {type_key: {field: value}}}.
#                  Top-level keys are exactly `version` and `types`,
#                  plus the reserved `x-...` namespace.
#   templates/{locale}.yaml -- {type: {channel: {field: template_str}}},
#                  1:1 with registry.register_templates.
#
# THE SCHEMA IS A CLOSED, TYPED RECORD (F1.1). A profile maps a type to
# how comms reaches out -- it declares, it never computes. That is not
# a rule written here for people to obey; it is the shape of the data.
# Every field has a fixed type (enumerations, lists drawn from
# enumerations, integers, a duration literal -- see _FIELDS), no field
# accepts a string that is later evaluated, and nothing in the schema
# can name the payload of a message. A condition such as "if the
# amount is large, use another channel" therefore cannot be WRITTEN:
# it would need to read the amount, and the schema has nothing to read
# it with. The product expresses such a choice by emitting a different
# type, each with its own record.
#
# STRICT INSIDE ONE VERSION, GROWTH THROUGH A NEW VERSION. The profile
# declares its schema version and exactly one version -- the current
# one -- is accepted. Any other version, or none, refuses startup and
# names both the declared and the required version: the version exists
# so that the refusal reads, not to keep an old format alive. Inside
# the version an unknown key -- at the top level or in a type record --
# refuses startup by name, with the list of allowed names: a typo such
# as `chanels:` must never be indistinguishable from "not declared",
# which is what silently falling back to the default would make it.
# Only keys in the reserved `x-...` namespace are tolerated; they are
# still covered by the domain fence and then dropped -- they never
# reach a type record. Rejected alternatives, so nobody reintroduces
# them as a "softer" mode: a warning instead of the refusal, a strict
# flag, a transition period, or reading two schema versions side by
# side -- each keeps a second format alive, and there is none.
#
# DEFAULTS COME IN LAYERS AND THE LAYER IS KEPT. comms supplies a
# default for every field; the profile overrides per type. For every
# field of every type the registry keeps the value AND the layer that
# decided it (registry.explain). The retry defaults are a REFERENCE to
# settings (NOTIFICATION_MAX_DELIVERY_ATTEMPTS and
# NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS), not a copy: one knob, one
# number.
#
# STORAGE CLASS is not a field yet: it arrives together with the
# storage of answers, and the set of classes is not defined today.
#
# CROSS-CHECK WITH THE DEPLOY (install_profile_from_settings): a type
# routed to a channel whose key set is entirely empty on this deploy
# (not_configured) refuses startup -- a route into a missing
# capability must not reach a running service. The channel-name check
# against the capability registry is the same enumeration check every
# channel list gets. A PARTIAL key set is not checked here at all: the
# settings validator refuses it before this module runs
# (app/core/config.py, via app/core/channels.py evaluate_channels), so
# that state never reaches the loader.
#
# EVERY VIOLATION AT ONCE. A broken profile raises ONE ProfileError
# listing every violation -- type, field, what was found, what was
# expected -- in a deterministic order (file, type, field), so an
# installation is not fixed one typo per restart. One exception, on
# purpose: a missing or wrong schema VERSION is reported alone. The
# rest of such a document was written against another schema, and
# judging it by this one would bury the one real problem in noise.
# Unparseable YAML is reported alone for the same reason; keys
# declared twice in one mapping (YAML would silently keep the last
# one) are collected over every file and reported together.
#
# TEMPLATES ARE A SUBSTITUTION LANGUAGE AND MUST STAY ONE. The template
# language is str.format_map. It can navigate into a value
# ({user[name]}) and format it ({amount:,.2f}) -- that is filling in
# the sender's form with the sender's data. It has no conditions, no
# arithmetic and no function calls, and that is exactly why it is safe
# here. Choosing text by the value of a field is a decision, and
# decisions belong to the product. A template engine with conditions
# (the Jinja class, {% if %}) is REJECTED for that reason: it would
# bring an expression language in through the templates, the back
# door the closed schema above shuts. The dry run below is part of
# this boundary and must not be relaxed.
#
# TEMPLATE CHECKS, at startup:
#   - tree shape (mappings at every level, strings at the leaves);
#   - a template for a type the profile does not declare, and a
#     channel key that is not a channel of this service, refuse
#     startup -- both are typos that would otherwise never render;
#   - the YAML flow-mapping trap: a bare `body: {title}` parses as a
#     dict, not a string -- caught with an explicit hint;
#   - presentation FLAG fields (disable_preview / silent) must be YAML
#     booleans; every other leaf must be a template string and is
#     dry-run;
#   - the dry run pushes every template through format_map with a
#     PROBE value whose __format__ accepts a spec iff at least one JSON
#     scalar type (str, int, float) accepts it. Money/number specs
#     ({amount:,.2f}) pass; garbage ({a:zzz}) and strftime specs
#     ({when:%d.%m}) die at startup -- variables travel through JSONB,
#     so a datetime can never reach render(). Attribute access
#     ({user.name}) is rejected too: JSON objects arrive as dicts, use
#     item access ({user[name]}).
#
# OTHER CHECKS:
#   - type keys and categories are length-checked against the widths
#     of the DB columns they land in (notifications.type,
#     category_mutes.category), shared with the column declarations
#     through app/core/constants.py;
#   - the CHAT-BASELINE contract (arch doc #15): comms emits the
#     built-in msg.* types unconditionally, so every profile must
#     declare all of MSG_TYPE_KEYS with a non-empty category -- a type
#     without a category bypasses the mute gate (§2.5);
#   - the DOMAIN fence over profile DATA (decision 13): an external
#     domain literal anywhere in types.yaml or in a template string
#     fails the startup -- domains live in env, URLs are assembled at
#     the edge. Patterns are shared with scripts/check_domain_literals.py
#     via app/core/constants.py (the image ships app/, not scripts/).
#
# DESIGN: the loader is a SOCKET. The source of raw profile data is
# abstracted behind ProfileSource (files today; a DB or an editor UI
# later plug in without touching parsing/validation or the registry).
# That raises the stakes of the schema: once a profile is edited
# through a UI, review disappears and the schema is the only guard.
#
# A broken profile raises ProfileError -> the service does not start.
# =============================================================================

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Protocol

import structlog
import yaml

from app.core.channels import (
    CHANNEL_SPECS,
    ChannelState,
    channel_env_keys,
    evaluate_channels,
)
from app.core.config import settings
from app.core.constants import (
    DOMAIN_LITERAL_PATTERNS,
    MAX_BODY_LEN,
    MAX_CATEGORY_LEN,
    MAX_TYPE_KEY_LEN,
    MSG_TYPE_KEYS,
)
from app.core.exceptions import ProfileError
from app.profile.registry import (
    Decided,
    Layer,
    ProfileRegistry,
    TemplateTree,
    TypeRecord,
    registry,
)

logger = structlog.get_logger()

# File names inside the profile root.
_TYPES_FILE = "types.yaml"
_TEMPLATES_SUBDIR = "templates"

# The one schema version this service reads. The pre-F1.1 format (a
# flat {type_key: spec} document without a version) is version 1 in
# retrospect -- which is what lets "older than required" name a real
# format instead of a number nobody ever wrote.
SCHEMA_VERSION = 2

# Top-level keys of types.yaml besides the x- namespace.
_VERSION_KEY = "version"
_TYPES_KEY = "types"
_TOP_LEVEL_KEYS = (_TYPES_KEY, _VERSION_KEY)

# Reserved extension namespace: tolerated, fenced, dropped.
_EXTENSION_PREFIX = "x-"

# Presentation FLAG fields of a template sheet (Phase 3a item 1):
# bool leaves, read via registry.get_flag. Everything else on a sheet
# is a template STRING (dry-run validated).
_FLAG_FIELDS = frozenset({"disable_preview", "silent"})

# Type keys are stored in notifications.type; categories in
# category_mutes.category. The limits are the SHARED constants the
# column declarations use (app/core/constants.py).
_TYPE_KEY_MAX_LENGTH = MAX_TYPE_KEY_LEN
_CATEGORY_MAX_LENGTH = MAX_CATEGORY_LEN

# Capability registry: every channel this service implements. The
# names are the values a channel list may hold.
CHANNEL_NAMES: tuple[str, ...] = tuple(
    sorted(spec.name for spec in CHANNEL_SPECS)
)

# Lanes (spec §8.3): a closed set comms offers, the product picks one per
# type. A type that declares none goes to the normal lane.
LANES: tuple[str, ...] = ("interactive", "critical", "normal", "bulk")
_DEFAULT_LANE = "normal"

# What to push to the product (spec §7.4): one of three declarations --
# the outcome only, the outcome and deferrals, or nothing. A closed
# set of three, not a free subset: {deferral} alone is not a
# declaration §7.4 offers, so it must not be writable.
PUSH_ON: tuple[str, ...] = ("outcome", "outcome_and_deferral", "none")
_DEFAULT_PUSH_ON = "none"

# Duration literal: a positive integer and one unit letter. No
# fractions, no compound forms ("1h30m"), no bare numbers -- a bare
# number would leave the unit to be guessed.
_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_DURATION_RE = re.compile(r"^([1-9][0-9]*)([smhd])$")


# -----------------------------------------------------------------------------
# Violations: every problem is collected, then reported once
# -----------------------------------------------------------------------------


@dataclass(frozen=True, order=True)
class Violation:
    """One problem in the profile.

    Field order IS the report order: file, type, field, then the
    message -- deterministic whatever order the source produced the
    data in. `message` names the type, the field, what was found and
    what was expected.
    """

    where: str
    type_key: str
    field: str
    message: str

    def render(self) -> str:
        return f"{self.where}: {self.message}"


def _raise_if_any(violations: list[Violation]) -> None:
    """One refusal naming every violation, sorted."""
    if not violations:
        return
    ordered = sorted(set(violations))
    noun = "problem" if len(ordered) == 1 else "problems"
    raise ProfileError(
        f"The product profile is invalid ({len(ordered)} {noun}); the "
        f"service does not start until every one is fixed:\n"
        + "\n".join(f"  - {v.render()}" for v in ordered)
    )


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


class _DuplicateKeyLoader(yaml.SafeLoader):
    """safe_load that records keys declared twice in one mapping.

    Plain YAML keeps the LAST of two equal keys without a word, so a
    type declared twice, or `category:` written twice, would silently
    lose one of them. Only the mapping's OWN scalar keys are compared:
    keys brought in by a `<<` merge are overrides by design, and a
    non-scalar key is left to the constructor, which rejects it.
    """

    def __init__(self, stream: Any) -> None:
        super().__init__(stream)
        self.duplicates: list[tuple[str, int, int]] = []

    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False,
    ) -> dict[Any, Any]:
        seen: dict[Any, int] = {}
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            if not isinstance(key_node, yaml.ScalarNode):
                continue
            key = self.construct_object(key_node)
            line = key_node.start_mark.line + 1
            if key in seen:
                self.duplicates.append((repr(key), line, seen[key]))
            else:
                seen[key] = line
        return super().construct_mapping(node, deep=deep)


class FileProfileSource:
    """Reads the profile from a directory (types.yaml + templates/)."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def load(self) -> RawProfile:
        """Read and YAML-parse the profile files; no shape validation.

        Keys declared twice are collected over EVERY file and refused
        together; unparseable YAML is refused at once (nothing after
        it in that file can be trusted).
        """
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
        violations: list[Violation] = []
        types_doc = self._read_yaml(types_path, _TYPES_FILE, violations)

        templates_by_locale: dict[str, Any] = {}
        templates_dir = root / _TEMPLATES_SUBDIR
        if templates_dir.is_dir():
            for path in sorted(templates_dir.glob("*.yaml")):
                locale = path.stem
                templates_by_locale[locale] = self._read_yaml(
                    path, f"{_TEMPLATES_SUBDIR}/{path.name}", violations,
                )

        _raise_if_any(violations)
        return RawProfile(
            types=types_doc,
            templates_by_locale=templates_by_locale,
        )

    @staticmethod
    def _read_yaml(
        path: Path, where: str, violations: list[Violation],
    ) -> Any:
        """Safe-load a file, recording keys declared twice."""
        try:
            with path.open(encoding="utf-8") as fh:
                loader = _DuplicateKeyLoader(fh)
                try:
                    doc = loader.get_single_data()
                finally:
                    loader.dispose()
        except yaml.YAMLError as exc:
            raise ProfileError(f"Invalid YAML in {path}: {exc}") from exc
        except OSError as exc:
            raise ProfileError(f"Cannot read {path}: {exc}") from exc
        for key, line, first in loader.duplicates:
            violations.append(Violation(
                where, "", f"line {line:06d}",
                f"key {key} at line {line} is declared a second time "
                f"(first at line {first}); YAML would silently keep "
                f"only the last one -- declare each key once",
            ))
        return doc


# -----------------------------------------------------------------------------
# The type record schema
# -----------------------------------------------------------------------------


def _describe(value: Any) -> str:
    """How a found value is named in a refusal."""
    if value is None:
        return "an empty value (null)"
    return f"{value!r} ({type(value).__name__})"


def _one_of(allowed: tuple[str, ...]) -> Callable[[Any], str | None]:
    """Parser for a single value from a closed set."""

    def check(value: Any) -> str | None:
        if isinstance(value, str) and value in allowed:
            return None
        return f"found {_describe(value)}; expected one of: {', '.join(allowed)}"

    return check


def _check_category(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return f"found {_describe(value)}; expected a non-empty string"
    if len(value) > _CATEGORY_MAX_LENGTH:
        return (
            f"found {value[:30]!r}... ({len(value)} characters); expected "
            f"at most {_CATEGORY_MAX_LENGTH} (width of the "
            f"category_mutes.category column)"
        )
    return None


def _check_channels(value: Any) -> str | None:
    expected = (
        f"expected a non-empty list of distinct channel names from: "
        f"{', '.join(CHANNEL_NAMES)}"
    )
    if not isinstance(value, list):
        return f"found {_describe(value)}; {expected}"
    if not value:
        return f"found an empty list (routes the type nowhere); {expected}"
    problems: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str) or item not in CHANNEL_NAMES:
            problems.append(f"{_describe(item)} is not a channel of this service")
        elif item in seen:
            problems.append(f"{item!r} is listed twice")
        else:
            seen.add(item)
    if problems:
        return f"found {value!r}: {'; '.join(problems)}; {expected}"
    return None


def _check_duration(value: Any) -> str | None:
    if isinstance(value, str) and _DURATION_RE.match(value):
        return None
    return (
        f"found {_describe(value)}; expected a duration literal: a "
        f"positive integer and one unit of {', '.join(_DURATION_UNITS)} "
        f"(e.g. \"15m\", \"24h\", \"7d\")"
    )


def _int_in_range(
    minimum: int, maximum: int | None = None,
) -> Callable[[Any], str | None]:
    """Parser for an integer in a range; bool is not an integer here."""

    def check(value: Any) -> str | None:
        bound = (
            f"an integer >= {minimum}" if maximum is None
            else f"an integer from {minimum} to {maximum}"
        )
        if type(value) is not int:
            return f"found {_describe(value)}; expected {bound}"
        if value < minimum or (maximum is not None and value > maximum):
            return f"found {value}; expected {bound}"
        return None

    return check


def _duration_value(literal: str) -> timedelta:
    match = _DURATION_RE.match(literal)
    assert match is not None  # checked by _check_duration first
    return timedelta(
        seconds=int(match.group(1)) * _DURATION_UNITS[match.group(2)],
    )


@dataclass(frozen=True)
class _Field:
    """One field of the type record: its check, value and default."""

    check: Callable[[Any], str | None]
    # The comms default: (value, source) read at parse time, so a
    # setting-backed default follows the deploy's settings.
    default: Callable[[], tuple[Any, str]]
    convert: Callable[[Any], Any] = lambda value: value


# THE SCHEMA. Every field is here, with a fixed type; nothing else may
# appear in a type record except x- keys. No field holds a string that
# is evaluated later: the only strings are names from closed sets, the
# category (an opaque key, never interpreted), and the duration
# literal (parsed by one fixed regex).
_FIELDS: dict[str, _Field] = {
    # Preference category for mute gating (opaque product key).
    "category": _Field(
        check=_check_category,
        default=lambda: (None, "comms default: no category (not mute-gated)"),
    ),
    # Channels the type is delivered through.
    "channels": _Field(
        check=_check_channels,
        default=lambda: (
            ("in_app",),
            "comms default: in_app (the one channel live on every deploy)",
        ),
        convert=tuple,
    ),
    # Isolation lane (spec §8.3).
    "lane": _Field(
        check=_one_of(LANES),
        default=lambda: (_DEFAULT_LANE, "comms default: lane not declared"),
    ),
    # What is pushed back to the product (spec §7.4).
    "push_on": _Field(
        check=_one_of(PUSH_ON),
        default=lambda: (
            _DEFAULT_PUSH_ON, "comms default: push_on not declared",
        ),
    ),
    # How long a request of this type stays deliverable.
    "expires_after": _Field(
        check=_check_duration,
        default=lambda: (None, "comms default: no expiry declared"),
        convert=_duration_value,
    ),
    # Transport retry ceiling -- a number, never a condition (spec §9.7).
    "retry_max_attempts": _Field(
        check=_int_in_range(1),
        default=lambda: (
            settings.notification_max_delivery_attempts,
            "settings: NOTIFICATION_MAX_DELIVERY_ATTEMPTS",
        ),
    ),
    # Transport retry backoff base, in seconds; 0 = no idle wait.
    "retry_backoff_seconds": _Field(
        check=_int_in_range(0),
        default=lambda: (
            settings.notification_retry_backoff_base_seconds,
            "settings: NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS",
        ),
    ),
    # Size limit, in characters of the body. The ceiling is the width
    # the body already has (MAX_BODY_LEN): a type may tighten it, never
    # widen it past the column.
    "max_body_chars": _Field(
        check=_int_in_range(1, MAX_BODY_LEN),
        default=lambda: (MAX_BODY_LEN, "comms default: MAX_BODY_LEN"),
    ),
}

RECORD_FIELDS: tuple[str, ...] = tuple(sorted(_FIELDS))


def _is_extension(key: Any) -> bool:
    """A key of the reserved x- namespace (lowercase, non-empty tail)."""
    return (
        isinstance(key, str)
        and key.startswith(_EXTENSION_PREFIX)
        and len(key) > len(_EXTENSION_PREFIX)
    )


# -----------------------------------------------------------------------------
# Parsed profile
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class Profile:
    """Validated product profile, ready to install into the registry.

    `types` maps type_key -> TypeRecord (every field, with its layer);
    `templates` maps locale -> TemplateTree.
    """

    types: dict[str, TypeRecord]
    templates: dict[str, TemplateTree]


def parse_profile(
    raw: RawProfile,
    channel_states: Mapping[str, ChannelState] | None = None,
) -> Profile:
    """Validate a raw profile and return the parsed form.

    Source-independent: everything loaded by any ProfileSource goes
    through the same checks. `channel_states` is this deploy's channel
    map; when given, routes are cross-checked against it (the startup
    path passes it, tests that only exercise the schema do not).

    Raises ProfileError: at once for a missing or wrong schema version;
    otherwise once, listing every violation found.
    """
    body = _versioned_body(raw.types)
    violations: list[Violation] = []
    types = _parse_types(body, violations)
    if types is not None:
        _require_msg_type_categories(types, violations)
        if channel_states is not None:
            _check_routes(types, channel_states, violations)

    declared = types or {}
    templates: dict[str, TemplateTree] = {}
    for locale, doc in raw.templates_by_locale.items():
        templates[locale] = _parse_template_tree(
            locale, doc, declared, violations,
        )

    _raise_if_any(violations)
    return Profile(types=declared, templates=templates)


def _versioned_body(doc: Any) -> dict[Any, Any]:
    """Gate on the schema version; return the top-level mapping.

    Reported ALONE on purpose (see the header): a document of another
    schema version is not judged by this one.
    """
    required = f"this comms requires `{_VERSION_KEY}: {SCHEMA_VERSION}`"
    if doc is None:
        raise ProfileError(
            f"{_TYPES_FILE} is empty: it declares no schema version; "
            f"{required}."
        )
    if isinstance(doc, list):
        raise ProfileError(
            f"{_TYPES_FILE} must be a mapping {{{_VERSION_KEY}: ..., "
            f"{_TYPES_KEY}: {{...}}}}, not a list; {required}."
        )
    if not isinstance(doc, dict):
        raise ProfileError(
            f"{_TYPES_FILE} must be a mapping {{{_VERSION_KEY}: ..., "
            f"{_TYPES_KEY}: {{...}}}}, got {type(doc).__name__}; {required}."
        )
    if _VERSION_KEY not in doc:
        raise ProfileError(
            f"{_TYPES_FILE} declares no schema version (declared: none); "
            f"{required}. A document without `{_VERSION_KEY}` is the "
            f"version-1 format (type keys at the top level): move them "
            f"under `{_TYPES_KEY}:` and declare `{_VERSION_KEY}: "
            f"{SCHEMA_VERSION}`."
        )
    declared = doc[_VERSION_KEY]
    if type(declared) is not int:
        raise ProfileError(
            f"{_TYPES_FILE}: schema version must be an integer, found "
            f"{_describe(declared)} (declared: {declared!r}); {required}."
        )
    if declared < SCHEMA_VERSION:
        raise ProfileError(
            f"{_TYPES_FILE} declares schema version {declared}, which is "
            f"older than this comms reads; {required}. Only the current "
            f"version is supported -- rewrite the profile to version "
            f"{SCHEMA_VERSION}."
        )
    if declared > SCHEMA_VERSION:
        raise ProfileError(
            f"{_TYPES_FILE} declares schema version {declared}, which is "
            f"newer than this comms reads; {required}. Deploy the comms "
            f"release that reads version {declared}, or write the profile "
            f"in version {SCHEMA_VERSION}."
        )
    return doc


def _parse_types(
    doc: dict[Any, Any], violations: list[Violation],
) -> dict[str, TypeRecord] | None:
    """Validate the top level and every type record.

    Returns None when there is no usable `types` section (then the
    chat-baseline check would only repeat that fact three times).
    """
    allowed_top = ", ".join(_TOP_LEVEL_KEYS)
    for key, value in doc.items():
        if key in _TOP_LEVEL_KEYS:
            continue
        if _is_extension(key):
            _fence(violations, _TYPES_FILE, "", str(key), value,
                   f"{key}")
            continue
        violations.append(Violation(
            _TYPES_FILE, "", str(key),
            f"unknown top-level key {key!r}; expected only: {allowed_top} "
            f"(or an `{_EXTENSION_PREFIX}...` extension key)",
        ))

    if _TYPES_KEY not in doc:
        violations.append(Violation(
            _TYPES_FILE, "", _TYPES_KEY,
            f"no `{_TYPES_KEY}` section; expected `{_TYPES_KEY}: "
            f"{{type_key: {{...}}}}` declaring every type the product emits",
        ))
        return None
    section = doc[_TYPES_KEY]
    if section is None:
        logger.warning("profile_types_empty")
        section = {}
    if isinstance(section, list):
        violations.append(Violation(
            _TYPES_FILE, "", _TYPES_KEY,
            f"`{_TYPES_KEY}` must be a mapping {{type_key: {{...}}}}, not a "
            f"list. A list cannot carry per-type fields such as `category`.",
        ))
        return None
    if not isinstance(section, dict):
        violations.append(Violation(
            _TYPES_FILE, "", _TYPES_KEY,
            f"`{_TYPES_KEY}` must be a mapping {{type_key: {{...}}}}, "
            f"got {type(section).__name__}",
        ))
        return None
    if not section:
        logger.warning("profile_types_empty")

    types: dict[str, TypeRecord] = {}
    for key, spec in section.items():
        if not isinstance(key, str) or not key:
            violations.append(Violation(
                _TYPES_FILE, str(key), "",
                f"type key must be a non-empty string, got {key!r}",
            ))
            continue
        if len(key) > _TYPE_KEY_MAX_LENGTH:
            violations.append(Violation(
                _TYPES_FILE, key, "",
                f"type key {key[:30]!r}... is {len(key)} characters; the "
                f"limit is {_TYPE_KEY_MAX_LENGTH} (width of the "
                f"notifications.type column).",
            ))
            continue
        record = _parse_record(key, spec, violations)
        if record is not None:
            types[key] = record
    return types


def _parse_record(
    key: str, spec: Any, violations: list[Violation],
) -> TypeRecord | None:
    """One type record: every field declared or defaulted."""
    where = f"type {key!r}"
    if spec is None:
        # A bare `type_key:` line: every field takes its default.
        spec = {}
    if not isinstance(spec, dict):
        violations.append(Violation(
            _TYPES_FILE, key, "",
            f"{where}: the record must be a mapping of fields (or empty "
            f"for all defaults), got {type(spec).__name__}",
        ))
        return None

    declared: dict[str, Any] = {}
    for fname, value in spec.items():
        _fence(violations, _TYPES_FILE, key, str(fname), value,
               f"{where}.{fname}")
        if _is_extension(fname):
            continue
        if not isinstance(fname, str) or fname not in _FIELDS:
            violations.append(Violation(
                _TYPES_FILE, key, str(fname),
                f"{where}: unknown field {fname!r}; expected one of: "
                f"{', '.join(RECORD_FIELDS)} (or an "
                f"`{_EXTENSION_PREFIX}...` extension key)",
            ))
            continue
        complaint = _FIELDS[fname].check(value)
        if complaint is not None:
            violations.append(Violation(
                _TYPES_FILE, key, fname, f"{where}: `{fname}` {complaint}",
            ))
            continue
        declared[fname] = value

    fields: dict[str, Decided] = {}
    for fname, spec_field in _FIELDS.items():
        if fname in declared:
            fields[fname] = Decided(
                value=spec_field.convert(declared[fname]),
                layer=Layer.PROFILE,
                source=f"{_TYPES_FILE}: {where}.{fname}",
            )
        else:
            value, source = spec_field.default()
            fields[fname] = Decided(
                value=value, layer=Layer.DEFAULT, source=source,
            )
    return TypeRecord(fields=fields)


def _require_msg_type_categories(
    types: dict[str, TypeRecord], violations: list[Violation],
) -> None:
    """Enforce the chat-baseline contract (arch doc #15).

    comms emits the MSG_TYPE_KEYS unconditionally (chat is a core
    capability), and a type without a category bypasses the mute gate
    (§2.5) -- chat notifications would be unmutable. So EVERY profile
    must declare all three keys, each with a non-empty category.
    """
    why = (
        "comms emits the built-in chat types unconditionally, so the "
        "profile must declare every one of them with a non-empty "
        "preference category -- otherwise chat notifications bypass the "
        "mute gate and cannot be muted. Declare each as e.g. "
        "`msg.support_message: {category: msg_support}`"
    )
    for key in MSG_TYPE_KEYS:
        if key not in types:
            violations.append(Violation(
                _TYPES_FILE, key, "",
                f"type {key!r} is not declared; {why}",
            ))
        elif types[key].value("category") is None:
            violations.append(Violation(
                _TYPES_FILE, key, "category",
                f"type {key!r} has no category; {why}",
            ))


def _check_routes(
    types: dict[str, TypeRecord],
    channel_states: Mapping[str, ChannelState],
    violations: list[Violation],
) -> None:
    """Refuse a route into a capability this deploy did not configure.

    Names were already checked against the capability registry; here
    only the deploy's state matters. A channel with an empty DECLARED
    key list (in_app) is live by definition and passes. A partial key
    set never reaches here -- the settings validator refused it.
    """
    for key, record in types.items():
        decided = record.fields["channels"]
        for channel in decided.value:
            if channel_states[channel] is ChannelState.NOT_CONFIGURED:
                violations.append(Violation(
                    _TYPES_FILE, key, "channels",
                    f"type {key!r} routes to channel {channel!r} (decided "
                    f"by: {decided.layer.value}), but every key of "
                    f"{channel!r} is empty on this deploy; set the "
                    f"channel's keys, or remove {channel!r} from the "
                    f"type's channels",
                ))


def _fence(
    violations: list[Violation],
    where: str,
    type_key: str,
    field_name: str,
    value: Any,
    path: str,
) -> None:
    """Domain fence over a YAML fragment (decision 13 extended to data).

    External domains must not live in profile YAML: links are assembled
    at the edge from env-held domains, so a domain baked into the
    profile dies with the domain (the t.me incident class). Walks every
    string, including x- extension values. YAML comments never reach
    here: safe_load drops them.
    """
    if isinstance(value, str):
        complaint = _domain_literal(value)
        if complaint is not None:
            violations.append(Violation(
                where, type_key, field_name, f"{path} {complaint}",
            ))
    elif isinstance(value, dict):
        for key, item in value.items():
            _fence(violations, where, type_key, field_name, item,
                   f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _fence(violations, where, type_key, field_name, item,
                   f"{path}[{index}]")


def _domain_literal(value: str) -> str | None:
    """The fence's complaint about one string, or None."""
    for pattern in DOMAIN_LITERAL_PATTERNS:
        match = pattern.search(value)
        if match:
            return (
                f"contains an external domain literal "
                f"({match.group(0)!r}). Domains live in env and URLs "
                f"are assembled at send time (arch doc §2.6, decision "
                f"13) -- put a deep-link intent into action_data "
                f"instead of a URL into the profile."
            )
    return None


def _parse_template_tree(
    locale: str,
    doc: Any,
    types: dict[str, TypeRecord],
    violations: list[Violation],
) -> TemplateTree:
    """Validate one locale's template document.

    Expected: {type: {channel: {field: template_str}}}. Every leaf is
    dry-run through format_map so a broken format spec is caught at
    startup instead of on the delivery path.
    """
    where = f"{_TEMPLATES_SUBDIR}/{locale}.yaml"
    if doc is None:
        logger.warning("profile_templates_empty", locale=locale)
        return {}
    if not isinstance(doc, dict):
        violations.append(Violation(
            where, "", "",
            f"must be a mapping {{type: {{channel: {{field: str}}}}}}, "
            f"got {type(doc).__name__}",
        ))
        return {}

    tree: TemplateTree = {}
    for type_key, channels in doc.items():
        if not isinstance(type_key, str) or not type_key:
            violations.append(Violation(
                where, str(type_key), "",
                f"type key must be a non-empty string, got {type_key!r}",
            ))
            continue
        if type_key not in types:
            violations.append(Violation(
                where, type_key, "",
                f"template for type {type_key!r}, which types.yaml does "
                f"not declare; declare the type, or fix the key -- a "
                f"template under an undeclared key is never rendered",
            ))
        if not isinstance(channels, dict):
            violations.append(Violation(
                where, type_key, "",
                f"{type_key} must map channels to fields, "
                f"got {type(channels).__name__}",
            ))
            continue
        tree_channels: dict[str, dict[str, str | bool]] = {}
        for channel, fields in channels.items():
            if not isinstance(channel, str) or channel not in CHANNEL_NAMES:
                violations.append(Violation(
                    where, type_key, str(channel),
                    f"{type_key}: channel key {channel!r} is not a channel "
                    f"of this service; expected one of: "
                    f"{', '.join(CHANNEL_NAMES)}",
                ))
                continue
            if not isinstance(fields, dict):
                violations.append(Violation(
                    where, type_key, channel,
                    f"{type_key}.{channel} must map fields to template "
                    f"strings, got {type(fields).__name__}",
                ))
                continue
            tree_fields: dict[str, str | bool] = {}
            for fname, template in fields.items():
                path = f"{channel}.{fname}"
                if not isinstance(fname, str) or not fname:
                    violations.append(Violation(
                        where, type_key, path,
                        f"field key under {type_key}.{channel} must be a "
                        f"non-empty string, got {fname!r}",
                    ))
                    continue
                complaint = _leaf_complaint(fname, template)
                if complaint is None and isinstance(template, str):
                    domain = _domain_literal(template)
                    if domain is not None:
                        complaint = f"{domain}"
                if complaint is not None:
                    violations.append(Violation(
                        where, type_key, path,
                        f"{type_key}.{channel}.{fname} {complaint}",
                    ))
                    continue
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


def _leaf_complaint(fname: str, template: Any) -> str | None:
    """Validate a single template leaf; the complaint, or None.

    Flag fields (_FLAG_FIELDS) must be booleans; every other field
    must be a template string that survives a dry run.

    The dict case gets a dedicated message: in YAML a bare
    `body: {title}` is a FLOW MAPPING (a dict {"title": None}), not
    the string "{title}" -- the single most likely authoring mistake.
    """
    if fname in _FLAG_FIELDS:
        # Presentation flags (Phase 3a item 1) are booleans, not
        # templates: no rendering, no dry run. Strict bool -- a quoted
        # "true" is a string and a 1 is an int; both are authoring
        # mistakes that must die at startup, not resolve to a
        # surprising default on the delivery path.
        if not isinstance(template, bool):
            return (
                f"is a presentation flag and must be a YAML boolean "
                f"(true / false), got {type(template).__name__}. Unquoted "
                f'true/false only -- "true" in quotes is a string.'
            )
        return None
    if isinstance(template, dict):
        return (
            f"is a mapping, not a string. In YAML a bare "
            f"`{fname}: {{title}}` parses as a flow mapping; quote the "
            f'value ("{{title}}") or use a block scalar (|-).'
        )
    if not isinstance(template, str):
        return f"must be a string, got {type(template).__name__}"
    try:
        # Dry run against probes: parser errors (unbalanced braces,
        # bad conversions) and specs no JSON scalar accepts surface
        # here -- exactly what must kill startup instead of the
        # delivery path. Specs valid for SOME runtime value pass.
        template.format_map(_ProbeDict())
    except AttributeError as exc:
        return (
            f"uses attribute access on a template variable ({exc}). "
            f"Variables are JSON values, so `{{user.name}}` fails at "
            f"runtime -- use item access (`{{user[name]}}`)."
        )
    except (ValueError, TypeError, IndexError, KeyError) as exc:
        return (
            f"has a broken format spec or field path: {exc}. Template "
            f"language is str.format_map -- check braces and format specs."
        )
    return None


# -----------------------------------------------------------------------------
# Installation
# -----------------------------------------------------------------------------


def load_profile(
    source: ProfileSource,
    channel_states: Mapping[str, ChannelState] | None = None,
) -> Profile:
    """Load and validate a profile from a source."""
    return parse_profile(source.load(), channel_states)


def deploy_channel_states() -> dict[str, ChannelState]:
    """This deploy's channel states, from the validated settings.

    The same key values the startup channel map reads
    (app/engine/formatters.py channel_map, which this package cannot
    import: profile sits below engine). The problems list is not
    consulted: settings with a broken key set never got constructed.
    """
    states, _ = evaluate_channels({
        key: getattr(settings, key.lower()) for key in channel_env_keys()
    })
    return states


def install_profile(
    profile: Profile,
    target: ProfileRegistry = registry,
) -> None:
    """Install a validated profile into a registry.

    Types are registered with their categories and full records;
    template trees are merged per locale via the registry's normal
    merge semantics.
    """
    for type_key, record in profile.types.items():
        target.register_type(
            type_key, category=record.value("category"), record=record,
        )
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
    missing profile is a deploy error -> ProfileError. Routes are
    cross-checked against this deploy's channel states.
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
    profile = load_profile(
        FileProfileSource(Path(settings.templates_dir)),
        channel_states=deploy_channel_states(),
    )
    install_profile(profile)
    return True
