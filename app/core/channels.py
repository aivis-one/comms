# =============================================================================
# COMMS Service -- Channel Specification (what a deploy may declare)
# =============================================================================
#
# THE RULE -- one for every channel, no per-channel exceptions:
#
#   A channel EXISTS on a deploy when it has an IMPLEMENTATION and every
#   one of its DECLARED KEYS is set. Its key set decides, and nothing
#   else does:
#
#     all keys empty   -> the channel is absent by design; the service
#                         starts, and a request for the channel fails
#                         permanently and loudly (never a quiet success)
#     all keys set     -> the channel is live
#     some keys set    -> an integrator typo; startup is REFUSED, and the
#                         message names the channel and every missing key
#     a malformed value-> same: startup is refused, the key is named
#
#   A channel with an EMPTY declared key list is live by definition --
#   the empty set is trivially complete. That is how in_app works: its
#   delivery IS the row the inbox reads, nothing leaves the process, so
#   there is nothing to configure. It is not a special case of the rule;
#   it is the rule applied to zero keys.
#
#   A channel with NO entry here has no implementation and is therefore
#   absent on every deploy, whatever the environment says.
#
# WHAT THIS REGISTRY IS NOT: it describes SENDING channels only. A
# future inbound side (receiving bot updates) is a separate capability
# with its own keys and its own switch, and is NOT a channel of this
# registry -- one bot has exactly one update receiver, and a receiver
# switched on by the mere presence of the sending token would start at
# a deploy that only asked to send.
#
# WHY A SEPARATE MODULE: it imports no settings. The test suite reads
# the declared keys BEFORE the settings object exists, to blank every
# external channel's keys (tests/conftest.py) -- so a channel added here
# is kept out of the network in tests without anyone remembering to.
#
# Channel names are plain strings on purpose: app.core sits below
# app.engine (package DAG), so it cannot import DeliveryChannel. A test
# pins that every name here is a DeliveryChannel value and that every
# channel here has a builder in app/engine/formatters.py.
# =============================================================================

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from urllib.parse import urlsplit

from aiogram.utils.token import TokenValidationError, validate_token

# A validator returns None for a well-formed value, or a short
# human-readable description of what the value should look like. It
# must NEVER echo the value: keys carry secrets.
Validator = Callable[[str], str | None]


@dataclass(frozen=True)
class ChannelSpec:
    """An implemented channel and the keys that make it live."""

    name: str
    # The keys that decide the channel: all empty = absent, all set =
    # live, some set = refusal.
    keys: tuple[str, ...]
    validators: Mapping[str, Validator] = field(default_factory=dict)
    # Keys that carry a DEFAULT and therefore do not decide anything --
    # "unset" and "set to the default" cannot be told apart. Their form
    # is still checked whenever the deploy spells them out, so a typo in
    # a defaulted value refuses startup like any other.
    optional_validators: Mapping[str, Validator] = field(
        default_factory=dict,
    )


class ChannelState(StrEnum):
    """What a deploy has for one channel (the startup channel map)."""

    LIVE = "live"
    # Implemented, but this deploy declared none of its keys.
    NOT_CONFIGURED = "not_configured"
    # No implementation exists in this service.
    NOT_IMPLEMENTED = "not_implemented"


def _telegram_token(value: str) -> str | None:
    """aiogram's own token-shape check -- the one Bot() would apply."""
    try:
        validate_token(value)
    except TokenValidationError:
        return "expected the bot token shape '<digits>:<secret>'"
    return None


def _telegram_url(value: str) -> str | None:
    """An https URL with a host and exactly one path segment (the bot).

    The host is deliberately NOT pinned: the canonical short-link host
    is not reachable everywhere, and deploys use its official aliases.
    """
    parts = urlsplit(value)
    segments = [s for s in parts.path.split("/") if s]
    if (
        parts.scheme != "https"
        or not parts.netloc
        or len(segments) != 1
        or parts.query
        or parts.fragment
    ):
        return (
            "expected an https URL with a host and exactly one path "
            f"segment, the bot username (got {value!r})"
        )
    return None


# -- email ------------------------------------------------------------------
#
# ONE PREFIX PER CHANNEL (EMAIL_), including the provider-named keys:
# the agreed fix of the KNOWN CEILING below scans the environment for a
# channel's prefix, and a channel spread over two prefixes would make it
# unperformable. The provider IS in the key names on purpose -- "domain"
# and "region" are its concepts, and a neutral name over a single
# provider would mislead the next integrator.

# The provider serves two regions from two different base addresses. A
# CLOSED SET, so an unknown region refuses startup instead of failing
# every send: "eu-west" or "europe" are the shapes an integrator
# actually writes, and they must not reach the delivery path. The
# addresses themselves live in app/core/config.py -- the one file the
# domain-literal fence allows (arch doc 2.6 / decision 13).
EMAIL_REGIONS: tuple[str, ...] = ("eu", "us")

_CRLF = ("\r", "\n")


def _email_region(value: str) -> str | None:
    """One of the known regions, spelled exactly."""
    if value not in EMAIL_REGIONS:
        return (
            f"expected one of {', '.join(EMAIL_REGIONS)} "
            f"(got {value!r})"
        )
    return None


def _address_shape(value: str) -> str | None:
    """A bare addr-spec: one @, non-empty both sides, no spaces."""
    local, sep, domain = value.partition("@")
    if not sep or not local or not domain:
        return "an address needs one @ with text on both sides"
    if "@" in domain:
        return "an address must not carry more than one @"
    if any(ch.isspace() for ch in value):
        return "an address must not contain spaces"
    if "." not in domain:
        return "the domain part needs a dot"
    return None


def _email_sender(value: str) -> str | None:
    """Either a bare address or a display-name form.

    CR/LF is checked over the WHOLE value BEFORE any parsing: a header
    injection must not depend on how the angle brackets happened to
    split. The display-name form is allowed on purpose -- a signed
    sender is materially better for deliverability, which is the whole
    reason a provider is used at all.
    """
    if any(ch in value for ch in _CRLF):
        return (
            "expected an address, or the form 'Name <address>'; the "
            "value carries a line break"
        )
    if value.endswith(">"):
        name, _, rest = value.partition("<")
        if not name.strip() or ">" in rest[:-1]:
            return (
                "expected an address, or the form 'Name <address>' with "
                "a non-empty name and one pair of angle brackets"
            )
        complaint = _address_shape(rest[:-1])
        return None if complaint is None else f"inside <>: {complaint}"
    if "<" in value or ">" in value:
        return (
            "expected an address, or the form 'Name <address>' -- angle "
            "brackets must wrap the address and close at the end"
        )
    complaint = _address_shape(value)
    if complaint is None:
        return None
    # The expected form belongs in EVERY complaint, not only in the ones
    # about brackets: this is a startup refusal on a value the provider
    # itself might have accepted, and "wrong shape" without the shape
    # leaves the integrator guessing.
    return f"expected an address, or the form 'Name <address>' -- {complaint}"


CHANNEL_SPECS: tuple[ChannelSpec, ...] = (
    ChannelSpec(name="in_app", keys=()),
    ChannelSpec(
        name="telegram",
        keys=("TELEGRAM_BOT_TOKEN", "TELEGRAM_BOT_URL"),
        validators={
            "TELEGRAM_BOT_TOKEN": _telegram_token,
            "TELEGRAM_BOT_URL": _telegram_url,
        },
    ),
    ChannelSpec(
        name="email",
        keys=(
            "EMAIL_MAILGUN_API_KEY",
            "EMAIL_MAILGUN_DOMAIN",
            "EMAIL_FROM_ADDRESS",
        ),
        validators={
            "EMAIL_FROM_ADDRESS": _email_sender,
        },
        # NOT a declared key: it has a default, so "unset" and "set to
        # the default" are indistinguishable, and counting it would make
        # an EMPTY set look PARTIAL -- refusing startup on every deploy
        # that has no email at all. Its FORM is still checked, but only
        # when the deploy spells it out.
        optional_validators={
            "EMAIL_MAILGUN_REGION": _email_region,
        },
    ),
)


def channel_env_keys() -> tuple[str, ...]:
    """Every DECIDING key of every channel, in declaration order.

    Defaulted keys are not here: blanking them (tests/conftest.py) would
    replace a working default with an invalid empty value, and they
    cannot make a channel live or absent anyway.
    """
    return tuple(key for spec in CHANNEL_SPECS for key in spec.keys)


def channel_optional_keys() -> tuple[str, ...]:
    """Every DEFAULTED key of every channel, in declaration order."""
    return tuple(
        key for spec in CHANNEL_SPECS for key in spec.optional_validators
    )


def _malformed_optionals(
    spec: ChannelSpec, values: Mapping[str, str],
) -> list[str]:
    """Complaints about spelled-out defaulted values, if any."""
    complaints: list[str] = []
    for key, validator in spec.optional_validators.items():
        value = values.get(key, "")
        if value == "":
            continue
        if value.strip() == "":
            complaints.append(f"{key} consists only of whitespace")
            continue
        complaint = validator(value)
        if complaint is not None:
            complaints.append(f"{key} is malformed: {complaint}")
    return complaints


def _problem(
    spec: ChannelSpec,
    missing: list[str],
    present: list[str],
    malformed: list[str],
) -> str:
    """One channel's refusal text, naming keys and the expected shape."""
    lines = [f"channel '{spec.name}' is misconfigured:"]
    if missing:
        lines.append(
            f"  missing: {', '.join(missing)} "
            f"(set: {', '.join(present) or 'none'})"
        )
    lines.extend(f"  {m}" for m in malformed)
    lines.append(
        f"  Set every key of '{spec.name}', or leave all of "
        f"{', '.join(spec.keys)} empty if this deploy has no "
        f"{spec.name}."
    )
    return "\n".join(lines)


def evaluate_channels(
    values: Mapping[str, str],
) -> tuple[dict[str, ChannelState], list[str]]:
    """Apply the rule to one deploy's key values.

    Args:
        values: Declared key name -> configured value ("" = not set).
            A key missing from the mapping counts as not set.

    Returns:
        (states, problems): the state of every IMPLEMENTED channel, and
        one human-readable problem per broken channel -- ALL of them,
        so an integrator fixes every typo in one pass. A channel with a
        problem has no state entry: it must never be reported as live
        or absent, because the deploy must not start.
    """
    states: dict[str, ChannelState] = {}
    problems: list[str] = []
    for spec in CHANNEL_SPECS:
        present = [k for k in spec.keys if values.get(k, "") != ""]
        if spec.keys and not present:
            # A defaulted value of the wrong shape on an ABSENT channel
            # is still a refusal: it is a typo in this deploy's env, and
            # silently ignoring it would teach the integrator that the
            # key does not exist.
            stray = _malformed_optionals(spec, values)
            if stray:
                problems.append(_problem(spec, [], present, stray))
                continue
            # KNOWN CEILING -- a typo in EVERY key name of a channel
            # reads as "no such channel" (consequence of extra="ignore"
            # on Settings, app/core/config.py -- a decision, described
            # there in prose).
            #   1. Mechanics: unknown env keys are ignored, so
            #      TELEGRAM_BOT_TOKN plus TELEGRAM_BOT_UR leave both
            #      declared keys empty -- by construction
            #      indistinguishable from a deploy that has no telegram.
            #      The service starts, and every request for the
            #      channel fails permanently. A typo in ONE key name is
            #      caught: the set is partial and startup is refused.
            #   2. Status: acknowledged by design.
            #   3. Backlog ref: none -- the case became visible in the
            #      same change that introduced its detector (item 4),
            #      and no automatic detection has been requested.
            #   4. Promotion trigger (observable): the channel map
            #      (`channels` field of the comms_started and
            #      channel_formatters_built logs, and of the GET
            #      /health body -- app/main.py) shows a channel as
            #      not_configured although its keys were written into
            #      the env. That map IS the detector: the ceiling is
            #      "caught by eye from the first start", not "not
            #      caught" -- before the map existed, this case was not
            #      detectable at all.
            #   5. Agreed fix: when a channel is not_configured, scan
            #      the process environment for keys that share the
            #      channel's prefix but are not declared (TELEGRAM_*)
            #      and name them in the startup log -- an extension of
            #      the existing channel map, not a new mechanism.
            #   6. Rejected: extra="forbid" -- installers keep writing
            #      keys this service no longer reads, and a leftover
            #      key would refuse startup of a working deploy.
            states[spec.name] = ChannelState.NOT_CONFIGURED
            continue

        missing = [k for k in spec.keys if k not in present]
        malformed: list[str] = []
        # Defaulted keys never make a set partial, but a spelled-out
        # value of the wrong shape is still a refusal.
        checked = present + [
            k for k in spec.optional_validators if values.get(k, "") != ""
        ]
        for key in checked:
            value = values[key]
            if value.strip() == "":
                malformed.append(f"{key} consists only of whitespace")
                continue
            validator = spec.validators.get(key) or (
                spec.optional_validators.get(key)
            )
            complaint = validator(value) if validator else None
            if complaint is not None:
                malformed.append(f"{key} is malformed: {complaint}")

        if not missing and not malformed:
            states[spec.name] = ChannelState.LIVE
            continue

        problems.append(_problem(spec, missing, present, malformed))
    return states, problems
