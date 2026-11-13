# =============================================================================
# COMMS Service -- Channel Specification (what a deploy may declare)
# =============================================================================
#
# THE RULE -- one for every channel, no per-channel exceptions:
#
#   A channel EXISTS on a deploy when it has an IMPLEMENTATION and its
#   FLAG switches it on. The flag decides, and nothing else does: under
#   the Primer's machine a channel is an option of the instance, the
#   instance's passport switches it, and comms learns of it only by the
#   flag (Primer protocol 5.2.3, contract 3 item 38).
#
#     flag `false`     -> the channel is switched off on this deploy; its
#                         keys -- the optional ones too -- are NOT read
#                         and NOT checked, whatever they hold; the
#                         service starts, and a delivery into the
#                         channel ends in the outcome "channel
#                         unavailable" (never a quiet success)
#     flag `true`      -> every declared key must be set and well
#                         formed: then the channel is live; a missing or
#                         malformed key REFUSES startup, and the message
#                         names the channel and every such key
#     any other value, or no flag at all
#                      -> startup is REFUSED, naming the flag: exactly
#                         `true` or `false`, nothing else (a red start,
#                         never a guess)
#
#   A channel with NO FLAG and an EMPTY declared key list is live by
#   definition: there is nothing to switch and nothing to configure.
#   That is in_app: its delivery IS the row the inbox reads, nothing
#   leaves the process.
#
#   A channel with NO entry here has no implementation and is therefore
#   absent on every deploy, whatever the environment says.
#
#   The value a machine writes into the keys of a switched-off option is
#   not known here and never compared: the keys of a switched-off
#   channel are simply not read.
#
# WHAT THIS REGISTRY IS NOT: it describes SENDING channels only. A
# future inbound side (receiving bot updates) is a separate capability
# with its own keys and its own switch, and is NOT a channel of this
# registry -- one bot has exactly one update receiver, and a receiver
# switched on by the mere presence of the sending token would start at
# a deploy that only asked to send.
#
# WHY A SEPARATE MODULE: it imports no settings. The test suite reads
# the flags and the declared keys BEFORE the settings object exists, to
# switch every external channel off and blank its keys
# (tests/conftest.py) -- so a channel added here is kept out of the
# network in tests without anyone remembering to.
#
# Channel names are plain strings on purpose: app.core sits below
# app.engine (package DAG), so it cannot import DeliveryChannel. A test
# pins that every name here is a DeliveryChannel value and that every
# channel here has a builder in app/engine/formatters.py.
# =============================================================================

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from urllib.parse import urlsplit

from aiogram.utils.token import TokenValidationError, validate_token

# A validator returns None for a well-formed value, or a short
# human-readable description of what the value should look like. It
# must NEVER echo the value: keys carry secrets.
Validator = Callable[[str], str | None]


@dataclass(frozen=True)
class ChannelSpec:
    """An implemented channel, its flag and the keys it needs when on."""

    name: str
    # The keys a switched-on channel needs: every one set and well
    # formed, or startup is refused. Not read at all when it is off.
    keys: tuple[str, ...]
    validators: Mapping[str, Validator] = field(default_factory=dict)
    # Keys that carry a DEFAULT and therefore do not decide anything --
    # "unset" and "set to the default" cannot be told apart. Their form
    # is still checked whenever the deploy spells them out, so a typo in
    # a defaulted value refuses startup like any other.
    optional_validators: Mapping[str, Validator] = field(
        default_factory=dict,
    )
    # The timeouts of the channel's network client, in seconds, by the
    # name of the client's own parameter -- the ONE place they are
    # written: the client is built from them (app/engine/formatters.py)
    # and startup holds every lane's deliver timeout above their sum
    # (app/core/config.py deliver_timeout_problems). Empty: the channel
    # has no network client (in_app -- its delivery is a row).
    client_timeouts: Mapping[str, float] = field(default_factory=dict)
    # True: the channel's call is bounded by its OWN deliver timeout, not
    # by the lane's (P5-1) -- a call that legitimately lasts minutes (a
    # model writing its answer) would otherwise force every lane's
    # timeout, the interactive email's included, above its client
    # (app/core/config.py deliver_timeout_problems leaves such a channel
    # out and holds the channel's own timeout above its client instead;
    # app/engine/service.py takes the bound from it).
    own_deliver_timeout: bool = False
    # The environment variable that switches the channel on (`true`) or
    # off (`false`) -- required, exactly one of the two. None only for a
    # channel with nothing to switch (in_app, no keys).
    flag: str | None = None

    @property
    def client_timeout_total(self) -> float:
        """The longest the client's own timeouts let one call run.

        The SUM of the phases, not the largest: each phase (waiting for
        a pooled connection, connecting, writing, reading) starts its
        own clock where the previous one stopped, so a call that spends
        almost all of every phase reaches the last one's timeout only
        after the sum. 0.0 for a channel without a client.
        """
        return sum(self.client_timeouts.values())


# The two values a channel's flag may hold, spelled exactly.
FLAG_VALUES: tuple[str, ...] = ("true", "false")


class ChannelState(StrEnum):
    """What a deploy has for one channel (the startup channel map)."""

    LIVE = "live"
    # Implemented, but this deploy switched it off (its flag is false).
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
# ONE PREFIX PER CHANNEL (EMAIL_), the flag and the provider-named keys
# alike: an operator reading an env file finds a channel's variables
# together. The provider IS in the key names on purpose -- "domain" and
# "region" are its concepts, and a neutral name over a single provider
# would mislead the next integrator.

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


# -- llm --------------------------------------------------------------------
#
# ONE KEY FOR THE CHANNEL, A NAMED SET OF KEYS INSIDE IT (P5-1, spec
# §14.12). A product routes an llm type to an ordered list of NAMED key
# sets -- `llm.text`, `llm.text_backup` -- and the names are the product's,
# so they cannot be fields of the settings class. LLM_KEY_SETS carries
# them all as one JSON object: {"<set name>": {"endpoint": ..., "key":
# ..., "model": ..., "gateway": true|false}}. The channel rule applies to
# that one key unchanged: LLM_ENABLED false -> it is not read; true ->
# it is required, and every set in it must be whole and well-formed, or
# startup is refused naming the set and the field -- never the value:
# the key is a secret.
#
# `gateway` HAS NO DEFAULT. A set that points at an aggregating gateway
# carries the gateway's routing conditions in every request (spec §12.1:
# no silent fallback of the gateway, no data collection); a set that
# points straight at a vendor must not -- a vendor refuses a parameter
# it does not know. Which one a set is, only the operator knows.

# The fields of one set, sorted (the order every refusal lists them in).
LLM_SET_FIELDS: tuple[str, ...] = ("endpoint", "gateway", "key", "model")
_LLM_STRING_FIELDS: tuple[str, ...] = ("endpoint", "key", "model")
# A set name: lower-case words joined by dots (`llm.text`), at most
# MAX_LLM_SET_NAME_LEN (app/core/constants.py; not imported -- this
# module imports nothing of comms, see the header) characters.
_LLM_SET_NAME_RE = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z0-9_]+)*\Z")
_LLM_SET_NAME_MAX = 64


@dataclass(frozen=True)
class LLMKeySet:
    """One named set: where the model is, the key to it, which model,
    and whether the address is a gateway. The key is kept out of repr:
    a set that reaches a log line or a traceback must not carry it."""

    name: str
    endpoint: str
    key: str = field(repr=False)
    model: str
    gateway: bool


def _refuse_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """json object hook: a name written twice is a refusal, not "the
    last one wins" -- which is what json.loads would do silently."""
    seen: dict[str, Any] = {}
    for name, value in pairs:
        if name in seen:
            raise _DuplicateNameError(name)
        seen[name] = value
    return seen


class _DuplicateNameError(ValueError):
    """A JSON object of LLM_KEY_SETS names one key twice."""


def _llm_endpoint(value: str) -> str | None:
    """An https base URL with a host; no credentials, query or fragment
    (the key travels in a header, never in the address)."""
    parts = urlsplit(value)
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
    ):
        return (
            "expected an https base URL with a host and no credentials, "
            "query or fragment (the adapter appends /chat/completions)"
        )
    return None


def _is_set_name(name: str) -> bool:
    return (
        len(name) <= _LLM_SET_NAME_MAX
        and _LLM_SET_NAME_RE.match(name) is not None
    )


# The longest word of a name a refusal repeats. A set or field name is
# words (`llm.text_backup`); a key is one long run of random characters
# -- one pasted into a name's place must not be repeated in the refusal.
_QUOTABLE_WORD_MAX = 24


def _quotable(name: str) -> str:
    """A JSON name as a refusal may quote it: a field name, or a name of
    set-name shape whose words are all short. Anything else may be a
    value pasted into the wrong place -- a key among them -- and is not
    repeated."""
    if name in LLM_SET_FIELDS or (
        _is_set_name(name)
        and all(
            len(word) <= _QUOTABLE_WORD_MAX
            for word in re.split(r"[._]", name)
        )
    ):
        return repr(name)
    return "a name not repeated (it may be a secret)"


def _llm_set_complaints(position: int, name: str, body: Any) -> list[str]:
    """What is wrong with one set, each complaint naming the set and
    the field -- never a value."""
    if not _is_set_name(name):
        return [
            f"set #{position}: {_quotable(name)} is not a set name -- a set "
            f"name is lower-case words joined by dots (e.g. 'llm.text'), "
            f"at most {_LLM_SET_NAME_MAX} characters"
        ]
    quoted = _quotable(name)
    where = f"set {quoted}" if quoted.startswith("'") else f"set #{position}"
    if not isinstance(body, dict):
        return [
            f"{where}: expected an object with the fields "
            f"{', '.join(LLM_SET_FIELDS)}"
        ]
    complaints: list[str] = []
    missing = [f for f in LLM_SET_FIELDS if f not in body]
    unknown = sorted(_quotable(str(f)) for f in body if f not in LLM_SET_FIELDS)
    if missing:
        complaints.append(f"{where}: missing {', '.join(missing)}")
    if unknown:
        complaints.append(
            f"{where}: unknown field(s) {', '.join(unknown)}; the fields "
            f"are {', '.join(LLM_SET_FIELDS)}"
        )
    for fname in _LLM_STRING_FIELDS:
        if fname not in body:
            continue
        value = body[fname]
        if not isinstance(value, str) or value.strip() == "":
            complaints.append(f"{where}: {fname} must be a non-empty string")
        elif any(ch.isspace() for ch in value):
            complaints.append(f"{where}: {fname} must not contain spaces")
        elif fname == "endpoint":
            complaint = _llm_endpoint(value)
            if complaint is not None:
                complaints.append(f"{where}: endpoint: {complaint}")
    if "gateway" in body and not isinstance(body["gateway"], bool):
        complaints.append(
            f"{where}: gateway must be a JSON boolean, true or false "
            f"(no default: whether the address is a gateway decides what "
            f"every request carries)"
        )
    return complaints


def parse_llm_key_sets(
    value: str,
) -> tuple[dict[str, LLMKeySet], list[str]]:
    """LLM_KEY_SETS -> (the sets by name, in declaration order;
    complaints). The sets are returned only when there is no complaint
    at all: a deploy with one broken set does not start, so no caller
    ever needs the others."""
    try:
        doc = json.loads(value, object_pairs_hook=_refuse_duplicates)
    except _DuplicateNameError as exc:
        return {}, [f"the name {_quotable(exc.args[0])} is written twice"]
    except json.JSONDecodeError as exc:
        return {}, [
            f"not JSON (line {exc.lineno}, column {exc.colno}: {exc.msg})"
        ]
    if not isinstance(doc, dict):
        return {}, [
            "expected a JSON object mapping set names to sets, "
            '{"llm.text": {"endpoint": ..., "gateway": ..., "key": ..., '
            '"model": ...}}'
        ]
    if not doc:
        return {}, [
            "names no set; leave LLM_KEY_SETS empty if this deploy has "
            "no llm"
        ]
    complaints: list[str] = []
    for position, (name, body) in enumerate(doc.items(), start=1):
        complaints.extend(_llm_set_complaints(position, name, body))
    if complaints:
        return {}, complaints
    sets = {
        name: LLMKeySet(
            name=name,
            endpoint=body["endpoint"].rstrip("/"),
            key=body["key"],
            model=body["model"],
            gateway=body["gateway"],
        )
        for name, body in doc.items()
    }
    return sets, []


def _llm_key_sets(value: str) -> str | None:
    """The channel-rule validator of LLM_KEY_SETS: every complaint about
    every set, in one line."""
    _, complaints = parse_llm_key_sets(value)
    return "; ".join(complaints) if complaints else None


# -- llm: the body of a letter by link (P5-2, W4-2) --------------------------
#
# A large body -- a voice recording -- does not travel in the envelope
# (spec §5.10): the product keeps it in its own object store and puts a
# SIGNED link to it in the envelope (`llm_audio.url`); comms fetches it
# on every attempt over the box's network and hands the bytes to the
# model. A link from a product's envelope is an address comms will GET
# -- without a limit, a way into every internal service of the box
# (comms-postgres, comms-redis, comms-app itself). The limit is ONE
# address: the point of the product's storage, which the deploy machine
# gives comms as the seam PRODUCT_STORAGE_ADDRESS -- `<alias>:<port>`,
# no scheme (Primer protocol 4.4) -- and a point inside the box is plain
# http (protocol 4.7 and its additions 1.10: the consumer builds
# `http://<address>`). A link is taken only when it BEGINS with exactly
# `http://<address>/`: the authority of a URL ends at its first `/`, so
# that prefix is the scheme, the host and the port, spelled as the seam
# spells them -- another scheme, host or port, credentials before an
# `@`, another case, a trailing dot or a port left to its default all
# fail it, with nothing parsed. Empty: the deploy has no product
# storage, and every link is refused. The prefix itself is built in
# app/core/config.py, the one file allowed a scheme.

_HOST_RE = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\Z")
_PORT_RE = re.compile(r"[1-9][0-9]{0,4}\Z")


def storage_address_problem(value: str) -> str | None:
    """What is wrong with PRODUCT_STORAGE_ADDRESS, or None when it reads
    as an address: `<host>:<port>` exactly as the machine gives it -- a
    lower-case name (or an IPv4 address) without a trailing dot, and a
    port from 1 to 65535 without a leading zero. Not normalized: the
    links are compared with it as it is spelled."""
    host, sep, port = value.rpartition(":")
    if (
        not sep
        or _HOST_RE.match(host) is None
        or _PORT_RE.match(port) is None
        or int(port) > 65535
    ):
        return (
            "PRODUCT_STORAGE_ADDRESS is not <host>:<port> -- a lower-case "
            "name or an IPv4 address without a trailing dot, a port from 1 "
            "to 65535, no scheme (the seam of the product's storage)"
        )
    return None


def body_link_allowed(url: str, prefix: str | None) -> bool:
    """Whether a body link may be fetched: it begins with `prefix`, the
    `http://<PRODUCT_STORAGE_ADDRESS>/` of app/core/config.py
    (Settings.body_link_prefix); never when the deploy has no product
    storage (None). The one test, at intake and at the attempt alike."""
    return prefix is not None and url.startswith(prefix)


# The client that fetches a body: its own numbers, in this one place.
# read is per chunk, not for the whole body -- the whole download is
# bounded by LLM_BODY_DOWNLOAD_TIMEOUT on top of the client, which is
# above the client's total, so a stalled store times out inside the
# client first. The job's call bound adds it once (app/engine/service.py).
LLM_BODY_CLIENT_TIMEOUTS: Mapping[str, float] = {
    "connect": 5.0, "read": 20.0, "write": 5.0, "pool": 4.0,
}
LLM_BODY_DOWNLOAD_TIMEOUT: float = sum(LLM_BODY_CLIENT_TIMEOUTS.values()) + 1.0


CHANNEL_SPECS: tuple[ChannelSpec, ...] = (
    ChannelSpec(name="in_app", keys=()),
    ChannelSpec(
        name="telegram",
        keys=("TELEGRAM_BOT_TOKEN", "TELEGRAM_BOT_URL"),
        validators={
            "TELEGRAM_BOT_TOKEN": _telegram_token,
            "TELEGRAM_BOT_URL": _telegram_url,
        },
        # aiogram's session takes ONE timeout, and aiohttp counts it
        # over the whole request (aiohttp ClientTimeout(total=...)):
        # connect, send and the answer together. Explicit since P4-3 --
        # aiogram's own default (60 s) sat above the 30 s deliver
        # timeout, so the session's timeout never fired. 29: below the
        # 30 s every lane defaults to (app/core/config.py).
        # See KNOWN CEILING (client totals) below.
        client_timeouts={"timeout": 29.0},
        flag="TELEGRAM_ENABLED",
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
        # the default" are indistinguishable, and requiring it would
        # refuse a deploy that relies on the default. Its FORM is still
        # checked, but only when the channel is on and the deploy spells
        # it out.
        optional_validators={
            "EMAIL_MAILGUN_REGION": _email_region,
        },
        # httpx times each phase on its own clock. read keeps its 15 s:
        # a read timeout is the one that may leave a letter on its way
        # (formatters.py _request_may_have_arrived), and shortening it
        # turns slow answers into retries, i.e. duplicates. write was
        # 10 and pool 5 (35 in all, above the 30 s deliver timeout --
        # the relation R-1 meant to keep had already broken): a form
        # of a few KB is not written for 5 s, and with every lane's
        # slots together (app/engine/service.py LANE_CONCURRENCY, 25)
        # below httpx's 100 pooled connections a call never waits for
        # one. 29 in all, below the 30 s every lane defaults to.
        client_timeouts={"connect": 5.0, "read": 15.0, "write": 5.0, "pool": 4.0},
        flag="EMAIL_ENABLED",
    ),
    ChannelSpec(
        name="llm",
        keys=("LLM_KEY_SETS",),
        validators={"LLM_KEY_SETS": _llm_key_sets},
        # One call to one set. read is the model writing its answer: a
        # long answer of a slow model takes tens of seconds, and a read
        # timeout here -- unlike a connect timeout -- may leave a paid
        # answer behind (the next set is called). 114 in all, below the
        # channel's own deliver timeout (LLM_DELIVER_TIMEOUT_SECONDS,
        # default 120) -- the lanes' 30 never meets it
        # (own_deliver_timeout).
        client_timeouts={"connect": 5.0, "read": 100.0, "write": 5.0, "pool": 4.0},
        own_deliver_timeout=True,
        flag="LLM_ENABLED",
    ),
)

# KNOWN CEILING (acknowledged by design -- P4-3, client totals): a
# client's total is not a wall-clock bound in every case.
#   1. Mechanics: startup holds each lane's deliver timeout above the
#      client total of every live channel, so that a call times out
#      INSIDE its client -- where "never reached" and "reached, no
#      answer" stay apart -- before the outer wait_for abandons it. Two
#      ways the client still runs past its total:
#        - email: httpx's read timeout is per read, not per response; a
#          provider that drips its answer in pieces, each within 15 s,
#          keeps reading past the sum;
#        - telegram: aiohttp rounds a timeout above 5 s up to the next
#          whole second of the loop's clock (aiohttp helpers
#          calculate_timeout_when), so 29 s fires 29 to 30 s after the
#          request starts -- and the request starts a few milliseconds
#          after the lane's wait_for did. With the lane at 30, the outer
#          timeout comes first when a whole second falls into those
#          milliseconds.
#      Either way the call ends as today's did before P4-3: the lane's
#      TimeoutError, journaled `timeout` -- transient, retried, the
#      distinction lost for that one call.
#   2. Status: acknowledged by design.
#   3. Backlog ref: none -- found while building P4-3; the numbers
#      (29 / 30) are the gate's decision; the rounding was read in
#      aiohttp's body after it.
#   4. Promotion trigger (observable): a channel row of the journal
#      (notification_transitions, subject `channel`) with outcome
#      `timeout` and channel telegram or email -- with the rule in place
#      every such row is one of these two.
#   5. Agreed fix: none agreed; candidates -- telegram's total one
#      rounding second below the lane (28), or the session handed an
#      aiohttp ClientTimeout with ceil_threshold above it; for email a
#      total bound of its own around the client call.
#   6. Rejected: raising the lane defaults above 30 (normal must stay
#      today's 30 s -- P4-3 done-when 1.3); shortening read (more
#      duplicates, above); clamping a lane timeout to fit instead of
#      refusing startup (§0: a red start is the result).


def channel_env_keys() -> tuple[str, ...]:
    """Every REQUIRED key of every channel, in declaration order.

    Defaulted keys are not here: blanking them (tests/conftest.py) would
    replace a working default with an invalid empty value.
    """
    return tuple(key for spec in CHANNEL_SPECS for key in spec.keys)


def channel_flag_keys() -> tuple[str, ...]:
    """Every channel's flag, in declaration order (in_app has none)."""
    return tuple(spec.flag for spec in CHANNEL_SPECS if spec.flag is not None)


def channel_spec(name: str) -> ChannelSpec:
    """The spec of the implemented channel `name` (KeyError otherwise)."""
    return {spec.name: spec for spec in CHANNEL_SPECS}[name]


def channel_optional_keys() -> tuple[str, ...]:
    """Every DEFAULTED key of every channel, in declaration order."""
    return tuple(
        key for spec in CHANNEL_SPECS for key in spec.optional_validators
    )


def switched_off_reason(name: str) -> str:
    """Why a switched-off channel takes no delivery -- the reason the
    outcome "channel unavailable" carries (app/engine/formatters.py),
    naming the flag that switched it off."""
    return f"not configured ({channel_spec(name).flag}=false on this deploy)"


def _flag_problem(spec: ChannelSpec, value: str | None) -> str:
    """The refusal of a flag that is not exactly true or false."""
    expected = " or ".join(FLAG_VALUES)
    if value is None:
        return (
            f"{spec.flag} is not set: it switches channel '{spec.name}' "
            f"on or off and must be exactly {expected}"
        )
    return (
        f"{spec.flag}={value!r} is not a switch: channel '{spec.name}' "
        f"is on or off by exactly {expected}"
    )


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
        f"  {spec.flag}=true: set every key of '{spec.name}' "
        f"({', '.join(spec.keys)}), or set {spec.flag}=false if this "
        f"deploy has no {spec.name}."
    )
    return "\n".join(lines)


def evaluate_channels(
    values: Mapping[str, str | None],
) -> tuple[dict[str, ChannelState], list[str]]:
    """Apply the rule to one deploy's flags and key values.

    Args:
        values: Flag or key name -> configured value. A key that is
            missing or "" is not set; a flag that is missing (or None)
            is not set either -- and that is a refusal.

    Returns:
        (states, problems): the state of every IMPLEMENTED channel, and
        one human-readable problem per broken channel -- ALL of them,
        so an integrator fixes every mistake in one pass. A channel with
        a problem has no state entry: it must never be reported as live
        or absent, because the deploy must not start.

    A flag that is not exactly true or false is named alone: whether
    the channel's keys are wanted is not known, so they are not judged.
    """
    states: dict[str, ChannelState] = {}
    problems: list[str] = []
    for spec in CHANNEL_SPECS:
        if spec.flag is not None:
            flag = values.get(spec.flag)
            if flag not in FLAG_VALUES:
                problems.append(_flag_problem(spec, flag))
                continue
            if flag == "false":
                # Switched off: nothing of the channel is read.
                states[spec.name] = ChannelState.NOT_CONFIGURED
                continue

        present = [k for k in spec.keys if values.get(k) or ""]
        missing = [k for k in spec.keys if k not in present]
        malformed: list[str] = []
        # Defaulted keys are never missing, but a spelled-out value of
        # the wrong shape is still a refusal.
        checked = present + [
            k for k in spec.optional_validators if values.get(k) or ""
        ]
        for key in checked:
            value = values[key] or ""
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
