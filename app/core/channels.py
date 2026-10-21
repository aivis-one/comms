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


# -- llm --------------------------------------------------------------------
#
# ONE KEY FOR THE CHANNEL, A NAMED SET OF KEYS INSIDE IT (P5-1, spec
# §14.12). A product routes an llm type to an ordered list of NAMED key
# sets -- `llm.text`, `llm.text_backup` -- and the names are the product's,
# so they cannot be fields of the settings class. LLM_KEY_SETS carries
# them all as one JSON object: {"<set name>": {"endpoint": ..., "key":
# ..., "model": ..., "gateway": true|false}}. The channel rule applies to
# that one key unchanged: empty -> the deploy has no llm; set -> every
# set in it must be whole and well-formed, or startup is refused naming
# the set and the field -- never the value: the key is a secret.
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


# -- llm: the body of a letter by link (P5-2) ---------------------------------
#
# A large body -- a voice recording -- does not travel in the envelope
# (spec §5.10): the product keeps it in its own object store and puts a
# SIGNED link to it in the envelope (`llm_audio.url`); comms fetches it
# on every attempt over the internal network and hands the bytes to the
# model. A link from a product's envelope is an address comms will GET
# -- without a limit, a way into every internal service of the box
# (comms-postgres, comms-redis, comms-app itself). LLM_BODY_HOSTS is the
# limit: the exact `host:port` pairs a link may point at, comma
# separated. Empty (the default): this deploy fetches nothing, and an
# envelope with a link is rejected at intake. A list of what is
# ALLOWED, never of what is forbidden -- a deny list is never complete.

_HOST_RE = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\Z")


def parse_llm_body_hosts(value: str) -> tuple[frozenset[tuple[str, int]], list[str]]:
    """LLM_BODY_HOSTS -> (the allowed (host, port) pairs; complaints).
    Hosts compare in lower case; no default port -- written out, so the
    list says exactly what is reached."""
    allowed: set[tuple[str, int]] = set()
    complaints: list[str] = []
    for position, raw in enumerate(value.split(","), start=1):
        entry = raw.strip()
        if entry == "":
            complaints.append(f"entry #{position} is empty")
            continue
        host, sep, port = entry.rpartition(":")
        host = host.lower()
        if (
            not sep
            or _HOST_RE.match(host) is None
            or not port.isdigit()
            or not 1 <= int(port) <= 65535
        ):
            complaints.append(
                f"entry #{position} {entry!r} is not host:port (a name or "
                f"an IPv4 address, a port from 1 to 65535)"
            )
            continue
        pair = (host, int(port))
        if pair in allowed:
            complaints.append(f"{host}:{port} is listed twice")
            continue
        allowed.add(pair)
    return frozenset(allowed), complaints


def body_endpoint(url: str) -> tuple[str, int]:
    """(host, port) of a body link, the host in lower case and the port
    written out -- the form LLM_BODY_HOSTS lists. Only for a link whose
    form intake accepted (app/transport/events.py validate_llm_audio)."""
    parts = urlsplit(url)
    if parts.hostname is None:
        raise ValueError("a body link without a host")
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return parts.hostname.lower(), port


def _llm_body_hosts(value: str) -> str | None:
    """The form validator of LLM_BODY_HOSTS: every complaint, one line."""
    _, complaints = parse_llm_body_hosts(value)
    return "; ".join(complaints) if complaints else None


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
    ),
    ChannelSpec(
        name="llm",
        keys=("LLM_KEY_SETS",),
        validators={"LLM_KEY_SETS": _llm_key_sets},
        # Defaulted (empty = no body is ever fetched), so not a key that
        # decides whether llm exists; its form is checked when spelled
        # out (P5-2).
        optional_validators={"LLM_BODY_HOSTS": _llm_body_hosts},
        # One call to one set. read is the model writing its answer: a
        # long answer of a slow model takes tens of seconds, and a read
        # timeout here -- unlike a connect timeout -- may leave a paid
        # answer behind (the next set is called). 114 in all, below the
        # channel's own deliver timeout (LLM_DELIVER_TIMEOUT_SECONDS,
        # default 120) -- the lanes' 30 never meets it
        # (own_deliver_timeout).
        client_timeouts={"connect": 5.0, "read": 100.0, "write": 5.0, "pool": 4.0},
        own_deliver_timeout=True,
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
    """Every DECIDING key of every channel, in declaration order.

    Defaulted keys are not here: blanking them (tests/conftest.py) would
    replace a working default with an invalid empty value, and they
    cannot make a channel live or absent anyway.
    """
    return tuple(key for spec in CHANNEL_SPECS for key in spec.keys)


def channel_spec(name: str) -> ChannelSpec:
    """The spec of the implemented channel `name` (KeyError otherwise)."""
    return {spec.name: spec for spec in CHANNEL_SPECS}[name]


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
