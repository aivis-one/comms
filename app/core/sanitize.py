# =============================================================================
# COMMS Service -- the one sanitizer (secrets out of every text comms
# writes: records, journal, logs)
# =============================================================================
#
# Lives in core (H8): the logging setup (app/core/logging.py) runs every
# log line through it, and core imports nothing above it -- core is
# rank 0 of the package graph (app/__init__.py). Every caller imports
# it from HERE; there is no second copy and no re-export
# (tests/test_log_sanitizing.py holds that with an AST check).
# =============================================================================

import re
import traceback
from pathlib import Path
from typing import Any

# Secret-redaction patterns (review 1.1, widened in F0-comms item 2 and
# in SAN-1).
#
# THE LIST IS DRIVEN BY THE SECRETS THIS SERVICE ACTUALLY HOLDS, in the
# form each takes where it can surface in an error text -- not by what a
# secret "usually" looks like. The secrets (app/core/config.py) and the
# form each is checked in:
#   DATABASE_URL        postgresql+asyncpg://comms:<pw>@host/db   userinfo
#   REDIS_URL           redis://:<pw>@host:6379/0 (EMPTY user --
#                       the form deploy/docker-compose.yml builds) userinfo
#   COMMS_SERVICE_TOKEN Authorization: Bearer <t>; bare hex         header,
#   and _NEXT           (R-4: the name of the next one in the key)  bearer, hex
#   TELEGRAM_BOT_TOKEN  .../bot<id>:<secret>/sendMessage inside an
#                       aiohttp error text (aiogram wraps it as
#                       "<ClientError class>: <error>")            telegram
#   EMAIL_MAILGUN_API_KEY bare <32hex>-<8hex>-<8hex> or key-<32hex>;
#                       Authorization: Basic <b64 of api:key>     mailgun,
#                                                                  header
#   LLM_KEY_SETS keys   bare sk-<key chars>; Authorization: Bearer   llm,
#                       <key> (the adapter's header); the whole    bearer,
#                       value after its name, a JSON object of     pair,
#                       sets; a set's key of any shape as          json key
#                       "key": "<key>" (R-4)
# The names of the list are written in the key below and held to the
# secrets of the instance -- primer.yaml, by the data SEC-1 reads -- by
# tests/test_sanitizer_service_secrets.py: a new secret whose name the
# key does not know is red there.
# The passwords and the service token the deploy machine mints
# (token_urlsafe: 43 characters of [A-Za-z0-9_-]) are caught in the
# userinfo, header and bearer forms above, and NOT bare -- see the KNOWN
# CEILING at the long-hex pattern below.
#
# ORDER MATTERS, and every pattern is IDEMPOTENT on its own output --
# the email path sanitizes the provider's text where it enters, and the
# service layer sanitizes the exception again when it writes the
# record, so every text goes through this twice:
#   1. the Authorization header, scheme included -- before bearer, so
#      "Authorization: Basic x" loses its credentials and not just the
#      word "Basic"; then a Cookie or Set-Cookie header, to the end of
#      its line (REV-9);
#   2. key=value / key: value for credential keywords (SAN-1) -- BEFORE
#      the shapes below (REV-4b): a shape that took the start of a value
#      ("password=sk-...", "token=<48 hex>") left its tail behind, since
#      the pair does not extend a value already [redacted]; the pair now
#      takes the whole value first, so "password=sk-... more" is
#      "password=[redacted]" -- up to the boundary, the pair's own rule;
#      then a field named exactly key, quoted, with a string value -- a
#      key of LLM_KEY_SETS where its JSON stands without the variable's
#      name (R-4; _JSON_KEY_RE below);
#   3. bearer on its own (a token with no header name in front);
#   4. userinfo in a URL/DSN, EMPTY user allowed, the password running
#      to the LAST @ before a space -- any other character in a password
#      leaves no tail: a raw @ or / (base64 has /), or both ("p@ss/w";
#      REV-6; REV-4b stopped at the first @ followed by a host, and
#      "p@ss/w@db" kept "ss/w"), and a raw ? or # (W6-2b: REV-6 stopped
#      the rest at them, and "comms:Zx9?Qw#7@db" kept its password whole
#      -- a regression against 3.14.0). A PORT IS NOT A PASSWORD (W6-2c):
#      after "host:", 1 to 5 digits and then "/", "?" or "#" are a port
#      and the path, the query or the fragment behind it -- the address
#      stays as it is, an @ later in it or not
#      ("http://storage:9000/a.ogg?user=john@example.com"), and a URL
#      further on in the same word is still looked at
#      ("...?next=https://u:p@h" -> "...?next=https://u:[redacted]@h").
#      THE PRICES:
#        - an @ anywhere after the authority of a URL WITH userinfo -- in
#          the path, the query or the fragment -- takes the authority
#          with it: "https://u:p@host/a@b" -> "https://u:[redacted]@b",
#          "https://u:p@host/p?e=a@b.c" -> "https://u:[redacted]@b.c" --
#          text cleaned that need not be, never a secret left (owner,
#          14.11: a hidden tail of an address is safer than a tail of a
#          password left);
#        - a password that STARTS with 1 to 5 digits followed at once by
#          "/", "?" or "#" ("://u:12/ab@db") reads as a port and is left
#          whole: the two have one shape. A password the deploy machine
#          mints is secrets.token_urlsafe -- [A-Za-z0-9_-], none of the
#          three -- so it is never this one; a password that is digits
#          alone ends at its "@" and is cleaned ("://u:1234@db");
#   5. token-shaped values with no keyword next to them.
#   Pattern 2 in detail, in two passes -- a query parameter first, then
#   every other form:
#      - THE KEY is a credential word (password, passwd, pwd, token,
#        secret, api_key/api-key/apikey, authorization, secret_key,
#        private_key, credential/credentials -- REV-4b; cookie,
#        session_id/sessionid, passphrase, pgpassword,
#        secret_access_key -- REV-6; and the two names of this service
#        that end in no such word: comms_service_token_next,
#        llm_key_sets -- R-4; never a bare
#        "key": idempotency_key and correlation_key are on every line
#        comms writes) with any
#        prefix joined by _ or - (access_token, db_password,
#        comms_service_token, x-api-key), quoted or not ("password":,
#        'password':). A word that only CONTAINS one does not count:
#        max_tokens, tokenizer, password_changed, passwords,
#        AuthorizationError -- the separator must follow the credential
#        word itself (an optional closing quote between them).
#      - THE VALUE runs: inside quotes -- to the closing quote, the
#        quotes kept (JSON, a dict's repr); after ? or & (a URL query)
#        -- to the next &, #, quote or whitespace (a URL holds no raw
#        space, and an access line keeps its ' HTTP/1.1" 200'); opening
#        with [ or { -- to the end of the line (R-4: a JSON value, as
#        LLM_KEY_SETS is, was cut at its first , ] or } and the keys
#        after it stayed); any other -- to the first , ; & ) ] } or the
#        end of the line, so a value with a space in it leaves no tail.
#      - A value that is already [redacted] (pattern 1's output, or
#        this pattern's own on the second pass) is not extended: that
#        is what keeps the text after a redacted header and makes the
#        pass idempotent.
#    The separator is kept as written (key: [redacted], key=[redacted]).
#    THE PRICE of pattern 2, by design: a secret that itself contains
#    , ; & ) ] } leaves the part after that character behind; the text
#    after an unquoted value on the same line, up to that boundary, is
#    eaten with it ("token=abc rest" -> "token=[redacted]"), and after a
#    value opening with [ or { the rest of the line is ("secret={...},
#    next" -> "secret=[redacted]"). The limit:
#    a camelCase key (accessToken, apiKey) is not recognised -- with no
#    _ or - before the credential word there is no boundary to stop at
#    without also matching ordinary words that end in one.
_AUTH_HEADER_RE = re.compile(
    r"(?i)\b(authorization)\b\s*[=:]\s*(?:(?:bearer|basic|digest|token)\s+)?\S+"
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+\S+")
# The userinfo: "://", a user, ":", and the rest of the URL up to a
# space -- the password is that rest up to its LAST "@", found by
# _userinfo_redacted, not by the expression: REV-6 wrote the "@" and a
# look-ahead into it, and "://a:" repeated made it try every "@" from
# every start -- quadratic, 91 s on 200 000 characters (REV-9). The same
# "@": past the last one there is no other before the space, which is
# all the look-ahead asked. The rest is not cut at "?" or "#" (W6-2b,
# pattern 4 above): a password may hold them. The one look-ahead left is
# the port's (W6-2c): at most 5 digits and one character, so it costs a
# bounded step at each "://", and a port it sees ends the match there --
# the word is not taken, and a URL further on in it is still matched.
_URL_USERINFO_RE = re.compile(r"(://[^/\s:@]*:)(?!\d{1,5}[/?#])(\S*)")
# A Cookie or Set-Cookie HEADER: its value is every cookie up to the end
# of the line, not the first pair (REV-9) -- "Cookie: a=b; c=d" kept
# "c=d" under the pair's boundary. The header form only, the colon; a
# pair "cookie=..." stays the pair's. The spaces after the colon are
# taken whole (possessive), so the guard sees what follows them and a
# second pass leaves "Cookie: [redacted]" as it is.
_COOKIE_HEADER_RE = re.compile(
    r"(?i)(?<![a-z0-9_-])((?:set-)?cookie)(\s*:[ \t]*+)(?!\[redacted\])[^\r\n]+"
)
_TELEGRAM_TOKEN_RE = re.compile(r"\b(?:bot)?\d{6,}:[A-Za-z0-9_-]{30,}")
_MAILGUN_KEY_RE = re.compile(
    r"(?i)\b(?:key-[0-9a-f]{32}|[0-9a-f]{32}-[0-9a-f]{8}-[0-9a-f]{8})\b"
)
# The key shape of OpenAI-compatible providers and gateways (P5-1, the
# llm channel's LLM_KEY_SETS): `sk-` and then a long run of key
# characters (`sk-or-v1-<hex>`, `sk-proj-...`). A vendor key of another
# shape is the KNOWN CEILING below.
_LLM_KEY_RE = re.compile(r"\bsk-[A-Za-z0-9_-]{20,}")
# KNOWN CEILING -- bare secrets in a shape no pattern here knows.
#   1. Mechanics: this pattern catches a secret with no keyword, header
#      or URL around it ONLY as 48+ hex characters -- the shape the old
#      installer minted (gone since W1-2). The secrets of an instance are
#      now minted by Primer's deploy machine as token_urlsafe(32), 43
#      characters of [A-Za-z0-9_-] (POSTGRES_PASSWORD, COMMS_SERVICE_TOKEN,
#      COMMS_EVENTS_PASSWORD), and the channels' keys are asked of the
#      operator: any of them surfacing bare in an error text passes
#      through every pattern here unchanged. The machine cleans the
#      values it holds out of the lines of comms' log itself (protocol
#      9.4); a text comms stores (a delivery's error) is
#      not a line of the log.
#   2. Status: acknowledged by design.
#   3. Task: none -- outside the table of W1-2, which found it (the
#      trigger below has fired: report of W1-2, observations).
#   4. Unfreeze trigger: a secret of the instance in a text comms stores
#      or answers -- a delivery's error, a response.
#   5. Agreed fix: redact by VALUE, not by shape -- read the configured
#      secrets from settings once and replace their literal occurrences.
#   6. Rejected: widening the shape patterns to "any long token" (eats
#      message ids, digests and ordinary identifiers, and still misses a
#      short password); refusing secrets of another shape at startup (not
#      ours to demand of a provider credential or of the machine, and a
#      red start on a valid configuration).
_LONG_HEX_RE = re.compile(r"\b[0-9a-fA-F]{48,}\b")
# The key: a credential word -- or a name of this service's secrets
# that ends in none (R-4) -- with any prefix joined by _ or -. LINEAR on
# any text (REV-6): the prefix starts only where no letter, digit, _ or -
# stands before it, and every repetition ends in a separator its letters
# cannot take -- one way to split a run, so no start inside a run is
# tried and nothing is tried twice. The prefix SAN-1 wrote,
# \b(?:[a-z0-9]+[_-])*, tried every start of a run like "a-a-a-...":
# quadratic, 22 s on 80 000 characters. The names R-4 added are words of
# the same list, behind the same prefix: whole literals, nothing that
# repeats -- they widen no form. A rule "a credential word anywhere in
# the name" is not this list: it would take token_status,
# password_changed, passphrase_hint, and still miss llm_key_sets (no
# bare "key" -- see the list of pattern 2).
_KEY = (
    r"(?<![a-z0-9_-])(?:[a-z0-9]*[_-])*"
    r"(?:password|passwd|pwd|token|secret|api[_-]?key|apikey|authorization"
    r"|secret[_-]?key|private[_-]?key|credentials?"
    r"|cookie|session[_-]?id|passphrase|pgpassword|secret[_-]?access[_-]?key"
    r"|comms[_-]service[_-]token[_-]next|llm[_-]key[_-]sets)"
)
_REDACTED = "[redacted]"
_QUERY_KEYVAL_RE = re.compile(
    rf"(?i)(?<=[?&])({_KEY}=)(?!\[redacted\])[^&#\s\"']+"
)
# One piece of an unquoted value: a mark an earlier pattern left (its ]
# is not the value's end) or a character that is not a boundary.
_UNQUOTED = r"(?:\[redacted\]|[^\s,;&)\]}])"
_KEYVAL_RE = re.compile(
    rf"(?i)({_KEY}[\"']?\s*[=:]\s*)"
    r"(\[redacted\]"
    r"|\"(?:[^\"\\\r\n]|\\.)*\""
    r"|'(?:[^'\\\r\n]|\\.)*'"
    r"|[\"'][^\r\n]*"
    r"|[\[{][^\r\n]*"
    rf"|{_UNQUOTED}(?:[ \t]*{_UNQUOTED})*)"
)
# A field named exactly key, in quotes, with a string value: the key of
# a set of LLM_KEY_SETS where its JSON or a dict's repr stands without
# the variable's name -- a key of any shape, not only sk- (R-4). The
# quote must stand right before "key": idempotency_key, "keys", "monkey"
# are not it, and neither is key= or a field `key` of a log event (a
# section's key; is_secret_name below). The value runs as pattern 2's
# quoted one: to the closing quote, or -- cut short -- to the end of the
# line. A key escaped inside a JSON string (\"key\") is not seen, as
# pattern 2 sees no escaped key either.
_JSON_KEY_RE = re.compile(
    r"([\"'])key\1(\s*:\s*)"
    r"(\"(?:[^\"\\\r\n]|\\.)*\"?|'(?:[^'\\\r\n]|\\.)*'?)"
)


def _keyval_redacted(match: re.Match[str]) -> str:
    """Pattern 2's replacement: the key and separator as written, the
    value's quotes kept around [redacted]."""
    head, value = match.group(1), match.group(2)
    if value[0] in "\"'":
        closed = len(value) > 1 and value[-1] == value[0]
        return f"{head}{value[0]}{_REDACTED}{value[0] if closed else ''}"
    return f"{head}{_REDACTED}"


def _json_key_redacted(match: re.Match[str]) -> str:
    """_JSON_KEY_RE's replacement: the key and separator as written, the
    value's quotes kept around [redacted] -- the closing one if it was
    there."""
    quote, sep, value = match.group(1), match.group(2), match.group(3)
    closed = len(value) > 1 and value[-1] == value[0]
    tail = value[0] if closed else ""
    return f"{quote}key{quote}{sep}{value[0]}{_REDACTED}{tail}"


def sanitize_text(text: str) -> str:
    """Remove every secret form listed above from a text. Not truncated.

    Idempotent: sanitize_text(sanitize_text(x)) == sanitize_text(x).
    Callers cut AFTER calling this, never before: a cut through the
    middle of a secret can leave a prefix no pattern recognises. The
    gaps in coverage: the KNOWN CEILING on _LONG_HEX_RE, and pattern 2's
    price and its camelCase limit (the list above).
    """
    text = _AUTH_HEADER_RE.sub(lambda m: f"{m.group(1)}=[redacted]", text)
    text = _COOKIE_HEADER_RE.sub(r"\1\2[redacted]", text)
    text = _QUERY_KEYVAL_RE.sub(rf"\1{_REDACTED}", text)
    text = _KEYVAL_RE.sub(_keyval_redacted, text)
    text = _JSON_KEY_RE.sub(_json_key_redacted, text)
    text = _BEARER_RE.sub("bearer [redacted]", text)
    text = _URL_USERINFO_RE.sub(_userinfo_redacted, text)
    text = _TELEGRAM_TOKEN_RE.sub("[redacted]", text)
    text = _MAILGUN_KEY_RE.sub("[redacted]", text)
    text = _LLM_KEY_RE.sub("[redacted]", text)
    return _LONG_HEX_RE.sub("[redacted]", text)


def _userinfo_redacted(match: re.Match[str]) -> str:
    """Pattern 4's replacement: the password up to the LAST "@" of the
    rest; no "@" -- no userinfo, the text as it is."""
    head, rest = match.group(1), match.group(2)
    at = rest.rfind("@")
    if at < 0:
        return match.group(0)
    return f"{head}{_REDACTED}{rest[at:]}"


_SECRET_NAME_RE = re.compile(rf"(?i){_KEY}")


def is_secret_name(name: str) -> bool:
    """Whether a NAME -- a field of a log event, a key of a dict in one
    -- is a credential's (REV-4b): the key of pattern 2, the whole name.
    One rule with the text: "password", "x-api-key", "db_secret_key",
    "llm_key_sets" are; "passwords_match", "max_tokens",
    "idempotency_key", "key" are not."""
    return _SECRET_NAME_RE.fullmatch(name) is not None


_APP_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = _APP_ROOT.parent
# notifications.pipeline_error and notification_transitions.error are
# varchar(300) (migrations 0015, 0017).
_PIPELINE_ERROR_MAX = 300


def pipeline_error_of(exc: BaseException) -> str:
    """The CLASS of an exception -- comms' own or a library's -- and the
    PLACE in comms nearest to where it was raised: the deepest frame of
    app/ on its traceback (module:line).

    NEVER the exception's text, sanitized or not. Sanitizing removes
    secrets, not content: a text raised from a template or a formatter
    can carry the letter's variables, a database error's text is its
    SQL and bound parameters, and a record of comms holds no letter
    content (spec §6.4) -- nor does the line of a dropped event
    (app/transport/consumer.py). The class and the place say where it
    tore; the redacted traceback in the log says the rest. Lives here,
    in app/core, since DB-1: the rule for a database error's text in a
    log (db_error_summary) is built on it, and core imports no engine.
    """
    name = f"{type(exc).__module__}.{type(exc).__qualname__}"
    frames = traceback.extract_tb(exc.__traceback__)
    place = "no traceback"
    for frame in reversed(frames):
        path = Path(frame.filename).resolve()
        if path.is_relative_to(_APP_ROOT):
            place = f"{path.relative_to(_REPO_ROOT).as_posix()}:{frame.lineno}"
            break
    else:
        if frames:
            place = f"{Path(frames[-1].filename).name}:{frames[-1].lineno}"
    return f"{name} at {place}"[:_PIPELINE_ERROR_MAX]




# =============================================================================
# A DATABASE ERROR'S TEXT NEVER REACHES A LOG (DB-1)
# =============================================================================
# The owner's decision of 1 November 2026 (spec §16.3): a database error
# in a log is its CLASS, its PLACE, its SQLSTATE and the NAME of the
# constraint -- never its text. Measured on real errors: the text of every
# link of the chain -- SQLAlchemy's error, the driver adapter's, asyncpg's
# own -- carries the server's DETAIL ("Key (id)=(...) already exists",
# "Failing row contains (...)": a whole row, a letter in it), and an error
# like 22P02 carries the value in its message itself; hide_parameters on
# the engine removes SQLAlchemy's [parameters] alone. So the text goes,
# link by link; the frames stay. ONE recognition (_is_database_error), for
# the traceback (sanitized_traceback), the one line (exception_text) and
# the logging pipeline (app/core/logging.py).


def _database_classes() -> tuple[type[BaseException], ...]:
    """The classes whose text is the database's or the driver's: SQLAlchemy's
    StatementError (DBAPIError's base; it carries [SQL] and [parameters]
    without a driver too), the asyncpg adapter's errors, asyncpg's own --
    the server's (PostgresError) and the client's (InterfaceError, its
    DataError for a parameter of the wrong type quoting the value) -- and
    SQLAlchemy's PendingRollbackError (SAN-1): a session used again after
    a failed flush raises it with the failed flush's error PASTED into its
    own text ("Original exception was: ...", orm/session.py) -- its [SQL]
    and the driver's message, which can quote a value (22P02, a
    parameter of the wrong type) -- and with NO chain to that error (no
    __cause__, no __context__): only its class can tell it is the
    database's."""
    from asyncpg.exceptions import InterfaceError, PostgresError
    from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
    from sqlalchemy.exc import PendingRollbackError, StatementError

    return (StatementError, AsyncAdapt_asyncpg_dbapi.Error, PostgresError,
            InterfaceError, PendingRollbackError)


def _is_database_error(exc: BaseException) -> bool:
    return isinstance(exc, _database_classes())


def _chain(exc: BaseException) -> list[BaseException]:
    """exc and what it wraps: SQLAlchemy's .orig, then __cause__ /
    __context__, without loops."""
    seen: list[BaseException] = []
    link: BaseException | None = exc
    while link is not None and all(link is not s for s in seen):
        seen.append(link)
        orig = getattr(link, "orig", None)
        link = orig if isinstance(orig, BaseException) else (
            link.__cause__ or link.__context__)
    return seen


def db_error_summary(exc: BaseException) -> str | None:
    """A database error as a log may hold it: "<class> at <place>",
    SQLSTATE and the constraint's name when the chain has them -- or None
    for an exception that is not the database's."""
    if not _is_database_error(exc):
        return None
    sqlstate = constraint = None
    for link in _chain(exc):
        sqlstate = sqlstate or getattr(link, "sqlstate", None) or getattr(
            link, "pgcode", None)
        constraint = constraint or getattr(link, "constraint_name", None)
    summary = pipeline_error_of(exc)
    if sqlstate:
        summary += f", SQLSTATE {sqlstate}"
    if constraint:
        summary += f", constraint {constraint}"
    return summary


def exception_text(exc: BaseException) -> str:
    """ONE line for an exception in a log or a record: a database error's
    summary (db_error_summary), any other exception's text, sanitized --
    comms' own words (a ValidationError, a provider's answer) stay."""
    summary = db_error_summary(exc)
    if summary is not None:
        return summary
    try:
        text = str(exc)
    except Exception:
        # Never a failure of its own (REV-6): an exception whose __str__
        # raises is named by its type, as unreadable_value does.
        return unreadable_value(exc)
    return sanitize_text(text)[:2000]


def unreadable_value(value: object) -> str:
    """What stands in a log or a record for a value that cannot be read
    -- its str or repr raises, it is nested past the walk: its type,
    never a part of it (REV-6)."""
    return f"<unreadable {type(value).__name__}>"


def sanitized_traceback(exc: BaseException) -> str:
    """The traceback structlog's format_exc_info would render: the frames
    whole, the text of every link that is a database error replaced by its
    summary (DB-1), the rest redacted as ONE text.

    Every place that logs an exception logs it as
    `exception=sanitized_traceback(exc)` -- never logger.exception() or
    exc_info, which would print the chain raw -- and the logging pipeline
    renders an exc_info it meets the same way (app/core/logging.py)."""
    rendered_exc = traceback.TracebackException.from_exception(exc)
    _replace_database_texts(rendered_exc, exc, set())
    rendered = "".join(rendered_exc.format())
    return sanitize_text(rendered.removesuffix("\n"))


def _replace_database_texts(
    rendered: Any, exc: BaseException | None, seen: set[int],
) -> None:
    """Walk the rendering and the exception chain together; a database
    link's printed text becomes its summary."""
    while rendered is not None and exc is not None and id(rendered) not in seen:
        seen.add(id(rendered))
        summary = db_error_summary(exc)
        if summary is not None:
            rendered._str = summary
        for inner, inner_exc in zip(
            getattr(rendered, "exceptions", None) or [],
            getattr(exc, "exceptions", None) or [], strict=False,
        ):
            _replace_database_texts(inner, inner_exc, seen)
        if rendered.__cause__ is not None:
            rendered, exc = rendered.__cause__, exc.__cause__
        elif rendered.__context__ is not None:
            rendered, exc = rendered.__context__, exc.__context__
        else:
            rendered = None
