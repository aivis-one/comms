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

# Secret-redaction patterns (review 1.1, widened in F0-comms item 2).
#
# THE LIST IS DRIVEN BY THE SECRETS THIS SERVICE ACTUALLY HOLDS, in the
# form each takes where it can surface in an error text -- not by what a
# secret "usually" looks like. The secrets (app/core/config.py) and the
# form each is checked in:
#   DATABASE_URL        postgresql+asyncpg://comms:<pw>@host/db   userinfo
#   REDIS_URL           redis://:<pw>@host:6379/0 (EMPTY user --
#                       the form deploy/comms-deploy.sh writes)    userinfo
#   COMMS_SERVICE_TOKEN Authorization: Bearer <t>; bare hex         header,
#                                                                  bearer, hex
#   TELEGRAM_BOT_TOKEN  .../bot<id>:<secret>/sendMessage inside an
#                       aiohttp error text (aiogram wraps it as
#                       "<ClientError class>: <error>")            telegram
#   EMAIL_MAILGUN_API_KEY bare <32hex>-<8hex>-<8hex> or key-<32hex>;
#                       Authorization: Basic <b64 of api:key>     mailgun,
#                                                                  header
#   LLM_KEY_SETS keys   bare sk-<key chars>; Authorization: Bearer   llm,
#                       <key> (the adapter's header)               bearer
# Generated passwords and the service token (openssl rand -hex 24/32 in
# comms-deploy.sh) are also caught BARE by the long-hex pattern.
#
# ORDER MATTERS, and every pattern is IDEMPOTENT on its own output --
# the email path sanitizes the provider's text where it enters, and the
# service layer sanitizes the exception again when it writes the
# record, so every text goes through this twice:
#   1. the Authorization header, scheme included -- before bearer, so
#      "Authorization: Basic x" loses its credentials and not just the
#      word "Basic";
#   2. bearer on its own (a token with no header name in front);
#   3. userinfo in a URL/DSN, EMPTY user allowed, and the password runs
#      to the LAST @ of the authority (a raw @ in a password must not
#      leave its tail behind);
#   4. token-shaped values with no keyword next to them;
#   5. key=value / key: value for credential keywords.
_AUTH_HEADER_RE = re.compile(
    r"(?i)\b(authorization)\b\s*[=:]\s*(?:(?:bearer|basic|digest|token)\s+)?\S+"
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+\S+")
_URL_USERINFO_RE = re.compile(r"(://[^/\s:@]*:)[^/\s]*(@)")
_TELEGRAM_TOKEN_RE = re.compile(r"\b(?:bot)?\d{6,}:[A-Za-z0-9_-]{30,}")
_MAILGUN_KEY_RE = re.compile(
    r"(?i)\b(?:key-[0-9a-f]{32}|[0-9a-f]{32}-[0-9a-f]{8}-[0-9a-f]{8})\b"
)
# The key shape of OpenAI-compatible providers and gateways (P5-1, the
# llm channel's LLM_KEY_SETS): `sk-` and then a long run of key
# characters (`sk-or-v1-<hex>`, `sk-proj-...`). A vendor key of another
# shape is the KNOWN CEILING below.
_LLM_KEY_RE = re.compile(r"\bsk-[A-Za-z0-9_-]{20,}")
# KNOWN CEILING -- bare secrets the operator chose by hand.
#   1. Mechanics: this pattern catches a secret with no keyword, header
#      or URL around it ONLY in the shape comms-deploy.sh mints (48+ hex
#      characters). A password, token or key the operator typed in
#      another shape, surfacing bare in an error text, passes through
#      every pattern here unchanged.
#   2. Status: acknowledged by design.
#   3. Task: none -- no secret outside the minted shapes exists in any
#      deploy we know of, and the shape of a hand-typed value cannot be
#      recognised by any pattern without also eating ordinary text.
#   4. Unfreeze trigger: a secret appears in a deploy's configuration
#      that was NOT written by comms-deploy.sh generate_env and is not
#      one of the provider credentials matched above.
#   5. Agreed fix: redact by VALUE, not by shape -- read the configured
#      secrets from settings once and replace their literal occurrences.
#   6. Rejected: widening the shape patterns to "any long token" (eats
#      message ids, digests and ordinary identifiers, and still misses a
#      short password); refusing non-hex secrets at startup (not ours to
#      demand of a provider credential, and a red start on a valid
#      configuration).
_LONG_HEX_RE = re.compile(r"\b[0-9a-fA-F]{48,}\b")
_KEYVAL_RE = re.compile(
    r"(?i)\b(password|passwd|pwd|token|secret|api[_-]?key|apikey"
    r"|authorization)\b\s*[=:]\s*\S+"
)


def sanitize_text(text: str) -> str:
    """Remove every secret form listed above from a text. Not truncated.

    Idempotent: sanitize_text(sanitize_text(x)) == sanitize_text(x).
    Callers cut AFTER calling this, never before: a cut through the
    middle of a secret can leave a prefix no pattern recognises. The
    one gap in coverage is the KNOWN CEILING on _LONG_HEX_RE.
    """
    text = _AUTH_HEADER_RE.sub(lambda m: f"{m.group(1)}=[redacted]", text)
    text = _BEARER_RE.sub("bearer [redacted]", text)
    text = _URL_USERINFO_RE.sub(r"\1[redacted]\2", text)
    text = _TELEGRAM_TOKEN_RE.sub("[redacted]", text)
    text = _MAILGUN_KEY_RE.sub("[redacted]", text)
    text = _LLM_KEY_RE.sub("[redacted]", text)
    text = _LONG_HEX_RE.sub("[redacted]", text)
    return _KEYVAL_RE.sub(lambda m: f"{m.group(1)}=[redacted]", text)


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
    content (spec §6.4) -- nor does a dead letter
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
    DataError for a parameter of the wrong type quoting the value)."""
    from asyncpg.exceptions import InterfaceError, PostgresError
    from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
    from sqlalchemy.exc import StatementError

    return (StatementError, AsyncAdapt_asyncpg_dbapi.Error, PostgresError,
            InterfaceError)


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
    return sanitize_text(str(exc))[:2000]


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
