# =============================================================================
# COMMS Service -- Structured Logging
# =============================================================================
#
# Ported from the cbshome backend (canonical base). structlog with two
# renderers:
#   production  -> the deploy machine's line (protocol 9.4, below)
#   development -> ConsoleRenderer (colored, human-readable)
#
# THE MACHINE'S LINE (W3-2, protocol 9.4): every line of every process --
# structlog's and the standard library's (uvicorn, sqlalchemy, asyncio),
# an exception nobody caught included -- is ONE JSON object of exactly
# LOG_FIELDS and, when the line has any, LOG_CTX:
#   ts          the moment, ISO 8601 in milliseconds, UTC, "Z";
#   level       debug | info | warning | error | critical;
#   service     "comms";
#   release     PRIMER_RELEASE, which the image carries (W1-1); without it
#               the process refuses to start (release_from_environment);
#   request_id  the id of the request or the stream entry being served
#               (REQUEST_ID, protocol 9.6), null outside both;
#   event       structlog: the event the call names (`comms_started`);
#               the standard library: the logger's name;
#   msg         structlog: the event again -- comms writes no sentence
#               beside it; the standard library: the message; and in
#               both the traceback, if any, after a line break -- JSON
#               escapes it, so a line is always one line;
#   ctx         every field of a structlog call, the ones bound to the
#               context too (an object of comms' own; absent when there
#               are none). Not cut at 8 KB: the machine cuts it.
# One function writes the line for both paths (_line).
#
# IDEMPOTENCY:
#   setup_logging() is safe to call multiple times (pytest calls it per
#   session). Structlog caches configuration on first use.
#
# RULE: NEVER use `import logging` in application code.
#       Always use structlog.get_logger().
#
# EVERY PATH TO THE LOG IS SANITIZED (H8): the structlog chain runs each
# string value of an event through the one sanitizer (app/core/sanitize.py)
# after the traceback is rendered and before the line is; the standard
# library's lines -- the root handler (sqlalchemy, asyncio, alembic) and
# the handlers uvicorn installs on its own loggers, which do not
# propagate -- pass through SanitizingFormatter; an exception nobody
# caught goes to the log through the same chain (sys.excepthook,
# threading.excepthook). The traceback stays, redacted -- it is how a
# failure is read. Values that ARE the thread between the product's
# logs and comms' (spec §6.6) are left whole: CORRELATION_FIELDS, and the
# request id, which is kept only in its form (request_id_of).
# =============================================================================

import copy
import json
import logging
import math
import numbers
import os
import re
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from types import TracebackType
from typing import Any

import structlog

from app.core.config import settings
from app.core.sanitize import (
    is_secret_name,
    sanitize_text,
    sanitized_traceback,
    unreadable_value,
)

_REDACTED = "[redacted]"

# The line (protocol 9.4): its fields in their order, and the free object.
LOG_FIELDS = ("ts", "level", "service", "release", "request_id", "event", "msg")
LOG_CTX = "ctx"
SERVICE = "comms"

# The release every line names: the image's PRIMER_RELEASE (W1-1).
RELEASE_VARIABLE = "PRIMER_RELEASE"


def release_from_environment() -> str:
    """PRIMER_RELEASE, or the process refuses to start naming it: a
    line without its release says nothing of which comms wrote it, and a
    default would say something false (§0 §4, a red start). Empty is
    absent. SystemExit with a string prints it to stderr and exits 1 --
    and inside the API's lifespan uvicorn writes it and exits 3."""
    release = os.environ.get(RELEASE_VARIABLE, "")
    if not release:
        raise SystemExit(
            f"comms: startup refused -- {RELEASE_VARIABLE} is not set: every "
            f"log line names the release (protocol 9.4). The image carries "
            f"it from its build argument (Dockerfile); a process started "
            f"outside the image sets it itself."
        )
    return release


# THE REQUEST ID (protocol 9.6): the id of what is being served -- an HTTP
# request (app/api/request_id.py) or a stream entry
# (app/transport/consumer.py) -- for the lines written meanwhile; None
# outside both, a null in the line. One variable for both paths of the
# log: structlog's renderer and SanitizingFormatter read it as they write.
REQUEST_ID: ContextVar[str | None] = ContextVar("comms_request_id", default=None)

# An incoming id is kept only in this form, whole: it goes into every line
# of what it names, so a break or a quote that could forge or end a line
# never does -- anything else is replaced, never trimmed or mended.
REQUEST_ID_FORM = re.compile(r"[A-Za-z0-9._-]{1,128}")


def request_id_of(candidate: str | bytes | None) -> str:
    """The id to serve under: `candidate` when it is of the form, else a
    fresh one (a uuid's 32 hex digits). Bytes are read as ASCII -- a
    byte outside it is not of the form either."""
    if isinstance(candidate, bytes):
        try:
            candidate = candidate.decode("ascii")
        except UnicodeDecodeError:
            candidate = None
    if candidate is not None and REQUEST_ID_FORM.fullmatch(candidate):
        return candidate
    return uuid.uuid4().hex


@contextmanager
def serving(request_id: str) -> Iterator[None]:
    """REQUEST_ID is `request_id` inside, what it was before after it."""
    token = REQUEST_ID.set(request_id)
    try:
        yield
    finally:
        REQUEST_ID.reset(token)


def _ts(created: float) -> str:
    return (
        datetime.fromtimestamp(created, UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _unjsonable(value: Any) -> str:
    """What JSON cannot take (a set, an object of a correlation field):
    its repr, sanitized."""
    return sanitize_text(_readable(repr, value))


def _finite(value: Any) -> Any:
    """`value` with every float that is not finite written as its text:
    NaN and Infinity are not JSON, and a line must parse anywhere."""
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    if isinstance(value, dict):
        return {key: _finite(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite(item) for item in value]
    return value


def _line(
    *, created: float, level: str, release: str, event: str, msg: str,
    ctx: Mapping[str, Any] | None,
) -> str:
    """THE line of protocol 9.4, for both paths. Never raises on a value
    of ctx: what JSON cannot take is its sanitized repr, a float that is
    not finite its text."""
    line: dict[str, Any] = {
        "ts": _ts(created),
        "level": level,
        "service": SERVICE,
        "release": release,
        "request_id": REQUEST_ID.get(),
        "event": event,
        "msg": msg,
    }
    if ctx:
        line[LOG_CTX] = dict(ctx)
    try:
        return json.dumps(line, default=_unjsonable, allow_nan=False)
    except ValueError:
        return json.dumps(_finite(line), default=_unjsonable, allow_nan=False)


# The keys of a structlog event that are not fields of the call: the
# event, its level and structlog's own timestamp (the line takes its own,
# _ts); and the traceback -- format_exc_info_by_the_rule's, or the one a
# call passes as `exception=sanitized_traceback(exc)` -- with the stack,
# both of which go into msg.
_TRACE_KEYS = ("exception", "stack")


class MachineRenderer:
    """structlog's last processor outside development: the event as the
    machine's line (_line). Runs after sanitize_event -- every value it
    takes is already through the one sanitizer."""

    def __init__(self) -> None:
        self.release = release_from_environment()

    def __call__(
        self, _logger: Any, method: str, event_dict: MutableMapping[str, Any],
    ) -> str:
        fields = dict(event_dict)
        event = str(fields.pop("event", ""))
        level = str(fields.pop("level", method))
        fields.pop("timestamp", None)
        msg = event
        for key in _TRACE_KEYS:
            trace = fields.pop(key, None)
            if trace is not None:
                msg = f"{msg}\n{trace}"
        return _line(
            created=time.time(), level=level, release=self.release,
            event=event, msg=msg, ctx=fields,
        )

# The fields of a log event NOT sanitized by value: the keys that join a
# product's own logs to comms' (spec §6.6 -- "one thread through both
# services"). They are identifiers the product or comms minted -- an
# idempotency key is often a 64-character hex digest, which the
# sanitizer's long-hex pattern would otherwise take for a secret and
# cut, and with it the thread. Nothing else is: the same hex inside an
# exception's text is redacted (tests/test_log_sanitizing.py).
CORRELATION_FIELDS = frozenset({
    "idempotency_key",
    "correlation",
    "notification_id",
    "delivery_id",
})


# The containers _sanitized walks, by EXACT type: a subclass (a
# namedtuple, whose constructor takes its fields one by one, a deque, a
# list subclass) is not rebuilt -- it goes to the log as its repr,
# sanitized, like any other value it cannot walk (SAN-1).
_WALKED = (list, tuple, set, frozenset)
_SCALARS = (bool, int, float)


# How deep _sanitized walks (REV-6): a value nested deeper is named, not
# walked -- the walk is recursion, and 2000 levels raised RecursionError
# out of the chain. The order of MAX_JSON_DEPTH (64), far from Python's
# own limit.
_MAX_LOG_DEPTH = 64


def _sanitized(
    value: Any, path: frozenset[int] = frozenset(), depth: int = 0,
) -> Any:
    """A value of a log event with every string in it through the one
    sanitizer. Never raises: what it cannot walk -- an object, a
    container it does not rebuild, a container that holds itself (`path`
    is the ids above this one; the same object twice side by side is
    walked twice, it is not a cycle) -- becomes sanitize_text(repr(v)),
    which is what the JSON renderer's fallback would print anyway; a
    value whose repr raises, or one nested past _MAX_LOG_DEPTH, becomes
    unreadable_value -- its type, nothing of it (REV-6). A pair -- a
    tuple or list of two whose first is a credential's name,
    ("x-api-key", "...") as headers come -- keeps the name and hides the
    value, as a field does (REV-6)."""
    if isinstance(value, str):
        return sanitize_text(value)
    if value is None or isinstance(value, _SCALARS):
        return value
    walked = isinstance(value, dict) or type(value) in _WALKED
    if not walked or id(value) in path:
        try:
            return sanitize_text(repr(value))
        except Exception:
            return unreadable_value(value)
    if depth >= _MAX_LOG_DEPTH:
        return unreadable_value(value)
    inner = path | {id(value)}
    if isinstance(value, dict):
        return {
            key: _field(key, item, inner, depth + 1)
            for key, item in value.items()
        }
    if (
        type(value) in (tuple, list) and len(value) == 2
        and isinstance(value[0], str) and is_secret_name(value[0])
    ):
        return type(value)((sanitize_text(value[0]), _REDACTED))
    return type(value)(_sanitized(item, inner, depth + 1) for item in value)


def _field(
    name: Any, value: Any, path: frozenset[int] = frozenset(), depth: int = 0,
) -> Any:
    """A named value -- a field of the event, a key of a dict in it: a
    credential's name hides its value whole, whatever its type
    (is_secret_name, REV-4b); any other is walked as above."""
    if isinstance(name, str) and is_secret_name(name):
        return _REDACTED
    return _sanitized(value, path, depth)


def sanitize_event(
    logger: Any, method: str, event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """structlog processor: every string in the event, nested ones too,
    through the one sanitizer -- the rendered traceback included, so it
    runs after format_exc_info. CORRELATION_FIELDS pass whole."""
    for key, value in event_dict.items():
        if key not in CORRELATION_FIELDS:
            event_dict[key] = _field(key, value)
    return event_dict


def format_exc_info_by_the_rule(
    _logger: Any, _method: str, event_dict: dict[str, Any],
) -> dict[str, Any]:
    """structlog's format_exc_info, with the traceback rendered by
    sanitized_traceback (DB-1): exc_info=True, an exception or a tuple."""
    exc_info = event_dict.pop("exc_info", None)
    if not exc_info:
        return event_dict
    if isinstance(exc_info, BaseException):
        exc: BaseException | None = exc_info
    elif isinstance(exc_info, tuple):
        exc = exc_info[1]
    else:
        exc = sys.exc_info()[1]
    if exc is not None:
        event_dict["exception"] = sanitized_traceback(exc)
    return event_dict


def processors(renderer: Any) -> list[Any]:
    """The structlog chain: the traceback rendered, then every string
    sanitized, then the line rendered."""
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        # DB-1: an exc_info from ANY logger renders by the one rule
        # (sanitized_traceback) -- never format_exc_info's raw chain.
        format_exc_info_by_the_rule,
        sanitize_event,
        renderer,
    ]


class SanitizingFormatter(logging.Formatter):
    """A standard-library formatter that writes the machine's line
    (_line): event the logger's name, msg the message -- its arguments
    laid in -- and the traceback logging appends, the whole of it through
    the one sanitizer. It replaces the formatter a handler had: uvicorn's
    lines take the machine's form too, their level a field of the line,
    not a prefix of the text. Reads the release as it is made, and
    refuses without one (release_from_environment)."""

    def __init__(self) -> None:
        super().__init__()
        self.release = release_from_environment()

    def format(self, record: logging.LogRecord) -> str:
        # DB-1: a library's traceback (uvicorn, SQLAlchemy's pool) renders
        # by the one rule -- a database error's text replaced by its
        # summary -- before the line is sanitized.
        if record.exc_info and record.exc_info[1] is not None:
            record.exc_text = sanitized_traceback(record.exc_info[1])
        # REV-6: the message and its arguments are DATA -- a provider's
        # text, a path a client sent -- and a line break or an escape
        # sequence in them would forge a line in msg or repaint the
        # terminal of whoever reads it. Escaped on a copy, before the
        # message is laid out (uvicorn's access line unpacks record.args
        # by position, and the strings keep their places); the traceback
        # appended (exc_text) is not data and stays as many lines as it
        # is -- inside msg, where JSON escapes them.
        # A message or an argument that is not a string (an exception, an
        # object) becomes text inside getMessage, by its str or repr --
        # escaped there too (REV-9): the message as getMessage would read
        # it, an argument wrapped so %s and %r print exactly what they
        # would, its controls escaped. A number stays a number: %d and %f
        # want one, and uvicorn passes its status as an int.
        shown = copy.copy(record)
        shown.msg = _escaped(
            record.msg if isinstance(record.msg, str)
            else _readable(str, record.msg)
        )
        if isinstance(record.args, tuple):
            shown.args = tuple(_argument(arg) for arg in record.args)
        elif isinstance(record.args, dict):
            shown.args = {key: _argument(arg) for key, arg in record.args.items()}
        msg = shown.getMessage()
        if record.exc_text:
            msg = f"{msg}\n{record.exc_text}"
        if record.stack_info:
            msg = f"{msg}\n{self.formatStack(record.stack_info)}"
        return _line(
            created=record.created, level=record.levelname.lower(),
            release=self.release, event=record.name, msg=sanitize_text(msg),
            ctx=None,
        )


# The characters a log line must not carry as they are: C0 controls but
# the tab, DEL, NEL and the Unicode line and paragraph separators.
_CONTROLS_RE = re.compile("[\x00-\x08\x0a-\x1f\x7f\x85\u2028\u2029]")


def _readable(read: Callable[[Any], str], value: Any) -> str:
    """read(value) -- str or repr -- or, when that raises, the value's
    type (unreadable_value): a log line never fails on its argument."""
    try:
        return read(value)
    except Exception:
        return unreadable_value(value)


class _Escaped:
    """An argument of a stdlib record that is not a string or a number:
    %s and %r print its str and repr, escaped (REV-9)."""

    __slots__ = ("_value",)

    def __init__(self, value: Any) -> None:
        self._value = value

    def __str__(self) -> str:
        return str(_escaped(_readable(str, self._value)))

    def __repr__(self) -> str:
        return str(_escaped(_readable(repr, self._value)))


def _argument(value: Any) -> Any:
    """A record's argument as the inner formatter should see it: a string
    escaped, a number as it is, anything else wrapped in _Escaped."""
    if isinstance(value, str):
        return _escaped(value)
    if isinstance(value, numbers.Number):
        return value
    return _Escaped(value)


def _escaped(value: Any) -> Any:
    """A string with every control _CONTROLS_RE names written as its
    escape (\\n, \\x1b, \\u2028); anything else as it is."""
    if not isinstance(value, str):
        return value
    return _CONTROLS_RE.sub(
        lambda m: m.group(0).encode("unicode_escape").decode("ascii"), value,
    )


def _sanitize_handlers() -> None:
    """Every handler logging knows of now -- the root's and those of the
    named loggers (uvicorn installs its own, before the app's lifespan
    runs: uvicorn _subprocess.subprocess_started configures logging,
    then the server starts the lifespan) -- gets SanitizingFormatter in
    place of the formatter it had."""
    loggers: list[logging.Logger] = [logging.getLogger()]
    loggers += [
        logger for logger in logging.root.manager.loggerDict.values()
        if isinstance(logger, logging.Logger)
    ]
    for logger in loggers:
        for handler in logger.handlers:
            if not isinstance(handler.formatter, SanitizingFormatter):
                handler.setFormatter(SanitizingFormatter())


# The HTTP client's own loggers write every request at INFO with its
# FULL address -- `HTTP Request: GET <url> "HTTP/1.1 200 OK"` (httpx
# _client.py). An address can be a secret: the signed link to a person's
# voice (P5-2, app/engine/formatters.py LLMFormatter._fetch_body), whose
# object path and credential scope the sanitizer cannot tell from a
# path. Held at WARNING whatever LOG_LEVEL says; comms writes its own
# line for each call it makes (email_accepted, llm_set_failed,
# llm_body_downloaded, ...), without the address.
HTTP_CLIENT_LOGGERS: tuple[str, ...] = ("httpx", "httpcore")


def _quiet_http_client_loggers() -> None:
    for name in HTTP_CLIENT_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


def _log_uncaught(
    kind: str, exc: BaseException | None, **fields: Any,
) -> None:
    structlog.get_logger().critical(
        kind,
        exception=sanitized_traceback(exc) if exc is not None else None,
        **fields,
    )


def _excepthook(
    exc_type: type[BaseException],
    exc: BaseException,
    tb: TracebackType | None,
) -> None:
    """An exception nobody caught in the main thread -- asyncio.run's
    included -- to the log, redacted, instead of raw to stderr."""
    _log_uncaught("unhandled_exception", exc.with_traceback(tb))


def _thread_excepthook(args: threading.ExceptHookArgs) -> None:
    value = args.exc_value
    if value is not None and args.exc_traceback is not None:
        value = value.with_traceback(args.exc_traceback)
    _log_uncaught(
        "unhandled_thread_exception", value,
        thread=args.thread.name if args.thread is not None else None,
    )


def setup_logging() -> None:
    """Configure structlog and the standard library. Safe to call
    multiple times. The release first: without PRIMER_RELEASE nothing is
    configured and the process refuses to start (release_from_environment).

    KNOWN CEILING (acknowledged by design -- H8 and protocol 9.4, before
    the setup):
      1. Mechanics: a line written before this runs in the worker or the
         consumer -- an import error of the app -- goes out through
         whatever logging the process had then: unsanitized and not in
         the machine's form (no JSON, no release, no request_id); so does
         a handler a library installs after this call. uvicorn's own
         lines are not here since W1-2: the API's command passes
         --log-config (app/core/log_config.json), and its supervisor's
         lines and each uvicorn worker's before the lifespan leave in the
         machine's form (app/core/log_launcher.py).
      2. Status: acknowledged by design.
      3. Backlog ref: none -- the line comes from code that runs before
         comms does (the interpreter importing the app); comms has no
         hook there short of owning the process launcher.
      4. Promotion trigger (observable): a secret in a comms container's
         log on a line that precedes the process's first structlog line
         (worker_started / consumer_started), or the machine refusing
         comms' log for a line that is not JSON.
      5. Agreed fix: a sitecustomize that installs the hooks before the
         app is imported.
      6. Rejected: turning tracebacks off (they are how a failure is
         read -- H8 forbids it); a second sanitizer at the launcher (one
         implementation, app/core/sanitize.py).
    """
    release_from_environment()
    level_name = settings.log_level.upper()
    level = getattr(logging, level_name, logging.INFO)

    if settings.is_dev:
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer(
            colors=True,
        )
    else:
        renderer = MachineRenderer()

    structlog.configure(
        processors=processors(renderer),
        context_class=dict,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Configure stdlib logging (uvicorn, sqlalchemy, alembic)
    # so their output respects the same level -- and is sanitized.
    logging.basicConfig(
        format="%(message)s",
        level=level,
        stream=sys.stdout,
    )
    _quiet_http_client_loggers()
    _sanitize_handlers()
    sys.excepthook = _excepthook
    threading.excepthook = _thread_excepthook
