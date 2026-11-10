# =============================================================================
# COMMS Service -- Structured Logging
# =============================================================================
#
# Ported as-is from the cbshome backend (canonical base). structlog with
# two renderers:
#   production  -> JSON (one object per line, easy to pipe to ELK/Datadog)
#   development -> ConsoleRenderer (colored, human-readable)
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
# logs and comms' (spec §6.6) are left whole: CORRELATION_FIELDS.
# =============================================================================

import copy
import logging
import numbers
import re
import sys
import threading
from collections.abc import Callable, MutableMapping
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
    """A standard-library formatter whose whole line -- message,
    arguments and the traceback logging appends -- goes through the one
    sanitizer. Wraps the formatter a handler already had, so uvicorn's
    lines keep their shape."""

    def __init__(self, inner: logging.Formatter | None = None) -> None:
        super().__init__()
        self.inner = inner or logging.Formatter("%(message)s")

    def format(self, record: logging.LogRecord) -> str:
        # DB-1: a library's traceback (uvicorn, SQLAlchemy's pool) renders
        # by the one rule -- a database error's text replaced by its
        # summary -- before the line is sanitized.
        if record.exc_info and record.exc_info[1] is not None:
            record.exc_text = sanitized_traceback(record.exc_info[1])
        # REV-6: the message and its arguments are DATA -- a provider's
        # text, a path a client sent -- and a line break or an escape
        # sequence in them would forge a line or repaint the terminal.
        # Escaped on a copy, before the inner formatter lays the line out
        # (uvicorn's access formatter unpacks record.args by position,
        # and the strings keep their places); the traceback it appends
        # (exc_text) is not data and stays as many lines as it is.
        # A message or an argument that is not a string (an exception, an
        # object) becomes text inside the inner formatter, by its str or
        # repr -- escaped there too (REV-9): the message as getMessage
        # would read it, an argument wrapped so %s and %r print exactly
        # what they would, its controls escaped. A number stays a number:
        # %d and %f want one, and uvicorn reads its status as an int.
        shown = copy.copy(record)
        shown.msg = _escaped(
            record.msg if isinstance(record.msg, str)
            else _readable(str, record.msg)
        )
        if isinstance(record.args, tuple):
            shown.args = tuple(_argument(arg) for arg in record.args)
        elif isinstance(record.args, dict):
            shown.args = {key: _argument(arg) for key, arg in record.args.items()}
        return sanitize_text(self.inner.format(shown))


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
    then the server starts the lifespan) -- gets SanitizingFormatter."""
    loggers: list[logging.Logger] = [logging.getLogger()]
    loggers += [
        logger for logger in logging.root.manager.loggerDict.values()
        if isinstance(logger, logging.Logger)
    ]
    for logger in loggers:
        for handler in logger.handlers:
            if not isinstance(handler.formatter, SanitizingFormatter):
                handler.setFormatter(SanitizingFormatter(handler.formatter))


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
    multiple times.

    KNOWN CEILING (acknowledged by design -- H8, before the setup):
      1. Mechanics: a line written before this runs -- an import error
         of the app, uvicorn's own parent (supervisor) process, which
         never runs the app's lifespan -- goes out through whatever
         logging the process had then, unsanitized; so does a handler a
         library installs after this call.
      2. Status: acknowledged by design.
      3. Backlog ref: none -- the line comes from code that runs before
         comms does (the interpreter, uvicorn's supervisor); comms has
         no hook there short of owning the process launcher.
      4. Promotion trigger (observable): a secret in a comms container's
         log on a line that precedes the process's first structlog line
         (worker_started / consumer_started / the lifespan's first).
      5. Agreed fix: none agreed; the candidate is a log config file
         passed to uvicorn (--log-config) whose formatter is
         SanitizingFormatter, and a sitecustomize that installs the
         hooks before the app is imported.
      6. Rejected: turning tracebacks off (they are how a failure is
         read -- H8 forbids it); a second sanitizer at the launcher (one
         implementation, app/core/sanitize.py).
    """
    level_name = settings.log_level.upper()
    level = getattr(logging, level_name, logging.INFO)

    if settings.is_dev:
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer(
            colors=True,
        )
    else:
        renderer = structlog.processors.JSONRenderer()

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
