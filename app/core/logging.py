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

import logging
import sys
import threading
from collections.abc import MutableMapping
from types import TracebackType
from typing import Any

import structlog

from app.core.config import settings
from app.core.sanitize import sanitize_text, sanitized_traceback

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


def _sanitized(value: Any) -> Any:
    if isinstance(value, str):
        return sanitize_text(value)
    if isinstance(value, dict):
        return {key: _sanitized(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return type(value)(_sanitized(item) for item in value)
    return value


def sanitize_event(
    logger: Any, method: str, event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """structlog processor: every string in the event, nested ones too,
    through the one sanitizer -- the rendered traceback included, so it
    runs after format_exc_info. CORRELATION_FIELDS pass whole."""
    for key, value in event_dict.items():
        if key not in CORRELATION_FIELDS:
            event_dict[key] = _sanitized(value)
    return event_dict


def processors(renderer: Any) -> list[Any]:
    """The structlog chain: the traceback rendered, then every string
    sanitized, then the line rendered."""
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
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
        return sanitize_text(self.inner.format(record))


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
    _sanitize_handlers()
    sys.excepthook = _excepthook
    threading.excepthook = _thread_excepthook
