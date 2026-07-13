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
# =============================================================================

import logging
import sys

import structlog

from app.core.config import settings


def setup_logging() -> None:
    """Configure structlog. Safe to call multiple times."""
    level_name = settings.log_level.upper()
    level = getattr(logging, level_name, logging.INFO)

    # Processors shared between development and production renderers.
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if settings.is_dev:
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer(
            colors=True,
        )
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            renderer,
        ],
        context_class=dict,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Configure stdlib logging (uvicorn, sqlalchemy, alembic)
    # so their output respects the same level.
    logging.basicConfig(
        format="%(message)s",
        level=level,
        stream=sys.stdout,
    )
