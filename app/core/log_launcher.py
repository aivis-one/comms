# =============================================================================
# COMMS Service -- uvicorn's own lines in the machine's form (W1-2)
# =============================================================================
#
# app/core/log_config.json, passed to uvicorn by the API's command
# (Dockerfile CMD, --log-config), names LauncherFormatter for uvicorn's
# loggers: its supervisor under --workers, which never runs the app's
# lifespan, and each worker's lines before the lifespan ("Started server
# process", "Waiting for application startup.") leave as the line of
# protocol 9.4 -- app/core/logging.py SanitizingFormatter, the one
# formatter and the one sanitizer.
#
# WHY THIS MODULE AND NOT SanitizingFormatter IN THE CONFIG: uvicorn
# configures logging in every worker BEFORE it starts the thread that
# answers its supervisor's healthcheck (uvicorn _subprocess
# subprocess_started, then supervisors/multiprocess.py Process.target).
# A formatter class in the config is imported there, and
# app.core.logging brings the settings, pydantic and structlog with it --
# seconds on a cold start (measured: a worker of the sandbox killed as
# hung by the 5 s healthcheck, and started again). This module imports
# nothing of comms; SanitizingFormatter is made at the first line, after
# the healthcheck answers -- the same order as without a log config.
#
# A process without PRIMER_RELEASE refuses at that first line
# (release_from_environment): SystemExit is no Exception, so logging's
# handleError does not swallow it, and the process exits naming the
# variable -- a red start, as every other entry point.
# =============================================================================

import logging


class LauncherFormatter(logging.Formatter):
    """SanitizingFormatter, made at the first record it formats."""

    def __init__(self) -> None:
        super().__init__()
        self._formatter: logging.Formatter | None = None

    def format(self, record: logging.LogRecord) -> str:
        if self._formatter is None:
            from app.core.logging import SanitizingFormatter

            self._formatter = SanitizingFormatter()
        return self._formatter.format(record)
