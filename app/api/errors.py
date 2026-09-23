# =============================================================================
# COMMS Service -- API Error Mapping
# =============================================================================
#
# The engine and audience layers raise plain service exceptions
# (app/core/exceptions.py) -- they know nothing about HTTP. This
# module maps them to responses at the app level, so routers call
# service functions without try/except ceremony:
#
#   NotFoundError   -> 404  (entity absent OR owned by someone else --
#                            the service layer deliberately does not
#                            distinguish, and neither do we)
#   ValidationError -> 422  (matches FastAPI's own request-validation
#                            status, so the client sees ONE status for
#                            "your input is wrong" regardless of which
#                            layer caught it)
#   DBAPIError      -> 500  (the safety net, R-2 item 5 -- see below)
#
# The messages of the three service exceptions are returned verbatim in
# `detail`. That is safe because of WHERE they are written: every one is
# raised by our own code with a text built from the request's input and
# entity ids -- never from settings, and never from a provider's or a
# driver's error (the two cursor handlers quote the decode error of the
# caller's own cursor). Channel failures never reach this module: they
# end in the delivery record, and the redaction of their text in the
# record and the logs (app/engine/formatters.py, sanitize_text) is
# unrelated to what this module returns. The database's messages are
# NOT safe, which is exactly why the last handler does not pass its own
# through.
# =============================================================================

import structlog
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError

from app.core.exceptions import (
    AuthorizationError,
    NotFoundError,
    ValidationError,
)

logger = structlog.get_logger()

# What the caller is told when a database error reaches this level.
# FIXED TEXT, carrying nothing from the exception: a DBAPIError's
# string is the failed SQL plus its bound parameters -- table and
# column names, and whatever the caller sent us. /api is internal, but
# "internal" is a network fact, not a reason to hand a product our
# schema and its own payload back in an error body.
_DB_FAILURE_DETAIL = (
    "The service could not complete the request. "
    "The failure has been logged."
)


def register_error_handlers(app: FastAPI) -> None:
    """Attach service-exception -> HTTP-response mapping to the app."""

    @app.exception_handler(NotFoundError)
    async def _not_found(
        request: Request, exc: NotFoundError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(exc)},
        )

    @app.exception_handler(AuthorizationError)
    async def _forbidden(
        request: Request, exc: AuthorizationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": str(exc)},
        )

    @app.exception_handler(ValidationError)
    async def _validation(
        request: Request, exc: ValidationError
    ) -> JSONResponse:
        # Literal 422: starlette renamed the constant
        # (HTTP_422_UNPROCESSABLE_ENTITY -> _CONTENT) mid-flight within
        # our fastapi pin range -- either spelling warns or breaks on
        # one end of the range; the number does neither.
        return JSONResponse(
            status_code=422,
            content={"detail": str(exc)},
        )

    @app.exception_handler(DBAPIError)
    async def _database_failure(
        request: Request, exc: DBAPIError
    ) -> JSONResponse:
        """A database error that reached the edge: 500, and loud.

        A SAFETY NET, NOT A SUBSTITUTE FOR BOUNDS. Every input this
        release could name is bounded in its model, where an oversized
        value is a 422 the caller can act on. Whatever still arrives
        here is by definition something we did not foresee, so it is
        OUR defect, not the caller's -- which is why it is a 500 and
        not a 422: a 422 would send a product to fix input that may be
        perfectly correct.

        AND IT IS LOGGED WITH THE TRACEBACK. A net that swallows
        quietly is worse than no net: the response would be clean, the
        service would look healthy, and the defect would be invisible
        until someone happened to reproduce it. exc_info carries the
        original error -- SQL, parameters and all -- to the one place
        that is ours, while the body carries none of it.
        """
        logger.error(
            "database_error_at_api_edge",
            path=request.url.path,
            method=request.method,
            error_type=type(exc).__name__,
            exc_info=exc,
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": _DB_FAILURE_DETAIL},
        )
