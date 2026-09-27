# =============================================================================
# COMMS Service -- API Error Mapping: ONE form of refusal (F1.4)
# =============================================================================
#
# Every error response of every route has ONE body (spec §10.10):
#
#     {"error": {"class": "<ErrorClass>", "message": "<text>",
#                "fields": [{"loc": [...], "message": "..."}]}}
#
# `class` is an enumeration a program reads -- like a delivery's failure
# class, never a text to parse. `fields` appears only on request
# validation, naming the offending inputs. The mapping:
#
#   ValidationError, request validation  -> 422  validation
#   the service token (app/api/deps.py)  -> 401  unauthorized
#   AuthorizationError                   -> 403  forbidden
#   NotFoundError, an unknown route      -> 404  not_found
#   a known route, another method        -> 405  method_not_allowed
#   ConflictError (by subclass)          -> 409  conflict | stale_snapshot
#                                                | recipient_deleted
#   DBAPIError                           -> 500  internal
#
# Starlette's own HTTPException (401 from the token dependency, the
# router's 404 / 405) is mapped here too, so no response leaves the
# service in the framework's default {"detail": ...} shape.
#
# The messages of the service exceptions are returned verbatim. That is
# safe because of WHERE they are written: every one is raised by our own
# code with a text built from the request's input and entity ids --
# never from settings, and never from a provider's or a driver's error
# (the cursor refusal quotes the decode error of the caller's own
# cursor). Channel failures never reach this module: they end in the
# delivery record, and the redaction of their text in the record and
# the logs (app/engine/formatters.py, sanitize_text) is unrelated to
# what this module returns. The database's messages are NOT safe, which
# is exactly why the internal handler does not pass its own through.
# =============================================================================

from enum import StrEnum
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.exceptions import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    ValidationError,
    conflict_class,
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


class ErrorClass(StrEnum):
    """The class of a refusal -- what a program branches on."""

    VALIDATION = "validation"
    UNAUTHORIZED = "unauthorized"
    FORBIDDEN = "forbidden"
    NOT_FOUND = "not_found"
    METHOD_NOT_ALLOWED = "method_not_allowed"
    CONFLICT = "conflict"
    STALE_SNAPSHOT = "stale_snapshot"
    RECIPIENT_DELETED = "recipient_deleted"
    INTERNAL = "internal"


_HTTP_CLASSES = {
    401: ErrorClass.UNAUTHORIZED,
    403: ErrorClass.FORBIDDEN,
    404: ErrorClass.NOT_FOUND,
    405: ErrorClass.METHOD_NOT_ALLOWED,
    422: ErrorClass.VALIDATION,
}


def error_response(
    status_code: int,
    error_class: str,
    message: str,
    *,
    fields: list[dict[str, Any]] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """THE error body. Every handler below returns through here."""
    error: dict[str, Any] = {"class": error_class, "message": message}
    if fields is not None:
        error["fields"] = fields
    return JSONResponse(
        status_code=status_code, content={"error": error}, headers=headers,
    )


def register_error_handlers(app: FastAPI) -> None:
    """Attach every refusal source to the one error body."""

    @app.exception_handler(NotFoundError)
    async def _not_found(request: Request, exc: NotFoundError) -> JSONResponse:
        return error_response(404, ErrorClass.NOT_FOUND, str(exc))

    @app.exception_handler(AuthorizationError)
    async def _forbidden(
        request: Request, exc: AuthorizationError
    ) -> JSONResponse:
        return error_response(403, ErrorClass.FORBIDDEN, str(exc))

    @app.exception_handler(ValidationError)
    async def _validation(
        request: Request, exc: ValidationError
    ) -> JSONResponse:
        # Literal 422: starlette renamed the constant mid-flight within
        # our fastapi pin range; the number does not move.
        return error_response(422, ErrorClass.VALIDATION, str(exc))

    @app.exception_handler(ConflictError)
    async def _conflict(request: Request, exc: ConflictError) -> JSONResponse:
        return error_response(409, conflict_class(exc), str(exc))

    @app.exception_handler(RequestValidationError)
    async def _request_validation(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """The request did not match its model: name each input."""
        fields = [
            {"loc": list(err.get("loc", ())), "message": str(err.get("msg", ""))}
            for err in exc.errors()
        ]
        return error_response(
            422, ErrorClass.VALIDATION, "the request is invalid", fields=fields,
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        """The token dependency's 401 and the router's 404 / 405."""
        error_class = _HTTP_CLASSES.get(exc.status_code, ErrorClass.INTERNAL)
        return error_response(
            exc.status_code, error_class, str(exc.detail),
            headers=getattr(exc, "headers", None),
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
        return error_response(500, ErrorClass.INTERNAL, _DB_FAILURE_DETAIL)
