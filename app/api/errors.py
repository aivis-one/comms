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
#
# Exception messages are written by our own service layer and carry no
# secrets (credentials are sanitized at the formatter boundary), so
# they are safe to return verbatim in `detail`.
# =============================================================================

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.core.exceptions import NotFoundError, ValidationError


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
