# =============================================================================
# COMMS Service -- the service token, asked before anything else (REV-3)
# =============================================================================
#
# Every request but /health and /ready presents the service token
# (app/api/deps.py require_service_auth) BEFORE comms reads anything of
# it: before routing, before the body is read, counted
# (app/api/body_limit.py) or parsed, and before any check of the query
# or the body. Without the right token the answer is the same 401 for
# every path, method and body -- an unknown path or a wrong method is
# not a 404 / 405, so nothing outside says which routes exist.
#
# WHY A MIDDLEWARE AND NOT A DEPENDENCY: FastAPI parses a route's JSON
# body before it solves any dependency (fastapi/routing.py
# get_request_handler) -- a tokenless body was answered 422 (malformed,
# or a NUL), 400 (nested too deep) or even 500 before the token was
# looked at. A middleware runs before routing, so before all of that.
# It is added last in app/main.py, the outermost; against the body
# limit (app/api/body_limit.py) the order is a choice, not a guard: that
# one refuses only while a body is read, and this never reads it.
#
# THE ANSWER IS BUILT HERE: a user middleware sits outside Starlette's
# ExceptionMiddleware, so an HTTPException raised here would reach no
# handler and become a 500. The refusal goes out through the one error
# body (app/api/errors.py error_response), the same 401 as before:
# class `unauthorized`, the constant message, `WWW-Authenticate: Bearer`.
#
# OPEN BY NAME: exactly /health and /ready -- the installer's and
# docker's probes carry no secret. A path is open by being equal to one
# of them, never by a prefix.
# =============================================================================

from fastapi import HTTPException
from starlette.types import ASGIApp, Receive, Scope, Send

from app.api.deps import require_service_auth
from app.api.errors import ErrorClass, error_response

OPEN_PATHS = frozenset({"/health", "/ready"})


def _authorization(scope: Scope) -> str | None:
    """The first Authorization header, as Header(str) read it before."""
    for name, value in scope.get("headers", ()):
        if name == b"authorization":
            text: str = value.decode("latin-1")
            return text
    return None


class RequireServiceToken:
    """Refuse every HTTP request without the service token, first."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] in OPEN_PATHS:
            await self.app(scope, receive, send)
            return
        try:
            await require_service_auth(_authorization(scope))
        except HTTPException as refusal:
            response = error_response(
                refusal.status_code,
                ErrorClass.UNAUTHORIZED,
                str(refusal.detail),
                headers=dict(refusal.headers or {}),
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)
