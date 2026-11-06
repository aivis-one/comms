# =============================================================================
# COMMS Service -- The size of a REST request's body (PERF-3)
# =============================================================================
#
# Nothing in front of comms bounds a body: the products call it on the
# internal network directly (protocol 4.1, 4.4), no proxy's max_body
# stands between. A body of 5 MB reached the model, and was parsed twice
# (app/api/deps.py refuse_unstorable_body, then FastAPI). Here comms
# bounds it itself, at MAX_REQUEST_BODY_BYTES -- above the largest body
# its own field limits accept (app/core/constants.py).
#
# HOW. A pure ASGI layer wraps `receive`: a declared Content-Length over
# the limit fails the first read of the body; a body without one (chunked) is
# counted as it streams and fails on the chunk that crosses the limit. The
# failure is an HTTPException(413) raised INSIDE the read of the body --
# within the application's ExceptionMiddleware, so the answer is comms'
# error body: 413, class `bad_request` (app/api/errors.py, the contract's
# table). No byte past the limit is held, and neither the model nor
# refuse_unstorable_body gets to parse what was refused. A request that
# never reads its body (a GET) is never touched. A declared length over
# the limit is refused at the first chunk the transport hands over --
# one chunk is the most that is held.
# =============================================================================

from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.constants import MAX_REQUEST_BODY_BYTES

_TOO_LARGE = (
    f"the request body is larger than {MAX_REQUEST_BODY_BYTES} bytes"
)


def _too_large() -> HTTPException:
    """A new object every time (API-2: no shared exception instance)."""
    return HTTPException(status_code=413, detail=_TOO_LARGE)


def _declared_length(scope: Scope) -> int | None:
    for name, value in scope.get("headers", ()):
        if name == b"content-length":
            try:
                return int(value)
            except ValueError:
                return None
    return None


class LimitRequestBody:
    """Bound every HTTP request's body at MAX_REQUEST_BODY_BYTES."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        declared = _declared_length(scope)
        seen = 0

        async def bounded() -> Message:
            nonlocal seen
            message = await receive()
            if message["type"] == "http.request":
                # Only a read of the BODY is refused: a response that
                # listens for the client's disconnect reads other
                # messages through the same receive.
                if declared is not None and declared > MAX_REQUEST_BODY_BYTES:
                    raise _too_large()
                seen += len(message.get("body", b""))
                if seen > MAX_REQUEST_BODY_BYTES:
                    raise _too_large()
            return message

        await self.app(scope, bounded, send)
