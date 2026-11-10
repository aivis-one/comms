# =============================================================================
# COMMS Service -- the request id of an HTTP request (W3-2, protocol 9.6)
# =============================================================================
#
# A product passes the X-Request-ID of its own request into every call of
# comms (deploy/INTEGRATION.md section 5, "The request id"). comms serves
# the request under it: every log line written meanwhile names it
# (app/core/logging.py REQUEST_ID), and the response carries it back in
# the same header.
#
# THE RULE (9.6): the header's value is kept when it is of the form
# [A-Za-z0-9._-]{1,128} (app/core/logging.py request_id_of); otherwise --
# absent, empty, too long, a space, a line break -- comms mints its own.
# Of several X-Request-ID headers the LAST is taken -- the one the nearest
# hop wrote -- and judged alone: a malformed last one is replaced, never
# an earlier one used instead.
#
# THE OUTERMOST LAYER OF comms' OWN (app/main.py adds it last): outside
# the service token (app/api/service_auth.py) and the body limit
# (app/api/body_limit.py), so the 401 of a missing token and the 413 of a
# body too large carry the id too, and so does the 500 of
# UnhandledErrorMiddleware (app/api/errors.py). Starlette's own
# ServerErrorMiddleware stays outside it: it answers only what every
# layer of comms let through, which UnhandledErrorMiddleware leaves to an
# exception after the response has started -- see the KNOWN CEILING below.
#
# KNOWN CEILING -- the server's own line after the request.
#   1. Mechanics: an exception raised after the response has started
#      leaves the application (UnhandledErrorMiddleware raises it on, it
#      cannot be answered any more); uvicorn writes "Exception in ASGI
#      application" once the application has returned (uvicorn
#      protocols/http run_asgi), after this layer has set the id back --
#      that line's request_id is null. The request's own lines before it
#      carry the id.
#   2. Status: acknowledged by design.
#   3. Backlog ref: none -- no route of comms streams a response, so an
#      exception after the start is not a path comms has today.
#   4. Promotion trigger (observable): an "Exception in ASGI application"
#      line of comms-app with request_id null.
#   5. Agreed fix shape: this layer writes the failure itself, inside the
#      request, as the model of protocol 9.4 does (primer-blank
#      primer_report/logs.py RequestIdMiddleware) -- and the server's own
#      line is no longer the only one.
#   6. Rejected: leaving the id set after the request -- uvicorn runs each
#      request in its own task, but a test client runs them in the
#      caller's, and the next request's lines outside its own scope would
#      name the previous one.
# =============================================================================

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.logging import request_id_of, serving

REQUEST_ID_HEADER = "X-Request-ID"

_HEADER_KEY = REQUEST_ID_HEADER.lower().encode("ascii")


def _last_request_id(scope: Scope) -> bytes | None:
    """The value of the last X-Request-ID header, or None."""
    last: bytes | None = None
    for name, value in scope.get("headers", ()):
        if name == _HEADER_KEY:
            last = value
    return last


class RequestId:
    """Serve every HTTP request under its request id; answer with it."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request_id = request_id_of(_last_request_id(scope))

        async def answered_with_it(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)[REQUEST_ID_HEADER] = request_id
            await send(message)

        with serving(request_id):
            await self.app(scope, receive, answered_with_it)
