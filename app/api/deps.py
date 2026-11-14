# =============================================================================
# COMMS Service -- API Dependencies (service-to-service auth; one query)
# =============================================================================
#
# Phase 3b item 1. The comms API is internal (arch decision 14): its
# only caller is the product backend, which presents the shared secret
# as "Authorization: Bearer <token>". require_service_auth is the check;
# it is asked by the outermost middleware (app/api/service_auth.py,
# REV-3) before anything of the request is read -- not as a dependency:
# FastAPI parses a JSON body before it solves any dependency.
#
# THE TRUST MODEL -- the service token is the ONLY thing comms checks.
# Every actor named in a request (participant, operator, sender,
# is_supervisor) is taken on trust from the product's proxy, which must
# derive them from its own session. Written down, with whose
# responsibility each part is, in ONE place: deploy/INTEGRATION.md,
# "What comms takes on trust". This module is where that model stops.
#
# TWO TOKENS (W2-1b, Primer protocol 5.4): COMMS_SERVICE_TOKEN and,
# while a key is being changed, COMMS_SERVICE_TOKEN_NEXT. The machine
# hands comms the next token beside the current one, moves the product
# onto it, then takes the old one away -- a request passes with either
# token in force (app/core/config.py Settings.service_tokens, the one
# place that knows them).
#
# SECRET HANDLING:
#   - comparison is constant-time (secrets.compare_digest) -- an
#     internal API on an isolated network hardly invites timing
#     attacks, but the correct primitive costs nothing;
#   - the presented token is compared with EVERY token in force, never
#     stopping at the first match: the answer's timing does not say
#     which of the two matched, nor whether one did before the other;
#   - nothing says, in a log or an answer, which token a request
#     passed with;
#   - the token NEVER appears in logs or error bodies (the same
#     principle as the formatter's secret sanitizer): the 401 detail
#     is a constant string, nothing from the request is echoed back;
#   - an auth failure is logged WITHOUT the presented value.
#
# EMPTY TOKEN: unreachable here -- startup validation in
#   app/core/config.py refuses to boot without the token (LEG-1: comms
#   has one mode, and in it the API is always closed).
#
# A REPEATED QUERY PARAMETER IS REFUSED (G1), on every route
# (refuse_repeated_query, an application-level dependency in
# app/main.py). FastAPI reads a scalar parameter given twice --
# ?limit=5&limit=500 -- as its LAST value, silently: the request that
# runs is not the one that was sent, the class app/api/paging.py
# refuses for a limit out of bounds. No route of comms takes a list in
# its query, so a key given twice is never meaningful: 422, class
# `validation`, naming the key.
#
# A STRING THE DATABASE CANNOT STORE is refused at the door it comes
# through, by the one rule of app/core/storable.py: in a JSON body
# (refuse_unstorable_body, API-2) and in the query string
# (refuse_unstorable_query, TIME-1) -- both application-level
# dependencies in app/main.py.
# =============================================================================

import json
import secrets
from collections import Counter
from typing import Annotated, Any

import structlog
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db_session
from app.core.exceptions import ValidationError
from app.core.storable import UNSTORABLE, holds_unstorable, storable

logger = structlog.get_logger()

def _unauthorized() -> HTTPException:
    """The 401, a NEW object for every refusal (API-1).

    Constant payload: nothing from the request is reflected. Not one
    module-level instance: every `raise` of the same exception object
    appends the raising frames to its __traceback__, so a shared
    instance carried the frames of every refused request -- and their
    locals -- for the life of the process, growing with each refusal.
    """
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing service token",
        headers={"WWW-Authenticate": "Bearer"},
    )


# The scheme name is compared without regard to case (RFC 9110 §11.1);
# exactly one space separates it from the token, as before.
_BEARER_SCHEME = "bearer"


async def require_service_auth(authorization: str | None) -> None:
    """Verify the service-to-service bearer token.

    Raises HTTP 401 when the Authorization header is missing, is not
    a Bearer scheme (in any case: `bearer`, `BEARER`), or carries a
    token that matches none of the tokens in force -- COMMS_SERVICE_TOKEN
    and, when set, COMMS_SERVICE_TOKEN_NEXT. Never a pass-through: with
    no token in force (a state startup refuses) nothing matches. Asked
    by app/api/service_auth.py RequireServiceToken for every request.
    """
    scheme, separator, presented = (authorization or "").partition(" ")
    if not separator or scheme.lower() != _BEARER_SCHEME:
        logger.warning("service_auth_failed", reason="missing_or_not_bearer")
        raise _unauthorized()

    offered = presented.encode("utf-8")
    matched = False
    for expected in settings.service_tokens:
        # Every token is compared, a match or not: `|=`, never `or`.
        matched |= secrets.compare_digest(offered, expected.encode("utf-8"))
    if not matched:
        # Do NOT log the presented value -- a near-miss token is still
        # a secret (e.g. the prod token hitting the test deploy).
        logger.warning("service_auth_failed", reason="token_mismatch")
        raise _unauthorized()


async def refuse_repeated_query(request: Request) -> None:
    """Refuse a request whose query names a key more than once."""
    counts = Counter(key for key, _ in request.query_params.multi_items())
    repeated = sorted(key for key, count in counts.items() if count > 1)
    if repeated:
        raise ValidationError(
            f"query parameter given more than once: {', '.join(repeated)}; "
            f"each is read once -- send it once"
        )


async def refuse_unstorable_query(request: Request) -> None:
    """Refuse a query string holding a string the database cannot store.

    TIME-1: a NUL in a query parameter that a route reads as a string --
    the job's key on the reads by key -- reached the SELECT and came back
    as a 500 from the driver; in a cursor it was dropped by the base64
    decoder and the cursor read as another. It is the caller's input: a
    422 `validation` here, before any route runs, by the rule of the
    bodies and the stream (app/core/storable.py), naming the parameter,
    never the value. Names are held to it as well, as a body's keys are.

    One check for every route, as refuse_repeated_query is. Only the NUL
    can arrive: the query string is percent-decoded with
    errors="replace", so the bytes of a lone surrogate come in as U+FFFD,
    which the database stores; the rule is the one rule all the same.
    """
    for name, value in request.query_params.multi_items():
        if not storable(name):
            raise ValidationError(f"a query parameter name is {UNSTORABLE}")
        if not storable(value):
            raise ValidationError(f"query parameter {name!r} is {UNSTORABLE}")


async def refuse_unstorable_body(request: Request) -> None:
    """Refuse a JSON body holding a string the database cannot store.

    API-2: a NUL or a lone surrogate in any string of a REST body -- a
    value or a key, at any depth -- reached the INSERT and came back as a
    500 from the driver. It is the caller's input, so it is a 422
    `validation` here, before any model or route runs, by the one rule
    the stream uses (app/core/storable.py). The refusal names the
    top-level field, never the value.

    One check for every route, as refuse_repeated_query is: a route
    added later is covered without being named. A body that is not JSON
    is left alone -- the framework refuses it (422, or 400 for one it
    cannot decode); one nested past the parser's depth is a 422 here.
    The body is read once by Starlette and cached; the route's model
    reads the same bytes.
    """
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return
    raw = await request.body()
    if not raw:
        return
    try:
        data: Any = json.loads(raw)
    except ValueError:
        return
    except RecursionError:
        # Nested past the parser's depth (REV-3): the caller's input, not
        # comms' failure. Reached on a route with no body model -- on one
        # with a model FastAPI has already failed to parse it (its 400).
        raise ValidationError(
            "the body is nested deeper than its JSON can be read"
        ) from None
    if not isinstance(data, dict):
        if holds_unstorable(data):
            raise ValidationError(f"the body holds {UNSTORABLE}")
        return
    for name, value in data.items():
        if not storable(name):
            raise ValidationError(f"a field name is {UNSTORABLE}")
        if holds_unstorable(value):
            raise ValidationError(f"field {name!r} is {UNSTORABLE}")


# THE WRITE SESSION OF A ROUTE (TX-1) -- the one place the transaction's
# boundary is set. scope="function": the session's commit (get_db_session,
# after its yield) runs before the response is sent, so a product never
# holds a success whose row was not committed, and a commit that fails is
# the answer (app/api/errors.py). Every route that writes takes its
# session as `session: DbSession`; a guard holds it so
# (tests/test_commit_before_answer.py).
DbSession = Annotated[AsyncSession, Depends(get_db_session, scope="function")]
