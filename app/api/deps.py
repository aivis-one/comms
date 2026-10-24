# =============================================================================
# COMMS Service -- API Dependencies (service-to-service auth; one query)
# =============================================================================
#
# Phase 3b item 1. The comms API is internal (arch decision 14): its
# only caller is the product backend, which presents the shared secret
# as "Authorization: Bearer <token>". This dependency guards every
# /api/v1 router.
#
# THE TRUST MODEL -- the service token is the ONLY thing comms checks.
# Every actor named in a request (participant, operator, sender,
# is_supervisor) is taken on trust from the product's proxy, which must
# derive them from its own session. Written down, with whose
# responsibility each part is, in ONE place: deploy/INTEGRATION.md,
# "What comms takes on trust". This module is where that model stops.
#
# SECRET HANDLING:
#   - comparison is constant-time (secrets.compare_digest) -- an
#     internal API on an isolated network hardly invites timing
#     attacks, but the correct primitive costs nothing;
#   - the token NEVER appears in logs or error bodies (the same
#     principle as the formatter's secret sanitizer): the 401 detail
#     is a constant string, nothing from the request is echoed back;
#   - an auth failure is logged WITHOUT the presented value.
#
# EMPTY TOKEN:
#   - outside development -> unreachable here: startup validation in
#     app/core/config.py refuses to boot without the token;
#   - development -> auth is DISABLED (local dev without the bearer
#     ceremony); app/main.py logs a loud warning at startup so an open
#     API is visible, not silent.
#
# A REPEATED QUERY PARAMETER IS REFUSED (G1), on every route
# (refuse_repeated_query, an application-level dependency in
# app/main.py). FastAPI reads a scalar parameter given twice --
# ?limit=5&limit=500 -- as its LAST value, silently: the request that
# runs is not the one that was sent, the class app/api/paging.py
# refuses for a limit out of bounds. No route of comms takes a list in
# its query, so a key given twice is never meaningful: 422, class
# `validation`, naming the key.
# =============================================================================

import secrets
from collections import Counter

import structlog
from fastapi import Header, HTTPException, Request, status

from app.core.config import settings
from app.core.exceptions import ValidationError

logger = structlog.get_logger()

# Constant 401 payload: never reflect anything from the request.
_UNAUTHORIZED = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid or missing service token",
    headers={"WWW-Authenticate": "Bearer"},
)

_BEARER_PREFIX = "Bearer "


async def require_service_auth(
    authorization: str | None = Header(default=None),
) -> None:
    """FastAPI dependency: verify the service-to-service bearer token.

    Raises HTTP 401 when the Authorization header is missing, is not
    a Bearer scheme, or carries a token that does not match
    COMMS_SERVICE_TOKEN. No-op when the token is unset (development
    only -- any other APP_ENV refuses to start without it).
    """
    expected = settings.comms_service_token
    if not expected:
        # Development with auth disabled (warned at startup).
        return

    if authorization is None or not authorization.startswith(_BEARER_PREFIX):
        logger.warning("service_auth_failed", reason="missing_or_not_bearer")
        raise _UNAUTHORIZED

    presented = authorization[len(_BEARER_PREFIX) :]
    if not secrets.compare_digest(
        presented.encode("utf-8"), expected.encode("utf-8")
    ):
        # Do NOT log the presented value -- a near-miss token is still
        # a secret (e.g. the prod token hitting the test deploy).
        logger.warning("service_auth_failed", reason="token_mismatch")
        raise _UNAUTHORIZED


async def refuse_repeated_query(request: Request) -> None:
    """Refuse a request whose query names a key more than once."""
    counts = Counter(key for key, _ in request.query_params.multi_items())
    repeated = sorted(key for key, count in counts.items() if count > 1)
    if repeated:
        raise ValidationError(
            f"query parameter given more than once: {', '.join(repeated)}; "
            f"each is read once -- send it once"
        )
