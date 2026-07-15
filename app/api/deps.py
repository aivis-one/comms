# =============================================================================
# COMMS Service -- API Dependencies (service-to-service auth)
# =============================================================================
#
# Phase 3b item 1. The comms API is internal (arch decision 14): its
# only caller is the product backend, which presents the shared secret
# as "Authorization: Bearer <token>". This dependency guards every
# /api/v1 router.
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
#   - real mode  -> unreachable here: startup validation in
#     app/core/config.py refuses to boot without the token;
#   - stub mode  -> auth is DISABLED (local dev / tests without the
#     bearer ceremony); app/main.py logs a loud warning at startup so
#     a misconfigured deployment is visible, not silent.
# =============================================================================

import secrets

import structlog
from fastapi import Header, HTTPException, status

from app.core.config import settings

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
    COMMS_SERVICE_TOKEN. No-op when the token is unset (stub mode --
    real mode refuses to start without it).
    """
    expected = settings.comms_service_token
    if not expected:
        # Stub mode with auth disabled (warned at startup).
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
