# =============================================================================
# COMMS Service -- Idempotency of the resource calls that create (F1.4)
# =============================================================================
#
# The two resource calls that are NOT idempotent by construction --
# POST /api/v1/threads (a subjectless ticket is a fresh thread every
# time) and POST /api/v1/threads/{id}/messages (every message is new)
# -- take a REQUIRED `Idempotency-Key` header, by the same discipline
# as a job's key (spec §5.8, §10.7):
#
#   same key, same request   -> the SAME entity, nothing created twice
#                               (no second message, no second ping);
#   same key, other request  -> 409, error class `conflict`;
#   no key / blank / > 200   -> 422, error class `validation`.
#
# THE FINGERPRINT COVERS THE WHOLE REQUEST: the method, the path with
# its query string, and the raw body bytes. The key's unique index is
# global, so a fingerprint over the body alone would let the same key
# and the same "ok" sent to ANOTHER thread match the first thread's
# message and come back as its replay. Bytes are compared, never read.
#
# A REPLAY RETURNS THE ENTITY IN ITS CURRENT STATE, not a stored copy of
# the first response: the row is the fact that the request was served,
# and a cached response body would be a second copy of that row's state
# -- stale the moment the thread is closed or retagged. The replay of a
# thread creation therefore shows the thread as it is now.
#
# The other mutating resource calls need no key, and why each one is
# listed in deploy/INTEGRATION.md ("Repeating a call").
# =============================================================================

import hashlib

from fastapi import Request

from app.core.constants import MAX_IDEMPOTENCY_KEY_LEN
from app.core.exceptions import ValidationError

IDEMPOTENCY_HEADER = "Idempotency-Key"


def require_key(value: str | None) -> str:
    """The header's value, or a validation refusal."""
    if value is None:
        raise ValidationError(
            f"the {IDEMPOTENCY_HEADER} header is required on this call"
        )
    if not value.strip() or len(value) > MAX_IDEMPOTENCY_KEY_LEN:
        raise ValidationError(
            f"the {IDEMPOTENCY_HEADER} header must be 1..{MAX_IDEMPOTENCY_KEY_LEN} "
            f"non-blank characters"
        )
    return value


async def request_fingerprint(request: Request) -> str:
    """Digest of method, path with query, and the raw body bytes."""
    digest = hashlib.sha256()
    digest.update(request.method.encode("ascii"))
    digest.update(b"\n")
    digest.update(request.url.path.encode("utf-8"))
    digest.update(b"?")
    digest.update(request.url.query.encode("utf-8"))
    digest.update(b"\n")
    digest.update(await request.body())
    return digest.hexdigest()
