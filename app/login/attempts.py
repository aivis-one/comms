# =============================================================================
# COMMS Service -- sign-in attempts: start, take once, purge (L-2)
# =============================================================================
#
# A product starts a sign-in through Telegram (POST /api/v1/login/start,
# app/api/login.py): comms makes the attempt's `state` (32 random bytes)
# and the PKCE verifier, keeps the attempt, and hands back the browser's
# way to Telegram. The attempt lives ATTEMPT_LIFETIME from its start and
# is TAKEN at most once; the row is deleted ATTEMPT_KEEP after it was
# created, by the worker's purge (app/worker.py).
#
# WHAT IS KEPT: the SHA-256 of the state -- never the state --, the
# provider, the product's redirect_uri, the verifier until the attempt is
# taken, bot_access and the times. Nothing personal: who signs in is not
# known to comms at the start, and the take does not learn it either.
#
# WHAT IS NEVER WRITTEN to a log, an answer of an error or a repr: the
# state, the verifier, the authorize URL (it carries the state). This
# module logs nothing; the results hide them from their repr.
#
# ONE CLOCK: every time of an attempt is the database's now() -- the
# start writes created_at and expires_at in one INSERT, the take and the
# purge compare against now() of their own statement.
#
# THE WRITERS of a row, and why they never wait on one another in a
# cycle: the start INSERTs a new row; the take UPDATEs one row younger
# than ATTEMPT_LIFETIME, in one statement; the purge DELETEs rows older
# than ATTEMPT_KEEP. Each statement takes one row; the take and the purge
# never want the same row (their predicates are disjoint); no foreign key
# points at or from the table. No lock order to keep, no deadlock to
# build.
# =============================================================================

import base64
import hashlib
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from urllib.parse import urlencode

from sqlalchemy import Interval, bindparam, func, insert, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import TELEGRAM_LOGIN_AUTHORIZE_URL
from app.core.database import get_session_factory
from app.login.models import LoginAttempt

# The terms of the contract (form of v4.2): constants of comms, not
# settings.
ATTEMPT_LIFETIME = timedelta(minutes=10)
ATTEMPT_KEEP = timedelta(hours=24)
# The state: 32 random bytes, 43 characters of base64url.
STATE_BYTES = 32
# The PKCE verifier: 48 random bytes, 64 characters of base64url (RFC
# 7636 allows 43 to 128).
VERIFIER_BYTES = 48
# The product's return address, at most (the column is as wide:
# migration 0042).
REDIRECT_URI_MAX = 2048
# How often the worker's purge runs (app/worker.py): a row lives at most
# ATTEMPT_KEEP plus this. The take does not depend on it -- an attempt
# older than ATTEMPT_KEEP is unknown whether or not it was purged.
PURGE_INTERVAL_SECONDS = 3600

PROVIDER_TELEGRAM = "telegram"
SCOPE = "openid profile"
BOT_ACCESS_SCOPE = "telegram:bot_access"


def state_fingerprint(state: str) -> str:
    """What the table keeps of a state: its SHA-256, hex."""
    return hashlib.sha256(state.encode("ascii")).hexdigest()


def code_challenge(verifier: str) -> str:
    """The S256 challenge of a verifier (RFC 7636 §4.2)."""
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


@dataclass(frozen=True)
class StartedAttempt:
    """What the start hands the product. The state and the URL that
    carries it are kept out of the repr."""

    state: str = field(repr=False)
    authorize_url: str = field(repr=False)
    expires_at: datetime


@dataclass(frozen=True)
class TakenAttempt:
    """An attempt taken: what the code exchange needs. The verifier is
    erased in the row already; here it is kept out of the repr."""

    code_verifier: str = field(repr=False)
    redirect_uri: str
    bot_access: bool


class NotTaken(StrEnum):
    """Why an attempt was not taken."""

    # The state was never given, or its attempt is older than
    # ATTEMPT_KEEP (purged or not yet).
    UNKNOWN = "unknown"
    # Not taken, and ATTEMPT_LIFETIME has passed since its start.
    EXPIRED = "expired"
    # Taken already -- by an earlier take, or one that won a race.
    REPLAYED = "replayed"


async def start_attempt(
    session: AsyncSession,
    *,
    redirect_uri: str,
    bot_access: bool,
    client_id: str,
) -> StartedAttempt:
    """Record a new attempt in `session` (the caller commits) and build
    the authorize URL: the parameters of the form's appendix A."""
    state = secrets.token_urlsafe(STATE_BYTES)
    verifier = secrets.token_urlsafe(VERIFIER_BYTES)
    expires_at = await session.scalar(
        insert(LoginAttempt)
        .values(
            state_hash=state_fingerprint(state),
            provider=PROVIDER_TELEGRAM,
            redirect_uri=redirect_uri,
            code_verifier=verifier,
            bot_access=bot_access,
            created_at=func.now(),
            expires_at=func.now() + bindparam(
                "lifetime", ATTEMPT_LIFETIME, type_=Interval(),
            ),
        )
        .returning(LoginAttempt.expires_at)
    )
    assert expires_at is not None  # RETURNING of the row just inserted
    query = urlencode({
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": f"{SCOPE} {BOT_ACCESS_SCOPE}" if bot_access else SCOPE,
        "state": state,
        "code_challenge": code_challenge(verifier),
        "code_challenge_method": "S256",
    })
    return StartedAttempt(
        state=state,
        authorize_url=f"{TELEGRAM_LOGIN_AUTHORIZE_URL}?{query}",
        expires_at=expires_at,
    )


# THE TAKE, one statement: the row is locked and re-read under the lock
# (a waiting FOR NO KEY UPDATE re-checks its WHERE on the version the
# winner committed), then marked taken with the verifier erased.
# RETURNING of an UPDATE gives the NEW values in Postgres 16, so the old
# verifier comes from the CTE.
_TAKE = text(
    "WITH free AS ("
    " SELECT state_hash, code_verifier, redirect_uri, bot_access"
    " FROM login_attempts"
    " WHERE state_hash = :state_hash AND taken_at IS NULL"
    " AND expires_at > now()"
    " FOR NO KEY UPDATE)"
    " UPDATE login_attempts AS a SET taken_at = now(), code_verifier = NULL"
    " FROM free WHERE a.state_hash = free.state_hash"
    " RETURNING free.code_verifier, free.redirect_uri, free.bot_access"
)
# Why not: the attempt younger than ATTEMPT_KEEP, taken or not.
_WHY_NOT = text(
    "SELECT taken_at IS NOT NULL FROM login_attempts"
    " WHERE state_hash = :state_hash AND created_at > now() - :keep"
).bindparams(bindparam("keep", ATTEMPT_KEEP, type_=Interval()))


async def take_attempt(state: str) -> TakenAttempt | NotTaken:
    """Take the attempt of `state` -- at most once, ever.

    In a session of its own, COMMITTED before it returns: the caller
    calls Telegram outside any transaction of the database.

    The take is ONE statement (_TAKE): of two takes of one state at once,
    the second waits on the row, re-reads it as the first committed it --
    taken -- and takes nothing. When nothing was taken, one more read
    says why (_WHY_NOT). No race between the two: what made the take find
    nothing cannot be undone -- a taken attempt is never untaken, an
    expired one never becomes good again, and a state that was never
    given does not appear (32 random bytes). The one change possible in
    between is the purge deleting the row, which makes it `unknown`: what
    the contract says of an attempt older than a day.
    """
    fingerprint = state_fingerprint(state)
    async with get_session_factory()() as session:
        row = (await session.execute(
            _TAKE, {"state_hash": fingerprint},
        )).one_or_none()
        await session.commit()
        if row is not None:
            return TakenAttempt(
                code_verifier=row.code_verifier,
                redirect_uri=row.redirect_uri,
                bot_access=row.bot_access,
            )
        taken = await session.scalar(_WHY_NOT, {"state_hash": fingerprint})
        await session.commit()
    if taken is None:
        return NotTaken.UNKNOWN
    return NotTaken.REPLAYED if taken else NotTaken.EXPIRED


_PURGE = text(
    "DELETE FROM login_attempts WHERE created_at < now() - :keep"
).bindparams(bindparam("keep", ATTEMPT_KEEP, type_=Interval()))


async def purge_login_attempts() -> int:
    """Delete every attempt older than ATTEMPT_KEEP; how many. One
    statement over ix_login_attempts_created_at, committed."""
    async with get_session_factory()() as session:
        result = await session.execute(_PURGE)
        await session.commit()
    count: int = result.rowcount  # type: ignore[attr-defined]
    return count
