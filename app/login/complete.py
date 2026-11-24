# =============================================================================
# COMMS Service -- the sign-in completed: one request of the product (L-3b)
# =============================================================================
#
#   take the attempt (committed) -> exchange the code -> fetch the keys
#   -> verify the id_token at one moment -> the person, or a refusal
#
# THE ATTEMPT IS TAKEN FIRST, and committed before anything goes to
# Telegram (app/login/attempts.py take_attempt, a session of its own):
# no request to Telegram runs inside a transaction of the database, no
# lock is held across one, and an answer of Telegram never enters a
# transaction. After any refusal past the take the attempt is spent: a
# second complete of the same state is `login_replayed` -- even while the
# first is still talking to Telegram. The contract: begin again with
# start. Nothing is retried.
#
# THE ROW'S WRITERS stay those of app/login/attempts.py (start, take,
# purge); this module adds none. One row, one statement, no lock across
# HTTP: there is no lock order to keep and no deadlock to build.
#
# THE MOMENT of the check is read ONCE (_utc_now), after the keys came:
# the id_token's exp and iat are checked against it, and the person says
# it was verified then.
#
# THE OUTCOME is a value: the person (app/login/identity.py), or
# SignInRefused -- its Refusal named exactly as the contract's class, the
# WORD of the reason, and the status of Telegram's answer it stopped at.
# The route (app/api/login.py) turns it into the answer and one log
# line; nothing of the code, the verifier, the secret, the id_token, the
# keys or the person is in a refusal.
# =============================================================================

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from app.login import telegram
from app.login.attempts import NotTaken, take_attempt
from app.login.identity import (
    IdTokenRejected,
    TelegramIdentity,
    bot_access_granted,
    verify_identity,
)


class Refusal(StrEnum):
    """Why a sign-in was not completed -- the contract's classes."""

    LOGIN_UNKNOWN = "login_unknown"
    LOGIN_EXPIRED = "login_expired"
    LOGIN_REPLAYED = "login_replayed"
    PROVIDER_REFUSED = "provider_refused"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    ID_TOKEN_INVALID = "id_token_invalid"


_NOT_TAKEN = {
    NotTaken.UNKNOWN: Refusal.LOGIN_UNKNOWN,
    NotTaken.EXPIRED: Refusal.LOGIN_EXPIRED,
    NotTaken.REPLAYED: Refusal.LOGIN_REPLAYED,
}


@dataclass(frozen=True)
class SignInRefused:
    """A refusal: the class, a word, Telegram's status where it answered."""

    refusal: Refusal
    reason: str
    telegram_status: int | None = None


@dataclass(frozen=True)
class SignedIn:
    """The person, and the status of Telegram's last answer."""

    identity: TelegramIdentity
    telegram_status: int


def _utc_now() -> datetime:
    """The moment of the check -- the one clock of the sign-in."""
    return datetime.now(UTC)


async def complete_sign_in(
    state: str, code: str, *, client_id: str, client_secret: str,
) -> SignedIn | SignInRefused:
    """Complete the sign-in of `state` with Telegram's `code`."""
    taken = await take_attempt(state)
    if isinstance(taken, NotTaken):
        return SignInRefused(_NOT_TAKEN[taken], taken.value)

    async with telegram.new_client() as http:
        exchanged = await telegram.exchange_code(
            http,
            code=code,
            redirect_uri=taken.redirect_uri,
            code_verifier=taken.code_verifier,
            client_id=client_id,
            client_secret=client_secret,
        )
        if isinstance(exchanged, telegram.Unavailable):
            return SignInRefused(
                Refusal.PROVIDER_UNAVAILABLE, exchanged.reason, exchanged.status,
            )
        if isinstance(exchanged, telegram.Refused):
            return SignInRefused(
                Refusal.PROVIDER_REFUSED, exchanged.reason, exchanged.status,
            )
        if isinstance(exchanged, telegram.NoIdToken):
            return SignInRefused(
                Refusal.ID_TOKEN_INVALID, "no_id_token", exchanged.status,
            )
        keys = await telegram.fetch_jwks(http)
    if isinstance(keys, telegram.Unavailable):
        return SignInRefused(Refusal.PROVIDER_UNAVAILABLE, keys.reason, keys.status)

    now = _utc_now()
    verified = verify_identity(
        exchanged.id_token,
        keys.jwks,
        client_id=client_id,
        now=now,
        bot_access_granted=bot_access_granted(taken.bot_access, exchanged.answer),
    )
    if isinstance(verified, IdTokenRejected):
        return SignInRefused(Refusal.ID_TOKEN_INVALID, verified.value, keys.status)
    return SignedIn(identity=verified, telegram_status=keys.status)
