# =============================================================================
# COMMS Service -- the sign-in's identity: Telegram's id_token (L-3a)
# =============================================================================
#
# What the sign-in hands a product is taken ONLY from an id_token that
# passed every check of the contract (form of v4.2):
#   - the signature, by Telegram's published key the token names by its
#     `kid` -- exactly one key of the set given;
#   - the algorithm RS256 or ES256, from the token's header but out of
#     OUR list (ALGORITHMS): `none`, HS256 -- a public key used as an HMAC
#     secret --, any other is refused before a key is looked at;
#   - the key of the algorithm (R-3): RS256 an RSA PUBLIC key of at least
#     RSA_MIN_BITS, ES256 an EC PUBLIC key on P-256 -- checked on the key
#     PyJWT built from the set's entry, the object the signature would be
#     verified with, so a private key, an HMAC (`oct`) key, a weak or a
#     foreign one is refused before any signature is checked. An `oct`
#     entry whose bytes are the PEM of an RSA key would otherwise verify
#     RS256: PyJWT reads such bytes as that key;
#   - `iss` is TELEGRAM_LOGIN_ISSUER, `aud` a string equal to the client id
#     (the bot's id);
#   - `exp` and `iat` present, numbers a float can hold, and right
#     against the moment of the check, with CLOCK_LEEWAY for the clocks of
#     the box and Telegram.
# Every failure is one outcome, IdTokenRejected, whose member is the WORD
# of the reason -- for the log, never a value. `nbf` is not checked: the
# contract does not name it, and Telegram sends none.
#
# A TOKEN WITHOUT `kid` IS REFUSED, unlike velo's sample, which then takes
# the one key of a set: the contract picks the key by `kid`.
#
# PURE: no network, no database, no clock. The key set and the moment are
# given by the caller -- the moment once, and the identity says it was
# verified then. PyJWT checks the signature and the presence of the
# claims; it reads its own clock for `exp`, `iat` and `nbf`, so those
# checks of its are off and done here, against the moment given.
#
# NOTHING OF A TOKEN, A CLAIM OR A KEY reaches a log, an exception or a
# repr: this module logs nothing and raises nothing of its own; PyJWT's
# exceptions are turned into a word here; the identity's repr names no
# field. NO VALUE OF TELEGRAM'S RAISES out of it (R-3): a number past a
# float, claims nested past what json reads -- each is a word.
# =============================================================================

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from urllib.parse import urlsplit

import jwt
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from app.core.config import TELEGRAM_LOGIN_ISSUER
from app.login.attempts import BOT_ACCESS_SCOPE, PROVIDER_TELEGRAM

# The algorithms an id_token of Telegram may be signed with (the
# contract): RS256 is Telegram's default, ES256 the other one BotFather
# offers with the profile scope.
ALGORITHMS = ("RS256", "ES256")
# The smallest RSA key an RS256 token of Telegram's is verified with.
RSA_MIN_BITS = 2048
# How far the clocks of the box and of Telegram may be apart for `exp`
# and `iat`.
CLOCK_LEEWAY = timedelta(seconds=30)
# The claims PyJWT must find present; their values are checked here.
_REQUIRED = ("exp", "iat", "iss", "aud")

# A Telegram id: 1 .. 2^63-1 (the contract).
TELEGRAM_ID_MAX = 2**63 - 1
_ASCII_DIGITS = re.compile(r"[0-9]+")


class IdTokenRejected(StrEnum):
    """Why an id_token is not taken -- a word for the log."""

    MALFORMED = "malformed"
    ALGORITHM = "algorithm"
    NO_KID = "no_kid"
    UNKNOWN_KEY = "unknown_key"
    AMBIGUOUS_KEY = "ambiguous_key"
    KEY_TYPE = "key_type"
    SIGNATURE = "signature"
    NO_EXP = "no_exp"
    EXPIRED = "expired"
    NO_IAT = "no_iat"
    IAT_IN_FUTURE = "iat_in_future"
    ISSUER = "issuer"
    AUDIENCE = "audience"
    NO_ID = "no_id"


@dataclass(frozen=True, repr=False)
class TelegramIdentity:
    """The person an id_token proved, in the fields of the contract."""

    telegram_id: int
    first_name: str | None
    last_name: str | None
    username: str | None
    photo_url: str | None
    bot_access_granted: bool
    verified_at: datetime

    @property
    def provider(self) -> str:
        return PROVIDER_TELEGRAM

    @property
    def subject(self) -> str:
        """The provider's subject: the Telegram id as a decimal string --
        never the token's `sub`."""
        return str(self.telegram_id)

    def __repr__(self) -> str:
        return "TelegramIdentity(<fields not shown>)"


_MISSING = {
    "exp": IdTokenRejected.NO_EXP,
    "iat": IdTokenRejected.NO_IAT,
    "iss": IdTokenRejected.ISSUER,
    "aud": IdTokenRejected.AUDIENCE,
}


def _key(jwks: object, kid: str) -> Any | IdTokenRejected:
    """The one key of `jwks` named `kid`, as PyJWT takes it."""
    entries = jwks.get("keys") if isinstance(jwks, Mapping) else None
    if not isinstance(entries, list):
        return IdTokenRejected.UNKNOWN_KEY
    named = [e for e in entries if isinstance(e, Mapping) and e.get("kid") == kid]
    if not named:
        return IdTokenRejected.UNKNOWN_KEY
    if len(named) > 1:
        return IdTokenRejected.AMBIGUOUS_KEY
    try:
        return jwt.PyJWK(dict(named[0])).key
    except (jwt.PyJWTError, TypeError, ValueError, KeyError):
        return IdTokenRejected.UNKNOWN_KEY


def _fits(key: object, alg: str) -> bool:
    """`key` -- as PyJWT built it from the set's entry -- is one an `alg`
    token of Telegram's may be verified with: a PUBLIC key of the
    algorithm's family, RSA of RSA_MIN_BITS or more, EC on P-256."""
    if alg == "RS256":
        return isinstance(key, rsa.RSAPublicKey) and key.key_size >= RSA_MIN_BITS
    if alg == "ES256":
        return isinstance(key, ec.EllipticCurvePublicKey) and isinstance(
            key.curve, ec.SECP256R1,
        )
    return False


def _numeric_date(value: object) -> float | None:
    """A JWT NumericDate: a finite JSON number -- never true or false, nor
    an integer past what a float holds (10**400 is valid JSON)."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if math.isfinite(number) else None


def verified_claims(
    id_token: object, jwks: object, *, client_id: str, now: datetime,
) -> dict[str, Any] | IdTokenRejected:
    """The claims of `id_token` once every check of the contract passed
    (the module's header), or the word of the first that failed."""
    if not isinstance(id_token, str) or not id_token:
        return IdTokenRejected.MALFORMED
    try:
        header = jwt.get_unverified_header(id_token)
    except jwt.PyJWTError:
        return IdTokenRejected.MALFORMED
    alg = header.get("alg")
    if not isinstance(alg, str) or alg not in ALGORITHMS:
        return IdTokenRejected.ALGORITHM
    kid = header.get("kid")
    if not isinstance(kid, str) or not kid:
        return IdTokenRejected.NO_KID
    key = _key(jwks, kid)
    if isinstance(key, IdTokenRejected):
        return key
    if not _fits(key, alg):
        return IdTokenRejected.KEY_TYPE
    try:
        claims: dict[str, Any] = jwt.decode(
            id_token,
            key,
            algorithms=[alg],
            options={
                "require": list(_REQUIRED),
                "verify_exp": False,
                "verify_iat": False,
                "verify_nbf": False,
                "verify_iss": False,
                "verify_aud": False,
            },
        )
    except jwt.MissingRequiredClaimError as missing:
        return _MISSING[missing.claim]
    except jwt.DecodeError as failed:
        # A signature that does not verify is a DecodeError's subclass;
        # anything else of it is a token that cannot be read.
        if isinstance(failed, jwt.InvalidSignatureError):
            return IdTokenRejected.SIGNATURE
        return IdTokenRejected.MALFORMED
    except RecursionError:
        # Claims nested deeper than json reads: no token to read.
        return IdTokenRejected.MALFORMED
    except (jwt.PyJWTError, TypeError, ValueError):
        # Any other failure of PyJWT to verify with the key that fits.
        return IdTokenRejected.SIGNATURE

    moment = now.timestamp()
    leeway = CLOCK_LEEWAY.total_seconds()
    exp = _numeric_date(claims["exp"])
    if exp is None:
        return IdTokenRejected.NO_EXP
    if moment >= exp + leeway:
        return IdTokenRejected.EXPIRED
    iat = _numeric_date(claims["iat"])
    if iat is None:
        return IdTokenRejected.NO_IAT
    if iat > moment + leeway:
        return IdTokenRejected.IAT_IN_FUTURE
    if claims["iss"] != TELEGRAM_LOGIN_ISSUER:
        return IdTokenRejected.ISSUER
    aud = claims["aud"]
    if not isinstance(aud, str) or aud != client_id:
        return IdTokenRejected.AUDIENCE
    return claims


def telegram_id(value: object) -> int | None:
    """The Telegram id of the `id` claim, or None.

    A JSON integer, or a string of ASCII digits -- Telegram gives either
    (the live token of 24.11 carried a string); leading zeros allowed; no
    sign, space, underscore or other script's digits (int() and isdigit()
    would take some of those, hence the pattern first); 1 .. 2^63-1. A
    JSON true or false is not an id, though bool is an int to Python.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and _ASCII_DIGITS.fullmatch(value):
        digits = value.lstrip("0")
        # Length first: int() refuses past 4300 digits by raising, and
        # nothing past 19 digits is an id anyway.
        if len(digits) > len(str(TELEGRAM_ID_MAX)):
            return None
        number = int(digits or "0")
    else:
        return None
    return number if 1 <= number <= TELEGRAM_ID_MAX else None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _https(value: object) -> str | None:
    """A photo's address: https with a host, or nothing."""
    text = _text(value)
    if text is None:
        return None
    try:
        parts = urlsplit(text)
    except ValueError:
        return None
    return text if parts.scheme == "https" and parts.hostname else None


def identity_from_claims(
    claims: Mapping[str, Any], *, verified_at: datetime, bot_access_granted: bool,
) -> TelegramIdentity | IdTokenRejected:
    """The person of verified claims, by the contract's table."""
    number = telegram_id(claims.get("id"))
    if number is None:
        return IdTokenRejected.NO_ID
    return TelegramIdentity(
        telegram_id=number,
        first_name=_text(claims.get("given_name")) or _text(claims.get("name")),
        last_name=_text(claims.get("family_name")),
        username=_text(claims.get("preferred_username")),
        photo_url=_https(claims.get("picture")),
        bot_access_granted=bot_access_granted,
        verified_at=verified_at,
    )


def verify_identity(
    id_token: object,
    jwks: object,
    *,
    client_id: str,
    now: datetime,
    bot_access_granted: bool,
) -> TelegramIdentity | IdTokenRejected:
    """The person of `id_token`, verified at `now` against `jwks`, or the
    word why not."""
    claims = verified_claims(id_token, jwks, client_id=client_id, now=now)
    if isinstance(claims, IdTokenRejected):
        return claims
    return identity_from_claims(
        claims, verified_at=now, bot_access_granted=bot_access_granted,
    )


_ABSENT = object()


def bot_access_granted(requested: bool, exchange: object) -> bool:
    """Whether the bot may write to the person after this sign-in.

    Asked for (bot_access), and Telegram's answer to the code exchange
    either names no `scope` -- RFC 6749 §5.1: then the scope granted is
    the one asked -- or names one that holds telegram:bot_access. A
    `scope` that is there but no string grants nothing.
    """
    if not requested or not isinstance(exchange, Mapping):
        return False
    scope = exchange.get("scope", _ABSENT)
    if scope is _ABSENT:
        return True
    return isinstance(scope, str) and BOT_ACCESS_SCOPE in scope.split()
