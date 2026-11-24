# =============================================================================
# COMMS Service -- the sign-in's two requests to Telegram (L-3b)
# =============================================================================
#
# The code exchange (TELEGRAM_LOGIN_TOKEN_URL: HTTP Basic with the client
# id and secret, the attempt's redirect_uri and verifier) and Telegram's
# keys (TELEGRAM_LOGIN_JWKS_URL, fetched for every sign-in: no cache, by
# the contract).
#
# EACH ANSWER IS READ UP TO BODY_MAX_BYTES (R-6) -- the bytes on the wire
# AND the bytes they decode to -- from the stream, so no more is ever
# held: httpx reads a body whole and inflates gzip whole, and on the
# 4.2.0 candidate 474 KB of gzip took 471 MB. The request asks for gzip
# alone (_ACCEPT_ENCODING), the one coding comms inflates, by zlib with
# a bound on its output; an answer in any other coding, or gzip that does
# not inflate, is `*_encoding`.
#
# EACH REQUEST IS BOUNDED WHOLE by TELEGRAM_TIMEOUT_SECONDS -- from its
# start to the last byte of its answer -- by asyncio.timeout around it
# (R-3). The client's own httpx.Timeout bounds each PHASE alone (connect,
# write, every read): a server that sends a byte now and then never trips
# it, and the 4.2.0 candidate held such an exchange for 16 s under a
# timeout of 2.5. The contract promises the product 10 s a request.
#
# THE CALLER HOLDS NO TRANSACTION while these run (app/login/complete.py):
# the attempt is taken and committed before the first.
#
# WHAT AN ANSWER MEANS (the contract and the live sign-in of 24.11):
#   - no answer (a timeout, the network, the whole bound passed), a
#     status other than 200, a body past BODY_MAX_BYTES or in a coding
#     comms does not inflate, a body that is not a JSON object -- or one
#     nested past what json reads -> Telegram is unavailable;
#   - 200 with `error` in the body -> Telegram refused the exchange. The
#     live sign-in answered a wrong code and a wrong secret alike: 200,
#     {"error": "invalid_grant"} -- the refusal is read off the BODY, not
#     the status;
#   - 200 without `error` and without a string `id_token` -> no id_token.
# The word kept of a refusal is an error code of RFC 6749 §5.2 when it is
# one of those, else `other`: nothing Telegram writes reaches a log.
#
# A CLIENT PER SIGN-IN (new_client), closed when the sign-in is done: a
# sign-in is rare, and no client outlives it. trust_env=False -- no proxy
# or certificate settings of the environment reach it -- and no redirect
# is followed (httpx's default), as for the channels' clients
# (app/engine/formatters.py). Tests replace new_client.
#
# NOTHING OF A REQUEST OR AN ANSWER is logged here, nor raised: the code,
# the verifier, the secret, the id_token and the keys stay out of every
# log line and exception; a failure of httpx is turned into a word.
# =============================================================================

import asyncio
import json
import zlib
from dataclasses import dataclass, field
from typing import Any

import httpx

from app.core.config import TELEGRAM_LOGIN_JWKS_URL, TELEGRAM_LOGIN_TOKEN_URL

# Each request to Telegram, at most, whole (the contract: two requests,
# 10 s each). Read at each request.
TELEGRAM_TIMEOUT_SECONDS = 10.0

# Each answer of Telegram, at most, on the wire and decoded. The live
# answers of 24.11 were a few KB: an id_token of RS256 about 1 KB, a key
# set of two keys under 1 KB.
BODY_MAX_BYTES = 64 * 1024
# The one coding asked for: the one comms inflates, by its own bound.
_ACCEPT_ENCODING = {"Accept-Encoding": "gzip"}

# The error codes of a token endpoint (RFC 6749 §5.2): the only words of
# a refusal that are kept.
OAUTH_ERRORS = frozenset({
    "invalid_request", "invalid_client", "invalid_grant",
    "unauthorized_client", "unsupported_grant_type", "invalid_scope",
})


def new_client() -> httpx.AsyncClient:
    """The client of one sign-in: both requests, then closed."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(TELEGRAM_TIMEOUT_SECONDS), trust_env=False,
    )


@dataclass(frozen=True)
class Unavailable:
    """Telegram did not answer usably. `reason` is a word."""

    reason: str
    status: int | None


@dataclass(frozen=True)
class Refused:
    """Telegram refused the code exchange (200 with `error`)."""

    reason: str
    status: int


@dataclass(frozen=True)
class NoIdToken:
    """The exchange answered without a string id_token."""

    status: int


@dataclass(frozen=True)
class Exchanged:
    """The exchange's answer: its id_token and the whole answer as a JSON
    object (the bot's right reads its `scope`). Kept out of the repr.
    Named `answer`, not `body`: tests/test_g1_hygiene.py watches every
    read of a letter's field names in app/ by name."""

    id_token: str = field(repr=False)
    answer: dict[str, Any] = field(repr=False)
    status: int


@dataclass(frozen=True)
class Keys:
    """Telegram's key set, as published. Kept out of the repr."""

    jwks: dict[str, Any] = field(repr=False)
    status: int


async def _read(response: httpx.Response, step: str) -> bytes | Unavailable:
    """The answer's body, decoded -- or `*_too_large` the moment the wire
    or the decoded bytes pass BODY_MAX_BYTES, `*_encoding` for a coding
    comms does not inflate. Read from the stream: never more is held."""
    coding = response.headers.get("content-encoding", "identity").strip().lower()
    if coding not in ("identity", "gzip"):
        return Unavailable(f"{step}_encoding", response.status_code)
    inflater = zlib.decompressobj(16 + zlib.MAX_WBITS) if coding == "gzip" else None
    wire = 0
    body = bytearray()
    try:
        async for chunk in response.aiter_raw():
            wire += len(chunk)
            if inflater is not None:
                # At most one byte past the bound: enough to know.
                chunk = inflater.decompress(chunk, BODY_MAX_BYTES + 1 - len(body))
            body += chunk
            if wire > BODY_MAX_BYTES or len(body) > BODY_MAX_BYTES:
                return Unavailable(f"{step}_too_large", response.status_code)
    except zlib.error:
        return Unavailable(f"{step}_encoding", response.status_code)
    # No flush(): decompress() inflates all the input it is given unless it
    # stops at max_length, and stopping there is refused above -- nothing
    # is left in the inflater.
    return bytes(body)


def _json_object(raw: bytes, status: int, step: str) -> dict[str, Any] | Unavailable:
    try:
        body = json.loads(raw)
    except (ValueError, RecursionError):
        # RecursionError: valid JSON nested deeper than json reads.
        return Unavailable(f"{step}_not_json", status)
    if not isinstance(body, dict):
        return Unavailable(f"{step}_not_object", status)
    return body


async def exchange_code(
    http: httpx.AsyncClient,
    *,
    code: str,
    redirect_uri: str,
    code_verifier: str,
    client_id: str,
    client_secret: str,
) -> Exchanged | Refused | NoIdToken | Unavailable:
    """Exchange the sign-in's code at Telegram's token endpoint."""
    try:
        async with (
            asyncio.timeout(TELEGRAM_TIMEOUT_SECONDS),
            http.stream(
                "POST",
                TELEGRAM_LOGIN_TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "client_id": client_id,
                    "code_verifier": code_verifier,
                },
                auth=(client_id, client_secret),
                headers=_ACCEPT_ENCODING,
            ) as response,
        ):
            if response.status_code != 200:
                return Unavailable("token_status", response.status_code)
            raw = await _read(response, "token")
    except TimeoutError:
        return Unavailable("token_timeout", None)
    except httpx.HTTPError:
        return Unavailable("token_unreachable", None)
    if isinstance(raw, Unavailable):
        return raw
    body = _json_object(raw, response.status_code, "token")
    if isinstance(body, Unavailable):
        return body
    if "error" in body:
        error = body["error"]
        word = error if isinstance(error, str) and error in OAUTH_ERRORS else "other"
        return Refused(word, response.status_code)
    id_token = body.get("id_token")
    if not isinstance(id_token, str) or not id_token:
        return NoIdToken(response.status_code)
    return Exchanged(id_token=id_token, answer=body, status=response.status_code)


async def fetch_jwks(http: httpx.AsyncClient) -> Keys | Unavailable:
    """Telegram's published keys -- fetched now, never kept."""
    try:
        async with (
            asyncio.timeout(TELEGRAM_TIMEOUT_SECONDS),
            http.stream(
                "GET", TELEGRAM_LOGIN_JWKS_URL, headers=_ACCEPT_ENCODING,
            ) as response,
        ):
            if response.status_code != 200:
                return Unavailable("jwks_status", response.status_code)
            raw = await _read(response, "jwks")
    except TimeoutError:
        return Unavailable("jwks_timeout", None)
    except httpx.HTTPError:
        return Unavailable("jwks_unreachable", None)
    if isinstance(raw, Unavailable):
        return raw
    body = _json_object(raw, response.status_code, "jwks")
    if isinstance(body, Unavailable):
        return body
    return Keys(jwks=body, status=response.status_code)
