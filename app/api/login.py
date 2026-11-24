# =============================================================================
# COMMS Service -- the sign-in: start and complete (the option `login`)
# =============================================================================
#
#   POST /api/v1/login/start      (L-2)
#   POST /api/v1/login/complete   (L-3b)
#
# The product starts a sign-in through Telegram: comms records the
# attempt (app/login/attempts.py) and answers with its `state`, the URL
# to send the browser to, and when the attempt expires. Included in the
# application ONLY with LOGIN_ENABLED true (app/main.py); off, the path
# is an unknown one, 404 not_found, as in v4.0.1. Behind the service
# token like every /api/v1 route (app/api/service_auth.py).
#
# THE BODY, refused as a 422 `validation` naming the field when it is
# not this -- nothing is kept of a refused start:
#   provider      "telegram", the one provider of v4.2;
#   redirect_uri  https, a host, no fragment, at most REDIRECT_URI_MAX;
#                 kept with the attempt for the code exchange. comms has
#                 no list of allowed addresses: the product's own;
#   bot_access    a bool, false when absent: true asks Telegram for the
#                 scope telegram:bot_access too.
# The refusal names the rule, never the value that broke it.
#
# client_id is the bot's id, read from TELEGRAM_BOT_TOKEN by the one
# function `comms derive bot-id` uses (app/core/channels.py
# telegram_bot_id). The option needs the channel on (app/core/modules.py),
# so the token is there and of its shape; and the option's start holds
# the id 1..19 ASCII digits (app/core/channels.py telegram_bot_id_problem)
# -- aiogram's shape takes any isdigit(), which int() fails on or reads.
#
# COMPLETE takes {state, code}: `state` in exactly the form start gives
# (STATE_PATTERN, ASCII -- checked BEFORE the attempt is taken), `code`
# a string of 1..CODE_MAX. The sign-in is done by app/login/complete.py:
# the attempt taken and committed first, then Telegram, outside any
# transaction -- so this route takes no session of its own. The answer
# is the person (§3 of the form) or a refusal of the class the contract
# names, with the status of its row (deploy/INTEGRATION.md, section 5)
# and a fixed message. One log line per outcome, `login_completed` or
# `login_refused`: the provider, the class, the word of the reason,
# Telegram's status -- never the state, the code, the verifier, the
# id_token, the secret, the keys, the Telegram id or the person's data.
# A refusal of the API's own -- validation, unauthorized, unavailable --
# is answered and logged by the API's common paths.
# =============================================================================

from typing import Any, Literal
from urllib.parse import urlsplit

import structlog
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictStr,
    field_validator,
)

from app.api.deps import DbSession
from app.api.errors import ErrorClass, error_response
from app.core.channels import telegram_bot_id
from app.core.config import settings
from app.login.attempts import PROVIDER_TELEGRAM, REDIRECT_URI_MAX, start_attempt
from app.login.complete import Refusal, SignInRefused, complete_sign_in

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/login", tags=["login"])

# The form of a state start gives: 32 random bytes, base64url, 43
# characters (app/login/attempts.py STATE_BYTES).
STATE_PATTERN = r"^[A-Za-z0-9_-]{43}$"
# Telegram's code, at most -- no form of it is promised.
CODE_MAX = 2048


def _redirect_uri_problem(value: str) -> str | None:
    """Why `value` is not a return address comms takes, or None."""
    if "#" in value:
        return "must have no fragment"
    try:
        parts = urlsplit(value)
    except ValueError:
        return "must be an https URL with a host"
    if parts.scheme != "https" or not parts.hostname:
        return "must be an https URL with a host"
    return None


class LoginStartIn(BaseModel):
    """The body of a start."""

    model_config = ConfigDict(extra="forbid")

    provider: Literal["telegram"]
    redirect_uri: StrictStr = Field(min_length=1, max_length=REDIRECT_URI_MAX)
    bot_access: StrictBool = False

    @field_validator("redirect_uri")
    @classmethod
    def _an_https_return_address(cls, value: str) -> str:
        problem = _redirect_uri_problem(value)
        if problem is not None:
            raise ValueError(problem)
        return value


@router.post("/start")
async def start(body: LoginStartIn, session: DbSession) -> dict[str, Any]:
    """Record a sign-in attempt; where to send the browser."""
    started = await start_attempt(
        session,
        redirect_uri=body.redirect_uri,
        bot_access=body.bot_access,
        client_id=telegram_bot_id(settings.telegram_bot_token),
    )
    return {
        "state": started.state,
        "authorize_url": started.authorize_url,
        "expires_at": started.expires_at.isoformat(),
    }


class LoginCompleteIn(BaseModel):
    """The body of a complete."""

    model_config = ConfigDict(extra="forbid")

    state: StrictStr = Field(pattern=STATE_PATTERN)
    code: StrictStr = Field(min_length=1, max_length=CODE_MAX)


# The status of each class: its row of the contract's table
# (deploy/INTEGRATION.md, section 5).
_STATUS = {
    Refusal.LOGIN_UNKNOWN: 404,
    Refusal.LOGIN_EXPIRED: 410,
    Refusal.LOGIN_REPLAYED: 409,
    Refusal.PROVIDER_REFUSED: 422,
    Refusal.PROVIDER_UNAVAILABLE: 503,
    Refusal.ID_TOKEN_INVALID: 502,
}
_MESSAGES = {
    Refusal.LOGIN_UNKNOWN: "the sign-in is not known; begin it again",
    Refusal.LOGIN_EXPIRED: "the sign-in has expired; begin it again",
    Refusal.LOGIN_REPLAYED: "the sign-in was completed already; begin it again",
    Refusal.PROVIDER_REFUSED: "Telegram refused the sign-in; begin it again",
    Refusal.PROVIDER_UNAVAILABLE: (
        "Telegram could not be asked; begin the sign-in again later"
    ),
    Refusal.ID_TOKEN_INVALID: "Telegram's answer did not prove the person",
}


def _refused(outcome: SignInRefused) -> JSONResponse:
    logger.info(
        "login_refused",
        provider=PROVIDER_TELEGRAM,
        error_class=outcome.refusal.value,
        reason=outcome.reason,
        telegram_status=outcome.telegram_status,
    )
    return error_response(
        _STATUS[outcome.refusal],
        ErrorClass(outcome.refusal.value),
        _MESSAGES[outcome.refusal],
    )


@router.post("/complete", response_model=None)
async def complete(body: LoginCompleteIn) -> JSONResponse:
    """Complete a sign-in: the person, or the refusal of the contract."""
    outcome = await complete_sign_in(
        body.state,
        body.code,
        client_id=telegram_bot_id(settings.telegram_bot_token),
        client_secret=settings.telegram_login_client_secret,
    )
    if isinstance(outcome, SignInRefused):
        return _refused(outcome)
    person = outcome.identity
    # Encoded HERE, before the line that says the product got it (R-6):
    # the log names only an outcome the product receives.
    answer = JSONResponse({
        "provider": person.provider,
        "subject": person.subject,
        "verified_at": person.verified_at.isoformat(),
        "telegram": {
            "id": person.telegram_id,
            "first_name": person.first_name,
            "last_name": person.last_name,
            "username": person.username,
            "photo_url": person.photo_url,
            "bot_access_granted": person.bot_access_granted,
        },
    })
    logger.info(
        "login_completed",
        provider=person.provider,
        telegram_status=outcome.telegram_status,
    )
    return answer
