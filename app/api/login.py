# =============================================================================
# COMMS Service -- the sign-in's start (the option `login`, L-2)
# =============================================================================
#
#   POST /api/v1/login/start
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
# so the token is there and of its shape.
# =============================================================================

from typing import Any, Literal
from urllib.parse import urlsplit

from fastapi import APIRouter
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictStr,
    field_validator,
)

from app.api.deps import DbSession
from app.core.channels import telegram_bot_id
from app.core.config import settings
from app.login.attempts import REDIRECT_URI_MAX, start_attempt

router = APIRouter(prefix="/api/v1/login", tags=["login"])


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
