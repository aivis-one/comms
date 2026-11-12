# =============================================================================
# COMMS Service -- the `comms` command (W2-2)
# =============================================================================
#
# `comms derive <name>` prints one value of the instance that comms works
# out from the others, for the machine's source `derived` (primer.yaml):
# the machine runs it in a throwaway container of the comms image with the
# instance's other values in the environment, takes stdout as the value and
# passes stderr on when the exit code is not 0.
#
#   bot-id   the bot's id: the digits before the colon of
#            TELEGRAM_BOT_TOKEN (aiogram's extract_bot_id). No network.
#   bot-url  https://<TELEGRAM_LINK_HOST>/<username>, the username from
#            Telegram's getMe -- the one fact of the bot the token does not
#            carry. The form is the one startup holds TELEGRAM_BOT_URL to.
#
# OUTPUT: the value alone, one line, exit 0. A refusal: one line on stderr
# naming the variable or the class of the failure, exit 1; a wrong command
# line: argparse's usage, exit 2.
#
# THE TOKEN IS NEVER PRINTED. Every refusal is a fixed text: a validator's
# message, or the CLASS of a failure, never an exception's text -- aiohttp's
# ClientError, which aiogram wraps as TelegramNetworkError, carries the
# request's URL, and the URL carries the token. aiogram logs nothing on this
# path (its request logging is a middleware nobody installs here).
#
# NO SETTINGS: the token is read from the variable the application reads,
# TELEGRAM_BOT_TOKEN, and held to the same form (app/core/channels.py, the
# telegram channel's validator) -- but not through app/core/config.py, which
# builds the whole settings at import and refuses a process without
# DATABASE_URL. The container of `derive` has the instance's values only;
# DATABASE_URL is assembled by compose, not one of them.
#
# The network: a service's `derive` keeps the default one (the machine's
# steps/env.py), so getMe reaches Telegram. The call is bounded by the
# telegram channel's own client timeout -- the one place it is written --
# well inside the machine's 300 s for a derive.
# =============================================================================

import argparse
import asyncio
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING
from urllib.parse import urlunsplit

from app.core.channels import channel_spec

if TYPE_CHECKING:
    from aiogram.client.telegram import TelegramAPIServer

TOKEN_VARIABLE = "TELEGRAM_BOT_TOKEN"
LINK_HOST_VARIABLE = "TELEGRAM_LINK_HOST"
URL_VARIABLE = "TELEGRAM_BOT_URL"

# Every variable this command reads -- held against primer.yaml's env by
# tests/test_primer_yaml.py.
READS: tuple[str, ...] = (TOKEN_VARIABLE, LINK_HOST_VARIABLE)

# Seconds getMe may take, the telegram channel's client total.
GET_ME_TIMEOUT: float = channel_spec("telegram").client_timeouts["timeout"]

# A host name and nothing more: lowercase labels, at least two, no scheme,
# port, path or address -- the form the link is built on.
_HOST = re.compile(r"(?=.{1,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}")


class DeriveRefusedError(Exception):
    """A value cannot be derived. `reason` says why, a fixed text of this
    module that never carries a secret -- the one thing of the refusal
    that is printed."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _token(environ: Mapping[str, str]) -> str:
    """TELEGRAM_BOT_TOKEN, held to the form startup holds it to."""
    token = environ.get(TOKEN_VARIABLE, "")
    if token == "":
        raise DeriveRefusedError(f"{TOKEN_VARIABLE} is not set")
    problem = channel_spec("telegram").validators[TOKEN_VARIABLE](token)
    if problem is not None:
        raise DeriveRefusedError(f"{TOKEN_VARIABLE} is malformed: {problem}")
    return token


def _link_host(environ: Mapping[str, str]) -> str:
    host = environ.get(LINK_HOST_VARIABLE, "")
    if host == "":
        raise DeriveRefusedError(f"{LINK_HOST_VARIABLE} is not set")
    if not _HOST.fullmatch(host):
        raise DeriveRefusedError(
            f"{LINK_HOST_VARIABLE} is not a host name: lowercase labels "
            f"joined by dots, no scheme, port or path"
        )
    return host


def derive_bot_id(environ: Mapping[str, str]) -> str:
    """The bot's id, from the token alone."""
    from aiogram.utils.token import extract_bot_id

    return str(extract_bot_id(_token(environ)))


async def derive_bot_url(
    environ: Mapping[str, str],
    *,
    client_timeout: float = GET_ME_TIMEOUT,
    api: "TelegramAPIServer | None" = None,
) -> str:
    """The bot's link, its username asked of Telegram.

    `api` is the Bot API server (aiogram's default when None): the suite
    points it at a local one.
    """
    token = _token(environ)
    host = _link_host(environ)

    from aiogram import Bot
    from aiogram.client.session.aiohttp import AiohttpSession
    from aiogram.exceptions import (
        AiogramError,
        TelegramAPIError,
        TelegramNetworkError,
        TelegramUnauthorizedError,
    )

    session = (
        AiohttpSession(timeout=client_timeout)
        if api is None
        else AiohttpSession(api=api, timeout=client_timeout)
    )
    try:
        bot = Bot(token=token, session=session)
        try:
            me = await bot.get_me()
        except TelegramUnauthorizedError:
            raise DeriveRefusedError(
                f"Telegram refused {TOKEN_VARIABLE} (getMe: unauthorized)"
            ) from None
        except TelegramNetworkError:
            raise DeriveRefusedError(
                f"Telegram was not reached: getMe timed out or the connection "
                f"failed (client timeout {client_timeout:g} s)"
            ) from None
        except TelegramAPIError as error:
            raise DeriveRefusedError(
                f"Telegram answered getMe with an error "
                f"({type(error).__name__})"
            ) from None
        except AiogramError as error:
            # aiogram's own: an answer it could not read as the Bot API's.
            raise DeriveRefusedError(
                f"Telegram's answer to getMe could not be read "
                f"({type(error).__name__})"
            ) from None
    finally:
        await session.close()
    if not me.username:
        raise DeriveRefusedError("Telegram's getMe answer carries no username")
    url = urlunsplit(("https", host, f"/{me.username}", "", ""))
    problem = channel_spec("telegram").validators[URL_VARIABLE](url)
    if problem is not None:
        raise DeriveRefusedError(f"the derived {URL_VARIABLE} is malformed")
    return url


def _bot_url(environ: Mapping[str, str]) -> str:
    return asyncio.run(derive_bot_url(environ))


_VALUES: dict[str, Callable[[Mapping[str, str]], str]] = {
    "bot-id": derive_bot_id,
    "bot-url": _bot_url,
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="comms")
    commands = parser.add_subparsers(dest="command", required=True)
    derive = commands.add_parser(
        "derive", help="print one derived value of the instance",
    )
    derive.add_argument("name", choices=sorted(_VALUES))
    return parser


def main(
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    """`comms derive <name>`: the value on stdout and 0, or one line on
    stderr and 1 (2: a wrong command line)."""
    args = _parser().parse_args(argv)
    source = os.environ if environ is None else environ
    try:
        value = _VALUES[args.name](source)
    except DeriveRefusedError as refused:
        print(f"comms derive {args.name}: {refused.reason}", file=sys.stderr)
        return 1
    except Exception as error:
        # Any other failure -- an answer aiogram cannot read, say -- by its
        # class alone: its text may carry the request's URL, and the token.
        print(
            f"comms derive {args.name}: failed ({type(error).__name__})",
            file=sys.stderr,
        )
        return 1
    print(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
