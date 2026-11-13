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
#
# `comms count` (W6-1) prints how many conversations and messages the
# database holds, one line -- `threads=<n> messages=<n>` -- for the count
# taken before and after a move of the database (protocol 11.2). Its SQL is
# one statement, written out in count_conversations' docstring, so the same
# count can be taken with psql where this command is not (a box still on
# 3.4.0). It works on every schema from 0025 on: both tables are there with
# those names.
#   READ ONLY, by the server: the session has default_transaction_read_only,
#   so the count cannot write whatever the statement says.
#   NO SETTINGS here either: DATABASE_URL is read as the application reads
#   it, and nothing else -- the whole settings refuse a process without the
#   service token. Its refusals are the class of the failure, never its
#   text: asyncpg's text names the host, the port and the user, and a URL
#   SQLAlchemy cannot parse is quoted whole, its password with it.
#   BOUNDED: COUNT_CONNECT_TIMEOUT for the connection, COUNT_TIMEOUT for the
#   whole count -- a database that does not answer is a refusal, not a hang.
# =============================================================================

import argparse
import asyncio
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING
from urllib.parse import urlunsplit

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.channels import channel_spec

if TYPE_CHECKING:
    from aiogram.client.telegram import TelegramAPIServer

TOKEN_VARIABLE = "TELEGRAM_BOT_TOKEN"
LINK_HOST_VARIABLE = "TELEGRAM_LINK_HOST"
URL_VARIABLE = "TELEGRAM_BOT_URL"

# Every variable `comms derive` reads -- held against primer.yaml's env by
# tests/test_primer_yaml.py. `comms count` reads DATABASE_URL_VARIABLE, a
# setting compose builds (tests/test_count.py holds it to the settings).
READS: tuple[str, ...] = (TOKEN_VARIABLE, LINK_HOST_VARIABLE)
DATABASE_URL_VARIABLE = "DATABASE_URL"

# Seconds `comms count` waits for the connection, and for the whole count.
COUNT_CONNECT_TIMEOUT: float = 10.0
COUNT_TIMEOUT: float = 60.0

# The count, one statement, one snapshot -- count_conversations' docstring
# shows it to the reader (tests/test_count.py holds the two equal).
COUNT_SQL = (
    "SELECT (SELECT count(*) FROM threads), (SELECT count(*) FROM messages)"
)

# Seconds getMe may take, the telegram channel's client total.
GET_ME_TIMEOUT: float = channel_spec("telegram").client_timeouts["timeout"]

# A host name and nothing more: lowercase labels, at least two, no scheme,
# port, path or address -- the form the link is built on.
_HOST = re.compile(r"(?=.{1,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}")


class RefusedError(Exception):
    """The command refuses. `reason` says why, a fixed text of this
    module that never carries a secret -- the one thing of the refusal
    that is printed."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class DeriveRefusedError(RefusedError):
    """A value cannot be derived."""


class CountRefusedError(RefusedError):
    """The database cannot be counted."""


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


def count_engine(
    url: str, *, connect_timeout: float = COUNT_CONNECT_TIMEOUT,
) -> AsyncEngine:
    """The engine of one count: no pool, no bound value in an error's
    text, every transaction of its session read only by the server."""
    return create_async_engine(
        url,
        hide_parameters=True,
        poolclass=NullPool,
        connect_args={
            "timeout": connect_timeout,
            "server_settings": {
                "application_name": "comms count",
                "default_transaction_read_only": "on",
            },
        },
    )


async def count_conversations(
    url: str,
    *,
    connect_timeout: float = COUNT_CONNECT_TIMEOUT,
    whole: float = COUNT_TIMEOUT,
) -> tuple[int, int]:
    """(threads, messages): every row of each table, in one statement --

        SELECT (SELECT count(*) FROM threads), (SELECT count(*) FROM messages)

    -- read only, the connection bounded by `connect_timeout` and the whole
    count by `whole` seconds (TimeoutError past it)."""
    engine = count_engine(url, connect_timeout=connect_timeout)
    try:
        async with asyncio.timeout(whole), engine.connect() as connection:
            row = (await connection.execute(text(COUNT_SQL))).one()
    finally:
        await engine.dispose()
    return int(row[0]), int(row[1])


def count(environ: Mapping[str, str]) -> str:
    """`comms count`'s line: `threads=<n> messages=<n>`."""
    url = environ.get(DATABASE_URL_VARIABLE, "")
    if url == "":
        raise CountRefusedError(f"{DATABASE_URL_VARIABLE} is not set")
    try:
        threads, messages = asyncio.run(count_conversations(url))
    except TimeoutError:
        raise CountRefusedError(
            f"the database did not answer in time (connection "
            f"{COUNT_CONNECT_TIMEOUT:g} s, count {COUNT_TIMEOUT:g} s)",
        ) from None
    return f"threads={threads} messages={messages}"


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
    commands.add_parser(
        "count", help="print how many threads and messages the database holds",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    """`comms derive <name>` / `comms count`: the value on stdout and 0,
    or one line on stderr and 1 (2: a wrong command line)."""
    args = _parser().parse_args(argv)
    source = os.environ if environ is None else environ
    produce: Callable[[Mapping[str, str]], str]
    if args.command == "count":
        label, produce = "comms count", count
    else:
        label, produce = f"comms derive {args.name}", _VALUES[args.name]
    try:
        value = produce(source)
    except RefusedError as refused:
        print(f"{label}: {refused.reason}", file=sys.stderr)
        return 1
    except Exception as error:
        # Any other failure -- an answer aiogram cannot read, a database
        # not reached -- by its class alone: its text may carry the
        # request's URL and the token, or the database's address, user and
        # password.
        print(f"{label}: failed ({type(error).__name__})", file=sys.stderr)
        return 1
    print(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
