"""`comms derive bot-id` and `comms derive bot-url` (W2-2, app/cli.py).

The machine takes stdout as the value and passes stderr on: so every
refusal is held to say nothing of the token, in either stream, and every
value to be one line. bot-url is asked of a Bot API server on this host
(aiogram's own session, pointed at it) -- the real client, its real
timeout, no network. The command line itself, through the image's
launcher (bin/comms), runs in a subprocess: bot-url's entry calls
asyncio.run, which must not run inside the suite's loop. The subprocess
gets no DATABASE_URL and no settings of the service -- the environment of
the machine's throwaway container is the instance's values only.

The tokens are assembled at run time: a literal of a real token's shape
would trip secret scanning.
"""

import asyncio
import json
import os
import socket
import subprocess
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiohttp import web

from app import cli
from app.cli import DeriveRefusedError, derive_bot_id, derive_bot_url, main

_REPO = Path(__file__).resolve().parents[1]
_LAUNCHER = _REPO / "bin" / "comms"

_BOT_ID = "123456789"
_SECRET = "Az" * 17 + "q"
_TOKEN = f"{_BOT_ID}:{_SECRET}"
_BAD_TOKEN = "not-a-token-" + "C" * 20
_HOST = "telegram.me"
_USERNAME = "comms_unit_bot"

# A bound on the whole call around a short client timeout: past it the
# client's own timeout did not fire.
_OUTER = 5.0

# aiogram's transport as it is, taken at import -- before the suite's
# network tripwire (tests/conftest.py network_tripwire, a session fixture)
# replaces it for every test.
_TRANSPORT = AiohttpSession.make_request
_LOCAL = "http://127.0.0.1:"


@pytest.fixture
def local_bot_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """aiogram's real transport, for a Bot API server on this host only:
    a request anywhere else still trips the wire."""

    async def transport(
        self: AiohttpSession, *args: object, **kwargs: object,
    ) -> object:
        if not self.api.base.startswith(_LOCAL):
            raise RuntimeError("network tripwire: a test reached the real Telegram API")
        return await _TRANSPORT(self, *args, **kwargs)

    monkeypatch.setattr(AiohttpSession, "make_request", transport)


def _environ(**values: str) -> dict[str, str]:
    return {"TELEGRAM_BOT_TOKEN": _TOKEN, "TELEGRAM_LINK_HOST": _HOST, **values}


# ---------------------------------------------------------------------------
# a Bot API server on this host
# ---------------------------------------------------------------------------

_Answer = Callable[[web.Request], Awaitable[web.StreamResponse]]


@asynccontextmanager
async def _serve(answer: _Answer) -> AsyncIterator[TelegramAPIServer]:
    app = web.Application()
    app.router.add_post("/{path:.*}", answer)
    runner = web.AppRunner(app)
    await runner.setup()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    site = web.SockSite(runner, sock)
    await site.start()
    port = sock.getsockname()[1]
    try:
        yield TelegramAPIServer.from_base(f"http://127.0.0.1:{port}")
    finally:
        await runner.cleanup()


def _json(status: int, body: dict[str, object]) -> _Answer:
    async def answer(request: web.Request) -> web.StreamResponse:
        assert request.path == f"/bot{_TOKEN}/getMe"
        return web.Response(
            status=status, text=json.dumps(body), content_type="application/json",
        )

    return answer


_ME = {
    "ok": True,
    "result": {"id": int(_BOT_ID), "is_bot": True, "first_name": "comms",
               "username": _USERNAME},
}


async def _derive_from(answer: _Answer, environ: dict[str, str] | None = None) -> str:
    async with _serve(answer) as api:
        return await asyncio.wait_for(
            derive_bot_url(_environ() if environ is None else environ, api=api),
            _OUTER,
        )


def _assert_no_token(text: str) -> None:
    assert _SECRET not in text
    assert _BAD_TOKEN not in text


# ---------------------------------------------------------------------------
# bot-id
# ---------------------------------------------------------------------------


class TestBotId:
    def test_the_digits_before_the_colon(self) -> None:
        assert derive_bot_id(_environ()) == _BOT_ID

    def test_a_secret_with_a_colon_keeps_the_first_part(self) -> None:
        """aiogram's own reading of a token: the id is what precedes the
        first colon, the rest is the secret whatever it holds."""
        assert derive_bot_id(_environ(TELEGRAM_BOT_TOKEN=f"{_BOT_ID}:a:b")) == _BOT_ID

    @pytest.mark.parametrize(
        ("token", "said"),
        [
            (None, "TELEGRAM_BOT_TOKEN is not set"),
            ("", "TELEGRAM_BOT_TOKEN is not set"),
            ("   ", "TELEGRAM_BOT_TOKEN is malformed"),
            (_BAD_TOKEN, "TELEGRAM_BOT_TOKEN is malformed"),
            (f"{_BOT_ID}:", "TELEGRAM_BOT_TOKEN is malformed"),
            (f"x{_BOT_ID}:{_SECRET}", "TELEGRAM_BOT_TOKEN is malformed"),
        ],
        ids=["unset", "empty", "spaces", "no-colon", "no-secret", "id-not-digits"],
    )
    def test_a_token_it_cannot_read_is_refused(
        self, token: str | None, said: str,
    ) -> None:
        environ = _environ() if token is None else _environ(TELEGRAM_BOT_TOKEN=token)
        if token is None:
            del environ["TELEGRAM_BOT_TOKEN"]
        with pytest.raises(DeriveRefusedError) as refused:
            derive_bot_id(environ)
        assert refused.value.reason.startswith(said)
        _assert_no_token(refused.value.reason)
        if token:
            assert token.strip() == "" or token not in refused.value.reason


# ---------------------------------------------------------------------------
# bot-url
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("local_bot_api")
class TestBotUrl:
    async def test_the_link_on_the_host_with_the_username(self) -> None:
        url = await _derive_from(_json(200, _ME))
        assert url == f"https://{_HOST}/{_USERNAME}"

    async def test_an_unauthorized_token_is_refused(self) -> None:
        body = {"ok": False, "error_code": 401, "description": "Unauthorized"}
        with pytest.raises(DeriveRefusedError) as refused:
            await _derive_from(_json(401, body))
        assert "Telegram refused TELEGRAM_BOT_TOKEN" in refused.value.reason
        _assert_no_token(refused.value.reason)

    async def test_an_error_of_telegram_is_refused(self) -> None:
        body = {"ok": False, "error_code": 500, "description": "Internal"}
        with pytest.raises(DeriveRefusedError) as refused:
            await _derive_from(_json(500, body))
        assert refused.value.reason.startswith("Telegram answered getMe with an error")
        _assert_no_token(refused.value.reason)

    async def test_an_answer_it_cannot_read_is_refused(self) -> None:
        async def garbage(request: web.Request) -> web.StreamResponse:
            return web.Response(status=200, text="not json")

        with pytest.raises(DeriveRefusedError) as refused:
            await _derive_from(garbage)
        assert refused.value.reason.startswith("Telegram's answer to getMe")
        _assert_no_token(refused.value.reason)

    async def test_telegram_silent_is_refused_within_the_client_timeout(self) -> None:
        """A server that takes the connection and never answers: the
        client's own timeout ends the call -- the outer bound of the test
        is never reached. A listening socket nobody accepts on is such a
        server: the kernel completes the handshake."""
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen(8)
        api = TelegramAPIServer.from_base(f"http://127.0.0.1:{sock.getsockname()[1]}")
        try:
            with pytest.raises(DeriveRefusedError) as refused:
                await asyncio.wait_for(
                    derive_bot_url(_environ(), api=api, client_timeout=0.5),
                    _OUTER,
                )
        finally:
            sock.close()
        assert refused.value.reason.startswith("Telegram was not reached")
        assert "client timeout 0.5 s" in refused.value.reason
        _assert_no_token(refused.value.reason)

    async def test_the_suite_never_reaches_telegram(self) -> None:
        """The pair of the fixture: with aiogram's own server the request
        is the tripwire's, not Telegram's."""
        with pytest.raises(RuntimeError, match="network tripwire"):
            await derive_bot_url(_environ())

    def test_the_timeout_is_the_telegram_channels(self) -> None:
        """One number for every call to Telegram: the channel's client
        timeout (app/core/channels.py), below the machine's 300 s."""
        from app.core.channels import channel_spec

        telegram = channel_spec("telegram").client_timeouts["timeout"]
        assert telegram == cli.GET_ME_TIMEOUT
        assert 0 < cli.GET_ME_TIMEOUT < 300

    @pytest.mark.parametrize(
        ("host", "said"),
        [
            (None, "TELEGRAM_LINK_HOST is not set"),
            ("", "TELEGRAM_LINK_HOST is not set"),
            ("https://telegram.me", "TELEGRAM_LINK_HOST is not a host name"),
            ("telegram.me/x", "TELEGRAM_LINK_HOST is not a host name"),
            ("telegram.me:443", "TELEGRAM_LINK_HOST is not a host name"),
            ("Telegram.me", "TELEGRAM_LINK_HOST is not a host name"),
            ("localhost", "TELEGRAM_LINK_HOST is not a host name"),
        ],
        ids=["unset", "empty", "scheme", "path", "port", "upper", "one-label"],
    )
    async def test_a_host_it_cannot_build_on_is_refused_before_telegram(
        self, host: str | None, said: str,
    ) -> None:
        """Refused before any call: the server here fails every request."""

        async def never(request: web.Request) -> web.StreamResponse:
            raise AssertionError("Telegram asked with a host that is no host")

        environ = _environ() if host is None else _environ(TELEGRAM_LINK_HOST=host)
        if host is None:
            del environ["TELEGRAM_LINK_HOST"]
        with pytest.raises(DeriveRefusedError) as refused:
            await _derive_from(never, environ)
        assert refused.value.reason.startswith(said)

    async def test_a_token_it_cannot_read_is_refused_before_telegram(self) -> None:
        async def never(request: web.Request) -> web.StreamResponse:
            raise AssertionError("Telegram asked with a malformed token")

        with pytest.raises(DeriveRefusedError) as refused:
            await _derive_from(never, _environ(TELEGRAM_BOT_TOKEN=_BAD_TOKEN))
        assert refused.value.reason.startswith("TELEGRAM_BOT_TOKEN is malformed")
        _assert_no_token(refused.value.reason)


# ---------------------------------------------------------------------------
# the command line: what reaches stdout and stderr
# ---------------------------------------------------------------------------


class TestMain:
    """main() in this process for bot-id (no event loop on its path)."""

    def test_the_value_alone_on_stdout(
        self, capsys: pytest.CaptureFixture[str],
    ) -> None:
        assert main(["derive", "bot-id"], _environ()) == 0
        out, err = capsys.readouterr()
        assert (out, err) == (f"{_BOT_ID}\n", "")

    def test_a_refusal_is_one_line_on_stderr(
        self, capsys: pytest.CaptureFixture[str],
    ) -> None:
        assert main(["derive", "bot-id"], _environ(TELEGRAM_BOT_TOKEN=_BAD_TOKEN)) == 1
        out, err = capsys.readouterr()
        assert out == ""
        assert err.startswith("comms derive bot-id: TELEGRAM_BOT_TOKEN is malformed")
        assert err.count("\n") == 1
        _assert_no_token(out + err)

    def test_any_other_failure_by_its_class_alone(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A failure no refusal names -- its text carrying the token, as
        aiohttp's does with the request's URL -- is printed by its class."""

        def leaks(environ: object) -> str:
            raise RuntimeError(f"POST https://example.org/bot{_TOKEN}/getMe")

        monkeypatch.setitem(cli._VALUES, "bot-id", leaks)
        assert main(["derive", "bot-id"], _environ()) == 1
        out, err = capsys.readouterr()
        assert (out, err) == ("", "comms derive bot-id: failed (RuntimeError)\n")


def _run(*args: str, **values: str) -> subprocess.CompletedProcess[str]:
    """The launcher of the image in a process of its own: the instance's
    values only, as in the machine's container -- no DATABASE_URL, no
    service token, no channel flag."""
    environ = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(_REPO),
        **values,
    }
    return subprocess.run(
        [sys.executable, str(_LAUNCHER), *args],
        env=environ, capture_output=True, text=True, timeout=60, check=False,
    )


class TestTheLauncher:
    def test_bot_id(self) -> None:
        done = _run("derive", "bot-id", TELEGRAM_BOT_TOKEN=_TOKEN)
        assert (done.returncode, done.stdout, done.stderr) == (0, f"{_BOT_ID}\n", "")

    def test_bot_id_of_a_malformed_token(self) -> None:
        done = _run("derive", "bot-id", TELEGRAM_BOT_TOKEN=_BAD_TOKEN)
        assert done.returncode == 1
        assert done.stdout == ""
        assert "TELEGRAM_BOT_TOKEN is malformed" in done.stderr
        _assert_no_token(done.stdout + done.stderr)

    def test_bot_url_of_a_malformed_token(self) -> None:
        """Refused before any network, through asyncio.run."""
        done = _run(
            "derive", "bot-url",
            TELEGRAM_BOT_TOKEN=_BAD_TOKEN, TELEGRAM_LINK_HOST=_HOST,
        )
        assert done.returncode == 1
        assert done.stdout == ""
        assert "comms derive bot-url: TELEGRAM_BOT_TOKEN is malformed" in done.stderr
        _assert_no_token(done.stdout + done.stderr)

    def test_bot_url_without_a_host(self) -> None:
        done = _run("derive", "bot-url", TELEGRAM_BOT_TOKEN=_TOKEN)
        assert (done.returncode, done.stdout) == (1, "")
        assert done.stderr == "comms derive bot-url: TELEGRAM_LINK_HOST is not set\n"

    @pytest.mark.parametrize(
        "args", [(), ("derive",), ("derive", "bot-name"), ("derive", "bot-id", "x")],
        ids=["nothing", "no-name", "unknown-name", "extra"],
    )
    def test_a_wrong_command_line(self, args: tuple[str, ...]) -> None:
        done = _run(*args, TELEGRAM_BOT_TOKEN=_TOKEN)
        assert (done.returncode, done.stdout) == (2, "")
        assert done.stderr.startswith("usage: comms")
        _assert_no_token(done.stderr)
