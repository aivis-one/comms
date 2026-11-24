"""L-2: POST /api/v1/login/start (app/api/login.py).

The suite's application is built with the option off (tests/conftest.py),
so the route is not in it -- exactly what a deploy without `login` has;
the flag's include itself is held by the guard's twin
(tests/test_login_switched_off.py TestTheRoutesWithLoginOn). Here the
route's behaviour: `login_on` includes its router in the suite's
application for one test and takes it back after (the first test holds
that the harness does both), with the bot's token the option needs.

Held: the answer and its fields; the authorize URL with every parameter
of the form's appendix A, the scope with telegram:bot_access only when
asked, the challenge S256 of the verifier kept; the state 43 characters
of base64url and only its SHA-256 in the database; a 422 `validation`
naming the field for a body that is not the contract's, nothing kept;
the state and the verifier in no log line; the client id from the one
reader of the bot's id.

Mutations: the scope always with bot_access (M7), the challenge plain
(M8), a fragment or http taken (M10), a client id parsed a second time
(M12) -- each red here.
"""

import base64
import hashlib
import re
from collections.abc import Iterator
from datetime import timedelta
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from structlog.testing import capture_logs

from app.api import login as login_api
from app.core.config import TELEGRAM_LOGIN_AUTHORIZE_URL, settings
from app.core.database import get_session_factory
from app.login.attempts import ATTEMPT_LIFETIME, REDIRECT_URI_MAX
from app.login.models import LoginAttempt
from app.main import app

# Assembled at run time (§0 §10).
_BOT_ID = "123456"
_TOKEN = _BOT_ID + ":" + "unit-test-login-start"
_RETURN = "https://product.example.org/auth/telegram/return"
_PATH = "/api/v1/login/start"
_STATE = re.compile(r"[A-Za-z0-9_-]{43}")


@pytest.fixture
def login_on(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """The option on for one test: the router in the application, the
    bot's token set; both taken back after."""
    monkeypatch.setattr(settings, "telegram_bot_token", _TOKEN)
    routes = app.router.routes
    before = list(routes)
    app.include_router(login_api.router)
    yield
    routes[:] = before


async def _rows() -> list[dict[str, Any]]:
    async with get_session_factory()() as session:
        rows = (await session.execute(select(LoginAttempt))).scalars().all()
        return [
            {c.name: getattr(row, c.name) for c in LoginAttempt.__table__.columns}
            for row in rows
        ]


async def _start(client: AsyncClient, **body: Any) -> Any:
    return await client.post(
        _PATH, json={"provider": "telegram", "redirect_uri": _RETURN, **body},
    )


def _s256(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


class TestTheHarness:
    async def test_the_router_comes_and_goes(self, client: AsyncClient) -> None:
        """The first test (gate L-2 §6): the suite's application has no
        start; included, it answers; taken back, it is gone again."""
        assert (await _start(client)).status_code == 404
        routes = app.router.routes
        before = list(routes)
        app.include_router(login_api.router)
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(settings, "telegram_bot_token", _TOKEN)
                assert (await _start(client)).status_code == 200
        finally:
            routes[:] = before
        gone = await _start(client)
        assert gone.status_code == 404
        assert gone.json()["error"]["class"] == "not_found"


@pytest.mark.usefixtures("login_on")
class TestTheAnswer:
    async def test_the_answer_and_the_row(self, client: AsyncClient) -> None:
        resp = await _start(client)
        assert resp.status_code == 200, resp.text
        answer = resp.json()
        assert sorted(answer) == ["authorize_url", "expires_at", "state"]
        state = answer["state"]
        assert _STATE.fullmatch(state)

        (row,) = await _rows()
        # Only the fingerprint: the state is in no column.
        assert row["state_hash"] == hashlib.sha256(state.encode()).hexdigest()
        assert all(state not in str(value) for value in row.values())
        assert row["provider"] == "telegram"
        assert row["redirect_uri"] == _RETURN
        assert row["bot_access"] is False
        assert row["taken_at"] is None
        assert len(row["code_verifier"]) == 64
        # The contract's term: 10 minutes from the start.
        assert timedelta(minutes=10) == ATTEMPT_LIFETIME
        assert row["expires_at"] - row["created_at"] == ATTEMPT_LIFETIME
        assert answer["expires_at"] == row["expires_at"].isoformat()

        url = urlsplit(answer["authorize_url"])
        assert f"{url.scheme}://{url.netloc}{url.path}" == TELEGRAM_LOGIN_AUTHORIZE_URL
        query = parse_qs(url.query, strict_parsing=True)
        assert {k: v for k, v in query.items()} == {
            "client_id": [_BOT_ID],
            "redirect_uri": [_RETURN],
            "response_type": ["code"],
            "scope": ["openid profile"],
            "state": [state],
            "code_challenge": [_s256(row["code_verifier"])],
            "code_challenge_method": ["S256"],
        }

    @pytest.mark.parametrize(("sent", "scope"), [
        ({}, "openid profile"),
        ({"bot_access": False}, "openid profile"),
        ({"bot_access": True}, "openid profile telegram:bot_access"),
    ], ids=["absent", "false", "true"])
    async def test_the_scope_asks_bot_access_only_when_asked(
        self, client: AsyncClient, sent: dict[str, Any], scope: str,
    ) -> None:
        resp = await _start(client, **sent)
        assert resp.status_code == 200, resp.text
        query = parse_qs(urlsplit(resp.json()["authorize_url"]).query)
        assert query["scope"] == [scope]
        (row,) = await _rows()
        assert row["bot_access"] is sent.get("bot_access", False)

    async def test_two_starts_are_two_attempts(self, client: AsyncClient) -> None:
        """REPEAT: the same body twice -- two attempts, two states."""
        first, second = await _start(client), await _start(client)
        assert first.json()["state"] != second.json()["state"]
        assert len(await _rows()) == 2

    async def test_the_longest_return_address(self, client: AsyncClient) -> None:
        longest = _RETURN + "?" + "a" * (REDIRECT_URI_MAX - len(_RETURN) - 1)
        assert len(longest) == REDIRECT_URI_MAX
        assert (await _start(client, redirect_uri=longest)).status_code == 200

    async def test_the_client_id_is_the_one_readers(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """client_id comes from app/core/channels.py telegram_bot_id, not a
        parse of its own: replaced, the URL carries the replacement."""
        monkeypatch.setattr(login_api, "telegram_bot_id", lambda token: "777")
        resp = await _start(client)
        query = parse_qs(urlsplit(resp.json()["authorize_url"]).query)
        assert query["client_id"] == ["777"]

    async def test_no_secret_in_a_log_or_a_repr(self, client: AsyncClient) -> None:
        with capture_logs() as logs:
            resp = await _start(client)
        state = resp.json()["state"]
        (row,) = await _rows()
        logged = repr(logs)
        assert state not in logged
        assert row["code_verifier"] not in logged
        async with get_session_factory()() as session:
            orm_row = (await session.execute(select(LoginAttempt))).scalar_one()
        assert row["code_verifier"] not in repr(orm_row)
        assert row["state_hash"] not in repr(orm_row)


_TOO_LONG = _RETURN + "?" + "a" * (REDIRECT_URI_MAX - len(_RETURN))
_MARK = "unit-mark-of-the-value"


@pytest.mark.usefixtures("login_on")
class TestTheRefusal:
    @pytest.mark.parametrize(("body", "field"), [
        ({"provider": "Telegram"}, "provider"),
        ({"provider": "other"}, "provider"),
        ({"provider": 1}, "provider"),
        ({"provider": None}, "provider"),
        ({"redirect_uri": f"http://{_MARK}.example.org/r"}, "redirect_uri"),
        ({"redirect_uri": f"{_MARK}.example.org/r"}, "redirect_uri"),
        ({"redirect_uri": "https:///r"}, "redirect_uri"),
        ({"redirect_uri": f"https://h.example.org/r#{_MARK}"}, "redirect_uri"),
        ({"redirect_uri": "https://h.example.org/r#"}, "redirect_uri"),
        ({"redirect_uri": _TOO_LONG}, "redirect_uri"),
        ({"redirect_uri": ""}, "redirect_uri"),
        ({"redirect_uri": 5}, "redirect_uri"),
        ({"redirect_uri": "https://[::1/r"}, "redirect_uri"),
        ({"bot_access": "true"}, "bot_access"),
        ({"bot_access": 1}, "bot_access"),
        ({"bot_access": None}, "bot_access"),
        ({"extra": True}, "extra"),
    ], ids=[
        "provider-case", "provider-other", "provider-not-a-string",
        "provider-null", "http", "no-scheme", "no-host", "fragment",
        "empty-fragment", "too-long", "empty", "not-a-string",
        "unparseable", "bot_access-string", "bot_access-int",
        "bot_access-null", "extra-field",
    ])
    async def test_a_body_not_the_contracts_is_a_422_naming_it(
        self, client: AsyncClient, body: dict[str, Any], field: str,
    ) -> None:
        resp = await _start(client, **body)
        assert resp.status_code == 422, resp.text
        error = resp.json()["error"]
        assert error["class"] == "validation"
        assert any(field in f["loc"] for f in error["fields"]), error
        assert _MARK not in resp.text
        assert await _rows() == []

    @pytest.mark.parametrize("missing", ["provider", "redirect_uri"])
    async def test_a_missing_field_is_named(
        self, client: AsyncClient, missing: str,
    ) -> None:
        body = {"provider": "telegram", "redirect_uri": _RETURN}
        del body[missing]
        resp = await client.post(_PATH, json=body)
        assert resp.status_code == 422
        assert any(missing in f["loc"] for f in resp.json()["error"]["fields"])
        assert await _rows() == []


async def test_with_the_option_off_there_is_no_start(client: AsyncClient) -> None:
    """The suite's application, as a deploy without `login`: 404
    not_found, nothing kept. The pair: the option is off here."""
    assert settings.login_enabled == "false"
    resp = await _start(client)
    assert resp.status_code == 404
    assert resp.json()["error"]["class"] == "not_found"
    assert await _rows() == []
