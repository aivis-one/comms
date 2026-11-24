"""L-3b: POST /api/v1/login/complete (app/api/login.py, app/login/complete.py,
app/login/telegram.py).

The route is included in the suite's application for each test (the
suite runs with `login` off, as a deploy without it); Telegram is a
MockTransport (app/login/telegram.py new_client replaced) -- the suite's
tripwire stays under every other client; the keys are the test's own.

Held: the body's form before the take; the take's three refusals; the
exchange and the keys, each answer of Telegram to its class -- the live
refusal at 200 by its body; the answer of a good token in the form of §3,
verified at the one moment read; every IdTokenRejected a 502; a repeat
after any outcome a 409, even while the first is running; no request to
Telegram inside a transaction of the database; the keys fetched for every
sign-in; each class with the status of its row of the contract's table;
one log line per outcome, and none of the secrets in it or in an answer.

Mutations M1-M12 of the gate -- each red here (and M6 in the guard,
tests/test_login_switched_off.py).
"""

import asyncio
import base64
import json
import re
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from httpx import AsyncClient
from jwt.algorithms import RSAAlgorithm
from sqlalchemy import select, text
from structlog.testing import capture_logs

from app.api import login as login_api
from app.api.errors import ErrorClass
from app.core.config import (
    TELEGRAM_LOGIN_ISSUER,
    TELEGRAM_LOGIN_JWKS_URL,
    TELEGRAM_LOGIN_TOKEN_URL,
    settings,
)
from app.core.database import get_session_factory
from app.login import complete as complete_module
from app.login import telegram
from app.login.complete import Refusal, SignInRefused
from app.login.identity import IdTokenRejected
from app.login.models import LoginAttempt
from app.main import app

_BOT_ID = "123456"
_TOKEN = _BOT_ID + ":" + "unit-test-login-complete"
_SECRET = "unit" + "-client-secret-" + "mark"
_CODE = "unit-code-mark-" + "x" * 20
_RETURN = "https://product.example.org/auth/telegram/return"
_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_KID = "oidc-1"
_TELEGRAM_ID = "987654321"
_DOC = (Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md").read_text(
    encoding="utf-8",
)


def _jwks() -> dict[str, Any]:
    public = json.loads(RSAAlgorithm.to_jwk(_KEY.public_key()))
    return {"keys": [{"kty": "RSA", "kid": _KID, "n": public["n"], "e": public["e"]}]}


def _id_token(at: datetime | None = None, **changes: Any) -> str:
    stamp = int((at or datetime.now(UTC)).timestamp())
    claims = {
        "iss": TELEGRAM_LOGIN_ISSUER, "aud": _BOT_ID, "exp": stamp + 300,
        "iat": stamp - 5, "sub": "unit-sub", "id": _TELEGRAM_ID,
        "given_name": "Ada", "family_name": "Lovelace",
        "preferred_username": "ada_mark",
        "picture": "https://cdn.example.org/ada.jpg",
        **changes,
    }
    return jwt.encode(claims, _KEY, algorithm="RS256", headers={"kid": _KID})


Handler = Callable[[httpx.Request], Awaitable[httpx.Response]]


class Telegram:
    """The test's Telegram: what each endpoint answers, what it was asked."""

    def __init__(self) -> None:
        self.token: Handler = self._good_token
        self.jwks: Handler = self._good_jwks
        self.requests: list[httpx.Request] = []
        self.exchange_body: dict[str, Any] = {}

    async def _good_token(self, request: httpx.Request) -> httpx.Response:
        body = {"access_token": "unit-access-mark", "expires_in": 3600,
                "id_token": _id_token(), "scope": "openid profile",
                "token_type": "Bearer", **self.exchange_body}
        return httpx.Response(200, json=body)

    async def _good_jwks(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_jwks())

    async def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if str(request.url) == TELEGRAM_LOGIN_TOKEN_URL:
            return await self.token(request)
        if str(request.url) == TELEGRAM_LOGIN_JWKS_URL:
            return await self.jwks(request)
        raise AssertionError(f"an address that is not Telegram's: {request.url}")

    def asked(self, url: str) -> list[httpx.Request]:
        return [r for r in self.requests if str(r.url) == url]


@pytest.fixture
def tg(monkeypatch: pytest.MonkeyPatch) -> Iterator[Telegram]:
    """The option on for one test: both routes in the application, the
    bot's token and the client's secret set, Telegram the test's."""
    fake = Telegram()
    monkeypatch.setattr(settings, "telegram_bot_token", _TOKEN)
    monkeypatch.setattr(settings, "telegram_login_client_secret", _SECRET)
    monkeypatch.setattr(
        telegram, "new_client",
        lambda: httpx.AsyncClient(transport=httpx.MockTransport(fake.handle)),
    )
    routes = app.router.routes
    before = list(routes)
    app.include_router(login_api.router)
    yield fake
    routes[:] = before


async def _start(client: AsyncClient, bot_access: bool = False) -> str:
    resp = await client.post("/api/v1/login/start", json={
        "provider": "telegram", "redirect_uri": _RETURN, "bot_access": bot_access,
    })
    assert resp.status_code == 200, resp.text
    state: str = resp.json()["state"]
    return state


async def _complete(client: AsyncClient, state: str, code: str = _CODE) -> Any:
    return await client.post("/api/v1/login/complete",
                             json={"state": state, "code": code})


async def _row() -> LoginAttempt:
    async with get_session_factory()() as session:
        return (await session.execute(select(LoginAttempt))).scalar_one()


def _class(resp: Any) -> str:
    return str(resp.json()["error"]["class"])


# -----------------------------------------------------------------------------
# Item 1 -- the route and the body
# -----------------------------------------------------------------------------


class TestTheBody:
    @pytest.mark.parametrize(("body", "field"), [
        ({"state": "A" * 42}, "state"),
        ({"state": "A" * 44}, "state"),
        ({"state": "+" * 43}, "state"),
        ({"state": "/" * 43}, "state"),
        ({"state": "=" * 43}, "state"),
        ({"state": "\u00e9" * 43}, "state"),
        ({"state": "\u0661" * 43}, "state"),
        ({"state": ""}, "state"),
        ({"state": 5}, "state"),
        ({"code": ""}, "code"),
        ({"code": "c" * 2049}, "code"),
        ({"code": 5}, "code"),
        ({"extra": 1}, "extra"),
    ], ids=["42", "44", "plus", "slash", "equals", "non-ascii", "arabic-digits",
            "empty-state", "state-int", "empty-code", "code-2049", "code-int",
            "extra"])
    async def test_a_body_not_the_contracts_is_a_422_before_the_take(
        self, client: AsyncClient, tg: Telegram, body: dict[str, Any], field: str,
    ) -> None:
        state = await _start(client)
        sent = {"state": state, "code": _CODE, **body}
        resp = await client.post("/api/v1/login/complete", json=sent)
        assert resp.status_code == 422, resp.text
        assert _class(resp) == "validation"
        assert any(field in f["loc"] for f in resp.json()["error"]["fields"])
        assert "\u00e9" not in resp.text and "unit-code-mark" not in resp.text
        assert (await _row()).taken_at is None  # not taken
        assert tg.requests == []

    @pytest.mark.parametrize("missing", ["state", "code"])
    async def test_a_missing_field(
        self, client: AsyncClient, tg: Telegram, missing: str,
    ) -> None:
        body = {"state": "A" * 43, "code": _CODE}
        del body[missing]
        resp = await client.post("/api/v1/login/complete", json=body)
        assert resp.status_code == 422
        assert any(missing in f["loc"] for f in resp.json()["error"]["fields"])

    async def test_the_longest_code(self, client: AsyncClient, tg: Telegram) -> None:
        state = await _start(client)
        assert (await _complete(client, state, "c" * 2048)).status_code == 200


class TestTheTake:
    async def test_unknown(self, client: AsyncClient, tg: Telegram) -> None:
        resp = await _complete(client, "Q" * 43)
        assert (resp.status_code, _class(resp)) == (404, "login_unknown")
        assert tg.requests == []

    async def test_expired(self, client: AsyncClient, tg: Telegram) -> None:
        state = await _start(client)
        async with get_session_factory()() as session:
            await session.execute(text(
                "UPDATE login_attempts SET created_at = created_at - "
                "interval '11 minutes', expires_at = expires_at - "
                "interval '11 minutes'"))
            await session.commit()
        resp = await _complete(client, state)
        assert (resp.status_code, _class(resp)) == (410, "login_expired")
        assert tg.requests == []

    async def test_replayed(self, client: AsyncClient, tg: Telegram) -> None:
        state = await _start(client)
        assert (await _complete(client, state)).status_code == 200
        asked = len(tg.requests)
        resp = await _complete(client, state)
        assert (resp.status_code, _class(resp)) == (409, "login_replayed")
        assert len(tg.requests) == asked  # nothing more to Telegram


async def test_with_the_option_off_there_is_no_complete(client: AsyncClient) -> None:
    assert settings.login_enabled == "false"
    resp = await _complete(client, "Q" * 43)
    assert (resp.status_code, _class(resp)) == (404, "not_found")


# -----------------------------------------------------------------------------
# Item 2 -- the exchange and the keys
# -----------------------------------------------------------------------------


class TestTheExchange:
    async def test_the_request_carries_what_the_attempt_kept(
        self, client: AsyncClient, tg: Telegram,
    ) -> None:
        state = await _start(client)
        verifier = (await _row()).code_verifier
        assert (await _complete(client, state)).status_code == 200
        (token,) = tg.asked(TELEGRAM_LOGIN_TOKEN_URL)
        assert token.method == "POST"
        basic = base64.b64encode(f"{_BOT_ID}:{_SECRET}".encode()).decode()
        assert token.headers["authorization"] == f"Basic {basic}"
        form = {k: v for k, (v,) in parse_qs(token.content.decode()).items()}
        assert form == {
            "grant_type": "authorization_code", "code": _CODE,
            "redirect_uri": _RETURN, "client_id": _BOT_ID,
            "code_verifier": verifier,
        }
        (keys,) = tg.asked(TELEGRAM_LOGIN_JWKS_URL)
        assert keys.method == "GET"

    async def test_the_live_refusal_at_200(
        self, client: AsyncClient, tg: Telegram,
    ) -> None:
        """The live sign-in of 24.11: a wrong code and a wrong secret alike
        -- 200, {"error": "invalid_grant"}, no id_token."""
        async def live(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"error": "invalid_grant"})
        tg.token = live
        state = await _start(client)
        with capture_logs() as logs:
            resp = await _complete(client, state)
        assert (resp.status_code, _class(resp)) == (422, "provider_refused")
        (line,) = [e for e in logs if e["event"] == "login_refused"]
        assert line["reason"] == "invalid_grant"
        assert line["telegram_status"] == 200
        assert tg.asked(TELEGRAM_LOGIN_JWKS_URL) == []

    async def test_an_error_word_not_of_the_rfc_is_other(
        self, client: AsyncClient, tg: Telegram,
    ) -> None:
        async def odd(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"error": "unit-odd-word-mark"})
        tg.token = odd
        state = await _start(client)
        with capture_logs() as logs:
            resp = await _complete(client, state)
        assert _class(resp) == "provider_refused"
        assert [e["reason"] for e in logs if e["event"] == "login_refused"] == [
            "other",
        ]

    @pytest.mark.parametrize(("answer", "reason", "status"), [
        (lambda r: httpx.Response(500, json={}), "token_status", 500),
        (lambda r: httpx.Response(400, json={"error": "invalid_grant"}),
         "token_status", 400),
        (lambda r: httpx.Response(302, headers={"location": "https://x.example"}),
         "token_status", 302),
        (lambda r: httpx.Response(200, text="<html>"), "token_not_json", 200),
        (lambda r: httpx.Response(200, json=[1, 2]), "token_not_object", 200),
    ], ids=["500", "400", "302", "not-json", "not-an-object"])
    async def test_unusable_answers_are_unavailable(
        self, client: AsyncClient, tg: Telegram, answer: Any, reason: str,
        status: int,
    ) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            response: httpx.Response = answer(request)
            return response
        tg.token = handler
        state = await _start(client)
        with capture_logs() as logs:
            resp = await _complete(client, state)
        assert (resp.status_code, _class(resp)) == (503, "provider_unavailable")
        (line,) = [e for e in logs if e["event"] == "login_refused"]
        assert (line["reason"], line["telegram_status"]) == (reason, status)

    @pytest.mark.parametrize("failure", [
        httpx.ReadTimeout("t"), httpx.ConnectError("c"),
    ], ids=["timeout", "network"])
    async def test_no_answer_is_unavailable(
        self, client: AsyncClient, tg: Telegram, failure: Exception,
    ) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            raise failure
        tg.token = handler
        state = await _start(client)
        with capture_logs() as logs:
            resp = await _complete(client, state)
        assert (resp.status_code, _class(resp)) == (503, "provider_unavailable")
        (line,) = [e for e in logs if e["event"] == "login_refused"]
        assert (line["reason"], line["telegram_status"]) == ("token_unreachable", None)

    @pytest.mark.parametrize("body", [
        {"access_token": "a", "scope": "openid profile"},
        {"id_token": 5},
        {"id_token": ""},
    ], ids=["absent", "not-a-string", "empty"])
    async def test_no_id_token_is_502(
        self, client: AsyncClient, tg: Telegram, body: dict[str, Any],
    ) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=body)
        tg.token = handler
        state = await _start(client)
        resp = await _complete(client, state)
        assert (resp.status_code, _class(resp)) == (502, "id_token_invalid")


class TestTheKeys:
    @pytest.mark.parametrize(("answer", "reason"), [
        (lambda r: httpx.Response(500), "jwks_status"),
        (lambda r: httpx.Response(200, text="nope"), "jwks_not_json"),
        (lambda r: httpx.Response(200, json=["k"]), "jwks_not_object"),
    ], ids=["500", "not-json", "not-an-object"])
    async def test_unusable_keys_are_unavailable(
        self, client: AsyncClient, tg: Telegram, answer: Any, reason: str,
    ) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            response: httpx.Response = answer(request)
            return response
        tg.jwks = handler
        state = await _start(client)
        with capture_logs() as logs:
            resp = await _complete(client, state)
        assert (resp.status_code, _class(resp)) == (503, "provider_unavailable")
        assert [e["reason"] for e in logs if e["event"] == "login_refused"] == [
            reason,
        ]

    async def test_no_answer_on_the_keys(
        self, client: AsyncClient, tg: Telegram,
    ) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("t")
        tg.jwks = handler
        state = await _start(client)
        resp = await _complete(client, state)
        assert (resp.status_code, _class(resp)) == (503, "provider_unavailable")

    async def test_the_keys_are_fetched_for_every_sign_in(
        self, client: AsyncClient, tg: Telegram,
    ) -> None:
        for _ in range(2):
            assert (await _complete(client, await _start(client))).status_code == 200
        assert len(tg.asked(TELEGRAM_LOGIN_JWKS_URL)) == 2


class TestNoTransactionAcrossTelegram:
    async def test_the_take_is_committed_and_nothing_is_open(
        self, client: AsyncClient, tg: Telegram,
    ) -> None:
        """At each request to Telegram: another session sees the attempt
        taken (committed), and no backend of this database is idle inside
        a transaction."""
        seen: list[tuple[bool, int]] = []

        async def probe() -> None:
            async with get_session_factory()() as session:
                taken = await session.scalar(text(
                    "SELECT taken_at IS NOT NULL FROM login_attempts"))
                open_ = await session.scalar(text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() "
                    "AND pid <> pg_backend_pid() "
                    "AND state LIKE 'idle in transaction%'"))
            seen.append((bool(taken), int(open_)))

        good_token, good_jwks = tg.token, tg.jwks

        async def token(request: httpx.Request) -> httpx.Response:
            await probe()
            return await good_token(request)

        async def keys(request: httpx.Request) -> httpx.Response:
            await probe()
            return await good_jwks(request)

        tg.token, tg.jwks = token, keys
        state = await _start(client)
        assert (await _complete(client, state)).status_code == 200
        assert seen == [(True, 0), (True, 0)]


# -----------------------------------------------------------------------------
# Item 3 -- the result
# -----------------------------------------------------------------------------


class TestTheResult:
    async def test_the_form_of_section_3(
        self, client: AsyncClient, tg: Telegram, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        moments = iter([datetime(2026, 11, 24, 10, 3, 12, 345678, tzinfo=UTC),
                        datetime(2026, 11, 24, 10, 9, 0, tzinfo=UTC)])
        first = datetime(2026, 11, 24, 10, 3, 12, 345678, tzinfo=UTC)
        calls: list[datetime] = []

        def clock() -> datetime:
            calls.append(next(moments))
            return calls[-1]

        monkeypatch.setattr(complete_module, "_utc_now", clock)
        tg.exchange_body = {"id_token": _id_token(first)}
        state = await _start(client, bot_access=True)
        resp = await _complete(client, state)
        assert resp.status_code == 200, resp.text
        assert resp.json() == {
            "provider": "telegram",
            "subject": _TELEGRAM_ID,
            "verified_at": first.isoformat(),
            "telegram": {
                "id": int(_TELEGRAM_ID),
                "first_name": "Ada",
                "last_name": "Lovelace",
                "username": "ada_mark",
                "photo_url": "https://cdn.example.org/ada.jpg",
                "bot_access_granted": False,  # the live cell: asked, not given
            },
        }
        assert calls == [first]  # the moment read once

    async def test_bot_access_granted_when_the_scope_is_absent(
        self, client: AsyncClient, tg: Telegram,
    ) -> None:
        async def no_scope(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"id_token": _id_token()})
        tg.token = no_scope
        state = await _start(client, bot_access=True)
        resp = await _complete(client, state)
        assert resp.json()["telegram"]["bot_access_granted"] is True

    @pytest.mark.parametrize("word", list(IdTokenRejected))
    async def test_every_rejection_is_a_502(
        self, client: AsyncClient, tg: Telegram, word: IdTokenRejected,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(complete_module, "verify_identity",
                            lambda *a, **k: word)
        state = await _start(client)
        with capture_logs() as logs:
            resp = await _complete(client, state)
        assert (resp.status_code, _class(resp)) == (502, "id_token_invalid")
        assert [e["reason"] for e in logs if e["event"] == "login_refused"] == [
            word.value,
        ]

    @pytest.mark.parametrize(("changes", "word"), [
        ({"aud": "1"}, "audience"),
        ({"iss": "https://other.example.org"}, "issuer"),
        ({"id": True}, "no_id"),
    ], ids=["audience", "issuer", "no-id"])
    async def test_real_rejections(
        self, client: AsyncClient, tg: Telegram, changes: dict[str, Any],
        word: str,
    ) -> None:
        tg.exchange_body = {"id_token": _id_token(**changes)}
        state = await _start(client)
        with capture_logs() as logs:
            resp = await _complete(client, state)
        assert resp.status_code == 502
        assert [e["reason"] for e in logs if e["event"] == "login_refused"] == [word]


class TestTheRepeat:
    async def _set(self, tg: Telegram, outcome: str) -> None:
        async def refuse(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"error": "invalid_grant"})

        async def down(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503)

        if outcome == "refused":
            tg.token = refuse
        elif outcome == "unavailable":
            tg.jwks = down
        elif outcome == "id-token":
            tg.exchange_body = {"id_token": _id_token(aud="1")}

    @pytest.mark.parametrize("outcome", ["ok", "refused", "unavailable", "id-token"])
    async def test_after_any_outcome_a_repeat_is_replayed(
        self, client: AsyncClient, tg: Telegram, outcome: str,
    ) -> None:
        await self._set(tg, outcome)
        state = await _start(client)
        await _complete(client, state)
        asked = len(tg.requests)
        resp = await _complete(client, state)
        assert (resp.status_code, _class(resp)) == (409, "login_replayed")
        assert len(tg.requests) == asked

    async def test_while_the_first_is_still_running(
        self, client: AsyncClient, tg: Telegram,
    ) -> None:
        """The second complete is answered while the first waits on
        Telegram: the attempt is taken before the exchange."""
        reached, release = asyncio.Event(), asyncio.Event()
        good_token = tg.token

        async def slow(request: httpx.Request) -> httpx.Response:
            reached.set()
            await release.wait()
            return await good_token(request)

        tg.token = slow
        state = await _start(client)
        first = asyncio.create_task(_complete(client, state))
        await asyncio.wait_for(reached.wait(), 10)
        second = await _complete(client, state)
        assert (second.status_code, _class(second)) == (409, "login_replayed")
        assert not first.done()
        release.set()
        assert (await first).status_code == 200
        assert len(tg.asked(TELEGRAM_LOGIN_TOKEN_URL)) == 1


# -----------------------------------------------------------------------------
# Item 4 -- the classes and the log
# -----------------------------------------------------------------------------

_ROWS = dict(
    (cls, int(status)) for status, cls in re.findall(
        r"^\| (\d{3}) \| `(\w+)` \|",
        _DOC.split("| status | class | when |", 1)[1].split("\n\n", 1)[0],
        re.M,
    )
)


class TestTheClasses:
    @pytest.mark.parametrize("refusal", list(Refusal))
    async def test_each_travels_with_the_status_of_its_row(
        self, client: AsyncClient, tg: Telegram, refusal: Refusal,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def refused(*args: Any, **kwargs: Any) -> SignInRefused:
            return SignInRefused(refusal, "unit-word", None)
        monkeypatch.setattr(login_api, "complete_sign_in", refused)
        resp = await _complete(client, "Q" * 43)
        assert refusal.value in {c.value for c in ErrorClass}
        assert _class(resp) == refusal.value
        assert resp.status_code == _ROWS[refusal.value]


class TestTheLog:
    async def test_one_line_per_outcome_and_no_secret(
        self, client: AsyncClient, tg: Telegram,
    ) -> None:
        marks: list[str] = []
        with capture_logs() as logs:
            state = await _start(client)
            verifier = (await _row()).code_verifier
            ok = await _complete(client, state)
            state2 = await _start(client)
            refused = await _complete(client, "Q" * 43)
        assert ok.status_code == 200 and refused.status_code == 404
        id_token = _id_token()
        marks = [state, state2, _CODE, str(verifier), _SECRET, _TELEGRAM_ID,
                 "Ada", "Lovelace", "ada_mark", "cdn.example.org",
                 _jwks()["keys"][0]["n"][:20], id_token.split(".")[0],
                 "unit-access-mark"]
        events = [e for e in logs if e["event"].startswith("login_")]
        assert [e["event"] for e in events] == ["login_completed", "login_refused"]
        assert events[0] == {"event": "login_completed", "provider": "telegram",
                             "telegram_status": 200, "log_level": "info"}
        assert events[1] == {"event": "login_refused", "provider": "telegram",
                             "error_class": "login_unknown", "reason": "unknown",
                             "telegram_status": None, "log_level": "info"}
        shown = repr(logs) + refused.text
        for mark in marks:
            assert mark not in shown, mark

    async def test_no_secret_in_a_refusal_answer(
        self, client: AsyncClient, tg: Telegram,
    ) -> None:
        tg.exchange_body = {"id_token": _id_token(aud="1")}
        state = await _start(client)
        resp = await _complete(client, state)
        assert resp.status_code == 502
        for mark in (state, _CODE, _SECRET, _TELEGRAM_ID, "Ada"):
            assert mark not in resp.text


async def test_the_client_is_bounded_and_ignores_the_environment() -> None:
    """10 s a request; no proxy or certificate setting of the environment;
    no redirect followed."""
    async with telegram.new_client() as built:
        assert built.timeout == httpx.Timeout(10.0)
        assert built.trust_env is False
        assert built.follow_redirects is False
