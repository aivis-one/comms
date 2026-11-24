"""L-4: deploy/INTEGRATION.md section 16 -- the sign-in's samples are the
real answers -- and what section 3a and "How long comms keeps things"
say of the option `login`, against the code.

The product writes its fake of comms by the samples of section 16
(``json`` blocks, each under its label: route, status, and the class of
a refusal). Each is held to an answer the code really gives: the suite's
application with the sign-in's router included for the test, Telegram a
MockTransport whose answers come as streams, as a real transport's do
(tests/helpers.py StreamingMockTransport; app/login/telegram.py
new_client replaced) -- every
refusal reached by its real path, none by a stub of the outcome. Held:

  - the keys at every depth and the JSON types, both ways (a key of the
    code missing in the sample is red, and so is the other way round);
    the four names of the person that may be `null` -- a string or
    `null` on either side, twinned by a token without them;
  - the fixed values exactly -- `provider`, the constant parameters of
    the authorize URL, a refusal's whole body;
  - the variable ones by form -- the state by the code's own pattern,
    the times ISO 8601 with `+00:00` (never `Z`), the subject the
    decimal string of `telegram.id`;
  - every class of the sign-in has its sample, under the status of its
    row of section 5's table.

Mutations (gate L-4), each red here: a field of an answer renamed in the
code (M1, M2), a refusal's message changed (M3), `telegram.id` a string
(M4), a key added to the answer (M5); in the text a key renamed (M6), a
fixed value changed (M7), a time written with `Z` (M8), a refusal's
sample removed (M9), a label's status not its row (M10); the option's
channel changed in the code, or its key taken out of the text (M11 --
the key renamed in the code is stopped first by the suite's harness,
tests/conftest.py, which blanks every module's key by name), the
attempt's keep changed in the code (M12).
"""

import json
import re
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from httpx import AsyncClient
from jwt.algorithms import RSAAlgorithm
from sqlalchemy import text

from app.api import login as login_api
from app.api.login import STATE_PATTERN
from app.core.config import (
    TELEGRAM_LOGIN_AUTHORIZE_URL,
    TELEGRAM_LOGIN_ISSUER,
    TELEGRAM_LOGIN_JWKS_URL,
    TELEGRAM_LOGIN_TOKEN_URL,
    settings,
)
from app.core.database import get_session_factory
from app.core.modules import MODULE_SPECS
from app.login import telegram
from app.login.attempts import (
    ATTEMPT_KEEP,
    PURGE_INTERVAL_SECONDS,
    state_fingerprint,
)
from app.login.complete import Refusal
from app.main import app
from tests.helpers import StreamingMockTransport

_DOC = (Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md").read_text(
    encoding="utf-8",
)

# Assembled at run time (§0 §10).
_BOT_ID = "123456"
_TOKEN = _BOT_ID + ":" + "unit-test-login-contract"
_SECRET = "unit" + "-contract-secret-" + "mark"
_RETURN = "https://product.example.org/auth/telegram/return"
_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_KID = "oidc-contract"
_TELEGRAM_ID = "987654321"

_START = "/api/v1/login/start"
_COMPLETE = "/api/v1/login/complete"


# -----------------------------------------------------------------------------
# The text
# -----------------------------------------------------------------------------


def _section(start: str, end: str = "\n## ") -> str:
    begin = _DOC.index(start)
    return _DOC[begin:_DOC.index(end, begin + 1)]


_S16 = _section("\n## 16. ")
_LABELED = re.compile(
    r"^\*\*`POST /api/v1/login/(start|complete)` -- (\d{3})(?: `([a-z_]+)`)?\*\*"
    r"\n\n```json\n(.*?)\n```",
    re.M | re.S,
)


def _paragraph(section: str, lead: str) -> str:
    """The paragraph of `section` that opens with `lead`."""
    rest = section[section.index(lead):]
    return rest.split("\n\n", 1)[0]


def _samples() -> list[tuple[str, int, str | None, Any]]:
    """(route, status, class or None, body) of every labeled sample."""
    return [
        (route, int(status), cls or None, json.loads(block))
        for route, status, cls, block in _LABELED.findall(_S16)
    ]


def _sample(route: str, cls: str | None = None) -> tuple[int, Any]:
    (found,) = [(s, b) for r, s, c, b in _samples() if (r, c) == (route, cls)]
    return found


def _section_5_rows() -> dict[str, int]:
    """class -> status of the table of section 5 (its first table)."""
    table = _DOC.split("| status | class | when |", 1)[1].split("\n\n", 1)[0]
    return {
        cls: int(status)
        for status, cls in re.findall(r"^\| (\d{3}) \| `(\w+)` \|", table, re.M)
    }


# -----------------------------------------------------------------------------
# The form
# -----------------------------------------------------------------------------

# The names of the person that are a string or null (section 16).
_NULLABLE = frozenset({
    "telegram.first_name", "telegram.last_name",
    "telegram.username", "telegram.photo_url",
})
# A time of the API: ISO 8601, UTC as `+00:00`, the fraction when not 0.
_UTC_TIME = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{6})?\+00:00",
)
# The parameters of the authorize URL whose values are fixed.
_FIXED_PARAMETERS = ("response_type", "scope", "code_challenge_method")


def _form_differences(sample: Any, real: Any, path: str = "") -> list[str]:
    """Where `sample` and `real` differ in keys or JSON types, both ways."""
    if isinstance(sample, dict) and isinstance(real, dict):
        if set(sample) != set(real):
            return [f"{path or '.'}: keys {sorted(sample)} != {sorted(real)}"]
        return [
            difference
            for key in sample
            for difference in _form_differences(
                sample[key], real[key], f"{path}.{key}" if path else key,
            )
        ]
    if path in _NULLABLE:
        allowed = (str, type(None))
        if type(sample) in allowed and type(real) in allowed:
            return []
        return [f"{path}: {type(sample).__name__} / {type(real).__name__}"]
    if type(sample) is not type(real):
        return [f"{path}: {type(sample).__name__} != {type(real).__name__}"]
    return []


def _a_time(value: str) -> bool:
    if not _UTC_TIME.fullmatch(value):
        return False
    offset = datetime.fromisoformat(value).utcoffset()
    return offset is not None and offset.total_seconds() == 0


def _start_problems(answer: dict[str, Any]) -> list[str]:
    """The variable values of a start's answer, by form."""
    problems = []
    if not re.fullmatch(STATE_PATTERN, answer["state"]):
        problems.append("state")
    if not _a_time(answer["expires_at"]):
        problems.append("expires_at")
    url = urlsplit(answer["authorize_url"])
    if f"{url.scheme}://{url.netloc}{url.path}" != TELEGRAM_LOGIN_AUTHORIZE_URL:
        problems.append("authorize_url: base")
    if parse_qs(url.query).get("state") != [answer["state"]]:
        problems.append("authorize_url: state")
    return problems


def _person_problems(answer: dict[str, Any]) -> list[str]:
    """The variable values of the person, by form."""
    problems = []
    if not _a_time(answer["verified_at"]):
        problems.append("verified_at")
    subject = answer["subject"]
    if not re.fullmatch(r"[1-9][0-9]*", subject):
        problems.append("subject")
    if subject != str(answer["telegram"]["id"]):
        problems.append("subject != telegram.id")
    photo = answer["telegram"]["photo_url"]
    if photo is not None and urlsplit(photo).scheme != "https":
        problems.append("photo_url")
    return problems


# -----------------------------------------------------------------------------
# The answers -- the suite's application, Telegram the test's
# -----------------------------------------------------------------------------


def _jwks() -> dict[str, Any]:
    public = json.loads(RSAAlgorithm.to_jwk(_KEY.public_key()))
    return {"keys": [{"kty": "RSA", "kid": _KID, "n": public["n"], "e": public["e"]}]}


_PERSON = {
    "given_name": "Ada", "family_name": "Lovelace",
    "preferred_username": "ada_mark", "picture": "https://cdn.example.org/ada.jpg",
}


def _id_token(person: dict[str, Any], **changes: Any) -> str:
    stamp = int(datetime.now(UTC).timestamp())
    claims = {
        "iss": TELEGRAM_LOGIN_ISSUER, "aud": _BOT_ID, "exp": stamp + 300,
        "iat": stamp - 5, "sub": "unit-sub", "id": _TELEGRAM_ID,
        **person, **changes,
    }
    return jwt.encode(claims, _KEY, algorithm="RS256", headers={"kid": _KID})


Handler = Callable[[httpx.Request], Awaitable[httpx.Response]]


class _Telegram:
    """The test's Telegram: what the token endpoint answers."""

    def __init__(self) -> None:
        self.person: dict[str, Any] = dict(_PERSON)
        self.token: Handler = self._good_token

    async def _good_token(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "access_token": "unit-access-mark", "token_type": "Bearer",
            "id_token": _id_token(self.person),
            "scope": "openid profile telegram:bot_access",
        })

    async def handle(self, request: httpx.Request) -> httpx.Response:
        if str(request.url) == TELEGRAM_LOGIN_TOKEN_URL:
            return await self.token(request)
        if str(request.url) == TELEGRAM_LOGIN_JWKS_URL:
            return httpx.Response(200, json=_jwks())
        raise AssertionError(f"an address that is not Telegram's: {request.url}")


@pytest.fixture
def tg(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Telegram]:
    """The option on for one test: the routes in the application, the
    bot's token and the client's secret set, Telegram the test's."""
    fake = _Telegram()
    monkeypatch.setattr(settings, "telegram_bot_token", _TOKEN)
    monkeypatch.setattr(settings, "telegram_login_client_secret", _SECRET)
    monkeypatch.setattr(
        telegram, "new_client",
        lambda: httpx.AsyncClient(transport=StreamingMockTransport(fake.handle)),
    )
    routes = app.router.routes
    before = list(routes)
    app.include_router(login_api.router)
    yield fake
    routes[:] = before


async def _start(client: AsyncClient) -> dict[str, Any]:
    resp = await client.post(_START, json={
        "provider": "telegram", "redirect_uri": _RETURN, "bot_access": True,
    })
    assert resp.status_code == 200, resp.text
    answer: dict[str, Any] = resp.json()
    return answer


async def _complete(client: AsyncClient, state: str) -> httpx.Response:
    return await client.post(_COMPLETE, json={"state": state, "code": "unit-code"})


async def _expire(state: str) -> None:
    async with get_session_factory()() as session:
        await session.execute(
            text(
                "UPDATE login_attempts SET"
                " created_at = created_at - interval '11 minutes',"
                " expires_at = expires_at - interval '11 minutes'"
                " WHERE state_hash = :state_hash"
            ),
            {"state_hash": state_fingerprint(state)},
        )
        await session.commit()


def _telegram_answers(status: int, body: dict[str, Any]) -> Handler:
    async def answer(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=body)
    return answer


async def _real_refusal(
    client: AsyncClient, tg: _Telegram, refusal: Refusal,
) -> httpx.Response:
    """The answer of `refusal`, reached by its real path."""
    if refusal is Refusal.LOGIN_UNKNOWN:
        return await _complete(client, "Q" * 43)
    state = (await _start(client))["state"]
    if refusal is Refusal.LOGIN_EXPIRED:
        await _expire(state)
    elif refusal is Refusal.LOGIN_REPLAYED:
        first = await _complete(client, state)
        assert first.status_code == 200, "the pair: the first one completed"
    elif refusal is Refusal.PROVIDER_REFUSED:
        tg.token = _telegram_answers(200, {"error": "invalid_grant"})
    elif refusal is Refusal.PROVIDER_UNAVAILABLE:
        tg.token = _telegram_answers(500, {"detail": "unit"})
    else:
        assert refusal is Refusal.ID_TOKEN_INVALID
        tg.person = {**_PERSON, "aud": "1"}
    return await _complete(client, state)


# -----------------------------------------------------------------------------
# Item 2 -- the samples of section 16 are the real answers
# -----------------------------------------------------------------------------


class TestTheSamplesAreThere:
    def test_every_answer_has_one_sample_and_no_other(self) -> None:
        """The pair of every test below: the text is read, and it holds
        exactly the two answers and the six refusals -- none twice."""
        labels = [(route, cls) for route, _, cls, _ in _samples()]
        expected = [("start", None), ("complete", None)] + [
            ("complete", refusal.value) for refusal in Refusal
        ]
        assert sorted(labels, key=str) == sorted(expected, key=str)
        assert all(body for *_, body in _samples())

    def test_a_refusals_status_is_its_row_of_section_5(self) -> None:
        rows = _section_5_rows()
        assert rows, "the pair: section 5's table is read"
        for route, status, cls, _ in _samples():
            if cls is not None:
                assert status == rows[cls], cls
            else:
                assert status == 200, route


class TestTheAnswers:
    async def test_start(self, client: AsyncClient, tg: _Telegram) -> None:
        status, sample = _sample("start")
        real = await _start(client)
        assert status == 200
        assert _form_differences(sample, real) == []
        assert _start_problems(sample) == []
        assert _start_problems(real) == []
        sample_query = parse_qs(urlsplit(sample["authorize_url"]).query)
        real_query = parse_qs(urlsplit(real["authorize_url"]).query)
        assert set(sample_query) == set(real_query)
        for name in _FIXED_PARAMETERS:
            assert sample_query[name] == real_query[name], name

    @pytest.mark.parametrize("person", [
        _PERSON,
        {},  # no name, no username, no photo: four nulls
    ], ids=["every-name", "no-names"])
    async def test_complete(
        self, client: AsyncClient, tg: _Telegram, person: dict[str, Any],
    ) -> None:
        tg.person = dict(person)
        status, sample = _sample("complete")
        resp = await _complete(client, (await _start(client))["state"])
        assert (resp.status_code, status) == (200, 200), resp.text
        real = resp.json()
        nulls = [real["telegram"][path.split(".")[1]] is None for path in _NULLABLE]
        assert all(nulls) if not person else not any(nulls), "the twin is real"
        assert _form_differences(sample, real) == []
        assert _person_problems(sample) == []
        assert _person_problems(real) == []
        assert sample["provider"] == real["provider"] == "telegram"

    @pytest.mark.parametrize("refusal", list(Refusal))
    async def test_a_refusal(
        self, client: AsyncClient, tg: _Telegram, refusal: Refusal,
    ) -> None:
        status, sample = _sample("complete", refusal.value)
        resp = await _real_refusal(client, tg, refusal)
        assert resp.status_code == status
        assert resp.json() == sample


# -----------------------------------------------------------------------------
# Item 3 -- section 3a and "How long comms keeps things"
# -----------------------------------------------------------------------------


class TestTheOption:
    def test_section_3a_names_the_option_its_channels_and_keys(self) -> None:
        section = _section("\n## 3a. ")
        lead = "**The sign-in is an option too, and no channel: `login`.**"
        paragraph = _paragraph(section, lead)
        (spec,) = [s for s in MODULE_SPECS if s.name == "login"]
        named = [f"`{spec.name}`", *(f"`{c}`" for c in spec.channels),
                 *(f"`{k}`" for k in spec.keys)]
        assert spec.channels and spec.keys, "the pair: there is something to name"
        assert [n for n in named if n not in paragraph] == []


class TestHowLong:
    def test_the_attempt_lives_the_codes_keep(self) -> None:
        section = _section("\n## How long comms keeps things", "\nA product that")
        lead = "- **A sign-in attempt** (section 16)"
        bullet = _paragraph(section, lead)
        hours = int(ATTEMPT_KEEP.total_seconds() // 3600)
        assert f"**{hours} hours**" in bullet
        assert PURGE_INTERVAL_SECONDS == 3600
        assert "within an hour after" in bullet
