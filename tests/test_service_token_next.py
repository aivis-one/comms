# =============================================================================
# W2-1b -- two service tokens: the current one and the next one
# =============================================================================
#
# A change of key without downtime (Primer protocol 5.4, owner's decision
# 01.11): comms accepts COMMS_SERVICE_TOKEN and COMMS_SERVICE_TOKEN_NEXT;
# a request passes with either, with any other it is the 401
# (app/api/deps.py require_service_auth, app/core/config.py
# Settings.service_tokens).
#
#   item 1  either token passes, any other is the 401; the next one is
#           optional and never alone; every token compared, always
#   item 2  no token in a log line -- not the refusal's, not startup's,
#           not the settings' repr; nothing says which token passed
#   item 3  deploy/.env.example said so until LEG-1 took the template
#           out (the next token's default, empty, is the code's: on a box
#           primer.yaml declares no key for it, tests/test_primer_yaml.py
#           NOT_IN_ENV). deploy/INTEGRATION.md said so too, held here
#           both ways, until DOC-1 took the paragraph out: the machine
#           changes no key, and the product presents the value its seam
#           gave it -- the change of key is the operator's, not the
#           product's
#
# THREE DOUBLE AXES. REPEAT: both tokens pass; the next equal to the
# current. EMPTY: an empty presented token, the next set to "". SHORTFALL:
# the next unset; the next without the current.
#
# MUTATIONS (one run, every one red here):
#   M1 only the current token                -> test_the_next_one_passes
#   M2 the comparison stops at the first match -> TestEveryTokenIsCompared
#   M3 the next without the current starts   -> TestStartup
#   M4 "" in the list of tokens              -> test_an_empty_token_...
#   M5 a token in the refusal's log line     -> TestNoTokenInTheLog
#   M6 an accepted request says which token  -> test_nothing_says_which_...
#   M7 repr=True on the next token           -> test_the_repr_carries_...
# =============================================================================

import secrets
from typing import Any
from uuid import uuid4

import pytest
from fastapi import HTTPException
from httpx import AsyncClient
from pydantic import ValidationError
from structlog.testing import capture_logs

from app import main
from app.api.deps import require_service_auth
from app.core.config import Settings, format_startup_error, settings
from tests.conftest import FIXTURE_PROFILE_DIR

# Assembled at run time, fresh per run: no literal of a token's shape.
_CURRENT = "-".join(("w21b", "current", uuid4().hex))
_NEXT = "-".join(("w21b", "next", uuid4().hex))
_FOREIGN = "-".join(("w21b", "foreign", uuid4().hex))
_ROUTE = "/api/v1/recipients"


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def both(monkeypatch: pytest.MonkeyPatch) -> None:
    """A deploy in the middle of a change: both tokens in force."""
    monkeypatch.setattr(settings, "comms_service_token", _CURRENT)
    monkeypatch.setattr(settings, "comms_service_token_next", _NEXT)


@pytest.fixture
def current_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "comms_service_token", _CURRENT)
    monkeypatch.setattr(settings, "comms_service_token_next", "")


def _settings(**fields: Any) -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://u:p@db/comms_unit",
        **fields,
    )


def _refusal(**fields: Any) -> str:
    with pytest.raises(ValidationError) as caught:
        _settings(**fields)
    return format_startup_error(caught.value)


# -----------------------------------------------------------------------------
# item 1 -- either token passes, on the real API
# -----------------------------------------------------------------------------


class TestEitherTokenPasses:
    async def test_the_current_one_passes(
        self, bare_client: AsyncClient, both: None,
    ) -> None:
        response = await bare_client.get(_ROUTE, headers=_bearer(_CURRENT))
        assert response.status_code == 200, response.text

    async def test_the_next_one_passes(
        self, bare_client: AsyncClient, both: None,
    ) -> None:
        """M1."""
        response = await bare_client.get(_ROUTE, headers=_bearer(_NEXT))
        assert response.status_code == 200, response.text

    @pytest.mark.parametrize(
        "token", [_FOREIGN, _CURRENT + "x", _NEXT[:-1]],
        ids=["foreign", "current-longer", "next-shorter"],
    )
    async def test_any_other_is_the_401(
        self, bare_client: AsyncClient, both: None, token: str,
    ) -> None:
        response = await bare_client.get(_ROUTE, headers=_bearer(token))
        assert response.status_code == 401
        assert response.json()["error"]["class"] == "unauthorized"

    @pytest.mark.parametrize("deploy", ["both", "current_only"])
    async def test_an_empty_token_is_the_401(
        self, bare_client: AsyncClient, deploy: str,
        request: pytest.FixtureRequest,
    ) -> None:
        """M4, EMPTY: "" is never a token in force, so an empty
        presented token matches nothing -- with the next one set or
        not."""
        request.getfixturevalue(deploy)
        response = await bare_client.get(_ROUTE, headers={"Authorization": "Bearer "})
        assert response.status_code == 401

    async def test_without_the_next_one_only_the_current_passes(
        self, bare_client: AsyncClient, current_only: None,
    ) -> None:
        """SHORTFALL: the next one taken away (the last step of a
        change) -- the old next token is the 401, the current passes."""
        assert (await bare_client.get(
            _ROUTE, headers=_bearer(_NEXT),
        )).status_code == 401
        assert (await bare_client.get(
            _ROUTE, headers=_bearer(_CURRENT),
        )).status_code == 200


class TestTheTokensInForce:
    @pytest.mark.parametrize("given", ["unset", "empty"])
    def test_an_unset_or_empty_next_is_no_token(self, given: str) -> None:
        fields = {} if given == "unset" else {"comms_service_token_next": ""}
        source = _settings(comms_service_token=_CURRENT, **fields)
        assert source.service_tokens == (_CURRENT,)

    def test_the_next_one_follows_the_current(self) -> None:
        source = _settings(
            comms_service_token=_CURRENT, comms_service_token_next=_NEXT,
        )
        assert source.service_tokens == (_CURRENT, _NEXT)

    def test_the_next_may_equal_the_current(self) -> None:
        """REPEAT: a repeated step of the machine -- one key, in force,
        compared twice."""
        source = _settings(
            comms_service_token=_CURRENT, comms_service_token_next=_CURRENT,
        )
        assert source.service_tokens == (_CURRENT, _CURRENT)


class TestStartup:
    def test_the_next_without_the_current_refuses_naming_both(self) -> None:
        """M3: the current one is required, and the next one alone is
        said by its own name."""
        refusal = _refusal(
            comms_service_token="",
            comms_service_token_next=_NEXT,
        )
        assert "COMMS_SERVICE_TOKEN is required" in refusal
        assert "COMMS_SERVICE_TOKEN_NEXT is set without COMMS_SERVICE_TOKEN" in refusal
        assert _NEXT not in refusal

    def test_the_pair_the_current_alone_starts(self) -> None:
        """Was test_the_pair_development_without_either_starts: without
        either token development started open; LEG-1 took the mode out
        (tests/test_api_closed_by_default.py TestOneMode). The pair of
        the refusal above: the current one alone starts."""
        source = _settings(comms_service_token=_CURRENT)
        assert source.service_tokens == (_CURRENT,)


# -----------------------------------------------------------------------------
# item 1 -- every token compared, a match or not (counted, not timed)
# -----------------------------------------------------------------------------


@pytest.fixture
def compared(monkeypatch: pytest.MonkeyPatch) -> list[bytes]:
    """Every expected token secrets.compare_digest is asked about."""
    seen: list[bytes] = []
    real = secrets.compare_digest

    def counting(a: Any, b: Any) -> bool:
        seen.append(b)
        return real(a, b)

    monkeypatch.setattr(secrets, "compare_digest", counting)
    return seen


class TestEveryTokenIsCompared:
    @pytest.mark.parametrize(
        "token", [_CURRENT, _NEXT, _FOREIGN], ids=["current", "next", "foreign"],
    )
    async def test_both_are_compared_whatever_is_presented(
        self, both: None, compared: list[bytes], token: str,
    ) -> None:
        """M2: a match on the current token does not end the check --
        the next one is compared as well, so the work does not say which
        matched."""
        try:
            await require_service_auth(f"Bearer {token}")
        except HTTPException:
            assert token == _FOREIGN
        assert compared == [_CURRENT.encode(), _NEXT.encode()]

    async def test_one_token_in_force_is_one_comparison(
        self, current_only: None, compared: list[bytes],
    ) -> None:
        await require_service_auth(f"Bearer {_CURRENT}")
        assert compared == [_CURRENT.encode()]


# -----------------------------------------------------------------------------
# item 2 -- no token in a log line
# -----------------------------------------------------------------------------


@pytest.fixture
def startup_spies(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every call of the lifespan with a global effect, recorded instead
    of run: logging setup, the process role, the shutdown."""
    called: list[str] = []

    def setup_logging() -> None:
        called.append("setup_logging")

    def set_process_role(role: Any) -> None:
        called.append("set_process_role")

    async def close_formatters() -> None:
        called.append("close_formatters")

    async def dispose_engine() -> None:
        called.append("dispose_engine")

    monkeypatch.setattr(main, "setup_logging", setup_logging)
    monkeypatch.setattr(main, "set_process_role", set_process_role)
    monkeypatch.setattr(main, "close_formatters", close_formatters)
    monkeypatch.setattr(main, "dispose_engine", dispose_engine)
    return called


def _carries_a_token(logs: list[dict[str, Any]]) -> list[str]:
    text = repr(logs)
    return [t for t in (_CURRENT, _NEXT, _FOREIGN) if t in text]


class TestNoTokenInTheLog:
    @pytest.mark.parametrize(
        "header",
        [None, f"Basic {_CURRENT}", f"Bearer {_FOREIGN}", f"Bearer {_NEXT}x"],
        ids=["no-header", "basic-current", "bearer-foreign", "bearer-next-longer"],
    )
    async def test_a_refusal_names_no_token(
        self, bare_client: AsyncClient, both: None, header: str | None,
    ) -> None:
        """M5. The pair: the refusal is in the log."""
        headers = {} if header is None else {"Authorization": header}
        with capture_logs() as logs:
            response = await bare_client.get(_ROUTE, headers=headers)
        assert response.status_code == 401
        assert [e for e in logs if e["event"] == "service_auth_failed"]
        assert _carries_a_token(logs) == []
        assert [t for t in (_CURRENT, _NEXT, _FOREIGN) if t in response.text] == []

    async def test_nothing_says_which_token_passed(
        self, bare_client: AsyncClient, both: None,
    ) -> None:
        """M6: what comms logs of a request that passed with the current
        token is what it logs of one that passed with the next -- the
        request id aside, which is minted per request."""

        async def logged(token: str) -> list[dict[str, Any]]:
            with capture_logs() as logs:
                response = await bare_client.get(_ROUTE, headers=_bearer(token))
            assert response.status_code == 200
            return [
                {k: v for k, v in e.items() if k != "request_id"} for e in logs
            ]

        with_current, with_next = await logged(_CURRENT), await logged(_NEXT)
        assert with_current == with_next
        assert _carries_a_token(with_current + with_next) == []

    async def test_startup_names_no_token(
        self, both: None, startup_spies: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The pair: comms_started is in the log."""
        monkeypatch.setattr(settings, "templates_dir", str(FIXTURE_PROFILE_DIR))
        with capture_logs() as logs:
            async with main.lifespan(main.app):
                pass
        assert [e for e in logs if e["event"] == "comms_started"]
        assert _carries_a_token(logs) == []
        assert startup_spies[:2] == ["setup_logging", "set_process_role"]

    def test_the_repr_carries_no_token(self) -> None:
        """M7. The pair: the repr is the settings' own, other fields in
        it."""
        source = _settings(
            comms_service_token=_CURRENT, comms_service_token_next=_NEXT,
        )
        text = repr(source)
        assert "database_url=" in text
        assert _CURRENT not in text and _NEXT not in text
