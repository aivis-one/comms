# =============================================================================
# COMMS Service -- Service-to-service auth tests (Phase 3b item 1)
# =============================================================================
#
# The /api/v1 surface is guarded by a shared bearer token
# (COMMS_SERVICE_TOKEN). Verified here against a real router (the
# inbox) driven through the ASGI transport:
#   - no header / wrong scheme / wrong token -> 401, and the response
#     never echoes anything the client sent;
#   - the right token -> through;
#   - no mode opens it (LEG-1): the suite's own client passes only by
#     presenting the suite's token -- without one, 401;
#   - /health and /ready stay open regardless (installer/docker
#     healthchecks carry no secret);
#   - the scheme name in any case (API-1, RFC 9110 §11.1), one space;
#   - every refusal is a NEW exception object, so no traceback grows
#     across refusals (API-1).
#
# MUTATIONS (API-1, each turns one red):
#   M8 one module-level 401 instance raised again and again ->
#      TestEveryRefusalIsItsOwn test_the_traceback_does_not_grow
#   M9 the scheme compared case-sensitively (startswith("Bearer ")) ->
#      TestTheSchemeInAnyCase test_the_right_token_passes[bearer|BEARER]
#   (LEG-1) an empty token passing again, the suite's token blanked ->
#      TestNoModeOpensIt test_without_a_token_the_suite_is_refused
# =============================================================================

from types import TracebackType
from uuid import uuid4

import pytest
from fastapi import HTTPException
from httpx import AsyncClient

from app.api.deps import require_service_auth
from app.core.config import settings
from tests.helpers import redis_answering

_TOKEN = "phase3b-unit-test-token"


@pytest.fixture
def auth_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the shared secret for the duration of one test."""
    monkeypatch.setattr(settings, "comms_service_token", _TOKEN)


def _inbox_url() -> str:
    return f"/api/v1/recipients/{uuid4()}/inbox"


class TestAuthRequired:
    async def test_missing_header_is_401(
        self, anonymous_client: AsyncClient, auth_enabled: None,
    ) -> None:
        response = await anonymous_client.get(_inbox_url())
        assert response.status_code == 401
        assert response.headers["WWW-Authenticate"] == "Bearer"

    async def test_wrong_scheme_is_401(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        response = await client.get(
            _inbox_url(), headers={"Authorization": f"Basic {_TOKEN}"},
        )
        assert response.status_code == 401

    async def test_wrong_token_is_401_and_never_echoed(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        presented = "wrong-token-that-must-not-leak"
        response = await client.get(
            _inbox_url(),
            headers={"Authorization": f"Bearer {presented}"},
        )
        assert response.status_code == 401
        # The constant 401 body reflects NOTHING from the request --
        # a near-miss token is still a secret.
        assert presented not in response.text
        assert _TOKEN not in response.text

    async def test_valid_token_passes(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        response = await client.get(
            _inbox_url(),
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert response.status_code == 200

    async def test_prefs_router_also_guarded(
        self, anonymous_client: AsyncClient, auth_enabled: None,
    ) -> None:
        response = await anonymous_client.get(
            f"/api/v1/recipients/{uuid4()}/preferences",
        )
        assert response.status_code == 401

    async def test_messaging_router_also_guarded(
        self, anonymous_client: AsyncClient, auth_enabled: None,
    ) -> None:
        response = await anonymous_client.get(
            f"/api/v1/threads/{uuid4()}/messages",
        )
        assert response.status_code == 401


class TestNoModeOpensIt:
    """Was TestAuthDisabledInDevelopment: an empty token opened the API in
    an explicit development, and the suite ran every test that way. LEG-1
    took the mode out -- startup refuses an empty token everywhere
    (tests/test_api_closed_by_default.py) -- so the suite runs closed:
    its token in force, its client presenting it."""

    async def test_without_a_token_the_suite_is_refused(
        self, anonymous_client: AsyncClient,
    ) -> None:
        response = await anonymous_client.get(_inbox_url())
        assert response.status_code == 401

    async def test_the_pair_the_suite_passes_with_its_token(
        self, client: AsyncClient,
    ) -> None:
        assert settings.comms_service_token != ""
        response = await client.get(_inbox_url())
        assert response.status_code == 200


class TestHealthStaysOpen:
    async def test_health_and_ready_unauthenticated(
        self, anonymous_client: AsyncClient, auth_enabled: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """/ready is 200 only with every check ok (W3-1): the suite has no
        Redis, so its check answers here (tests/helpers.py) -- what is
        asked is that no token is."""
        redis_answering(monkeypatch)
        assert (await anonymous_client.get("/health")).status_code == 200
        assert (await anonymous_client.get("/ready")).status_code == 200


def _depth(tb: TracebackType | None) -> int:
    n = 0
    while tb is not None:
        n += 1
        tb = tb.tb_next
    return n


class TestEveryRefusalIsItsOwn:
    async def test_the_traceback_does_not_grow(
        self, auth_enabled: None,
    ) -> None:
        """Before API-1 one instance was raised for every refusal and its
        traceback grew by the raising frames each time (2, 4, 6, ...),
        keeping every refused request's frames alive."""
        refused: list[HTTPException] = []
        for header in (None, "Basic x", "Bearer nope") * 4:
            with pytest.raises(HTTPException) as caught:
                await require_service_auth(header)
            refused.append(caught.value)
        assert len({id(exc) for exc in refused}) == len(refused)
        depths = {_depth(exc.__traceback__) for exc in refused}
        assert len(depths) == 1, depths
        assert depths != {0}

    async def test_the_answer_is_the_same(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        presented = "-".join(("not", "the", "token"))
        answers = [
            await client.get(
                _inbox_url(), headers={"Authorization": f"bearer {presented}"}
            )
            for _ in range(3)
        ]
        for response in answers:
            assert response.status_code == 401
            assert response.headers["WWW-Authenticate"] == "Bearer"
            assert response.json() == answers[0].json()
            assert presented not in response.text
        assert answers[0].json() != {}


class TestTheSchemeInAnyCase:
    @pytest.mark.parametrize("scheme", ["Bearer", "bearer", "BEARER"])
    async def test_the_right_token_passes(
        self, client: AsyncClient, auth_enabled: None, scheme: str,
    ) -> None:
        response = await client.get(
            _inbox_url(), headers={"Authorization": f"{scheme} {_TOKEN}"},
        )
        assert response.status_code == 200, response.text

    @pytest.mark.parametrize(
        "header",
        [
            "bearer wrong",
            "BEARER wrong",
            "bearer ",
            "BEARER",
            f"bearer  {_TOKEN}",
            f"Bearer{_TOKEN}",
        ],
        ids=[
            "lower-wrong", "upper-wrong", "lower-empty", "no-token",
            "two-spaces", "no-space",
        ],
    )
    async def test_a_wrong_or_empty_token_is_401(
        self, client: AsyncClient, auth_enabled: None, header: str,
    ) -> None:
        response = await client.get(
            _inbox_url(), headers={"Authorization": header},
        )
        assert response.status_code == 401
