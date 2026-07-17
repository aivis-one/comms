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
#   - empty configured token (stub mode) -> auth disabled;
#   - /health and /ready stay open regardless (installer/docker
#     healthchecks carry no secret).
# =============================================================================

from uuid import uuid4

import pytest
from httpx import AsyncClient

from app.core.config import settings

_TOKEN = "phase3b-unit-test-token"


@pytest.fixture
def auth_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the shared secret for the duration of one test."""
    monkeypatch.setattr(settings, "comms_service_token", _TOKEN)


def _inbox_url() -> str:
    return f"/api/v1/recipients/{uuid4()}/inbox"


class TestAuthRequired:
    async def test_missing_header_is_401(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        response = await client.get(_inbox_url())
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
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        response = await client.get(
            f"/api/v1/recipients/{uuid4()}/preferences",
        )
        assert response.status_code == 401

    async def test_messaging_router_also_guarded(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        response = await client.get(
            f"/api/v1/threads/{uuid4()}/messages",
        )
        assert response.status_code == 401


class TestAuthDisabledInStub:
    async def test_empty_token_disables_auth(
        self, client: AsyncClient,
    ) -> None:
        """Default test config: no token -> the API is open (stub
        mode; real mode cannot even start without the token, see
        test_config_validation)."""
        assert settings.comms_service_token == ""
        response = await client.get(_inbox_url())
        assert response.status_code == 200


class TestHealthStaysOpen:
    async def test_health_and_ready_unauthenticated(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        assert (await client.get("/health")).status_code == 200
        assert (await client.get("/ready")).status_code == 200
