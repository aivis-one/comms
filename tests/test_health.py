# =============================================================================
# COMMS Service -- Health endpoint tests (skeleton, handoff item 1)
# =============================================================================

from httpx import AsyncClient


async def test_health_ok(client: AsyncClient) -> None:
    """GET /health returns 200 with db status."""
    response = await client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["db"] == "ok"


async def test_ready_ok(client: AsyncClient) -> None:
    """GET /ready returns 200 when the database is reachable."""
    response = await client.get("/ready")
    assert response.status_code == 200
    assert response.json()["db"] == "ok"
