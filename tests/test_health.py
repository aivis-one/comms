# =============================================================================
# COMMS Service -- Health endpoint tests (skeleton, handoff item 1)
# =============================================================================
#
# Since R-1a the endpoint also carries the CHANNEL MAP -- the operator's
# detector for "did this deploy get the channels its installer meant to
# give it" (deploy/INTEGRATION.md). It is the same map the startup log
# carries, and it is STATES ONLY: /health is unauthenticated.
# =============================================================================

from typing import Any
from unittest.mock import patch

from httpx import AsyncClient

from app.core.channels import ChannelState
from app.core.config import Settings
from app.engine.constants import DeliveryChannel

# A deploy with both external channels fully configured. Every value is
# a SENTINEL, and every one of them is non-empty on purpose: the suite
# blanks all channel keys (tests/conftest.py), so a leak assertion
# written against the real settings would be an assertion about the
# empty string -- which is a substring of every text, and would pass
# without checking anything.
_TOKEN = "8123456789:AA-health-sentinel-token"
_BOT_URL = "https://telegram.me/health_sentinel_bot"
_API_KEY = "key-health-sentinel"
_MAIL_DOMAIN = "mail.sentinel.test"
_SENDER = "noreply@mail.sentinel.test"
_REGION = "eu"
_KEY_VALUES = (_TOKEN, _BOT_URL, _API_KEY, _MAIL_DOMAIN, _SENDER, _REGION)


def _configured(**overrides: Any) -> Settings:
    """Settings of a deploy that has both external channels."""
    kwargs: dict[str, Any] = {
        "_env_file": None,
        "app_env": "production",
        "database_url": "postgresql+asyncpg://u:p@db/comms_unit",
        "comms_service_token": "health-test-service-token",
        "telegram_bot_token": _TOKEN,
        "telegram_bot_url": _BOT_URL,
        "email_mailgun_api_key": _API_KEY,
        "email_mailgun_domain": _MAIL_DOMAIN,
        "email_from_address": _SENDER,
        "email_mailgun_region": _REGION,
    }
    kwargs.update(overrides)
    return Settings(**kwargs)


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


async def test_health_carries_the_state_of_every_channel(
    client: AsyncClient,
) -> None:
    """The map an operator comes here for, by request instead of by
    grepping a container's log.

    EVERY channel is named, including the one with no implementation:
    a channel missing from the map would read as "nothing to see",
    which is the opposite of what its absence means.
    """
    with patch("app.main.settings", _configured()):
        payload = (await client.get("/health")).json()
    channels = payload["channels"]
    assert set(channels) == {channel.value for channel in DeliveryChannel}
    assert set(channels.values()) <= {state.value for state in ChannelState}
    assert channels["telegram"] == ChannelState.LIVE
    assert channels["email"] == ChannelState.LIVE
    assert channels["in_app"] == ChannelState.LIVE
    assert channels["push"] == ChannelState.NOT_IMPLEMENTED


async def test_health_names_the_channels_a_deploy_lacks(
    client: AsyncClient,
) -> None:
    """A deploy without external channels gets the same full map: "this
    product has no email" is exactly what an operator comes to confirm,
    and it must be readable as a state rather than as silence."""
    bare = _configured(
        telegram_bot_token="",
        telegram_bot_url="",
        email_mailgun_api_key="",
        email_mailgun_domain="",
        email_from_address="",
    )
    with patch("app.main.settings", bare):
        channels = (await client.get("/health")).json()["channels"]
    assert set(channels) == {channel.value for channel in DeliveryChannel}
    assert channels["telegram"] == ChannelState.NOT_CONFIGURED
    assert channels["email"] == ChannelState.NOT_CONFIGURED
    assert channels["in_app"] == ChannelState.LIVE


async def test_health_carries_no_key_value(client: AsyncClient) -> None:
    """/health has no authentication on purpose (installer and docker
    healthchecks hold no secret), so what it may say about a channel is
    that the deploy has it -- never what made it live.

    The pair to the absence: the sentinel settings really did produce
    live channels, so the values checked for below were there to leak.
    """
    with patch("app.main.settings", _configured()):
        response = await client.get("/health")
    assert response.json()["channels"]["email"] == ChannelState.LIVE
    body = response.text
    for value in _KEY_VALUES:
        assert value, "an empty sentinel would make the check vacuous"
        assert value not in body


async def test_health_reports_the_same_map_on_every_call(
    client: AsyncClient,
) -> None:
    """Twice is the same: the map is derived from settings and from
    nothing that drifts between requests."""
    with patch("app.main.settings", _configured()):
        first = (await client.get("/health")).json()["channels"]
        second = (await client.get("/health")).json()["channels"]
    assert first == second
