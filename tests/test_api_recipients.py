# =============================================================================
# COMMS Service -- Recipient upsert API tests (T-64)
# =============================================================================
#
# The synchronous door the product uses before its first message:
# PUT /api/v1/recipients/{id}. What is pinned here:
#
#   1: an unknown id is created, and the body comes back as stored
#   2: a repeat call updates rather than duplicating, and leaves the
#      COMMS-OWNED preference fields alone
#   3: snapshot semantics -- nulls in a later call OVERWRITE earlier
#      values, because this contract has no "keep what you had"
#   4: an ABSENT field is a 422, not a default; so is an unknown one
#   5: an unresolvable timezone is stored as-is (poison-pill rule),
#      never rejected
#   6: the route is behind the service token like every other
#
# The service function underneath (audience.sync.user_upserted) is the
# one the stream consumer calls; it is not re-tested here, only the
# route's contract with it.
#
# telegram_ids come from the T-64 band 92180-92199 -- see the allocator
# in tests/helpers.py for why that is narrower than the assigned range.
# Seeds commit, because the request handler opens its own session;
# clean_db wipes between tests.
# =============================================================================

from datetime import time
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from app.audience.models import Recipient
from app.core.config import settings
from app.core.database import get_session_factory
from tests.helpers import create_recipient, next_t64_telegram_id

_TOKEN = "t64-recipient-upsert-token"


@pytest.fixture
def auth_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the shared secret for the duration of one test."""
    monkeypatch.setattr(settings, "comms_service_token", _TOKEN)


def _url(recipient_id: UUID) -> str:
    return f"/api/v1/recipients/{recipient_id}"


def _snapshot(**overrides: Any) -> dict[str, Any]:
    """A complete six-field snapshot; overrides replace single fields.

    Spelled out in full every time on purpose -- a helper that filled
    in missing keys would hide the very discipline these tests exist
    to check.
    """
    body: dict[str, Any] = {
        "telegram_id": next_t64_telegram_id(),
        "email": "recipient@example.test",
        "locale": "en",
        "timezone": "Europe/Berlin",
        "active": True,
    }
    body.update(overrides)
    return body


async def _load(recipient_id: UUID) -> Recipient | None:
    factory = get_session_factory()
    async with factory() as session:
        return await session.get(Recipient, recipient_id)


# ---------------------------------------------------------------------------
# 1-2. Create, then repeat
# ---------------------------------------------------------------------------


class TestUpsert:
    async def test_unknown_id_is_created(self, client: AsyncClient) -> None:
        recipient_id = uuid4()
        body = _snapshot()

        response = await client.put(_url(recipient_id), json=body)

        assert response.status_code == 200
        assert response.json() == {"recipient_id": str(recipient_id), **body}

        stored = await _load(recipient_id)
        assert stored is not None
        assert stored.telegram_id == body["telegram_id"]
        assert stored.email == body["email"]
        assert stored.locale == body["locale"]
        assert stored.timezone == body["timezone"]
        assert stored.active is True

    async def test_repeat_updates_and_spares_comms_owned_fields(
        self, client: AsyncClient
    ) -> None:
        """Repeat axis, and the ownership boundary in one test.

        The second call must find the existing row rather than add a
        second one -- and must not touch quiet_*, which the recipient
        owns through the preferences API and the product knows nothing
        about. A re-sync that wiped someone's quiet hours would be
        invisible until the notification that woke them at 3am.
        """
        factory = get_session_factory()
        async with factory() as session:
            seeded = await create_recipient(
                session, telegram_id=next_t64_telegram_id(), locale="de"
            )
            recipient_id = seeded.id
            seeded.quiet_from = time(22, 0)
            seeded.quiet_to = time(8, 0)
            seeded.quiet_days = [1, 5]
            await session.commit()

        body = _snapshot(locale="fr")
        first = await client.put(_url(recipient_id), json=body)
        second = await client.put(_url(recipient_id), json=body)

        assert first.status_code == 200
        assert second.status_code == 200
        assert first.json() == second.json()

        stored = await _load(recipient_id)
        assert stored is not None
        assert stored.locale == "fr"
        assert stored.quiet_from is not None, "comms-owned field survived"
        assert stored.quiet_to is not None
        assert stored.quiet_days == [1, 5]

        async with factory() as session:
            count = await session.scalar(
                select(func.count()).select_from(Recipient)
            )
        assert count == 1, "a repeat upsert must not add a row"


# ---------------------------------------------------------------------------
# 3-4. Snapshot discipline
# ---------------------------------------------------------------------------


class TestSnapshotDiscipline:
    async def test_nulls_overwrite_previously_synced_values(
        self, client: AsyncClient
    ) -> None:
        """Emptiness axis: null is a VALUE, not an omission.

        This is the sharpest assertion of the delivery. If a later
        snapshot's nulls were read as "keep", a user who cleared their
        e-mail or left Telegram would keep receiving messages there,
        and nothing anywhere would report an error.
        """
        recipient_id = uuid4()
        await client.put(_url(recipient_id), json=_snapshot())

        cleared = _snapshot(telegram_id=None, email=None, timezone=None)
        response = await client.put(_url(recipient_id), json=cleared)

        assert response.status_code == 200
        stored = await _load(recipient_id)
        assert stored is not None
        assert stored.telegram_id is None
        assert stored.email is None
        assert stored.timezone is None

    async def test_absent_field_is_rejected(
        self, client: AsyncClient
    ) -> None:
        """Shortage axis: a missing key must not become a default.

        timezone is the one that would hurt quietly: defaulting it to
        None would silently clear a synced zone on every product that
        forgot to send it.
        """
        body = _snapshot()
        del body["timezone"]

        response = await client.put(_url(uuid4()), json=body)

        assert response.status_code == 422

    async def test_unknown_field_is_rejected(
        self, client: AsyncClient
    ) -> None:
        """A typo must not be silently dropped."""
        response = await client.put(
            _url(uuid4()), json=_snapshot(quiet_from="22:00")
        )
        assert response.status_code == 422

    async def test_unresolvable_timezone_is_stored_not_rejected(
        self, client: AsyncClient
    ) -> None:
        """Poison-pill rule: a bad value never jams the sync.

        The zone is kept as sent (with a warning at intake) and degrades
        to the service default when quiet hours are computed. Rejecting
        it here would block a product's whole identity sync on one row.
        """
        recipient_id = uuid4()
        response = await client.put(
            _url(recipient_id), json=_snapshot(timezone="Mars/Olympus")
        )

        assert response.status_code == 200
        stored = await _load(recipient_id)
        assert stored is not None
        assert stored.timezone == "Mars/Olympus"


# ---------------------------------------------------------------------------
# 6. The door is locked
# ---------------------------------------------------------------------------


class TestAuth:
    async def test_missing_token_is_rejected(
        self, client: AsyncClient, auth_enabled: None
    ) -> None:
        response = await client.put(_url(uuid4()), json=_snapshot())
        assert response.status_code == 401
        assert response.headers["WWW-Authenticate"] == "Bearer"

    async def test_wrong_scheme_is_rejected(
        self, client: AsyncClient, auth_enabled: None
    ) -> None:
        """Shortage axis on the credential: the right secret in the
        wrong scheme is still not authentication."""
        response = await client.put(
            _url(uuid4()),
            json=_snapshot(),
            headers={"Authorization": f"Basic {_TOKEN}"},
        )
        assert response.status_code == 401

    async def test_wrong_token_is_rejected(
        self, client: AsyncClient, auth_enabled: None
    ) -> None:
        response = await client.put(
            _url(uuid4()),
            json=_snapshot(),
            headers={"Authorization": f"Bearer {_TOKEN}-not-quite"},
        )
        assert response.status_code == 401
        assert _TOKEN not in response.text

    async def test_correct_token_passes(
        self, client: AsyncClient, auth_enabled: None
    ) -> None:
        recipient_id = uuid4()
        response = await client.put(
            _url(recipient_id),
            json=_snapshot(),
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert response.status_code == 200
        assert await _load(recipient_id) is not None
