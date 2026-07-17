# =============================================================================
# COMMS Service -- Preferences facade tests (Phase 3b item 3) -- frozen
# =============================================================================
#
# The E8-shaped facade over the two preference homes (category_mutes +
# quiet_* recipient columns). Fixture profile declares two categories:
# unit_updates and unit_reminder (types.yaml). Seeds commit (the
# request handler opens its own session); clean_db wipes between
# tests. telegram_ids from the Phase 3b band 84000-84999.
# =============================================================================

from uuid import UUID, uuid4

from httpx import AsyncClient

from app.core.database import get_session_factory
from tests.helpers import create_recipient, next_phase3b_telegram_id

_SCHEDULE = {"from": "22:00", "to": "08:00", "days": ["mon", "fri"]}


async def _seed_recipient() -> UUID:
    factory = get_session_factory()
    async with factory() as session:
        recipient = await create_recipient(
            session, telegram_id=next_phase3b_telegram_id(),
        )
        recipient_id = recipient.id
        await session.commit()
    return recipient_id


def _prefs(recipient_id: UUID) -> str:
    return f"/api/v1/recipients/{recipient_id}/preferences"


class TestGetForm:
    async def test_default_form(self, client: AsyncClient) -> None:
        """Fresh recipient: every declared category enabled, no
        schedule, timezone unset (nothing synced one)."""
        recipient_id = await _seed_recipient()
        response = await client.get(_prefs(recipient_id))
        assert response.status_code == 200
        assert response.json() == {
            "categories": {
                "unit_reminder": True, "unit_updates": True,
                "msg_participants": True, "msg_support": True,
            },
            "schedule": None,
            "timezone": None,
        }

    async def test_unknown_recipient_is_404(
        self, client: AsyncClient,
    ) -> None:
        """Unlike the inbox: preferences hang on the recipient row,
        so an unsynced recipient has none to show."""
        response = await client.get(_prefs(uuid4()))
        assert response.status_code == 404


class TestPatchCategories:
    async def test_toggle_off_and_back_on(
        self, client: AsyncClient,
    ) -> None:
        recipient_id = await _seed_recipient()

        response = await client.patch(
            _prefs(recipient_id),
            json={"categories": {"unit_reminder": False}},
        )
        assert response.status_code == 200
        form = response.json()
        # PATCH answers with the FULL updated form.
        assert form["categories"] == {
            "unit_reminder": False, "unit_updates": True,
            "msg_participants": True, "msg_support": True,
        }
        assert form["schedule"] is None

        response = await client.patch(
            _prefs(recipient_id),
            json={"categories": {"unit_reminder": True}},
        )
        assert response.json()["categories"] == {
            "unit_reminder": True, "unit_updates": True,
            "msg_participants": True, "msg_support": True,
        }

    async def test_partial_touches_only_listed(
        self, client: AsyncClient,
    ) -> None:
        recipient_id = await _seed_recipient()
        await client.patch(
            _prefs(recipient_id),
            json={"categories": {"unit_updates": False}, "schedule": _SCHEDULE},
        )
        # A later patch listing NEITHER the other toggle NOR the
        # schedule leaves both alone.
        response = await client.patch(
            _prefs(recipient_id),
            json={"categories": {"unit_reminder": False}},
        )
        form = response.json()
        assert form["categories"] == {
            "unit_reminder": False, "unit_updates": False,
            "msg_participants": True, "msg_support": True,
        }
        assert form["schedule"] == _SCHEDULE

    async def test_unknown_category_is_422(
        self, client: AsyncClient,
    ) -> None:
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"categories": {"not_a_category": False}},
        )
        assert response.status_code == 422

    async def test_unknown_recipient_is_404(
        self, client: AsyncClient,
    ) -> None:
        response = await client.patch(
            _prefs(uuid4()),
            json={"categories": {"unit_updates": False}},
        )
        assert response.status_code == 404


class TestPatchSchedule:
    async def test_set_and_round_trip(self, client: AsyncClient) -> None:
        """Write-then-read is a fixed point (frozen contract)."""
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id), json={"schedule": _SCHEDULE},
        )
        assert response.status_code == 200
        assert response.json()["schedule"] == _SCHEDULE
        assert (
            (await client.get(_prefs(recipient_id))).json()["schedule"]
            == _SCHEDULE
        )

    async def test_days_normalized_to_week_order(
        self, client: AsyncClient,
    ) -> None:
        """Input day order is free; the form comes back mon..sun,
        de-duplicated -- ONE canonical spelling per window."""
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": {
                "from": "23:00", "to": "07:00",
                "days": ["sun", "mon", "sun"],
            }},
        )
        assert response.json()["schedule"]["days"] == ["mon", "sun"]

    async def test_clear_with_null(self, client: AsyncClient) -> None:
        recipient_id = await _seed_recipient()
        await client.patch(_prefs(recipient_id), json={"schedule": _SCHEDULE})
        response = await client.patch(
            _prefs(recipient_id), json={"schedule": None},
        )
        assert response.status_code == 200
        assert response.json()["schedule"] is None

    async def test_bad_day_code_is_422(self, client: AsyncClient) -> None:
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": {
                "from": "22:00", "to": "08:00", "days": ["monday"],
            }},
        )
        assert response.status_code == 422

    async def test_empty_days_is_422(self, client: AsyncClient) -> None:
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": {"from": "22:00", "to": "08:00", "days": []}},
        )
        assert response.status_code == 422

    async def test_partial_window_is_422(
        self, client: AsyncClient,
    ) -> None:
        """schedule replaces WHOLE: a window missing a field is not
        a partial update, it is a malformed window."""
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": {"from": "22:00", "days": ["mon"]}},
        )
        assert response.status_code == 422

    async def test_seconds_are_422(self, client: AsyncClient) -> None:
        """The wire granularity is HH:MM -- sub-minute input would
        round-trip unstably."""
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": {
                "from": "22:00:30", "to": "08:00", "days": ["mon"],
            }},
        )
        assert response.status_code == 422


class TestTimezoneReadOnly:
    async def test_timezone_in_patch_is_422(
        self, client: AsyncClient,
    ) -> None:
        """timezone is sync-owned (arch §2.5): writing it here must
        fail loudly, not be silently dropped."""
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id), json={"timezone": "Europe/Berlin"},
        )
        assert response.status_code == 422

    async def test_unknown_key_is_422(self, client: AsyncClient) -> None:
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id), json={"schedul": _SCHEDULE},
        )
        assert response.status_code == 422

    async def test_synced_timezone_is_displayed(
        self, client: AsyncClient,
    ) -> None:
        recipient_id = await _seed_recipient()
        # Simulate the sync writing identity (product-owned field).
        factory = get_session_factory()
        async with factory() as session:
            from app.audience.models import Recipient

            recipient = await session.get(Recipient, recipient_id)
            assert recipient is not None
            recipient.timezone = "Europe/Berlin"
            await session.commit()

        response = await client.get(_prefs(recipient_id))
        assert response.json()["timezone"] == "Europe/Berlin"
