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

# The wire form after R-5: a LIST of periods when delivery is
# ALLOWED, each owned by its own day. The old constant was one quiet
# window with a day set -- {"from": "22:00", "to": "08:00", "days":
# ["mon", "fri"]} -- and it could not say what this says.
_SCHEDULE = [
    {"day": "mon", "from": "09:00", "to": "21:00"},
    {"day": "fri", "from": "09:00", "to": "24:00"},
]


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

    async def test_periods_come_back_in_canonical_order(
        self, client: AsyncClient,
    ) -> None:
        """Input order is free; the form comes back sorted by day then
        start -- ONE canonical spelling per schedule.

        The old form of this test asserted the same property about the
        DAY LIST of a single window (mon..sun, de-duplicated). It was
        right about that model; there is no day list any more, so the
        property moved to the periods themselves.
        """
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": [
                {"day": "fri", "from": "09:00", "to": "12:00"},
                {"day": "mon", "from": "18:00", "to": "20:00"},
                {"day": "mon", "from": "09:00", "to": "12:00"},
            ]},
        )
        assert response.status_code == 200, response.text
        assert response.json()["schedule"] == [
            {"day": "mon", "from": "09:00", "to": "12:00"},
            {"day": "mon", "from": "18:00", "to": "20:00"},
            {"day": "fri", "from": "09:00", "to": "12:00"},
        ]

    async def test_touching_periods_are_422(
        self, client: AsyncClient,
    ) -> None:
        """One stretch has one spelling: 09-12 plus 12-18 is 09-18
        written twice, and accepting both would let two stored values
        mean one thing -- the defect class this release removes.
        Refused rather than merged: a merge would change what the
        person entered without telling them.
        """
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": [
                {"day": "mon", "from": "09:00", "to": "12:00"},
                {"day": "mon", "from": "12:00", "to": "18:00"},
            ]},
        )
        assert response.status_code == 422, response.text

    async def test_a_full_day_ends_at_24_00(
        self, client: AsyncClient,
    ) -> None:
        """THE ONE-MINUTE HOLE, closed on the wire too. The old model
        could only approximate a whole day as 00:00-23:59, and a
        notification went out at 23:59 on a day marked silent."""
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": [
                {"day": "sat", "from": "00:00", "to": "24:00"},
            ]},
        )
        assert response.status_code == 200, response.text
        assert response.json()["schedule"] == [
            {"day": "sat", "from": "00:00", "to": "24:00"},
        ]

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
            json={"schedule": [
                {"day": "monday", "from": "09:00", "to": "12:00"},
            ]},
        )
        assert response.status_code == 422

    async def test_empty_schedule_is_422(self, client: AsyncClient) -> None:
        """An empty list would mean "never deliver": the deliveries
        defer until they expire, which is a black hole rather than a
        schedule. Clearing is `null`, and muting is what says "do not
        send me this"."""
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id), json={"schedule": []},
        )
        assert response.status_code == 422

    async def test_partial_period_is_422(
        self, client: AsyncClient,
    ) -> None:
        """schedule replaces WHOLE: a period missing a field is not a
        partial update, it is a malformed period."""
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": [{"day": "mon", "from": "09:00"}]},
        )
        assert response.status_code == 422

    async def test_seconds_are_422(self, client: AsyncClient) -> None:
        """The wire granularity is HH:MM -- sub-minute input would
        round-trip unstably."""
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": [
                {"day": "mon", "from": "09:00:30", "to": "12:00"},
            ]},
        )
        assert response.status_code == 422

    async def test_a_start_of_24_00_is_422(
        self, client: AsyncClient,
    ) -> None:
        """24:00 is an END, not a time: a period starting there has
        nothing after it."""
        recipient_id = await _seed_recipient()
        response = await client.patch(
            _prefs(recipient_id),
            json={"schedule": [
                {"day": "mon", "from": "24:00", "to": "24:00"},
            ]},
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
