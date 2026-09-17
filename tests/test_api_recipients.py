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

import json
from datetime import time
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from app.api.recipients import RecipientSnapshot
from app.audience.models import GroupMembership, Recipient
from app.core.config import settings
from app.core.constants import (
    MAX_EMAIL_LEN,
    MAX_GROUP_KEY_LEN,
    MAX_LOCALE_LEN,
    MAX_TELEGRAM_ID,
    MAX_TIMEZONE_LEN,
    MIN_TELEGRAM_ID,
)
from app.core.database import get_session_factory
from app.core.exceptions import ValidationError as ServiceValidationError
from app.transport.events import parse_event
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


def _parse_user_upserted(data: dict[str, Any]) -> Any:
    """The same snapshot as it arrives over the event stream.

    The route and the stream are two doors into one column, and this
    file checks both against one constant -- that pairing IS the item.
    """
    return parse_event({
        "event": "user_upserted",
        "data": json.dumps({"v": 1, "recipient_id": str(uuid4()), **data}),
    })


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


# ---------------------------------------------------------------------------
# 7. Input bounds -- one width per field, read by all three places (R-2)
# ---------------------------------------------------------------------------


def _unbanded(**overrides: Any) -> dict[str, Any]:
    """A full snapshot that draws NO id from the T-64 band.

    The band is twenty ids wide and the allocator refuses to overflow
    it (tests/helpers.py explains why it is that narrow). The bound
    tests below are about string widths and about the two ends of the
    BigInteger range -- none of them needs an id from the shared space,
    and spending one per parametrized case would exhaust the band for
    the tests that do.
    """
    body: dict[str, Any] = {
        "telegram_id": None,
        "email": "recipient@example.test",
        "locale": "en",
        "timezone": "Europe/Berlin",
        "active": True,
    }
    body.update(overrides)
    return body


class TestSnapshotBounds:
    """Every bounded field of the snapshot, at the bound and past it.

    Before R-2 none of these four had a bound here, and each of them
    reached the INSERT: the caller got a 500 naming nothing, on input
    only the caller could fix.
    """

    @pytest.mark.parametrize(
        ("field", "over"),
        [
            ("email", "a" * (MAX_EMAIL_LEN - 11) + "@example.test"),
            ("locale", "x" * (MAX_LOCALE_LEN + 1)),
            ("timezone", "z" * (MAX_TIMEZONE_LEN + 1)),
            ("telegram_id", MAX_TELEGRAM_ID + 1),
            ("telegram_id", MIN_TELEGRAM_ID - 1),
        ],
    )
    async def test_past_the_bound_is_422(
        self, client: AsyncClient, field: str, over: Any,
    ) -> None:
        response = await client.put(
            _url(uuid4()), json=_unbanded(**{field: over}),
        )
        assert response.status_code == 422, response.text
        assert await _load(uuid4()) is None

    @pytest.mark.parametrize(
        ("field", "at"),
        [
            ("email", "a" * (MAX_EMAIL_LEN - 13) + "@example.test"),
            ("locale", "x" * MAX_LOCALE_LEN),
            ("timezone", "z" * MAX_TIMEZONE_LEN),
        ],
    )
    async def test_exactly_at_the_bound_is_stored(
        self, client: AsyncClient, field: str, at: str,
    ) -> None:
        """The pair to the refusals above.

        Without it, a model that rejected EVERYTHING would pass the
        test above and look correct.
        """
        recipient_id = uuid4()
        response = await client.put(
            _url(recipient_id), json=_unbanded(**{field: at}),
        )
        assert response.status_code == 200, response.text
        stored = await _load(recipient_id)
        assert stored is not None
        assert getattr(stored, field) == at
        assert len(at) == {
            "email": MAX_EMAIL_LEN,
            "locale": MAX_LOCALE_LEN,
            "timezone": MAX_TIMEZONE_LEN,
        }[field]

    def test_telegram_id_at_the_bound_is_accepted(self) -> None:
        """Checked on the MODEL, not through the route, on purpose.

        Accepting it through the route would store a recipient whose
        telegram_id sits outside this suite's assigned band (see
        tests/helpers.py) -- the id space is shared with the products,
        and the band rule has no exceptions for convenience. The
        refusal one past the bound is driven through the route above,
        where nothing is stored.
        """
        assert RecipientSnapshot(
            telegram_id=MAX_TELEGRAM_ID,
            email=None,
            locale="en",
            timezone=None,
            active=True,
        ).telegram_id == MAX_TELEGRAM_ID

    async def test_nulls_are_still_accepted(
        self, client: AsyncClient,
    ) -> None:
        """PUSTOTA on the nullable fields: a bound is not a demand for
        a value. The snapshot contract carries explicit nulls, and
        max_length on an optional field must not turn None into a 422.
        """
        recipient_id = uuid4()
        response = await client.put(
            _url(recipient_id),
            json=_unbanded(email=None, timezone=None),
        )
        assert response.status_code == 200, response.text
        stored = await _load(recipient_id)
        assert stored is not None
        assert stored.email is None


class TestOneWidthPerField:
    """The three places that must not drift: column, route, envelope.

    This is the actual claim of R-2 item 1 -- not "there is a bound"
    but "there is ONE bound, and everybody reads it". The failure it
    pins is the one the release fixed: the envelope accepted 20
    characters of locale against a column of MAX_LOCALE_LEN, and the
    difference died on the INSERT.
    """

    @pytest.mark.parametrize(
        ("column", "constant"),
        [
            ("email", MAX_EMAIL_LEN),
            ("locale", MAX_LOCALE_LEN),
            ("timezone", MAX_TIMEZONE_LEN),
        ],
    )
    def test_column_width_is_the_constant(
        self, column: str, constant: int,
    ) -> None:
        assert Recipient.__table__.c[column].type.length == constant

    def test_group_key_column_width_is_the_constant(self) -> None:
        assert (
            GroupMembership.__table__.c.group_key.type.length
            == MAX_GROUP_KEY_LEN
        )

    @pytest.mark.parametrize(
        ("field", "constant"),
        [
            ("email", MAX_EMAIL_LEN),
            ("locale", MAX_LOCALE_LEN),
            ("timezone", MAX_TIMEZONE_LEN),
        ],
    )
    def test_route_model_bound_is_the_constant(
        self, field: str, constant: int,
    ) -> None:
        """Read off the model's own metadata rather than re-stated:
        a test that repeated the number would drift with the code it
        is meant to hold still."""
        bounds = [
            item.max_length
            for item in RecipientSnapshot.model_fields[field].metadata
            if getattr(item, "max_length", None) is not None
        ]
        assert bounds == [constant]

    @pytest.mark.parametrize(
        ("field", "constant"),
        [
            ("email", MAX_EMAIL_LEN),
            ("locale", MAX_LOCALE_LEN),
            ("timezone", MAX_TIMEZONE_LEN),
        ],
    )
    def test_envelope_bound_is_the_same_constant(
        self, field: str, constant: int,
    ) -> None:
        """The envelope is the third place, and the one that used to
        disagree. One character past the shared constant is a terminal
        parse error; exactly the constant parses."""
        at_bound = _unbanded(email=None, timezone=None)
        at_bound[field] = "y" * constant
        parsed = _parse_user_upserted(at_bound)
        assert getattr(parsed, field) == "y" * constant

        too_long = dict(at_bound)
        too_long[field] = "y" * (constant + 1)
        with pytest.raises(ServiceValidationError, match=field):
            _parse_user_upserted(too_long)
