# =============================================================================
# COMMS Service -- Test Helpers
# =============================================================================
#
# TEST-BAND RULE (handoff / dispatch plan §5):
#   recipient.id = product user id -- the id-space is SHARED with the
#   product. Every test telegram_id therefore comes from the band
#   assigned to the phase:
#
#       80000-80999   comms Phase 1 (engine tests)
#       81000-81999   comms Phase 2 (profile / prefs / gating tests)
#       82000-82999   comms Phase 3a (presentation / retention tests)
#
#   83xxx and 89xxx belong to VELO's own suites -- never use them
#   here. The band allocators below hand out ids and refuse to
#   overflow their band.
# =============================================================================

import itertools
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import GroupMembership, Recipient
from app.audience.sync import apply_snapshot, snapshot_fingerprint
from app.messaging.models import Section

TELEGRAM_ID_BAND_START = 80000
TELEGRAM_ID_BAND_END = 80999

_telegram_id_counter = itertools.count(TELEGRAM_ID_BAND_START)


def next_telegram_id() -> int:
    """Next telegram_id from the comms Phase 1 band (80000-80999)."""
    tid = next(_telegram_id_counter)
    if tid > TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 80000-80999 exhausted"
        )
    return tid


PHASE2_TELEGRAM_ID_BAND_START = 81000
PHASE2_TELEGRAM_ID_BAND_END = 81999

_phase2_telegram_id_counter = itertools.count(PHASE2_TELEGRAM_ID_BAND_START)


def next_phase2_telegram_id() -> int:
    """Next telegram_id from the comms Phase 2 band (81000-81999)."""
    tid = next(_phase2_telegram_id_counter)
    if tid > PHASE2_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 81000-81999 exhausted"
        )
    return tid


PHASE3A_TELEGRAM_ID_BAND_START = 82000
PHASE3A_TELEGRAM_ID_BAND_END = 82999

_phase3a_telegram_id_counter = itertools.count(PHASE3A_TELEGRAM_ID_BAND_START)


def next_phase3a_telegram_id() -> int:
    """Next telegram_id from the comms Phase 3a band (82000-82999)."""
    tid = next(_phase3a_telegram_id_counter)
    if tid > PHASE3A_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 82000-82999 exhausted"
        )
    return tid


PHASE3C_TELEGRAM_ID_BAND_START = 85000
PHASE3C_TELEGRAM_ID_BAND_END = 85999

_phase3c_telegram_id_counter = itertools.count(PHASE3C_TELEGRAM_ID_BAND_START)


def next_phase3c_telegram_id() -> int:
    """Next telegram_id from the comms Phase 3c band (85000-85999)."""
    tid = next(_phase3c_telegram_id_counter)
    if tid > PHASE3C_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 85000-85999 exhausted"
        )
    return tid


PHASE3B_TELEGRAM_ID_BAND_START = 84000
PHASE3B_TELEGRAM_ID_BAND_END = 84999

_phase3b_telegram_id_counter = itertools.count(PHASE3B_TELEGRAM_ID_BAND_START)


def next_phase3b_telegram_id() -> int:
    """Next telegram_id from the comms Phase 3b band (84000-84999).

    83000-83999 is SKIPPED on purpose: VELO facts live there (see the
    band registry in the dispatch plan; discovered in Phase 1).
    """
    tid = next(_phase3b_telegram_id_counter)
    if tid > PHASE3B_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 84000-84999 exhausted"
        )
    return tid


PHASE4A_TELEGRAM_ID_BAND_START = 86000
PHASE4A_TELEGRAM_ID_BAND_END = 86999

_phase4a_telegram_id_counter = itertools.count(PHASE4A_TELEGRAM_ID_BAND_START)


def next_phase4a_telegram_id() -> int:
    """Next telegram_id from the comms Phase 4a band (86000-86999).

    85000-85999 belongs to Phase 3c; 83xxx / 89xxx are VELO's -- never
    reused here.
    """
    tid = next(_phase4a_telegram_id_counter)
    if tid > PHASE4A_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 86000-86999 exhausted"
        )
    return tid


PHASE4B_TELEGRAM_ID_BAND_START = 87000
PHASE4B_TELEGRAM_ID_BAND_END = 87999

_phase4b_telegram_id_counter = itertools.count(PHASE4B_TELEGRAM_ID_BAND_START)


def next_phase4b_telegram_id() -> int:
    """Next telegram_id from the comms Phase 4b band (87000-87999)."""
    tid = next(_phase4b_telegram_id_counter)
    if tid > PHASE4B_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 87000-87999 exhausted"
        )
    return tid


PHASE4C_TELEGRAM_ID_BAND_START = 88000
PHASE4C_TELEGRAM_ID_BAND_END = 88999

_phase4c_telegram_id_counter = itertools.count(PHASE4C_TELEGRAM_ID_BAND_START)


def next_phase4c_telegram_id() -> int:
    """Next telegram_id from the comms Phase 4c band (88000-88999)."""
    tid = next(_phase4c_telegram_id_counter)
    if tid > PHASE4C_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 88000-88999 exhausted"
        )
    return tid


SEAM_T2_TELEGRAM_ID_BAND_START = 92100
SEAM_T2_TELEGRAM_ID_BAND_END = 92139

_seam_t2_telegram_id_counter = itertools.count(SEAM_T2_TELEGRAM_ID_BAND_START)


def next_seam_t2_telegram_id() -> int:
    """Next telegram_id from the comms seam-T2 band (92100-92139).

    A NARROW band, unlike the per-phase thousands above: the seam adds
    a handful of actors, not a module.
    """
    tid = next(_seam_t2_telegram_id_counter)
    if tid > SEAM_T2_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 92100-92139 exhausted"
        )
    return tid


T51_TELEGRAM_ID_BAND_START = 92140
T51_TELEGRAM_ID_BAND_END = 92179

_t51_telegram_id_counter = itertools.count(T51_TELEGRAM_ID_BAND_START)


def next_t51_telegram_id() -> int:
    """Next telegram_id from the T-51 unread-aggregates band.

    Narrow, like the seam-T2 band above: the unread contracts add a
    handful of actors (a client, a master, an agent, a supervisor),
    not a module.
    """
    tid = next(_t51_telegram_id_counter)
    if tid > T51_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 92140-92179 exhausted"
        )
    return tid


T64_TELEGRAM_ID_BAND_START = 92180
T64_TELEGRAM_ID_BAND_END = 92199

_t64_telegram_id_counter = itertools.count(T64_TELEGRAM_ID_BAND_START)


def next_t64_telegram_id() -> int:
    """Next telegram_id from the T-64 recipient-upsert band.

    NARROWER THAN THE BAND THE HANDOFF ASSIGNED, on purpose. T-64 was
    issued 92140-92199, but 92140-92179 is already held by the T-51
    allocator right above -- the registry double-booked the lower half.
    Rather than hand out ids a sibling suite also hands out, this
    allocator takes only the free remainder. Twenty ids is ample: these
    tests need a handful of recipients, not a module.
    """
    tid = next(_t64_telegram_id_counter)
    if tid > T64_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 92180-92199 exhausted"
        )
    return tid


T67_TELEGRAM_ID_BAND_START = 92200
T67_TELEGRAM_ID_BAND_END = 92259

_t67_telegram_id_counter = itertools.count(T67_TELEGRAM_ID_BAND_START)


def next_t67_telegram_id() -> int:
    """Next telegram_id from the T-67 section-membership band.

    92200-92259, checked against every allocator above before being
    taken: the highest previously held id is 92199 (T-64), so this band
    starts one past the end of the occupied space rather than where a
    document says it should. The registry has double-booked before --
    see the T-64 allocator's own note.
    """
    tid = next(_t67_telegram_id_counter)
    if tid > T67_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 92200-92259 exhausted"
        )
    return tid


async def create_recipient(
    session: AsyncSession,
    *,
    recipient_id: UUID | None = None,
    telegram_id: int | None = None,
    email: str | None = None,
    locale: str = "en",
    active: bool = True,
) -> Recipient:
    """Create a recipient row (id = simulated product user id)."""
    resolved_tg = telegram_id if telegram_id is not None else next_telegram_id()
    recipient = Recipient(
        id=recipient_id or uuid4(),
        # A row written straight into the table stands for a snapshot
        # the product sent at version 1 (F1.4: every row has one).
        version=1,
        snapshot_fingerprint=snapshot_fingerprint(
            telegram_id=resolved_tg, email=email, locale=locale,
            timezone=None, active=active,
        ),
        telegram_id=resolved_tg,
        email=email,
        locale=locale,
        active=active,
    )
    session.add(recipient)
    await session.flush()
    return recipient


async def add_to_group(
    session: AsyncSession,
    group_key: str,
    recipient_id: UUID,
) -> GroupMembership:
    """Add a recipient to an opaque product group."""
    membership = GroupMembership(
        group_key=group_key,
        recipient_id=recipient_id,
    )
    session.add(membership)
    await session.flush()
    return membership


async def create_section(
    session: AsyncSession,
    *,
    key: str,
    label: str | None = None,
) -> Section:
    """Create a messaging Section row (Phase 4a)."""
    section = Section(key=key, label=label if label is not None else key)
    session.add(section)
    await session.flush()
    return section


# -- Intake (F1.2) -------------------------------------------------------------
# Every job carries an idempotency key and a fingerprint. Tests that
# create notifications directly (not through intake) get a FRESH key per
# call -- two direct creates never collide -- and one fixed fingerprint:
# the key alone decides, the fingerprint only matters to intake tests,
# which build their own.
TEST_FINGERPRINT = "f" * 64


def intake_fields() -> dict[str, str]:
    """A fresh idempotency key and the test fingerprint."""
    return {"idempotency_key": f"test:{uuid4()}", "fingerprint": TEST_FINGERPRINT}


def notification_row_fields(channels: list[str] | None = None) -> dict[str, object]:
    """The NOT NULL intake columns for a Notification built DIRECTLY
    (tests that construct rows around intake, F1.2). A fresh key, the
    test fingerprint, the channels the row is for (in_app unless said)
    and the default expiry layer -- what intake would have written for a
    type with no declared expiry."""
    return {
        **intake_fields(),
        "channels": channels if channels is not None else ["in_app"],
        "expiry_layer": "default",
    }


def configure_every_channel(monkeypatch: object) -> None:
    """Make every external channel LIVE on the suite's settings.

    The suite runs with every external channel's keys blanked
    (tests/conftest.py), and since F1.1 the startup path refuses a
    route into a channel the deploy did not configure. The fixture
    profile routes types to telegram and email (F1.2: the channel is
    the profile's), so a test that loads it through the STARTUP path
    must stand on a deploy that has them.
    """
    from app.core.config import settings

    for name, value in {
        "telegram_bot_token": "123456:unit-test-bot-token",
        "telegram_bot_url": "https://t.me/unit_test_bot",
        "email_mailgun_api_key": "key-unit-test",
        "email_mailgun_domain": "mg.unit-test.invalid",
        "email_from_address": "comms@unit-test.invalid",
    }.items():
        monkeypatch.setattr(settings, name, value)  # type: ignore[attr-defined]



# -- Snapshots (F1.4) ----------------------------------------------------------
# A snapshot carries the product's monotonic version. Tests that apply a
# sequence of snapshots to one recipient -- the way a product changes a
# person over time -- take the next version from one counter, so each
# call supersedes the one before it, as the product's would.
_snapshot_versions = itertools.count(1)


def next_snapshot_version() -> int:
    return next(_snapshot_versions)


async def upsert_snapshot(session: AsyncSession, **fields: Any) -> Recipient:
    """apply_snapshot with the next version (see above)."""
    return await apply_snapshot(
        session, version=next_snapshot_version(), **fields,
    )
