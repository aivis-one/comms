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
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import GroupMembership, Recipient

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
    recipient = Recipient(
        id=recipient_id or uuid4(),
        telegram_id=(
            telegram_id if telegram_id is not None else next_telegram_id()
        ),
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
