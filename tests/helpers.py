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
