# =============================================================================
# COMMS Service -- Test Helpers
# =============================================================================
#
# TEST-BAND RULE (handoff / dispatch plan §5):
#   recipient.id = product user id -- the id-space is SHARED with the
#   product. Every test telegram_id therefore comes from the band
#   assigned to comms Phase 1:
#
#       80000-80999   (comms engine tests -- THIS band)
#
#   89xxx belongs to VELO's own suites -- never use it here.
#   next_telegram_id() hands out ids from the band and refuses to
#   overflow it.
# =============================================================================

import itertools
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import GroupMembership, Recipient

TELEGRAM_ID_BAND_START = 80000
TELEGRAM_ID_BAND_END = 80999

_telegram_id_counter = itertools.count(TELEGRAM_ID_BAND_START)


def next_telegram_id() -> int:
    """Next telegram_id from the comms test band (80000-80999)."""
    tid = next(_telegram_id_counter)
    if tid > TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 80000-80999 exhausted"
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
