# =============================================================================
# COMMS Service -- Sync Receivers (audience projection, Phase 2)
# =============================================================================
#
# Service-level receivers that keep the audience projection
# (recipients / group_memberships) in step with the product. The
# product emits `user_upserted` and `group_changed` events; a
# transport (HTTP / Redis Streams, Phase 3) will call these functions.
# Until then they are the contract -- tests call them directly.
#
# EVENT CONTRACT (what the transport must deliver):
#   user_upserted: {recipient_id, telegram_id, email, locale,
#                   timezone?, active}
#     -- full snapshot of the SIX sync-owned identity fields; the
#        product is the source of truth for them and every event
#        carries all of them (no partial patches). `timezone` is
#        optional in the payload: a product that does not track it
#        maps the absence to None, and comms falls back to
#        DEFAULT_TIMEZONE at quiet-hours computation time.
#   group_changed: {group_key, recipient_id, member}
#     -- member=True adds the pair, member=False removes it.
#
# IDEMPOTENCY: both receivers are safe to replay (at-least-once
# transports re-deliver). Re-applying the same event is a no-op.
#
# OWNERSHIP BOUNDARY: user_upserted writes ONLY the six identity
# fields above. Snapshot semantics apply to all of them: timezone=None
# in the event OVERWRITES a previously synced value with NULL ("None
# means keep" would be a partial patch smuggled into a snapshot
# contract). The comms-owned preference fields on Recipient
# (quiet_from / quiet_to / quiet_days) are never touched -- a re-sync
# must not wipe a recipient's settings. That is why this is a
# field-by-field upsert and not a session.merge.
#
# POISON-PILL RULE: a sync event is NEVER rejected for a bad value
# (that would jam the product's at-least-once stream on one row).
# An unknown timezone is stored as-is with a loud warning here (early
# signal) and degrades to DEFAULT_TIMEZONE at computation time
# (app/audience/quiet_hours.py).
# =============================================================================

from uuid import UUID
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import GroupMembership, Recipient
from app.core.config import settings
from app.core.exceptions import NotFoundError

logger = structlog.get_logger()


def _warn_if_unknown_timezone(
    recipient_id: UUID,
    timezone: str | None,
) -> None:
    """Early signal for an unresolvable synced timezone.

    Never rejects (poison-pill rule): the value is stored as-is and
    quiet-hours math degrades to DEFAULT_TIMEZONE with its own warning
    at computation time. This one fires at intake so the problem shows
    up in logs when the sync happens, not when a delivery is due.
    """
    if timezone is None:
        return
    try:
        ZoneInfo(timezone)
    except (KeyError, ValueError):
        logger.warning(
            "invalid_timezone_synced",
            recipient_id=str(recipient_id),
            timezone=timezone,
            fallback=settings.default_timezone,
        )


async def user_upserted(
    session: AsyncSession,
    *,
    recipient_id: UUID,
    telegram_id: int | None,
    email: str | None,
    locale: str,
    timezone: str | None,
    active: bool,
) -> Recipient:
    """Apply a `user_upserted` sync event (create or update).

    recipient_id is the PRODUCT user id (shared id-space, Model B).
    Writes only the sync-owned identity fields (incl. timezone, which
    the product owns -- e.g. VELO users.timezone); the comms-owned
    quiet_* preference fields survive re-syncs untouched. `timezone`
    is a required parameter on purpose: the transport must map "field
    absent in payload" to None explicitly (snapshot discipline), not
    get it silently via a default. Idempotent.
    """
    _warn_if_unknown_timezone(recipient_id, timezone)

    recipient = await session.get(Recipient, recipient_id)
    if recipient is None:
        recipient = Recipient(
            id=recipient_id,
            telegram_id=telegram_id,
            email=email,
            locale=locale,
            timezone=timezone,
            active=active,
        )
        session.add(recipient)
        await session.flush()
        logger.info(
            "recipient_created",
            recipient_id=str(recipient_id),
            active=active,
        )
        return recipient

    # Field-by-field on purpose: ONLY the product-owned identity
    # fields; quiet_* preferences stay untouched.
    recipient.telegram_id = telegram_id
    recipient.email = email
    recipient.locale = locale
    recipient.timezone = timezone
    recipient.active = active
    await session.flush()
    logger.info(
        "recipient_updated",
        recipient_id=str(recipient_id),
        active=active,
    )
    return recipient


async def group_changed(
    session: AsyncSession,
    *,
    group_key: str,
    recipient_id: UUID,
    member: bool,
) -> None:
    """Apply a `group_changed` sync event (add/remove membership).

    member=True ensures the (group_key, recipient_id) pair exists;
    member=False ensures it does not. Idempotent both ways. The
    recipient must have been synced first (user_upserted precedes
    group_changed in the product's event order); an unknown recipient
    is a sync-ordering bug and raises NotFoundError rather than
    silently creating a half-empty row.
    """
    if member:
        recipient = await session.get(Recipient, recipient_id)
        if recipient is None:
            raise NotFoundError(
                f"Cannot add unknown recipient {recipient_id} to group "
                f"{group_key!r}: user_upserted must precede group_changed"
            )
        existing = await session.scalar(
            select(GroupMembership).where(
                GroupMembership.group_key == group_key,
                GroupMembership.recipient_id == recipient_id,
            )
        )
        if existing is not None:
            return
        session.add(
            GroupMembership(group_key=group_key, recipient_id=recipient_id)
        )
        await session.flush()
        logger.info(
            "group_member_added",
            group_key=group_key,
            recipient_id=str(recipient_id),
        )
        return

    existing = await session.scalar(
        select(GroupMembership).where(
            GroupMembership.group_key == group_key,
            GroupMembership.recipient_id == recipient_id,
        )
    )
    if existing is None:
        return
    await session.delete(existing)
    await session.flush()
    logger.info(
        "group_member_removed",
        group_key=group_key,
        recipient_id=str(recipient_id),
    )
