# =============================================================================
# COMMS Service -- Recipient Preferences (Phase 2)
# =============================================================================
#
# Read/write API for per-recipient notification preferences:
#   - category mutes (CategoryMute rows; presence = muted),
#   - quiet hours (window on the Recipient row).
#
# TIMEZONE IS NOT A PREFERENCE (Phase 2.1): it is product-owned
# identity synced via user_upserted (a re-sync would silently clobber
# anything written here, so there is deliberately NO set_timezone).
# "The user changed their timezone" is a PRODUCT feature: the product
# updates its own user record and syncs. RecipientPreferences exposes
# the synced value read-only for display.
#
# Categories are PROFILE vocabulary: the type dictionary maps each
# type to a category (family granularity -- reminder_24h/1h/10min all
# map to "reminder"), and writes here validate against
# registry.registered_categories(). No hardcoded category list exists
# in comms.
#
# GATING CONSUMERS:
#   - service.resolve_notification calls muted_recipient_ids() to drop
#     muted recipients before deliveries are created;
#   - service.deliver_notification calls quiet_hours.recipient_quiet_until
#     to defer sends inside a quiet window.
# =============================================================================

from dataclasses import dataclass
from datetime import time
from uuid import UUID

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import CategoryMute, Recipient
from app.core.exceptions import NotFoundError, ValidationError
from app.engine.registry import registry

logger = structlog.get_logger()


@dataclass(frozen=True)
class RecipientPreferences:
    """Snapshot of one recipient's preferences.

    `timezone` is read-only here: sync-owned (see module docstring),
    included for display alongside the quiet-hours window it governs.
    """

    recipient_id: UUID
    muted_categories: frozenset[str]
    timezone: str | None
    quiet_from: time | None
    quiet_to: time | None
    quiet_days: tuple[int, ...] | None


async def _get_recipient(
    session: AsyncSession,
    recipient_id: UUID,
) -> Recipient:
    """Load a recipient or raise NotFoundError."""
    recipient = await session.get(Recipient, recipient_id)
    if recipient is None:
        raise NotFoundError(f"Recipient {recipient_id} does not exist")
    return recipient


def _validate_category(category: str) -> None:
    """Reject categories the profile does not declare."""
    known = registry.registered_categories()
    if category not in known:
        raise ValidationError(
            f"Unknown preference category: {category!r}. "
            f"Profile declares: {', '.join(sorted(known)) or '(none)'}"
        )


async def set_category_muted(
    session: AsyncSession,
    recipient_id: UUID,
    category: str,
    muted: bool,
) -> None:
    """Mute or unmute one category for one recipient. Idempotent."""
    _validate_category(category)
    await _get_recipient(session, recipient_id)

    existing = await session.scalar(
        select(CategoryMute).where(
            CategoryMute.recipient_id == recipient_id,
            CategoryMute.category == category,
        )
    )
    if muted:
        if existing is not None:
            return
        session.add(
            CategoryMute(recipient_id=recipient_id, category=category)
        )
        await session.flush()
        logger.info(
            "category_muted",
            recipient_id=str(recipient_id),
            category=category,
        )
        return

    if existing is None:
        return
    await session.delete(existing)
    await session.flush()
    logger.info(
        "category_unmuted",
        recipient_id=str(recipient_id),
        category=category,
    )


async def set_quiet_hours(
    session: AsyncSession,
    recipient_id: UUID,
    *,
    quiet_from: time | None,
    quiet_to: time | None,
    days: list[int] | None,
) -> None:
    """Set or clear the recipient's quiet-hours window.

    All-or-nothing: either all three of (quiet_from, quiet_to, days)
    are provided, or all three are None (= clear). A partial window is
    meaningless and rejected. from == to is rejected (a zero-length or
    ambiguous 24h window); from > to means overnight. Days are ISO
    weekdays (1=Mon..7=Sun) of window STARTS, stored de-duplicated
    and sorted.
    """
    recipient = await _get_recipient(session, recipient_id)

    fields = (quiet_from, quiet_to, days)
    if all(f is None for f in fields):
        recipient.quiet_from = None
        recipient.quiet_to = None
        recipient.quiet_days = None
        await session.flush()
        logger.info("quiet_hours_cleared", recipient_id=str(recipient_id))
        return
    if any(f is None for f in fields):
        raise ValidationError(
            "Quiet hours are all-or-nothing: provide quiet_from, "
            "quiet_to and days together, or all None to clear"
        )
    # For mypy: the any() guard above excludes None from all three.
    assert quiet_from is not None and quiet_to is not None
    assert days is not None

    if quiet_from == quiet_to:
        raise ValidationError(
            "quiet_from must differ from quiet_to (equal values are "
            "ambiguous between an empty and a 24h window)"
        )
    if not days:
        raise ValidationError("Quiet-hours days must be non-empty")
    normalized = sorted(set(days))
    if normalized[0] < 1 or normalized[-1] > 7:
        raise ValidationError(
            f"Quiet-hours days must be ISO weekdays 1..7, got {days!r}"
        )

    recipient.quiet_from = quiet_from
    recipient.quiet_to = quiet_to
    recipient.quiet_days = normalized
    await session.flush()
    logger.info(
        "quiet_hours_set",
        recipient_id=str(recipient_id),
        quiet_from=quiet_from.isoformat(),
        quiet_to=quiet_to.isoformat(),
        days=normalized,
    )


async def get_preferences(
    session: AsyncSession,
    recipient_id: UUID,
) -> RecipientPreferences:
    """Read one recipient's full preference snapshot."""
    recipient = await _get_recipient(session, recipient_id)
    muted = await session.scalars(
        select(CategoryMute.category).where(
            CategoryMute.recipient_id == recipient_id
        )
    )
    return RecipientPreferences(
        recipient_id=recipient_id,
        muted_categories=frozenset(muted),
        timezone=recipient.timezone,
        quiet_from=recipient.quiet_from,
        quiet_to=recipient.quiet_to,
        quiet_days=(
            tuple(recipient.quiet_days)
            if recipient.quiet_days is not None
            else None
        ),
    )


async def muted_recipient_ids(
    session: AsyncSession,
    category: str,
    recipient_ids: list[UUID],
) -> set[UUID]:
    """Which of the given recipients muted the given category.

    The resolver-side gating probe: called with a concrete resolved
    audience, returns the subset to drop. Point lookups over the
    (recipient_id, category) PK.
    """
    if not recipient_ids:
        return set()
    rows = await session.scalars(
        select(CategoryMute.recipient_id).where(
            CategoryMute.category == category,
            CategoryMute.recipient_id.in_(recipient_ids),
        )
    )
    return set(rows)
