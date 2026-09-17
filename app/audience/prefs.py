# =============================================================================
# COMMS Service -- Recipient Preferences (Phase 2)
# =============================================================================
#
# Read/write API for per-recipient notification preferences:
#   - category mutes (CategoryMute rows; presence = muted),
#   - the delivery schedule (allowed periods on the Recipient row).
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
#   - service.deliver_notification calls schedule.recipient_deferred_until
#     to defer a send that falls outside the allowed periods.
# =============================================================================

from dataclasses import dataclass
from uuid import UUID

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import CategoryMute, Recipient
from app.audience.schedule import MINUTES_PER_DAY
from app.core.exceptions import NotFoundError, ValidationError
from app.profile.registry import registry

logger = structlog.get_logger()


@dataclass(frozen=True)
class RecipientPreferences:
    """Snapshot of one recipient's preferences.

    `timezone` is read-only here: sync-owned (see module docstring),
    included for display alongside the schedule it governs -- the
    periods are local wall-clock, so they mean nothing without it.
    """

    recipient_id: UUID
    muted_categories: frozenset[str]
    timezone: str | None
    allowed_windows: tuple[dict[str, int], ...] | None


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


async def set_schedule(
    session: AsyncSession,
    recipient_id: UUID,
    *,
    windows: list[dict[str, int]] | None,
) -> None:
    """Replace the recipient's delivery schedule, or clear it.

    THE PERIODS WHEN DELIVERY IS ALLOWED, each owned by the weekday it
    falls in, none crossing midnight. See app/audience/schedule.py for
    why the polarity and the day ownership are what they are.

    None clears the schedule: no restriction, deliver at any time.

    EVERY REFUSAL BELOW EXISTS SO THAT ONE SCHEDULE HAS ONE SPELLING.
    Overlapping and touching periods are the same stretch written
    twice; accepting either would let two different stored values mean
    one thing, which is the class of defect that produced this
    release. The normalization is a REFUSAL, not a silent merge: a
    merge would change what the person entered, and the caller would
    never learn its input was ambiguous.
    """
    recipient = await _get_recipient(session, recipient_id)

    if windows is None:
        recipient.allowed_windows = None
        await session.flush()
        logger.info("schedule_cleared", recipient_id=str(recipient_id))
        return

    if not windows:
        # "Never" is not a schedule: the deliveries would defer until
        # they expire, which reads as a silent black hole. Muting a
        # category is how "do not send me this" is said.
        raise ValidationError(
            "Schedule must contain at least one period; send null to "
            "clear it instead"
        )

    normalized = sorted(
        (
            {
                "day": _validated_day(window),
                "from": _validated_minute(window, "from", MINUTES_PER_DAY - 1),
                "to": _validated_minute(window, "to", MINUTES_PER_DAY),
            }
            for window in windows
        ),
        key=lambda window: (window["day"], window["from"]),
    )

    for window in normalized:
        if window["from"] >= window["to"]:
            raise ValidationError(
                f"A period must end after it starts, got "
                f"{window['from']}..{window['to']} on day {window['day']}"
            )

    previous: dict[str, int] | None = None
    for window in normalized:
        if (
            previous is not None
            and previous["day"] == window["day"]
            and window["from"] <= previous["to"]
        ):
            raise ValidationError(
                f"Periods on day {window['day']} overlap or touch "
                f"({previous['from']}..{previous['to']} and "
                f"{window['from']}..{window['to']}); one stretch is "
                f"one period"
            )
        previous = window

    recipient.allowed_windows = normalized
    await session.flush()
    logger.info(
        "schedule_set",
        recipient_id=str(recipient_id),
        periods=len(normalized),
    )


def _validated_day(window: dict[str, int]) -> int:
    day = window.get("day")
    if not isinstance(day, int) or isinstance(day, bool):
        raise ValidationError(f"Period day must be an integer, got {day!r}")
    if not 1 <= day <= 7:
        raise ValidationError(
            f"Period day must be an ISO weekday 1..7, got {day}"
        )
    return day


def _validated_minute(
    window: dict[str, int], field: str, maximum: int
) -> int:
    value = window.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError(
            f"Period {field!r} must be minutes from midnight, got {value!r}"
        )
    if not 0 <= value <= maximum:
        raise ValidationError(
            f"Period {field!r} must be 0..{maximum} minutes, got {value}"
        )
    return value


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
        allowed_windows=(
            tuple(recipient.allowed_windows)
            if recipient.allowed_windows is not None
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

    KNOWN CEILING (acknowledged by design, do not "fix" ad hoc):
    the expanding IN materializes one bind parameter per recipient,
    and the Postgres wire protocol caps a statement at 32767
    parameters (int16) -- a single-notification audience past that
    crashes the query. Dispositioned to the "broadcast hardening"
    backlog (trigger: audience of ONE notification approaching ~10k,
    OR a real broadcast/digest planned in the product). The agreed
    fix shape when reopened: set-based resolve with an anti-join
    (this probe disappears from the hot path) or an unnest(:ids)
    join (array travels as ONE parameter, planner nested-loops into
    the existing PK). Do NOT invert the probe into a bare
    `WHERE category = :cat` scan: it cannot enter the
    (recipient_id, category) PK -- deliberately ordered for THIS
    lookup, see migration 0003 -- and degrades with mute-table
    growth.
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
