# =============================================================================
# COMMS Service -- Reminder Scheduling
# =============================================================================
#
# The velo reminders donor (Phase 7.4), de-domainized.
#
# THE MECHANISM (kept 1:1 -- this is why no broker is needed):
#   A reminder is just a Notification with a FUTURE scheduled_at.
#   The regular worker picks it up when the time comes (the processor
#   only selects scheduled_at <= now()). Specifically:
#     - N notifications at fixed lead offsets before an anchor time
#       (velo: 24h / 1h / 10min before practice.scheduled_at),
#     - a minimum-lead cutoff so we never schedule something already
#       (almost) due (velo: 5 minutes),
#     - expiry_at defaulting to the anchor -- no point reminding about
#       something that already started,
#     - cancellation by expiring PENDING reminders matched through a
#       correlation key stored in action_data (velo:
#       action_data["practice_id"].astext == str(practice_id)).
#
# WHAT WAS LEFT BEHIND (product-side, returns in Phase 6 as domain
# orchestration on top of these primitives):
#   Booking/Practice/User loading, master display names, participants
#   counts, per-booking loops, reschedule_reminders_for_practice.
#   Rescheduling in the core is simply cancel_reminders() +
#   schedule_reminders() by the caller.
#
# SESSION RULES (P-01): no commits here. Caller manages the transaction.
# =============================================================================

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.constants import NotificationStatus
from app.engine.models import Notification
from app.engine.service import create_notification

logger = structlog.get_logger()

# Buffer: don't schedule a reminder if it's less than 5 minutes away
# (donor default).
DEFAULT_MIN_LEAD = timedelta(minutes=5)

# Priority for reminders (higher than the default 5, lower than urgent 1).
DEFAULT_REMINDER_PRIORITY = 2


@dataclass(frozen=True)
class ReminderSpec:
    """One reminder in a series: which type key, how long before anchor.

    The type key must be registered by the product profile (e.g. a
    profile registers rem_24h / rem_1h / rem_10m with leads of
    24h / 1h / 10min).
    """

    type: str
    lead: timedelta


async def schedule_reminders(
    session: AsyncSession,
    *,
    specs: list[ReminderSpec],
    anchor_at: datetime,
    target_type: str,
    target_value: str,
    title: str = "Reminder",
    body: str = "",
    action_data: dict[str, Any] | None = None,
    correlation_key: str | None = None,
    correlation_value: str | None = None,
    channels: list[str] | None = None,
    priority: int = DEFAULT_REMINDER_PRIORITY,
    min_lead: timedelta = DEFAULT_MIN_LEAD,
    expiry_at: datetime | None = None,
) -> list[Notification]:
    """Schedule a series of reminder notifications before an anchor time.

    For each spec, creates a Notification with
    scheduled_at = anchor_at - spec.lead, skipping any send time that
    is less than min_lead away. expiry_at defaults to the anchor
    (no point sending after the event starts).

    Args:
        session: Database session (caller manages commit).
        specs: Reminder series (type key + lead before anchor).
        anchor_at: The event time the reminders lead up to.
        target_type: TargetType value (user, group, all).
        target_value: Bare target specifier.
        title: Stored fallback title (templates usually override).
        body: Stored fallback body.
        action_data: Deep-link intent + template variables.
        correlation_key: action_data key used to find these reminders
            later (e.g. "<entity>_id"). Stored automatically when
            provided together with correlation_value.
        correlation_value: Value for the correlation key.
        channels: Delivery channels (defaults handled by
            create_notification).
        priority: Queue priority (defaults to DEFAULT_REMINDER_PRIORITY).
        min_lead: Skip reminders due sooner than this from now.
        expiry_at: TTL override; defaults to anchor_at.

    Returns:
        List of created Notification objects (0..len(specs)).
    """
    now = datetime.now(UTC)
    cutoff = now + min_lead
    created: list[Notification] = []

    data: dict[str, Any] | None = action_data
    if correlation_key is not None and correlation_value is not None:
        data = {**(action_data or {}), correlation_key: correlation_value}

    if expiry_at is None:
        expiry_at = anchor_at

    for spec in specs:
        send_at = anchor_at - spec.lead

        if send_at < cutoff:
            continue

        notification = await create_notification(
            session,
            type=spec.type,
            title=title,
            body=body,
            target_type=target_type,
            target_value=target_value,
            channels=channels,
            action_data=data,
            priority=priority,
            scheduled_at=send_at,
            expiry_at=expiry_at,
        )
        created.append(notification)

    if created:
        logger.info(
            "reminders_scheduled",
            anchor_at=anchor_at.isoformat(),
            target=f"{target_type}:{target_value}",
            count=len(created),
            types=[n.type for n in created],
        )

    return created


async def cancel_reminders(
    session: AsyncSession,
    *,
    types: set[str],
    correlation_key: str,
    correlation_value: str,
    target_type: str | None = None,
    target_value: str | None = None,
) -> int:
    """Cancel pending reminders matched by correlation.

    Marks PENDING notifications as EXPIRED where:
      - type is in the given set,
      - action_data[correlation_key] matches correlation_value,
      - optionally, the target matches (velo's per-booking cancel
        passed the user target; its per-practice cancel did not).

    Args:
        session: Database session (caller manages commit).
        types: Reminder type keys to cancel.
        correlation_key: action_data key (e.g. "<entity>_id").
        correlation_value: Value to match.
        target_type: Optional target_type filter.
        target_value: Optional target_value filter.

    Returns:
        Count of expired notifications.
    """
    conditions = [
        Notification.type.in_(types),
        Notification.status == NotificationStatus.PENDING,
        Notification.action_data[correlation_key].astext
        == correlation_value,
    ]
    if target_type is not None:
        conditions.append(Notification.target_type == target_type)
    if target_value is not None:
        conditions.append(Notification.target_value == target_value)

    stmt = (
        update(Notification)
        .where(*conditions)
        .values(status=NotificationStatus.EXPIRED)
    )
    result = await session.execute(stmt)
    count: int = result.rowcount  # type: ignore[attr-defined]

    if count > 0:
        logger.info(
            "reminders_cancelled",
            correlation=f"{correlation_key}={correlation_value}",
            expired_count=count,
        )

    return count
