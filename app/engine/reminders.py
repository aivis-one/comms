# =============================================================================
# COMMS Service -- Reminder Cancellation
# =============================================================================
#
# The velo reminders donor (Phase 7.4), de-domainized.
#
# THE MECHANISM (why no broker is needed):
#   A reminder is just a Notification with a FUTURE scheduled_at -- the
#   envelope's "not before" (F1.2). The regular worker picks it up when
#   the time comes (the processor only selects scheduled_at <= now()).
#   The product schedules each reminder as its own notification_request
#   under its own key.
#
#   What stays here is CANCELLATION: expiring PENDING reminders matched
#   through a correlation key stored in action_data (velo:
#   action_data["practice_id"].astext == str(practice_id)), reached by
#   the reminder_cancel event.
#
#   The series scheduler that lived here (schedule_reminders) was
#   removed in F1.2: nothing in the service called it, and every intake
#   now needs an idempotency key per request, which a series helper
#   would have had to invent.
#
# SESSION RULES (P-01): no commits here. Caller manages the transaction.
# =============================================================================

import structlog
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.constants import NotificationStatus
from app.engine.models import Notification

logger = structlog.get_logger()


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
