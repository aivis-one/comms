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
#   What stays here is CANCELLATION (reached by the reminder_cancel
#   event): jobs are found by their ENVELOPE correlation -- equality on
#   an opaque string the product sent, never a key of the letter
#   (F1.3; before, the match read action_data[correlation_key], which
#   is comms reading the letter to decide) -- and take the outcome
#   CANCELLED, not EXPIRED.
#
#   The series scheduler that lived here (schedule_reminders) was
#   removed in F1.2: nothing in the service called it, and every intake
#   now needs an idempotency key per request, which a series helper
#   would have had to invent.
#
# SESSION RULES (P-01): no commits here. Caller manages the transaction.
# =============================================================================

import structlog
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.constants import NotificationStatus
from app.engine.models import Notification
from app.engine.service import close_notifications

logger = structlog.get_logger()


async def cancel_reminders(
    session: AsyncSession,
    *,
    types: set[str],
    correlation: str,
    target_type: str | None = None,
    target_value: str | None = None,
) -> int:
    """Cancel the active jobs of these types sent with this correlation.

    Matches ACTIVE jobs (pending, or resolved and still waiting --
    before F1.3 only pending ones, so a reminder already resolved and
    held by the recipient's schedule could not be cancelled) where:
      - type is in the given set,
      - correlation EQUALS the given one,
      - optionally, the target matches.
    Their waiting deliveries are cancelled and each job is folded
    (app/engine/service.py THE FOLD). Finished jobs are untouched.

    Args:
        session: Database session (caller manages commit).
        types: Type keys to cancel.
        correlation: The envelope correlation to match.
        target_type: Optional target_type filter.
        target_value: Optional target_value filter.

    Returns:
        Count of cancelled jobs.
    """
    conditions = [
        Notification.type.in_(types),
        Notification.correlation == correlation,
    ]
    if target_type is not None:
        conditions.append(Notification.target_type == target_type)
    if target_value is not None:
        conditions.append(Notification.target_value == target_value)

    count = await close_notifications(
        session, and_(*conditions), NotificationStatus.CANCELLED,
    )
    if count > 0:
        logger.info(
            "reminders_cancelled",
            correlation=correlation,
            cancelled_count=count,
        )
    return count
