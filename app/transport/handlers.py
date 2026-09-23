# =============================================================================
# COMMS Service -- Event Handlers (Phase 3c)
# =============================================================================
#
# The bridge from parsed events to the EXISTING service layer -- no
# business logic of its own:
#
#   NotificationRequest -> engine.accept_notification (F1.2): accepted,
#                          or a duplicate (same key, same bytes), or a
#                          conflict (same key, other bytes) recorded
#                          under the key
#   RejectedNotificationRequest / a request create_notification cannot
#                          accept -> recorded under its key as
#                          rejected_at_intake
#   UserUpserted        -> audience.sync.user_upserted   (item 4)
#   GroupChanged        -> audience.sync.group_changed   (item 4)
#   ReminderCancel      -> engine.reminders.cancel_reminders
#                          (Phase 6/T1 additive event; naturally
#                          idempotent -- a replay or a no-match set is
#                          a zero-row update, never an error)
#
# The Phase 2 sync functions are called AS-IS (the handoff's explicit
# rule: wire them, do not rewrite them).
#
# Error classification (consumed by consumer.py):
#   HandleResult.REJECTED / CONFLICT -- a notification request that was
#                      not accepted and IS recorded under its key: the
#                      product's side of the line (spec §5.4). ACK,
#                      no DLQ -- the record is the one copy of the fact.
#   ValidationError -- terminal for the other events: the event will
#                      never succeed -> DLQ + ACK.
#   NotFoundError   -- retryable: group_changed arrived before its
#                      user_upserted (momentary sync lag) -> bounded
#                      backoff, then DLQ.
#   HandleResult.DUPLICATE -- not an error: an at-least-once replay
#                      of the same bytes -> ACK, no DLQ.
#
# Each event is handled inside ITS OWN session/transaction (the
# consumer opens it): a failed event rolls back completely and never
# poisons its neighbors in the batch.
# =============================================================================

import enum

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience import sync
from app.core.exceptions import ValidationError
from app.engine.constants import IntakeOutcomeClass
from app.engine.reminders import cancel_reminders
from app.engine.service import (
    Intake,
    accept_notification,
    record_intake_outcome,
)
from app.messaging.membership import set_membership
from app.transport.events import (
    GroupChanged,
    NotificationRequest,
    ParsedEvent,
    RejectedNotificationRequest,
    ReminderCancel,
    SectionMembershipChanged,
    UserUpserted,
)

logger = structlog.get_logger()


class HandleResult(enum.StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    # Not accepted, recorded under the key (intake_outcomes).
    REJECTED = "rejected"
    CONFLICT = "conflict"


async def handle_event(
    session: AsyncSession, event: ParsedEvent
) -> HandleResult:
    """Apply one parsed event to the database.

    The caller owns the session lifecycle (commit on return, rollback
    on raise). Raises ValidationError (terminal) or NotFoundError
    (retryable) -- see the module header for the classification.
    """
    if isinstance(event, NotificationRequest):
        return await _handle_notification_request(session, event)
    if isinstance(event, RejectedNotificationRequest):
        return await _reject(
            session, event.idempotency_key, event.fingerprint, event.reason,
        )
    if isinstance(event, UserUpserted):
        await sync.user_upserted(
            session,
            recipient_id=event.recipient_id,
            telegram_id=event.telegram_id,
            email=event.email,
            locale=event.locale,
            timezone=event.timezone,
            active=event.active,
        )
        return HandleResult.PROCESSED
    if isinstance(event, GroupChanged):
        # May raise NotFoundError when the recipient has not been
        # synced yet (user_upserted lagging) -- classified RETRYABLE
        # by the consumer.
        await sync.group_changed(
            session,
            group_key=event.group_key,
            recipient_id=event.recipient_id,
            member=event.member,
        )
        return HandleResult.PROCESSED
    if isinstance(event, SectionMembershipChanged):
        # The section is created if absent (a roster may be declared
        # before anyone writes in); an operator comms has not been told
        # about yet fails the recipient FK, which the consumer
        # classifies RETRYABLE -- the same lag group_changed has.
        await set_membership(
            session,
            section_key=event.section_key,
            section_label=event.section_label,
            operator_id=event.operator_id,
            member=event.member,
        )
        return HandleResult.PROCESSED
    # ReminderCancel (Phase 6/T1). cancel_reminders expires PENDING
    # matches only -- replays and no-match sets are zero-row updates,
    # so at-least-once delivery needs no dedup here.
    assert isinstance(event, ReminderCancel)
    cancelled = await cancel_reminders(
        session,
        types=set(event.types),
        correlation_key=event.correlation_key,
        correlation_value=event.correlation_value,
        target_type=event.target_type,
        target_value=event.target_value,
    )
    logger.info(
        "reminder_cancel_handled",
        correlation=(
            f"{event.correlation_key}={event.correlation_value}"
        ),
        expired_count=cancelled,
    )
    return HandleResult.PROCESSED


async def _handle_notification_request(
    session: AsyncSession, event: NotificationRequest
) -> HandleResult:
    """Accept a notification request, or record why it was not.

    The DATABASE is the arbiter of "same key" (accept_notification);
    this function only maps its answer onto the stream's vocabulary and
    records what the product must be able to find under its key.
    """
    try:
        acceptance = await accept_notification(
            session,
            idempotency_key=event.idempotency_key,
            fingerprint=event.fingerprint,
            type=event.type,
            title=event.title,
            body=event.body,
            target_type=event.target_type,
            target_value=event.target_value,
            action_data=event.action_data,
            scheduled_at=event.scheduled_at,
            expiry_at=event.expiry_at,
            correlation=event.correlation,
        )
    except ValidationError as exc:
        # The request cannot be accepted (undeclared type, an expiry
        # already passed, ...): the product's responsibility, recorded.
        return await _reject(
            session, event.idempotency_key, event.fingerprint, str(exc),
        )

    notification = acceptance.notification
    if acceptance.outcome is Intake.CONFLICT:
        await record_intake_outcome(
            session,
            idempotency_key=event.idempotency_key,
            fingerprint=event.fingerprint,
            outcome=IntakeOutcomeClass.CONFLICT,
            reason=(
                f"idempotency key {event.idempotency_key!r} is taken by "
                f"notification {notification.id} with different content; "
                f"a new request needs a new key"
            ),
            notification_id=notification.id,
        )
        logger.warning(
            "notification_request_conflict",
            idempotency_key=event.idempotency_key,
            notification_id=str(notification.id),
        )
        return HandleResult.CONFLICT
    if acceptance.outcome is Intake.DUPLICATE:
        logger.info(
            "notification_request_duplicate",
            idempotency_key=event.idempotency_key,
            notification_id=str(notification.id),
        )
        return HandleResult.DUPLICATE

    logger.info(
        "notification_request_materialized",
        notification_id=str(notification.id),
        idempotency_key=event.idempotency_key,
        type=event.type,
        target=f"{event.target_type}:{event.target_value}",
    )
    return HandleResult.PROCESSED


async def _reject(
    session: AsyncSession, idempotency_key: str, fingerprint: str, reason: str,
) -> HandleResult:
    await record_intake_outcome(
        session,
        idempotency_key=idempotency_key,
        fingerprint=fingerprint,
        outcome=IntakeOutcomeClass.REJECTED_AT_INTAKE,
        reason=reason,
    )
    logger.warning(
        "notification_request_rejected",
        idempotency_key=idempotency_key,
        reason=reason,
    )
    return HandleResult.REJECTED
