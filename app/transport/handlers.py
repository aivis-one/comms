# =============================================================================
# COMMS Service -- Event Handlers (Phase 3c)
# =============================================================================
#
# The bridge from parsed events to the EXISTING service layer -- no
# business logic of its own:
#
#   NotificationRequest -> engine.create_notification (+ dedup by the
#                          idempotency_key unique index, item 2/3)
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
#   ValidationError -- terminal (unregistered type, invalid channel):
#                      the event will never succeed -> DLQ + ACK.
#   NotFoundError   -- retryable: group_changed arrived before its
#                      user_upserted (momentary sync lag) -> bounded
#                      backoff, then DLQ.
#   HandleResult.DUPLICATE -- not an error: an at-least-once replay
#                      collapsed by the unique index -> ACK, no DLQ.
#
# Each event is handled inside ITS OWN session/transaction (the
# consumer opens it): a failed event rolls back completely and never
# poisons its neighbors in the batch.
# =============================================================================

import enum

import structlog
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience import sync
from app.engine.reminders import cancel_reminders
from app.engine.service import create_notification
from app.messaging.membership import set_membership
from app.transport.events import (
    GroupChanged,
    NotificationRequest,
    ParsedEvent,
    ReminderCancel,
    SectionMembershipChanged,
    UserUpserted,
)

logger = structlog.get_logger()


class HandleResult(enum.StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"


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
    """Materialize a notification request; collapse replays.

    The DATABASE is the dedup arbiter: the partial unique index on
    notifications.idempotency_key (migration 0005) turns a replayed
    insert into an IntegrityError on flush -- caught here, reported as
    DUPLICATE. No pre-flight SELECT: a check-then-insert would race
    with itself under redelivery, the constraint cannot.
    """
    try:
        # SAVEPOINT so the IntegrityError rolls back only this insert,
        # keeping the outer session usable for the caller's commit.
        async with session.begin_nested():
            notification = await create_notification(
                session,
                type=event.type,
                title=event.title,
                body=event.body,
                target_type=event.target_type,
                target_value=event.target_value,
                channels=event.channels,
                action_data=event.action_data,
                priority=event.priority,
                scheduled_at=event.scheduled_at,
                expiry_at=event.expiry_at,
                idempotency_key=event.idempotency_key,
            )
    except IntegrityError as exc:
        # Only OUR unique index means "duplicate" -- any other
        # integrity violation is a real bug and must not be silently
        # swallowed as a replay.
        if "uq_notifications_idempotency_key" not in str(exc.orig):
            raise
        logger.info(
            "notification_request_duplicate",
            idempotency_key=event.idempotency_key,
            type=event.type,
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
