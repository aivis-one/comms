# =============================================================================
# COMMS Service -- Notification Service
# =============================================================================
#
# Ported from the cbshome notification service (canonical base),
# de-domainized:
#   - type validated against the profile registry (not a hardcoded enum)
#   - deliveries reference recipients (sync projection), not Users
#   - formatter credentials/locale come from Recipient columns
#
# FUNCTIONS (pipeline):
#   create_notification()   -- create a Notification record (validated)
#   resolve_notification()  -- expand targets into NotificationDelivery rows
#   deliver_notification()  -- call formatters for pending deliveries
#   rollup_notification()   -- update Notification.status from deliveries
#
# FUNCTIONS (in-app inbox, cbshome Sprint 8.3 -- service level only;
# the HTTP surface for the product is Phase 3 transport work):
#   list_recipient_deliveries() -- paginated sent deliveries + parent data
#   get_unread_count()          -- badge counter
#   mark_delivery_read()        -- mark single delivery as read (idempotent)
#   mark_all_read()             -- mark all sent deliveries as read
#
# RELIABILITY (cbshome Sprint 8.2):
#   - asyncio.wait_for timeout on formatter.deliver()
#   - concurrent delivery via gather + semaphore
#   - PermanentDeliveryError -> immediate FAILED, no attempts increment
#   - error messages sanitized to prevent credential leaks
#
# COMMIT RULE (P-01):
#   Service never commits. Caller manages the transaction.
# =============================================================================

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import Recipient
from app.audience.prefs import muted_recipient_ids
from app.audience.quiet_hours import recipient_quiet_until
from app.core.config import settings
from app.core.exceptions import NotFoundError, ValidationError
from app.engine.constants import (
    DeliveryChannel,
    DeliveryStatus,
    NotificationStatus,
    TargetType,
)
from app.engine.formatters import (
    ChannelFormatter,
    PermanentDeliveryError,
    get_formatter,
    sanitize_error,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.registry import registry
from app.engine.resolver import resolve_targets

logger = structlog.get_logger()

# Valid infrastructure enum values for input validation.
_VALID_TARGET_TYPES = frozenset(e.value for e in TargetType)
_VALID_CHANNELS = frozenset(e.value for e in DeliveryChannel)

# Timeout for a single formatter.deliver() call.
_DELIVER_TIMEOUT_SECONDS = 30

# Max concurrent formatter.deliver() calls per notification.
_MAX_CONCURRENT_DELIVERIES = 20


async def create_notification(
    session: AsyncSession,
    *,
    type: str,
    title: str,
    body: str,
    target_type: str,
    target_value: str,
    channels: list[str] | None = None,
    action_data: dict[str, Any] | None = None,
    priority: int = 5,
    scheduled_at: datetime | None = None,
    expiry_at: datetime | None = None,
) -> Notification:
    """Create a new Notification record.

    Args:
        session: Active DB session (caller commits).
        type: A notification type key registered by the product profile.
        title: Notification title.
        body: Notification body text.
        target_type: TargetType value (user, group, all).
        target_value: Bare target specifier ("<uuid>", "<group_key>", "*").
        channels: Delivery channels. Defaults to ["in_app"].
        action_data: Optional JSONB action payload (deep-link intent +
            template variables).
        priority: 1=highest, 5=default.
        scheduled_at: When to process. Defaults to now.
        expiry_at: Optional TTL deadline.

    Returns:
        The created Notification (flushed, not committed).

    Raises:
        ValidationError: On unregistered type, invalid target_type or
            channel values.
    """
    # -- Validate against the profile registry (de-domainization) --
    if not registry.is_registered(type):
        raise ValidationError(
            f"Unregistered notification type: {type}. "
            f"Registered: {', '.join(sorted(registry.registered_types()))}"
        )

    if target_type not in _VALID_TARGET_TYPES:
        raise ValidationError(
            f"Invalid target_type: {target_type}. "
            f"Valid: {', '.join(sorted(_VALID_TARGET_TYPES))}"
        )

    if channels is None:
        channels = [DeliveryChannel.IN_APP]

    invalid_channels = set(channels) - _VALID_CHANNELS
    if invalid_channels:
        raise ValidationError(
            f"Invalid channels: {invalid_channels}. "
            f"Valid: {', '.join(sorted(_VALID_CHANNELS))}"
        )

    if scheduled_at is None:
        scheduled_at = datetime.now(UTC)

    notification = Notification(
        type=type,
        title=title,
        body=body,
        target_type=target_type,
        target_value=target_value,
        action_data=action_data,
        priority=priority,
        scheduled_at=scheduled_at,
        expiry_at=expiry_at,
        status=NotificationStatus.PENDING,
    )
    session.add(notification)
    await session.flush()

    # Store channels in action_data for the resolve stage (cbshome
    # mechanism: reassigning the whole dict keeps SQLAlchemy tracking).
    if notification.action_data is None:
        notification.action_data = {"_channels": channels}
    else:
        notification.action_data = {
            **notification.action_data,
            "_channels": channels,
        }

    logger.info(
        "notification_created",
        notification_id=str(notification.id),
        type=type,
        target=f"{target_type}:{target_value}",
        channels=channels,
        scheduled_at=scheduled_at.isoformat(),
    )

    return notification


async def resolve_notification(
    session: AsyncSession,
    notification: Notification,
) -> list[NotificationDelivery]:
    """Expand notification targets into NotificationDelivery rows.

    Idempotent: if deliveries already exist (PROCESSING retry),
    skips resolve and returns existing deliveries.

    MUTE GATING (Phase 2): recipients who muted the notification
    type's category are dropped HERE, before deliveries exist -- no
    dead rows, honest delivery metrics, and "everyone muted" is
    decided in one place. Types without a category bypass gating.

    Transitions notification status: pending -> processing.
    Empty audience (nobody resolved, or everyone muted) -> SKIPPED:
    the pipeline worked, there was just nobody to deliver to. FAILED
    stays reserved for real faults (Phase 1 marked empty resolve as
    FAILED -- cbshome base behavior; changed in Phase 2).

    Args:
        session: Active DB session (caller commits).
        notification: The notification to resolve.

    Returns:
        List of NotificationDelivery objects (created or existing).
    """
    # -- Idempotency: check if already resolved --
    existing_stmt = select(func.count()).where(
        NotificationDelivery.notification_id == notification.id,
    )
    existing_result = await session.execute(existing_stmt)
    existing_count = existing_result.scalar_one()

    if existing_count > 0:
        # Already resolved -- return existing deliveries.
        stmt = select(NotificationDelivery).where(
            NotificationDelivery.notification_id == notification.id,
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    # -- Resolve target recipients over the sync projection --
    recipient_ids = await resolve_targets(
        session,
        notification.target_type,
        notification.target_value,
    )

    if not recipient_ids:
        notification.status = NotificationStatus.SKIPPED
        logger.warning(
            "notification_no_targets",
            notification_id=str(notification.id),
        )
        return []

    # -- Mute gate: drop recipients who muted this type's category --
    # Evaluated at resolve time; for reminders that is the moment the
    # notification comes due, so the mute state is current as of send.
    # A mute set AFTER deliveries exist is caught by the second line
    # at deliver time (late-mute re-check -> DeliveryStatus.SKIPPED).
    category = registry.category_of(notification.type)
    if category is not None:
        muted = await muted_recipient_ids(session, category, recipient_ids)
        if muted:
            recipient_ids = [r for r in recipient_ids if r not in muted]
            logger.info(
                "recipients_muted_category",
                notification_id=str(notification.id),
                category=category,
                muted=len(muted),
                remaining=len(recipient_ids),
            )
        if not recipient_ids:
            notification.status = NotificationStatus.SKIPPED
            logger.info(
                "notification_all_muted",
                notification_id=str(notification.id),
                category=category,
            )
            return []

    # Get channels from action_data.
    channels = (notification.action_data or {}).get(
        "_channels", [DeliveryChannel.IN_APP]
    )

    # Create delivery records.
    deliveries: list[NotificationDelivery] = []
    for recipient_id in recipient_ids:
        for channel in channels:
            delivery = NotificationDelivery(
                notification_id=notification.id,
                recipient_id=recipient_id,
                channel=channel,
                status=DeliveryStatus.PENDING,
            )
            session.add(delivery)
            deliveries.append(delivery)

    notification.status = NotificationStatus.PROCESSING
    await session.flush()

    logger.info(
        "notification_resolved",
        notification_id=str(notification.id),
        recipients=len(recipient_ids),
        channels=len(channels),
        deliveries=len(deliveries),
    )

    return deliveries


async def deliver_notification(
    session: AsyncSession,
    notification: Notification,
) -> None:
    """Deliver pending deliveries for a notification via formatters.

    - Batch-loads Recipient objects for credentials and locale.
    - LATE-MUTE RE-CHECK (Phase 2.1): the resolve-time mute gate is
      the first line, but a delivery can sit gated for HOURS (quiet
      hours stretched the window far past the old 30-60s backoff). So
      right before sending, recipients who muted the notification's
      category since resolve are closed out terminally with
      DeliveryStatus.SKIPPED -- not FAILED (nothing broke), mirroring
      the notification-level SKIPPED. One batched lookup per pass;
      checked BEFORE the quiet gate (no point deferring a muted
      delivery). Attempts and error_message stay untouched (a skip is
      not an attempt; prior transient history is kept).
    - QUIET HOURS (Phase 2): a delivery whose recipient is inside
      their quiet window is DEFERRED, not sent -- next_retry_at is set
      to the window's end (recipient's timezone) and the existing
      retry gate keeps it invisible to the poll until then. Attempts
      and error_message stay untouched: deferral is not a failure.
      Checked per attempt, so backoff retries landing in a quiet
      window are deferred too.
    - Concurrent delivery via asyncio.gather + Semaphore.
    - asyncio.wait_for with timeout per formatter call.
    - PermanentDeliveryError -> immediate FAILED, no attempts increment.
    - Transient failure gates the next attempt via next_retry_at
      (exponential backoff, review 1.1); gated deliveries are skipped.
    - Error messages sanitized to prevent credential leaks.

    Args:
        session: Active DB session (caller commits).
        notification: The notification whose deliveries to process.
    """
    now = datetime.now(UTC)
    stmt = select(NotificationDelivery).where(
        NotificationDelivery.notification_id == notification.id,
        NotificationDelivery.status == DeliveryStatus.PENDING,
        or_(
            NotificationDelivery.next_retry_at.is_(None),
            NotificationDelivery.next_retry_at <= now,
        ),
    )
    result = await session.execute(stmt)
    deliveries = list(result.scalars().all())

    if not deliveries:
        return

    # -- Batch load recipients for all pending deliveries --
    recipient_ids: set[UUID] = {d.recipient_id for d in deliveries}
    recipient_stmt = select(Recipient).where(Recipient.id.in_(recipient_ids))
    recipient_result = await session.execute(recipient_stmt)
    recipients_by_id: dict[UUID, Recipient] = {
        r.id: r for r in recipient_result.scalars().all()
    }

    # -- Late-mute re-check: one batched probe per pass --
    category = registry.category_of(notification.type)
    muted: set[UUID] = set()
    if category is not None:
        muted = await muted_recipient_ids(
            session, category, list(recipient_ids),
        )

    # -- Concurrent delivery via gather + semaphore --
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT_DELIVERIES)
    tasks = []

    for delivery in deliveries:
        recipient = recipients_by_id.get(delivery.recipient_id)
        if recipient is None:
            delivery.status = DeliveryStatus.FAILED
            delivery.error_message = "Recipient not found"
            logger.warning(
                "delivery_recipient_not_found",
                delivery_id=str(delivery.id),
                recipient_id=str(delivery.recipient_id),
            )
            continue

        # -- Late-mute gate: muted while gated -> close out, no send --
        if delivery.recipient_id in muted:
            delivery.status = DeliveryStatus.SKIPPED
            logger.info(
                "delivery_muted_skipped",
                delivery_id=str(delivery.id),
                recipient_id=str(delivery.recipient_id),
                category=category,
            )
            continue

        # -- Quiet-hours gate: defer, never suppress --
        quiet_until = recipient_quiet_until(recipient, now)
        if quiet_until is not None:
            delivery.next_retry_at = quiet_until
            logger.info(
                "delivery_quiet_deferred",
                delivery_id=str(delivery.id),
                recipient_id=str(recipient.id),
                until=quiet_until.isoformat(),
            )
            continue

        formatter = get_formatter(delivery.channel)
        tasks.append(
            _deliver_single(
                semaphore, formatter, notification, delivery, recipient,
            )
        )

    if tasks:
        results = await asyncio.gather(*tasks)

        # Apply results to delivery objects (sequential, session-safe).
        max_attempts = settings.notification_max_delivery_attempts
        for delivery, outcome in results:
            if outcome.permanent:
                delivery.status = DeliveryStatus.FAILED
                delivery.error_message = outcome.error
            elif outcome.success:
                delivery.attempts += 1
                delivery.status = DeliveryStatus.SENT
                delivery.sent_at = datetime.now(UTC)
            else:
                delivery.attempts += 1
                delivery.error_message = outcome.error
                if delivery.attempts >= max_attempts:
                    delivery.status = DeliveryStatus.FAILED
                else:
                    # Exponential backoff gate: base * 2**(attempts-1),
                    # capped. Without it all attempts burned within one
                    # poll interval (review 1.1).
                    backoff_seconds = min(
                        settings.notification_retry_backoff_base_seconds
                        * 2 ** (delivery.attempts - 1),
                        settings.notification_retry_backoff_max_seconds,
                    )
                    delivery.next_retry_at = datetime.now(UTC) + timedelta(
                        seconds=backoff_seconds,
                    )

    await session.flush()


class _DeliveryOutcome:
    """Result of a single delivery attempt."""

    __slots__ = ("error", "permanent", "success")

    def __init__(
        self,
        *,
        success: bool = False,
        permanent: bool = False,
        error: str | None = None,
    ) -> None:
        self.success = success
        self.permanent = permanent
        self.error = error


async def _deliver_single(
    semaphore: asyncio.Semaphore,
    formatter: ChannelFormatter,
    notification: Notification,
    delivery: NotificationDelivery,
    recipient: Recipient,
) -> tuple[NotificationDelivery, _DeliveryOutcome]:
    """Deliver a single notification with concurrency control.

    Runs formatter.deliver() under semaphore with timeout.
    Does NOT touch the SQLAlchemy session -- only external API calls.
    Returns (delivery, outcome) for the caller to apply to the session.
    """
    async with semaphore:
        try:
            success = await asyncio.wait_for(
                formatter.deliver(notification, delivery, recipient),
                timeout=_DELIVER_TIMEOUT_SECONDS,
            )
            return delivery, _DeliveryOutcome(success=success)
        except PermanentDeliveryError as exc:
            logger.warning(
                "delivery_permanent_failure",
                delivery_id=str(delivery.id),
                channel=delivery.channel,
                error=str(exc)[:200],
            )
            return delivery, _DeliveryOutcome(
                permanent=True, error=sanitize_error(exc),
            )
        except TimeoutError:
            logger.warning(
                "delivery_timeout",
                delivery_id=str(delivery.id),
                channel=delivery.channel,
                timeout=_DELIVER_TIMEOUT_SECONDS,
            )
            return delivery, _DeliveryOutcome(
                error=f"Timeout after {_DELIVER_TIMEOUT_SECONDS}s",
            )
        except Exception as exc:
            logger.exception(
                "delivery_error",
                delivery_id=str(delivery.id),
                channel=delivery.channel,
            )
            return delivery, _DeliveryOutcome(
                error=sanitize_error(exc),
            )


async def rollup_notification(
    session: AsyncSession,
    notification: Notification,
) -> None:
    """Update Notification.status based on delivery statuses.

    Only acts on PROCESSING notifications: terminal states set by
    resolve (SKIPPED for empty/all-muted audiences) or the processor
    (EXPIRED) must not be overwritten -- without the guard the
    "no deliveries -> FAILED" branch below would clobber SKIPPED.
    That branch stays as a safety net: a PROCESSING notification
    without any deliveries is an anomaly, not a skip.

    SKIPPED deliveries (late mutes, Phase 2.1) are non-events: they
    are subtracted before the verdict, so they drag the outcome
    neither toward FAILED nor toward SENT. Matrix:
      - only skipped              -> notification SKIPPED (late
        edition of "nobody to deliver to")
      - skipped + sent            -> sent
      - skipped + failed          -> failed
      - skipped + sent + failed   -> partial_sent
      - skipped + pending         -> stays processing (a gated
        delivery is still alive; do not finalize early)

    Rules over the remaining statuses (cbshome base, incl.
    PARTIAL_SENT):
      - All sent         -> sent
      - All failed       -> failed
      - Mix sent+failed  -> partial_sent
      - Any pending      -> stays processing (not all delivered yet)

    Args:
        session: Active DB session (caller commits).
        notification: The notification to roll up.
    """
    if notification.status != NotificationStatus.PROCESSING:
        return

    stmt = select(NotificationDelivery.status).where(
        NotificationDelivery.notification_id == notification.id,
    )
    result = await session.execute(stmt)
    statuses = {row[0] for row in result.all()}

    if not statuses:
        notification.status = NotificationStatus.FAILED
        return

    # Skips are non-events -- judge the outcome by the rest.
    active = statuses - {DeliveryStatus.SKIPPED}
    if not active:
        notification.status = NotificationStatus.SKIPPED
        await session.flush()
        logger.info(
            "notification_rollup",
            notification_id=str(notification.id),
            status=notification.status,
        )
        return

    has_pending = DeliveryStatus.PENDING in active
    has_sent = DeliveryStatus.SENT in active
    has_failed = DeliveryStatus.FAILED in active

    if has_pending:
        # Still processing -- don't change status.
        return

    if has_sent and not has_failed:
        notification.status = NotificationStatus.SENT
    elif has_failed and not has_sent:
        notification.status = NotificationStatus.FAILED
    else:
        notification.status = NotificationStatus.PARTIAL_SENT

    await session.flush()

    logger.info(
        "notification_rollup",
        notification_id=str(notification.id),
        status=notification.status,
    )


# ---------------------------------------------------------------------------
# In-app inbox functions (cbshome Sprint 8.3; HTTP surface is Phase 3)
# ---------------------------------------------------------------------------


async def list_recipient_deliveries(
    session: AsyncSession,
    recipient_id: UUID,
    *,
    page: int = 1,
    per_page: int = 20,
    type_filter: str | None = None,
    channel_filter: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """List sent deliveries for a recipient with parent notification data.

    Only deliveries with status=sent are returned (the recipient sees
    only what was actually delivered). Results enriched with title,
    body, type, action_data, priority from the parent Notification.

    Args:
        session: Active DB session (read-only).
        recipient_id: Recipient (= product user) id.
        page: Page number (1-based).
        per_page: Items per page.
        type_filter: Filter by Notification.type (exact match).
        channel_filter: Filter by NotificationDelivery.channel.

    Returns:
        (items, total) where items are plain dicts.
    """
    conditions = [
        NotificationDelivery.recipient_id == recipient_id,
        NotificationDelivery.status == DeliveryStatus.SENT,
    ]

    if type_filter:
        conditions.append(Notification.type == type_filter)
    if channel_filter:
        conditions.append(NotificationDelivery.channel == channel_filter)

    count_stmt = (
        select(func.count())
        .select_from(NotificationDelivery)
        .join(
            Notification,
            NotificationDelivery.notification_id == Notification.id,
        )
        .where(*conditions)
    )
    total = (await session.execute(count_stmt)).scalar_one()

    stmt = (
        select(NotificationDelivery, Notification)
        .join(
            Notification,
            NotificationDelivery.notification_id == Notification.id,
        )
        .where(*conditions)
        .order_by(NotificationDelivery.sent_at.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
    )
    result = await session.execute(stmt)
    rows = result.all()

    items: list[dict[str, Any]] = []
    for delivery, notification in rows:
        items.append({
            "id": delivery.id,
            "channel": delivery.channel,
            "status": delivery.status,
            "read_at": delivery.read_at,
            "sent_at": delivery.sent_at,
            "created_at": delivery.created_at,
            "type": notification.type,
            "title": notification.title,
            "body": notification.body,
            "action_data": {
                k: v for k, v in (notification.action_data or {}).items()
                if not k.startswith("_")
            } or None,
            "priority": notification.priority,
        })

    return items, total


async def get_unread_count(
    session: AsyncSession,
    recipient_id: UUID,
) -> int:
    """Count unread sent deliveries for the badge counter.

    Unread = status=sent AND read_at IS NULL.
    """
    stmt = (
        select(func.count())
        .select_from(NotificationDelivery)
        .where(
            NotificationDelivery.recipient_id == recipient_id,
            NotificationDelivery.status == DeliveryStatus.SENT,
            NotificationDelivery.read_at.is_(None),
        )
    )
    result = await session.execute(stmt)
    return result.scalar_one()


async def mark_delivery_read(
    session: AsyncSession,
    recipient_id: UUID,
    delivery_id: UUID,
) -> None:
    """Mark a single delivery as read (idempotent).

    Sets read_at to now if not already set. Does nothing if already read.

    Raises:
        NotFoundError: If delivery not found or belongs to another
            recipient.
    """
    stmt = select(NotificationDelivery).where(
        NotificationDelivery.id == delivery_id,
        NotificationDelivery.recipient_id == recipient_id,
    )
    result = await session.execute(stmt)
    delivery = result.scalar_one_or_none()

    if delivery is None:
        raise NotFoundError("Notification not found")

    # Idempotent: skip if already read.
    if delivery.read_at is not None:
        return

    delivery.read_at = datetime.now(UTC)
    await session.flush()


async def mark_all_read(
    session: AsyncSession,
    recipient_id: UUID,
) -> int:
    """Mark all sent deliveries as read for a recipient.

    Only updates deliveries with status=sent AND read_at IS NULL.

    Returns:
        Number of deliveries marked as read.
    """
    stmt = (
        update(NotificationDelivery)
        .where(
            NotificationDelivery.recipient_id == recipient_id,
            NotificationDelivery.status == DeliveryStatus.SENT,
            NotificationDelivery.read_at.is_(None),
        )
        .values(read_at=datetime.now(UTC))
    )
    result = await session.execute(stmt)
    await session.flush()
    marked: int = result.rowcount  # type: ignore[attr-defined]
    return marked
