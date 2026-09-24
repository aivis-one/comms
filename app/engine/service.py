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
# FUNCTIONS (intake, F1.2):
#   accept_notification()   -- THE intake: create under a key, or answer
#                              duplicate / conflict by fingerprint
#   stream_fingerprint(), canonical_fingerprint() -- the two ways a
#                              request's bytes are fingerprinted
#   record_intake_outcome(), intake_outcomes_for() -- requests that
#                              were NOT accepted, under their key
#   expiry_of()             -- the expiry of a job and the layer that
#                              decided it
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
import hashlib
import json
import random
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import delete, func, or_, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import Recipient
from app.audience.prefs import muted_recipient_ids
from app.audience.schedule import recipient_deferred_until
from app.core.config import settings
from app.core.exceptions import NotFoundError, ValidationError
from app.engine.constants import (
    DeliveryStatus,
    FailureClass,
    IntakeOutcomeClass,
    NotificationStatus,
    TargetType,
    WaitReason,
)
from app.engine.formatters import (
    ChannelFormatter,
    ConfigurationError,
    MessageRejectedError,
    NoAddressError,
    PermanentDeliveryError,
    RateLimitedError,
    get_formatter,
    sanitize_error,
    sanitize_text,
    sanitized_traceback,
)
from app.engine.models import IntakeOutcome, Notification, NotificationDelivery
from app.engine.resolver import resolve_targets
from app.profile.registry import Decided, Layer, registry

logger = structlog.get_logger()

# Valid infrastructure enum values for input validation.
_VALID_TARGET_TYPES = frozenset(e.value for e in TargetType)

# The one unique index that means "this key is taken" (migration 0012).
IDEMPOTENCY_INDEX_NAME = "uq_notifications_idempotency_key"

# Timeout for a single formatter.deliver() call.
_DELIVER_TIMEOUT_SECONDS = 30

# Phase 3a item 7: 429 jitter as a FRACTION of the honored wait
# (uniform(0, fraction) x honored, one-sided -- see the deferral
# block in _process_single_notification and fix D note in config.py).
# 0.5 spreads one burst's herd over half its own wait window: wide
# enough to decorrelate, still the same order as the server's ask.
_RATE_LIMIT_JITTER_MAX_FRACTION = 0.5

# Max concurrent formatter.deliver() calls per notification.
_MAX_CONCURRENT_DELIVERIES = 20


# -----------------------------------------------------------------------------
# Intake (F1.2)
# -----------------------------------------------------------------------------
#
# TWO WAYS TO FINGERPRINT, ONE PER KIND OF PRODUCER, and why two. The
# fingerprint answers one question -- "are these the same bytes under
# this key?" -- and it must be answered WITHOUT reading the request.
#
#   stream_fingerprint     -- a product's request arrives as bytes on
#                             the wire (the `data` field of the stream
#                             entry). The digest is taken over exactly
#                             those bytes, before any parsing: nothing
#                             is normalised, so nothing is interpreted.
#                             A product that re-serialises the same
#                             meaning differently gets a conflict --
#                             loud, which is the right side to err on.
#   canonical_fingerprint  -- comms' own producers (the chat notifier)
#                             never had wire bytes; their request is a
#                             set of arguments. The digest is taken over
#                             a canonical JSON of those arguments
#                             (sorted keys, fixed separators), so the
#                             same call always yields the same digest.
#
# The two never have to agree: a product key and an internal key live
# in one index, and if they ever coincide the digests differ by
# construction, so the collision surfaces as a conflict instead of as a
# silent duplicate.


def stream_fingerprint(data: bytes) -> str:
    """Digest of a stream request's `data` bytes, as they arrived."""
    return hashlib.sha256(data).hexdigest()


def canonical_fingerprint(fields: Mapping[str, Any]) -> str:
    """Digest of an internal producer's arguments, canonically encoded."""
    encoded = json.dumps(
        fields,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class Intake(StrEnum):
    """What accept_notification did with a request."""

    ACCEPTED = "accepted"
    # Same key, same bytes: the existing job, nothing new created.
    DUPLICATE = "duplicate"
    # Same key, other bytes: nothing created, the existing job untouched.
    CONFLICT = "conflict"


@dataclass(frozen=True)
class Acceptance:
    """The outcome of one intake and the job it concerns.

    `notification` is the created job for ACCEPTED, and the job that
    already holds the key for DUPLICATE and CONFLICT.
    """

    outcome: Intake
    notification: Notification


async def accept_notification(
    session: AsyncSession,
    *,
    idempotency_key: str,
    fingerprint: str,
    **fields: Any,
) -> Acceptance:
    """Accept one request under its key -- the single intake.

    The DATABASE decides "is this key taken": the insert runs inside a
    SAVEPOINT, and the unique index turns a second insert into an
    IntegrityError. No pre-flight SELECT -- a check-then-insert races
    with itself; the constraint does not. Two concurrent intakes of one
    key therefore produce ONE job: the second insert waits for the
    first transaction and then fails, and only then is the holder read
    and its fingerprint compared -- equal bytes answer DUPLICATE, other
    bytes CONFLICT. Never two jobs.

    The comparison is of digests only (spec §5.8): the request is not
    parsed, decoded or interpreted for it.

    ValidationError from create_notification (a request that cannot be
    accepted) propagates -- the savepoint is rolled back, and the
    caller records the rejection.
    """
    try:
        async with session.begin_nested():
            notification = await create_notification(
                session,
                idempotency_key=idempotency_key,
                fingerprint=fingerprint,
                **fields,
            )
    except IntegrityError as exc:
        # Only OUR index means "key taken"; any other violation is a
        # real bug and must not be swallowed as a replay.
        if IDEMPOTENCY_INDEX_NAME not in str(exc.orig):
            raise
        holder = (
            await session.execute(
                select(Notification).where(
                    Notification.idempotency_key == idempotency_key,
                )
            )
        ).scalar_one()
        outcome = (
            Intake.DUPLICATE
            if holder.fingerprint == fingerprint
            else Intake.CONFLICT
        )
        return Acceptance(outcome=outcome, notification=holder)
    return Acceptance(outcome=Intake.ACCEPTED, notification=notification)


async def record_intake_outcome(
    session: AsyncSession,
    *,
    idempotency_key: str,
    fingerprint: str,
    outcome: IntakeOutcomeClass,
    reason: str,
    notification_id: UUID | None = None,
) -> None:
    """Record a request that was not accepted, under its key.

    The reason is redacted: it quotes the producer's input, and this
    row outlives the event. A replay of the same bytes with the same
    outcome records nothing new (unique index, migration 0012).
    """
    statement = (
        pg_insert(IntakeOutcome)
        .values(
            idempotency_key=idempotency_key,
            fingerprint=fingerprint,
            outcome=outcome.value,
            reason=sanitize_text(reason)[:2000],
            notification_id=notification_id,
        )
        .on_conflict_do_nothing(
            index_elements=["idempotency_key", "fingerprint", "outcome"],
        )
    )
    await session.execute(statement)


async def intake_outcomes_for(
    session: AsyncSession, idempotency_key: str,
) -> list[IntakeOutcome]:
    """Every recorded non-acceptance under a key, oldest first.

    The programmatic answer until reading by key exists for the
    product (phase 2).
    """
    rows = await session.execute(
        select(IntakeOutcome)
        .where(IntakeOutcome.idempotency_key == idempotency_key)
        .order_by(IntakeOutcome.received_at, IntakeOutcome.id)
    )
    return list(rows.scalars().all())


async def delete_intake_outcomes_before(
    session: AsyncSession, *, cutoff: datetime,
) -> int:
    """Retention for intake_outcomes: rows received before `cutoff`."""
    result = await session.execute(
        delete(IntakeOutcome).where(IntakeOutcome.received_at < cutoff)
    )
    return int(result.rowcount or 0)  # type: ignore[attr-defined]


def expiry_of(notification: Notification) -> Decided:
    """The job's expiry and the layer that decided it."""
    layer = Layer(notification.expiry_layer)
    source = {
        Layer.ENVELOPE: "envelope: expiry_at",
        Layer.PROFILE: (
            f"profile: expires_after of {notification.type!r}, "
            f"counted from scheduled_at"
        ),
        Layer.DEFAULT: "comms default: no expiry declared",
    }[layer]
    return Decided(value=notification.expiry_at, layer=layer, source=source)


async def create_notification(
    session: AsyncSession,
    *,
    type: str,
    title: str,
    body: str,
    target_type: str,
    target_value: str,
    idempotency_key: str,
    fingerprint: str,
    action_data: dict[str, Any] | None = None,
    scheduled_at: datetime | None = None,
    expiry_at: datetime | None = None,
    correlation: str | None = None,
) -> Notification:
    """Create a new Notification record.

    Intake goes through accept_notification, which adds the
    duplicate / conflict decision; this function is the insert under
    it and never decides anything about the key.

    Args:
        session: Active DB session (caller commits).
        type: A notification type key declared by the product profile.
            The type's CHANNELS come from its profile record and are
            snapshotted onto the row -- the caller never names one.
        title: Stored fallback title (the letter).
        body: Stored fallback body (the letter).
        target_type: TargetType value (user, group, all).
        target_value: Bare target specifier ("<uuid>", "<group_key>", "*").
        idempotency_key: The request's key -- required on every path.
        fingerprint: Digest of the request's bytes (see above).
        action_data: Optional JSONB letter (deep-link intent + template
            variables).
        scheduled_at: "Not before" -- when the job becomes deliverable.
            Defaults to now.
        expiry_at: The envelope's expiry. Wins over the profile's
            expires_after, which counts from scheduled_at.
        correlation: The product's own reference, stored untouched.

    Returns:
        The created Notification (flushed, not committed).

    Raises:
        ValidationError: The request cannot be accepted -- undeclared
            type, invalid target_type, an expiry already passed or not
            after scheduled_at.
    """
    # -- The profile record IS the registration (every installed type
    # has one; a type without a record is not declared). --
    record = registry.record_of(type)
    if record is None:
        raise ValidationError(
            f"Unregistered notification type: {type}. "
            f"Registered: {', '.join(sorted(registry.registered_types()))}"
        )

    if target_type not in _VALID_TARGET_TYPES:
        raise ValidationError(
            f"Invalid target_type: {target_type}. "
            f"Valid: {', '.join(sorted(_VALID_TARGET_TYPES))}"
        )

    now = datetime.now(UTC)
    if scheduled_at is None:
        scheduled_at = now

    # -- Expiry: the envelope over the profile, the layer kept. --
    if expiry_at is not None:
        if expiry_at <= now:
            raise ValidationError(
                f"expiry_at {expiry_at.isoformat()} has already passed "
                f"(now {now.isoformat()}): the request could never be "
                f"delivered in time"
            )
        if expiry_at <= scheduled_at:
            raise ValidationError(
                f"expiry_at {expiry_at.isoformat()} is not after "
                f"scheduled_at {scheduled_at.isoformat()}: the request "
                f"would expire before it becomes deliverable"
            )
        expiry_layer = Layer.ENVELOPE
    else:
        expires_after: timedelta | None = record.value("expires_after")
        if expires_after is not None:
            expiry_at = scheduled_at + expires_after
            expiry_layer = Layer.PROFILE
        else:
            expiry_layer = Layer.DEFAULT

    channels = list(record.value("channels"))
    category = record.value("category")

    notification = Notification(
        type=type,
        title=title,
        body=body,
        target_type=target_type,
        target_value=target_value,
        action_data=action_data,
        channels=channels,
        category=category,
        correlation=correlation,
        scheduled_at=scheduled_at,
        expiry_at=expiry_at,
        expiry_layer=expiry_layer.value,
        idempotency_key=idempotency_key,
        fingerprint=fingerprint,
        status=NotificationStatus.PENDING,
    )
    session.add(notification)
    await session.flush()

    logger.info(
        "notification_created",
        notification_id=str(notification.id),
        type=type,
        target=f"{target_type}:{target_value}",
        channels=channels,
        scheduled_at=scheduled_at.isoformat(),
        expiry_layer=expiry_layer.value,
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
    Nobody resolved -> NO_RECIPIENTS (fix the sync); everyone muted ->
    SUPPRESSED (the recipients' decision). Neither is a failure (F1.3:
    the two were one SKIPPED, which made a product look for a defect
    where there was a choice, and miss one where there was a defect).

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
        # Nobody to deliver to: the product's sync, not comms, is what
        # to look at -- and not the recipients' choice either.
        notification.status = NotificationStatus.NO_RECIPIENTS
        logger.warning(
            "notification_no_targets",
            notification_id=str(notification.id),
        )
        return []

    # -- Mute gate: drop recipients who muted this type's category --
    # Evaluated at resolve time; for reminders that is the moment the
    # notification comes due, so the mute state is current as of send.
    # A mute set AFTER deliveries exist is caught by the second line
    # at deliver time (late-mute re-check -> DeliveryStatus.SUPPRESSED).
    # The category is the one snapshotted at intake (F1.3): a type
    # removed from the profile since is still gated.
    category = notification.category
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
            # Every recipient muted it: their decision, not a failure.
            notification.status = NotificationStatus.SUPPRESSED
            logger.info(
                "notification_all_muted",
                notification_id=str(notification.id),
                category=category,
            )
            return []

    # The channels the profile routed the type to at intake.
    channels = notification.channels

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
      the first line, but a delivery can sit gated for HOURS (the
      schedule gate stretched the wait far past the old 30-60s
      backoff). So
      right before sending, recipients who muted the notification's
      category since resolve are closed out terminally with
      DeliveryStatus.SUPPRESSED -- not FAILED (nothing broke): the
      recipient's decision, mirroring the notification-level
      SUPPRESSED. The category is the intake snapshot (F1.3). One
      batched lookup per pass;
      checked BEFORE the schedule gate (no point deferring a muted
      delivery). Attempts and error_message stay untouched (a skip is
      not an attempt; prior transient history is kept).
    - DELIVERY SCHEDULE (Phase 2; polarity flipped in R-5): a
      delivery whose recipient is OUTSIDE their allowed periods is
      DEFERRED, not sent -- next_retry_at is set to the next period's
      start (recipient's timezone) and the existing retry gate keeps
      it invisible to the poll until then. Attempts and error_message
      stay untouched: deferral is not a failure. Checked per attempt,
      so backoff retries landing outside the periods are deferred too
      -- which is one of the two reasons this gate cannot live in the
      product (the other: comms creates message pings and close
      notices itself, for which no product ever computed a time).
      TIGHT EXPIRY OUTSIDE THE PERIODS: when the next opening lands
      past the notification's expiry_at, the step-0 expire sweep will
      mark it EXPIRED before the gate reopens -- a deliberate expiry,
      not a late send (a "1 hour before" reminder deferred past its
      anchor must die quietly, not arrive mid-session). The
      delivery_schedule_deferred log carries beyond_expiry=true for
      causality.
    - CHANNEL RATE LIMIT (Phase 2.2): a 429 is "come back later", not
      a message failure -- the delivery is deferred via next_retry_at
      using the SERVER-NAMED retry_after (capped at
      notification_max_retry_after_seconds -- a dedicated trust limit
      on channel-named waits, generous so capping stays exceptional;
      plus proportional one-sided jitter, up to +50% of the honored
      wait, against thundering herd -- Phase 3a item 7), without
      burning an attempt; a per-delivery deferral budget
      (rate_limit_deferrals vs
      settings.notification_max_rate_limit_deferrals) bounds the
      loop, past it a 429 degrades to a regular transient failure.
      Its deferral log carries the same beyond_expiry causality flag
      as the quiet gate. (Third path past expiry -- the plain
      transient backoff gate, 30-600s -- is known and unflagged: the
      shortest window of the three, not worth threading the
      notification through _apply_transient_failure for one log
      field.)
    - Concurrent delivery via asyncio.gather + Semaphore.
    - asyncio.wait_for with timeout per formatter call.
    - PermanentDeliveryError -> immediate FAILED with its FailureClass,
      no attempts increment (F1.3: the class is the exception's type).
    - WAIT REASONS (F1.3): every path that sets next_retry_at sets
      wait_reason with it (recipient schedule, provider rate limit,
      transient backoff); a delivery taken into an attempt has both
      cleared first, so a reason never outlives the wait it explains.
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

    # -- Late-mute re-check: one batched probe per pass, over the
    # category snapshotted at intake (F1.3) --
    category = notification.category
    muted: set[UUID] = set()
    if category is not None:
        muted = await muted_recipient_ids(
            session, category, list(recipient_ids),
        )

    # -- Concurrent delivery via gather + semaphore --
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT_DELIVERIES)
    tasks = []

    for delivery in deliveries:
        # Every delivery has its recipient row: recipient_id is a
        # foreign key with ON DELETE CASCADE, so deleting a recipient
        # deletes its deliveries. (A "recipient not found -> FAILED"
        # branch stood here for a state that cannot exist; removed in
        # F1.3 rather than given a failure class.)
        recipient = recipients_by_id[delivery.recipient_id]

        # -- Leaving the wait: the gate that held this delivery has
        # opened; its reason goes with it. A gate below may set both
        # again. --
        delivery.next_retry_at = None
        delivery.wait_reason = None

        # -- Late activity gate (F1.4): deactivated or deleted by the
        # product after resolve -> close out, no send. Checked first: a
        # person the product removed is not asked about their mutes. --
        if not recipient.active:
            delivery.status = DeliveryStatus.RECIPIENT_INACTIVE
            logger.info(
                "delivery_recipient_inactive",
                delivery_id=str(delivery.id),
                recipient_id=str(delivery.recipient_id),
            )
            continue

        # -- Late-mute gate: muted while gated -> close out, no send --
        if delivery.recipient_id in muted:
            delivery.status = DeliveryStatus.SUPPRESSED
            logger.info(
                "delivery_muted_skipped",
                delivery_id=str(delivery.id),
                recipient_id=str(delivery.recipient_id),
                category=category,
            )
            continue

        # -- Schedule gate: defer, never suppress --
        # The recipient's allowed periods (app/audience/schedule.py).
        # None means "now is fine" -- both when a period covers now
        # and when there is no schedule at all.
        quiet_until = recipient_deferred_until(recipient, now)
        if quiet_until is not None:
            delivery.next_retry_at = quiet_until
            delivery.wait_reason = WaitReason.RECIPIENT_SCHEDULE
            # Causality flag: the deferral pushes the delivery past
            # the notification's expiry -> step-0 will EXPIRE it
            # before it ever sends. Deliberate (a reminder deferred
            # past its anchor must die, not arrive late), but the log
            # must show WHY it died.
            beyond_expiry = (
                notification.expiry_at is not None
                and quiet_until > notification.expiry_at
            )
            logger.info(
                "delivery_schedule_deferred",
                delivery_id=str(delivery.id),
                recipient_id=str(recipient.id),
                until=quiet_until.isoformat(),
                beyond_expiry=beyond_expiry,
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
        for delivery, outcome in results:
            if outcome.failure_class is not None:
                delivery.status = DeliveryStatus.FAILED
                delivery.failure_class = outcome.failure_class
                delivery.error_message = outcome.error
            elif outcome.retry_after is not None:
                # -- Channel rate limit (429): defer, don't burn --
                budget = settings.notification_max_rate_limit_deferrals
                if delivery.rate_limit_deferrals < budget:
                    delivery.rate_limit_deferrals += 1
                    # CAP the honored server wait (Phase 2.3):
                    # retry_after is UNTRUSTED channel output steering
                    # our scheduler -- a pathological value (ms-vs-s
                    # mixup, buggy server) must not park the delivery
                    # for hours. The ceiling is a dedicated TRUST knob
                    # (max_retry_after), deliberately generous so that
                    # capping stays EXCEPTIONAL: capped=true below
                    # means "the channel asked to wait longer than we
                    # are willing to honor" -- overriding the very
                    # server that rate-limits us is the road to bot
                    # bans if it ever becomes routine. Pairs with
                    # rate_limit_deferrals as a promotion signal for
                    # the broadcast-hardening backlog. A LEGITIMATE
                    # wait beyond the cap burns the deferral budget in
                    # cap-sized bites and ends in an explicit FAILED
                    # with full history -- better observability than
                    # silently parking on a value we cannot verify.
                    honored = min(
                        outcome.retry_after,
                        float(settings.notification_max_retry_after_seconds),
                    )
                    capped = outcome.retry_after > honored
                    # Jitter on top (added AFTER the cap -- the jitter
                    # is ours, not the server's): every delivery
                    # deferred by one burst must NOT wake in the same
                    # tick and 429 again (thundering herd).
                    # PROPORTIONAL (Phase 3a item 7): a fixed 1-2s
                    # spreads a 3s wait fine and a 3000s flood wait not
                    # at all -- the spread must scale with the wait.
                    # ONE-SIDED (fix D): uniform(0, max) never wakes a
                    # delivery EARLIER than the server asked; the
                    # effective ceiling is cap x (1 + max fraction) =
                    # cap x 1.5 (documented on the cap knob in
                    # app/core/config.py).
                    delay = honored * (
                        1.0 + random.uniform(
                            0.0, _RATE_LIMIT_JITTER_MAX_FRACTION,
                        )
                    )
                    next_retry_at = datetime.now(UTC) + timedelta(
                        seconds=delay,
                    )
                    delivery.next_retry_at = next_retry_at
                    delivery.wait_reason = WaitReason.PROVIDER_RATE_LIMIT
                    # The provider's words go into the record on EVERY
                    # deferral (F1.3), not only when the budget runs out:
                    # "where did it tear" is answered by the row.
                    delivery.error_message = outcome.error
                    # Same causality flag as the quiet-hours gate: the
                    # deferral pushes the delivery past expiry -> the
                    # step-0 sweep will EXPIRE it, deliberately.
                    beyond_expiry = (
                        notification.expiry_at is not None
                        and next_retry_at > notification.expiry_at
                    )
                    logger.info(
                        "delivery_rate_limit_deferred",
                        delivery_id=str(delivery.id),
                        retry_after=outcome.retry_after,
                        capped=capped,
                        deferrals=delivery.rate_limit_deferrals,
                        budget=budget,
                        beyond_expiry=beyond_expiry,
                    )
                else:
                    # Budget exhausted: this 429 degrades to a regular
                    # transient failure -- the attempts budget takes
                    # over, which is finite (no infinite deferral).
                    logger.warning(
                        "delivery_rate_limit_budget_exhausted",
                        delivery_id=str(delivery.id),
                        deferrals=delivery.rate_limit_deferrals,
                        budget=budget,
                    )
                    _apply_transient_failure(delivery, outcome.error)
            elif outcome.success:
                delivery.attempts += 1
                delivery.status = DeliveryStatus.SENT
                delivery.sent_at = datetime.now(UTC)
            else:
                _apply_transient_failure(delivery, outcome.error)

    await session.flush()


def _apply_transient_failure(
    delivery: NotificationDelivery,
    error: str | None,
) -> None:
    """Apply one transient failure: burn an attempt, gate or fail.

    Shared by the regular transient path and the exhausted-budget 429
    path (a 429 past the deferral budget behaves exactly like any
    other transient error).
    """
    delivery.attempts += 1
    delivery.error_message = error
    if delivery.attempts >= settings.notification_max_delivery_attempts:
        delivery.status = DeliveryStatus.FAILED
        delivery.failure_class = FailureClass.TRANSIENT_EXHAUSTED
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
        delivery.wait_reason = WaitReason.TRANSIENT_BACKOFF


def _failure_class_of(exc: PermanentDeliveryError) -> FailureClass:
    """The failure class a permanent channel exception stands for."""
    if isinstance(exc, ConfigurationError):
        return FailureClass.CONFIGURATION
    if isinstance(exc, NoAddressError):
        return FailureClass.NO_ADDRESS
    if isinstance(exc, MessageRejectedError):
        return FailureClass.MESSAGE_REJECTED
    # PermanentDeliveryError refuses construction; a fourth subclass
    # added without a class must fail loudly here, not pick one.
    raise TypeError(f"no failure class for {type(exc).__name__}")


def _error_text(exc: Exception) -> str:
    """What goes into error_message: the redacted text, or -- when the
    exception has none (KeyError(), a bare RuntimeError) -- its type.
    An empty string would be the same silence F0 closed for provider
    answers."""
    return sanitize_error(exc) or type(exc).__name__


class _DeliveryOutcome:
    """Result of a single delivery attempt."""

    __slots__ = ("error", "failure_class", "retry_after", "success")

    def __init__(
        self,
        *,
        success: bool = False,
        failure_class: FailureClass | None = None,
        error: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        self.success = success
        # Non-None marks a permanent failure: its class, decided once.
        self.failure_class = failure_class
        self.error = error
        # Non-None marks a channel rate limit (429): the server-named
        # wait in seconds. Handled by the apply loop against the
        # deferral budget.
        self.retry_after = retry_after


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
            # THE one place a channel exception becomes a failure class:
            # the exception's type is the class (formatters raise only
            # the three subclasses -- the base refuses construction).
            failure_class = _failure_class_of(exc)
            # A dead channel is LOUD (spec §5.6): every message will die
            # the same way until the deploy changes.
            log = (
                logger.error
                if failure_class is FailureClass.CONFIGURATION
                else logger.warning
            )
            log(
                "delivery_permanent_failure",
                delivery_id=str(delivery.id),
                channel=delivery.channel,
                failure_class=failure_class.value,
                error=sanitize_text(str(exc))[:200],
            )
            return delivery, _DeliveryOutcome(
                failure_class=failure_class, error=_error_text(exc),
            )
        except RateLimitedError as exc:
            # Deliberately no log here: whether this becomes a deferral
            # or degrades to a transient failure is decided in the
            # apply loop (it owns the budget counter) -- that decision
            # log is the valuable one, and doubling it would be noise.
            return delivery, _DeliveryOutcome(
                error=_error_text(exc),
                retry_after=exc.retry_after,
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
            # Not logger.exception(): the renderer would print the chain
            # raw, and the telegram network error carries the bot token
            # in its text and in its __cause__. Same event, same level,
            # same `exception` key -- redacted (formatters.py).
            logger.error(
                "delivery_error",
                delivery_id=str(delivery.id),
                channel=delivery.channel,
                exception=sanitized_traceback(exc),
            )
            return delivery, _DeliveryOutcome(
                error=_error_text(exc),
            )


# -----------------------------------------------------------------------------
# The parent's outcome -- THE FOLD (F1.3, spec §5.5)
# -----------------------------------------------------------------------------
#
# A notification is one job with many deliveries; its outcome is the
# FOLD of theirs. One rule, applied by rollup_notification and by
# nothing else:
#
#   1. SUPPRESSED and RECIPIENT_INACTIVE deliveries are taken out
#      first: a recipient's decision (a mute) and a product's decision
#      (deactivated or deleted, F1.4) are neither a delivery nor a
#      failure. If nothing else is left: SUPPRESSED when any recipient
#      muted it, otherwise NO_RECIPIENTS -- by send time the product
#      had removed everyone. Never FAILED.
#   2. Any delivery still PENDING -> the job stays PROCESSING.
#   3. Every remaining delivery SENT                 -> SENT.
#   4. At least one SENT and at least one not sent   -> PARTIAL_SENT.
#   5. None SENT: any FAILED -> FAILED; else any EXPIRED -> EXPIRED;
#      else CANCELLED. (A failure is what a product must act on, so it
#      outranks the two outcomes that were decided on purpose.)
#   6. No deliveries at all -> NO_RECIPIENTS: the recipients the
#      deliveries were made for are gone (ON DELETE CASCADE removed
#      them), so the audience is empty -- the same outcome as an empty
#      resolve, not an anomaly FAILED.
#
# EXPIRY AND CANCELLATION GO THROUGH THE DELIVERIES (close_notifications
# below): the waiting deliveries get the outcome, then the job is
# folded by the same rule. A job half of which went out before its
# expiry is PARTIAL_SENT, not EXPIRED. Only a job with no deliveries
# yet (PENDING, not resolved) takes EXPIRED / CANCELLED directly.
#
# IN FLIGHT: an attempt runs inside the worker's transaction under a
# row lock on the notification (processor.py, FOR UPDATE). Expiry and
# cancellation lock the same row (FOR UPDATE, waiting, not skipping),
# so they apply to the state AFTER the attempt: what was sent stays
# sent, what is still waiting is closed.


async def rollup_notification(
    session: AsyncSession,
    notification: Notification,
) -> None:
    """Fold the deliveries' outcomes into the job's (see THE FOLD).

    Acts on PROCESSING only: every other status is either not resolved
    yet or already an outcome, and an outcome is never re-decided.

    Args:
        session: Active DB session (caller commits).
        notification: The notification to roll up.
    """
    if notification.status != NotificationStatus.PROCESSING:
        return

    stmt = (
        select(NotificationDelivery.status)
        .where(NotificationDelivery.notification_id == notification.id)
        # Only DISTINCT statuses: the verdict needs the set, not one
        # row per delivery (Phase 2.2 -- constant-size result).
        .distinct()
    )
    result = await session.execute(stmt)
    statuses = {row[0] for row in result.all()}

    verdict = _fold(statuses)
    if verdict is None:
        return
    notification.status = verdict
    await session.flush()
    logger.info(
        "notification_rollup",
        notification_id=str(notification.id),
        status=notification.status,
    )


def _fold(statuses: set[str]) -> NotificationStatus | None:
    """THE FOLD over the set of delivery statuses; None = still open."""
    if not statuses:
        return NotificationStatus.NO_RECIPIENTS
    active = statuses - {
        DeliveryStatus.SUPPRESSED, DeliveryStatus.RECIPIENT_INACTIVE,
    }
    if not active:
        if DeliveryStatus.SUPPRESSED in statuses:
            return NotificationStatus.SUPPRESSED
        return NotificationStatus.NO_RECIPIENTS
    if DeliveryStatus.PENDING in active:
        return None
    if active == {DeliveryStatus.SENT}:
        return NotificationStatus.SENT
    if DeliveryStatus.SENT in active:
        return NotificationStatus.PARTIAL_SENT
    if DeliveryStatus.FAILED in active:
        return NotificationStatus.FAILED
    if DeliveryStatus.EXPIRED in active:
        return NotificationStatus.EXPIRED
    return NotificationStatus.CANCELLED


# The two outcomes a job can be CLOSED with from outside the attempt,
# and the delivery outcome each gives the deliveries still waiting.
_CLOSING = {
    NotificationStatus.EXPIRED: DeliveryStatus.EXPIRED,
    NotificationStatus.CANCELLED: DeliveryStatus.CANCELLED,
}

_ACTIVE_STATUSES = (NotificationStatus.PENDING, NotificationStatus.PROCESSING)


async def withdraw_recipient(session: AsyncSession, recipient_id: UUID) -> int:
    """Close a deleted recipient's deliveries (F1.4, forgetting).

    Every delivery of theirs still waiting takes RECIPIENT_INACTIVE
    (wait fields cleared) and each affected job is folded (THE FOLD).
    The jobs are locked FOR UPDATE and the lock is WAITED for, like
    expiry and cancellation: an attempt in flight finishes first. Every
    delivery of theirs, finished or not, loses its error_message -- a
    provider's words may quote the address being forgotten.

    Returns the number of deliveries closed.
    """
    parents = (
        await session.execute(
            select(Notification)
            .where(
                Notification.id.in_(
                    select(NotificationDelivery.notification_id).where(
                        NotificationDelivery.recipient_id == recipient_id,
                        NotificationDelivery.status == DeliveryStatus.PENDING,
                    )
                ),
            )
            .with_for_update()
        )
    ).scalars().all()
    closed = await session.execute(
        update(NotificationDelivery)
        .where(
            NotificationDelivery.recipient_id == recipient_id,
            NotificationDelivery.status == DeliveryStatus.PENDING,
        )
        .values(
            status=DeliveryStatus.RECIPIENT_INACTIVE,
            next_retry_at=None,
            wait_reason=None,
        )
    )
    await session.execute(
        update(NotificationDelivery)
        .where(NotificationDelivery.recipient_id == recipient_id)
        .values(error_message=None)
    )
    for notification in parents:
        await rollup_notification(session, notification)
    await session.flush()
    return int(closed.rowcount or 0)  # type: ignore[attr-defined]


async def close_notifications(
    session: AsyncSession,
    condition: Any,
    outcome: NotificationStatus,
) -> int:
    """Expire or cancel every ACTIVE job matching `condition`.

    The waiting deliveries take the outcome and the job is folded (see
    THE FOLD); a job with no deliveries yet takes the outcome directly.
    A job already at an outcome is not touched -- that is the guard,
    and a second call is a zero-row no-op. Rows are locked FOR UPDATE
    and the lock is WAITED for: an attempt in flight finishes first.

    Returns the number of jobs closed.
    """
    delivery_outcome = _CLOSING[outcome]
    rows = await session.execute(
        select(Notification)
        .where(condition, Notification.status.in_(_ACTIVE_STATUSES))
        .with_for_update()
    )
    notifications = list(rows.scalars().all())
    for notification in notifications:
        if notification.status == NotificationStatus.PENDING:
            # Not resolved: no deliveries exist to carry the outcome.
            notification.status = outcome
            continue
        await session.execute(
            update(NotificationDelivery)
            .where(
                NotificationDelivery.notification_id == notification.id,
                NotificationDelivery.status == DeliveryStatus.PENDING,
            )
            .values(
                status=delivery_outcome,
                next_retry_at=None,
                wait_reason=None,
            )
        )
        await rollup_notification(session, notification)
    await session.flush()
    return len(notifications)


# ---------------------------------------------------------------------------
# In-app inbox functions (cbshome Sprint 8.3; HTTP surface is Phase 3)
# ---------------------------------------------------------------------------


# Phase 3a item 5: terminal statuses subject to retention -- ALL of
# them (seven since F1.3). PARTIAL_SENT was missing from the original spec list; the
# Phase 3a report flagged it (rows would be immortal: polling never
# picks them up, rollup never returns to them) and Master-chat ruled
# it IN (Phase 3a.1): a partial send is no less finished than a full
# one, and 90 days covers any incident review. Active statuses
# (PENDING / PROCESSING) must never appear here.
_RETENTION_TERMINAL_STATUSES = (
    NotificationStatus.SENT,
    NotificationStatus.PARTIAL_SENT,
    NotificationStatus.FAILED,
    NotificationStatus.EXPIRED,
    NotificationStatus.CANCELLED,
    NotificationStatus.SUPPRESSED,
    NotificationStatus.NO_RECIPIENTS,
)


async def delete_terminal_notifications_batch(
    session: AsyncSession,
    *,
    cutoff: datetime,
    limit: int,
) -> int:
    """Delete ONE batch of terminal notifications older than cutoff.

    COMMIT-FREE (P-01: the service never commits) -- returns the
    number of rows deleted in this batch; the caller (the retention
    pass in app/engine/processor.py, fix C) owns the drain loop and
    the per-batch commit. Deliveries follow by FK cascade.

    The batch is picked oldest-first via an IN subquery (ORDER BY
    created_at LIMIT n): DELETE ... LIMIT is not portable SQL, and the
    subquery bounds each transaction to `limit` rows plus their
    cascade -- an unbounded DELETE over a 90-day backlog was rejected
    in the handoff (one long transaction + cascade).

    Age is measured on created_at: terminal rows are immutable and the
    model carries no updated_at.
    """
    batch_ids = (
        select(Notification.id)
        .where(
            Notification.status.in_(_RETENTION_TERMINAL_STATUSES),
            Notification.created_at < cutoff,
        )
        .order_by(Notification.created_at)
        .limit(limit)
        .scalar_subquery()
    )
    result = await session.execute(
        delete(Notification).where(Notification.id.in_(batch_ids)),
    )
    deleted: int = result.rowcount  # type: ignore[attr-defined]
    return deleted


def _navigation_intent(
    action_data: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Extract the NAVIGATIONAL subset of action_data for the inbox.

    action_data carries TWO things (arch §2.3): the deep-link intent
    ({action, params}) and the template variables. The variables are
    internal rendering material -- already substituted into the
    title/body the inbox returns -- and must NOT leak into the frozen
    inbox contract (Master-chat review, Phase 3b amendment A). The
    frontend needs exactly one thing from action_data: where a tap
    goes.

    Whitelist, not blacklist: only "action" (and "params", when
    present and non-empty) survive. No action -> None (the item is
    not tappable). The params dict is passed through as-is -- shape
    validation belongs to the producer (Phase 3c), not to a read path.
    """
    if not action_data:
        return None
    action = action_data.get("action")
    if not action:
        return None
    intent: dict[str, Any] = {"action": action}
    params = action_data.get("params")
    if params:
        intent["params"] = params
    return intent




async def list_recipient_deliveries(
    session: AsyncSession,
    recipient_id: UUID,
    *,
    limit: int = 20,
    cursor: tuple[datetime, UUID] | None = None,
    type_filter: str | None = None,
    channel_filter: str | None = None,
) -> tuple[list[dict[str, Any]], tuple[datetime, UUID] | None]:
    """List sent deliveries for a recipient, keyset-paginated.

    Only deliveries with status=sent are returned (the recipient sees
    only what was actually delivered). Results enriched with title,
    body, type and the NAVIGATIONAL action_data subset from the parent
    Notification, newest-first.

    KEYSET (Phase 3b item 2, replaces the Phase 1 offset version --
    offset pagination re-scans every skipped row and shifts under
    concurrent inserts; its only consumers were tests):
      - order: (sent_at DESC, id DESC). sent_at alone is not unique
        (one notification fans out a batch of deliveries within the
        same timestamp resolution), so the delivery id breaks ties --
        together they are a stable total order.
      - cursor: the (sent_at, id) pair of the LAST row of the previous
        page; the next page is WHERE (sent_at, id) < (cursor) in that
        order (a Postgres row-value comparison).
      - limit+1 rows are fetched to learn whether a next page exists
        without a COUNT.

    Args:
        session: Active DB session (read-only).
        recipient_id: Recipient (= product user) id.
        limit: Page size, already bounded by the route
            (app/api/paging.py refuses anything outside 1..PAGE_LIMIT_MAX).
        cursor: (sent_at, id) of the last row already seen, or None
            for the first page.
        type_filter: Filter by Notification.type (exact match).
        channel_filter: Filter by NotificationDelivery.channel.

    Returns:
        (items, next_cursor) where items are plain dicts and
        next_cursor is the (sent_at, id) pair to request the next page
        with, or None when this page is the last.
    """

    conditions = [
        NotificationDelivery.recipient_id == recipient_id,
        NotificationDelivery.status == DeliveryStatus.SENT,
    ]

    if type_filter:
        conditions.append(Notification.type == type_filter)
    if channel_filter:
        conditions.append(NotificationDelivery.channel == channel_filter)
    if cursor is not None:
        # Row-value comparison: tuple_(cols) against a plain Python
        # tuple of the cursor values -- Postgres evaluates
        # (sent_at, id) < (:sent_at, :id) natively.
        conditions.append(
            tuple_(NotificationDelivery.sent_at, NotificationDelivery.id)
            < cursor
        )

    stmt = (
        select(NotificationDelivery, Notification)
        .join(
            Notification,
            NotificationDelivery.notification_id == Notification.id,
        )
        .where(*conditions)
        .order_by(
            NotificationDelivery.sent_at.desc(),
            NotificationDelivery.id.desc(),
        )
        .limit(limit + 1)
    )
    result = await session.execute(stmt)
    rows = result.all()

    has_more = len(rows) > limit
    rows = rows[:limit]

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
            "action_data": _navigation_intent(notification.action_data),
        })

    next_cursor: tuple[datetime, UUID] | None = None
    if has_more and rows:
        last_delivery, _ = rows[-1]
        # sent_at is non-null for every status=sent row by pipeline
        # construction; assert keeps mypy honest about the invariant.
        assert last_delivery.sent_at is not None
        next_cursor = (last_delivery.sent_at, last_delivery.id)

    return items, next_cursor


async def get_unread_count(
    session: AsyncSession,
    recipient_id: UUID,
    *,
    channel: str | None = None,
) -> int:
    """Count unread sent deliveries for the badge counter.

    Unread = status=sent AND read_at IS NULL. `channel` scopes the
    count (the inbox badge counts in_app only -- a telegram delivery
    is never "read" and its read_at stays NULL forever; counting it
    would inflate the badge permanently). None = all channels
    (service-level generality).
    """
    conditions = [
        NotificationDelivery.recipient_id == recipient_id,
        NotificationDelivery.status == DeliveryStatus.SENT,
        NotificationDelivery.read_at.is_(None),
    ]
    if channel is not None:
        conditions.append(NotificationDelivery.channel == channel)
    stmt = (
        select(func.count())
        .select_from(NotificationDelivery)
        .where(*conditions)
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
    *,
    channel: str | None = None,
) -> int:
    """Mark all sent deliveries as read for a recipient.

    Only updates deliveries with status=sent AND read_at IS NULL.
    `channel` scopes the update (the inbox read-all touches in_app
    only); None = all channels.

    Returns:
        Number of deliveries marked as read.
    """
    conditions = [
        NotificationDelivery.recipient_id == recipient_id,
        NotificationDelivery.status == DeliveryStatus.SENT,
        NotificationDelivery.read_at.is_(None),
    ]
    if channel is not None:
        conditions.append(NotificationDelivery.channel == channel)
    stmt = (
        update(NotificationDelivery)
        .where(*conditions)
        .values(read_at=datetime.now(UTC))
    )
    result = await session.execute(stmt)
    await session.flush()
    marked: int = result.rowcount  # type: ignore[attr-defined]
    return marked
