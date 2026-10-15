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
#   - asyncio.wait_for timeout on formatter.deliver(), the job's lane's
#     (P4-3, settings.lane_deliver_timeout)
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
from typing import Any, cast
from uuid import UUID

import structlog
from sqlalchemy import any_, delete, exists, func, or_, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import Recipient
from app.audience.prefs import muted_recipient_ids
from app.audience.schedule import recipient_deferred_until
from app.core.config import settings
from app.core.constants import MAX_LLM_LETTER_CHARS
from app.core.exceptions import NotFoundError, ValidationError
from app.core.lanes import Lane
from app.core.sanitize import sanitize_text, sanitized_traceback
from app.engine import journal
from app.engine.constants import (
    ChannelAnswer,
    DeliveryChannel,
    DeliveryStatus,
    FailureClass,
    IntakeOutcomeClass,
    JournalStep,
    NotificationStatus,
    TargetType,
    WaitReason,
)
from app.engine.formatters import (
    ChannelFormatter,
    ConfigurationError,
    LLMReply,
    MessageRejectedError,
    ModelChannel,
    NoAddressError,
    PermanentDeliveryError,
    RateLimitedError,
    get_formatter,
    get_model_channel,
)
from app.engine.models import (
    IntakeOutcome,
    LLMAnswer,
    LLMCall,
    Notification,
    NotificationDelivery,
    NotificationTransition,
    PushOutbox,
)
from app.engine.resolver import resolve_targets
from app.engine.wake import signal_lane
from app.profile.loader import is_llm_route
from app.profile.registry import Decided, Layer, RecipientSchedule, registry

logger = structlog.get_logger()

# Valid infrastructure enum values for input validation.
_VALID_TARGET_TYPES = frozenset(e.value for e in TargetType)

# The one unique index that means "this key is taken" (migration 0012).
IDEMPOTENCY_INDEX_NAME = "uq_notifications_idempotency_key"

# Phase 3a item 7: 429 jitter as a FRACTION of the honored wait
# (uniform(0, fraction) x honored, one-sided -- see the deferral
# block in _process_single_notification and fix D note in config.py).
# 0.5 spreads one burst's herd over half its own wait window: wide
# enough to decorrelate, still the same order as the server's ask.
_RATE_LIMIT_JITTER_MAX_FRACTION = 0.5

# Concurrent formatter.deliver() calls of one job, by the job's lane
# (P4-1, spec §8.2): each lane's own slots, which no other lane can
# take. One semaphore per job IS one per lane: a lane's loop attempts
# its jobs one after another (app/engine/processor.py process_lane), so
# at most one job of a lane is in flight in a process. Constants, like
# the one number they replaced, because they share the worker's
# connection pool: every call writes the channel's answer in its own
# session while it holds its slot (journal.record_channel_answer), so
# the sum of these, plus one attempt session per lane and the
# maintenance loop's session, must fit the pool
# (app/core/database.py POOLS, the worker's) -- a call that
# waits for a connection widens the accepted window of
# record_channel_answer. tests/test_lanes.py holds the sum. The wake
# listener's connection is not in it: it has its own engine, outside
# the pool (app/engine/wake.py).
#   normal 20 -- the number every job had before lanes: the default lane
#                behaves exactly as the one queue did;
#   interactive, critical 2 -- a person or a payment is one recipient,
#                rarely more; two slots keep a second one from queueing;
#   bulk 1 -- a send that may wait; the slot it cannot exceed is what
#                keeps it off everyone else's.
LANE_CONCURRENCY: dict[Lane, int] = {
    Lane.INTERACTIVE: 2,
    Lane.CRITICAL: 2,
    Lane.NORMAL: 20,
    Lane.BULK: 1,
}


def delivery_portion(lane: Lane) -> int:
    """The deliveries one attempt of a `lane` job takes (H7): one round
    of the lane's slots.

    An attempt holds its job's row and a pooled connection, and keeps
    the change feed's xmin, for as long as it runs (app/engine/changes.py
    KNOWN CEILING P2-3) -- and expiry, cancellation and forgetting of the
    job wait for it. Over every due delivery of a job at once that is
    ceil(N / slots) channel calls end to end: a bulk send to a thousand
    people, one slot, was a thousand calls in one transaction. Over one
    round it is ONE call's time, the lane's deliver timeout at most. The
    rest of the job goes in the lane's next passes, each its own
    attempt; a pass whose portion was full wakes its own loop
    (app/worker.py), so there is no sleep between portions. The number
    is the slots, not one of its own: a round fills them once, and a
    larger portion would only lengthen the transaction again.
    """
    return LANE_CONCURRENCY[lane]


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

    An ACCEPTED job wakes the loops of its lane when the caller's
    transaction commits (app/engine/wake.py, spec §8.4); DUPLICATE and
    CONFLICT created nothing and wake nothing.
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
    await signal_lane(session, notification.lane)
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
    """Every recorded non-acceptance under a key, oldest first -- the
    `intake` of reading by key (read_job_by_key, P2-2)."""
    rows = await session.execute(
        select(IntakeOutcome)
        .where(IntakeOutcome.idempotency_key == idempotency_key)
        .order_by(IntakeOutcome.received_at, IntakeOutcome.id)
    )
    return list(rows.scalars().all())


# -----------------------------------------------------------------------------
# Reading a job by its key (P2-2, spec §6.2 / §6.6 / §7.1)
# -----------------------------------------------------------------------------
#
# The product's last link: its own record -> the key -> the job in comms
# -> the steps. READ ONLY: app/api/jobs.py runs every reader below in a
# read-only REPEATABLE READ transaction, so the summary's three queries
# see one snapshot and nothing here can write. A fixed number of
# queries per read -- never one per delivery.
#
# NEVER the letter: these return ORM rows, and the wire form
# (app/api/jobs.py) is a closed field set that names no title, body,
# action_data, channel_options or error_message (the last can carry the
# text of a comms exception -- service._deliver_single).


@dataclass(frozen=True)
class JobRead:
    """What a key answers: its non-acceptances, its job, the job's
    deliveries counted by (channel, status) -- and, for a job of the llm
    kind (P5-1), its call and its answer (None while there is none)."""

    intake: list[IntakeOutcome]
    job: Notification | None
    delivery_counts: list[tuple[str, str, int]]
    llm_call: LLMCall | None = None
    llm_answer: LLMAnswer | None = None


async def _job_by_key(
    session: AsyncSession, idempotency_key: str,
) -> Notification | None:
    return (
        await session.execute(
            select(Notification).where(
                Notification.idempotency_key == idempotency_key,
            )
        )
    ).scalar_one_or_none()


async def read_job_by_key(
    session: AsyncSession, idempotency_key: str,
) -> JobRead:
    """The summary under a key. NotFoundError when the key answers
    nothing at all -- never received, or past retention (the two are
    not told apart: telling them apart would need a write at cleanup)."""
    job = await _job_by_key(session, idempotency_key)
    intake = await intake_outcomes_for(session, idempotency_key)
    if job is None and not intake:
        raise NotFoundError("no job and no intake outcome under this key")
    counts: list[tuple[str, str, int]] = []
    if job is not None:
        rows = await session.execute(
            select(
                NotificationDelivery.channel,
                NotificationDelivery.status,
                func.count(),
            )
            .where(NotificationDelivery.notification_id == job.id)
            .group_by(NotificationDelivery.channel, NotificationDelivery.status)
            .order_by(NotificationDelivery.channel, NotificationDelivery.status)
        )
        counts = [(channel, status, int(n)) for channel, status, n in rows.all()]
    llm_call: LLMCall | None = None
    llm_answer: LLMAnswer | None = None
    if job is not None and is_llm_route(job.channels):
        # Two reads by primary key -- a fixed number per read.
        llm_call = await session.get(LLMCall, job.id)
        llm_answer = await session.get(LLMAnswer, job.id)
    return JobRead(
        intake=intake,
        job=job,
        delivery_counts=counts,
        llm_call=llm_call,
        llm_answer=llm_answer,
    )


async def job_id_by_key(session: AsyncSession, idempotency_key: str) -> UUID:
    """The accepted job under a key, or NotFoundError -- a key with only
    intake outcomes has no deliveries and no path."""
    job = await _job_by_key(session, idempotency_key)
    if job is None:
        raise NotFoundError("no accepted job under this key")
    return job.id


async def list_job_deliveries(
    session: AsyncSession,
    notification_id: UUID,
    *,
    limit: int,
    cursor: tuple[datetime, UUID] | None,
) -> tuple[list[NotificationDelivery], tuple[datetime, UUID] | None]:
    """One page of a job's deliveries, oldest first, keyset
    (created_at, id).

    KNOWN CEILING (acknowledged by design -- P2-2, delivery page cost):
      1. Mechanics: a page is found through the notification_id index
         and a top-N sort of ALL of the job's deliveries: the cost of
         one page grows with the size of the job (measured: 0.47 ms at
         1 000 deliveries, 5.1 ms at 10 000; reading every page of a
         job is quadratic in its size).
      2. Status: acknowledged by design.
      3. Backlog ref: none -- at the measured sizes a page is cheap and
         reading by key is a diagnostic call, not a feed.
      4. Promotion trigger (observable): a group job whose deliveries
         page takes longer than 50 ms, or the product paging a job's
         deliveries as a routine (not diagnostic) read.
      5. Agreed fix: an index (notification_id, created_at, id) in a new
         migration -- the page becomes an index range scan.
      6. Rejected: ordering by id alone (a uuid: no meaningful order,
         and the (timestamp, uuid) codec is the deliveries' kind of
         key); adding the index now -- every delivery insert of every
         job would pay for a diagnostic read, and the measurement says
         it is not needed yet.
    """
    statement = select(NotificationDelivery).where(
        NotificationDelivery.notification_id == notification_id,
    )
    if cursor is not None:
        statement = statement.where(
            tuple_(NotificationDelivery.created_at, NotificationDelivery.id)
            > cursor
        )
    rows = list(
        (
            await session.execute(
                statement.order_by(
                    NotificationDelivery.created_at, NotificationDelivery.id,
                ).limit(limit + 1)
            )
        ).scalars()
    )
    if len(rows) <= limit:
        return rows, None
    last = rows[limit - 1]
    return rows[:limit], (last.created_at, last.id)


async def list_job_path(
    session: AsyncSession,
    notification_id: UUID,
    *,
    limit: int,
    cursor: int | None,
) -> tuple[list[NotificationTransition], int | None]:
    """One page of a job's journal, in order (the journal's identity;
    ix_transitions_path). A job accepted before the journal existed
    (migration 0017) has a partial or empty path."""
    statement = select(NotificationTransition).where(
        NotificationTransition.notification_id == notification_id,
    )
    if cursor is not None:
        statement = statement.where(NotificationTransition.id > cursor)
    rows = list(
        (
            await session.execute(
                statement.order_by(NotificationTransition.id).limit(limit + 1)
            )
        ).scalars()
    )
    if len(rows) <= limit:
        return rows, None
    return rows[:limit], rows[limit - 1].id


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


def _letter_by_kind(
    type: str,
    record: Any,
    *,
    title: str | None,
    body: str | None,
    target_type: str | None,
    target_value: str | None,
    action_data: dict[str, Any] | None,
    llm: dict[str, Any] | None,
) -> None:
    """Refuse a request whose fields do not match its TYPE's kind (P5-1).

    A type for people takes an address and a letter for people -- target
    and title and body -- and no model's letter. A type of the llm kind
    (channels exactly [llm], app/profile/loader.py) takes the model's
    letter and nothing a person would need: it has no audience. Each
    refusal names the fields, never a value.
    """
    if is_llm_route(record.value("channels")):
        if llm is None:
            raise ValidationError(
                f"type {type} asks a model: the request needs the field "
                f"'llm' (its letter)"
            )
        present = [
            name for name, value in (
                ("target_type", target_type),
                ("target_value", target_value),
                ("title", title),
                ("body", body),
                ("action_data", action_data),
            )
            if value is not None
        ]
        if present:
            raise ValidationError(
                f"type {type} asks a model and has no audience: the "
                f"request may not carry {', '.join(present)}"
            )
        return
    if llm is not None:
        raise ValidationError(
            f"type {type} is not an llm type: the request may not carry "
            f"'llm'"
        )
    missing = [
        name for name, value in (
            ("target_type", target_type),
            ("target_value", target_value),
            ("title", title),
            ("body", body),
        )
        if value is None
    ]
    if missing:
        raise ValidationError(
            f"type {type} is delivered to people: the request needs "
            f"{', '.join(missing)}"
        )


def llm_letter_encoding(letter: Mapping[str, Any]) -> str:
    """The canonical JSON of an llm letter -- what its size is measured
    on and its hash taken over (the encoding of canonical_fingerprint)."""
    return json.dumps(
        letter,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


async def create_notification(
    session: AsyncSession,
    *,
    type: str,
    title: str | None,
    body: str | None,
    target_type: str | None,
    target_value: str | None,
    idempotency_key: str,
    fingerprint: str,
    action_data: dict[str, Any] | None = None,
    llm: dict[str, Any] | None = None,
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
            snapshotted onto the row -- the caller never names one; so
            are its category, its push_on (what it pushes back), its
            recipient_schedule (whether it waits for the allowed
            periods), its lane (which of the worker's loops picks it,
            with whose budget) and its transport retry
            (retry_max_attempts, retry_backoff_seconds).
        title: Stored fallback title (the letter). None exactly for a
            type of the llm kind (P5-1), like body and the target.
        body: Stored fallback body (the letter).
        target_type: TargetType value (user, group, all).
        target_value: Bare target specifier ("<uuid>", "<group_key>", "*").
        idempotency_key: The request's key -- required on every path.
        fingerprint: Digest of the request's bytes (see above).
        action_data: Optional JSONB letter (deep-link intent + template
            variables).
        llm: The model's letter -- required for a type of the llm kind
            and refused for any other (P5-1). Kept in llm_calls until the
            job's outcome, with its hash and the type's key sets.
        scheduled_at: "Not before" -- when the job becomes deliverable.
            Defaults to now.
        expiry_at: The envelope's expiry. Wins over the profile's
            expires_after, which counts from scheduled_at.
        correlation: The product's own reference, stored untouched.

    Returns:
        The created Notification (flushed, not committed).

    Raises:
        ValidationError: The request cannot be accepted -- undeclared
            type, a body longer than the type's max_body_chars, invalid
            target_type, an expiry already passed or not after
            scheduled_at.
    """
    # -- The profile record IS the registration (every installed type
    # has one; a type without a record is not declared). --
    record = registry.record_of(type)
    if record is None:
        raise ValidationError(
            f"Unregistered notification type: {type}. "
            f"Registered: {', '.join(sorted(registry.registered_types()))}"
        )

    _letter_by_kind(
        type, record,
        title=title, body=body,
        target_type=target_type, target_value=target_value,
        action_data=action_data, llm=llm,
    )

    # -- The size limit (H1, spec §5.9): its LENGTH, never its content.
    # Refused here, not at delivery: comms does not accept a job it
    # already knows it will not deliver. The product reads the refusal
    # and its reason under the key (intake_outcomes). A letter for
    # people by its type's max_body_chars; a model's letter by the one
    # ceiling of comms (P5-1). --
    llm_encoded: str | None = None
    if llm is not None:
        llm_encoded = llm_letter_encoding(llm)
        if len(llm_encoded) > MAX_LLM_LETTER_CHARS:
            raise ValidationError(
                f"llm letter has {len(llm_encoded)} characters (canonical "
                f"JSON); the limit is {MAX_LLM_LETTER_CHARS}"
            )
    elif body is not None:
        max_body_chars: int = record.value("max_body_chars")
        if len(body) > max_body_chars:
            raise ValidationError(
                f"body has {len(body)} characters; type {type} allows at "
                f"most {max_body_chars} (max_body_chars)"
            )

    if target_type is not None and target_type not in _VALID_TARGET_TYPES:
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
    push_on = record.value("push_on")
    recipient_schedule = record.value("recipient_schedule")
    lane = record.value("lane")
    retry_max_attempts = record.value("retry_max_attempts")
    retry_backoff_seconds = record.value("retry_backoff_seconds")

    notification = Notification(
        type=type,
        title=title,
        body=body,
        target_type=target_type,
        target_value=target_value,
        action_data=action_data,
        channels=channels,
        category=category,
        push_on=push_on,
        recipient_schedule=recipient_schedule,
        lane=lane,
        retry_max_attempts=retry_max_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
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
    if llm is not None and llm_encoded is not None:
        # The letter, its hash and the type's key sets and answer term,
        # AT INTAKE (P5-1): a snapshot like `channels`.
        session.add(LLMCall(
            notification_id=notification.id,
            sets=list(record.value("llm_sets")),
            letter=llm,
            letter_hash=hashlib.sha256(
                llm_encoded.encode("utf-8"),
            ).hexdigest(),
            answer_keep_seconds=int(
                record.value("answer_keep").total_seconds(),
            ),
        ))
        await session.flush()
    # The birth of the job (null -> pending) is its first transition;
    # inside accept_notification's savepoint, so a key collision leaves
    # no row.
    journal.record_job(session, notification, JournalStep.INTAKE)

    logger.info(
        "notification_created",
        notification_id=str(notification.id),
        type=type,
        target=(
            f"{target_type}:{target_value}" if target_type is not None
            else None
        ),
        channels=channels,
        lane=lane,
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

    # -- A job of the llm kind (P5-1) has no audience: ONE delivery, on
    # its one channel, without a recipient -- no resolve over the sync
    # projection and no mute gate, which ask about people. --
    if is_llm_route(notification.channels):
        delivery = NotificationDelivery(
            notification_id=notification.id,
            recipient_id=None,
            channel=DeliveryChannel.LLM,
            status=DeliveryStatus.PENDING,
        )
        session.add(delivery)
        journal.record_delivery(
            session, notification, delivery, JournalStep.RESOLVE,
        )
        notification.status = NotificationStatus.PROCESSING
        journal.record_job(session, notification, JournalStep.RESOLVE)
        await session.flush()
        logger.info(
            "notification_resolved",
            notification_id=str(notification.id),
            recipients=0,
            channels=1,
            deliveries=1,
        )
        return [delivery]

    # -- Resolve target recipients over the sync projection --
    # A job for people has its address (ck_notifications_letter_by_kind).
    assert notification.target_type is not None
    assert notification.target_value is not None
    recipient_ids = await resolve_targets(
        session,
        notification.target_type,
        notification.target_value,
    )

    if not recipient_ids:
        # Nobody to deliver to: the product's sync, not comms, is what
        # to look at -- and not the recipients' choice either.
        notification.status = NotificationStatus.NO_RECIPIENTS
        journal.record_job(session, notification, JournalStep.RESOLVE)
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
            # Each dropped recipient is a gate row (spec §6.4: "suppressed
            # by preferences, by this category"): no delivery will exist
            # to carry the answer to "why did R not get it".
            for recipient_id in recipient_ids:
                if recipient_id in muted:
                    journal.record_gate(
                        session, notification, recipient_id, category,
                    )
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
            journal.record_job(session, notification, JournalStep.RESOLVE)
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
            journal.record_delivery(
                session, notification, delivery, JournalStep.RESOLVE,
            )

    notification.status = NotificationStatus.PROCESSING
    journal.record_job(session, notification, JournalStep.RESOLVE)
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
) -> bool:
    """Deliver pending deliveries for a notification via formatters --
    ONE PORTION of them (H7, delivery_portion): the due ones, in id
    order, at most one round of the lane's slots. Returns True when the
    portion was full, i.e. more of the job may be due: the caller wakes
    the lane's loop for the next portion rather than wait its interval.

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
      DEFERRED, not sent -- unless the job's type declared
      recipient_schedule: bypass (H3), which skips this gate and no
      other -- next_retry_at is set to the next period's
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
    - Concurrent delivery via asyncio.gather + Semaphore, sized by the
      job's lane (LANE_CONCURRENCY).
    - asyncio.wait_for with timeout per formatter call: the job's
      lane's (P4-3), around the call and not around the wait for a slot.
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

    Returns:
        Whether the portion was full.
    """
    now = datetime.now(UTC)
    portion = delivery_portion(Lane(notification.lane))
    stmt = (
        select(NotificationDelivery)
        .where(
            NotificationDelivery.notification_id == notification.id,
            NotificationDelivery.status == DeliveryStatus.PENDING,
            or_(
                NotificationDelivery.next_retry_at.is_(None),
                NotificationDelivery.next_retry_at <= now,
            ),
        )
        .order_by(NotificationDelivery.id)
        .limit(portion)
    )
    result = await session.execute(stmt)
    deliveries = list(result.scalars().all())

    if not deliveries:
        return False

    # -- Batch load recipients for all pending deliveries. The llm
    # delivery has none (P5-1): its job's call is loaded instead. --
    recipient_ids: set[UUID | None] = {d.recipient_id for d in deliveries}
    people: set[UUID] = {r for r in recipient_ids if r is not None}
    recipients_by_id: dict[UUID, Recipient] = {}
    if people:
        recipient_result = await session.execute(
            select(Recipient).where(Recipient.id.in_(people)),
        )
        recipients_by_id = {r.id: r for r in recipient_result.scalars().all()}
    call: LLMCall | None = None
    if None in recipient_ids:
        call = await session.get(LLMCall, notification.id)

    # -- Accepted by the channel in an attempt that did not commit: the
    # letter is out. Read once per pass, by (recipient, channel). --
    # Narrowed to the portion's recipients (H7): the job's whole journal
    # read once per portion would grow with every portion sent.
    accepted = await journal.accepted_answers(
        session, [notification.id], recipient_ids=recipient_ids,
    )

    # -- Late-mute re-check: one batched probe per pass, over the
    # category snapshotted at intake (F1.3) --
    category = notification.category
    muted: set[UUID] = set()
    if category is not None and people:
        muted = await muted_recipient_ids(session, category, list(people))

    # -- Concurrent delivery via gather + the lane's slots (P4-1) --
    semaphore = asyncio.Semaphore(
        LANE_CONCURRENCY[Lane(notification.lane)],
    )
    tasks = []

    for delivery in deliveries:
        # -- Leaving the wait: the gate that held this delivery has
        # opened; its reason goes with it. A gate below may set both
        # again. --
        delivery.next_retry_at = None
        delivery.wait_reason = None

        # -- Already accepted (KNOWN CEILING T12 closed): an attempt
        # handed this letter to the channel and then tore before its
        # commit. The channel's answer was journaled in its own
        # transaction; the delivery is closed SENT at the moment the
        # channel took it, and the channel is NOT called again. Checked
        # before every gate: a letter that went out is not un-sent by a
        # deactivation, a mute or a schedule that came after it. --
        accepted_at = accepted.get(
            (notification.id, delivery.recipient_id, delivery.channel),
        )
        if accepted_at is not None:
            _close_accepted(delivery, accepted_at)
            journal.record_delivery(
                session, notification, delivery, JournalStep.DELIVER,
            )
            logger.info(
                "delivery_closed_from_journal",
                delivery_id=str(delivery.id),
                accepted_at=accepted_at.isoformat(),
            )
            continue

        # -- The llm delivery (P5-1): no recipient, so none of the gates
        # below, which ask about a person. Its job's call carries the
        # letter and the sets (ck_deliveries_recipient_by_channel: the
        # llm delivery and the recipient-less one are the same). --
        if delivery.recipient_id is None:
            assert call is not None and call.letter is not None
            tasks.append(
                _deliver_single(
                    semaphore, get_model_channel(), notification, delivery,
                    None, call,
                )
            )
            continue

        # Every delivery for a person has its recipient row:
        # recipient_id is a foreign key with ON DELETE CASCADE, so
        # deleting a recipient deletes its deliveries. (A "recipient not
        # found -> FAILED" branch stood here for a state that cannot
        # exist; removed in F1.3 rather than given a failure class.)
        recipient = recipients_by_id[delivery.recipient_id]

        # -- Late activity gate (F1.4): deactivated or deleted by the
        # product after resolve -> close out, no send. Checked first: a
        # person the product removed is not asked about their mutes. --
        if not recipient.active:
            delivery.status = DeliveryStatus.RECIPIENT_INACTIVE
            journal.record_delivery(
                session, notification, delivery, JournalStep.DELIVER,
            )
            logger.info(
                "delivery_recipient_inactive",
                delivery_id=str(delivery.id),
                recipient_id=str(delivery.recipient_id),
            )
            continue

        # -- Late-mute gate: muted while gated -> close out, no send --
        if delivery.recipient_id in muted:
            delivery.status = DeliveryStatus.SUPPRESSED
            journal.record_delivery(
                session, notification, delivery, JournalStep.DELIVER,
                category=category,
            )
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
        # and when there is no schedule at all. A job whose type
        # declared recipient_schedule: bypass (snapshotted at intake,
        # H3) does not wait for them -- a code someone is waiting for
        # right now goes at night too. The mute gates above are not
        # this gate and do not read the field.
        # The polarity errs toward the recipient (H5): the schedule is
        # asked for EVERY value but bypass, so a value this gate does
        # not know -- the enum grown, a row it was never written for --
        # waits for the recipient's period instead of going at night.
        quiet_until = (
            recipient_deferred_until(recipient, now)
            if notification.recipient_schedule != RecipientSchedule.BYPASS
            else None
        )
        if quiet_until is not None:
            delivery.next_retry_at = quiet_until
            delivery.wait_reason = WaitReason.RECIPIENT_SCHEDULE
            journal.record_delivery(
                session, notification, delivery, JournalStep.DELIVER,
            )
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
                    _apply_transient_failure(notification, delivery, outcome.error)
            elif outcome.success:
                delivery.attempts += 1
                delivery.status = DeliveryStatus.SENT
                delivery.sent_at = datetime.now(UTC)
            else:
                _apply_transient_failure(notification, delivery, outcome.error)
            journal.record_delivery(
                session, notification, delivery, JournalStep.DELIVER,
            )

    await session.flush()
    return len(deliveries) == portion


def _close_accepted(delivery: NotificationDelivery, accepted_at: datetime) -> None:
    """Close a waiting delivery the channel already accepted: SENT at
    the moment it was accepted, counted as the attempt that sent it --
    the state the rolled-back attempt would have committed."""
    delivery.next_retry_at = None
    delivery.wait_reason = None
    delivery.attempts += 1
    delivery.status = DeliveryStatus.SENT
    delivery.sent_at = accepted_at


def _apply_transient_failure(
    notification: Notification,
    delivery: NotificationDelivery,
    error: str | None,
) -> None:
    """Apply one transient failure: burn an attempt, gate or fail.

    Shared by the regular transient path and the exhausted-budget 429
    path (a 429 past the deferral budget behaves exactly like any
    other transient error).

    The ceiling and the backoff base are the JOB's -- its type's, as
    snapshotted at intake (H1, spec §9.7); the cap is the deploy's.
    """
    delivery.attempts += 1
    delivery.error_message = error
    if delivery.attempts >= notification.retry_max_attempts:
        delivery.status = DeliveryStatus.FAILED
        delivery.failure_class = FailureClass.TRANSIENT_EXHAUSTED
    else:
        # Exponential backoff gate: base * 2**(attempts-1),
        # capped. Without it all attempts burned within one
        # poll interval (review 1.1).
        backoff_seconds = min(
            notification.retry_backoff_seconds
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
    formatter: ChannelFormatter | ModelChannel,
    notification: Notification,
    delivery: NotificationDelivery,
    recipient: Recipient | None,
    call: LLMCall | None = None,
) -> tuple[NotificationDelivery, _DeliveryOutcome]:
    """Deliver a single notification with concurrency control.

    THE LLM DELIVERY (P5-1) runs here too -- this is the one place a
    channel exception becomes a failure class. With `call` the channel
    is ASKED (ModelChannel.ask, the letter and the job's sets) and its
    answer is stored with the journal row (journal.record_channel_answer)
    -- a delivery is SENT only with its answer stored. Its bound is the
    channel's own, LLM_DELIVER_TIMEOUT_SECONDS for each set of the job's
    list: the lanes' 30 is email's and telegram's, and a model writes for
    longer (app/core/channels.py own_deliver_timeout).

    KNOWN CEILING (acknowledged by design -- P5-1, the head of a lane):
      1. Mechanics: a lane attempts its jobs one after another
         (app/engine/processor.py process_lane), and an llm call may run
         up to LLM_DELIVER_TIMEOUT_SECONDS for each set of its list. Every
         job of the same lane behind it -- a login code by email, if the
         profile put both in one lane -- waits that long.
      2. Status: acknowledged by design.
      3. Backlog ref: none -- parallel jobs inside a lane are a separate
         task (the owner's answer at the P5-1 gate).
      4. Promotion trigger (observable): the worker's startup line
         `lane_mixes_llm` (app/worker.py), which names every lane that
         carries llm types beside types for people; or a
         notifications_processed line of such a lane whose duration_ms
         is above its poll interval.
      5. Agreed fix: jobs of a lane attempted concurrently, within the
         lane's slots.
      6. Rejected: refusing a lane that mixes the two at startup (a red
         start on a valid profile; the choice of lane is the product's);
         raising the lanes' timeouts to the model's (the interactive
         email would wait minutes for a dead provider); a lane of comms'
         own for llm (the lanes are a closed set, spec §8).
    deploy/INTEGRATION.md tells the product to keep llm types in a lane
    without types for people.

    Runs formatter.deliver() under semaphore with the timeout of the
    job's lane (P4-3, spec §8.5) -- inside the slot, so a call waiting
    for one is not timed; startup holds every lane's timeout above the
    client timeout of every live channel (app/core/config.py
    deliver_timeout_problems), so the client's own error, which still
    tells a call that never reached the provider from one left without
    an answer, comes first. Does NOT
    touch the attempt's SQLAlchemy session -- only the external call and
    the journal: whatever the channel answered is written in its OWN
    transaction right here, before the attempt goes on
    (journal.record_channel_answer). Returns (delivery, outcome) for the
    caller to apply to the attempt's session.
    """
    llm_answer: LLMAnswer | None = None
    if call is None:
        timeout = settings.lane_deliver_timeout(Lane(notification.lane))
    else:
        timeout = settings.llm_deliver_timeout_seconds * len(call.sets)
    async with semaphore:
        try:
            # The two shapes of a channel: a person's channel is given a
            # recipient, the model channel a call -- the caller passes
            # exactly one of the two (deliver_notification).
            if call is None:
                assert recipient is not None
                success = await asyncio.wait_for(
                    cast(ChannelFormatter, formatter).deliver(
                        notification, delivery, recipient,
                    ),
                    timeout=timeout,
                )
            else:
                assert call.letter is not None
                reply = await asyncio.wait_for(
                    cast(ModelChannel, formatter).ask(call.letter, call.sets),
                    timeout=timeout,
                )
                llm_answer = _answer_row(call, reply)
                success = True
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
            answer = journal.answer_of(exc, failure_class)
            outcome = _DeliveryOutcome(
                failure_class=failure_class,
                error=journal.answer_text(answer, exc),
            )
        except RateLimitedError as exc:
            # Deliberately no log here: whether this becomes a deferral
            # or degrades to a transient failure is decided in the
            # apply loop (it owns the budget counter) -- that decision
            # log is the valuable one, and doubling it would be noise.
            answer = journal.answer_of(exc)
            outcome = _DeliveryOutcome(
                error=journal.answer_text(answer, exc),
                retry_after=exc.retry_after,
            )
        except TimeoutError as exc:
            logger.warning(
                "delivery_timeout",
                delivery_id=str(delivery.id),
                channel=delivery.channel,
                timeout=timeout,
            )
            outcome = _DeliveryOutcome(
                error=f"Timeout after {timeout}s",
            )
            answer = journal.answer_of(exc)
        except Exception as exc:
            # Not logger.exception(): the renderer would print the chain
            # raw, and the telegram network error carries the bot token
            # in its text and in its __cause__. Same event, same level,
            # same `exception` key -- redacted (app/core/sanitize.py).
            logger.error(
                "delivery_error",
                delivery_id=str(delivery.id),
                channel=delivery.channel,
                exception=sanitized_traceback(exc),
            )
            # error_message by the journal's rule (H8): this is comms' own
            # exception, so its class and place -- never its text.
            answer = journal.answer_of(exc)
            outcome = _DeliveryOutcome(
                error=journal.answer_text(answer, exc),
            )
        else:
            outcome = _DeliveryOutcome(success=success)
            answer = (
                journal.ACCEPTED
                if success
                else journal.Answer(outcome=ChannelAnswer.TRANSIENT)
            )
        stored = await journal.record_channel_answer(
            delivery, answer, llm_answer=llm_answer,
        )
        if llm_answer is not None and not stored:
            # The model answered and the answer could not be kept: not a
            # SENT without its answer -- transient, asked again.
            outcome = _DeliveryOutcome(
                error="the model answered, but the answer could not be "
                "stored (journal_channel_answer_error in the log)",
            )
        return delivery, outcome


def _answer_row(call: LLMCall, reply: LLMReply) -> LLMAnswer:
    """The answer as it is kept: who answered, when, until when."""
    answered_at = datetime.now(UTC)
    return LLMAnswer(
        notification_id=call.notification_id,
        body=reply.body,
        answered_set=reply.set_name,
        answered_provider=reply.provider,
        answered_model=reply.model,
        answered_at=answered_at,
        keep_until=answered_at + timedelta(seconds=call.answer_keep_seconds),
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
# below): the waiting deliveries get the outcome (or SENT, when the
# channel already accepted them), then the job is folded by the same
# rule. A job half of which went out before its
# expiry is PARTIAL_SENT, not EXPIRED. Only a job with no deliveries
# yet (PENDING, not resolved) takes EXPIRED / CANCELLED directly.
#
# IN FLIGHT: an attempt runs inside the worker's transaction under a
# row lock on the notification (processor.py, FOR NO KEY UPDATE -- the
# lock rule is written at the selection). Expiry, cancellation and
# forgetting lock the same row FOR UPDATE, waiting, not skipping; the
# two modes conflict, so these apply to the state AFTER the attempt:
# what was sent stays sent, what is still waiting is closed -- and a
# waiting delivery the channel already accepted is closed SENT, not
# with the closing outcome (journal.accepted_answers), because the
# letter went out.
#
# THE ORDER OF ROW LOCKS (H7): a path that locks MORE THAN ONE job row
# locks them in ascending id -- ORDER BY notifications.id under the FOR
# UPDATE; Postgres sorts before it locks (LockRows over Sort), so the
# rows are taken in that order. Two such paths over overlapping sets
# then queue on the first row they share and never hold each other's
# next one: no 40P01. It matters because a deadlock is not retried:
# asyncpg's DeadlockDetectedError reaches SQLAlchemy as a bare
# DBAPIError, not an OperationalError, so the consumer -- where
# cancellation and forgetting arrive -- dead-letters the event at once
# (app/transport/consumer.py), and the reminders stay or the person is
# not forgotten. The paths: close_notifications (expiry, cancellation)
# and withdraw_recipient (forgetting), which locks EVERY job it will
# write, finished ones included, before its mass updates. The batch
# deletes of retention and of the expiry cleanup take their jobs in the
# same order, through the primary key (_id_in_batch: id = ANY(ARRAY),
# whose keys an index scan walks sorted), and their cascade reaches a
# job's deliveries and journal only after the job -- so forgetting and
# a cleanup queue on a job row, never on its children
# (tests/test_lock_order.py).


async def rollup_notification(
    session: AsyncSession,
    notification: Notification,
    step: JournalStep = JournalStep.ROLLUP,
) -> None:
    """Fold the deliveries' outcomes into the job's (see THE FOLD).

    Acts on PROCESSING only: every other status is either not resolved
    yet or already an outcome, and an outcome is never re-decided.

    Args:
        session: Active DB session (caller commits).
        notification: The notification to roll up.
        step: The journal step the fold is recorded under -- the
            pipeline's rollup, or the closing path that called it.
    """
    if notification.status != NotificationStatus.PROCESSING:
        return

    # A delivery still waiting decides alone (H9): the fold of any set
    # that holds a PENDING is "still open" (_fold), so the set is read
    # only once none waits. After every portion but the last (H7) one
    # probe on ix_notification_deliveries_pending (migration 0025) --
    # the job's waiting deliveries, nothing else -- instead of a read of
    # every delivery of the job: the square H7 took off accepted_answers.
    still_waiting = await session.scalar(
        select(
            exists().where(
                NotificationDelivery.notification_id == notification.id,
                NotificationDelivery.status == DeliveryStatus.PENDING,
            )
        )
    )
    if still_waiting:
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
    journal.record_job(session, notification, step)
    await forget_llm_letter(session, notification)
    await session.flush()
    logger.info(
        "notification_rollup",
        notification_id=str(notification.id),
        status=notification.status,
    )


async def forget_llm_letter(
    session: AsyncSession, notification: Notification,
) -> None:
    """Clear the model's letter of a job that reached its outcome (P5-1,
    spec §14.12 item 5): the letter was only ever needed to ask; its
    hash stays (llm_calls.letter_hash). A no-op for a job for people,
    and for one still active.

    Called beside the three transitions that give an llm job its
    outcome: the fold (rollup_notification -- the attempt, expiry and
    cancellation of a resolved job, the pipeline's ceiling), the direct
    close of a job not resolved yet (close_notifications), and the
    ceiling's close of a job whose resolve failed
    (app/engine/processor.py _close_at_ceiling). The caller holds the
    job's row lock. The row written is llm_calls, whose other writers are
    intake and the cascade -- and its foreign key is not changed, so the
    UPDATE takes no lock on the job.
    """
    if not is_llm_route(notification.channels):
        return
    if notification.status in (
        NotificationStatus.PENDING, NotificationStatus.PROCESSING,
    ):
        return
    await session.execute(
        update(LLMCall)
        .where(
            LLMCall.notification_id == notification.id,
            LLMCall.letter.is_not(None),
        )
        .values(letter=None)
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

# The journal step each closing outcome is recorded under.
_CLOSING_STEP = {
    NotificationStatus.EXPIRED: JournalStep.EXPIRE,
    NotificationStatus.CANCELLED: JournalStep.CANCEL,
}

_ACTIVE_STATUSES = (NotificationStatus.PENDING, NotificationStatus.PROCESSING)


async def withdraw_recipient(session: AsyncSession, recipient_id: UUID) -> int:
    """Close a deleted recipient's deliveries (F1.4, forgetting).

    Every delivery of theirs still waiting takes RECIPIENT_INACTIVE
    (wait fields cleared) -- or SENT, when the channel already accepted
    it (close_waiting_deliveries) -- and each affected job is folded
    (THE FOLD).
    The jobs are locked FOR UPDATE and the lock is WAITED for, like
    expiry and cancellation: an attempt in flight finishes first. EVERY
    job the mass updates below reach is locked -- finished ones too, in
    ascending id (THE ORDER OF ROW LOCKS): those updates touch the
    deliveries and journal rows of finished jobs that retention and the
    expiry cleanup delete in batches, and two writers of the same rows
    that took them in two orders would deadlock. Every
    delivery of theirs, finished or not, loses its error_message -- a
    provider's words may quote the address being forgotten -- and every
    journal row of theirs loses its provider_text, for the same reason.

    Returns the number of deliveries closed.
    """
    touched = (
        await session.execute(
            select(Notification)
            .where(
                Notification.id.in_(
                    select(NotificationDelivery.notification_id).where(
                        NotificationDelivery.recipient_id == recipient_id,
                    )
                ),
            )
            .order_by(Notification.id)
            .with_for_update()
        )
    ).scalars().all()
    # Read under the locks: no attempt adds or moves a delivery of a
    # locked job.
    waiting_in = set(
        (
            await session.execute(
                select(NotificationDelivery.notification_id).where(
                    NotificationDelivery.recipient_id == recipient_id,
                    NotificationDelivery.status == DeliveryStatus.PENDING,
                )
            )
        ).scalars().all()
    )
    parents = [job for job in touched if job.id in waiting_in]
    closed = 0
    for notification in parents:
        closed += await close_waiting_deliveries(
            session,
            notification,
            DeliveryStatus.RECIPIENT_INACTIVE,
            JournalStep.WITHDRAW,
            NotificationDelivery.recipient_id == recipient_id,
        )
    await session.execute(
        update(NotificationDelivery)
        .where(NotificationDelivery.recipient_id == recipient_id)
        .values(error_message=None)
    )
    # THE ONE EDIT OF THE JOURNAL, and why it is allowed: forgetting
    # outweighs append-only. Append-only is a property of the design;
    # forgetting is an obligation to a person (spec §10.4). A provider's
    # words may quote the address being forgotten -- the reason
    # error_message is cleared just above -- and the journal keeps the
    # same words for the same recipient. Only that column, only this
    # recipient; every other field of the row stays, and so does the
    # path. The fence test names this function as the single exception.
    await session.execute(
        update(NotificationTransition)
        .where(
            NotificationTransition.recipient_id == recipient_id,
            NotificationTransition.provider_text.is_not(None),
        )
        .values(provider_text=None)
    )
    for notification in parents:
        await rollup_notification(session, notification, JournalStep.WITHDRAW)
    await session.flush()
    return closed


async def close_notifications(
    session: AsyncSession,
    condition: Any,
    outcome: NotificationStatus,
) -> int:
    """Expire or cancel every ACTIVE job matching `condition`.

    The waiting deliveries take the outcome (one the channel already
    accepted is closed SENT -- close_waiting_deliveries) and the job is
    folded (see THE FOLD); a job with no deliveries yet takes the
    outcome directly.
    A job already at an outcome is not touched -- that is the guard,
    and a second call is a zero-row no-op. Rows are locked FOR UPDATE
    and the lock is WAITED for: an attempt in flight finishes first --
    in ascending id (THE ORDER OF ROW LOCKS).

    Returns the number of jobs closed.
    """
    delivery_outcome = _CLOSING[outcome]
    step = _CLOSING_STEP[outcome]
    rows = await session.execute(
        select(Notification)
        .where(condition, Notification.status.in_(_ACTIVE_STATUSES))
        .order_by(Notification.id)
        .with_for_update()
    )
    notifications = list(rows.scalars().all())
    for notification in notifications:
        if notification.status == NotificationStatus.PENDING:
            # Not resolved: no deliveries exist to carry the outcome.
            notification.status = outcome
            journal.record_job(session, notification, step)
            await forget_llm_letter(session, notification)
            continue
        await close_waiting_deliveries(
            session, notification, delivery_outcome, step,
        )
        await rollup_notification(session, notification, step)
    await session.flush()
    return len(notifications)


async def close_waiting_deliveries(
    session: AsyncSession,
    notification: Notification,
    outcome: DeliveryStatus,
    step: JournalStep,
    *conditions: Any,
    failure_class: FailureClass | None = None,
) -> int:
    """Close the job's waiting deliveries (optionally narrowed by
    `conditions`) with `outcome` -- one journal row per delivery.

    Shared by every path that closes deliveries from outside an attempt:
    expiry, cancellation, forgetting, and the pipeline's ceiling
    (app/engine/processor.py). A waiting delivery the channel already
    accepted -- in an attempt that tore before its commit -- is closed
    SENT instead: the letter went out, and no closing outcome may say
    otherwise. The rest are closed in ONE UPDATE; its RETURNING is the
    loop the journal rows are written from (a mass transition has no
    loop over objects of its own).

    The caller holds the job's row lock and folds the job afterwards.
    Returns the number of deliveries closed.
    """
    notification_id = notification.id
    waiting = (
        NotificationDelivery.notification_id == notification_id,
        NotificationDelivery.status == DeliveryStatus.PENDING,
        *conditions,
    )
    closed = 0
    accepted = await journal.accepted_answers(session, [notification_id])
    if accepted:
        sent = (
            await session.execute(select(NotificationDelivery).where(*waiting))
        ).scalars().all()
        for delivery in sent:
            accepted_at = accepted.get(
                (notification_id, delivery.recipient_id, delivery.channel),
            )
            if accepted_at is None:
                continue
            _close_accepted(delivery, accepted_at)
            journal.record_delivery(session, notification, delivery, step)
            closed += 1
        await session.flush()
    rows = await session.execute(
        update(NotificationDelivery)
        .where(*waiting)
        .values(
            status=outcome,
            failure_class=failure_class,
            next_retry_at=None,
            wait_reason=None,
        )
        .returning(
            NotificationDelivery.recipient_id,
            NotificationDelivery.channel,
            NotificationDelivery.status,
            NotificationDelivery.attempts,
            NotificationDelivery.failure_class,
        )
    )
    closed += journal.record_closed_rows(session, notification_id, rows, step)
    return closed


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
        delete(Notification).where(_id_in_batch(batch_ids)),
    )
    deleted: int = result.rowcount  # type: ignore[attr-defined]
    return deleted


def _id_in_batch(batch_ids: Any) -> Any:
    """`id = ANY(ARRAY(<batch select>))` -- the delete of a batch (H5).

    The batch is selected ONCE (an InitPlan) and its rows are then
    deleted through the primary key -- the SHAPE is fixed here, not
    left to the estimates. The plain `id IN (<batch select>)` leaves
    the join to the planner: on a large table with a small batch it
    picks the same nested loop on the pkey (measured: identical
    buffers and time at 50k rows / a 250-row batch), but on other
    estimates -- a batch that is a large fraction of the table -- it
    picks a hash semi-join over a seq scan of notifications. That
    choice is the planner's to make per ANALYZE, and this runs on
    EVERY pass of the worker's maintenance loop (beside the lane loops
    since P4-1, no longer on the delivery path): a pass that runs that
    often should not change plan under it. Used by both batch deletes:
    retention's and the expiry cleanup's.
    """
    return Notification.id == any_(func.array(batch_ids))


# The outcomes an expired job may be deleted in by the expiry cleanup
# (app/engine/processor.py cleanup_expired_notifications): every outcome
# but failed -- a failure is kept for inspection until general
# retention takes it. The SAME list is the predicate of the partial
# index the batch select runs on (ix_notifications_expired_deletable,
# migration 0023) -- the planner uses that index only while this
# list implies its predicate; tests/test_expired_cleanup_index.py holds
# the two equal.
_EXPIRED_DELETABLE_STATUSES = (
    NotificationStatus.SENT,
    NotificationStatus.PARTIAL_SENT,
    NotificationStatus.EXPIRED,
    NotificationStatus.CANCELLED,
    NotificationStatus.SUPPRESSED,
    NotificationStatus.NO_RECIPIENTS,
)


async def delete_expired_notifications_batch(
    session: AsyncSession,
    *,
    now: datetime,
    limit: int,
) -> int:
    """Delete ONE batch of expired terminal notifications (H2).

    COMMIT-FREE, like delete_terminal_notifications_batch: the caller
    (the expiry cleanup in app/engine/processor.py) owns the drain loop
    and the per-batch commit, so the FK cascade of one transaction --
    deliveries, journal, push_outbox rows -- is bounded to `limit` jobs
    and the outbox rows it holds are let go batch by batch. Returns the
    number of rows deleted in this batch.

    BY INDEX (H5): the select is served by the partial index
    ix_notifications_expired_deletable -- (expiry_at) over the rows
    this very filter can match -- in expiry order, so neither a scan of
    the table nor a sort of it; the delete goes through the primary key
    (_id_in_batch).

    NOT WHILE A PUSH IS OWED (H7): a job with a row in push_outbox is
    skipped -- the cascade would take the push with the job, and the
    product, told nothing and finding nothing by its key, would lose
    the result itself (deploy/INTEGRATION.md section 10: a push may be
    lost, the result may not). The relay deletes the row once it has
    published; the next pass deletes the job. No deadline of its own:
    a push still owed when general retention reaches the job
    (delete_terminal_notifications_batch, by created_at) goes with it
    -- retention is the one bound on how long anything is kept. The
    check is an anti-join on ix_push_outbox_notification, beside the
    partial index, not instead of it (tests/test_push_survives_cleanup.py).

    NOT WHILE AN ANSWER IS KEPT (P5-1): a job whose model answer still
    has its body is skipped the same way -- an anti-join on the primary
    key of llm_answers; once purge_llm_answers clears the body at
    keep_until, the job goes like any other. General retention bounds
    this too.
    """
    batch_ids = (
        select(Notification.id)
        .where(
            Notification.expiry_at.isnot(None),
            Notification.expiry_at < now,
            Notification.status.in_(_EXPIRED_DELETABLE_STATUSES),
            ~exists().where(PushOutbox.notification_id == Notification.id),
            # NOT WHILE A MODEL'S ANSWER IS KEPT (P5-1): the job's expiry
            # bounds when it may be ASKED, not how long its answer is
            # kept -- that is the type's answer_keep. The purge clears
            # the body at keep_until; the next pass deletes the job.
            ~exists().where(
                LLMAnswer.notification_id == Notification.id,
                LLMAnswer.body.is_not(None),
            ),
        )
        .order_by(Notification.expiry_at)
        .limit(limit)
        .scalar_subquery()
    )
    result = await session.execute(
        delete(Notification).where(_id_in_batch(batch_ids)),
    )
    deleted: int = result.rowcount  # type: ignore[attr-defined]
    return deleted


async def purge_llm_answers(
    session: AsyncSession, *, now: datetime, limit: int,
) -> int:
    """Clear ONE batch of model answers past their term (P5-1, spec
    §14.12 item 5): body -> NULL where keep_until has passed. The
    snapshot -- set, provider, model, when -- stays with the job, and so
    does the letter's hash: what answered which letter outlives the
    answer. COMMIT-FREE like the batch deletes: the caller commits.

    By ix_llm_answers_keep (keep_until WHERE body IS NOT NULL), in term
    order, through the primary key. The rows written: llm_answers only,
    whose other writers are the channel's answer (an INSERT -- never a
    row this sees) and the cascade of a job's deletion. Returns the
    number cleared.
    """
    batch_ids = (
        select(LLMAnswer.notification_id)
        .where(LLMAnswer.body.is_not(None), LLMAnswer.keep_until < now)
        .order_by(LLMAnswer.keep_until)
        .limit(limit)
        .scalar_subquery()
    )
    result = await session.execute(
        update(LLMAnswer)
        .where(LLMAnswer.notification_id == any_(func.array(batch_ids)))
        .values(body=None)
    )
    cleared: int = result.rowcount  # type: ignore[attr-defined]
    return cleared


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
