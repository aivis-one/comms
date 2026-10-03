# =============================================================================
# COMMS Service -- The transition journal (spec §6.4, P2-1)
# =============================================================================
#
# THE ONE WRITER of notification_transitions. Every transition of a job
# or a delivery, every channel answer and every recipient a gate dropped
# leaves one row, written through a function of this module and through
# nothing else. A test walks app/ for the points that change a status,
# a wait or a pipeline record and holds each one to a call into this
# module, and for any UPDATE or DELETE of the table (the one edit is
# forgetting, app/engine/service.py withdraw_recipient).
#
# TWO TRANSACTIONS, ON PURPOSE.
#   job / delivery / gate rows are added to the session of the change
#   they record: a rolled-back attempt leaves none of them, so the
#   journal never claims a transition that did not stick.
#   channel rows are written in their OWN transaction right after the
#   channel answered (record_channel_answer): what a provider said is a
#   fact whether or not the attempt commits -- a 401 in an attempt that
#   later tears on the rollup is still in the journal, and "the channel
#   accepted the letter" survives the rollback that erases SENT. That
#   row is what the next attempt and every closing path read
#   (accepted_answers): such a delivery is closed SENT, never re-sent.
#
# WHAT A ROW NEVER HOLDS: the letter (title, body, action_data, the
# template variables) and the text of an exception of comms' own --
# only its class and place (pipeline_error_of). A provider's answer is
# kept, sanitized before it is cut.
#
# Rows go only with their job (ON DELETE CASCADE, migration 0017).
#
# THE PUSH (P3-1, spec §7.4, §7.6). Being the one writer of
# transitions, this module is also the one writer of the pushes they
# owe: a job or delivery row that push_due accepts under the job's
# push_on adds a push_outbox row to the SAME session, so the push
# commits with the transition or not at all, and the relay publishes it
# only after the commit (app/transport/push_relay.py). A transition
# point that forgot to record would forget its push too -- the fence
# that holds every point to this module (tests/test_transition_journal.py,
# TestEveryPointRecords) holds the push with it. Channel and gate rows
# never push; neither does a delivery closed by a mass transition: the
# job's outcome that follows is its own row.
# =============================================================================

import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import func, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.engine.constants import (
    ChannelAnswer,
    DeliveryStatus,
    FailureClass,
    JobWaitReason,
    JournalStep,
    JournalSubject,
    NotificationStatus,
)
from app.engine.formatters import (
    EmailTransientError,
    PermanentDeliveryError,
    RateLimitedError,
    sanitize_error,
    sanitized_traceback,
)
from app.engine.models import (
    Notification,
    NotificationDelivery,
    NotificationTransition,
    PushOutbox,
)
from app.profile.registry import PushOn

logger = structlog.get_logger()

# The package whose frames name the place of a failure: the innermost
# frame inside comms' own code is where comms broke, a frame inside a
# library is where the library was called from comms.
_APP_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = _APP_ROOT.parent
# notifications.pipeline_error and notification_transitions.error are
# varchar(300) (migrations 0015, 0017).
_PIPELINE_ERROR_MAX = 300

# The provider libraries whose exceptions carry the PROVIDER's words:
# their text is a channel answer. Named by module root rather than by
# import -- this module must stay importable on a deploy without them.
_PROVIDER_MODULE_ROOTS = frozenset({"aiogram", "httpx", "httpcore"})


def pipeline_error_of(exc: BaseException) -> str:
    """The CLASS of an exception of comms' own and the PLACE in comms
    where it was raised (module:line).

    NEVER the exception's text, sanitized or not. Sanitizing removes
    secrets, not content: a text raised from a template or a formatter
    can carry the letter's variables, and a record of comms holds no
    letter content (spec §6.4). The class and the place say where it
    tore; the redacted traceback in the log says the rest.
    """
    name = f"{type(exc).__module__}.{type(exc).__qualname__}"
    frames = traceback.extract_tb(exc.__traceback__)
    place = "no traceback"
    for frame in reversed(frames):
        path = Path(frame.filename).resolve()
        if path.is_relative_to(_APP_ROOT):
            place = f"{path.relative_to(_REPO_ROOT).as_posix()}:{frame.lineno}"
            break
    else:
        if frames:
            place = f"{Path(frames[-1].filename).name}:{frames[-1].lineno}"
    return f"{name} at {place}"[:_PIPELINE_ERROR_MAX]


def _provider_text(exc: Exception) -> str | None:
    """The provider's answer, sanitized BEFORE it is cut; None, never a
    blank string, when it says nothing."""
    return sanitize_error(exc) or None


# -----------------------------------------------------------------------------
# The push a transition owes (P3-1)
# -----------------------------------------------------------------------------

# A job's outcome: every status but the two active ones.
OUTCOME_STATUSES = frozenset(NotificationStatus) - {
    NotificationStatus.PENDING, NotificationStatus.PROCESSING,
}

# session.info key: the last push row this session added, per job.
_PUSH_ADDED = "journal_push_added"


def push_due(push_on: str, row: NotificationTransition) -> bool:
    """Whether a journal row owes the product a push under `push_on`.

    Read off the row alone -- its subject, its outcome, its wait -- and
    the job's push_on; never off the letter or a provider's answer.
      outcome: a JOB row whose outcome is the job's outcome.
      deferral (outcome_and_deferral only): a JOB or DELIVERY row that
        puts the job or a delivery behind a timed gate -- wait_until
        set: the recipient's schedule, the provider's 429, comms' own
        backoff, the pipeline's gate. A delivery waiting its turn has
        no wait_until and is not a deferral; a future scheduled_at is
        the product's own and has no row of its kind.
    Channel and gate rows never push.
    """
    if push_on == PushOn.NONE:
        return False
    if row.subject == JournalSubject.JOB and row.outcome in OUTCOME_STATUSES:
        return True
    return (
        push_on == PushOn.OUTCOME_AND_DEFERRAL
        and row.subject in (JournalSubject.JOB, JournalSubject.DELIVERY)
        and row.wait_until is not None
    )


def _owe_push(session: AsyncSession, notification: Notification) -> None:
    """Add the job's push row to the session -- unless one this session
    added for the job is still unflushed: one push per job per flush, so
    a pass that defers a thousand deliveries owes one push, not a
    thousand. The check is on that very row's state, not a remembered
    flag: a savepoint rollback expunges the row it added (pending ->
    transient) and a flush writes it (pending -> persistent), and in
    both cases the next transition adds a fresh one. A duplicate after a
    flush is harmless (spec §7.2)."""
    added: dict[UUID, PushOutbox] = session.info.setdefault(_PUSH_ADDED, {})
    previous = added.get(notification.id)
    if previous is not None and inspect(previous).pending:
        return
    push = PushOutbox(notification_id=notification.id)
    session.add(push)
    added[notification.id] = push


def _record(
    session: AsyncSession,
    notification: Notification,
    row: NotificationTransition,
) -> None:
    """Add a job or delivery row, and the push it owes."""
    session.add(row)
    if push_due(notification.push_on, row):
        _owe_push(session, notification)


# -----------------------------------------------------------------------------
# Rows in the session of the change (job, delivery, gate)
# -----------------------------------------------------------------------------


def record_job(
    session: AsyncSession,
    notification: Notification,
    step: JournalStep,
    *,
    error: str | None = None,
    wait_until: datetime | None = None,
) -> None:
    """A transition of the job: its status after it, its pipeline
    attempt count. `wait_until` is the pipeline gate (T12)."""
    _record(session, notification, NotificationTransition(
        notification_id=notification.id,
        subject=JournalSubject.JOB,
        step=step,
        outcome=notification.status,
        attempt=notification.pipeline_attempts or 0,
        wait_reason=(
            JobWaitReason.PIPELINE_RETRY if wait_until is not None else None
        ),
        wait_until=wait_until,
        error=error,
    ))


def record_delivery(
    session: AsyncSession,
    notification: Notification,
    delivery: NotificationDelivery,
    step: JournalStep,
    *,
    category: str | None = None,
) -> None:
    """A transition of a delivery of `notification`, read off the
    delivery as it stands after the change: status, attempts, wait,
    failure class. The job is passed for what it pushes (push_on)."""
    _record(session, notification, NotificationTransition(
        notification_id=delivery.notification_id,
        recipient_id=delivery.recipient_id,
        channel=delivery.channel,
        subject=JournalSubject.DELIVERY,
        step=step,
        outcome=delivery.status,
        attempt=delivery.attempts or 0,
        wait_reason=delivery.wait_reason,
        wait_until=delivery.next_retry_at,
        failure_class=delivery.failure_class,
        category=category,
    ))


def record_closed_rows(
    session: AsyncSession,
    notification_id: UUID,
    rows: Any,
    step: JournalStep,
) -> int:
    """One row per delivery a bulk UPDATE closed -- the rows its
    RETURNING gave (recipient_id, channel, status, attempts,
    failure_class). A mass transition has no loop over objects; this is
    the loop over its result. Returns the count."""
    count = 0
    for row in rows:
        session.add(NotificationTransition(
            notification_id=notification_id,
            recipient_id=row.recipient_id,
            channel=row.channel,
            subject=JournalSubject.DELIVERY,
            step=step,
            outcome=row.status,
            attempt=row.attempts,
            failure_class=row.failure_class,
        ))
        count += 1
    return count


def record_gate(
    session: AsyncSession,
    notification: Notification,
    recipient_id: UUID,
    category: str,
) -> None:
    """A recipient the mute gate dropped at resolve, before a delivery
    existed for them: "suppressed by preferences, by this category"."""
    session.add(NotificationTransition(
        notification_id=notification.id,
        recipient_id=recipient_id,
        subject=JournalSubject.GATE,
        step=JournalStep.RESOLVE,
        outcome=DeliveryStatus.SUPPRESSED,
        attempt=0,
        category=category,
    ))


# -----------------------------------------------------------------------------
# Channel answers -- their own transaction
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class Answer:
    """What a channel call came to, as the journal records it."""

    outcome: ChannelAnswer
    failure_class: FailureClass | None = None
    provider_text: str | None = None
    error: str | None = None


ACCEPTED = Answer(outcome=ChannelAnswer.ACCEPTED)


def answer_of(
    exc: Exception, failure_class: FailureClass | None = None,
) -> Answer:
    """The journal's reading of an exception from a channel call.

    The provider's words are kept only where the exception IS the
    provider's answer: the typed answers the adapters raise, and the
    exceptions of the provider libraries. Anything else is comms' own
    (a template, a formatter, a bug) and gets its class and place --
    never its text.

    A permanent refusal comes with the class the caller decided for it
    (app/engine/service.py is the one place that decides it).
    """
    if isinstance(exc, PermanentDeliveryError):
        if failure_class is None:
            raise TypeError("a permanent refusal is recorded with its class")
        return Answer(
            outcome=ChannelAnswer.REFUSED,
            failure_class=failure_class,
            provider_text=_provider_text(exc),
        )
    if isinstance(exc, RateLimitedError):
        return Answer(
            outcome=ChannelAnswer.RATE_LIMITED,
            provider_text=_provider_text(exc),
        )
    if isinstance(exc, TimeoutError):
        return Answer(outcome=ChannelAnswer.TIMEOUT)
    root = type(exc).__module__.partition(".")[0]
    if isinstance(exc, EmailTransientError) or root in _PROVIDER_MODULE_ROOTS:
        return Answer(
            outcome=ChannelAnswer.TRANSIENT,
            provider_text=_provider_text(exc),
        )
    return Answer(outcome=ChannelAnswer.ERROR, error=pipeline_error_of(exc))


async def record_channel_answer(
    delivery: NotificationDelivery, answer: Answer,
) -> None:
    """Write one channel answer in its OWN transaction, now.

    Called from inside the attempt while it holds its row lock on the
    job. The insert's foreign key takes FOR KEY SHARE on that row,
    which FOR NO KEY UPDATE -- the attempt's lock -- does not block
    (app/engine/processor.py, the lock rule); under FOR UPDATE this
    session would wait for the attempt and the attempt for it.

    A failure to write is logged loudly and does not fail the attempt:
    the answer is still applied to the delivery, and failing the attempt
    would re-send the letter anyway.

    KNOWN CEILING (acknowledged by design -- P2-1, the accepted window):
      1. Mechanics: between the channel taking the letter and this
         transaction committing, a process that dies (OOM, kill -9, a
         lost database connection) leaves no "accepted" row; the next
         attempt sends the letter again. At-least-once, in a window of
         one INSERT.
      2. Status: acknowledged by design.
      3. Backlog ref: none -- neither channel offers idempotency on its
         send endpoint today (app/engine/formatters.py,
         _request_may_have_arrived), so there is nothing to pass yet.
      4. Promotion trigger (observable): two CHANNEL rows with outcome
         `accepted` for one (notification_id, recipient_id, channel).
      5. Agreed fix: a provider-side idempotency key, carried through
         the adapter, on a channel whose provider supports one.
      6. Rejected: an "intent" row written before the call, with
         "intent without answer -> do not send" (at-most-once: a lost
         letter is worse than a repeated one, and the contract is
         at-least-once -- deploy/INTEGRATION.md); committing each
         delivery's outcome right after its call (splits the attempt
         across transactions and breaks the one-lock-per-attempt rule
         expiry and cancellation rely on).
    """
    factory = get_session_factory()
    async with factory() as session:
        try:
            session.add(NotificationTransition(
                notification_id=delivery.notification_id,
                recipient_id=delivery.recipient_id,
                channel=delivery.channel,
                subject=JournalSubject.CHANNEL,
                step=JournalStep.DELIVER,
                outcome=answer.outcome,
                attempt=(delivery.attempts or 0) + 1,
                failure_class=answer.failure_class,
                provider_text=answer.provider_text,
                error=answer.error,
            ))
            await session.commit()
        except Exception as exc:
            await session.rollback()
            logger.error(
                "journal_channel_answer_error",
                notification_id=str(delivery.notification_id),
                recipient_id=str(delivery.recipient_id),
                channel=delivery.channel,
                answer=answer.outcome.value,
                exception=sanitized_traceback(exc),
            )


async def accepted_answers(
    session: AsyncSession, notification_ids: list[UUID],
) -> dict[tuple[UUID, UUID, str], datetime]:
    """Every (notification_id, recipient_id, channel) a channel has
    accepted the letter for, with the moment it first did.

    The key is the triple, not a delivery id: the deliveries of a first
    attempt are created in its transaction and vanish with its rollback,
    the answer does not.
    """
    if not notification_ids:
        return {}
    rows = await session.execute(
        select(
            NotificationTransition.notification_id,
            NotificationTransition.recipient_id,
            NotificationTransition.channel,
            func.min(NotificationTransition.at),
        )
        .where(
            NotificationTransition.notification_id.in_(notification_ids),
            NotificationTransition.subject == JournalSubject.CHANNEL,
            NotificationTransition.outcome == ChannelAnswer.ACCEPTED,
        )
        .group_by(
            NotificationTransition.notification_id,
            NotificationTransition.recipient_id,
            NotificationTransition.channel,
        )
    )
    accepted: dict[tuple[UUID, UUID, str], datetime] = {}
    for notification_id, recipient_id, channel, at in rows.all():
        accepted[(notification_id, recipient_id, channel)] = at
    return accepted
