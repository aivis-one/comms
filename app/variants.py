# =============================================================================
# COMMS Service -- A message in another language: variants (P5-4)
# =============================================================================
#
# Translation in a thread is the PRODUCT's use of the llm channel (spec
# §12-§14): the product decides who needs which language, asks a model
# with its own prompt, reads the answer with its own schema. comms does
# not translate and reads neither the message nor the answer. What it
# gives is a PLACE for the result -- a variant next to the untouched
# original -- and a thread feed in the reader's language that says, for
# every message, the variant or WHY there is none.
#
# TWO RECORDS, both written by the product, both insert-only
# (app/messaging/models.py, migration 0033):
#   - the variant REQUEST -- "a variant in L was asked for, by job K",
#     written when the product queues the model. It is how comms can
#     say "preparing": the link between a job and (message, language)
#     is the product's knowledge, and comms is told it, never infers it
#     from a convention of keys;
#   - the VARIANT -- the text in L (with the language the model found
#     the message written in), or "none is needed" (that language IS
#     L), or "declined" (the product read the answer and writes none).
#     The provider and model that answered are copied from the job's
#     answer snapshot (llm_answers) when it is written.
#
# THE READER'S STATE, decided in this order -- the first that holds:
#   1. the variant in L: its text            -> READY
#                        declined            -> IMPOSSIBLE
#                        none needed         -> NOT_NEEDED
#   2. another variant of the message whose source_locale is L
#                                            -> NOT_NEEDED
#   3. the latest request in L, by ITS job's status -- the job it named
#      when it was written, by id (3.8.0 -- fix: retention frees a key,
#      and a later job under the same key is another job):
#        pending / processing / sent         -> PREPARING
#        failed / expired / cancelled, or the job gone with retention
#                                            -> IMPOSSIBLE
#   4. nothing                               -> NOT_REQUESTED
# "Sent" is PREPARING: the model answered and the product has not
# written the variant -- a model that answered "I can't" is an answer,
# not a failure (spec §14.4); the product says it with "declined".
# An llm job has one recipient-less delivery, so it never ends
# suppressed, no_recipients or partial_sent (app/engine/service.py
# _fold): those have no place in step 3.
#
# HOW LONG IT LIVES: as long as its message. Both tables go with the
# message (ON DELETE CASCADE) -- and no path of comms deletes a message
# or a thread today: forgetting keeps threads and messages (their
# bodies are the product's decision, spec §10.6 -- and so are the
# variants), retention deletes jobs, auto-close changes a status. A
# request whose job retention deleted reads as IMPOSSIBLE.
#
# A NEUTRAL MODULE, like app/notifier.py: it reads messaging and the
# engine's jobs; neither imports the other.
# =============================================================================

from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import is_unique_violation
from app.core.exceptions import (
    IdempotencyConflictError,
    NotFoundError,
    ValidationError,
    VariantExistsError,
)
from app.engine.constants import NotificationStatus
from app.engine.models import LLMAnswer, LLMCall, Notification
from app.messaging.constants import VariantState
from app.messaging.models import Message, MessageVariant, MessageVariantRequest

# Step 3: a request's job status -> the reader's state. Only the
# statuses an llm job can have (see the header); tests/
# test_message_variants.py TestTheStatusTable holds the table to
# NotificationStatus minus the three an llm job never reaches.
_BY_JOB_STATUS: dict[str, VariantState] = {
    NotificationStatus.PENDING: VariantState.PREPARING,
    NotificationStatus.PROCESSING: VariantState.PREPARING,
    NotificationStatus.SENT: VariantState.PREPARING,
    NotificationStatus.FAILED: VariantState.IMPOSSIBLE,
    NotificationStatus.EXPIRED: VariantState.IMPOSSIBLE,
    NotificationStatus.CANCELLED: VariantState.IMPOSSIBLE,
}


async def _message_of_thread(
    session: AsyncSession, thread_id: UUID, message_id: UUID,
) -> Message:
    message = await session.get(Message, message_id)
    if message is None or message.thread_id != thread_id:
        raise NotFoundError(
            f"message {message_id} does not exist in thread {thread_id}"
        )
    return message


async def _llm_job(session: AsyncSession, job_key: str) -> Notification:
    """The llm job the product names by its key, or a refusal."""
    job = await session.scalar(
        select(Notification).where(Notification.idempotency_key == job_key)
    )
    if job is None or await session.get(LLMCall, job.id) is None:
        raise ValidationError(
            f"job_key {job_key!r} names no job of the llm kind"
        )
    return job


async def _replay[R: (MessageVariant, MessageVariantRequest)](
    session: AsyncSession, model: type[R], key: str, fingerprint: str,
) -> R | None:
    row = await session.scalar(select(model).where(model.idempotency_key == key))
    if row is None:
        return None
    if row.fingerprint != fingerprint:
        raise IdempotencyConflictError(
            f"the Idempotency-Key {key!r} was used for another request"
        )
    return row


async def _insert[R: (MessageVariant, MessageVariantRequest)](
    session: AsyncSession,
    row: R,
    existing: "_Existing[R]",
) -> R:
    """Insert one row; a unique violation is a replay or a conflict,
    decided by reading what is there now -- never by a constraint name."""
    try:
        async with session.begin_nested():
            session.add(row)
            await session.flush()
    except IntegrityError as exc:
        if not is_unique_violation(exc):
            raise
        winner = await existing.replay()
        if winner is not None:
            return winner
        raise existing.conflict() from exc
    return row


@dataclass
class _Existing[R: (MessageVariant, MessageVariantRequest)]:
    session: AsyncSession
    model: type[R]
    key: str
    fingerprint: str
    what: str

    async def replay(self) -> R | None:
        return await _replay(self.session, self.model, self.key, self.fingerprint)

    def conflict(self) -> VariantExistsError:
        return VariantExistsError(f"{self.what} is already written")


async def write_request(
    session: AsyncSession,
    *,
    thread_id: UUID,
    message_id: UUID,
    locale: str,
    job_key: str,
    key: str,
    fingerprint: str,
) -> MessageVariantRequest:
    """Record that a variant in `locale` was asked for, by `job_key`."""
    replayed = await _replay(session, MessageVariantRequest, key, fingerprint)
    if replayed is not None:
        return replayed
    await _message_of_thread(session, thread_id, message_id)
    job = await _llm_job(session, job_key)
    what = f"the request for {locale!r} by job {job_key!r}"
    taken = await session.scalar(select(MessageVariantRequest).where(
        MessageVariantRequest.message_id == message_id,
        MessageVariantRequest.locale == locale,
        MessageVariantRequest.job_id == job.id,
    ))
    if taken is not None:
        raise VariantExistsError(f"{what} is already written")
    return await _insert(
        session,
        MessageVariantRequest(
            message_id=message_id, locale=locale, job_key=job_key,
            job_id=job.id, idempotency_key=key, fingerprint=fingerprint,
        ),
        _Existing(session, MessageVariantRequest, key, fingerprint, what),
    )


async def write_variant(
    session: AsyncSession,
    *,
    thread_id: UUID,
    message_id: UUID,
    locale: str,
    job_key: str,
    source_locale: str | None,
    body: str | None,
    declined: bool,
    key: str,
    fingerprint: str,
) -> MessageVariant:
    """Write the variant in `locale` -- text, none needed, or declined.

    The form is checked by the caller's body model (one of the three);
    here: the message is in the thread, the job is an llm job that has
    answered, and no variant in `locale` exists yet.
    """
    replayed = await _replay(session, MessageVariant, key, fingerprint)
    if replayed is not None:
        return replayed
    await _message_of_thread(session, thread_id, message_id)
    job = await _llm_job(session, job_key)
    answer = await session.get(LLMAnswer, job.id)
    if answer is None:
        raise ValidationError(
            f"job {job_key!r} has no answer: a variant is written from one"
        )
    what = f"the variant in {locale!r}"
    taken = await session.scalar(select(MessageVariant.id).where(
        MessageVariant.message_id == message_id,
        MessageVariant.locale == locale,
    ))
    if taken is not None:
        raise VariantExistsError(f"{what} is already written")
    return await _insert(
        session,
        MessageVariant(
            message_id=message_id, locale=locale, job_key=job_key,
            source_locale=source_locale, body=body, declined=declined,
            answered_provider=answer.answered_provider,
            answered_model=answer.answered_model,
            idempotency_key=key, fingerprint=fingerprint,
        ),
        _Existing(session, MessageVariant, key, fingerprint, what),
    )


def decide_state(
    locale: str,
    variants: Sequence[MessageVariant],
    latest_request: MessageVariantRequest | None,
    job_status: str | None,
) -> tuple[VariantState, MessageVariant | None]:
    """The reader's state for one message (the order in the header).

    `variants` are ALL of the message's variants; `job_status` is the
    status of the latest request's own job (by id), None when it is
    gone."""
    mine = next((v for v in variants if v.locale == locale), None)
    if mine is not None:
        if mine.body is not None:
            return VariantState.READY, mine
        if mine.declined:
            return VariantState.IMPOSSIBLE, None
        return VariantState.NOT_NEEDED, None
    if any(v.source_locale == locale for v in variants):
        return VariantState.NOT_NEEDED, None
    if latest_request is not None:
        if job_status is None:
            return VariantState.IMPOSSIBLE, None
        # .get, not [...]: the request's job is read by id, so it is the
        # llm job it named and its status is in the table -- but a feed
        # is no place to fail on a status nobody foresaw (3.8.0 -- fix:
        # read by key, a person's job under a reused key gave KeyError
        # 'partial_sent', a 500). Unforeseen is no variant: IMPOSSIBLE.
        # TestTheStatusTable is where an unforeseen status shows.
        return _BY_JOB_STATUS.get(job_status, VariantState.IMPOSSIBLE), None
    return VariantState.NOT_REQUESTED, None


async def reader_states(
    session: AsyncSession, message_ids: Sequence[UUID], locale: str,
) -> dict[UUID, tuple[VariantState, MessageVariant | None]]:
    """The state of every message of a feed page for a reader in
    `locale`: three queries for the page, whatever its size -- the
    variants of its messages, their requests in `locale`, and those
    requests' jobs by id, each by a unique index."""
    if not message_ids:
        return {}
    ids = list(message_ids)
    variants: dict[UUID, list[MessageVariant]] = {}
    for variant in (await session.scalars(
        select(MessageVariant).where(MessageVariant.message_id.in_(ids))
    )).all():
        variants.setdefault(variant.message_id, []).append(variant)
    latest: dict[UUID, MessageVariantRequest] = {}
    for request in (await session.scalars(
        select(MessageVariantRequest)
        .where(
            MessageVariantRequest.message_id.in_(ids),
            MessageVariantRequest.locale == locale,
        )
        .order_by(
            MessageVariantRequest.created_at.desc(),
            MessageVariantRequest.id.desc(),
        )
    )).all():
        latest.setdefault(request.message_id, request)
    statuses: dict[UUID, str] = {}
    jobs = sorted({request.job_id for request in latest.values()})
    if jobs:
        statuses = dict((await session.execute(
            select(Notification.id, Notification.status)
            .where(Notification.id.in_(jobs))
        )).tuples().all())
    return {
        message_id: decide_state(
            locale,
            variants.get(message_id, []),
            latest.get(message_id),
            statuses.get(latest[message_id].job_id)
            if message_id in latest else None,
        )
        for message_id in ids
    }
