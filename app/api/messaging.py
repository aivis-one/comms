# =============================================================================
# COMMS Service -- Messaging API (threads / messages / operator ops)
# =============================================================================
#
# Phase 4c item 3. The HTTP surface over the 4a/4b messaging domain:
# create-or-get a thread, post a message, page a thread's feed, thread
# read-state (position IN a thread -- NOT the delivery inbox "bell",
# which is a different router), and the operator verbs claim / assign /
# release / set-status / retag plus the paginated visible-threads list.
#
# POSTing a message runs notify_new_message in the SAME transaction
# (fork 3): the message row and the other side's notification rows
# commit together -- never a message without its ping, never a ping
# without its message. Delivery stays async on the worker tick.
#
# CONTRACT (Phase 6 product proxy consumes as-is):
#
#   POST /api/v1/threads                      -> 200 <thread> + created
#   DELETE /api/v1/threads/{tid}              -> 204 (also when absent)
#   GET  /api/v1/threads?operator=|client=&is_supervisor=&with_unread=
#        &with_last_message=&locale=&limit=&cursor=
#                                             -> 200 {items, next_cursor}
#   POST /api/v1/threads/{tid}/messages       -> 200 <message>  (+ notify)
#   GET  /api/v1/threads/{tid}/messages?limit=&cursor=&locale=
#                                             -> 200 {items, next_cursor}
#   POST /api/v1/threads/{tid}/messages/{mid}/variant-requests
#                                             -> 200 <variant request>
#   POST /api/v1/threads/{tid}/messages/{mid}/variants
#                                             -> 200 <variant>
#   POST /api/v1/threads/{tid}/read           -> 200 {unread}   (fresh badge)
#   GET  /api/v1/threads/{tid}/unread-count?participant=
#                                             -> 200 {unread}
#   POST /api/v1/threads/{tid}/claim          -> 200 {claimed, thread}
#   POST /api/v1/threads/{tid}/assign         -> 200 <thread>
#   POST /api/v1/threads/{tid}/release        -> 200 <thread>
#   POST /api/v1/threads/{tid}/status         -> 200 <thread>
#   POST /api/v1/threads/{tid}/retag          -> 200 <thread>
#   POST /api/v1/threads/unread-counts        -> 200 {counts}
#
#   POST /api/v1/sections                     -> 200 <section>
#
#   GET  /api/v1/participants/{pid}/unread-summary
#                                             -> 200 {has_unread,
#                                                     threads_with_unread,
#                                                     unread_messages}
#
#   - sections live on their OWN router (prefix /api/v1/sections) in
#     this module: they are part of messaging, but a section is not a
#     thread and must not hide under the threads prefix;
#   - CREATE-OR-FIND, not upsert. Posting a key that already exists
#     returns the EXISTING section with 200 and does NOT touch its
#     label. Sending the same key with a different label is therefore
#     a successful no-op that returns the OLD label -- it is not a
#     rename and not a 409. Said here because the opposite is the
#     natural assumption, and a consumer who assumes it will spend an
#     afternoon debugging a rename that was never implemented;
#   - 200 rather than 201 for the same reason the thread endpoint uses
#     it: the call is idempotent, and "created" is not a property of
#     the response. Unlike POST /threads there is NO `created` flag --
#     see the endpoint docstring for why it cannot be derived here
#     honestly.
#
#   - THE THREE UNREAD AGGREGATES (T-51) SHARE ONE ABSENCE RULE:
#     ABSENCE MEANS "NOT A PARTICIPANT". The summary aggregates only
#     the caller's own threads; the batch OMITS a thread the caller
#     does not take part in (and, by the same single rule, a thread id
#     that does not exist at all); the list omits the `unread` KEY on
#     such a row. Never a silent zero -- a zero would read as "nothing
#     unread here" and hide an integration mistake (wrong participant,
#     stale thread id) behind a plausible number. Never a whole-batch
#     error either: one bad id must not cost the caller the other 99.
#     Participation is the three-role clause in
#     messaging/operators.participation_clause -- NOT thread
#     visibility: an unclaimed section thread is visible to every
#     operator and belongs to none of them, so it has no unread key;
#   - `created` (bool) rides ONLY on the create response: True for the
#     call that inserted the row, False on a dedup hit or a lost insert
#     race. ADDITIVE to the frozen 3b shape (seam T2 / ID-10, precedent
#     reminder_cancel) -- every pre-existing field is unchanged, and no
#     OTHER endpoint gained the key;
#   - THE LIST (MSG-1): an operator's or a client's (exactly one), and
#     with_last_message / locale add each row's last message and its
#     variant -- deploy/INTEGRATION.md section 13;
#   - A MESSAGE IN ANOTHER LANGUAGE (P5-4, app/variants.py): the two
#     variant routes are insert-only records the product writes; with
#     `locale` the feed adds `variant` and `variant_state` to every item,
#     WITHOUT it the feed is exactly what it was -- deploy/INTEGRATION.md
#     section 12;
#   - limit / cursor: the one paging scheme (app/api/paging.py) -- out
#     of 1..100 or a malformed cursor -> 422 `validation`;
#   - a bad status transition / half subject_ref -> 422; an absent
#     thread -- never created, or deleted (TK-2) -- on every route that
#     names one (the feed, unread-count, post, read, the variant routes
#     and every operator verb) -> 404; the input's own 422 comes first.
#     DELETE alone answers an absent thread as a present one: 204;
#   - THE OPERATOR VERBS -- who may call, refusals, the status matrix
#     and what a release does to the pool -- are the contract's section
#     14 (deploy/INTEGRATION.md), held to this code by
#     tests/test_thread_handoff.py.
#
# TRUST MODEL (frozen -- identical stance to the inbox contract): the
# ACTOR identifiers in a request body / query (client, sender,
# participant, operator) are TRUSTED. comms does NOT verify that the
# actor is the calling end user; the shared service token authenticates
# the PRODUCT (arch decision 14), and the product proxy is the sole
# owner of "user X may act only as user X". Phase 6 MUST substitute the
# actor server-side from its own authenticated session, never accept it
# from the client. Phase 4c write-authz (item 4) is authorization by
# ROLE IN THE THREAD (participant / serving operator / supervisor
# read-only), NOT identity -- it is orthogonal to this trust model.
# =============================================================================

import re
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Header, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import DbSession, require_service_auth
from app.api.idempotency import (
    IDEMPOTENCY_HEADER,
    request_fingerprint,
    require_key,
)
from app.api.paging import (
    PAGE_LIMIT_DEFAULT,
    decode_cursor,
    encode_cursor,
    page,
    page_limit,
)
from app.core.constants import MAX_IDEMPOTENCY_KEY_LEN, PAGE_LIMIT_MAX
from app.core.database import (
    get_db_reader,
    is_unique_violation_of,
)
from app.core.exceptions import (
    AuthorizationError,
    ClaimTakenError,
    IdempotencyConflictError,
    NotFoundError,
    ValidationError,
)
from app.messaging.constants import (
    MAX_MESSAGE_BODY_LEN,
    MAX_SECTION_KEY_LEN,
    MAX_SECTION_LABEL_LEN,
    MAX_SUBJECT_ID_LEN,
    MAX_SUBJECT_TYPE_LEN,
    MAX_THREAD_PRIORITY,
    MAX_THREAD_TITLE_LEN,
    MIN_THREAD_PRIORITY,
    VARIANT_LOCALE_PATTERN,
    OperatorKind,
    ThreadKind,
    ThreadStatus,
    VariantState,
)
from app.messaging.models import (
    Message,
    MessageVariant,
    MessageVariantRequest,
    Section,
    Thread,
)
from app.messaging.operators import (
    KEEP,
    KeepSubject,
    SubjectRef,
    assign_thread,
    can_claim,
    can_operate,
    can_post_message,
    claim_thread,
    list_client_threads,
    list_visible_threads,
    release_thread,
    retag_thread,
)
from app.messaging.read_state import (
    count_unread,
    mark_read,
    participant_unread_summary,
    unread_counts_for_participant,
)
from app.messaging.rows import take_thread
from app.messaging.sections import get_or_create_section
from app.messaging.status import set_status
from app.messaging.threads import (
    create_or_get_thread_detailed,
    delete_thread,
    last_messages,
    list_thread_messages,
    post_message,
)
from app.notifier import notify_new_message
from app.variants import reader_states, write_request, write_variant

router = APIRouter(
    prefix="/api/v1/threads",
    tags=["messaging"],
    dependencies=[Depends(require_service_auth)],
)

# Sections get their OWN router rather than a path under /threads: a
# section is not a thread, and a route that says otherwise sends the
# next reader looking for it in the wrong place. Same module, because
# sections are part of messaging and this codebase does not add files
# for one endpoint. Same auth as every neighbour -- the shared service
# token authenticates the PRODUCT (arch decision 14); nothing new is
# invented here.
sections_router = APIRouter(
    prefix="/api/v1/sections",
    tags=["messaging"],
    dependencies=[Depends(require_service_auth)],
)

# Participants get their own router for the same reason sections do: a
# participant is not a thread, and the summary is an aggregate ACROSS
# threads -- hiding it under /threads would say the opposite. Same
# module (this codebase does not add a file for one endpoint), same
# service-token dependency as every neighbour.
participants_router = APIRouter(
    prefix="/api/v1/participants",
    tags=["messaging"],
    dependencies=[Depends(require_service_auth)],
)


# The unique index of each writer's Idempotency-Key (migration 0014): a
# unique violation of exactly this one is a concurrent request with the
# same key, and its winner is replayed (create_thread,
# post_thread_message). Recognized by the driver's constraint name,
# never by the error text (app/core/database.py is_unique_violation_of).
_THREADS_KEY_INDEX = "uq_threads_idempotency_key"
_MESSAGES_KEY_INDEX = "uq_messages_idempotency_key"

# One page of a chat list, matched to the list endpoint's own page
# ceiling (PAGE_LIMIT_MAX, app/api/paging.py): the batch exists to serve
# a rendered list, so it takes exactly as many ids as a list can show.
UNREAD_COUNTS_MAX_THREAD_IDS = PAGE_LIMIT_MAX



# ---------------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------------
def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _thread_out(thread: Thread) -> dict[str, Any]:
    return {
        "id": str(thread.id),
        "client": str(thread.client),
        "operator_kind": thread.operator_kind,
        "operator_value": str(thread.operator_value),
        "assignee": str(thread.assignee) if thread.assignee else None,
        "kind": thread.kind,
        "status": thread.status,
        "subject_type": thread.subject_type,
        "subject_id": thread.subject_id,
        "title": thread.title,
        "priority": thread.priority,
        "last_message_at": _iso(thread.last_message_at),
        "created_at": _iso(thread.created_at),
        # RV-1 (3.12.0): the row's revision AFTER this call's write --
        # the ORM fetches it on flush (Thread.revision), the Core verbs
        # re-read the row before answering.
        "revision": thread.revision,
    }


def _section_out(section: Section) -> dict[str, Any]:
    return {
        "id": str(section.id),
        "key": section.key,
        "label": section.label,
        "created_at": _iso(section.created_at),
    }


def _message_out(message: Message) -> dict[str, Any]:
    return {
        "id": str(message.id),
        "thread_id": str(message.thread_id),
        "sender": str(message.sender),
        "body": message.body,
        "created_at": _iso(message.created_at),
    }


# The closed field sets of the variant records (P5-4) -- the contract's
# tables in deploy/INTEGRATION.md section 12 are held to these.
VARIANT_REQUEST_FIELDS = ("message_id", "locale", "job_key", "created_at")
VARIANT_FIELDS = (
    "message_id", "locale", "source_locale", "body", "declined", "job_key",
    "answered_provider", "answered_model", "created_at",
)
# What a feed item in a reader's language carries besides the message's
# own fields: the variant (READY only, else null) and the state.
FEED_VARIANT_FIELDS = (
    "locale", "source_locale", "body", "job_key", "answered_provider",
    "answered_model", "created_at",
)


def _variant_request_out(row: MessageVariantRequest) -> dict[str, Any]:
    return {
        "message_id": str(row.message_id),
        "locale": row.locale,
        "job_key": row.job_key,
        "created_at": _iso(row.created_at),
    }


def _variant_out(row: MessageVariant) -> dict[str, Any]:
    return {
        "message_id": str(row.message_id),
        "locale": row.locale,
        "source_locale": row.source_locale,
        "body": row.body,
        "declined": row.declined,
        "job_key": row.job_key,
        "answered_provider": row.answered_provider,
        "answered_model": row.answered_model,
        "created_at": _iso(row.created_at),
    }


def _feed_variant_out(row: MessageVariant) -> dict[str, Any]:
    full = _variant_out(row)
    return {name: full[name] for name in FEED_VARIANT_FIELDS}


def _in_reader_language(
    states: dict[UUID, tuple[VariantState, MessageVariant | None]],
    message_id: UUID,
) -> dict[str, Any]:
    """`variant` and `variant_state` of one message for a reader -- the
    two keys the feed's items and a list row's last_message carry, from
    ONE reader_states (app/variants.py): one decision, two places."""
    state, variant = states[message_id]
    return {
        "variant": _feed_variant_out(variant) if variant else None,
        "variant_state": state.value,
    }


def _checked_locale(locale: str | None) -> str | None:
    """A reader's language, of the variant form, or a 422."""
    if locale is not None and not re.fullmatch(VARIANT_LOCALE_PATTERN, locale):
        raise ValidationError(
            f"locale must match {VARIANT_LOCALE_PATTERN}, got {locale!r}"
        )
    return locale


# The fields of a list row's last_message (MSG-1): the message as the
# feed gives it, but its thread -- the row IS the thread.
LAST_MESSAGE_FIELDS = ("id", "sender", "body", "created_at")


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------
# THE RULE FOR EVERY BODY BELOW (R-2 items 2, 3, 6), generalized from
# SectionIn, which carried it alone:
#
#   1. Every field with a column behind it is bounded by that COLUMN's
#      constant, so an over-long value is a 422 AT THE EDGE rather
#      than a database error mid-transaction. Before this, a 5001-
#      character message body, a 600-character title or a priority
#      past the Integer range all passed the model and came back to
#      the caller as a 500 -- which tells an integrator the service
#      broke, on input only they can fix.
#   2. extra="forbid" everywhere. A misspelled field used to be
#      accepted and dropped: the product got a 200 for a value that
#      went nowhere. Silence is the worse failure -- the same reason
#      already written into RecipientSnapshot and PreferencesPatch.
#   3. min_length is for fields whose emptiness is meaningless, NOT
#      for every string: a section nobody can name, a message nobody
#      can read. An empty title is simply a thread without a title.
class ThreadCreateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client: UUID
    operator_kind: OperatorKind
    operator_value: UUID
    kind: ThreadKind
    subject_type: str | None = Field(
        default=None, max_length=MAX_SUBJECT_TYPE_LEN,
    )
    subject_id: str | None = Field(
        default=None, max_length=MAX_SUBJECT_ID_LEN,
    )
    title: str | None = Field(default=None, max_length=MAX_THREAD_TITLE_LEN)
    priority: int | None = Field(
        default=None, ge=MIN_THREAD_PRIORITY, le=MAX_THREAD_PRIORITY,
    )


class SectionIn(BaseModel):
    # Widths mirror the column definitions (MAX_SECTION_KEY_LEN /
    # MAX_SECTION_LABEL_LEN) so an over-long value is a 422 at the
    # edge, not a database error mid-transaction. min_length=1 makes
    # an empty string a rejection rather than a section nobody can
    # name -- the column is NOT NULL but "" would satisfy it.
    model_config = ConfigDict(extra="forbid")

    key: str = Field(min_length=1, max_length=MAX_SECTION_KEY_LEN)
    label: str = Field(min_length=1, max_length=MAX_SECTION_LABEL_LEN)


_LOCALE_FIELD = Field(pattern=VARIANT_LOCALE_PATTERN)
_JOB_KEY_FIELD = Field(min_length=1, max_length=MAX_IDEMPOTENCY_KEY_LEN)


def _not_blank(name: str, value: str) -> str:
    if not value.strip():
        raise ValueError(f"{name} must contain a non-whitespace character")
    return value


class VariantRequestIn(BaseModel):
    """'A variant of this message in `locale` was asked for, by the job
    `job_key`' (P5-4)."""

    model_config = ConfigDict(extra="forbid")

    locale: str = _LOCALE_FIELD
    job_key: str = _JOB_KEY_FIELD

    @field_validator("job_key")
    @classmethod
    def _job_key_not_blank(cls, value: str) -> str:
        return _not_blank("job_key", value)


class VariantIn(BaseModel):
    """The variant in `locale`, in exactly one of three forms (P5-4):
      - the text: `body`, and `source_locale` -- the language the
        model found the message written in -- other than `locale`;
      - none is needed: `source_locale` equal to `locale`, no body;
      - declined: `declined: true`, no body; `source_locale` may be
        given, never equal to `locale`.
    Any other combination is a 422 naming what is wrong."""

    model_config = ConfigDict(extra="forbid")

    locale: str = _LOCALE_FIELD
    job_key: str = _JOB_KEY_FIELD
    source_locale: str | None = Field(default=None, pattern=VARIANT_LOCALE_PATTERN)
    body: str | None = Field(
        default=None, min_length=1, max_length=MAX_MESSAGE_BODY_LEN,
    )
    declined: bool = Field(default=False, strict=True)

    @field_validator("job_key")
    @classmethod
    def _job_key_not_blank(cls, value: str) -> str:
        return _not_blank("job_key", value)

    @field_validator("body")
    @classmethod
    def _body_not_blank(cls, value: str | None) -> str | None:
        return None if value is None else _not_blank("body", value)

    @model_validator(mode="after")
    def _one_form(self) -> "VariantIn":
        same = self.source_locale == self.locale
        if self.declined:
            if self.body is not None:
                raise ValueError("a declined variant has no body")
            if same:
                raise ValueError(
                    "declined and source_locale equal to locale contradict: "
                    "a message in the reader's language needs no variant"
                )
        elif self.source_locale is None:
            raise ValueError(
                "source_locale is required: the language the model found "
                "the message written in"
            )
        elif same and self.body is not None:
            raise ValueError(
                "source_locale equal to locale means no variant is needed: "
                "send no body"
            )
        elif not same and self.body is None:
            raise ValueError("body is required for a variant in another language")
        return self


class MessageIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sender: UUID
    body: str = Field(min_length=1, max_length=MAX_MESSAGE_BODY_LEN)

    @field_validator("body")
    @classmethod
    def _not_only_whitespace(cls, value: str) -> str:
        """A body of spaces is a message nobody can read.

        min_length=1 alone would let "   " through, and it lands as a
        real row in a real feed: the other side is notified, the thread
        is bumped to the top, and there is nothing to see. Rejected,
        not trimmed -- silently editing a person's message is a
        separate decision, and leading whitespace can be meant (an
        indented snippet).
        """
        if not value.strip():
            raise ValueError(
                "body must contain at least one non-whitespace character"
            )
        return value


class ClaimIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operator: UUID


class AssignIn(BaseModel):
    """`operator` is the ACTOR, as on every operator verb; `assignee` is
    the operator the thread is handed to."""

    model_config = ConfigDict(extra="forbid")

    operator: UUID
    assignee: UUID


class ReleaseIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operator: UUID


class StatusIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operator: UUID
    status: ThreadStatus


class RetagIn(BaseModel):
    """The subject is read by PRESENCE (D1 / R1): both fields absent --
    keep the thread's subject; both present and null -- clear it; both
    present with values -- set it. Any other combination is a 422: half
    a reference names nothing."""

    model_config = ConfigDict(extra="forbid")

    operator: UUID
    section: UUID
    subject_type: str | None = Field(
        default=None, max_length=MAX_SUBJECT_TYPE_LEN,
    )
    subject_id: str | None = Field(
        default=None, max_length=MAX_SUBJECT_ID_LEN,
    )

    @model_validator(mode="after")
    def _both_or_neither(self) -> "RetagIn":
        present = {"subject_type", "subject_id"} & self.model_fields_set
        if len(present) == 1:
            raise ValueError(
                "subject_type and subject_id are sent together or not at "
                f"all; got only {present.pop()}"
            )
        if present and (self.subject_type is None) != (self.subject_id is None):
            raise ValueError(
                "subject_type and subject_id are both null (clear) or both "
                "set; got one of each"
            )
        return self

    def subject(self) -> SubjectRef | KeepSubject | None:
        """The three states, for retag_thread."""
        if "subject_type" not in self.model_fields_set:
            return KEEP
        if self.subject_type is None or self.subject_id is None:
            return None
        return SubjectRef(self.subject_type, self.subject_id)


class ReadIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    participant: UUID
    last_read_at: datetime | None = None


class UnreadCountsIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    participant: UUID
    # max_length is measured on the RAW list, BEFORE the handler
    # de-duplicates it: 120 ids of which 80 are distinct is a 422, not
    # a 200. A limit applied after dedup would depend on the CONTENT of
    # the request rather than its shape, and a caller could not predict
    # from the contract whether their list is acceptable.
    thread_ids: list[UUID] = Field(max_length=UNREAD_COUNTS_MAX_THREAD_IDS)


# ---------------------------------------------------------------------------
# Endpoints -- threads
# ---------------------------------------------------------------------------
async def _replay[M: (Thread, Message)](
    session: AsyncSession,
    model: type[M],
    key: str,
    fingerprint: str,
) -> M | None:
    """The row a key already created, if its request was THIS one.

    None when the key is unused. The same key with another request is a
    conflict (409) -- never the old row returned as if it were new.
    """
    row = await session.scalar(select(model).where(model.idempotency_key == key))
    if row is None:
        return None
    if row.fingerprint != fingerprint:
        raise IdempotencyConflictError(
            f"the Idempotency-Key {key!r} was used for another request"
        )
    return row


async def _require_thread(session: AsyncSession, thread_id: UUID) -> Thread:
    """Load a thread for a write, or 404. Fetched before the authz check
    so a role decision is made against real thread state."""
    thread = await session.get(Thread, thread_id)
    if thread is None:
        raise NotFoundError(f"thread {thread_id} does not exist")
    return thread




@router.post("")
async def create_thread(
    session: DbSession,
    request: Request,
    payload: ThreadCreateIn = Body(...),
    idempotency_key: str | None = Header(default=None, alias=IDEMPOTENCY_HEADER),
) -> dict[str, Any]:
    """Create a thread, or return the existing one per the dedup key.

    ADDITIVE `created` (seam T2 / ID-10): create-or-get hides whether
    this call inserted the row or found it, so the product could not
    tell "a conversation just started" from "the same conversation
    again" -- and it cannot re-derive it without racing. The flag is
    True exactly once per thread, for the caller that inserted it, so
    the product can act on its OWN request (write its diary entry)
    without comms pushing anything back.

    The key is attached HERE, not inside _thread_out: that serializer
    is shared with the list / claim / assign / release / status / retag
    responses, whose shape is FROZEN (3b) -- a key enters it only by a
    decision of its own. One has: `revision`, in 3.12.0 (RV-1), asked
    for by a product to order two answers about one thread; the next
    key is again a separate decision, not a freedom to add fields.
    """
    key = require_key(idempotency_key)
    fingerprint = await request_fingerprint(request)
    replayed = await _replay(session, Thread, key, fingerprint)
    if replayed is not None:
        # The creating request again (app/api/idempotency.py): the SAME
        # thread in its CURRENT state. `created` stays true for it --
        # the flag belongs to the request, and this is that request.
        return {**_thread_out(replayed), "created": True}
    try:
        async with session.begin_nested():
            thread, created = await create_or_get_thread_detailed(
                session,
                client=payload.client,
                operator_kind=payload.operator_kind,
                operator_value=payload.operator_value,
                kind=payload.kind,
                subject_type=payload.subject_type,
                subject_id=payload.subject_id,
                title=payload.title,
                priority=payload.priority,
                idempotency_key=key,
                fingerprint=fingerprint,
            )
    except IntegrityError as exc:
        if not is_unique_violation_of(exc, _THREADS_KEY_INDEX):
            raise
        # A concurrent request with this key won the insert.
        winner = await _replay(session, Thread, key, fingerprint)
        assert winner is not None
        return {**_thread_out(winner), "created": True}
    return {**_thread_out(thread), "created": created}


@sections_router.post("")
async def create_section(
    session: DbSession,
    payload: SectionIn = Body(...),
) -> dict[str, Any]:
    """Create a section for this key, or return the existing one.

    CREATE-OR-FIND, not upsert. An existing section comes back exactly
    as it is: `label` is NOT updated. Posting a known key with a new
    label is a successful no-op returning the OLD label -- not a
    rename, not a conflict. That is the domain function's documented
    behaviour and it is repeated here because the opposite is what a
    consumer will assume, and the assumption fails silently: the call
    succeeds, the name does not change, and there is nothing to debug
    because nothing is broken.

    Concurrency is the DATABASE's business, not this handler's: the
    unique index on key turns a losing race into an IntegrityError that
    the domain function catches and resolves to the winner's row,
    inside a SAVEPOINT so this request's transaction stays usable.

    NO `created` FLAG, unlike POST /threads -- deliberately, because
    here it could not be honest. The thread endpoint gets its flag from
    create_or_get_thread_detailed, which knows which branch it took;
    the section function returns only the row. Deriving the flag out
    here would mean probing for the key first and reporting "created"
    when the probe found nothing -- and under a race BOTH callers probe
    nothing, so BOTH would report created=True for a single row. A flag
    that can be true twice is worse than no flag: the thread flag's
    entire value is that it is true exactly once. The honest way to add
    it is a `_detailed` variant in app/messaging/sections.py returning
    (section, created), mirroring the thread function -- out of scope
    here, named in the report as a candidate.
    """
    section = await get_or_create_section(
        session,
        key=payload.key,
        label=payload.label,
    )
    return _section_out(section)


@router.get("")
async def list_threads(
    operator: UUID | None = Query(default=None),
    client: UUID | None = Query(default=None),
    is_supervisor: bool = Query(default=False),
    with_unread: bool = Query(default=False),
    with_last_message: bool = Query(default=False),
    locale: str | None = Query(default=None),
    limit: int = Query(default=PAGE_LIMIT_DEFAULT),
    cursor: str | None = Query(default=None),
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, Any]:
    """A reader's threads, most-recently-active first: the OPERATOR's
    visible threads, or -- with `client` instead (MSG-1) -- the threads
    in which that person is the client. Exactly one of the two.

    THE CLIENT LIST (MSG-1): the same order, keyset and cursor as the
    operator's (app/messaging/operators.py _activity_page); `unread` is
    counted FOR THE CLIENT, through the same participation rule -- the
    client takes part in every thread of their list, so every row
    carries it. `is_supervisor` widens an operator's read and means
    nothing for a client: with `client` it is a 422. The client is
    TRUSTED exactly as `operator` is (below).

    THE LAST MESSAGE (MSG-1), STRICTLY OPT-IN like `unread`: with
    with_last_message every row gains "last_message" -- the first row
    its feed would give (id, sender, body, created_at), or null for a
    thread with no message -- read for the whole page in one statement.
    With `locale` too, last_message carries `variant` and
    `variant_state` from the SAME reader_states as the feed. `locale`
    without with_last_message is a 422: it would apply to nothing.

    READ-SCOPING IS THE PROXY'S JOB (frozen trust model). `operator`
    and especially `is_supervisor` are TRUSTED query params: comms does
    not know who is a supervisor (no role registry -- only the product
    does), so it accepts the assertion. `is_supervisor=true` WIDENS the
    read to EVERY thread. Phase 6 MUST derive both server-side from its
    authenticated session and MUST NEVER let a client set is_supervisor
    -- forwarding a client-supplied value here is a full read-authz
    bypass. Same for the thread_id-only read endpoints (feed / unread).

    ADDITIVE `unread`, STRICTLY OPT-IN (T-51). Without with_unread the
    response is unchanged BYTE FOR BYTE -- the 3b shape is frozen and
    the product proxy mirrors it. With it, each row the `operator`
    takes part in gains "unread": n, counted FOR THAT OPERATOR, so a
    chat list needs one call instead of one per row.

    The key is attached HERE, not inside _thread_out, for the same
    reason `created` is (see create_thread): that serializer is shared
    with claim / status / retag, whose shapes are frozen.

    NO KEY ON A ROW THE OPERATOR DOES NOT TAKE PART IN. Visibility is
    wider than participation, so this is a normal row, not an oddity:
    an UNCLAIMED SECTION THREAD sits in every operator's pool and
    belongs to none of them (assignee empty, operator_value is a
    section id) -- and with is_supervisor=true every foreign thread
    arrives as well. An absent key says "not yours"; a zero would say
    "nothing unread here" and be indistinguishable from a thread the
    operator has fully read.
    """
    if (operator is None) == (client is None):
        raise ValidationError(
            "exactly one of operator / client: the reader whose threads "
            "are listed"
        )
    if client is not None and is_supervisor:
        raise ValidationError(
            "is_supervisor widens an operator's read; a client's list has "
            "no such widening"
        )
    if locale is not None and not with_last_message:
        raise ValidationError(
            "locale applies to the last message: send with_last_message=true"
        )
    _checked_locale(locale)
    if client is not None:
        reader = client
        threads, next_cursor = await list_client_threads(
            session,
            client=client,
            limit=page_limit(limit),
            cursor=decode_cursor(cursor),
        )
    else:
        assert operator is not None  # exactly one of the two, above
        reader = operator
        threads, next_cursor = await list_visible_threads(
            session,
            operator=operator,
            is_supervisor=is_supervisor,
            limit=page_limit(limit),
            cursor=decode_cursor(cursor),
        )
    rows = [_thread_out(t) for t in threads]
    if with_unread:
        counts = await unread_counts_for_participant(
            session,
            participant=reader,
            thread_ids=[t.id for t in threads],
        )
        rows = [
            ({**row, "unread": counts[thread.id]} if thread.id in counts
             else row)
            for row, thread in zip(rows, threads, strict=True)
        ]
    if with_last_message:
        last = await last_messages(session, [t.id for t in threads])
        states = (
            await reader_states(
                session, [message.id for message in last.values()], locale,
            )
            if locale is not None else {}
        )

        def _last(thread: Thread) -> dict[str, Any] | None:
            message = last.get(thread.id)
            if message is None:
                return None
            full = _message_out(message)
            out = {name: full[name] for name in LAST_MESSAGE_FIELDS}
            if locale is not None:
                out.update(_in_reader_language(states, message.id))
            return out

        rows = [
            {**row, "last_message": _last(thread)}
            for row, thread in zip(rows, threads, strict=True)
        ]
    # The one listing shape (app/api/paging.py): `items`, as everywhere.
    return {
        "items": rows,
        "next_cursor": encode_cursor(next_cursor) if next_cursor else None,
    }


# Declared ABOVE every /{thread_id} route: a literal path segment must
# not be reachable as a thread_id. There is no bare POST /{thread_id}
# today, so nothing shadows it either way -- the ordering is what keeps
# that true when one is added.
@router.post("/unread-counts")
async def unread_counts(
    payload: UnreadCountsIn = Body(...),
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, dict[str, int]]:
    """Unread counts for a list of threads, for one participant.

    READ-SCOPING IS THE PROXY'S JOB (frozen trust model). `participant`
    is a TRUSTED body field: comms does not verify that the caller is
    that participant -- the shared service token authenticates the
    PRODUCT (arch decision 14). Phase 6 MUST substitute it server-side
    from its own authenticated session and MUST NEVER accept it from
    the client; forwarding a client-supplied participant here lets one
    user read another's unread state.

    POST because the input is a list -- semantically this is a READ and
    it is idempotent: the same body returns the same counts and changes
    nothing. A repeated thread_id collapses to one entry (the response
    is keyed by thread id), it does not double a count.

    A THREAD THE PARTICIPANT DOES NOT TAKE PART IN IS ABSENT from
    `counts` -- not zero, and not an error for the whole batch. So is
    an id that matches no thread: one rule, not two. See the module
    header for why absence rather than zero.

    # KNOWN CEILING (this contract disagrees with
    # GET /threads/{id}/unread-count; acknowledged by design):
    #   1. Mechanics: the per-thread endpoint does NOT check
    #      participation -- it answers "what is the count for this
    #      (thread, participant) pair" and returns a real number to a
    #      non-participant. This batch endpoint, and the summary,
    #      answer "which of these threads are yours, and what is
    #      unread in them", so they omit the row instead. Same input,
    #      two different numbers, on purpose.
    #   2. Status: acknowledged by design.
    #   3. Backlog ref: none -- the divergence is the contract, not a
    #      deferred fix.
    #   4. Promotion trigger: a consumer needs one endpoint to answer
    #      the other's question -- concretely, a product that renders a
    #      badge from the per-thread endpoint on threads the reader
    #      does not take part in.
    #   5. Agreed fix: a new, explicitly-named contract for whichever
    #      question is missing; never a silent change of an existing
    #      one.
    #   6. Rejected: "aligning" the per-thread endpoint to this rule.
    #      Its shape is frozen (3b) and the product proxy consumes it
    #      as-is, so turning its answer into a zero would make an
    #      additive release a breaking one -- and would break it
    #      silently, since the response shape would not change at all.
    """
    counts = await unread_counts_for_participant(
        session,
        participant=payload.participant,
        thread_ids=payload.thread_ids,
    )
    return {"counts": {str(tid): n for tid, n in counts.items()}}


# ---------------------------------------------------------------------------
# Endpoints -- messages
# ---------------------------------------------------------------------------
@router.delete("/{thread_id}", status_code=204)
async def delete_thread_route(
    session: DbSession,
    thread_id: UUID,
) -> Response:
    """Delete a thread with its messages, their variants and requests,
    and its read pointers (TK-2) -- the product's call, no actor.

    204 whether the thread was there or not: a repeat after a lost
    response, or a thread already gone, ends where the first call did.
    deploy/INTEGRATION.md section 14, "Deleting a thread"."""
    await delete_thread(session, thread_id=thread_id)
    return Response(status_code=204)


@router.post("/{thread_id}/messages")
async def post_thread_message(
    session: DbSession,
    request: Request,
    thread_id: UUID,
    payload: MessageIn = Body(...),
    idempotency_key: str | None = Header(default=None, alias=IDEMPOTENCY_HEADER),
) -> dict[str, Any]:
    """Append a message AND ping the other side in one transaction.

    Idempotent under its key (app/api/idempotency.py): a repeat after a
    lost response returns the same message and pings nobody again.
    """
    key = require_key(idempotency_key)
    fingerprint = await request_fingerprint(request)
    replayed = await _replay(session, Message, key, fingerprint)
    if replayed is not None:
        return _message_out(replayed)
    # TAKEN, not read (TK-2): FOR NO KEY UPDATE -- the message writes the
    # thread's row. A deletion running now is waited for and this is a
    # 404; one that comes second waits for this message and takes it.
    # The sender's role -- and the client's reopen -- are decided on the
    # row as it is under the lock (closes TK-1's observation 2;
    # tests/test_thread_delete.py TestRaces).
    thread = await take_thread(session, thread_id)
    if not can_post_message(thread, payload.sender):
        raise AuthorizationError(
            "sender is neither a participant nor the serving operator "
            "of this thread"
        )
    try:
        async with session.begin_nested():
            message = await post_message(
                session,
                thread_id=thread_id,
                sender=payload.sender,
                body=payload.body,
                idempotency_key=key,
                fingerprint=fingerprint,
            )
    except IntegrityError as exc:
        if not is_unique_violation_of(exc, _MESSAGES_KEY_INDEX):
            raise
        # A concurrent request with this key won: its message, no ping.
        winner = await _replay(session, Message, key, fingerprint)
        assert winner is not None
        return _message_out(winner)
    await notify_new_message(session, thread=thread, message=message)
    return _message_out(message)


@router.get("/{thread_id}/messages")
async def get_thread_feed(
    thread_id: UUID,
    limit: int = Query(default=PAGE_LIMIT_DEFAULT),
    cursor: str | None = Query(default=None),
    locale: str | None = Query(default=None),
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, Any]:
    """A thread's messages, newest-first, keyset-paginated.

    With `locale` -- the reader's language, a token of the variant form
    -- every item also carries `variant` and `variant_state` (P5-4,
    app/variants.py); the page is selected exactly as without it.
    Without `locale` the response is what it always was.

    An absent thread -- never created, or deleted (TK-2) -- is a 404,
    after the input's own 422s: a deleted thread reads as one that
    never existed, and neither is an empty page."""
    _checked_locale(locale)
    size, position = page_limit(limit), decode_cursor(cursor)
    await _require_thread(session, thread_id)
    messages, next_cursor = await list_thread_messages(
        session, thread_id=thread_id, limit=size, cursor=position,
    )
    if locale is None:
        return page(messages, next_cursor, _message_out)
    states = await reader_states(session, [m.id for m in messages], locale)

    def _in_locale(message: Message) -> dict[str, Any]:
        return {
            **_message_out(message),
            **_in_reader_language(states, message.id),
        }

    return page(messages, next_cursor, _in_locale)


@router.post("/{thread_id}/messages/{message_id}/variant-requests")
async def post_variant_request(
    session: DbSession,
    request: Request,
    thread_id: UUID,
    message_id: UUID,
    payload: VariantRequestIn = Body(...),
    idempotency_key: str | None = Header(default=None, alias=IDEMPOTENCY_HEADER),
) -> dict[str, Any]:
    """Record that a variant of the message in `locale` was asked for,
    by the llm job `job_key` (P5-4). Insert only, idempotent under its
    key; pings nobody."""
    row = await write_request(
        session,
        thread_id=thread_id,
        message_id=message_id,
        locale=payload.locale,
        job_key=payload.job_key,
        key=require_key(idempotency_key),
        fingerprint=await request_fingerprint(request),
    )
    return _variant_request_out(row)


@router.post("/{thread_id}/messages/{message_id}/variants")
async def post_variant(
    session: DbSession,
    request: Request,
    thread_id: UUID,
    message_id: UUID,
    payload: VariantIn = Body(...),
    idempotency_key: str | None = Header(default=None, alias=IDEMPOTENCY_HEADER),
) -> dict[str, Any]:
    """Write the variant of the message in `locale` (P5-4): one per
    language, never replaced; the original is not touched. Idempotent
    under its key; pings nobody."""
    row = await write_variant(
        session,
        thread_id=thread_id,
        message_id=message_id,
        locale=payload.locale,
        job_key=payload.job_key,
        source_locale=payload.source_locale,
        body=payload.body,
        declined=payload.declined,
        key=require_key(idempotency_key),
        fingerprint=await request_fingerprint(request),
    )
    return _variant_out(row)


# ---------------------------------------------------------------------------
# Endpoints -- thread read-state (position IN the thread)
# ---------------------------------------------------------------------------
@router.post("/{thread_id}/read")
async def mark_thread_read(
    session: DbSession,
    thread_id: UUID,
    payload: ReadIn = Body(...),
) -> dict[str, int]:
    """Advance the participant's read pointer; return the fresh unread
    count computed in the SAME transaction (race-safe badge).

    A client-supplied last_read_at is clamped to now: the pointer is
    monotonic (a past value is a no-op), and a FUTURE value would
    otherwise pre-clear the participant's own badge for messages not
    yet read. Only the caller's own badge is affected either way.

    THE THREAD IS REQUIRED FIRST (R-2 item 4), exactly as it is for
    posting a message two handlers above: without it a pointer into a
    thread that does not exist reached the read-state INSERT and came
    back as a foreign-key violation -- a 500 that tells the product
    comms is broken when the truth is "no such thread". The other half
    of the same endpoint -- a participant who is not a recipient -- is
    checked in mark_read, where that foreign key lives.
    """
    await _require_thread(session, thread_id)
    now = datetime.now(UTC)
    requested = payload.last_read_at
    if requested is not None and requested.tzinfo is None:
        requested = requested.replace(tzinfo=UTC)  # tolerate naive input
    when = min(requested, now) if requested is not None else now
    await mark_read(
        session,
        thread_id=thread_id,
        participant=payload.participant,
        last_read_at=when,
    )
    unread = await count_unread(
        session, thread_id=thread_id, participant=payload.participant
    )
    return {"unread": unread}


@router.get("/{thread_id}/unread-count")
async def thread_unread_count(
    thread_id: UUID,
    participant: UUID = Query(...),
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, int]:
    """The participant's unread count for one thread; an absent thread
    -- never created, or deleted (TK-2) -- is a 404, not a zero."""
    await _require_thread(session, thread_id)
    unread = await count_unread(
        session, thread_id=thread_id, participant=participant
    )
    return {"unread": unread}


# ---------------------------------------------------------------------------
# Endpoints -- operator verbs
# ---------------------------------------------------------------------------
@router.post("/{thread_id}/claim")
async def claim(
    session: DbSession,
    thread_id: UUID,
    payload: ClaimIn = Body(...),
) -> dict[str, Any]:
    """Claim an unassigned thread; report whether THIS call won it."""
    thread = await _require_thread(session, thread_id)
    if not await can_claim(session, thread, payload.operator):
        raise AuthorizationError(
            "thread is not claimable (only section threads are claimed)"
        )
    await claim_thread(session, thread_id=thread_id, operator=payload.operator)
    await session.refresh(thread)
    # IDEMPOTENT BY OUTCOME (F1.4): `claimed` answers "is the thread
    # yours now", not "did THIS call write it" -- a claim repeated after
    # a lost response is still yours. Held by another operator: 409.
    if thread.assignee != payload.operator:
        raise ClaimTakenError(
            f"thread {thread_id} is assigned to another operator"
        )
    return {"claimed": True, "thread": _thread_out(thread)}


@router.post("/{thread_id}/assign")
async def assign(
    session: DbSession,
    thread_id: UUID,
    payload: AssignIn = Body(...),
) -> dict[str, Any]:
    """Hand a section thread to an operator who serves its section."""
    thread = await _require_thread(session, thread_id)
    if not await can_operate(session, thread, payload.operator):
        raise AuthorizationError(
            "actor is not the serving operator of this thread"
        )
    thread = await assign_thread(
        session, thread=thread, assignee=payload.assignee
    )
    return _thread_out(thread)


@router.post("/{thread_id}/release")
async def release(
    session: DbSession,
    thread_id: UUID,
    payload: ReleaseIn = Body(...),
) -> dict[str, Any]:
    """Return a section thread to its section's pool."""
    thread = await _require_thread(session, thread_id)
    if not await can_operate(session, thread, payload.operator):
        raise AuthorizationError(
            "actor is not the serving operator of this thread"
        )
    thread = await release_thread(session, thread=thread)
    return _thread_out(thread)


@router.post("/{thread_id}/status")
async def change_status(
    session: DbSession,
    thread_id: UUID,
    payload: StatusIn = Body(...),
) -> dict[str, Any]:
    """Apply a manual status transition (D5 matrix)."""
    # Taken, not read (TK-1): the role is decided on the row set_status
    # will change, so a hand-over landing in between cannot leave the
    # previous holder deciding on a thread that is no longer theirs.
    thread = await take_thread(session, thread_id)
    if not await can_operate(session, thread, payload.operator):
        raise AuthorizationError(
            "actor is not the serving operator of this thread"
        )
    thread = await set_status(
        session, thread_id=thread_id, target=payload.status
    )
    return _thread_out(thread)


@router.post("/{thread_id}/retag")
async def retag(
    session: DbSession,
    thread_id: UUID,
    payload: RetagIn = Body(...),
) -> dict[str, Any]:
    """Retag a section thread to a new section / subject_ref.

    The row is taken before WHO is decided (TK-2): a thread being
    deleted is waited for, and the role is checked on the row retag
    writes."""
    thread = await take_thread(session, thread_id)
    if not await can_operate(session, thread, payload.operator):
        raise AuthorizationError(
            "actor is not the serving operator of this thread"
        )
    thread = await retag_thread(
        session,
        thread_id=thread_id,
        section=payload.section,
        subject=payload.subject(),
    )
    return _thread_out(thread)


# ---------------------------------------------------------------------------
# Endpoints -- participant aggregates (across threads)
# ---------------------------------------------------------------------------
@participants_router.get("/{participant_id}/unread-summary")
async def participant_unread_summary_endpoint(
    participant_id: UUID,
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, Any]:
    """One participant's unread state across ALL their threads.

    READ-SCOPING IS THE PROXY'S JOB (frozen trust model).
    `participant_id` is a TRUSTED path parameter: comms does not verify
    that the caller is that participant -- the shared service token
    authenticates the PRODUCT (arch decision 14). Phase 6 MUST derive
    it server-side from its own authenticated session and MUST NEVER
    take it from the client; forwarding a client-supplied id here hands
    any user another user's unread state.

    Answers the bell in one call: has_unread for the dot,
    threads_with_unread and unread_messages for a number. has_unread is
    sugar over threads_with_unread > 0 and is kept ON PURPOSE -- a
    consumer that only draws a dot should not have to know that the
    contract has counts in it, nor decide for itself what "> 0" means.

    Threads counted are the ones the participant TAKES PART IN, by all
    three roles: client, assignee, and DM operator (operator_value on a
    user thread). Unknown participant id, or one with no threads at
    all -> zeros, not a 404: there is no foreign key on the way in and
    "you have nothing unread" is the true answer to the question asked.

    # KNOWN CEILING (an unclaimed section thread is in nobody's
    # summary; acknowledged by design):
    #   1. Mechanics: an UNCLAIMED section thread has an empty assignee
    #      and an operator_value that is a SECTION id rather than a
    #      recipient, so no participant role matches and the thread
    #      enters no one's summary. The visible consequence: a support
    #      agent's bell stays dark over an unhandled queue, which reads
    #      as "comms is broken" while it is in fact the same deferral
    #      as the push side.
    #   2. Status: acknowledged by design.
    #   3. Backlog ref: none yet -- registered by the owner at the T-67
    #      delivery. THE SIBLING MARKER THIS ONE USED TO POINT AT IS
    #      GONE: the pool-push deferral in app/notifier.py was resolved
    #      by T-67 (section membership + a push over the declared
    #      roster), so the reference was removed rather than left
    #      pointing at nothing. What that proves is exactly the
    #      distinction the old text drew -- same root, different
    #      surface: fixing the PUSH left this READ aggregate standing,
    #      because an unclaimed section thread still has no participant
    #      to aggregate for, roster or no roster.
    #   4. Promotion trigger: FIRED, and only half-consumed. Section
    #      membership now exists (messaging/membership.py), so agents
    #      CAN be resolved for a section -- the missing ingredient
    #      arrived. This aggregate was deliberately not rewritten in
    #      the same pass; the remaining trigger is a consumer that
    #      needs an unclaimed queue to raise a bell, rather than the
    #      pull list it raises today.
    #   5. Agreed fix: unchanged, and now concrete -- this aggregate
    #      counts a section thread for the operators in
    #      messaging.membership.member_ids, the SAME roster the pool
    #      push fans out over. One membership, two surfaces, as the
    #      original text required.
    #   6. Rejected: counting unclaimed section threads for every agent
    #      here. Before membership that meant materializing a broadcast
    #      audience (BL-1 territory) to answer a bell; with a roster it
    #      is no longer impossible, but it is still not free -- and it
    #      remains rejected until asked for, because an aggregate that
    #      silently changes what a badge counts is a breaking change
    #      wearing a compatible shape. Also rejected, unchanged: a
    #      stored counter -- unmaintainable for exactly this class of
    #      thread, which is why on-read counting was chosen at all.
    """
    threads_with_unread, unread_messages = await participant_unread_summary(
        session, participant=participant_id
    )
    return {
        "has_unread": threads_with_unread > 0,
        "threads_with_unread": threads_with_unread,
        "unread_messages": unread_messages,
    }
