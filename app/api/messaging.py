# =============================================================================
# COMMS Service -- Messaging API (threads / messages / operator ops)
# =============================================================================
#
# Phase 4c item 3. The HTTP surface over the 4a/4b messaging domain:
# create-or-get a thread, post a message, page a thread's feed, thread
# read-state (position IN a thread -- NOT the delivery inbox "bell",
# which is a different router), and the operator verbs claim / set-
# status / retag plus the paginated visible-threads list.
#
# POSTing a message runs notify_new_message in the SAME transaction
# (fork 3): the message row and the other side's notification rows
# commit together -- never a message without its ping, never a ping
# without its message. Delivery stays async on the worker tick.
#
# CONTRACT (Phase 6 product proxy consumes as-is):
#
#   POST /api/v1/threads                      -> 200 <thread> + created
#   GET  /api/v1/threads?operator=&is_supervisor=&limit=&cursor=
#                                             -> 200 {threads, next_cursor}
#   POST /api/v1/threads/{tid}/messages       -> 200 <message>  (+ notify)
#   GET  /api/v1/threads/{tid}/messages?limit=&cursor=
#                                             -> 200 {messages, next_cursor}
#   POST /api/v1/threads/{tid}/read           -> 200 {unread}   (fresh badge)
#   GET  /api/v1/threads/{tid}/unread-count?participant=
#                                             -> 200 {unread}
#   POST /api/v1/threads/{tid}/claim          -> 200 {claimed, thread}
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
#   - limit: 1..100, default 20 (clamped, not rejected); cursor is the
#     previous page's opaque next_cursor, malformed -> 422;
#   - a bad status transition / half subject_ref -> 422; an absent
#     thread on a verb that must exist (post/status/retag/claim) -> 404.
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

import base64
import binascii
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_service_auth
from app.core.database import get_db_reader, get_db_session
from app.core.exceptions import (
    AuthorizationError,
    NotFoundError,
    ValidationError,
)
from app.messaging.constants import (
    MAX_SECTION_KEY_LEN,
    MAX_SECTION_LABEL_LEN,
    OperatorKind,
    ThreadKind,
    ThreadStatus,
)
from app.messaging.models import Message, Section, Thread
from app.messaging.operators import (
    can_claim,
    can_operate,
    can_post_message,
    claim_thread,
    list_visible_threads,
    retag_thread,
)
from app.messaging.read_state import (
    count_unread,
    mark_read,
    participant_unread_summary,
    unread_counts_for_participant,
)
from app.messaging.sections import get_or_create_section
from app.messaging.status import set_status
from app.messaging.threads import (
    create_or_get_thread_detailed,
    list_thread_messages,
    post_message,
)
from app.notifier import notify_new_message

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


# One page of a chat list, matched to the list endpoint's own page
# ceiling (VISIBLE_THREADS_MAX_PAGE_SIZE): the batch exists to serve a
# rendered list, so it takes exactly as many ids as a list can show.
UNREAD_COUNTS_MAX_THREAD_IDS = 100


# ---------------------------------------------------------------------------
# Cursor codec (opaque; parallel to the inbox codec -- same (datetime,
# UUID) keyset shape, kept separate so the inbox contract stays frozen)
# ---------------------------------------------------------------------------
def _encode_cursor(cursor: tuple[datetime, UUID]) -> str:
    when, ident = cursor
    raw = f"{when.isoformat()}|{ident}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _decode_cursor(value: str) -> tuple[datetime, UUID]:
    """Decode a wire cursor; any malformation -> ValidationError (422)."""
    try:
        raw = base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8")
        when_text, _, id_text = raw.partition("|")
        if not id_text:
            raise ValueError("missing separator")
        return datetime.fromisoformat(when_text), UUID(id_text)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValidationError(f"Malformed thread cursor: {exc}") from exc


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


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------
class ThreadCreateIn(BaseModel):
    client: UUID
    operator_kind: OperatorKind
    operator_value: UUID
    kind: ThreadKind
    subject_type: str | None = None
    subject_id: str | None = None
    title: str | None = None
    priority: int | None = None


class SectionIn(BaseModel):
    # Widths mirror the column definitions (MAX_SECTION_KEY_LEN /
    # MAX_SECTION_LABEL_LEN) so an over-long value is a 422 at the
    # edge, not a database error mid-transaction. min_length=1 makes
    # an empty string a rejection rather than a section nobody can
    # name -- the column is NOT NULL but "" would satisfy it.
    key: str = Field(min_length=1, max_length=MAX_SECTION_KEY_LEN)
    label: str = Field(min_length=1, max_length=MAX_SECTION_LABEL_LEN)


class MessageIn(BaseModel):
    sender: UUID
    body: str


class ClaimIn(BaseModel):
    operator: UUID


class StatusIn(BaseModel):
    operator: UUID
    status: ThreadStatus


class RetagIn(BaseModel):
    operator: UUID
    section: UUID
    subject_type: str | None = None
    subject_id: str | None = None


class ReadIn(BaseModel):
    participant: UUID
    last_read_at: datetime | None = None


class UnreadCountsIn(BaseModel):
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
async def _require_thread(session: AsyncSession, thread_id: UUID) -> Thread:
    """Load a thread for a write, or 404. Fetched before the authz check
    so a role decision is made against real thread state."""
    thread = await session.get(Thread, thread_id)
    if thread is None:
        raise NotFoundError(f"thread {thread_id} does not exist")
    return thread


@router.post("")
async def create_thread(
    payload: ThreadCreateIn = Body(...),
    session: AsyncSession = Depends(get_db_session),
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
    is shared with the list / claim / status / retag responses, whose
    shapes are frozen (3b) and must stay byte-for-byte.
    """
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
    )
    return {**_thread_out(thread), "created": created}


@sections_router.post("")
async def create_section(
    payload: SectionIn = Body(...),
    session: AsyncSession = Depends(get_db_session),
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
    operator: UUID = Query(...),
    is_supervisor: bool = Query(default=False),
    with_unread: bool = Query(default=False),
    limit: int = Query(default=20),
    cursor: str | None = Query(default=None),
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, Any]:
    """The operator's visible threads, most-recently-active first.

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
    decoded = _decode_cursor(cursor) if cursor is not None else None
    threads, next_cursor = await list_visible_threads(
        session,
        operator=operator,
        is_supervisor=is_supervisor,
        limit=limit,
        cursor=decoded,
    )
    rows = [_thread_out(t) for t in threads]
    if with_unread:
        counts = await unread_counts_for_participant(
            session,
            participant=operator,
            thread_ids=[t.id for t in threads],
        )
        rows = [
            ({**row, "unread": counts[thread.id]} if thread.id in counts
             else row)
            for row, thread in zip(rows, threads, strict=True)
        ]
    return {
        "threads": rows,
        "next_cursor": (
            _encode_cursor(next_cursor) if next_cursor is not None else None
        ),
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
@router.post("/{thread_id}/messages")
async def post_thread_message(
    thread_id: UUID,
    payload: MessageIn = Body(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Append a message AND ping the other side in one transaction."""
    thread = await _require_thread(session, thread_id)
    if not can_post_message(thread, payload.sender):
        raise AuthorizationError(
            "sender is neither a participant nor the serving operator "
            "of this thread"
        )
    message = await post_message(
        session,
        thread_id=thread_id,
        sender=payload.sender,
        body=payload.body,
    )
    await notify_new_message(session, thread=thread, message=message)
    return _message_out(message)


@router.get("/{thread_id}/messages")
async def get_thread_feed(
    thread_id: UUID,
    limit: int = Query(default=20),
    cursor: str | None = Query(default=None),
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, Any]:
    """A thread's messages, newest-first, keyset-paginated."""
    decoded = _decode_cursor(cursor) if cursor is not None else None
    messages, next_cursor = await list_thread_messages(
        session, thread_id=thread_id, limit=limit, cursor=decoded,
    )
    return {
        "messages": [_message_out(m) for m in messages],
        "next_cursor": (
            _encode_cursor(next_cursor) if next_cursor is not None else None
        ),
    }


# ---------------------------------------------------------------------------
# Endpoints -- thread read-state (position IN the thread)
# ---------------------------------------------------------------------------
@router.post("/{thread_id}/read")
async def mark_thread_read(
    thread_id: UUID,
    payload: ReadIn = Body(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, int]:
    """Advance the participant's read pointer; return the fresh unread
    count computed in the SAME transaction (race-safe badge).

    A client-supplied last_read_at is clamped to now: the pointer is
    monotonic (a past value is a no-op), and a FUTURE value would
    otherwise pre-clear the participant's own badge for messages not
    yet read. Only the caller's own badge is affected either way.
    """
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
    """The participant's unread count for one thread."""
    unread = await count_unread(
        session, thread_id=thread_id, participant=participant
    )
    return {"unread": unread}


# ---------------------------------------------------------------------------
# Endpoints -- operator verbs
# ---------------------------------------------------------------------------
@router.post("/{thread_id}/claim")
async def claim(
    thread_id: UUID,
    payload: ClaimIn = Body(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Claim an unassigned thread; report whether THIS call won it."""
    thread = await _require_thread(session, thread_id)
    if not await can_claim(session, thread, payload.operator):
        raise AuthorizationError(
            "thread is not claimable (only section threads are claimed)"
        )
    claimed = await claim_thread(
        session, thread_id=thread_id, operator=payload.operator
    )
    await session.refresh(thread)
    return {"claimed": claimed, "thread": _thread_out(thread)}


@router.post("/{thread_id}/status")
async def change_status(
    thread_id: UUID,
    payload: StatusIn = Body(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Apply a manual status transition (D5 matrix)."""
    thread = await _require_thread(session, thread_id)
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
    thread_id: UUID,
    payload: RetagIn = Body(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Retag a section thread to a new section / subject_ref."""
    thread = await _require_thread(session, thread_id)
    if not await can_operate(session, thread, payload.operator):
        raise AuthorizationError(
            "actor is not the serving operator of this thread"
        )
    thread = await retag_thread(
        session,
        thread_id=thread_id,
        section=payload.section,
        subject_type=payload.subject_type,
        subject_id=payload.subject_id,
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
