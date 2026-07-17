# =============================================================================
# COMMS Service -- Messaging: Threads (Phase 4a, items 2 + 3)
# =============================================================================
#
# create_or_get_thread -- the thread_id invariant in code: the dedup
# key is applied ONLY at creation. Three cases (arch doc §2.4):
#   - subject present  -> one thread per (client, operator, subject_ref)
#                         (kind-agnostic: a subject is the identity);
#   - subjectless dm   -> one eternal thread per (client, operator);
#   - subjectless ticket -> a fresh thread every time.
# The first two are DB-enforced by partial unique indexes (migration
# 0006); the third by the ABSENCE of a constraint. The race between two
# creators is closed by the database, not a pre-flight check: optimistic
# SELECT -> insert in a SAVEPOINT -> on the IntegrityError of OUR index,
# re-SELECT and return the existing row (mirror of the 3c dedup in
# app/transport/handlers.py).
#
# OPERATOR REFERENT VALIDATION (both forms): operator_value carries no
# hard FK (its target is polymorphic), so its referent is checked here
# BEFORE insert -- section -> a row in `sections`; user -> a row in
# `recipients` (probed by name; messaging does not import app.audience).
# This makes the operator side symmetric with `client`, which the DB
# already guards via its FK.
#
# NOT here: resolving operator_target into recipients, claim,
# visibility, status transitions (Phase 4b); the message -> notification
# path (Phase 4c). Callers commit.
# =============================================================================

from collections.abc import Sequence
from datetime import UTC, datetime
from uuid import UUID

import structlog
from sqlalchemy import column, exists, select, table, tuple_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundError, ValidationError
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Message, Section, Thread
from app.messaging.status import apply_client_message_reopen

logger = structlog.get_logger()

# Lightweight, by-name reference to the audience-owned recipients table.
# Lets the USER-form operator referent be verified without importing an
# app.audience model (package DAG: core <- messaging).
_recipients = table("recipients", column("id"))

# Names of the two dedup indexes (migration 0006). Matched against the
# IntegrityError so ONLY a dedup collision is treated as "already
# exists" -- any other integrity violation is a real bug and re-raises.
_DEDUP_INDEX_NAMES = ("uq_threads_dedup_subject", "uq_threads_dedup_dm")


async def _recipient_exists(session: AsyncSession, recipient_id: UUID) -> bool:
    """True if a recipient row with this id exists (by-name probe)."""
    return bool(
        await session.scalar(
            select(exists().where(_recipients.c.id == recipient_id))
        )
    )


async def _require_operator_referent(
    session: AsyncSession,
    operator_kind: OperatorKind,
    operator_value: UUID,
) -> None:
    """Reject creation if operator_value points at nothing (both forms).

    Compensates for operator_value having no hard FK (see module
    header). SECTION -> a `sections` row; USER -> a `recipients` row.
    """
    if operator_kind is OperatorKind.SECTION:
        if await session.get(Section, operator_value) is None:
            raise NotFoundError(
                f"operator section {operator_value} does not exist"
            )
        return
    if not await _recipient_exists(session, operator_value):
        raise NotFoundError(
            f"operator recipient {operator_value} does not exist"
        )


def _build_thread(
    *,
    client: UUID,
    operator_kind: OperatorKind,
    operator_value: UUID,
    kind: ThreadKind,
    subject_type: str | None,
    subject_id: str | None,
    title: str | None,
    priority: int | None,
) -> Thread:
    """Construct an unsaved Thread (status defaults to open in the ORM).

    D1(i) (Phase 4b): a USER thread's operator is fixed by the form, so
    its assignee is PRE-ASSIGNED to operator_value (the master) at
    creation -- assigned_at set with it (invariant: assignee set <=>
    assigned_at set). A SECTION thread is created unassigned (claimed
    later). The pre-assign is why the claim guard `assignee IS NULL`
    rejects user threads with no special-case.
    """
    assignee: UUID | None = None
    assigned_at: datetime | None = None
    if operator_kind is OperatorKind.USER:
        assignee = operator_value
        assigned_at = datetime.now(UTC)
    return Thread(
        client=client,
        operator_kind=operator_kind,
        operator_value=operator_value,
        kind=kind,
        subject_type=subject_type,
        subject_id=subject_id,
        title=title,
        priority=priority,
        assignee=assignee,
        assigned_at=assigned_at,
    )


async def _select_by_dedup_key(
    session: AsyncSession,
    *,
    client: UUID,
    operator_kind: OperatorKind,
    operator_value: UUID,
    subject_type: str | None,
    subject_id: str | None,
) -> Thread | None:
    """Find the existing thread for a dedup key (subject or dm branch).

    Only called when a dedup key applies. The predicate MATCHES the
    partial unique index that guards the case, so it returns the single
    row that index permits (subject branch is kind-agnostic).
    """
    stmt = select(Thread).where(
        Thread.client == client,
        Thread.operator_kind == operator_kind,
        Thread.operator_value == operator_value,
    )
    if subject_type is not None:
        stmt = stmt.where(
            Thread.subject_type == subject_type,
            Thread.subject_id == subject_id,
        )
    else:
        stmt = stmt.where(
            Thread.kind == ThreadKind.DM,
            Thread.subject_type.is_(None),
        )
    result: Thread | None = await session.scalar(stmt)
    return result


def _is_dedup_violation(exc: IntegrityError) -> bool:
    """True iff the IntegrityError is one of OUR dedup indexes firing."""
    message = str(exc.orig)
    return any(name in message for name in _DEDUP_INDEX_NAMES)


async def create_or_get_thread(
    session: AsyncSession,
    *,
    client: UUID,
    operator_kind: OperatorKind,
    operator_value: UUID,
    kind: ThreadKind,
    subject_type: str | None = None,
    subject_id: str | None = None,
    title: str | None = None,
    priority: int | None = None,
) -> Thread:
    """Create a thread, or return the existing one per the dedup key.

    Dedup is applied ONLY here (the thread_id invariant). Raises
    ValidationError on a half-populated subject_ref and NotFoundError
    if the operator referent does not exist (either form). See module
    header for the three dedup cases and the race handling.
    """
    if (subject_type is None) != (subject_id is None):
        # Mirror the DB CHECK with a clear error instead of a raw
        # IntegrityError -- a half subject_ref is a caller bug.
        raise ValidationError(
            "subject_ref must be both-or-neither: "
            f"subject_type={subject_type!r} subject_id={subject_id!r}"
        )

    await _require_operator_referent(session, operator_kind, operator_value)

    dedup_applies = subject_type is not None or kind is ThreadKind.DM
    if not dedup_applies:
        # Subjectless ticket -> always a fresh thread (no constraint).
        thread = _build_thread(
            client=client,
            operator_kind=operator_kind,
            operator_value=operator_value,
            kind=kind,
            subject_type=subject_type,
            subject_id=subject_id,
            title=title,
            priority=priority,
        )
        session.add(thread)
        await session.flush()
        logger.info(
            "thread_created",
            thread_id=str(thread.id),
            kind=str(kind),
            deduped=False,
        )
        return thread

    existing = await _select_by_dedup_key(
        session,
        client=client,
        operator_kind=operator_kind,
        operator_value=operator_value,
        subject_type=subject_type,
        subject_id=subject_id,
    )
    if existing is not None:
        return existing

    thread = _build_thread(
        client=client,
        operator_kind=operator_kind,
        operator_value=operator_value,
        kind=kind,
        subject_type=subject_type,
        subject_id=subject_id,
        title=title,
        priority=priority,
    )
    try:
        async with session.begin_nested():
            session.add(thread)
            await session.flush()
    except IntegrityError as exc:
        if not _is_dedup_violation(exc):
            raise
        # A concurrent creator won the race; its row is now visible.
        existing = await _select_by_dedup_key(
            session,
            client=client,
            operator_kind=operator_kind,
            operator_value=operator_value,
            subject_type=subject_type,
            subject_id=subject_id,
        )
        if existing is None:
            raise
        logger.info("thread_create_lost_race", thread_id=str(existing.id))
        return existing

    logger.info(
        "thread_created",
        thread_id=str(thread.id),
        kind=str(kind),
        deduped=True,
    )
    return thread


async def post_message(
    session: AsyncSession,
    *,
    thread_id: UUID,
    sender: UUID,
    body: str,
    created_at: datetime | None = None,
) -> Message:
    """Append a message to an existing thread and bump its activity.

    Sets Thread.last_message_at to the message time (the 4b auto-close
    socket). Raises NotFoundError if the thread does not exist. Does
    NOT emit any "message sent" event / notification / callback -- that
    is Phase 4c. `created_at` is accepted for deterministic ordering;
    it defaults to now (UTC).
    """
    thread = await session.get(Thread, thread_id)
    if thread is None:
        raise NotFoundError(f"thread {thread_id} does not exist")

    when = created_at if created_at is not None else datetime.now(UTC)
    message = Message(
        thread_id=thread_id,
        sender=sender,
        body=body,
        created_at=when,
    )
    session.add(message)
    # Activity marker for 4b auto-close. Advance monotonically: an
    # out-of-order backfilled message must not pull the marker back.
    if thread.last_message_at is None or thread.last_message_at < when:
        thread.last_message_at = when
    # D5 auto-reopen: a CLIENT message revives a resolved/closed thread
    # to `open` (operator messages never reopen); a pending close-notify
    # is voided inside the helper. Sender identity is compared to the
    # thread's client -- the write-authz for operators lands in 4c.
    if sender == thread.client:
        apply_client_message_reopen(thread)
    await session.flush()
    logger.info(
        "message_posted",
        thread_id=str(thread_id),
        message_id=str(message.id),
    )
    return message


# Thread-feed keyset bounds (Phase 4c) -- mirror the inbox: clamp, do
# not reject. Kept local (threads.py must not import operators.py --
# operators imports threads, so a shared constant there would cycle).
MESSAGE_FEED_MAX_PAGE_SIZE = 100
MESSAGE_FEED_DEFAULT_PAGE_SIZE = 20


async def list_thread_messages(
    session: AsyncSession,
    *,
    thread_id: UUID,
    limit: int = MESSAGE_FEED_DEFAULT_PAGE_SIZE,
    cursor: tuple[datetime, UUID] | None = None,
) -> tuple[Sequence[Message], tuple[datetime, UUID] | None]:
    """Thread feed: a thread's messages, NEWEST-first, keyset-paginated
    by (created_at, id) -- the same cursor shape as the inbox and the
    visible-threads list. `limit` clamps to 1..100 (default 20);
    `cursor` is the (created_at, id) of the last row already seen and
    the next page is WHERE (created_at, id) < cursor. An absent thread
    yields an empty feed (the trust-model "empty is the honest answer").
    Returns (messages, next_cursor); next_cursor is None on the last
    page. The opaque wire cursor lives at the API edge.
    """
    limit = max(1, min(limit, MESSAGE_FEED_MAX_PAGE_SIZE))
    stmt = select(Message).where(Message.thread_id == thread_id)
    if cursor is not None:
        stmt = stmt.where(tuple_(Message.created_at, Message.id) < cursor)
    stmt = (
        stmt.order_by(Message.created_at.desc(), Message.id.desc())
        .limit(limit + 1)
    )
    rows = list(await session.scalars(stmt))

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor: tuple[datetime, UUID] | None = None
    if has_more and page:
        last = page[-1]
        next_cursor = (last.created_at, last.id)
    return page, next_cursor
