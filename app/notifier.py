# =============================================================================
# COMMS Service -- Messaging <-> Notification integration (Phase 4c)
# =============================================================================
#
# The NEUTRAL layer over `engine` and `messaging` (the DAG invariant
# from 4b: engine and messaging never import each other). Only this
# module, app/api/messaging.py and app/worker.py import BOTH sides.
#
# Two integration points:
#   - notify_new_message         -- item 1: a posted message pings the
#     OTHER side of the thread through the Phase 2 gate (create a
#     Notification of the right msg_* type; the engine mutes / quiet-
#     hours / SKIPPED-on-empty it like any other). Called in-process by
#     the POST handler, in the SAME session as post_message (creation
#     atomic with the message; delivery stays async on the worker tick).
#   - consume_close_notifications -- item 2: the sibling of auto-close.
#     Reads section threads flagged closed (close_notify_pending_at),
#     emits the "conversation closed" notification and clears the flag.
#
# TYPE / CATEGORY SOURCE (fork 2a): comms owns the ABSTRACT chat type
# keys below; the product PROFILE maps them to the locked msg_*
# categories (§2.5). In tests the fixture profile does it; the concrete
# VELO dictionary is a Phase 6 input. A type with NO category bypasses
# the mute gate (§2.5) -- so the profile MUST map these three; Phase 6
# adds a load-time validation for it.
# =============================================================================

from datetime import datetime
from uuid import UUID

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.engine.constants import TargetType
from app.engine.models import Notification
from app.engine.service import create_notification
from app.messaging.models import Message, Thread

logger = structlog.get_logger()

_CLOSE_NOTIFY_BATCH_SIZE = 500

# The ONE unique on notifications is the idempotency key. Filter the
# dedup IntegrityError by that index name (mirroring messaging's
# _is_dedup_violation) so a FUTURE constraint is NOT silently swallowed
# as a "duplicate" -- it re-raises and surfaces on the caller path.
_IDEMPOTENCY_INDEX_NAME = "uq_notifications_idempotency_key"


def _is_idempotency_violation(exc: IntegrityError) -> bool:
    """True iff the IntegrityError is the notifications idempotency
    unique firing (a benign dedup), not some other constraint."""
    return _IDEMPOTENCY_INDEX_NAME in str(exc.orig)

# -- Locked preference categories (§2.5, E8-canon). The gate reads a
# type's category from the profile registry; these are the values the
# profile must map the chat types to. --
CATEGORY_PARTICIPANTS = "msg_participants"
CATEGORY_SUPPORT = "msg_support"

# -- Abstract chat notification TYPE keys comms emits. The profile maps
# each to a category above (participant side -> participants, support
# side -> support; a thread-closed notice goes to the client, i.e. the
# participant side -> participants). --
TYPE_PARTICIPANT_MESSAGE = "msg.participant_message"
TYPE_SUPPORT_MESSAGE = "msg.support_message"
TYPE_THREAD_CLOSED = "msg.thread_closed"

# Navigational action for the inbox deep-link (comms-defined name; the
# product proxy interprets it). Phase 3a deep-link encoding carries AT
# MOST ONE param, <= 64 chars: "open_thread" (11) + "__" + a UUID fits.
_ACTION_OPEN_THREAD = "open_thread"

# Minimal English FALLBACK title/body (NOT domain literals -- generic
# notification chrome). The profile template overrides presentation per
# rendering channel; the in_app inbox shows these stored values.
_NEW_MESSAGE_TITLE = "New message"
_NEW_MESSAGE_BODY = ""
_THREAD_CLOSED_TITLE = "Conversation closed"
_THREAD_CLOSED_BODY = ""


def _open_thread_action_data(
    thread_id: UUID,
    *,
    sender_id: UUID | None = None,
) -> dict[str, object]:
    """Build action_data for a chat notification.

    Edit 3: `params` carries EXACTLY the one deep-link parameter
    (thread_id). Presentation variables (sender) are TOP-LEVEL keys --
    the engine's build_variables exposes them as template variables
    (SafeDict) for rendering channels; they are NOT deep-link params
    and never enter `params`.
    """
    data: dict[str, object] = {
        "action": _ACTION_OPEN_THREAD,
        "params": {"thread_id": str(thread_id)},
    }
    if sender_id is not None:
        data["sender_id"] = str(sender_id)
    return data


def _message_idempotency_key(message_id: UUID, recipient_id: UUID) -> str:
    """Dedup key for a per-recipient message ping (item 1).

    One message may ping up to two recipients; the key is per-recipient
    so both pings are distinct, and a replay of the same (message,
    recipient) is the DB's dedup arbiter (partial-unique on
    notifications.idempotency_key).
    """
    return f"msg:{message_id}:{recipient_id}"


def _close_idempotency_key(thread_id: UUID, when: datetime) -> str:
    """Dedup key for a thread-closed notice (item 2).

    Keyed by the close instant, so a re-close after a client reopen
    (a fresh close_notify_pending_at) is a distinct notification, while
    a second consumer pass over the same pending flag is deduped.
    """
    return f"close:{thread_id}:{when.isoformat()}"


async def _emit_message_notification(
    session: AsyncSession,
    *,
    thread: Thread,
    message: Message,
    recipient: UUID,
    type_key: str,
) -> Notification | None:
    """Create ONE message-ping notification, deduped on replay.

    Returns the created notification, or None when this (message,
    recipient) pair was already emitted -- the partial-unique index on
    notifications.idempotency_key is the dedup arbiter (a SAVEPOINT
    keeps the caller's session usable on the collision).
    """
    key = _message_idempotency_key(message.id, recipient)
    try:
        async with session.begin_nested():
            notification = await create_notification(
                session,
                type=type_key,
                title=_NEW_MESSAGE_TITLE,
                body=_NEW_MESSAGE_BODY,
                target_type=TargetType.USER.value,
                target_value=str(recipient),
                action_data=_open_thread_action_data(
                    thread.id, sender_id=message.sender
                ),
                idempotency_key=key,
            )
    except IntegrityError as exc:
        if not _is_idempotency_violation(exc):
            raise  # a real, non-dedup constraint -- surface it
        logger.info(
            "message_notification_deduped",
            thread_id=str(thread.id),
            recipient=str(recipient),
        )
        return None

    logger.info(
        "message_notification_created",
        thread_id=str(thread.id),
        recipient=str(recipient),
        type=type_key,
    )
    return notification


async def notify_new_message(
    session: AsyncSession,
    *,
    thread: Thread,
    message: Message,
) -> list[Notification]:
    """Item 1: ping the OTHER side of the thread for a posted message.

    Mapping (fork 1, confirmed) -- pushes are strictly to KNOWN
    recipients, never a materialized list (BL-1):
      - the client, category participant, iff the sender is NOT the
        client (the operator side wrote);
      - the assigned operator (thread.assignee), category support, iff
        assignee is set AND is NOT the sender.
    The sender never pings itself; a message yields 0..2 notifications
    (independent categories, independent mute). Each is created with a
    per-recipient idempotency key, so a recipient on both sides (or a
    replay) is pinged at most once.

    Creation is atomic with the message (same session -- fork 3); the
    engine delivers on the worker tick and applies the SAME Phase 2
    gate (mute / quiet hours / SKIPPED-on-empty) as any notification.
    Caller commits.
    """
    created: list[Notification] = []
    sender = message.sender

    if sender != thread.client:
        participant = await _emit_message_notification(
            session,
            thread=thread,
            message=message,
            recipient=thread.client,
            type_key=TYPE_PARTICIPANT_MESSAGE,
        )
        if participant is not None:
            created.append(participant)

    if thread.assignee is not None and sender != thread.assignee:
        support = await _emit_message_notification(
            session,
            thread=thread,
            message=message,
            recipient=thread.assignee,
            type_key=TYPE_SUPPORT_MESSAGE,
        )
        if support is not None:
            created.append(support)

    # KNOWN CEILING (pool-push deferred -- acknowledged by design):
    # when thread.assignee is None (an UNCLAIMED section thread) there
    # is no operator to push to, and we deliberately do NOT fan out to
    # "the section pool".
    #   1. Mechanics: pool-push = pinging every agent serving the
    #      section -> needs a MATERIALIZED agent list, which BL-1 (no
    #      set materialization) + trivial/starred section membership do
    #      not provide; it is also a broadcast -- exactly the BL-1
    #      territory deferred in 4a/4b.
    #   2. Status: acknowledged by design.
    #   3. Backlog ref: BL-1 + starred section<->operator membership.
    #   4. Promotion trigger: section operators gain a real consumer
    #      (cbshome / TP onboards, Phase 7) AND section membership is
    #      introduced -- the two arrive together.
    #   5. Agreed fix: pool-push via membership + broadcast-hardening.
    #   6. Rejected: materializing the agent list on the push path
    #      (breaks BL-1); pushing to ALL recipients without membership.
    # NOT a hole: an unclaimed thread is visible to the pool through
    # list_visible_threads (pull / inbox badge), and the push arrives
    # once it is claimed. For the v1 consumer (VELO is user-form)
    # assignee is ALWAYS set, so every client message pushes the master.

    return created


async def _emit_close_notification(session: AsyncSession, thread: Thread) -> None:
    """Create the 'conversation closed' notice for one flagged thread.

    Targets the client (participant side -> msg_participants). Deduped on
    (thread, close-instant): a replayed pass over the same flag is the
    DB's dedup arbiter, so a crash between emit and flag-clear cannot
    double-notify. Only section threads are ever flagged (4b), so this
    never reaches a user/DM thread.
    """
    when = thread.close_notify_pending_at
    if when is None:
        return  # defensive: only flagged threads are passed here
    key = _close_idempotency_key(thread.id, when)
    try:
        async with session.begin_nested():
            await create_notification(
                session,
                type=TYPE_THREAD_CLOSED,
                title=_THREAD_CLOSED_TITLE,
                body=_THREAD_CLOSED_BODY,
                target_type=TargetType.USER.value,
                target_value=str(thread.client),
                action_data=_open_thread_action_data(thread.id),
                idempotency_key=key,
            )
    except IntegrityError as exc:
        if not _is_idempotency_violation(exc):
            raise  # a real, non-dedup constraint -- surface it
        logger.info("close_notification_deduped", thread_id=str(thread.id))
        return
    logger.info(
        "close_notification_created",
        thread_id=str(thread.id),
        recipient=str(thread.client),
    )


async def consume_close_notifications(
    *,
    limit: int = _CLOSE_NOTIFY_BATCH_SIZE,
) -> int:
    """Item 2: emit the close notice for flagged section threads, clear
    the flag. Runs EVERY worker tick -- unlike the auto-close scan over
    a growing table (its own slow gate), this hits a partial index over
    a NORMALLY-EMPTY set (the flag is transient: set on close, cleared
    here), so it is cheap and prompt.

    Idempotent two ways: the flag is cleared on emit (a second pass does
    not re-scan the thread), and the idempotency key (thread + close
    instant) dedups a concurrent pass or a crash before the clear.
    A client reopen in 4b already clears the flag, so a thread reopened
    before this pass is never notified. user/DM threads are never
    flagged -> never notified. Owns its own sessions; commits per batch.

    Returns the number of threads notified this pass.
    """
    factory = get_session_factory()
    total = 0
    while True:
        async with factory() as session:
            threads = (
                await session.scalars(
                    select(Thread)
                    .where(Thread.close_notify_pending_at.is_not(None))
                    .order_by(Thread.close_notify_pending_at)
                    .limit(limit)
                )
            ).all()
            if not threads:
                break
            for thread in threads:
                await _emit_close_notification(session, thread)
                thread.close_notify_pending_at = None
            await session.commit()
            total += len(threads)
            if len(threads) < limit:
                break

    if total:
        logger.info("close_notify_pass", notified=total)
    return total
