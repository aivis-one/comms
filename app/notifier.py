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
#     Notification of the right msg_* type; the engine mutes /
#     hours / SKIPPED-on-empty it like any other). Called in-process by
#     the POST handler, in the SAME session as post_message (creation
#     atomic with the message; delivery stays async on the worker tick).
#   - consume_close_notifications -- item 2: the sibling of auto-close.
#     Reads section threads flagged closed (close_notify_pending_at),
#     emits the "conversation closed" notification and clears the flag.
#
# TYPE / CATEGORY SOURCE (fork 2a): comms owns the ABSTRACT chat type
# keys below; the product PROFILE gives each of them a preference
# category. WHICH categories those are is the product's business and
# not this service's: the loader requires every chat type to declare a
# NON-EMPTY category and checks nothing else, so two products can and do
# use different names. A type with NO category bypasses the mute gate
# (§2.5), which is the whole reason the loader enforces presence at
# startup (Release-Hardening item 3a, keys shared via
# app/core/constants.py).
#
# This paragraph used to say the profile must map the types onto a
# "locked" pair of msg_* names, and two constants below spelled them
# out. Neither was true: no code ever read the constants, and no check
# ever compared a profile against them.
# =============================================================================

from datetime import datetime
from uuid import UUID

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import (
    MSG_TYPE_PARTICIPANT_MESSAGE,
    MSG_TYPE_SUPPORT_MESSAGE,
    MSG_TYPE_THREAD_CLOSED,
)
from app.core.database import get_session_factory
from app.engine.constants import TargetType
from app.engine.models import Notification
from app.engine.service import Intake, accept_notification, canonical_fingerprint
from app.messaging.constants import OperatorKind
from app.messaging.membership import member_ids
from app.messaging.models import Message, Thread

logger = structlog.get_logger()

_CLOSE_NOTIFY_BATCH_SIZE = 500

# -- Abstract chat notification TYPE keys comms emits. The profile
# attaches a preference category to each; this service reads that
# category out of the registry at gate time and never compares it to a
# name of its own. Canonical home is app/core/constants.py
# (Release-Hardening: the profile loader validates the same three keys
# at startup and must not import this module); the legacy TYPE_* names
# stay as the stable notifier API. --
TYPE_PARTICIPANT_MESSAGE = MSG_TYPE_PARTICIPANT_MESSAGE
TYPE_SUPPORT_MESSAGE = MSG_TYPE_SUPPORT_MESSAGE
TYPE_THREAD_CLOSED = MSG_TYPE_THREAD_CLOSED

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
    recipient) is the DB's dedup arbiter (unique on
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
    recipient) pair was already emitted -- the unique index on
    notifications.idempotency_key is the dedup arbiter
    (accept_notification; its SAVEPOINT keeps the caller's session
    usable on the collision).
    """
    key = _message_idempotency_key(message.id, recipient)
    fields = {
        "type": type_key,
        "title": _NEW_MESSAGE_TITLE,
        "body": _NEW_MESSAGE_BODY,
        "target_type": TargetType.USER.value,
        "target_value": str(recipient),
        "action_data": _open_thread_action_data(
            thread.id, sender_id=message.sender
        ),
    }
    acceptance = await accept_notification(
        session,
        idempotency_key=key,
        fingerprint=canonical_fingerprint(fields),
        **fields,
    )
    if acceptance.outcome is not Intake.ACCEPTED:
        # DUPLICATE is the replay this key exists for. CONFLICT means
        # the key is held by other content -- a product key that
        # happens to equal this internal one: logged loudly, the chat
        # message itself is unaffected.
        log = (
            logger.info
            if acceptance.outcome is Intake.DUPLICATE
            else logger.error
        )
        log(
            "message_notification_deduped"
            if acceptance.outcome is Intake.DUPLICATE
            else "message_notification_key_conflict",
            thread_id=str(thread.id),
            recipient=str(recipient),
            idempotency_key=key,
        )
        return None
    notification = acceptance.notification

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
        assignee is set AND is NOT the sender;
      - T-67: when there is NO assignee and the thread is a section
        thread, the operators DECLARED as serving that section, category
        support -- the pool push. An undeclared section has an empty
        roster and yields nothing, which is the behaviour this branch
        replaced.
    The sender never pings itself; a message yields 0..N notifications
    (independent categories, independent mute) -- at most one client
    ping plus the operator side, which is one assignee OR the section
    roster, never both. Each is created with a per-recipient idempotency
    key, so a recipient on both sides (or a replay) is pinged at most
    once; that key is what makes a fan-out over a roster safe without a
    second dedup mechanism.

    Creation is atomic with the message (same session -- fork 3); the
    engine delivers on the worker tick and applies the SAME Phase 2
    gate (mute / schedule / SKIPPED-on-empty) as any notification.
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

    elif thread.assignee is None and (
        OperatorKind(thread.operator_kind) is OperatorKind.SECTION
    ):
        # THE POOL PUSH (T-67). An unclaimed section thread has no
        # assignee to ping, so the ping goes to the operators DECLARED
        # as serving that section -- the roster the product synced, in
        # membership.member_ids.
        #
        # This is the fix the deferred marker that used to stand here
        # named as agreed, arriving on its own trigger: section
        # membership exists now. What that marker REJECTED stays
        # rejected and is not what happens below -- there is no
        # materialized audience here, no "every agent", no broadcast to
        # all recipients. An undeclared section yields an EMPTY roster
        # and therefore ZERO notifications, which is precisely the
        # behaviour of this branch before it existed: a product that
        # declares nobody is pinged for nobody, and its pool is still
        # served (and seen) by everyone through list_visible_threads.
        #
        # The sender is skipped by the same rule as the assignee branch
        # above: an operator who is also a member of the section they
        # wrote into does not ping themselves. It is reachable -- staff
        # opening a request of their own are the client AND on the
        # roster.
        for operator in await member_ids(session, thread.operator_value):
            if operator == sender:
                continue
            pooled = await _emit_message_notification(
                session,
                thread=thread,
                message=message,
                recipient=operator,
                type_key=TYPE_SUPPORT_MESSAGE,
            )
            if pooled is not None:
                created.append(pooled)

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
    fields = {
        "type": TYPE_THREAD_CLOSED,
        "title": _THREAD_CLOSED_TITLE,
        "body": _THREAD_CLOSED_BODY,
        "target_type": TargetType.USER.value,
        "target_value": str(thread.client),
        "action_data": _open_thread_action_data(thread.id),
    }
    acceptance = await accept_notification(
        session,
        idempotency_key=key,
        fingerprint=canonical_fingerprint(fields),
        **fields,
    )
    if acceptance.outcome is not Intake.ACCEPTED:
        log = (
            logger.info
            if acceptance.outcome is Intake.DUPLICATE
            else logger.error
        )
        log(
            "close_notification_deduped"
            if acceptance.outcome is Intake.DUPLICATE
            else "close_notification_key_conflict",
            thread_id=str(thread.id),
            idempotency_key=key,
        )
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
