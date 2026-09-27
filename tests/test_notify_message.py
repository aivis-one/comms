# =============================================================================
# COMMS Service -- message -> notification tests (Phase 4c item 1)
# =============================================================================
# Mapping (fork 1): client message -> assignee (support); operator
# message -> client (participant); sender never pinged; 0..2 notifs.
# Gated by the SAME Phase 2 pipeline (mute -> SUPPRESSED, SKIPPED before
# F1.3). Per-recipient
# idempotency dedups replays. Unclaimed section (assignee None) + client
# sender -> no push (KNOWN CEILING: pool-push deferred).
# =============================================================================

from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.prefs import set_category_muted
from app.engine.constants import NotificationStatus
from app.engine.models import Notification
from app.engine.service import Intake, accept_notification, resolve_notification
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Message, Thread
from app.messaging.operators import claim_thread
from app.messaging.threads import create_or_get_thread, post_message
from app.notifier import (
    TYPE_PARTICIPANT_MESSAGE,
    TYPE_SUPPORT_MESSAGE,
    notify_new_message,
)
from tests.helpers import (
    create_recipient,
    create_section,
    intake_fields,
    next_phase4c_telegram_id,
    next_seam_t2_telegram_id,
)


async def _rid(session: AsyncSession) -> UUID:
    r = await create_recipient(session, telegram_id=next_phase4c_telegram_id())
    return r.id


async def _dm(session: AsyncSession) -> tuple[Thread, UUID, UUID]:
    """DM thread; returns (thread, client_id, master_id=assignee)."""
    client = await _rid(session)
    master = await _rid(session)
    thread = await create_or_get_thread(
        session, **intake_fields(), client=client,
        operator_kind=OperatorKind.USER, operator_value=master,
        kind=ThreadKind.DM,
    )
    return thread, client, master


async def _section(session: AsyncSession) -> tuple[Thread, UUID]:
    client = await _rid(session)
    section = await create_section(session, key=f"nm-{uuid4().hex[:8]}")
    thread = await create_or_get_thread(
        session, **intake_fields(), client=client,
        operator_kind=OperatorKind.SECTION, operator_value=section.id,
        kind=ThreadKind.TICKET, subject_type="practice", subject_id="s",
    )
    return thread, client


async def _post(session: AsyncSession, thread: Thread, sender: UUID) -> Message:
    return await post_message(
        session, **intake_fields(), thread_id=thread.id, sender=sender, body="hello"
    )


class TestMapping:
    async def test_client_message_pings_assignee_support(
        self, db_session: AsyncSession
    ) -> None:
        thread, client, master = await _dm(db_session)
        msg = await _post(db_session, thread, client)
        notifs = await notify_new_message(db_session, thread=thread, message=msg)
        assert len(notifs) == 1
        assert notifs[0].type == TYPE_SUPPORT_MESSAGE
        assert notifs[0].target_value == str(master)

    async def test_operator_message_pings_client_participant(
        self, db_session: AsyncSession
    ) -> None:
        thread, client, master = await _dm(db_session)
        msg = await _post(db_session, thread, master)
        notifs = await notify_new_message(db_session, thread=thread, message=msg)
        assert len(notifs) == 1
        assert notifs[0].type == TYPE_PARTICIPANT_MESSAGE
        assert notifs[0].target_value == str(client)

    async def test_sender_never_targeted(
        self, db_session: AsyncSession
    ) -> None:
        thread, client, _master = await _dm(db_session)
        msg = await _post(db_session, thread, client)
        notifs = await notify_new_message(db_session, thread=thread, message=msg)
        assert str(client) not in {n.target_value for n in notifs}

    async def test_action_data_single_deeplink_param(
        self, db_session: AsyncSession
    ) -> None:
        """Edit 3: params carries ONLY thread_id; sender is a top-level
        template variable, never a deep-link param."""
        thread, client, _master = await _dm(db_session)
        msg = await _post(db_session, thread, client)
        notifs = await notify_new_message(db_session, thread=thread, message=msg)
        action_data = notifs[0].action_data
        assert action_data is not None
        assert action_data["params"] == {"thread_id": str(thread.id)}
        assert action_data["sender_id"] == str(client)
        assert "sender_id" not in action_data["params"]


class TestGating:
    async def test_muted_support_recipient_suppressed(
        self, db_session: AsyncSession
    ) -> None:
        thread, client, master = await _dm(db_session)
        await set_category_muted(db_session, master, "msg_support", True)
        msg = await _post(db_session, thread, client)
        (notif,) = await notify_new_message(
            db_session, thread=thread, message=msg
        )
        deliveries = await resolve_notification(db_session, notif)
        assert deliveries == []
        assert notif.status == NotificationStatus.SUPPRESSED

    async def test_unmuted_recipient_resolves_to_delivery(
        self, db_session: AsyncSession
    ) -> None:
        thread, client, master = await _dm(db_session)
        msg = await _post(db_session, thread, client)
        (notif,) = await notify_new_message(
            db_session, thread=thread, message=msg
        )
        deliveries = await resolve_notification(db_session, notif)
        assert [d.recipient_id for d in deliveries] == [master]
        assert notif.status == NotificationStatus.PROCESSING


class TestIdempotency:
    async def test_replay_is_deduped(self, db_session: AsyncSession) -> None:
        thread, client, master = await _dm(db_session)
        msg = await _post(db_session, thread, client)
        first = await notify_new_message(db_session, thread=thread, message=msg)
        second = await notify_new_message(db_session, thread=thread, message=msg)
        assert len(first) == 1
        assert second == []
        count = await db_session.scalar(
            select(func.count())
            .select_from(Notification)
            .where(Notification.idempotency_key == f"msg:{msg.id}:{master}")
        )
        assert count == 1


class TestUnclaimedSection:
    async def test_unclaimed_client_message_no_push(
        self, db_session: AsyncSession
    ) -> None:
        """assignee None + client is sender -> zero notifications
        (pool-push deferred; thread surfaces via list_visible_threads)."""
        thread, client = await _section(db_session)
        msg = await _post(db_session, thread, client)
        notifs = await notify_new_message(db_session, thread=thread, message=msg)
        assert notifs == []

    async def test_claimed_client_message_pings_assignee(
        self, db_session: AsyncSession
    ) -> None:
        thread, client = await _section(db_session)
        agent = await _rid(db_session)
        await claim_thread(db_session, thread_id=thread.id, operator=agent)
        # re-read assignee onto the thread object the notifier reads
        await db_session.refresh(thread)
        msg = await _post(db_session, thread, client)
        notifs = await notify_new_message(db_session, thread=thread, message=msg)
        assert len(notifs) == 1
        assert notifs[0].type == TYPE_SUPPORT_MESSAGE
        assert notifs[0].target_value == str(agent)


class TestSectionOperatorSide:
    """Coverage symmetry (seam T2, band 92100-92139): the operator ->
    client direction was only exercised on the user form. The mapping
    axis is the RECIPIENT'S SIDE, not the thread's operator form, so an
    agent writing from a section thread must reach the client with the
    SAME participant type a user-form operator does."""

    async def test_section_operator_message_pings_client_participant(
        self, db_session: AsyncSession
    ) -> None:
        thread, client = await _section(db_session)
        agent = await create_recipient(
            db_session, telegram_id=next_seam_t2_telegram_id()
        )
        await claim_thread(db_session, thread_id=thread.id, operator=agent.id)
        await db_session.refresh(thread)
        msg = await _post(db_session, thread, agent.id)
        notifs = await notify_new_message(db_session, thread=thread, message=msg)
        assert len(notifs) == 1
        assert notifs[0].type == TYPE_PARTICIPANT_MESSAGE
        assert notifs[0].target_value == str(client)


class TestIdempotencyGuard:
    """4c.1-A: the dedup catch is name-filtered -- the idempotency
    unique is a duplicate, any other constraint must re-raise.

    The guard lived in app/notifier.py as _is_idempotency_violation and
    was tested as a predicate over a hand-built IntegrityError. F1.2
    moved it into app/engine/service.py accept_notification, the one
    intake every producer now shares; the property is asserted there,
    through the real path, in both directions."""

    async def test_idempotency_index_is_recognized(
        self, db_session: AsyncSession,
    ) -> None:
        """A second intake under a taken key is answered, not raised."""
        fields = {
            "type": TYPE_PARTICIPANT_MESSAGE,
            "title": "T",
            "body": "B",
            "target_type": "all",
            "target_value": "*",
        }
        first = await accept_notification(
            db_session, idempotency_key="guard:k", fingerprint="a" * 64,
            **fields,
        )
        second = await accept_notification(
            db_session, idempotency_key="guard:k", fingerprint="a" * 64,
            **fields,
        )
        assert first.outcome is Intake.ACCEPTED
        assert second.outcome is Intake.DUPLICATE
        assert second.notification.id == first.notification.id

    async def test_other_constraint_is_not_swallowed(
        self, db_session: AsyncSession,
    ) -> None:
        exc = IntegrityError(
            "INSERT ...",
            {},
            Exception(
                'insert violates foreign key constraint '
                '"notifications_some_future_fkey"'
            ),
        )
        with (
            patch(
                "app.engine.service.create_notification", side_effect=exc,
            ),
            pytest.raises(IntegrityError, match="some_future_fkey"),
        ):
            await accept_notification(
                db_session, idempotency_key="guard:x", fingerprint="a" * 64,
                type=TYPE_PARTICIPANT_MESSAGE, title="T", body="B",
                target_type="all", target_value="*",
            )
