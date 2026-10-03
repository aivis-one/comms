# =============================================================================
# P2-1 -- the transition journal (spec §6.4) and the T12 resend closed
# =============================================================================
#
# Every transition of a job or a delivery leaves one row; every channel
# answer leaves one row in its OWN transaction; the journal holds no
# letter and no text of comms' own exceptions; nothing in app/ edits or
# deletes it except forgetting's one column; a letter the channel took
# is never sent twice by the pipeline's retry.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 a transition point without its record (late mute)
#                               -> TestGrid.test_late_mute_..., fence
#   M2 a comms exception's text into the record
#                               -> TestNoLetterContent
#   M3 an UPDATE of the journal outside forgetting
#                               -> TestAppendOnly
#   M4 "accepted" written in the attempt's transaction
#                               -> TestAcceptedSurvives, TestRefusalSurvives
#   M5 the "accepted" check removed from deliver
#                               -> TestAcceptedSurvives (spy: two calls)
#   M6 the attempt back on FOR UPDATE
#                               -> TestLockRule (fails by lock_timeout)
#   M7 expiry without its FOR UPDATE
#                               -> TestLockRule.test_expiry_waits_...
#   M8 a mass close writing one row instead of one per delivery
#                               -> TestMassTransitions
#   M9 forgetting clearing provider_text without the recipient filter
#                               -> TestForgetting
# =============================================================================

import ast
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import CategoryMute, Recipient
from app.core.config import settings
from app.core.database import dispose_engine, get_engine, get_session_factory
from app.engine import processor, service
from app.engine.constants import (
    ChannelAnswer,
    DeliveryStatus,
    FailureClass,
    JobWaitReason,
    JournalStep,
    JournalSubject,
    NotificationStatus,
    TargetType,
    WaitReason,
)
from app.engine.formatters import (
    ConfigurationError,
    EmailTransientError,
    RateLimitedError,
)
from app.engine.models import (
    Notification,
    NotificationDelivery,
    NotificationTransition,
)
from app.engine.processor import (
    cleanup_terminal_notifications,
    process_pending_notifications,
)
from app.engine.reminders import cancel_reminders
from app.engine.service import (
    Intake,
    accept_notification,
    close_notifications,
    create_notification,
    withdraw_recipient,
)
from tests.helpers import create_recipient, intake_fields

_REAL_ROLLUP = processor.rollup_notification
_REAL_DELIVER = processor.deliver_notification
_APP = Path(__file__).resolve().parents[1] / "app"

# Values no column, no status and no log key ever contains by itself.
_LETTER_SENTINEL = "zq7" + "LETTERVAR" + "xk4"
_COMMS_SENTINEL = "zq7" + "COMMSTEXT" + "xk4"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


class _Spy:
    """A channel that counts its calls and answers per channel."""

    def __init__(self, answers: dict[str, Any] | None = None) -> None:
        # channel -> True / False / an exception instance
        self.answers = answers or {}
        self.calls: list[str] = []

    async def deliver(
        self,
        notification: Notification,
        delivery: NotificationDelivery,
        recipient: Recipient,
    ) -> bool:
        self.calls.append(delivery.channel)
        answer = self.answers.get(delivery.channel, True)
        if isinstance(answer, BaseException):
            raise answer
        return bool(answer)


@contextmanager
def _channel(spy: _Spy) -> Iterator[None]:
    with patch("app.engine.service.get_formatter", return_value=spy):
        yield


async def _rows(notification_id: UUID) -> list[NotificationTransition]:
    async with get_session_factory()() as session:
        return list(
            (
                await session.execute(
                    select(NotificationTransition)
                    .where(NotificationTransition.notification_id == notification_id)
                    .order_by(NotificationTransition.id)
                )
            ).scalars()
        )


async def _job(notification_id: UUID) -> Notification:
    async with get_session_factory()() as session:
        return (
            await session.execute(
                select(Notification).where(Notification.id == notification_id)
            )
        ).scalar_one()


async def _deliveries(notification_id: UUID) -> list[NotificationDelivery]:
    async with get_session_factory()() as session:
        return list(
            (
                await session.execute(
                    select(NotificationDelivery).where(
                        NotificationDelivery.notification_id == notification_id
                    )
                )
            ).scalars()
        )


async def _assert_consistent(notification_id: UUID) -> None:
    """The journal's last word on the job and on each delivery is the
    row's current status: no transition went unrecorded."""
    rows = await _rows(notification_id)
    job = await _job(notification_id)
    job_rows = [r for r in rows if r.subject == JournalSubject.JOB]
    assert job_rows, "a job always has its birth row"
    assert job_rows[-1].outcome == job.status
    for delivery in await _deliveries(notification_id):
        own = [
            r
            for r in rows
            if r.subject == JournalSubject.DELIVERY
            and r.recipient_id == delivery.recipient_id
            and r.channel == delivery.channel
        ]
        assert own, f"delivery {delivery.channel} has no row"
        assert own[-1].outcome == delivery.status
        assert own[-1].attempt == delivery.attempts


async def _intake(
    session: AsyncSession,
    target: UUID | str,
    *,
    type: str = "unit_event_in_app",
    target_type: str = TargetType.USER,
    **extra: Any,
) -> UUID:
    notification = await create_notification(
        session,
        **intake_fields(),
        type=type,
        title="T",
        body="B",
        target_type=target_type,
        target_value=str(target),
        **extra,
    )
    await session.commit()
    return notification.id


async def _open_gates() -> None:
    async with get_session_factory()() as session:
        await session.execute(
            text(
                "UPDATE notifications SET pipeline_retry_at = now() - "
                "interval '1 second' WHERE pipeline_retry_at IS NOT NULL"
            )
        )
        await session.execute(
            text(
                "UPDATE notification_deliveries SET next_retry_at = now() - "
                "interval '1 second', wait_reason = wait_reason "
                "WHERE next_retry_at IS NOT NULL"
            )
        )
        await session.commit()


class _PlantedDefectError(RuntimeError):
    """An exception of comms' own, planted by the tests."""


async def _poisoned_rollup(
    session: AsyncSession,
    notification: Notification,
    step: JournalStep = JournalStep.ROLLUP,
) -> None:
    """The attempt's own rollup tears; the ceiling's fold stays real."""
    if step == JournalStep.ROLLUP:
        raise _PlantedDefectError(_COMMS_SENTINEL)
    await _REAL_ROLLUP(session, notification, step)


def _of(rows: list[NotificationTransition], **kw: Any) -> list[NotificationTransition]:
    return [r for r in rows if all(getattr(r, k) == v for k, v in kw.items())]


# -----------------------------------------------------------------------------
# Б1 (2) -- the grid: one row per transition, with its fields
# -----------------------------------------------------------------------------


class TestGrid:
    async def test_intake_is_the_birth_row(self, db_session: AsyncSession) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        rows = await _rows(nid)
        assert len(rows) == 1
        birth = rows[0]
        assert (birth.subject, birth.step, birth.outcome, birth.attempt) == (
            JournalSubject.JOB,
            JournalStep.INTAKE,
            NotificationStatus.PENDING,
            0,
        )
        assert birth.recipient_id is None and birth.channel is None

    async def test_duplicate_and_conflict_write_nothing(
        self,
        db_session: AsyncSession,
    ) -> None:
        fields = dict(
            type="unit_event_in_app",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
        )
        key = f"test:{uuid4()}"
        first = await accept_notification(
            db_session,
            idempotency_key=key,
            fingerprint="a" * 64,
            **fields,
        )
        await db_session.commit()
        again = await accept_notification(
            db_session,
            idempotency_key=key,
            fingerprint="a" * 64,
            **fields,
        )
        other = await accept_notification(
            db_session,
            idempotency_key=key,
            fingerprint="b" * 64,
            **fields,
        )
        await db_session.commit()
        assert (again.outcome, other.outcome) == (Intake.DUPLICATE, Intake.CONFLICT)
        assert len(await _rows(first.notification.id)) == 1

    async def test_nobody_resolved(self, db_session: AsyncSession) -> None:
        nid = await _intake(db_session, uuid4())
        with _channel(_Spy()):
            await process_pending_notifications()
        rows = await _rows(nid)
        assert [(r.step, r.outcome) for r in rows] == [
            (JournalStep.INTAKE, NotificationStatus.PENDING),
            (JournalStep.RESOLVE, NotificationStatus.NO_RECIPIENTS),
        ]
        await _assert_consistent(nid)

    async def test_everyone_muted_names_each_and_the_category(
        self,
        db_session: AsyncSession,
    ) -> None:
        a = await create_recipient(db_session)
        b = await create_recipient(db_session)
        for r in (a, b):
            db_session.add(CategoryMute(recipient_id=r.id, category="unit_updates"))
        nid = await _intake(db_session, "*", target_type=TargetType.ALL)
        with _channel(_Spy()):
            await process_pending_notifications()
        rows = await _rows(nid)
        gates = _of(rows, subject=JournalSubject.GATE)
        assert {g.recipient_id for g in gates} == {a.id, b.id}
        assert all(
            (g.outcome, g.category, g.channel)
            == (
                DeliveryStatus.SUPPRESSED,
                "unit_updates",
                None,
            )
            for g in gates
        )
        assert rows[-1].outcome == NotificationStatus.SUPPRESSED
        await _assert_consistent(nid)

    async def test_partly_muted_gates_some_and_births_the_rest(
        self,
        db_session: AsyncSession,
    ) -> None:
        muted = await create_recipient(db_session)
        kept = await create_recipient(db_session)
        db_session.add(CategoryMute(recipient_id=muted.id, category="unit_updates"))
        nid = await _intake(db_session, "*", target_type=TargetType.ALL)
        spy = _Spy()
        with _channel(spy):
            await process_pending_notifications()
        rows = await _rows(nid)
        assert [g.recipient_id for g in _of(rows, subject=JournalSubject.GATE)] == [
            muted.id
        ]
        births = _of(rows, subject=JournalSubject.DELIVERY, step=JournalStep.RESOLVE)
        assert [(b.recipient_id, b.outcome, b.attempt) for b in births] == [
            (kept.id, DeliveryStatus.PENDING, 0),
        ]
        assert _of(rows, subject=JournalSubject.JOB, step=JournalStep.RESOLVE)[
            0
        ].outcome == (NotificationStatus.PROCESSING)
        await _assert_consistent(nid)

    async def test_accepted_send_is_answer_then_transition_then_fold(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        with _channel(_Spy()):
            await process_pending_notifications()
        rows = await _rows(nid)
        answer, sent, fold = rows[-3:]
        assert (answer.subject, answer.outcome, answer.attempt) == (
            JournalSubject.CHANNEL,
            ChannelAnswer.ACCEPTED,
            1,
        )
        assert answer.provider_text is None
        assert (sent.subject, sent.outcome, sent.attempt) == (
            JournalSubject.DELIVERY,
            DeliveryStatus.SENT,
            1,
        )
        assert (fold.subject, fold.step, fold.outcome) == (
            JournalSubject.JOB,
            JournalStep.ROLLUP,
            NotificationStatus.SENT,
        )
        # The answer precedes the transition it led to, by id.
        assert answer.id < sent.id < fold.id
        await _assert_consistent(nid)

    async def test_late_inactive(self, db_session: AsyncSession) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        async with get_session_factory()() as session:
            job = (
                await session.execute(
                    select(Notification).where(Notification.id == nid)
                )
            ).scalar_one()
            await service.resolve_notification(session, job)
            await session.execute(
                update(Recipient)
                .where(Recipient.id == recipient.id)
                .values(active=False)
            )
            await service.deliver_notification(session, job)
            await service.rollup_notification(session, job)
            await session.commit()
        rows = await _rows(nid)
        closed = _of(rows, subject=JournalSubject.DELIVERY, step=JournalStep.DELIVER)
        assert [c.outcome for c in closed] == [DeliveryStatus.RECIPIENT_INACTIVE]
        assert not _of(rows, subject=JournalSubject.CHANNEL)
        await _assert_consistent(nid)

    async def test_late_mute_names_the_category(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        async with get_session_factory()() as session:
            job = (
                await session.execute(
                    select(Notification).where(Notification.id == nid)
                )
            ).scalar_one()
            await service.resolve_notification(session, job)
            session.add(
                CategoryMute(recipient_id=recipient.id, category="unit_updates")
            )
            await session.flush()
            await service.deliver_notification(session, job)
            await service.rollup_notification(session, job)
            await session.commit()
        closed = _of(
            await _rows(nid),
            subject=JournalSubject.DELIVERY,
            step=JournalStep.DELIVER,
        )
        assert [(c.outcome, c.category) for c in closed] == [
            (DeliveryStatus.SUPPRESSED, "unit_updates"),
        ]
        await _assert_consistent(nid)

    async def test_schedule_deferral_names_reason_and_until(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        until = datetime.now(UTC) + timedelta(hours=3)
        with (
            _channel(_Spy()),
            patch("app.engine.service.recipient_deferred_until", return_value=until),
        ):
            await process_pending_notifications()
        wait = _of(
            await _rows(nid), subject=JournalSubject.DELIVERY, step=JournalStep.DELIVER
        )
        assert [(w.outcome, w.wait_reason, w.wait_until) for w in wait] == [
            (DeliveryStatus.PENDING, WaitReason.RECIPIENT_SCHEDULE, until),
        ]
        await _assert_consistent(nid)

    async def test_permanent_refusal_class_and_sanitized_words(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        secret = "ab" * 20  # glued at runtime: no literal in key form
        refusal = ConfigurationError(
            f"provider refused on configuration (401): Bearer {secret} Forbidden"
        )
        with _channel(_Spy({"in_app": refusal})):
            await process_pending_notifications()
        rows = await _rows(nid)
        (answer,) = _of(rows, subject=JournalSubject.CHANNEL)
        assert answer.outcome == ChannelAnswer.REFUSED
        assert answer.failure_class == FailureClass.CONFIGURATION
        assert answer.provider_text is not None
        assert "(401)" in answer.provider_text and "Forbidden" in answer.provider_text
        assert "[redacted]" in answer.provider_text
        assert secret not in answer.provider_text
        assert answer.error is None
        failed = _of(rows, subject=JournalSubject.DELIVERY, step=JournalStep.DELIVER)
        assert [(f.outcome, f.failure_class, f.attempt) for f in failed] == [
            (DeliveryStatus.FAILED, FailureClass.CONFIGURATION, 0),
        ]
        await _assert_consistent(nid)

    async def test_rate_limit_deferral(self, db_session: AsyncSession) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        with _channel(_Spy({"in_app": RateLimitedError(42.0, ": slow down")})):
            await process_pending_notifications()
        rows = await _rows(nid)
        (answer,) = _of(rows, subject=JournalSubject.CHANNEL)
        assert answer.outcome == ChannelAnswer.RATE_LIMITED
        assert answer.provider_text and "slow down" in answer.provider_text
        (wait,) = _of(rows, subject=JournalSubject.DELIVERY, step=JournalStep.DELIVER)
        assert wait.wait_reason == WaitReason.PROVIDER_RATE_LIMIT
        assert wait.wait_until is not None and wait.attempt == 0
        await _assert_consistent(nid)

    async def test_provider_transient_keeps_words_and_backs_off(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        with _channel(
            _Spy({"in_app": EmailTransientError("provider error (503): busy")})
        ):
            await process_pending_notifications()
        rows = await _rows(nid)
        (answer,) = _of(rows, subject=JournalSubject.CHANNEL)
        assert (answer.outcome, answer.provider_text) == (
            ChannelAnswer.TRANSIENT,
            "provider error (503): busy",
        )
        (wait,) = _of(rows, subject=JournalSubject.DELIVERY, step=JournalStep.DELIVER)
        assert (wait.wait_reason, wait.attempt) == (WaitReason.TRANSIENT_BACKOFF, 1)
        await _assert_consistent(nid)

    async def test_comms_exception_in_the_call_is_class_and_place(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        with _channel(_Spy({"in_app": _PlantedDefectError(_COMMS_SENTINEL)})):
            await process_pending_notifications()
        (answer,) = _of(await _rows(nid), subject=JournalSubject.CHANNEL)
        assert answer.outcome == ChannelAnswer.ERROR
        assert answer.provider_text is None
        assert answer.error is not None
        assert answer.error.startswith(f"{__name__}._PlantedDefectError at ")
        await _assert_consistent(nid)

    async def test_timeout(self, db_session: AsyncSession) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)

        class _Slow:
            async def deliver(self, *args: Any) -> bool:
                import asyncio

                await asyncio.sleep(1.0)
                return True

        with (
            patch("app.engine.service.get_formatter", return_value=_Slow()),
            patch("app.engine.service._DELIVER_TIMEOUT_SECONDS", 0.05),
        ):
            await process_pending_notifications()
        (answer,) = _of(await _rows(nid), subject=JournalSubject.CHANNEL)
        assert (answer.outcome, answer.provider_text, answer.error) == (
            ChannelAnswer.TIMEOUT,
            None,
            None,
        )
        await _assert_consistent(nid)

    async def test_transient_exhausted(
        self,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "notification_max_delivery_attempts", 1)
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        with _channel(_Spy({"in_app": EmailTransientError("provider error (503)")})):
            await process_pending_notifications()
        rows = await _rows(nid)
        (failed,) = _of(rows, subject=JournalSubject.DELIVERY, step=JournalStep.DELIVER)
        assert (failed.outcome, failed.failure_class) == (
            DeliveryStatus.FAILED,
            FailureClass.TRANSIENT_EXHAUSTED,
        )
        assert rows[-1].outcome == NotificationStatus.FAILED
        await _assert_consistent(nid)

    async def test_a_fold_with_a_pending_delivery_writes_no_job_row(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        with _channel(_Spy({"in_app": EmailTransientError("provider error (503)")})):
            await process_pending_notifications()
        rows = await _rows(nid)
        assert not _of(rows, subject=JournalSubject.JOB, step=JournalStep.ROLLUP)
        assert (await _job(nid)).status == NotificationStatus.PROCESSING


# -----------------------------------------------------------------------------
# Б1 (3) -- mass transitions: one row per delivery
# -----------------------------------------------------------------------------


async def _three_waiting(
    db_session: AsyncSession, **extra: Any
) -> tuple[UUID, list[Recipient]]:
    """A job with three deliveries, all deferred by the schedule."""
    recipients = [await create_recipient(db_session) for _ in range(3)]
    nid = await _intake(db_session, "*", target_type=TargetType.ALL, **extra)
    with (
        _channel(_Spy()),
        patch(
            "app.engine.service.recipient_deferred_until",
            return_value=datetime.now(UTC) + timedelta(hours=1),
        ),
    ):
        await process_pending_notifications()
    return nid, recipients


class TestMassTransitions:
    async def test_expiry_writes_one_row_per_delivery(
        self,
        db_session: AsyncSession,
    ) -> None:
        nid, _ = await _three_waiting(db_session)
        async with get_session_factory()() as session:
            closed = await close_notifications(
                session,
                Notification.id == nid,
                NotificationStatus.EXPIRED,
            )
            await session.commit()
        assert closed == 1
        rows = _of(await _rows(nid), step=JournalStep.EXPIRE)
        assert (
            len(
                _of(
                    rows,
                    subject=JournalSubject.DELIVERY,
                    outcome=DeliveryStatus.EXPIRED,
                )
            )
            == 3
        )
        assert [r.outcome for r in _of(rows, subject=JournalSubject.JOB)] == [
            NotificationStatus.EXPIRED,
        ]
        await _assert_consistent(nid)

    async def test_expiry_of_an_unresolved_job_is_one_job_row(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        async with get_session_factory()() as session:
            await close_notifications(
                session,
                Notification.id == nid,
                NotificationStatus.EXPIRED,
            )
            await session.commit()
        rows = await _rows(nid)
        assert [(r.subject, r.step, r.outcome) for r in rows[1:]] == [
            (JournalSubject.JOB, JournalStep.EXPIRE, NotificationStatus.EXPIRED),
        ]

    async def test_a_second_close_writes_nothing(
        self,
        db_session: AsyncSession,
    ) -> None:
        nid, _ = await _three_waiting(db_session)
        for _ in range(2):
            async with get_session_factory()() as session:
                await close_notifications(
                    session,
                    Notification.id == nid,
                    NotificationStatus.EXPIRED,
                )
                await session.commit()
        assert len(_of(await _rows(nid), step=JournalStep.EXPIRE)) == 4

    async def test_cancellation_by_correlation(
        self,
        db_session: AsyncSession,
    ) -> None:
        nid, _ = await _three_waiting(db_session, correlation="corr-p2-1")
        async with get_session_factory()() as session:
            await cancel_reminders(
                session,
                types={"unit_event_in_app"},
                correlation="corr-p2-1",
            )
            await session.commit()
        rows = _of(await _rows(nid), step=JournalStep.CANCEL)
        assert (
            len(
                _of(
                    rows,
                    subject=JournalSubject.DELIVERY,
                    outcome=DeliveryStatus.CANCELLED,
                )
            )
            == 3
        )
        assert len(_of(rows, subject=JournalSubject.JOB)) == 1
        await _assert_consistent(nid)

    async def test_forgetting_closes_each_of_theirs_and_folds_each_job(
        self,
        db_session: AsyncSession,
    ) -> None:
        first, recipients = await _three_waiting(db_session)
        second = await _intake(db_session, recipients[0].id)
        with (
            _channel(_Spy()),
            patch(
                "app.engine.service.recipient_deferred_until",
                return_value=datetime.now(UTC) + timedelta(hours=1),
            ),
        ):
            await process_pending_notifications()
        async with get_session_factory()() as session:
            closed = await withdraw_recipient(session, recipients[0].id)
            await session.commit()
        assert closed == 2
        for nid in (first, second):
            rows = _of(await _rows(nid), step=JournalStep.WITHDRAW)
            assert [
                (r.recipient_id, r.outcome)
                for r in rows
                if r.subject == JournalSubject.DELIVERY
            ] == [
                (recipients[0].id, DeliveryStatus.RECIPIENT_INACTIVE),
            ]
            await _assert_consistent(nid)
        # The single-delivery job folded; the three-delivery one waits on.
        assert _of(
            await _rows(second), step=JournalStep.WITHDRAW, subject=JournalSubject.JOB
        )[0].outcome == (NotificationStatus.NO_RECIPIENTS)
        assert not _of(
            await _rows(first), step=JournalStep.WITHDRAW, subject=JournalSubject.JOB
        )


# -----------------------------------------------------------------------------
# Б1 (4) -- no letter, no comms exception text; provider words sanitized
# -----------------------------------------------------------------------------


async def _every_value(notification_id: UUID) -> list[str]:
    async with get_session_factory()() as session:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT row_to_json(t)::text FROM notification_transitions t "
                        "WHERE notification_id = :n"
                    ),
                    {"n": notification_id},
                )
            )
            .scalars()
            .all()
        )
    return list(rows)


class TestNoLetterContent:
    async def test_neither_the_letter_nor_comms_text_reaches_a_column(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(
            db_session,
            recipient.id,
            action_data={"variables": {"name": _LETTER_SENTINEL}},
        )
        with (
            _channel(_Spy({"in_app": _PlantedDefectError(_COMMS_SENTINEL)})),
            patch.object(processor, "rollup_notification", _poisoned_rollup),
        ):
            await process_pending_notifications()
        values = await _every_value(nid)
        # The pair: the rows exist and say something.
        assert len(values) >= 3
        assert all('"step"' in v and '"outcome"' in v for v in values)
        joined = "\n".join(values)
        assert _LETTER_SENTINEL not in joined
        assert _COMMS_SENTINEL not in joined
        # ...and the comms failures are there as class and place.
        assert f"{__name__}._PlantedDefectError at " in joined

    async def test_the_sentinels_are_not_substrings_of_anything_routine(self) -> None:
        for routine in ("pending", "processing", "sent", "deliver", "rollup"):
            assert _LETTER_SENTINEL not in routine
            assert _COMMS_SENTINEL not in routine
        assert _LETTER_SENTINEL and _COMMS_SENTINEL


# -----------------------------------------------------------------------------
# Б1 (5) -- append-only: no UPDATE / DELETE of the journal in app/
# -----------------------------------------------------------------------------


def _enclosing_calls(tree: ast.AST) -> Iterator[tuple[str, ast.Call]]:
    """Every call in the module with the name of its enclosing function."""
    stack: list[str] = []

    def walk(node: ast.AST) -> Iterator[tuple[str, ast.Call]]:
        is_func = isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        if is_func:
            stack.append(node.name)  # type: ignore[union-attr]
        if isinstance(node, ast.Call):
            yield (stack[-1] if stack else "<module>"), node
        for child in ast.iter_child_nodes(node):
            yield from walk(child)
        if is_func:
            stack.pop()

    yield from walk(tree)


def _call_name(call: ast.Call) -> str:
    f = call.func
    return f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", "")


def _journal_statements() -> list[tuple[str, str, str, ast.Call]]:
    """(file, function, update|delete, call) for every update(...) /
    delete(...) whose first argument is the journal model."""
    found = []
    for path in sorted(_APP.rglob("*.py")):
        tree = ast.parse(path.read_text(), str(path))
        for function, call in _enclosing_calls(tree):
            name = _call_name(call)
            if (
                name in ("update", "delete")
                and call.args
                and isinstance(
                    call.args[0],
                    ast.Name,
                )
                and call.args[0].id == "NotificationTransition"
            ):
                found.append(
                    (path.relative_to(_APP.parent).as_posix(), function, name, call)
                )
    return found


def _model_statements() -> set[tuple[str, str]]:
    found = set()
    for path in sorted(_APP.rglob("*.py")):
        tree = ast.parse(path.read_text(), str(path))
        for _, call in _enclosing_calls(tree):
            name = _call_name(call)
            if (
                name in ("update", "delete")
                and call.args
                and isinstance(
                    call.args[0],
                    ast.Name,
                )
            ):
                found.add((name, call.args[0].id))
    return found


class TestAppendOnly:
    def test_the_only_edit_is_forgetting_one_column_by_recipient(self) -> None:
        statements = _journal_statements()
        assert [(f, fn, kind) for f, fn, kind, _ in statements] == [
            ("app/engine/service.py", "withdraw_recipient", "update"),
        ]
        # The chain around that update(): .where(... recipient_id ...)
        # and .values(provider_text=None), nothing else.
        source = (_APP / "engine" / "service.py").read_text()
        tree = ast.parse(source)
        chain_values = [
            call
            for fn, call in _enclosing_calls(tree)
            if fn == "withdraw_recipient"
            and _call_name(call) == "values"
            and "NotificationTransition" in ast.unparse(call)
        ]
        (values,) = chain_values
        assert [k.arg for k in values.keywords] == ["provider_text"]
        assert ast.unparse(values.keywords[0].value) == "None"
        assert "NotificationTransition.recipient_id == recipient_id" in (
            ast.unparse(values)
        )

    def test_the_scanner_sees_statements_at_all(self) -> None:
        """The pair: the same scan finds the updates of the other tables,
        so an empty journal result is a finding, not a blind scanner."""
        seen = _model_statements()
        assert ("update", "NotificationDelivery") in seen
        assert ("delete", "Notification") in seen

    def test_no_raw_sql_edits_the_journal(self) -> None:
        pattern = re.compile(
            r"(update|delete\s+from|truncate)\s+\"?notification_transitions",
            re.IGNORECASE,
        )
        offenders = []
        literals = 0
        for path in sorted(_APP.rglob("*.py")):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    literals += 1
                    if pattern.search(node.value):
                        offenders.append(path.name)
        assert offenders == []
        assert literals > 100  # the pair: the scan read strings

    def test_orm_deletes_are_the_three_known_ones(self) -> None:
        """session.delete(obj) cannot name its table to an AST; the set of
        functions that call it is pinned, each deleting a mute, a group
        membership or a section membership -- never a journal row."""
        sites = set()
        for path in sorted(_APP.rglob("*.py")):
            for fn, call in _enclosing_calls(ast.parse(path.read_text())):
                f = call.func
                if (
                    isinstance(f, ast.Attribute)
                    and f.attr == "delete"
                    and isinstance(f.value, ast.Name)
                    and f.value.id == "session"
                ):
                    sites.add((path.relative_to(_APP.parent).as_posix(), fn))
        assert sites == {
            ("app/audience/prefs.py", "set_category_muted"),
            ("app/audience/sync.py", "_group_changed_once"),
            ("app/messaging/membership.py", "set_membership"),
        }

    async def test_rows_go_with_their_job_only(
        self,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        with _channel(_Spy()):
            await process_pending_notifications()
        assert await _rows(nid)
        monkeypatch.setattr(settings, "notification_retention_days", 1)
        async with get_session_factory()() as session:
            await session.execute(
                update(Notification)
                .where(Notification.id == nid)
                .values(created_at=datetime.now(UTC) - timedelta(days=2))
            )
            await session.commit()
        assert await cleanup_terminal_notifications() == 1
        assert await _rows(nid) == []


# -----------------------------------------------------------------------------
# Б1 (1)(7) -- the fence over transition points
# -----------------------------------------------------------------------------

_TRANSITION_ATTRS = frozenset(
    {
        "status",
        "wait_reason",
        "next_retry_at",
        "failure_class",
        "pipeline_attempts",
        "pipeline_step",
        "pipeline_error",
        "pipeline_retry_at",
    }
)

# Every function in app/ that changes a status, a wait or a pipeline
# record, and how its transitions reach the journal. A new one turns
# this red until it is classified here.
_RECORDS = "records"
_POINTS: dict[tuple[str, str], str] = {
    ("app/engine/service.py", "create_notification"): _RECORDS,
    ("app/engine/service.py", "resolve_notification"): _RECORDS,
    ("app/engine/service.py", "deliver_notification"): _RECORDS,
    ("app/engine/service.py", "rollup_notification"): _RECORDS,
    ("app/engine/service.py", "close_notifications"): _RECORDS,
    ("app/engine/service.py", "close_waiting_deliveries"): _RECORDS,
    ("app/engine/processor.py", "_record_pipeline_failure"): _RECORDS,
    # Helpers whose every caller records right after them.
    ("app/engine/service.py", "_close_accepted"): (
        "by caller: deliver_notification, close_waiting_deliveries"
    ),
    ("app/engine/service.py", "_apply_transient_failure"): (
        "by caller: deliver_notification"
    ),
    # Not a transition: the gate that let a successful attempt through
    # is cleared; the attempt's transitions are recorded by its steps.
    ("app/engine/processor.py", "process_pending_notifications"): ("not a transition"),
    # Thread statuses (messaging), not jobs or deliveries.
    ("app/messaging/status.py", "set_status"): "thread",
    ("app/messaging/status.py", "apply_client_message_reopen"): "thread",
    ("app/messaging/status.py", "auto_close_idle_threads_batch"): "thread",
}


def _points_in(
    node: ast.AST,
    rel: str,
    stack: list[str],
    points: set[tuple[str, str]],
) -> None:
    """Collect (file, function) for every transition write under node."""
    is_func = isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    if is_func:
        stack.append(node.name)  # type: ignore[union-attr]
    where = stack[-1] if stack else "<module>"
    if isinstance(node, ast.Assign | ast.AugAssign | ast.AnnAssign):
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for t in targets:
            if (
                isinstance(t, ast.Attribute)
                and t.attr in _TRANSITION_ATTRS
                and not (isinstance(t.value, ast.Name) and t.value.id == "self")
            ):
                points.add((rel, where))
    if (
        isinstance(node, ast.Call)
        and _call_name(node) in ("values", "Notification", "NotificationDelivery")
        and any(k.arg in _TRANSITION_ATTRS for k in node.keywords)
    ):
        points.add((rel, where))
    for child in ast.iter_child_nodes(node):
        _points_in(child, rel, stack, points)
    if is_func:
        stack.pop()


def _transition_points() -> set[tuple[str, str]]:
    points: set[tuple[str, str]] = set()
    for path in sorted(_APP.rglob("*.py")):
        rel = path.relative_to(_APP.parent).as_posix()
        _points_in(ast.parse(path.read_text()), rel, [], points)
    return points


def _calls_journal(rel: str, function: str) -> bool:
    tree = ast.parse((_APP.parent / rel).read_text())
    for fn, call in _enclosing_calls(tree):
        f = call.func
        if (
            fn == function
            and isinstance(f, ast.Attribute)
            and isinstance(
                f.value,
                ast.Name,
            )
            and f.value.id == "journal"
        ):
            return True
    return False


class TestEveryPointRecords:
    def test_every_transition_point_is_classified(self) -> None:
        assert _transition_points() == set(_POINTS)

    def test_every_recording_point_calls_the_journal(self) -> None:
        recording = [k for k, v in _POINTS.items() if v == _RECORDS]
        assert recording  # the pair
        assert [k for k in recording if not _calls_journal(*k)] == []

    def test_every_helper_is_followed_by_its_callers_record(self) -> None:
        for (rel, _), how in _POINTS.items():
            if not how.startswith("by caller: "):
                continue
            for caller in how.removeprefix("by caller: ").split(", "):
                assert _POINTS[(rel, caller)] == _RECORDS
                assert _calls_journal(rel, caller)


# -----------------------------------------------------------------------------
# Б1 (6), Б2 -- rollback, the accepted record, the lock rule
# -----------------------------------------------------------------------------


async def _poisoned_after_deliver(
    session: AsyncSession, notification: Notification
) -> None:
    """deliver runs for real -- the channel takes the letter -- and the
    step then tears before the attempt can commit."""
    await _REAL_DELIVER(session, notification)
    raise _PlantedDefectError("after the channel")


class _CommitTearsError(RuntimeError):
    pass


async def _rollup_then_bad_commit(
    session: AsyncSession,
    notification: Notification,
    step: JournalStep = JournalStep.ROLLUP,
) -> None:
    """The rollup is real; the commit after it violates a CHECK."""
    await _REAL_ROLLUP(session, notification, step)
    if step == JournalStep.ROLLUP:
        notification.pipeline_step = "rollup"  # attempts 0 -> CHECK fails


class TestAcceptedSurvives:
    @pytest.mark.parametrize("tear", ["deliver", "rollup", "commit"])
    async def test_the_next_attempt_closes_sent_without_a_second_call(
        self,
        db_session: AsyncSession,
        tear: str,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        spy = _Spy()
        poison = {
            "deliver": patch.object(
                processor, "deliver_notification", _poisoned_after_deliver
            ),
            "rollup": patch.object(processor, "rollup_notification", _poisoned_rollup),
            "commit": patch.object(
                processor, "rollup_notification", _rollup_then_bad_commit
            ),
        }[tear]
        with _channel(spy), poison:
            await process_pending_notifications()
        job = await _job(nid)
        assert job.pipeline_attempts == 1 and job.pipeline_step == tear
        # The attempt's own rows went with its rollback; the answer stayed.
        rows = await _rows(nid)
        assert (
            len(
                _of(
                    rows, subject=JournalSubject.CHANNEL, outcome=ChannelAnswer.ACCEPTED
                )
            )
            == 1
        )
        assert not _of(rows, subject=JournalSubject.DELIVERY)
        failure = rows[-1]
        assert (failure.subject, failure.step, failure.wait_reason) == (
            JournalSubject.JOB,
            tear,
            JobWaitReason.PIPELINE_RETRY,
        )
        assert failure.error and failure.wait_until is not None

        await _open_gates()
        with _channel(spy):
            await process_pending_notifications()
        assert spy.calls == ["in_app"]  # ONE call, ever
        (delivery,) = await _deliveries(nid)
        accepted_at = _of(rows, subject=JournalSubject.CHANNEL)[0].at
        assert (delivery.status, delivery.sent_at, delivery.attempts) == (
            DeliveryStatus.SENT,
            accepted_at,
            1,
        )
        assert (await _job(nid)).status == NotificationStatus.SENT
        await _assert_consistent(nid)

    async def test_the_ceiling_closes_accepted_sent_and_the_rest_pipeline(
        self,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The decided case: the deliveries exist (an earlier attempt
        committed the resolve); the last attempt tears after in_app was
        accepted. in_app -> SENT, telegram -> FAILED / pipeline, the job
        folded."""
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", 1)
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id, type="unit_event_telegram_in_app")
        busy = EmailTransientError("provider error (503)")
        spy = _Spy({"telegram": busy, "in_app": busy})
        with _channel(spy):
            await process_pending_notifications()
        assert (await _job(nid)).status == NotificationStatus.PROCESSING
        await _open_gates()
        spy.answers["in_app"] = True
        with (
            _channel(spy),
            patch.object(processor, "rollup_notification", _poisoned_rollup),
        ):
            await process_pending_notifications()
        by_channel = {d.channel: d for d in await _deliveries(nid)}
        assert by_channel["in_app"].status == DeliveryStatus.SENT
        assert (
            by_channel["telegram"].status,
            by_channel["telegram"].failure_class,
        ) == (
            DeliveryStatus.FAILED,
            FailureClass.PIPELINE,
        )
        assert (await _job(nid)).status == NotificationStatus.PARTIAL_SENT
        rows = _of(await _rows(nid), step=JournalStep.CEILING)
        assert {
            (r.channel, r.outcome) for r in rows if r.subject == JournalSubject.DELIVERY
        } == {
            ("in_app", DeliveryStatus.SENT),
            ("telegram", DeliveryStatus.FAILED),
        }
        assert [r.outcome for r in rows if r.subject == JournalSubject.JOB] == [
            NotificationStatus.PARTIAL_SENT,
        ]
        await _assert_consistent(nid)

    async def test_the_ceiling_of_a_first_attempt_keeps_only_the_answer(
        self,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """REPORTED, NOT DECIDED (P2-1 report): the attempt that tears
        is the job's FIRST, so its rollback also takes the deliveries
        resolve created; at the ceiling the job is PENDING without
        deliveries and closes FAILED, although the channel took the
        in_app letter. The journal keeps that answer -- the path is
        true -- while the job's status is not. Pinned as the behavior
        of this delivery, so that a decision changes it knowingly."""
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", 1)
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        with (
            _channel(_Spy()),
            patch.object(processor, "rollup_notification", _poisoned_rollup),
        ):
            await process_pending_notifications()
        assert (await _job(nid)).status == NotificationStatus.FAILED
        assert await _deliveries(nid) == []
        assert _of(
            await _rows(nid),
            subject=JournalSubject.CHANNEL,
            outcome=ChannelAnswer.ACCEPTED,
        )

    async def test_the_ceiling_of_an_unresolved_job(
        self,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", 1)
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)

        async def broken_resolve(*args: Any) -> None:
            raise _PlantedDefectError("resolve")

        with patch.object(processor, "resolve_notification", broken_resolve):
            await process_pending_notifications()
        rows = await _rows(nid)
        assert [(r.step, r.outcome) for r in rows] == [
            (JournalStep.INTAKE, NotificationStatus.PENDING),
            (JournalStep.RESOLVE, NotificationStatus.PENDING),
            (JournalStep.CEILING, NotificationStatus.FAILED),
        ]
        assert rows[1].error is not None and rows[1].wait_until is None
        await _assert_consistent(nid)

    async def test_expiry_after_a_torn_attempt_closes_the_accepted_sent(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id, type="unit_event_telegram_in_app")
        spy = _Spy({"telegram": EmailTransientError("provider error (503)")})
        # The attempt tears after in_app was accepted; expiry comes
        # before any further attempt sends.
        with (
            _channel(spy),
            patch.object(processor, "rollup_notification", _poisoned_rollup),
        ):
            await process_pending_notifications()
        # Resolve again (the rollback took the deliveries), no sending.
        async with get_session_factory()() as session:
            job = (
                await session.execute(
                    select(Notification).where(Notification.id == nid).with_for_update()
                )
            ).scalar_one()
            await service.resolve_notification(session, job)
            await session.commit()
        async with get_session_factory()() as session:
            await close_notifications(
                session,
                Notification.id == nid,
                NotificationStatus.EXPIRED,
            )
            await session.commit()
        by_channel = {d.channel: d.status for d in await _deliveries(nid)}
        assert by_channel == {
            "in_app": DeliveryStatus.SENT,
            "telegram": DeliveryStatus.EXPIRED,
        }
        assert (await _job(nid)).status == NotificationStatus.PARTIAL_SENT
        assert spy.calls.count("in_app") == 1
        await _assert_consistent(nid)


class TestRefusalSurvives:
    async def test_a_401_in_an_attempt_that_tears_is_in_the_journal(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        refusal = ConfigurationError(
            "provider refused on configuration (401): Forbidden"
        )
        with (
            _channel(_Spy({"in_app": refusal})),
            patch.object(processor, "rollup_notification", _poisoned_rollup),
        ):
            await process_pending_notifications()
        rows = await _rows(nid)
        (answer,) = _of(rows, subject=JournalSubject.CHANNEL)
        assert (answer.outcome, answer.failure_class) == (
            ChannelAnswer.REFUSED,
            FailureClass.CONFIGURATION,
        )
        assert answer.provider_text and "Forbidden" in answer.provider_text
        # The FAILED transition itself was rolled back with the attempt.
        assert not _of(rows, subject=JournalSubject.DELIVERY)
        assert (await _job(nid)).status == NotificationStatus.PENDING


@contextmanager
def _lock_timeout(ms: int) -> Iterator[None]:
    """Every connection the app checks out waits at most `ms` for a
    lock: a lock conflict FAILS the test instead of hanging it."""
    sync_engine = get_engine().sync_engine

    def on_checkout(dbapi_conn: Any, record: Any, proxy: Any) -> None:
        dbapi_conn.run_async(lambda c: c.execute(f"SET lock_timeout = '{ms}ms'"))

    event.listen(sync_engine, "checkout", on_checkout)
    try:
        yield
    finally:
        event.remove(sync_engine, "checkout", on_checkout)


class TestLockRule:
    """Every connection in these tests waits at most 2 s for a lock, so
    a lock conflict FAILS a test instead of hanging the suite -- for
    every pass of the pipeline in the test, not only the one under
    scrutiny."""

    @pytest.fixture(autouse=True)
    async def bounded_lock_waits(self) -> Any:
        try:
            with _lock_timeout(2000):
                yield
        finally:
            await dispose_engine()  # drop connections carrying the setting

    async def test_the_answer_is_written_while_the_attempt_holds_its_lock(
        self,
        db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        with _channel(_Spy()):
            await process_pending_notifications()
        assert _of(
            await _rows(nid),
            subject=JournalSubject.CHANNEL,
            outcome=ChannelAnswer.ACCEPTED,
        )
        assert (await _job(nid)).status == NotificationStatus.SENT

    async def test_expiry_waits_for_the_attempt(
        self,
        db_session: AsyncSession,
    ) -> None:
        """On a SECOND attempt: the job is already PROCESSING, resolve
        does not touch its row, so only the attempt's row lock stands
        between it and an expiry."""
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        with _channel(_Spy({"in_app": EmailTransientError("provider error (503)")})):
            await process_pending_notifications()
        assert (await _job(nid)).status == NotificationStatus.PROCESSING
        await _open_gates()
        blocked: list[BaseException] = []

        class _ExpireWhileInFlight:
            async def deliver(self, *args: Any) -> bool:
                async with get_session_factory()() as other:
                    await other.execute(text("SET LOCAL lock_timeout = '500ms'"))
                    try:
                        await close_notifications(
                            other,
                            Notification.id == nid,
                            NotificationStatus.EXPIRED,
                        )
                    except DBAPIError as exc:
                        blocked.append(exc)
                    await other.rollback()
                return True

        with patch(
            "app.engine.service.get_formatter",
            return_value=_ExpireWhileInFlight(),
        ):
            await process_pending_notifications()
        assert len(blocked) == 1
        assert "lock" in str(blocked[0]).lower()
        assert (await _job(nid)).status == NotificationStatus.SENT


# -----------------------------------------------------------------------------
# Forgetting -- the journal's one edit (P2-1 gate, item 1)
# -----------------------------------------------------------------------------


class TestForgetting:
    async def test_forgetting_clears_their_provider_words_and_only_theirs(
        self,
        db_session: AsyncSession,
    ) -> None:
        forgotten = await create_recipient(db_session)
        kept = await create_recipient(db_session)
        nid = await _intake(db_session, "*", target_type=TargetType.ALL)
        with _channel(_Spy({"in_app": RateLimitedError(42.0, ": addr@example.test")})):
            await process_pending_notifications()
        before = _of(await _rows(nid), subject=JournalSubject.CHANNEL)
        assert {r.recipient_id for r in before if r.provider_text} == {
            forgotten.id,
            kept.id,
        }
        async with get_session_factory()() as session:
            await withdraw_recipient(session, forgotten.id)
            await session.commit()
        after = {
            r.recipient_id: r
            for r in _of(await _rows(nid), subject=JournalSubject.CHANNEL)
        }
        assert after[forgotten.id].provider_text is None
        # The pair: the other recipient's words are untouched, and the
        # forgotten one's row is still there -- only the column went.
        assert after[kept.id].provider_text is not None
        assert after[forgotten.id].outcome == ChannelAnswer.RATE_LIMITED


# -----------------------------------------------------------------------------
# Б1 (9) -- the CHECKs, each with its NULL twin
# -----------------------------------------------------------------------------


def _row(notification_id: UUID, **overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = dict(
        notification_id=notification_id,
        subject=JournalSubject.JOB,
        step=JournalStep.INTAKE,
        outcome=NotificationStatus.PENDING,
        attempt=0,
    )
    row.update(overrides)
    return row


_R = uuid4()

_REFUSED = [
    (
        "delivery without recipient",
        dict(subject="delivery", recipient_id=None, channel="in_app"),
    ),
    (
        "delivery without channel",
        dict(subject="delivery", recipient_id=_R, channel=None),
    ),
    ("channel without channel", dict(subject="channel", recipient_id=_R, channel=None)),
    ("gate without recipient", dict(subject="gate", recipient_id=None)),
    ("gate with channel", dict(subject="gate", recipient_id=_R, channel="in_app")),
    ("job with recipient", dict(recipient_id=_R)),
    ("unknown subject", dict(subject="other")),
    ("reason without until", dict(wait_reason="x", wait_until=None)),
    ("until without reason", dict(wait_reason=None, wait_until=datetime.now(UTC))),
    ("provider words on a job", dict(provider_text="words")),
    (
        "blank provider words",
        dict(subject="channel", recipient_id=_R, channel="in_app", provider_text=""),
    ),
    ("blank error", dict(error="")),
    ("negative attempt", dict(attempt=-1)),
]

_ACCEPTED = [
    ("job, every nullable NULL", dict()),
    (
        "channel, provider words NULL",
        dict(subject="channel", recipient_id=_R, channel="in_app", provider_text=None),
    ),
    ("gate", dict(subject="gate", recipient_id=_R, category="c")),
    ("wait both", dict(wait_reason="x", wait_until=datetime.now(UTC))),
]


class TestChecks:
    @pytest.mark.parametrize(
        ("case", "overrides"), _REFUSED, ids=[c for c, _ in _REFUSED]
    )
    async def test_refused(
        self,
        db_session: AsyncSession,
        case: str,
        overrides: dict[str, Any],
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        db_session.add(NotificationTransition(**_row(nid, **overrides)))
        with pytest.raises(IntegrityError, match="ck_transitions_"):
            await db_session.flush()
        await db_session.rollback()

    @pytest.mark.parametrize(
        ("case", "overrides"), _ACCEPTED, ids=[c for c, _ in _ACCEPTED]
    )
    async def test_accepted(
        self,
        db_session: AsyncSession,
        case: str,
        overrides: dict[str, Any],
    ) -> None:
        recipient = await create_recipient(db_session)
        nid = await _intake(db_session, recipient.id)
        db_session.add(NotificationTransition(**_row(nid, **overrides)))
        await db_session.commit()
        assert len(await _rows(nid)) == 2
