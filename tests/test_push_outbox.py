# =============================================================================
# P3-1 Б1 -- the reverse outbox: which transition owes a push, and that
# the push commits with its transition or not at all
# =============================================================================
#
# The grid "push_on x transition -> push" is asserted twice: once on
# app/engine/journal.py push_due itself, row by row, and once end to
# end through the pipeline, so a transition point that does not reach
# the registrar is caught as well as a wrong rule in it.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the push row written in its own session, not the transition's
#                               -> TestTransaction.test_a_rolled_back_...
#   M2  a push on PROCESSING under `outcome`
#                               -> TestRule, TestPipeline (resolve rows)
#   M3  a deferral pushed under `outcome`
#                               -> TestRule, TestPipeline (deferrals)
#   M4  a transition point without its journal call (rollup)
#                               -> TestPipeline.test_..._sent, and the
#                                  journal's own fence
#   M5  `none` pushing
#                               -> TestPipeline (the none column)
#   M6  push_on read from the registry instead of the job's snapshot
#                               -> TestSnapshot.test_a_profile_changed_...
#   M11 the per-job dedup trusting a remembered flag, not the row's state
#                               -> TestTransaction.test_a_savepoint_...
# =============================================================================

import subprocess
import sys
from collections.abc import AsyncGenerator, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, func, inspect, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import CategoryMute, Recipient
from app.core.config import settings
from app.core.database import dispose_engine, get_session_factory
from app.engine import journal, processor, service
from app.engine.constants import (
    DeliveryStatus,
    JournalStep,
    JournalSubject,
    NotificationStatus,
    TargetType,
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
    PushOutbox,
)
from app.engine.processor import process_pending_notifications
from app.engine.reminders import cancel_reminders
from app.engine.service import (
    close_notifications,
    create_notification,
    withdraw_recipient,
)
from app.profile.loader import PUSH_ON
from app.profile.registry import Decided, Layer, PushOn, TypeRecord, registry
from tests.helpers import create_recipient, intake_fields, notification_row_fields

_TYPE = "unit_event_in_app"
_REPO = Path(__file__).resolve().parents[1]

NONE = PushOn.NONE.value
OUTCOME = PushOn.OUTCOME.value
DEFERRAL = PushOn.OUTCOME_AND_DEFERRAL.value


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _declare(push_on: str, type_key: str = _TYPE) -> None:
    """Re-install the fixture type with `push_on` declared -- through the
    registry's own API; the autouse profile fixture resets it."""
    record = registry.record_of(type_key)
    assert record is not None
    fields = dict(record.fields)
    fields["push_on"] = Decided(push_on, Layer.PROFILE, "test")
    registry.register_type(
        type_key,
        category=registry.category_of(type_key),
        record=TypeRecord(fields=fields),
    )


class _Spy:
    """A channel that answers per channel: True, False or an exception."""

    def __init__(self, answer: Any = True) -> None:
        self.answer = answer

    async def deliver(self, *args: Any) -> bool:
        if isinstance(self.answer, BaseException):
            raise self.answer
        return bool(self.answer)


@contextmanager
def _channel(answer: Any = True) -> Iterator[None]:
    with patch("app.engine.service.get_formatter", return_value=_Spy(answer)):
        yield


@contextmanager
def _schedule_defers() -> Iterator[None]:
    with patch(
        "app.engine.service.recipient_deferred_until",
        return_value=datetime.now(UTC) + timedelta(hours=1),
    ):
        yield


async def _intake(
    session: AsyncSession,
    target: UUID | str,
    *,
    target_type: str = TargetType.USER,
    **extra: Any,
) -> UUID:
    notification = await create_notification(
        session,
        **intake_fields(),
        type=_TYPE,
        title="T",
        body="B",
        target_type=target_type,
        target_value=str(target),
        **extra,
    )
    await session.commit()
    return notification.id


async def _pushes(notification_id: UUID | None = None) -> int:
    async with get_session_factory()() as session:
        query = select(func.count()).select_from(PushOutbox)
        if notification_id is not None:
            query = query.where(PushOutbox.notification_id == notification_id)
        return int((await session.execute(query)).scalar_one())


async def _forget_pushes() -> None:
    """Empty the outbox, so the next count is the next transition's."""
    async with get_session_factory()() as session:
        await session.execute(delete(PushOutbox))
        await session.commit()


async def _status(notification_id: UUID) -> str:
    async with get_session_factory()() as session:
        job = await session.get(Notification, notification_id)
        assert job is not None
        return job.status


def _row(**kw: Any) -> NotificationTransition:
    return NotificationTransition(
        notification_id=uuid4(), step=JournalStep.DELIVER, attempt=0, **kw,
    )


_SOON = datetime(2090, 1, 1, tzinfo=UTC)


# -----------------------------------------------------------------------------
# The rule, row by row: push_due
# -----------------------------------------------------------------------------

# (row, pushes under none, under outcome, under outcome_and_deferral)
_RULE: list[tuple[str, NotificationTransition, tuple[bool, bool, bool]]] = [
    ("job intake pending", _row(
        subject=JournalSubject.JOB, outcome=NotificationStatus.PENDING,
    ), (False, False, False)),
    ("job resolved processing", _row(
        subject=JournalSubject.JOB, outcome=NotificationStatus.PROCESSING,
    ), (False, False, False)),
    *[
        (f"job outcome {status}", _row(
            subject=JournalSubject.JOB, outcome=status,
        ), (False, True, True))
        for status in sorted(journal.OUTCOME_STATUSES)
    ],
    ("job pipeline gate", _row(
        subject=JournalSubject.JOB, outcome=NotificationStatus.PROCESSING,
        wait_reason="pipeline_retry", wait_until=_SOON,
    ), (False, False, True)),
    ("job pipeline gate while pending", _row(
        subject=JournalSubject.JOB, outcome=NotificationStatus.PENDING,
        wait_reason="pipeline_retry", wait_until=_SOON,
    ), (False, False, True)),
    *[
        (f"delivery deferred {reason}", _row(
            subject=JournalSubject.DELIVERY, outcome=DeliveryStatus.PENDING,
            wait_reason=reason, wait_until=_SOON,
        ), (False, False, True))
        for reason in (
            "recipient_schedule", "provider_rate_limit", "transient_backoff",
        )
    ],
    ("delivery born, waiting its turn", _row(
        subject=JournalSubject.DELIVERY, outcome=DeliveryStatus.PENDING,
    ), (False, False, False)),
    *[
        (f"delivery closed {status}", _row(
            subject=JournalSubject.DELIVERY, outcome=status,
        ), (False, False, False))
        for status in sorted(set(DeliveryStatus) - {DeliveryStatus.PENDING})
    ],
    ("channel answer", _row(
        subject=JournalSubject.CHANNEL, outcome="accepted",
    ), (False, False, False)),
    ("gate", _row(
        subject=JournalSubject.GATE, outcome=DeliveryStatus.SUPPRESSED,
    ), (False, False, False)),
]


class TestRule:
    @pytest.mark.parametrize(
        "name,row,expected", _RULE, ids=[name for name, _, _ in _RULE],
    )
    def test_the_grid(
        self, name: str, row: NotificationTransition,
        expected: tuple[bool, bool, bool],
    ) -> None:
        assert tuple(
            journal.push_due(value, row) for value in (NONE, OUTCOME, DEFERRAL)
        ) == expected

    def test_the_grid_covers_every_value_and_pushes_somewhere(self) -> None:
        """The pair: the three columns are the three values the profile
        offers, and every non-none column has a row that pushes."""
        assert {NONE, OUTCOME, DEFERRAL} == set(PUSH_ON)
        assert any(expected[1] for _, _, expected in _RULE)
        assert any(
            expected[2] and not expected[1] for _, _, expected in _RULE
        )

    def test_an_outcome_is_every_status_but_the_active_two(self) -> None:
        """The journal's outcome set is retention's terminal set: one
        notion of "finished", not two."""
        assert frozenset(
            service._RETENTION_TERMINAL_STATUSES,
        ) == journal.OUTCOME_STATUSES
        assert NotificationStatus.PENDING not in journal.OUTCOME_STATUSES
        assert NotificationStatus.PROCESSING not in journal.OUTCOME_STATUSES


# -----------------------------------------------------------------------------
# The grid end to end: each scenario under each push_on
# -----------------------------------------------------------------------------


async def _s_intake_only(db: AsyncSession) -> UUID:
    return await _intake(db, (await create_recipient(db)).id)


async def _s_nobody(db: AsyncSession) -> UUID:
    nid = await _intake(db, uuid4())
    with _channel():
        await process_pending_notifications()
    assert await _status(nid) == NotificationStatus.NO_RECIPIENTS
    return nid


async def _s_all_muted(db: AsyncSession) -> UUID:
    recipient = await create_recipient(db)
    db.add(CategoryMute(recipient_id=recipient.id, category="unit_updates"))
    nid = await _intake(db, recipient.id)
    with _channel():
        await process_pending_notifications()
    assert await _status(nid) == NotificationStatus.SUPPRESSED
    return nid


async def _s_sent(db: AsyncSession) -> UUID:
    nid = await _intake(db, (await create_recipient(db)).id)
    with _channel():
        await process_pending_notifications()
    assert await _status(nid) == NotificationStatus.SENT
    return nid


async def _s_refused(db: AsyncSession) -> UUID:
    nid = await _intake(db, (await create_recipient(db)).id)
    with _channel(ConfigurationError("provider refused (401)")):
        await process_pending_notifications()
    assert await _status(nid) == NotificationStatus.FAILED
    return nid


async def _s_schedule(db: AsyncSession) -> UUID:
    nid = await _intake(db, (await create_recipient(db)).id)
    with _channel(), _schedule_defers():
        await process_pending_notifications()
    assert await _status(nid) == NotificationStatus.PROCESSING
    return nid


async def _s_rate_limited(db: AsyncSession) -> UUID:
    nid = await _intake(db, (await create_recipient(db)).id)
    with _channel(RateLimitedError(42.0, ": slow down")):
        await process_pending_notifications()
    assert await _status(nid) == NotificationStatus.PROCESSING
    return nid


async def _s_backoff(db: AsyncSession) -> UUID:
    nid = await _intake(db, (await create_recipient(db)).id)
    with _channel(EmailTransientError("provider error (503)")):
        await process_pending_notifications()
    assert await _status(nid) == NotificationStatus.PROCESSING
    return nid


async def _s_exhausted(db: AsyncSession) -> UUID:
    # The ceiling is the job's since H1 (snapshot at intake), set on it.
    nid = await _intake(db, (await create_recipient(db)).id)
    await db.execute(
        update(Notification)
        .where(Notification.id == nid)
        .values(retry_max_attempts=1)
    )
    await db.commit()
    with _channel(EmailTransientError("provider error (503)")):
        await process_pending_notifications()
    assert await _status(nid) == NotificationStatus.FAILED
    return nid


async def _s_late_inactive(db: AsyncSession) -> UUID:
    recipient = await create_recipient(db)
    nid = await _intake(db, recipient.id)
    async with get_session_factory()() as session:
        job = await session.get(Notification, nid)
        assert job is not None
        await service.resolve_notification(session, job)
        await session.execute(
            text("UPDATE recipients SET active = false WHERE id = :id"),
            {"id": recipient.id},
        )
        await service.deliver_notification(session, job)
        await service.rollup_notification(session, job)
        await session.commit()
    assert await _status(nid) == NotificationStatus.NO_RECIPIENTS
    return nid


async def _s_late_mute(db: AsyncSession) -> UUID:
    recipient = await create_recipient(db)
    nid = await _intake(db, recipient.id)
    async with get_session_factory()() as session:
        job = await session.get(Notification, nid)
        assert job is not None
        await service.resolve_notification(session, job)
        session.add(CategoryMute(recipient_id=recipient.id, category="unit_updates"))
        await session.flush()
        await service.deliver_notification(session, job)
        await service.rollup_notification(session, job)
        await session.commit()
    assert await _status(nid) == NotificationStatus.SUPPRESSED
    return nid


class _PlantedDefectError(RuntimeError):
    """An exception of comms' own, planted by the tests."""


_REAL_ROLLUP = processor.rollup_notification


async def _torn_rollup(
    session: AsyncSession,
    notification: Notification,
    step: JournalStep = JournalStep.ROLLUP,
) -> None:
    """The attempt's own rollup tears; the ceiling's fold stays real."""
    if step == JournalStep.ROLLUP:
        raise _PlantedDefectError("torn")
    await _REAL_ROLLUP(session, notification, step)


async def _s_pipeline_gate(db: AsyncSession) -> UUID:
    settings.notification_max_pipeline_attempts = 3
    nid = await _intake(db, (await create_recipient(db)).id)
    with _channel(False), patch.object(
        processor, "rollup_notification", _torn_rollup,
    ):
        await process_pending_notifications()
    async with get_session_factory()() as session:
        job = await session.get(Notification, nid)
        assert job is not None and job.pipeline_retry_at is not None
    return nid


async def _s_ceiling(db: AsyncSession) -> UUID:
    """The attempt tears at its rollup on the last allowed attempt: the
    torn row (no gate) pushes nothing, the ceiling's fold pushes."""
    settings.notification_max_pipeline_attempts = 1
    nid = await _intake(db, (await create_recipient(db)).id)
    with _channel(False), patch.object(
        processor, "rollup_notification", _torn_rollup,
    ):
        await process_pending_notifications()
    assert await _status(nid) == NotificationStatus.FAILED
    return nid


async def _s_ceiling_resolve_fails(db: AsyncSession) -> UUID:
    settings.notification_max_pipeline_attempts = 1
    nid = await _intake(db, (await create_recipient(db)).id)

    async def broken_resolve(session: AsyncSession, *args: Any) -> None:
        await session.execute(text("SELECT * FROM no_such_table_p3_1"))

    with patch.object(processor, "resolve_notification", broken_resolve):
        await process_pending_notifications()
    assert await _status(nid) == NotificationStatus.FAILED
    return nid


# (scenario, pushes under none, outcome, outcome_and_deferral)
_SCENARIOS = [
    ("intake only", _s_intake_only, (0, 0, 0)),
    ("resolve: nobody", _s_nobody, (0, 1, 1)),
    ("resolve: everyone muted", _s_all_muted, (0, 1, 1)),
    ("deliver: sent, folded", _s_sent, (0, 1, 1)),
    ("deliver: refused, folded failed", _s_refused, (0, 1, 1)),
    ("deliver: schedule deferral", _s_schedule, (0, 0, 1)),
    ("deliver: rate limit deferral", _s_rate_limited, (0, 0, 1)),
    ("deliver: transient backoff", _s_backoff, (0, 0, 1)),
    ("deliver: transient exhausted", _s_exhausted, (0, 1, 1)),
    ("deliver: late inactive, folded", _s_late_inactive, (0, 1, 1)),
    ("deliver: late mute, folded", _s_late_mute, (0, 1, 1)),
    ("pipeline: below the ceiling, gated", _s_pipeline_gate, (0, 0, 1)),
    ("pipeline: ceiling, folded", _s_ceiling, (0, 1, 1)),
    ("pipeline: ceiling, resolve fails", _s_ceiling_resolve_fails, (0, 1, 1)),
]


class TestPipeline:
    @pytest.fixture(autouse=True)
    def _restore_ceilings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Scenarios set these directly; monkeypatch puts them back.
        monkeypatch.setattr(
            settings, "notification_max_delivery_attempts",
            settings.notification_max_delivery_attempts,
        )
        monkeypatch.setattr(
            settings, "notification_max_pipeline_attempts",
            settings.notification_max_pipeline_attempts,
        )

    @pytest.mark.parametrize("column,push_on", list(enumerate(
        (NONE, OUTCOME, DEFERRAL),
    )), ids=[NONE, OUTCOME, DEFERRAL])
    @pytest.mark.parametrize(
        "name,scenario,expected", _SCENARIOS,
        ids=[name for name, _, _ in _SCENARIOS],
    )
    async def test_the_grid(
        self,
        db_session: AsyncSession,
        name: str,
        scenario: Any,
        expected: tuple[int, int, int],
        column: int,
        push_on: str,
    ) -> None:
        _declare(push_on)
        nid = await scenario(db_session)
        assert await _pushes(nid) == expected[column]
        # The pair: the job is there and its journal is not empty, so a
        # zero above is a decision, not an empty table.
        async with get_session_factory()() as session:
            rows = (await session.execute(
                select(func.count()).where(
                    NotificationTransition.notification_id == nid,
                )
            )).scalar_one()
        assert rows >= 1


class TestMassTransitions:
    """Expiry, cancellation, forgetting -- counted from an emptied outbox,
    so the count is the mass transition's own."""

    async def _three_waiting(
        self, db: AsyncSession, **extra: Any,
    ) -> tuple[UUID, list[Recipient]]:
        recipients = [await create_recipient(db) for _ in range(3)]
        nid = await _intake(db, "*", target_type=TargetType.ALL, **extra)
        with _channel(), _schedule_defers():
            await process_pending_notifications()
        await _forget_pushes()
        return nid, recipients

    @pytest.mark.parametrize("push_on,expected", [
        (NONE, 0), (OUTCOME, 1), (DEFERRAL, 1),
    ])
    async def test_expiry(
        self, db_session: AsyncSession, push_on: str, expected: int,
    ) -> None:
        _declare(push_on)
        nid, _ = await self._three_waiting(db_session)
        async with get_session_factory()() as session:
            await close_notifications(
                session, Notification.id == nid, NotificationStatus.EXPIRED,
            )
            await session.commit()
        assert await _status(nid) == NotificationStatus.EXPIRED
        assert await _pushes(nid) == expected

    @pytest.mark.parametrize("push_on,expected", [
        (NONE, 0), (OUTCOME, 1), (DEFERRAL, 1),
    ])
    async def test_expiry_of_an_unresolved_job(
        self, db_session: AsyncSession, push_on: str, expected: int,
    ) -> None:
        _declare(push_on)
        nid = await _intake(db_session, uuid4())
        async with get_session_factory()() as session:
            await close_notifications(
                session, Notification.id == nid, NotificationStatus.EXPIRED,
            )
            await session.commit()
        assert await _status(nid) == NotificationStatus.EXPIRED
        assert await _pushes(nid) == expected

    @pytest.mark.parametrize("push_on,expected", [
        (NONE, 0), (OUTCOME, 1), (DEFERRAL, 1),
    ])
    async def test_cancellation(
        self, db_session: AsyncSession, push_on: str, expected: int,
    ) -> None:
        _declare(push_on)
        nid, _ = await self._three_waiting(db_session, correlation="corr-p3-1")
        async with get_session_factory()() as session:
            await cancel_reminders(
                session, types={_TYPE}, correlation="corr-p3-1",
            )
            await session.commit()
        assert await _status(nid) == NotificationStatus.CANCELLED
        assert await _pushes(nid) == expected

    @pytest.mark.parametrize("push_on,expected", [
        (NONE, (0, 0)), (OUTCOME, (1, 0)), (DEFERRAL, (1, 0)),
    ])
    async def test_forgetting(
        self, db_session: AsyncSession, push_on: str,
        expected: tuple[int, int],
    ) -> None:
        """Forgetting the one recipient of a job closes it -- an outcome;
        forgetting one of three leaves the other job waiting -- nothing."""
        _declare(push_on)
        three, recipients = await self._three_waiting(db_session)
        single = await _intake(db_session, recipients[0].id)
        with _channel(), _schedule_defers():
            await process_pending_notifications()
        await _forget_pushes()
        async with get_session_factory()() as session:
            await withdraw_recipient(session, recipients[0].id)
            await session.commit()
        assert await _status(single) == NotificationStatus.NO_RECIPIENTS
        assert await _status(three) == NotificationStatus.PROCESSING
        assert (await _pushes(single), await _pushes(three)) == expected

    async def test_one_push_for_many_deferrals_in_one_pass(
        self, db_session: AsyncSession,
    ) -> None:
        """Three deliveries deferred in one pass owe ONE push, not three
        (the dedup); and at least one (the pair)."""
        _declare(DEFERRAL)
        for _ in range(3):
            await create_recipient(db_session)
        nid = await _intake(db_session, "*", target_type=TargetType.ALL)
        with _channel(), _schedule_defers():
            await process_pending_notifications()
        deferred = (
            await db_session.execute(
                select(func.count()).where(
                    NotificationDelivery.notification_id == nid,
                    NotificationDelivery.wait_reason.is_not(None),
                )
            )
        ).scalar_one()
        assert deferred == 3
        assert await _pushes(nid) == 1


# -----------------------------------------------------------------------------
# The transaction: a push commits with its transition or not at all
# -----------------------------------------------------------------------------


class TestTransaction:
    async def _nobody_job(self, db: AsyncSession) -> UUID:
        _declare(OUTCOME)
        return await _intake(db, uuid4())

    @pytest.mark.parametrize("commit,expected", [(False, 0), (True, 1)])
    async def test_a_rolled_back_transition_owes_nothing(
        self, db_session: AsyncSession, commit: bool, expected: int,
    ) -> None:
        """The resolve to NO_RECIPIENTS is flushed with its push and then
        rolled back: no push. The pair: committed, one."""
        nid = await self._nobody_job(db_session)
        async with get_session_factory()() as session:
            job = await session.get(Notification, nid)
            assert job is not None
            await service.resolve_notification(session, job)
            await session.flush()
            if commit:
                await session.commit()
            else:
                await session.rollback()
        assert await _pushes(nid) == expected

    async def test_an_uncommitted_push_is_invisible_outside(
        self, db_session: AsyncSession,
    ) -> None:
        nid = await self._nobody_job(db_session)
        async with get_session_factory()() as session:
            job = await session.get(Notification, nid)
            assert job is not None
            await service.resolve_notification(session, job)
            await session.flush()
            assert await _pushes(nid) == 0
            await session.commit()
        assert await _pushes(nid) == 1

    async def test_a_savepoint_rollback_takes_its_push_the_outer_keeps_its_own(
        self, db_session: AsyncSession,
    ) -> None:
        """What the dedup stands on (plan, section 7 item 1): a push row
        added inside a savepoint leaves the session's pending set when
        the savepoint rolls back -- it is transient again -- so the next
        transition of the same job in the outer transaction adds a fresh
        one, and that one commits."""
        nid = await self._nobody_job(db_session)
        async with get_session_factory()() as session:
            job = await session.get(Notification, nid)
            assert job is not None
            job.status = NotificationStatus.NO_RECIPIENTS
            nested = await session.begin_nested()
            journal.record_job(session, job, JournalStep.CEILING)
            (inner,) = [o for o in session.new if isinstance(o, PushOutbox)]
            assert inspect(inner).pending
            await nested.rollback()
            assert inner not in session.new
            assert inspect(inner).transient
            journal.record_job(session, job, JournalStep.CEILING)
            await session.commit()
        assert await _pushes(nid) == 1

    async def test_a_second_flush_adds_a_second_harmless_push(
        self, db_session: AsyncSession,
    ) -> None:
        """One push per job per flush, not per transaction: after a flush
        the next owing transition adds another row -- a duplicate the
        product reads twice, harmless (spec §7.2), never a lost one."""
        nid = await self._nobody_job(db_session)
        async with get_session_factory()() as session:
            job = await session.get(Notification, nid)
            assert job is not None
            job.status = NotificationStatus.NO_RECIPIENTS
            journal.record_job(session, job, JournalStep.CEILING)
            journal.record_job(session, job, JournalStep.CEILING)
            await session.flush()
            journal.record_job(session, job, JournalStep.CEILING)
            await session.commit()
        assert await _pushes(nid) == 2

    async def test_the_push_goes_with_its_job(
        self, db_session: AsyncSession,
    ) -> None:
        nid = await self._nobody_job(db_session)
        with _channel():
            await process_pending_notifications()
        assert await _pushes(nid) == 1
        async with get_session_factory()() as session:
            await session.execute(delete(Notification).where(Notification.id == nid))
            await session.commit()
        assert await _pushes(nid) == 0


# -----------------------------------------------------------------------------
# The snapshot: notifications.push_on
# -----------------------------------------------------------------------------


class TestSnapshot:
    @pytest.mark.parametrize("push_on", list(PUSH_ON))
    async def test_intake_snapshots_the_declared_value(
        self, db_session: AsyncSession, push_on: str,
    ) -> None:
        _declare(push_on)
        nid = await _intake(db_session, uuid4())
        job = await db_session.get(Notification, nid)
        assert job is not None and job.push_on == push_on

    async def test_an_undeclared_type_snapshots_none(
        self, db_session: AsyncSession,
    ) -> None:
        record = registry.record_of(_TYPE)
        assert record is not None
        assert record.fields["push_on"].layer is Layer.DEFAULT
        nid = await _intake(db_session, uuid4())
        job = await db_session.get(Notification, nid)
        assert job is not None and job.push_on == NONE

    async def test_the_fixture_type_that_declares_it_snapshots_it(
        self, db_session: AsyncSession,
    ) -> None:
        """The pair to the default: `unit_routed` declares outcome in the
        fixture profile, through the real loader."""
        notification = await create_notification(
            db_session, **intake_fields(), type="unit_routed", title="T",
            body="B", target_type=TargetType.ALL, target_value="*",
        )
        assert notification.push_on == OUTCOME

    async def test_a_profile_changed_after_intake_does_not_change_the_job(
        self, db_session: AsyncSession,
    ) -> None:
        _declare(OUTCOME)
        nid = await _intake(db_session, uuid4())
        _declare(NONE)
        with _channel():
            await process_pending_notifications()
        assert await _status(nid) == NotificationStatus.NO_RECIPIENTS
        assert await _pushes(nid) == 1

    @pytest.mark.parametrize("value", [None, "deferral", ""])
    async def test_the_column_refuses_a_value_outside_the_three(
        self, db_session: AsyncSession, value: str | None,
    ) -> None:
        fields = notification_row_fields()
        fields["push_on"] = value
        db_session.add(Notification(
            type=_TYPE, title="T", body="B", target_type="all",
            target_value="*", **fields,
        ))
        with pytest.raises(IntegrityError, match="push_on"):
            await db_session.flush()

    @pytest.mark.parametrize("value", list(PUSH_ON))
    async def test_the_column_takes_each_of_the_three(
        self, db_session: AsyncSession, value: str,
    ) -> None:
        fields = notification_row_fields()
        fields["push_on"] = value
        db_session.add(Notification(
            type=_TYPE, title="T", body="B", target_type="all",
            target_value="*", **fields,
        ))
        await db_session.flush()


# -----------------------------------------------------------------------------
# Migration 0020 on a non-empty table
# -----------------------------------------------------------------------------

_BEFORE = "0019_changes_feed_index"
_SUBJECT = "0020_push_outbox"


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=_REPO, capture_output=True, text=True,
    )


async def _migrate(*args: str) -> None:
    await dispose_engine()
    done = _alembic(*args)
    await dispose_engine()
    assert done.returncode == 0, done.stderr


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    yield
    async with get_session_factory()() as session:
        await session.execute(text("DELETE FROM notifications"))
        await session.commit()
    await _migrate("upgrade", "head")


async def _sql(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        result = await session.execute(text(statement), params)
        await session.commit()
        return result


async def test_migration_0020(at_head_afterwards: None) -> None:
    """Existing jobs take push_on 'none' -- what each was accepted with in
    effect -- and the column ends NOT NULL with its CHECK; the outbox
    exists empty. Down and up again on the same rows."""
    await _migrate("downgrade", _BEFORE)
    id_ = uuid4()
    await _sql(
        "INSERT INTO notifications (id, type, title, body, target_type, "
        "target_value, idempotency_key, fingerprint, channels, "
        "expiry_layer, status) VALUES (:id, 'unit_event', 'T', 'B', 'all', "
        "'*', :key, :fp, '[\"in_app\"]'::jsonb, 'default', 'sent')",
        id=id_, key=f"m20:{id_}", fp="a" * 64,
    )
    await _migrate("upgrade", _SUBJECT)
    assert (await _sql(
        "SELECT push_on FROM notifications WHERE id = :id", id=id_,
    )).scalar_one() == NONE
    assert (await _sql(
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_name = 'notifications' AND column_name = 'push_on'",
    )).scalar_one() == "NO"
    assert (await _sql(
        "SELECT count(*) FROM pg_constraint "
        "WHERE conname = 'ck_notifications_push_on'",
    )).scalar_one() == 1
    assert (await _sql("SELECT count(*) FROM push_outbox")).scalar_one() == 0
    await _migrate("downgrade", _BEFORE)
    assert (await _sql(
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_name = 'notifications' AND column_name = 'push_on'",
    )).scalar_one() == 0
    assert (await _sql(
        "SELECT to_regclass('push_outbox') IS NULL",
    )).scalar_one() is True
    await _migrate("upgrade", _SUBJECT)
    assert (await _sql(
        "SELECT push_on FROM notifications WHERE id = :id", id=id_,
    )).scalar_one() == NONE
