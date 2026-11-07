# =============================================================================
# D1 / T12 + R6 -- a pipeline defect becomes an outcome; no traceback
# reaches the log unredacted.
# =============================================================================
#
# T12. Before: an exception in resolve / deliver / rollup rolled the
# attempt back and left the job selectable forever, without an outcome;
# the selection is oldest-first and capped, so poisoned rows numbering
# at least the batch size stopped delivery whole.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 the counter does not grow       -> TestOutcome.test_..._after_n
#   M2 the gate is left out of the selection
#                                      -> TestNoStarvation.test_healthy_...
#   M3 ORDER BY pipeline_attempts, scheduled_at (the rejected ordering)
#                                      -> TestNoStarvation.test_failing_...
#   M4 pipeline_error = str(exc)       -> TestNoLetterContent
#   M5 the ceiling leaves waiting deliveries open
#                                      -> TestOutcome.test_waiting_...
#   M6 one R6 site back on logger.exception / exc_info=
#                                      -> TestNoRawTraceback (AST fence)
# =============================================================================

import ast
import subprocess
import sys
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.core.config import settings
from app.core.database import dispose_engine, get_session_factory
from app.core.lanes import Lane
from app.engine import processor
from app.engine.constants import (
    DeliveryStatus,
    FailureClass,
    JournalStep,
    JournalSubject,
    NotificationStatus,
    PipelineStep,
    TargetType,
)
from app.engine.models import (
    Notification,
    NotificationDelivery,
    NotificationTransition,
)
from app.engine.processor import (
    pipeline_error_of,
    process_lane,
    process_pending_notifications,
)
from app.engine.service import create_notification
from app.profile.registry import Decided, Layer, TypeRecord, registry
from tests.helpers import create_recipient, intake_fields, notification_row_fields

_REAL_DELIVER = processor.deliver_notification

# A value no column, no status and no log key ever contains by itself.
_SENTINEL = "zq7" + "LETTERVAR" + "xk4"


class _PlantedDefectError(RuntimeError):
    """The defect the tests plant: an exception of comms' own."""


async def _job(
    session: AsyncSession,
    recipient_id: UUID,
    *,
    age_minutes: int,
    status: str = NotificationStatus.PENDING,
) -> UUID:
    """A due in_app job, `age_minutes` old by scheduled_at."""
    notification = Notification(
        **notification_row_fields(),
        type="unit_event_in_app",
        title="T",
        body="B",
        target_type=TargetType.USER,
        target_value=str(recipient_id),
        scheduled_at=datetime.now(UTC) - timedelta(minutes=age_minutes),
        status=status,
    )
    session.add(notification)
    await session.flush()
    return notification.id


async def _fetch(notification_id: UUID) -> Notification:
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


async def _open_gates() -> None:
    """Backdate every pipeline gate -- the backoff window has passed."""
    async with get_session_factory()() as session:
        await session.execute(text(
            "UPDATE notifications SET pipeline_retry_at = now() - interval '1 second' "
            "WHERE pipeline_retry_at IS NOT NULL"
        ))
        await session.commit()


def _poisoned_deliver(poisoned: set[UUID], message: str = "planted defect"):  # type: ignore[no-untyped-def]
    """deliver that raises for the poisoned jobs and is real for the rest."""

    async def deliver(
        session: AsyncSession, notification: Notification, slots: Any = None,
    ) -> None:
        # P5-L: the attempt hands deliver its lane's slots; forwarded.
        if notification.id in poisoned:
            raise _PlantedDefectError(message)
        await _REAL_DELIVER(session, notification, slots)

    return deliver


class TestOutcome:
    async def test_a_job_failing_in_deliver_gets_an_outcome_after_n(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """done-when (1): after N attempts the job is terminal, with the
        step and the class recorded on the job itself. M1."""
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", 3)
        recipient = await create_recipient(db_session)
        job = await _job(db_session, recipient.id, age_minutes=5)
        await db_session.commit()

        with patch.object(
            processor, "deliver_notification", _poisoned_deliver({job}),
        ):
            for attempt in (1, 2):
                await process_pending_notifications()
                row = await _fetch(job)
                assert row.status == NotificationStatus.PENDING
                assert row.pipeline_attempts == attempt
                # Recorded on the job from the FIRST failure on, not only
                # at the end: "where it tore" is answered by the row.
                assert row.pipeline_step == PipelineStep.DELIVER
                assert row.pipeline_retry_at is not None
                await _open_gates()
            await process_pending_notifications()

        row = await _fetch(job)
        assert row.status == NotificationStatus.FAILED
        assert row.pipeline_attempts == 3
        assert row.pipeline_step == PipelineStep.DELIVER
        assert row.pipeline_error is not None
        assert "_PlantedDefectError" in row.pipeline_error
        assert row.pipeline_retry_at is None
        # Terminal means terminal: further ticks leave it alone.
        with patch.object(
            processor, "deliver_notification", _poisoned_deliver({job}),
        ):
            await process_pending_notifications()
        assert (await _fetch(job)).pipeline_attempts == 3

    async def test_waiting_deliveries_close_with_class_pipeline(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A job that HAS deliveries (resolved earlier) folds through
        them: the waiting ones become FAILED / pipeline. M5."""
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", 1)
        recipient = await create_recipient(db_session)
        job = await _job(
            db_session, recipient.id, age_minutes=5,
            status=NotificationStatus.PROCESSING,
        )
        db_session.add(NotificationDelivery(
            notification_id=job, recipient_id=recipient.id,
            channel="in_app", status=DeliveryStatus.PENDING,
        ))
        await db_session.commit()

        with patch.object(
            processor, "deliver_notification", _poisoned_deliver({job}),
        ):
            await process_pending_notifications()

        (delivery,) = await _deliveries(job)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.failure_class == FailureClass.PIPELINE
        assert delivery.next_retry_at is None
        row = await _fetch(job)
        assert row.status == NotificationStatus.FAILED
        assert row.pipeline_step == PipelineStep.DELIVER

    async def test_a_failure_in_resolve_has_no_deliveries_to_fold(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The other half of the grid: resolve never committed, so the
        job is FAILED outright, with step resolve."""
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", 1)
        recipient = await create_recipient(db_session)
        job = await _job(db_session, recipient.id, age_minutes=5)
        await db_session.commit()

        async def resolve(session: AsyncSession, notification: Notification) -> None:
            raise _PlantedDefectError("resolve tore")

        with patch.object(processor, "resolve_notification", resolve):
            await process_pending_notifications()

        row = await _fetch(job)
        assert row.status == NotificationStatus.FAILED
        assert row.pipeline_step == PipelineStep.RESOLVE
        assert await _deliveries(job) == []

    async def test_a_job_that_recovers_ends_its_run_and_keeps_its_history(
        self, db_session: AsyncSession,
    ) -> None:
        """Below the ceiling a success closes the gate AND ends the run of
        failures (REV-2): the count, the step and the error go back to 0 /
        null together (ck_notifications_pipeline), and the failure stays
        in the journal, its history.

        This test used to assert that the record stayed on the job
        (attempts 1, step `deliver`): right while the count was the job's
        lifetime total -- REV-2 made it the run of failures in a row, so
        a big send no longer dies of three scattered transient ones."""
        recipient = await create_recipient(db_session)
        job = await _job(db_session, recipient.id, age_minutes=5)
        await db_session.commit()
        with patch.object(
            processor, "deliver_notification", _poisoned_deliver({job}),
        ):
            await process_pending_notifications()
        await _open_gates()
        await process_pending_notifications()

        row = await _fetch(job)
        assert row.status == NotificationStatus.SENT
        assert (row.pipeline_attempts, row.pipeline_step, row.pipeline_error) == (
            0, None, None,
        )
        assert row.pipeline_retry_at is None
        failures = await _failure_rows(job)
        assert [(r.step, r.attempt) for r in failures] == [
            (JournalStep.DELIVER, 1),
        ]
        error = failures[0].error
        assert error is not None and "_PlantedDefectError" in error


# -----------------------------------------------------------------------------
# REV-2 item 1 -- the budget is the failures IN A ROW
# -----------------------------------------------------------------------------
#
# MUTATIONS these tests were written against (each turns one red):
#   R1 no reset on a successful attempt     -> both TestTheRunOfFailures
#      alternating tests
#   R2 the reset only when the attempt's portion is not full (the job's
#      last portion) -> test_scattered_failures_do_not_close_a_big_send
#   R3 the count reset, the step and the error left (CHECK
#      ck_notifications_pipeline refuses the commit) -> the same


_BULK_TYPE = "unit_event_in_app"


def _bulk_lane() -> None:
    """The test type on the bulk lane: one slot, a portion of ONE
    delivery per attempt (app/engine/service.py delivery_portion)."""
    record = registry.record_of(_BULK_TYPE)
    assert record is not None
    fields = dict(record.fields)
    fields["lane"] = Decided(Lane.BULK.value, Layer.PROFILE, "test")
    registry.register_type(
        _BULK_TYPE, category=registry.category_of(_BULK_TYPE),
        record=TypeRecord(fields=fields),
    )


async def _send_to_all(recipients: int) -> UUID:
    """One bulk job to `recipients` people (target: all), via intake."""
    _bulk_lane()
    async with get_session_factory()() as session:
        for _ in range(recipients):
            await create_recipient(session)
        job = await create_notification(
            session, **intake_fields(), type=_BULK_TYPE, title="T", body="B",
            target_type=TargetType.ALL, target_value="*",
        )
        await session.commit()
        return job.id


async def _failure_rows(nid: UUID) -> list[NotificationTransition]:
    """The job's failure rows in the journal: JOB rows with an error."""
    async with get_session_factory()() as session:
        return list((await session.execute(
            select(NotificationTransition)
            .where(
                NotificationTransition.notification_id == nid,
                NotificationTransition.subject == JournalSubject.JOB,
                NotificationTransition.error.is_not(None),
            )
            .order_by(NotificationTransition.id)
        )).scalars())


_REAL_ROLLUP = processor.rollup_notification


def _scripted_rollup(script: list[bool]):  # type: ignore[no-untyped-def]
    """rollup that tears an attempt when the next entry of `script` is
    True and is real otherwise. Only the attempt's own rollup reads the
    script; the ceiling's (JournalStep.CEILING) is always real."""

    async def rollup(
        session: AsyncSession,
        notification: Notification,
        step: JournalStep = JournalStep.ROLLUP,
    ) -> None:
        if step == JournalStep.ROLLUP and script.pop(0):
            raise _PlantedDefectError("planted defect")
        await _REAL_ROLLUP(session, notification, step)

    return rollup


async def _passes(script: list[bool]) -> None:
    """One pass per entry of `script`, the gate opened after each."""
    with patch.object(processor, "rollup_notification", _scripted_rollup(script)):
        while script:
            left = len(script)
            torn = script[0]
            processed = await process_lane(Lane.BULK)
            # Attempted every pass (the script was read), and as scripted.
            assert (len(script), processed) == (left - 1, 0 if torn else 1)
            await _open_gates()


class TestTheRunOfFailures:
    async def test_scattered_failures_do_not_close_a_big_send(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A send in portions -- fail, succeed, four times over: more
        failures than the ceiling in all, never a ceiling in a row. The
        job is not closed by the ceiling; every delivery is tried and
        goes out; the journal keeps all four failures, each the first of
        its run. R1, R2, R3."""
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", 3)
        nid = await _send_to_all(4)
        await _passes([True, False] * 4)

        status, deliveries = await _statuses(nid)
        assert status == NotificationStatus.SENT
        assert deliveries == [DeliveryStatus.SENT] * 4
        row = await _fetch(nid)
        assert (row.pipeline_attempts, row.pipeline_step, row.pipeline_error) == (
            0, None, None,
        )
        failures = await _failure_rows(nid)
        assert [(r.step, r.attempt) for r in failures] == [
            (JournalStep.ROLLUP, 1),
        ] * 4
        assert not [
            r for r in await _job_rows(nid) if r.step == JournalStep.CEILING
        ]

    async def test_a_run_reaching_the_ceiling_closes_as_before(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The pair: one portion goes, then the attempts fail in a row up
        to the ceiling -- the job is closed there (_close_at_ceiling), as
        before REV-2: the delivery that went stays SENT, the one the
        channel took in the first torn attempt is closed SENT from the
        journal, the two never tried are FAILED with class `pipeline`;
        the journal counts the run 1, 2, 3."""
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", 3)
        nid = await _send_to_all(4)
        await _passes([False, True, True, True])

        status, deliveries = await _statuses(nid)
        assert status == NotificationStatus.PARTIAL_SENT
        assert sorted(deliveries) == sorted(
            [DeliveryStatus.SENT] * 2 + [DeliveryStatus.FAILED] * 2
        )
        assert await _failure_classes(nid) == [FailureClass.PIPELINE] * 2
        row = await _fetch(nid)
        assert (row.pipeline_attempts, row.pipeline_step) == (3, PipelineStep.ROLLUP)
        failures = await _failure_rows(nid)
        assert [r.attempt for r in failures] == [1, 2, 3]
        assert [
            r.outcome for r in await _job_rows(nid) if r.step == JournalStep.CEILING
        ] == [NotificationStatus.PARTIAL_SENT]

    async def test_a_failure_after_a_success_starts_a_new_run(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two failures, a success, two failures: under a ceiling of 3
        the job is still open after the last -- the success reset the
        count, and the second run is at 2. R1."""
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", 3)
        nid = await _send_to_all(4)
        await _passes([True, True, False, True, True])

        row = await _fetch(nid)
        assert row.status == NotificationStatus.PROCESSING
        assert (row.pipeline_attempts, row.pipeline_step) == (2, PipelineStep.ROLLUP)
        assert [r.attempt for r in await _failure_rows(nid)] == [1, 2, 1, 2]


async def _statuses(nid: UUID) -> tuple[str, list[str]]:
    async with get_session_factory()() as session:
        job = await session.get(Notification, nid)
        assert job is not None
        deliveries = list((await session.execute(
            select(NotificationDelivery.status)
            .where(NotificationDelivery.notification_id == nid)
            .order_by(NotificationDelivery.id)
        )).scalars())
        return job.status, deliveries


async def _failure_classes(nid: UUID) -> list[str | None]:
    async with get_session_factory()() as session:
        return list((await session.execute(
            select(NotificationDelivery.failure_class).where(
                NotificationDelivery.notification_id == nid,
                NotificationDelivery.status == DeliveryStatus.FAILED,
            )
        )).scalars())


async def _job_rows(nid: UUID) -> list[NotificationTransition]:
    async with get_session_factory()() as session:
        return list((await session.execute(
            select(NotificationTransition)
            .where(
                NotificationTransition.notification_id == nid,
                NotificationTransition.subject == JournalSubject.JOB,
            )
            .order_by(NotificationTransition.id)
        )).scalars())


class TestNoStarvation:
    async def test_healthy_job_goes_out_the_tick_after_the_poison_first_fails(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """done-when (2) / gate correction (a): batch_size + 1 poisoned
        rows, all OLDER than the healthy one. Tick 1 takes a full batch
        of poison; tick 2 must deliver the healthy job. M2: without the
        gate tick 2 takes the same poison again."""
        batch = 3
        monkeypatch.setattr(settings, "notification_lane_normal_batch_size", batch)
        recipient = await create_recipient(db_session)
        poisoned = {
            await _job(db_session, recipient.id, age_minutes=100 + i)
            for i in range(batch + 1)
        }
        healthy = await _job(db_session, recipient.id, age_minutes=1)
        await db_session.commit()

        with patch.object(
            processor, "deliver_notification", _poisoned_deliver(poisoned),
        ):
            await process_pending_notifications()
            assert (await _fetch(healthy)).status == NotificationStatus.PENDING
            await process_pending_notifications()

        assert (await _fetch(healthy)).status == NotificationStatus.SENT

    async def test_failing_job_reaches_its_outcome_under_a_flood_of_fresh_ones(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Gate correction (b): a steady stream of fresh jobs larger than
        the batch must not keep a failing job from its N-th attempt.
        M3: ordering by attempts put every fresh row ahead of it, and it
        never got an outcome."""
        batch, ceiling = 2, 3
        monkeypatch.setattr(settings, "notification_lane_normal_batch_size", batch)
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", ceiling)
        recipient = await create_recipient(db_session)
        failing = await _job(db_session, recipient.id, age_minutes=500)
        await db_session.commit()

        with patch.object(
            processor, "deliver_notification", _poisoned_deliver({failing}),
        ):
            for tick in range(ceiling + 2):
                async with get_session_factory()() as session:
                    for i in range(batch + 1):
                        await _job(session, recipient.id, age_minutes=tick * 10 + i)
                    await session.commit()
                await _open_gates()
                await process_pending_notifications()

        row = await _fetch(failing)
        assert row.status == NotificationStatus.FAILED
        assert row.pipeline_attempts == ceiling


class TestNoLetterContent:
    async def test_the_exception_text_is_not_recorded_anywhere(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Gate correction 2: the record holds the class and the place,
        never the text -- a text from a template can carry the letter's
        variables (spec §6.4). M4. The pair: the class IS recorded."""
        monkeypatch.setattr(settings, "notification_max_pipeline_attempts", 1)
        recipient = await create_recipient(db_session)
        job = await _job(db_session, recipient.id, age_minutes=5)
        db_session.add(NotificationDelivery(
            notification_id=job, recipient_id=recipient.id,
            channel="in_app", status=DeliveryStatus.PENDING,
        ))
        await db_session.execute(text(
            "UPDATE notifications SET status = 'processing' WHERE id = :id"
        ), {"id": job})
        await db_session.commit()

        with patch.object(
            processor, "deliver_notification",
            _poisoned_deliver({job}, f"template variable {_SENTINEL}"),
        ):
            await process_pending_notifications()

        assert _SENTINEL, "an empty needle would make this vacuous"
        async with get_session_factory()() as session:
            rows = (await session.execute(text(
                "SELECT row_to_json(n)::text FROM notifications n WHERE id = :id "
                "UNION ALL SELECT row_to_json(d)::text FROM "
                "notification_deliveries d WHERE notification_id = :id"
            ), {"id": job})).scalars().all()
        assert len(rows) == 2
        for row in rows:
            assert _SENTINEL not in row
        recorded = (await _fetch(job)).pipeline_error
        assert recorded, "the pair: the class must be recorded and non-empty"
        assert "_PlantedDefectError" in recorded

    def test_the_place_is_the_innermost_frame_in_comms(self) -> None:
        """pipeline_error_of names module:line of comms' code: here a
        TypeError raised inside processor.py itself."""
        with pytest.raises(TypeError) as caught:
            processor._pipeline_backoff("x")  # type: ignore[arg-type]
        recorded = pipeline_error_of(caught.value)
        assert recorded.startswith("builtins.TypeError at app/engine/processor.py:")
        assert recorded.rsplit(":", 1)[1].isdigit()

    def test_no_traceback_is_recorded_as_such(self) -> None:
        """An exception never raised has no frames: the place says so."""
        assert pipeline_error_of(_PlantedDefectError(_SENTINEL)) == (
            f"{__name__}._PlantedDefectError at no traceback"
        )


class TestTheChecks:
    """done-when (4): a twin with NULL for every new CHECK."""

    @pytest.mark.parametrize(
        ("attempts", "step", "error", "gate", "ok"),
        [
            (0, None, None, None, True),
            (1, "deliver", "X at y:1", None, True),
            (1, "deliver", "X at y:1", "now()", True),
            (1, None, "X at y:1", None, False),     # NULL step
            (1, "deliver", None, None, False),      # NULL error
            (1, "nowhere", "X at y:1", None, False),
            (0, "deliver", "X at y:1", None, False),
            (0, None, None, "now()", False),        # gate without record
        ],
    )
    async def test_notifications(
        self, db_session: AsyncSession, attempts: int, step: str | None,
        error: str | None, gate: str | None, ok: bool,
    ) -> None:
        recipient = await create_recipient(db_session)
        job = await _job(db_session, recipient.id, age_minutes=1)
        await db_session.commit()
        statement = text(
            "UPDATE notifications SET pipeline_attempts = :a, "
            "pipeline_step = :s, pipeline_error = :e, pipeline_retry_at = "
            + ("now()" if gate else "NULL") + " WHERE id = :id"
        )
        params: dict[str, Any] = {"a": attempts, "s": step, "e": error, "id": job}
        if ok:
            await db_session.execute(statement, params)
            await db_session.commit()
        else:
            with pytest.raises(IntegrityError):
                await db_session.execute(statement, params)
            await db_session.rollback()

    @pytest.mark.parametrize(
        ("status", "failure_class", "ok"),
        [
            ("failed", "pipeline", True),
            ("failed", None, False),
            ("pending", "pipeline", False),
        ],
    )
    async def test_deliveries_take_the_new_class(
        self, db_session: AsyncSession, status: str,
        failure_class: str | None, ok: bool,
    ) -> None:
        recipient = await create_recipient(db_session)
        job = await _job(db_session, recipient.id, age_minutes=1)
        delivery = NotificationDelivery(
            notification_id=job, recipient_id=recipient.id,
            channel="in_app", status=DeliveryStatus.PENDING,
        )
        db_session.add(delivery)
        await db_session.commit()
        statement = text(
            "UPDATE notification_deliveries SET status = :st, "
            "failure_class = :fc WHERE id = :id"
        )
        params = {"st": status, "fc": failure_class, "id": delivery.id}
        if ok:
            await db_session.execute(statement, params)
            await db_session.commit()
        else:
            with pytest.raises(IntegrityError):
                await db_session.execute(statement, params)
            await db_session.rollback()


# -- R6 -----------------------------------------------------------------------

_APP = Path(__file__).resolve().parents[1] / "app"
_SANITIZER = ("sanitize.py", "sanitized_traceback")


def _raw_traceback_calls(source: str, filename: str) -> list[str]:
    """Every form that renders a traceback without the redactor:
    `<x>.exception(...)`, `exc_info=` / `stack_info=` keywords, and
    traceback.format_* / print_* -- the last allowed only inside the
    redactor itself."""
    found: list[str] = []
    tree = ast.parse(source, filename)
    allowed: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and (
            Path(filename).name, node.name,
        ) == _SANITIZER:
            allowed |= {id(n) for n in ast.walk(node)}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "exception":
            found.append(f"{filename}:{node.lineno}: .exception()")
        for keyword in node.keywords:
            if keyword.arg in ("exc_info", "stack_info"):
                found.append(f"{filename}:{node.lineno}: {keyword.arg}=")
        if (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Name)
            and func.value.id == "traceback"
            and func.attr.startswith(("format_", "print_"))
            and id(node) not in allowed
        ):
            found.append(f"{filename}:{node.lineno}: traceback.{func.attr}")
    return found


class TestNoRawTraceback:
    def test_no_form_renders_a_traceback_raw_in_app(self) -> None:
        """R6 done-when (1), held by the code: every form of the gate,
        over the whole tree. M6: one site back on logger.exception."""
        files = sorted(_APP.rglob("*.py"))
        assert len(files) > 40, "the scan must walk the tree, not nothing"
        found = [
            hit
            for path in files
            for hit in _raw_traceback_calls(path.read_text(), str(path))
        ]
        assert found == []

    @pytest.mark.parametrize(
        "planted",
        [
            "logger.exception('x')",
            "log.error('x', exc_info=True)",
            "log.error('x', stack_info=True)",
            "traceback.format_exc()",
            "traceback.print_exc()",
        ],
    )
    def test_the_scanner_sees_every_form(self, planted: str) -> None:
        """The same gate on the tool: a scanner that sees nothing would
        keep the test above green on any code."""
        assert _raw_traceback_calls(planted, "planted.py")

    async def test_a_secret_in_a_pipeline_exception_does_not_reach_the_log(
        self, db_session: AsyncSession,
    ) -> None:
        """done-when (2), runtime: a secret glued at runtime, inside the
        exception's text, is redacted in the pipeline's log line. The
        pair: the line keeps a non-empty traceback."""
        secret = "hun" + "ter" + "2zzQ"
        recipient = await create_recipient(db_session)
        job = await _job(db_session, recipient.id, age_minutes=5)
        await db_session.commit()
        message = f"connect postgresql://comms:{secret}@db/comms failed"
        with capture_logs() as logs, patch.object(
            processor, "deliver_notification", _poisoned_deliver({job}, message),
        ):
            await process_pending_notifications()
        (entry,) = [
            log for log in logs if log["event"] == "notification_pipeline_error"
        ]
        assert "exc_info" not in entry
        assert entry["exception"].startswith("Traceback")
        assert "[redacted]" in entry["exception"]
        assert secret not in str(logs)

    async def test_a_secret_in_a_failed_auto_close_pass_does_not_reach_the_log(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The messaging pass cannot import the redactor; the worker
        logs its failure, redacted, under the same event and with the
        count of threads it had closed."""
        from app import worker
        from app.messaging import processor as messaging_processor

        secret = "hun" + "ter" + "2zzQ"

        async def boom(*_: Any, **__: Any) -> int:
            raise RuntimeError(f"redis://:{secret}@comms-redis:6379/0 down")

        monkeypatch.setattr(
            messaging_processor, "auto_close_idle_threads_batch", boom,
        )
        monkeypatch.setattr(worker, "run_engine_maintenance", _zero)
        monkeypatch.setattr(worker, "cleanup_terminal_notifications", _zero)
        monkeypatch.setattr(settings, "thread_auto_close_days", 30)
        monkeypatch.setattr(worker, "_last_auto_close_at", None)
        with capture_logs() as logs:
            await worker.run_maintenance_pass()
        (entry,) = [
            log for log in logs if log["event"] == "thread_auto_close_pass_error"
        ]
        assert entry["closed"] == 0
        assert entry["exception"].startswith("Traceback")
        assert secret not in str(logs)


async def _zero(*_: Any, **__: Any) -> int:
    return 0


# -- Migration 0015 -----------------------------------------------------------


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True,
    )


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    yield
    async with get_session_factory()() as session:
        await session.execute(text("DELETE FROM notifications"))
        await session.commit()
    await dispose_engine()
    done = _alembic("upgrade", "head")
    await dispose_engine()
    assert done.returncode == 0, done.stderr


class TestMigration0015:
    async def test_downgrade_refuses_while_a_pipeline_class_exists(
        self, db_session: AsyncSession, at_head_afterwards: None,
    ) -> None:
        """The old CHECK has no class for `pipeline`; picking one of the
        four channel classes would be a guess, so the downgrade refuses
        and names the count. The pair: with the row gone it passes."""
        recipient = await create_recipient(db_session)
        job = await _job(
            db_session, recipient.id, age_minutes=1,
            status=NotificationStatus.FAILED,
        )
        db_session.add(NotificationDelivery(
            notification_id=job, recipient_id=recipient.id, channel="in_app",
            status=DeliveryStatus.FAILED, failure_class=FailureClass.PIPELINE,
        ))
        await db_session.commit()
        await db_session.close()

        await dispose_engine()
        refused = _alembic("downgrade", "0014_resources_snapshot")
        await dispose_engine()
        assert refused.returncode != 0
        assert "1 deliveries carry failure_class 'pipeline'" in refused.stderr

        async with get_session_factory()() as session:
            await session.execute(text("DELETE FROM notifications"))
            await session.commit()
        await dispose_engine()
        passed = _alembic("downgrade", "0014_resources_snapshot")
        await dispose_engine()
        assert passed.returncode == 0, passed.stderr
