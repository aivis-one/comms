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
from app.engine import processor
from app.engine.constants import (
    DeliveryStatus,
    FailureClass,
    NotificationStatus,
    PipelineStep,
    TargetType,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import pipeline_error_of, process_pending_notifications
from tests.helpers import create_recipient, notification_row_fields

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

    async def deliver(session: AsyncSession, notification: Notification) -> None:
        if notification.id in poisoned:
            raise _PlantedDefectError(message)
        await _REAL_DELIVER(session, notification)

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

    async def test_a_job_that_recovers_keeps_its_record_and_loses_its_gate(
        self, db_session: AsyncSession,
    ) -> None:
        """Below the ceiling a success closes the gate (the CHECK allows a
        gate only behind a record, and the record stays as history)."""
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
        assert row.pipeline_attempts == 1
        assert row.pipeline_step == PipelineStep.DELIVER
        assert row.pipeline_retry_at is None


class TestNoStarvation:
    async def test_healthy_job_goes_out_the_tick_after_the_poison_first_fails(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """done-when (2) / gate correction (a): batch_size + 1 poisoned
        rows, all OLDER than the healthy one. Tick 1 takes a full batch
        of poison; tick 2 must deliver the healthy job. M2: without the
        gate tick 2 takes the same poison again."""
        batch = 3
        monkeypatch.setattr(settings, "notification_batch_size", batch)
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
        monkeypatch.setattr(settings, "notification_batch_size", batch)
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
_SANITIZER = ("formatters.py", "sanitized_traceback")


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
        monkeypatch.setattr(worker, "run_notification_batch", _zero)
        monkeypatch.setattr(worker, "cleanup_terminal_notifications", _zero)
        monkeypatch.setattr(settings, "thread_auto_close_days", 30)
        monkeypatch.setattr(worker, "_last_auto_close_at", None)
        with capture_logs() as logs:
            await worker.run_worker_batch()
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
