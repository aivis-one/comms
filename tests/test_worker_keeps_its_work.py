# =============================================================================
# TX-3 -- the worker keeps its work through a transient database failure
# =============================================================================
#
# What is held here:
#   0 the attempt takes a job only while it is still ready by the
#     selection's condition (WK-2): a gate, an expiry, the ceiling or no
#     delivery due, met between the selection and the lock on real
#     sessions -- nothing written, the gate kept;
#   1 a step of the pipeline failing with a REAL transient failure of the
#     database (tests/helpers.py real_serialization_failure, real_deadlock,
#     real_own_backend_terminated) -- the consumer's rule (WK-2): counted
#     while the database answers, up to the ceiling and the job's
#     outcome; while it does not answer (the REAL probe asking port 1),
#     nothing written, the job rests uncounted, delivered once it
#     answers. The pair: an ordinary failure, the probe not asked. A
#     provider's network error (a real aiohttp ClientConnectorError, a
#     real httpx ConnectError) never reaches the rule: it stays the
#     delivery's outcome. The database refuses a gate with no counted
#     failure (0015's CHECK, back since 0038);
#   3 a rollback on a dead connection and a failure record that cannot be
#     written do not take the lane down; the job rests in the process and
#     is taken again after; a record failing transiently is counted in
#     the process while the database answers, a rest while it does not;
#     a close at the ceiling failing transiently is STUCK while it
#     answers, nothing written while it does not;
#   4 the maintenance passes, each on its own; the cadence marks move
#     after success only;
#   5 a job's deliveries all end before its attempt rolls back; a stop is
#     never swallowed;
#   6 auto-close does not close a thread a client's message reached while
#     it waited (a race on real sessions, the wait by pg_blocking_pids);
#   7 the contract.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the lock without ready_now -> TestTheAttemptKeepsTheGate (WK-2)
#   M2  the step's rollback unguarded -> TestTheLaneLives
#       test_a_rollback_on_a_dead_connection (no gate recorded)
#   M3  the failure record's rollback unguarded -> the same class, the
#       record test (the attempt's last line logs instead)
#   M4  _attempt without its last line -> test_an_exception_past_the_record
#   M5  no rest in the process -> test_a_job_whose_failure_was_not_written
#   M6  the passes not isolated -> TestMaintenance
#   M7  the retention mark before the pass -> TestMaintenance
#   M8  gather without return_exceptions -> TestDeliveries
#   M9  the idle condition only in the subquery -> TestAutoClose
#   M10 a transient failure never counted (the probe not asked, the
#       database taken as silent) -> test_a_live_database_counts_it (WK-2)
#   M11 a transient failure always counted (the probe not asked, the
#       database taken as answering) -> test_a_silent_database_writes_nothing
#   M12 the close's transient failure a rest while the database answers ->
#       test_a_transient_failure_of_the_close_while_it_answers_is_stuck
#   M13 _kind_of without the probe -> test_a_record_failing_transiently_...
# =============================================================================

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import aiohttp
import httpx
import pytest
from sqlalchemy import and_, func, select, text, update
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app import worker
from app.core.config import settings
from app.core.database import get_engine, get_session_factory, is_transient_db_error
from app.core.lanes import Lane
from app.core.sanitize import pipeline_error_of
from app.engine import processor, service
from app.engine.constants import ACTIVE_STATUSES, NotificationStatus, TargetType
from app.engine.models import (
    Notification,
    NotificationDelivery,
    NotificationTransition,
)
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.models import Thread
from app.messaging.status import auto_close_idle_threads_batch
from app.messaging.threads import create_or_get_thread, post_message
from tests.helpers import (
    create_recipient,
    create_section,
    intake_fields,
    next_tx1_telegram_id,
    real_deadlock,
    real_own_backend_terminated,
    real_serialization_failure,
)

_REPO = Path(__file__).resolve().parent.parent
_BOUND = 10.0


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------


async def _job(recipients: int = 1) -> UUID:
    """A job for people on the in_app channel, committed."""
    async with get_session_factory()() as s:
        ids = []
        for _ in range(recipients):
            rid = uuid4()
            await create_recipient(s, recipient_id=rid,
                                   telegram_id=next_tx1_telegram_id())
            ids.append(rid)
        await s.commit()
        if recipients == 1:
            target_type, target_value = TargetType.USER, str(ids[0])
        else:
            target_type, target_value = TargetType.ALL, "*"
        job = await service.create_notification(
            s, **intake_fields(), type="unit_event_in_app", title="T", body="B",
            target_type=target_type, target_value=target_value,
        )
        await s.commit()
        return job.id


async def _row(job_id: UUID) -> Notification:
    async with get_session_factory()() as s:
        job = await s.get(Notification, job_id)
        assert job is not None
        return job


async def _rows_with_error(job_id: UUID) -> list[tuple[str, str]]:
    """The journal rows of the job that carry a pipeline error."""
    async with get_session_factory()() as s:
        rows = await s.execute(
            select(NotificationTransition.step, NotificationTransition.error)
            .where(NotificationTransition.notification_id == job_id,
                   NotificationTransition.error.is_not(None))
            .order_by(NotificationTransition.id)
        )
        return [(r[0], r[1]) for r in rows.all()]


def _failing(
    monkeypatch: pytest.MonkeyPatch, name: str, failure: BaseException,
) -> None:
    """The step `name` of the pipeline raises `failure`."""
    async def broken(*_args: Any, **_kwargs: Any) -> Any:
        raise failure

    monkeypatch.setattr(processor, name, broken)


async def _attempt(job_id: UUID) -> None:
    await processor._attempt_job(job_id, asyncio.Semaphore(4))


async def real_lock_not_available() -> Exception:
    """55P03 from Postgres -- an ORDINARY failure of the same class as a
    serialization failure (a bare DBAPIError): a row locked, NOWAIT."""
    async with get_session_factory()() as s:
        section = await create_section(s, key=f"tx3-{uuid4().hex[:8]}")
        await s.commit()
        sid = section.id
    factory = get_session_factory()
    async with factory() as holder, factory() as other:
        await holder.execute(text("SELECT 1 FROM sections WHERE id = :id FOR UPDATE"),
                             {"id": sid})
        try:
            await other.execute(
                text("SELECT 1 FROM sections WHERE id = :id FOR UPDATE NOWAIT"),
                {"id": sid},
            )
        except Exception as exc:
            await other.rollback()
            await holder.rollback()
            return exc
    raise AssertionError("no lock-not-available")


# ---------------------------------------------------------------------------
# 0 -- the attempt takes a job only while it is still ready (WK-2)
# ---------------------------------------------------------------------------


async def _deliveries(job_id: UUID) -> int:
    async with get_session_factory()() as s:
        return int(await s.scalar(
            select(func.count()).select_from(NotificationDelivery)
            .where(NotificationDelivery.notification_id == job_id)
        ) or 0)


async def _write(job_id: UUID, **values: Any) -> None:
    async with get_session_factory()() as s:
        await s.execute(update(Notification)
                        .where(Notification.id == job_id).values(**values))
        await s.commit()


async def _gated_by_another(job_id: UUID) -> None:
    """Another worker's failure record, committed between this pass's
    selection and its attempt's lock: one counted failure, a gate an
    hour ahead."""
    await _write(
        job_id, pipeline_attempts=1, pipeline_step="deliver",
        pipeline_error="builtins.RuntimeError at app/engine/processor.py:1",
        pipeline_retry_at=datetime.now(UTC) + timedelta(hours=1),
    )


async def _stuck_by_another(job_id: UUID) -> None:
    """Another worker's _record_stuck: at the ceiling, not closed."""
    await _write(
        job_id, pipeline_attempts=settings.notification_max_pipeline_attempts,
        pipeline_step="rollup",
        pipeline_error="builtins.RuntimeError at app/engine/processor.py:1",
    )


async def _expired_meanwhile(job_id: UUID) -> None:
    """Its expiry passed between the selection and the lock (time moved
    on; the sweep has not reached it)."""
    await _write(job_id, expiry_at=datetime.now(UTC) - timedelta(seconds=1))


async def _deliveries_put_behind_a_wait(job_id: UUID) -> None:
    """Another attempt delivered its portion and put every waiting
    delivery behind a wait: the job is PROCESSING with none due."""
    async with get_session_factory()() as s:
        await s.execute(update(NotificationDelivery)
                        .where(NotificationDelivery.notification_id == job_id)
                        .values(next_retry_at=datetime.now(UTC) + timedelta(hours=1),
                                wait_reason="transient_backoff"))
        await s.commit()


class TestTheAttemptKeepsTheGate:
    """done-when 1. Before WK-2 the attempt locked the row, checked its
    status only and wiped pipeline_retry_at: a gate another worker set
    between the selection and the lock was erased and the job went ahead
    of its backoff (reproduced before the fix: the gated job went SENT,
    its gate None). Now the lock carries ready_now itself.

    The race on REAL sessions and the REAL pass: process_lane selects the
    job; the attempt's task, before the real _attempt_job, lets another
    session commit its write -- the row as the lock then finds it. The
    twin "scheduled_at in the future" is not built: scheduled_at is
    written only at intake, so a selected job cannot become not yet
    scheduled (unreachable)."""

    async def _race(
        self, monkeypatch: pytest.MonkeyPatch, job_id: UUID,
        meanwhile: Callable[[UUID], Awaitable[None]],
    ) -> tuple[Notification, Notification, list[tuple[bool, bool]]]:
        before = await _row(job_id)
        real = processor._attempt_job
        results: list[tuple[bool, bool]] = []

        async def after_another(notif_id: UUID, *args: Any, **kwargs: Any) -> Any:
            assert notif_id == job_id  # the pass did select it
            await meanwhile(notif_id)
            result = await real(notif_id, *args, **kwargs)
            results.append(result)
            return result

        monkeypatch.setattr(processor, "_attempt_job", after_another)
        await processor.process_lane(Lane(before.lane))
        return before, await _row(job_id), results

    async def test_a_gate_set_meanwhile_is_kept(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job_id = await _job()
        _, after, results = await self._race(monkeypatch, job_id, _gated_by_another)
        assert results == [(False, False)]  # selected, not attempted
        assert after.pipeline_retry_at is not None
        assert after.pipeline_retry_at > datetime.now(UTC)
        assert after.pipeline_attempts == 1
        assert after.status == NotificationStatus.PENDING
        assert await _deliveries(job_id) == 0

    async def test_the_pair_a_job_still_ready_is_attempted(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The pair: the same race with a write that leaves the job ready
        -- an old gate, already past -- and the attempt goes and wipes it."""
        job_id = await _job()

        async def past_gate(notif_id: UUID) -> None:
            await _write(
                notif_id, pipeline_attempts=1, pipeline_step="deliver",
                pipeline_error="builtins.RuntimeError at app/engine/processor.py:1",
                pipeline_retry_at=datetime.now(UTC) - timedelta(seconds=1),
            )

        _, after, results = await self._race(monkeypatch, job_id, past_gate)
        assert results == [(True, False)]
        assert after.status == NotificationStatus.SENT
        assert after.pipeline_retry_at is None
        assert await _deliveries(job_id) == 1

    async def test_expired_meanwhile(self, monkeypatch: pytest.MonkeyPatch) -> None:
        job_id = await _job()
        before, after, results = await self._race(
            monkeypatch, job_id, _expired_meanwhile,
        )
        assert results == [(False, False)]
        assert after.status == NotificationStatus.PENDING
        assert (after.pipeline_attempts, after.pipeline_retry_at) == (
            before.pipeline_attempts, before.pipeline_retry_at,
        )
        assert await _deliveries(job_id) == 0

    async def test_stuck_meanwhile(self, monkeypatch: pytest.MonkeyPatch) -> None:
        job_id = await _job()
        _, after, results = await self._race(monkeypatch, job_id, _stuck_by_another)
        assert results == [(False, False)]
        assert after.status == NotificationStatus.PENDING
        assert after.pipeline_attempts == settings.notification_max_pipeline_attempts
        assert after.pipeline_retry_at is None
        assert await _deliveries(job_id) == 0

    async def test_no_delivery_due_meanwhile(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """PROCESSING, its deliveries committed by a first attempt whose
        channel did not take the letter yet; the second pass selects it,
        and before its lock every waiting delivery is put behind a wait:
        not attempted, the channel not called again."""
        calls = 0

        async def not_now(*_args: Any, **_kwargs: Any) -> bool:
            nonlocal calls
            calls += 1
            return False

        monkeypatch.setattr(service.get_formatter("in_app"), "deliver", not_now)
        job_id = await _job()
        await _attempt(job_id)
        first = await _row(job_id)
        assert first.status == NotificationStatus.PROCESSING
        assert calls == 1
        await _write_deliveries_due(job_id)
        _, after, results = await self._race(
            monkeypatch, job_id, _deliveries_put_behind_a_wait,
        )
        assert results == [(False, False)]
        assert calls == 1
        assert after.status == NotificationStatus.PROCESSING
        assert after.pipeline_attempts == 0


async def _write_deliveries_due(job_id: UUID) -> None:
    """The job's waiting deliveries due now (their wait run out)."""
    async with get_session_factory()() as s:
        await s.execute(update(NotificationDelivery)
                        .where(NotificationDelivery.notification_id == job_id)
                        .values(next_retry_at=None, wait_reason=None))
        await s.commit()


# ---------------------------------------------------------------------------
# 1 -- a transient failure on a step: the consumer's rule (WK-2)
# ---------------------------------------------------------------------------

_TRANSIENT = {
    "40001": real_serialization_failure,
    "40P01": real_deadlock,
    "own backend": lambda: real_own_backend_terminated(mid_statement=True),
}


def _silence_the_database(monkeypatch: pytest.MonkeyPatch) -> None:
    """The REAL probe (app/core/database.py database_answers), asking a
    database that does not answer: nothing listens on port 1. The
    probe reads the URL when it asks; the attempt's own engine is
    already built and is not touched."""
    silent = make_url(settings.database_url).set(port=1)
    monkeypatch.setattr(settings, "database_url",
                        silent.render_as_string(hide_password=False))


def _asked(monkeypatch: pytest.MonkeyPatch) -> list[bool]:
    """Every answer of the probe, as the worker gets it."""
    answers: list[bool] = []
    real = processor.database_answers

    async def asking() -> bool:
        answer = await real()
        answers.append(answer)
        return answer

    monkeypatch.setattr(processor, "database_answers", asking)
    return answers


async def _open_gate(job_id: UUID) -> None:
    """The backoff run out: the job's gate in the past -- time passing,
    as a lane meets it on its next pass after the gate."""
    async with get_session_factory()() as s:
        await s.execute(
            update(Notification)
            .where(Notification.id == job_id,
                   Notification.pipeline_retry_at.is_not(None))
            .values(pipeline_retry_at=datetime.now(UTC) - timedelta(seconds=1))
        )
        await s.commit()


async def _columns(job_id: UUID) -> dict[str, Any]:
    """Every column of the job's row, as the database holds it."""
    async with get_session_factory()() as s:
        row = (await s.execute(
            text("SELECT * FROM notifications WHERE id = :id"), {"id": job_id},
        )).mappings().one()
        return dict(row)


class TestTransientOnAStep:
    """done-when 2. Before WK-2 a transient failure gated the job without
    a count whether the database answered or not: a job whose rollup
    met a REAL deadlock every time went on until its expiry (reproduced
    before the fix: ceiling + 1 attempts, attempts 0, PENDING). The rule
    is the consumer's now: counted while the database answers, nothing
    written while it does not."""

    @pytest.mark.parametrize("kind", sorted(_TRANSIENT))
    async def test_a_live_database_counts_it(
        self, monkeypatch: pytest.MonkeyPatch, kind: str,
    ) -> None:
        failure = await _TRANSIENT[kind]()
        assert is_transient_db_error(failure)
        job_id = await _job()
        answers = _asked(monkeypatch)
        _failing(monkeypatch, "rollup_notification", failure)
        await _attempt(job_id)
        assert answers == [True]  # asked, and the database answered
        job = await _row(job_id)
        assert job.pipeline_attempts == 1
        assert job.pipeline_step == "rollup"
        assert job.pipeline_error is not None
        assert job.pipeline_error.split(" at ", 1)[0] == pipeline_error_of(
            failure).split(" at ", 1)[0]
        assert job.pipeline_retry_at is not None
        assert job.pipeline_retry_at > datetime.now(UTC)
        assert job.status in ACTIVE_STATUSES
        (row,) = await _rows_with_error(job_id)  # the path's row of it
        assert row[0] == "rollup"

    async def test_a_deadlock_every_time_closes_at_the_ceiling(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The ceiling by the ordinary path: one row per counted failure,
        under the step that tore, then the job's outcome. Deliver, not
        rollup: the close goes through the fold."""
        failure = await real_deadlock()
        job_id = await _job()
        _failing(monkeypatch, "deliver_notification", failure)
        ceiling = settings.notification_max_pipeline_attempts
        for _ in range(ceiling):
            await _open_gate(job_id)
            await _attempt(job_id)
        job = await _row(job_id)
        assert job.pipeline_attempts == ceiling
        assert job.status not in ACTIVE_STATUSES
        assert [r[0] for r in await _rows_with_error(job_id)] == ["deliver"] * ceiling

    @pytest.mark.parametrize("kind", sorted(_TRANSIENT))
    async def test_a_silent_database_writes_nothing(
        self, monkeypatch: pytest.MonkeyPatch, kind: str,
    ) -> None:
        """More attempts than the ceiling while the database does not
        answer: the row is the same, column by column; no row in the
        path; the job rests in the process, uncounted -- then, the
        database back, it is delivered."""
        failure = await _TRANSIENT[kind]()
        job_id = await _job()
        before = await _columns(job_id)
        kinds: list[Any] = []
        with monkeypatch.context() as patch:
            _failing(patch, "rollup_notification", failure)
            _silence_the_database(patch)
            with capture_logs() as logs:
                for _ in range(settings.notification_max_pipeline_attempts + 1):
                    await processor._attempt_job(
                        job_id, asyncio.Semaphore(4), rest=kinds.append,
                    )
        assert await _columns(job_id) == before
        assert await _rows_with_error(job_id) == []
        assert kinds == [processor._Recorded.TRANSIENT] * (
            settings.notification_max_pipeline_attempts + 1
        )
        silent = [e for e in logs
                  if e["event"] == "notification_pipeline_database_silent"]
        assert len(silent) == len(kinds)
        await _attempt(job_id)
        assert (await _row(job_id)).status == NotificationStatus.SENT

    async def test_the_lane_rests_it_uncounted(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """In the lane: a rest of the backoff, not a count toward the park."""
        job_id = await _job()
        _failing(monkeypatch, "rollup_notification", await real_deadlock())
        _silence_the_database(monkeypatch)
        jobs = await _in_a_lane(job_id)
        assert job_id in jobs.resting
        assert jobs.resting[job_id] != float("inf")
        assert jobs.unrecorded == {}

    async def test_the_pair_an_ordinary_failure_reaches_the_ceiling(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The probe is not asked for an ordinary failure. Since WK-2 the
        gate holds the next attempt back, so the backoff is run out
        between attempts (_open_gate) -- before, the attempt wiped it."""
        failure = await real_lock_not_available()
        assert not is_transient_db_error(failure)
        job_id = await _job()
        answers = _asked(monkeypatch)
        # deliver, not rollup: the close at the ceiling goes through the fold
        _failing(monkeypatch, "deliver_notification", failure)
        for _ in range(settings.notification_max_pipeline_attempts):
            await _open_gate(job_id)
            await _attempt(job_id)
        job = await _row(job_id)
        assert job.pipeline_attempts == settings.notification_max_pipeline_attempts
        assert job.status not in (NotificationStatus.PENDING,
                                  NotificationStatus.PROCESSING)
        assert answers == []

    @pytest.mark.parametrize("client", ["aiohttp", "httpx"])
    async def test_a_providers_network_error_is_the_deliverys(
        self, monkeypatch: pytest.MonkeyPatch, client: str,
    ) -> None:
        """A real ClientConnectorError (an OSError, ECONNREFUSED) and a
        real httpx.ConnectError from the channel are caught inside
        _deliver_single: the delivery's outcome, never the job's count.
        The pair (WK-2): a real transient failure of the database on that
        step, the database answering -- counted. Since WK-2 the attempt
        takes only a ready job, so the delivery's wait is run out first
        (_write_deliveries_due) -- before, the attempt took it anyway."""
        async def refusing(*_args: Any, **_kwargs: Any) -> bool:
            if client == "aiohttp":
                async with aiohttp.ClientSession() as http:
                    await http.get("http://127.0.0.1:1/")
            else:
                async with httpx.AsyncClient() as http:
                    await http.get("http://127.0.0.1:1/")
            return True

        formatter = service.get_formatter("in_app")
        monkeypatch.setattr(formatter, "deliver", refusing)
        job_id = await _job()
        await _attempt(job_id)
        job = await _row(job_id)
        assert job.pipeline_attempts == 0
        assert job.pipeline_retry_at is None
        failure = await real_serialization_failure()
        _failing(monkeypatch, "deliver_notification", failure)
        await _write_deliveries_due(job_id)
        await _attempt(job_id)
        job = await _row(job_id)
        assert job.pipeline_attempts == 1
        assert job.pipeline_step == "deliver"


class TestTheGateHasAGround:
    """Since 0038 a gate stands only behind a counted failure again
    (0015's ck_notifications_pipeline_gate): no path writes a gate at
    zero attempts -- the wait of TX-3 that did is gone with its fields."""

    async def test_a_gate_with_no_counted_failure_is_refused(self) -> None:
        job_id = await _job()
        with pytest.raises(IntegrityError, match="ck_notifications_pipeline_gate"):
            await _write(job_id, pipeline_retry_at=datetime.now(UTC))

    async def test_the_pair_a_gate_behind_a_counted_failure(self) -> None:
        job_id = await _job()
        await _gated_by_another(job_id)
        assert (await _row(job_id)).pipeline_retry_at is not None


# ---------------------------------------------------------------------------
# 3 -- the lane lives
# ---------------------------------------------------------------------------


async def _terminate(session: AsyncSession) -> None:
    """Terminate the backend of `session` -- this test's own."""
    pid = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
    async with get_engine().connect() as killer:
        await killer.execute(text("SELECT pg_terminate_backend(:p)"), {"p": pid})
        await killer.commit()
    await asyncio.sleep(0.2)


async def _in_a_lane(*job_ids: UUID) -> processor.LaneJobs:
    """Each job attempted by _attempt in one lane task group, as the
    worker does; returns the lane's jobs."""
    async with asyncio.TaskGroup() as group:
        jobs = processor.LaneJobs(Lane.NORMAL, group)
        tally = processor._PassTally(lane=Lane.NORMAL, total=len(job_ids),
                                     started=time.monotonic())
        for job_id in job_ids:
            jobs.in_work.add(job_id)
            await jobs.places.acquire()
            group.create_task(processor._attempt(job_id, jobs, tally, None))
    return jobs


class TestTheLaneLives:
    async def test_a_rollback_on_a_dead_connection(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The step's own connection dies, then the step raises: the
        rollback raises too (measured). Guarded: the failure is still
        recorded, the next job of the lane is processed."""
        broken_job, fine_job = await _job(), await _job()
        real_rollup = service.rollup_notification

        async def dying(session: AsyncSession, notification: Notification) -> Any:
            if notification.id == broken_job:
                await _terminate(session)
                raise RuntimeError("the step tore")
            return await real_rollup(session, notification)

        monkeypatch.setattr(processor, "rollup_notification", dying)
        with capture_logs() as logs:
            await _in_a_lane(broken_job, fine_job)
        assert (await _row(fine_job)).status == NotificationStatus.SENT
        broken = await _row(broken_job)
        assert broken.pipeline_attempts == 1  # recorded despite the rollback
        events = [e["event"] for e in logs]
        assert "notification_rollback_error" in events
        assert "notification_attempt_error" not in events

    async def test_the_record_on_a_dead_connection(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The REAL failure record: its own connection dies on its commit,
        then a non-database error -- its rollback raises too (the second
        case of the measurement). Guarded, the record reports False: the
        job rests, and the attempt's last line is not what saved the lane."""
        job_id = await _job()
        _failing(monkeypatch, "rollup_notification", RuntimeError("tore"))

        async def dying_commit(self: AsyncSession) -> None:
            await _terminate(self)
            raise RuntimeError("the record tore")

        monkeypatch.setattr(AsyncSession, "commit", dying_commit)
        with capture_logs() as logs:
            jobs = await _in_a_lane(job_id)
        events = [e["event"] for e in logs]
        assert job_id in jobs.resting
        assert "notification_pipeline_record_error" in events
        assert "notification_attempt_error" not in events

    async def test_an_exception_past_the_record(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Whatever still escapes the attempt is the attempt's last line's:
        logged, the job rests, the other job of the lane is processed."""
        broken_job, fine_job = await _job(), await _job()
        real_rollup = service.rollup_notification

        async def tearing(session: AsyncSession, notification: Notification) -> Any:
            if notification.id == broken_job:
                raise RuntimeError("tore")
            return await real_rollup(session, notification)

        async def raising_record(*_args: Any) -> bool:
            raise RuntimeError("the record itself")

        monkeypatch.setattr(processor, "rollup_notification", tearing)
        monkeypatch.setattr(processor, "_record_pipeline_failure", raising_record)
        with capture_logs() as logs:
            jobs = await _in_a_lane(broken_job, fine_job)
        assert (await _row(fine_job)).status == NotificationStatus.SENT
        assert broken_job in jobs.resting
        (line,) = [e for e in logs if e["event"] == "notification_attempt_error"]
        assert line["error"].startswith("builtins.RuntimeError at ")

    async def test_a_job_whose_failure_was_not_written_rests_then_returns(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job_id = await _job()

        async def unwritten(*_args: Any) -> Any:
            return processor._Recorded.TRANSIENT

        with monkeypatch.context() as patch:
            _failing(patch, "rollup_notification", RuntimeError("tore"))
            patch.setattr(processor, "_record_pipeline_failure", unwritten)
            jobs = await _in_a_lane(job_id)
        assert job_id in jobs.resting
        # Not selected while it rests ...
        async with asyncio.TaskGroup() as group:
            jobs.attempts = group
            assert await processor._pass(Lane.NORMAL, jobs, None) is None
        # ... and taken again once the rest has run out.
        jobs.resting[job_id] = time.monotonic() - 1
        async with asyncio.TaskGroup() as group:
            jobs.attempts = group
            tally = await processor._pass(Lane.NORMAL, jobs, None)
        assert tally is not None
        assert (await _row(job_id)).status == NotificationStatus.SENT
        assert jobs.resting == {}


class TestTheRest:
    def test_it_does_not_leak(self) -> None:
        jobs = processor.LaneJobs(Lane.NORMAL, None)  # type: ignore[arg-type]
        past = time.monotonic() - 1
        jobs.resting = {uuid4(): past for _ in range(1000)}
        jobs.purge_rest()
        assert jobs.resting == {}

    def test_a_rest_still_running_stays(self) -> None:
        jobs = processor.LaneJobs(Lane.NORMAL, None)  # type: ignore[arg-type]
        job = uuid4()
        jobs.rest(job)
        jobs.purge_rest()
        assert job in jobs.resting


class TestABrokenFold:
    """BEFORE TX-3 (established in the TX-3 report): a job whose rollup
    failed every time was counted up to the ceiling less one, and AT the
    ceiling the close -- through the same fold -- failed and took its
    record back: never closed, never at the ceiling, taken again every
    pass, forever. Now the close's failure leaves the job STUCK, written
    so, and the lane's selection leaves it."""

    async def _to_the_ceiling(
        self, monkeypatch: pytest.MonkeyPatch, failure: BaseException,
    ) -> UUID:
        """Since WK-2 the gate holds the next attempt back: the backoff is
        run out between attempts (_open_gate)."""
        job_id = await _job()
        _failing(monkeypatch, "rollup_notification", failure)
        for _ in range(settings.notification_max_pipeline_attempts):
            await _open_gate(job_id)
            await _attempt(job_id)
        return job_id

    async def _selected(self, job_id: UUID) -> bool:
        """Whether a FRESH lane (a restart, or a second worker process --
        either has its own LaneJobs) selects the job."""
        jobs = processor.LaneJobs(Lane.NORMAL, None)  # type: ignore[arg-type]
        async with asyncio.TaskGroup() as group:
            jobs.attempts = group
            tally = await processor._pass(Lane.NORMAL, jobs, None)
        return tally is not None

    async def test_stuck_and_left(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with capture_logs() as logs:
            job_id = await self._to_the_ceiling(monkeypatch, RuntimeError("fold"))
        job = await _row(job_id)
        ceiling = settings.notification_max_pipeline_attempts
        assert job.pipeline_attempts == ceiling
        assert job.status in ACTIVE_STATUSES
        assert job.pipeline_retry_at is None
        assert job.pipeline_error is not None
        assert job.pipeline_error.startswith("builtins.RuntimeError at ")
        (line,) = [e for e in logs if e["event"] == "notification_pipeline_stuck"]
        assert "fold" not in str(line)  # class and place, never the text
        assert not await self._selected(job_id)  # a restart
        assert not await self._selected(job_id)  # a second process

    async def test_a_raised_ceiling_selects_it_again(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job_id = await self._to_the_ceiling(monkeypatch, RuntimeError("fold"))
        assert not await self._selected(job_id)
        monkeypatch.setattr(
            settings, "notification_max_pipeline_attempts",
            settings.notification_max_pipeline_attempts + 1,
        )
        assert await self._selected(job_id)

    async def _close_failing_once(
        self, monkeypatch: pytest.MonkeyPatch, transient: BaseException,
    ) -> tuple[UUID, list[Any]]:
        """Ordinary failures of deliver up to the ceiling; at the ceiling
        the close fails once on `transient`. Returns the job and the kinds
        the attempts reported."""
        job_id = await _job()
        _failing(monkeypatch, "deliver_notification", await real_lock_not_available())
        real_close = processor._close_at_ceiling
        calls = 0

        async def close_once_failing(*args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise transient
            return await real_close(*args, **kwargs)

        monkeypatch.setattr(processor, "_close_at_ceiling", close_once_failing)
        kinds: list[Any] = []
        for _ in range(settings.notification_max_pipeline_attempts):
            await _open_gate(job_id)
            await processor._attempt_job(job_id, asyncio.Semaphore(4),
                                         rest=kinds.append)
        return job_id, kinds

    async def test_a_transient_failure_of_the_close_while_it_answers_is_stuck(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """WK-2 (replaces TX-3's "a transient failure of the close is a
        wait", right while a transient failure was never counted): the
        close at the ceiling fails on a REAL 40001 while the database
        answers -- an ordinary failure of the close: STUCK, recorded,
        no longer selected. A retry of the close would be TX-3's loop."""
        transient = await real_serialization_failure()
        job_id, kinds = await self._close_failing_once(monkeypatch, transient)
        job = await _row(job_id)
        assert kinds == []  # every failure written
        assert job.pipeline_attempts == settings.notification_max_pipeline_attempts
        assert job.status in ACTIVE_STATUSES
        assert job.pipeline_error is not None
        assert job.pipeline_error.startswith("sqlalchemy.exc.DBAPIError at ")
        assert not await self._selected(job_id)

    async def test_the_pair_while_the_database_is_silent_nothing_is_written(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The same close failure while the database does not answer:
        nothing written -- the count stays below the ceiling, the job
        rests; once the database answers, the next attempt -- its step
        failing as before -- reaches the ceiling and the close goes
        through."""
        transient = await real_serialization_failure()
        with monkeypatch.context() as patch:
            silent_from = settings.notification_max_pipeline_attempts
            real_answers = processor.database_answers
            asked = 0

            async def silent_at_the_close() -> bool:
                nonlocal asked
                asked += 1
                with pytest.MonkeyPatch.context() as inner:
                    _silence_the_database(inner)
                    return await real_answers()

            patch.setattr(processor, "database_answers", silent_at_the_close)
            job_id, kinds = await self._close_failing_once(monkeypatch, transient)
            assert asked == 1  # only the close's failure is transient
        job = await _row(job_id)
        assert kinds == [processor._Recorded.TRANSIENT]
        assert job.pipeline_attempts == silent_from - 1
        assert job.status in ACTIVE_STATUSES
        await _open_gate(job_id)
        await _attempt(job_id)
        closed = await _row(job_id)
        assert closed.status not in ACTIVE_STATUSES
        assert closed.pipeline_attempts == settings.notification_max_pipeline_attempts


@pytest.mark.usefixtures("pristine_tables")
class TestThePriceOfTheSelection:
    """The selection with not_stuck against the selection without it, on
    a seeded notifications table with fresh exact statistics, GENERIC
    plans (the value never bound -- plpgsql EXECUTE, as in TK-2).
    THE RULE, written before the measure: no Seq Scan on notifications,
    and buffers no worse than the old selection's by more than 1.25."""

    async def test_no_seq_scan_and_buffers(self) -> None:
        await _seed_the_price()
        now = datetime.now(UTC)
        new = select(Notification.id).where(
            Notification.lane == Lane.NORMAL.value, processor.ready_now(now),
        ).order_by(Notification.scheduled_at).limit(50)
        old = select(Notification.id).where(
            Notification.lane == Lane.NORMAL.value, _without_not_stuck(now),
        ).order_by(Notification.scheduled_at).limit(50)
        plans = {}
        buffers = {}
        async with get_session_factory()() as s:
            await s.execute(text(
                "CREATE FUNCTION pg_temp.generic_plan(q text) RETURNS SETOF "
                "text LANGUAGE plpgsql AS $f$ BEGIN RETURN QUERY EXECUTE "
                "'EXPLAIN (GENERIC_PLAN) ' || q; END $f$"
            ))
            raw = await s.connection()
            for name, statement in (("new", new), ("old", old)):
                compiled = statement.compile(dialect=raw.dialect)
                sql = str(compiled)
                params = tuple(
                    compiled.params[key] for key in compiled.positiontup or []
                )
                plans[name] = "\n".join(await s.scalars(text(
                    "SELECT * FROM pg_temp.generic_plan(:q)"
                ), {"q": sql}))
                analyzed: Any = (await raw.exec_driver_sql(
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params,
                )).scalar_one()
                buffers[name] = _shared_buffers(analyzed[0]["Plan"])
            await s.rollback()
        print(f"\nGENERIC new\n{plans['new']}\nbuffers {buffers}")
        for plan in plans.values():
            assert "$1" in plan, plan
        assert "Seq Scan on notifications" not in plans["new"], plans["new"]
        assert buffers["new"] <= 1.25 * max(buffers["old"], 1), buffers


    async def test_the_attempts_lock_reads_one_row(self) -> None:
        """WK-2: the attempt's lock carries ready_now (processor._lock_of)
        -- on the same seeded table, its plan reads the job by its primary
        key, never a Seq Scan of notifications or of its deliveries."""
        await _seed_the_price()
        async with get_session_factory()() as s:
            job_id = await s.scalar(text(
                "SELECT id FROM notifications WHERE status = 'pending' LIMIT 1"
            ))
            raw = await s.connection()
            compiled = processor._lock_of(job_id, datetime.now(UTC)).compile(
                dialect=raw.dialect,
            )
            params = tuple(compiled.params[key] for key in compiled.positiontup or [])
            plan: Any = (await raw.exec_driver_sql(
                "EXPLAIN (FORMAT JSON) " + str(compiled), params,
            )).scalar_one()
            await s.rollback()
        nodes = _plan_nodes(plan[0]["Plan"])
        scans = {(n["Node Type"], n.get("Relation Name"), n.get("Index Name"))
                 for n in nodes if "Scan" in n["Node Type"]}
        assert ("Index Scan", "notifications", "notifications_pkey") in scans, scans
        assert not [n for n in scans if n[0] == "Seq Scan"], scans


async def _seed_the_price() -> None:
    """notifications seeded as TestThePriceOfTheSelection measures it:
    40000 jobs, one in ten pending, one in fifty at the ceiling, fresh
    exact statistics."""
    async with get_session_factory()() as s:
        await s.execute(text(
            "INSERT INTO notifications (id, type, title, body, target_type, "
            "target_value, scheduled_at, status, created_at, idempotency_key, "
            "fingerprint, channels, expiry_layer, pipeline_attempts, push_on, "
            "retry_max_attempts, retry_backoff_seconds, recipient_schedule, "
            "lane, pipeline_step, pipeline_error) SELECT gen_random_uuid(), "
            "'unit_event_in_app', 'T', 'B', "
            "'all', '*', now() - interval '1 hour', CASE WHEN g % 10 = 0 "
            "THEN 'pending' ELSE 'sent' END, now(), 'price:' || g, "
            "repeat('f', 64), '[\"in_app\"]', 'none', CASE WHEN g % 50 = 0 "
            "THEN 3 ELSE 0 END, 'none', 3, 30, 'respect', CASE WHEN g % 4 = 0 "
            "THEN 'bulk' ELSE 'normal' END, CASE WHEN g % 50 = 0 THEN 'rollup' "
            "END, CASE WHEN g % 50 = 0 THEN 'builtins.RuntimeError at "
            "app/engine/processor.py:1' END FROM generate_series(1, 40000) g"
        ))
        await s.commit()
    async with get_engine().connect() as connection:
        auto = await connection.execution_options(isolation_level="AUTOCOMMIT")
        await auto.execute(text("ANALYZE notifications"))


def _plan_nodes(node: dict[str, Any]) -> list[dict[str, Any]]:
    out = [node]
    for child in node.get("Plans", []):
        out.extend(_plan_nodes(child))
    return out


def _without_not_stuck(now: datetime) -> Any:
    """ready_now as it was before TX-3: its terms less not_stuck."""
    full = processor.ready_now(now)
    return and_(*[c for c in full.clauses if "pipeline_attempts" not in str(c)])


def _shared_buffers(node: dict[str, Any]) -> int:
    total = node.get("Shared Hit Blocks", 0) + node.get("Shared Read Blocks", 0)
    return int(total)


class TestARecordTheDatabaseRefuses:
    async def _record_failing_on(
        self, monkeypatch: pytest.MonkeyPatch, failure: BaseException,
    ) -> processor.LaneJobs:
        """The step fails (ordinary); the failure's record fails on
        `failure` at its journal row. One attempt in a lane."""
        job_id = await _job()

        def refused(*_args: Any, **_kwargs: Any) -> None:
            raise failure

        _failing(monkeypatch, "rollup_notification", RuntimeError("tore"))
        monkeypatch.setattr(processor.journal, "record_job", refused)
        return await _in_a_lane(job_id)

    async def test_a_record_failing_transiently_while_it_answers_is_counted(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """WK-2 (_kind_of by the rule): the record meets a REAL 40001
        while the database answers -- counted in the process toward the
        park (LaneJobs.unrecordable), not a rest without a count."""
        jobs = await self._record_failing_on(
            monkeypatch, await real_serialization_failure(),
        )
        (job_id,) = jobs.resting
        assert jobs.unrecorded == {job_id: 1}

    async def test_the_pair_while_the_database_is_silent_a_rest(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        failure = await real_serialization_failure()
        _silence_the_database(monkeypatch)
        jobs = await self._record_failing_on(monkeypatch, failure)
        (job_id,) = jobs.resting
        assert jobs.resting[job_id] != float("inf")
        assert jobs.unrecorded == {}

    async def test_nothing_can_be_written_the_job_is_parked(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Both records refused for good: said loudly every time, parked
        in the process at the ceiling -- attempts bounded, not a loop."""
        job_id = await _job()
        unique = (await real_constraint_violations_once())

        def refused(*_args: Any, **_kwargs: Any) -> None:
            raise unique

        _failing(monkeypatch, "rollup_notification", RuntimeError("tore"))
        monkeypatch.setattr(processor.journal, "record_job", refused)
        jobs = processor.LaneJobs(Lane.NORMAL, None)  # type: ignore[arg-type]
        attempts = 0
        with capture_logs() as logs:
            for _ in range(settings.notification_max_pipeline_attempts + 2):
                if jobs.resting.get(job_id) == float("inf"):
                    break  # parked: the lane no longer takes it
                attempts += 1
                async with asyncio.TaskGroup() as group:
                    jobs.attempts = group
                    tally = processor._PassTally(lane=Lane.NORMAL, total=1,
                                                 started=time.monotonic())
                    jobs.in_work.add(job_id)
                    await jobs.places.acquire()
                    group.create_task(processor._attempt(job_id, jobs, tally, None))
        assert attempts == settings.notification_max_pipeline_attempts
        assert jobs.resting[job_id] == float("inf")
        events = [e["event"] for e in logs]
        assert events.count("notification_pipeline_unrecordable") == attempts
        assert "notification_pipeline_parked" in events


async def real_constraint_violations_once() -> Exception:
    from tests.helpers import real_constraint_violations

    return (await real_constraint_violations())["unique"]


# ---------------------------------------------------------------------------
# 4 -- the maintenance passes, each on its own
# ---------------------------------------------------------------------------

_PASSES = {
    "engine": "run_engine_maintenance",
    "close": "consume_close_notifications",
    "retention": "cleanup_terminal_notifications",
    "auto_close": "auto_close_idle_threads",
}


@pytest.fixture
def passes(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    calls = dict.fromkeys(_PASSES, 0)
    for name, attribute in _PASSES.items():
        async def run(name: str = name) -> int:
            calls[name] += 1
            return 0
        monkeypatch.setattr(worker, attribute, run)
    monkeypatch.setattr(settings, "notification_retention_days", 30)
    monkeypatch.setattr(settings, "thread_auto_close_days", 30)
    worker.reset_retention_gate()
    worker.reset_auto_close_gate()
    yield calls
    worker.reset_retention_gate()
    worker.reset_auto_close_gate()


class TestMaintenance:
    @pytest.mark.parametrize("failing", sorted(_PASSES))
    async def test_one_fails_the_others_run(
        self, monkeypatch: pytest.MonkeyPatch, passes: dict[str, int], failing: str,
    ) -> None:
        async def broken() -> int:
            passes[failing] += 1
            raise RuntimeError("down")

        monkeypatch.setattr(worker, _PASSES[failing], broken)
        with capture_logs() as logs:
            await worker.run_maintenance_pass()
        assert all(count == 1 for count in passes.values()), passes
        assert any(e["event"] in ("maintenance_pass_error",
                                  "thread_auto_close_pass_error") for e in logs)

    async def test_a_failed_retention_runs_again_next_cycle(
        self, monkeypatch: pytest.MonkeyPatch, passes: dict[str, int],
    ) -> None:
        async def broken() -> int:
            passes["retention"] += 1
            raise RuntimeError("down")

        monkeypatch.setattr(worker, _PASSES["retention"], broken)
        await worker.run_maintenance_pass()
        assert worker._last_retention_at is None
        await worker.run_maintenance_pass()
        assert passes["retention"] == 2

    async def test_the_pair_marks_move_on_success(
        self, passes: dict[str, int],
    ) -> None:
        await worker.run_maintenance_pass()
        assert worker._last_retention_at is not None
        assert worker._last_auto_close_at is not None
        await worker.run_maintenance_pass()
        assert passes["retention"] == 1  # within its interval
        assert passes["engine"] == 2


# ---------------------------------------------------------------------------
# 5 -- a job's deliveries end before its attempt rolls back
# ---------------------------------------------------------------------------


class TestDeliveries:
    async def test_every_delivery_ends_before_the_rollback(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job_id = await _job(recipients=2)
        moments: dict[str, float] = {}
        real_single = service._deliver_single
        first: list[UUID] = []

        async def single(semaphore: Any, formatter: Any, notification: Any,
                         delivery: Any, recipient: Any, call: Any = None) -> Any:
            if not first:
                first.append(delivery.id)
                raise RuntimeError("one delivery tore")
            await asyncio.sleep(0.3)
            result = await real_single(semaphore, formatter, notification,
                                       delivery, recipient, call)
            moments["sent"] = time.monotonic()
            return result

        real_rollback = processor._guarded_rollback

        async def rollback(session: AsyncSession, notif_id: Any) -> None:
            moments["rollback"] = time.monotonic()
            await real_rollback(session, notif_id)

        monkeypatch.setattr(service, "_deliver_single", single)
        monkeypatch.setattr(processor, "_guarded_rollback", rollback)
        await _attempt(job_id)
        assert "sent" in moments and "rollback" in moments
        assert moments["sent"] < moments["rollback"]

    async def test_a_stop_is_not_swallowed(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job_id = await _job(recipients=2)
        started = asyncio.Event()

        async def slow(*_args: Any, **_kwargs: Any) -> Any:
            started.set()
            await asyncio.sleep(30)

        monkeypatch.setattr(service, "_deliver_single", slow)
        attempt = asyncio.ensure_future(_attempt(job_id))
        await asyncio.wait_for(started.wait(), _BOUND)
        attempt.cancel()
        with pytest.raises(asyncio.CancelledError):
            await attempt


# ---------------------------------------------------------------------------
# 6 -- auto-close against a client's message
# ---------------------------------------------------------------------------


async def _pid(session: AsyncSession) -> int:
    pid = await session.scalar(text("SELECT pg_backend_pid()"))
    assert isinstance(pid, int)
    return pid


async def _waiting_on(holder: int) -> bool:
    async with get_session_factory()() as s:
        return bool(await s.scalar(text(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = "
            "current_database() AND CAST(:h AS integer) = ANY(pg_blocking_pids(pid))"
        ), {"h": holder}))


async def _idle_thread() -> tuple[UUID, UUID]:
    now = datetime.now(UTC)
    async with get_session_factory()() as s:
        customer = uuid4()
        await create_recipient(s, recipient_id=customer,
                               telegram_id=next_tx1_telegram_id())
        section = await create_section(s, key=f"tx3-{uuid4().hex[:8]}")
        thread = await create_or_get_thread(
            s, **intake_fields(), client=customer,
            operator_kind=OperatorKind.SECTION, operator_value=section.id,
            kind=ThreadKind.TICKET, subject_type="practice",
            subject_id=uuid4().hex,
        )
        await s.execute(update(Thread).where(Thread.id == thread.id)
                        .values(created_at=now - timedelta(days=40)))
        await s.commit()
        return thread.id, customer


class TestAutoClose:
    async def test_a_message_that_arrived_while_it_waited(self) -> None:
        now = datetime.now(UTC)
        tid, customer = await _idle_thread()
        factory = get_session_factory()
        async with factory() as late, factory() as early:
            await post_message(
                early, thread_id=tid, sender=customer, body="still here",
                idempotency_key=f"tx3:{uuid4()}", fingerprint="f" * 64,
            )
            holder = await _pid(early)
            closing = asyncio.ensure_future(auto_close_idle_threads_batch(
                late, cutoff=now - timedelta(days=30), when=now, limit=1000,
            ))
            deadline = time.monotonic() + _BOUND
            while not await _waiting_on(holder):
                assert not closing.done(), "auto-close never waited"
                assert time.monotonic() < deadline
                await asyncio.sleep(0.02)
            await early.commit()
            closed = await asyncio.wait_for(closing, _BOUND)
            await late.commit()
        assert closed == 0
        async with factory() as s:
            row = await s.get(Thread, tid)
            assert row is not None
            assert row.status == "open"
            assert row.close_notify_pending_at is None

    async def test_the_pair_an_idle_thread_is_closed(self) -> None:
        now = datetime.now(UTC)
        tid, _ = await _idle_thread()
        async with get_session_factory()() as s:
            closed = await auto_close_idle_threads_batch(
                s, cutoff=now - timedelta(days=30), when=now, limit=1000,
            )
            await s.commit()
        assert closed == 1
        async with get_session_factory()() as s:
            row = await s.get(Thread, tid)
            assert row is not None
            assert row.status == "closed"


# ---------------------------------------------------------------------------
# 7 -- the contract
# ---------------------------------------------------------------------------

_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


class TestTheContract:
    """deploy/INTEGRATION.md, the job's `pipeline` and its path, against
    the code both ways. WK-2 replaces TX-3's "the wait is said": a
    transient failure of the database is counted while it answers, and
    no gate stands at zero attempts."""

    def _pipeline(self) -> str:
        return _DOC.split("### `pipeline`", 1)[1].split("**A job STUCK", 1)[0]

    def test_the_rule_is_said(self) -> None:
        block = self._pipeline().replace("\n", " ")
        for said in ("counts while that database answers",
                     "while it does not answer, nothing is counted or written",
                     "never with `attempts` 0"):
            assert said in block, said
        part = _DOC.split("**A transient failure of comms' database.**", 1)[1]
        part = part.split("\n\n", 1)[0].replace("\n", " ")
        for said in ("an attempt is counted", "at the ceiling the job gets its outcome",
                     "nothing can be written", "no attempt is counted"):
            assert said in part, said

    def test_the_old_wait_is_not_said(self) -> None:
        """"X is not" with "Y is": the old paragraph gone, the new one
        there (test_the_rule_is_said)."""
        assert "one row, not one\nper retry" not in _DOC
        assert "**A transient failure of comms' database.**" in _DOC

    async def test_the_database_holds_what_it_says(self) -> None:
        """The code's side: the gate at zero attempts the contract rules
        out is refused by the database (0015's CHECK, back since 0038)."""
        async with get_session_factory()() as s:
            definition = await s.scalar(text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname = 'ck_notifications_pipeline_gate'"
            ))
        assert definition == (
            "CHECK (((pipeline_retry_at IS NULL) OR (pipeline_attempts > 0)))"
        )
