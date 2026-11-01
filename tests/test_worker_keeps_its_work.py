# =============================================================================
# TX-3 -- the worker keeps its work through a transient database failure
# =============================================================================
#
# What is held here:
#   1 a step of the pipeline failing with a REAL transient failure of the
#     database (tests/helpers.py real_serialization_failure, real_deadlock,
#     real_own_backend_terminated) more times than the ceiling: the job is
#     not closed, no attempt is spent; once the failure passes, it is
#     delivered. The pair: an ordinary failure as many times -> closed at
#     the ceiling, as before. A provider's network error (a real
#     aiohttp ClientConnectorError, a real httpx ConnectError) never
#     reaches the rule: it stays the delivery's outcome;
#   2 the journal: a transient wait is one row -- written when the wait
#     begins (pipeline_transient_since empty), again on a change of step
#     or class; a successful step and an ordinary failure end the wait;
#   3 a rollback on a dead connection and a failure record that cannot be
#     written do not take the lane down; the job rests in the process and
#     is taken again after;
#   4 the maintenance passes, each on its own; the cadence marks move
#     after success only;
#   5 a job's deliveries all end before its attempt rolls back; a stop is
#     never swallowed;
#   6 auto-close does not close a thread a client's message reached while
#     it waited (a race on real sessions, the wait by pg_blocking_pids);
#   7 the contract.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the step not asking the rule -> TestTransientOnAStep (closed at the
#       ceiling)
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
#   M10 the episode compared with the job's last step and error -> (b)
#   M11 an ordinary failure not ending the wait -> (d)
# =============================================================================

import asyncio
import re
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import aiohttp
import httpx
import pytest
from sqlalchemy import and_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app import worker
from app.core.config import settings
from app.core.database import get_engine, get_session_factory, is_transient_db_error
from app.core.lanes import Lane
from app.engine import processor, service
from app.engine.constants import ACTIVE_STATUSES, NotificationStatus, TargetType
from app.engine.models import Notification, NotificationTransition
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
# 1 -- a transient failure on a step spends no attempt
# ---------------------------------------------------------------------------

_TRANSIENT = {
    "40001": real_serialization_failure,
    "40P01": real_deadlock,
    "own backend": lambda: real_own_backend_terminated(mid_statement=True),
}


class TestTransientOnAStep:
    @pytest.mark.parametrize("kind", sorted(_TRANSIENT))
    async def test_more_than_the_ceiling_then_delivered(
        self, monkeypatch: pytest.MonkeyPatch, kind: str,
    ) -> None:
        failure = await _TRANSIENT[kind]()
        assert is_transient_db_error(failure)
        job_id = await _job()
        with monkeypatch.context() as patch:
            _failing(patch, "rollup_notification", failure)
            for _ in range(settings.notification_max_pipeline_attempts + 1):
                await _attempt(job_id)
                job = await _row(job_id)
                assert job.status in (NotificationStatus.PENDING,
                                      NotificationStatus.PROCESSING)
                assert job.pipeline_attempts == 0
                assert job.pipeline_retry_at is not None
                assert job.pipeline_transient_since is not None
                # (a) the counted failures' trace stays empty
                assert (job.pipeline_step, job.pipeline_error) == (None, None)
                assert job.pipeline_transient_step == "rollup"
        await _attempt(job_id)  # the failure has passed
        job = await _row(job_id)
        assert job.status == NotificationStatus.SENT
        assert job.pipeline_retry_at is None
        assert job.pipeline_transient_since is None

    async def test_the_pair_an_ordinary_failure_reaches_the_ceiling(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        failure = await real_lock_not_available()
        assert not is_transient_db_error(failure)
        job_id = await _job()
        # deliver, not rollup: the close at the ceiling goes through the fold
        _failing(monkeypatch, "deliver_notification", failure)
        for _ in range(settings.notification_max_pipeline_attempts):
            await _attempt(job_id)
        job = await _row(job_id)
        assert job.pipeline_attempts == settings.notification_max_pipeline_attempts
        assert job.status not in (NotificationStatus.PENDING,
                                  NotificationStatus.PROCESSING)

    @pytest.mark.parametrize("client", ["aiohttp", "httpx"])
    async def test_a_providers_network_error_is_the_deliverys(
        self, monkeypatch: pytest.MonkeyPatch, client: str,
    ) -> None:
        """A real ClientConnectorError (an OSError, ECONNREFUSED) and a
        real httpx.ConnectError from the channel are caught inside
        _deliver_single: the delivery's outcome, never a database gate."""
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
        assert job.pipeline_transient_since is None
        # the pair: a real transient failure of the database on that step
        failure = await real_serialization_failure()
        _failing(monkeypatch, "deliver_notification", failure)
        await _attempt(job_id)
        job = await _row(job_id)
        assert job.pipeline_attempts == 0
        assert job.pipeline_transient_since is not None


# ---------------------------------------------------------------------------
# 2 -- the journal: one row per wait
# ---------------------------------------------------------------------------


class TestTheWaitsRows:
    async def test_a_many_repeats_one_row(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job_id = await _job()
        _failing(monkeypatch, "rollup_notification", await real_serialization_failure())
        for _ in range(5):
            await _attempt(job_id)
        assert len(await _rows_with_error(job_id)) == 1

    async def test_b_a_success_ends_the_wait(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Transient -> a step that succeeds (the job stays open: the
        channel answered transiently) -> the same transient on the same
        step: a second row."""
        failure = await real_serialization_failure()
        job_id = await _job()
        with monkeypatch.context() as patch:
            _failing(patch, "rollup_notification", failure)
            await _attempt(job_id)

        async def no_fold(*_args: Any, **_kwargs: Any) -> None:
            return None

        with monkeypatch.context() as patch:
            # A step that succeeds and leaves the job open: a fold that
            # decides nothing yet.
            patch.setattr(processor, "rollup_notification", no_fold)
            await _attempt(job_id)
        job = await _row(job_id)
        assert job.pipeline_transient_since is None
        assert job.status == NotificationStatus.PROCESSING
        with monkeypatch.context() as patch:
            _failing(patch, "rollup_notification", failure)
            await _attempt(job_id)
        assert len(await _rows_with_error(job_id)) == 2

    async def test_c_a_change_of_step_or_class(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job_id = await _job()
        serialization = await real_serialization_failure()
        idle = await real_own_backend_terminated(mid_statement=False)
        with monkeypatch.context() as patch:
            _failing(patch, "rollup_notification", serialization)
            await _attempt(job_id)
        with monkeypatch.context() as patch:
            _failing(patch, "rollup_notification", idle)  # another class
            await _attempt(job_id)
        with monkeypatch.context() as patch:
            _failing(patch, "resolve_notification", idle)  # another step
            await _attempt(job_id)
        rows = await _rows_with_error(job_id)
        assert len(rows) == 3
        assert [r[0] for r in rows] == ["rollup", "rollup", "resolve"]

    async def test_d_an_ordinary_failure_of_the_same_class_ends_it(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The corner a comparison with the job's last error missed:
        transient (DBAPIError) -> ordinary (DBAPIError, 55P03) on the
        same step -> transient again: a row for the new wait."""
        job_id = await _job()
        transient = await real_serialization_failure()
        ordinary = await real_lock_not_available()
        assert type(transient) is type(ordinary)
        for failure in (transient, ordinary, transient):
            with monkeypatch.context() as patch:
                _failing(patch, "rollup_notification", failure)
                await _attempt(job_id)
        assert len(await _rows_with_error(job_id)) == 3
        job = await _row(job_id)
        assert job.pipeline_attempts == 1

    async def test_the_counted_trace_is_not_the_waits(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """(b) A counted failure, then a transient one: attempts, step and
        error stay the counted failure's; the wait's fields are the
        transient one's. (c) The wait ended by a step that succeeds: its
        fields empty, the counted trace in place."""
        job_id = await _job()
        ordinary = await real_lock_not_available()
        transient = await real_own_backend_terminated(mid_statement=False)
        with monkeypatch.context() as patch:
            _failing(patch, "rollup_notification", ordinary)
            await _attempt(job_id)
        counted = await _row(job_id)
        with monkeypatch.context() as patch:
            _failing(patch, "resolve_notification", transient)
            await _attempt(job_id)
        job = await _row(job_id)
        assert (job.pipeline_attempts, job.pipeline_step, job.pipeline_error) == (
            1, "rollup", counted.pipeline_error,
        )
        assert job.pipeline_transient_step == "resolve"
        assert job.pipeline_transient_error is not None
        assert job.pipeline_transient_error.startswith("sqlalchemy.exc.InterfaceError")

        async def not_now(*_args: Any, **_kwargs: Any) -> bool:
            return False

        with monkeypatch.context() as patch:
            patch.setattr(service.get_formatter("in_app"), "deliver", not_now)
            await _attempt(job_id)
        job = await _row(job_id)
        assert (job.pipeline_transient_since, job.pipeline_transient_step,
                job.pipeline_transient_error) == (None, None, None)
        assert (job.pipeline_attempts, job.pipeline_step, job.pipeline_error) == (
            1, "rollup", counted.pipeline_error,
        )

    async def test_the_database_holds_the_waits_shape(self) -> None:
        """(d) The wait's fields not whole, and a gate with no ground (no
        counted attempt, no open wait): refused by name."""
        job_id = await _job()
        for values, check in (
            ({"pipeline_retry_at": datetime.now(UTC),
              "pipeline_transient_since": datetime.now(UTC)},
             "ck_notifications_transient_wait_whole"),
            ({"pipeline_retry_at": datetime.now(UTC)},
             "ck_notifications_pipeline_gate"),
        ):
            with pytest.raises(IntegrityError, match=check):
                async with get_session_factory()() as s:
                    await s.execute(update(Notification)
                                    .where(Notification.id == job_id).values(**values))
                    await s.commit()


# ---------------------------------------------------------------------------
# 2b -- no close leaves a wait behind: in the code and in the database
# ---------------------------------------------------------------------------


async def _open_wait(job_id: UUID) -> None:
    """Put the job in an open transient wait, as the gate leaves it."""
    async with get_session_factory()() as s:
        await s.execute(update(Notification).where(Notification.id == job_id).values(
            pipeline_retry_at=datetime.now(UTC) - timedelta(seconds=1),
            pipeline_transient_since=datetime.now(UTC) - timedelta(minutes=1),
            pipeline_transient_step="rollup",
            pipeline_transient_error=(
                "sqlalchemy.exc.DBAPIError"
                " at app/engine/processor.py:1"
            ),
        ))
        await s.commit()


class TestNoCloseLeavesAWait:
    """(a) Every close of the TX-3 report's table, from an open wait: the
    field is empty after it and the CHECK did not fire. Under the
    mutation "one close skips end_transient_wait" the close itself fails
    in the database -- an IntegrityError of ck_notifications_transient_wait_open."""

    async def test_the_fold(self) -> None:
        job_id = await _job()
        await _open_wait(job_id)
        await _attempt(job_id)
        job = await _row(job_id)
        assert job.status == NotificationStatus.SENT
        assert job.pipeline_transient_since is None

    async def test_no_recipients(self) -> None:
        async with get_session_factory()() as s:
            job = await service.create_notification(
                s, **intake_fields(), type="unit_event_in_app", title="T", body="B",
                target_type=TargetType.USER, target_value=str(uuid4()),
            )
            await s.commit()
        await _open_wait(job.id)
        await _attempt(job.id)
        row = await _row(job.id)
        assert row.status == NotificationStatus.NO_RECIPIENTS
        assert row.pipeline_transient_since is None

    async def test_expiry(self) -> None:
        job_id = await _job()
        await _open_wait(job_id)
        async with get_session_factory()() as s:
            await s.execute(update(Notification).where(Notification.id == job_id)
                            .values(expiry_at=datetime.now(UTC) - timedelta(minutes=1)))
            await s.commit()
        await processor.expire_overdue_notifications(settings.worker_lanes)
        row = await _row(job_id)
        assert row.status == NotificationStatus.EXPIRED
        assert row.pipeline_transient_since is None

    async def test_cancellation_directly(self) -> None:
        """close_notifications itself, in a session of the test -- the
        path expiry, cancellation and forgetting share. Without its
        end_transient_wait the database refuses the close here, an
        IntegrityError of ck_notifications_transient_wait_open (the
        expiry pass above swallows its own failures into its log)."""
        job_id = await _job()
        await _open_wait(job_id)
        async with get_session_factory()() as s:
            closed = await service.close_notifications(
                s, Notification.id == job_id, NotificationStatus.CANCELLED,
            )
            await s.commit()
        assert closed == 1
        row = await _row(job_id)
        assert row.status == NotificationStatus.CANCELLED
        assert row.pipeline_transient_since is None
        assert row.pipeline_retry_at is None

    async def test_the_ceiling(self, monkeypatch: pytest.MonkeyPatch) -> None:
        job_id = await _job()
        _failing(monkeypatch, "deliver_notification", await real_lock_not_available())
        for _ in range(settings.notification_max_pipeline_attempts):
            await _open_wait(job_id)
            await _attempt(job_id)
        row = await _row(job_id)
        assert row.status not in ACTIVE_STATUSES
        assert row.pipeline_transient_since is None

    async def test_the_database_refuses_a_close_with_a_wait(self) -> None:
        job_id = await _job()
        await _open_wait(job_id)
        refused = "ck_notifications_transient_wait_open"
        with pytest.raises(IntegrityError, match=refused):
            async with get_session_factory()() as s:
                await s.execute(update(Notification).where(Notification.id == job_id)
                                .values(status=NotificationStatus.EXPIRED))
                await s.commit()

    async def test_the_checks_set_is_the_codes(self) -> None:
        """One copy: the CHECK's statuses are ACTIVE_STATUSES."""
        async with get_session_factory()() as s:
            definition = await s.scalar(text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname = 'ck_notifications_transient_wait_open'"
            ))
        assert definition is not None
        found = set(re.findall(r"'([a-z_]+)'::character varying", definition))
        assert found == {status.value for status in ACTIVE_STATUSES}


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


class TestTheWaitAndTheCount:
    async def test_an_ordinary_failure_from_a_wait_at_zero(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A wait at zero attempts, then an ordinary failure: the wait and
        its gate go first, the counted failure sets its own -- attempts 1,
        a gate, the wait's fields empty, no CHECK fired."""
        job_id = await _job()
        await _open_wait(job_id)
        _failing(monkeypatch, "deliver_notification", await real_lock_not_available())
        await _attempt(job_id)
        job = await _row(job_id)
        assert job.pipeline_attempts == 1
        assert job.pipeline_retry_at is not None
        assert job.pipeline_step == "deliver"
        assert (job.pipeline_transient_since, job.pipeline_transient_step) == (
            None, None,
        )

    async def test_the_wait_survives_a_failed_attempt(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The attempt clears the wait at its start -- in its own
        transaction: when it fails again on the same transient failure,
        the rollback gives the wait back, the same episode: one row, the
        same pipeline_transient_since."""
        failure = await real_serialization_failure()
        job_id = await _job()
        _failing(monkeypatch, "rollup_notification", failure)
        await _attempt(job_id)
        since = (await _row(job_id)).pipeline_transient_since
        await _attempt(job_id)
        await _attempt(job_id)
        assert (await _row(job_id)).pipeline_transient_since == since
        assert len(await _rows_with_error(job_id)) == 1

    @pytest.mark.parametrize("attempts", [0, 1])
    async def test_closes_from_both_counts(self, attempts: int) -> None:
        """Expiry (cancellation and forgetting close through the same
        close_notifications) from an open wait: at zero attempts the wait
        and its gate go; with an attempt counted the wait goes and the
        counted failure's gate stays. No CHECK fires."""
        job_id = await _job()
        await _open_wait(job_id)
        if attempts:
            async with get_session_factory()() as s:
                await s.execute(update(Notification)
                                .where(Notification.id == job_id).values(
                    pipeline_attempts=1, pipeline_step="deliver",
                    pipeline_error=(
                        "sqlalchemy.exc.DBAPIError"
                        " at app/engine/processor.py:2"
                    ),
                ))
                await s.commit()
        async with get_session_factory()() as s:
            await s.execute(update(Notification).where(Notification.id == job_id)
                            .values(expiry_at=datetime.now(UTC) - timedelta(minutes=1)))
            await s.commit()
        await processor.expire_overdue_notifications(settings.worker_lanes)
        job = await _row(job_id)
        assert job.status == NotificationStatus.EXPIRED
        assert job.pipeline_transient_since is None
        if attempts:
            assert job.pipeline_retry_at is not None
        else:
            assert job.pipeline_retry_at is None


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
        job_id = await _job()
        _failing(monkeypatch, "rollup_notification", failure)
        for _ in range(settings.notification_max_pipeline_attempts):
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

    async def test_a_transient_failure_of_the_close_is_a_wait(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The close at the ceiling fails once on a REAL 40001: a wait, not
        stuck; the next attempt closes the job at the ceiling."""
        job_id = await _job()
        ordinary = await real_lock_not_available()
        transient = await real_serialization_failure()
        _failing(monkeypatch, "deliver_notification", ordinary)
        real_close = processor._close_at_ceiling
        calls = 0

        async def close_once_failing(*args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise transient
            return await real_close(*args, **kwargs)

        monkeypatch.setattr(processor, "_close_at_ceiling", close_once_failing)
        ceiling = settings.notification_max_pipeline_attempts
        for _ in range(ceiling):
            await _attempt(job_id)
        waiting = await _row(job_id)
        assert waiting.pipeline_attempts == ceiling - 1
        assert waiting.pipeline_transient_since is not None
        await _attempt(job_id)
        closed = await _row(job_id)
        assert closed.status not in ACTIVE_STATUSES
        assert closed.pipeline_attempts == ceiling


@pytest.mark.usefixtures("pristine_tables")
class TestThePriceOfTheSelection:
    """The selection with not_stuck against the selection without it, on
    a seeded notifications table with fresh exact statistics, GENERIC
    plans (the value never bound -- plpgsql EXECUTE, as in TK-2).
    THE RULE, written before the measure: no Seq Scan on notifications,
    and buffers no worse than the old selection's by more than 1.25."""

    async def test_no_seq_scan_and_buffers(self) -> None:
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


def _without_not_stuck(now: datetime) -> Any:
    """ready_now as it was before TX-3: its terms less not_stuck."""
    full = processor.ready_now(now)
    return and_(*[c for c in full.clauses if "pipeline_attempts" not in str(c)])


def _shared_buffers(node: dict[str, Any]) -> int:
    total = node.get("Shared Hit Blocks", 0) + node.get("Shared Read Blocks", 0)
    return int(total)


class TestARecordTheDatabaseRefuses:
    async def test_a_refused_gate_takes_the_ordinary_path(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The gate of a transient failure refused for a reason no rest
        cures -- here 0015's ck_notifications_pipeline_gate put back for
        the length of the test: the ordinary path is taken, an attempt is
        counted. From then on 0015's CHECK allows the gate (an attempt is
        counted), so the next transient failures are gated in the
        database and journaled -- a visible wait, not a rest in memory and
        not a quiet loop."""
        failure = await real_serialization_failure()
        job_id = await _job()
        _failing(monkeypatch, "deliver_notification", failure)
        await _set_gate_check("pipeline_retry_at IS NULL OR pipeline_attempts > 0")
        try:
            for _ in range(settings.notification_max_pipeline_attempts):
                await _attempt(job_id)
        finally:
            await _set_gate_check(
                "pipeline_retry_at IS NULL OR pipeline_attempts > 0 "
                "OR pipeline_transient_since IS NOT NULL"
            )
        job = await _row(job_id)
        assert job.pipeline_attempts == 1  # the one fallback, counted
        assert job.pipeline_retry_at is not None
        assert job.pipeline_transient_since is not None
        assert job.status in ACTIVE_STATUSES
        assert len(await _rows_with_error(job_id)) == 2  # the count, the wait

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


async def _set_gate_check(definition: str) -> None:
    async with get_engine().begin() as conn:
        await conn.execute(text(
            "ALTER TABLE notifications DROP CONSTRAINT ck_notifications_pipeline_gate"
        ))
        await conn.execute(text(
            "ALTER TABLE notifications ADD CONSTRAINT "
            f"ck_notifications_pipeline_gate CHECK ({definition})"
        ))


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
    def test_the_wait_is_said(self) -> None:
        assert "A transient failure of comms' own database" in _DOC
        part = _DOC.split("**A wait on a transient failure of comms' database is "
                          "one row, not one\nper retry.**", 1)[1].split("\n\n", 1)[0]
        for said in ("does not spend an attempt", "when the wait begins",
                     "A step that succeeds ends the wait",
                     "an ordinary failure"):
            assert said in part.replace("\n", " "), said


