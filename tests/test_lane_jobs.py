# =============================================================================
# P5-L -- the jobs of a lane are attempted side by side; a long call
# holds one of the lane's places, not the lane
# =============================================================================
#
# Section 0 holds the facts the code stands on, measured before it was
# written: a session gives its connection back to the pool at commit
# and at rollback (the failure record of a torn attempt costs no second
# connection); cancelling the lane's loop reaches an attempt inside its
# channel call and rolls it back; the selection with its LIMIT grown by
# the jobs in work keeps its plan (pristine tables, fresh statistics
# among the states).
#
# The rest drives the real pipeline -- the lane loop where it says so
# (app/worker.py _lane_loop), real sessions in the races.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  one place per lane (LANE_JOBS = 1)      -> TestTheLongJob,
#       TestModelCallsSideBySide, TestTwoAttemptsOfOneLane
#   M2  the slots a job's own, not the lane's   -> TestTheSlotsAreTheLanes
#   M3  no limit of jobs at once                -> TestModelCallsSideBySide
#   M4  a job in work not dropped by the pass   -> TestThePass,
#       tests/test_lane_wake.py TestTheStorm
#   M5  the LIMIT not grown by the jobs in work -> TestThePass
#   M6  places above the pool                   -> tests/test_lanes.py
#       TestTheBudgets.test_the_lanes_fit_the_pool
#   M7  places above a lane's slots             -> TestThePlaces
#   M8  the selection outside a place           -> TestThePass
#   M9  attempts outside the lane loop's group  -> TestCancellingTheLoop,
#       TestNoAttemptOutlivesTheLoop
#   M11 the texts off the code                  -> TestTheContract,
#       tests/test_llm_channel.py TestTheLaneWarning
#   M12 the failure recorded before the rollback -> TestTheConnectionGoesBack
#   M13 the jobs in work dropped by the live set,
#       not the one taken before the selection -> TestThePass
# NOT BUILT: on_more called before the place is given back -- no await
# lies between the two in _attempt, so in one event loop the order is
# not observable; it matters only if an await is put there.
# =============================================================================

import asyncio
import contextlib
import json
import shutil
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID

import httpx
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

import app.engine.processor as processor
import app.worker as worker
import tests.test_llm_channel as llm
import tests.test_waiting_probe as probe
from app.core.config import settings
from app.core.database import get_engine, get_session_factory
from app.core.lanes import Lane
from app.engine import formatters as formatters_module
from app.engine.constants import NotificationStatus, TargetType
from app.engine.formatters import build_formatters
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import process_lane
from app.engine.service import (
    LANE_CONCURRENCY,
    LANE_JOBS,
    create_notification,
    resolve_notification,
    withdraw_recipient,
)
from app.profile.loader import FileProfileSource, install_profile, load_profile
from app.profile.registry import Decided, Layer, TypeRecord, registry
from tests.conftest import make_tables_pristine
from tests.helpers import create_recipient, intake_fields
from tests.test_waiting_probe import _captured as h12_capture

_BOUND = 10.0


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _declare(type_key: str, lane: Lane) -> None:
    """The type's record with `lane` declared (the profile layer)."""
    record = registry.record_of(type_key)
    assert record is not None
    fields = dict(record.fields)
    fields["lane"] = Decided(lane.value, Layer.PROFILE, "test")
    registry.register_type(
        type_key, category=registry.category_of(type_key),
        record=TypeRecord(fields=fields),
    )


async def _recipient() -> UUID:
    async with get_session_factory()() as session:
        recipient = await create_recipient(session)
        await session.commit()
        return recipient.id


async def _job_to(recipient: UUID, type_key: str = "unit_event_in_app") -> UUID:
    async with get_session_factory()() as session:
        job = await create_notification(
            session, **intake_fields(), type=type_key, title="T", body="B",
            target_type=TargetType.USER, target_value=str(recipient),
        )
        await session.commit()
        return job.id


async def _state(nid: UUID) -> tuple[str, list[str]]:
    async with get_session_factory()() as session:
        job = await session.get(Notification, nid)
        assert job is not None
        deliveries = list((await session.execute(
            select(NotificationDelivery.status)
            .where(NotificationDelivery.notification_id == nid)
        )).scalars())
        return job.status, deliveries


def _checked_out() -> int:
    pool: Any = get_engine().pool
    return int(pool.checkedout())


class _Held:
    """Every channel; the calls of `held` jobs wait for `release`."""

    def __init__(self) -> None:
        self.held: set[UUID] = set()
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0
        # Set once `enough_at` calls have come.
        self.enough_at = 1
        self.enough = asyncio.Event()

    async def deliver(self, notification: Any, *rest: Any) -> bool:
        self.calls += 1
        if self.calls >= self.enough_at:
            self.enough.set()
        if notification.id in self.held:
            self.started.set()
            await self.release.wait()
        return True


@asynccontextmanager
async def _channel(spy: Any) -> AsyncIterator[Any]:
    with patch("app.engine.service.get_formatter", return_value=spy):
        yield spy


# -----------------------------------------------------------------------------
# 0. The facts the code stands on
# -----------------------------------------------------------------------------


class TestTheConnectionGoesBack:
    """A session holds a pooled connection from its first statement to
    the end of its transaction -- commit or rollback -- not to the end of
    its `async with`. The budget counts an attempt as ONE connection,
    and a torn attempt's failure record (in a second session, opened
    inside the first's `async with`, after its rollback) as the same
    one."""

    async def test_commit_and_rollback_each_give_it_back(self) -> None:
        base = _checked_out()
        async with get_session_factory()() as session:
            assert _checked_out() == base
            await session.execute(text("SELECT 1"))
            assert _checked_out() == base + 1
            await session.commit()
            assert _checked_out() == base
            await session.execute(text("SELECT 1"))
            assert _checked_out() == base + 1
            await session.rollback()
            assert _checked_out() == base

    async def test_the_failure_record_is_the_attempts_one_connection(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        nid = await _job_to(await _recipient())
        seen: list[int] = []
        record = processor._record_pipeline_failure

        async def counted(*args: Any) -> None:
            seen.append(_checked_out())
            await record(*args)

        async def torn(*args: Any) -> bool:
            raise RuntimeError("torn")

        monkeypatch.setattr(processor, "_record_pipeline_failure", counted)
        monkeypatch.setattr(processor, "deliver_notification", torn)
        base = _checked_out()
        await process_lane(Lane.NORMAL)
        # The attempt's session was rolled back: its connection is in
        # the pool while the record opens its own.
        assert seen == [base]
        # The pair: the record was written.
        async with get_session_factory()() as session:
            job = await session.get(Notification, nid)
            assert job is not None
            assert job.pipeline_attempts == 1


class TestCancellingTheLoop:
    """SIGTERM cancels the lane's loop; an attempt in its channel call is
    cancelled at that await and its session rolled back -- nothing it did
    commits, before or after the call would have ended. M9."""

    async def test_an_attempt_in_its_call_is_rolled_back(self) -> None:
        _declare("unit_event_in_app", Lane.INTERACTIVE)
        nid = await _job_to(await _recipient())
        spy = _Held()
        spy.held.add(nid)
        wake = asyncio.Event()
        base = _checked_out()
        async with _channel(spy):
            loop = asyncio.ensure_future(
                worker._lane_loop(Lane.INTERACTIVE, wake),
            )
            await asyncio.wait_for(spy.started.wait(), _BOUND)
            loop.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await loop
            # The call would end now: nothing is left to commit it.
            spy.release.set()
            await asyncio.sleep(0.3)
        assert await _state(nid) == (NotificationStatus.PENDING, [])
        assert _checked_out() == base
        # The pair: the job is there, untouched.
        async with get_session_factory()() as session:
            assert (await session.execute(
                select(func.count()).select_from(Notification)
            )).scalar_one() == 1


# The places a normal pass may find taken by its own jobs at most (the
# pass holds one itself).
_IN_WORK = LANE_JOBS[Lane.NORMAL] - 1


async def _normal_selection() -> tuple[str, Any]:
    """The normal lane's selection as the code sends it, captured on the
    empty pristine tables."""
    with h12_capture() as seen, patch(
        "app.engine.service.get_formatter", return_value=_Held(),
    ):
        await process_lane(Lane.NORMAL)
    (statement, parameters), = [
        (s, p) for s, p in seen
        if "notifications.lane =" in s and "ORDER BY" in s
    ]
    return statement, parameters


def _with_limit(parameters: Any, limit: int) -> Any:
    """The selection's parameters with its LIMIT -- the last one, the
    lane's batch as captured -- set to `limit`."""
    values = list(parameters)
    assert values[-1] == settings.lane_batch_size(Lane.NORMAL)
    values[-1] = limit
    return tuple(values)


async def _seed_lanes(statistics: str) -> None:
    """tests/test_lanes.py TestThePlan's seed: 50 000 jobs over four
    lanes, most sent, 2 000 active; statistics fresh or none."""
    async with get_session_factory()() as session:
        await session.execute(text(
            "INSERT INTO notifications (id, type, title, body, target_type, "
            "target_value, idempotency_key, fingerprint, channels, "
            "expiry_layer, push_on, retry_max_attempts, "
            "retry_backoff_seconds, recipient_schedule, lane, status, "
            "scheduled_at) "
            "SELECT gen_random_uuid(), 'unit_bare', 'T', 'B', 'all', '*', "
            "'p5l-seed-' || g, repeat('a', 64), '[\"in_app\"]'::jsonb, "
            "'default', 'none', 3, 30, 'respect', "
            "(ARRAY['interactive','critical','normal','bulk'])[g % 4 + 1], "
            "CASE WHEN g % 25 <> 0 THEN 'sent' "
            "     WHEN g % 50 = 0 THEN 'processing' ELSE 'pending' END, "
            "now() - (g % 5000) * interval '1 minute' "
            "FROM generate_series(1, 50000) g"
        ))
        if statistics == "taken_after_the_sends":
            await session.execute(text("ANALYZE notifications"))
            await session.execute(text("ANALYZE notification_deliveries"))
        await session.commit()


_PLAN_STATES = [
    *probe._STATES,
    *[
        (f"lanes-{statistics}", lambda _form, s: _seed_lanes(s), "", statistics)
        for statistics in ("taken_after_the_sends", "none")
    ],
]


@pytest.mark.usefixtures("pristine_tables")
class TestTheSelectionWithJobsInWork:
    """A pass selects its lane's batch PLUS the jobs it has in work, and
    drops those: the statement is the one every pass sends, its LIMIT
    larger by at most LANE_JOBS - 1. Explained with that LIMIT on every
    state the selection's plan tests build (tests/test_waiting_probe.py,
    tests/test_lanes.py TestThePlan), fresh statistics among them: the
    jobs by the lane index, never a scan; the deliveries by their
    probe's index, within its budget, never a hash."""

    @pytest.mark.parametrize(
        ("seed", "form", "statistics"),
        [state[1:] for state in _PLAN_STATES],
        ids=[state[0] for state in _PLAN_STATES],
    )
    async def test_the_larger_limit_keeps_the_plan(
        self, seed: Any, form: str, statistics: str,
    ) -> None:
        statement, parameters = await _normal_selection()
        await make_tables_pristine()
        await seed(form, statistics)
        limit = settings.lane_batch_size(Lane.NORMAL) + _IN_WORK
        root = await probe._explain(statement, _with_limit(parameters, limit))
        nodes = probe._nodes(root["Plan"])
        assert not [
            n for n in nodes
            if n["Node Type"] == "Seq Scan"
            and n.get("Relation Name") == "notifications"
        ], root
        on_deliveries = [
            n for n in nodes
            if n.get("Relation Name") == "notification_deliveries"
            or n.get("Index Name", "").startswith("ix_notification_deliveries")
        ]
        if form:
            # The probe's states hold waiting deliveries: read by the
            # probe's index only. The lanes' seed holds none -- there a
            # scan of the empty table is free; the budget below holds it.
            assert {n.get("Index Name") for n in on_deliveries} == {
                probe._INDEX,
            }, root
        assert not [
            n for n in nodes if "hashed" in n.get("Subplan Name", "")
        ], root
        on_lane = [
            n for n in nodes
            if n.get("Index Name") == "ix_notifications_lane_status_scheduled"
        ]
        # The pair: the jobs ARE read, by the lane.
        assert on_lane, root
        assert all("lane" in n["Index Cond"] for n in on_lane), root
        for node in on_deliveries:
            if "Actual Loops" in node:
                assert (
                    probe._buffers(node) / max(1, node["Actual Loops"])
                    <= probe._BUDGET
                ), node


# -----------------------------------------------------------------------------
# More helpers
# -----------------------------------------------------------------------------


async def _until(nids: list[UUID], status: str = NotificationStatus.SENT) -> None:
    """Until every job of `nids` is in `status` (bounded)."""
    async with asyncio.timeout(_BOUND):
        while True:
            states = [(await _state(nid))[0] for nid in nids]
            if all(state == status for state in states):
                return
            await asyncio.sleep(0.02)


async def _waiting_on_a_lock() -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(text(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE wait_event_type = 'Lock' AND datname = current_database()"
        ))).scalar_one())


async def _until_someone_waits() -> None:
    for _ in range(int(_BOUND / 0.02)):
        if await _waiting_on_a_lock():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("nobody waits on a lock")


async def _row_is_held(nid: UUID) -> bool:
    """Whether another transaction holds the job's row (FOR UPDATE NOWAIT
    from a third session: 55P03 when it does)."""
    async with get_session_factory()() as session:
        try:
            await session.execute(
                select(Notification.id).where(Notification.id == nid)
                .with_for_update(nowait=True)
            )
        except DBAPIError as exc:
            await session.rollback()
            assert "could not obtain lock" in str(exc)
            return True
        await session.rollback()
        return False


async def _all_to(type_key: str) -> UUID:
    async with get_session_factory()() as session:
        job = await create_notification(
            session, **intake_fields(), type=type_key, title="T", body="B",
            target_type=TargetType.ALL, target_value="*",
        )
        await session.commit()
        return job.id


class _Counting:
    """Every channel, answering yes after `seconds`; counts the calls in
    flight at once."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.now = 0
        self.most = 0

    async def deliver(self, *args: Any) -> bool:
        self.now += 1
        self.most = max(self.most, self.now)
        await asyncio.sleep(self.seconds)
        self.now -= 1
        return True


def _attempt_tasks() -> list[asyncio.Task[Any]]:
    return [
        task for task in asyncio.all_tasks()
        if task.get_coro().__qualname__ == processor._attempt.__qualname__  # type: ignore[union-attr]
    ]


# -----------------------------------------------------------------------------
# 1. A long job holds one place, not the lane
# -----------------------------------------------------------------------------


class TestTheLongJob:
    """done-when 1 (1): through the real lane loop (app/worker.py
    _lane_loop), interactive -- two places. One job is held in its
    channel call; the lane's other jobs, selected with it and accepted
    after it, are delivered while it is still in the call. The boundary
    is explicit: the held call is released only after. M1."""

    async def test_the_others_go_while_it_is_in_its_call(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "notification_poll_interval_seconds", 30)
        monkeypatch.setattr(settings, "notification_max_backoff_seconds", 30)
        _declare("unit_event_in_app", Lane.INTERACTIVE)
        recipient = await _recipient()
        held = await _job_to(recipient)
        others = [await _job_to(recipient) for _ in range(3)]
        spy = _Held()
        spy.held.add(held)
        wake = asyncio.Event()
        async with _channel(spy):
            loop = asyncio.ensure_future(worker._lane_loop(Lane.INTERACTIVE, wake))
            try:
                await asyncio.wait_for(spy.started.wait(), _BOUND)
                await _until(others)
                # Accepted while the long call goes on; the wake intake
                # would give.
                late = await _job_to(recipient)
                wake.set()
                await _until([late])
                # The boundary: the long call has not ended.
                assert not spy.release.is_set()
                assert (await _state(held))[0] == NotificationStatus.PENDING
                spy.release.set()
                await _until([held])
            finally:
                loop.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await loop


# -----------------------------------------------------------------------------
# 1 (2), (3). N model calls of one lane: ceil(N / places), never more
# -----------------------------------------------------------------------------


_CALL = 1.0


@pytest.fixture
def model(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> Iterator[llm._Provider]:
    """tests/test_llm_channel.py's deploy: the llm channel live on the
    mock provider, the profile with its llm types (lane: normal)."""
    root = tmp_path / "profile"
    shutil.copytree(llm._FIXTURE / "templates", root / "templates")
    types = (llm._FIXTURE / "types.yaml").read_text(encoding="utf-8")
    (root / "types.yaml").write_text(types + llm._LLM_TYPES, encoding="utf-8")
    provider = llm._Provider()
    monkeypatch.setattr(settings, "llm_key_sets", json.dumps(llm._SETS))
    registry.reset()
    install_profile(load_profile(
        FileProfileSource(root), llm_set_names=tuple(settings.llm_sets),
    ))
    monkeypatch.setattr(formatters_module, "_registry", build_formatters(
        settings,
        lambda **kwargs: None,
        llm_http_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(provider.handle),
        ),
    ))
    yield provider


class _SlowModel:
    """The provider's answer after `seconds`; counts the calls at once."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.now = 0
        self.most = 0

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.now += 1
        self.most = max(self.most, self.now)
        await asyncio.sleep(self.seconds)
        self.now -= 1
        return httpx.Response(200, content=llm._completion_bytes())


async def _model_jobs(provider: llm._Provider, n: int, call: float) -> _SlowModel:
    slow = _SlowModel(call)
    provider.on(llm._GW_HOST, *([slow] * n))
    for _ in range(n):
        await llm._send(llm._request(llm._key()))
    return slow


class TestModelCallsSideBySide:
    """done-when 1 (2), (3): N llm jobs of one lane, each one model call
    of _CALL seconds. M1, M3."""

    async def test_n_calls_take_ceil_n_over_the_places(
        self, model: llm._Provider,
    ) -> None:
        places = LANE_JOBS[Lane.NORMAL]
        n = 8
        slow = await _model_jobs(model, n, _CALL)
        started = time.monotonic()
        assert await process_lane(Lane.NORMAL) == n
        took = time.monotonic() - started
        # ceil(n / places) rounds -- two -- not n calls end to end.
        rounds = -(-n // places)
        assert rounds * _CALL <= took < (rounds + 1) * _CALL
        assert took < n * _CALL / 2
        # (3) never more than the places at once -- and all of them.
        assert slow.most == places
        async with get_session_factory()() as session:
            assert (await session.execute(
                select(func.count()).select_from(Notification)
                .where(Notification.status == NotificationStatus.SENT)
            )).scalar_one() == n

    async def test_bulk_takes_them_one_after_another(
        self, model: llm._Provider,
    ) -> None:
        """The pair: bulk has one place -- its llm jobs go strictly one
        after another (deploy/INTEGRATION.md says so)."""
        assert LANE_JOBS[Lane.BULK] == 1
        _declare("ask.model", Lane.BULK)
        n, call = 3, 0.3
        slow = await _model_jobs(model, n, call)
        started = time.monotonic()
        assert await process_lane(Lane.BULK) == n
        assert time.monotonic() - started >= n * call
        assert slow.most == 1


class TestTheSlotsAreTheLanes:
    """The lane's slots are one semaphore over all its jobs in work: two
    interactive jobs to two people each -- four deliveries, two slots --
    call at most two at once, and two (the pair: the counter counts).
    M2."""

    async def test_two_jobs_share_the_lanes_slots(self) -> None:
        _declare("unit_event_in_app", Lane.INTERACTIVE)
        for _ in range(2):
            await _recipient()
        nids = [await _all_to("unit_event_in_app") for _ in range(2)]
        spy = _Counting(0.2)
        async with _channel(spy):
            assert await process_lane(Lane.INTERACTIVE) == 2
        await _until(nids)
        assert spy.most == LANE_CONCURRENCY[Lane.INTERACTIVE] == 2


# -----------------------------------------------------------------------------
# 1. The pass: a batch of jobs not in work; the selection under a place
# -----------------------------------------------------------------------------


class TestThePass:
    async def test_a_pass_takes_a_batch_of_jobs_not_in_work(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Interactive, batch 3: the first pass starts three, the oldest
        held in its call; the second pass -- while it is -- selects the
        batch PLUS the job in work, drops it, and starts three more. The
        held job is called once. M4 (the job in work kept: two new, the
        held one attempted again and skipped), M5 (the LIMIT not grown:
        two new)."""
        monkeypatch.setattr(settings, "notification_lane_interactive_batch_size", 3)
        _declare("unit_event_in_app", Lane.INTERACTIVE)
        recipient = await _recipient()
        nids = [await _job_to(recipient) for _ in range(6)]
        spy = _Held()
        spy.held.add(nids[0])
        calls: list[UUID] = []
        deliver = spy.deliver

        async def counted(notification: Any, *rest: Any) -> bool:
            calls.append(notification.id)
            return await deliver(notification, *rest)

        spy.deliver = counted  # type: ignore[method-assign]
        async with _channel(spy), asyncio.TaskGroup() as group:
            jobs = processor.LaneJobs(Lane.INTERACTIVE, group)
            assert await process_lane(Lane.INTERACTIVE, jobs=jobs) == 3
            await asyncio.wait_for(spy.started.wait(), _BOUND)
            await _until(nids[1:3])
            assert jobs.in_work == {nids[0]}
            assert await process_lane(Lane.INTERACTIVE, jobs=jobs) == 3
            await _until(nids[3:])
            assert (await _state(nids[0]))[0] == NotificationStatus.PENDING
            spy.release.set()
        assert jobs.in_work == set()
        await _until(nids)
        assert calls.count(nids[0]) == 1
        assert sorted(map(str, calls)) == sorted(map(str, nids))

    async def test_a_job_that_ends_during_the_selection_is_not_new(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """FIX (the stand, TestTheStorm 18 == 19): the jobs in work are
        taken BEFORE the selection. Interactive, batch 3: the oldest job
        is held in its call through the first pass; the second pass's
        selection sees it due -- and the held call ends, the job SENT and
        out of the lane's work, before its rows come back. It is still
        dropped: the pass starts three NEW jobs, and the held job is
        attempted once. M13 (dropped by the live set: the ended job
        counted as one of the batch's new -- two new started, the last
        job left behind)."""
        monkeypatch.setattr(settings, "notification_lane_interactive_batch_size", 3)
        _declare("unit_event_in_app", Lane.INTERACTIVE)
        recipient = await _recipient()
        nids = [await _job_to(recipient) for _ in range(6)]
        spy = _Held()
        spy.held.add(nids[0])
        real_factory = processor.get_session_factory
        armed: list[processor.LaneJobs] = []
        ended = asyncio.Event()

        def gapped_factory() -> Any:
            factory = real_factory()

            @asynccontextmanager
            async def session_scope() -> AsyncIterator[Any]:
                async with factory() as session:
                    execute = session.execute

                    async def execute_then_end(
                        statement: Any, *a: Any, **k: Any,
                    ) -> Any:
                        result = await execute(statement, *a, **k)
                        if armed and "ORDER BY" in str(statement):
                            # The rows are read; the held call ends now.
                            jobs = armed.pop()
                            spy.release.set()
                            async with asyncio.timeout(_BOUND):
                                while nids[0] in jobs.in_work:
                                    await ended.wait()
                                    ended.clear()
                        return result

                    session.execute = execute_then_end
                    yield session

            return session_scope

        async with _channel(spy), asyncio.TaskGroup() as group:
            jobs = processor.LaneJobs(Lane.INTERACTIVE, group)
            assert await process_lane(Lane.INTERACTIVE, jobs=jobs) == 3
            await asyncio.wait_for(spy.started.wait(), _BOUND)
            await _until(nids[1:3])
            assert jobs.in_work == {nids[0]}
            release = jobs.places.release

            def released() -> None:
                release()
                ended.set()

            monkeypatch.setattr(jobs.places, "release", released)
            monkeypatch.setattr(processor, "get_session_factory", gapped_factory)
            armed.append(jobs)
            assert await process_lane(Lane.INTERACTIVE, jobs=jobs) == 3
            # The pair: the gap was really opened -- the held job ended
            # between the selection and the drop.
            assert armed == []
            assert (await _state(nids[0]))[0] == NotificationStatus.SENT
        await _until(nids)
        assert spy.calls == len(nids)

    async def test_with_every_place_taken_a_pass_does_not_select(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Interactive, two places, batch 2: both held in their calls. The
        next pass waits for a place BEFORE its selection -- its session is
        one of the connections a place stands for -- and selects only
        when one comes free. M8."""
        monkeypatch.setattr(settings, "notification_lane_interactive_batch_size", 2)
        _declare("unit_event_in_app", Lane.INTERACTIVE)
        recipient = await _recipient()
        first, second, third = [await _job_to(recipient) for _ in range(3)]
        spy = _Held()
        spy.held.update({first, second})
        async with _channel(spy), asyncio.TaskGroup() as group:
            jobs = processor.LaneJobs(Lane.INTERACTIVE, group)
            assert await process_lane(Lane.INTERACTIVE, jobs=jobs) == 2
            await asyncio.wait_for(spy.started.wait(), _BOUND)
            with h12_capture() as seen:
                waiting = asyncio.ensure_future(
                    process_lane(Lane.INTERACTIVE, jobs=jobs),
                )
                await asyncio.sleep(0.5)
                selections = [s for s, _ in seen if "notifications.lane =" in s]
                assert selections == []
                assert not waiting.done()
                spy.held.clear()
                spy.release.set()
                assert await asyncio.wait_for(waiting, _BOUND) == 1
                # The pair: the pass did select, once a place was free.
                assert len([s for s, _ in seen if "notifications.lane =" in s]) == 1
        await _until([first, second, third])


# -----------------------------------------------------------------------------
# 1 (5). Shutdown: the loop's attempts go with it
# -----------------------------------------------------------------------------


class TestNoAttemptOutlivesTheLoop:
    async def test_no_attempt_task_is_left(self) -> None:
        """The pair to TestCancellingTheLoop: after the cancel, no attempt
        task is alive in the event loop. M9."""
        _declare("unit_event_in_app", Lane.INTERACTIVE)
        nid = await _job_to(await _recipient())
        spy = _Held()
        spy.held.add(nid)
        wake = asyncio.Event()
        async with _channel(spy):
            loop = asyncio.ensure_future(worker._lane_loop(Lane.INTERACTIVE, wake))
            await asyncio.wait_for(spy.started.wait(), _BOUND)
            assert len(_attempt_tasks()) == 1
            loop.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await loop
            assert _attempt_tasks() == []


# -----------------------------------------------------------------------------
# 3. Competitors: two attempts of one lane at once, on real sessions
# -----------------------------------------------------------------------------


class TestTwoAttemptsOfOneLane:
    """done-when 3: two jobs of one lane, to one person, both in their
    channel call at once -- each holds its own row (seen from a third
    session); both reach SENT, no 40P01. Against them: the expiry and
    forgetting take the rows FOR UPDATE, in ascending id
    (app/engine/service.py THE ORDER OF ROW LOCKS), and WAIT (seen in
    pg_stat_activity) for both attempts -- as for one. The invariant is
    held, not a winner. M1 (one attempt at a time: the second row is
    not held)."""

    async def _both_in_their_call(
        self, spy: _Held, *, expiry_at: datetime | None = None,
        resolved: bool = False,
    ) -> tuple[UUID, list[UUID], asyncio.Future[int]]:
        """Two jobs to one person, both in their call (the spy holds
        them); returns the person, the jobs and the pass in work.
        `resolved`: their deliveries committed before the attempts --
        the state forgetting finds a person's jobs by."""
        _declare("unit_event_in_app", Lane.INTERACTIVE)
        recipient = await _recipient()
        nids = []
        for _ in range(2):
            async with get_session_factory()() as session:
                job = await create_notification(
                    session, **intake_fields(), type="unit_event_in_app",
                    title="T", body="B", target_type=TargetType.USER,
                    target_value=str(recipient), expiry_at=expiry_at,
                )
                if resolved:
                    await resolve_notification(session, job)
                await session.commit()
                nids.append(job.id)
        spy.held.update(nids)
        attempts = asyncio.ensure_future(process_lane(Lane.INTERACTIVE))
        spy.enough_at = 2
        await asyncio.wait_for(spy.enough.wait(), _BOUND)
        for nid in nids:
            assert await _row_is_held(nid)
        return recipient, nids, attempts

    async def test_both_reach_their_outcome(self) -> None:
        spy = _Held()
        async with _channel(spy):
            _, nids, attempts = await self._both_in_their_call(spy)
            spy.release.set()
            assert await asyncio.wait_for(attempts, _BOUND) == 2
        await _until(nids)
        for nid in nids:
            async with get_session_factory()() as session:
                job = await session.get(Notification, nid)
                assert job is not None
                assert job.pipeline_attempts == 0

    async def test_the_expiry_waits_for_both(self) -> None:
        soon = datetime.now(UTC) + timedelta(seconds=1)
        spy = _Held()
        async with _channel(spy):
            _, nids, attempts = await self._both_in_their_call(
                spy, expiry_at=soon,
            )
            await asyncio.sleep(
                max(0.0, (soon - datetime.now(UTC)).total_seconds()) + 0.1,
            )
            expiry = asyncio.ensure_future(
                processor.expire_overdue_notifications([Lane.INTERACTIVE]),
            )
            await _until_someone_waits()
            assert not expiry.done()
            spy.release.set()
            assert await asyncio.wait_for(attempts, _BOUND) == 2
            # The attempts went first: their letters are out, nothing is
            # left for the expiry to close.
            assert await asyncio.wait_for(expiry, _BOUND) == 0
        await _until(nids)

    async def test_forgetting_waits_for_both(self) -> None:
        spy = _Held()
        async with _channel(spy):
            recipient, nids, attempts = await self._both_in_their_call(
                spy, resolved=True,
            )

            async def forget() -> int:
                async with get_session_factory()() as session:
                    closed = await withdraw_recipient(session, recipient)
                    await session.commit()
                    return closed

            forgetting = asyncio.ensure_future(forget())
            await _until_someone_waits()
            assert not forgetting.done()
            spy.release.set()
            assert await asyncio.wait_for(attempts, _BOUND) == 2
            # The attempts went first; nothing of theirs was left waiting.
            assert await asyncio.wait_for(forgetting, _BOUND) == 0
        await _until(nids)


# -----------------------------------------------------------------------------
# 2. The numbers
# -----------------------------------------------------------------------------


class TestThePlaces:
    def test_every_lane_has_places_within_its_slots(self) -> None:
        """A place beyond the lane's slots is a job holding a connection
        while it waits for a slot (app/engine/service.py LANE_JOBS). M7."""
        assert set(LANE_JOBS) == set(Lane) == set(LANE_CONCURRENCY)
        for lane in Lane:
            assert 1 <= LANE_JOBS[lane] <= LANE_CONCURRENCY[lane], lane

    def test_the_agreed_numbers(self) -> None:
        assert {lane.value: n for lane, n in LANE_JOBS.items()} == {
            "interactive": 2, "critical": 2, "normal": 4, "bulk": 1,
        }


# -----------------------------------------------------------------------------
# 4. What the texts say, against the code
# -----------------------------------------------------------------------------


_REPO = Path(__file__).resolve().parents[1]
_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


def _flat(text_: str) -> str:
    return " ".join(text_.split())


def _lane_section() -> str:
    start = _DOC.index("### `lane` -- which lane a type's jobs travel in")
    return _DOC[start:_DOC.index("\n### ", start + 1)]


def _section_11() -> str:
    start = _DOC.index("\n## 11. ")
    return _DOC[start:_DOC.index("\n## ", start + 1)]


class TestTheContract:
    """deploy/INTEGRATION.md tells the product the numbers and where an
    llm type belongs; held against app/engine/service.py both ways."""

    def test_the_lane_table_is_the_code(self) -> None:
        rows = {}
        for line in _lane_section().splitlines():
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) == 5 and cells[3].isdigit() and cells[4].isdigit():
                rows[cells[0].strip("`")] = (int(cells[3]), int(cells[4]))
        assert rows == {
            lane.value: (LANE_JOBS[lane], LANE_CONCURRENCY[lane])
            for lane in Lane
        }
        assert (
            "| value | for | the type's jobs | jobs in work at once "
            "| channel calls at once |"
            in _lane_section()
        )

    def test_a_lane_of_one_place_is_said_to_go_one_after_another(self) -> None:
        """Both ways: every lane with one place, and only those, is named
        in section 11 as taking its llm jobs strictly one after another,
        and in the lane section as one place."""
        single = {lane.value for lane in Lane if LANE_JOBS[lane] == 1}
        assert single == {"bulk"}
        eleven, lanes = _flat(_section_11()), _flat(_lane_section())
        for lane in Lane:
            said = (
                f"In `{lane.value}`, with one place, llm jobs go strictly "
                "one after another" in eleven
            )
            assert said is (lane.value in single), lane
            assert (
                f"`{lane.value}` has one place: its jobs go strictly one "
                "after another" in lanes
            ) is (lane.value in single), lane
        assert (
            "a lane that, on this box, carries no types for people and has "
            "more than one job in work at once" in eleven
        )
        assert "`lane_mixes_llm`" in eleven and "(`jobs`)" in eleven
        # The removed advice is gone with its reason.
        assert "A lane attempts its jobs one after another" not in _DOC

    def test_the_marker_and_the_warning_say_the_places(self) -> None:
        from app.engine import service
        from app.profile import loader

        doc = _flat(service._deliver_single.__doc__ or "")
        assert "KNOWN CEILING (acknowledged by design -- P5-1, P5-L" in doc
        assert "holds one of its lane's places (LANE_JOBS)" in doc
        assert "`lane_mixes_llm`" in doc and "oldest_ready_seconds" in doc
        source = Path(worker.__file__).read_text(encoding="utf-8")
        assert '"lane_mixes_llm",' in source
        assert "a lane attempts one job at a time" not in source
        # The loader's pointer names the function the marker is on.
        pointer = _flat(loader.lanes_mixing_llm.__doc__ or "")
        assert "app/engine/service.py _deliver_single" in pointer
        assert hasattr(service, "_deliver_single")
