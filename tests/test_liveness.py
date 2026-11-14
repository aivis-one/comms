# =============================================================================
# W3-1 item 4 -- a loop that stopped turning is not alive (app/core/liveness.py)
# =============================================================================
#
# done-when: a loop that stopped turning is red by the liveness check, a
# working one green, a long channel call within its timeout green; the
# compose healthcheck calls this check (tests/test_report.py
# TestTheContract holds the compose file and the contract alike); the
# hook in the lane's pass changes no attempt's outcome.
#
# MUTATIONS these tests were written against (each turns one red):
#   M10 the ticker touches the pulse whatever the promises
#                                   -> TestThePulse.test_a_stalled_loop_ages_the_pulse
#   M11 the healthcheck back to /proc/1/cmdline
#                                   -> tests/test_report.py TestTheContract
#   M12 no step when a lane's pass takes a place
#                                   -> TestTheLaneLoop.test_a_long_call_...
# =============================================================================

import asyncio
import os
import time
from pathlib import Path
from typing import Any

import pytest
from fakeredis import aioredis as fakeaioredis
from structlog.testing import capture_logs

from app import worker
from app.core import liveness
from app.core.channels import LLM_BODY_DOWNLOAD_TIMEOUT
from app.core.config import settings
from app.core.lanes import Lane
from app.engine import processor, service
from app.engine.constants import NotificationStatus
from app.transport import consumer as consumer_module
from app.transport import push_relay
from app.transport.connection import redis_socket_timeout
from tests.test_worker_keeps_its_work import _job, _row


@pytest.fixture(autouse=True)
def _no_watches() -> None:
    """Every test starts with no loop watched in this process."""
    liveness._WATCHES.clear()


async def _sample_late(seconds: float, every: float = 0.02) -> list[str]:
    """Every loop seen late while `seconds` pass."""
    seen: set[str] = set()
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        seen.update(liveness.late(time.monotonic()))
        await asyncio.sleep(every)
    return sorted(seen)


class TestThePromise:
    def test_outside_a_loop_a_step_and_a_rest_do_nothing(self) -> None:
        liveness.step()
        liveness.rest(10)
        assert liveness._WATCHES == {}

    async def test_a_step_rearms_the_loop_of_the_current_task(self) -> None:
        with liveness.watched("one", 0.1):
            await asyncio.sleep(0.15)
            assert liveness.late(time.monotonic()) == ["one"]
            liveness.step()
            assert liveness.late(time.monotonic()) == []
        assert liveness._WATCHES == {}, "the loop leaves with its body"

    async def test_a_rest_promises_the_sleep_and_the_bound(self) -> None:
        with liveness.watched("one", 0.1):
            liveness.rest(0.3)
            await asyncio.sleep(0.2)
            assert liveness.late(time.monotonic()) == []
            await asyncio.sleep(0.25)
            assert liveness.late(time.monotonic()) == ["one"]

    async def test_a_task_started_in_a_loop_steps_for_it(self) -> None:
        """The ContextVar: a step made deep in a task the loop started
        (a pass, an attempt) is the loop's."""
        with liveness.watched("one", 0.1):
            await asyncio.sleep(0.15)
            await asyncio.create_task(self._step())
            assert liveness.late(time.monotonic()) == []

    @staticmethod
    async def _step() -> None:
        liveness.step()


class TestTheCheck:
    def test_fresh_stale_and_absent(self, tmp_path: Path) -> None:
        path = tmp_path / "pulse"
        assert not liveness.is_alive(path)
        path.touch()
        assert liveness.is_alive(path)
        old = time.time() - liveness.STALE_SECONDS - 1
        os.utime(path, (old, old))
        assert not liveness.is_alive(path)

    def test_the_command(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(liveness, "pulse_path", lambda role: tmp_path / role)
        assert liveness.main(["worker"]) == 1
        (tmp_path / "worker").touch()
        assert liveness.main(["worker"]) == 0
        assert liveness.main(["consumer"]) == 1
        assert liveness.main(["api"]) == 2
        assert liveness.main([]) == 2

    def test_the_pulse_is_the_containers_own(self) -> None:
        assert liveness.pulse_path("worker") == Path("/tmp/comms-worker.alive")
        assert liveness.ROLES == ("worker", "consumer")


class TestThePulse:
    async def test_an_old_pulse_goes_and_none_comes_without_a_loop(
        self, tmp_path: Path,
    ) -> None:
        path = tmp_path / "pulse"
        path.touch()
        ticker = asyncio.ensure_future(liveness.keep_alive(path, tick=0.02))
        try:
            await asyncio.sleep(0.1)
            assert not path.exists(), "no loop watched yet: no pulse"
            with liveness.watched("one", 5):
                await asyncio.sleep(0.1)
                assert liveness.is_alive(path, stale=0.1), "the pair: it pulses"
        finally:
            ticker.cancel()

    async def test_a_stalled_loop_ages_the_pulse(self, tmp_path: Path) -> None:
        path = tmp_path / "pulse"
        ticker = asyncio.ensure_future(liveness.keep_alive(path, tick=0.02))
        try:
            with capture_logs() as logs, liveness.watched("stuck", 0.2):
                await asyncio.sleep(0.1)
                assert liveness.is_alive(path, stale=0.1)
                await asyncio.sleep(0.5)
                assert not liveness.is_alive(path, stale=0.1)
            lines = [e for e in logs if e["event"] == "liveness_stalled"]
            assert [line["loops"] for line in lines] == [["stuck"]], "on the turn"
        finally:
            ticker.cancel()


class TestTheBounds:
    def test_a_lane_without_llm(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(service, "longest_llm_list", lambda lane: 0)
        monkeypatch.setattr(
            settings, "notification_lane_bulk_deliver_timeout_seconds", 30,
        )
        assert worker.lane_step_bound(Lane.BULK) == (
            service.LANE_JOBS[Lane.BULK] * 30 + liveness.STEP_MARGIN_SECONDS
        )

    def test_a_lane_with_llm_takes_the_models_call(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(service, "longest_llm_list", lambda lane: 2)
        monkeypatch.setattr(settings, "llm_deliver_timeout_seconds", 120)
        call = 120 * 2 + LLM_BODY_DOWNLOAD_TIMEOUT
        assert worker.lane_step_bound(Lane.NORMAL) == (
            service.LANE_JOBS[Lane.NORMAL] * call + liveness.STEP_MARGIN_SECONDS
        )

    def test_the_maintenance_loop_takes_the_longest_lane(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "comms_worker_lanes", None)
        name, bound = worker._step_bound({"loop": "maintenance"})
        assert name == "maintenance"
        assert bound == max(worker.lane_step_bound(lane) for lane in Lane)

    def test_the_consumer_and_the_relay(self) -> None:
        assert consumer_module.consumer_step_bound() == (
            consumer_module._WAIT_CEILING + redis_socket_timeout()
            + liveness.STEP_MARGIN_SECONDS
        )
        assert push_relay.relay_step_bound() == (
            push_relay.PUSH_RELAY_INTERVAL_SECONDS + redis_socket_timeout()
            + liveness.STEP_MARGIN_SECONDS
        )


class _SlowChannel:
    """The in_app channel's double: each letter takes `seconds`."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.accepted = 0

    async def deliver(self, *_args: Any, **_kwargs: Any) -> bool:
        await asyncio.sleep(self.seconds)
        self.accepted += 1
        return True


async def _all_sent(jobs: list[Any]) -> bool:
    statuses = [(await _row(job_id)).status for job_id in jobs]
    return all(status == NotificationStatus.SENT for status in statuses)


class TestTheLaneLoop:
    async def test_a_stopped_pass_is_late_a_working_loop_is_not(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(worker, "lane_step_bound", lambda lane: 0.2)
        monkeypatch.setattr(settings, "notification_poll_interval_seconds", 0.02)
        monkeypatch.setattr(settings, "notification_max_backoff_seconds", 0.02)

        async def turns() -> int:
            return 0

        loop = asyncio.ensure_future(
            worker._poll_loop(turns, asyncio.Event(), lane="bulk"),
        )
        try:
            assert await _sample_late(0.6) == []
            assert "bulk" in liveness._WATCHES, "the pair: the loop is watched"
        finally:
            loop.cancel()

        async def stops() -> int:
            await asyncio.Event().wait()
            return 0

        loop = asyncio.ensure_future(
            worker._poll_loop(stops, asyncio.Event(), lane="bulk"),
        )
        try:
            assert await _sample_late(0.5) == ["bulk"]
        finally:
            loop.cancel()

    async def test_a_long_call_within_its_timeout_is_not_a_stuck_loop(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One place, four jobs, a call of 0.6 s against a timeout of 1 s:
        the pass waits for a place three times and lasts past the lane's
        bound (1 * 1 + 0.3) -- each wait within it, so never late."""
        first = await _job()
        lane = Lane((await _row(first)).lane)
        jobs = [first, *[await _job() for _ in range(3)]]
        monkeypatch.setitem(service.LANE_JOBS, lane, 1)
        monkeypatch.setattr(
            settings, f"notification_lane_{lane.value}_deliver_timeout_seconds", 1,
        )
        monkeypatch.setattr(service, "longest_llm_list", lambda name: 0)
        monkeypatch.setattr(liveness, "STEP_MARGIN_SECONDS", 0.3)
        assert worker.lane_step_bound(lane) == 1.3
        channel = _SlowChannel(0.6)
        monkeypatch.setattr(
            service.get_formatter("in_app"), "deliver", channel.deliver,
        )
        started = time.monotonic()
        loop = asyncio.ensure_future(worker._lane_loop(lane, asyncio.Event()))
        late: set[str] = set()
        try:
            while channel.accepted < 4 and time.monotonic() - started < 20:
                late.update(liveness.late(time.monotonic()))
                await asyncio.sleep(0.02)
            elapsed = time.monotonic() - started
            while time.monotonic() - started < 20 and not await _all_sent(jobs):
                late.update(liveness.late(time.monotonic()))
                await asyncio.sleep(0.05)
        finally:
            loop.cancel()
            await asyncio.gather(loop, return_exceptions=True)
        assert channel.accepted == 4
        assert elapsed > 1.3 * 1.5, "the pass outlasted its bound"
        assert late == set()
        assert await _all_sent(jobs)


class TestTheHookChangesNoOutcome:
    async def test_an_attempt_with_and_without_a_watched_loop(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        channel = _SlowChannel(0)
        monkeypatch.setattr(
            service.get_formatter("in_app"), "deliver", channel.deliver,
        )
        bare = await _job()
        lane = Lane((await _row(bare)).lane)
        assert await processor.process_lane(lane) == 1
        watched = await _job()
        with liveness.watched("lane", 100):
            before = liveness._WATCHES["lane"].deadline
            assert await processor.process_lane(lane) == 1
            assert liveness._WATCHES["lane"].deadline > before, "the hook ran"
        first, second = await _row(bare), await _row(watched)
        assert first.status == second.status == NotificationStatus.SENT
        assert channel.accepted == 2


class TestTheConsumerSide:
    async def test_the_relay_turns_while_its_ticks_fail(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(push_relay, "PUSH_RELAY_INTERVAL_SECONDS", 0.02)

        async def failing(_redis: Any) -> int:
            raise ConnectionError("redis is away")

        monkeypatch.setattr(push_relay, "relay_once", failing)
        with liveness.watched("push_relay", 0.2):
            task = asyncio.ensure_future(push_relay.run_push_relay(None))  # type: ignore[arg-type]
            try:
                assert await _sample_late(0.5) == []
            finally:
                task.cancel()

    async def test_the_consumer_turns_on_an_empty_stream(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "comms_events_stream", "comms:test:liveness")
        monkeypatch.setattr(settings, "consumer_block_ms", 20)
        stream = consumer_module.StreamConsumer(fakeaioredis.FakeRedis())
        with liveness.watched("consumer", 0.2):
            task = asyncio.ensure_future(stream.run())
            try:
                assert await _sample_late(0.5) == []
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
