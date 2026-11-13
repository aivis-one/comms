# =============================================================================
# WK-3 -- a stop of the worker: the channel's "accepted" is not lost, and
# _main closes what it opened however the loop ends
# =============================================================================
#
# What is held here:
#   1 a stop that comes -- by the signal's own path: the handler _main
#     installs, task.cancel() -- after the channel took the letter and
#     before its journal row committed: the row is written within its
#     bound, then the attempt rolls back, then the engine is disposed;
#     the next attempt closes the delivery SENT without calling the
#     channel (one call). Before WK-3 the cancellation cut the write and
#     the next attempt sent the letter again (reproduced before the
#     fix: two calls). The pair: a stop before the channel is called --
#     the next attempt sends once. A second stop inside the write does
#     not reach the attempt (the task groups cancel once); a write past
#     its bound is cut (KNOWN CEILING P2-1);
#   2 _main closes the formatters and the engine when the loop is
#     cancelled and when it raises; the loop's error reaches the
#     process; the engine is disposed even when closing the formatters
#     fails.
#
# THE ENTRY POINT'S GLOBAL EFFECTS are replaced (COMMS-0 §9): the signal
# handlers (captured, called as the loop would call them), the
# formatters' close and the engine's dispose (recorded), the wake
# listener and the maintenance pass (not under test: one lane, nothing
# else touching the job).
#
# MUTATIONS (each turns one red):
#   W1 the write awaited without its protection -> TestTheAcceptedStays
#   W2 no bound on the write at a stop -> test_a_write_past_its_bound
#   M2a _main without its finally -> TestMainCloses (the loop's error)
#   M2b the loop's error swallowed -> the same test (pytest.raises)
#   M2c dispose_engine not in the inner finally ->
#       test_the_engine_goes_when_the_formatters_fail
# =============================================================================

import asyncio
import signal
import time
from collections.abc import Callable
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select
from structlog.testing import capture_logs

from app import worker
from app.core.config import settings
from app.core.database import get_session_factory
from app.engine import journal, processor, service
from app.engine.constants import ChannelAnswer, NotificationStatus
from app.engine.models import NotificationDelivery, NotificationTransition
from tests.test_worker_keeps_its_work import _attempt, _job, _row

_GUARD = 20.0


class _Entry:
    """_main's global effects, replaced; what happened, in order."""

    def __init__(self) -> None:
        self.events: list[str] = []
        self.handlers: dict[int, Callable[[], None]] = {}

    def stop(self) -> None:
        """The signal, by its own path: the handler _main installed."""
        self.events.append("stop")
        self.handlers[signal.SIGTERM]()


@pytest.fixture
async def entry(monkeypatch: pytest.MonkeyPatch) -> _Entry:
    found = _Entry()
    loop = asyncio.get_running_loop()

    def capture(sig: int, callback: Callable[..., None], *args: Any) -> None:
        found.handlers[sig] = lambda: callback(*args)

    async def close() -> None:
        found.events.append("close_formatters")

    async def dispose() -> None:
        found.events.append("dispose_engine")

    async def forever(*_args: Any, **_kwargs: Any) -> None:
        await asyncio.Event().wait()

    async def nothing() -> int:
        return 0

    monkeypatch.setattr(loop, "add_signal_handler", capture)
    monkeypatch.setattr(worker, "close_formatters", close)
    monkeypatch.setattr(worker, "dispose_engine", dispose)
    monkeypatch.setattr(worker, "listen_for_wakes", forever)
    monkeypatch.setattr(worker, "run_maintenance_pass", nothing)
    # The pulse file (W3-1) is a global effect of _main too: a file in the
    # host's /tmp. Replaced like the others; the pulse has its own tests
    # (tests/test_liveness.py).
    monkeypatch.setattr(worker.liveness, "keep_alive", forever)
    return found


class _Channel:
    """The in_app channel's double: takes every letter, counts them."""

    def __init__(self) -> None:
        self.accepted = 0

    async def deliver(self, *_args: Any, **_kwargs: Any) -> bool:
        self.accepted += 1
        return True


def _channel(monkeypatch: pytest.MonkeyPatch) -> _Channel:
    channel = _Channel()
    monkeypatch.setattr(service.get_formatter("in_app"), "deliver", channel.deliver)
    return channel


def _write_hook(
    monkeypatch: pytest.MonkeyPatch, entry: _Entry, *,
    hold: float = 0.3, stops: int = 1,
) -> None:
    """INSIDE the journal's write of the channel's answer, before its
    COMMIT: the stop comes (`stops` times), the write is held `hold`
    seconds -- the window between the channel's answer and the row."""
    real_factory = journal.get_session_factory
    fired = False

    def hooked() -> Any:
        factory = real_factory()

        def make() -> Any:
            session = factory()
            real_commit = session.commit

            async def commit() -> None:
                nonlocal fired
                if not fired:
                    fired = True
                    for _ in range(stops):
                        entry.stop()
                        await asyncio.sleep(hold / stops)
                await real_commit()
                entry.events.append("answer_committed")

            session.commit = commit  # type: ignore[method-assign]
            return session

        return make

    monkeypatch.setattr(journal, "get_session_factory", hooked)


def _closes_of_the_attempt(monkeypatch: pytest.MonkeyPatch, entry: _Entry) -> None:
    """Every close of a processor session after the stop: the attempt's
    own session ending -- its rollback -- is the first of them."""
    real_factory = processor.get_session_factory

    def hooked() -> Any:
        factory = real_factory()

        def make() -> Any:
            session = factory()
            real_close = session.close

            async def close() -> None:
                await real_close()
                if "stop" in entry.events:
                    entry.events.append("attempt_closed")

            session.close = close  # type: ignore[method-assign]
            return session

        return make

    monkeypatch.setattr(processor, "get_session_factory", hooked)


async def _one_lane(monkeypatch: pytest.MonkeyPatch, job_id: UUID) -> None:
    lane = (await _row(job_id)).lane
    monkeypatch.setattr(settings, "comms_worker_lanes", lane)


async def _accepted_rows(job_id: UUID) -> int:
    async with get_session_factory()() as s:
        rows = await s.execute(
            select(NotificationTransition.id).where(
                NotificationTransition.notification_id == job_id,
                NotificationTransition.outcome == ChannelAnswer.ACCEPTED,
            )
        )
        return len(rows.all())


async def _delivery_status(job_id: UUID) -> str:
    async with get_session_factory()() as s:
        status = await s.scalar(
            select(NotificationDelivery.status)
            .where(NotificationDelivery.notification_id == job_id)
        )
        return str(status)


async def _run_main() -> float:
    started = time.monotonic()
    await asyncio.wait_for(worker._main(), _GUARD)
    return time.monotonic() - started


class TestTheAcceptedStays:
    """done-when 1."""

    async def test_a_stop_after_the_channel_answered(
        self, monkeypatch: pytest.MonkeyPatch, entry: _Entry,
    ) -> None:
        job_id = await _job()
        await _one_lane(monkeypatch, job_id)
        channel = _channel(monkeypatch)
        _write_hook(monkeypatch, entry)
        _closes_of_the_attempt(monkeypatch, entry)
        await _run_main()
        assert channel.accepted == 1
        assert (await _row(job_id)).status == NotificationStatus.PENDING  # rolled back
        await _attempt(job_id)  # the next attempt
        assert channel.accepted == 1  # not sent a second time
        assert await _accepted_rows(job_id) == 1
        # the row first, then the attempt's rollback, then the engine
        order = [e for e in entry.events
                 if e in ("answer_committed", "attempt_closed", "dispose_engine")]
        assert order[:3] == ["answer_committed", "attempt_closed", "dispose_engine"]
        assert await _delivery_status(job_id) == "sent"
        assert (await _row(job_id)).status == NotificationStatus.SENT

    async def test_the_pair_a_stop_before_the_channel_is_called(
        self, monkeypatch: pytest.MonkeyPatch, entry: _Entry,
    ) -> None:
        job_id = await _job()
        await _one_lane(monkeypatch, job_id)
        channel = _channel(monkeypatch)
        real = service._deliver_single

        async def stopped_first(*args: Any, **kwargs: Any) -> Any:
            entry.stop()
            await asyncio.sleep(5)  # the cancellation lands here
            return await real(*args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(service, "_deliver_single", stopped_first)
            await _run_main()
        assert channel.accepted == 0
        assert await _accepted_rows(job_id) == 0
        await _attempt(job_id)
        assert channel.accepted == 1
        assert (await _row(job_id)).status == NotificationStatus.SENT

    async def test_a_second_stop_inside_the_write(
        self, monkeypatch: pytest.MonkeyPatch, entry: _Entry,
    ) -> None:
        """An operator's second Ctrl-C, or a second SIGTERM, inside the
        write: it still ends. Held by the path, not by a loop of
        journal's: the handler cancels the loop's task again, and a
        TaskGroup already aborting does not cancel its children a second
        time -- the attempt is cancelled once (a gate premise of WK-3
        refuted: a wait through repeated cancellations was not needed)."""
        job_id = await _job()
        await _one_lane(monkeypatch, job_id)
        channel = _channel(monkeypatch)
        _write_hook(monkeypatch, entry, stops=2)
        await _run_main()
        assert await _accepted_rows(job_id) == 1
        await _attempt(job_id)
        assert channel.accepted == 1

    async def test_a_write_past_its_bound(
        self, monkeypatch: pytest.MonkeyPatch, entry: _Entry,
    ) -> None:
        """The write outlasts _RECORD_ON_STOP_SECONDS: cut, said, no row
        -- the next attempt sends again (KNOWN CEILING P2-1, the same
        at-least-once); the stop is not held past the bound."""
        monkeypatch.setattr(journal, "_RECORD_ON_STOP_SECONDS", 0.3)
        job_id = await _job()
        await _one_lane(monkeypatch, job_id)
        channel = _channel(monkeypatch)
        _write_hook(monkeypatch, entry, hold=5.0)
        with capture_logs() as logs:
            took = await _run_main()
        assert took < 0.3 + 3.0, took
        assert await _accepted_rows(job_id) == 0
        assert "journal_channel_answer_cut_by_stop" in [e["event"] for e in logs]
        await _attempt(job_id)
        assert channel.accepted == 2


class TestMainCloses:
    """done-when 2. Before WK-3 the closes stood after `await task`
    without a finally: a loop that raised left both undone (reproduced
    before the fix)."""

    async def test_the_loops_error(
        self, monkeypatch: pytest.MonkeyPatch, entry: _Entry,
    ) -> None:
        async def broken() -> None:
            raise RuntimeError("the loop broke")

        monkeypatch.setattr(worker, "run_worker_loop", broken)
        with capture_logs() as logs, pytest.raises(RuntimeError, match="broke"):
            await asyncio.wait_for(worker._main(), _GUARD)
        assert entry.events == ["close_formatters", "dispose_engine"]
        assert "worker_loop_failed" in [e["event"] for e in logs]

    async def test_the_pair_a_stop(
        self, monkeypatch: pytest.MonkeyPatch, entry: _Entry,
    ) -> None:
        async def forever() -> None:
            await asyncio.Event().wait()

        monkeypatch.setattr(worker, "run_worker_loop", forever)
        main = asyncio.ensure_future(worker._main())
        await asyncio.sleep(0.05)
        entry.stop()
        await asyncio.wait_for(main, _GUARD)
        assert entry.events == ["stop", "close_formatters", "dispose_engine"]

    async def test_the_engine_goes_when_the_formatters_fail(
        self, monkeypatch: pytest.MonkeyPatch, entry: _Entry,
    ) -> None:
        async def broken() -> None:
            raise RuntimeError("the loop broke")

        async def close() -> None:
            entry.events.append("close_formatters")
            raise ValueError("the bot session would not close")

        monkeypatch.setattr(worker, "run_worker_loop", broken)
        monkeypatch.setattr(worker, "close_formatters", close)
        with pytest.raises(ValueError) as raised:
            await asyncio.wait_for(worker._main(), _GUARD)
        assert entry.events == ["close_formatters", "dispose_engine"]
        # the loop's error is in the chain, not lost
        assert isinstance(raised.value.__context__, RuntimeError)
