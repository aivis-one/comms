# =============================================================================
# FIX review 3.13.0 -- the consumer keeps its entries and itself
# =============================================================================
#
#   1 a failure of comms on one entry never takes the process down: the
#     entry stays PENDING (held, W5-1), the next entry -- another
#     person's -- is processed; the review's own entry (a key with a NUL,
#     then 1e999) is dropped, its line without its data;
#   2 a held entry is met ONCE per start (_drain_pending's cursor), goes
#     out at a start where it can, and stays at one where it cannot --
#     new events of other people flow either way; and (WK-3) it is met
#     again while the process runs, once per pass over the pending list
#     (_PENDING_REDRAIN_SECONDS), not only at a restart;
#   3 a transient failure of the database: waited for, uncounted, while
#     the database does not answer -- the event applied once it does;
#     the bounded budget, then held, while it answers;
#   4 (gone in W1-2 with the old installer: the order of its update and
#     install; under the deploy machine nothing of comms runs while the
#     schema is migrated -- primer.yaml stop_for_migrate).
#
# Until W5-1 the entry held in 1 and 2 was one whose dead letter could
# not be built, and the event of 3 went to the dead letter after its
# budget; W5-1 removed the dead letters -- a failure of comms holds the
# entry itself, by its subject (tests/test_consumer_drops_and_holds.py).
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 (gone with the dead letter's reader of the data, W5-1)
#   M2 _process_guarded without its except -> TestHeld (process dies)
#   M3 the transient failure always waited for -> TestWaitingForTheDatabase
#      (b) and (c): the stream stands
#   M4 _drain_pending from "0" again -> TestTheCursor (a)
#   M5, M6 (gone with item 4)
#   M7 the success's ack back inside the try -> TestHeld::test_an_ack
#   M8 no pass over the pending list while running (WK-3) ->
#      TestNotUntilARestart (the entry waits for a restart)
#   M9 that pass at every read, the interval not kept ->
#      TestNotUntilARestart::test_an_entry_held_for_good_once_per_pass
# =============================================================================

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.core.config import settings
from app.core.database import database_answers, get_session_factory
from app.transport import consumer as consumer_module
from app.transport.consumer import StreamConsumer
from tests.helpers import (
    real_connection_refused,
    real_misuse_of_a_connection,
    real_serialization_failure,
)
from tests.test_transport_consumer import (
    _no_pending,
    _run_until,
    _user_upserted_data,
    _xadd,
)


@pytest.fixture
def fake_redis() -> fakeaioredis.FakeRedis:
    return fakeaioredis.FakeRedis()


@pytest.fixture
def a_stream(monkeypatch: pytest.MonkeyPatch) -> str:
    name = f"comms:test:{uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", name)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    monkeypatch.setattr(consumer_module, "_BACKOFF_DELAYS", (0.01, 0.01, 0.01))
    monkeypatch.setattr(consumer_module, "_WAIT_CEILING", 0.02)
    return name


async def _keys(redis: fakeaioredis.FakeRedis) -> set[str]:
    return {k.decode() for k in await redis.keys("*")}


async def _pending(redis: fakeaioredis.FakeRedis, stream: str) -> int:
    info = await redis.xpending(stream, settings.comms_consumer_group)
    return int(info["pending"])


async def _recipient(rid: Any) -> bool:
    async with get_session_factory()() as s:
        return await s.scalar(select(Recipient.id).where(Recipient.id == rid)) \
            is not None


_REVIEW = '{"v":1,"idempotency_key":"a\\u0000b","type":1e999}'


class TestTheReviewsEntry:
    async def test_on_the_real_loop(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        await fake_redis.xadd(a_stream, {"event": "notification_request",
                                         "data": _REVIEW})
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def done() -> bool:
            return await _recipient(person) and await _no_pending(fake_redis, a_stream)

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), done)
        # The NUL of the key came first; the number past the float range
        # behind it: dropped by the parse, nothing of its data in a line.
        (line,) = [e for e in logs if e["event"] == "event_dropped"]
        assert (line["kind"], line["stage"]) == ("notification_request", "parse")
        assert line["data_bytes"] == len(_REVIEW)
        assert "1e999" not in json.dumps(logs, default=str)
        assert await _keys(fake_redis) == {a_stream}


def _failing_handling(monkeypatch: pytest.MonkeyPatch, times: int) -> list[int]:
    """The handling of a user_deleted fails in comms `times` times (a
    defect of comms -- handle_event raises), then it works.

    Until W5-1 these tests failed the BUILD of the entry's dead letter
    (before review 3.14.0, N0, its WRITE): that was what left an entry
    pending. W5-1 removed the dead letters; a failure of comms on the
    entry is what holds it now."""
    real = consumer_module.handle_event
    calls = [0]

    async def handling(session: Any, event: Any) -> Any:
        if type(event).__name__ == "UserDeleted":
            calls[0] += 1
            if calls[0] <= times:
                raise TypeError("a defect of comms")
        return await real(session, event)

    monkeypatch.setattr(consumer_module, "handle_event", handling)
    return calls


def _bad() -> dict[str, str]:
    """A user_deleted of a person of its own: its hold holds no one
    else."""
    data = {"v": 1, "recipient_id": str(uuid4()), "version": 1}
    return {"event": "user_deleted", "data": json.dumps(data)}


class TestHeld:
    async def test_a_failure_of_comms(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _failing_handling(monkeypatch, times=99)
        await fake_redis.xadd(a_stream, _bad())
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def next_one() -> bool:
            return await _recipient(person)

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), next_one)
        assert await _pending(fake_redis, a_stream) == 1  # the held one
        assert await _keys(fake_redis) == {a_stream}
        assert "event_held" in [e["event"] for e in logs]

    async def test_an_ack_that_fails(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        real = StreamConsumer._ack
        calls = [0]

        async def ack(self: StreamConsumer, entry_id: Any) -> None:
            calls[0] += 1
            if calls[0] == 1:
                raise ConnectionError("XACK refused")
            await real(self, entry_id)

        monkeypatch.setattr(StreamConsumer, "_ack", ack)
        first, second = uuid4(), uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(first))
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(second))

        async def both() -> bool:
            return await _recipient(first) and await _recipient(second)

        async def both_acked() -> bool:
            return await both() and await _pending(fake_redis, a_stream) == 0

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), both_acked)
        # Until review 3.14.0 (2.3) the entry stayed pending here -- right
        # then: the ack was tried once, and the replay of it was the
        # duplicate. That replay re-applies a membership event over a newer
        # one; the ack is now retried in place (_ack_until_done), so
        # nothing is left pending. The property this test holds (M7) is the
        # same: a failed ack is no failure of the event -- neither held nor
        # dropped.
        assert calls[0] == 3  # the failed one, its retry, the second's
        said = [e["event"] for e in logs]
        assert "event_held" not in said and "event_dropped" not in said
        assert "event_ack_failed" in said


class TestTheCursor:
    async def _hold_one(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _failing_handling(monkeypatch, times=1)
        await fake_redis.xadd(a_stream, _bad())
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def next_one() -> bool:
            return await _recipient(person)

        await _run_until(StreamConsumer(fake_redis), next_one)
        assert await _pending(fake_redis, a_stream) == 1

    async def test_a_held_entry_is_met_once_per_start_and_new_ones_flow(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """(a) and (c): at the restart the handling fails again -- the
        held entry is met ONCE, said again, still pending; a new event of
        another person behind it is processed."""
        await self._hold_one(fake_redis, a_stream, monkeypatch)
        _failing_handling(monkeypatch, times=99)
        later = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(later))
        met: list[str] = []
        real = StreamConsumer._process_entry

        async def counted(self: StreamConsumer, entry_id: Any, fields: Any) -> None:
            if fields.get(b"event") == b"user_deleted":
                met.append(str(entry_id))
            await real(self, entry_id, fields)

        monkeypatch.setattr(StreamConsumer, "_process_entry", counted)

        async def later_done() -> bool:
            return await _recipient(later)

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), later_done)
        assert len(met) == 1
        assert await _pending(fake_redis, a_stream) == 1
        assert "event_held" in [e["event"] for e in logs]

    async def test_a_held_entry_goes_out_when_it_can(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """(b): at the next start the handling works -- the held entry is
        applied and acknowledged, the pending list empty."""
        await self._hold_one(fake_redis, a_stream, monkeypatch)

        async def emptied() -> bool:
            return await _pending(fake_redis, a_stream) == 0

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), emptied)
        (done,) = [e for e in logs if e["event"] == "event_processed"]
        assert done["event_type"] == "UserDeleted"


class TestNotUntilARestart:
    """WK-3. Before, the own pending list was read once, in run()'s
    start: an entry left pending waited for the next start of the
    process, however long that was (reproduced before the fix: the
    entry still pending past the interval). Now run() reads it again
    every _PENDING_REDRAIN_SECONDS (0.2 s here), its cursor rule kept."""

    _INTERVAL = 0.2
    _TOLERANCE = 1.0

    @pytest.fixture(autouse=True)
    def _short_interval(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(consumer_module, "_PENDING_REDRAIN_SECONDS", self._INTERVAL)

    async def test_an_entry_that_failed_once_goes_out_while_running(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Its handling raises once (left pending, event_left_pending),
        then works: in the same run, within the interval and a
        tolerance, it is processed and acknowledged; a new event between
        is read and processed as well."""
        failed, later = uuid4(), uuid4()
        real = StreamConsumer._process_entry
        raised: list[float] = []
        tore = asyncio.Event()

        async def once(self: StreamConsumer, entry_id: Any, fields: Any) -> None:
            if not raised:
                raised.append(time.monotonic())
                tore.set()
                raise RuntimeError("one entry's handling tore")
            await real(self, entry_id, fields)

        monkeypatch.setattr(StreamConsumer, "_process_entry", once)
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(failed))
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            await asyncio.wait_for(tore.wait(), 5)
            await _xadd(fake_redis, a_stream, "user_upserted",
                        _user_upserted_data(later))
            deadline = raised[0] + self._INTERVAL + self._TOLERANCE
            while not (await _recipient(failed) and await _recipient(later)
                       and await _pending(fake_redis, a_stream) == 0):
                assert time.monotonic() < deadline, "not without a restart"
                assert not task.done()
                await asyncio.sleep(0.02)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    async def test_an_entry_held_for_good_once_per_pass(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A handling that always fails in comms: the entry is met once
        per pass over the pending list -- more than once in a second of
        running, never round and round -- and new events of other people
        keep flowing."""
        _failing_handling(monkeypatch, times=10_000)
        await fake_redis.xadd(a_stream, _bad())
        met: list[float] = []
        real = StreamConsumer._process_entry

        async def counted(self: StreamConsumer, entry_id: Any, fields: Any) -> None:
            if fields.get(b"event") == b"user_deleted":
                met.append(time.monotonic())
            await real(self, entry_id, fields)

        monkeypatch.setattr(StreamConsumer, "_process_entry", counted)
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            await asyncio.sleep(1.0)
            later = uuid4()
            await _xadd(fake_redis, a_stream, "user_upserted",
                        _user_upserted_data(later))
            deadline = time.monotonic() + self._TOLERANCE
            while not await _recipient(later):
                assert time.monotonic() < deadline, "new events must flow"
                await asyncio.sleep(0.02)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        span = met[-1] - met[0]
        assert len(met) >= 3, met  # met again while running
        assert len(met) <= span / self._INTERVAL + 2, (len(met), span)
        assert await _pending(fake_redis, a_stream) == 1


def _probe_down_for(times: int) -> tuple[Callable[[], Awaitable[bool]], list[int]]:
    """A REAL probe: a SELECT 1 against a port nothing listens on, for the
    first `times` asks (the database down), then the real one -- the
    worker's too since WK-2 (app/core/database.py database_answers)."""
    calls = [0]

    async def probe() -> bool:
        calls[0] += 1
        if calls[0] <= times:
            await real_connection_refused()  # a real refusal, returned
            return False
        return await database_answers()

    return probe, calls


class TestWaitingForTheDatabase:
    async def test_a_down_database_is_waited_for(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """(a) The database does not answer for 12 waits -- longer than
        the old budget of six tries -- and the event, failing on a REAL
        transient error meanwhile, is applied once it does: no dead
        letter, said loudly on the 1st and the 10th wait."""
        failure = await real_serialization_failure()
        probe, _ = _probe_down_for(12)
        real = consumer_module.handle_event
        calls = [0]

        async def handle(*args: Any, **kwargs: Any) -> Any:
            calls[0] += 1
            if calls[0] <= 12:
                raise failure
            return await real(*args, **kwargs)

        monkeypatch.setattr(consumer_module, "handle_event", handle)
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def applied() -> bool:
            return await _recipient(person)

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis, database_probe=probe), applied)
        said = [e["event"] for e in logs]
        assert "event_held" not in said and "event_dropped" not in said
        loud = [e for e in logs if e["event"] == "event_waiting_for_database"]
        assert [e["waits"] for e in loud] == [1, 10]

    @pytest.mark.parametrize("make", [real_serialization_failure,
                                      real_misuse_of_a_connection],
                             ids=["40001", "misuse"])
    async def test_a_live_database_holds_the_event_after_its_budget(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
        make: Callable[[], Awaitable[Exception]],
    ) -> None:
        """(b) and (c): the database answers, the event keeps failing --
        the budget, then held (until W5-1: the dead letter); the next
        event, another person's, is processed by the same process."""
        failure = await make()

        async def always(*_args: Any, **_kwargs: Any) -> Any:
            raise failure

        real = consumer_module.handle_event
        failing = uuid4()

        async def handle(session: Any, event: Any) -> Any:
            if getattr(event, "recipient_id", None) == failing:
                return await always(session, event)
            return await real(session, event)

        monkeypatch.setattr(consumer_module, "handle_event", handle)
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(failing))
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        with capture_logs() as logs:

            async def held_then_next() -> bool:
                return await _recipient(person) and any(
                    e["event"] == "event_held" for e in logs)

            await _run_until(StreamConsumer(fake_redis), held_then_next)
        (held,) = [e for e in logs if e["event"] == "event_held"]
        assert held["attempts"] == len(consumer_module._BACKOFF_DELAYS) + 1
        assert await _pending(fake_redis, a_stream) == 1

