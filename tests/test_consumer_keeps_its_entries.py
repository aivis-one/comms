# =============================================================================
# FIX review 3.13.0 -- the consumer keeps its entries and itself
# =============================================================================
#
#   1 a dead letter that cannot be built or written never takes the
#     process down: the entry stays PENDING (_dead_letter_or_hold), the
#     next entry is processed; the review's own entry (a key with a NUL,
#     then 1e999) is dead-lettered without its data;
#   2 an entry held pending is met ONCE per start (_drain_pending's
#     cursor), goes to the dead letter at a start where it can, and stays
#     at one where it cannot -- new events flow either way;
#   3 a transient failure of the database: waited for, uncounted, while
#     the database does not answer -- the event applied once it does;
#     the bounded budget, then the dead letter, while it answers;
#   4 update and install stop comms before the datastores converge, and
#     bring it back as it was on a failure.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 event_data_or_none on the first defect only -> TestTheReviewsEntry
#   M2 _dead_letter_or_hold without its except -> TestHeld (process dies)
#   M3 the transient failure always waited for -> TestWaitingForTheDatabase
#      (b) and (c): the stream stands
#   M4 _drain_pending from "0" again -> TestTheCursor (a)
#   M5 convergence before the stop -> TestTheOrder
#   M6 no start of the old containers on a failure -> TestTheOrder
#   M7 the success's ack back inside the try -> TestHeld::test_an_ack
# =============================================================================

import json
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
from app.core.database import get_session_factory
from app.transport import consumer as consumer_module
from app.transport.consumer import StreamConsumer
from app.transport.events import event_data_or_none
from tests.helpers import (
    real_connection_refused,
    real_misuse_of_a_connection,
    real_serialization_failure,
)
from tests.test_deploy_hardening import _index, _run
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


async def _dlq(redis: fakeaioredis.FakeRedis) -> list[dict[str, str]]:
    entries = await redis.xrange(settings.dlq_stream)
    return [{k.decode(): v.decode() for k, v in e[1].items()} for e in entries]


async def _pending(redis: fakeaioredis.FakeRedis, stream: str) -> int:
    info = await redis.xpending(stream, settings.comms_consumer_group)
    return int(info["pending"])


async def _recipient(rid: Any) -> bool:
    async with get_session_factory()() as s:
        return await s.scalar(select(Recipient.id).where(Recipient.id == rid)) \
            is not None


_REVIEW = '{"v":1,"idempotency_key":"a\\u0000b","type":1e999}'


class TestTheReviewsEntry:
    def test_every_defect_is_seen(self) -> None:
        """The NUL of the key came first; the number past the float range
        behind it must still keep the data out of the dead letter."""
        assert event_data_or_none(_REVIEW.encode()) is None
        # the pair: a string defect alone is written (ensure_ascii)
        kept = event_data_or_none(b'{"v":1,"idempotency_key":"a\\u0000b"}')
        assert kept == {"v": 1, "idempotency_key": "a\x00b"}

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

        await _run_until(StreamConsumer(fake_redis), done)
        (dead,) = await _dlq(fake_redis)
        assert "data" in json.loads(dead["_dlq_dropped"])


def _failing_dead_letters(monkeypatch: pytest.MonkeyPatch, times: int) -> list[int]:
    """The dead letter's writing fails `times` times (a Redis XADD to the
    dead-letter stream refused), then works."""
    real = StreamConsumer._to_dlq
    calls = [0]

    async def to_dlq(self: StreamConsumer, *args: Any, **kwargs: Any) -> None:
        calls[0] += 1
        if calls[0] <= times:
            raise ConnectionError("the dead-letter stream is unreachable")
        await real(self, *args, **kwargs)

    monkeypatch.setattr(StreamConsumer, "_to_dlq", to_dlq)
    return calls


_BAD = {"event": "user_deleted", "data": '{"v": 1}'}  # malformed: a dead letter


class TestHeld:
    async def test_a_dead_letter_that_cannot_be_written(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _failing_dead_letters(monkeypatch, times=99)
        await fake_redis.xadd(a_stream, _BAD)
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def next_one() -> bool:
            return await _recipient(person)

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), next_one)
        assert await _pending(fake_redis, a_stream) == 1  # the held one
        assert await _dlq(fake_redis) == []
        assert "dead_letter_write_failed" in [e["event"] for e in logs]

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

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), both)
        assert await _pending(fake_redis, a_stream) == 1
        assert await _dlq(fake_redis) == []  # applied, not a failure
        assert "event_ack_failed" in [e["event"] for e in logs]


class TestTheCursor:
    async def _hold_one(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _failing_dead_letters(monkeypatch, times=1)
        await fake_redis.xadd(a_stream, _BAD)
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
        """(a) and (c): at the restart the dead letter fails again --
        the held entry is met ONCE, said again, still pending; a new
        event behind it is processed."""
        await self._hold_one(fake_redis, a_stream, monkeypatch)
        _failing_dead_letters(monkeypatch, times=99)
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
        assert "dead_letter_write_failed" in [e["event"] for e in logs]

    async def test_a_held_entry_goes_out_when_it_can(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """(b): at the next start the dead-letter stream answers -- the
        held entry is dead-lettered and acknowledged, the pending list
        empty."""
        await self._hold_one(fake_redis, a_stream, monkeypatch)

        async def emptied() -> bool:
            return await _pending(fake_redis, a_stream) == 0

        await _run_until(StreamConsumer(fake_redis), emptied)
        (dead,) = await _dlq(fake_redis)
        assert dead["event"] == "user_deleted"


def _probe_down_for(times: int) -> tuple[Callable[[], Awaitable[bool]], list[int]]:
    """A REAL probe: a SELECT 1 against a port nothing listens on, for the
    first `times` asks (the database down), then the real one."""
    calls = [0]

    async def probe() -> bool:
        calls[0] += 1
        if calls[0] <= times:
            await real_connection_refused()  # a real refusal, returned
            return False
        return await consumer_module._database_answers()

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
        assert await _dlq(fake_redis) == []
        loud = [e for e in logs if e["event"] == "event_waiting_for_database"]
        assert [e["waits"] for e in loud] == [1, 10]

    @pytest.mark.parametrize("make", [real_serialization_failure,
                                      real_misuse_of_a_connection],
                             ids=["40001", "misuse"])
    async def test_a_live_database_dead_letters_the_event_after_its_budget(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
        make: Callable[[], Awaitable[Exception]],
    ) -> None:
        """(b) and (c): the database answers, the event keeps failing --
        the budget, then the dead letter; the next event is processed by
        the same process."""
        failure = await make()

        async def always(*_args: Any, **_kwargs: Any) -> Any:
            raise failure

        real = consumer_module.handle_event
        first = [True]

        async def handle(*args: Any, **kwargs: Any) -> Any:
            if first[0]:
                return await always(*args, **kwargs)
            return await real(*args, **kwargs)

        monkeypatch.setattr(consumer_module, "handle_event", handle)
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(uuid4()))
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def dead_then_next() -> bool:
            if await fake_redis.xlen(settings.dlq_stream) == 1:
                first[0] = False
            return await _recipient(person)

        await _run_until(StreamConsumer(fake_redis), dead_then_next)
        (dead,) = await _dlq(fake_redis)
        assert dead["_dlq_error"].startswith("retries exhausted: ")


class TestTheOrder:
    def test_update_stops_comms_before_the_datastores(self, tmp_path: Any) -> None:
        done, calls = _run(tmp_path, "cmd_update")
        assert done.returncode == 0, done.stdout + done.stderr
        stop = _index(calls, "compose stop comms-consumer comms-worker comms-app")
        converge = _index(calls, "compose up -d comms-postgres comms-redis")
        dump = _index(calls, "compose exec -T comms-postgres pg_dump")
        app = _index(calls, "compose up -d --force-recreate --no-deps")
        assert stop < converge < dump < app

    @pytest.mark.parametrize("failure", [{"HEALTH": "starting",
                                          "COMMS_HEALTH_WAIT_SECONDS": "2"},
                                         {"DUMP_RC": "1"}],
                             ids=["converge", "dump"])
    def test_update_brings_the_old_ones_back(
        self, tmp_path: Any, failure: dict[str, str],
    ) -> None:
        done, calls = _run(tmp_path, "cmd_update", **failure)
        assert done.returncode == 1
        assert any(c == "compose start comms-app comms-worker comms-consumer"
                   for c in calls)
        assert not any("--force-recreate" in c for c in calls)

    def test_install_on_a_live_base_the_same(self, tmp_path: Any) -> None:
        done, calls = _run(tmp_path, "cmd_install", SCHEMA="t")
        assert done.returncode == 0, done.stdout + done.stderr
        stop = _index(calls, "compose stop comms-consumer comms-worker comms-app")
        converge = _index(calls, "compose up -d comms-postgres comms-redis")
        dump = _index(calls, "compose exec -T comms-postgres pg_dump")
        assert stop < converge < dump < calls.index("compose up -d")
        failed, calls = _run(tmp_path, "cmd_install", SCHEMA="t", DUMP_RC="1")
        assert failed.returncode == 1
        assert "compose start comms-app comms-worker comms-consumer" in calls
