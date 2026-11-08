# =============================================================================
# A dead letter is written to the end; a refusal of redis ends the process
# =============================================================================
#
#   1 the XADD of a dead letter is retried IN PLACE until redis takes it
#     (_until_done): one letter, the entry acknowledged, the next entry
#     not taken before; the review's case N0 -- a group_changed true that
#     waited for its person, its dead letter failing, then the person and
#     a false -- leaves no membership (TestTheDeadLetterIsWritten);
#   2 a refusal of redis that no retry mends (a ResponseError but OOM, an
#     authentication or authorization error) on the XACK or on the
#     dead letter's XADD ends run() after _REDIS_REFUSALS_BEFORE_EXIT of
#     them, the entry left pending and handled FIRST by the next run();
#     the pair -- a lost connection is retried, the process alive, and is
#     not counted among the refusals (TestARefusalEndsTheProcess,
#     TestTheKinds).
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 the dead letter's XADD tried once, the entry held -> TestTheDead-
#      LetterIsWritten
#   M2 a refusal retried like a lost connection -> TestARefusalEnds...
#   M3 _process_guarded swallows RedisRefusedError -> TestARefusalEnds...
#   M4 the exit on the first refusal -> test_seven_refusals_then_the_ack
#   M5 AuthenticationError taken as a lost connection -> TestTheKinds
#   M6 OutOfMemoryError taken as a refusal -> TestTheKinds
#   M7 a lost connection counted among the refusals ->
#      test_lost_connections_between_refusals_are_not_counted
#   M8 a dead letter that cannot be built retried in place ->
#      tests/test_consumer_keeps_its_entries.py::TestHeld
#   M9 _dlq_error not cut after the redaction -> TestTheErrorOfALetter
# =============================================================================

import asyncio
import json
from collections.abc import Callable
from typing import Any
from uuid import UUID, uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from redis.exceptions import (
    AuthenticationError,
    AuthorizationError,
    BusyLoadingError,
    NoPermissionError,
    OutOfMemoryError,
    ReadOnlyError,
    ResponseError,
)
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import GroupMembership, Recipient
from app.core.config import settings
from app.core.database import get_session_factory
from app.transport import consumer as consumer_module
from app.transport.consumer import StreamConsumer
from tests.test_transport_consumer import _user_upserted_data

_BOUND = 5.0
_REDRAIN = 0.1
_N = 8  # the contract of this test: _REDIS_REFUSALS_BEFORE_EXIT
_BAD = {"event": "user_deleted", "data": '{"v": 1}'}  # malformed: a dead letter


@pytest.fixture
def fake_redis() -> fakeaioredis.FakeRedis:
    return fakeaioredis.FakeRedis()


@pytest.fixture
def a_stream(monkeypatch: pytest.MonkeyPatch) -> str:
    name = f"comms:test:{uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", name)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    monkeypatch.setattr(consumer_module, "_BACKOFF_DELAYS", (0.001, 0.001, 0.001))
    monkeypatch.setattr(consumer_module, "_WAIT_CEILING", 0.002)
    monkeypatch.setattr(consumer_module, "_PENDING_REDRAIN_SECONDS", _REDRAIN)
    return name


class _Failing:
    """One command of the redis client fails by a script of exceptions
    (one per call, then it works); only the calls `which` picks fail.
    `calls` counts the picked calls. The code under test is untouched --
    the failure is the client's."""

    def __init__(
        self,
        redis: fakeaioredis.FakeRedis,
        command: str,
        which: Callable[..., bool],
        script: list[BaseException],
    ) -> None:
        self.calls = 0
        self.script = script
        real = getattr(redis, command)

        async def failing(*args: Any, **kwargs: Any) -> Any:
            if which(*args, **kwargs):
                self.calls += 1
                if self.calls <= len(self.script):
                    raise self.script[self.calls - 1]
            return await real(*args, **kwargs)

        setattr(redis, command, failing)


def _the_dlq(name: Any, *_: Any, **__: Any) -> bool:
    return bool(name == settings.dlq_stream)


def _the_entry(entry_id: bytes) -> Callable[..., bool]:
    def which(_name: Any, _group: Any, *ids: Any) -> bool:
        return entry_id in ids
    return which


def _lost() -> ConnectionError:
    return ConnectionError("the connection was lost")


def _readonly() -> ReadOnlyError:
    return ReadOnlyError("You can't write against a read only replica.")


async def _xadd(
    redis: fakeaioredis.FakeRedis, stream: str, event: str, data: dict[str, Any],
) -> bytes:
    entry_id: bytes = await redis.xadd(
        stream, {"event": event, "data": json.dumps(data)},
    )
    return entry_id


async def _pending(redis: fakeaioredis.FakeRedis, stream: str) -> list[Any]:
    try:
        return list(await redis.xpending_range(
            stream, settings.comms_consumer_group, min="-", max="+", count=10,
        ))
    except Exception:
        return [None]  # the group is not there yet: not "nothing pending"


async def _recipient(rid: UUID) -> bool:
    async with get_session_factory()() as s:
        return await s.scalar(
            select(Recipient.id).where(Recipient.id == rid),
        ) is not None


async def _member(group_key: str, rid: UUID) -> bool:
    async with get_session_factory()() as s:
        return await s.scalar(
            select(GroupMembership.recipient_id).where(
                GroupMembership.group_key == group_key,
                GroupMembership.recipient_id == rid,
            ),
        ) is not None


async def _dlq(redis: fakeaioredis.FakeRedis) -> list[dict[str, str]]:
    entries = await redis.xrange(settings.dlq_stream)
    return [{k.decode(): v.decode() for k, v in e[1].items()} for e in entries]


def _counted(monkeypatch: pytest.MonkeyPatch) -> list[bytes]:
    """The entries handled, in order."""
    met: list[bytes] = []
    real = StreamConsumer._process_entry

    async def counted(self: StreamConsumer, entry_id: Any, fields: Any) -> None:
        met.append(entry_id)
        await real(self, entry_id, fields)

    monkeypatch.setattr(StreamConsumer, "_process_entry", counted)
    return met


async def _until(task: "asyncio.Future[None]", condition: Any) -> None:
    deadline = asyncio.get_event_loop().time() + _BOUND
    while not await condition():
        assert asyncio.get_event_loop().time() < deadline, "not within the bound"
        assert not task.done(), f"the consumer went down: {task.exception()!r}"
        await asyncio.sleep(0.02)


async def _stop(task: "asyncio.Future[None]") -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def _ended(task: "asyncio.Future[None]") -> BaseException:
    """run() ended by itself within the bound; its exception."""
    done, _ = await asyncio.wait({task}, timeout=_BOUND)
    if not done:
        await _stop(task)
        raise AssertionError("run() did not end on the refusals")
    exc = task.exception()
    assert exc is not None
    return exc


# -----------------------------------------------------------------------------
# 1 -- the dead letter is written to the end
# -----------------------------------------------------------------------------


class TestTheDeadLetterIsWritten:
    async def test_one_letter_and_the_next_not_taken_before(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The dead letter's XADD fails three times (a lost connection),
        then works: one letter, the entry acknowledged, the entry behind
        handled only after the letter -- each handled once; the retry said
        and its end said."""
        bad = await fake_redis.xadd(a_stream, _BAD)
        later = uuid4()
        later_id = await _xadd(fake_redis, a_stream, "user_upserted",
                               _user_upserted_data(later))
        failing = _Failing(fake_redis, "xadd", _the_dlq, [_lost()] * 3)
        met = _counted(monkeypatch)
        with capture_logs() as logs:
            task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
            try:
                async def done() -> bool:
                    return await _recipient(later) and not await _pending(
                        fake_redis, a_stream,
                    )

                await _until(task, done)
                await asyncio.sleep(_REDRAIN * 3)
            finally:
                await _stop(task)
        assert failing.calls == 4
        (dead,) = await _dlq(fake_redis)
        assert dead["event"] == "user_deleted"
        assert met == [bad, later_id]  # in order, each once
        events = [e["event"] for e in logs]
        assert events.count("dead_letter_write_failed") == 1
        assert "dead_letter_written_after_retry" in events
        assert "dead_letter_build_failed" not in events

    async def test_the_reviews_case_n0(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        """group_changed true for a person not yet known: its retries run
        out, its dead letter fails three times; then the person, then
        false. The true is dead-lettered once, never applied; several passes
        over the pending list later the person is not in the group."""
        person, group = uuid4(), f"admins-{uuid4().hex[:6]}"
        await _xadd(fake_redis, a_stream, "group_changed", {
            "v": 1, "group_key": group, "recipient_id": str(person),
            "member": True,
        })
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(person))
        await _xadd(fake_redis, a_stream, "group_changed", {
            "v": 1, "group_key": group, "recipient_id": str(person),
            "member": False,
        })
        failing = _Failing(fake_redis, "xadd", _the_dlq, [_lost()] * 3)
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            async def done() -> bool:
                return failing.calls > 3 and not await _pending(
                    fake_redis, a_stream,
                )

            await _until(task, done)
            await asyncio.sleep(_REDRAIN * 5)
            assert not task.done()
        finally:
            await _stop(task)
        assert await _recipient(person)  # the pair: the person is known
        assert not await _member(group, person)
        (dead,) = await _dlq(fake_redis)
        assert dead["event"] == "group_changed"


# -----------------------------------------------------------------------------
# 2 -- a refusal of redis ends the process; the entry goes first after it
# -----------------------------------------------------------------------------


class TestARefusalEndsTheProcess:
    async def test_the_ack_refused_ends_run_and_the_entry_goes_first(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """XACK of the first entry: READONLY every time (a replica after a
        failover). run() ends with RedisRefusedError after _N of them; the entry
        behind is not handled; the entry stays pending. A new run() -- the
        restart -- handles it FIRST, then the next."""
        first, second = uuid4(), uuid4()
        first_id = await _xadd(fake_redis, a_stream, "user_upserted",
                               _user_upserted_data(first))
        second_id = await _xadd(fake_redis, a_stream, "user_upserted",
                                _user_upserted_data(second))
        failing = _Failing(fake_redis, "xack", _the_entry(first_id),
                           [_readonly() for _ in range(_N)])
        met = _counted(monkeypatch)
        with capture_logs() as logs:
            exc = await _ended(asyncio.ensure_future(
                StreamConsumer(fake_redis).run(),
            ))
        assert isinstance(exc, consumer_module.RedisRefusedError)
        assert isinstance(exc.__cause__, ReadOnlyError)
        assert failing.calls == _N
        assert met == [first_id]
        assert not await _recipient(second)
        assert [p["message_id"] for p in await _pending(fake_redis, a_stream)
                ][:1] == [first_id]
        assert "redis_refused" in [e["event"] for e in logs]
        # the restart: the script is spent, the ack works now
        met.clear()
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            async def done() -> bool:
                return await _recipient(second) and not await _pending(
                    fake_redis, a_stream,
                )

            await _until(task, done)
        finally:
            await _stop(task)
        assert met == [first_id, second_id]

    async def test_the_dead_letter_refused_ends_run(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        bad = await fake_redis.xadd(a_stream, _BAD)
        _Failing(fake_redis, "xadd", _the_dlq,
                 [NoPermissionError("NOPERM") for _ in range(_N)])
        exc = await _ended(asyncio.ensure_future(
            StreamConsumer(fake_redis).run(),
        ))
        assert isinstance(exc, consumer_module.RedisRefusedError)
        assert await _dlq(fake_redis) == []
        assert [p["message_id"] for p in await _pending(fake_redis, a_stream)
                ] == [bad]

    async def test_a_refusal_in_the_drain_at_the_start_ends_run(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        """The entry was delivered before (a crash between): the start's
        _drain_pending handles it, its XACK is refused -- run() ends there,
        through the same guard."""
        person = uuid4()
        entry = await _xadd(fake_redis, a_stream, "user_upserted",
                            _user_upserted_data(person))
        await fake_redis.xgroup_create(
            a_stream, settings.comms_consumer_group, id="0",
        )
        await fake_redis.xreadgroup(
            groupname=settings.comms_consumer_group,
            consumername=settings.comms_consumer_name,
            streams={a_stream: ">"}, count=10,
        )
        _Failing(fake_redis, "xack", _the_entry(entry),
                 [ResponseError("WRONGTYPE") for _ in range(_N)])
        exc = await _ended(asyncio.ensure_future(
            StreamConsumer(fake_redis).run(),
        ))
        assert isinstance(exc, consumer_module.RedisRefusedError)
        assert await _recipient(person)

    async def test_seven_refusals_then_the_ack(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        """The edge: _N - 1 refusals, then the XACK is taken -- the process
        lives on, the next entry follows."""
        first, second = uuid4(), uuid4()
        first_id = await _xadd(fake_redis, a_stream, "user_upserted",
                               _user_upserted_data(first))
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(second))
        failing = _Failing(fake_redis, "xack", _the_entry(first_id),
                           [_readonly() for _ in range(_N - 1)])
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            async def done() -> bool:
                return await _recipient(second) and not await _pending(
                    fake_redis, a_stream,
                )

            await _until(task, done)
        finally:
            await _stop(task)
        assert failing.calls == _N

    async def test_lost_connections_between_refusals_are_not_counted(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        """_N - 1 refusals with a lost connection after each: 2 * (_N - 1)
        failures, more than _N, the process alive -- only the refusals
        count."""
        first, second = uuid4(), uuid4()
        first_id = await _xadd(fake_redis, a_stream, "user_upserted",
                               _user_upserted_data(first))
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(second))
        script: list[BaseException] = []
        for _ in range(_N - 1):
            script += [_readonly(), _lost()]
        failing = _Failing(fake_redis, "xack", _the_entry(first_id), script)
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            async def done() -> bool:
                return await _recipient(second) and not await _pending(
                    fake_redis, a_stream,
                )

            await _until(task, done)
        finally:
            await _stop(task)
        assert failing.calls == len(script) + 1

    async def test_a_lost_connection_is_retried_and_the_process_lives(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        """The pair: the XACK loses its connection 3 * _N times -- retried,
        the process alive, the entry acknowledged in the end; no line of a
        refusal."""
        person = uuid4()
        entry = await _xadd(fake_redis, a_stream, "user_upserted",
                            _user_upserted_data(person))
        failing = _Failing(fake_redis, "xack", _the_entry(entry),
                           [_lost() for _ in range(3 * _N)])
        with capture_logs() as logs:
            task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
            try:
                async def done() -> bool:
                    return not await _pending(fake_redis, a_stream)

                await _until(task, done)
            finally:
                await _stop(task)
        assert failing.calls == 3 * _N + 1
        assert "redis_refused" not in [e["event"] for e in logs]


# -----------------------------------------------------------------------------
# Which failures are refusals
# -----------------------------------------------------------------------------


class TestTheKinds:
    def test_the_number(self) -> None:
        assert consumer_module._REDIS_REFUSALS_BEFORE_EXIT == _N

    @pytest.mark.parametrize("exc", [
        ReadOnlyError("READONLY"),
        NoPermissionError("NOPERM"),
        AuthenticationError("NOAUTH"),
        AuthenticationError("WRONGPASS"),
        AuthorizationError("not allowed"),
        ResponseError("WRONGTYPE Operation against a key"),
    ], ids=lambda e: type(e).__name__ + ":" + str(e).split()[0])
    def test_a_refusal(self, exc: BaseException) -> None:
        assert consumer_module._is_refusal(exc)

    @pytest.mark.parametrize("exc", [
        RedisConnectionError("lost"),
        RedisTimeoutError("timeout"),
        BusyLoadingError("LOADING"),
        OutOfMemoryError("OOM"),
        ConnectionError("builtin"),
        OSError("socket"),
        RuntimeError("a defect"),
    ], ids=lambda e: f"{type(e).__module__.split('.')[0]}.{type(e).__name__}")
    def test_retried(self, exc: BaseException) -> None:
        assert not consumer_module._is_refusal(exc)


class TestTheErrorOfALetter:
    def test_cut_at_its_bound_after_the_redaction(
        self, fake_redis: fakeaioredis.FakeRedis,
    ) -> None:
        """_dlq_error is cut at _DLQ_ERROR_MAX once redacted -- the number
        the contract states ("What a dead letter holds"); the pair: a
        short reason is kept whole."""
        assert consumer_module._DLQ_ERROR_MAX == 2000
        c = StreamConsumer(fake_redis)
        fields = {b"event": b"user_deleted", b"data": b"{}"}
        long = c._dead_letter_of("1-0", fields, reason="x" * 5000, attempts=0)
        assert long["_dlq_error"] == "x" * 2000
        short = c._dead_letter_of("1-0", fields, reason="why", attempts=0)
        assert short["_dlq_error"] == "why"
