# =============================================================================
# The consumer's acknowledgement is retried in place; the wait never
# overflows; a dead letter is bounded in bytes
# =============================================================================
#
#   1 an XACK after the commit is retried IN PLACE until it is written
#     (_ack_until_done): the event applied once, the next entry not taken
#     before the ack; a group_changed true whose ack failed is not
#     replayed over the false behind it; an ack that never comes holds
#     the consumer (alive, cancellable, the entry pending); a dead letter
#     already written is acknowledged the same way -- one letter, not
#     one per pass over the pending list (TestTheAckIsHeld);
#   2 the doubling wait (_wait_delay) at any count stays within
#     _WAIT_CEILING and raises nothing (TestTheWait);
#   3 a dead letter is always written and bounded: a value of `data`
#     over _DLQ_VALUE_MAX, an event name or a dropped name over
#     _DLQ_NAME_MAX are not kept -- named, or counted; _dlq_dropped lists
#     the first _DLQ_DROPPED_LISTED names and _dlq_dropped_more counts
#     the rest; the pair -- an ordinary dead letter is what it was
#     (TestTheBounds); the redis client checks its connection
#     (TestTheClient).
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  _ack_until_done = one XACK, as before -> TestTheAckIsHeld (next
#       taken; membership true; pending stays)
#   M2  the retry in a background task -> TestTheAckIsHeld::
#       test_applied_once_and_the_next_not_taken
#   M3  _dead_letter_or_hold acknowledges once -> TestTheAckIsHeld::
#       test_a_dead_letter_written_is_acknowledged_once (two letters)
#   M4  the old wait formula -> TestTheWait (OverflowError)
#   M5  no ceiling in the wait -> TestTheWait
#   M6  a value of data kept whatever its length -> TestTheBounds
#   M7  every dropped name listed -> TestTheBounds
#   M8  a dropped name kept whatever its length -> TestTheBounds
#   M9  _dlq_dropped_more counts only the names past N -> TestTheBounds
#   M10 the client without health_check_interval or socket_keepalive ->
#       TestTheClient
# =============================================================================

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import GroupMembership, Recipient
from app.core import constants
from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.service import stream_fingerprint
from app.transport import consumer as consumer_module
from app.transport.connection import redis_client
from app.transport.consumer import StreamConsumer
from tests.test_transport_consumer import _user_upserted_data

_BOUND = 5.0  # the test's own ceiling: a hang fails here, never forever
_REDRAIN = 0.1  # the pass over the pending list, test-sized


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
    monkeypatch.setattr(consumer_module, "_PENDING_REDRAIN_SECONDS", _REDRAIN)
    return name


async def _xadd(
    redis: fakeaioredis.FakeRedis, stream: str, event: str, data: dict[str, Any],
) -> bytes:
    entry_id: bytes = await redis.xadd(
        stream, {"event": event, "data": json.dumps(data)},
    )
    return entry_id


class _FailingXack:
    """The redis client's XACK of one entry raises while `failures`
    allows (a connection lost), then works; every other XACK works.
    `calls` counts the XACKs of that entry; the code under test is
    untouched -- the failure is the client's."""

    def __init__(
        self, redis: fakeaioredis.FakeRedis, entry_id: bytes, failures: int,
    ) -> None:
        self.calls = 0
        self.failures = failures
        real = redis.xack

        async def xack(name: Any, groupname: Any, *ids: Any) -> Any:
            if entry_id in ids:
                self.calls += 1
                if self.calls <= self.failures:
                    raise ConnectionError("XACK: the connection was lost")
            return await real(name, groupname, *ids)

        redis.xack = xack  # type: ignore[method-assign]


def _counted_entries(monkeypatch: pytest.MonkeyPatch) -> dict[bytes, int]:
    """How many times each entry was handled."""
    met: dict[bytes, int] = {}
    real = StreamConsumer._process_entry

    async def counted(self: StreamConsumer, entry_id: Any, fields: Any) -> None:
        met[entry_id] = met.get(entry_id, 0) + 1
        await real(self, entry_id, fields)

    monkeypatch.setattr(StreamConsumer, "_process_entry", counted)
    return met


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


async def _until(task: "asyncio.Future[None]", condition: Any) -> None:
    deadline = asyncio.get_event_loop().time() + _BOUND
    while not await condition():
        assert asyncio.get_event_loop().time() < deadline, "not within the bound"
        assert not task.done(), "the consumer went down"
        await asyncio.sleep(0.02)


async def _stop(task: "asyncio.Future[None]") -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# -----------------------------------------------------------------------------
# 1 -- the ack is held until it is written
# -----------------------------------------------------------------------------


class TestTheAckIsHeld:
    async def test_applied_once_and_the_next_not_taken(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The first entry's XACK fails while the gate is shut: the event is
        committed once, the entry behind it is not handled; the gate open,
        the ack is written, the next entry follows -- each handled once,
        nothing pending, nothing dead-lettered, the hold said loudly and
        its end said too."""
        first, second = uuid4(), uuid4()
        first_id = await _xadd(fake_redis, a_stream, "user_upserted",
                               _user_upserted_data(first))
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(second))
        failing = _FailingXack(fake_redis, first_id, failures=10**9)
        met = _counted_entries(monkeypatch)
        with capture_logs() as logs:
            task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
            try:
                async def held() -> bool:
                    return failing.calls >= 3

                await _until(task, held)
                assert await _recipient(first)
                assert not await _recipient(second)  # not taken
                assert met == {first_id: 1}
                assert not task.done()
                failing.failures = failing.calls  # the next XACK is written

                async def both() -> bool:
                    return await _recipient(second) and not await _pending(
                        fake_redis, a_stream,
                    )

                await _until(task, both)
            finally:
                await _stop(task)
        assert sorted(met.values()) == [1, 1]
        assert await _dlq(fake_redis) == []
        events = [e["event"] for e in logs]
        assert "event_ack_failed" in events
        assert "event_acked_after_retry" in events

    async def test_the_membership_true_is_not_replayed_over_the_false(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        """The review's case (2.3): group_changed true, its XACK failing three
        times, then group_changed false. The true is applied once; the
        false behind it is the last word -- several passes over the pending
        list later, the person is still not in the group."""
        person, group = uuid4(), f"admins-{uuid4().hex[:6]}"
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(person))
        joined = await _xadd(fake_redis, a_stream, "group_changed", {
            "v": 1, "group_key": group, "recipient_id": str(person),
            "member": True,
        })
        await _xadd(fake_redis, a_stream, "group_changed", {
            "v": 1, "group_key": group, "recipient_id": str(person),
            "member": False,
        })
        failing = _FailingXack(fake_redis, joined, failures=3)
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            async def done() -> bool:
                return failing.calls > 3 and not await _pending(fake_redis, a_stream)

            await _until(task, done)
            await asyncio.sleep(_REDRAIN * 5)  # passes over the pending list
            assert not task.done()
        finally:
            await _stop(task)
        assert await _recipient(person)  # the pair: the person is known
        assert not await _member(group, person)

    async def test_an_ack_that_never_comes_holds_the_consumer(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        """The shortfall: the XACK never succeeds. The consumer stays alive and
        keeps the next entry back; the stop is still a stop, and the entry
        is left pending for the next start (the KNOWN CEILING of
        _ack_until_done)."""
        first, second = uuid4(), uuid4()
        first_id = await _xadd(fake_redis, a_stream, "user_upserted",
                               _user_upserted_data(first))
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(second))
        failing = _FailingXack(fake_redis, first_id, failures=10**9)
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            async def many() -> bool:
                return failing.calls >= 10

            await _until(task, many)
            assert not await _recipient(second)
        finally:
            await _stop(task)
        assert await _recipient(first)
        assert [p["message_id"] for p in await _pending(fake_redis, a_stream)
                ][:1] == [first_id]

    async def test_a_dead_letter_written_is_acknowledged_once(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        """A malformed entry is dead-lettered; its XACK fails three times.
        One dead letter, not one per pass over the pending list; the entry
        behind it is handled; nothing pending."""
        bad = await fake_redis.xadd(
            a_stream, {"event": "user_deleted", "data": '{"v": 1}'},
        )
        later = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(later))
        failing = _FailingXack(fake_redis, bad, failures=3)
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            async def done() -> bool:
                return (failing.calls > 3 and await _recipient(later)
                        and not await _pending(fake_redis, a_stream))

            await _until(task, done)
            await asyncio.sleep(_REDRAIN * 5)
        finally:
            await _stop(task)
        (dead,) = await _dlq(fake_redis)
        assert dead["event"] == "user_deleted"

    async def test_an_ack_that_works_says_nothing(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        """The empty case: no failure -- no line of the hold, the event processed."""
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(person))
        with capture_logs() as logs:
            task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
            try:
                async def done() -> bool:
                    return await _recipient(person) and not await _pending(
                        fake_redis, a_stream,
                    )

                await _until(task, done)
            finally:
                await _stop(task)
        events = [e["event"] for e in logs]
        assert "event_processed" in events
        assert "event_ack_failed" not in events
        assert "event_acked_after_retry" not in events


# -----------------------------------------------------------------------------
# 2 -- the wait never overflows
# -----------------------------------------------------------------------------


class TestTheWait:
    @pytest.mark.parametrize("n", [1, 10, 1025, 10**6])
    def test_within_the_ceiling_at_any_count(self, n: int) -> None:
        """Before: 2 ** (waited - 1) at ~1025 waits (~8.5 h of a database
        away) raised OverflowError, out of the wait itself."""
        delay = consumer_module._wait_delay(n)
        assert 0 < delay <= consumer_module._WAIT_CEILING

    def test_it_doubles_from_the_first_delay_to_the_ceiling(self) -> None:
        """The pair: the bound is not the only value -- the first delay is
        the first of _BACKOFF_DELAYS, the second twice it, and the ceiling
        is reached and held."""
        first = consumer_module._BACKOFF_DELAYS[0]
        assert consumer_module._wait_delay(1) == first
        assert consumer_module._wait_delay(2) == 2 * first
        assert consumer_module._wait_delay(10) == consumer_module._WAIT_CEILING
        assert consumer_module._wait_delay(10**6) == consumer_module._WAIT_CEILING


# -----------------------------------------------------------------------------
# 3 -- a dead letter is bounded in bytes
# -----------------------------------------------------------------------------

# The contract's numbers (deploy/INTEGRATION.md, "What a dead letter
# holds"), held to the code's in TestTheBounds::test_the_numbers.
_VALUE_MAX = 2048
_NAME_MAX = 128
_LISTED = 20
_RID = "00000000-0000-4000-8000-000000000001"
_DOC = Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md"


def _dead(fields: dict[bytes, bytes]) -> dict[str, str]:
    return consumer_module.dead_letter_fields(
        fields, reason="why", attempts=1, entry_id="1-0",
    )


def _upserted(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "v": 1, "recipient_id": _RID, "version": 3, "telegram_id": 42,
        "email": "a@example.com", "locale": "en", "timezone": "UTC",
        "active": True,
    }
    data.update(overrides)
    return data


def _entry(event: str, data: dict[str, Any], **extra: bytes) -> dict[bytes, bytes]:
    fields = {b"event": event.encode(),
              b"data": json.dumps(data, separators=(",", ":")).encode()}
    for name, value in extra.items():
        fields[name.encode()] = value
    return fields


def _within_the_bounds(dead: dict[str, str]) -> None:
    for value in json.loads(dead["_dlq_data"]).values():
        assert len(json.dumps(value, sort_keys=True)) <= _VALUE_MAX
    names = json.loads(dead["_dlq_dropped"])
    assert len(names) <= _LISTED
    for name in names:
        assert len(json.dumps(name)) <= _NAME_MAX
    assert len(dead.get("event", "").encode()) <= _NAME_MAX


class TestTheBounds:
    def test_the_numbers(self) -> None:
        """The contract states the code's numbers, and the code holds them."""
        assert consumer_module._DLQ_VALUE_MAX == _VALUE_MAX
        assert consumer_module._DLQ_NAME_MAX == _NAME_MAX
        assert consumer_module._DLQ_DROPPED_LISTED == _LISTED
        text = " ".join(_DOC.read_text(encoding="utf-8").split())
        assert f"value of `data` longer than **{_VALUE_MAX}** is not kept" in text
        assert f"the first **{_LISTED}** names in sorted order" in text
        assert f"each at most **{_NAME_MAX}** long as JSON" in text

    def test_a_huge_value_is_not_kept_and_the_letter_is_written(self) -> None:
        """The review's case (5): user_upserted with an email of ~4 MB --
        before, all of it in _dlq_data. Now the letter is there, the email
        is named, the rest of the address is kept."""
        email = "a" * (4 * 1024 * 1024 - 300)
        dead = _dead(_entry("user_upserted", _upserted(email=email)))
        kept = json.loads(dead["_dlq_data"])
        assert "email" not in kept
        assert kept["recipient_id"] == _RID  # the pair: the address is kept
        assert "data.email" in json.loads(dead["_dlq_dropped"])
        assert dead["_dlq_dropped_more"] == "0"
        assert dead["_dlq_data_bytes"] == str(
            len(json.dumps(_upserted(email=email), separators=(",", ":"))),
        )
        _within_the_bounds(dead)
        assert sum(len(v) for v in dead.values()) < 10_000

    def test_the_value_at_the_bound_is_kept_one_past_is_not(self) -> None:
        at = "a" * (_VALUE_MAX - 2)  # the quotes make it _VALUE_MAX
        past = "a" * (_VALUE_MAX - 1)
        assert json.loads(_dead(_entry("user_upserted", _upserted(email=at)))
                          ["_dlq_data"])["email"] == at
        assert "email" not in json.loads(
            _dead(_entry("user_upserted", _upserted(email=past)))["_dlq_data"],
        )

    def test_hundreds_of_thousands_of_keys(self) -> None:
        """The review's case (5): 300 000 keys not of the event -- before,
        4.8 MB of names. Now the first twenty, sorted, and the count."""
        data: dict[str, Any] = {"v": 1, "recipient_id": _RID, "version": 2}
        extra = {f"x{i:06d}": 1 for i in range(300_000)}
        data.update(extra)
        dead = _dead(_entry("user_deleted", data))
        names = json.loads(dead["_dlq_dropped"])
        assert names == [f"data.x{i:06d}" for i in range(_LISTED)]
        assert dead["_dlq_dropped_more"] == str(300_000 - _LISTED)
        assert json.loads(dead["_dlq_data"]) == {
            "v": 1, "recipient_id": _RID, "version": 2,
        }
        _within_the_bounds(dead)
        assert sum(len(v) for v in dead.values()) < 10_000

    @pytest.mark.parametrize(("extra", "more"), [(_LISTED, "0"), (_LISTED + 1, "1")])
    def test_the_count_at_the_bound(self, extra: int, more: str) -> None:
        data: dict[str, Any] = {"v": 1, "recipient_id": _RID, "version": 2}
        data.update({f"k{i:03d}": 0 for i in range(extra)})
        dead = _dead(_entry("user_deleted", data))
        assert len(json.loads(dead["_dlq_dropped"])) == _LISTED
        assert dead["_dlq_dropped_more"] == more

    def test_a_long_name_is_counted_not_listed(self) -> None:
        """A field of the entry named with a megabyte, and a name exactly
        at the bound beside it: the long one counted, the other listed."""
        at = "n" * (_NAME_MAX - 2)
        dead = _dead(_entry("user_deleted",
                            {"v": 1, "recipient_id": _RID, "version": 2},
                            **{"x" * 1_000_000: b"1", at: b"1"}))
        assert json.loads(dead["_dlq_dropped"]) == [at]
        assert dead["_dlq_dropped_more"] == "1"
        past = "n" * (_NAME_MAX - 1)
        dead = _dead(_entry("user_deleted",
                            {"v": 1, "recipient_id": _RID, "version": 2},
                            **{past: b"1"}))
        assert json.loads(dead["_dlq_dropped"]) == []
        assert dead["_dlq_dropped_more"] == "1"

    def test_a_long_event_name_is_not_kept(self) -> None:
        dead = _dead({b"event": b"e" * 200, b"data": b"{}"})
        assert "event" not in dead
        assert json.loads(dead["_dlq_dropped"]) == ["data", "event"]
        assert dead["_dlq_dropped_more"] == "0"
        # the pair: a name at the bound is kept (an unknown event, so
        # its data is not)
        dead = _dead({b"event": b"e" * _NAME_MAX, b"data": b"{}"})
        assert dead["event"] == "e" * _NAME_MAX

    def test_an_ordinary_dead_letter_is_what_it_was(self) -> None:
        """The pair: a dead letter of a valid size, every field as before,
        _dlq_dropped_more "0" beside it."""
        fields = _entry("user_upserted", _upserted(), x=b"y")
        raw = fields[b"data"]
        dead = _dead(fields)
        assert dead == {
            "event": "user_upserted",
            "_dlq_data": json.dumps(_upserted(), sort_keys=True),
            "_dlq_data_fingerprint": stream_fingerprint(raw),
            "_dlq_data_bytes": str(len(raw)),
            "_dlq_dropped": '["x"]',
            "_dlq_dropped_more": "0",
            "_dlq_error": "why",
            "_dlq_attempts": "1",
            "_dlq_source_entry_id": "1-0",
        }

    async def test_on_the_real_loop(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        db_session: AsyncSession,
    ) -> None:
        """The huge email through the consumer: refused, dead-lettered within
        the bounds, acknowledged; the next event processed."""
        email = "a" * (4 * 1024 * 1024 - 300)
        await fake_redis.xadd(a_stream, _entry("user_upserted",
                                               _upserted(email=email)))
        later = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(later))
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            async def done() -> bool:
                return await _recipient(later) and not await _pending(
                    fake_redis, a_stream,
                )

            await _until(task, done)
        finally:
            await _stop(task)
        (dead,) = await _dlq(fake_redis)
        assert dead["event"] == "user_upserted"
        assert "data.email" in json.loads(dead["_dlq_dropped"])
        _within_the_bounds(dead)
        assert sum(len(v) for v in dead.values()) < 10_000


# -----------------------------------------------------------------------------
# The client checks its connection
# -----------------------------------------------------------------------------


class TestTheClient:
    async def test_idle_connections_are_checked_and_kept_alive(self) -> None:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(settings, "redis_url", "redis://127.0.0.1:6379/0")
            client = redis_client()
        try:
            kwargs = client.connection_pool.connection_kwargs
            interval = constants.REDIS_HEALTH_CHECK_SECONDS
            assert kwargs["health_check_interval"] == interval
            assert interval > 0
            assert kwargs["socket_keepalive"] is True
        finally:
            await client.aclose()
