# =============================================================================
# P3-1 Б2 -- the push relay: after the commit, the key and nothing else,
# at least once, through a redis that goes away
# =============================================================================
#
# MUTATIONS these tests were written against (each turns one red):
#   M7  a push published before its transition commits (a "fast path"
#       that XADDs the keys the registrar collected)
#                               -> TestCommitted
#   M8  a field besides the version and the key in an entry
#                               -> TestEntry, tests/test_push_contract.py
#   M9  the rows deleted BEFORE the XADD
#                               -> TestFailure.test_redis_away_...
#   M10 the rows dropped when redis refuses
#                               -> TestFailure.test_redis_away_...
# =============================================================================

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from fakeredis import FakeServer
from fakeredis import aioredis as fakeaioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

import app.consumer as entrypoint
import app.transport.push_relay as relay
from app.core.config import Settings, settings
from app.core.database import get_session_factory
from app.engine import service
from app.engine.constants import NotificationStatus, TargetType
from app.engine.models import Notification, PushOutbox
from app.engine.processor import process_pending_notifications
from app.engine.service import create_notification
from app.profile.registry import Decided, Layer, PushOn, TypeRecord, registry
from tests.helpers import create_recipient, intake_fields, notification_row_fields

_WAIT = 5.0
# Never a substring of anything routine (a key, a version, a field name).
_SENTINEL = "zq7" + "PUSHLETTER" + "xk4"


@pytest.fixture
def server() -> FakeServer:
    return FakeServer()


@pytest.fixture
def redis(server: FakeServer) -> fakeaioredis.FakeRedis:
    return fakeaioredis.FakeRedis(server=server)


@pytest.fixture(autouse=True)
def stream(monkeypatch: pytest.MonkeyPatch) -> str:
    """A stream of this test's own, through the derivation."""
    monkeypatch.setattr(
        settings, "comms_events_stream", f"comms:test:{uuid4().hex[:8]}",
    )
    return settings.changes_stream


async def _owed(key: str | None = None, *, copies: int = 1) -> UUID:
    """A committed job with `copies` push rows owed for it."""
    async with get_session_factory()() as session:
        fields = notification_row_fields()
        if key is not None:
            fields["idempotency_key"] = key
        job = Notification(
            type="unit_event_in_app", title="T", body="B",
            target_type=TargetType.ALL, target_value="*",
            status=NotificationStatus.SENT, **fields,
        )
        session.add(job)
        await session.flush()
        for _ in range(copies):
            session.add(PushOutbox(notification_id=job.id))
        await session.commit()
        return job.id


async def _outbox() -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(
            select(func.count()).select_from(PushOutbox)
        )).scalar_one())


async def _key_of(notification_id: UUID) -> str:
    async with get_session_factory()() as session:
        job = await session.get(Notification, notification_id)
        assert job is not None
        return job.idempotency_key


async def _entries(redis: Any) -> list[dict[bytes, bytes]]:
    return [fields for _, fields in await redis.xrange(settings.changes_stream)]


async def _until_drained() -> None:
    """Poll the outbox until the running relay has emptied it."""
    for _ in range(int(_WAIT / 0.02)):
        if not await _outbox():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("the outbox did not drain")


def _keys(entries: list[dict[bytes, bytes]]) -> list[str]:
    return [e[b"idempotency_key"].decode() for e in entries]


@contextmanager
def _declared_outcome() -> Iterator[None]:
    record = registry.record_of("unit_event_in_app")
    assert record is not None
    fields = dict(record.fields)
    fields["push_on"] = Decided(PushOn.OUTCOME.value, Layer.PROFILE, "test")
    registry.register_type(
        "unit_event_in_app",
        category=registry.category_of("unit_event_in_app"),
        record=TypeRecord(fields=fields),
    )
    yield


class _Spy:
    async def deliver(self, *args: Any) -> bool:
        return True


# -----------------------------------------------------------------------------
# The entry
# -----------------------------------------------------------------------------


class TestEntry:
    async def test_an_entry_is_the_version_and_the_key_never_the_letter(
        self, db_session: AsyncSession, redis: Any,
    ) -> None:
        """Through the real pipeline: a job whose letter carries a
        sentinel everywhere it can, sent, pushed, relayed. The entry is
        exactly {v, idempotency_key}; the sentinel is nowhere in the
        stream. The pair: the key is there and not empty."""
        recipient = await create_recipient(db_session)
        with _declared_outcome():
            job = await create_notification(
                db_session, **intake_fields(), type="unit_event_in_app",
                title=f"T {_SENTINEL}", body=f"B {_SENTINEL}",
                action_data={"note": _SENTINEL, _SENTINEL: 1},
                target_type=TargetType.USER, target_value=str(recipient.id),
            )
            await db_session.commit()
            with patch("app.engine.service.get_formatter", return_value=_Spy()):
                await process_pending_notifications()
        assert await relay.relay_once(redis) == 1
        (entry,) = await _entries(redis)
        assert set(entry) == {b"v", b"idempotency_key"}
        assert entry[b"v"] == b"1"
        assert entry[b"idempotency_key"].decode() == job.idempotency_key
        assert job.idempotency_key
        raw = b"".join(k + v for k, v in entry.items())
        assert _SENTINEL.encode() not in raw

    def test_the_sentinel_is_not_a_substring_of_anything_routine(self) -> None:
        routine = " ".join([
            *relay.PUSH_FIELDS, str(relay.PUSH_FORMAT_VERSION),
            settings.changes_stream, "test:" + str(uuid4()),
        ])
        assert _SENTINEL not in routine

    def test_push_entry_has_exactly_the_contract_fields(self) -> None:
        entry = relay.push_entry("k-1")
        assert tuple(entry) == relay.PUSH_FIELDS
        assert entry == {"v": "1", "idempotency_key": "k-1"}


# -----------------------------------------------------------------------------
# Only what committed
# -----------------------------------------------------------------------------


class TestCommitted:
    async def test_an_uncommitted_push_is_not_published_until_it_commits(
        self, db_session: AsyncSession, redis: Any,
    ) -> None:
        """Session A has resolved a job to its outcome, push included,
        flushed but not committed; the relay, in sessions of its own,
        publishes nothing. A commits; the next tick publishes the key."""
        with _declared_outcome():
            job = await create_notification(
                db_session, **intake_fields(), type="unit_event_in_app",
                title="T", body="B", target_type=TargetType.USER,
                target_value=str(uuid4()),
            )
            await db_session.commit()
            async with get_session_factory()() as writer:
                mine = await writer.get(Notification, job.id)
                assert mine is not None
                await service.resolve_notification(writer, mine)
                await writer.flush()
                assert mine.status == NotificationStatus.NO_RECIPIENTS
                assert await relay.relay_once(redis) == 0
                assert await _entries(redis) == []
                await writer.commit()
        assert await relay.relay_once(redis) == 1
        assert _keys(await _entries(redis)) == [job.idempotency_key]


# -----------------------------------------------------------------------------
# Failures: at least once
# -----------------------------------------------------------------------------


class _DeleteTearsError(RuntimeError):
    """The relay dies between the XADD and the delete."""


class TestFailure:
    async def test_death_between_xadd_and_delete_publishes_again(
        self, redis: Any,
    ) -> None:
        nid = await _owed()

        def tearing_delete(*args: Any) -> Any:
            raise _DeleteTearsError

        with (
            patch.object(relay, "delete", tearing_delete),
            pytest.raises(_DeleteTearsError),
        ):
            await relay.relay_once(redis)
        assert len(await _entries(redis)) == 1
        assert await _outbox() == 1
        assert await relay.relay_once(redis) == 1
        key = await _key_of(nid)
        assert _keys(await _entries(redis)) == [key, key]
        assert await _outbox() == 0

    async def test_redis_away_keeps_the_debt_and_transitions_go_on(
        self, db_session: AsyncSession, server: FakeServer, redis: Any,
    ) -> None:
        """Redis refuses: the worker (DB only) still takes the job to its
        outcome, the push waits in the outbox, the tick fails without
        losing it. Redis back: published, outbox empty."""
        server.connected = False
        recipient = await create_recipient(db_session)
        with _declared_outcome():
            job = await create_notification(
                db_session, **intake_fields(), type="unit_event_in_app",
                title="T", body="B", target_type=TargetType.USER,
                target_value=str(recipient.id),
            )
            await db_session.commit()
            with patch("app.engine.service.get_formatter", return_value=_Spy()):
                await process_pending_notifications()
        async with get_session_factory()() as session:
            mine = await session.get(Notification, job.id)
            assert mine is not None and mine.status == NotificationStatus.SENT
        with pytest.raises(RedisConnectionError):
            await relay.relay_once(redis)
        assert await _outbox() == 1
        server.connected = True
        assert await relay.relay_once(redis) == 1
        assert _keys(await _entries(redis)) == [job.idempotency_key]
        assert await _outbox() == 0

    async def test_the_loop_outlives_a_redis_outage(
        self, server: FakeServer, redis: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(relay, "PUSH_RELAY_INTERVAL_SECONDS", 0.01)
        nid = await _owed()
        server.connected = False
        task = asyncio.create_task(relay.run_push_relay(redis))
        try:
            await asyncio.sleep(0.1)
            assert not task.done()
            assert await _outbox() == 1
            server.connected = True
            await _until_drained()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert _keys(await _entries(redis)) == [await _key_of(nid)]

    async def test_a_backlog_drains_without_pausing_between_full_batches(
        self, redis: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(relay, "PUSH_RELAY_BATCH", 2)
        monkeypatch.setattr(relay, "PUSH_RELAY_INTERVAL_SECONDS", 30.0)
        for _ in range(5):
            await _owed()
        task = asyncio.create_task(relay.run_push_relay(redis))
        try:
            await _until_drained()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert len(await _entries(redis)) == 5


# -----------------------------------------------------------------------------
# The batch and the stream
# -----------------------------------------------------------------------------


class TestBatch:
    async def test_nothing_owed_publishes_nothing(self, redis: Any) -> None:
        assert await relay.relay_once(redis) == 0
        assert await redis.exists(settings.changes_stream) == 0

    async def test_the_rows_of_one_job_in_a_batch_are_one_entry(
        self, redis: Any,
    ) -> None:
        nid = await _owed(copies=3)
        assert await relay.relay_once(redis) == 3
        assert _keys(await _entries(redis)) == [await _key_of(nid)]
        assert await _outbox() == 0

    async def test_distinct_jobs_are_distinct_entries_in_outbox_order(
        self, redis: Any,
    ) -> None:
        first = await _owed()
        second = await _owed()
        await relay.relay_once(redis)
        assert _keys(await _entries(redis)) == [
            await _key_of(first), await _key_of(second),
        ]

    async def test_the_stream_is_capped(
        self, redis: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "changes_stream_maxlen", 5)
        owed = [await _owed() for _ in range(30)]
        await relay.relay_once(redis)
        length = await redis.xlen(settings.changes_stream)
        assert 0 < length < 30
        assert _keys(await _entries(redis))[-1] == await _key_of(owed[-1])


class TestStreamName:
    def test_the_name_is_derived_and_is_neither_inbound_nor_dlq(self) -> None:
        assert settings.changes_stream == f"{settings.comms_events_stream}:changes"
        assert settings.changes_stream not in (
            settings.comms_events_stream, settings.dlq_stream,
        )

    def test_the_defaults(self) -> None:
        fields = Settings.model_fields
        assert fields["comms_events_stream"].default == "comms:events"
        assert fields["changes_stream_maxlen"].default == 100_000


# -----------------------------------------------------------------------------
# The consumer process runs both loops
# -----------------------------------------------------------------------------


class _ConsumerDiedError(RuntimeError):
    pass


class TestEntrypoint:
    @pytest.fixture(autouse=True)
    async def _no_signal_handlers(self) -> AsyncIterator[None]:
        """_main installs SIGTERM/SIGINT handlers on the running loop --
        the suite's own loop here; kept off it."""
        loop = asyncio.get_running_loop()
        with patch.object(loop, "add_signal_handler"):
            yield

    async def test_a_dead_consumer_ends_the_process_and_cancels_the_relay(
        self,
    ) -> None:
        cancelled = asyncio.Event()

        async def dying_consumer() -> None:
            await asyncio.sleep(0.01)
            raise _ConsumerDiedError

        async def endless_relay() -> None:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        with (
            patch.object(entrypoint, "run_consumer_loop", dying_consumer),
            patch.object(entrypoint, "run_push_relay_loop", endless_relay),
            pytest.raises(_ConsumerDiedError),
        ):
            await entrypoint._main()
        assert cancelled.is_set()

    async def test_both_loops_run(self) -> None:
        """The pair: neither loop is left out -- each was started."""
        started: list[str] = []

        async def consumer() -> None:
            started.append("consumer")
            await asyncio.sleep(0.05)
            raise _ConsumerDiedError

        async def push_relay() -> None:
            started.append("relay")
            await asyncio.sleep(3600)

        with (
            patch.object(entrypoint, "run_consumer_loop", consumer),
            patch.object(entrypoint, "run_push_relay_loop", push_relay),
            pytest.raises(_ConsumerDiedError),
        ):
            await entrypoint._main()
        assert sorted(started) == ["consumer", "relay"]
