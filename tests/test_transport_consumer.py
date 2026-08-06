# =============================================================================
# COMMS Service -- Consumer e2e tests (Phase 3c items 1, 2, 3, 4, 6)
# =============================================================================
#
# End-to-end over fakeredis: XADD into the stream -> StreamConsumer
# processes -> rows in Postgres / entries in the DLQ. The consumer
# loop runs as a background task and is cancelled once the awaited
# condition holds (or the deadline hits).
#
# Streams are per-test (unique names monkeypatched into settings) so
# fakeredis state never bleeds between tests. Backoff delays are
# shrunk via monkeypatch where retries matter -- the production
# values (~6s) are a BL-4-documented ceiling, not something to sleep
# through in CI. telegram_ids from the Phase 3c band 85000-85999.
# =============================================================================

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import pytest
from fakeredis import aioredis as fakeaioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import app.transport.consumer as consumer_module
from app.audience.models import GroupMembership, Recipient
from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.models import Notification
from app.transport.consumer import StreamConsumer
from tests.helpers import create_recipient, next_phase3c_telegram_id

_WAIT_TIMEOUT = 5.0


@pytest.fixture
def redis() -> fakeaioredis.FakeRedis:
    return fakeaioredis.FakeRedis()


@pytest.fixture
def stream(monkeypatch: pytest.MonkeyPatch) -> str:
    """Unique stream per test + fast loop settings."""
    name = f"comms:test:{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", name)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    return name


@pytest.fixture
def fast_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shrunk but NOT razor-thin: the ordering test injects the
    lagging upsert from a parallel task, and its commit must land
    within the retry budget even on a loaded CI box -- ~0.75s of
    budget against a ~0.03s injection leaves two orders of margin."""
    monkeypatch.setattr(
        consumer_module, "_BACKOFF_DELAYS", (0.05, 0.1, 0.2, 0.4),
    )


async def _xadd(
    redis: fakeaioredis.FakeRedis,
    stream: str,
    event: str,
    data: dict[str, Any],
) -> None:
    await redis.xadd(stream, {"event": event, "data": json.dumps(data)})


async def _run_until(
    consumer: StreamConsumer,
    condition: Callable[[], Awaitable[bool]],
) -> None:
    """Run the consumer loop until the condition holds, then cancel."""
    task = asyncio.ensure_future(consumer.run())
    try:
        deadline = asyncio.get_event_loop().time() + _WAIT_TIMEOUT
        while True:
            if await condition():
                return
            if asyncio.get_event_loop().time() > deadline:
                pytest.fail("condition not reached before timeout")
            if task.done():
                # Surface a crashed loop instead of spinning silently.
                task.result()
                pytest.fail("consumer loop exited unexpectedly")
            await asyncio.sleep(0.02)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


def _user_upserted_data(recipient_id: UUID, **overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "v": 1,
        "recipient_id": str(recipient_id),
        "telegram_id": next_phase3c_telegram_id(),
        "email": None,
        "locale": "en",
        "timezone": "Europe/Berlin",
        "active": True,
    }
    data.update(overrides)
    return data


def _request_data(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "v": 1,
        "idempotency_key": f"key-{uuid4().hex}",
        "type": "unit_event",
        "target_type": "user",
        "target_value": str(uuid4()),
        "title": "T",
        "body": "B",
    }
    data.update(overrides)
    return data


async def _no_pending(
    redis: fakeaioredis.FakeRedis, stream: str,
) -> bool:
    """True when the group exists and has nothing pending.

    Uses the RANGE form (the summary form trips redis-py's parser
    against fakeredis while the group races into existence)."""
    try:
        still_pending = await redis.xpending_range(
            stream, settings.comms_consumer_group,
            min="-", max="+", count=10,
        )
    except Exception:
        return False  # group not created yet
    return not still_pending


async def _seed_recipient() -> UUID:
    factory = get_session_factory()
    async with factory() as session:
        recipient = await create_recipient(
            session, telegram_id=next_phase3c_telegram_id(),
        )
        rid = recipient.id
        await session.commit()
    return rid


async def _notification_count(
    db_session: AsyncSession, idempotency_key: str | None = None,
) -> int:
    stmt = select(Notification)
    if idempotency_key is not None:
        stmt = stmt.where(Notification.idempotency_key == idempotency_key)
    return len((await db_session.execute(stmt)).scalars().all())


class TestEndToEnd:
    async def test_user_upserted_creates_recipient(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
    ) -> None:
        rid = uuid4()
        await _xadd(redis, stream, "user_upserted", _user_upserted_data(rid))

        async def synced_and_acked() -> bool:
            # Commit precedes XACK inside the consumer: waiting only
            # for the row would race the assertion with the ack.
            db_session.expire_all()
            if await db_session.get(Recipient, rid) is None:
                return False
            return await _no_pending(redis, stream)

        await _run_until(StreamConsumer(redis), synced_and_acked)

    async def test_group_changed_after_upsert(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
    ) -> None:
        rid = await _seed_recipient()
        await _xadd(redis, stream, "group_changed", {
            "v": 1, "group_key": "practice_42",
            "recipient_id": str(rid), "member": True,
        })

        async def member() -> bool:
            db_session.expire_all()
            row = await db_session.scalar(
                select(GroupMembership).where(
                    GroupMembership.group_key == "practice_42",
                    GroupMembership.recipient_id == rid,
                )
            )
            return row is not None

        await _run_until(StreamConsumer(redis), member)

    async def test_notification_request_materialized(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
    ) -> None:
        data = _request_data(
            action_data={"action": "open_unit",
                         "params": {"unit_id": "42"}, "amount": 100},
            channels=["in_app"],
        )
        await _xadd(redis, stream, "notification_request", data)

        async def materialized() -> bool:
            db_session.expire_all()
            return (
                await _notification_count(
                    db_session, data["idempotency_key"],
                ) == 1
            )

        await _run_until(StreamConsumer(redis), materialized)

        row = (await db_session.execute(
            select(Notification).where(
                Notification.idempotency_key == data["idempotency_key"],
            )
        )).scalar_one()
        assert row.type == "unit_event"
        assert row.title == "T"
        # The pipeline's channel stash merged in by create_notification.
        assert row.action_data is not None
        assert row.action_data["_channels"] == ["in_app"]
        assert row.action_data["amount"] == 100


class TestIdempotency:
    async def test_replay_does_not_duplicate(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
    ) -> None:
        """The same event delivered twice -> ONE notification, both
        entries acked, nothing dead-lettered (a replay is the
        at-least-once contract working)."""
        data = _request_data()
        await _xadd(redis, stream, "notification_request", data)
        await _xadd(redis, stream, "notification_request", data)

        async def both_acked() -> bool:
            length: int = await redis.xlen(stream)
            return length == 2 and await _no_pending(redis, stream)

        await _run_until(StreamConsumer(redis), both_acked)

        db_session.expire_all()
        assert await _notification_count(
            db_session, data["idempotency_key"],
        ) == 1
        assert await redis.exists(settings.dlq_stream) == 0


class TestRestartRecovery:
    async def test_pending_replayed_after_crash(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
    ) -> None:
        """Delivered-but-unacked entries (crash between XREADGROUP and
        XACK) are drained by the next start under the same stable
        consumer name."""
        rid = uuid4()
        consumer = StreamConsumer(redis)
        await consumer.ensure_group()
        await _xadd(redis, stream, "user_upserted", _user_upserted_data(rid))

        # Simulate the crash: read WITHOUT processing or acking.
        response = await redis.xreadgroup(
            groupname=settings.comms_consumer_group,
            consumername=settings.comms_consumer_name,
            streams={stream: ">"},
            count=10,
        )
        assert response and response[0][1]

        async def synced() -> bool:
            db_session.expire_all()
            return await db_session.get(Recipient, rid) is not None

        async def synced_and_acked() -> bool:
            if not await synced():
                return False
            return await _no_pending(redis, stream)

        # "Restart": a fresh consumer instance drains its pending.
        await _run_until(StreamConsumer(redis), synced_and_acked)


class TestOrderingRetry:
    async def test_group_changed_waits_for_upsert(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
        fast_backoff: None,
    ) -> None:
        """Item 6a: group_changed arrives FIRST; the consumer retries
        with backoff, user_upserted lands mid-retry, membership
        materializes -- no DLQ."""
        rid = uuid4()
        await _xadd(redis, stream, "group_changed", {
            "v": 1, "group_key": "practice_42",
            "recipient_id": str(rid), "member": True,
        })

        async def inject_upsert() -> None:
            # Land the lagging upsert while the retry backoff sleeps.
            await asyncio.sleep(0.03)
            factory = get_session_factory()
            async with factory() as session:
                await create_recipient(
                    session, recipient_id=rid,
                    telegram_id=next_phase3c_telegram_id(),
                )
                await session.commit()

        async def member() -> bool:
            db_session.expire_all()
            row = await db_session.scalar(
                select(GroupMembership).where(
                    GroupMembership.recipient_id == rid,
                )
            )
            return row is not None

        inject = asyncio.ensure_future(inject_upsert())
        try:
            await _run_until(StreamConsumer(redis), member)
        finally:
            await inject
        assert await redis.exists(settings.dlq_stream) == 0

    async def test_retries_exhausted_to_dlq(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
        fast_backoff: None,
    ) -> None:
        """The upsert never arrives -> bounded retries, then DLQ with
        attempt count; the entry is acked so the stream moves on."""
        await _xadd(redis, stream, "group_changed", {
            "v": 1, "group_key": "practice_42",
            "recipient_id": str(uuid4()), "member": True,
        })

        async def dead_lettered_and_acked() -> bool:
            length: int = await redis.xlen(settings.dlq_stream)
            return length == 1 and await _no_pending(redis, stream)

        await _run_until(StreamConsumer(redis), dead_lettered_and_acked)

        entries = await redis.xrange(settings.dlq_stream)
        fields = {k.decode(): v.decode() for k, v in entries[0][1].items()}
        assert "retries exhausted" in fields["_dlq_error"]
        assert int(fields["_dlq_attempts"]) == 5  # 1 try + 4 fast retries


class TestPoisonPill:
    async def test_garbage_does_not_wedge_the_stream(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
    ) -> None:
        """Item 6b: broken JSON, unknown event and an unsupported
        version each land in the DLQ; the valid event BEHIND them is
        still processed."""
        rid = uuid4()
        await redis.xadd(stream, {"event": "notification_request",
                                  "data": "{broken"})
        await _xadd(redis, stream, "user_deleted", {"v": 1})
        await _xadd(redis, stream, "notification_request",
                    _request_data(v=2))
        await _xadd(redis, stream, "user_upserted", _user_upserted_data(rid))

        async def tail_processed_and_acked() -> bool:
            db_session.expire_all()
            if await db_session.get(Recipient, rid) is None:
                return False
            return await _no_pending(redis, stream)

        await _run_until(StreamConsumer(redis), tail_processed_and_acked)

        assert await redis.xlen(settings.dlq_stream) == 3
        reasons = [
            {k.decode(): v.decode() for k, v in fields.items()}["_dlq_error"]
            for _id, fields in await redis.xrange(settings.dlq_stream)
        ]
        assert any("not valid JSON" in r for r in reasons)
        assert any("unknown event" in r for r in reasons)
        assert any("unsupported schema version" in r for r in reasons)

    async def test_unregistered_type_is_terminal(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
    ) -> None:
        """A ValidationError from the SERVICE layer (unregistered
        type) is terminal too: DLQ, no retry burn."""
        await _xadd(redis, stream, "notification_request",
                    _request_data(type="not_in_profile"))

        async def dead_lettered() -> bool:
            length: int = await redis.xlen(settings.dlq_stream)
            return length == 1

        await _run_until(StreamConsumer(redis), dead_lettered)
        entries = await redis.xrange(settings.dlq_stream)
        fields = {k.decode(): v.decode() for k, v in entries[0][1].items()}
        assert "Unregistered notification type" in fields["_dlq_error"]
        # Original envelope preserved verbatim for re-ingestion; the
        # consumer's diagnostics live under the _dlq_ prefix and can
        # never shadow producer fields (review 3c.1).
        assert fields["event"] == "notification_request"
        assert "not_in_profile" in fields["data"]
        assert fields["_dlq_source_entry_id"]


class TestEntrypoint:
    def test_consumer_refuses_to_start_without_redis_url(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import app.consumer as entrypoint

        monkeypatch.setattr(settings, "redis_url", "")
        with pytest.raises(RuntimeError, match="REDIS_URL"):
            entrypoint.main()


class TestReminderCancel:
    """Phase 6/T1 additive event, e2e over the stream: a reminder is
    a notification_request with a FUTURE scheduled_at; reminder_cancel
    expires the PENDING matches by correlation. Recipients are not
    needed on either path (resolve happens at due time), so the issued
    T1 band 92000-92099 stays untouched here."""

    @staticmethod
    def _reminder_request(
        correlation_value: str, type_: str = "unit_rem_1h",
    ) -> dict[str, Any]:
        from datetime import UTC, datetime, timedelta

        anchor = datetime.now(UTC) + timedelta(hours=2)
        return _request_data(
            type=type_,
            scheduled_at=(anchor - timedelta(hours=1)).isoformat(),
            expiry_at=anchor.isoformat(),
            action_data={"booking_id": correlation_value},
        )

    async def test_cancel_expires_pending_reminders(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
    ) -> None:
        from app.engine.constants import NotificationStatus

        booking_id = str(uuid4())
        first = self._reminder_request(booking_id, "unit_rem_1h")
        second = self._reminder_request(booking_id, "unit_rem_10m")
        bystander = self._reminder_request(str(uuid4()), "unit_rem_1h")
        await _xadd(redis, stream, "notification_request", first)
        await _xadd(redis, stream, "notification_request", second)
        await _xadd(redis, stream, "notification_request", bystander)
        await _xadd(redis, stream, "reminder_cancel", {
            "v": 1,
            "types": ["unit_rem_24h", "unit_rem_1h", "unit_rem_10m"],
            "correlation_key": "booking_id",
            "correlation_value": booking_id,
        })

        async def all_acked() -> bool:
            length: int = await redis.xlen(stream)
            return length == 4 and await _no_pending(redis, stream)

        await _run_until(StreamConsumer(redis), all_acked)

        db_session.expire_all()
        by_key = {}
        for data in (first, second, bystander):
            row = (await db_session.execute(
                select(Notification).where(
                    Notification.idempotency_key
                    == data["idempotency_key"],
                )
            )).scalar_one()
            by_key[data["idempotency_key"]] = row
        assert (
            by_key[first["idempotency_key"]].status
            == NotificationStatus.EXPIRED
        )
        assert (
            by_key[second["idempotency_key"]].status
            == NotificationStatus.EXPIRED
        )
        # A different correlation value stays scheduled.
        assert (
            by_key[bystander["idempotency_key"]].status
            == NotificationStatus.PENDING
        )
        assert await redis.exists(settings.dlq_stream) == 0

    async def test_no_match_cancel_is_acked_not_dead_lettered(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
    ) -> None:
        """A cancel that matches nothing (already expired / never
        scheduled / replay) is the idempotency contract working --
        acked, zero rows, no DLQ."""
        await _xadd(redis, stream, "reminder_cancel", {
            "v": 1,
            "types": ["unit_rem_1h"],
            "correlation_key": "booking_id",
            "correlation_value": str(uuid4()),
        })

        async def acked() -> bool:
            length: int = await redis.xlen(stream)
            return length == 1 and await _no_pending(redis, stream)

        await _run_until(StreamConsumer(redis), acked)
        assert await redis.exists(settings.dlq_stream) == 0
