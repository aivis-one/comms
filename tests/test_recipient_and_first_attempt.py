# =============================================================================
# W6-2a A2 -- the recipient's row against a FIRST attempt of a job
# =============================================================================
#
# A first attempt inserts its job's deliveries; their foreign key takes
# FOR KEY SHARE on each recipient's row until the attempt commits -- the
# whole channel call. W5-3 lifted the wait for a job's row only
# (app/core/database.py wait_for_attempts); a write of the recipient's
# row met the KEY SHARE under the role's own lock_timeout and failed,
# 55P03: the API answered 500, the consumer held the event.
#
#   1 a snapshot (PUT /api/v1/recipients/{id}, user_upserted) takes the
#     row FOR NO KEY UPDATE -- it writes no key -- which KEY SHARE does
#     not block: it passes AT ONCE, the attempt still standing;
#   2 a tombstone (DELETE /api/v1/recipients/{id}, user_deleted) takes
#     the row FOR UPDATE -- the order forgetting stands on
#     (app/forgetting.py WHAT HOLDS THE WINDOW) -- and WAITS for the
#     attempt, as long as its hold: then the attempt's deliveries,
#     committed, are withdrawn;
#   3 past the hold and its margin, the outcome it was: 55P03 -- the API
#     500, the consumer's event held.
#
# The attempt here is another connection that takes the job's row as an
# attempt does (FOR NO KEY UPDATE) and resolves it -- the deliveries'
# INSERT, uncommitted -- for as long as the test says. The limits are
# made small (tests/test_db_limits.py `bounded`): the same code, the
# seconds of a test.
#
# MUTATIONS these tests were written against (each turns one red):
#   W1 the snapshot FOR UPDATE again -> TestTheSnapshot (55P03)
#   W2 wait_for_attempts off the tombstone -> TestTheTombstone (55P03)
#   W3 (unkillable, not built: the wait before the SAVEPOINT or inside it
#      behaves the same -- SET LOCAL is rolled back with the SAVEPOINT and
#      the second run waits again)
# =============================================================================

import asyncio
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from httpx import AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.core.config import settings
from app.core.database import ProcessRole, get_session_factory
from app.engine import service
from app.engine.constants import DeliveryStatus
from app.engine.models import Notification, NotificationDelivery
from app.transport import consumer as consumer_module
from app.transport.consumer import StreamConsumer
from tests.test_db_limits import bounded  # noqa: F401 -- the fixture
from tests.test_transport_consumer import _run_until, _user_upserted_data, _xadd
from tests.test_worker_keeps_its_work import _job

_SLACK = 1.5


async def _target(job_id: UUID) -> UUID:
    async with get_session_factory()() as s:
        job = await s.get(Notification, job_id)
        assert job is not None
        return UUID(str(job.target_value))


async def _first_attempt(
    job_id: UUID, seconds: float, inserted: asyncio.Event,
) -> None:
    """A first attempt of the job, on a connection of its own (the
    worker's, another process): the job's row FOR NO KEY UPDATE, its
    deliveries inserted -- KEY SHARE on the recipient's row -- held
    `seconds`, then committed."""
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as conn:
            session = AsyncSession(bind=conn)
            await session.execute(text(
                "SELECT 1 FROM notifications WHERE id = :id FOR NO KEY UPDATE"
            ), {"id": job_id})
            job = await session.get(Notification, job_id)
            assert job is not None
            await service.resolve_notification(session, job)
            await session.flush()
            inserted.set()
            await asyncio.sleep(seconds)
            await session.commit()
    finally:
        await engine.dispose()


@pytest.fixture
async def attempt() -> AsyncIterator[
    Callable[[float], Awaitable[tuple[UUID, UUID, asyncio.Future[None]]]]
]:
    """Start a first attempt of a fresh job held `seconds`: (job, its
    recipient, the attempt's task), once the deliveries are inserted."""
    tasks: list[asyncio.Future[None]] = []

    async def start(seconds: float) -> tuple[UUID, UUID, asyncio.Future[None]]:
        job_id = await _job()
        recipient = await _target(job_id)
        inserted = asyncio.Event()
        task = asyncio.ensure_future(_first_attempt(job_id, seconds, inserted))
        tasks.append(task)
        await asyncio.wait_for(inserted.wait(), 5)
        return job_id, recipient, task

    yield start
    for task in tasks:
        await asyncio.gather(task, return_exceptions=True)


async def _waiting_on_a_lock() -> int:
    """Backends of this database waiting for a lock now."""
    async with get_session_factory()() as s:
        return int((await s.execute(text(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() AND wait_event_type = 'Lock'"
        ))).scalar_one())


async def _deliveries(job_id: UUID) -> list[str]:
    async with get_session_factory()() as s:
        rows = await s.execute(
            select(NotificationDelivery.status).where(
            NotificationDelivery.notification_id == job_id,
        )
        )
        return [str(r[0]) for r in rows.all()]


async def _recipient(rid: UUID) -> Recipient | None:
    async with get_session_factory()() as s:
        return await s.get(Recipient, rid)


@pytest.fixture
def a_stream(monkeypatch: pytest.MonkeyPatch) -> str:
    name = f"comms:test:{uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", name)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    monkeypatch.setattr(consumer_module, "_BACKOFF_DELAYS", (0.01,))
    return name


def _snapshot(version: int) -> dict[str, Any]:
    return {"version": version, "telegram_id": None, "email": None,
            "locale": "ru", "timezone": None, "active": True}


class TestTheSnapshot:
    async def test_the_api_passes_at_once(
        self, client: AsyncClient, bounded: Callable[..., Awaitable[None]],  # noqa: F811
        attempt: Callable[..., Awaitable[tuple[UUID, UUID, Any]]],
    ) -> None:
        await bounded(ProcessRole.API, statement=0.6, lock=0.2, hold=1.0)
        _, recipient, task = await attempt(1.0)
        started = time.monotonic()
        resp = await client.put(f"/api/v1/recipients/{recipient}", json=_snapshot(2))
        took = time.monotonic() - started
        assert resp.status_code == 200, resp.text
        # At once: the attempt still stands, its KEY SHARE held.
        assert not task.done()
        assert took < 0.2 + 0.3
        await task
        row = await _recipient(recipient)
        assert row is not None and (row.version, row.locale) == (2, "ru")

    async def test_the_consumer_passes_at_once(
        self, a_stream: str, bounded: Callable[..., Awaitable[None]],  # noqa: F811
        attempt: Callable[..., Awaitable[tuple[UUID, UUID, Any]]],
    ) -> None:
        await bounded(ProcessRole.CONSUMER, statement=0.6, lock=0.2, hold=1.5)
        _, recipient, task = await attempt(1.5)
        redis = fakeaioredis.FakeRedis()
        await _xadd(redis, a_stream, "user_upserted", {
            **_user_upserted_data(recipient), "version": 2,
        })
        with capture_logs() as logs:

            async def processed() -> bool:
                return any(e["event"] == "event_processed" for e in logs)

            await _run_until(StreamConsumer(redis), processed)
        assert not task.done()  # before the attempt's commit
        assert "event_held" not in [e["event"] for e in logs]
        await task
        row = await _recipient(recipient)
        assert row is not None and row.version == 2


class TestTheTombstone:
    async def test_the_api_waits_and_withdraws_what_the_attempt_wrote(
        self, client: AsyncClient, bounded: Callable[..., Awaitable[None]],  # noqa: F811
        attempt: Callable[..., Awaitable[tuple[UUID, UUID, Any]]],
    ) -> None:
        """DELETE while the attempt stands: it waits on the recipient's
        row (seen waiting for a lock), past the API's own limits; the
        attempt commits; the deletion then withdraws its deliveries."""
        await bounded(ProcessRole.API, statement=0.3, lock=0.2, hold=1.5)
        job_id, recipient, task = await attempt(0.8)
        deleting = asyncio.ensure_future(client.request(
            "DELETE", f"/api/v1/recipients/{recipient}", json={"version": 2},
        ))
        started = time.monotonic()
        deadline = started + 5
        while await _waiting_on_a_lock() == 0:
            assert time.monotonic() < deadline and not deleting.done()
            await asyncio.sleep(0.02)
        resp = await deleting
        took = time.monotonic() - started
        assert task.done()  # it waited for the attempt's commit
        assert resp.status_code == 200, resp.text
        assert took > 0.3 + 0.2  # past its own limits
        statuses = await _deliveries(job_id)
        # The attempt's deliveries, committed, then withdrawn.
        assert statuses
        assert set(statuses) == {str(DeliveryStatus.RECIPIENT_INACTIVE)}
        row = await _recipient(recipient)
        assert row is not None and row.deleted_at is not None

    async def test_the_consumer_waits_and_forgets(
        self, a_stream: str, bounded: Callable[..., Awaitable[None]],  # noqa: F811
        attempt: Callable[..., Awaitable[tuple[UUID, UUID, Any]]],
    ) -> None:
        await bounded(ProcessRole.CONSUMER, statement=0.3, lock=0.2, hold=1.5)
        job_id, recipient, task = await attempt(0.8)
        redis = fakeaioredis.FakeRedis()
        await _xadd(redis, a_stream, "user_deleted",
                    {"v": 1, "recipient_id": str(recipient), "version": 2})
        with capture_logs() as logs:

            async def processed() -> bool:
                return any(e["event"] == "event_processed" for e in logs)

            await _run_until(StreamConsumer(redis), processed)
        assert task.done()
        said = [e["event"] for e in logs]
        assert "event_held" not in said and "event_retry_scheduled" not in said
        assert set(await _deliveries(job_id)) == {
            str(DeliveryStatus.RECIPIENT_INACTIVE)
        }

    async def test_past_the_hold_the_api_answers_as_it_did(
        self, client: AsyncClient, bounded: Callable[..., Awaitable[None]],  # noqa: F811
        attempt: Callable[..., Awaitable[tuple[UUID, UUID, Any]]],
    ) -> None:
        """The pair: the wait is the hold and its margin, not unbounded."""
        await bounded(ProcessRole.API, statement=0.3, lock=0.2,
                      hold=0.3, margin=0.2)
        _, recipient, task = await attempt(2.0)
        started = time.monotonic()
        resp = await client.request(
            "DELETE", f"/api/v1/recipients/{recipient}", json={"version": 2},
        )
        took = time.monotonic() - started
        assert resp.status_code == 500
        assert not task.done()
        assert 0.5 - 0.1 < took < 0.5 + _SLACK
        await task
        row = await _recipient(recipient)
        assert row is not None and row.deleted_at is None

    async def test_past_the_hold_the_consumer_holds_the_event(
        self, a_stream: str, bounded: Callable[..., Awaitable[None]],  # noqa: F811
        attempt: Callable[..., Awaitable[tuple[UUID, UUID, Any]]],
    ) -> None:
        await bounded(ProcessRole.CONSUMER, statement=0.3, lock=0.2,
                      hold=0.3, margin=0.2)
        _, recipient, task = await attempt(2.0)
        redis = fakeaioredis.FakeRedis()
        await _xadd(redis, a_stream, "user_deleted",
                    {"v": 1, "recipient_id": str(recipient), "version": 2})
        with capture_logs() as logs:

            async def held() -> bool:
                return any(e["event"] == "event_held" for e in logs)

            await _run_until(StreamConsumer(redis), held)
        (line,) = [e for e in logs if e["event"] == "event_held"]
        assert "55P03" in json.dumps(line)
        await task

