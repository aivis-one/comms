# =============================================================================
# H7 R-B -- a job is not deleted with an unpublished push
# =============================================================================
#
# A finished job of a type with push_on, past its expiry_at and past the
# retention period, whose push the relay could not publish (redis down),
# stays until the push goes: the cascade would delete the push_outbox
# row with the job, and the product would lose both the push and,
# reading by its key, the result (deploy/INTEGRATION.md section 10).
# Through the real pipeline, the real relay against a fakeredis server
# that is down and then back, the real maintenance and retention passes.
#
# Until R-1 the deleter here was the expiry cleanup, which took a
# finished job as soon as its expiry_at passed; R-1 removed it
# (tests/test_kept_until_retention.py), and the push is held the same
# way by retention, the one deleter left (app/engine/service.py
# delete_terminal_notifications_batch, REV-2). The cleanup's plan test
# went with the cleanup; retention's select has no index of its own
# (KNOWN CEILING BL-3).
#
# MUTATIONS these tests were written against (each turns one red):
#   M3  the outbox condition dropped from retention's batch select
#                                         -> TestThePushWaits
# =============================================================================

from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from fakeredis import FakeServer
from fakeredis import aioredis as fakeaioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import func, select, text

import app.transport.push_relay as relay
from app.core.config import settings
from app.core.database import get_session_factory
from app.core.lanes import Lane
from app.engine.constants import NotificationStatus, TargetType
from app.engine.models import Notification, PushOutbox
from app.engine.processor import cleanup_terminal_notifications, process_lane
from app.engine.service import create_notification
from app.engine.worker import run_engine_maintenance
from tests.helpers import create_recipient, intake_fields

_PUSHED = "unit_routed"  # push_on: outcome, lane interactive (fixture)
_SILENT = "unit_event_in_app"  # push_on: none, lane normal


class _Accepts:
    async def deliver(self, *args: Any) -> bool:
        return True


@pytest.fixture(autouse=True)
def stream(monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setattr(
        settings, "comms_events_stream", f"comms:test:{uuid4().hex[:8]}",
    )
    return settings.changes_stream


async def _finished(type_key: str, lane: Lane) -> UUID:
    """A job of `type_key` taken to its outcome by the real pipeline,
    then put past its expiry and past the retention period."""
    async with get_session_factory()() as session:
        recipient = await create_recipient(session)
        job = await create_notification(
            session, **intake_fields(), type=type_key, title="T", body="B",
            target_type=TargetType.USER, target_value=str(recipient.id),
        )
        await session.commit()
        nid = job.id
    with patch("app.engine.service.get_formatter", return_value=_Accepts()):
        await process_lane(lane)
    async with get_session_factory()() as session:
        await session.execute(text(
            # The deadline past by the period too: since A1 retention
            # counts from the latest of creation, scheduled_at and
            # expiry_at (until A1 a minute behind was enough).
            "UPDATE notifications SET "
            "expiry_at = now() - make_interval(days => :d) + interval '1 hour', "
            "created_at = now() - make_interval(days => :d), "
            "scheduled_at = now() - make_interval(days => :d) WHERE id = :i"
        ), {"i": nid, "d": settings.notification_retention_days + 1})
        await session.commit()
    return nid


async def _exists(nid: UUID) -> bool:
    async with get_session_factory()() as session:
        return await session.get(Notification, nid) is not None


async def _owed(nid: UUID) -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(
            select(func.count()).select_from(PushOutbox)
            .where(PushOutbox.notification_id == nid)
        )).scalar_one())


async def _passes() -> int:
    """Every pass that may delete a job: the maintenance loop's engine
    share, then retention. Returns what retention deleted."""
    await run_engine_maintenance()
    return await cleanup_terminal_notifications()


class TestThePushWaits:
    """done-when R-B (1), (2), on retention since R-1. M3."""

    async def test_kept_while_owed_deleted_once_published(self) -> None:
        server = FakeServer()
        redis = fakeaioredis.FakeRedis(server=server)
        nid = await _finished(_PUSHED, Lane.INTERACTIVE)
        async with get_session_factory()() as session:
            job = await session.get(Notification, nid)
            assert job is not None and job.status == NotificationStatus.SENT
            key = job.idempotency_key
        assert await _owed(nid) == 1

        server.connected = False  # redis is down: the relay cannot publish
        with pytest.raises(RedisConnectionError):
            await relay.relay_once(redis)
        assert await _owed(nid) == 1
        assert await _passes() == 0
        assert await _exists(nid)

        server.connected = True  # back: the relay publishes and deletes
        assert await relay.relay_once(redis) == 1
        entries = await redis.xrange(settings.changes_stream)
        assert [e[1][b"idempotency_key"].decode() for e in entries] == [key]
        assert await _owed(nid) == 0
        assert await _passes() == 1
        assert not await _exists(nid)

    async def test_a_job_with_no_push_goes_as_before(self) -> None:
        """The pair: nothing owed -- retention deletes it at once."""
        nid = await _finished(_SILENT, Lane.NORMAL)
        assert await _owed(nid) == 0
        assert await _passes() == 1
        assert not await _exists(nid)

    async def test_two_pushes_owed_both_must_go(self) -> None:
        """REPEAT: two rows owed for one job -- kept while either is."""
        nid = await _finished(_PUSHED, Lane.INTERACTIVE)
        async with get_session_factory()() as session:
            session.add(PushOutbox(notification_id=nid))
            await session.commit()
            rows = list((await session.execute(
                select(PushOutbox.id).where(PushOutbox.notification_id == nid)
                .order_by(PushOutbox.id)
            )).scalars())
        async with get_session_factory()() as session:
            assert await relay.delete_published(session, rows[:1]) == 1
            await session.commit()
        assert await _passes() == 0
        async with get_session_factory()() as session:
            assert await relay.delete_published(session, rows[1:]) == 1
            await session.commit()
        assert await _passes() == 1
