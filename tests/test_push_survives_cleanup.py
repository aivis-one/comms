# =============================================================================
# H7 R-B -- the expiry cleanup does not take an unpublished push with it
# =============================================================================
#
# A finished job of a type with push_on, past its expiry_at, whose push
# the relay could not publish (redis down), stays until the push goes:
# the cascade would delete the push_outbox row with the job, and the
# product would lose both the push and, reading by its key, the result
# (deploy/INTEGRATION.md section 10). Through the real pipeline, the
# real relay against a fakeredis server that is down and then back.
#
# MUTATIONS these tests were written against (each turns one red):
#   M4  the outbox condition dropped from the cleanup's batch select
#                                         -> TestThePushWaits, TestThePlan
# TRIED, SURVIVES, NOT A DEFECT: the condition as NOT IN over the outbox
# -- the planner keeps the partial index and scans no notifications
# either way; the form is not pinned, the plan is.
# =============================================================================

from datetime import UTC, datetime, timedelta
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
from app.engine.processor import cleanup_expired_notifications, process_lane
from app.engine.service import (
    create_notification,
    delete_expired_notifications_batch,
)
from tests.helpers import create_recipient, intake_fields
from tests.test_expired_cleanup_index import (
    _index_names,
    _nodes,
    _plan_of,
    _seed,
    _seq_scans_of_notifications,
)

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
    then put past its expiry."""
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
            "UPDATE notifications SET expiry_at = now() - interval '1 minute' "
            "WHERE id = :i"
        ), {"i": nid})
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


class TestThePushWaits:
    """done-when R-B (1), (2). M4."""

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
        assert await cleanup_expired_notifications() == 0
        assert await _exists(nid)

        server.connected = True  # back: the relay publishes and deletes
        assert await relay.relay_once(redis) == 1
        entries = await redis.xrange(settings.changes_stream)
        assert [e[1][b"idempotency_key"].decode() for e in entries] == [key]
        assert await _owed(nid) == 0
        assert await cleanup_expired_notifications() == 1
        assert not await _exists(nid)

    async def test_a_job_with_no_push_goes_as_before(self) -> None:
        """The pair: nothing owed -- the cleanup deletes it at once."""
        nid = await _finished(_SILENT, Lane.NORMAL)
        assert await _owed(nid) == 0
        assert await cleanup_expired_notifications() == 1
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
        assert await cleanup_expired_notifications() == 0
        async with get_session_factory()() as session:
            assert await relay.delete_published(session, rows[1:]) == 1
            await session.commit()
        assert await cleanup_expired_notifications() == 1


class TestThePlan:
    """done-when R-B (3): the batch select still runs on the partial
    index of H5 and scans nothing of notifications outside it -- the
    statement the cleanup sends, captured and explained, with a backlog
    of owed pushes (redis was down), the state the condition is for.
    Measured while building H7: with that backlog the planner hashes the
    outbox and sorts the expired candidates it found on the partial
    index; with the outbox empty it walks the index in expiry order as
    in H5 (tests/test_expired_cleanup_index.py, unchanged). Either way
    the table itself is not scanned. M4 (the pair: the outbox in the
    plan)."""

    async def test_the_partial_index_with_pushes_owed(self) -> None:
        await _seed(total=50000, every=200)
        async with get_session_factory()() as session:
            await session.execute(text(
                "INSERT INTO push_outbox (notification_id) "
                "SELECT id FROM notifications WHERE expiry_at < now() "
                "AND status = 'sent' LIMIT 500"
            ))
            await session.commit()
            await session.execute(text("ANALYZE push_outbox"))
            await session.commit()
        plan = await _plan_of(
            delete_expired_notifications_batch,
            now=datetime.now(UTC) + timedelta(seconds=1), limit=1000,
        )
        nodes = _nodes(plan)
        names = _index_names(nodes)
        assert "ix_notifications_expired_deletable" in names
        assert _seq_scans_of_notifications(nodes) == []
        # The pair: the outbox is in the plan -- the condition is there.
        assert any(n.get("Relation Name") == "push_outbox" for n in nodes)
