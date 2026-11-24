# =============================================================================
# R-1 -- a finished job lives until retention, whatever its expiry_at
# =============================================================================
#
# deploy/INTEGRATION.md section 7: a job that finished, expired or was
# cancelled keeps its key, and the key is released only when retention
# removes the job ("How long comms keeps things"). expiry_at bounds when
# a job may be DELIVERED (the expiry sweep, app/engine/processor.py
# expire_overdue_notifications), not how long it is KEPT.
#
# Until R-1 a pass of the maintenance loop deleted a finished job as soon
# as its expiry_at passed: read by its key it answered 404, the
# recipient's inbox was empty, and a replay of the same event -- whose
# expiry, from the profile's expires_after, counts from the replay --
# was accepted as a new job: a second letter. Here through the real
# intake (handle_event), the real pipeline (in_app into the inbox), the
# real maintenance pass and the real retention pass.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  run_engine_maintenance deletes a delivered job past its
#       expiry_at again                    -> TestTheJobStays
#   M2  retention takes a job past its expiry_at before its age
#                                          -> TestTheJobStays
#   M5  retention ages a job from expiry_at, not from _retention_age
#                                          -> TestTheKeyIsReleasedByRetention
# =============================================================================

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text

from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import (
    cleanup_terminal_notifications,
    process_pending_notifications,
)
from app.engine.worker import run_engine_maintenance
from app.transport.events import parse_event
from app.transport.handlers import HandleResult, handle_event
from tests.helpers import create_recipient

_BY_KEY = "/api/v1/notifications/by-key"

# in_app only (the inbox is the recipient's side); the first carries no
# expiry of its own -- the envelope's expiry_at is the one -- the second
# expires by the profile (expires_after: 1h, from scheduled_at).
_ENVELOPE_TYPE = "unit_bare"
_PROFILE_TYPE = "unit_expiring"


async def _recipient() -> UUID:
    async with get_session_factory()() as session:
        recipient = await create_recipient(session)
        await session.commit()
        return recipient.id


def _event(type_key: str, recipient: UUID, **extra: Any) -> str:
    """One notification_request, serialized ONCE: a replay is the same
    bytes, as a product's outbox sends them again."""
    data: dict[str, Any] = {
        "v": 1,
        "idempotency_key": f"r1-{uuid4().hex}",
        "type": type_key,
        "target_type": "user",
        "target_value": str(recipient),
        "title": "T",
        "body": "B",
    }
    data.update(extra)
    return json.dumps(data)


async def _send(raw: str) -> HandleResult:
    entry = {"event": "notification_request", "data": raw}
    async with get_session_factory()() as session:
        result = await handle_event(session, parse_event(entry))
        await session.commit()
        return result


def _key(raw: str) -> str:
    key: str = json.loads(raw)["idempotency_key"]
    return key


async def _jobs(key: str) -> list[UUID]:
    async with get_session_factory()() as session:
        return list((await session.execute(
            select(Notification.id).where(Notification.idempotency_key == key)
        )).scalars())


async def _deliveries(key: str) -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(
            select(func.count()).select_from(NotificationDelivery)
            .join(Notification)
            .where(Notification.idempotency_key == key)
        )).scalar_one())


async def _past_its_expiry(key: str) -> None:
    """Time passes: the job's expiry_at is behind it."""
    async with get_session_factory()() as session:
        await session.execute(text(
            "UPDATE notifications SET expiry_at = now() - interval '1 minute' "
            "WHERE idempotency_key = :k"
        ), {"k": key})
        await session.commit()


async def _past_retention(key: str) -> None:
    """More time passes: the job is older than the retention period,
    counted from the later of its creation and scheduled_at."""
    days = settings.notification_retention_days + 1
    async with get_session_factory()() as session:
        await session.execute(text(
            "UPDATE notifications SET "
            "created_at = now() - make_interval(days => :d), "
            "scheduled_at = now() - make_interval(days => :d) "
            "WHERE idempotency_key = :k"
        ), {"k": key, "d": days})
        await session.commit()


async def _maintenance() -> None:
    """Every pass that may delete: the maintenance loop's engine share
    (twice -- a REPEAT) and the retention pass."""
    await run_engine_maintenance()
    await run_engine_maintenance()
    await cleanup_terminal_notifications()


async def _delivered(type_key: str, **extra: Any) -> tuple[str, UUID, UUID]:
    """A request accepted, delivered into the inbox, then past its
    expiry. Returns the event's bytes, the job and the recipient."""
    recipient = await _recipient()
    raw = _event(type_key, recipient, **extra)
    assert await _send(raw) is HandleResult.PROCESSED
    await process_pending_notifications()
    key = _key(raw)
    [job] = await _jobs(key)
    assert await _deliveries(key) == 1
    await _past_its_expiry(key)
    return raw, job, recipient


def _envelope_expiry() -> str:
    return (datetime.now(UTC) + timedelta(seconds=25)).isoformat()


class TestTheJobStays:
    """Finding 2.1, requirement 4: delivered, past its expiry_at, the
    maintenance passes run -- read by its key 200, the inbox holds it, a
    replay of the same event is the same job, no second letter. M1, M2."""

    @pytest.mark.parametrize(
        "type_key", [_ENVELOPE_TYPE, _PROFILE_TYPE], ids=["envelope", "profile"],
    )
    async def test_read_by_key_the_inbox_and_a_replay(
        self, client: AsyncClient, type_key: str,
    ) -> None:
        extra = {"expiry_at": _envelope_expiry()} if type_key == _ENVELOPE_TYPE else {}
        raw, job, recipient = await _delivered(type_key, **extra)
        key = _key(raw)

        await _maintenance()

        response = await client.get(_BY_KEY, params={"key": key})
        assert response.status_code == 200
        assert response.json()["job"]["id"] == str(job)
        inbox = await client.get(f"/api/v1/recipients/{recipient}/inbox")
        assert inbox.status_code == 200
        assert len(inbox.json()["items"]) == 1

        # REPEAT: the same bytes twice more -- the same job each time.
        assert await _send(raw) is HandleResult.DUPLICATE
        assert await _send(raw) is HandleResult.DUPLICATE
        await process_pending_notifications()
        assert await _jobs(key) == [job]
        assert await _deliveries(key) == 1

    async def test_retention_off_keeps_it(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """SHORTFALL: retention disabled -- nothing deletes the job."""
        monkeypatch.setattr(settings, "notification_retention_days", 0)
        raw, job, _ = await _delivered(
            _ENVELOPE_TYPE, expiry_at=_envelope_expiry(),
        )
        await _maintenance()
        assert await _jobs(_key(raw)) == [job]


class TestTheKeyIsReleasedByRetention:
    """The key's other end: retention removes the job, and only then is
    the same event a new request (section 7). The pair of the class
    above -- the job is not simply never deleted. M5."""

    async def test_past_retention_the_replay_is_a_new_job(self) -> None:
        raw, job, _ = await _delivered(
            _ENVELOPE_TYPE, expiry_at=_envelope_expiry(),
        )
        key = _key(raw)
        await _maintenance()
        assert await _jobs(key) == [job]

        await _past_retention(key)
        assert await cleanup_terminal_notifications() == 1
        assert await _jobs(key) == []

        # EMPTY: the key answers nothing; the same event is accepted anew.
        assert await _send(raw) is HandleResult.PROCESSED
        [again] = await _jobs(key)
        assert again != job
