# =============================================================================
# A1 -- a job's key lives at least as long as the job may be delivered
# =============================================================================
#
# Retention counted a finished job's age from the later of its creation
# and its scheduled_at. A job whose expiry_at lies further out -- an
# envelope's absolute expiry_at, or a profile's expires_after longer
# than the period -- was deleted while its event could still be
# delivered, and its key with it: a replay of the same event was then a
# new job, a second letter (deploy/INTEGRATION.md section 7). Now the
# age counts from the latest of creation, scheduled_at and expiry_at: a
# job's key is kept until its deadline and the period after it. A job
# without an expiry ages as before; a cancelled one still from its
# creation (REV-8). No request is refused for it.
#
# Through the real intake (handle_event), the real pipeline (in_app)
# and the real retention pass; time passing is the job's instants moved
# back.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the age without expiry_at again      -> TestTheKeyLivesItsDeadline
#   M2  the age on expiry_at alone            -> TestWithoutADeadline
#   M3  a cancelled job aging by its expiry_at too
#                                             -> TestACancelledJob
#   M4  expires_after longer than retention refused, as answer_keep
#                                             -> TestNothingIsRefused
#   M5  "How long" without expiry_at          -> TestTheContract
# =============================================================================

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text

from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import (
    cleanup_terminal_notifications,
    process_pending_notifications,
)
from app.transport.events import parse_event
from app.transport.handlers import HandleResult, handle_event
from tests.helpers import create_recipient
from tests.test_profile_schema import _doc, _parse

_INTEGRATION = Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md"
# in_app, no expiry of its own; and in_app expiring by the profile.
_ENVELOPE_TYPE = "unit_bare"
_PROFILE_TYPE = "unit_expiring"


async def _recipient() -> UUID:
    async with get_session_factory()() as session:
        recipient = await create_recipient(session)
        await session.commit()
        return recipient.id


def _event(type_key: str, recipient: UUID, **extra: Any) -> str:
    data: dict[str, Any] = {
        "v": 1, "idempotency_key": f"a1-{uuid4().hex}", "type": type_key,
        "target_type": "user", "target_value": str(recipient),
        "title": "T", "body": "B", **extra,
    }
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
            .join(Notification).where(Notification.idempotency_key == key)
        )).scalar_one())


async def _delivered(type_key: str, **extra: Any) -> str:
    """A request accepted and delivered into the inbox; its bytes."""
    raw = _event(type_key, await _recipient(), **extra)
    assert await _send(raw) is HandleResult.PROCESSED
    await process_pending_notifications()
    assert await _deliveries(_key(raw)) == 1
    return raw


async def _time_passes(
    key: str, *, aged_days: int, expiry_in: timedelta | None,
    status: str | None = None,
) -> None:
    """The job created and due `aged_days` ago; its expiry_at now +
    `expiry_in` (negative: behind), or left as it is when None."""
    sets = [
        "created_at = now() - make_interval(days => :d)",
        "scheduled_at = now() - make_interval(days => :d)",
    ]
    params: dict[str, Any] = {"k": key, "d": aged_days}
    if expiry_in is not None:
        sets.append("expiry_at = now() + make_interval(secs => :e)")
        params["e"] = expiry_in.total_seconds()
    if status is not None:
        sets.append("status = :s")
        params["s"] = status
    async with get_session_factory()() as session:
        await session.execute(text(
            f"UPDATE notifications SET {', '.join(sets)} "
            "WHERE idempotency_key = :k"
        ), params)
        await session.commit()


def _past_the_period() -> int:
    return settings.notification_retention_days + 1


class TestTheKeyLivesItsDeadline:
    """Finding A1: a job older than the period by its creation, whose
    deadline is still ahead -- or behind it by less than the period --
    is kept with its key; a replay of the same event is the same job,
    no second letter. M1."""

    @pytest.mark.parametrize(
        ("type_key", "expiry_in"),
        [
            (_ENVELOPE_TYPE, timedelta(days=100)),
            (_PROFILE_TYPE, timedelta(days=1)),
            (_PROFILE_TYPE, -timedelta(days=10)),
        ],
        ids=["envelope-ahead", "profile-ahead", "profile-behind-within"],
    )
    async def test_a_replay_is_the_same_job(
        self, type_key: str, expiry_in: timedelta,
    ) -> None:
        extra: dict[str, Any] = {}
        if type_key == _ENVELOPE_TYPE:
            extra["expiry_at"] = (datetime.now(UTC) + timedelta(days=200)).isoformat()
        raw = await _delivered(type_key, **extra)
        key = _key(raw)
        [job] = await _jobs(key)
        await _time_passes(key, aged_days=_past_the_period(), expiry_in=expiry_in)

        assert await cleanup_terminal_notifications() == 0
        # REPEAT: twice -- the same job, one delivery.
        assert await _send(raw) is HandleResult.DUPLICATE
        assert await _send(raw) is HandleResult.DUPLICATE
        await process_pending_notifications()
        assert await _jobs(key) == [job]
        assert await _deliveries(key) == 1

    async def test_the_period_past_the_deadline_releases_it(self) -> None:
        """SHORTFALL: the deadline behind by more than the period -- the
        job goes with its key, as the contract says; the pair of the
        class above, the job is not simply kept for ever."""
        raw = await _delivered(_PROFILE_TYPE)
        key = _key(raw)
        await _time_passes(
            key, aged_days=_past_the_period() + 30,
            expiry_in=-timedelta(days=_past_the_period()),
        )
        assert await cleanup_terminal_notifications() == 1
        assert await _jobs(key) == []


class TestWithoutADeadline:
    """Requirement 2, the pair: a job with no expiry_at ages as before
    -- older than the period by its creation, it is deleted. M2."""

    async def test_it_goes_as_before(self) -> None:
        raw = await _delivered(_ENVELOPE_TYPE)
        key = _key(raw)
        async with get_session_factory()() as session:
            expiry = (await session.execute(
                select(Notification.expiry_at)
                .where(Notification.idempotency_key == key)
            )).scalar_one()
        assert expiry is None, "the pair's premise: no deadline"
        await _time_passes(key, aged_days=_past_the_period(), expiry_in=None)
        assert await cleanup_terminal_notifications() == 1
        assert await _jobs(key) == []


class TestACancelledJob:
    """A cancelled job ages from its creation, whatever its deadline
    (REV-8, the contract): its far expiry_at does not keep it. M3."""

    async def test_it_goes_by_its_creation(self) -> None:
        raw = await _delivered(
            _ENVELOPE_TYPE,
            expiry_at=(datetime.now(UTC) + timedelta(days=400)).isoformat(),
        )
        key = _key(raw)
        await _time_passes(
            key, aged_days=_past_the_period(), expiry_in=timedelta(days=300),
            status="cancelled",
        )
        assert await cleanup_terminal_notifications() == 1
        assert await _jobs(key) == []


class TestNothingIsRefused:
    """Requirement 3: the rule is in retention, not at intake -- a
    profile's expires_after at the ceiling loads, and an envelope's
    expiry_at far past the period is accepted. M4."""

    def test_the_longest_expires_after_loads(self) -> None:
        record = _parse(_doc({"t": {"expires_after": "3650d"}})).types["t"]
        assert record.value("expires_after") == timedelta(days=3650)
        assert settings.notification_retention_days < 3650, "the pair"

    async def test_a_far_envelope_expiry_is_accepted(self) -> None:
        far = datetime.now(UTC) + timedelta(days=3000)
        raw = _event(_ENVELOPE_TYPE, await _recipient(), expiry_at=far.isoformat())
        assert await _send(raw) is HandleResult.PROCESSED
        assert len(await _jobs(_key(raw))) == 1


class TestTheContract:
    """"How long comms keeps things" says the rule -- the latest of
    creation, scheduled_at and expiry_at; a cancelled job from its
    creation -- and the tests above hold the code to it. M5."""

    def test_the_history_bullet_names_the_three(self) -> None:
        doc = _INTEGRATION.read_text(encoding="utf-8")
        section = doc[doc.index("\n## How long comms keeps things"):]
        lead = "- **Notification history**"
        bullet = section[section.index(lead):]
        bullet = bullet[:bullet.index("\n- **")]
        flat = " ".join(bullet.split())
        assert "the latest of its creation, its `scheduled_at` and its `expiry_at`" \
            in flat
        assert "a `cancelled` one, counted from its creation" in flat
