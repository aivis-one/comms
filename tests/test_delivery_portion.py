# =============================================================================
# H7 R-F -- an attempt delivers one portion; the lane wakes itself for
# the next
# =============================================================================
#
# One round of the lane's slots per attempt (app/engine/service.py
# delivery_portion), committed; the job goes on in the lane's next pass,
# which a pass with a full portion starts at once (app/worker.py
# _lane_pass -> the lane's wake). Through the real pipeline and the real
# lane loop; bulk -- one slot, a portion of one -- throughout.
#
# MUTATIONS these tests were written against (each turns one red):
#   M6  the portion dropped (every due delivery in one attempt)
#                                   -> TestTheFeedIsNotHeld, TestPortions
#   M7  no self-wake: the next portion waits the interval
#                                   -> TestNoSleepBetweenPortions
#   M8  the job folded while deliveries still wait -> TestPortions
#   M9  the accepted answers read for none of the portion
#                                   -> TestAcceptedAcrossTheBoundary
# =============================================================================

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest
from sqlalchemy import func, select

import app.worker as worker
from app.core.config import settings
from app.core.database import get_session_factory
from app.engine import service
from app.engine.changes import list_changes
from app.engine.constants import (
    DeliveryStatus,
    JournalSubject,
    NotificationStatus,
    TargetType,
)
from app.engine.models import (
    Notification,
    NotificationDelivery,
    NotificationTransition,
)
from app.engine.processor import process_lane
from app.engine.service import create_notification
from app.profile.registry import Decided, Lane, Layer, TypeRecord, registry
from tests.helpers import create_recipient, intake_fields

_TYPE = "unit_event_in_app"
_BOUND = 10.0


def _bulk(type_key: str = _TYPE) -> None:
    record = registry.record_of(type_key)
    assert record is not None
    fields = dict(record.fields)
    fields["lane"] = Decided(Lane.BULK.value, Layer.PROFILE, "test")
    registry.register_type(
        type_key, category=registry.category_of(type_key),
        record=TypeRecord(fields=fields),
    )


async def _send(recipients: int) -> UUID:
    """One bulk job to `recipients` people (target: all)."""
    _bulk()
    async with get_session_factory()() as session:
        for _ in range(recipients):
            await create_recipient(session)
        job = await create_notification(
            session, **intake_fields(), type=_TYPE, title="T", body="B",
            target_type=TargetType.ALL, target_value="*",
        )
        await session.commit()
        return job.id


async def _one(type_key: str = "unit_event_in_app_email") -> UUID:
    """A second bulk job, to the first recipient only."""
    _bulk(type_key)
    async with get_session_factory()() as session:
        someone = (await session.execute(
            select(NotificationDelivery.recipient_id).limit(1)
        )).scalar_one()
        job = await create_notification(
            session, **intake_fields(), type=type_key, title="T", body="B",
            target_type=TargetType.USER, target_value=str(someone),
        )
        await session.commit()
        return job.id


async def _statuses(nid: UUID) -> tuple[str, list[str]]:
    async with get_session_factory()() as session:
        job = await session.get(Notification, nid)
        assert job is not None
        deliveries = list((await session.execute(
            select(NotificationDelivery.status)
            .where(NotificationDelivery.notification_id == nid)
            .order_by(NotificationDelivery.id)
        )).scalars())
        return job.status, deliveries


async def _job_rows(nid: UUID) -> list[str]:
    async with get_session_factory()() as session:
        return list((await session.execute(
            select(NotificationTransition.outcome)
            .where(
                NotificationTransition.notification_id == nid,
                NotificationTransition.subject == JournalSubject.JOB,
            )
            .order_by(NotificationTransition.id)
        )).scalars())


class _Spy:
    """Every channel; records (job, recipient) per call, optionally
    stopping the n-th call on an event."""

    def __init__(self, seconds: float = 0.0) -> None:
        self.seconds = seconds
        self.calls: list[tuple[UUID, UUID]] = []
        self.stop_at: dict[int, asyncio.Event] = {}
        self.reached: dict[int, asyncio.Event] = {}

    def stop(self, n: int) -> tuple[asyncio.Event, asyncio.Event]:
        self.stop_at[n] = asyncio.Event()
        self.reached[n] = asyncio.Event()
        return self.reached[n], self.stop_at[n]

    async def deliver(self, notification: Any, delivery: Any, *rest: Any) -> bool:
        n = len(self.calls) + 1
        self.calls.append((notification.id, delivery.recipient_id))
        if n in self.stop_at:
            self.reached[n].set()
            await self.stop_at[n].wait()
        await asyncio.sleep(self.seconds)
        return True


@asynccontextmanager
async def _channel(spy: _Spy) -> AsyncIterator[_Spy]:
    with patch("app.engine.service.get_formatter", return_value=spy):
        yield spy


class TestPortions:
    """done-when R-F (1): N > portion in a one-slot lane -- each pass
    commits one portion, the next pass of the lane goes on, the job is
    folded only after the last. M6, M8."""

    async def test_three_people_three_passes_one_fold(self) -> None:
        assert service.delivery_portion(Lane.BULK) == 1
        nid = await _send(3)
        woken: list[int] = []
        async with _channel(_Spy()):
            for n in (1, 2):
                passed = await process_lane(
                    Lane.BULK, on_more=lambda: woken.append(1),
                )
                assert passed == 1
                status, deliveries = await _statuses(nid)
                assert status == NotificationStatus.PROCESSING
                assert deliveries.count(DeliveryStatus.SENT) == n
                assert deliveries.count(DeliveryStatus.PENDING) == 3 - n
            await process_lane(Lane.BULK, on_more=lambda: woken.append(1))
        status, deliveries = await _statuses(nid)
        assert (status, deliveries) == (
            NotificationStatus.SENT, [DeliveryStatus.SENT] * 3,
        )
        # Every pass had a full portion (the third: one left, one taken).
        assert woken == [1, 1, 1]
        # One fold to the outcome, at the end -- none per portion.
        assert (await _job_rows(nid)).count(NotificationStatus.SENT) == 1

    async def test_fewer_than_a_portion_one_pass(self) -> None:
        """SHORTAGE: one person, a portion of one -- one pass, folded."""
        nid = await _send(1)
        async with _channel(_Spy()):
            await process_lane(Lane.BULK)
        assert (await _statuses(nid))[0] == NotificationStatus.SENT

    async def test_a_normal_portion_is_its_twenty_slots(self) -> None:
        assert {lane: service.delivery_portion(lane) for lane in Lane} == (
            dict(service.LANE_CONCURRENCY)
        )


class TestTheFeedIsNotHeld:
    """done-when R-F (2): while a send is under way, another job's write
    shows in the change feed. The feed hands out rows below the reading
    snapshot's xmin (app/engine/changes.py, P2-3): one attempt over the
    whole audience began before that write and held it back to the
    end; a portion's attempt begins after it. M6."""

    async def test_a_write_between_two_portions_is_read(self) -> None:
        nid = await _send(2)
        spy = _Spy()
        first_reached, first_go = spy.stop(1)
        second_reached, second_go = spy.stop(2)

        async def rollout() -> None:
            await process_lane(Lane.BULK)
            await process_lane(Lane.BULK)

        async with _channel(spy):
            task = asyncio.ensure_future(rollout())
            await asyncio.wait_for(first_reached.wait(), _BOUND)
            # Another job written while the first portion is in flight.
            async with get_session_factory()() as session:
                other = await create_notification(
                    session, **intake_fields(), type="unit_event",
                    title="T", body="B",
                    target_type=TargetType.ALL, target_value="*",
                )
                await session.commit()
                other_key = other.idempotency_key
            first_go.set()
            # The second portion is in flight -- the send is not over.
            await asyncio.wait_for(second_reached.wait(), _BOUND)
            async with get_session_factory()() as session:
                page = await list_changes(
                    session, limit=100, position=None, read_at=None,
                )
            second_go.set()
            await asyncio.wait_for(task, _BOUND)
        assert other_key in page.keys
        assert (await _statuses(nid))[0] == NotificationStatus.SENT


class TestNoSleepBetweenPortions:
    """правка 1 (gate): the real lane loop with an interval far longer
    than the send -- the send ends in about N calls, not N intervals,
    and a job of the same lane accepted mid-send goes before the send
    ends. M7."""

    async def test_the_lane_wakes_itself(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        interval, call, people = 5, 0.1, 6
        monkeypatch.setattr(settings, "notification_poll_interval_seconds", interval)
        monkeypatch.setattr(settings, "notification_max_backoff_seconds", interval)
        nid = await _send(people)
        spy = _Spy(seconds=call)
        third, go = spy.stop(3)
        wake = asyncio.Event()
        started = time.monotonic()
        async with _channel(spy):
            loop = asyncio.ensure_future(
                worker._poll_loop(worker._lane_pass(Lane.BULK, wake), wake),
            )
            try:
                await asyncio.wait_for(third.wait(), _BOUND)
                late = await _one()
                go.set()
                for _ in range(int(_BOUND / 0.05)):
                    if (await _statuses(nid))[0] == NotificationStatus.SENT:
                        break
                    await asyncio.sleep(0.05)
                took = time.monotonic() - started
            finally:
                loop.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await loop
        assert (await _statuses(nid))[0] == NotificationStatus.SENT
        # Explicit bound: well under ONE interval for six portions;
        # a sleep between portions would cost five intervals.
        assert took < interval
        # The late job's calls come before the send's last one.
        jobs = [job for job, _ in spy.calls]
        assert late in jobs
        assert jobs.index(late) < len(jobs) - 1 - jobs[::-1].index(nid)


class TestAcceptedAcrossTheBoundary:
    """done-when R-F (3): the channel accepted a letter and the attempt
    tore before its commit; the next pass's portion closes that delivery
    SENT from the journal with no second call (P2-1), and the job's
    other delivery goes in the pass after. M9."""

    async def test_no_second_send(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "notification_retry_backoff_base_seconds", 0)
        nid = await _send(2)
        # Resolved and committed first: the torn attempt must leave the
        # two deliveries behind, as a later portion's would.
        async with get_session_factory()() as session:
            job = await session.get(Notification, nid)
            assert job is not None
            await service.resolve_notification(session, job)
            await session.commit()
        spy = _Spy()
        real = service.rollup_notification
        torn = False

        async def tear_once(*args: Any, **kwargs: Any) -> None:
            nonlocal torn
            if not torn:
                torn = True
                raise RuntimeError("torn after the channel accepted")
            await real(*args, **kwargs)

        async with _channel(spy):
            with patch("app.engine.processor.rollup_notification", tear_once):
                await process_lane(Lane.BULK)  # call 1, accepted, torn
            first = spy.calls[0]
            assert (await _statuses(nid))[1] == [DeliveryStatus.PENDING] * 2
            await process_lane(Lane.BULK)  # closes the accepted one, no call
            assert len(spy.calls) == 1
            await process_lane(Lane.BULK)  # the other person
        assert len(spy.calls) == 2
        assert spy.calls[1] != first
        assert await _statuses(nid) == (
            NotificationStatus.SENT, [DeliveryStatus.SENT] * 2,
        )
        async with get_session_factory()() as session:
            sent_without_call = (await session.execute(
                select(func.count()).select_from(NotificationDelivery)
                .where(
                    NotificationDelivery.notification_id == nid,
                    NotificationDelivery.recipient_id == first[1],
                    NotificationDelivery.attempts == 1,
                )
            )).scalar_one()
        assert sent_without_call == 1
