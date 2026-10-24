# =============================================================================
# G1 item 1 -- forgetting a recipient while an attempt is in flight
# =============================================================================
#
# The question: a job's attempt has resolved its recipients and stands
# in the channel call, its deliveries not yet committed; the product
# forgets one of them. Does the letter reach the forgotten person, and
# is anything of theirs left behind?
#
# It does not reach them after they are forgotten, and nothing is left.
# What holds the window (app/forgetting.py, WHAT HOLDS THE WINDOW):
#   - the FIRST attempt of a job inserts its deliveries; their foreign
#     key takes FOR KEY SHARE on the recipient row until the attempt
#     commits, and the tombstone takes that row FOR UPDATE -- the two
#     conflict, so forgetting waits for the attempt;
#   - a tombstone committed between the attempt's choice of ids and that
#     insert is seen by the late activity gate of deliver_notification,
#     which reads the recipient fresh (resolve selects ids only, so no
#     stale Recipient sits in the attempt's identity map);
#   - a LATER attempt inserts no delivery: the tombstone passes, and
#     withdraw_recipient waits on the job row (FOR UPDATE against the
#     attempt's FOR NO KEY UPDATE);
#   - the channel's answer is journaled from a second session, and the
#     journal's recipient_id carries no foreign key, so that session
#     takes no lock on the recipient and cannot close a cycle.
# withdraw_recipient ALONE, during the first attempt, returns 0 and
# waits for nothing: it sees no committed delivery. Only forget_recipient
# is the path, and its tombstone is what waits.
#
# Real sessions; a wait is read from pg_stat_activity, never timed.
#
# MUTATIONS (one run, every one red here):
#   M1 the tombstone without FOR UPDATE   -> test_forgetting_waits_for_
#                                            the_first_attempt
#   M2 the late activity gate removed     -> test_a_tombstone_between_
#                                            the_ids_and_the_insert
#   M3 the attempt loads Recipient rows   -> the same
#      before resolve_targets and keeps
#      them referenced (a stale identity
#      map)
# NOT BUILT: "the journal gains a foreign key to recipients" -- a schema
# change, not a mutation of code; it is named in app/forgetting.py as
# what the absence of a cycle stands on.
# =============================================================================

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select, text

import app.engine.processor as processor
import app.engine.service as service_module
from app.audience.models import Recipient
from app.core.database import get_session_factory
from app.engine.constants import DeliveryStatus, TargetType
from app.engine.formatters import RateLimitedError
from app.engine.models import NotificationDelivery, NotificationTransition
from app.engine.service import create_notification, withdraw_recipient
from app.forgetting import forget_recipient
from tests.helpers import create_recipient, intake_fields, sql_shape

_BOUND = 10.0
# The provider's words, quoting the address being forgotten. A sentinel:
# not a substring of anything comms writes itself.
_WORDS = " -- provider: mailbox gnu-7713@example.test is over quota"


class _Channel:
    """A channel whose call stands until released, counting calls."""

    def __init__(self, answer: str) -> None:
        self.answer = answer
        self.calls = 0
        self.in_call = asyncio.Event()
        self.release = asyncio.Event()

    async def deliver(self, *_: Any) -> bool:
        self.calls += 1
        self.in_call.set()
        await self.release.wait()
        if self.answer == "rate_limited":
            raise RateLimitedError(0.01, _WORDS)
        return True


@pytest.fixture
def channel(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], _Channel]:
    def install(answer: str) -> _Channel:
        fake = _Channel(answer)
        monkeypatch.setattr(service_module, "get_formatter", lambda _: fake)
        return fake
    return install


async def _recipient_and_job() -> tuple[UUID, UUID]:
    async with get_session_factory()() as session:
        recipient = await create_recipient(session)
        job = await create_notification(
            session, **intake_fields(), type="unit_event_in_app",
            title="T", body="B",
            target_type=TargetType.USER, target_value=str(recipient.id),
        )
        await session.commit()
        return recipient.id, job.id


async def _forget(recipient_id: UUID, pid: list[int] | None = None) -> bool:
    """forget_recipient in its own session; `pid` receives the session's
    backend pid, so a test waits for THIS session's lock wait -- never
    for whatever else in the database happens to wait."""
    async with get_session_factory()() as session:
        backend = (await session.execute(
            text("SELECT pg_backend_pid()"),
        )).scalar_one()
        if pid is not None:
            pid.append(backend)
        forgotten = await forget_recipient(
            session, recipient_id=recipient_id, version=2,
        )
        await session.commit()
        return forgotten


async def _waiting_statement(pid: list[int]) -> str | None:
    """The statement the forgetting session waits in on a lock, or None."""
    if not pid:
        return None
    async with get_session_factory()() as session:
        query: str | None = (await session.execute(text(
            "SELECT query FROM pg_stat_activity "
            "WHERE pid = :pid AND wait_event_type = 'Lock'"
        ), {"pid": pid[0]})).scalar_one_or_none()
        return query


async def _until_waiting(pid: list[int]) -> str:
    """The statement the forgetting session waits in -- that session's,
    by its pid."""
    for _ in range(int(_BOUND / 0.02)):
        query = await _waiting_statement(pid)
        if query is not None:
            return query
        await asyncio.sleep(0.02)
    raise AssertionError("the forgetting session never waited on a lock")


async def _until(event: asyncio.Event) -> None:
    await asyncio.wait_for(event.wait(), _BOUND)


async def _state(recipient_id: UUID, job_id: UUID) -> dict[str, Any]:
    async with get_session_factory()() as session:
        recipient = await session.get(Recipient, recipient_id)
        deliveries = (await session.execute(
            select(NotificationDelivery).where(
                NotificationDelivery.notification_id == job_id,
            )
        )).scalars().all()
        journal = (await session.execute(
            select(NotificationTransition).where(
                NotificationTransition.recipient_id == recipient_id,
            )
        )).scalars().all()
        assert recipient is not None
        return {
            "tombstone": (
                recipient.deleted_at is not None,
                recipient.active,
                recipient.telegram_id,
            ),
            "deliveries": [(d.status, d.error_message) for d in deliveries],
            "provider_texts": [j.provider_text for j in journal],
            "outcomes": [j.outcome for j in journal],
        }


def _attempt(job_id: UUID) -> Awaitable[tuple[bool, bool]]:
    return processor._attempt_job(job_id, asyncio.Semaphore(4))


class TestTheFirstAttempt:
    @pytest.mark.parametrize("answer", ["accepted", "rate_limited"])
    async def test_forgetting_waits_for_the_first_attempt(
        self, channel: Callable[[str], _Channel], answer: str,
    ) -> None:
        """The attempt stands in the call with its deliveries inserted
        and not committed. forget_recipient waits -- on the recipient
        row, in the tombstone's FOR UPDATE -- until the attempt commits.
        The letter went while the person was still known; the channel
        was called once; after the commit the tombstone stands and the
        provider's words are gone from the delivery and the journal.
        The pair: those words WERE written (the journal holds the
        rate-limited answer)."""
        fake = channel(answer)
        recipient_id, job_id = await _recipient_and_job()
        attempt = asyncio.create_task(_attempt(job_id))
        await _until(fake.in_call)
        pid: list[int] = []
        forgetting = asyncio.create_task(_forget(recipient_id, pid))
        waiting = await _until_waiting(pid)
        # The tombstone's own read of the recipient row, FOR UPDATE.
        shape = sql_shape(waiting)
        assert (shape.table, shape.lock) == ("recipients", "FOR UPDATE")
        assert not forgetting.done()

        fake.release.set()
        await asyncio.wait_for(attempt, _BOUND)
        assert await asyncio.wait_for(forgetting, _BOUND) is True

        state = await _state(recipient_id, job_id)
        assert fake.calls == 1
        assert state["tombstone"] == (True, False, None)
        assert state["provider_texts"] and all(
            t is None for t in state["provider_texts"]
        )
        (status, error_message), = state["deliveries"]
        assert error_message is None
        if answer == "accepted":
            assert status == DeliveryStatus.SENT
        else:
            assert status == DeliveryStatus.RECIPIENT_INACTIVE
            assert "rate_limited" in state["outcomes"]

    async def test_withdraw_alone_returns_zero_and_waits_for_nothing(
        self, channel: Callable[[str], _Channel],
    ) -> None:
        """withdraw_recipient is not the path: during the first attempt
        it sees no committed delivery, closes nothing and does not wait.
        The pair: the same call after the attempt's commit sees it."""
        fake = channel("rate_limited")
        recipient_id, job_id = await _recipient_and_job()
        attempt = asyncio.create_task(_attempt(job_id))
        await _until(fake.in_call)
        async with get_session_factory()() as session:
            closed = await asyncio.wait_for(
                withdraw_recipient(session, recipient_id), _BOUND,
            )
            await session.commit()
        assert closed == 0
        fake.release.set()
        await asyncio.wait_for(attempt, _BOUND)
        async with get_session_factory()() as session:
            assert await withdraw_recipient(session, recipient_id) == 1
            await session.commit()

    async def test_a_tombstone_between_the_ids_and_the_insert(
        self,
        channel: Callable[[str], _Channel],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The tombstone commits after resolve chose the ids and before
        the deliveries are inserted: the insert passes (the row exists),
        and the late activity gate sees the person gone -- the channel
        is not called. The pair: the delivery exists, closed
        RECIPIENT_INACTIVE."""
        fake = channel("accepted")
        recipient_id, job_id = await _recipient_and_job()
        real = service_module.resolve_targets

        async def resolve_then_forget(*args: Any) -> list[UUID]:
            ids = await real(*args)
            assert ids == [recipient_id]
            assert await _forget(recipient_id) is True
            return ids

        monkeypatch.setattr(service_module, "resolve_targets", resolve_then_forget)
        await asyncio.wait_for(_attempt(job_id), _BOUND)
        state = await _state(recipient_id, job_id)
        assert fake.calls == 0
        assert state["deliveries"] == [(DeliveryStatus.RECIPIENT_INACTIVE, None)]
        assert state["tombstone"] == (True, False, None)


class TestALaterAttempt:
    async def test_withdraw_waits_on_the_job(
        self, channel: Callable[[str], _Channel],
    ) -> None:
        """The job's deliveries are committed (a first attempt was
        rate-limited); the second attempt stands in the call. The
        tombstone passes -- no delivery is inserted, no key share on
        the recipient -- and withdraw_recipient waits on the job row
        until the attempt commits; then it closes and clears."""
        fake = channel("rate_limited")
        fake.release.set()
        recipient_id, job_id = await _recipient_and_job()
        await asyncio.wait_for(_attempt(job_id), _BOUND)
        async with get_session_factory()() as session:
            delivery = (await session.execute(
                select(NotificationDelivery).where(
                    NotificationDelivery.notification_id == job_id,
                )
            )).scalar_one()
            assert delivery.error_message and _WORDS.strip() in \
                delivery.error_message
            delivery.next_retry_at = None
            delivery.wait_reason = None
            await session.commit()

        fake.release.clear()
        fake.in_call.clear()
        attempt = asyncio.create_task(_attempt(job_id))
        await _until(fake.in_call)
        pid: list[int] = []
        forgetting = asyncio.create_task(_forget(recipient_id, pid))
        waiting = await _until_waiting(pid)
        # withdraw_recipient's lock of the job rows.
        shape = sql_shape(waiting)
        assert (shape.table, shape.lock) == ("notifications", "FOR UPDATE")
        assert not forgetting.done()

        fake.release.set()
        await asyncio.wait_for(attempt, _BOUND)
        assert await asyncio.wait_for(forgetting, _BOUND) is True
        state = await _state(recipient_id, job_id)
        assert fake.calls == 2
        assert state["tombstone"] == (True, False, None)
        assert state["deliveries"] == [(DeliveryStatus.RECIPIENT_INACTIVE, None)]
        assert state["provider_texts"] and all(
            t is None for t in state["provider_texts"]
        )

