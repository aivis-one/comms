# =============================================================================
# A2 -- the intake under a taken key outlives retention removing the
# key's holder in the same moment
# =============================================================================
#
# accept_notification learns "the key is taken" from the unique index:
# its insert fails, and only then is the holder read. A failed insert
# does not lock the holder, so between the two retention can remove it
# (READ COMMITTED: the read takes a new snapshot) -- and by the contract
# the key is then free (deploy/INTEGRATION.md section 7: released when
# retention removes the job). Until A2 the read was scalar_one(): the
# intake raised NoResultFound instead of answering, and each caller let
# it out its own way --
#   the stream     the entry HELD (event_held), its key, correlation and
#                  person held with it, the next pass over the pending
#                  list (_PENDING_REDRAIN_SECONDS) accepting it;
#   a message ping POST /threads/{id}/messages 500, the message rolled
#                  back with its ping;
#   a close notice close_notification_error, the flag kept for the next
#                  pass.
# Now the holder gone is the key free: ONE more insert, in a new
# savepoint; a writer that took the key again in that moment is answered
# by (DUPLICATE / CONFLICT).
#
# THE HARNESS builds the window deterministically, through the code's
# own statements: when the intake flushes its job under the key, another
# session commits a HOLDER of that key -- finished, older than the
# retention period -- so the intake's insert fails on the index; when
# the intake then reads the holder, the REAL retention pass
# (cleanup_terminal_notifications) runs in its own session and removes
# it. A third writer, when a test wants one, takes the key in that same
# moment.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the new path removed (scalar_one() at the first read)
#                                -> TestEachCaller, TestTheIntake
#   M2  the retry without a bound (a loop) -> TestTheBound
#   M3  the retry outside a savepoint     -> TestTheThirdWriter
#   M4  a second collision raised, not read -> TestTheThirdWriter
#   M5  the holder gone answered DUPLICATE/CONFLICT, no retry
#                                -> TestTheIntake, TestEachCaller
#   M6  the retry run with the holder found -> TestTheIntake (the pair)
#   Unkillable, not built: the ACCEPTED of the retry without
#   signal_lane -- the lane's wake is a NOTIFY after the commit, and the
#   lane's next poll takes the job all the same; no answer and no row
#   differs.
# =============================================================================

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from httpx import AsyncClient
from sqlalchemy import Select, func, select, text
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.models import Notification
from app.engine.processor import cleanup_terminal_notifications
from app.engine.service import Intake, accept_notification
from app.messaging.constants import OperatorKind, ThreadKind, ThreadStatus
from app.messaging.status import set_status
from app.messaging.threads import create_or_get_thread
from app.notifier import TYPE_THREAD_CLOSED, consume_close_notifications
from app.transport import consumer as consumer_module
from app.transport.consumer import StreamConsumer
from tests.helpers import (
    TEST_FINGERPRINT,
    create_recipient,
    create_section,
    intake_fields,
    notification_row_fields,
)
from tests.test_transport_consumer import (
    _no_pending,
    _request_data,
    _run_until,
    _user_upserted_data,
    _xadd,
)

_BOUND = 10.0
_POLL = 0.02
_OTHER = "0" * 64


# -----------------------------------------------------------------------------
# The harness
# -----------------------------------------------------------------------------


class _Window:
    """Plants a holder of the key at the intake's flush of its job, and
    runs `at_read` -- by default the real retention -- at the intake's
    read of the holder; `at_retry`, when given, at the flush of a second
    insert, before it. `every_read`: at every read, a holder planted at
    every flush (an outside writer and deleter that never stop)."""

    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        matches: Callable[[str], bool],
        *,
        at_read: Callable[[str], Awaitable[None]] | None = None,
        at_retry: Callable[[str], Awaitable[None]] | None = None,
        every_read: bool = False,
    ) -> None:
        self.matches = matches
        self.at_read = at_read or _retention_removes_the_holder
        self.at_retry = at_retry
        self.every_read = every_read
        self.inserts = 0
        self.planted: list[UUID] = []
        self.reads = 0
        self.pid: int | None = None
        self._busy = False
        self._key: str | None = None
        flush, execute = AsyncSession.flush, AsyncSession.execute
        window = self

        async def planting_flush(session: AsyncSession, objects: Any = None) -> None:
            if not window._busy:
                for obj in list(session.sync_session.new):
                    if isinstance(obj, Notification) and window.matches(
                        obj.idempotency_key,
                    ):
                        window.inserts += 1
                        if window.inserts == 2 and window.at_retry is not None:
                            window._busy = True
                            try:
                                await window.at_retry(obj.idempotency_key)
                            finally:
                                window._busy = False
                        if window.every_read or not window.planted:
                            window._key = obj.idempotency_key
                            window.pid = (await execute(
                                session, text("SELECT pg_backend_pid()"),
                            )).scalar_one()
                            await window._plant(obj.idempotency_key)
            await flush(session, objects)

        async def removing_execute(
            session: AsyncSession, statement: Any, *args: Any, **kwargs: Any,
        ) -> Any:
            if (
                not window._busy
                and window._key is not None
                and _reads_the_holder(statement)
                and (window.every_read or window.reads == 0)
            ):
                window.reads += 1
                window._busy = True
                try:
                    await window.at_read(window._key)
                finally:
                    window._busy = False
            return await execute(session, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "flush", planting_flush)
        monkeypatch.setattr(AsyncSession, "execute", removing_execute)

    async def _plant(self, key: str) -> None:
        self._busy = True
        try:
            old = datetime.now(UTC) - timedelta(
                days=settings.notification_retention_days + 1,
            )
            async with get_session_factory()() as session:
                holder = Notification(
                    **{**notification_row_fields(), "idempotency_key": key},
                    type="unit_event_in_app", title="T", body="B",
                    target_type="all", target_value="*", status="sent",
                    scheduled_at=old, created_at=old,
                )
                session.add(holder)
                await session.commit()
                self.planted.append(holder.id)
        finally:
            self._busy = False


def _reads_the_holder(statement: Any) -> bool:
    """The intake's read of the key's holder: a SELECT of notifications
    whose WHERE names idempotency_key."""
    if not isinstance(statement, Select):
        return False
    entities = [d.get("entity") for d in statement.column_descriptions]
    where = statement.whereclause
    return (
        Notification in entities
        and where is not None
        and "idempotency_key" in str(where)
    )


async def _retention_removes_the_holder(key: str) -> None:
    removed = await cleanup_terminal_notifications()
    assert removed >= 1, "the harness: retention removed the holder"


def _third_writer(fingerprint: str) -> Callable[[str], Awaitable[None]]:
    """A writer of this moment takes the key again -- committed --
    after the intake found the holder gone and before its second insert
    (at_retry)."""

    async def at_retry(key: str) -> None:
        async with get_session_factory()() as session:
            session.add(Notification(
                **{**notification_row_fields(), "idempotency_key": key,
                   "fingerprint": fingerprint},
                type="unit_event_in_app", title="T", body="B",
                target_type="all", target_value="*",
            ))
            await session.commit()

    return at_retry


async def _jobs(key: str) -> list[UUID]:
    async with get_session_factory()() as session:
        return list((await session.execute(
            select(Notification.id).where(Notification.idempotency_key == key)
        )).scalars())


async def _accept(session: AsyncSession, key: str, fingerprint: str) -> Any:
    return await accept_notification(
        session, idempotency_key=key, fingerprint=fingerprint,
        type="unit_event_in_app", title="T", body="B",
        target_type="all", target_value="*",
    )


async def _accept_committed(key: str, fingerprint: str = TEST_FINGERPRINT) -> Any:
    async with get_session_factory()() as session:
        acceptance = await _accept(session, key, fingerprint)
        await session.commit()
        return acceptance


# -----------------------------------------------------------------------------
# The intake itself
# -----------------------------------------------------------------------------


class TestTheIntake:
    """The window on accept_notification: the holder gone is the key
    free -- ACCEPTED, a new job, two inserts. M1, M5. The pair: a holder
    that stays is answered by, ONE insert (M6)."""

    async def test_the_holder_gone_is_a_new_job(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        key = f"a2-{uuid4().hex}"
        window = _Window(monkeypatch, lambda k: k == key)
        with capture_logs() as logs:
            acceptance = await _accept_committed(key)
        assert acceptance.outcome is Intake.ACCEPTED
        assert window.inserts == 2
        released = [e for e in logs if e["event"] == "intake_key_released"]
        assert released == [
            {"event": "intake_key_released", "idempotency_key": key,
             "log_level": "info"},
        ]
        assert await _jobs(key) == [acceptance.notification.id]
        assert acceptance.notification.id not in window.planted
        # REPEAT: the same request again is the new job.
        monkeypatch.undo()
        again = await _accept_committed(key)
        assert again.outcome is Intake.DUPLICATE
        assert again.notification.id == acceptance.notification.id

    @pytest.mark.parametrize(
        ("fingerprint", "outcome"),
        [(TEST_FINGERPRINT, Intake.DUPLICATE), (_OTHER, Intake.CONFLICT)],
        ids=["duplicate", "conflict"],
    )
    async def test_a_holder_that_stays_is_answered_by(
        self, monkeypatch: pytest.MonkeyPatch, fingerprint: str,
        outcome: Intake,
    ) -> None:
        key = f"a2-{uuid4().hex}"
        first = await _accept_committed(key)

        async def nothing(_: str) -> None:
            return None

        window = _Window(monkeypatch, lambda k: k == key, at_read=nothing)
        window.planted.append(first.notification.id)  # no plant: it is there
        window._key = key
        answer = await _accept_committed(key, fingerprint)
        assert answer.outcome is outcome
        assert answer.notification.id == first.notification.id
        assert window.inserts == 1


class TestTheThirdWriter:
    """The key taken again in the window: the retry's collision is read
    and answered by -- DUPLICATE for the same bytes, CONFLICT for other.
    M3, M4."""

    @pytest.mark.parametrize(
        ("fingerprint", "outcome"),
        [(TEST_FINGERPRINT, Intake.DUPLICATE), (_OTHER, Intake.CONFLICT)],
        ids=["duplicate", "conflict"],
    )
    async def test_committed(
        self, monkeypatch: pytest.MonkeyPatch, fingerprint: str,
        outcome: Intake,
    ) -> None:
        key = f"a2-{uuid4().hex}"
        window = _Window(
            monkeypatch, lambda k: k == key, at_retry=_third_writer(fingerprint),
        )
        async with get_session_factory()() as session:
            answer = await _accept(session, key, TEST_FINGERPRINT)
            # The caller's transaction is still usable after both
            # collisions (M3: a retry outside a savepoint aborts it).
            assert (await session.execute(text("SELECT 1"))).scalar_one() == 1
            await session.commit()
        assert answer.outcome is outcome
        assert window.inserts == 2
        assert await _jobs(key) == [answer.notification.id]

    @pytest.mark.parametrize("commits", [True, False], ids=["commits", "rolls-back"])
    async def test_in_flight_waits_for_it(
        self, monkeypatch: pytest.MonkeyPatch, commits: bool,
    ) -> None:
        """The third writer's insert not yet committed: the retry WAITS on
        it -- seen in pg_stat_activity, not timed -- and is answered by
        its end: committed -> DUPLICATE by it, rolled back -> ACCEPTED."""
        key = f"a2-{uuid4().hex}"
        factory = get_session_factory()
        third = factory()

        async def at_read(k: str) -> None:
            await _retention_removes_the_holder(k)
            third.add(Notification(
                **{**notification_row_fields(), "idempotency_key": k},
                type="unit_event_in_app", title="T", body="B",
                target_type="all", target_value="*",
            ))
            await third.flush()

        window = _Window(monkeypatch, lambda k: k == key, at_read=at_read)
        task = asyncio.ensure_future(_accept_committed(key))
        try:
            waited = False
            for _ in range(int(_BOUND / _POLL)):
                if task.done():
                    break
                if window.pid is not None:
                    async with factory() as observer:
                        kind = (await observer.execute(text(
                            "SELECT wait_event_type FROM pg_stat_activity "
                            "WHERE pid = :p"
                        ), {"p": window.pid})).scalar_one_or_none()
                    if kind == "Lock" and window.reads == 1:
                        waited = True
                        break
                await asyncio.sleep(_POLL)
            assert waited
            if commits:
                await third.commit()
            else:
                await third.rollback()
            answer = await asyncio.wait_for(task, _BOUND)
        finally:
            await third.close()
        assert answer.outcome is (Intake.DUPLICATE if commits else Intake.ACCEPTED)
        assert await _jobs(key) == [answer.notification.id]


class TestTheBound:
    """One retry, as once_more_on_insert_race: a holder that is gone
    AGAIN -- only an outside writer and deleter can make that, comms'
    retention never removes a job of this moment -- is raised after
    exactly two inserts, not chased. M2: unbounded, this never ends."""

    async def test_two_inserts_then_raised(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        key = f"a2-{uuid4().hex}"
        window = _Window(monkeypatch, lambda k: k == key, every_read=True)
        with pytest.raises(NoResultFound):
            await asyncio.wait_for(_accept_committed(key), _BOUND)
        assert window.inserts == 2


# -----------------------------------------------------------------------------
# Each caller -- what comes out
# -----------------------------------------------------------------------------


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
    return name


async def _recipient_known(rid: UUID) -> bool:
    async with get_session_factory()() as session:
        found = await session.scalar(select(Recipient.id).where(Recipient.id == rid))
    return found is not None


class TestEachCaller:
    """M1, M5 on every caller of accept_notification."""

    async def test_the_stream_takes_it_and_the_next_one_passes(
        self, monkeypatch: pytest.MonkeyPatch,
        fake_redis: fakeaioredis.FakeRedis, a_stream: str,
    ) -> None:
        """A request to a person under a key whose holder retention
        removes in the window, and behind it the person's user_upserted
        -- an event the request, held, would hold. Both go through on
        the first pass: nothing pending, no event_held, the request a
        new job, the person known."""
        person = uuid4()
        request = _request_data(target_value=str(person))
        key = request["idempotency_key"]
        window = _Window(monkeypatch, lambda k: k == key)
        await _xadd(fake_redis, a_stream, "notification_request", request)
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def both() -> bool:
            return (
                await _no_pending(fake_redis, a_stream)
                and await _recipient_known(person)
            )

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), both)
        assert [e for e in logs if e["event"].startswith("event_held")] == []
        jobs = await _jobs(key)
        assert len(jobs) == 1
        assert jobs[0] not in window.planted
        assert window.inserts == 2

    async def test_a_message_ping_answers_200(
        self, monkeypatch: pytest.MonkeyPatch, client: AsyncClient,
    ) -> None:
        """POST /threads/{id}/messages whose ping's key had a holder
        that retention removes in the window: 200, the message and its
        ping created."""
        async with get_session_factory()() as session:
            sender = (await create_recipient(session)).id
            master = (await create_recipient(session)).id
            thread = await create_or_get_thread(
                session, **intake_fields(), client=sender,
                operator_kind=OperatorKind.USER, operator_value=master,
                kind=ThreadKind.DM,
            )
            thread_id = thread.id
            await session.commit()
        window = _Window(monkeypatch, lambda k: k.startswith("msg:"))
        response = await client.post(
            f"/api/v1/threads/{thread_id}/messages",
            headers={"Idempotency-Key": f"a2-{uuid4().hex}"},
            json={"sender": str(sender), "body": "hello"},
        )
        assert response.status_code == 200, response.text
        assert window.inserts == 2
        async with get_session_factory()() as session:
            pings = (await session.execute(
                select(func.count()).select_from(Notification)
                .where(Notification.target_value == str(master))
            )).scalar_one()
        assert pings == 1

    async def test_a_close_notice_is_created_on_the_first_pass(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A closed section thread whose notice's key had a holder that
        retention removes in the window: the notice is created on the
        first pass, the flag cleared, no close_notification_error."""
        factory = get_session_factory()
        async with factory() as session:
            client_id = (await create_recipient(session)).id
            section = await create_section(session, key=f"a2-{uuid4().hex[:8]}")
            thread = await create_or_get_thread(
                session, **intake_fields(), client=client_id,
                operator_kind=OperatorKind.SECTION, operator_value=section.id,
                kind=ThreadKind.TICKET, subject_type="practice", subject_id="s",
            )
            thread_id = thread.id
            await session.commit()
        async with factory() as session:
            await set_status(session, thread_id=thread_id, target=ThreadStatus.CLOSED)
            await session.commit()
        window = _Window(monkeypatch, lambda k: k.startswith(f"close:{thread_id}:"))
        with capture_logs() as logs:
            assert await consume_close_notifications() == 1
        assert [e for e in logs if e["event"] == "close_notification_error"] == []
        assert window.inserts == 2
        async with factory() as session:
            notices = (await session.execute(
                select(func.count()).select_from(Notification).where(
                    Notification.type == TYPE_THREAD_CLOSED,
                    Notification.target_value == str(client_id),
                )
            )).scalar_one()
        assert notices == 1
