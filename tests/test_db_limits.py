# =============================================================================
# W5-3 -- every connection of comms carries the database's limits, and a
# statement past one is an outcome, not a hang
# =============================================================================
#
# What is held here (app/core/database.py LIMITS, THE DATABASE'S LIMITS):
#   1 every connection of each process -- its pool's, the probe's, the
#     wake listener's -- carries its role's limits (SHOW, and the
#     driver's command_timeout), derived from the attempt's hold;
#   2 a statement past statement_timeout (pg_sleep), a lock held by
#     another past lock_timeout, a transaction idle past
#     idle_in_transaction_session_timeout, a server that does not answer
#     past command_timeout -- each fails within its limit and a margin,
#     by the path that already was: the API's answer (500 / 503), the
#     consumer's hold (event_held), the worker's counted failure; the
#     line carries the SQLSTATE, never the database's text;
#   3 a wait for an attempt's row (wait_for_attempts) outlasts the
#     process's own limits: forgetting and expiry during a long attempt
#     answer as before; past the attempt's hold they fail; the lift is
#     the transaction's, not the pooled connection's;
#   4 an attempt that sits idle through a long channel call is not cut;
#   5 the numbers: above the measure by MEASURE_MARGIN, lock below
#     statement, the liveness margin above the statement limit.
#
# The limits are made small here (the fixture `bounded`): the same code,
# a shorter clock.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  connect_args off the pool's engine -> TestEveryConnection
#   M2  the API's and the worker's limits swapped -> TestEveryConnection
#   M3  command_timeout off connect_args -> TestEveryConnection,
#       TestTheApi.test_a_server_that_does_not_answer
#   M4  the probe or the wake listener without the limits ->
#       TestEveryConnection
#   M5  wait_for_attempts off withdraw_recipient -> TestTheWaits
#   M6  wait_for_attempts off close_notifications -> TestTheWaits
#   M7  the worker's idle limit a fixed one, not the attempt's hold ->
#       TestTheAttemptThroughItsCall
#   M8  SET instead of SET LOCAL in wait_for_attempts -> TestTheWaits
#   M9  STEP_MARGIN_SECONDS below a statement limit -> TestTheNumbers
#   M10 a limit below its measure times the margin -> TestTheNumbers
#   M11 the API's database error logged with its text -> TestTheApi
# =============================================================================

import asyncio
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any
from uuid import UUID

import fakeredis.aioredis as fakeaioredis
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from structlog.testing import capture_logs

from app.core import database, liveness
from app.core.config import settings
from app.core.database import (
    LIMITS,
    MEASURE_MARGIN,
    MEASURED_SECONDS,
    PROBE_APPLICATION_NAME,
    Limits,
    ProcessRole,
    dispose_engine,
    get_engine,
    get_session_factory,
    set_process_role,
    wait_seconds,
)
from app.core.lanes import Lane
from app.engine import processor, service, wake
from app.engine.constants import NotificationStatus
from app.engine.models import Notification
from app.transport import consumer as consumer_module
from app.transport.consumer import StreamConsumer
from tests import measure_db_limits
from tests.test_transport_consumer import _run_until, _user_upserted_data, _xadd
from tests.test_worker_keeps_its_work import _attempt, _job, _row

# The margin a failure is allowed past its limit: the loop's turn, the
# log, the rollback.
_SLACK = 1.5

_LIMIT_TEXTS = (
    "canceling statement",
    "lock timeout",
    "idle-in-transaction",
)


@pytest.fixture
async def bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[Callable[..., Awaitable[None]], None]:
    """Make `role`'s engine anew with small limits; the suite's own back
    at the end."""

    async def apply(
        role: ProcessRole, *, statement: float = 0.6, lock: float = 0.3,
        idle: float = 0.4, hold: float = 0.5, margin: float = 0.4,
        client: float = 10.0,
    ) -> None:
        # The worker keeps its own rule for idle: the hold and the margin.
        own_idle = None if LIMITS[role].idle is None else idle
        await dispose_engine()
        monkeypatch.setitem(database.LIMITS, role, Limits(statement, lock, own_idle))
        monkeypatch.setattr(database, "WAIT_MARGIN_SECONDS", margin)
        monkeypatch.setattr(database, "CLIENT_MARGIN_SECONDS", client)
        monkeypatch.setattr(database, "_attempt_hold", hold)
        set_process_role(role)

    yield apply
    await dispose_engine()
    set_process_role(ProcessRole.WORKER)


async def _shown(engine: Any) -> dict[str, str]:
    async with engine.connect() as conn:
        # In milliseconds, as pg_settings keeps them (SHOW rounds to units).
        shown = {
            name: (await conn.execute(text(
                "SELECT setting FROM pg_settings WHERE name = :name"
            ), {"name": name})).scalar_one()
            for name in (
                "statement_timeout", "lock_timeout",
                "idle_in_transaction_session_timeout", "application_name",
            )
        }
        raw = await conn.get_raw_connection()
        shown["command_timeout"] = raw.driver_connection._config.command_timeout
        await conn.rollback()
    return shown


def _expected(role: ProcessRole) -> dict[str, Any]:
    limits = LIMITS[role]
    idle = wait_seconds() if limits.idle is None else limits.idle
    return {
        "statement_timeout": _in_ms(limits.statement),
        "lock_timeout": _in_ms(limits.lock),
        "idle_in_transaction_session_timeout": _in_ms(idle),
        "command_timeout": (
            max(limits.statement, database.waiting_statement_seconds())
            + database.CLIENT_MARGIN_SECONDS
        ),
    }


def _in_ms(seconds: float) -> str:
    return str(round(seconds * 1000))


def _all_text(logs: list[dict[str, Any]]) -> str:
    return "\n".join(str(entry) for entry in logs)


def _no_database_text(logs: list[dict[str, Any]], body: str = "") -> None:
    said = _all_text(logs) + body
    for phrase in _LIMIT_TEXTS:
        assert phrase not in said, phrase


# ---------------------------------------------------------------------------
# 1 -- every connection carries its role's limits
# ---------------------------------------------------------------------------


class TestEveryConnection:
    @pytest.mark.parametrize("role", list(ProcessRole))
    async def test_the_pool(self, role: ProcessRole) -> None:
        await dispose_engine()
        set_process_role(role)
        try:
            shown = await _shown(get_engine())
            # The pair: in force, and not the server's default 0.
            assert {k: shown[k] for k in _expected(role)} == _expected(role)
            assert "0" not in (
                shown["statement_timeout"], shown["lock_timeout"],
                shown["idle_in_transaction_session_timeout"],
            )
        finally:
            await dispose_engine()
            set_process_role(ProcessRole.WORKER)

    def test_the_roles_differ(self) -> None:
        """The API's statements are bounded shorter than the worker's, and
        only the worker's idle limit is the attempt's hold."""
        assert LIMITS[ProcessRole.API].statement < LIMITS[ProcessRole.WORKER].statement
        assert LIMITS[ProcessRole.WORKER].idle is None
        assert LIMITS[ProcessRole.API].idle is not None
        assert LIMITS[ProcessRole.CONSUMER].idle is not None

    @pytest.mark.parametrize("role", [ProcessRole.WORKER, ProcessRole.CONSUMER])
    async def test_the_probe(
        self, role: ProcessRole, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The probe's own connection, as _probe_once builds it."""
        made: list[Any] = []

        def capture(*args: Any, **kwargs: Any) -> Any:
            engine = create_async_engine(*args, **kwargs)
            made.append(engine)
            return engine

        monkeypatch.setattr(database, "create_async_engine", capture)
        set_process_role(role)
        try:
            assert await database._probe_once()
            (engine,) = made
            shown = await _shown(engine)
            await engine.dispose()
        finally:
            set_process_role(ProcessRole.WORKER)
        assert {k: shown[k] for k in _expected(role)} == _expected(role)
        assert shown["application_name"] == PROBE_APPLICATION_NAME

    async def test_the_wake_listener(self) -> None:
        engine = wake.wake_engine()
        try:
            shown = await _shown(engine)
        finally:
            await engine.dispose()
        expected = _expected(ProcessRole.WORKER)
        assert {k: shown[k] for k in expected} == expected
        assert shown["application_name"] == wake.WAKE_APPLICATION_NAME

    async def test_the_listener_outlives_its_idle_limit(
        self, bounded: Callable[..., Awaitable[None]],
    ) -> None:
        """LISTEN runs outside a transaction: the idle limit never ends the
        listening session (the pair: the limit is in force and short)."""
        await bounded(ProcessRole.WORKER, hold=0.2, margin=0.1)
        events = {lane.value: asyncio.Event() for lane in Lane}
        with capture_logs() as logs:
            task = asyncio.ensure_future(wake.listen_for_wakes(events))
            await asyncio.sleep(1.2)
            async with get_session_factory()() as session:
                state = (await session.execute(text(
                    "SELECT state FROM pg_stat_activity "
                    "WHERE application_name = :name "
                    "AND datname = current_database()"
                ), {"name": wake.WAKE_APPLICATION_NAME})).scalars().all()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert state == ["idle"]
        assert not [e for e in logs if e["event"] == "lane_wake_listener_error"]

    async def test_no_hold_no_engine(self, monkeypatch: pytest.MonkeyPatch) -> None:
        await dispose_engine()
        monkeypatch.setattr(database, "_attempt_hold", None)
        with pytest.raises(RuntimeError, match="no attempt hold"):
            get_engine()

    @pytest.mark.parametrize("entry", ["app.worker", "app.consumer", "app.main"])
    def test_each_entry_point_names_the_hold_after_its_profile(
        self, entry: str,
    ) -> None:
        import importlib
        import inspect

        module = importlib.import_module(entry)
        body = inspect.getsource(
            module.lifespan if entry == "app.main" else module.main,
        )
        call = "set_attempt_hold(longest_attempt_hold())"
        assert call in body
        assert body.index("install_profile_from_settings()") < body.index(call)
        if "asyncio.run" in body:
            assert body.index(call) < body.index("asyncio.run")


# ---------------------------------------------------------------------------
# 2 -- past a limit: an outcome, by the path that was
# ---------------------------------------------------------------------------


async def _sleep(session: AsyncSession) -> None:
    await session.execute(text("SELECT pg_sleep(5)"))


async def _locked_by_another(session: AsyncSession) -> None:
    """Wait for the lock _hold_mutes holds in another session."""
    await session.execute(text("LOCK TABLE category_mutes IN SHARE MODE"))


async def _forgotten(session: AsyncSession, idle: float) -> None:
    await session.execute(text("SELECT 1"))
    await asyncio.sleep(idle + 0.4)
    await session.execute(text("SELECT 1"))


async def _hold_mutes(holder: AsyncSession) -> None:
    await holder.execute(text("LOCK TABLE category_mutes IN EXCLUSIVE MODE"))


class TestTheApi:
    async def _post_section(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
        work: Callable[[AsyncSession], Awaitable[None]],
    ) -> tuple[Any, float, list[dict[str, Any]]]:
        async def route_work(session: AsyncSession, **_k: Any) -> Any:
            await work(session)

        monkeypatch.setattr("app.api.messaging.get_or_create_section", route_work)
        started = time.monotonic()
        with capture_logs() as logs:
            resp = await client.post(
                "/api/v1/sections", json={"key": "k", "label": "L"},
            )
        return resp, time.monotonic() - started, logs

    async def test_a_statement_past_its_limit(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        await bounded(ProcessRole.API)
        resp, took, logs = await self._post_section(client, monkeypatch, _sleep)
        assert resp.status_code == 500
        assert resp.json()["error"]["class"] == "internal"
        assert took < 0.6 + _SLACK
        assert "SQLSTATE 57014" in _all_text(logs)
        _no_database_text(logs, resp.text)

    async def test_a_lock_held_by_another(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        await bounded(ProcessRole.API)
        async with get_session_factory()() as holder:
            await _hold_mutes(holder)
            resp, took, logs = await self._post_section(
                client, monkeypatch, _locked_by_another)
            await holder.rollback()
        assert resp.status_code == 500
        assert took < 0.3 + _SLACK
        assert "SQLSTATE 55P03" in _all_text(logs)
        _no_database_text(logs, resp.text)

    async def test_a_transaction_left_idle(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        """The session the server ended: the connection gone -- 503."""
        await bounded(ProcessRole.API)
        resp, took, logs = await self._post_section(
            client, monkeypatch, lambda s: _forgotten(s, 0.4),
        )
        assert resp.status_code == 503
        assert took < 0.4 + 0.4 + _SLACK
        _no_database_text(logs, resp.text)

    async def test_a_server_that_does_not_answer(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        """Past command_timeout the client gives up: 503. The server's own
        limit lifted for the statement, as a server that does not speak."""
        await bounded(ProcessRole.API, hold=0.1, margin=0.1, client=0.4)

        async def silent(session: AsyncSession) -> None:
            await session.execute(text("SET LOCAL statement_timeout = 0"))
            await _sleep(session)

        resp, took, logs = await self._post_section(client, monkeypatch, silent)
        assert resp.status_code == 503
        assert took < 0.6 + 0.4 + _SLACK
        _no_database_text(logs, resp.text)


class TestTheConsumer:
    async def _held(
        self, monkeypatch: pytest.MonkeyPatch,
        work: Callable[[AsyncSession], Awaitable[None]],
    ) -> tuple[float, list[dict[str, Any]]]:
        async def handle(session: AsyncSession, _event: Any) -> Any:
            await work(session)

        redis = fakeaioredis.FakeRedis()
        stream = "comms:test:w53"
        monkeypatch.setattr(settings, "comms_events_stream", stream)
        monkeypatch.setattr(settings, "consumer_block_ms", 20)
        monkeypatch.setattr(consumer_module, "handle_event", handle)
        monkeypatch.setattr(consumer_module, "_BACKOFF_DELAYS", (0.01,))
        from uuid import uuid4

        await _xadd(redis, stream, "user_upserted", _user_upserted_data(uuid4()))
        started = time.monotonic()
        with capture_logs() as logs:

            async def held() -> bool:
                return any(e["event"] == "event_held" for e in logs)

            await _run_until(StreamConsumer(redis), held)
        return time.monotonic() - started, logs

    async def test_a_statement_past_its_limit(
        self, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        await bounded(ProcessRole.CONSUMER)
        took, logs = await self._held(monkeypatch, _sleep)
        assert took < 0.6 + _SLACK
        assert "SQLSTATE 57014" in _all_text(logs)
        _no_database_text(logs)

    async def test_a_lock_held_by_another(
        self, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        await bounded(ProcessRole.CONSUMER)
        async with get_session_factory()() as holder:
            await _hold_mutes(holder)
            took, logs = await self._held(monkeypatch, _locked_by_another)
            await holder.rollback()
        assert took < 0.3 + _SLACK
        assert "SQLSTATE 55P03" in _all_text(logs)
        _no_database_text(logs)

    async def test_a_transaction_left_idle(
        self, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        """Transient (the connection gone) while the database answers:
        retried by the budget, then held."""
        await bounded(ProcessRole.CONSUMER)
        took, logs = await self._held(monkeypatch, lambda s: _forgotten(s, 0.4))
        assert took < 2 * (0.4 + 0.4) + _SLACK
        assert any(e["event"] == "event_retry_scheduled" for e in logs)
        _no_database_text(logs)


class TestTheWorker:
    async def _failed(
        self, monkeypatch: pytest.MonkeyPatch,
        work: Callable[[AsyncSession], Awaitable[None]],
    ) -> tuple[Notification, float, list[dict[str, Any]]]:
        job_id = await _job()

        async def resolve(session: AsyncSession, _job: Any) -> Any:
            await work(session)

        monkeypatch.setattr(processor, "resolve_notification", resolve)
        started = time.monotonic()
        with capture_logs() as logs:
            await _attempt(job_id)
        return await _row(job_id), time.monotonic() - started, logs

    async def test_a_statement_past_its_limit(
        self, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        await bounded(ProcessRole.WORKER)
        row, took, logs = await self._failed(monkeypatch, _sleep)
        assert took < 0.6 + _SLACK
        assert (row.pipeline_attempts, row.pipeline_step) == (1, "resolve")
        assert "SQLSTATE 57014" in _all_text(logs)
        _no_database_text(logs, row.pipeline_error or "")

    async def test_a_lock_held_by_another(
        self, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        await bounded(ProcessRole.WORKER)
        async with get_session_factory()() as holder:
            await _hold_mutes(holder)
            row, took, logs = await self._failed(monkeypatch, _locked_by_another)
            await holder.rollback()
        assert took < 0.3 + _SLACK
        assert (row.pipeline_attempts, row.pipeline_step) == (1, "resolve")
        assert "SQLSTATE 55P03" in _all_text(logs)
        _no_database_text(logs, row.pipeline_error or "")

    async def test_a_transaction_left_idle(
        self, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        """The worker's idle limit is the attempt's hold and the margin:
        a transaction idle past it is ended -- transient, the database
        answers, so counted."""
        await bounded(ProcessRole.WORKER, hold=0.2, margin=0.2)
        row, took, logs = await self._failed(
            monkeypatch, lambda s: _forgotten(s, 0.4),
        )
        assert took < 0.4 + 0.4 + _SLACK
        assert (row.pipeline_attempts, row.pipeline_step) == (1, "resolve")
        assert "InterfaceError" in (row.pipeline_error or "")
        _no_database_text(logs, row.pipeline_error or "")


# ---------------------------------------------------------------------------
# 3 -- the waits for an attempt's row outlast the process's own limits
# ---------------------------------------------------------------------------


async def _resolved_job() -> tuple[UUID, UUID]:
    """A job resolved to its one person, its delivery committed."""
    job_id = await _job()
    async with get_session_factory()() as session:
        job = await session.get(Notification, job_id)
        assert job is not None
        await service.resolve_notification(session, job)
        await session.commit()
        recipient = UUID(str(job.target_value))
    return job_id, recipient


async def _attempt_holding(job_id: UUID, seconds: float) -> None:
    """Another session takes the job's row as an attempt does and holds
    it `seconds` -- on a connection of its own, as the worker's attempt
    is (another process, its own limits)."""
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as holder:
            await holder.execute(text(
                "SELECT 1 FROM notifications WHERE id = :id FOR NO KEY UPDATE"
            ), {"id": job_id})
            await asyncio.sleep(seconds)
            await holder.commit()
    finally:
        await engine.dispose()


class TestTheWaits:
    async def test_forgetting_during_a_long_attempt(
        self, client: AsyncClient, bounded: Callable[..., Awaitable[None]],
    ) -> None:
        """The API forgets a person while an attempt holds their job longer
        than the API's own limits: the answer is the one it was."""
        await bounded(ProcessRole.API, statement=0.3, lock=0.2, hold=1.0)
        job_id, recipient = await _resolved_job()
        holding = asyncio.ensure_future(_attempt_holding(job_id, 0.8))
        await asyncio.sleep(0.1)
        started = time.monotonic()
        resp = await client.request(
            "DELETE", f"/api/v1/recipients/{recipient}",
            # Above the snapshot's 1 (tests/helpers.py create_recipient).
            json={"version": 2},
        )
        took = time.monotonic() - started
        await holding
        assert resp.status_code == 200, resp.text
        # The pair: it did wait for the attempt, past its own limits.
        assert took > 0.3 + 0.2

    async def test_expiry_during_a_long_attempt(
        self, bounded: Callable[..., Awaitable[None]],
    ) -> None:
        await bounded(ProcessRole.WORKER, statement=0.3, lock=0.2, hold=1.0)
        job_id, _ = await _resolved_job()
        holding = asyncio.ensure_future(_attempt_holding(job_id, 0.8))
        await asyncio.sleep(0.1)
        async with get_session_factory()() as session:
            closed = await service.close_notifications(
                session, Notification.id == job_id, NotificationStatus.EXPIRED,
            )
            await session.commit()
        await holding
        assert closed == 1
        assert (await _row(job_id)).status == NotificationStatus.EXPIRED

    async def test_past_the_attempts_hold_it_fails(
        self, bounded: Callable[..., Awaitable[None]],
    ) -> None:
        """The pair: the lift is the hold and the margin, not unbounded."""
        await bounded(ProcessRole.WORKER, statement=0.3, lock=0.2,
                      hold=0.3, margin=0.2)
        job_id, _ = await _resolved_job()
        holding = asyncio.ensure_future(_attempt_holding(job_id, 2.0))
        await asyncio.sleep(0.1)
        started = time.monotonic()
        with pytest.raises(Exception) as caught:
            async with get_session_factory()() as session:
                await service.close_notifications(
                    session, Notification.id == job_id,
                    NotificationStatus.EXPIRED,
                )
        took = time.monotonic() - started
        await holding
        # One wait past the hold is the lock it is (lock_timeout below the
        # lifted statement_timeout).
        assert database._sqlstate(caught.value) == "55P03"
        assert not database.is_transient_db_error(caught.value)
        assert 0.5 - 0.1 < took < 0.5 + _SLACK

    async def test_the_lift_is_the_transactions(
        self, bounded: Callable[..., Awaitable[None]],
    ) -> None:
        """After the waiting transaction the pooled connection is back at
        its role's limits: SET LOCAL, never SET."""
        await bounded(ProcessRole.WORKER, statement=0.3, lock=0.2, hold=1.0)
        async with get_engine().connect() as conn:
            session = AsyncSession(bind=conn)
            await database.wait_for_attempts(session)
            lifted = (
                (await conn.execute(text("SHOW statement_timeout"))).scalar(),
                (await conn.execute(text("SHOW lock_timeout"))).scalar(),
            )
            await conn.commit()
            after = (
                (await conn.execute(text("SHOW statement_timeout"))).scalar(),
                (await conn.execute(text("SHOW lock_timeout"))).scalar(),
            )
            await conn.rollback()
        # The hold 1.0 and the margin 0.4: the lock waits 1.4 s, the
        # statement 1.8 s.
        assert lifted == ("1800ms", "1400ms")
        assert after == ("300ms", "200ms")


# ---------------------------------------------------------------------------
# 4 -- an attempt through a long call is not cut
# ---------------------------------------------------------------------------


class TestTheAttemptThroughItsCall:
    async def test_a_call_longer_than_the_statement_limit(
        self, monkeypatch: pytest.MonkeyPatch,
        bounded: Callable[..., Awaitable[None]],
    ) -> None:
        """The attempt's transaction sits idle through the channel's call
        (THE LOCK RULE): the worker's idle limit is the attempt's hold and
        the margin, so a call within the hold commits."""
        await bounded(ProcessRole.WORKER, statement=0.3, lock=0.2, hold=1.0,
                      margin=0.2)
        job_id = await _job()
        formatter = service.get_formatter("in_app")
        original = formatter.deliver

        async def slow(*args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(0.8)
            return await original(*args, **kwargs)

        monkeypatch.setattr(formatter, "deliver", slow)
        await _attempt(job_id)
        row = await _row(job_id)
        assert row.pipeline_attempts == 0, row.pipeline_error
        assert row.status == NotificationStatus.SENT


# ---------------------------------------------------------------------------
# 5 -- the numbers
# ---------------------------------------------------------------------------


class TestTheNumbers:
    def test_every_role_has_its_limits_and_its_measure(self) -> None:
        assert set(LIMITS) == set(MEASURED_SECONDS) == set(ProcessRole)
        assert set(measure_db_limits.OPERATIONS) == set(ProcessRole)
        assert all(measure_db_limits.OPERATIONS.values())

    @pytest.mark.parametrize("role", list(ProcessRole))
    def test_above_the_measure(self, role: ProcessRole) -> None:
        limits = LIMITS[role]
        floor = MEASURED_SECONDS[role] * MEASURE_MARGIN
        assert MEASURED_SECONDS[role] > 0
        assert limits.statement >= floor
        if limits.idle is not None:
            assert limits.idle >= floor

    @pytest.mark.parametrize("role", list(ProcessRole))
    def test_the_lock_fires_before_the_statement(self, role: ProcessRole) -> None:
        """At equal limits 57014 comes first (measured): a lock_timeout not
        below statement_timeout would never fire."""
        assert 0 < LIMITS[role].lock < LIMITS[role].statement

    @pytest.mark.parametrize("role", [ProcessRole.WORKER, ProcessRole.CONSUMER])
    def test_the_liveness_margin_holds_a_statement(self, role: ProcessRole) -> None:
        """The database's part of a watched step (liveness.STEP_MARGIN_SECONDS)
        holds one statement at its limit: a statement can no longer run
        longer than that. One source would make the healthcheck import the
        settings (app/core/liveness.py, THE CHECK): held here instead."""
        assert LIMITS[role].statement <= liveness.STEP_MARGIN_SECONDS

    def test_the_hold_is_the_lanes_longest(self) -> None:
        assert service.longest_attempt_hold() == max(
            service.attempt_hold(lane) for lane in Lane
        )
        assert wait_seconds() == (
            database._attempt_hold + database.WAIT_MARGIN_SECONDS
        )
