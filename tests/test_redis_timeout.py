# =============================================================================
# H1 Б1 -- a redis that accepted the connection and went silent stops
# neither the relay nor the consumer for good; an empty blocking wait on
# a live redis is not an error
# =============================================================================
#
# Real redis-py clients, built by the production path
# (app/transport/connection.py redis_client), against two local TCP
# servers: one that accepts and never writes a byte (silence), one that
# speaks enough RESP to answer the consumer's commands -- XREADGROUP
# with BLOCK only after the block has passed, as redis does.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the consumer's client without a socket timeout
#                      -> test_the_consumer_on_a_silent_redis_... fails
#                         by its own asyncio.timeout, it does not hang
#   M2  the relay's client without a socket timeout
#                      -> test_the_relay_on_a_silent_redis_... (same)
#   M3  the timeout derived shorter than the block
#                      -> test_an_empty_blocking_wait_...
#   M4  _main without its consumer_loop_failed line
#                      -> TestTheProcess
# =============================================================================

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Any
from unittest.mock import patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from structlog.testing import capture_logs

import app.consumer as entrypoint
import app.transport.push_relay as relay
from app.core import constants
from app.core.config import NUMERIC_BOUNDS, settings
from app.core.database import get_session_factory
from app.engine.constants import NotificationStatus, TargetType
from app.engine.models import Notification, PushOutbox
from app.transport.connection import redis_client, redis_socket_timeout
from app.transport.consumer import run_consumer_loop
from tests.helpers import notification_row_fields

_BOUND = 5.0  # the test's own ceiling: a hang fails here, never forever
# The production margin, read before any fixture shortens it.
_PRODUCTION_MARGIN = constants.REDIS_TIMEOUT_MARGIN_SECONDS
_BLOCK_MS = 300
_MARGIN = 0.3


# -----------------------------------------------------------------------------
# Servers
# -----------------------------------------------------------------------------


@asynccontextmanager
async def _silent() -> AsyncIterator[int]:
    """Accept every connection; never write a byte."""
    held: list[asyncio.StreamWriter] = []

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        held.append(writer)
        await reader.read()  # until the client gives up

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        for writer in held:
            writer.close()
        server.close()
        await server.wait_closed()


async def _command(reader: asyncio.StreamReader) -> list[str] | None:
    header = await reader.readline()
    if not header:
        return None
    assert header.startswith(b"*"), header
    parts = []
    for _ in range(int(header[1:])):
        size = int((await reader.readline())[1:])
        parts.append((await reader.readexactly(size + 2))[:-2].decode())
    return parts


@asynccontextmanager
async def _live(seen: list[list[str]]) -> AsyncIterator[int]:
    """Answer like redis does for an empty stream: XREADGROUP with BLOCK
    replies nil only after the block; everything else at once."""

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while (command := await _command(reader)) is not None:
                seen.append(command)
                name = command[0].upper()
                if name == "XREADGROUP":
                    upper = [part.upper() for part in command]
                    if "BLOCK" in upper:
                        block_ms = int(command[upper.index("BLOCK") + 1])
                        await asyncio.sleep(block_ms / 1000)
                    writer.write(b"*-1\r\n")
                else:
                    writer.write(b"+OK\r\n")
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        await server.wait_closed()


def _closed_port() -> int:
    import socket

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.fixture(autouse=True)
def _short(monkeypatch: pytest.MonkeyPatch) -> None:
    """A short block and margin -- the same derivation, test-sized."""
    monkeypatch.setattr(settings, "consumer_block_ms", _BLOCK_MS)
    monkeypatch.setattr(constants, "REDIS_TIMEOUT_MARGIN_SECONDS", _MARGIN)
    monkeypatch.setattr(relay, "PUSH_RELAY_INTERVAL_SECONDS", 0.05)


@contextmanager
def _redis_at(port: int) -> Iterator[None]:
    with patch.object(settings, "redis_url", f"redis://127.0.0.1:{port}/0"):
        yield


async def _owe_one() -> None:
    """A committed job with one push owed -- the relay only calls redis
    when it has something to publish."""
    async with get_session_factory()() as session:
        job = Notification(
            type="unit_event_in_app", title="T", body="B",
            target_type=TargetType.ALL, target_value="*",
            status=NotificationStatus.SENT, **notification_row_fields(),
        )
        session.add(job)
        await session.flush()
        session.add(PushOutbox(notification_id=job.id))
        await session.commit()


async def _until(predicate: Any) -> None:
    for _ in range(int(_BOUND / 0.02)):
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("not within the test's bound")


# -----------------------------------------------------------------------------
# The value
# -----------------------------------------------------------------------------


class TestTheValue:
    @pytest.mark.parametrize("block_ms", [
        NUMERIC_BOUNDS["consumer_block_ms"].lo,
        5000,
        NUMERIC_BOUNDS["consumer_block_ms"].hi,
    ])
    def test_longer_than_the_block_at_every_block_the_deploy_can_set(
        self, block_ms: int, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "consumer_block_ms", block_ms)
        monkeypatch.setattr(
            constants, "REDIS_TIMEOUT_MARGIN_SECONDS", 10.0,
        )
        assert redis_socket_timeout() == block_ms / 1000 + 10.0
        assert redis_socket_timeout() > block_ms / 1000

    def test_the_margin_is_positive(self) -> None:
        """The pair to the derivation: with a zero margin the timeout
        would equal the block, and an empty wait would race it."""
        assert _PRODUCTION_MARGIN > 0

    async def test_the_client_carries_it(self) -> None:
        with _redis_at(6379):
            client = redis_client()
        try:
            kwargs = client.connection_pool.connection_kwargs
            assert kwargs["socket_timeout"] == redis_socket_timeout()
            assert kwargs["socket_timeout"] > settings.consumer_block_ms / 1000
        finally:
            await client.aclose()


# -----------------------------------------------------------------------------
# Silence, refusal, an empty wait
# -----------------------------------------------------------------------------


class TestTheRelay:
    async def test_the_relay_on_a_silent_redis_logs_and_keeps_ticking(
        self,
    ) -> None:
        await _owe_one()
        async with _silent() as port:
            with _redis_at(port), capture_logs() as logs:
                task = asyncio.create_task(relay.run_push_relay_loop())
                try:
                    await _until(lambda: sum(
                        e["event"] == "push_relay_error" for e in logs
                    ) >= 2)
                    assert not task.done()
                finally:
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
        errors = [e for e in logs if e["event"] == "push_relay_error"]
        assert "Timeout" in errors[0]["exception"]
        async with get_session_factory()() as session:
            owed = (await session.execute(
                PushOutbox.__table__.select()
            )).all()
        assert len(owed) == 1  # still owed, not lost

    async def test_the_relay_on_a_refused_connection_logs_at_once(self) -> None:
        await _owe_one()
        with _redis_at(_closed_port()), capture_logs() as logs:
            task = asyncio.create_task(relay.run_push_relay_loop())
            try:
                await _until(lambda: any(
                    e["event"] == "push_relay_error" for e in logs
                ))
                assert not task.done()
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
        assert "Timeout" not in next(
            e for e in logs if e["event"] == "push_relay_error"
        )["exception"]


class TestTheConsumer:
    async def test_the_consumer_on_a_silent_redis_raises_in_bounded_time(
        self,
    ) -> None:
        async with _silent() as port:
            with _redis_at(port), pytest.raises(RedisTimeoutError):
                async with asyncio.timeout(_BOUND):
                    await run_consumer_loop()

    async def test_the_consumer_on_a_refused_connection_raises_at_once(
        self,
    ) -> None:
        with _redis_at(_closed_port()), pytest.raises(RedisConnectionError):
            async with asyncio.timeout(_BOUND):
                await run_consumer_loop()

    async def test_an_empty_blocking_wait_on_a_live_redis_is_not_an_error(
        self,
    ) -> None:
        """Several empty BLOCK waits in a row, each answered after the
        block: the loop keeps reading; nothing is raised."""
        seen: list[list[str]] = []
        async with _live(seen) as port:
            with _redis_at(port):
                task = asyncio.create_task(run_consumer_loop())
                try:
                    await _until(lambda: task.done() or sum(
                        "BLOCK" in [p.upper() for p in c] for c in seen
                    ) >= 3)
                    assert not task.done(), task.exception()
                finally:
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
        blocking = [c for c in seen if "BLOCK" in [p.upper() for p in c]]
        assert len(blocking) >= 3
        assert str(_BLOCK_MS) in blocking[0]


class TestTheProcess:
    @pytest.fixture(autouse=True)
    async def _no_signal_handlers(self) -> AsyncIterator[None]:
        loop = asyncio.get_running_loop()
        with patch.object(loop, "add_signal_handler"):
            yield

    async def test_a_consumer_loop_that_ends_is_one_log_line(self) -> None:
        """The silent redis surfaced as TimeoutError ends the process --
        the container restarts it -- and the line says which loop and
        why. The pair: the relay, cancelled with it, logs nothing."""

        async def timed_out() -> None:
            raise RedisTimeoutError("Timeout reading from 127.0.0.1:6379")

        async def endless() -> None:
            await asyncio.sleep(3600)

        with (
            patch.object(entrypoint, "run_consumer_loop", timed_out),
            patch.object(entrypoint, "run_push_relay_loop", endless),
            capture_logs() as logs,
            pytest.raises(RedisTimeoutError),
        ):
            await entrypoint._main()
        failed = [e for e in logs if e["event"] == "consumer_loop_failed"]
        assert len(failed) == 1
        assert failed[0]["loop"] == "consumer"
        assert failed[0]["log_level"] == "error"
        assert "Timeout reading" in failed[0]["exception"]
