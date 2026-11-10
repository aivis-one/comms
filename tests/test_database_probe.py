# =============================================================================
# WK-2 -- "does the database answer": one probe for the worker and the
# consumer (app/core/database.py database_answers)
# =============================================================================
#
# What is held here:
#   1 bounded as a WHOLE: a server that takes the TCP connection and never
#     speaks (the connection's opening hangs, not the statement) -> False
#     within the probe's timeout and a tolerance. Before WK-2 the
#     consumer's probe had wait_for around execute only, and hung there
#     past 8 s with a 0.5 s timeout (reproduced before the fix). The
#     pair: the live database -> True;
#   2 OUTSIDE THE POOL: the process's pool held whole, the live database
#     still answers -> True; a probe from the pool would wait for it and
#     say "no" of a database that answers;
#   3 SINGLE-FLIGHT: probes asked at once share one connection;
#   4 the worker and the consumer ask the same function.
#
# MUTATIONS (each turns one red):
#   P1 the timeout around the statement only -> 1
#   P2 the probe from the process's pool (get_engine) -> 2
#   P3 no single-flight -> 3
# =============================================================================

import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncConnection

from app.core import database
from app.core.config import settings
from app.core.database import POOLS, Pool, ProcessRole, dispose_engine, get_engine
from app.engine import processor
from app.transport import consumer

_TIMEOUT = 0.5
_TOLERANCE = 1.0
_GUARD = 10.0


@pytest.fixture
def short_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(database, "PROBE_TIMEOUT_SECONDS", _TIMEOUT)


@pytest.fixture
async def a_silent_server() -> AsyncGenerator[int, None]:
    """A TCP server that accepts and never says a word; its port."""
    held: list[asyncio.StreamWriter] = []

    async def silent(_reader: asyncio.StreamReader,
                     writer: asyncio.StreamWriter) -> None:
        held.append(writer)

    server = await asyncio.start_server(silent, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        for writer in held:
            writer.close()
        server.close()
        await server.wait_closed()


@pytest.mark.usefixtures("short_probe")
class TestBoundedAsAWhole:
    async def test_a_connection_that_never_opens(
        self, monkeypatch: pytest.MonkeyPatch, a_silent_server: int,
    ) -> None:
        url = make_url(settings.database_url).set(
            host="127.0.0.1", port=a_silent_server,
        )
        monkeypatch.setattr(settings, "database_url",
                            url.render_as_string(hide_password=False))
        started = time.monotonic()
        answer = await asyncio.wait_for(database.database_answers(), _GUARD)
        took = time.monotonic() - started
        assert answer is False
        assert took <= _TIMEOUT + _TOLERANCE, took

    async def test_the_pair_the_live_database(self) -> None:
        assert await asyncio.wait_for(database.database_answers(), _GUARD) is True


@pytest.fixture
async def a_pool_of_one(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[None, None]:
    """The worker's pool cut to one connection and no overflow, built
    afresh; the suite's engine is rebuilt after."""
    monkeypatch.setitem(POOLS, ProcessRole.WORKER, Pool(size=1, overflow=0))
    await dispose_engine()
    try:
        yield
    finally:
        await dispose_engine()


@pytest.mark.usefixtures("short_probe", "a_pool_of_one")
class TestOutsideThePool:
    async def test_the_pool_held_whole_the_database_answers(self) -> None:
        held: AsyncConnection
        async with get_engine().connect() as held:
            assert get_engine().pool.checkedout() == 1  # type: ignore[attr-defined]
            answer = await asyncio.wait_for(database.database_answers(), _GUARD)
            assert held is not None
        assert answer is True


@pytest.mark.usefixtures("short_probe")
class TestSingleFlight:
    async def test_probes_at_once_share_one_connection(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        engines: list[Any] = []
        real = database.create_async_engine

        def counting(*args: Any, **kwargs: Any) -> Any:
            engine = real(*args, **kwargs)
            engines.append(engine)
            return engine

        monkeypatch.setattr(database, "create_async_engine", counting)
        answers = await asyncio.wait_for(
            asyncio.gather(*(database.database_answers() for _ in range(5))),
            _GUARD,
        )
        assert answers == [True] * 5
        assert len(engines) == 1

    async def test_the_pair_one_after_another_each_asks(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Not a cache: a probe that has ended is not the next one's
        answer."""
        engines: list[Any] = []
        real = database.create_async_engine

        def counting(*args: Any, **kwargs: Any) -> Any:
            engines.append(None)
            return real(*args, **kwargs)

        monkeypatch.setattr(database, "create_async_engine", counting)
        for _ in range(2):
            assert await database.database_answers() is True
        assert len(engines) == 2


class TestOneProbe:
    def test_the_worker_and_the_consumer_ask_the_same(self) -> None:
        assert processor.database_answers is database.database_answers
        assert consumer.database_answers is database.database_answers
        assert not hasattr(consumer, "_database_answers")
        assert not hasattr(consumer, "_PROBE_TIMEOUT")
