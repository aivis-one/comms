"""`comms count` (W6-1, app/cli.py count_conversations): how many threads
and messages the database holds, for the count taken before and after a
move of the database (protocol 11.2).

One line, `threads=<n> messages=<n>`, exit 0; a refusal is one line on
stderr and exit 1, and says nothing of the database's address, user or
password -- in either stream. The session is read only by the server. The
count runs on 0025's schema too: tests/test_migration_chain.py counts the
same database before and after the chain.

The command line runs in a subprocess, through the image's launcher
(bin/comms): its entry calls asyncio.run, which must not run inside the
suite's loop. The password of a refused URL is assembled at run time.

MUTATIONS (one run, each red here):
  M1 the statement counts threads twice      -> TestTheCount, the launcher's count
  M2 a refusal printed with the error's text -> test_*_says_nothing_of_*
  M3 default_transaction_read_only dropped   -> test_the_session_is_read_only
  M4 the docstring's statement edited        -> test_the_statement_shown_is_run
"""

import asyncio
import os
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from app import cli
from app.cli import COUNT_SQL, count_conversations, count_engine
from app.core.config import Settings, settings
from app.core.database import get_session_factory

_REPO = Path(__file__).resolve().parents[1]
_LAUNCHER = _REPO / "bin" / "comms"

_SECRET = "pw-" + "k7" * 8
_USER = "w61user"


def _url(host: str = "127.0.0.1", port: int = 1, database: str = "comms_test") -> str:
    return f"postgresql+asyncpg://{_USER}:{_SECRET}@{host}:{port}/{database}"


@asynccontextmanager
async def _silent_server() -> AsyncIterator[int]:
    """A port that takes the connection and never answers; its port."""
    stop = asyncio.Event()

    async def silent(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        await stop.wait()
        writer.close()

    server = await asyncio.start_server(silent, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        stop.set()
        server.close()
        await server.wait_closed()


def _assert_says_nothing_of_the_database(output: str) -> None:
    for secret in (_SECRET, _USER, "127.0.0.1", "localhost"):
        assert secret not in output, output


async def _rows(threads: int, messages: int) -> None:
    """`threads` threads of one client, `messages` messages in the first."""
    async with get_session_factory()() as session:
        client = "6b000000-0000-4000-8000-000000000001"
        await session.execute(text(
            "INSERT INTO recipients (id, version, snapshot_fingerprint) "
            "VALUES (:id, 1, :fp)"), {"id": client, "fp": "f" * 64})
        for n in range(threads):
            await session.execute(text(
                "INSERT INTO threads (id, client, operator_kind, operator_value, "
                "kind, idempotency_key, fingerprint) VALUES (:id, :client, "
                "'user', :client, 'ticket', :key, :fp)"),
                {"id": f"6b000000-0000-4000-8000-1{n:011d}", "client": client,
                 "key": f"w61:count:t{n}", "fp": "f" * 64})
        for n in range(messages):
            await session.execute(text(
                "INSERT INTO messages (id, thread_id, sender, body, "
                "idempotency_key, fingerprint) VALUES (:id, :thread, :client, "
                "'m', :key, :fp)"),
                {"id": f"6b000000-0000-4000-8000-2{n:011d}",
                 "thread": "6b000000-0000-4000-8000-100000000000",
                 "client": client, "key": f"w61:count:m{n}", "fp": "f" * 64})
        await session.commit()


class TestTheCount:
    async def test_an_empty_database_is_zeros(self) -> None:
        assert await count_conversations(settings.database_url) == (0, 0)

    async def test_every_row_of_each_table(self) -> None:
        await _rows(threads=2, messages=3)
        assert await count_conversations(settings.database_url) == (2, 3)
        # twice, the same
        assert await count_conversations(settings.database_url) == (2, 3)

    async def test_the_session_is_read_only(self) -> None:
        """The server refuses a write in the count's session (25006),
        whatever the statement -- the pair: it reads there."""
        engine = count_engine(settings.database_url)
        try:
            async with engine.connect() as connection:
                assert await connection.scalar(text("SELECT 1")) == 1
                await connection.rollback()
                with pytest.raises(DBAPIError) as refused:
                    await connection.execute(text(
                        "UPDATE threads SET status = status WHERE false"))
                assert getattr(refused.value.orig, "sqlstate", None) == "25006"
        finally:
            await engine.dispose()

    def test_the_statement_shown_is_run(self) -> None:
        """The docstring shows the one statement psql can run where the
        command is not (a box on 3.4.0): the same text as the one run."""
        assert count_conversations.__doc__ is not None
        assert f"\n        {COUNT_SQL}\n" in count_conversations.__doc__

    def test_the_variable_is_the_settings(self) -> None:
        """The command reads the variable the application reads."""
        assert cli.DATABASE_URL_VARIABLE.lower() in Settings.model_fields
        assert cli.DATABASE_URL_VARIABLE not in cli.READS  # derive's only

    async def test_a_silent_server_is_bounded(self) -> None:
        """A server that takes the connection and never answers: the
        connection's bound, then the whole count's -- TimeoutError each."""
        async with _silent_server() as port:
            for bounds in ({"connect_timeout": 0.3}, {"whole": 0.3}):
                started = time.monotonic()
                with pytest.raises(TimeoutError):
                    await count_conversations(_url(port=port), **bounds)
                assert time.monotonic() - started < 5, bounds


def _run(*args: str, **values: str) -> subprocess.CompletedProcess[str]:
    """The launcher of the image in a process of its own: DATABASE_URL and
    nothing else of the service's settings."""
    environ = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(_REPO),
        **values,
    }
    return subprocess.run(
        [sys.executable, *args],
        env=environ, capture_output=True, text=True, timeout=60, check=False,
    )


def _comms(*args: str, **values: str) -> subprocess.CompletedProcess[str]:
    return _run(str(_LAUNCHER), *args, **values)


class TestTheLauncher:
    def test_an_empty_database(self) -> None:
        done = _comms("count", DATABASE_URL=settings.database_url)
        assert (done.returncode, done.stdout, done.stderr) == (
            0, "threads=0 messages=0\n", "")

    async def test_the_rows(self) -> None:
        await _rows(threads=1, messages=2)
        done = await asyncio.to_thread(
            _comms, "count", DATABASE_URL=settings.database_url)
        assert (done.returncode, done.stdout, done.stderr) == (
            0, "threads=1 messages=2\n", "")

    def test_no_database_url(self) -> None:
        done = _comms("count")
        assert (done.returncode, done.stdout, done.stderr) == (
            1, "", "comms count: DATABASE_URL is not set\n")

    def test_a_closed_port_says_nothing_of_the_database(self) -> None:
        done = _comms("count", DATABASE_URL=_url(port=1))
        assert (done.returncode, done.stdout) == (1, "")
        assert done.stderr.startswith("comms count: failed (")
        assert done.stderr.count("\n") == 1
        _assert_says_nothing_of_the_database(done.stderr)

    def test_a_wrong_password_says_nothing_of_the_database(self) -> None:
        """The real server, another user and password: refused by it."""
        host, port = "127.0.0.1", 5432
        done = _comms("count", DATABASE_URL=_url(host=host, port=port))
        assert (done.returncode, done.stdout) == (1, "")
        assert done.stderr.startswith("comms count: failed (")
        _assert_says_nothing_of_the_database(done.stderr)

    def test_a_malformed_url_says_nothing_of_the_database(self) -> None:
        """SQLAlchemy quotes a URL it cannot parse whole."""
        done = _comms("count", DATABASE_URL=f"{_USER}:{_SECRET}@127.0.0.1 nowhere")
        assert (done.returncode, done.stdout) == (1, "")
        assert done.stderr == "comms count: failed (ArgumentError)\n"
        _assert_says_nothing_of_the_database(done.stderr)

    def test_a_database_without_the_tables(self) -> None:
        """Reached, but not comms' database: refused by its class."""
        url = settings.database_url.rsplit("/", 1)[0] + "/postgres"
        done = _comms("count", DATABASE_URL=url)
        assert (done.returncode, done.stdout) == (1, "")
        assert done.stderr == "comms count: failed (ProgrammingError)\n"

    async def test_a_silent_server_is_refused_in_time(self) -> None:
        """The connection's bound shortened in the process: the refusal
        names the timeout, not the address."""
        program = (
            "import sys\n"
            "from app import cli\n"
            "cli.count_conversations.__kwdefaults__['connect_timeout'] = 0.3\n"
            "sys.exit(cli.main(['count']))\n"
        )
        async with _silent_server() as port:
            started = time.monotonic()
            done = await asyncio.to_thread(
                _run, "-c", program, DATABASE_URL=_url(port=port))
            took = time.monotonic() - started
        assert (done.returncode, done.stdout) == (1, "")
        assert done.stderr.startswith(
            "comms count: the database did not answer in time"), done.stderr
        _assert_says_nothing_of_the_database(done.stderr)
        assert took < 30

    @pytest.mark.parametrize("args", [("count", "x"), ("count", "--threads")],
                             ids=["extra", "unknown-option"])
    def test_a_wrong_command_line(self, args: tuple[str, ...]) -> None:
        done = _comms(*args, DATABASE_URL=settings.database_url)
        assert (done.returncode, done.stdout) == (2, "")
        assert done.stderr.startswith("usage: comms")
