# =============================================================================
# W6-2b -- a schema that is not the code's head is not ready, and no
# worker or consumer starts on it (app/core/schema.py)
# =============================================================================
#
# done-when:
#   - the database's revision is the head of the migrations: /ready 200,
#     the worker's and the consumer's check pass;
#   - another revision -- behind the head (a red `migrate`) or one this
#     code does not know (an image older than the schema): /ready 503
#     naming `db`, /health `degraded` with `alembic_head` the DATABASE's
#     revision (the machine's `migrate` reads it), said once in the log
#     with both revisions; the worker and the consumer end at start with
#     "the schema is at X, the code expects Y";
#   - an empty alembic_version is a schema at none.
# The form of /health is held by tests/test_report.py (the golden file,
# the keys of `checks`), which this item leaves as it was.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 Report takes any revision for ok              -> TestTheApi
#   M2 the worker's _main does not ask               -> TestTheWorker
#   M3 the consumer's _main does not ask             -> TestTheConsumer
#   M4 refuse_foreign_schema passes any revision     -> TestTheRefusal,
#                                                       TestTheWorker,
#                                                       TestTheConsumer
#   M5 alembic_head the code's head, not the database's -> TestTheApi
# =============================================================================

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from httpx import AsyncClient
from structlog.testing import capture_logs

import app.consumer as consumer_module
import app.core.schema as schema_module
import app.main as main_module
import app.worker as worker_module
from app.api.report import DB
from app.core import liveness
from app.core.schema import code_head, read_revision, refuse_foreign_schema
from tests.helpers import redis_answering

_REPO = Path(__file__).resolve().parents[1]


def _script() -> ScriptDirectory:
    """The migrations as the machine reads them: `alembic heads` by
    alembic.ini, in the repository's root."""
    return ScriptDirectory.from_config(Config(str(_REPO / "alembic.ini")))


def _behind() -> str:
    """The revision before the head: the stamp a red `migrate` leaves."""
    down = _script().get_revision(code_head()).down_revision
    assert isinstance(down, str) and down
    return down


_UNKNOWN = "9999_from_a_newer_image"


def _reading(revision: str) -> Callable[[], Awaitable[str]]:
    async def read() -> str:
        return revision
    return read


async def _empty() -> str:
    raise LookupError("alembic_version holds no revision")


class TestTheHead:
    def test_the_code_s_head_is_the_machines(self) -> None:
        head = _script().get_current_head()
        assert head, "the pair: a head, not an empty one any value matches"
        assert code_head() == head

    async def test_the_suite_s_database_is_at_it(self) -> None:
        assert await read_revision() == code_head()

    def test_the_api_compares_with_it(self) -> None:
        assert main_module.report._head() == code_head()

    async def test_no_head_no_api(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Migrations that hold no one head refuse the API's start, not
        its first request. The entry point's global effects replaced."""
        monkeypatch.setattr(main_module, "setup_logging", lambda: None)
        monkeypatch.setenv("PRIMER_COMMIT", "abc123")

        def two_heads() -> str:
            raise ValueError("too many values to unpack (expected 1)")

        monkeypatch.setattr(main_module, "code_head", two_heads)
        with pytest.raises(ValueError, match="expected 1"):
            async with main_module.lifespan(main_module.app):
                pass


class TestTheApi:
    @pytest.fixture(autouse=True)
    def _measured_afresh(self, monkeypatch: pytest.MonkeyPatch) -> None:
        main_module.report.reset_turns()
        redis_answering(monkeypatch)

    async def test_at_the_head_ready(self, client: AsyncClient) -> None:
        response = await client.get("/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ready", "failing": []}

    @pytest.mark.parametrize("which", ["behind", "unknown"])
    async def test_another_revision_not_ready(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch, which: str,
    ) -> None:
        revision = _behind() if which == "behind" else _UNKNOWN
        monkeypatch.setattr(main_module, "read_revision", _reading(revision))
        with capture_logs() as logs:
            ready = await client.get("/ready")
            health = (await client.get("/health")).json()
        assert ready.status_code == 503
        assert ready.json() == {"status": "not_ready", "failing": [DB]}
        assert health["status"] == "degraded"
        assert health["checks"][DB] == "error"
        assert health["alembic_head"] == revision, "the database's, not the code's"
        assert health["alembic_head"] != code_head()
        # Two answers, one turn: said once, with both revisions.
        (line,) = [e for e in logs if e["event"] == "health_check_failed"]
        assert line["check"] == DB
        assert (line["schema"], line["expects"]) == (revision, code_head())


class TestTheRefusal:
    async def test_at_the_head_it_passes(self) -> None:
        assert await refuse_foreign_schema() is None

    @pytest.mark.parametrize(
        ("read", "named"),
        [
            (lambda: _reading(_behind()), lambda: _behind()),
            (lambda: _reading(_UNKNOWN), lambda: _UNKNOWN),
            (lambda: _empty, lambda: "none"),
        ],
        ids=["behind", "unknown", "empty"],
    )
    async def test_another_ends_the_process_naming_both(
        self,
        monkeypatch: pytest.MonkeyPatch,
        read: Callable[[], Callable[[], Awaitable[str]]],
        named: Callable[[], str],
    ) -> None:
        monkeypatch.setattr(schema_module, "read_revision", read())
        with capture_logs() as logs, pytest.raises(SystemExit) as refused:
            await refuse_foreign_schema()
        text = str(refused.value.code)
        assert f"the schema is at {named()}, the code expects {code_head()}" in text
        (line,) = [e for e in logs if e["event"] == "schema_foreign"]
        assert line["expects"] == code_head()


class _StartedError(Exception):
    """What a loop raises in these tests: the process went past its check."""


async def _started(*_: Any, **__: Any) -> None:
    raise _StartedError


def _closes(monkeypatch: pytest.MonkeyPatch, module: Any) -> list[str]:
    """Record the closes of `module`'s _main instead of running them."""
    closed: list[str] = []
    for name in ("close_formatters", "dispose_engine"):
        if hasattr(module, name):
            async def close(name: str = name) -> None:
                closed.append(name)
            monkeypatch.setattr(module, name, close)
    return closed


class TestTheWorker:
    async def test_a_foreign_schema_starts_nothing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(schema_module, "read_revision", _reading(_behind()))
        monkeypatch.setattr(worker_module, "run_worker_loop", _started)
        monkeypatch.setattr(liveness, "keep_alive", _started)
        closed = _closes(monkeypatch, worker_module)
        with pytest.raises(SystemExit) as refused:
            await worker_module._main()
        assert f"the schema is at {_behind()}" in str(refused.value.code)
        assert closed == ["close_formatters", "dispose_engine"]


class TestTheConsumer:
    async def test_a_foreign_schema_starts_nothing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(schema_module, "read_revision", _reading(_UNKNOWN))
        monkeypatch.setattr(consumer_module, "run_consumer_loop", _started)
        monkeypatch.setattr(consumer_module, "run_push_relay_loop", _started)
        monkeypatch.setattr(liveness, "keep_alive", _started)
        closed = _closes(monkeypatch, consumer_module)
        with pytest.raises(SystemExit) as refused:
            await consumer_module._main()
        assert f"the schema is at {_UNKNOWN}" in str(refused.value.code)
        assert closed == ["dispose_engine"]
