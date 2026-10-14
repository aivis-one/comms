# =============================================================================
# H9 -- the package graph, the fold without a full read, a partial lane
# set said aloud, the number 3.4.0
# =============================================================================
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  a module of core imports from app.profile        -> TestThePackageGraph
#   M2  `import app.engine...` in core (the other form)   -> TestThePackageGraph
#   M3  Lane imported from anywhere but app.core.lanes    -> TestThePackageGraph
#   M4  the fold's "still waiting" probe removed          -> TestTheFold
#   M5  the probe asks for any delivery, not a waiting one -> TestTheFold
#   M6  the warning line removed                          -> TestPartialLanes
#   M7  the warning written for the full set too          -> TestPartialLanes
#   I1  migration 0025's index gone (downgrade -1)        -> TestTheProbePlan
# =============================================================================

import ast
import asyncio
import inspect
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest
from sqlalchemy import event, text
from structlog.testing import capture_logs

import app.core.config as config_module
import app.worker as worker
from app.core.config import settings
from app.core.database import get_engine, get_session_factory
from app.core.lanes import Lane
from app.engine.constants import DeliveryStatus, NotificationStatus, TargetType
from app.engine.formatters import MessageRejectedError
from app.engine.models import Notification
from app.engine.processor import process_lane
from app.engine.service import create_notification, rollup_notification
from app.profile.registry import Decided, Layer, TypeRecord, registry
from tests.helpers import create_recipient, intake_fields

_REPO = Path(__file__).resolve().parents[1]
_APP = _REPO / "app"

# The package graph (app/__init__.py): an import goes to a strictly
# lower rank, or stays inside its own package.
_RANKS: dict[str, int] = {
    "core": 0,
    "profile": 1,
    "audience": 2, "messaging": 2,
    "engine": 3,
    "forgetting": 4, "notifier": 4,
    "api": 5, "transport": 5,
    "main": 6, "worker": 6, "consumer": 6,
}


# -----------------------------------------------------------------------------
# 1 -- the package graph
# -----------------------------------------------------------------------------


def _unit(path: Path) -> str:
    """The package or top module a file of app/ belongs to."""
    parts = path.relative_to(_APP).with_suffix("").parts
    return parts[0]


def _module_of(path: Path) -> list[str]:
    return ["app", *path.relative_to(_APP).with_suffix("").parts]


def _imports(path: Path) -> list[tuple[int, str]]:
    """Every app module `path` imports, in every form: `from app.x
    import`, `import app.x [as y]`, relative imports (resolved), and
    importlib.import_module / __import__ with a literal -- under
    TYPE_CHECKING too: an edge is an edge."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    here = _module_of(path)
    package = here if path.name == "__init__.py" else here[:-1]
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level:
                base = package[: len(package) - node.level + 1]
                module = ".".join(base + ([node.module] if node.module else []))
            else:
                module = node.module or ""
            found.append((node.lineno, module))
        elif isinstance(node, ast.Import):
            found += [(node.lineno, alias.name) for alias in node.names]
        elif (
            isinstance(node, ast.Call)
            and ast.unparse(node.func) in (
                "importlib.import_module", "import_module", "__import__",
            )
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            found.append((node.lineno, node.args[0].value))
    return [(line, module) for line, module in found if module.startswith("app.")]


def _sources() -> list[Path]:
    return [
        path for path in sorted(_APP.rglob("*.py"))
        if path != _APP / "__init__.py"
    ]


class TestThePackageGraph:
    """done-when 1. M1-M3."""

    def test_every_unit_has_a_rank(self) -> None:
        """A new package or top module fails here until it is ranked."""
        units = {_unit(path) for path in _sources()}
        assert units == set(_RANKS), sorted(units ^ set(_RANKS))

    def test_every_import_goes_down(self) -> None:
        upward = []
        edges = 0
        for path in _sources():
            source = _unit(path)
            for line, module in _imports(path):
                target = module.split(".")[1]
                if target == source:
                    continue
                edges += 1
                if _RANKS.get(target, 99) >= _RANKS[source]:
                    upward.append(
                        f"{path.relative_to(_REPO)}:{line}: {source} -> {module}"
                    )
        assert upward == []
        # The pair: the walk sees the graph -- and, in core, imports.
        assert edges > 50
        assert any(_imports(path) for path in (_APP / "core").glob("*.py"))

    def test_core_imports_nothing_above_itself(self) -> None:
        above = [
            f"{path.name}:{line}: {module}"
            for path in sorted((_APP / "core").glob("*.py"))
            for line, module in _imports(path)
            if module.split(".")[1] != "core"
        ]
        assert above == []

    def test_lane_has_one_home(self) -> None:
        """Lane is imported from app.core.lanes and nowhere else -- in
        app/ and in the tests: no second door through a module that
        happens to bind the name."""
        wrong: list[str] = []
        homes = 0
        for root in (_APP, _REPO / "tests"):
            for path in sorted(root.rglob("*.py")):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom) and any(
                        alias.name == "Lane" for alias in node.names
                    ):
                        if node.module == "app.core.lanes":
                            homes += 1
                        else:
                            wrong.append(f"{path.name}:{node.lineno}: {node.module}")
                    if isinstance(node, ast.Attribute) and node.attr == "Lane":
                        wrong.append(f"{path.name}:{node.lineno}: {ast.unparse(node)}")
        assert wrong == []
        assert homes >= 10


# -----------------------------------------------------------------------------
# 2 -- the fold reads the set only once nothing waits
# -----------------------------------------------------------------------------


def _bulk(type_key: str) -> None:
    record = registry.record_of(type_key)
    assert record is not None
    fields = dict(record.fields)
    fields["lane"] = Decided(Lane.BULK.value, Layer.PROFILE, "test")
    registry.register_type(
        type_key, category=registry.category_of(type_key),
        record=TypeRecord(fields=fields),
    )


async def _send(people: int) -> UUID:
    _bulk("unit_event_in_app")
    async with get_session_factory()() as session:
        for _ in range(people):
            await create_recipient(session)
        job = await create_notification(
            session, **intake_fields(), type="unit_event_in_app",
            title="T", body="B", target_type=TargetType.ALL, target_value="*",
        )
        await session.commit()
        return job.id


@contextmanager
def _statements() -> Iterator[list[str]]:
    seen: list[str] = []
    engine = get_engine().sync_engine

    def record(conn: Any, cursor: Any, statement: str, *args: Any) -> None:
        seen.append(statement)

    event.listen(engine, "before_cursor_execute", record)
    try:
        yield seen
    finally:
        event.remove(engine, "before_cursor_execute", record)


def _set_reads(statements: list[str]) -> int:
    return sum(
        1 for s in statements
        if s.lstrip().startswith("SELECT DISTINCT notification_deliveries.status")
    )


class _Channel:
    def __init__(self, refuse: int | None = None) -> None:
        self.calls = 0
        self.refuse = refuse

    async def deliver(self, *args: Any) -> bool:
        self.calls += 1
        if self.calls == self.refuse:
            raise MessageRejectedError("provider rejected the message")
        return True


async def _status(nid: UUID) -> str:
    async with get_session_factory()() as session:
        job = await session.get(Notification, nid)
        assert job is not None
        return job.status


class TestTheFold:
    """done-when 2. M4, M5."""

    @pytest.mark.parametrize(
        ("refuse", "outcome"),
        [(None, NotificationStatus.SENT), (2, NotificationStatus.PARTIAL_SENT)],
        ids=["all_sent", "one_refused"],
    )
    async def test_the_set_is_read_once_after_the_last(
        self, refuse: int | None, outcome: str,
    ) -> None:
        nid = await _send(3)
        reads: list[int] = []
        with patch(
            "app.engine.service.get_formatter", return_value=_Channel(refuse),
        ):
            for _ in range(3):
                with _statements() as seen:
                    await process_lane(Lane.BULK)
                reads.append(_set_reads(seen))
                # The pair: the pass did fold-or-wait -- the probe ran.
                assert any("EXISTS" in s for s in seen)
        assert reads == [0, 0, 1]
        assert await _status(nid) == outcome

    async def test_no_delivery_at_all_reads_the_set_once(self) -> None:
        """EMPTY: a job resolved to nobody -- the probe says none waits,
        the set (empty) is read once, the outcome is no_recipients."""
        async with get_session_factory()() as session:
            job = await create_notification(
                session, **intake_fields(), type="unit_event_in_app",
                title="T", body="B", target_type=TargetType.ALL,
                target_value="*",
            )
            job.status = NotificationStatus.PROCESSING
            await session.flush()
            with _statements() as seen:
                await rollup_notification(session, job)
            await session.commit()
        assert _set_reads(seen) == 1
        assert await _status(job.id) == NotificationStatus.NO_RECIPIENTS


class TestTheProbePlan:
    """правка 1 of the H9 gate: the probe the fold sends, captured and
    explained with ANALYZE, on a job of 5000 deliveries of which one
    waits, beside another send with 5000 waiting, among 20 000 finished
    jobs (40 000 finished deliveries) -- by the pending index, a handful
    of buffers.
    I1."""

    async def test_by_the_pending_index(self) -> None:
        async with get_session_factory()() as session:
            await session.execute(text(
                "INSERT INTO recipients (id, locale, version, snapshot_fingerprint) "
                "SELECT gen_random_uuid(), 'en', 1, 'h9' FROM generate_series(1, 5000)"
            ))
            jobs: list[UUID] = []
            for key in ("h9-probe-mine", "h9-probe-other"):
                job = await create_notification(
                    session, **{**intake_fields(), "idempotency_key": key},
                    type="unit_event_in_app", title="T", body="B",
                    target_type=TargetType.ALL, target_value="*",
                )
                job.status = NotificationStatus.PROCESSING
                jobs.append(job.id)
            await session.flush()
            for job_id in jobs:
                await session.execute(text(
                    "INSERT INTO notification_deliveries "
                    "(id, notification_id, recipient_id, channel, status) "
                    "SELECT gen_random_uuid(), :j, id, 'in_app', 'pending' "
                    "FROM recipients WHERE snapshot_fingerprint = 'h9'"
                ), {"j": job_id})
            # The table around them, as in life: a fortnight of finished
            # sends -- 20 000 jobs, two deliveries each. On a table that
            # held only these two sends the planner would take every
            # job for a coin toss between them and expect thousands of
            # matches; that is not the table the probe runs on. What it
            # needs is the DISTINCTNESS of notification_id (20 000 jobs),
            # not the row count: two deliveries a job give the planner
            # the same n_distinct as ten at a fifth of the seed (review
            # of 3.4.0: the call went from ~2 minutes to ~2 seconds).
            await session.execute(text(
                "INSERT INTO notifications (id, type, title, body, target_type, "
                "target_value, idempotency_key, fingerprint, channels, "
                "expiry_layer, push_on, retry_max_attempts, "
                "retry_backoff_seconds, recipient_schedule, lane, status, "
                "scheduled_at) "
                "SELECT gen_random_uuid(), 'unit_event', 'T', 'B', 'all', '*', "
                "'h9-done-' || g, repeat('a', 64), '[\"in_app\"]'::jsonb, "
                "'default', 'none', 3, 30, 'respect', 'normal', 'sent', now() "
                "FROM generate_series(1, 20000) g"
            ))
            await session.execute(text(
                "INSERT INTO notification_deliveries "
                "(id, notification_id, recipient_id, channel, status) "
                "SELECT gen_random_uuid(), n.id, (SELECT id FROM recipients "
                "WHERE snapshot_fingerprint = 'h9' LIMIT 1), 'in_app', 'sent' "
                "FROM notifications n, generate_series(1, 2) "
                "WHERE n.idempotency_key LIKE 'h9-done-%'"
            ))
            # Mine: all but one done.
            await session.execute(text(
                "UPDATE notification_deliveries SET status = 'sent' "
                "WHERE notification_id = :j AND id <> (SELECT id FROM "
                "notification_deliveries WHERE notification_id = :j "
                "ORDER BY id DESC LIMIT 1)"
            ), {"j": jobs[0]})
            await session.commit()
        # The 4999 deliveries just marked done leave dead entries in the
        # partial index; in a send they die one per portion and the next
        # probe marks each dead as it passes (btree kill_prior_tuple), so
        # a probe meets a handful. Autovacuum's pass stands in for that
        # history here; VACUUM runs outside a transaction.
        async with get_engine().connect() as connection:
            autocommit = await connection.execution_options(
                isolation_level="AUTOCOMMIT",
            )
            await autocommit.execute(text("VACUUM ANALYZE notification_deliveries"))
        async with get_session_factory()() as session:
            mine = await session.get(Notification, jobs[0])
            assert mine is not None
            with _statements() as seen:
                await rollup_notification(session, mine)
            assert mine.status == NotificationStatus.PROCESSING  # one waits
            (probe,) = [s for s in seen if "EXISTS" in s]
            raw = await session.connection()
            plan = (await raw.exec_driver_sql(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + probe,
                (jobs[0], DeliveryStatus.PENDING.value),
            )).scalar_one()
            await session.rollback()
        flat = json.dumps(plan)
        assert "ix_notification_deliveries_pending" in flat, flat
        assert "Seq Scan" not in flat
        top = plan[0]["Plan"]
        assert top["Shared Hit Blocks"] + top.get("Shared Read Blocks", 0) <= 10


# -----------------------------------------------------------------------------
# 3 -- a partial lane set is said aloud
# -----------------------------------------------------------------------------


async def _started(monkeypatch: pytest.MonkeyPatch, value: str | None) -> list[Any]:
    """The worker's start, its loops replaced by ones that end at once;
    the log lines it wrote."""
    async def loop(*args: Any, **kwargs: Any) -> None:
        return None

    async def listen(events: Any) -> None:
        return None

    monkeypatch.setattr(worker, "_poll_loop", loop)
    monkeypatch.setattr(worker, "listen_for_wakes", listen)
    monkeypatch.setattr(settings, "comms_worker_lanes", value)
    with capture_logs() as logs:
        await asyncio.wait_for(worker.run_worker_loop(), 10)
    return [entry for entry in logs if entry["event"] == "worker_lanes_partial"]


class TestPartialLanes:
    """done-when 3. M6, M7."""

    @pytest.mark.parametrize(
        ("value", "not_taken"),
        [
            ("critical", ["interactive", "normal", "bulk"]),
            ("interactive,normal,bulk", ["critical"]),
        ],
        ids=["one", "three"],
    )
    async def test_a_subset_warns_once_naming_the_rest(
        self, monkeypatch: pytest.MonkeyPatch, value: str, not_taken: list[str],
    ) -> None:
        (line,) = await _started(monkeypatch, value)
        assert line["log_level"] == "warning"
        assert line["lanes_not_taken"] == not_taken
        assert "GET /api/v1/lanes/depth" in line["hint"]
        assert "another worker process" in line["hint"]

    @pytest.mark.parametrize(
        "value", [None, "bulk,normal,critical,interactive"], ids=["unset", "all"],
    )
    async def test_the_full_set_says_nothing(
        self, monkeypatch: pytest.MonkeyPatch, value: str | None,
    ) -> None:
        assert await _started(monkeypatch, value) == []


# -----------------------------------------------------------------------------
# 4 -- the number
# -----------------------------------------------------------------------------


def test_the_number_and_its_line() -> None:
    """3.4.0 in code; its comment says what 3.4.0 is, like 3.1.0-3.3.0.
    (APP_VERSION == pyproject's version: tests/test_delivery_contract.py.)"""
    assert config_module.APP_VERSION == "3.4.0"
    source = inspect.getsource(config_module)
    comment = source[: source.index('APP_VERSION = "3.4.0"')]
    assert re.search(r"3\.4\.0 is\s+#?\s*phase 4", comment)
    for part in ("lanes", "H7", "H8"):
        assert part in comment.rsplit("3.4.0 is", 1)[1]
