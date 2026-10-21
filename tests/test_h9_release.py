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
#   I1  migration 0027 undone (an index on status alone again)
#                                        -> TestTheProbePlan, by cost (H11)
#   P1  mine's waiting delivery written before the other send's
#                                        -> TestTheProbePlan, the premise
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
from tests.conftest import analyze_with_nothing_waiting
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


def _is_probe(statement: str) -> bool:
    """The fold's probe (app/engine/service.py first_waiting_delivery,
    sent by rollup_notification): a SELECT of a delivery id ordered by
    the columns of ix_notification_deliveries_status, LIMIT 1. Until H14
    it was an EXISTS, and found by that word; a lane's selection carries
    the same subquery correlated, inside a SELECT of notifications, and
    is not the fold's."""
    return statement.lstrip().startswith(
        "SELECT notification_deliveries.id",
    ) and (
        "ORDER BY notification_deliveries.status, "
        "notification_deliveries.notification_id, notification_deliveries.id"
    ) in " ".join(statement.split())


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
                assert any(_is_probe(s) for s in seen)
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


# Two sends side by side: mine, 5000 deliveries of which ONE waits, and
# another, 5000 waiting -- written BEFORE mine's waiting one, so on any
# path that walks waiting deliveries in heap order (an index on status
# alone: equal keys sit in TID order) the other send's 5000 come first.
_MINE = UUID("00000000-0000-0000-0000-0000000000a1")
_OTHER = UUID("00000000-0000-0000-0000-0000000000b2")
# Finished sends around them, (jobs, deliveries each): the share of
# waiting deliveries in the table comes to about 10 % and about 2.6 %.
_AROUND = {"ten_percent": (8000, 5), "two_percent": (60000, 3)}
# The probe's scan, in buffers. A path bounded by the job reads the
# index's few pages and one heap page: 3-4 measured. A path through the
# waiting deliveries of every job reads past the other send's 5000: 90
# and more measured (H11 gate). The bound sits between, an order of
# magnitude from each.
_PROBE_BUFFERS = 10


async def _sends(around: tuple[int, int], statistics: str) -> None:
    jobs, each = around
    async with get_session_factory()() as session:
        await session.execute(text(
            "INSERT INTO recipients (id, locale, version, snapshot_fingerprint) "
            "SELECT gen_random_uuid(), 'en', 1, 'h11' FROM generate_series(1, 5000)"
        ))
        await session.execute(text(
            "INSERT INTO notifications (id, type, title, body, target_type, "
            "target_value, idempotency_key, fingerprint, channels, "
            "expiry_layer, push_on, retry_max_attempts, "
            "retry_backoff_seconds, recipient_schedule, lane, status, "
            "scheduled_at) "
            "SELECT id, 'unit_event', 'T', 'B', 'all', '*', key, "
            "repeat('a', 64), '[\"in_app\"]'::jsonb, 'default', 'none', 3, "
            "30, 'respect', lane, status, now() FROM (VALUES "
            "(CAST(:mine AS uuid), 'h11-mine', 'bulk', 'processing'), "
            "(CAST(:other AS uuid), 'h11-other', 'bulk', 'processing')) "
            "AS v(id, key, lane, status) "
            "UNION ALL "
            "SELECT gen_random_uuid(), 'unit_event', 'T', 'B', 'all', '*', "
            "'h11-done-' || g, repeat('a', 64), '[\"in_app\"]'::jsonb, "
            "'default', 'none', 3, 30, 'respect', 'normal', 'sent', now() "
            "FROM generate_series(1, :jobs) g"
        ), {"mine": _MINE, "other": _OTHER, "jobs": jobs})
        await session.execute(text(
            "INSERT INTO notification_deliveries "
            "(id, notification_id, recipient_id, channel, status) "
            "SELECT gen_random_uuid(), n.id, (SELECT id FROM recipients "
            "WHERE snapshot_fingerprint = 'h11' LIMIT 1), 'in_app', 'sent' "
            "FROM notifications n, generate_series(1, :each) "
            "WHERE n.idempotency_key LIKE 'h11-done-%'"
        ), {"each": each})
        if statistics == "taken_before_the_sends":
            await session.execute(text("ANALYZE notification_deliveries"))
        recipients = "FROM recipients WHERE snapshot_fingerprint = 'h11'"
        await session.execute(text(
            "INSERT INTO notification_deliveries "
            "(id, notification_id, recipient_id, channel, status) "
            f"SELECT gen_random_uuid(), :mine, id, 'in_app', 'sent' {recipients} "
            "OFFSET 1"
        ), {"mine": _MINE})
        await session.execute(text(
            "INSERT INTO notification_deliveries "
            "(id, notification_id, recipient_id, channel, status) "
            f"SELECT gen_random_uuid(), :other, id, 'in_app', 'pending' {recipients}"
        ), {"other": _OTHER})
        await session.execute(text(
            "INSERT INTO notification_deliveries "
            "(id, notification_id, recipient_id, channel, status) "
            f"SELECT gen_random_uuid(), :mine, id, 'in_app', 'pending' {recipients} "
            "LIMIT 1"
        ), {"mine": _MINE})
        if statistics == "taken_after_the_sends":
            await session.execute(text("ANALYZE notification_deliveries"))
        if statistics == "taken_with_nothing_waiting":
            await analyze_with_nothing_waiting(session)
        await session.commit()


class TestTheProbePlan:
    """H11: the fold's probe "does any delivery of this job still wait?"
    is cheap whatever the planner believes -- held by what it COSTS, on
    the order of rows that makes the costly path costly, under every
    statistics the tables meet in a send:

    - taken before the sends (the common case on a box: a send inserts
      thousands of waiting rows between two autovacuum passes, and the
      planner still believes 'pending' rare);
    - taken after them;
    - none at all: a table nobody has analyzed, as on a new box;
    - taken when nothing waited, the waiting rows back since, with as
      many dead entries beside them (H14 FIX: the statistics CI H13 met,
      tests/conftest.py analyze_with_nothing_waiting).

    On tables as on a new box (pristine_tables, tests/conftest.py, H14):
    every run starts with no dead rows, no dead index entries, no free
    space and no statistics but the ones taken here. Until H14 the third
    case was "not taken by this test (whatever the suite left)" -- right
    as a reminder that a box's statistics are older than its send, but
    not a state: in one process it was the statistics, dead entries and
    free space of whichever test ran before, and the case measured them
    (554 buffers through another index, H13 report; the premise below
    false, H12 report). The old-statistics case of a box is held by
    "taken before the sends"; "none" is the other end, made exact.

    Until H11 this test held the plan by the NAME of migration 0025's
    partial index, which was right while the planner preferred it; it
    did with fresh statistics by 2.61 against 3.03 -- a coin CI tossed
    (3.4.0, CI / image) -- and with statistics from before a send it
    went by the index on status alone and read the other send's 5000
    waiting deliveries: 92 buffers a probe. Migration 0027 left no index
    on status alone (ix_notification_deliveries_status is now (status,
    notification_id)), so no path of the probe walks other jobs' rows.
    M1 (0027 undone) turned the "before" and "not taken" rows red by
    cost, not by a name (H11; "not taken" is "none" since H14 -- M1 is
    measured again in the H14 report)."""

    @pytest.mark.usefixtures("pristine_tables")
    @pytest.mark.parametrize("around", list(_AROUND), ids=list(_AROUND))
    @pytest.mark.parametrize(
        "statistics",
        [
            "taken_before_the_sends", "taken_after_the_sends", "none",
            "taken_with_nothing_waiting",
        ],
    )
    async def test_a_handful_of_buffers_on_the_worst_order(
        self, around: str, statistics: str,
    ) -> None:
        await _sends(_AROUND[around], statistics)
        async with get_session_factory()() as session:
            # The premise, checked: mine's waiting delivery lies after
            # every waiting delivery of the other send in the heap.
            behind = (await session.execute(text(
                "SELECT (SELECT ctid FROM notification_deliveries "
                "WHERE notification_id = :mine AND status = 'pending') > "
                "(SELECT max(ctid) FROM notification_deliveries "
                "WHERE notification_id = :other AND status = 'pending')"
            ), {"mine": _MINE, "other": _OTHER})).scalar_one()
            assert behind is True
            mine = await session.get(Notification, _MINE)
            assert mine is not None
            with _statements() as seen:
                await rollup_notification(session, mine)
            assert mine.status == NotificationStatus.PROCESSING  # one waits
            (probe,) = [s for s in seen if _is_probe(s)]
            raw = await session.connection()
            plan = (await raw.exec_driver_sql(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + probe,
                (_MINE, DeliveryStatus.PENDING.value, 1),
            )).scalar_one()
            await session.rollback()
        top = plan[0]["Plan"]
        read = top["Shared Hit Blocks"] + top.get("Shared Read Blocks", 0)
        assert read <= _PROBE_BUFFERS, json.dumps(plan)
        assert top["Actual Rows"] == 1  # the pair: the probe answered


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
    """The number in code says what it is, like 3.1.0-3.4.0; 3.4.0 stays
    described as phase 4. This test once pinned APP_VERSION == "3.4.0" --
    true for that release, and false for every release after it: a pin on
    the number itself is replaced by the rule it stood for -- the comment
    above the number names the CURRENT number.
    (APP_VERSION == pyproject's version: tests/test_delivery_contract.py.)"""
    version = config_module.APP_VERSION
    source = inspect.getsource(config_module)
    comment = source[: source.index(f'APP_VERSION = "{version}"')]
    assert f"{version} is" in comment
    assert re.search(r"3\.4\.0 is\s+#?\s*phase 4", comment)
    for part in ("lanes", "H7", "H8"):
        assert part in comment.rsplit("3.4.0 is", 1)[1]
