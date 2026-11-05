# =============================================================================
# COMMS Service -- Migration 0037: when a transient wait began (TX-3)
# =============================================================================
# upgrade -> downgrade -> upgrade through the alembic CLI, over a job that
# existed before the column: up adds the column (NULL on old rows) and
# its CHECKs and replaces 0015's pipeline gate; down removes exactly
# those and puts 0015's gate back; up again restores them -- definitions
# compared, not names.
#
# AT 0037, NOT AT HEAD (WK-2): 0038 removes what 0037 adds, so this file
# holds 0037 at its own revision (_0037) -- up to it, down from it.
# Before 0038, head WAS 0037 and "upgrade head" said the same thing.
# The fixture still leaves the database at head.
# =============================================================================

import asyncio
import itertools
import re
import subprocess
import sys
import time
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from app.core.database import dispose_engine, get_engine, get_session_factory

REPO_ROOT = Path(__file__).resolve().parents[1]
_BEFORE = "0036_notification_email"
_0037 = "0037_pipeline_transient_since"


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )


async def _migrate(*args: str) -> None:
    await dispose_engine()
    done = _alembic(*args)
    await dispose_engine()
    assert done.returncode == 0, done.stderr


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    yield
    await _migrate("upgrade", "head")


async def _scalar(statement: str) -> Any:
    async with get_session_factory()() as session:
        return await session.scalar(text(statement))


_CHECKS = (
    "ck_notifications_transient_wait_whole",
    "ck_notifications_transient_wait_gated",
    "ck_notifications_transient_wait_open",
)


async def _definition(name: str) -> str | None:
    return await _scalar(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        f"WHERE conname = '{name}'"
    )


async def _objects() -> dict[str, Any]:
    return {
        "columns": await _scalar(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = "
            "'notifications' AND column_name LIKE 'pipeline_transient_%'"
        ),
        **{name: await _definition(name) for name in _CHECKS},
        "gate": await _definition("ck_notifications_pipeline_gate"),
    }


_GATE_0015 = "CHECK (((pipeline_retry_at IS NULL) OR (pipeline_attempts > 0)))"
_GATE_0037 = (
    "CHECK (((pipeline_retry_at IS NULL) OR (pipeline_attempts > 0) "
    "OR (pipeline_transient_since IS NOT NULL)))"
)


def _before(found: dict[str, Any]) -> None:
    assert found["columns"] == 0
    assert all(found[name] is None for name in _CHECKS)
    assert found["gate"] == _GATE_0015


def _after(found: dict[str, Any]) -> None:
    assert found["columns"] == 3
    assert "pipeline_transient_step" in found["ck_notifications_transient_wait_whole"]
    assert "'commit'" in found["ck_notifications_transient_wait_whole"]
    assert found["ck_notifications_transient_wait_gated"] == (
        "CHECK (((pipeline_transient_since IS NULL) OR "
        "(pipeline_retry_at IS NOT NULL)))"
    )
    assert "'processing'" in found["ck_notifications_transient_wait_open"]
    assert found["gate"] == _GATE_0037


async def test_up_down_up(at_head_afterwards: None) -> None:
    await _migrate("downgrade", _0037)
    _after(await _objects())
    await _migrate("downgrade", _BEFORE)
    _before(await _objects())
    await _migrate("upgrade", _0037)
    _after(await _objects())
    await _migrate("downgrade", _BEFORE)
    _before(await _objects())
    await _migrate("upgrade", _0037)
    _after(await _objects())


# ---------------------------------------------------------------------------
# FIX TX-3 -- no pass over the table under ACCESS EXCLUSIVE
# ---------------------------------------------------------------------------

_CONSTRAINTS_0037 = (*_CHECKS, "ck_notifications_pipeline_gate")


async def _validated() -> dict[str, bool]:
    async with get_session_factory()() as session:
        rows = await session.execute(text(
            "SELECT conname, convalidated FROM pg_constraint "
            "WHERE conname = ANY(:names)"
        ), {"names": list(_CONSTRAINTS_0037)})
        return {name: valid for name, valid in rows.all()}


# A job as revision 0037 holds it, by an explicit INSERT of that
# revision's columns -- never through the current ORM model, which carries
# the columns of later revisions (FIX TPL-1 #1: 0040's
# notification_transitions.template_unresolved reached this schema through
# create_notification's journal row). No journal row: the tests read the
# job's gate and wait only.
_JOB_AT_0037 = (
    "INSERT INTO notifications (id, type, title, body, target_type, "
    "target_value, idempotency_key, fingerprint, channels, expiry_layer, "
    "push_on, retry_max_attempts, retry_backoff_seconds, recipient_schedule, "
    "lane) VALUES (:id, 'unit_event_in_app', 'T', 'B', 'all', '*', :key, "
    ":fp, '[\"in_app\"]'::jsonb, 'default', 'none', 3, 0, 'respect', "
    "'normal')"
)


async def _job_at_0037() -> UUID:
    """One pending job written in the form of revision 0037."""
    job_id = uuid4()
    async with get_session_factory()() as session:
        await session.execute(
            text(_JOB_AT_0037),
            {"id": job_id, "key": f"m0037:{job_id}", "fp": "a" * 64},
        )
        await session.commit()
    return job_id


async def _execute(*statements: str) -> None:
    async with get_session_factory()() as session:
        for statement in statements:
            await session.execute(text(statement))
        await session.commit()


async def test_every_constraint_is_validated(at_head_afterwards: None) -> None:
    """Added NOT VALID, then VALIDATEd: at head every one is valid."""
    await _migrate("downgrade", _BEFORE)
    await _migrate("upgrade", _0037)
    assert await _validated() == dict.fromkeys(_CONSTRAINTS_0037, True)


async def test_a_half_applied_run_goes_through(at_head_afterwards: None) -> None:
    """A VALIDATE that failed leaves the columns and the NOT VALID
    constraints committed while alembic_version still says 0036 (the
    stamp comes after upgrade()). The next upgrade -- comms-app's restart
    runs it -- goes through: idempotent up to the autocommit block."""
    await _migrate("downgrade", _BEFORE)
    await _execute(
        "ALTER TABLE notifications ADD COLUMN pipeline_transient_since "
        "timestamptz, ADD COLUMN pipeline_transient_step varchar(10), "
        "ADD COLUMN pipeline_transient_error varchar(300)",
        "ALTER TABLE notifications ADD CONSTRAINT "
        "ck_notifications_transient_wait_gated CHECK "
        "(pipeline_transient_since IS NULL OR pipeline_retry_at IS NOT NULL) "
        "NOT VALID",
    )
    await _migrate("upgrade", _0037)
    assert await _validated() == dict.fromkeys(_CONSTRAINTS_0037, True)
    _after(await _objects())


async def test_down_with_an_open_wait_at_zero_attempts(
    at_head_afterwards: None,
) -> None:
    """A wait at zero attempts holds a gate 0015's CHECK refuses: down
    takes that gate away with the wait, first, and passes. (A CLOSED job
    holding a wait's fields cannot be built: ck_notifications_transient_
    wait_open refuses it -- so it is not a case down meets.)"""
    await _migrate("downgrade", _0037)
    job_id = await _job_at_0037()
    await _execute(
        "UPDATE notifications SET pipeline_retry_at = now(), "
        "pipeline_transient_since = now(), pipeline_transient_step = 'rollup', "
        "pipeline_transient_error = 'sqlalchemy.exc.DBAPIError at "
        f"app/engine/processor.py:1' WHERE id = '{job_id}'",
    )
    await _migrate("downgrade", _BEFORE)
    _before(await _objects())
    gate = await _scalar(
        f"SELECT pipeline_retry_at FROM notifications WHERE id = '{job_id}'"
    )
    assert gate is None


_BLOATED_ROWS, _LIVE_EVERY = 460_000, 42


async def _bloat(rows: int) -> int:
    """notifications as velo-test had it: hundreds of MB of heap for ~11
    thousand live rows -- inserted, the rest deleted, VACUUMed (not
    FULL). Returns its heap pages. Autovacuum is left OFF on the table
    (MEAS-1): the test turns it back on after both measures
    (_autovacuum_back) -- an autoanalyze of the hundreds of thousands of
    rows just changed, met by 0037's ALTER, makes the ALTER wait up to
    deadlock_timeout before it cancels the worker, and every write waits
    in the queue behind it."""
    engine = get_engine()
    async with engine.connect() as connection:
        auto = await connection.execution_options(isolation_level="AUTOCOMMIT")
        await auto.execute(text(
            "ALTER TABLE notifications SET (autovacuum_enabled = false)"))
        await auto.execute(text(
            "INSERT INTO notifications (id, type, title, body, target_type, "
            "target_value, scheduled_at, status, created_at, idempotency_key, "
            "fingerprint, channels, expiry_layer, pipeline_attempts, push_on, "
            "retry_max_attempts, retry_backoff_seconds, recipient_schedule, "
            "lane) SELECT gen_random_uuid(), 'unit_event_in_app', 'T', "
            "repeat('b', 900), 'all', '*', now(), CASE WHEN g % :every = 0 "
            "THEN 'pending' ELSE 'sent' END, now(), 'bloat:' || g, "
            "repeat('f', 64), '[\"in_app\"]', 'none', 0, 'none', 3, 30, "
            "'respect', 'normal' FROM generate_series(1, :rows) g"
        ), {"rows": rows, "every": _LIVE_EVERY})
        await auto.execute(text("DELETE FROM notifications WHERE status = 'sent'"))
        # PARALLEL 0: notifications has six indexes, and a parallel
        # VACUUM keeps its dead-row list in dynamic shared memory, sized
        # by maintenance_work_mem (64 MB) -- in /dev/shm, which a Docker
        # container (CI's postgres service) has at 64 MB: the segment did
        # not fit (DiskFullError). Without workers the list lives in this
        # backend's own memory; the heap is cleaned the same.
        await auto.execute(text("VACUUM (PARALLEL 0) notifications"))
        pages = await auto.scalar(text(
            "SELECT pg_relation_size('notifications') / 8192"))
    return int(pages)


async def _autovacuum_back() -> None:
    await _execute("ALTER TABLE notifications RESET (autovacuum_enabled)")


# One look at the holder of a granted ACCESS EXCLUSIVE on notifications:
# the SERVER's clock, and what the holder is doing (pg_stat_activity).
_LOOK = text(
    "SELECT clock_timestamp(), a.state, a.wait_event_type, a.wait_event, "
    "left(a.query, 40) FROM pg_locks l "
    "JOIN pg_class c ON c.oid = l.relation "
    "JOIN pg_stat_activity a ON a.pid = l.pid "
    "WHERE c.relname = 'notifications' "
    "AND l.mode = 'AccessExclusiveLock' AND l.granted "
    "AND l.database = (SELECT oid FROM pg_database "
    "WHERE datname = current_database())"
)

# A look: (server time, holder's state, wait_event_type, wait_event,
# its statement) -- or (server time, None, ...) when no one holds it.
_Look = tuple[Any, str | None, str | None, str | None, str | None]


async def _look(observer: Any) -> list[_Look]:
    """One look (the observer's connection); every holder's row, or one
    row with no holder carrying the server's clock."""
    rows = [tuple(r) for r in (await observer.execute(_LOOK)).all()]
    if not rows:
        now = await observer.scalar(text("SELECT clock_timestamp()"))
        rows = [(now, None, None, None, None)]
    await observer.rollback()
    return rows  # type: ignore[return-value]


def _busy(look: _Look) -> bool:
    """The holder is doing the migration's own work in the server: a
    statement running (`active`), not waiting for its client and not a
    COMMIT's WAL fsync."""
    _, state, kind, _event, query = look
    return (
        state == "active"
        and kind != "Client"
        and not (query or "").lstrip().upper().startswith("COMMIT")
    )


def _work(looks: list[_Look]) -> float:
    """Server seconds the holder spent working under the lock: every
    interval between two consecutive looks that BOTH saw it busy. A
    pause of this process between two looks is no time of the server's
    to it unless the server worked through it; an interval at the edge
    of a busy run is left out (at most one look's gap, under)."""
    total = 0.0
    for earlier, later in itertools.pairwise(looks):
        if _busy(earlier) and _busy(later):
            total += (later[0] - earlier[0]).total_seconds()
    return total


def _phase(looks: list[_Look]) -> str:
    """What the phase was made of: its span on the server's clock and the
    holder's states, counted -- said by a red assertion."""
    held = [look for look in looks if look[1] is not None]
    if not held:
        return "no look saw the lock"
    span = (held[-1][0] - held[0][0]).total_seconds()
    states: dict[str, int] = {}
    for _, state, kind, event, query in held:
        key = f"{state}/{kind}/{event}" + (
            " COMMIT" if (query or "").lstrip().upper().startswith("COMMIT") else ""
        )
        states[key] = states.get(key, 0) + 1
    return f"held {span:.3f}s on the server, {len(held)} looks: {states}"


class _Measure:
    """One measure of 0036 -> 0037: the migration's work under the lock,
    the longest write, the migration's whole time, what the phase was."""

    def __init__(self, looks: list[_Look], writes: list[float], took: float) -> None:
        self.looks = looks
        self.work = _work(looks)
        self.write = max(writes, default=0.0)
        self.took = took
        self.seen = sum(1 for look in looks if look[1] is not None)
        self.phase = _phase(looks)


async def _measure() -> _Measure:
    """Run 0036 -> 0037 through the CLI; meanwhile look at the holder of
    a granted ACCESS EXCLUSIVE on notifications without pause, and write
    a job row in a loop, each write's wait taken by the server inside
    its own statement (RETURNING clock_timestamp() -
    statement_timestamp()): a pause of this process is in neither."""
    await _migrate("downgrade", _BEFORE)
    looks: list[_Look] = []
    writes: list[float] = []
    stop = asyncio.Event()

    async def watch() -> None:
        async with get_engine().connect() as observer:
            while not stop.is_set():
                looks.extend(await _look(observer))

    async def write() -> None:
        async with get_engine().connect() as writer:
            while not stop.is_set():
                waited = await writer.scalar(text(
                    "UPDATE notifications SET body = body WHERE id = "
                    "(SELECT id FROM notifications LIMIT 1) RETURNING "
                    "extract(epoch FROM clock_timestamp() - statement_timestamp())"))
                await writer.commit()
                writes.append(float(waited))
                await asyncio.sleep(0.02)

    tasks = [asyncio.ensure_future(watch()), asyncio.ensure_future(write())]
    await asyncio.sleep(0.2)
    started = time.monotonic()
    await dispose_engine()
    done = await asyncio.to_thread(_alembic, "upgrade", _0037)
    took = time.monotonic() - started
    stop.set()
    await asyncio.gather(*tasks)
    assert done.returncode == 0, done.stderr
    return _Measure(looks, writes, took)


@pytest.mark.usefixtures("pristine_tables")
async def test_the_lock_does_not_grow_with_the_heap(
    at_head_afterwards: None,
) -> None:
    """THE RULE, written before the measure: under ACCESS EXCLUSIVE on
    notifications 0037 does no work that grows with the heap, and works
    under it 1 s at most -- no longer on a bloated heap than on a small
    one. Measured on ~600 MB of heap with ~11 thousand live rows
    (velo-test before its VACUUM FULL) and on the same rows without the
    bloat. The migration's whole time is printed too: comms' API does
    not serve during it (comms-app migrates before uvicorn).

    WHAT IS COUNTED (MEAS-1): the server's seconds in which the holder of
    the lock runs a statement of its own (state `active`, not waiting
    for its client, not a COMMIT) -- the work the migration does under
    the lock; a statement that reads the heap (a CHECK added without
    NOT VALID) is counted whole. WHAT IS NOT: the holder waiting for the
    next statement from its own client (`ClientRead`, `idle in
    transaction` -- the alembic process between its statements) and
    the WAL fsync of the COMMIT. Both are the machine, not the
    migration: on a slow runner the alembic process or the disk stalls,
    the lock is held longer, and the old measure -- the client's clock
    between the first and the last look that saw the lock -- went red on
    CI with 0.47 s and 0.28 s, where the server, measured here, showed a
    phase of 10-15 ms on either heap, the holder in ClientRead and WAL
    I/O. The time is the server's (clock_timestamp()): a pause of this
    test's own process is not in it. The writes too: each write's wait
    is taken inside its statement; autovacuum is off on the table for
    the whole of it (_bloat, _autovacuum_back) -- an autoanalyze met by
    the ALTER makes the writes wait deadlock_timeout (1 s).
    A red assertion says what the phase was made of (_phase)."""
    try:
        small = await _bloat(_BLOATED_ROWS // 42)
        before = await _measure()
        await _execute("TRUNCATE notifications CASCADE")
        pages = await _bloat(_BLOATED_ROWS)
        bloated = await _measure()
    finally:
        await _autovacuum_back()
    print(f"\nsmall: {small} pages, work {before.work:.3f}s, write max "
          f"{before.write:.3f}s, migration {before.took:.2f}s; {before.phase}"
          f"\nbloated: {pages} pages, work {bloated.work:.3f}s, write max "
          f"{bloated.write:.3f}s, migration {bloated.took:.2f}s; {bloated.phase}")
    assert pages > 60_000, pages  # the instrument: the heap IS bloated
    # the instrument: the looks saw the lock at all, on either heap
    assert before.seen > 0 and bloated.seen > 0, (before.phase, bloated.phase)
    assert bloated.work <= 1.0, bloated.phase
    assert bloated.work - before.work <= 0.2, (bloated.phase, before.phase)
    assert bloated.write <= 1.0, bloated.write  # writes went on through the VALIDATEs


# ---------------------------------------------------------------------------
# FIX TX-3 -- the stamp and the schema agree, in both directions
# ---------------------------------------------------------------------------

_0035 = "0035_thread_revision"


async def _state() -> tuple[str, bool, bool]:
    """(alembic_version, the wait's columns there, 0036's email there)."""
    version = await _scalar("SELECT version_num FROM alembic_version")
    wait = await _scalar(
        "SELECT count(*) FROM information_schema.columns WHERE table_name = "
        "'notifications' AND column_name = 'pipeline_transient_since'")
    email = await _scalar(
        "SELECT count(*) FROM information_schema.columns WHERE table_name = "
        "'notifications' AND column_name = 'email'")
    return str(version), bool(wait), bool(email)


_HEAD = ("0037_pipeline_transient_since", True, True)


async def test_a_downgrade_that_fails_later_keeps_stamp_and_schema_together(
    at_head_afterwards: None,
) -> None:
    """One downgrade through 0037 to 0035, where 0036's step fails (a view
    depends on the column it drops): 0037's step must roll back with it,
    so the stamp and the schema stay at head together. With an
    autocommit_block in 0037's downgrade, its undoing was committed while
    the stamp rolled back to 0037 -- the stamp ahead of the schema."""
    await _migrate("downgrade", _0037)
    await _execute("CREATE VIEW zz_holds_email AS SELECT email FROM notifications")
    try:
        await dispose_engine()
        failed = _alembic("downgrade", _0035)
        await dispose_engine()
        assert failed.returncode != 0
        assert await _state() == _HEAD
    finally:
        await _execute("DROP VIEW IF EXISTS zz_holds_email")
    # every step, one at a time, down and back up
    for target, expected in (
        ("-1", ("0036_notification_email", False, True)),
        ("-1", (_0035, False, False)),
    ):
        await _migrate("downgrade", target)
        assert await _state() == expected
    for target, expected in (
        ("+1", ("0036_notification_email", False, True)),
        ("+1", _HEAD),
    ):
        await _migrate("upgrade", target)
        assert await _state() == expected
    assert await _validated() == dict.fromkeys(_CONSTRAINTS_0037, True)


def _with_a_block() -> list[Path]:
    from tests.test_migration_safety import _later_migrations, uses_a_block

    return [path for path in _later_migrations() if uses_a_block(path)]


def _revisions(path: Path) -> tuple[str, str]:
    module_text = path.read_text(encoding="utf-8")
    revision = re.search(r'^revision: str = "([^"]+)"', module_text, re.M)
    previous = re.search(r'^down_revision: str \| None = "([^"]+)"', module_text,
                         re.M)
    assert revision and previous
    return revision.group(1), previous.group(1)


@pytest.mark.parametrize("path", _with_a_block(), ids=lambda p: p.name)
async def test_upgrade_runs_again_after_its_stamp_was_left_behind(
    at_head_afterwards: None, path: Path,
) -> None:
    """An upgrade() with an autocommit_block commits its first part before
    alembic stamps it: when a later step of the same run fails, the stamp
    goes back to the revision before while the schema stays. Upgrading
    again must go through, to the same schema, every constraint valid.
    The database is brought to `revision` itself from head, down as well
    as up (WK-2): with 0038 after it, "upgrade 0037" at head moved
    nothing and the run compared head's schema with 0037's."""
    revision, previous = _revisions(path)
    await _migrate("upgrade", "head")
    await _migrate("downgrade", revision)
    before = await _objects()
    await _migrate("stamp", previous)  # the stamp left behind
    await _migrate("upgrade", revision)
    assert await _objects() == before
    assert await _scalar("SELECT version_num FROM alembic_version") == revision
    invalid = await _scalar("SELECT count(*) FROM pg_constraint WHERE NOT convalidated "
                            "AND conrelid = 'notifications'::regclass")
    assert invalid == 0
