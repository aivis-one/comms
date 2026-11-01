# =============================================================================
# COMMS Service -- Migration 0037: when a transient wait began (TX-3)
# =============================================================================
# upgrade -> downgrade -> upgrade through the alembic CLI, over a job that
# existed before the column: up adds the column (NULL on old rows) and
# its CHECKs and replaces 0015's pipeline gate; down removes exactly
# those and puts 0015's gate back; up again restores them -- definitions
# compared, not names.
# =============================================================================

import asyncio
import re
import subprocess
import sys
import time
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

from app.core.database import dispose_engine, get_engine, get_session_factory
from app.engine.constants import TargetType
from app.engine.service import create_notification
from tests.helpers import intake_fields

REPO_ROOT = Path(__file__).resolve().parents[1]
_BEFORE = "0036_notification_email"


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
    _after(await _objects())
    await _migrate("downgrade", _BEFORE)
    _before(await _objects())
    await _migrate("upgrade", "head")
    _after(await _objects())
    await _migrate("downgrade", _BEFORE)
    _before(await _objects())
    await _migrate("upgrade", "head")
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


async def _execute(*statements: str) -> None:
    async with get_session_factory()() as session:
        for statement in statements:
            await session.execute(text(statement))
        await session.commit()


async def test_every_constraint_is_validated(at_head_afterwards: None) -> None:
    """Added NOT VALID, then VALIDATEd: at head every one is valid."""
    await _migrate("downgrade", _BEFORE)
    await _migrate("upgrade", "head")
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
    await _migrate("upgrade", "head")
    assert await _validated() == dict.fromkeys(_CONSTRAINTS_0037, True)
    _after(await _objects())


async def test_down_with_an_open_wait_at_zero_attempts(
    at_head_afterwards: None,
) -> None:
    """A wait at zero attempts holds a gate 0015's CHECK refuses: down
    takes that gate away with the wait, first, and passes. (A CLOSED job
    holding a wait's fields cannot be built: ck_notifications_transient_
    wait_open refuses it -- so it is not a case down meets.)"""
    async with get_session_factory()() as session:
        job = await create_notification(
            session, **intake_fields(), type="unit_event_in_app", title="T",
            body="B", target_type=TargetType.ALL, target_value="*",
        )
        await session.commit()
        job_id = job.id
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
    FULL). Returns its heap pages."""
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
        await auto.execute(text("VACUUM notifications"))
        await auto.execute(text("ALTER TABLE notifications RESET (autovacuum_enabled)"))
        pages = await auto.scalar(text(
            "SELECT pg_relation_size('notifications') / 8192"))
    return int(pages)


async def _measure() -> tuple[float, float, float]:
    """Run 0036 -> head through the CLI; meanwhile watch pg_locks for a
    granted AccessExclusiveLock on notifications and time an UPDATE of
    a job row in a loop. Returns (held, longest write, the migration)."""
    await _migrate("downgrade", _BEFORE)
    held: list[float] = []
    writes: list[float] = []
    stop = asyncio.Event()

    async def watch() -> None:
        async with get_engine().connect() as observer:
            while not stop.is_set():
                seen = await observer.scalar(text(
                    "SELECT count(*) FROM pg_locks l JOIN pg_class c "
                    "ON c.oid = l.relation WHERE c.relname = 'notifications' "
                    "AND l.mode = 'AccessExclusiveLock' AND l.granted "
                    "AND l.database = (SELECT oid FROM pg_database "
                    "WHERE datname = current_database())"))
                await observer.rollback()
                if seen:
                    held.append(time.monotonic())
                await asyncio.sleep(0.02)

    async def write() -> None:
        async with get_engine().connect() as writer:
            while not stop.is_set():
                started = time.monotonic()
                await writer.execute(text(
                    "UPDATE notifications SET body = body WHERE id = "
                    "(SELECT id FROM notifications LIMIT 1)"))
                await writer.commit()
                writes.append(time.monotonic() - started)
                await asyncio.sleep(0.02)

    tasks = [asyncio.ensure_future(watch()), asyncio.ensure_future(write())]
    await asyncio.sleep(0.2)
    started = time.monotonic()
    await dispose_engine()
    done = await asyncio.to_thread(_alembic, "upgrade", "head")
    took = time.monotonic() - started
    stop.set()
    await asyncio.gather(*tasks)
    assert done.returncode == 0, done.stderr
    return (held[-1] - held[0] if held else 0.0), max(writes, default=0.0), took


@pytest.mark.usefixtures("pristine_tables")
async def test_the_lock_does_not_grow_with_the_heap(
    at_head_afterwards: None,
) -> None:
    """THE RULE, written before the measure: 0037 holds ACCESS EXCLUSIVE
    on notifications 1 s at most, and no longer on a bloated heap than
    on a small one. Measured on ~600 MB of heap with ~11 thousand live
    rows (velo-test before its VACUUM FULL) and on the same rows
    without the bloat. The migration's whole time is printed too: comms'
    API does not serve during it (comms-app migrates before uvicorn)."""
    small = await _bloat(_BLOATED_ROWS // 42)
    small_held, small_write, small_took = await _measure()
    await _execute("TRUNCATE notifications CASCADE")
    pages = await _bloat(_BLOATED_ROWS)
    held, write, took = await _measure()
    print(f"\nsmall: {small} pages, held {small_held:.2f}s, write max "
          f"{small_write:.2f}s, migration {small_took:.2f}s"
          f"\nbloated: {pages} pages, held {held:.2f}s, write max "
          f"{write:.2f}s, migration {took:.2f}s")
    assert pages > 60_000, pages  # the instrument: the heap IS bloated
    assert held <= 1.0, held
    assert held - small_held <= 0.2, (held, small_held)
    assert write <= 1.0, write  # writes went on through the VALIDATEs


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
    again must go through, to the same schema, every constraint valid."""
    revision, previous = _revisions(path)
    await _migrate("upgrade", revision)
    before = await _objects()
    await _migrate("stamp", previous)  # the stamp left behind
    await _migrate("upgrade", revision)
    assert await _objects() == before
    assert await _scalar("SELECT version_num FROM alembic_version") == revision
    invalid = await _scalar("SELECT count(*) FROM pg_constraint WHERE NOT convalidated "
                            "AND conrelid = 'notifications'::regclass")
    assert invalid == 0
