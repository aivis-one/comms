# =============================================================================
# COMMS Service -- Migration 0041: the correlation index (PERF-2)
# =============================================================================
# upgrade -> downgrade -> upgrade through the alembic CLI, over jobs that
# existed before the index -- written at 0040 by explicit SQL of that
# revision's columns, never by today's model (FIX TPL-1 #1). The index is
# read from the catalog (pg_index), not from the model:
#   - up builds it valid and partial; down drops it; up builds it again;
#   - a CONCURRENTLY build that failed before left an INVALID index under
#     the name -- up drops it and builds a valid one (IF NOT EXISTS alone
#     would have kept the invalid one).
#
# MUTATION: the block's DROP INDEX CONCURRENTLY taken out -> test_an_
# invalid_leftover_is_rebuilt (the index stays invalid).
# =============================================================================

import subprocess
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.core.database import dispose_engine, get_engine, get_session_factory

REPO_ROOT = Path(__file__).resolve().parents[1]
_0040 = "0040_template_unresolved"
_0041 = "0041_correlation_index"
_INDEX = "ix_notifications_correlation"


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


async def _index() -> tuple[Any, ...] | None:
    async with get_session_factory()() as session:
        row = (await session.execute(text(
            "SELECT i.indisvalid, i.indisunique, "
            "pg_get_expr(i.indpred, i.indrelid) "
            "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE c.relname = :name"), {"name": _INDEX})).first()
    return tuple(row) if row is not None else None


_AT_0040 = (
    "INSERT INTO notifications (id, type, title, body, target_type, "
    "target_value, status, idempotency_key, fingerprint, channels, "
    "correlation, expiry_layer, push_on, retry_max_attempts, "
    "retry_backoff_seconds, recipient_schedule, lane) "
    "SELECT gen_random_uuid(), 'practice.reminder_1h', 'T', 'B', 'user', "
    "'u', 'pending', 'm41:' || gen_random_uuid(), 'f', "
    "'[\"in_app\"]'::jsonb, CASE WHEN g % 2 = 0 THEN 'practice:m41' END, "
    "'default', 'none', 5, 60, 'respect', 'normal' "
    "FROM generate_series(1, 4) g"
)


async def _jobs_at_0040() -> None:
    async with get_session_factory()() as session:
        await session.execute(text(_AT_0040))
        await session.commit()


async def test_up_down_up(at_head_afterwards: None) -> None:
    await _migrate("downgrade", _0040)
    assert await _index() is None
    await _jobs_at_0040()
    await _migrate("upgrade", _0041)
    assert await _index() == (True, False, "(correlation IS NOT NULL)")
    await _migrate("downgrade", _0040)
    assert await _index() is None
    await _migrate("upgrade", _0041)
    assert await _index() == (True, False, "(correlation IS NOT NULL)")


async def test_an_invalid_leftover_is_rebuilt(at_head_afterwards: None) -> None:
    """A CONCURRENTLY build that failed: a UNIQUE build over duplicate
    correlations leaves an INVALID index under the name."""
    await _migrate("downgrade", _0040)
    await _jobs_at_0040()
    async with get_engine().connect() as connection:
        auto = await connection.execution_options(isolation_level="AUTOCOMMIT")
        with pytest.raises(IntegrityError, match="could not create unique index"):
            await auto.exec_driver_sql(
                f"CREATE UNIQUE INDEX CONCURRENTLY {_INDEX} "
                "ON notifications (correlation) WHERE correlation IS NOT NULL"
            )
    assert await _index() == (False, True, "(correlation IS NOT NULL)")
    await _migrate("upgrade", _0041)
    assert await _index() == (True, False, "(correlation IS NOT NULL)")
