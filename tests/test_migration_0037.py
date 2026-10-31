# =============================================================================
# COMMS Service -- Migration 0037: when a transient wait began (TX-3)
# =============================================================================
# upgrade -> downgrade -> upgrade through the alembic CLI, over a job that
# existed before the column: up adds the column (NULL on old rows) and
# its CHECKs and replaces 0015's pipeline gate; down removes exactly
# those and puts 0015's gate back; up again restores them -- definitions
# compared, not names.
# =============================================================================

import subprocess
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

from app.core.database import dispose_engine, get_session_factory

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
