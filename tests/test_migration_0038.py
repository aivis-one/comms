# =============================================================================
# COMMS Service -- Migration 0038: the in-row transient wait removed (WK-2)
# =============================================================================
# The upgrade through the alembic CLI, from 0037 (the suite's database
# brought down to it to seed it -- the way down itself is not held: a
# rollout is rolled back by restoring a dump, owner's decision 14.11).
# Up drops 0037's three columns and three CHECKs and puts 0015's gate
# back, validated; the gate of an open wait at zero attempts goes with
# the wait, a counted failure's gate stays. A run whose stamp was left
# behind goes through again --
# tests/test_migration_0037.py
# test_upgrade_runs_again_after_its_stamp_was_left_behind, parametrized
# over every migration with a block, holds that for 0038 too; here, a
# run that stopped after its columns went.
#
# MUTATIONS (each turns one red):
#   G2 the UPDATE of the zero-attempt gates left out -> the open waits test
#      (the VALIDATE fails)
# =============================================================================

from collections.abc import AsyncGenerator
from typing import Any

import pytest
from sqlalchemy import text

from app.core.database import get_session_factory
from tests.test_migration_0037 import _job_at_0037, _migrate

_0037 = "0037_pipeline_transient_since"
_0038 = "0038_drop_transient_wait"
_WAIT_CHECKS = (
    "ck_notifications_transient_wait_whole",
    "ck_notifications_transient_wait_gated",
    "ck_notifications_transient_wait_open",
)
_GATE_0015 = "CHECK (((pipeline_retry_at IS NULL) OR (pipeline_attempts > 0)))"


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    yield
    await _migrate("upgrade", "head")


async def _scalar(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        return await session.scalar(text(statement), params)


async def _execute(statement: str) -> None:
    async with get_session_factory()() as session:
        await session.execute(text(statement))
        await session.commit()


async def _objects() -> dict[str, Any]:
    """The wait's columns, its CHECKs and the gate, as the catalog has them
    -- with the column the gate stands on, so "gone" is never read off an
    empty catalog."""
    return {
        "wait_columns": await _scalar(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = "
            "'notifications' AND column_name LIKE 'pipeline_transient_%'"
        ),
        "retry_at": await _scalar(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = "
            "'notifications' AND column_name = 'pipeline_retry_at'"
        ),
        **{
            name: await _scalar(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname = :name", name=name,
            )
            for name in (*_WAIT_CHECKS, "ck_notifications_pipeline_gate")
        },
        "gate_valid": await _scalar(
            "SELECT convalidated FROM pg_constraint "
            "WHERE conname = 'ck_notifications_pipeline_gate'"
        ),
    }


def _at_0038(found: dict[str, Any]) -> None:
    assert found["wait_columns"] == 0
    assert found["retry_at"] == 1
    assert all(found[name] is None for name in _WAIT_CHECKS)
    assert found["ck_notifications_pipeline_gate"] == _GATE_0015
    assert found["gate_valid"] is True


async def test_the_open_waits_and_the_counted_gates(at_head_afterwards: None) -> None:
    """At 0037: a wait at zero attempts (its gate's only ground) and a
    wait behind a counted failure. Up: the first gate goes with its
    wait, the second stays; the CHECK validates."""
    await _migrate("downgrade", _0037)
    waiting, counted = await _job_at_0037(), await _job_at_0037()
    await _execute(
        "UPDATE notifications SET pipeline_retry_at = now() + interval '1 hour', "
        "pipeline_transient_since = now(), pipeline_transient_step = 'rollup', "
        "pipeline_transient_error = 'sqlalchemy.exc.DBAPIError at "
        f"app/engine/processor.py:1' WHERE id IN ('{waiting}', '{counted}')"
    )
    await _execute(
        "UPDATE notifications SET pipeline_attempts = 1, pipeline_step = "
        "'deliver', pipeline_error = 'builtins.RuntimeError at "
        f"app/engine/processor.py:2' WHERE id = '{counted}'"
    )
    await _migrate("upgrade", _0038)
    _at_0038(await _objects())
    gate = "SELECT pipeline_retry_at FROM notifications WHERE id = :id"
    assert await _scalar(gate, id=waiting) is None
    assert await _scalar(gate, id=counted) is not None


async def test_a_run_stopped_after_its_columns_goes_through(
    at_head_afterwards: None,
) -> None:
    """The stamp at 0037, the schema past every step before the block --
    the columns and the wait's CHECKs gone, the gate NOT VALID: upgrade
    again goes through to the same schema, the gate validated."""
    await _migrate("downgrade", _0037)
    for name in (*_WAIT_CHECKS, "ck_notifications_pipeline_gate"):
        await _execute(f"ALTER TABLE notifications DROP CONSTRAINT {name}")
    await _execute(
        "ALTER TABLE notifications ADD CONSTRAINT ck_notifications_pipeline_gate "
        "CHECK (pipeline_retry_at IS NULL OR pipeline_attempts > 0) NOT VALID"
    )
    await _execute(
        "ALTER TABLE notifications DROP COLUMN pipeline_transient_error, "
        "DROP COLUMN pipeline_transient_step, DROP COLUMN pipeline_transient_since"
    )
    assert await _scalar("SELECT version_num FROM alembic_version") == _0037
    await _migrate("upgrade", _0038)
    _at_0038(await _objects())
    assert await _scalar("SELECT version_num FROM alembic_version") == _0038

