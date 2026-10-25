# =============================================================================
# COMMS Service -- Migration 0035: a thread's revision (RV-1)
# =============================================================================
# upgrade -> downgrade -> upgrade through the alembic CLI, over a thread
# that existed before the column:
#   up    -- the existing thread gets revision 1, a new one 1, a write 2;
#            the trigger, its function and the CHECK are in the catalogs;
#   down  -- every one of them is gone, and nothing else (the thread and
#            ix_threads_client stay);
#   up    -- all back, the thread at 1 again.
# The trigger's shape at head: tests/test_schema_objects.py.
# =============================================================================

import subprocess
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from app.core.database import dispose_engine, get_session_factory
from tests.helpers import create_recipient, next_rv1_telegram_id

REPO_ROOT = Path(__file__).resolve().parents[1]
_BEFORE = "0034_threads_client_activity"


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
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


async def _scalar(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        value = await session.scalar(text(statement), params)
        await session.commit()
        return value


async def _objects() -> dict[str, int]:
    """Each object up creates (and the index it must leave alone)."""
    return {
        "column": await _scalar(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_name = 'threads' AND column_name = 'revision'"
        ),
        "trigger": await _scalar(
            "SELECT count(*) FROM pg_trigger WHERE tgname = 'threads_revision'"
        ),
        "function": await _scalar(
            "SELECT count(*) FROM pg_proc "
            "WHERE proname = 'threads_revision_next'"
        ),
        "check": await _scalar(
            "SELECT count(*) FROM pg_constraint "
            "WHERE conname = 'ck_threads_revision_positive'"
        ),
        "client index": await _scalar(
            "SELECT count(*) FROM pg_indexes "
            "WHERE indexname = 'ix_threads_client'"
        ),
    }


async def _insert_thread(client: UUID) -> UUID:
    tid = uuid4()
    await _scalar(
        "INSERT INTO threads (id, client, operator_kind, operator_value, "
        "kind, status, created_at, idempotency_key, fingerprint) VALUES "
        "(:id, :c, 'section', gen_random_uuid(), 'ticket', 'open', now(), "
        ":k, repeat('f', 64)) RETURNING id",
        id=tid, c=client, k=f"rv1:{tid}",
    )
    return tid


async def test_up_down_up(at_head_afterwards: None) -> None:
    async with get_session_factory()() as session:
        person = uuid4()
        await create_recipient(
            session, recipient_id=person, telegram_id=next_rv1_telegram_id()
        )
        await session.commit()
    await _migrate("downgrade", _BEFORE)
    assert await _objects() == {
        "column": 0, "trigger": 0, "function": 0, "check": 0,
        "client index": 1,
    }
    old = await _insert_thread(person)

    await _migrate("upgrade", "head")
    assert await _objects() == {
        "column": 1, "trigger": 1, "function": 1, "check": 1,
        "client index": 1,
    }
    revision = "SELECT revision FROM threads WHERE id = :id"
    assert await _scalar(revision, id=old) == 1  # existed before: 1
    new = await _insert_thread(person)
    assert await _scalar(revision, id=new) == 1  # born after: 1
    await _scalar(
        "UPDATE threads SET status = 'resolved' WHERE id = :id "
        "RETURNING id", id=old,
    )
    assert await _scalar(revision, id=old) == 2

    await _migrate("downgrade", _BEFORE)
    assert await _objects() == {
        "column": 0, "trigger": 0, "function": 0, "check": 0,
        "client index": 1,
    }
    # the rows survive the step down
    assert await _scalar("SELECT count(*) FROM threads WHERE id IN (:a, :b)",
                         a=old, b=new) == 2

    await _migrate("upgrade", "head")
    assert await _scalar(revision, id=old) == 1  # the column is new again
