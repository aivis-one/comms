# =============================================================================
# COMMS Service -- Migration 0038: the email block lives until the outcome
# (ST-1)
# =============================================================================
# upgrade -> downgrade -> upgrade through the alembic CLI, over jobs that
# existed before it: a closed job with a block (the boxes have them since
# 3.12.0), an open job with a block, a closed job without one. Up clears
# the first only and leaves ck_notifications_email_open validated; down
# removes the CHECK and does not bring a block back; up again validates.
# The rules every later migration keeps (NOT VALID, VALIDATE in the
# block, idempotent up to it, no block in downgrade) are
# tests/test_migration_safety.py's; a run again after a stamp left behind
# is tests/test_migration_0037.py's, for every migration with a block.
#
# Mutation M7 (built once, by hand): 0038 without its UPDATE -> the
# VALIDATE refuses the closed job with its block, the upgrade fails,
# test_up_down_up red.
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
from app.engine.constants import NotificationStatus, TargetType
from app.engine.service import create_notification
from tests.helpers import intake_fields

REPO_ROOT = Path(__file__).resolve().parents[1]
_BEFORE = "0037_pipeline_transient_since"
_CHECK = "ck_notifications_email_open"
_BLOCK = {"subject": "s", "text": "t"}


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


async def _scalar(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        return await session.scalar(text(statement), params)


async def _job(status: str, email: dict[str, str] | None) -> UUID:
    """A job written at 0037, its status set directly -- what a box
    holds before 0038."""
    async with get_session_factory()() as session:
        job = await create_notification(
            session, **intake_fields(), type="unit_event_in_app_email",
            title="T", body="B", email=email,
            target_type=TargetType.USER, target_value=str(uuid4()),
        )
        await session.commit()
        await session.execute(
            text("UPDATE notifications SET status = :s WHERE id = :id"),
            {"s": status, "id": job.id},
        )
        await session.commit()
        return job.id


async def _email(job_id: UUID) -> Any:
    return await _scalar("SELECT email FROM notifications WHERE id = :id", id=job_id)


async def _check() -> tuple[bool, str] | None:
    async with get_session_factory()() as session:
        row = (await session.execute(text(
            "SELECT convalidated, pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conname = :name"), {"name": _CHECK})).first()
    return None if row is None else (bool(row[0]), str(row[1]))


async def test_up_down_up(at_head_afterwards: None) -> None:
    await _migrate("downgrade", _BEFORE)
    assert await _check() is None
    closed = await _job(NotificationStatus.SENT, dict(_BLOCK))
    open_ = await _job(NotificationStatus.PENDING, dict(_BLOCK))
    bare = await _job(NotificationStatus.EXPIRED, None)

    await _migrate("upgrade", "head")
    after_up = await _check()
    assert after_up is not None and after_up[0], after_up
    assert await _email(closed) is None
    assert await _email(open_) == _BLOCK  # the pair: an open job keeps it
    assert await _email(bare) is None
    assert await _scalar(
        "SELECT title || body FROM notifications WHERE id = :id", id=closed,
    ) == "TB"

    await _migrate("downgrade", _BEFORE)
    assert await _check() is None
    assert await _email(closed) is None  # not brought back: it is gone
    assert await _email(open_) == _BLOCK

    await _migrate("upgrade", "head")
    assert await _check() == after_up


async def test_a_closed_job_with_a_block_at_head_is_refused() -> None:
    """At head the CHECK stands: the database, not only the code."""
    job_id = await _job(NotificationStatus.PENDING, dict(_BLOCK))
    async with get_session_factory()() as session:
        with pytest.raises(Exception, match=_CHECK):
            await session.execute(
                text("UPDATE notifications SET status = 'failed' WHERE id = :id"),
                {"id": job_id},
            )
        await session.rollback()
