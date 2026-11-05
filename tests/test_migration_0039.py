# =============================================================================
# COMMS Service -- Migration 0039: a section remembers its roster (API-1)
# =============================================================================
# upgrade -> downgrade -> upgrade through the alembic CLI, over sections
# that existed before the column: up adds sections.roster_declared and
# declares exactly the sections that have members; down removes the
# column; up again declares them again. The column is read from the
# catalog (information_schema), not from the model.
#
# MUTATION: M6 the migration without its UPDATE -> test_the_sections_with
# _members_are_declared (the section with a member stays undeclared).
# =============================================================================

import subprocess
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from app.audience.sync import snapshot_fingerprint
from app.core.database import dispose_engine, get_session_factory
from tests.helpers import next_api1_telegram_id

REPO_ROOT = Path(__file__).resolve().parents[1]
_0038 = "0038_drop_transient_wait"
_0039 = "0039_section_roster_declared"


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


async def _column() -> tuple[Any, ...] | None:
    async with get_session_factory()() as session:
        row = (
            await session.execute(
                text(
                    "SELECT data_type, is_nullable, column_default "
                    "FROM information_schema.columns "
                    "WHERE table_name = 'sections' "
                    "AND column_name = 'roster_declared'"
                )
            )
        ).first()
    return tuple(row) if row is not None else None


async def _declared(section: UUID) -> Any:
    return await _scalar(
        "SELECT roster_declared FROM sections WHERE id = :id", id=section
    )


async def _sections_at_0038() -> tuple[UUID, UUID]:
    """At 0038 (no column): a section with a member, one without.

    Every row by an explicit INSERT of 0038's columns (FIX TPL-1 #1): the
    operator went through create_recipient -- the current Recipient model
    -- right only while the model had no column 0038 lacks."""
    operator = uuid4()
    with_member, without = uuid4(), uuid4()
    telegram_id = next_api1_telegram_id()
    async with get_session_factory()() as session:
        await session.execute(
            text(
                "INSERT INTO recipients (id, telegram_id, locale, active, "
                "version, snapshot_fingerprint) VALUES (:id, :tg, 'en', true, "
                "1, :fp)"
            ),
            {
                "id": operator, "tg": telegram_id,
                "fp": snapshot_fingerprint(
                    telegram_id=telegram_id, email=None, locale="en",
                    timezone=None, active=True,
                ),
            },
        )
        for sid in (with_member, without):
            await session.execute(
                text(
                    "INSERT INTO sections (id, key, label) "
                    "VALUES (:id, :key, :key)"
                ),
                {"id": sid, "key": f"m39-{sid.hex[:8]}"},
            )
        await session.execute(
            text(
                "INSERT INTO section_members (section_id, operator_id) "
                "VALUES (:s, :o)"
            ),
            {"s": with_member, "o": operator},
        )
        await session.commit()
    return with_member, without


async def test_up_down_up(at_head_afterwards: None) -> None:
    await _migrate("upgrade", _0039)
    assert await _column() == ("boolean", "NO", "false")
    await _migrate("downgrade", _0038)
    assert await _column() is None
    await _migrate("upgrade", _0039)
    assert await _column() == ("boolean", "NO", "false")


async def test_the_sections_with_members_are_declared(
    at_head_afterwards: None,
) -> None:
    """Members now -> declared; none -> undeclared. Twice: up, down, up."""
    await _migrate("downgrade", _0038)
    with_member, without = await _sections_at_0038()
    for _ in range(2):
        await _migrate("upgrade", _0039)
        assert await _declared(with_member) is True
        assert await _declared(without) is False
        await _migrate("downgrade", _0038)
