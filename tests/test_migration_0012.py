# =============================================================================
# COMMS Service -- Migration 0012 on a non-empty table (F1.2)
# =============================================================================
# The cycle upgrade -> downgrade -> upgrade, run through the same
# alembic CLI the VPS uses, over rows of every form the pre-0012 table
# could hold:
#
#   r1  no key, "_channels" in action_data, no expiry   (internal path)
#   r2  a key, "_channels" plus a template variable, an expiry (stream)
#   r3  no key, NO action_data at all                   (direct insert)
#   r4  a key, action_data WITHOUT "_channels"          (direct insert)
#
# r1 and r2 are what create_notification wrote; r3 and r4 are rows
# inserted around it. The downgrade restores r1 and r2 exactly; r3 and
# r4 come back with "_channels": ["in_app"] -- the value the old resolve
# stage read for them anyway (the migration docstring names this).
# =============================================================================

import json
import subprocess
import sys
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from app.core.database import dispose_engine, get_session_factory

REPO_ROOT = Path(__file__).resolve().parents[1]
_BEFORE = "0011_delivery_schedule"


def _alembic(*args: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "alembic", *args], check=True, cwd=REPO_ROOT,
    )


async def _migrate(*args: str) -> None:
    """Run alembic, then drop the pooled connections: asyncpg caches
    statement plans per connection, and a plan prepared against the old
    schema is invalid after the change."""
    await dispose_engine()
    _alembic(*args)
    await dispose_engine()


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    """Whatever happens, the session's schema is back at head."""
    yield
    await _migrate("upgrade", "head")


async def _rows(ids: list[UUID]) -> dict[UUID, dict[str, Any]]:
    async with get_session_factory()() as session:
        result = await session.execute(
            text("SELECT * FROM notifications WHERE id = ANY(:ids)"),
            {"ids": ids},
        )
        return {row["id"]: dict(row) for row in result.mappings()}


async def _index_definition() -> str:
    async with get_session_factory()() as session:
        return (await session.execute(text(
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname = 'uq_notifications_idempotency_key'"
        ))).scalar_one()


async def _table_exists(name: str) -> bool:
    async with get_session_factory()() as session:
        return bool((await session.execute(
            text("SELECT to_regclass(:name) IS NOT NULL"), {"name": name},
        )).scalar_one())


async def test_round_trip_on_every_form_of_row(at_head_afterwards: None) -> None:
    await _migrate("downgrade", _BEFORE)
    r1, r2, r3, r4 = (uuid4() for _ in range(4))
    rows = [
        (r1, None, {"_channels": ["telegram"]}, None),
        (r2, "k2", {"_channels": ["in_app", "email"], "x": 1},
         datetime(2090, 1, 1, tzinfo=UTC)),
        (r3, None, None, None),
        (r4, "k4", {"x": 2}, None),
    ]
    async with get_session_factory()() as session:
        for id_, key, action_data, expiry in rows:
            await session.execute(
                text(
                    "INSERT INTO notifications (id, type, title, body, "
                    "target_type, target_value, idempotency_key, "
                    "action_data, expiry_at, status) VALUES (:id, "
                    "'unit_event', 'T', 'B', 'all', '*', :key, "
                    "CAST(:ad AS jsonb), :exp, 'sent')"
                ),
                {
                    "id": id_, "key": key,
                    "ad": None if action_data is None else json.dumps(action_data),
                    "exp": expiry,
                },
            )
        await session.commit()
    before = await _rows([r1, r2, r3, r4])

    # -- upgrade ---------------------------------------------------------
    await _migrate("upgrade", "head")
    up = await _rows([r1, r2, r3, r4])
    assert up[r1]["idempotency_key"] == f"pre-0012:{r1}"
    assert up[r2]["idempotency_key"] == "k2"
    assert up[r3]["idempotency_key"] == f"pre-0012:{r3}"
    for row in up.values():
        assert len(row["fingerprint"]) == 64
        assert not set(row["fingerprint"]) <= set("0123456789abcdef")
    assert up[r1]["channels"] == ["telegram"]
    assert up[r2]["channels"] == ["in_app", "email"]
    assert up[r3]["channels"] == ["in_app"]
    assert up[r4]["channels"] == ["in_app"]
    assert up[r1]["action_data"] is None
    assert up[r2]["action_data"] == {"x": 1}
    assert up[r3]["action_data"] is None
    assert up[r4]["action_data"] == {"x": 2}
    assert up[r2]["expiry_layer"] == "envelope"
    assert up[r1]["expiry_layer"] == "default"
    assert all(row["correlation"] is None for row in up.values())
    assert "WHERE" not in await _index_definition()
    assert "UNIQUE" in await _index_definition()
    assert await _table_exists("intake_outcomes")

    # -- downgrade -------------------------------------------------------
    await _migrate("downgrade", _BEFORE)
    down = await _rows([r1, r2, r3, r4])
    assert down[r1] == before[r1]
    assert down[r2] == before[r2]
    for rid, stashed in ((r3, {"_channels": ["in_app"]}),
                         (r4, {"x": 2, "_channels": ["in_app"]})):
        expected = {**before[rid], "action_data": stashed}
        assert down[rid] == expected
    assert "WHERE (idempotency_key IS NOT NULL)" in await _index_definition()
    assert not await _table_exists("intake_outcomes")

    # -- upgrade again ---------------------------------------------------
    await _migrate("upgrade", "head")
    again = await _rows([r1, r2, r3, r4])
    for rid in (r1, r2):
        assert again[rid]["channels"] == up[rid]["channels"]
        assert again[rid]["idempotency_key"] == up[rid]["idempotency_key"]
        assert again[rid]["action_data"] == up[rid]["action_data"]
