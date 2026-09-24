# =============================================================================
# COMMS Service -- Migration 0014 on a non-empty table (F1.4)
# =============================================================================
# upgrade -> downgrade -> upgrade through the alembic CLI, over rows of
# every form the pre-0014 tables could hold:
#   recipients  a full row; a row with every sentinel for "no value"
#               ('' locale, ' ' email, '' timezone, telegram_id 0);
#   messages / threads  a row each, without a key;
#   notifications       with a priority other than the default.
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

REPO_ROOT = Path(__file__).resolve().parents[1]
_BEFORE = "0013_lifecycle_outcomes"


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


async def _sql(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        result = await session.execute(text(statement), params)
        await session.commit()
        return result


async def _row(table: str, id_: UUID) -> dict[str, Any]:
    result = await _sql(f"SELECT * FROM {table} WHERE id = :id", id=id_)
    return dict(result.mappings().one())


async def test_round_trip_on_every_form_of_row(at_head_afterwards: None) -> None:
    await _migrate("downgrade", _BEFORE)
    full, blank = uuid4(), uuid4()
    await _sql(
        "INSERT INTO recipients (id, telegram_id, email, locale, timezone, "
        "active) VALUES (:a, 91001, 'a@example.test', 'de', 'Europe/Berlin', "
        "true), (:b, 0, ' ', '', '', true)",
        a=full,
        b=blank,
    )
    section, thread, message = uuid4(), uuid4(), uuid4()
    await _sql(
        "INSERT INTO sections (id, key, label) VALUES (:s, :k, 'L')",
        s=section,
        k=f"m14-{section.hex[:8]}",
    )
    await _sql(
        "INSERT INTO threads (id, client, operator_kind, operator_value, kind, "
        "status) VALUES (:t, :c, 'section', :s, 'ticket', 'open')",
        t=thread,
        c=full,
        s=section,
    )
    await _sql(
        "INSERT INTO messages (id, thread_id, sender, body) "
        "VALUES (:m, :t, :c, 'hello')",
        m=message,
        t=thread,
        c=full,
    )
    job = uuid4()
    await _sql(
        "INSERT INTO notifications (id, type, title, body, target_type, "
        "target_value, idempotency_key, fingerprint, channels, expiry_layer, "
        "status, priority) VALUES (:n, 'unit_event', 'T', 'B', 'all', '*', "
        ":k, :f, '[\"in_app\"]'::jsonb, 'default', 'sent', 2)",
        n=job,
        k=f"m14:{job}",
        f="a" * 64,
    )

    # -- upgrade: versions, one rule for "no value", keys, no priority --
    await _migrate("upgrade", "head")
    kept = await _row("recipients", full)
    assert (kept["version"], kept["locale"], kept["email"]) == (
        0,
        "de",
        "a@example.test",
    )
    assert kept["deleted_at"] is None and len(kept["snapshot_fingerprint"]) == 64
    cleared = await _row("recipients", blank)
    assert (
        cleared["telegram_id"],
        cleared["email"],
        cleared["locale"],
        cleared["timezone"],
    ) == (None, None, None, None)
    for table, id_ in (("threads", thread), ("messages", message)):
        row = await _row(table, id_)
        assert row["idempotency_key"] == f"pre-0014:{id_}"
        assert len(row["fingerprint"]) == 64
    assert "priority" not in await _row("notifications", job)
    checks = (
        (
            await _sql(
                "SELECT conname FROM pg_constraint WHERE conname LIKE 'ck_recipients_%'"
            )
        )
        .scalars()
        .all()
    )
    assert set(checks) == {
        "ck_recipients_locale_not_blank",
        "ck_recipients_email_not_blank",
        "ck_recipients_timezone_not_blank",
        "ck_recipients_telegram_id_not_zero",
        "ck_recipients_version_not_negative",
        "ck_recipients_tombstone",
    }

    # -- downgrade: the documented inexact mappings --
    await _migrate("downgrade", _BEFORE)
    back = await _row("recipients", blank)
    assert back["locale"] == ""  # NULL -> '' (the old form of "no language")
    assert back["telegram_id"] is None and back["email"] is None
    assert (await _row("notifications", job))["priority"] == 5
    assert "idempotency_key" not in await _row("messages", message)

    # -- and up again --
    await _migrate("upgrade", "head")
    assert (await _row("recipients", blank))["locale"] is None
