# =============================================================================
# COMMS Service -- Migration 0013 on a non-empty table (F1.3)
# =============================================================================
# Through the same alembic CLI the VPS uses:
#   1. every kind of row the upgrade cannot translate without a guess
#      makes it REFUSE, one kind at a time, naming the kind and its
#      count and pointing at the drain step;
#   2. the rows the row itself explains are translated;
#   3. upgrade -> downgrade -> upgrade, with the documented inexact
#      downgrade mappings.
# =============================================================================

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
from tests.helpers import create_recipient

REPO_ROOT = Path(__file__).resolve().parents[1]
_BEFORE = "0012_envelope_intake"


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )


async def _migrate(*args: str, expect_ok: bool = True) -> str:
    await dispose_engine()
    done = _alembic(*args)
    await dispose_engine()
    if expect_ok:
        assert done.returncode == 0, done.stderr
    return done.stderr


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    yield
    async with get_session_factory()() as session:
        await session.execute(text("DELETE FROM notifications"))
        await session.commit()
    await _migrate("upgrade", "head")


async def _sql(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        result = await session.execute(text(statement), params)
        await session.commit()
        return result


async def _job(status: str) -> UUID:
    id_ = uuid4()
    await _sql(
        "INSERT INTO notifications (id, type, title, body, target_type, "
        "target_value, idempotency_key, fingerprint, channels, "
        "expiry_layer, status) VALUES (:id, 'unit_event', 'T', 'B', 'all', "
        "'*', :key, :fp, '[\"in_app\"]'::jsonb, 'default', :status)",
        id=id_, key=f"m13:{id_}", fp="a" * 64, status=status,
    )
    return id_


async def _delivery(
    job: UUID, recipient: UUID, status: str, *, gated: bool = False,
) -> UUID:
    id_ = uuid4()
    await _sql(
        "INSERT INTO notification_deliveries (id, notification_id, "
        "recipient_id, channel, status, next_retry_at) VALUES (:id, :job, "
        ":rid, 'in_app', :status, :nra)",
        id=id_, job=job, rid=recipient, status=status,
        nra=datetime(2090, 1, 1, tzinfo=UTC) if gated else None,
    )
    return id_


async def _status(table: str, id_: UUID) -> str:
    result = await _sql(f"SELECT status FROM {table} WHERE id = :id", id=id_)
    return str(result.scalar_one())


async def _column(table: str, column: str, id_: UUID) -> Any:
    result = await _sql(f"SELECT {column} FROM {table} WHERE id = :id", id=id_)
    return result.scalar_one()


async def test_migration_0013(at_head_afterwards: None) -> None:
    async with get_session_factory()() as session:
        rid = (await create_recipient(session)).id
        await session.commit()
    await _migrate("downgrade", _BEFORE)

    # -- 1. each ambiguous kind refuses, alone, by name ---------------------
    async def pending_job() -> None:
        await _job("pending")

    async def skipped_empty() -> None:
        await _job("skipped")

    async def skipped_mixed() -> None:
        job = await _job("skipped")
        await _delivery(job, rid, "skipped")
        await _delivery(job, rid, "sent")

    async def expired() -> None:
        await _job("expired")

    async def failed_empty() -> None:
        await _job("failed")

    async def with_failed() -> None:
        job = await _job("partial_sent")
        await _delivery(job, rid, "sent")
        await _delivery(job, rid, "failed")

    for kind, make in (
        ("active_jobs", pending_job),
        ("skipped_without_children", skipped_empty),
        ("skipped_mixed_children", skipped_mixed),
        ("expired", expired),
        ("failed_without_children", failed_empty),
        ("with_failed_delivery", with_failed),
    ):
        await make()
        stderr = await _migrate("upgrade", "head", expect_ok=False)
        assert f"{kind}=1" in stderr, (kind, stderr[-600:])
        # The refusal names the command that drains it (F1.5; before, it
        # pointed at a section of raw queries in the document, which an
        # operator limited to the product's CLI could not run).
        assert "comms-deploy.sh drain" in stderr
        # The refusal names only what is there (the pair to the name).
        others = {"active_jobs", "skipped_without_children",
                  "skipped_mixed_children", "expired",
                  "failed_without_children", "with_failed_delivery"} - {kind}
        tail = stderr[stderr.index("migration 0013 refuses"):]
        assert not any(f"{o}=" in tail for o in others), (kind, tail)
        await _sql("DELETE FROM notifications")

    # -- 2. the rows the row itself explains are translated -----------------
    muted = await _job("skipped")
    muted_children = [await _delivery(muted, rid, "skipped") for _ in range(2)]
    sent = await _job("sent")
    sent_child = await _delivery(sent, rid, "sent", gated=True)
    late_muted = await _delivery(sent, rid, "skipped")

    await _migrate("upgrade", "head")
    assert await _status("notifications", muted) == "suppressed"
    for child in muted_children:
        assert await _status("notification_deliveries", child) == "suppressed"
    assert await _status("notifications", sent) == "sent"
    assert await _status("notification_deliveries", sent_child) == "sent"
    assert await _column("notification_deliveries", "next_retry_at", sent_child) is None
    assert await _status("notification_deliveries", late_muted) == "suppressed"
    assert await _column("notifications", "category", sent) is None
    checks = (await _sql(
        "SELECT conname FROM pg_constraint WHERE conname LIKE 'ck_deliveries_%'"
    )).scalars().all()
    assert sorted(checks) == [
        "ck_deliveries_failure_class", "ck_deliveries_wait_reason",
    ]

    # -- 3. the new outcomes, then the documented inexact downgrade ---------
    cancelled = await _job("cancelled")
    cancelled_child = await _delivery(cancelled, rid, "cancelled")
    nobody = await _job("no_recipients")
    expired_parent = await _job("partial_sent")
    await _delivery(expired_parent, rid, "sent")
    expired_child = await _delivery(expired_parent, rid, "expired")

    await _migrate("downgrade", _BEFORE)
    assert await _status("notifications", muted) == "skipped"
    assert await _status("notifications", nobody) == "skipped"
    assert await _status("notifications", cancelled) == "expired"
    assert await _status("notification_deliveries", cancelled_child) == "pending"
    assert await _status("notification_deliveries", expired_child) == "pending"
    assert await _status("notification_deliveries", late_muted) == "skipped"

    # -- and up again, after the drain the downgrade made necessary ---------
    await _sql("DELETE FROM notifications WHERE id IN (:a, :b, :c)",
               a=cancelled, b=nobody, c=expired_parent)
    await _migrate("upgrade", "head")
    assert await _status("notifications", muted) == "suppressed"
    assert await _status("notifications", sent) == "sent"
