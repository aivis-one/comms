# =============================================================================
# PERF-2 -- the reminder cancel finds its jobs by ix_notifications_correlation
# =============================================================================
#
# What is held here, on the planner (never by a clock):
#   1 the cancel's select -- type IN (...) AND correlation = :c AND status
#     IN (active), FOR UPDATE, as app/engine/service.py close_notifications
#     writes it for app/engine/reminders.py cancel_reminders -- on a
#     velo-shaped table (a few reminder types per practice, most jobs
#     without a correlation) takes ix_notifications_correlation, not the
#     BitmapAnd of ix_notifications_type x ix_notifications_status that
#     read 13 504 blocks for 18 rows at a million jobs (PERF-2 report);
#   2 the index is partial: a job without a correlation is not in it.
# On tables as on a new box (pristine_tables, tests/conftest.py); the
# table is filled and ANALYZEd in a transaction that is rolled back.
#
# MUTATIONS (each turns one red):
#   M2 the index not built (0041's CREATE taken out) -> test_the_cancel_
#      takes_the_index
#   M3 the index built without its WHERE -> test_the_index_is_partial
# =============================================================================

import json
from typing import Any

import pytest
from sqlalchemy import select, text

from app.core.database import get_session_factory
from app.engine.models import Notification
from app.engine.service import _ACTIVE_STATUSES

_INDEX = "ix_notifications_correlation"
_JOBS = 20_000
_TYPES = ["practice.reminder_24h", "practice.reminder_1h", "practice.reminder_10m"]


def _cancel_sql() -> str:
    statement = (
        select(Notification.id)
        .where(
            Notification.type.in_(_TYPES),
            Notification.correlation == "practice:7",
            Notification.status.in_(_ACTIVE_STATUSES),
        )
        .order_by(Notification.id)
        .with_for_update()
    )
    return str(statement.compile(compile_kwargs={"literal_binds": True}))


_FILL = text(
    "INSERT INTO notifications (id, type, title, body, target_type, "
    "target_value, status, idempotency_key, fingerprint, channels, "
    "correlation, expiry_layer, push_on, retry_max_attempts, "
    "retry_backoff_seconds, recipient_schedule, lane) "
    "SELECT gen_random_uuid(), "
    "CASE WHEN g % 100 < 6 THEN (ARRAY['practice.reminder_24h', "
    "'practice.reminder_1h', 'practice.reminder_10m'])[1 + g % 3] "
    "ELSE 'booking.confirmed' END, 'T', 'B', 'user', 'u', "
    "CASE WHEN g % 100 < 8 THEN 'pending' ELSE 'sent' END, "
    "'perf2:' || gen_random_uuid(), 'f', '[\"in_app\"]'::jsonb, "
    "CASE WHEN g % 100 < 6 THEN 'practice:' || (g / 300) END, "
    "'default', 'none', 5, 60, 'respect', 'normal' "
    "FROM generate_series(1, CAST(:n AS integer)) g"
)


def _indexes(plan: dict[str, Any]) -> list[str]:
    found = [plan["Index Name"]] if "Index Name" in plan else []
    for child in plan.get("Plans", []):
        found += _indexes(child)
    return found


class TestTheCancelIndex:
    @pytest.mark.usefixtures("pristine_tables")
    async def test_the_cancel_takes_the_index(self) -> None:
        async with get_session_factory()() as s:
            await s.execute(_FILL, {"n": _JOBS})
            await s.execute(text("ANALYZE notifications"))
            raw = await s.scalar(text("EXPLAIN (FORMAT JSON) " + _cancel_sql()))
            await s.rollback()
        plan = (json.loads(raw) if isinstance(raw, str) else raw)[0]["Plan"]
        used = _indexes(plan)
        assert _INDEX in used, used
        assert "ix_notifications_type" not in used, used

    async def test_the_index_is_partial(self) -> None:
        async with get_session_factory()() as s:
            row = (await s.execute(text(
                "SELECT i.indisvalid, pg_get_expr(i.indpred, i.indrelid) "
                "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
                "WHERE c.relname = :name"), {"name": _INDEX})).one()
        assert row[0] is True
        assert row[1] == "(correlation IS NOT NULL)"
