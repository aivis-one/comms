# =============================================================================
# H12 -- a portion's selection reads the job's deliveries, not the table
# =============================================================================
#
# deliver_notification selects the job's waiting, due deliveries ORDER
# BY id LIMIT the portion. Held here by what it COSTS -- buffers of the
# real statement, captured from a real attempt and explained with
# ANALYZE -- on the two orders of rows that make the costly paths
# costly, under every statistics a send meets:
#
#   few_waiting_last_by_id   three of the job's 5000 deliveries wait, and
#       their ids are the highest in the table: a walk of the primary key
#       in id order passes every other row first (50 000 buffers and more
#       measured before migration 0029);
#   all_waiting_last_in_heap all 5000 wait, written after every other
#       row: a scan of the heap passes every other row first (656), a
#       sort of the job's waiting deliveries reads all 5000 (96);
#
#   statistics taken before the sends / after them / none at all.
#
# On tables as on a new box (pristine_tables, tests/conftest.py, H14):
# no dead rows or index entries, no free space, no statistics but the
# ones taken here -- what this file built for itself until H14 (a
# VACUUM FULL, ids of its own on every run) is the harness's now, and
# "not taken here" (whatever the suite left) became "none".
#
# The bound, 10 buffers: the path bounded by the job, in id order off
# ix_notification_deliveries_status (status, notification_id, id), reads
# 4 (measured, all six); the paths above read 69 and more. Each premise
# about the order is checked, not assumed.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  migration 0029 undone -- (status, notification_id) again
#                                        -> by cost, few_waiting_last_by_id
#   M2  the job's waiting deliveries given low ids / written first
#                                        -> by the premise
# =============================================================================

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, text

from app.core.database import get_engine, get_session_factory
from app.core.lanes import Lane
from app.engine.processor import process_lane
from tests.conftest import analyze_with_nothing_waiting
from tests.helpers import sql_shape

_BUFFERS = 10
_SHAPES = ["few_waiting_last_by_id", "all_waiting_last_in_heap"]
_STATISTICS = [
    "taken_before_the_sends", "taken_after_the_sends", "none",
    # H14 FIX: taken when nothing waited, the waiting rows back since
    # (tests/conftest.py analyze_with_nothing_waiting).
    "taken_with_nothing_waiting",
]


async def _seed(shape: str, statistics: str) -> tuple[UUID, UUID]:
    """Mine and the other send, on pristine tables (the test's
    pristine_tables): rows written now lie in the order they are written,
    so "written last" is "last in the heap" -- the premise is still
    checked."""
    mine, other = uuid4(), uuid4()
    recipients = "FROM recipients WHERE snapshot_fingerprint = 'h12'"
    async with get_session_factory()() as session:
        await session.execute(text(
            "INSERT INTO recipients (id, locale, version, snapshot_fingerprint) "
            "SELECT gen_random_uuid(), 'en', 1, 'h12' FROM generate_series(1, 5000)"
        ))
        # Mine in bulk (its lane's pass is the one run below); the other
        # send in normal, so the bulk pass attempts mine alone.
        await session.execute(text(
            "INSERT INTO notifications (id, type, title, body, target_type, "
            "target_value, idempotency_key, fingerprint, channels, "
            "expiry_layer, push_on, retry_max_attempts, "
            "retry_backoff_seconds, recipient_schedule, lane, status, "
            "scheduled_at) "
            "SELECT id, 'unit_event_in_app', 'T', 'B', 'all', '*', key, "
            "repeat('a', 64), '[\"in_app\"]'::jsonb, 'default', 'none', 3, "
            "30, 'respect', lane, status, now() - interval '1 minute' "
            "FROM (VALUES "
            "(CAST(:mine AS uuid), 'h12-mine', 'bulk', 'processing'), "
            "(CAST(:other AS uuid), 'h12-other', 'normal', 'processing')) "
            "AS v(id, key, lane, status) "
            "UNION ALL "
            "SELECT gen_random_uuid(), 'unit_event', 'T', 'B', 'all', '*', "
            "'h12-done-' || g, repeat('a', 64), '[\"in_app\"]'::jsonb, "
            "'default', 'none', 3, 30, 'respect', 'normal', 'sent', now() "
            "FROM generate_series(1, 8000) g"
        ), {"mine": mine, "other": other})
        await session.execute(text(
            "INSERT INTO notification_deliveries "
            "(id, notification_id, recipient_id, channel, status) "
            "SELECT gen_random_uuid(), n.id, (SELECT id FROM recipients "
            "WHERE snapshot_fingerprint = 'h12' LIMIT 1), 'in_app', 'sent' "
            "FROM notifications n, generate_series(1, 5) "
            "WHERE n.idempotency_key LIKE 'h12-done-%'"
        ))
        if statistics == "taken_before_the_sends":
            await session.execute(text("ANALYZE notification_deliveries"))
        await session.execute(text(
            "INSERT INTO notification_deliveries "
            "(id, notification_id, recipient_id, channel, status) "
            f"SELECT gen_random_uuid(), :other, id, 'in_app', 'pending' {recipients}"
        ), {"other": other})
        if shape == "few_waiting_last_by_id":
            await session.execute(text(
                "INSERT INTO notification_deliveries "
                "(id, notification_id, recipient_id, channel, status) "
                "SELECT gen_random_uuid(), :mine, id, 'in_app', 'sent' "
                f"{recipients} ORDER BY id OFFSET 3"
            ), {"mine": mine})
            await session.execute(text(
                "INSERT INTO notification_deliveries "
                "(id, notification_id, recipient_id, channel, status) "
                "SELECT CAST('ffffffff-ffff-ffff-' "
                "|| substr(md5(random()::text), 1, 4) "
                "|| '-' || lpad(n::text, 12, '0') AS uuid), :mine, r.id, "
                "'in_app', 'pending' FROM (SELECT id, "
                f"row_number() OVER (ORDER BY id) AS n {recipients}) r "
                "WHERE n <= 3"
            ), {"mine": mine})
        else:
            await session.execute(text(
                "INSERT INTO notification_deliveries "
                "(id, notification_id, recipient_id, channel, status) "
                f"SELECT gen_random_uuid(), :mine, id, 'in_app', 'pending' {recipients}"
            ), {"mine": mine})
        if statistics == "taken_after_the_sends":
            await session.execute(text("ANALYZE notification_deliveries"))
        if statistics == "taken_with_nothing_waiting":
            await analyze_with_nothing_waiting(session)
        await session.commit()
    return mine, other


async def _premise(shape: str, mine: UUID) -> bool:
    """The order that makes the costly path costly, as the rows lie."""
    query = (
        # (PostgreSQL 16 has no min/max over uuid: ORDER BY ... LIMIT 1.)
        "SELECT (SELECT id FROM notification_deliveries "
        "WHERE notification_id = :mine AND status = 'pending' "
        "ORDER BY id LIMIT 1) > "
        "(SELECT id FROM notification_deliveries "
        "WHERE NOT (notification_id = :mine AND status = 'pending') "
        "ORDER BY id DESC LIMIT 1)"
        if shape == "few_waiting_last_by_id" else
        "SELECT (SELECT min(ctid) FROM notification_deliveries "
        "WHERE notification_id = :mine) > "
        "(SELECT max(ctid) FROM notification_deliveries "
        "WHERE notification_id <> :mine)"
    )
    async with get_session_factory()() as session:
        return bool((await session.execute(
            text(query), {"mine": mine},
        )).scalar_one())


@contextmanager
def _statements() -> Iterator[list[tuple[str, Any]]]:
    seen: list[tuple[str, Any]] = []
    engine = get_engine().sync_engine

    def record(conn: Any, cursor: Any, statement: str, parameters: Any,
               *rest: Any) -> None:
        seen.append((statement, parameters))

    event.listen(engine, "before_cursor_execute", record)
    try:
        yield seen
    finally:
        event.remove(engine, "before_cursor_execute", record)


class _Channel:
    def __init__(self) -> None:
        self.delivered: list[UUID] = []

    async def deliver(self, notification: Any, delivery: Any, *rest: Any) -> bool:
        self.delivered.append(delivery.id)
        return True


@pytest.mark.usefixtures("pristine_tables")
class TestThePortionSelect:
    """done-when H12 (2)-(4)."""

    @pytest.mark.parametrize("shape", _SHAPES)
    @pytest.mark.parametrize("statistics", _STATISTICS)
    async def test_a_handful_of_buffers(self, shape: str, statistics: str) -> None:
        mine, _ = await _seed(shape, statistics)
        assert await _premise(shape, mine) is True
        async with get_session_factory()() as session:
            first = (await session.execute(text(
                "SELECT id FROM notification_deliveries "
                "WHERE notification_id = :mine AND status = 'pending' "
                "ORDER BY id LIMIT 1"
            ), {"mine": mine})).scalar_one()
        channel = _Channel()
        with (
            patch("app.engine.service.get_formatter", return_value=channel),
            _statements() as seen,
        ):
            await process_lane(Lane.BULK)
        # The pair: the portion is the job's lowest id, and only it.
        assert channel.delivered == [first]
        (statement, parameters), = [
            (s, p) for s, p in seen
            if (shape := sql_shape(s)).table == "notification_deliveries"
            and shape.order_by.startswith("notification_deliveries.id")
        ]
        async with get_session_factory()() as session:
            raw = await session.connection()
            plan = (await raw.exec_driver_sql(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement, parameters,
            )).scalar_one()
            await session.rollback()
        top = plan[0]["Plan"]
        read = top["Shared Hit Blocks"] + top.get("Shared Read Blocks", 0)
        assert read <= _BUFFERS, json.dumps(plan)
