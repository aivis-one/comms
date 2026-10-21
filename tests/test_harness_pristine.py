# =============================================================================
# H14 -- the harness of plan and cost tests: pristine tables, the watchman
# =============================================================================
#
# pristine_tables and explain_needs_pristine live in tests/conftest.py;
# this file holds what they promise, measured on the database's own
# catalogues (pg_class, pg_statistic), not on the fixture's word.
# =============================================================================

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import text

from app.core.database import get_session_factory
from tests.conftest import (
    ExplainWatch,
    explain_complaint,
    make_tables_pristine,
    refuse_rows_before_pristine,
)
from tests.helpers import create_recipient, notification_row_fields


async def _scalar(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        value = (await session.execute(text(statement), params)).scalar_one()
        await session.commit()
        return value


async def _statistics_rows() -> int:
    count: int = await _scalar(
        "SELECT count(*) FROM pg_statistic WHERE starelid IN ("
        "SELECT c.oid FROM pg_class c JOIN pg_namespace n "
        "ON n.oid = c.relnamespace WHERE n.nspname = 'public')"
    )
    return count


async def _heap_pages() -> int:
    pages: int = await _scalar(
        "SELECT coalesce(sum(pg_relation_size(c.oid)), 0) / 8192 "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relkind = 'r' "
        "AND c.relname <> 'alembic_version'"
    )
    return int(pages)


async def _dirty_the_tables() -> None:
    """What a heavy earlier test leaves: rows written and deleted (dead
    tuples, freed space), and statistics taken."""
    async with get_session_factory()() as session:
        await session.execute(text(
            "INSERT INTO recipients (id, locale, version, snapshot_fingerprint) "
            "SELECT gen_random_uuid(), 'en', 1, 'h14' "
            "FROM generate_series(1, 3000)"
        ))
        await session.commit()
        await session.execute(text("ANALYZE recipients"))
        await session.execute(text(
            "DELETE FROM recipients WHERE snapshot_fingerprint = 'h14'"
        ))
        await session.commit()


class TestPristine:
    async def test_no_rows_no_pages_no_statistics(self) -> None:
        await _dirty_the_tables()
        # The pair: the earlier test's traces are really there.
        assert await _heap_pages() > 0
        assert await _statistics_rows() > 0
        await make_tables_pristine()
        assert await _heap_pages() == 0
        assert await _statistics_rows() == 0

    async def test_rows_lie_in_the_order_they_are_written(self) -> None:
        """The premise plan tests build on: after pristine, the heap
        order is the insertion order -- freed space is not reused."""
        await _dirty_the_tables()
        await make_tables_pristine()
        async with get_session_factory()() as session:
            first = await create_recipient(session)
            second = await create_recipient(session)
            await session.commit()
        assert await _scalar(
            "SELECT (SELECT ctid FROM recipients WHERE id = :a) < "
            "(SELECT ctid FROM recipients WHERE id = :b)",
            a=first.id, b=second.id,
        ) is True

    async def test_identity_continues(self) -> None:
        """CONTINUE IDENTITY, as clean_db's DELETE: a journal id after
        pristine is above every id before it -- no test may see a
        restart."""
        async def journal_id() -> int:
            async with get_session_factory()() as session:
                job = await session.execute(text(
                    "INSERT INTO notifications (id, type, title, body, "
                    "target_type, target_value, idempotency_key, fingerprint, "
                    "channels, expiry_layer, push_on, retry_max_attempts, "
                    "retry_backoff_seconds, recipient_schedule, lane, status) "
                    "VALUES (gen_random_uuid(), 'unit_event', 'T', 'B', 'all', "
                    "'*', gen_random_uuid()::text, repeat('a', 64), "
                    "'[\"in_app\"]'::jsonb, :layer, :push, :tries, :backoff, "
                    ":schedule, :lane, 'pending') RETURNING id"
                ), {
                    "layer": row["expiry_layer"], "push": row["push_on"],
                    "tries": row["retry_max_attempts"],
                    "backoff": row["retry_backoff_seconds"],
                    "schedule": row["recipient_schedule"], "lane": row["lane"],
                })
                transition = await session.execute(text(
                    "INSERT INTO notification_transitions (notification_id, "
                    "subject, step, outcome, attempt) VALUES (:job, 'job', "
                    "'intake', 'pending', 0) RETURNING id"
                ), {"job": job.scalar_one()})
                value: int = transition.scalar_one()
                await session.commit()
                return value

        row = notification_row_fields()
        before = await journal_id()
        await make_tables_pristine()
        assert await journal_id() > before


class TestTheWatchman:
    @pytest.mark.usefixtures("pristine_tables")
    async def test_the_watch_sees_an_explain_and_only_that(self) -> None:
        with ExplainWatch() as watch:
            await _scalar("SELECT 1")
            async with get_session_factory()() as session:
                await session.execute(text("EXPLAIN SELECT 1"))
        assert watch.seen == ["EXPLAIN SELECT 1"]

    def test_the_verdict(self) -> None:
        seen = ["EXPLAIN (FORMAT JSON) SELECT ..."]
        complaint = explain_complaint("t::a", ["clean_db"], seen)
        assert complaint is not None
        assert "t::a reads a plan without pristine_tables" in complaint
        # The pairs: with the fixture, or with no plan read, no verdict.
        assert explain_complaint("t::a", ["pristine_tables"], seen) is None
        assert explain_complaint("t::a", ["clean_db"], []) is None


class TestTheSeedOrder:
    async def test_a_row_before_the_fixture_is_refused_by_table(self) -> None:
        """A seed set up ahead of pristine_tables -- a class's autouse
        fixture, which pytest runs before a usefixtures mark -- would be
        truncated silently; the fixture refuses it, naming the table."""
        async with get_session_factory()() as session:
            await create_recipient(session)
            await session.commit()
        with pytest.raises(
            pytest.fail.Exception, match=r"rows written before it \(recipients\)",
        ):
            await refuse_rows_before_pristine()

    async def test_empty_tables_pass(self) -> None:
        """The pair: after clean_db, nothing is refused."""
        await refuse_rows_before_pristine()
