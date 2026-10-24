# =============================================================================
# H15 -- the badge's index holds no waiting delivery; forgetting keeps one
# =============================================================================
#
# ix_notification_deliveries_unread (migration 0031) is (recipient_id,
# channel) WHERE status = 'sent' AND read_at IS NULL. A waiting delivery
# is not in it, so the path CI H13 caught -- a probe of "does the job
# still wait?" walking the old ix_notification_deliveries_inbox for every
# waiting delivery of every job -- does not exist. Held here on the
# planner's whole set of paths: ix_notification_deliveries_status, the
# one these queries are meant to take, is dropped in a transaction that
# is rolled back, and whatever the planner takes instead is read off the
# plan (the H14 FIX measurement, as a test).
# =============================================================================

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import select, text

import tests.test_h9_release as h11
import tests.test_portion_select as h12
from app.core.database import get_session_factory
from app.engine.constants import DeliveryStatus
from app.engine.models import Notification
from app.engine.processor import ready_now
from app.engine.service import first_waiting_delivery
from tests.conftest import analyze_with_nothing_waiting

_STATUS_INDEX = "ix_notification_deliveries_status"
# Indexes no probe of a job's waiting deliveries may walk.
_NOT_A_PATH = {
    "ix_notification_deliveries_unread",
    "ix_notification_deliveries_recipient_id",
}


def _sql(statement: Any) -> str:
    return str(statement.compile(compile_kwargs={"literal_binds": True}))


def _queries(job: Any) -> dict[str, str]:
    """The four queries of the H14 FIX that have the status index as
    their path. The probe and ready_now are the code's own builders;
    the portion select and close_waiting_deliveries are written out as
    app/engine/service.py writes them (deliver_notification,
    close_waiting_deliveries)."""
    pending = DeliveryStatus.PENDING.value
    due = "(next_retry_at IS NULL OR next_retry_at <= now())"
    return {
        "fold probe": _sql(first_waiting_delivery(job)),
        "ready_now": _sql(
            select(Notification.id)
            .where(ready_now(datetime.now(UTC)))
            .order_by(Notification.id)
            .limit(50)
        ),
        "portion select": (
            f"SELECT * FROM notification_deliveries "
            f"WHERE notification_id = '{job}' AND status = '{pending}' "
            f"AND {due} ORDER BY id LIMIT 5"
        ),
        "close_waiting_deliveries": (
            f"SELECT * FROM notification_deliveries "
            f"WHERE notification_id = '{job}' AND status = '{pending}' AND {due}"
        ),
    }


def _indexes(node: dict[str, Any]) -> set[str]:
    found = {node["Index Name"]} if "Index Name" in node else set()
    for child in node.get("Plans", []):
        found |= _indexes(child)
    return found


async def _paths(statement: str, *, without_status_index: bool) -> set[str]:
    async with get_session_factory()() as session:
        if without_status_index:
            await session.execute(text(f"DROP INDEX {_STATUS_INDEX}"))
        plan = (await session.execute(
            text("EXPLAIN (FORMAT JSON) " + statement),
        )).scalar_one()
        await session.rollback()
    root = (json.loads(plan) if isinstance(plan, str) else plan)[0]
    return _indexes(root["Plan"])


_STATES = (
    [(f"h11-{a}-{s}", "h11", a, s) for a in h11._AROUND
     for s in ("taken_with_nothing_waiting", "taken_after_the_sends")]
    + [(f"h12-{a}-{s}", "h12", a, s) for a in h12._SHAPES
       for s in ("taken_with_nothing_waiting", "taken_after_the_sends")]
)


@pytest.mark.usefixtures("pristine_tables")
class TestNoProbeWalksTheBadgesIndex:
    @pytest.mark.parametrize(
        ("kind", "form", "statistics"),
        [s[1:] for s in _STATES], ids=[s[0] for s in _STATES],
    )
    async def test_the_paths_left_without_the_status_index(
        self, kind: str, form: str, statistics: str,
    ) -> None:
        if kind == "h11":
            job, other = h11._sends_ids()
            await h11._sends(h11._AROUND[form], statistics, job, other)
        else:
            job, _ = await h12._seed(form, statistics)
        for name, statement in _queries(job).items():
            # The pair: with the status index the query takes it.
            assert _STATUS_INDEX in await _paths(
                statement, without_status_index=False,
            ), name
            paths = await _paths(statement, without_status_index=True)
            assert not paths & _NOT_A_PATH, (name, paths)
            assert "ix_notification_deliveries_inbox" not in paths, name


@pytest.mark.usefixtures("pristine_tables")
class TestTheBadgeAndForgetting:
    async def _seed(self) -> str:
        """A recipient of the other send (one waiting delivery) given a
        long read history of the done sends and three unread."""
        await h11._sends(h11._AROUND["two_percent"], "none", *h11._sends_ids())
        async with get_session_factory()() as session:
            recipient: str = (await session.execute(text(
                "SELECT recipient_id FROM notification_deliveries "
                "WHERE status = 'pending' GROUP BY recipient_id "
                "ORDER BY count(*), recipient_id LIMIT 1"
            ))).scalar_one()
            # Everyone's sent deliveries read (a box's inbox is mostly
            # read), then this recipient's long read history and three
            # unread: the badge's index holds those three, nothing else.
            await session.execute(text(
                "UPDATE notification_deliveries SET read_at = now() "
                "WHERE status = 'sent'"
            ))
            await session.execute(text(
                "UPDATE notification_deliveries SET recipient_id = :r, "
                "channel = 'in_app', read_at = now() WHERE id IN ("
                "SELECT id FROM notification_deliveries WHERE status = 'sent' "
                "ORDER BY id LIMIT 3003)"
            ), {"r": recipient})
            await session.execute(text(
                "UPDATE notification_deliveries SET read_at = NULL WHERE id IN ("
                "SELECT id FROM notification_deliveries WHERE recipient_id = :r "
                "AND status = 'sent' ORDER BY id LIMIT 3)"
            ), {"r": recipient})
            await session.execute(text("ANALYZE notification_deliveries"))
            await session.commit()
        return recipient

    async def test_the_badge_reads_its_index(self) -> None:
        recipient = await self._seed()
        for statement in (
            "SELECT count(*) FROM notification_deliveries WHERE recipient_id = "
            f"'{recipient}' AND status = 'sent' AND read_at IS NULL "
            "AND channel = 'in_app'",
            "UPDATE notification_deliveries SET read_at = now() WHERE "
            f"recipient_id = '{recipient}' AND status = 'sent' "
            "AND read_at IS NULL AND channel = 'in_app'",
        ):
            assert "ix_notification_deliveries_unread" in await _paths(
                statement, without_status_index=False,
            ), statement

    async def test_forgetting_has_an_index(self) -> None:
        """withdraw_recipient's reads by recipient -- its jobs, their
        waiting deliveries -- take ix_notification_deliveries_recipient_id
        (the badge's index no longer holds waiting rows); the probes do
        not (TestNoProbeWalksTheBadgesIndex)."""
        recipient = await self._seed()
        for statement in (
            "SELECT notification_id FROM notification_deliveries "
            f"WHERE recipient_id = '{recipient}'",
            "SELECT notification_id FROM notification_deliveries "
            f"WHERE recipient_id = '{recipient}' AND status = 'pending'",
        ):
            assert "ix_notification_deliveries_recipient_id" in await _paths(
                statement, without_status_index=False,
            ), statement


@pytest.mark.usefixtures("pristine_tables")
class TestNothingWaiting:
    async def test_the_seed_leaves_the_planner_as_it_was(self) -> None:
        """The index scans are off only for the seed's transaction
        (SET LOCAL): after its commit, the session plans with the
        defaults -- the queries the tests measure see the real planner."""
        await h11._sends(h11._AROUND["two_percent"], "none", *h11._sends_ids())
        async with get_session_factory()() as session:
            await analyze_with_nothing_waiting(session)
            assert (await session.execute(
                text("SHOW enable_indexscan"),
            )).scalar_one() == "off"  # the pair: it WAS off, in the seed
            await session.commit()
            for setting in ("enable_indexscan", "enable_bitmapscan"):
                assert (await session.execute(
                    text(f"SHOW {setting}"),
                )).scalar_one() == "on", setting

    async def test_the_statistics_say_nothing_waits_and_the_rows_do(self) -> None:
        await h11._sends(h11._AROUND["ten_percent"], "none", *h11._sends_ids())
        async with get_session_factory()() as session:
            await analyze_with_nothing_waiting(session)
            await session.commit()
            waiting = (await session.execute(text(
                "SELECT count(*) FROM notification_deliveries "
                "WHERE status = 'pending'"
            ))).scalar_one()
            in_statistics = (await session.execute(text(
                "SELECT coalesce('pending' = ANY(most_common_vals::text::text[]), "
                "false) FROM pg_stats WHERE tablename = 'notification_deliveries' "
                "AND attname = 'status'"
            ))).scalar_one()
        assert waiting == 5001
        assert in_statistics is False
