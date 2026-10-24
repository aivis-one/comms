# =============================================================================
# H14 -- "does the job still wait?" has no path through a full scan
# =============================================================================
#
# app/engine/service.py first_waiting_delivery is THE probe of a job's
# waiting deliveries: the fold's (tests/test_h9_release.py
# TestTheProbePlan holds that one) and, correlated to the job row, a
# lane's selection and its depth (app/engine/processor.py ready_now) --
# held here. Before H14 ready_now asked by EXISTS under an OR, and on
# stale statistics Postgres planned it as a hashed subplan that reads
# every waiting delivery of every job to build the hash: 4 993 buffers
# on 50 000 deliveries (H14 report).
#
# The states are the ones TestTheProbePlan (H11) and
# tests/test_portion_select.py (H12) build -- the job's waiting rows
# last in the heap or last by id, beside another send's -- under every
# statistics those files take, on pristine tables (tests/conftest.py).
# The statements are the ones the code sends, captured off the
# connection, explained with their own parameters.
# =============================================================================

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import event

import tests.test_h9_release as h11
import tests.test_portion_select as h12
from app.core.database import get_engine, get_session_factory
from app.core.lanes import Lane
from app.engine.depth import lane_depth
from app.engine.processor import process_lane
from tests.conftest import make_tables_pristine
from tests.helpers import sql_shape

# The probe's whole budget, buffers -- TestTheProbePlan's for the fold.
_BUDGET = 10
_INDEX = "ix_notification_deliveries_status"


@contextmanager
def _captured() -> Iterator[list[tuple[str, Any]]]:
    seen: list[tuple[str, Any]] = []
    engine = get_engine().sync_engine

    def record(conn: Any, cursor: Any, statement: str, parameters: Any,
               context: Any, executemany: bool) -> None:
        seen.append((statement, parameters))

    event.listen(engine, "before_cursor_execute", record)
    try:
        yield seen
    finally:
        event.remove(engine, "before_cursor_execute", record)


class _Delivered:
    async def deliver(self, *args: Any) -> bool:
        return True


async def _statements() -> list[tuple[str, Any]]:
    """The lane selection and the lane depth, as the code sends them --
    captured on the empty pristine tables, before any seed."""
    with _captured() as seen:
        with patch("app.engine.service.get_formatter", return_value=_Delivered()):
            await process_lane(Lane.BULK)
        async with get_session_factory()() as session:
            await lane_depth(session)
    probing = [
        (statement, parameters) for statement, parameters in seen
        if (shape := sql_shape(statement)).table == "notifications"
        and "notification_deliveries" in shape.sources
    ]
    assert len(probing) >= 2, "the selection and the depth both probe"
    return probing


async def _explain(statement: str, parameters: Any) -> dict[str, Any]:
    async with get_session_factory()() as session:
        raw = await session.connection()
        plan = (await raw.exec_driver_sql(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement, parameters,
        )).scalar_one()
        await session.rollback()
    root: dict[str, Any] = (json.loads(plan) if isinstance(plan, str) else plan)[0]
    return root


def _nodes(node: dict[str, Any]) -> list[dict[str, Any]]:
    out = [node]
    for child in node.get("Plans", []):
        out.extend(_nodes(child))
    return out


def _buffers(node: dict[str, Any]) -> int:
    return int(node["Shared Hit Blocks"]) + int(node.get("Shared Read Blocks", 0))


async def _seed_h11(around: str, statistics: str) -> None:
    await h11._sends(h11._AROUND[around], statistics, *h11._sends_ids())


async def _seed_h12(shape: str, statistics: str) -> None:
    await h12._seed(shape, statistics)


_STATES = (
    [
        (f"h11-{around}-{statistics}", _seed_h11, around, statistics)
        for around in h11._AROUND
        for statistics in (
            "taken_before_the_sends", "taken_after_the_sends", "none",
            "taken_with_nothing_waiting",
        )
    ]
    + [
        (f"h12-{shape}-{statistics}", _seed_h12, shape, statistics)
        for shape in h12._SHAPES
        for statistics in h12._STATISTICS
    ]
)


@pytest.mark.usefixtures("pristine_tables")
class TestReadyNow:
    @pytest.mark.parametrize(
        ("seed", "form", "statistics"),
        [state[1:] for state in _STATES],
        ids=[state[0] for state in _STATES],
    )
    async def test_the_selection_and_the_depth_probe_on_the_index(
        self, seed: Any, form: str, statistics: str,
    ) -> None:
        statements = await _statements()
        await make_tables_pristine()
        await seed(form, statistics)
        for statement, parameters in statements:
            root = await _explain(statement, parameters)
            nodes = _nodes(root["Plan"])
            on_deliveries = [
                n for n in nodes
                if n.get("Relation Name") == "notification_deliveries"
                or n.get("Index Name", "").startswith("ix_notification_deliveries")
            ]
            # Never a scan of the table, never a hash of its waiting rows.
            assert not [
                n for n in on_deliveries if n["Node Type"] == "Seq Scan"
            ], root
            assert not [
                n for n in nodes
                if "hashed" in n.get("Subplan Name", "")
            ], root
            # The pair: the deliveries ARE probed, on the index, and ONE
            # probe -- of one job: a node's buffers over its loops, per
            # loop -- stays in the budget. The depth runs it three times
            # a job (ready, not ready, oldest ready), the selection once;
            # the rest of the statement reads the jobs, the lane index's
            # business (tests/test_lanes.py, tests/test_lane_depth.py).
            assert {n.get("Index Name") for n in on_deliveries} == {_INDEX}, root
            for node in (n for n in on_deliveries if "Index Name" in n):
                assert _buffers(node) / max(1, node["Actual Loops"]) <= _BUDGET, (
                    node
                )
