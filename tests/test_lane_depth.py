# =============================================================================
# H8 R-E -- the depth of each lane's queue
# =============================================================================
#
# GET /api/v1/lanes/depth, read from the job rows at the moment of the
# request (app/engine/depth.py): ready -- what the lane's next pass would
# select (the SAME condition, processor.ready_now); waiting -- active and
# not ready yet; oldest_ready_seconds. Every lane, every key.
#
# MUTATIONS these tests were written against (each turns one red):
#   M9   ready counted as status = pending (not the lane's condition)
#                                         -> TestTheStates
#   M10  a lane with no row left out of the answer -> TestEmptyAndUntaken
#   M11  a key added to or dropped from INTEGRATION.md -> TestTheContract
#   M12  the status condition dropped from the statement -> TestThePlan
# =============================================================================

import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from httpx import AsyncClient
from sqlalchemy import event, text

from app.core.config import settings
from app.core.database import get_engine, get_session_factory
from app.core.lanes import Lane
from app.engine.constants import (
    DeliveryStatus,
    NotificationStatus,
    TargetType,
    WaitReason,
)
from app.engine.depth import lane_depth
from app.engine.models import Notification, NotificationDelivery
from tests.helpers import create_recipient, notification_row_fields

_URL = "/api/v1/lanes/depth"
_DOC = (Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md").read_text(
    encoding="utf-8",
)
_TOKEN = "zq7" + "-depth-token-" + "xk4"


async def _job(
    lane: Lane,
    status: NotificationStatus = NotificationStatus.PENDING,
    *,
    scheduled: timedelta = timedelta(minutes=-1),
    gate: timedelta | None = None,
    expires: timedelta | None = None,
    delivery_due: timedelta | None = None,
) -> UUID:
    """A job of `lane` built directly; `delivery_due` adds one waiting
    delivery due at now + that (a negative one is due)."""
    now = datetime.now(UTC)
    fields = notification_row_fields()
    fields["lane"] = lane.value
    async with get_session_factory()() as session:
        job = Notification(
            type="unit_event_in_app", title="T", body="B",
            target_type=TargetType.ALL, target_value="*",
            status=status,
            scheduled_at=now + scheduled,
            pipeline_retry_at=None if gate is None else now + gate,
            # ck_notifications_pipeline(_gate): a gate follows a failure,
            # which names its step and its error.
            pipeline_attempts=0 if gate is None else 1,
            pipeline_step=None if gate is None else "deliver",
            pipeline_error=None if gate is None else "builtins.X at app/x.py:1",
            expiry_at=None if expires is None else now + expires,
            **fields,
        )
        session.add(job)
        await session.flush()
        if delivery_due is not None:
            recipient = (await create_recipient(session)).id
            session.add(NotificationDelivery(
                notification_id=job.id, recipient_id=recipient,
                channel="in_app", status=DeliveryStatus.PENDING,
                next_retry_at=now + delivery_due,
                # ck_deliveries_wait_reason: a retry time says why.
                wait_reason=WaitReason.TRANSIENT_BACKOFF, attempts=1,
            ))
        await session.commit()
        return job.id


async def _depth(client: AsyncClient) -> dict[str, Any]:
    response = await client.get(_URL)
    assert response.status_code == 200
    body: dict[str, Any] = response.json()
    return body


class TestTheStates:
    """Each state of a job, where it counts. M9."""

    async def test_every_state(self, client: AsyncClient) -> None:
        normal = Lane.NORMAL
        await _job(normal)  # pending, due -> ready
        await _job(normal, scheduled=timedelta(hours=1))  # later -> waiting
        await _job(normal, gate=timedelta(minutes=5))  # pipeline gate -> waiting
        await _job(  # processing, a delivery due -> ready
            normal, NotificationStatus.PROCESSING, delivery_due=timedelta(seconds=-1),
        )
        await _job(  # processing, every delivery on its backoff -> waiting
            normal, NotificationStatus.PROCESSING, delivery_due=timedelta(minutes=3),
        )
        await _job(normal, expires=timedelta(minutes=-1))  # expired -> neither
        await _job(normal, NotificationStatus.SENT)  # outcome -> neither
        depth = (await _depth(client))["lanes"]["normal"]
        assert (depth["ready"], depth["waiting"]) == (2, 3)
        assert depth["oldest_ready_seconds"] is not None
        assert 55 <= depth["oldest_ready_seconds"] <= 120

    async def test_ready_is_what_the_pass_would_select(self) -> None:
        """The pair to the shared condition: depth's ready equals the
        number the lane's own selection returns, batch limit lifted."""
        from sqlalchemy import func, select

        from app.engine.processor import ready_now

        for _ in range(3):
            await _job(Lane.CRITICAL)
        await _job(Lane.CRITICAL, scheduled=timedelta(hours=1))
        async with get_session_factory()() as session:
            depth = (await lane_depth(session))["lanes"]["critical"]
            selected = (await session.execute(
                select(func.count()).select_from(Notification).where(
                    Notification.lane == Lane.CRITICAL.value,
                    ready_now(datetime.now(UTC)),
                )
            )).scalar_one()
        assert depth["ready"] == selected == 3


class TestEmptyAndUntaken:
    """done-when R-E (2), (3). M10."""

    async def test_an_empty_lane_is_zeros_not_a_missing_key(
        self, client: AsyncClient,
    ) -> None:
        await _job(Lane.NORMAL)
        lanes = (await _depth(client))["lanes"]
        assert set(lanes) == {lane.value for lane in Lane}
        assert lanes["interactive"] == {
            "ready": 0, "waiting": 0, "oldest_ready_seconds": None,
        }
        assert lanes["normal"]["ready"] == 1  # the pair: something counts

    async def test_a_lane_no_process_takes_still_counts(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            settings, "comms_worker_lanes", "interactive,critical,normal",
        )
        assert Lane.BULK not in settings.worker_lanes
        await _job(Lane.BULK)
        assert (await _depth(client))["lanes"]["bulk"]["ready"] == 1


class TestReadOnlyAndAuth:
    """done-when R-E (1): the service token, and a READ ONLY snapshot."""

    async def test_the_session_is_read_only(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def probe(session: Any) -> dict[str, Any]:
            show = "SHOW transaction_read_only"
            mode = (await session.execute(text(show))).scalar_one()
            show = "SHOW transaction_isolation"
            level = (await session.execute(text(show))).scalar_one()
            return {"lanes": {}, "mode": mode, "level": level}

        monkeypatch.setattr("app.api.lanes.lane_depth", probe)
        body = await _depth(client)
        assert (body["mode"], body["level"]) == ("on", "repeatable read")

    async def test_no_token_401_the_token_200(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "comms_service_token", _TOKEN)
        assert (await client.get(_URL)).status_code == 401
        response = await client.get(_URL, headers={"Authorization": f"Bearer {_TOKEN}"})
        assert response.status_code == 200
        assert response.json()["lanes"]


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


class TestThePlan:
    """done-when R-E (4): on 50 000 rows of realistic shape -- most jobs
    finished, a few active in every lane -- the statement the depth
    sends goes by the lane index and scans no table. M12."""

    async def test_by_the_lane_index(self) -> None:
        async with get_session_factory()() as session:
            await session.execute(text(
                "INSERT INTO notifications (id, type, title, body, target_type, "
                "target_value, idempotency_key, fingerprint, channels, "
                "expiry_layer, push_on, retry_max_attempts, "
                "retry_backoff_seconds, recipient_schedule, lane, status, "
                "scheduled_at, created_at) "
                "SELECT gen_random_uuid(), 'unit_event', 'T', 'B', 'all', '*', "
                "'h8-seed-' || g, repeat('a', 64), '[\"in_app\"]'::jsonb, "
                "'default', 'none', 3, 30, 'respect', "
                "(ARRAY['interactive','critical','normal','bulk'])[g % 4 + 1], "
                "CASE WHEN g % 40 = 0 THEN 'pending' "
                "     WHEN g % 41 = 0 THEN 'processing' ELSE 'sent' END, "
                "now() - (g % 600) * interval '1 minute', now() "
                "FROM generate_series(1, 50000) g"
            ))
            await session.commit()
            # One delivery per job, as the table has in life: the "a
            # delivery is due" probe must go by its index, not a scan.
            recipient = (await create_recipient(session)).id
            await session.execute(text(
                "INSERT INTO notification_deliveries "
                "(id, notification_id, recipient_id, channel, status) "
                "SELECT gen_random_uuid(), id, :r, 'in_app', "
                "CASE WHEN status = 'processing' THEN 'pending' ELSE 'sent' END "
                "FROM notifications WHERE idempotency_key LIKE 'h8-seed-%'"
            ), {"r": recipient})
            await session.commit()
            await session.execute(text("ANALYZE notifications"))
            await session.execute(text("ANALYZE notification_deliveries"))
            await session.commit()
        async with get_session_factory()() as session:
            with _captured() as seen:
                await lane_depth(session)
            (statement, parameters), = [
                (s, p) for s, p in seen if "FROM notifications" in s
            ]
            raw = await session.connection()
            plan = (await raw.exec_driver_sql(
                "EXPLAIN (FORMAT JSON) " + statement, parameters,
            )).scalar_one()
            await session.rollback()
        shape = _shape(plan)
        # Only the active rows, through an index: of status, or of lane
        # and status -- the planner's choice between two indexes whose
        # active ranges are the same rows (measured: status, on this
        # shape). Neither table is scanned.
        assert re.search(
            r"Index (Only )?Scan notifications "
            r"(ix_notifications_status|ix_notifications_lane_status_scheduled)",
            shape,
        ), shape
        assert "Seq Scan notifications" not in shape, shape
        assert "Seq Scan notification_deliveries" not in shape, shape
        # And the index is entered by the ACTIVE statuses -- the finished
        # rows, nearly all of the table, are never read.
        scans = [
            node for node in _nodes(plan)
            if node.get("Relation Name") == "notifications"
        ]
        assert scans and all(
            "status" in node.get("Index Cond", "") for node in scans
        ), scans


def _nodes(plan: Any) -> list[dict[str, Any]]:
    node = plan[0]["Plan"] if isinstance(plan, list) else plan
    found = [node]
    for child in node.get("Plans", []):
        found.extend(_nodes(child))
    return found


def _shape(plan: Any, depth: int = 0) -> str:
    """The plan's nodes, one per line: type, relation, index."""
    node = plan[0]["Plan"] if isinstance(plan, list) else plan
    line = " ".join(filter(None, (
        "  " * depth + node["Node Type"],
        node.get("Relation Name"), node.get("Index Name"),
    )))
    return "\n".join([line, *(_shape(c, depth + 1) for c in node.get("Plans", []))])


class TestTheContract:
    """done-when R-E (5): deploy/INTEGRATION.md "Lane depth" against the
    code, both ways. M11."""

    @staticmethod
    def _section() -> str:
        start = _DOC.index("### Lane depth\n")
        end = re.compile(r"^#{2,3} ", re.M).search(_DOC, start + 4)
        assert end is not None
        return _DOC[start:end.start()]

    async def test_the_example_is_a_real_answer(self, client: AsyncClient) -> None:
        (block,) = re.findall(r"```json\n(.*?)```", self._section(), re.S)
        example = json.loads(block)
        real = await _depth(client)
        assert set(example) == set(real) == {"lanes"}
        assert set(example["lanes"]) == set(real["lanes"]) == {
            lane.value for lane in Lane
        }
        for lane, entry in real["lanes"].items():
            assert set(example["lanes"][lane]) == set(entry), lane

    async def test_the_table_names_every_field(self, client: AsyncClient) -> None:
        fields = set(re.findall(r"^\| `(\w+)` \|", self._section(), re.M))
        entry = next(iter((await _depth(client))["lanes"].values()))
        assert fields == set(entry)
        assert fields  # the pair: the table is found
