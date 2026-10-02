# =============================================================================
# D1 / R2 -- the insert race of a brand-new id survives on every path.
# =============================================================================
#
# Two real sessions, no mocks: session A runs the write and holds its
# INSERT uncommitted; session B, in a task, reads (finds nothing, A has
# not committed), inserts and waits on A's key lock; A commits; B's
# INSERT fails on the key and the helper runs the rule again. The log
# line of the retry proves the race happened -- without it a green test
# could have run the two writes one after the other.
#
# Before D1 the event path had no SAVEPOINT: B's IntegrityError reached
# the consumer's catch-all and the event went to the dead-letter stream.
# Here the handler is called as the consumer calls it; "no exception out
# of the handler" is "no dead letter".
#
# MUTATIONS:
#   M12 the helper without its SAVEPOINT -> every race test (the retry
#       runs inside an aborted transaction)
#   M13 the PUT route keeps its own begin_nested -> test_put_has_no_copy
# =============================================================================

import ast
import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import GroupMembership, Recipient
from app.core.database import get_session_factory
from app.core.exceptions import StaleSnapshotError
from app.transport.events import parse_event
from app.transport.handlers import handle_event
from tests.helpers import create_recipient


def _event(event: str, **data: Any) -> Any:
    return parse_event({"event": event, "data": json.dumps({"v": 1, **data})})


def _snapshot(recipient_id: UUID, version: int, locale: str = "en") -> Any:
    return _event(
        "user_upserted", recipient_id=str(recipient_id), version=version,
        telegram_id=None, email=None, locale=locale, timezone=None,
        active=True,
    )


async def _race(
    first: Callable[[AsyncSession], Awaitable[Any]],
    second: Callable[[AsyncSession], Awaitable[Any]],
) -> Any:
    """Run `first` in session A and hold it; start `second` in session B;
    commit A; return B's result (or raise B's exception)."""
    factory = get_session_factory()
    async with factory() as a, factory() as b:
        await first(a)

        async def run_b() -> Any:
            result = await second(b)
            await b.commit()
            return result

        task = asyncio.create_task(run_b())
        await asyncio.sleep(0.5)  # B is now waiting on A's key lock
        assert not task.done(), "B must be blocked on A's INSERT, not finished"
        await a.commit()
        return await task


async def _count(model: Any, **where: Any) -> int:
    async with get_session_factory()() as s:
        query = select(func.count()).select_from(model)
        for column, value in where.items():
            query = query.where(getattr(model, column) == value)
        return int(await s.scalar(query) or 0)


class TestTheRace:
    async def test_two_snapshots_of_a_new_id(self) -> None:
        """apply_snapshot: one row, the loser's replay is no error."""
        rid = uuid4()
        with capture_logs() as logs:
            await _race(
                lambda s: handle_event(s, _snapshot(rid, 1)),
                lambda s: handle_event(s, _snapshot(rid, 1)),
            )
        assert await _count(Recipient, id=rid) == 1
        assert [log for log in logs if log["event"] == "recipient_upsert_raced"]

    async def test_two_deletions_of_an_id_never_seen(self) -> None:
        """tombstone: a deletion of an unknown id inserts its tombstone;
        two at once -- one row."""
        rid = uuid4()
        deleted = _event("user_deleted", recipient_id=str(rid), version=1)
        with capture_logs() as logs:
            await _race(
                lambda s: handle_event(s, deleted),
                lambda s: handle_event(s, deleted),
            )
        assert await _count(Recipient, id=rid) == 1
        assert [log for log in logs if log["event"] == "recipient_tombstone_raced"]

    async def test_two_memberships_of_one_pair(self) -> None:
        """group_changed(member=true): one membership row."""
        rid = uuid4()
        async with get_session_factory()() as s:
            await create_recipient(s, recipient_id=rid)
            await s.commit()
        joined = _event(
            "group_changed", group_key="g-race", recipient_id=str(rid),
            member=True,
        )
        with capture_logs() as logs:
            await _race(
                lambda s: handle_event(s, joined),
                lambda s: handle_event(s, joined),
            )
        assert await _count(GroupMembership, recipient_id=rid) == 1
        assert [log for log in logs if log["event"] == "group_member_add_raced"]


class TestTheLoserDecidesByTheRule:
    """done-when (2): the loser meets the winner's row and the version
    rule decides, exactly as without the race."""

    async def test_a_newer_loser_is_applied(self) -> None:
        rid = uuid4()
        await _race(
            lambda s: handle_event(s, _snapshot(rid, 1, locale="en")),
            lambda s: handle_event(s, _snapshot(rid, 2, locale="de")),
        )
        async with get_session_factory()() as s:
            row = await s.get(Recipient, rid)
        assert row is not None
        assert (row.version, row.locale) == (2, "de")

    async def test_an_older_loser_is_stale_not_a_dead_letter(self) -> None:
        """StaleSnapshotError is a ConflictError: the consumer refuses it
        by class and acknowledges it -- never the DLQ."""
        rid = uuid4()
        with pytest.raises(StaleSnapshotError):
            await _race(
                lambda s: handle_event(s, _snapshot(rid, 2)),
                lambda s: handle_event(s, _snapshot(rid, 1)),
            )
        async with get_session_factory()() as s:
            row = await s.get(Recipient, rid)
        assert row is not None
        assert row.version == 2


_RECIPIENTS_ROUTE = (
    Path(__file__).resolve().parents[1] / "app" / "api" / "recipients.py"
)


def test_put_has_no_copy_of_the_mechanism() -> None:
    """done-when (4). M13. The pair: the route still calls the rule."""
    tree = ast.parse(_RECIPIENTS_ROUTE.read_text())
    attributes = {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }
    assert "begin_nested" not in attributes
    assert "apply_snapshot" in attributes
