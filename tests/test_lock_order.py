# =============================================================================
# H7 R-A -- multi-row lockers take job rows in one order: no 40P01
# =============================================================================
#
# Real sessions on real connections; the order is built with locks and
# observed in pg_stat_activity / pg_locks, never with sleeps.
#
# THE CROSSING. Two developments over overlapping sets deadlock only if
# each holds a row the other needs NEXT. Plan settings alone do not
# build that: after a blocker commits, one side can take every row
# before the other wakes (measured while building H7). So each side is
# stopped on a row of ITS OWN first:
#   rows a, m, b, n; X's set is {a, m, b}, Y's is {b, n, a};
#   Z holds m and n;
#   without ORDER BY, X scans a, m, b (a sequential scan, heap order)
#   and Y scans b, n, a (the lane index, scheduled_at order) -- X takes
#   a and waits on m, Y takes b and waits on n;
#   Z commits: X takes m and needs b (Y's), Y takes n and needs a (X's)
#   -> 40P01, every time.
# With ORDER BY id both walk one order, so they queue on the first row
# they share and never hold each other's next one. The harness checks
# its own premise on the layout it builds: both scans, without ORDER
# BY, give those two orders. The layout is built ONCE, on tables as on a
# new box (pristine_tables, tests/conftest.py, H14): rows lie in the
# order they are written, and the plan is the one the statistics taken
# here give. Until H14 a layout that did not cross was removed and laid
# again, up to twenty times -- space and statistics earlier tests left
# decided where a row landed and which index a scan walked; the harness
# builds the crossing, the code decides the colour.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  close_notifications without ORDER BY      -> TestCloseAgainstClose
#   M2  withdraw_recipient without ORDER BY        -> TestWithdrawAgainstClose
#   M3  withdraw locking only the jobs it closes   -> TestWithdrawAgainstCleanup
#       (the cleanup's cascade then waits on a delivery, not on the job)
# NOT BUILT: which side wins the shared row once Z commits -- the
# loop's, not the code's; the tests hold "both end, no 40P01".
# =============================================================================

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import and_, select, text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.engine.constants import DeliveryStatus, NotificationStatus, TargetType
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import cleanup_expired_notifications
from app.engine.service import (
    close_notifications,
    create_notification,
    resolve_notification,
    withdraw_recipient,
)
from tests.helpers import (
    create_recipient,
    intake_fields,
    notification_row_fields,
)

_BOUND = 10.0
_POLL = 0.02

# X walks the table in insertion order, Y walks the lane index.
_SEQUENTIAL = (
    "SET LOCAL enable_indexscan = off",
    "SET LOCAL enable_indexonlyscan = off",
    "SET LOCAL enable_bitmapscan = off",
)
_BY_INDEX = (
    "SET LOCAL enable_seqscan = off",
    "SET LOCAL enable_bitmapscan = off",
)
# The detector's wait, short: a deadlock is reported in a fifth of a
# second, so a test that builds one does not sit out the default second.
_DETECT = "SET LOCAL deadlock_timeout = '200ms'"


async def _job(recipient: UUID, correlation: str, *, resolve: bool) -> UUID:
    async with get_session_factory()() as session:
        job = await create_notification(
            session, **intake_fields(), type="unit_event_in_app",
            title="T", body="B", target_type=TargetType.USER,
            target_value=str(recipient), correlation=correlation,
        )
        if resolve:
            await resolve_notification(session, job)
        await session.commit()
        return job.id


async def _recipients() -> tuple[UUID, UUID]:
    async with get_session_factory()() as session:
        mine = (await create_recipient(session)).id
        theirs = (await create_recipient(session)).id
        await session.commit()
        return mine, theirs


async def _placed(
    ids: dict[str, int], mine: UUID, theirs: UUID, shared: str,
) -> dict[str, UUID]:
    """Jobs a, m, b, n -- inserted in that order, never updated after
    (an update moves a row), with the ids given: each PROCESSING with
    one waiting in_app delivery, a, m, b to `mine` and n to `theirs`;
    a, b, n share the correlation; scheduled_at puts b first, then n,
    then a (the lane index's order)."""
    hours = {"a": 1, "m": 4, "b": 3, "n": 2}
    rows = {name: UUID(int=number) for name, number in ids.items()}
    now = datetime.now(UTC)
    async with get_session_factory()() as session:
        for name in "ambn":
            session.add(Notification(
                id=rows[name], type="unit_event_in_app", title="T", body="B",
                target_type=TargetType.USER, target_value="placed",
                status=NotificationStatus.PROCESSING,
                scheduled_at=now - timedelta(hours=hours[name]),
                correlation=shared if name != "m" else str(uuid4()),
                **notification_row_fields(),
            ))
            await session.flush()
            session.add(NotificationDelivery(
                notification_id=rows[name], channel="in_app",
                recipient_id=theirs if name == "n" else mine,
                status=DeliveryStatus.PENDING,
            ))
            await session.flush()
        await session.commit()
    return rows


async def _laid_out(
    ids: dict[str, int],
    premise: Callable[[dict[str, UUID], UUID, str], Awaitable[bool]],
) -> tuple[dict[str, UUID], UUID, str]:
    """Rows on which `premise` -- the crossing of the two unordered
    scans -- holds, laid once on pristine tables (the caller's
    pristine_tables): the rows lie in the order they are written, and
    the plans are those of the statistics taken here. The premise is
    checked, not retried."""
    mine, theirs = await _recipients()
    shared = str(uuid4())
    rows = await _placed(ids, mine, theirs, shared)
    async with get_session_factory()() as session:
        await session.execute(text("ANALYZE notifications"))
        await session.commit()
    assert await premise(rows, mine, shared), (
        "the layout does not cross: the two unordered scans do not walk "
        "a, m, b and b, n, a -- the harness's premise, not the code's"
    )
    return rows, mine, shared


async def _order(condition: Any, plan: tuple[str, ...]) -> list[UUID]:
    """The order the scan of `condition` walks under `plan`, WITHOUT an
    ORDER BY -- the order an unordered FOR UPDATE would lock in."""
    async with get_session_factory()() as session:
        for statement in plan:
            await session.execute(text(statement))
        found = list((await session.execute(
            select(Notification.id).where(
                condition,
                Notification.status.in_([
                    NotificationStatus.PENDING, NotificationStatus.PROCESSING,
                ]),
            )
        )).scalars())
        await session.rollback()
        return found


async def _lock_waiters() -> int:
    async with get_session_factory()() as session:
        return int((await session.execute(text(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE wait_event_type = 'Lock' AND datname = current_database()"
        ))).scalar_one())


Side = Callable[[AsyncSession], Awaitable[int]]


async def _cross(
    held: list[UUID], x: Side, x_plan: tuple[str, ...],
    y: Side, y_plan: tuple[str, ...],
) -> list[int | BaseException]:
    """Z holds `held`; X and Y start under their plans and are seen
    waiting; Z commits; both are awaited."""
    factory = get_session_factory()

    async def run(side: Side, plan: tuple[str, ...]) -> int:
        async with factory() as session:
            for statement in (*plan, _DETECT):
                await session.execute(text(statement))
            done = await side(session)
            await session.commit()
            return done

    async with factory() as z:
        await z.execute(
            select(Notification.id).where(Notification.id.in_(held))
            .with_for_update()
        )
        tasks = [
            asyncio.ensure_future(run(x, x_plan)),
            asyncio.ensure_future(run(y, y_plan)),
        ]
        for _ in range(int(_BOUND / _POLL)):
            if await _lock_waiters() >= 2:
                break
            await asyncio.sleep(_POLL)
        assert await _lock_waiters() == 2, "both sides must wait on Z"
        await z.commit()
    return list(await asyncio.gather(*tasks, return_exceptions=True))


def _no_deadlock(results: list[int | BaseException]) -> list[int]:
    for result in results:
        if isinstance(result, BaseException):
            raise AssertionError(f"a side failed: {result!r}") from result
    return [result for result in results if isinstance(result, int)]


class TestTheDeadlockIsNotRetried:
    """Why the order is needed, measured on the real stack: a 40P01 from
    asyncpg reaches the code as a bare DBAPIError, not an
    OperationalError -- the consumer retries the latter and dead-letters
    everything else (app/transport/consumer.py). Built with an explicit
    crossing of two raw sessions; the pair -- it IS 40P01."""

    async def test_a_deadlock_is_a_dbapi_error_not_operational(self) -> None:
        mine, theirs = await _recipients()
        rows = await _placed(
            {"a": 1, "m": 2, "b": 3, "n": 4}, mine, theirs, str(uuid4()),
        )
        a, b = rows["a"], rows["b"]

        def take(first: UUID, second: UUID) -> Side:
            async def side(session: AsyncSession) -> int:
                await session.execute(
                    select(Notification.id).where(Notification.id == first)
                    .with_for_update()
                )
                await asyncio.sleep(0.3)
                await session.execute(
                    select(Notification.id).where(Notification.id == second)
                    .with_for_update()
                )
                return 1
            return side

        factory = get_session_factory()

        async def run(side: Side) -> int:
            async with factory() as session:
                await session.execute(text(_DETECT))
                done = await side(session)
                await session.commit()
                return done

        results = await asyncio.gather(
            run(take(a, b)), run(take(b, a)), return_exceptions=True,
        )
        (failure,) = [r for r in results if isinstance(r, BaseException)]
        assert isinstance(failure, DBAPIError)
        assert not isinstance(failure, OperationalError)
        assert getattr(failure.orig, "sqlstate", None) == "40P01"


@pytest.mark.usefixtures("pristine_tables")
class TestCloseAgainstClose:
    """done-when R-A (2): expiry and cancellation over overlapping sets
    at once -- both end, no 40P01, every job closed once. M1."""

    async def test_both_end(self) -> None:
        def sets(rows: dict[str, UUID], shared: str) -> tuple[Any, Any]:
            return (
                Notification.id.in_([rows["a"], rows["m"], rows["b"]]),
                and_(
                    Notification.lane == "normal",
                    Notification.correlation == shared,
                ),
            )

        async def crosses(rows: dict[str, UUID], _: UUID, shared: str) -> bool:
            x_set, y_set = sets(rows, shared)
            a, m, b, n = (rows[k] for k in "ambn")
            return (
                await _order(x_set, _SEQUENTIAL) == [a, m, b]
                and await _order(y_set, _BY_INDEX) == [b, n, a]
            )

        rows, _, shared = await _laid_out(
            {"a": 1, "m": 2, "b": 3, "n": 4}, crosses,
        )
        a, m, b, n = (rows[k] for k in "ambn")
        x_set, y_set = sets(rows, shared)

        async def x(session: AsyncSession) -> int:
            return await close_notifications(
                session, x_set, NotificationStatus.CANCELLED,
            )

        async def y(session: AsyncSession) -> int:
            return await close_notifications(
                session, y_set, NotificationStatus.EXPIRED,
            )

        closed = _no_deadlock(await _cross([m, n], x, _SEQUENTIAL, y, _BY_INDEX))
        assert sum(closed) == 4  # a, m, b, n -- each by one side
        async with get_session_factory()() as session:
            statuses = set((await session.execute(
                select(Notification.status)
                .where(Notification.id.in_([a, m, b, n]))
            )).scalars())
        assert statuses <= {NotificationStatus.CANCELLED, NotificationStatus.EXPIRED}


@pytest.mark.usefixtures("pristine_tables")
class TestWithdrawAgainstClose:
    """Forgetting against cancellation over overlapping jobs at once --
    both end, no 40P01. M2.

    The cancellation walks its set in id order (THE ORDER OF ROW LOCKS),
    so the crossing is built from ids chosen here -- b < n < a (< m) --
    and a layout where forgetting, unordered, walks a, m, b: against
    the cancellation's b, n, a. The premise is checked on each layout."""

    async def test_both_end(self) -> None:
        def x_set(mine: UUID) -> Any:
            return Notification.id.in_(
                select(NotificationDelivery.notification_id)
                .where(NotificationDelivery.recipient_id == mine)
            )

        async def crosses(rows: dict[str, UUID], mine: UUID, _: str) -> bool:
            a, m, b = rows["a"], rows["m"], rows["b"]
            return await _order(x_set(mine), _SEQUENTIAL) == [a, m, b]

        rows, mine, shared = await _laid_out(
            {"b": 10, "n": 20, "a": 30, "m": 40}, crosses,
        )
        a, m, b, n = (rows[k] for k in "ambn")
        assert sorted([a, b, n]) == [b, n, a]
        y_set = Notification.correlation == shared

        async def x(session: AsyncSession) -> int:
            return await withdraw_recipient(session, mine)

        async def y(session: AsyncSession) -> int:
            return await close_notifications(
                session, y_set, NotificationStatus.CANCELLED,
            )

        _no_deadlock(await _cross([m, n], x, _SEQUENTIAL, y, ()))
        async with get_session_factory()() as session:
            left = (await session.execute(
                select(Notification.status)
                .where(Notification.id.in_([a, m, b, n]))
            )).scalars().all()
        assert len(left) == 4
        assert NotificationStatus.PROCESSING not in left


class TestWithdrawAgainstCleanup:
    """The implicit pair (H7 gate 0.3): forgetting rewrites the
    deliveries and journal of FINISHED jobs; the expiry cleanup deletes
    finished jobs with their deliveries and journal by cascade. Since
    forgetting locks every job it will write, the two queue on the JOB
    row -- never on a child, where their orders would cross. Seen in
    pg_locks: the waiting cleanup holds its tuple lock on notifications.
    M3 (only the jobs it closes locked) moves the wait onto
    notification_deliveries. Both end; the job is gone, forgotten
    first."""

    async def test_they_queue_on_the_job(self) -> None:
        async with get_session_factory()() as session:
            recipient = (await create_recipient(session)).id
            await session.commit()
        job = await _job(recipient, str(uuid4()), resolve=True)
        async with get_session_factory()() as session:
            await session.execute(text(
                "UPDATE notification_deliveries SET status = 'sent', "
                "error_message = 'provider said x' WHERE notification_id = :i"
            ), {"i": job})
            await session.execute(text(
                "UPDATE notifications SET status = 'sent', "
                "expiry_at = now() - interval '1 minute' WHERE id = :i"
            ), {"i": job})
            await session.commit()

        factory = get_session_factory()
        async with factory() as forgetting:
            await withdraw_recipient(forgetting, recipient)
            cleanup = asyncio.ensure_future(cleanup_expired_notifications())
            relation = None
            for _ in range(int(_BOUND / _POLL)):
                async with factory() as observer:
                    relation = (await observer.execute(text(
                        "SELECT l.relation::regclass::text FROM pg_locks l "
                        "JOIN pg_stat_activity a ON a.pid = l.pid "
                        "WHERE a.wait_event_type = 'Lock' "
                        "AND a.query ILIKE 'DELETE FROM notifications%' "
                        "AND l.locktype = 'tuple' AND l.granted"
                    ))).scalar_one_or_none()
                if relation is not None or cleanup.done():
                    break
                await asyncio.sleep(_POLL)
            assert relation == "notifications"
            assert not cleanup.done()
            await forgetting.commit()
        assert await asyncio.wait_for(cleanup, _BOUND) == 1
        async with factory() as session:
            assert await session.get(Notification, job) is None


@pytest.mark.parametrize("name", ["close_notifications", "withdraw_recipient"])
def test_the_rule_is_written_where_the_rows_are_locked(name: str) -> None:
    """The order is stated once, in the block both paths point to."""
    import inspect

    import app.engine.service as service

    source = inspect.getsource(getattr(service, name))
    assert ".order_by(Notification.id)" in source
    assert "THE ORDER OF ROW LOCKS" in inspect.getsource(service)
