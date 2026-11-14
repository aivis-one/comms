"""THE MEASUREMENT behind app/core/database.py LIMITS (W5-3).

Not a test (no test_ prefix, never collected): run by hand, against a
database whose name ends in _test, with the environment of CI
(.github/workflows/ci.yml):

    python -m tests.measure_db_limits

It seeds a volume shaped like production at its largest expected size
(SEED: jobs, deliveries, people), then times, on the real functions of
comms, the longest legitimate database work of each process -- its
statements, not its waits for another transaction's lock (those are the
attempt's, bounded by app/engine/service.py longest_attempt_hold). Each is
run REPEATS times, in a transaction rolled back, so the seed stays the
same for the next; the worst run is printed. The seed is deleted at the
end. The printed numbers are written into LIMITS' MEASURED by hand, with
the date and the commit they were taken at.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import (
    ProcessRole,
    dispose_engine,
    get_session_factory,
    set_attempt_hold,
    set_process_role,
)
from app.core.lanes import Lane
from app.engine.constants import NotificationStatus
from app.engine.depth import lane_depth
from app.engine.models import Notification
from app.engine.processor import ready_now
from app.engine.reminders import cancel_reminders
from app.engine.service import (
    accept_notification,
    close_notifications,
    delete_expired_notifications_batch,
    delete_terminal_notifications_batch,
    get_unread_count,
    list_recipient_deliveries,
    longest_attempt_hold,
    resolve_notification,
    withdraw_recipient,
)
from app.profile.loader import FileProfileSource, install_profile, load_profile
from tests.helpers import TEST_FINGERPRINT

SEED = {"jobs": 100_000, "people": 10_000}
"""The volume (W5-3 gate, decided 13 November 2026: 100 000 jobs, 200 000
deliveries, 10 000 people). The deliveries: _DELIVERIES_PER_JOB to every
job but the pending tenth -- 270 000, above the 200 000 decided."""

_DELIVERIES_PER_JOB = 3

REPEATS = 3

_PROFILE_DIR = Path(__file__).parent / "fixtures" / "profile"
_PREFIX = "w53-seed-"


async def _seed(session: AsyncSession) -> None:
    people, jobs = SEED["people"], SEED["jobs"]
    await session.execute(text(
        "INSERT INTO recipients (id, telegram_id, locale, active, version, "
        "snapshot_fingerprint) "
        "SELECT ('00000000-0000-4000-8000-' || lpad(g::text, 12, '0'))::uuid, "
        "9000000000 + g, 'en', true, 1, repeat('a', 64) "
        "FROM generate_series(1, :n) g"
    ), {"n": people})
    # Jobs: most finished long ago (retention's work), a tenth expired
    # past their expiry (the expiry cleanup's), the rest waiting in every
    # lane (the passes' and the sweep's); a tenth carry a correlation
    # (the reminders' cancel), one person each.
    await session.execute(text(
        "INSERT INTO notifications (id, type, title, body, target_type, "
        "target_value, idempotency_key, fingerprint, channels, "
        "expiry_layer, push_on, retry_max_attempts, retry_backoff_seconds, "
        "recipient_schedule, lane, status, scheduled_at, created_at, "
        "expiry_at, correlation) "
        "SELECT gen_random_uuid(), 'unit_event_in_app', 'T', 'B', 'user', "
        "('00000000-0000-4000-8000-' || lpad((g % :p + 1)::text, 12, '0')), "
        ":prefix || g, repeat('a', 64), '[\"in_app\"]'::jsonb, 'default', "
        "'none', 3, 30, 'respect', "
        "(ARRAY['interactive','critical','normal','bulk'])[g % 4 + 1], "
        "CASE WHEN g % 10 = 0 THEN 'pending' WHEN g % 10 = 1 THEN 'processing' "
        "     WHEN g % 10 = 2 THEN 'expired' ELSE 'sent' END, "
        "now() - (g % 1440) * interval '1 hour', "
        "now() - (g % 1440) * interval '1 hour', "
        "CASE WHEN g % 10 = 2 THEN now() - interval '1 day' "
        "     WHEN g % 10 = 0 THEN now() - interval '1 minute' END, "
        "CASE WHEN g % 10 = 5 THEN 'corr-' || (g % 1000) END "
        "FROM generate_series(1, :n) g"
    ), {"n": jobs, "p": people, "prefix": _PREFIX})
    # _DELIVERIES_PER_JOB a resolved job, to its person and the next ones.
    await session.execute(text(
        "INSERT INTO notification_deliveries (id, notification_id, "
        "recipient_id, channel, status, sent_at) "
        "SELECT gen_random_uuid(), n.id, "
        "('00000000-0000-4000-8000-' || lpad(((right(n.target_value, 12)::int "
        "  + k - 1) % :p + 1)::text, 12, '0'))::uuid, 'in_app', "
        "CASE WHEN n.status = 'sent' THEN 'sent' "
        "     WHEN n.status = 'expired' THEN 'expired' ELSE 'pending' END, "
        "CASE WHEN n.status = 'sent' THEN n.created_at END "
        "FROM notifications n, generate_series(1, :k) k "
        "WHERE n.idempotency_key LIKE :like AND n.status <> 'pending'"
    ), {"p": people, "k": _DELIVERIES_PER_JOB, "like": _PREFIX + "%"})
    await session.commit()
    for table in ("recipients", "notifications", "notification_deliveries"):
        await session.execute(text(f"ANALYZE {table}"))
    await session.commit()


async def _unseed(session: AsyncSession) -> None:
    await session.execute(text(
        "DELETE FROM notifications WHERE idempotency_key LIKE :like"
        " OR idempotency_key LIKE 'w53-intake-%'"
    ), {"like": _PREFIX + "%"})
    await session.execute(text(
        "DELETE FROM recipients WHERE telegram_id > 9000000000"
    ))
    await session.commit()


def _person(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"


async def _broadcast(session: AsyncSession) -> None:
    job = Notification(
        id=uuid4(), type="unit_event_in_app", title="T", body="B",
        target_type="all", target_value="*",
        idempotency_key=f"{_PREFIX}all-{uuid4()}", fingerprint=TEST_FINGERPRINT,
        channels=["in_app"], expiry_layer="default", push_on="none",
        retry_max_attempts=3, retry_backoff_seconds=30,
        recipient_schedule="respect", lane="bulk",
    )
    session.add(job)
    await session.flush()
    await resolve_notification(session, job)


async def _intake(session: AsyncSession) -> None:
    await accept_notification(
        session, idempotency_key=f"w53-intake-{uuid4()}",
        fingerprint=TEST_FINGERPRINT, type="unit_event_in_app",
        title="T", body="B", target_type="user", target_value=_person(7),
    )


def _selection(lane: Lane) -> Callable[[AsyncSession], Awaitable[Any]]:
    async def run(session: AsyncSession) -> None:
        await session.execute(
            select(Notification.id)
            .where(Notification.lane == lane.value,
                   ready_now(datetime.now(UTC)))
            .order_by(Notification.scheduled_at)
            .limit(settings.lane_batch_size(lane) + 4)
        )
    return run


OPERATIONS: dict[ProcessRole, dict[str, Callable[[AsyncSession], Awaitable[Any]]]] = {
    ProcessRole.WORKER: {
        **{f"selection {lane.value}": _selection(lane) for lane in Lane},
        "resolve to everyone": _broadcast,
        "expiry sweep portion": lambda s: close_notifications(
            s, Notification.expiry_at < datetime.now(UTC),
            NotificationStatus.EXPIRED, limit=500,
        ),
        "retention batch": lambda s: delete_terminal_notifications_batch(
            s, cutoff=datetime.now(UTC) - timedelta(days=30), limit=1000,
        ),
        "expired cleanup batch": lambda s: delete_expired_notifications_batch(
            s, now=datetime.now(UTC), limit=1000,
        ),
        "lane depth": lane_depth,
    },
    ProcessRole.CONSUMER: {
        "intake": _intake,
        "forget a person (withdraw)": lambda s: withdraw_recipient(s, _person(5)),
        "cancel reminders": lambda s: cancel_reminders(
            s, types={"unit_event_in_app"}, correlation="corr-5",
        ),
    },
    ProcessRole.API: {
        "intake": _intake,
        "forget a person (withdraw)": lambda s: withdraw_recipient(s, _person(5)),
        "inbox page": lambda s: list_recipient_deliveries(s, _person(9), limit=50),
        "unread count": lambda s: get_unread_count(s, _person(9)),
        "lane depth": lane_depth,
    },
}


async def measure() -> dict[ProcessRole, dict[str, float]]:
    if not (make_url(settings.database_url).database or "").endswith("_test"):
        raise SystemExit("refused: the database's name must end in _test")
    set_process_role(ProcessRole.WORKER)
    install_profile(load_profile(FileProfileSource(_PROFILE_DIR)))
    set_attempt_hold(longest_attempt_hold())
    factory = get_session_factory()
    async with factory() as session:
        await _unseed(session)
        started = time.perf_counter()
        await _seed(session)
        counts = {
            table: (await session.execute(text(
                f"SELECT count(*) FROM {table}"
            ))).scalar_one()
            for table in ("notifications", "notification_deliveries", "recipients")
        }
        print(f"seeded in {time.perf_counter() - started:.1f} s: {counts}")
    worst: dict[ProcessRole, dict[str, float]] = {}
    try:
        for role, operations in OPERATIONS.items():
            for name, operation in operations.items():
                runs = []
                for _ in range(REPEATS):
                    async with factory() as session:
                        started = time.perf_counter()
                        await operation(session)
                        runs.append(time.perf_counter() - started)
                        await session.rollback()
                worst.setdefault(role, {})[name] = max(runs)
                print(f"{role.value:9} {name:28} {max(runs) * 1000:9.1f} ms")
    finally:
        async with factory() as session:
            await _unseed(session)
        await dispose_engine()
    return worst


if __name__ == "__main__":
    asyncio.run(measure())
