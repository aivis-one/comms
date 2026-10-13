# =============================================================================
# COMMS Service -- Lane Wake (P4-2, spec §8.4)
# =============================================================================
#
# The idle backoff of a lane's loop (app/worker.py _poll_loop) saves the
# processor; it must not also decide how soon new work is noticed. So
# intake wakes the loop of the job's lane at once, and the backoff goes
# on deciding only how long an idle loop sleeps when nobody wakes it.
#
# THE SIGNAL is a Postgres NOTIFY on WAKE_CHANNEL whose payload is the
# job's lane -- the envelope, never the letter. signal_lane() runs it in
# the intake's own transaction (app/engine/service.py
# accept_notification, ACCEPTED only), so the database, not the order
# of lines, decides when it goes: a NOTIFY reaches a listener only after
# its transaction commits, never after a rollback, and identical ones in
# one transaction arrive once (tests/test_lane_wake.py TestPostgresFacts).
# A loop woken by it therefore always sees the committed job, and a
# batch of one lane's jobs in one transaction wakes that lane once.
# Every one of the three processes that accept (the consumer, the API,
# the worker's own close notices) goes through that one call.
#
# THE LISTENER (listen_for_wakes) is one per worker process, on its OWN
# connection -- an engine with NullPool, outside the process's pool. The
# pool's budget is held to the sessions that serve deliveries
# (tests/test_lanes.py test_the_lanes_fit_the_pool), and a connection
# held forever needs neither its recycling nor its pre-ping. It sets the
# event of the lane named in the payload; a lane this process does not
# take has no event here and is ignored.
#
# THE SIGNAL IS ONLY A SPEED-UP. A lost one -- sent while the listener
# was not listening, or while its connection was down -- costs what the
# poll costs today, never the job: the loop still wakes at its interval.
# Each time LISTEN is (re)established every event of the process is set,
# so the signals lost in the gap cost one pass at once, not an interval.
# A connection that died silently is found by a ping every
# NOTIFICATION_MAX_BACKOFF_SECONDS; one that closed says so at once.
#
# KNOWN CEILING -- a job that becomes due after its intake is noticed by
# the poll, not by a wake.
#   1. Mechanics: the only wake is at intake. A job accepted with
#      scheduled_at in the future (a reminder), a delivery whose
#      next_retry_at opens later, a job behind its pipeline_retry_at:
#      each becomes due with no signal, and an idle loop sees it only
#      at its next interval -- up to NOTIFICATION_MAX_BACKOFF_SECONDS
#      after it was due. Intake wakes the lane whatever scheduled_at
#      says; for such a job that pass is empty.
#   2. Status: acknowledged by design.
#   3. Task: none -- no lane today carries jobs whose due moment needs
#      to be met to the second; the wake at intake is what spec §8.4
#      asks for.
#   4. Unfreeze when: a type whose jobs are scheduled into the future,
#      or retried, has to go out within seconds of becoming due -- seen
#      as the delay between a job's due moment and its first attempt
#      in the transition journal of that type.
#   5. Agreed fix: the lane's loop sleeps until the nearest due moment
#      of its own lane (scheduled_at, next_retry_at, pipeline_retry_at),
#      but never longer than its current interval.
#   6. Rejected: a shorter backoff or poll for everyone (glues the two
#      tasks back together and burns the processor, spec §8.4);
#      deciding at intake whether scheduled_at is "now" (compares the
#      acceptor's clock with the worker's and buys only one empty pass).
# =============================================================================

import asyncio
import contextlib
from collections.abc import Mapping

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.engine.formatters import sanitized_traceback

logger = structlog.get_logger()

# The Postgres channel of the wake: written by intake, listened to by
# the worker. The payload is a lane value (app/profile/registry.py Lane).
WAKE_CHANNEL = "comms_lane_wake"

# How the listening connection names itself in pg_stat_activity, so the
# worker's one long-lived connection outside its pool can be told apart
# from the pool's.
WAKE_APPLICATION_NAME = "comms-lane-wake"


async def signal_lane(session: AsyncSession, lane: str) -> None:
    """Wake the loops of `lane` once this session's transaction commits.

    Commit-free (P-01): the NOTIFY is queued in the caller's
    transaction; Postgres sends it at the commit and drops it at a
    rollback.
    """
    await session.execute(select(func.pg_notify(WAKE_CHANNEL, lane)))


def wake_engine() -> AsyncEngine:
    """The listener's own engine: the service's database, no pool."""
    return create_async_engine(
        settings.database_url,
        poolclass=NullPool,
        connect_args={
            "server_settings": {"application_name": WAKE_APPLICATION_NAME},
        },
    )


async def listen_for_wakes(events: Mapping[str, asyncio.Event]) -> None:
    """Set events[lane] on every wake of a lane of this process, forever.

    `events` maps each lane value this process takes to its loop's
    event. A failure of the listening connection is logged and the
    listener connects again after NOTIFICATION_MAX_BACKOFF_SECONDS;
    meanwhile the loops poll as they would without it. Cancellation
    ends it.
    """
    engine = wake_engine()
    try:
        while True:
            try:
                await _listen(engine, events)
            except Exception as exc:
                logger.error(
                    "lane_wake_listener_error",
                    exception=sanitized_traceback(exc),
                )
            await asyncio.sleep(settings.notification_max_backoff_seconds)
    finally:
        await engine.dispose()


async def _listen(
    engine: AsyncEngine, events: Mapping[str, asyncio.Event],
) -> None:
    """One listening connection, from LISTEN until it fails."""
    lost = asyncio.Event()

    def on_wake(conn: object, pid: int, channel: str, payload: str) -> None:
        event = events.get(payload)
        if event is not None:
            event.set()

    async with engine.connect() as connection:
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        if driver is None:
            raise ConnectionError("the wake listener got no connection")
        driver.add_termination_listener(lambda conn: lost.set())
        await driver.add_listener(WAKE_CHANNEL, on_wake)
        # Whatever was signalled before LISTEN took effect is lost:
        # one pass of every lane now instead of an interval later.
        for event in events.values():
            event.set()
        logger.info("lane_wake_listening", lanes=sorted(events))

        interval = settings.notification_max_backoff_seconds
        while True:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(interval):
                    await lost.wait()
            if lost.is_set():
                raise ConnectionError("the wake listener's connection closed")
            async with asyncio.timeout(interval):
                await driver.fetchval("SELECT 1")
