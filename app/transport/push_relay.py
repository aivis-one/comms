# =============================================================================
# COMMS Service -- The push relay (P3-1, spec §7.1, §7.2, §7.6)
# =============================================================================
#
# The reverse direction. A transition the job's type declared in
# push_on owes the product a push; app/engine/journal.py writes that
# debt into push_outbox in the transition's own transaction. This relay
# publishes the debts that COMMITTED: it XADDs the job's idempotency
# key into the push stream of comms' own redis (settings.changes_stream)
# and then deletes the rows. comms never calls the product: the product
# listens (spec §7.3).
#
# A PUSH IS THE KEY AND NOTHING ELSE -- the format version and the
# idempotency key (PUSH_FIELDS). No status, no channel, never the
# letter. So two pushes of one job in either order, or one push twice,
# lead the product to the same read of the current truth (spec §7.2),
# and the relay needs no ordering and no deduplication beyond
# collapsing the keys of one batch. The fields are written once, in
# deploy/INTEGRATION.md section 10, and held to PUSH_FIELDS by
# tests/test_push_contract.py.
#
# ONE TICK, THREE STEPS, NO TRANSACTION ACROSS REDIS:
#   1. read a batch of committed rows with their keys (a short read --
#      an uncommitted row is not visible to it, so nothing is published
#      before its transition commits). The read takes the rows FOR
#      UPDATE OF push_outbox SKIP LOCKED and lets go before the XADD
#      (H2): a row a cascade is deleting is not read -- it is not
#      published again tick after tick, and it does not block the free
#      rows behind it. OF push_outbox and nothing wider: the join's
#      job row is held FOR NO KEY UPDATE by a delivery attempt, and a
#      read that locked it too would stand against the attempt;
#   2. XADD one entry per distinct key of the batch (approximate MAXLEN);
#   3. delete the rows that were read, in a second short transaction.
#   Deleting is the mark "published": a published row has no reader.
#   The order is XADD, THEN DELETE, and it must never be swapped: a
#   relay that dies between the two leaves the rows, and the next tick
#   publishes them again -- a duplicate, harmless; deleted first, a
#   death in between would lose the push. No transaction stays open
#   while redis is called: an open transaction holds back the changes
#   feed (app/engine/changes.py, the xmin rule), and a redis that hangs
#   must not hold back the feed the product falls back on.
#
# THE DELETE NEVER WAITS (H1). Two other writers delete outbox rows: the
# ON DELETE CASCADE of a job removed by retention
# (app/engine/processor.py cleanup_terminal_notifications) or by the
# expiry cleanup (cleanup_expired_notifications). A cascade locks the
# rows of each job it deletes, job after job; a relay delete that WAITED
# for one of those rows while holding another could close a cycle with
# it -- 40P01. So the relay deletes only the rows it can lock at once
# (FOR UPDATE SKIP LOCKED): a waiting relay is what a cycle needs, and
# there is none. A row it skipped is held by a cascade: when that
# commits, the row went with its job; when it rolls back, the row is
# still owed and the next tick publishes it again -- a duplicate,
# harmless. No row is lost either way (tests/test_relay_vs_retention.py).
#
# REDIS AWAY: the tick fails, the rows stay, the next tick tries again.
# The worker never touches redis, so transitions go on and the outbox
# grows; when redis is back the backlog is published batch by batch
# without pausing. A tick that fails is logged ONCE, on the turn into
# failure (push_relay_error), and the way back once
# (push_relay_recovered) -- not a line per tick while redis stays away.
# Two relays at once (a deploy rolling over) publish some keys twice,
# nothing worse.
#
# THE PAUSE IS BY PROGRESS (H2): the loop goes straight on only after a
# tick that published and deleted a full batch -- a backlog. A tick that
# read rows it could not delete (a cascade took them between the read
# and the delete) made no such progress and pauses, so a held row can
# never spin the loop.
#
# RUNS IN THE CONSUMER PROCESS (app/consumer.py), the one process that
# already holds a redis URL; the API and the worker stay DB-only.
# =============================================================================

import asyncio

import structlog
from redis.asyncio import Redis
from redis.typing import EncodableT, FieldT
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.constants import PUSH_RELAY_BATCH, PUSH_RELAY_INTERVAL_SECONDS
from app.core.database import get_session_factory
from app.engine.formatters import sanitized_traceback
from app.engine.models import Notification, PushOutbox
from app.transport.connection import redis_client

logger = structlog.get_logger()

# The push's format version: a listener that meets another one stops
# rather than guesses (deploy/INTEGRATION.md section 10).
PUSH_FORMAT_VERSION = 1

# Every field of a push entry -- the contract; nothing else is written.
PUSH_FIELDS = ("v", "idempotency_key")


def push_entry(idempotency_key: str) -> dict[FieldT, EncodableT]:
    """The stream entry of one push: the format version and the key."""
    return {"v": str(PUSH_FORMAT_VERSION), "idempotency_key": idempotency_key}


async def delete_published(session: AsyncSession, ids: list[int]) -> int:
    """Delete the published rows `ids` that can be locked without
    waiting; skip the rest (a cascade holds them -- see THE DELETE NEVER
    WAITS above). Commit-free; returns the number of rows deleted."""
    lockable = (
        select(PushOutbox.id)
        .where(PushOutbox.id.in_(ids))
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(
        delete(PushOutbox).where(PushOutbox.id.in_(lockable)),
    )
    deleted: int = result.rowcount  # type: ignore[attr-defined]
    return deleted


async def relay_once(redis: Redis) -> int:
    """Publish one batch of committed pushes, then delete their rows.

    Returns the PROGRESS of the tick: the number of outbox rows it
    published AND deleted (0 when nothing was owed, or every row it
    read was taken by a cascade before its delete). Rows read but not
    deleted are not counted -- the loop pauses on them. Raises whatever
    redis or the database raised -- the rows of a batch that failed
    before its delete stay owed.
    """
    factory = get_session_factory()
    async with factory() as session:
        rows = (
            await session.execute(
                select(PushOutbox.id, Notification.idempotency_key)
                .join(Notification, Notification.id == PushOutbox.notification_id)
                .order_by(PushOutbox.id)
                .limit(PUSH_RELAY_BATCH)
                .with_for_update(of=PushOutbox, skip_locked=True)
            )
        ).all()
        # The read's row locks go here, before any redis call.
        await session.rollback()
    if not rows:
        return 0
    keys = list(dict.fromkeys(key for _, key in rows))
    for key in keys:
        await redis.xadd(
            settings.changes_stream,
            push_entry(key),
            maxlen=settings.changes_stream_maxlen,
            approximate=True,
        )
    async with factory() as session:
        deleted = await delete_published(session, [ident for ident, _ in rows])
        await session.commit()
    logger.debug(
        "push_relay_published", rows=len(rows), keys=len(keys), deleted=deleted,
    )
    return deleted


async def run_push_relay(redis: Redis) -> None:
    """Relay forever: a tick that published and deleted a full batch is
    followed at once by the next, any other tick by a pause. A failed
    tick is retried after the pause; it is logged on the turn into
    failure and the way back, not on every tick. It never ends the loop
    -- only cancellation does.

    The failure state lives in this call, not in the module: every run
    -- every test -- starts healthy."""
    failed_ticks = 0
    while True:
        try:
            progress = await relay_once(redis)
        except Exception as exc:
            if failed_ticks == 0:
                logger.error(
                    "push_relay_error", exception=sanitized_traceback(exc),
                )
            failed_ticks += 1
            progress = 0
        else:
            if failed_ticks:
                logger.info("push_relay_recovered", failed_ticks=failed_ticks)
            failed_ticks = 0
        if progress < PUSH_RELAY_BATCH:
            await asyncio.sleep(PUSH_RELAY_INTERVAL_SECONDS)


async def run_push_relay_loop() -> None:
    """Build the relay's own redis client and relay until cancelled. A
    silent redis fails a tick after the socket timeout -- logged, and
    retried on the next tick (app/transport/connection.py)."""
    redis: Redis = redis_client()
    try:
        await run_push_relay(redis)
    finally:
        await redis.aclose()
