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
#      before its transition commits);
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
# REDIS AWAY: the tick fails, the rows stay, the next tick tries again.
# The worker never touches redis, so transitions go on and the outbox
# grows; when redis is back the backlog is published batch by batch
# without pausing. Two relays at once (a deploy rolling over) publish
# some keys twice, nothing worse.
#
# RUNS IN THE CONSUMER PROCESS (app/consumer.py), the one process that
# already holds a redis URL; the API and the worker stay DB-only.
# =============================================================================

import asyncio

import structlog
from redis.asyncio import Redis
from redis.typing import EncodableT, FieldT
from sqlalchemy import delete, select

from app.core.config import settings
from app.core.constants import PUSH_RELAY_BATCH, PUSH_RELAY_INTERVAL_SECONDS
from app.core.database import get_session_factory
from app.engine.formatters import sanitized_traceback
from app.engine.models import Notification, PushOutbox

logger = structlog.get_logger()

# The push's format version: a listener that meets another one stops
# rather than guesses (deploy/INTEGRATION.md section 10).
PUSH_FORMAT_VERSION = 1

# Every field of a push entry -- the contract; nothing else is written.
PUSH_FIELDS = ("v", "idempotency_key")


def push_entry(idempotency_key: str) -> dict[FieldT, EncodableT]:
    """The stream entry of one push: the format version and the key."""
    return {"v": str(PUSH_FORMAT_VERSION), "idempotency_key": idempotency_key}


async def relay_once(redis: Redis) -> int:
    """Publish one batch of committed pushes, then delete their rows.

    Returns the number of outbox rows the batch read (0 when there was
    nothing owed). Raises whatever redis or the database raised -- the
    rows of a batch that failed before its delete stay owed.
    """
    factory = get_session_factory()
    async with factory() as session:
        rows = (
            await session.execute(
                select(PushOutbox.id, Notification.idempotency_key)
                .join(Notification, Notification.id == PushOutbox.notification_id)
                .order_by(PushOutbox.id)
                .limit(PUSH_RELAY_BATCH)
            )
        ).all()
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
        await session.execute(
            delete(PushOutbox).where(PushOutbox.id.in_([ident for ident, _ in rows]))
        )
        await session.commit()
    logger.debug("push_relay_published", rows=len(rows), keys=len(keys))
    return len(rows)


async def run_push_relay(redis: Redis) -> None:
    """Relay forever: a full batch is followed at once by the next, any
    other tick by a pause. A failed tick is logged and retried after
    the pause; it never ends the loop -- only cancellation does."""
    while True:
        try:
            read = await relay_once(redis)
        except Exception as exc:
            logger.error(
                "push_relay_error", exception=sanitized_traceback(exc),
            )
            read = 0
        if read < PUSH_RELAY_BATCH:
            await asyncio.sleep(PUSH_RELAY_INTERVAL_SECONDS)


async def run_push_relay_loop() -> None:
    """Build the relay's own redis client and relay until cancelled."""
    redis: Redis = Redis.from_url(settings.redis_url)
    try:
        await run_push_relay(redis)
    finally:
        await redis.aclose()
