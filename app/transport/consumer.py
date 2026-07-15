# =============================================================================
# COMMS Service -- Redis Streams Consumer (Phase 3c items 1, 6)
# =============================================================================
#
# The durable inbound loop. One consumer group on the product's event
# stream; entries are parsed (events.py), dispatched (handlers.py) and
# XACKed. At-least-once end to end:
#
#   startup:  XGROUP CREATE MKSTREAM (BUSYGROUP swallowed) -- the
#             group exists whether comms or the product boots first;
#             then DRAIN OWN PENDING (XREADGROUP id "0"): the consumer
#             name is STABLE across restarts, so everything delivered
#             but not acked before a crash is replayed here. No
#             XAUTOCLAIM machinery -- single consumer per deploy by
#             design (see comms_consumer_name in config).
#   steady:   blocking XREADGROUP ">" in batches; each entry is
#             processed in ITS OWN DB transaction and acked
#             individually -- a failed neighbor never holds back an
#             acked one.
#
# ERROR DISCIPLINE (item 6, poison-pill rule from Phase 2):
#   terminal   (ValidationError: broken envelope/JSON/schema/version,
#              unregistered type, invalid channel) -> log + DLQ + XACK:
#              the event can never succeed; the stream MUST keep
#              moving.
#   retryable  (NotFoundError: group_changed before its user_upserted;
#              OperationalError: transient DB hiccup) -> bounded
#              inline backoff, then DLQ + XACK.
#   duplicate  (idempotency_key collision) -> XACK only: a replay is
#              the at-least-once contract working, not an error.
#   unexpected (any other exception) -> log with traceback + DLQ +
#              XACK: an unknown bug in ONE event must not wedge the
#              whole stream (poison-pill rule) -- the DLQ preserves
#              the evidence.
#
# KNOWN CEILING -- head-of-line blocking on the ordering retry (BL-4).
# Mechanics: a group_changed that arrived before its user_upserted is
# retried INLINE (~6s worst case, _BACKOFF_DELAYS below); the whole
# stream -- including notification_requests that should not wait --
# stalls behind it.
# Status: acknowledged by design (Phase 3c review, part C).
# Backlog: BL-4 in the dispatch plan.
# Unfreeze trigger: a noticeable share of ordering retries in the
# logs, OR a second consumer / high traffic.
# Agreed fix shape: a parking stream for lagging events (set aside,
# keep the main stream moving), instead of the inline sleep.
# Rejected: re-XADD with a delay -- it breaks the ordering of events
# for the same user and drags an attempt counter into the payload.
# =============================================================================

import asyncio
import json
from typing import Any

import structlog
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.typing import EncodableT, FieldT
from sqlalchemy.exc import OperationalError

from app.core.config import settings
from app.core.database import get_session_factory
from app.core.exceptions import NotFoundError, ValidationError
from app.transport.events import parse_event
from app.transport.handlers import HandleResult, handle_event

logger = structlog.get_logger()

# Inline backoff for RETRYABLE failures: 6 attempts total (first try
# + 5 retries), ~6.2s worst case -- sized for "the upsert is moments
# behind", not for outages (outages land in the DLQ and are visible).
_BACKOFF_DELAYS: tuple[float, ...] = (0.2, 0.4, 0.8, 1.6, 3.2)

# Pause between EMPTY reads -- see the comment at the read site: real
# Redis has already waited server-side (BLOCK), so this only guards
# against clients whose blocking read returns immediately.
_IDLE_SLEEP_SECONDS = 0.05


class StreamConsumer:
    """One consumer-group reader over the product's event stream."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._stream = settings.comms_events_stream
        self._group = settings.comms_consumer_group
        self._consumer = settings.comms_consumer_name
        self._dlq = settings.dlq_stream
        self._session_factory = get_session_factory()

    # -- bootstrap ---------------------------------------------------------

    async def ensure_group(self) -> None:
        """Create the group (and the stream) if absent; idempotent."""
        try:
            await self._redis.xgroup_create(
                self._stream, self._group, id="0", mkstream=True
            )
            logger.info(
                "consumer_group_created",
                stream=self._stream,
                group=self._group,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
            # Group already exists -- the normal case after the first
            # boot.

    # -- main loop ---------------------------------------------------------

    async def run(self) -> None:
        """Consume forever: drain own pending, then follow new entries.

        Cancellation-safe in the at-least-once sense (review 3c.1
        wording fix): a CancelledError landing on an INNER await
        interrupts the current entry mid-flight -- its transaction
        rolls back and the entry stays UNACKED. Correctness is held by
        the replay, not by graceful completion: the next start's
        pending drain re-processes it (and dedup collapses a replayed
        notification_request).
        """
        await self.ensure_group()
        drained = await self._drain_pending()
        logger.info(
            "consumer_started",
            stream=self._stream,
            group=self._group,
            consumer=self._consumer,
            pending_replayed=drained,
        )
        while True:
            response = await self._redis.xreadgroup(
                groupname=self._group,
                consumername=self._consumer,
                streams={self._stream: ">"},
                count=settings.consumer_batch_size,
                block=settings.consumer_block_ms,
            )
            if not response:
                # Yield between empty reads. With REAL Redis this adds
                # a negligible pause after the server-side BLOCK has
                # already waited; without it a client whose blocking
                # read returns immediately (fakeredis in tests) would
                # busy-spin and STARVE the event loop -- including the
                # very cancellation that stops this task.
                await asyncio.sleep(_IDLE_SLEEP_SECONDS)
                continue
            for _stream_name, entries in response:
                for entry_id, fields in entries:
                    await self._process_entry(entry_id, fields)

    async def _drain_pending(self) -> int:
        """Replay entries delivered to THIS consumer but never acked
        (crash between XREADGROUP and XACK). Reading from id "0"
        returns the pending list of this consumer name; a stable name
        makes restart recovery this one loop."""
        drained = 0
        while True:
            response = await self._redis.xreadgroup(
                groupname=self._group,
                consumername=self._consumer,
                streams={self._stream: "0"},
                count=settings.consumer_batch_size,
            )
            if not response or not response[0][1]:
                return drained
            for _stream_name, entries in response:
                for entry_id, fields in entries:
                    await self._process_entry(entry_id, fields)
                    drained += 1

    # -- per-entry processing ----------------------------------------------

    async def _process_entry(
        self, entry_id: str | bytes, fields: dict[Any, Any]
    ) -> None:
        """Parse -> handle (with bounded retry) -> ack. Never raises
        for a bad EVENT (poison-pill rule); only infrastructure
        failures (Redis down, DB down past the backoff budget) surface
        via the DLQ or propagate out of the loop."""
        try:
            event = parse_event(fields)
        except ValidationError as exc:
            await self._to_dlq(entry_id, fields, reason=str(exc), attempts=0)
            await self._ack(entry_id)
            return

        attempt = 0
        while True:
            attempt += 1
            try:
                async with self._session_factory() as session:
                    result = await handle_event(session, event)
                    await session.commit()
                if result is HandleResult.DUPLICATE:
                    logger.info(
                        "event_duplicate_acked",
                        entry_id=_id_str(entry_id),
                    )
                else:
                    logger.info(
                        "event_processed",
                        entry_id=_id_str(entry_id),
                        # NB: "event" is structlog's reserved key for
                        # the message itself -- hence event_type.
                        event_type=type(event).__name__,
                        attempt=attempt,
                    )
                await self._ack(entry_id)
                return
            except ValidationError as exc:
                # Terminal by classification (e.g. unregistered type):
                # retrying cannot fix a wrong event.
                await self._to_dlq(
                    entry_id, fields, reason=str(exc), attempts=attempt,
                )
                await self._ack(entry_id)
                return
            except (NotFoundError, OperationalError) as exc:
                # Retryable: sync ordering lag / transient DB failure.
                # See the KNOWN CEILING (BL-4) in the module header:
                # this inline sleep blocks the whole stream.
                if attempt > len(_BACKOFF_DELAYS):
                    logger.warning(
                        "event_retries_exhausted",
                        entry_id=_id_str(entry_id),
                        attempts=attempt,
                        error=str(exc),
                    )
                    await self._to_dlq(
                        entry_id, fields,
                        reason=f"retries exhausted: {exc}",
                        attempts=attempt,
                    )
                    await self._ack(entry_id)
                    return
                delay = _BACKOFF_DELAYS[attempt - 1]
                logger.info(
                    "event_retry_scheduled",
                    entry_id=_id_str(entry_id),
                    attempt=attempt,
                    delay=delay,
                    error=str(exc),
                )
                await asyncio.sleep(delay)
            except Exception as exc:
                # Unknown bug in ONE event must not wedge the stream
                # (poison-pill rule): preserve the evidence, move on.
                logger.exception(
                    "event_unexpected_error",
                    entry_id=_id_str(entry_id),
                )
                await self._to_dlq(
                    entry_id, fields,
                    reason=f"unexpected: {type(exc).__name__}: {exc}",
                    attempts=attempt,
                )
                await self._ack(entry_id)
                return

    # -- plumbing ------------------------------------------------------------

    async def _ack(self, entry_id: str | bytes) -> None:
        await self._redis.xack(self._stream, self._group, entry_id)

    async def _to_dlq(
        self,
        entry_id: str | bytes,
        fields: dict[Any, Any],
        *,
        reason: str,
        attempts: int,
    ) -> None:
        """Dead-letter the original entry with diagnostics attached.

        The DLQ entry carries the ORIGINAL envelope fields verbatim
        (re-ingestable after a fix) plus error metadata under the
        _dlq_ prefix (review 3c.1): un-prefixed names could collide
        with fields of a thoroughly broken producer's entry and
        silently overwrite them -- breaking the "verbatim" promise
        exactly on the records where the evidence matters most. The
        prefix also self-documents which fields the CONSUMER added.
        The stream is capped (approximate MAXLEN) so a misbehaving
        producer cannot grow it without bound.
        """
        # redis-py's FieldT alias is invariant in dict params, so a
        # plain dict[str, str] does not satisfy it; alias it exactly.
        payload: dict[FieldT, EncodableT] = {
            _field_str(k): _field_str(v) for k, v in fields.items()
        }
        payload["_dlq_error"] = reason
        payload["_dlq_attempts"] = str(attempts)
        payload["_dlq_source_entry_id"] = _id_str(entry_id)
        await self._redis.xadd(
            self._dlq,
            payload,
            maxlen=settings.dlq_maxlen,
            approximate=True,
        )
        logger.warning(
            "event_dead_lettered",
            entry_id=_id_str(entry_id),
            dlq=self._dlq,
            reason=reason,
            attempts=attempts,
        )


def _id_str(entry_id: str | bytes) -> str:
    return (
        entry_id.decode("ascii")
        if isinstance(entry_id, bytes)
        else str(entry_id)
    )


def _field_str(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    return json.dumps(value)


async def run_consumer_loop() -> None:
    """Build the Redis client and run the consumer until cancelled."""
    redis: Redis = Redis.from_url(settings.redis_url)
    consumer = StreamConsumer(redis)
    try:
        await consumer.run()
    finally:
        await redis.aclose()
