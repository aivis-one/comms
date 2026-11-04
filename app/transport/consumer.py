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
#   waiting    a TRANSIENT failure of the database while the database
#              does NOT answer (SELECT 1): waited for, uncounted, the
#              delay doubling to 30 s, said loudly on the 1st and every
#              10th wait -- the event is not lost (FIX review 3.13.0).
#              While it DOES answer, the transient failure is the
#              event's: the bounded budget below, then the DLQ.
#   retryable  (NotFoundError: group_changed before its user_upserted;
#              a TRANSIENT failure of the database by the one rule,
#              app/core/database.py is_transient_db_error -- a deadlock,
#              a serialization failure, a lost or refused connection, a
#              restart, an overload) -> bounded inline backoff, then
#              DLQ + XACK, "retries exhausted". Until TX-1 only an
#              OperationalError was retried here -- which asyncpg's
#              adapter never raises: a deadlock or a dropped connection
#              reached the DLQ on the first try, and the event (a
#              user_deleted among them) was lost.
#   duplicate  (same key, same bytes) -> XACK only: a replay is the
#              at-least-once contract working, not an error.
#   rejected / conflict (a notification request not accepted, F1.2)
#              -> XACK only: the refusal is RECORDED under the request's
#              key in the same transaction; the DLQ would be a second
#              copy of the same fact.
#
# DURABILITY OF INTAKE -- the order is COMMIT, THEN XACK, and it must
# never be swapped (spec §5.4: a receipt without a record is a promise
# that disappears on a crash). The entry is acknowledged only after
# the transaction that wrote the job -- or the record of its refusal --
# has committed. A crash between the two leaves the entry pending; the
# next start replays it (_drain_pending), and the replay is collapsed
# by the key: same bytes -> duplicate, nothing created twice. An ack
# before the commit would lose the job on a crash in between; batching
# acks or acknowledging on read would do exactly that.
#   unexpected (any other exception) -> log with a redacted traceback + DLQ +
#              XACK: an unknown bug in ONE event must not wedge the
#              whole stream (poison-pill rule) -- the DLQ keeps what
#              the investigation needs (DEAD LETTERS below).
#
# DEAD LETTERS -- for the investigation, never a replay. Nothing reads
# the DLQ back into the stream: the product resends from its outbox, the
# source of the request (deploy/INTEGRATION.md, "How long comms keeps
# things"). So a dead letter keeps the ADDRESS of the entry, never its
# letter: the event name, the keys of `data` named by the event's own
# field set (dead_letter_fields), the fingerprint and length of the
# `data` bytes -- by which the product finds the event at home -- the
# names of everything dropped, the reason, the attempts and the source
# entry id. The stream lives in the shared redis with no expiry; a
# letter kept there would outlive every promise of the contract.
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
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.typing import EncodableT, FieldT

from app.core.config import settings
from app.core.database import (
    database_answers,
    get_session_factory,
    is_transient_db_error,
)
from app.core.exceptions import (
    ConflictError,
    NotFoundError,
    ValidationError,
    conflict_class,
)
from app.core.sanitize import (
    exception_text,
    pipeline_error_of,
    sanitize_text,
    sanitized_traceback,
)
from app.engine.service import stream_fingerprint
from app.transport.connection import redis_client
from app.transport.events import (
    ENVELOPE_DATA_FIELD,
    ENVELOPE_EVENT_FIELD,
    EVENT_FIELDS,
    EVENT_NOTIFICATION_REQUEST,
    NOTIFICATION_REQUEST_ENVELOPE_FIELDS,
    NOTIFICATION_REQUEST_FRAME_FIELDS,
    entry_field_name,
    event_data_or_none,
    parse_event,
)
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

# WAITING FOR THE DATABASE (FIX review 3.13.0): the delay doubles from
# _BACKOFF_DELAYS[0] to this ceiling, uncounted, while the database does
# not answer -- a restart of Postgres takes seconds to minutes, and 30 s
# keeps the stream within half a minute of its return without growing
# the log. Said loudly on the first wait and every _WAIT_LOUD_EVERY-th.
_WAIT_CEILING = 30.0
_WAIT_LOUD_EVERY = 10

# The fields of a dead letter -- the contract's list
# (deploy/INTEGRATION.md, "How long comms keeps things"), held to this
# tuple both ways by tests/test_dlq_no_letter.py. Every name the
# consumer adds carries the _dlq_ prefix; `event` is the entry's own.
DLQ_FIELDS: tuple[str, ...] = (
    ENVELOPE_EVENT_FIELD,
    "_dlq_data",
    "_dlq_data_fingerprint",
    "_dlq_data_bytes",
    "_dlq_dropped",
    "_dlq_error",
    "_dlq_attempts",
    "_dlq_source_entry_id",
)

# Withheld from a dead letter although it is a field of the envelope:
# llm_audio is the SIGNED link to a recording of a person's voice.
# deploy/INTEGRATION.md section 11 promises the link is in no answer,
# path, log line or error text and the body in no dead letter of comms;
# a link kept in a stream without expiry would be the key to that body
# for as long as the object lives. The one place this exception is
# written.
_DLQ_WITHHELD_ENVELOPE_FIELDS = frozenset({"llm_audio"})


class StreamConsumer:
    """One consumer-group reader over the product's event stream."""

    def __init__(
        self,
        redis: Redis,
        database_probe: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        self._redis = redis
        # Does the database answer at all (FIX review 3.13.0)? Asked when
        # an event meets a transient failure: a database that does not
        # answer is waited for; one that does makes it the EVENT's. The
        # probe is the worker's too -- one rule (app/core/database.py
        # database_answers).
        self._database_answers = database_probe or database_answers
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
                    await self._process_guarded(entry_id, fields)

    async def _process_guarded(
        self, entry_id: str | bytes, fields: dict[Any, Any],
    ) -> None:
        """THE LAST LINE (FIX review 3.13.0): whatever _process_entry
        still raises is the entry's, not the process's -- logged with its
        class and place, the entry left pending for the next start. A
        failure of the loop's own read (Redis down) is outside this and
        still ends run(): the restart policy reconnects."""
        try:
            await self._process_entry(entry_id, fields)
        except Exception as exc:
            logger.error(
                "event_left_pending",
                entry_id=_id_str(entry_id),
                error=pipeline_error_of(exc),
            )

    async def _drain_pending(self) -> int:
        """Replay entries delivered to THIS consumer but never acked
        (crash between XREADGROUP and XACK). Reading from id "0"
        returns the pending list of this consumer name; a stable name
        makes restart recovery this one loop."""
        drained = 0
        # A CURSOR, not "0" every time (FIX review 3.13.0): an entry left
        # pending on purpose (a dead letter that could not be written)
        # stays in the pending list, and reading from "0" again would meet
        # it forever -- no new event would ever be read. Each read starts
        # after the last entry seen: a held entry is met once per start.
        cursor: str | bytes = "0"
        while True:
            response = await self._redis.xreadgroup(
                groupname=self._group,
                consumername=self._consumer,
                streams={self._stream: cursor},
                count=settings.consumer_batch_size,
            )
            if not response or not response[0][1]:
                return drained
            for _stream_name, entries in response:
                for entry_id, fields in entries:
                    await self._process_guarded(entry_id, fields)
                    drained += 1
                    cursor = entry_id

    # -- per-entry processing ----------------------------------------------

    async def _process_entry(
        self, entry_id: str | bytes, fields: dict[Any, Any]
    ) -> None:
        """Parse -> handle -> ack. Never raises for a bad EVENT
        (poison-pill rule), and never for a dead letter or an ack that
        cannot be written either (FIX review 3.13.0): those leave the
        entry pending (_dead_letter_or_hold, _ack_or_hold).

        RETRIES:
          - NotFoundError (the address book has not caught up) and a
            transient failure of the database WHILE THE DATABASE
            ANSWERS -- the bounded budget, _BACKOFF_DELAYS, then the dead
            letter, "retries exhausted": one event failing over and
            over must not stop the stream behind it;
          - a transient failure while the database does NOT answer
            (app/core/database.py database_answers) -- WAIT, uncounted,
            the delay doubling to _WAIT_CEILING, said loudly on the
            first and every _WAIT_LOUD_EVERY-th wait: it says nothing of
            the event, and
            the event is not lost. Until this fix a restart of Postgres
            longer than the budget (~6 s) dead-lettered every event of
            the window, a user_deleted among them.
        See the KNOWN CEILING (BL-4) in the module header: the inline
        sleep blocks the whole stream -- as a database that does not
        answer would anyway."""
        try:
            event = parse_event(fields)
        except ValidationError as exc:
            await self._dead_letter_or_hold(
                entry_id, fields, reason=exception_text(exc), attempts=0,
            )
            return
        except Exception as exc:
            # ANY failure of the parse is the entry's, never the
            # process's (poison-pill rule, EM-1 FIX-2). The class and the
            # place -- in the dead letter and in the log alike, no
            # traceback: what the parse raises may quote the data it was
            # reading, and a request's data is the letter.
            logger.error(
                "event_unexpected_error",
                entry_id=_id_str(entry_id),
                stage="parse",
                error=pipeline_error_of(exc),
            )
            await self._dead_letter_or_hold(
                entry_id, fields,
                reason=f"unexpected: {pipeline_error_of(exc)}",
                attempts=0,
            )
            return

        attempt = 0  # tries while the database answered
        waited = 0  # waits while it did not
        while True:
            attempt += 1
            try:
                async with self._session_factory() as session:
                    result = await handle_event(session, event)
                    await session.commit()
            except ConflictError as exc:
                logger.warning(
                    "event_refused",
                    entry_id=_id_str(entry_id),
                    event_type=type(event).__name__,
                    refusal_class=conflict_class(exc),
                    reason=exception_text(exc),
                )
                await self._ack_or_hold(entry_id)
                return
            except ValidationError as exc:
                await self._dead_letter_or_hold(
                    entry_id, fields, reason=exception_text(exc), attempts=attempt,
                )
                return
            except Exception as exc:
                if not _retryable(exc):
                    # Unknown bug in ONE event must not wedge the stream
                    # (poison-pill rule): preserve the evidence, move on.
                    # Not logger.exception(): the renderer would print
                    # the chain raw -- a database error's text is its SQL
                    # and bound parameters. Same event, level and
                    # `exception` key, redacted (app/core/sanitize.py).
                    logger.error(
                        "event_unexpected_error",
                        entry_id=_id_str(entry_id),
                        exception=sanitized_traceback(exc),
                    )
                    await self._dead_letter_or_hold(
                        entry_id, fields,
                        reason=f"unexpected: {pipeline_error_of(exc)}",
                        attempts=attempt,
                    )
                    return
                if is_transient_db_error(exc) and not await self._database_answers():
                    attempt -= 1  # the database's moment, not the event's
                    waited += 1
                    delay = min(_WAIT_CEILING, _BACKOFF_DELAYS[0] * 2 ** (waited - 1))
                    if waited == 1 or waited % _WAIT_LOUD_EVERY == 0:
                        logger.warning(
                            "event_waiting_for_database",
                            entry_id=_id_str(entry_id),
                            waits=waited,
                            delay=delay,
                            error=pipeline_error_of(exc),
                        )
                    await asyncio.sleep(delay)
                    continue
                if attempt > len(_BACKOFF_DELAYS):
                    logger.warning(
                        "event_retries_exhausted",
                        entry_id=_id_str(entry_id),
                        attempts=attempt,
                        error=exception_text(exc),
                    )
                    # The class and the place, never the text: a database
                    # error's text is its SQL and bound parameters -- the
                    # letter, on a job's insert. The text stays in the
                    # log line above (redacted).
                    await self._dead_letter_or_hold(
                        entry_id, fields,
                        reason=f"retries exhausted: {pipeline_error_of(exc)}",
                        attempts=attempt,
                    )
                    return
                delay = _BACKOFF_DELAYS[attempt - 1]
                logger.info(
                    "event_retry_scheduled",
                    entry_id=_id_str(entry_id),
                    attempt=attempt,
                    delay=delay,
                    error=exception_text(exc),
                )
                await asyncio.sleep(delay)
                continue
            if result is HandleResult.DUPLICATE:
                logger.info("event_duplicate_acked", entry_id=_id_str(entry_id))
            elif result in (HandleResult.REJECTED, HandleResult.CONFLICT):
                logger.info(
                    "event_not_accepted_recorded",
                    entry_id=_id_str(entry_id),
                    result=result.value,
                )
            else:
                logger.info(
                    "event_processed",
                    entry_id=_id_str(entry_id),
                    event_type=type(event).__name__,
                    attempt=attempt,
                )
            # Acknowledged OUTSIDE the try (FIX review 3.13.0): an XACK
            # that fails is no failure of the event to dead-letter; the
            # entry stays pending, and its replay is a duplicate.
            await self._ack_or_hold(entry_id)
            return

    async def _ack_or_hold(self, entry_id: str | bytes) -> None:
        """Acknowledge; if Redis refuses, the entry stays pending (said
        loudly) -- never an exception out of an entry's handling."""
        try:
            await self._ack(entry_id)
        except Exception as exc:
            logger.error(
                "event_ack_failed",
                entry_id=_id_str(entry_id),
                error=pipeline_error_of(exc),
            )

    async def _dead_letter_or_hold(
        self,
        entry_id: str | bytes,
        fields: dict[Any, Any],
        *,
        reason: str,
        attempts: int,
    ) -> None:
        """THE ONE WAY an entry goes to the dead letter (FIX review
        3.13.0): written, then acknowledged. If the writing or the ack
        fails -- Redis down, a dead letter that cannot be built -- the
        entry STAYS PENDING: not lost, seen in XPENDING and in this log
        line, offered again at the next start (_drain_pending, its
        cursor past it). Never an exception out of an except clause:
        before, a TypeError of the dead letter's own JSON left
        _process_entry from inside one and took the consumer down, again
        at every restart."""
        try:
            await self._to_dlq(entry_id, fields, reason=reason, attempts=attempts)
            await self._ack(entry_id)
        except Exception as exc:
            logger.error(
                "dead_letter_write_failed",
                entry_id=_id_str(entry_id),
                error=pipeline_error_of(exc),
            )

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
        """Dead-letter the entry's address with diagnostics attached --
        DEAD LETTERS in the module header; the fields are
        dead_letter_fields'.

        The reason is redacted HERE, the one point every dead-letter
        path passes. It is a refusal of comms' own (a ValidationError:
        field names and the address, never the letter) or the class and
        place of an exception (pipeline_error_of), never an exception's
        text. The stream is capped (approximate MAXLEN) so a
        misbehaving producer cannot grow it without bound.
        """
        reason = sanitize_text(reason)
        payload = dead_letter_fields(
            fields, reason=reason, attempts=attempts, entry_id=_id_str(entry_id),
        )
        # redis-py's FieldT alias is invariant in dict params, so a
        # plain dict[str, str] does not satisfy it; alias it exactly.
        entry: dict[FieldT, EncodableT] = dict(payload.items())
        await self._redis.xadd(
            self._dlq,
            entry,
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


def _retryable(exc: BaseException) -> bool:
    """Whether the consumer tries an event again: an ordering lag (a
    NotFoundError -- the address book has not caught up) or a transient
    failure of the database, by the one rule (TX-1)."""
    return isinstance(exc, NotFoundError) or is_transient_db_error(exc)


def dlq_kept_data_fields(event: str | None) -> frozenset[str] | None:
    """The keys of `data` a dead letter keeps for this event; None when
    it keeps none.

    A list of what is ALLOWED, the event's own field set: a key that is
    not in it -- `titel` is the letter under another name -- is dropped
    like the letter. notification_request keeps its frame and envelope
    (deploy/INTEGRATION.md section 7) without the withheld link; the
    other events are the address book comms keeps anyway. An unknown or
    absent event has no field set: nothing of its `data` is kept.
    """
    if event == EVENT_NOTIFICATION_REQUEST:
        return NOTIFICATION_REQUEST_FRAME_FIELDS | (
            NOTIFICATION_REQUEST_ENVELOPE_FIELDS - _DLQ_WITHHELD_ENVELOPE_FIELDS
        )
    if event is None:
        return None
    return EVENT_FIELDS.get(event)


def dead_letter_fields(
    fields: dict[Any, Any], *, reason: str, attempts: int, entry_id: str,
) -> dict[str, str]:
    """The dead letter of one stream entry (DEAD LETTERS, module header).

    Values are written only where they are kept: `event` (when it is
    UTF-8) and the allowed keys of `data` (when it is a UTF-8 JSON
    object). Everything else is dropped by NAME -- a field of the entry
    under its own name (entry_field_name: a byte that is not UTF-8 as a
    backslash escape), a key of `data` as `data.<key>`, the whole of
    `data` as `data` -- and the names are listed in _dlq_dropped, so the
    reader sees what was there without seeing it. The fingerprint is of the
    `data` bytes as the stream delivered them, the digest a refusal at
    intake records (stream_fingerprint).
    """
    dead: dict[str, str] = {}
    dropped: list[str] = []
    event: str | None = None
    raw_data: bytes | None = None
    for key, value in fields.items():
        name = entry_field_name(key)
        raw = value if isinstance(value, bytes) else str(value).encode("utf-8")
        if name == ENVELOPE_EVENT_FIELD:
            event = _utf8_or_none(raw)
            if event is None:
                dropped.append(name)
        elif name == ENVELOPE_DATA_FIELD:
            raw_data = raw
        else:
            dropped.append(name)
    if event is not None:
        dead[ENVELOPE_EVENT_FIELD] = event
    if raw_data is not None:
        dead["_dlq_data_fingerprint"] = stream_fingerprint(raw_data)
        dead["_dlq_data_bytes"] = str(len(raw_data))
        kept = dlq_kept_data_fields(event)
        # Read by the one reader of event data (app/transport/events.py):
        # a bomb is not recursed into here, a NaN is not written back.
        data = event_data_or_none(raw_data)
        if kept is None or data is None:
            dropped.append(ENVELOPE_DATA_FIELD)
        else:
            # ensure_ascii: a lone surrogate (a JSON escape the producer
            # may send) is not encodable as UTF-8 on the way to redis.
            dead["_dlq_data"] = json.dumps(
                {k: v for k, v in data.items() if k in kept}, sort_keys=True,
            )
            dropped += [
                f"{ENVELOPE_DATA_FIELD}.{k}" for k in data if k not in kept
            ]
    dead["_dlq_dropped"] = json.dumps(sorted(dropped))
    dead["_dlq_error"] = reason
    dead["_dlq_attempts"] = str(attempts)
    dead["_dlq_source_entry_id"] = entry_id
    return dead


def _utf8_or_none(raw: bytes) -> str | None:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _id_str(entry_id: str | bytes) -> str:
    return (
        entry_id.decode("ascii")
        if isinstance(entry_id, bytes)
        else str(entry_id)
    )


async def run_consumer_loop() -> None:
    """Build the Redis client and run the consumer until cancelled. A
    redis that stays silent past the socket timeout raises TimeoutError
    out of here (app/transport/connection.py)."""
    redis: Redis = redis_client()
    consumer = StreamConsumer(redis)
    try:
        await consumer.run()
    finally:
        await redis.aclose()
