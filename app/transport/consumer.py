# =============================================================================
# COMMS Service -- Redis Streams Consumer (Phase 3c items 1, 6; W5-1)
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
#             acked one; and every _PENDING_REDRAIN_SECONDS the own
#             pending list once more (WK-3), so a held entry does not
#             wait for a restart.
#
# WHOSE ERROR IT IS (W5-1; the owner's decision of 10.11: a product's
# wrong input is the product's problem). What the product sent wrong is
# refused cheaply and kept nowhere -- no stream, no row, no replay:
#   refused under the key  a notification_request whose key is readable
#              and which cannot be accepted (events.py
#              RejectedNotificationRequest, handlers.py) -> recorded under
#              the key in the same transaction, XACK; the product reads it
#              by the key (deploy/INTEGRATION.md section 7).
#   dropped    anything else comms cannot take -- a broken frame, an
#              unknown event, data that cannot be read, a request whose
#              key cannot be read, an event refused by its parse or its
#              handler (ValidationError), a person comms was never told
#              about past the ordering retry (NotFoundError past
#              _BACKOFF_DELAYS) -> ONE log line, event_dropped (_drop),
#              and XACK. The line names the event's kind (a known kind,
#              else "unknown"), the stage, the class and place of the
#              refusal (pipeline_error_of -- never its text: a refusal
#              quotes the values it refused) and the SHA-256 and length
#              of the `data` bytes, by which the product finds the event
#              in its outbox; nothing else of the entry.
#   refused by class  a sync event late or repeated (ConflictError) ->
#              event_refused, XACK.
#   duplicate  (same key, same bytes) -> XACK only: a replay is the
#              at-least-once contract working, not an error.
# What comms broke is comms':
#   waiting    a TRANSIENT failure of the database while the database
#              does NOT answer (SELECT 1): waited for in place, uncounted,
#              the delay doubling to 30 s, said loudly on the 1st and
#              every 10th wait -- the event is not lost (FIX review
#              3.13.0).
#   retried    a NotFoundError (group_changed before its user_upserted)
#              and a TRANSIENT failure of the database while it DOES
#              answer, by the one rule (app/core/database.py
#              is_transient_db_error) -> bounded inline backoff
#              (_BACKOFF_DELAYS).
#   held       a transient failure past that budget while the database
#              answers, any other exception of the handling -> the entry
#              is HELD (below): left pending, said once (event_held), and
#              handled again at every pass over the pending list until
#              comms is mended.
#   dropped loudly  an exception of the parse that is not its refusal:
#              event_dropped at ERROR -- an entry that was never parsed
#              has no subject to be held by, and holding it would hold
#              the whole stream behind one defect; the ERROR line is the
#              defect's trace.
#
# HELD BY SUBJECT (W5-1, W6-2a). A held entry is handled again at the
# next pass, after the newer entries read meanwhile -- so no newer entry
# that its order matters to may be applied before it. Every parsed event
# names the subjects it WRITES and the subjects it READS (subjects_of):
#   writes  a sync event: the person it is about; a reminder_cancel: the
#           correlation it cancels by; a request: its key;
#   reads   a request: its correlation (a reminder_cancel finds its jobs
#           by it) and the person it is addressed to.
# Two of them need an order: a write and anything else of one subject
# (two writes of a person, a request and the cancel of its
# correlation); two reads do not (two requests to one person or under
# one correlation). self._held_writes and self._held_reads hold what
# the entries left pending write and read; an entry is left pending too
# (event_held_behind) when anything it writes or reads is a held write,
# or anything it writes is a held read. A held entry holds all it
# writes and reads. An entry held BEHIND one holds what it writes and the
# correlation it reads, not the person (review of 4.0.0, A1): a request
# to a person does not read the person at intake (create_notification
# resolves nobody), so its order against that person's events matters
# only as the promise a held entry keeps (W5-1: no event of its person
# overtakes it); carried along behind, it froze every event of that
# person and of each person it met in turn -- one held entry, six sound
# ones stopped. A chain A{p} -> B{p reads, c reads} -> C{c writes}
# keeps C behind B. Each pass over the pending list starts with both
# sets empty and goes in the stream's order from "0", so the held entry
# comes first, then the entries behind it, each once; the sets are
# rebuilt from what stays pending. Invariant: the sets hold, for every
# entry left pending, what it writes and the correlation it reads -- and
# the person, for one held by its own failure. The events of every other
# subject flow.
#
# DURABILITY OF INTAKE -- the order is COMMIT, THEN XACK, and it must
# never be swapped (spec §5.4: a receipt without a record is a promise
# that disappears on a crash). The entry is acknowledged only after
# the transaction that wrote the job -- or the record of its refusal --
# has committed. A crash between the two leaves the entry pending; the
# next start replays it (_drain_pending) -- and, an entry left pending
# without a crash, the next pass over the pending list in the running
# process (WK-3) -- and the replay is collapsed
# by the key: same bytes -> duplicate, nothing created twice. An ack
# before the commit would lose the job on a crash in between; batching
# acks or acknowledging on read would do exactly that.
#
# REDIS ITSELF, on the XACK (review 3.14.0, 2.3, N3). It is retried IN
# PLACE (_until_done), the next entry not taken meanwhile: an entry left
# pending would be handled again later, over the newer entries behind
# it. A lost connection, a timeout, a redis loading its data, an OOM and
# an error of any other kind are retried for as long as they last, said
# loudly. A REFUSAL -- a ResponseError but OOM, an authentication or
# authorization error (_is_refusal) -- that comes
# _REDIS_REFUSALS_BEFORE_EXIT times ends run() with RedisRefusedError: the
# connection that keeps meeting a replica after a failover is mended by
# the restart, not by a retry on it, and the entry, still pending, is
# handled first at the next start.
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
#
# KNOWN CEILING -- a subject held for good (W5-1).
# Mechanics: an entry whose handling fails every time -- a defect of
# comms on that entry, a transient failure that outlasts every budget --
# stays pending and keeps its subjects held (self._held_writes,
# self._held_reads): every later event that its order matters to is held
# behind it, and the
# entry is handled again at every pass (_PENDING_REDRAIN_SECONDS), a
# transient one with its inline budget (BL-4) each time. Said once per
# entry and start (event_held, event_held_behind), not at every pass.
# Status: acknowledged by design.
# Task: none -- a defect of comms is mended by a release of comms, and
# the entry is applied in its order once it is.
# Unfreeze trigger: event_held in the log for an entry that a release
# has not mended, OR XPENDING of the group growing while new events of
# other people flow.
# Agreed fix shape: mend the defect the event_held line names; nothing
# in the consumer -- the held entry and the ones behind it then go out
# in their order at the next pass.
# Rejected: the dead-letter stream (W5-1: no reader, and a copy of the
# product's input kept by comms); the entry dropped after N passes (an
# event lost to a defect of comms, the product never told); the whole
# stream held behind it (one defect on one product's event would stop
# every other person's events -- REVIEW-ACCEPTED 9); the entry handled
# again later without its subjects held (the reorder this closes: a
# group_changed true applied over the false behind it).
# =============================================================================

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import structlog
from redis.asyncio import Redis
from redis.exceptions import (
    AuthenticationError,
    AuthorizationError,
    OutOfMemoryError,
    ResponseError,
)

from app.core import liveness
from app.core.config import settings
from app.core.database import (
    database_answers,
    get_session_factory,
    is_transient_db_error,
    waiting_statement_seconds,
)
from app.core.exceptions import (
    ConflictError,
    NotFoundError,
    ValidationError,
    conflict_class,
)
from app.core.logging import request_id_of, serving
from app.core.sanitize import (
    exception_text,
    pipeline_error_of,
    sanitized_traceback,
)
from app.engine.constants import TargetType
from app.engine.service import stream_fingerprint
from app.transport.connection import redis_client, redis_socket_timeout
from app.transport.events import (
    ENVELOPE_DATA_FIELD,
    ENVELOPE_EVENT_FIELD,
    ENVELOPE_REQUEST_ID_FIELD,
    KNOWN_EVENTS,
    GroupChanged,
    NotificationRequest,
    ParsedEvent,
    RejectedNotificationRequest,
    ReminderCancel,
    SectionMembershipChanged,
    UserDeleted,
    UserUpserted,
    entry_field_name,
    parse_event,
)
from app.transport.handlers import HandleResult, handle_event

logger = structlog.get_logger()

# Inline backoff for RETRYABLE failures: 6 attempts total (first try
# + 5 retries), ~6.2s worst case -- sized for "the upsert is moments
# behind", not for outages (a database that does not answer is waited
# for; one that answers and keeps failing holds the entry).
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
# The doublings _wait_delay counts at most: 0.2 s * 2**32 is past any
# ceiling a deploy could want, and an exponent kept this small never
# overflows the float -- before, a wait of ~8.5 h (2**1024) raised
# OverflowError out of the wait itself.
_WAIT_DOUBLINGS = 32

# A REFUSAL OF REDIS (REDIS ITSELF, the header): this many of them on one
# operation of one entry end run(). With the doubling wait (0.2 s to the
# ceiling) the process holds ~25 s before it goes: time for a failover to
# promote the very replica the connection meets, which mends READONLY in
# place; past that a retry on the same connection mends nothing, and the
# restart costs seconds and one entry replayed in the stream's order.
_REDIS_REFUSALS_BEFORE_EXIT = 8

# THE PENDING LIST AGAIN, WHILE RUNNING (WK-3): every this many seconds
# run() reads its own pending list once more (_drain_pending, its cursor
# rule kept) between its reads of new entries. An entry left pending --
# held, or held behind a held one (HELD BY SUBJECT, the header) -- is
# offered again in the running process, no longer only at the next
# start. An ack is never left pending: it is retried in place
# (_until_done). The cost, named: an entry held for good is handled
# again every interval, an ordering retry of it (BL-4) included (the
# KNOWN CEILING in the header).
_PENDING_REDRAIN_SECONDS = 60.0


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
        self._session_factory = get_session_factory()
        # What the entries left pending write and read (HELD BY SUBJECT, the
        # header): emptied at the start of every pass over the pending
        # list, which rebuilds them.
        self._held_writes: set[str] = set()
        self._held_reads: set[str] = set()
        # The entries whose hold is said: one line per entry, not one per
        # pass (event_held, event_held_behind, event_left_pending); an
        # entry leaves the set with its ack.
        self._said: set[str] = set()

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
        """Consume forever: drain own pending, then follow new entries --
        and the own pending list again every _PENDING_REDRAIN_SECONDS
        (WK-3), checked before each read of new entries: one task, no
        second reader of the stream. A blocking read waits at most
        consumer_block_ms, so a pass comes within the interval and one
        block.

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
        next_drain = time.monotonic() + _PENDING_REDRAIN_SECONDS
        while True:
            liveness.step()
            if time.monotonic() >= next_drain:
                again = await self._drain_pending()
                next_drain = time.monotonic() + _PENDING_REDRAIN_SECONDS
                if again:
                    logger.info("consumer_pending_redrained", entries=again)
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
        class and place, once per entry, the entry left pending (its
        subjects held, _process_entry) for the next pass over the pending
        list (WK-3: every _PENDING_REDRAIN_SECONDS, and at the next
        start). A failure of the loop's own read (Redis down) is outside
        this and still ends run(): the restart policy reconnects. So does
        RedisRefusedError, let through here: a refusal of redis on the
        entry's XACK (REDIS ITSELF, the module header) -- caught, it
        would leave the entry pending and take the next, the window
        _until_done closes.

        THE ENTRY'S REQUEST ID (W3-2, protocol 9.6): every line written
        for the entry -- its parse, its handling, its drop, this last
        line -- carries the id of its envelope (entry_request_id), or one
        of comms' own; the next entry starts from its own, and a line
        after the entry from none."""
        with serving(entry_request_id(fields)):
            try:
                await self._process_entry(entry_id, fields)
            except RedisRefusedError:
                raise  # the process's, not the entry's (REDIS ITSELF, header)
            except Exception as exc:
                self._say_once(
                    entry_id, logger.error, "event_left_pending",
                    entry_id=_id_str(entry_id),
                    error=pipeline_error_of(exc),
                )

    async def _drain_pending(self) -> int:
        """Replay entries delivered to THIS consumer but never acked
        (crash between XREADGROUP and XACK, or an entry left pending).
        Reading from id "0" returns the pending list of this consumer
        name; a stable name makes restart recovery this one loop. Run at
        the start and every _PENDING_REDRAIN_SECONDS (run, WK-3).

        The held subjects are emptied first (HELD BY SUBJECT, the module
        header): every entry left pending is in this list and is met in
        it in the stream's order -- the held one before the ones held
        behind it -- so the pass rebuilds the set from what stays
        pending, and a subject whose held entry goes out now lets the
        entries behind it go out after it, in this same pass."""
        self._held_writes.clear()
        self._held_reads.clear()
        drained = 0
        # A CURSOR, not "0" every time (FIX review 3.13.0): a held entry
        # stays in the pending list, and reading from "0" again would meet
        # it forever -- no new event would ever be read. Each read starts
        # after the last entry seen: a held entry is met once per pass.
        cursor: str | bytes = "0"
        while True:
            liveness.step()
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
        """Parse -> (held behind?) -> handle -> ack, or the entry held.
        Never raises for a bad EVENT: what the product sent wrong is
        refused or dropped and acknowledged (WHOSE ERROR IT IS, the
        module header). An ack that fails is retried in place until it is
        written (_ack_until_done): the next entry waits.

        An entry whose order a held one matters to is not handled: it is
        left pending behind it, and what it writes and the correlation it
        reads are held too (HELD BY SUBJECT, the module header). An entry
        whose handling fails in comms (_apply returns the failure) is
        held: left pending, all it writes and reads held, said once
        (event_held). An exception out of the handling holds them too,
        and the last line
        (_process_guarded) says it."""
        try:
            event = parse_event(fields)
        except ValidationError as exc:
            await self._drop(entry_id, fields, stage="parse", exc=exc)
            return
        except Exception as exc:
            # ANY failure of the parse is the entry's, never the
            # process's (poison-pill rule, EM-1 FIX-2) -- and a defect of
            # comms: never parsed, the entry has no subject to be held by
            # (WHOSE ERROR IT IS, the header), so it is dropped at ERROR.
            # The class and the place, no traceback: what the parse raises
            # may quote the data it was reading, and a request's data is
            # the letter.
            await self._drop(entry_id, fields, stage="parse", exc=exc, loud=True)
            return

        writes, reads = subjects_of(event)
        if (writes | reads) & self._held_writes or writes & self._held_reads:
            self._held_writes |= writes
            # Behind: the correlation it reads, never the person.
            self._held_reads |= {
                r for r in reads if r.startswith("correlation:")
            }
            self._say_once(
                entry_id, logger.info, "event_held_behind",
                entry_id=_id_str(entry_id),
                kind=_kind_of(fields),
            )
            return
        try:
            held = await self._apply(entry_id, fields, event)
        except Exception:
            self._hold(writes, reads)  # left pending by the last line
            raise
        if held is None:
            return
        failure, attempts = held
        self._hold(writes, reads)
        # Not logger.exception(): the renderer would print the chain raw
        # -- a database error's text is its SQL and bound parameters. The
        # `exception` key, redacted (app/core/sanitize.py).
        self._say_once(
            entry_id, logger.error, "event_held",
            entry_id=_id_str(entry_id),
            kind=_kind_of(fields),
            attempts=attempts,
            error=pipeline_error_of(failure),
            exception=sanitized_traceback(failure),
        )

    async def _apply(
        self, entry_id: str | bytes, fields: dict[Any, Any], event: ParsedEvent,
    ) -> tuple[BaseException, int] | None:
        """Handle the event and acknowledge it -- None -- or give back the
        failure that holds it and the attempts made.

        RETRIES:
          - NotFoundError (the address book has not caught up) and a
            transient failure of the database WHILE THE DATABASE
            ANSWERS -- the bounded budget, _BACKOFF_DELAYS; past it a
            NotFoundError is the product's (a person comms was never
            told about): dropped; a transient failure is comms': held;
          - a transient failure while the database does NOT answer
            (app/core/database.py database_answers) -- WAIT, uncounted,
            the delay doubling to _WAIT_CEILING, said loudly on the
            first and every _WAIT_LOUD_EVERY-th wait: it says nothing of
            the event, and the event is not lost.
        The retry lines of an entry already held are not written again:
        its hold was said once (event_held), its retries every pass would
        repeat it. See the KNOWN CEILING (BL-4) in the module header: the
        inline sleep blocks the whole stream -- as a database that does
        not answer would anyway."""
        quiet = _id_str(entry_id) in self._said
        attempt = 0  # tries while the database answered
        waited = 0  # waits while it did not
        while True:
            liveness.step()
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
                await self._ack_until_done(entry_id)
                return None
            except ValidationError as exc:
                await self._drop(entry_id, fields, stage="handle", exc=exc)
                return None
            except Exception as exc:
                if not _retryable(exc):
                    return exc, attempt
                if is_transient_db_error(exc) and not await self._database_answers():
                    attempt -= 1  # the database's moment, not the event's
                    waited += 1
                    delay = _wait_delay(waited)
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
                    if not quiet:
                        logger.warning(
                            "event_retries_exhausted",
                            entry_id=_id_str(entry_id),
                            attempts=attempt,
                            error=exception_text(exc),
                        )
                    if isinstance(exc, NotFoundError):
                        await self._drop(entry_id, fields, stage="retries", exc=exc)
                        return None
                    return exc, attempt
                delay = _BACKOFF_DELAYS[attempt - 1]
                if not quiet:
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
            # that fails is no failure of the event. It is retried here,
            # the session already closed -- no connection to the database
            # and no row lock held while it waits.
            await self._ack_until_done(entry_id)
            return None

    async def _drop(
        self,
        entry_id: str | bytes,
        fields: dict[Any, Any],
        *,
        stage: str,
        exc: BaseException,
        loud: bool = False,
    ) -> None:
        """THE ONE WAY an entry is dropped (WHOSE ERROR IT IS, the module
        header): one line, event_dropped, then the ack. WARNING for the
        product's input, ERROR (`loud`) for a defect of comms met on it.

        The line holds the ADDRESS of the entry, never its letter nor a
        value of its fields: the kind (_kind_of), the stage, the class and
        place of the refusal (pipeline_error_of -- a refusal's text quotes
        the values it refused: an unknown event's name, a version), and
        the SHA-256 of the `data` bytes as the stream delivered them -- the
        digest a refusal at intake records (stream_fingerprint) -- with
        their length; both None when the entry has no `data`."""
        data = _data_of(fields)
        (logger.error if loud else logger.warning)(
            "event_dropped",
            entry_id=_id_str(entry_id),
            kind=_kind_of(fields),
            stage=stage,
            reason=pipeline_error_of(exc),
            data_fingerprint=None if data is None else stream_fingerprint(data),
            data_bytes=None if data is None else len(data),
        )
        await self._ack_until_done(entry_id)

    def _hold(self, writes: frozenset[str], reads: frozenset[str]) -> None:
        """An entry held by its own failure holds all it writes and reads
        (HELD BY SUBJECT, the module header)."""
        self._held_writes |= writes
        self._held_reads |= reads

    def _say_once(
        self,
        entry_id: str | bytes,
        log: Callable[..., Any],
        event: str,
        /,
        **fields: Any,
    ) -> None:
        """Say the hold of an entry once, not at every pass over the
        pending list that meets it again (the KNOWN CEILING of a subject
        held for good, the module header)."""
        key = _id_str(entry_id)
        if key in self._said:
            return
        self._said.add(key)
        log(event, **fields)

    # KNOWN CEILING -- the process stopped while an ack is retried.
    # Mechanics: an event is committed and its XACK keeps failing; the
    # process stops in the retry (SIGTERM, a crash, the OOM killer) or
    # ends it on purpose -- a refusal of redis _REDIS_REFUSALS_BEFORE_EXIT
    # times (RedisRefusedError). The entry is still pending, and the next
    # start's _drain_pending handles it again FIRST -- before any new
    # entry, which is read only after the drain -- so the stream's order
    # is kept, but the event is applied a second time. A sync event
    # re-applied with nothing newer between is the same state; a
    # notification_request is collapsed by its key; a dropped entry is
    # dropped again, one more line.
    # Status: acknowledged by design.
    # Task: none -- the closure is the ordering of membership events,
    # v4.0's, and no task of this repository carries it.
    # Unfreeze trigger: a writer of memberships outside the stream (a
    # REST route), OR a handler whose second application is not the same
    # state, OR event_ack_failed or redis_refused in the log followed by
    # a restart before event_acked_after_retry.
    # Agreed fix shape: an order for membership events -- the stream
    # entry id kept on the membership row, or a version from the product
    # -- and a replay older than the row refused.
    # Rejected: the ack before the commit (spec §5.4 -- the job lost on a
    # crash between them); the retry in a task of its own (the next
    # entry taken before the ack, and a replay of this one applied over
    # it -- the window this closes); a refusal retried for as long as it
    # lasts (review 3.14.0, N3: READONLY from a replica after a failover,
    # the stream standing, the healthcheck green, and the restart that
    # would reconnect never coming); every failure of redis ending the
    # process (a lost connection mends in place, and a restart per blip
    # replays an entry each time).
    async def _ack_until_done(self, entry_id: str | bytes) -> None:
        """Acknowledge, retrying IN PLACE until redis takes it: the event
        is committed, a replay of it would apply it again, and the next
        entry is not taken meanwhile -- a group_changed true replayed over
        the false behind it would put the person back (review 3.14.0,
        2.3). _until_done; the KNOWN CEILING above. Acknowledged, the
        entry is no longer held: its hold may be said again if it ever
        is (_say_once)."""
        await self._until_done(
            entry_id,
            lambda: self._ack(entry_id),
            failed="event_ack_failed",
            done_after_retry="event_acked_after_retry",
        )
        self._said.discard(_id_str(entry_id))

    async def _until_done(
        self,
        entry_id: str | bytes,
        call: Callable[[], Awaitable[None]],
        *,
        failed: str,
        done_after_retry: str,
    ) -> None:
        """One operation of redis on one entry, retried IN PLACE until it
        is done (REDIS ITSELF, the module header): the delay doubling to
        _WAIT_CEILING (_wait_delay); `failed` said on the first and every
        _WAIT_LOUD_EVERY-th failure, `done_after_retry` once when it is
        done. A refusal (_is_refusal) counts; at
        _REDIS_REFUSALS_BEFORE_EXIT of them RedisRefusedError leaves run(),
        the entry pending. Nothing else counts -- a lost connection between
        refusals neither adds to them nor clears them. A cancellation is
        not caught: the stop stays a stop."""
        failures = 0
        refusals = 0
        while True:
            liveness.step()
            try:
                await call()
            except Exception as exc:
                failures += 1
                if _is_refusal(exc):
                    refusals += 1
                    if refusals >= _REDIS_REFUSALS_BEFORE_EXIT:
                        logger.error(
                            "redis_refused",
                            entry_id=_id_str(entry_id),
                            operation=failed,
                            refusals=refusals,
                            attempts=failures,
                            error=pipeline_error_of(exc),
                        )
                        raise RedisRefusedError(
                            f"redis refused {failed} of {_id_str(entry_id)} "
                            f"{refusals} times",
                        ) from exc
                delay = _wait_delay(failures)
                if failures == 1 or failures % _WAIT_LOUD_EVERY == 0:
                    logger.error(
                        failed,
                        entry_id=_id_str(entry_id),
                        attempts=failures,
                        delay=delay,
                        error=pipeline_error_of(exc),
                    )
                await asyncio.sleep(delay)
                continue
            if failures:
                logger.warning(
                    done_after_retry,
                    entry_id=_id_str(entry_id),
                    attempts=failures + 1,
                )
            return

    async def _ack(self, entry_id: str | bytes) -> None:
        await self._redis.xack(self._stream, self._group, entry_id)


def _retryable(exc: BaseException) -> bool:
    """Whether the consumer tries an event again: an ordering lag (a
    NotFoundError -- the address book has not caught up) or a transient
    failure of the database, by the one rule (TX-1)."""
    return isinstance(exc, NotFoundError) or is_transient_db_error(exc)


class RedisRefusedError(RuntimeError):
    """Redis refused one operation of one entry _REDIS_REFUSALS_BEFORE_EXIT
    times (REDIS ITSELF, the module header): out of run() and out of the
    process, the restart policy bringing it back. Its cause is the last
    refusal."""


def _is_refusal(exc: BaseException) -> bool:
    """A refusal of redis that a retry on the same connection does not
    mend: an authentication or authorization error (redis-py raises them
    as ConnectionError, so they are asked first), any ResponseError --
    READONLY, NOPERM, WRONGTYPE and the codes without a class of their
    own -- but OOM, which a restart does not mend either: memory is freed
    by redis, not by a reconnect. Everything else -- a lost connection, a
    timeout, LOADING, an error that is not redis' -- is retried."""
    if isinstance(exc, (AuthenticationError, AuthorizationError)):
        return True
    return isinstance(exc, ResponseError) and not isinstance(exc, OutOfMemoryError)


def _wait_delay(n: int) -> float:
    """The n-th delay (n >= 1) of a wait that doubles from
    _BACKOFF_DELAYS[0] to _WAIT_CEILING: the database's absence and an
    ack's retry alike. The exponent is bounded (_WAIT_DOUBLINGS), so a
    wait of any length is a number, never an OverflowError."""
    doublings = min(n - 1, _WAIT_DOUBLINGS)
    return float(min(_WAIT_CEILING, _BACKOFF_DELAYS[0] * 2 ** doublings))



def subjects_of(event: ParsedEvent) -> tuple[frozenset[str], frozenset[str]]:
    """What the event WRITES and what it READS, the subjects whose order
    a held entry keeps (HELD BY SUBJECT, the module header) -- read from
    the envelope only, never from the letter.

    Writes: a sync event, the person it is about (an operator is a person
    of the address book, set_membership checks it as one); a
    reminder_cancel, its correlation; a notification request, its key (a
    second request under the same key is decided by the first), and so
    does a request refused at its parse. Reads: a request, its
    correlation (a reminder_cancel finds its jobs by it) and the person
    it is addressed to, when it is one. A person's id is written in one
    form -- the UUID's -- whatever case the request sent it in."""
    none: frozenset[str] = frozenset()
    if isinstance(event, UserUpserted | UserDeleted | GroupChanged):
        return frozenset({f"person:{event.recipient_id}"}), none
    if isinstance(event, SectionMembershipChanged):
        return frozenset({f"person:{event.operator_id}"}), none
    if isinstance(event, ReminderCancel):
        return frozenset({f"correlation:{event.correlation}"}), none
    if isinstance(event, RejectedNotificationRequest):
        return frozenset({f"key:{event.idempotency_key}"}), none
    assert isinstance(event, NotificationRequest)
    reads = set()
    if event.correlation is not None:
        reads.add(f"correlation:{event.correlation}")
    if event.target_type == TargetType.USER and event.target_value is not None:
        reads.add(f"person:{UUID(event.target_value)}")
    return frozenset({f"key:{event.idempotency_key}"}), frozenset(reads)


def _kind_of(fields: dict[Any, Any]) -> str:
    """The entry's `event` when it names a known kind, else "unknown":
    an unknown name is the producer's value, and a line of comms carries
    none (_drop)."""
    for key, value in fields.items():
        if entry_field_name(key) != ENVELOPE_EVENT_FIELD:
            continue
        name = value if isinstance(value, str) else value.decode(
            "utf-8", errors="replace",
        )
        return name if name in KNOWN_EVENTS else "unknown"
    return "unknown"


def _data_of(fields: dict[Any, Any]) -> bytes | None:
    """The `data` bytes as the stream delivered them, or None."""
    for key, value in fields.items():
        if entry_field_name(key) == ENVELOPE_DATA_FIELD:
            return value if isinstance(value, bytes) else str(value).encode("utf-8")
    return None


def entry_request_id(fields: dict[Any, Any]) -> str:
    """The id an entry's lines are written under: its envelope's
    request_id when that is of the form (app/core/logging.py
    request_id_of), else a fresh one -- absent, empty, too long, not
    ASCII or not UTF-8 alike. Never a refusal of the event (9.6): this is
    the address of the lines, not a part of the event."""
    for key, value in fields.items():
        if entry_field_name(key) == ENVELOPE_REQUEST_ID_FIELD:
            return request_id_of(value)
    return request_id_of(None)


def _id_str(entry_id: str | bytes) -> str:
    return (
        entry_id.decode("ascii")
        if isinstance(entry_id, bytes)
        else str(entry_id)
    )


def consumer_step_bound() -> float:
    """The longest the consumer's loop may wait between two steps
    (app/core/liveness.py): one wait of a retry at its ceiling
    (_WAIT_CEILING), one operation of redis (its socket timeout), ONE
    statement that waits for an attempt's row (app/core/database.py
    waiting_statement_seconds -- the attempt's hold and the margins) and
    the database around them (liveness.STEP_MARGIN_SECONDS). Its steps:
    every read of the stream, every try of an entry, every retry of an
    operation of redis on it, and every wait for an attempt's row
    (wait_for_attempts steps before its lock: a try that waits twice --
    forgetting: the tombstone, then the jobs -- steps twice). A wait for
    the database that does not answer goes on stepping: the loop turns,
    the entry waits by design.

    Until W6-2a the bound had no wait for an attempt in it (105 s), and a
    consumer waiting by design -- withdraw_recipient, close_notifications,
    since W6-2a the tombstone -- for an attempt's hold of 150 s and more
    was taken for a hung one (review of 4.0.0, B2). Called after the
    entry point named the hold (app/consumer.py main)."""
    return (
        _WAIT_CEILING + redis_socket_timeout() + waiting_statement_seconds()
        + liveness.STEP_MARGIN_SECONDS
    )


async def run_consumer_loop() -> None:
    """Build the Redis client and run the consumer until cancelled, as
    the watched loop `consumer` (app/core/liveness.py). A redis that stays
    silent past the socket timeout raises TimeoutError out of here
    (app/transport/connection.py)."""
    redis: Redis = redis_client()
    consumer = StreamConsumer(redis)
    try:
        with liveness.watched("consumer", consumer_step_bound()):
            await consumer.run()
    finally:
        await redis.aclose()
