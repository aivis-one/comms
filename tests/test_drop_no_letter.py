# =============================================================================
# COMMS Service -- a dropped or held entry: its address, never its letter
# =============================================================================
#
# Until W5-1 an entry comms could not take went to a dead-letter stream
# that held its address (G4, then review 3.14.0); W5-1 removed the stream
# (no reader; a copy of the product's input kept by comms). What is left
# of such an entry is one line of comms' log -- event_dropped for the
# product's input, event_held for a failure of comms
# (app/transport/consumer.py, WHOSE ERROR IT IS) -- and what those lines
# may hold is held here, on the real loop:
#
#   1  the grid of entries (event x data x extra fields): a sentinel in
#      every field of the letter, in llm_audio, in an unknown key of
#      data, in an extra field of the entry, in a data that is not JSON
#      -- in no line of the log; the pair: one event_dropped per entry,
#      its kind, its fingerprint and length (TestTheGrid). A request
#      whose key is readable is no row: it is refused under its key
#      (tests/test_consumer_drops_and_holds.py)
#   2  the fingerprint is stream_fingerprint of the data bytes as sent
#      (TestTheFingerprint)
#   3  every path of a failure of comms, with a letter: the exhausted
#      retries (by hand and real), the unexpected exception (mocked, and
#      from the database) -- held, its line the class and the place,
#      never the text (TestEveryHeldPath)
#   4  a malformed event other than a request -- a string the database
#      cannot store, a nesting bomb, a number comms cannot store, a byte
#      that is not UTF-8 -- dropped by the parse, at once, and the next
#      entry processed by the same loop (TestUnstorable, TestPoisonPill,
#      TestNotUtf8)
#   5  the contract: the split of notification_request in section 7
#      equals the code's three sets, both ways; the line of a drop is the
#      one section 7 describes (TestTheContract)
#
# Mutations (built once, by hand, each failing the named class):
#   M1  the drop's reason the refusal's text           -> TestTheGrid
#   M2  the fingerprint of the re-serialized data      -> TestTheFingerprint
#   M3  event_held says str(exc)                       -> TestEveryHeldPath
#   M4  the UTF-8 refusal taken out of parse_event     -> TestNotUtf8
#   M5  a field of the line renamed                    -> TestTheContract
# =============================================================================

import asyncio
import json
import re
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fakeredis import aioredis as fakeaioredis
from sqlalchemy.exc import DataError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

import app.transport.consumer as consumer_module
from app.audience.models import Recipient
from app.core.config import settings
from app.engine.models import Notification
from app.engine.service import stream_fingerprint
from app.transport.consumer import StreamConsumer
from app.transport.events import (
    EVENT_FIELDS,
    NOTIFICATION_REQUEST_ENVELOPE_FIELDS,
    NOTIFICATION_REQUEST_FRAME_FIELDS,
    NOTIFICATION_REQUEST_LETTER_FIELDS,
)
from tests.helpers import (
    next_phase3c_telegram_id,
    next_snapshot_version,
    real_serialization_failure,
)

_REPO = Path(__file__).resolve().parents[1]
_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")
_WAIT_TIMEOUT = 5.0

# Not a substring of anything comms writes on its own: a uuid in hex.
_S = "sentinel" + uuid4().hex

# The keys of a line of a drop (app/transport/consumer.py _drop).
_DROP_LINE = {"event", "log_level", "entry_id", "kind", "stage", "reason",
              "data_fingerprint", "data_bytes"}


def _letter_request(**overrides: Any) -> dict[str, Any]:
    """A notification_request with a sentinel in every field of the
    letter, in the link to the audio body and in an unknown key."""
    data: dict[str, Any] = {
        "v": 1,
        "idempotency_key": f"key-{uuid4().hex}",
        "type": "unit_event",
        "target_type": "user",
        "target_value": str(uuid4()),
        "scheduled_at": "2030-01-01T00:00:00+00:00",
        "expiry_at": "2030-01-02T00:00:00+00:00",
        "correlation": "corr-1",
        "title": f"title {_S}",
        "body": f"body {_S}",
        "action_data": {"name": f"{_S}", "params": {"p": f"{_S}"}},
        "llm": {"messages": [{"role": "user", "content": f"{_S}"}]},
        "email": {
            "subject": f"subject {_S}", "text": f"text {_S}",
            "html": f"<p>{_S}</p>",
        },
        "llm_audio": {"url": f"http://rustfs:9000/a?sig={_S}", "format": "wav"},
        "titel": f"misspelled {_S}",
    }
    data.update(overrides)
    return data


def _user_upserted(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "v": 1,
        "recipient_id": str(uuid4()),
        "version": next_snapshot_version(),
        "telegram_id": next_phase3c_telegram_id(),
        "email": None,
        "locale": "en",
        "timezone": "Europe/Berlin",
        "active": True,
    }
    data.update(overrides)
    return data


def _entry(
    event: bytes | None, data: bytes | None, **extra: bytes,
) -> dict[bytes, bytes]:
    fields: dict[bytes, bytes] = {}
    if event is not None:
        fields[b"event"] = event
    if data is not None:
        fields[b"data"] = data
    for name, value in extra.items():
        fields[name.encode()] = value
    return fields


def _lines(logs: list[dict[str, Any]], event: str) -> list[dict[str, Any]]:
    return [e for e in logs if e["event"] == event]


def _assert_no_sentinel_logged(logs: list[dict[str, Any]]) -> None:
    """No line carries the letter -- but the redacted traceback of a held
    entry (`exception`), which keeps an exception's chain as comms logged
    it before W5-1 (tests/test_transport_consumer.py
    TestConsumerRedactsItsFailures holds what it keeps and hides)."""
    assert logs  # the pair: something was said
    lines = [{k: v for k, v in e.items()
              if not (e["event"] == "event_held" and k == "exception")}
             for e in logs]
    assert _S not in json.dumps(lines, default=str)


# -----------------------------------------------------------------------------
# the real loop
# -----------------------------------------------------------------------------


@pytest.fixture
def redis() -> fakeaioredis.FakeRedis:
    return fakeaioredis.FakeRedis()


@pytest.fixture
def stream(monkeypatch: pytest.MonkeyPatch) -> str:
    name = f"comms:test:{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", name)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    monkeypatch.setattr(consumer_module, "_BACKOFF_DELAYS", (0.01, 0.01))
    return name


async def _run_until(
    consumer: StreamConsumer, condition: Callable[[], Awaitable[bool]],
) -> None:
    task = asyncio.ensure_future(consumer.run())
    try:
        deadline = asyncio.get_event_loop().time() + _WAIT_TIMEOUT
        while not await condition():
            if task.done():
                task.result()
                pytest.fail("consumer loop exited unexpectedly")
            if asyncio.get_event_loop().time() > deadline:
                pytest.fail("condition not reached before timeout")
            await asyncio.sleep(0.02)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def _pending(redis: fakeaioredis.FakeRedis, stream: str) -> int:
    try:
        pending = await redis.xpending_range(
            stream, settings.comms_consumer_group, min="-", max="+", count=100,
        )
    except Exception:
        return -1
    return len(pending)


async def _consume(
    redis: fakeaioredis.FakeRedis, stream: str, logs: list[dict[str, Any]],
    *, said: str, count: int, pending: int = 0,
) -> list[dict[str, Any]]:
    """Run the loop until `count` lines `said` are written and `pending`
    entries are left; those lines."""
    async def done() -> bool:
        return (len(_lines(logs, said)) == count
                and await _pending(redis, stream) == pending)

    await _run_until(StreamConsumer(redis), done)
    return _lines(logs, said)


# -----------------------------------------------------------------------------
# 1 -- the grid
# -----------------------------------------------------------------------------


_REQUEST = json.dumps(_letter_request()).encode()
_UPSERT = json.dumps(_user_upserted(title=f"{_S}")).encode()

# (id, entry, the kind its line names)
_GRID: list[tuple[str, dict[bytes, bytes], str]] = [
    ("request_without_key",
     _entry(b"notification_request", json.dumps(
         {k: v for k, v in _letter_request().items() if k != "idempotency_key"},
     ).encode()), "notification_request"),
    ("address_book_event_unknown_key", _entry(b"user_upserted", _UPSERT),
     "user_upserted"),
    ("unknown_event_object", _entry(_S.encode(), _REQUEST), "unknown"),
    ("empty_event_object", _entry(b"", _REQUEST), "unknown"),
    ("no_event_object", _entry(None, _REQUEST), "unknown"),
    ("request_data_not_json",
     _entry(b"notification_request", f'{{"title": "{_S}"'.encode()),
     "notification_request"),
    ("request_data_empty", _entry(b"notification_request", b""),
     "notification_request"),
    ("request_data_json_list",
     _entry(b"notification_request", json.dumps([_S]).encode()),
     "notification_request"),
    ("request_data_json_string",
     _entry(b"notification_request", json.dumps(_S).encode()),
     "notification_request"),
    ("request_data_absent_extra_field",
     _entry(b"notification_request", None, note=_S.encode()),
     "notification_request"),
    ("request_data_empty_object", _entry(b"notification_request", b"{}"),
     "notification_request"),
    ("request_repeated_key",
     _entry(b"notification_request",
            f'{{"type": "t", "title": "{_S}a", "title": "{_S}b"}}'.encode()),
     "notification_request"),
    ("data_not_utf8",
     _entry(b"notification_request", _S.encode() + b"\xff"),
     "notification_request"),
    ("event_not_utf8", _entry(b"notification_\xffrequest", _REQUEST), "unknown"),
    ("field_name_not_utf8",
     {b"event": b"notification_request", b"data": b"{}",
      b"no\xfete" + _S.encode(): _S.encode()},
     "notification_request"),
]


class TestTheGrid:
    async def test_the_address_stays_the_letter_goes(
        self, redis: fakeaioredis.FakeRedis, stream: str,
    ) -> None:
        """Every row of the grid through one loop: one line each, its
        kind, its fingerprint and length -- and the sentinel, which every
        row carries but one, in no line of the log."""
        ids = {}
        for name, fields, kind in _GRID:
            entry_id = (await redis.xadd(stream, fields)).decode()
            ids[entry_id] = (name, fields, kind)
        with capture_logs() as logs:
            lines = await _consume(redis, stream, logs,
                                   said="event_dropped", count=len(_GRID))
        _assert_no_sentinel_logged(logs)
        assert {line["entry_id"] for line in lines} == set(ids)
        for line in lines:
            name, fields, kind = ids[line["entry_id"]]
            assert set(line) == _DROP_LINE, name
            assert line["kind"] == kind, name
            assert line["stage"] == "parse", name
            raw = fields.get(b"data")
            if raw is None:
                assert line["data_fingerprint"] is None, name
                assert line["data_bytes"] is None, name
            else:
                assert line["data_fingerprint"] == stream_fingerprint(raw), name
                assert line["data_bytes"] == len(raw), name
        # The pair: the rows did carry it, but the two with nothing in.
        empty = {n for n, f, _k in _GRID
                 if _S.encode() not in b"".join([*f, *f.values()])}
        assert empty == {"request_data_empty", "request_data_empty_object"}


class TestTheFingerprint:
    async def test_it_is_the_digest_of_the_bytes_as_sent(
        self, redis: fakeaioredis.FakeRedis, stream: str,
    ) -> None:
        """Spacing that a re-serialization would change: the digest is
        of the bytes, the one a rejection at intake records."""
        raw = b'{ "v" : 1 ,  "type":"t" }'
        await redis.xadd(stream, _entry(b"notification_request", raw))
        with capture_logs() as logs:
            (line,) = await _consume(redis, stream, logs,
                                     said="event_dropped", count=1)
        assert line["data_fingerprint"] == stream_fingerprint(raw)
        assert line["data_fingerprint"] != stream_fingerprint(
            json.dumps(json.loads(raw)).encode(),
        )


# -----------------------------------------------------------------------------
# 3 -- a failure of comms: held, its line without the letter
# -----------------------------------------------------------------------------


class TestEveryHeldPath:
    async def _held(
        self, redis: fakeaioredis.FakeRedis, stream: str, request: dict[str, Any],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        await redis.xadd(stream, {
            "event": "notification_request", "data": json.dumps(request),
        })
        with capture_logs() as logs:
            (line,) = await _consume(redis, stream, logs,
                                     said="event_held", count=1, pending=1)
        _assert_no_sentinel_logged(logs)
        assert _lines(logs, "event_dropped") == []
        return line, logs

    async def test_the_exhausted_retries(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An OperationalError's text is its SQL and bound parameters --
        on a job's insert, the letter."""
        async def failing(*_args: Any, **_kwargs: Any) -> Any:
            raise OperationalError(
                "INSERT INTO notifications (title) VALUES ($1)",
                (f"title {_S}",),
                Exception(f"connection lost {_S}"),
            )

        monkeypatch.setattr(consumer_module, "handle_event", failing)
        line, _ = await self._held(redis, stream, _letter_request())
        assert line["error"].startswith("sqlalchemy.exc.OperationalError at app/")
        assert line["attempts"] == 3
        assert line["kind"] == "notification_request"

    async def test_the_exhausted_retries_on_a_real_transient_error(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The pair of test_the_exhausted_retries on a REAL failure (TX-1):
        its OperationalError is built by hand and asyncpg never raises
        one. A 40001 from Postgres, whose bound parameter carries the
        letter: retried, then held -- the class and the place, never the
        letter."""
        real = await real_serialization_failure(label=f"title {_S}")
        # Until DB-1 the letter stood in the error's text ([parameters]) and
        # this instrument asserted it there; the engine now hides bound
        # parameters, and what remains to hold is that nothing of it
        # reaches the log either way.
        assert _S not in str(real)
        assert "[SQL parameters hidden" in str(real)

        async def failing(*_args: Any, **_kwargs: Any) -> Any:
            raise real

        monkeypatch.setattr(consumer_module, "handle_event", failing)
        line, _ = await self._held(redis, stream, _letter_request())
        assert line["error"].startswith("sqlalchemy.exc.DBAPIError at app/")

    async def test_the_unexpected_exception_mocked(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def failing(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError(f"cannot render {_S}")

        monkeypatch.setattr(consumer_module, "handle_event", failing)
        line, _ = await self._held(redis, stream, _letter_request())
        assert line["error"].startswith(
            "builtins.RuntimeError at app/transport/consumer.py:",
        )
        assert line["attempts"] == 1

    async def test_the_unexpected_exception_from_the_database(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A database error at the INSERT of the job, whose text quotes
        the bound parameters -- the letter. MOCKED, and that is the
        honest way now.

        It ran without a mock while comms let through a value the
        database or the driver would not take: a NUL in the title (until
        EM-1), a lone surrogate (until the EM-1 FIX), then a NaN and a
        date outside a timestamp's range (until FIX-2, which refuses
        every value of that class at the intake, under the key). No input
        reaches the INSERT and fails there any more -- that is the gain,
        and the path a database error takes is the same whatever raised
        it: the flush raises a DataError carrying the letter, as the
        driver's own did."""
        request = _letter_request()
        real_flush = AsyncSession.flush

        async def failing_flush(
            self: AsyncSession, *args: Any, **kw: Any,
        ) -> None:
            if any(isinstance(o, Notification) for o in self.new):
                raise DataError(
                    "INSERT INTO notifications (title, action_data) "
                    "VALUES ($1, $2)",
                    (f"title {_S}", f"{{\"p\": \"{_S}\"}}"),
                    Exception(f"invalid input for query argument {_S}"),
                )
            await real_flush(self, *args, **kw)

        monkeypatch.setattr(AsyncSession, "flush", failing_flush)
        # A request comms ACCEPTS, up to the INSERT -- `email` out too:
        # unit_event has no email channel, so the block would be refused.
        for name in ("llm", "llm_audio", "titel", "email"):
            del request[name]
        line, _ = await self._held(redis, stream, request)
        # The mocked error, not some other one.
        assert line["error"].startswith("sqlalchemy.exc.DataError")
        assert " at app/" in line["error"]


# -----------------------------------------------------------------------------
# 4 -- malformed events: dropped by the parse, the next one processed
# -----------------------------------------------------------------------------


class TestUnstorable:
    """A string the database will not take -- a NUL, a lone surrogate --
    in an event other than the request (EM-1 FIX): a malformation of the
    event, dropped at once by the parse -- no attempt at the database, no
    retries. Until the FIX such an event reached its write and failed
    there: a database error, the retries, "unexpected"."""

    @pytest.mark.parametrize("bad", ["\x00", "\ud800"], ids=["nul", "surrogate"])
    @pytest.mark.parametrize(("event", "data"), [
        ("user_upserted", {
            "v": 1, "recipient_id": str(uuid4()), "version": 1,
            "telegram_id": None, "email": None, "locale": "en{bad}",
            "timezone": None, "active": True,
        }),
        ("user_deleted", {"v": 1, "recipient_id": "{bad}", "version": 2}),
        ("group_changed", {
            "v": 1, "group_key": "g{bad}", "recipient_id": str(uuid4()),
            "member": True,
        }),
        ("section_membership_changed", {
            "v": 1, "section_key": "support", "section_label": "S{bad}",
            "operator_id": str(uuid4()), "member": True,
        }),
        ("reminder_cancel", {
            "v": 1, "types": ["unit_rem_1h"], "correlation": "c{bad}",
        }),
    ])
    async def test_dropped_at_once(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
        bad: str,
        event: str,
        data: dict[str, Any],
    ) -> None:
        payload = {
            key: value.replace("{bad}", bad) if isinstance(value, str) else value
            for key, value in data.items()
        }
        await redis.xadd(stream, {"event": event, "data": json.dumps(payload)})
        with capture_logs() as logs:
            (line,) = await _consume(redis, stream, logs,
                                     said="event_dropped", count=1)
        assert (line["kind"], line["stage"]) == (event, "parse")
        assert line["reason"].startswith(
            "app.core.exceptions.ValidationError at app/transport/events.py:",
        )
        assert _lines(logs, "event_retry_scheduled") == []


class TestPoisonPill:
    """A bad entry costs one line, never the consumer (EM-1 FIX-2): the
    next entry is processed by the same process, nothing stays pending.
    Before, a nested bomb raised a RecursionError out of the parse --
    only a ValidationError was caught there -- and the consumer went
    down, and down again on the same pending entry after a restart
    (reproduced on a real Redis: the FIX-2 report)."""

    _NEXT = {
        "event": "reminder_cancel",
        "data": json.dumps({
            "v": 1, "types": ["unit_rem_1h"], "correlation": "next-entry",
        }),
    }

    async def _then_the_next(
        self, redis: fakeaioredis.FakeRedis, stream: str, entry: dict[str, str],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        await redis.xadd(stream, entry)
        await redis.xadd(stream, self._NEXT)
        with capture_logs() as logs:
            await _consume(redis, stream, logs, said="event_processed", count=1)
        (line,) = _lines(logs, "event_dropped")
        processed = [e for e in _lines(logs, "event_processed")
                     if e.get("event_type") == "ReminderCancel"]
        assert len(processed) == 1  # the next entry, the same process
        return line, logs

    @pytest.mark.parametrize("depth", [500, 200_000])
    async def test_a_nested_bomb(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
        depth: int,
    ) -> None:
        data = (
            '{"v": 1, "recipient_id": ' + "[" * depth + "]" * depth
            + ', "version": 2}'
        )
        line, _ = await self._then_the_next(
            redis, stream, {"event": "user_deleted", "data": data},
        )
        assert (line["stage"], line["log_level"]) == ("parse", "warning")
        assert line["data_bytes"] == len(data)

    async def test_any_exception_of_the_parse(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Whatever the parse raises -- a RecursionError nothing saw
        coming, here -- the entry is dropped at ERROR with the class and
        the place, is acknowledged, and the line carries no traceback and
        no text of it."""
        real = consumer_module.parse_event
        calls = 0

        def parse(fields: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RecursionError(f"bomb {_S}")
            return real(fields)

        monkeypatch.setattr(consumer_module, "parse_event", parse)
        line, logs = await self._then_the_next(redis, stream, {
            "event": "user_deleted",
            "data": json.dumps({"v": 1, "recipient_id": str(uuid4()),
                                "version": 2}),
        })
        assert line["reason"].startswith("builtins.RecursionError")
        assert (line["stage"], line["log_level"]) == ("parse", "error")
        _assert_no_sentinel_logged(logs)
        assert "exception" not in line  # class and place, no traceback

    async def test_no_storable_number(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
    ) -> None:
        """Infinity in an address-book event: dropped by the parse."""
        data = json.dumps({
            "v": 1, "recipient_id": str(uuid4()), "version": float("inf"),
        })
        line, _ = await self._then_the_next(
            redis, stream, {"event": "user_deleted", "data": data},
        )
        assert (line["kind"], line["stage"]) == ("user_deleted", "parse")


class TestNotUtf8:
    """Until G4 a byte that is not UTF-8 in a field of the entry ended
    the consumer's process, and the restart replayed the same entry from
    the pending list, forever. Now it is a broken frame like any other."""

    @pytest.mark.parametrize(
        "fields",
        [
            {b"event": b"user_upserted", b"data": b'{"v": 1, "x": "\xff"}'},
            {b"event": b"user_\xffupserted", b"data": b'{"v": 1}'},
            {b"event": b"user_upserted", b"data": b'{"v": 1}', b"no\xfete": b"x"},
        ],
        ids=["in_data", "in_event", "in_a_field_name"],
    )
    async def test_dropped_and_the_next_is_processed(
        self,
        redis: fakeaioredis.FakeRedis,
        stream: str,
        db_session: AsyncSession,
        fields: dict[bytes, bytes],
    ) -> None:
        await redis.xadd(stream, fields)
        rid = uuid4()
        await redis.xadd(stream, {
            "event": "user_upserted",
            "data": json.dumps(_user_upserted(recipient_id=str(rid))),
        })
        with capture_logs() as logs:

            async def both_done() -> bool:
                db_session.expire_all()
                return (
                    len(_lines(logs, "event_dropped")) == 1
                    and await db_session.get(Recipient, rid) is not None
                    and await _pending(redis, stream) == 0
                )

            await _run_until(StreamConsumer(redis), both_done)
        (line,) = _lines(logs, "event_dropped")
        assert line["stage"] == "parse"
        assert line["reason"].startswith(
            "app.core.exceptions.ValidationError at app/transport/events.py:",
        )
        assert line["data_fingerprint"] == stream_fingerprint(fields[b"data"])


# -----------------------------------------------------------------------------
# 5 -- the contract
# -----------------------------------------------------------------------------


class TestTheContract:
    def test_the_split_of_a_request_is_section_7s(self) -> None:
        """The "envelope / letter" column of the notification_request
        table against the code's three sets, both ways.

        The count was 13 until EM-1, and right; EM-1 added `email` to
        the letter, so the request has 14 fields now."""
        start = _DOC.index("### `notification_request` -- a job")
        table = _DOC[start:_DOC.index("\n### ", start + 1)]
        column: dict[str, set[str]] = {}
        for match in re.finditer(
            r"^\| `([a-z_]+)` \| [^|]+ \| (frame|envelope|letter) \|",
            table, re.M,
        ):
            column.setdefault(match.group(2), set()).add(match.group(1))
        assert column == {
            "frame": set(NOTIFICATION_REQUEST_FRAME_FIELDS),
            "envelope": set(NOTIFICATION_REQUEST_ENVELOPE_FIELDS),
            "letter": set(NOTIFICATION_REQUEST_LETTER_FIELDS),
        }
        # The pair: the table was read, every field of the request is in it.
        assert sum(map(len, column.values())) == len(
            EVENT_FIELDS["notification_request"]
        ) == 14

    async def test_the_line_of_a_drop_is_section_7s(
        self, redis: fakeaioredis.FakeRedis, stream: str,
    ) -> None:
        """What section 7 says a drop leaves -- its line's name, its
        stages and its fields by name -- against the line the loop writes:
        every field of the line the product can use is named there."""
        start = _DOC.index('**What "dropped" is.**')
        paragraph = _DOC[start:_DOC.index("\n\n", start)]
        named = set(re.findall(r"`([a-z_]+)`", paragraph))
        assert {"event_dropped", "parse", "handle", "retries", "unknown",
                "data_fingerprint", "data_bytes"} <= named
        await redis.xadd(stream, _entry(b"user_deleted", b"{}"))
        with capture_logs() as logs:
            (line,) = await _consume(redis, stream, logs,
                                     said="event_dropped", count=1)
        assert set(line) == _DROP_LINE
        assert line["event"] in named
        assert {"data_fingerprint", "data_bytes"} <= set(line) & named
