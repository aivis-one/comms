# =============================================================================
# COMMS Service -- a dead letter keeps the address, never the letter (G4)
# =============================================================================
#
# The DLQ is a stream in the shared redis with no expiry, and nothing
# replays it (app/transport/consumer.py, DEAD LETTERS). What it keeps is
# held here, against the code and against the contract:
#
#   1  the grid of states (event x data x extra fields): a sentinel in
#      every field of the letter, in llm_audio, in an unknown key of
#      data, in an extra field of the entry, in a data that is not JSON
#      -- in no value of the dead letter; the pair: the event, the
#      envelope, the fingerprint, the attempts, the source entry id are
#      there and not empty (TestTheGrid)
#   2  the fingerprint is stream_fingerprint of the same data bytes
#      (TestTheFingerprint)
#   3  every path to the DLQ, on the real loop, with a letter: the parse
#      (key unreadable), the exhausted retries, the unexpected exception
#      -- the last two also through the reason: the class and the place,
#      never the text (TestEveryPath)
#   4  an entry that is not UTF-8 is dead-lettered, acknowledged, and
#      the next entry is processed by the same loop (TestNotUtf8)
#   5  the contract: the split of notification_request in section 7
#      equals the code's three sets, both ways; the dead letter's fields
#      and DLQ_MAXLEN's default in "How long comms keeps things" equal
#      the code's, both ways (TestTheContract)
#
# Mutations (built once, by hand, each failing the named class):
#   M1  data verbatim (the old _to_dlq)              -> TestTheGrid
#   M2  kept = the whole field set of the request    -> TestTheGrid
#   M3  data as it is for the events without letter  -> TestTheGrid
#   M4  extra fields of the entry kept               -> TestTheGrid
#   M5  fingerprint of the re-serialized data        -> TestTheFingerprint
#   M6  _dlq_dropped always []                       -> TestTheGrid
#   M7  a data that is not an object into _dlq_data  -> TestTheGrid
#   M8  the unexpected branch says str(exc) again    -> TestEveryPath
#   M9  the retries branch says {exc} again          -> TestEveryPath
#   M10 a name of DLQ_FIELDS renamed                 -> TestTheContract
#   M11 dlq_maxlen's default changed                 -> TestTheContract
#   M15 the UTF-8 refusal taken out of parse_event   -> TestNotUtf8
#   M16 nothing withheld from the envelope           -> TestTheGrid
# (M12-M14 are the template's: tests/test_env_template.py.)
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
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

import app.transport.consumer as consumer_module
from app.audience.models import Recipient
from app.core.config import Settings, settings
from app.engine.service import stream_fingerprint
from app.transport.consumer import (
    _DLQ_WITHHELD_ENVELOPE_FIELDS,
    DLQ_FIELDS,
    StreamConsumer,
    dead_letter_fields,
)
from app.transport.events import (
    EVENT_FIELDS,
    NOTIFICATION_REQUEST_ENVELOPE_FIELDS,
    NOTIFICATION_REQUEST_FRAME_FIELDS,
    NOTIFICATION_REQUEST_LETTER_FIELDS,
)
from tests.helpers import next_phase3c_telegram_id, next_snapshot_version

_REPO = Path(__file__).resolve().parents[1]
_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")
_WAIT_TIMEOUT = 5.0

# Not a substring of anything comms writes on its own: a uuid in hex.
_S = "sentinel" + uuid4().hex


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


def _dead(fields: dict[Any, Any]) -> dict[str, str]:
    return dead_letter_fields(
        fields, reason="why", attempts=2, entry_id="1-0",
    )


def _assert_no_sentinel(dead: dict[str, str]) -> None:
    for name, value in dead.items():
        assert _S not in value, name
        assert _S not in name


def _assert_the_pair(dead: dict[str, str]) -> None:
    """What the investigation needs is there and not empty."""
    assert set(dead) <= set(DLQ_FIELDS)
    for name in ("_dlq_error", "_dlq_attempts", "_dlq_source_entry_id",
                 "_dlq_dropped"):
        assert dead[name], name
    # Every value can go to redis as it is.
    for value in dead.values():
        value.encode("utf-8")


# -----------------------------------------------------------------------------
# 1 -- the grid
# -----------------------------------------------------------------------------


_REQUEST = json.dumps(_letter_request()).encode()
_KEPT_REQUEST = sorted(
    NOTIFICATION_REQUEST_FRAME_FIELDS
    | NOTIFICATION_REQUEST_ENVELOPE_FIELDS - _DLQ_WITHHELD_ENVELOPE_FIELDS
)
_UPSERT = json.dumps(_user_upserted(title=f"{_S}")).encode()

# (id, entry, the event kept, the keys of _dlq_data or None, dropped)
_GRID: list[tuple[str, dict[bytes, bytes], str | None, list[str] | None, list[str]]] = [
    (
        "request_object_extra_field",
        _entry(b"notification_request", _REQUEST, note=_S.encode()),
        "notification_request", _KEPT_REQUEST,
        ["data.action_data", "data.body", "data.email", "data.llm",
         "data.llm_audio", "data.title", "data.titel", "note"],
    ),
    (
        "request_without_key",
        _entry(b"notification_request", json.dumps(
            {k: v for k, v in _letter_request().items()
             if k != "idempotency_key"},
        ).encode()),
        "notification_request",
        [k for k in _KEPT_REQUEST if k != "idempotency_key"],
        ["data.action_data", "data.body", "data.email", "data.llm",
         "data.llm_audio", "data.title", "data.titel"],
    ),
    (
        "address_book_event_unknown_key",
        _entry(b"user_upserted", _UPSERT),
        "user_upserted", sorted(EVENT_FIELDS["user_upserted"]), ["data.title"],
    ),
    (
        "unknown_event_object",
        _entry(b"user_renamed", _REQUEST),
        "user_renamed", None, ["data"],
    ),
    (
        "empty_event_object",
        _entry(b"", _REQUEST),
        "", None, ["data"],
    ),
    (
        "no_event_object",
        _entry(None, _REQUEST),
        None, None, ["data"],
    ),
    (
        "request_data_not_json",
        _entry(b"notification_request", f'{{"title": "{_S}"'.encode()),
        "notification_request", None, ["data"],
    ),
    (
        "request_data_empty",
        _entry(b"notification_request", b""),
        "notification_request", None, ["data"],
    ),
    (
        "request_data_json_list",
        _entry(b"notification_request", json.dumps([_S]).encode()),
        "notification_request", None, ["data"],
    ),
    (
        "request_data_json_string",
        _entry(b"notification_request", json.dumps(_S).encode()),
        "notification_request", None, ["data"],
    ),
    (
        "request_data_absent_extra_field",
        _entry(b"notification_request", None, note=_S.encode()),
        "notification_request", None, ["note"],
    ),
    (
        "request_data_empty_object",
        _entry(b"notification_request", b"{}"),
        "notification_request", [], [],
    ),
    (
        # json.loads keeps the LAST of a repeated key: both are dropped
        # with their name, neither value is kept.
        "request_repeated_key",
        _entry(b"notification_request",
               f'{{"type": "t", "title": "{_S}a", "title": "{_S}b"}}'.encode()),
        "notification_request", ["type"], ["data.title"],
    ),
    (
        "data_not_utf8",
        _entry(b"notification_request", _S.encode() + b"\xff"),
        "notification_request", None, ["data"],
    ),
    (
        "event_not_utf8",
        _entry(b"notification_\xffrequest", _REQUEST),
        None, None, ["data", "event"],
    ),
    (
        "field_name_not_utf8",
        {b"event": b"notification_request", b"data": b"{}",
         b"no\xfete": _S.encode()},
        "notification_request", [], ["no\\xfete"],
    ),
]


class TestTheGrid:
    @pytest.mark.parametrize(
        ("fields", "event", "kept", "dropped"),
        [case[1:] for case in _GRID],
        ids=[case[0] for case in _GRID],
    )
    def test_the_address_stays_the_letter_goes(
        self,
        fields: dict[bytes, bytes],
        event: str | None,
        kept: list[str] | None,
        dropped: list[str],
    ) -> None:
        dead = _dead(fields)
        _assert_no_sentinel(dead)
        _assert_the_pair(dead)
        if event is None:
            assert "event" not in dead
        else:
            assert dead["event"] == event
        if kept is None:
            assert "_dlq_data" not in dead
        else:
            assert sorted(json.loads(dead["_dlq_data"])) == kept
        assert json.loads(dead["_dlq_dropped"]) == sorted(dropped)
        raw = fields.get(b"data")
        if raw is None:
            assert "_dlq_data_fingerprint" not in dead
            assert "_dlq_data_bytes" not in dead
        else:
            assert dead["_dlq_data_fingerprint"] == stream_fingerprint(raw)
            assert dead["_dlq_data_bytes"] == str(len(raw))

    def test_the_envelope_is_there_and_not_empty(self) -> None:
        """The pair of the first row: the values of the envelope, the
        ones the product finds its request by, are the ones it sent."""
        request = _letter_request()
        dead = _dead(_entry(b"notification_request", json.dumps(request).encode()))
        kept = json.loads(dead["_dlq_data"])
        assert kept == {k: request[k] for k in _KEPT_REQUEST}
        assert all(kept[k] not in (None, "") for k in _KEPT_REQUEST)

    def test_a_lone_surrogate_goes_to_redis(self) -> None:
        """A JSON escape of a lone surrogate in a kept field decodes to
        a str UTF-8 cannot encode; the dead letter writes it escaped."""
        data = b'{"type": "t", "correlation": "\\ud800"}'
        dead = _dead(_entry(b"notification_request", data))
        _assert_the_pair(dead)
        assert json.loads(dead["_dlq_data"])["correlation"] == "\ud800"

    def test_the_withheld_link_is_an_envelope_field(self) -> None:
        """The one exception to "the envelope stays" is a field of the
        envelope -- a name outside it would withhold nothing."""
        assert _DLQ_WITHHELD_ENVELOPE_FIELDS
        assert _DLQ_WITHHELD_ENVELOPE_FIELDS <= NOTIFICATION_REQUEST_ENVELOPE_FIELDS
        assert "llm_audio" in _DLQ_WITHHELD_ENVELOPE_FIELDS


class TestTheFingerprint:
    def test_it_is_the_digest_of_the_bytes_as_sent(self) -> None:
        """Spacing that a re-serialization would change: the digest is
        of the bytes, the one a rejection at intake records."""
        raw = b'{ "v" : 1 ,  "type":"t" }'
        dead = _dead(_entry(b"notification_request", raw))
        assert dead["_dlq_data_fingerprint"] == stream_fingerprint(raw)
        assert dead["_dlq_data_fingerprint"] != stream_fingerprint(
            json.dumps(json.loads(raw)).encode(),
        )


# -----------------------------------------------------------------------------
# 3, 4 -- the real loop
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


async def _no_pending(redis: fakeaioredis.FakeRedis, stream: str) -> bool:
    try:
        pending = await redis.xpending_range(
            stream, settings.comms_consumer_group, min="-", max="+", count=10,
        )
    except Exception:
        return False
    return not pending


async def _dead_letters(redis: fakeaioredis.FakeRedis) -> list[dict[str, str]]:
    return [
        {k.decode(): v.decode() for k, v in fields.items()}
        for _id, fields in await redis.xrange(settings.dlq_stream)
    ]


async def _consume(
    redis: fakeaioredis.FakeRedis, stream: str, count: int,
) -> list[dict[str, str]]:
    async def done() -> bool:
        length: int = await redis.xlen(settings.dlq_stream)
        return length == count and await _no_pending(redis, stream)

    await _run_until(StreamConsumer(redis), done)
    return await _dead_letters(redis)


def _assert_no_sentinel_logged(logs: list[dict[str, Any]]) -> None:
    lines = [e for e in logs if e["event"] == "event_dead_lettered"]
    assert lines and all(e["reason"] for e in lines)
    for entry in lines:
        assert _S not in str(entry["reason"])


class TestEveryPath:
    async def test_the_parse(
        self, redis: fakeaioredis.FakeRedis, stream: str, db_session: AsyncSession,
    ) -> None:
        request = _letter_request()
        del request["idempotency_key"]
        await redis.xadd(stream, {
            "event": "notification_request", "data": json.dumps(request),
            "note": _S,
        })
        with capture_logs() as logs:
            (dead,) = await _consume(redis, stream, 1)
        _assert_no_sentinel(dead)
        _assert_the_pair(dead)
        _assert_no_sentinel_logged(logs)
        assert dead["_dlq_attempts"] == "0"
        assert json.loads(dead["_dlq_data"])["type"] == "unit_event"
        assert "idempotency_key" in dead["_dlq_error"]

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
        await redis.xadd(stream, {
            "event": "notification_request",
            "data": json.dumps(_letter_request()),
        })
        with capture_logs() as logs:
            (dead,) = await _consume(redis, stream, 1)
        _assert_no_sentinel(dead)
        _assert_the_pair(dead)
        _assert_no_sentinel_logged(logs)
        assert dead["_dlq_error"].startswith(
            "retries exhausted: sqlalchemy.exc.OperationalError at app/",
        )
        assert int(dead["_dlq_attempts"]) == 3
        assert "idempotency_key" in json.loads(dead["_dlq_data"])

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
        await redis.xadd(stream, {
            "event": "notification_request",
            "data": json.dumps(_letter_request()),
        })
        with capture_logs() as logs:
            (dead,) = await _consume(redis, stream, 1)
        _assert_no_sentinel(dead)
        _assert_the_pair(dead)
        _assert_no_sentinel_logged(logs)
        assert dead["_dlq_error"].startswith(
            "unexpected: builtins.RuntimeError at app/transport/consumer.py:",
        )

    async def test_the_unexpected_exception_from_the_database(
        self, redis: fakeaioredis.FakeRedis, stream: str, db_session: AsyncSession,
    ) -> None:
        """No mock: a title the database path refuses reaches the INSERT
        of the job, and the error that comes back quotes what it was
        given -- the letter.

        Until EM-1 the title carried a NUL, which Postgres refuses at
        the INSERT; that was right while the intake let a NUL through.
        EM-1 refuses a NUL at the intake, under the key (a rejection,
        not a dead letter), so the title carries a lone surrogate
        instead: valid JSON, a Python string, and no UTF-8 for the
        driver to send -- still past every rule of the intake, still
        failing at the INSERT, still without a mock."""
        request = _letter_request(title=f"title {_S}\ud800")
        # A request comms ACCEPTS, up to the INSERT -- `email` out too:
        # unit_event has no email channel, so the block would be refused.
        for name in ("llm", "llm_audio", "titel", "email"):
            del request[name]
        await redis.xadd(stream, {
            "event": "notification_request", "data": json.dumps(request),
        })
        with capture_logs() as logs:
            (dead,) = await _consume(redis, stream, 1)
        _assert_no_sentinel(dead)
        _assert_the_pair(dead)
        _assert_no_sentinel_logged(logs)
        assert dead["_dlq_error"].startswith("unexpected: ")
        assert " at app/" in dead["_dlq_error"]


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
    async def test_dead_lettered_and_the_next_is_processed(
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

        async def both_done() -> bool:
            db_session.expire_all()
            length: int = await redis.xlen(settings.dlq_stream)
            return (
                length == 1
                and await db_session.get(Recipient, rid) is not None
                and await _no_pending(redis, stream)
            )

        await _run_until(StreamConsumer(redis), both_done)
        (dead,) = await _dead_letters(redis)
        _assert_the_pair(dead)
        assert "is not UTF-8" in dead["_dlq_error"]
        assert dead["_dlq_attempts"] == "0"
        assert dead["_dlq_data_fingerprint"] == stream_fingerprint(fields[b"data"])


# -----------------------------------------------------------------------------
# 5 -- the contract
# -----------------------------------------------------------------------------


def _section(start: str) -> str:
    begin = _DOC.index(start)
    end = _DOC.find("\n## ", begin + len(start))
    return _DOC[begin:] if end < 0 else _DOC[begin:end]


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

    def test_the_fields_of_a_dead_letter_are_the_code_s(self) -> None:
        section = _section("## How long comms keeps things")
        start = section.index("**What a dead letter holds.**")
        documented = re.findall(r"^\| `([a-z_]+)` \|", section[start:], re.M)
        assert documented == list(DLQ_FIELDS)
        assert len(documented) == 8

    def test_the_withheld_link_is_named(self) -> None:
        section = _section("## How long comms keeps things")
        row = re.search(r"^\| `_dlq_data` \|.*$", section, re.M)
        assert row is not None
        for name in _DLQ_WITHHELD_ENVELOPE_FIELDS:
            assert f"`{name}`" in row.group(0)

    def test_the_cap_and_its_default_are_the_code_s(self) -> None:
        section = " ".join(_section("## How long comms keeps things").split())
        default = Settings.model_fields["dlq_maxlen"].default
        assert default == 10000
        assert f"capped at `DLQ_MAXLEN` entries, default **{default}**" in section

    async def test_the_loop_writes_only_the_documented_fields(
        self, redis: fakeaioredis.FakeRedis, stream: str, db_session: AsyncSession,
    ) -> None:
        await redis.xadd(stream, {"event": "nope", "data": "{}", "x": "y"})
        (dead,) = await _consume(redis, stream, 1)
        assert set(dead) <= set(DLQ_FIELDS)
        assert {"event", "_dlq_error", "_dlq_dropped"} <= set(dead)
