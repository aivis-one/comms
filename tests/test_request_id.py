# =============================================================================
# W3-2 -- the request id (protocol 9.6): HTTP, the stream, the contract
# =============================================================================
#
# Item 3: X-Request-ID kept when it is of the form, else comms' own; of
# several the last, judged alone; the response carries it -- the 401 of
# the token and the 413 of the body too -- and so does every line of the
# request, read on the rendered line (machine_lines: the real chain with
# MachineRenderer, into a buffer). The requests are driven as raw ASGI
# calls: a header with a line break never leaves an HTTP client.
#
# Item 4: the request_id of a stream entry names the lines of its
# processing; absent or of another form -- not UTF-8 either -- it is
# comms' own, and the event is processed as without it: no refusal, no
# drop; the next entry starts from its own.
#
# The contract: deploy/INTEGRATION.md section 5 and section 7 name the
# header, the envelope field and the form the code has -- both ways.
#
# MUTATIONS (one run, each red here):
#   R1 the first header instead of the last      -> test_of_several_the_last
#   R2 match / $ instead of fullmatch            -> test_a_malformed_id_is_
#                                                   replaced (the line break)
#   R3 RequestId added before RequireServiceToken -> test_the_401_carries_it,
#                                                   test_the_outermost_layer
#   R4 the id not set back after the request     -> test_a_line_after_the_
#                                                   request_names_none
#   R5 a malformed request_id refuses the event  -> TestTheStream
#   R6 _decode_entry checks request_id as UTF-8  -> TestTheStream (not UTF-8)
#   R7 an absent id takes the previous entry's   -> test_the_next_entry_
#                                                   starts_from_its_own
#   R8 the header renamed in the contract        -> TestTheContract
# NOT KILLABLE, not built: the order against LimitRequestBody seen from
# outside -- its 413 is raised inside ExceptionMiddleware, under any user
# layer (REV-3), so it is held by test_the_outermost_layer alone; a
# request_id written twice in one stream entry -- redis-py hands the entry
# over as a dict, the last value, and comms never sees the first.
# =============================================================================

import asyncio
import io
import json
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Generator
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
import structlog
from fakeredis import aioredis as fakeaioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.body_limit import LimitRequestBody
from app.api.errors import UnhandledErrorMiddleware
from app.api.request_id import REQUEST_ID_HEADER, RequestId
from app.api.service_auth import RequireServiceToken
from app.audience.models import Recipient
from app.core.config import settings
from app.core.constants import MAX_REQUEST_BODY_BYTES
from app.core.logging import (
    RELEASE_VARIABLE,
    REQUEST_ID_FORM,
    MachineRenderer,
    SanitizingFormatter,
    processors,
)
from app.main import app
from app.transport.consumer import StreamConsumer, entry_request_id
from app.transport.events import ENVELOPE_REQUEST_ID_FIELD
from tests.helpers import next_phase3c_telegram_id, next_snapshot_version

_TOKEN = "w32-" + "not-a-secret-token"
_MINTED = re.compile(r"[0-9a-f]{32}")
_DOC = (
    Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md"
).read_text(encoding="utf-8")


@pytest.fixture
def machine_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[Callable[[], list[dict[str, Any]]], None, None]:
    """structlog through the real chain and MachineRenderer, into a
    buffer; the configuration before it is put back after the test."""
    monkeypatch.setenv(RELEASE_VARIABLE, "w3-2-probe")
    stream = io.StringIO()
    saved = structlog.get_config()
    structlog.configure(
        processors=processors(MachineRenderer()),
        context_class=dict,
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        logger_factory=structlog.PrintLoggerFactory(file=stream),
        cache_logger_on_first_use=False,
    )

    def read() -> list[dict[str, Any]]:
        return [
            json.loads(line) for line in stream.getvalue().splitlines()
            if line.strip()
        ]

    try:
        yield read
    finally:
        structlog.configure(**saved)


@pytest.fixture
def token_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "comms_service_token", _TOKEN)


async def _call(
    application: Any, headers: list[tuple[bytes, bytes]],
    method: str = "GET", path: str = "/api/v1/inbox", body: bytes = b"",
) -> tuple[int, list[tuple[bytes, bytes]]]:
    """One raw ASGI request: its status and response headers."""
    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": method, "scheme": "http", "path": path, "raw_path": path.encode(),
        "root_path": "", "query_string": b"", "headers": headers,
        "client": ("127.0.0.1", 5000), "server": ("test", 80),
    }
    sent = False
    started: dict[str, Any] = {}

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if sent:
            await asyncio.sleep(3600)
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            started.update(message)

    await application(scope, receive, send)
    return started["status"], list(started.get("headers", []))


def _answered_id(headers: list[tuple[bytes, bytes]]) -> str:
    (value,) = [v for k, v in headers if k == REQUEST_ID_HEADER.lower().encode()]
    return value.decode("ascii")


def _ids_of(lines: list[dict[str, Any]], event: str) -> list[str | None]:
    found = [line["request_id"] for line in lines if line["event"] == event]
    assert found, f"the pair: {event} was written"
    return found


class TestTheHeader:
    """done-when 3. R1-R4."""

    async def test_the_401_carries_it(
        self, token_set: None, machine_lines: Callable[[], list[dict[str, Any]]],
    ) -> None:
        """R3: a 401 RequireServiceToken answers carries the id only
        from a layer outside it -- and its line names it."""
        status, headers = await _call(app, [(b"x-request-id", b"Ab-1.2_z")])
        assert status == 401
        assert _answered_id(headers) == "Ab-1.2_z"
        assert _ids_of(machine_lines(), "service_auth_failed") == ["Ab-1.2_z"]

    @pytest.mark.parametrize("given", [
        None, b"", b"a" * 129, b"a b", b"abc\n", b"abc\r\nX: y",
        "\u0430\u0431\u0432".encode(), b"a/b", b"\xff",
    ])
    async def test_a_malformed_id_is_replaced(
        self, token_set: None, given: bytes | None,
        machine_lines: Callable[[], list[dict[str, Any]]],
    ) -> None:
        """R2: absent, empty, 129 characters, a space, a line break --
        comms' own id, of the form, in the answer and in the line."""
        headers = [] if given is None else [(b"x-request-id", given)]
        _, answered = await _call(app, headers)
        minted = _answered_id(answered)
        assert _MINTED.fullmatch(minted)
        assert given is None or minted.encode() != given
        assert _ids_of(machine_lines(), "service_auth_failed") == [minted]

    async def test_the_longest_is_kept(self, token_set: None) -> None:
        """The pair of 129: 128 characters is of the form."""
        _, answered = await _call(app, [(b"x-request-id", b"a" * 128)])
        assert _answered_id(answered) == "a" * 128

    @pytest.mark.parametrize(("given", "kept"), [
        ([b"first-1", b"last-2"], "last-2"),
        ([b"first-1", b"bad id"], None),
        ([b"bad id", b"last-2"], "last-2"),
    ])
    async def test_of_several_the_last(
        self, token_set: None, given: list[bytes], kept: str | None,
    ) -> None:
        """R1: the last, judged alone -- a malformed last one is
        replaced, never the first used instead."""
        _, answered = await _call(app, [(b"x-request-id", v) for v in given])
        answer = _answered_id(answered)
        if kept is None:
            assert _MINTED.fullmatch(answer) and answer != "first-1"
        else:
            assert answer == kept

    async def test_the_413_carries_it(self, token_set: None) -> None:
        status, headers = await _call(
            app,
            [
                (b"x-request-id", b"big-1"),
                (b"authorization", f"Bearer {_TOKEN}".encode()),
                (b"content-type", b"application/json"),
                (b"content-length", str(MAX_REQUEST_BODY_BYTES + 1).encode()),
            ],
            method="POST", path="/api/v1/threads", body=b"{}",
        )
        assert status == 413
        assert _answered_id(headers) == "big-1"

    def test_the_outermost_layer(self) -> None:
        """R3: outside the token, the body limit and the 500 of comms;
        Starlette's ServerErrorMiddleware alone outside it."""
        layers = [m.cls for m in app.user_middleware]
        assert layers[0] is RequestId
        for inner in (RequireServiceToken, LimitRequestBody, UnhandledErrorMiddleware):
            assert layers.index(inner) > 0, layers

    async def test_a_line_after_the_request_names_none(
        self, token_set: None, machine_lines: Callable[[], list[dict[str, Any]]],
    ) -> None:
        """R4: the id is the request's, not the caller's after it."""
        await _call(app, [(b"x-request-id", b"before-1")])
        structlog.get_logger().info("w32_after_the_request")
        assert _ids_of(machine_lines(), "service_auth_failed") == ["before-1"]
        assert _ids_of(machine_lines(), "w32_after_the_request") == [None]

    async def test_a_standard_library_line_of_the_request(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """uvicorn writes its access line from inside the application's
        call (its send): a stdlib line inside the request names it."""
        monkeypatch.setenv(RELEASE_VARIABLE, "w3-2-probe")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(SanitizingFormatter())
        probe = logging.getLogger("w32.probe")
        probe.addHandler(handler)
        probe.propagate = False

        async def inner(scope: Any, receive: Any, send: Any) -> None:
            probe.warning("inside")
            await send({"type": "http.response.start", "status": 204, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        try:
            _, headers = await _call(RequestId(inner), [(b"x-request-id", b"std-1")])
            probe.warning("outside")
        finally:
            probe.removeHandler(handler)
            probe.propagate = True
        lines = [json.loads(line) for line in stream.getvalue().splitlines()]
        assert [(x["msg"], x["request_id"]) for x in lines] == [
            ("inside", "std-1"), ("outside", None),
        ]
        assert _answered_id(headers) == "std-1"


# -----------------------------------------------------------------------------
# Item 4 -- the stream
# -----------------------------------------------------------------------------


@pytest.fixture
def redis() -> fakeaioredis.FakeRedis:
    return fakeaioredis.FakeRedis()


@pytest.fixture
def stream(monkeypatch: pytest.MonkeyPatch) -> str:
    name = f"comms:test:{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", name)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    return name


def _upserted(recipient_id: UUID) -> str:
    return json.dumps({
        "v": 1, "recipient_id": str(recipient_id),
        "version": next_snapshot_version(),
        "telegram_id": next_phase3c_telegram_id(), "email": None,
        "locale": "en", "timezone": "Europe/Berlin", "active": True,
    })


async def _until_acked(
    consumer: StreamConsumer, redis: fakeaioredis.FakeRedis, stream: str,
    count: int,
) -> None:
    """Run the loop until `count` entries were read and none is pending."""
    async def done() -> bool:
        groups = await redis.xinfo_groups(stream)
        return bool(groups) and groups[0]["pending"] == 0 and (
            groups[0].get("entries-read") or 0) >= count

    await _run(consumer, done)


async def _run(
    consumer: StreamConsumer, condition: Callable[[], Awaitable[bool]],
) -> None:
    task = asyncio.ensure_future(consumer.run())
    try:
        deadline = asyncio.get_event_loop().time() + 5.0
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


class TestTheStream:
    """done-when 4. R5-R7."""

    async def test_a_good_id_names_the_entry_s_lines(
        self, redis: fakeaioredis.FakeRedis, stream: str,
        machine_lines: Callable[[], list[dict[str, Any]]],
    ) -> None:
        await redis.xadd(stream, {
            "event": "user_upserted", "data": _upserted(uuid.uuid4()),
            ENVELOPE_REQUEST_ID_FIELD: "env-1",
        })
        await _until_acked(StreamConsumer(redis), redis, stream, 1)
        assert _ids_of(machine_lines(), "event_processed") == ["env-1"]
        # The pair: a line of the loop, outside the entry, names none.
        assert _ids_of(machine_lines(), "consumer_started") == [None]

    @pytest.mark.parametrize("given", [
        None, "", "a b", "x" * 129, "line\nbreak", b"\xff\xfe",
    ])
    async def test_a_malformed_id_refuses_nothing(
        self, redis: fakeaioredis.FakeRedis, stream: str,
        db_session: AsyncSession, given: str | bytes | None,
        machine_lines: Callable[[], list[dict[str, Any]]],
    ) -> None:
        """R5, R6: comms' own id; the event processed as without the
        field -- the row is there, nothing dropped."""
        recipient_id = uuid.uuid4()
        fields: dict[Any, Any] = {
            "event": "user_upserted", "data": _upserted(recipient_id),
        }
        if given is not None:
            fields[ENVELOPE_REQUEST_ID_FIELD] = given
        await redis.xadd(stream, fields)
        await _until_acked(StreamConsumer(redis), redis, stream, 1)
        (minted,) = _ids_of(machine_lines(), "event_processed")
        assert minted is not None and _MINTED.fullmatch(minted)
        assert "event_dropped" not in {e.get("event") for e in machine_lines()}
        row = (await db_session.execute(
            select(Recipient).where(Recipient.id == recipient_id)
        )).scalar_one_or_none()
        assert row is not None

    async def test_the_next_entry_starts_from_its_own(
        self, redis: fakeaioredis.FakeRedis, stream: str,
        machine_lines: Callable[[], list[dict[str, Any]]],
    ) -> None:
        """R7."""
        await redis.xadd(stream, {
            "event": "user_upserted", "data": _upserted(uuid.uuid4()),
            ENVELOPE_REQUEST_ID_FIELD: "env-A",
        })
        await redis.xadd(stream, {
            "event": "user_upserted", "data": _upserted(uuid.uuid4()),
        })
        await _until_acked(StreamConsumer(redis), redis, stream, 2)
        first, second = _ids_of(machine_lines(), "event_processed")
        assert first == "env-A"
        assert second is not None and _MINTED.fullmatch(second)

    async def test_a_dropped_entry_s_line_names_it(
        self, redis: fakeaioredis.FakeRedis, stream: str,
        machine_lines: Callable[[], list[dict[str, Any]]],
    ) -> None:
        """An event refused for itself: its line carries its id (until
        W5-1 the line of its dead letter)."""
        await redis.xadd(stream, {
            "event": "user_deleted", "data": json.dumps({"v": 1}),
            ENVELOPE_REQUEST_ID_FIELD: "env-dead",
        })
        await _until_acked(StreamConsumer(redis), redis, stream, 1)
        assert _ids_of(machine_lines(), "event_dropped") == ["env-dead"]

    async def test_a_held_entry_s_line_names_it(
        self, redis: fakeaioredis.FakeRedis, stream: str,
        monkeypatch: pytest.MonkeyPatch,
        machine_lines: Callable[[], list[dict[str, Any]]],
    ) -> None:
        """A failure of comms on the event: the line of its hold carries
        its id too (W5-1)."""
        from app.transport import consumer as consumer_module

        async def failing(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("a defect of comms")

        monkeypatch.setattr(consumer_module, "handle_event", failing)
        await redis.xadd(stream, {
            "event": "user_upserted", "data": _upserted(uuid.uuid4()),
            ENVELOPE_REQUEST_ID_FIELD: "env-held",
        })

        async def held() -> bool:
            return "event_held" in {e.get("event") for e in machine_lines()}

        await _run(StreamConsumer(redis), held)
        assert _ids_of(machine_lines(), "event_held") == ["env-held"]

    @pytest.mark.parametrize(("fields", "kept"), [
        ({b"request_id": b"r.1-_Z"}, "r.1-_Z"),
        ({"request_id": "r.1-_Z"}, "r.1-_Z"),
        ({b"request_id": b"\xff"}, None),
        ({b"other": b"r-1"}, None),
    ])
    def test_entry_request_id(self, fields: dict[Any, Any], kept: str | None) -> None:
        found = entry_request_id(fields)
        assert found == kept if kept is not None else _MINTED.fullmatch(found)


# -----------------------------------------------------------------------------
# The contract
# -----------------------------------------------------------------------------


def _section(start: str, end: str) -> str:
    begin = _DOC.index(start)
    return " ".join(_DOC[begin:_DOC.index(end, begin)].split())


class TestTheContract:
    """deploy/INTEGRATION.md holds the header, the field and the form the
    code has; the code holds what the paragraphs say. R8."""

    _HTTP = _section("**The request id.**", "**A write is answered")
    _FRAME = _section("### The frame", "### `notification_request`")

    def test_the_header_and_its_form(self) -> None:
        assert f"`{REQUEST_ID_HEADER}`" in self._HTTP
        assert f"`{REQUEST_ID_FORM.pattern}`" in self._HTTP
        assert "the last is taken and judged alone" in self._HTTP
        assert "`401`" in self._HTTP and "`413`" in self._HTTP

    def test_the_envelope_field_and_its_form(self) -> None:
        assert f"optionally, `{ENVELOPE_REQUEST_ID_FIELD}`" in self._FRAME
        assert f"`{REQUEST_ID_FORM.pattern}`" in self._FRAME
        assert "never refused, and never dropped" in self._FRAME

    def test_the_doc_s_form_is_the_code_s(self) -> None:
        """The other way: every form either paragraph writes is the
        code's one."""
        written = set(re.findall(r"`(\[[^`]*\]\{\d+,\d+\})`", self._HTTP + self._FRAME))
        assert written == {REQUEST_ID_FORM.pattern}
