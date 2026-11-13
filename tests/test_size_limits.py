# =============================================================================
# PERF-3 -- every input has a size limit, checked before it is read
# =============================================================================
#
# What is held here, against the code and against deploy/INTEGRATION.md:
#   2 a stream entry whose `data` is one byte over MAX_EVENT_DATA_BYTES
#     is dropped through the real consumer (until W5-1 a dead letter),
#     its line naming the size -- and the whole of it was never
#     json.loads-ed, neither by the parse nor by the drop (every
#     json.loads of the process counted by its argument's size); exactly
#     the limit is taken;
#   3 action_data one byte over MAX_ACTION_DATA_BYTES of compact UTF-8
#     JSON is refused at intake, naming the field and the limit; exactly
#     the limit is taken;
#   4 a REST body one byte over MAX_REQUEST_BODY_BYTES -- with a
#     Content-Length and streamed without one -- is a 413 `bad_request`
#     and nothing parsed it (no json.loads of it, no row); exactly the
#     limit goes on to the route as before;
#   and the three numbers in the contract are the constants.
#
# MUTATIONS (each turns one red):
#   M1 the size check taken out of parse_event -> TestTheStreamEntry
#   M2 (gone with the dead letter's reader of the data, W5-1)
#   M3 the action_data limit taken out -> TestActionData
#   M4 the body layer not mounted -> TestTheRequestBody
#   M5 the layer counting only Content-Length -> test_streamed_over
#   M7 a number of the contract changed -> TestTheContract
# =============================================================================

import json
import re
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from structlog.testing import capture_logs

from app.core.config import settings
from app.core.constants import (
    MAX_ACTION_DATA_BYTES,
    MAX_EVENT_DATA_BYTES,
    MAX_REQUEST_BODY_BYTES,
)
from app.core.database import get_session_factory
from app.messaging.models import Section
from app.transport import consumer as consumer_module
from app.transport.consumer import StreamConsumer
from app.transport.events import (
    NotificationRequest,
    RejectedNotificationRequest,
    parse_event,
)
from tests.test_consumer_keeps_its_entries import _recipient
from tests.test_transport_consumer import _no_pending, _run_until, _user_upserted_data
from tests.test_transport_events import _request_data

_REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def fake_redis() -> fakeaioredis.FakeRedis:
    return fakeaioredis.FakeRedis()


@pytest.fixture
def a_stream(monkeypatch: pytest.MonkeyPatch) -> str:
    name = f"comms:test:{uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", name)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    monkeypatch.setattr(consumer_module, "_BACKOFF_DELAYS", (0.01, 0.01, 0.01))
    monkeypatch.setattr(consumer_module, "_WAIT_CEILING", 0.02)
    return name


@pytest.fixture
def loads_sizes(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[int]]:
    """The size of every json.loads argument in the process meanwhile."""
    sizes: list[int] = []
    real = json.loads

    def counted(text: Any, *args: Any, **kwargs: Any) -> Any:
        sizes.append(len(text))
        return real(text, *args, **kwargs)

    monkeypatch.setattr(json, "loads", counted)
    yield sizes


def _padded(data: dict[str, Any], size: int) -> bytes:
    """The data as JSON bytes, padded with trailing whitespace (JSON
    allows it) to exactly `size` bytes."""
    raw = json.dumps(data).encode()
    assert len(raw) <= size
    return raw + b" " * (size - len(raw))


# ---------------------------------------------------------------------------
# 2 -- a stream entry
# ---------------------------------------------------------------------------


class TestTheStreamEntry:
    async def test_one_byte_over(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        loads_sizes: list[int],
    ) -> None:
        person = uuid4()
        raw = _padded(_user_upserted_data(person), MAX_EVENT_DATA_BYTES + 1)
        await fake_redis.xadd(a_stream, {"event": "user_upserted", "data": raw})
        # The pair's entry: one the consumer reads, through the same
        # json.loads. Until W5-1 the pair was this test's own load of the
        # dead letter's names -- the counter seeing the test, not the
        # process.
        later = uuid4()
        await fake_redis.xadd(a_stream, {
            "event": "user_upserted",
            "data": json.dumps(_user_upserted_data(later)),
        })

        with capture_logs() as logs:

            async def done() -> bool:
                return await _recipient(later) and await _no_pending(
                    fake_redis, a_stream,
                )

            await _run_until(StreamConsumer(fake_redis), done)
        (line,) = [e for e in logs if e["event"] == "event_dropped"]
        assert line["data_bytes"] == MAX_EVENT_DATA_BYTES + 1
        assert line["stage"] == "parse"
        assert not await _recipient(person)
        assert loads_sizes, "the pair: the counter sees the process's loads"
        assert max(loads_sizes) < MAX_EVENT_DATA_BYTES, "the whole was parsed"

    async def test_exactly_the_limit_is_taken(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
    ) -> None:
        person = uuid4()
        raw = _padded(_user_upserted_data(person), MAX_EVENT_DATA_BYTES)
        await fake_redis.xadd(a_stream, {"event": "user_upserted", "data": raw})

        async def done() -> bool:
            return await _recipient(person) and await _no_pending(
                fake_redis, a_stream
            )

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), done)
        assert "event_dropped" not in [e["event"] for e in logs]


# ---------------------------------------------------------------------------
# 3 -- action_data
# ---------------------------------------------------------------------------


def _action_data(size: int) -> dict[str, Any]:
    """action_data whose compact UTF-8 JSON is exactly `size` bytes."""
    shell = {"action": "open", "params": {"k": ""}}
    empty = len(json.dumps(shell, ensure_ascii=False, separators=(",", ":")))
    shell["params"]["k"] = "a" * (size - empty)
    encoded = json.dumps(shell, ensure_ascii=False, separators=(",", ":"))
    assert len(encoded.encode()) == size
    return shell


def _parse(action_data: dict[str, Any]) -> Any:
    data = _request_data(action_data=action_data)
    return parse_event({b"event": b"notification_request",
                        b"data": json.dumps(data).encode()})


class TestActionData:
    def test_one_byte_over_is_refused(self) -> None:
        event = _parse(_action_data(MAX_ACTION_DATA_BYTES + 1))
        assert isinstance(event, RejectedNotificationRequest), event
        assert (
            f"field 'action_data' is larger than {MAX_ACTION_DATA_BYTES} "
            "bytes of JSON" in event.reason
        )
        assert "aaaa" not in event.reason

    def test_exactly_the_limit_is_taken(self) -> None:
        event = _parse(_action_data(MAX_ACTION_DATA_BYTES))
        assert isinstance(event, NotificationRequest), event
        assert event.action_data is not None


# ---------------------------------------------------------------------------
# 4 -- a REST body
# ---------------------------------------------------------------------------


def _section_body(size: int) -> tuple[bytes, str]:
    key = f"p3-{uuid4().hex[:8]}"
    shell = json.dumps({"key": key, "label": ""}).encode()
    pad = size - len(shell)
    body = json.dumps({"key": key, "label": "x" * pad}).encode()
    assert len(body) == size
    return body, key


async def _sections(key: str) -> int:
    async with get_session_factory()() as s:
        return int(await s.scalar(
            select(func.count()).select_from(Section).where(Section.key == key)
        ) or 0)


def _chunks(body: bytes) -> Callable[[], AsyncIterator[bytes]]:
    async def gen() -> AsyncIterator[bytes]:
        for i in range(0, len(body), 64 * 1024):
            yield body[i:i + 64 * 1024]
    return gen


_JSON = {"content-type": "application/json"}


class TestTheRequestBody:
    async def test_one_byte_over(
        self, client: AsyncClient, loads_sizes: list[int],
    ) -> None:
        body, key = _section_body(MAX_REQUEST_BODY_BYTES + 1)
        resp = await client.post("/api/v1/sections", content=body, headers=_JSON)
        assert resp.status_code == 413, resp.text
        error = resp.json()["error"]
        assert error["class"] == "bad_request"
        assert str(MAX_REQUEST_BODY_BYTES) in error["message"]
        assert not [n for n in loads_sizes if n > MAX_REQUEST_BODY_BYTES]
        assert await _sections(key) == 0

    async def test_streamed_over(
        self, client: AsyncClient, loads_sizes: list[int],
    ) -> None:
        body, key = _section_body(MAX_REQUEST_BODY_BYTES + 1)
        resp = await client.post(
            "/api/v1/sections", content=_chunks(body)(), headers=_JSON,
        )
        assert "content-length" not in resp.request.headers
        assert resp.status_code == 413, resp.text
        assert resp.json()["error"]["class"] == "bad_request"
        assert not [n for n in loads_sizes if n > MAX_REQUEST_BODY_BYTES]
        assert await _sections(key) == 0

    @pytest.mark.parametrize("streamed", [False, True], ids=["length", "stream"])
    async def test_exactly_the_limit_goes_on(
        self, client: AsyncClient, streamed: bool,
    ) -> None:
        """The route answers it as before: a label far past its column
        is the model's 422, not the layer's 413."""
        body, _ = _section_body(MAX_REQUEST_BODY_BYTES)
        content: Any = _chunks(body)() if streamed else body
        resp = await client.post("/api/v1/sections", content=content, headers=_JSON)
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["class"] == "validation"

    async def test_a_get_is_not_touched(self, client: AsyncClient) -> None:
        resp = await client.get("/health")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# the contract's numbers are the code's
# ---------------------------------------------------------------------------


def _doc() -> str:
    return re.sub(r"\s+", " ", (_REPO / "deploy" / "INTEGRATION.md").read_text())


class TestTheContract:
    def test_the_entry(self) -> None:
        assert (
            f"An entry whose `data` is larger than {MAX_EVENT_DATA_BYTES} "
            "bytes (4 MiB)" in _doc()
        )
        assert MAX_EVENT_DATA_BYTES == 4 * 1024 * 1024

    def test_action_data(self) -> None:
        assert (
            f"at most {MAX_ACTION_DATA_BYTES} bytes as compact UTF-8 JSON"
            in _doc()
        )

    def test_the_body(self) -> None:
        assert (
            f"| 413 | `bad_request` | the request's body is larger than "
            f"{MAX_REQUEST_BODY_BYTES} bytes (1 MiB)" in _doc()
        )
        assert MAX_REQUEST_BODY_BYTES == 1024 * 1024
