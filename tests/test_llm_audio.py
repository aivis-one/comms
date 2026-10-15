# =============================================================================
# P5-2 -- the audio body of an llm letter, by signed link
# =============================================================================
#
# Two mock transports behind the channel's real adapter: the model's
# provider (tests/test_llm_channel.py _Provider) and the product's object
# store, which counts every fetch and every chunk it hands out. The
# signed link is joined at runtime and shaped so the sanitizer catches
# none of it -- "not in" then proves the link was never written, not
# that it was redacted; every "not in" has its pair.
# =============================================================================

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from httpx import AsyncClient
from pydantic import ValidationError
from sqlalchemy import select, update
from sqlalchemy.exc import DBAPIError
from structlog.testing import capture_logs

from app.api import jobs
from app.core.channels import (
    LLM_BODY_CLIENT_TIMEOUTS,
    LLM_BODY_DOWNLOAD_TIMEOUT,
    parse_llm_body_hosts,
)
from app.core.config import Settings, format_startup_error, settings
from app.core.database import get_session_factory
from app.core.lanes import Lane
from app.core.logging import HTTP_CLIENT_LOGGERS
from app.engine import formatters as formatters_module
from app.engine.constants import DeliveryStatus, NotificationStatus
from app.engine.formatters import _real_llm_body_client_factory, build_formatters
from app.engine.models import LLMCall, Notification
from app.engine.processor import _close_at_ceiling, process_lane
from app.engine.service import close_notifications
from app.profile.loader import FileProfileSource, install_profile, load_profile
from app.profile.registry import Layer, registry
from app.transport.events import LLM_AUDIO_FIELDS
from tests.test_llm_channel import (
    _FIXTURE,
    _GW_HOST,
    _LETTER,
    _LLM_TYPES,
    _SETS,
    _answer,
    _call,
    _delivery,
    _everything,
    _job,
    _ok,
    _path,
    _Provider,
    _read,
    _send,
)

_REPO = Path(__file__).resolve().parents[1]
_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")

_STORE = "store.internal"
_STORE_PORT = 9000
# The signed link: its credential and signature are NOT hex and NOT in
# any shape the sanitizer knows, so nothing would redact them.
_CRED = "zz" + "cred" + uuid4().hex[:12] + "Q"
_SIG = "zz" + "sig" + uuid4().hex[:12] + "Q"


def _link(name: str = "a") -> str:
    return (
        f"http://{_STORE}:{_STORE_PORT}/voice/{name}%2Fpart.wav"
        f"?X-Amz-Credential={_CRED}%2F20261015&X-Amz-Expires=86400"
        f"&X-Amz-Signature={_SIG}"
    )


_AUDIO_TYPES = _LLM_TYPES + """
  ask.voice:
    channels: [llm]
    llm_sets: [llm.gw, llm.direct]
    lane: normal
    max_audio_bytes: 1000
    retry_max_attempts: 2
    retry_backoff_seconds: 0
"""

_BODY = bytes(range(256)) * 3  # 768 bytes, every byte value


class _Store:
    """The product's object store: scripted answers, every request and
    every chunk it handed out counted."""

    def __init__(self) -> None:
        self.scripts: list[Callable[[httpx.Request], Any]] = []
        self.requests: list[httpx.Request] = []
        self.chunks_out = 0

    def on(self, *scripts: Callable[[httpx.Request], Any]) -> None:
        self.scripts.extend(scripts)

    async def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        result = self.scripts.pop(0)(request)
        if asyncio.iscoroutine(result):
            result = await result
        assert isinstance(result, httpx.Response)
        return result

    def body(self, data: bytes = _BODY, *, length: int | None = None,
             chunk: int = 100) -> Callable[[httpx.Request], httpx.Response]:
        """200 with `data` streamed in chunks; Content-Length as given
        (None: no header)."""
        store = self

        async def stream() -> AsyncIterator[bytes]:
            for start in range(0, len(data), chunk):
                store.chunks_out += 1
                yield data[start:start + chunk]

        def respond(request: httpx.Request) -> httpx.Response:
            headers = {} if length is None else {"content-length": str(length)}
            return httpx.Response(200, headers=headers, content=stream())

        return respond


def _store_status(code: int) -> Callable[[httpx.Request], httpx.Response]:
    return lambda request: httpx.Response(code, content=b"<Error/>")


def _store_raises(exc: type[Exception]) -> Callable[[httpx.Request], httpx.Response]:
    def raise_it(request: httpx.Request) -> httpx.Response:
        raise exc("boom", request=request)

    return raise_it


@pytest.fixture(scope="module")
def audio_profile_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("audio_profile")
    shutil.copytree(_FIXTURE / "templates", root / "templates")
    types = (_FIXTURE / "types.yaml").read_text(encoding="utf-8")
    (root / "types.yaml").write_text(types + _AUDIO_TYPES, encoding="utf-8")
    return root


class _Deploy:
    def __init__(self, provider: _Provider, store: _Store) -> None:
        self.provider = provider
        self.store = store


@pytest.fixture
def deploy(
    monkeypatch: pytest.MonkeyPatch, audio_profile_dir: Path,
) -> Iterator[_Deploy]:
    provider, store = _Provider(), _Store()
    monkeypatch.setattr(settings, "llm_key_sets", json.dumps(_SETS))
    monkeypatch.setattr(settings, "llm_body_hosts", f"{_STORE}:{_STORE_PORT}")
    registry.reset()
    install_profile(load_profile(
        FileProfileSource(audio_profile_dir),
        llm_set_names=tuple(settings.llm_sets),
    ))
    _rebuild(provider, store)
    yield _Deploy(provider, store)


def _rebuild(provider: _Provider, store: _Store) -> None:
    formatters_module._registry = build_formatters(
        settings,
        lambda **kwargs: None,
        llm_http_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(provider.handle),
        ),
        llm_body_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(store.handle),
            trust_env=False, follow_redirects=False,
        ),
    )


def _key() -> str:
    return f"voice:{uuid4()}"


def _voice(key: str, link: str | None = None, fmt: str = "wav",
           type_: str = "ask.voice", **extra: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "v": 1, "idempotency_key": key, "type": type_, "llm": _LETTER,
        "llm_audio": {"url": link or _link(), "format": fmt},
    }
    data.update(extra)
    return data


async def _pass() -> int:
    return await process_lane(Lane.NORMAL)


async def _reason(client: AsyncClient, key: str) -> str:
    (item,) = (await _read(client, key))["intake"]
    assert item["outcome"] == "rejected_at_intake"
    reason: str = item["reason"]
    return reason


def _sent_bodies(provider: _Provider) -> list[dict[str, Any]]:
    return [json.loads(r.content) for r in provider.requests]


# =============================================================================
# B1 -- the link in the envelope
# =============================================================================


class TestTheEnvelope:
    @pytest.mark.parametrize(
        ("audio", "reason"),
        [
            ("x", "llm_audio must be a JSON object"),
            ({}, "required field 'url' is missing"),
            ({"url": _link()}, "required field 'format' is missing"),
            ({"url": _link(), "format": "wav", "size": 1},
             "llm_audio has unknown field(s) 'size'"),
            ({"url": "", "format": "wav"}, "llm_audio.url must be a non-empty"),
            ({"url": _link() + "x" * 4096, "format": "wav"}, "at most 4096"),
            ({"url": _link().replace("http:", "ftp:"), "format": "wav"},
             "must be an http(s) URL"),
            ({"url": _link().replace("//", "//u:p@"), "format": "wav"},
             "must be an http(s) URL"),
            ({"url": _link() + "#frag", "format": "wav"}, "must be an http(s) URL"),
            ({"url": _link(), "format": ""}, "llm_audio.format must be"),
            ({"url": _link(), "format": "WAV"}, "llm_audio.format must be"),
        ],
    )
    async def test_a_malformed_link_is_rejected_without_quoting_it(
        self, deploy: _Deploy, bare_client: AsyncClient,
        audio: Any, reason: str,
    ) -> None:
        key = _key()
        data = _voice(key)
        data["llm_audio"] = audio
        await _send(data)
        found = await _reason(bare_client, key)
        assert reason in found
        assert _SIG not in found and _CRED not in found

    @pytest.mark.parametrize(
        "host",
        [
            "comms-postgres:5432", "comms-redis:6379", "comms-app:8000",
            "localhost:80", "127.0.0.1:9000", "169.254.169.254:80",
            f"{_STORE}:9001",
        ],
    )
    async def test_a_link_outside_the_list_is_rejected(
        self, deploy: _Deploy, bare_client: AsyncClient, host: str,
    ) -> None:
        key = _key()
        await _send(_voice(key, _link().replace(f"{_STORE}:{_STORE_PORT}", host)))
        found = await _reason(bare_client, key)
        assert f"{host} is not in LLM_BODY_HOSTS" in found
        assert _SIG not in found
        assert deploy.store.requests == []

    async def test_an_empty_list_fetches_nothing(
        self, deploy: _Deploy, bare_client: AsyncClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "llm_body_hosts", "")
        key = _key()
        await _send(_voice(key))
        assert "this deploy fetches no body" in await _reason(bare_client, key)

    async def test_a_type_for_people_takes_no_link(
        self, deploy: _Deploy, bare_client: AsyncClient,
    ) -> None:
        key = _key()
        await _send({"v": 1, "idempotency_key": key, "type": "unit_event_in_app",
                     "target_type": "all", "target_value": "*", "title": "T",
                     "body": "B", "llm_audio": {"url": _link(), "format": "wav"}})
        assert "may not carry 'llm_audio'" in await _reason(bare_client, key)

    async def test_the_link_is_kept_with_the_types_limit(
        self, deploy: _Deploy,
    ) -> None:
        key = _key()
        await _send(_voice(key))
        call = await _call((await _job(key)).id)
        assert call.audio == {"url": _link(), "format": "wav", "max_bytes": 1000}

    async def test_a_resend_fetches_nothing_new_and_another_link_conflicts(
        self, deploy: _Deploy, bare_client: AsyncClient,
    ) -> None:
        deploy.store.on(deploy.store.body())
        deploy.provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_voice(key))
        await _send(_voice(key))
        await _pass()
        assert len(deploy.store.requests) == 1
        await _send(_voice(key, _link("b")))
        read = await _read(bare_client, key)
        assert [i["outcome"] for i in read["intake"]] == ["conflict"]
        assert read["job"]["status"] == "sent"


class TestTheLinkGoesAtTheOutcome:
    async def test_after_the_answer(self, deploy: _Deploy) -> None:
        deploy.store.on(deploy.store.body())
        deploy.provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_voice(key))
        job_id = (await _job(key)).id
        assert (await _call(job_id)).audio is not None  # the pair
        await _pass()
        call = await _call(job_id)
        assert call.audio is None and call.letter is None

    async def test_closed_before_its_resolve(self, deploy: _Deploy) -> None:
        key = _key()
        await _send(_voice(key))
        job_id = (await _job(key)).id
        async with get_session_factory()() as session:
            await close_notifications(
                session, Notification.id == job_id, NotificationStatus.CANCELLED,
            )
            await session.commit()
        assert (await _call(job_id)).audio is None

    async def test_expired_while_waiting(self, deploy: _Deploy) -> None:
        deploy.store.on(_store_status(503))
        key = _key()
        await _send(_voice(key))
        await _pass()
        job_id = (await _job(key)).id
        assert (await _call(job_id)).audio is not None
        async with get_session_factory()() as session:
            await close_notifications(
                session, Notification.id == job_id, NotificationStatus.EXPIRED,
            )
            await session.commit()
        assert (await _call(job_id)).audio is None

    async def test_closed_by_the_ceiling(
        self, deploy: _Deploy, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.engine import processor

        async def _fails(session: Any, notification: Any) -> bool:
            return False

        monkeypatch.setattr(processor, "_resolve_at_ceiling", _fails)
        key = _key()
        await _send(_voice(key))
        job_id = (await _job(key)).id
        async with get_session_factory()() as session:
            job = (await session.execute(
                select(Notification).where(Notification.id == job_id)
                .with_for_update()
            )).scalar_one()
            await _close_at_ceiling(session, job)
            await session.commit()
        assert (await _call(job_id)).audio is None

    async def test_no_link_outlives_its_letter(self, deploy: _Deploy) -> None:
        """ck_llm_calls_audio_with_letter, with its NULL twin."""
        key = _key()
        await _send(_voice(key))
        job_id = (await _job(key)).id
        async with get_session_factory()() as session:
            with pytest.raises(
                DBAPIError, match='check constraint "ck_llm_calls_audio_with_letter"',
            ):
                await session.execute(
                    update(LLMCall).where(LLMCall.notification_id == job_id)
                    .values(letter=None)
                )
            await session.rollback()
        async with get_session_factory()() as session:
            await session.execute(
                update(LLMCall).where(LLMCall.notification_id == job_id)
                .values(letter=None, audio=None)
            )
            await session.commit()


class TestTheList:
    def test_the_form(self) -> None:
        allowed, complaints = parse_llm_body_hosts("Store.Internal:9000, s3:443")
        assert complaints == []
        assert allowed == {("store.internal", 9000), ("s3", 443)}

    @pytest.mark.parametrize(
        ("value", "complaint"),
        [
            ("a:1,a:1", "a:1 is listed twice"),
            ("a:1,,b:2", "entry #2 is empty"),
            ("store", "is not host:port"),
            ("store:0", "is not host:port"),
            ("store:70000", "is not host:port"),
            ("[::1]:80", "is not host:port"),
        ],
    )
    def test_a_broken_list_refuses_startup(self, value: str, complaint: str) -> None:
        with pytest.raises(ValidationError) as caught:
            Settings(
                _env_file=None, app_env="production",
                database_url="postgresql+asyncpg://u:p@db/comms_unit",
                comms_service_token="unit-test-service-token",
                llm_body_hosts=value,
            )
        refusal = format_startup_error(caught.value)
        assert "LLM_BODY_HOSTS is malformed" in refusal
        assert complaint in refusal

    async def test_a_host_dropped_since_intake_is_the_deploys(
        self, deploy: _Deploy, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        key = _key()
        await _send(_voice(key))
        monkeypatch.setattr(settings, "llm_body_hosts", "other.internal:9000")
        _rebuild(deploy.provider, deploy.store)
        await _pass()
        delivery = await _delivery((await _job(key)).id)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.failure_class == "configuration"
        assert deploy.store.requests == []

    def test_the_limit_is_a_field_of_the_llm_kind(self, deploy: _Deploy) -> None:
        declared = registry.explain("ask.voice", "max_audio_bytes")
        assert declared.value == 1000 and declared.layer is Layer.PROFILE
        default = registry.explain("ask.model", "max_audio_bytes")
        assert default.value == 2 * 1024 * 1024 and default.layer is Layer.DEFAULT


# =============================================================================
# B2 -- the download
# =============================================================================


class TestTheClient:
    def test_no_proxy_from_the_environment(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy"):
            monkeypatch.setenv(name, "http://proxy.unit-test.invalid:3128")
        client = _real_llm_body_client_factory()
        assert client._mounts == {}
        assert client.follow_redirects is False
        assert client.timeout == httpx.Timeout(**LLM_BODY_CLIENT_TIMEOUTS)
        # The pair: under the same environment a default client DOES
        # take the proxy -- the measurement can see one.
        assert httpx.AsyncClient()._mounts != {}

    def test_the_download_bound_is_above_its_client(self) -> None:
        assert sum(LLM_BODY_CLIENT_TIMEOUTS.values()) < LLM_BODY_DOWNLOAD_TIMEOUT


class TestTheGrid:
    async def test_within_the_limit_the_bytes_go_as_one_more_message(
        self, deploy: _Deploy, bare_client: AsyncClient,
    ) -> None:
        deploy.store.on(deploy.store.body(length=len(_BODY)))
        deploy.provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_voice(key))
        await _pass()
        (fetch,) = deploy.store.requests
        # The link as it came: the same string, escapes and all.
        assert str(fetch.url) == _link()
        (sent,) = _sent_bodies(deploy.provider)
        assert sent["messages"][:-1] == _LETTER["messages"]
        assert sent["messages"][-1] == {
            "role": "user",
            "content": [{
                "type": "input_audio",
                "input_audio": {
                    "data": base64.b64encode(_BODY).decode(), "format": "wav",
                },
            }],
        }
        raw = deploy.provider.requests[0].content.decode()
        assert _SIG not in raw and _CRED not in raw and _STORE not in raw
        assert (await _job(key)).status == NotificationStatus.SENT

    async def test_exactly_the_limit_goes(self, deploy: _Deploy) -> None:
        deploy.store.on(deploy.store.body(b"x" * 1000, length=None))
        deploy.provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_voice(key))
        await _pass()
        assert (await _job(key)).status == NotificationStatus.SENT

    @pytest.mark.parametrize(
        ("length", "data"),
        [
            (1001, b"x" * 1001),          # the header says so
            (None, b"x" * 5000),          # no header: counted
            (10, b"x" * 5000),            # the header lies: counted
        ],
        ids=["by-header", "counted", "lying-header"],
    )
    async def test_above_the_limit_is_refused_unread(
        self, deploy: _Deploy, length: int | None, data: bytes,
    ) -> None:
        deploy.store.on(deploy.store.body(data, length=length, chunk=100))
        key = _key()
        await _send(_voice(key))
        await _pass()
        delivery = await _delivery((await _job(key)).id)
        assert delivery.failure_class == "message_rejected"
        assert "larger than max_audio_bytes" in (delivery.error_message or "")
        assert deploy.provider.requests == []
        # Not read further: by the header, nothing; counted, the chunk
        # that crossed the limit is the last one taken.
        assert deploy.store.chunks_out <= (0 if length == 1001 else 11)

    @pytest.mark.parametrize("code", [403, 404, 400, 410, 302])
    async def test_a_refused_link_is_the_messages(
        self, deploy: _Deploy, code: int,
    ) -> None:
        deploy.store.on(_store_status(code))
        key = _key()
        await _send(_voice(key))
        await _pass()
        delivery = await _delivery((await _job(key)).id)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.failure_class == "message_rejected"
        assert f"body download: status {code}" in (delivery.error_message or "")
        assert deploy.provider.requests == []

    @pytest.mark.parametrize(
        "script",
        [_store_status(500), _store_status(503), _store_status(408),
         _store_status(429), _store_raises(httpx.ConnectError),
         _store_raises(httpx.ReadTimeout)],
        ids=["500", "503", "408", "429", "connect", "read-timeout"],
    )
    async def test_an_unavailable_store_is_transient_and_fetched_again(
        self, deploy: _Deploy, script: Callable[[httpx.Request], Any],
    ) -> None:
        deploy.store.on(script, deploy.store.body())
        deploy.provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_voice(key))
        await _pass()
        job_id = (await _job(key)).id
        delivery = await _delivery(job_id)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.attempts == 1
        assert deploy.provider.requests == []
        await _pass()
        assert len(deploy.store.requests) == 2
        assert (await _job(key)).status == NotificationStatus.SENT

    async def test_a_store_that_never_finishes_times_out(
        self, deploy: _Deploy, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def _stall(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(30)
            raise AssertionError("never reached")

        monkeypatch.setattr(formatters_module, "LLM_BODY_DOWNLOAD_TIMEOUT", 0.5)
        deploy.store.on(_stall)
        key = _key()
        await _send(_voice(key))
        await _pass()
        delivery = await _delivery((await _job(key)).id)
        assert delivery.status == DeliveryStatus.PENDING
        assert "did not answer in time" in (delivery.error_message or "")

    async def test_the_call_bound_makes_room_for_the_download(
        self, deploy: _Deploy, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One second per set, a store that takes 1.5 s: without the
        download's own bound added once, the call would be cut."""
        body = deploy.store.body()

        async def _slow(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(1.5)
            return body(request)

        monkeypatch.setattr(settings, "llm_deliver_timeout_seconds", 1)
        deploy.store.on(_slow)
        deploy.provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_voice(key, type_="ask.solo"))
        started = time.monotonic()
        await _pass()
        assert time.monotonic() - started >= 1.5
        assert (await _job(key)).status == NotificationStatus.SENT


class TestTheAudioHash:
    async def test_two_bodies_two_hashes_one_letter_hash(
        self, deploy: _Deploy, bare_client: AsyncClient,
    ) -> None:
        other = b"\x00" * 300
        deploy.store.on(deploy.store.body(), deploy.store.body(other))
        deploy.provider.on(_GW_HOST, _ok(), _ok())
        first, second, plain = _key(), _key(), _key()
        await _send(_voice(first))
        await _send(_voice(second, _link("b")))
        await _send({"v": 1, "idempotency_key": plain, "type": "ask.voice",
                     "llm": _LETTER})
        deploy.provider.on(_GW_HOST, _ok())
        await _pass()
        one = (await _read(bare_client, first))["job"]["answer"]
        two = (await _read(bare_client, second))["job"]["answer"]
        none = (await _read(bare_client, plain))["job"]["answer"]
        assert list(one) == list(jobs.ANSWER_FIELDS)
        assert one["audio_hash"] == hashlib.sha256(_BODY).hexdigest()
        assert two["audio_hash"] == hashlib.sha256(other).hexdigest()
        assert one["letter_hash"] == two["letter_hash"] == none["letter_hash"]
        assert none["audio_hash"] is None
        answer = await _answer((await _job(first)).id)
        assert answer is not None and answer.audio_hash == one["audio_hash"]


# =============================================================================
# B3 -- the link nowhere
# =============================================================================


_OUTCOMES: list[tuple[str, Callable[[_Store], Callable[[httpx.Request], Any]]]] = [
    ("answered", lambda s: s.body()),
    ("too-large", lambda s: s.body(b"x" * 5000)),
    ("403", lambda s: _store_status(403)),
    ("404", lambda s: _store_status(404)),
    ("503", lambda s: _store_status(503)),
    ("connect", lambda s: _store_raises(httpx.ConnectError)),
    ("timeout", lambda s: _store_raises(httpx.ReadTimeout)),
]


@pytest.fixture
def production_http_logging() -> Iterator[None]:
    """The HTTP client loggers as setup_logging leaves them in a comms
    process (the child-process test below runs the real setup_logging);
    restored after, since a logger's level is process state."""
    from app.core.logging import _quiet_http_client_loggers

    before = {n: logging.getLogger(n).level for n in HTTP_CLIENT_LOGGERS}
    _quiet_http_client_loggers()
    yield
    for name, level in before.items():
        logging.getLogger(name).setLevel(level)


class TestTheLinkIsNowhere:
    @pytest.mark.parametrize(
        ("name", "script"), _OUTCOMES, ids=[o[0] for o in _OUTCOMES],
    )
    @pytest.mark.usefixtures("production_http_logging")
    async def test_on_every_outcome(
        self, deploy: _Deploy, bare_client: AsyncClient,
        caplog: pytest.LogCaptureFixture, name: str,
        script: Callable[[_Store], Callable[[httpx.Request], Any]],
    ) -> None:
        deploy.store.on(script(deploy.store))
        deploy.provider.on(_GW_HOST, _ok())
        key = _key()
        caplog.set_level(logging.DEBUG)
        with capture_logs() as logs:
            await _send(_voice(key))
            await _pass()
        job_id = (await _job(key)).id
        seen = _everything(
            logs,
            caplog.text,
            await _path(job_id),
            (await _delivery(job_id)).error_message,
            await _read(bare_client, key),
            await _read(bare_client, key, "/path"),
            await _read(bare_client, key, "/deliveries"),
            [r.content for r in deploy.provider.requests],
        )
        assert _SIG not in seen and _CRED not in seen
        # The pair: the lines and rows are there.
        events = {e["event"] for e in logs}
        assert events & {"llm_body_downloaded", "llm_body_download_failed"}
        assert any(row.subject == "channel" for row in await _path(job_id))

    def test_the_http_client_loggers_are_held_at_warning(self) -> None:
        """In a child process (setup_logging changes the process), at
        LOG_LEVEL=DEBUG: an httpx request to the signed link writes no
        line with it -- and the line after it IS written."""
        script = (
            "import asyncio, httpx, structlog\n"
            "from app.core.logging import setup_logging\n"
            "setup_logging()\n"
            "async def main():\n"
            "    t = httpx.MockTransport(lambda r: httpx.Response(200))\n"
            "    async with httpx.AsyncClient(transport=t) as c:\n"
            f"        await c.get({_link()!r})\n"
            "    structlog.get_logger().info('p52_probe_after')\n"
            "asyncio.run(main())\n"
        )
        env = {**os.environ, "LOG_LEVEL": "DEBUG"}
        done = subprocess.run(
            [sys.executable, "-c", script], cwd=_REPO, env=env,
            capture_output=True, text=True, timeout=60, check=False,
        )
        output = done.stdout + done.stderr
        assert done.returncode == 0, output
        assert _SIG not in output and _CRED not in output
        assert "p52_probe_after" in output  # the pair
        assert set(HTTP_CLIENT_LOGGERS) == {"httpx", "httpcore"}


# =============================================================================
# B4 -- the contract
# =============================================================================


def _section_11() -> str:
    start = _DOC.index("\n## 11. ")
    return _DOC[start:_DOC.index("\n## ", start + 1)]


def _first_column(heading: str) -> list[str]:
    import re

    block = _section_11().split(f"\n### {heading}", 1)[1]
    lines = block.split("\n### ")[0].splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("|"))
    rows: list[str] = []
    for line in lines[start:]:  # the FIRST table under the heading
        if not line.startswith("|"):
            break
        rows.append(line)
    assert rows[0].split("|")[1].strip() == "field"
    return [re.findall(r"`([^`]+)`", row.split("|")[1])[0] for row in rows[2:]]


class TestTheContract:
    def test_the_link_is_its_fields(self) -> None:
        assert set(_first_column("`llm_audio`")) == LLM_AUDIO_FIELDS

    def test_the_insertion_rule_is_the_codes(self) -> None:
        from app.core.channels import LLMKeySet
        from app.engine.formatters import build_llm_request

        request = build_llm_request(
            {"messages": [{"role": "user", "content": "x"}]},
            LLMKeySet("s", "https://a.invalid", "k", "m", False),
            ("<base64>", "<format>"),
        )
        documented = json.dumps(request["messages"][-1], separators=(", ", ": "))
        assert documented in " ".join(_section_11().split())

    def test_the_operator_finds_the_list(self) -> None:
        example = (_REPO / "deploy" / ".env.example").read_text()
        assert "\nLLM_BODY_HOSTS=\n" in example
        assert "`LLM_BODY_HOSTS`" in _section_11()

    def test_the_limits_numbers(self) -> None:
        flat = " ".join(_section_11().split())
        assert "1..20971520 (20 MiB), `2097152` (2 MiB)" in flat
        assert f"no answer in {int(LLM_BODY_DOWNLOAD_TIMEOUT)} s" in flat

    async def test_the_audio_hash_is_documented(self) -> None:
        start = _DOC.index("\n### `answer`")
        block = _DOC[start:_DOC.index("\n### ", start + 1)]
        assert "| `audio_hash` |" in block

    def test_no_body_kept_anywhere(self) -> None:
        """The body is in no column: the two new ones are the link (until
        the outcome) and its hash."""
        from app.engine.models import LLMAnswer

        assert {c.name for c in LLMCall.__table__.columns} >= {"audio"}
        assert "audio_hash" in {c.name for c in LLMAnswer.__table__.columns}
        assert not any(
            "body" in c.name and c.table.name == "llm_calls"
            for c in LLMCall.__table__.columns
        )

