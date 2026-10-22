# =============================================================================
# P5-1 -- the llm channel: key sets, route, adapter, the answer on the job
# =============================================================================
#
# The provider is an httpx.MockTransport behind the channel's real
# adapter (app/engine/formatters.py LLMFormatter), built by the real
# registry from LLM_KEY_SETS; the jobs go through the stream intake and
# the lane's real pass. Sentinels are joined at runtime: a literal in the
# shape of a real key blocks the push, and a sentinel no ordinary text
# contains keeps every "not in" honest -- each one has its pair, "and
# this IS there".
# =============================================================================

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import shutil
import time
from collections.abc import AsyncGenerator, Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from httpx import AsyncClient
from pydantic import ValidationError
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError
from structlog.testing import capture_logs

from app.api import jobs
from app.core.channels import (
    LLM_SET_FIELDS,
    LLMKeySet,
    parse_llm_key_sets,
)
from app.core.config import Settings, format_startup_error, settings
from app.core.database import get_session_factory
from app.core.exceptions import ProfileError
from app.core.lanes import Lane
from app.engine import formatters as formatters_module
from app.engine import journal
from app.engine.constants import ChannelAnswer, DeliveryStatus, NotificationStatus
from app.engine.formatters import (
    LLM_GATEWAY_ROUTING,
    ConfigurationError,
    LLMTransientError,
    MessageRejectedError,
    RateLimitedError,
    _SetFailure,
    _SetOutcome,
    build_formatters,
    build_llm_request,
    fold_llm_failures,
    interpret_llm_response,
)
from app.engine.models import (
    LLMAnswer,
    LLMCall,
    Notification,
    NotificationDelivery,
    NotificationTransition,
    PushOutbox,
)
from app.engine.processor import (
    _close_at_ceiling,
    cleanup_expired_notifications,
    process_lane,
    purge_expired_llm_answers,
)
from app.engine.service import (
    LANE_JOBS,
    close_notifications,
    llm_letter_encoding,
)
from app.profile.loader import (
    FileProfileSource,
    install_profile,
    lanes_mixing_llm,
    load_profile,
)
from app.profile.registry import Layer, registry
from app.transport.events import (
    LLM_LETTER_FIELDS,
    LLM_RESERVED_PARAMS,
    parse_event,
)
from app.transport.handlers import handle_event

_REPO = Path(__file__).resolve().parents[1]
_FIXTURE = _REPO / "tests" / "fixtures" / "profile"
_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")
_BASE = "/api/v1/notifications/by-key"

# -- Sentinels: joined at runtime, never a substring of routine text --------
_KEY_GW = "zz" + "llmkey" + uuid4().hex
_KEY_DIRECT = "zz" + "llmkey" + uuid4().hex
_LETTER_SENTINEL = "zq9" + "LETTER" + "wk2"
_ANSWER_SENTINEL = "zq9" + "ANSWER" + "wk2"

_GW_HOST = "gw.unit-test.invalid"
_DIRECT_HOST = "vendor.unit-test.invalid"
_SETS = {
    "llm.gw": {
        "endpoint": f"https://{_GW_HOST}/api/v1",
        "key": _KEY_GW,
        "model": "vendor/asked-model",
        "gateway": True,
    },
    "llm.direct": {
        "endpoint": f"https://{_DIRECT_HOST}/v1/",
        "key": _KEY_DIRECT,
        "model": "direct-model",
        "gateway": False,
    },
}

_LLM_TYPES = """
  ask.model:
    channels: [llm]
    llm_sets: [llm.gw, llm.direct]
    lane: normal
    push_on: outcome
    retry_max_attempts: 2
    retry_backoff_seconds: 0
  ask.solo:
    channels: [llm]
    llm_sets: [llm.gw]
    lane: normal
    answer_keep: 2h
    retry_max_attempts: 3
    retry_backoff_seconds: 0
"""

_LETTER: dict[str, Any] = {
    "messages": [
        {"role": "system", "content": "Answer in one word."},
        {"role": "user", "content": f"Say {_LETTER_SENTINEL}"},
    ],
    "response_format": {
        "type": "json_schema",
        "json_schema": {"name": "one", "schema": {"type": "object"}},
    },
    "params": {"temperature": 0.2, "max_tokens": 64},
}


# -----------------------------------------------------------------------------
# The provider
# -----------------------------------------------------------------------------


Script = Callable[[httpx.Request], Any]


class _Provider:
    """A scripted OpenAI-compatible provider per host; records every
    request it received."""

    def __init__(self) -> None:
        self.scripts: dict[str, list[Script]] = {_GW_HOST: [], _DIRECT_HOST: []}
        self.requests: list[httpx.Request] = []

    def on(self, host: str, *responses: Script) -> None:
        self.scripts[host].extend(responses)

    def calls(self, host: str) -> int:
        return sum(1 for r in self.requests if r.url.host == host)

    def bodies(self, host: str) -> list[dict[str, Any]]:
        return [json.loads(r.content) for r in self.requests if r.url.host == host]

    async def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        script = self.scripts[request.url.host].pop(0)
        result = script(request)
        if asyncio.iscoroutine(result):
            result = await result
        assert isinstance(result, httpx.Response)
        return result


def _completion_bytes(
    content: str | None = _ANSWER_SENTINEL,
    *,
    model: str | None = "vendor/answered-model",
    provider: str | None = "UpstreamCo",
    refusal: str | None = None,
    finish_reason: str = "stop",
) -> bytes:
    """A chat completion with the spacing and key order of a real one:
    the stored body must be these bytes, not a re-serialization."""
    message: dict[str, Any] = {"role": "assistant", "content": content}
    if refusal is not None:
        message["refusal"] = refusal
    doc: dict[str, Any] = {"id": "gen-1", "object": "chat.completion"}
    if provider is not None:
        doc["provider"] = provider
    if model is not None:
        doc["model"] = model
    doc["choices"] = [
        {"index": 0, "message": message, "finish_reason": finish_reason},
    ]
    doc["usage"] = {"prompt_tokens": 3, "completion_tokens": 1}
    return json.dumps(doc, indent=1).encode()


def _ok(**kw: Any) -> Script:
    return lambda request: httpx.Response(200, content=_completion_bytes(**kw))


def _status(code: int, body: Any = None, **headers: str) -> Script:
    content = b"" if body is None else json.dumps(body).encode()
    return lambda request: httpx.Response(code, content=content, headers=headers)


def _boom(exc: type[Exception]) -> Script:
    def raise_it(request: httpx.Request) -> httpx.Response:
        raise exc("boom", request=request)

    return raise_it


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture(scope="module")
def llm_profile_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The fixture profile plus the two llm types of this file."""
    root = tmp_path_factory.mktemp("llm_profile")
    shutil.copytree(_FIXTURE / "templates", root / "templates")
    types = (_FIXTURE / "types.yaml").read_text(encoding="utf-8")
    (root / "types.yaml").write_text(types + _LLM_TYPES, encoding="utf-8")
    return root


@pytest.fixture
def provider(
    monkeypatch: pytest.MonkeyPatch, llm_profile_dir: Path,
) -> Iterator[_Provider]:
    """A deploy with the llm channel live (two sets), the profile with
    the llm types installed, the registry built on the mock provider."""
    the_provider = _Provider()
    monkeypatch.setattr(settings, "llm_key_sets", json.dumps(_SETS))
    registry.reset()
    install_profile(load_profile(
        FileProfileSource(llm_profile_dir),
        llm_set_names=tuple(settings.llm_sets),
    ))
    formatters_module._registry = build_formatters(
        settings,
        lambda **kwargs: None,
        llm_http_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(the_provider.handle),
        ),
    )
    yield the_provider


def _key() -> str:
    return f"llm:{uuid4()}"


def _request(key: str, type_: str = "ask.model", **extra: Any) -> dict[str, Any]:
    data: dict[str, Any] = {"v": 1, "idempotency_key": key, "type": type_}
    data["llm"] = _LETTER
    data.update(extra)
    return data


async def _send(data: dict[str, Any]) -> str:
    parsed = parse_event({"event": "notification_request", "data": json.dumps(data)})
    async with get_session_factory()() as session:
        result = await handle_event(session, parsed)
        await session.commit()
    return str(result)


async def _pass() -> int:
    return await process_lane(Lane.NORMAL)


async def _job(key: str) -> Notification:
    async with get_session_factory()() as session:
        return (
            await session.execute(
                select(Notification).where(Notification.idempotency_key == key)
            )
        ).scalar_one()


async def _delivery(job_id: UUID) -> NotificationDelivery:
    async with get_session_factory()() as session:
        return (
            await session.execute(
                select(NotificationDelivery).where(
                    NotificationDelivery.notification_id == job_id,
                )
            )
        ).scalar_one()


async def _call(job_id: UUID) -> LLMCall:
    async with get_session_factory()() as session:
        call = await session.get(LLMCall, job_id)
        assert call is not None
        return call


async def _answer(job_id: UUID) -> LLMAnswer | None:
    async with get_session_factory()() as session:
        return await session.get(LLMAnswer, job_id)


async def _path(job_id: UUID) -> list[NotificationTransition]:
    async with get_session_factory()() as session:
        return list((
            await session.execute(
                select(NotificationTransition)
                .where(NotificationTransition.notification_id == job_id)
                .order_by(NotificationTransition.id)
            )
        ).scalars())


async def _read(client: AsyncClient, key: str, tail: str = "") -> Any:
    return (await client.get(f"{_BASE}{tail}", params={"key": key})).json()


def _everything(*parts: Any) -> str:
    return "\n".join(repr(part) for part in parts)


# =============================================================================
# B1 -- key sets and the route
# =============================================================================


class TestKeySets:
    def _sets(self, **override: Any) -> str:
        sets = json.loads(json.dumps(_SETS))
        sets["llm.gw"].update(override)
        return json.dumps(sets)

    def test_a_whole_value_parses_into_its_sets_in_order(self) -> None:
        sets, complaints = parse_llm_key_sets(json.dumps(_SETS))
        assert complaints == []
        assert list(sets) == ["llm.gw", "llm.direct"]
        assert sets["llm.direct"] == LLMKeySet(
            name="llm.direct",
            endpoint=f"https://{_DIRECT_HOST}/v1",
            key=_KEY_DIRECT,
            model="direct-model",
            gateway=False,
        )

    def test_the_key_is_not_in_repr(self) -> None:
        sets, _ = parse_llm_key_sets(json.dumps(_SETS))
        assert _KEY_GW not in repr(sets)
        assert "llm.gw" in repr(sets)  # the pair

    @pytest.mark.parametrize(
        ("value", "complaint"),
        [
            # EMPTY: an object without sets, a set without a field.
            ("{}", "names no set"),
            (
                json.dumps({"llm.gw": {"endpoint": "https://a.invalid/v1",
                                       "key": "", "model": "m",
                                       "gateway": True}}),
                "set 'llm.gw': key must be a non-empty string",
            ),
            # SHORTFALL: a field missing; gateway without its value.
            (
                json.dumps({"llm.gw": {"endpoint": "https://a.invalid/v1",
                                       "key": "k", "gateway": True}}),
                "set 'llm.gw': missing model",
            ),
            (
                json.dumps({"llm.gw": {"endpoint": "https://a.invalid/v1",
                                       "key": "k", "model": "m",
                                       "gateway": "true"}}),
                "set 'llm.gw': gateway must be a JSON boolean",
            ),
            # REPEAT: a name written twice is not "the last one wins".
            (
                '{"llm.gw": {}, "llm.gw": {}}',
                "the name 'llm.gw' is written twice",
            ),
            ("not json", "not JSON (line 1, column 1"),
            ("[1]", "expected a JSON object mapping set names to sets"),
            (
                json.dumps({"llm.gw": {"endpoint": "http://a.invalid/v1",
                                       "key": "k", "model": "m",
                                       "gateway": True}}),
                "set 'llm.gw': endpoint: expected an https base URL",
            ),
            (
                json.dumps({"LLM Text": {}}),
                "set #1: a name not repeated (it may be a secret)",
            ),
        ],
    )
    def test_every_broken_form_is_named(self, value: str, complaint: str) -> None:
        sets, complaints = parse_llm_key_sets(value)
        assert sets == {}
        assert any(complaint in c for c in complaints), complaints

    def test_a_secret_in_the_wrong_place_is_not_repeated(self) -> None:
        """A key pasted as a set name, or as a field name, is refused
        without being quoted -- and the refusal still says where."""
        value = json.dumps({_KEY_GW: {}, "llm.ok": {_KEY_DIRECT: 1}})
        _, complaints = parse_llm_key_sets(value)
        joined = " ".join(complaints)
        assert _KEY_GW not in joined and _KEY_DIRECT not in joined
        assert "set #1" in joined and "set 'llm.ok'" in joined  # the pair
        assert "unknown field(s) a name not repeated" in joined


def _settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "_env_file": None,
        "app_env": "production",
        "database_url": "postgresql+asyncpg://u:p@db/comms_unit",
        "comms_service_token": "unit-test-service-token",
    }
    return Settings(**{**base, **overrides})


def _refusal(**overrides: Any) -> str:
    with pytest.raises(ValidationError) as caught:
        _settings(**overrides)
    return format_startup_error(caught.value)


class TestRedStart:
    def test_a_broken_set_refuses_naming_the_variable_and_the_set(self) -> None:
        sets = json.loads(json.dumps(_SETS))
        del sets["llm.direct"]["model"]
        refusal = _refusal(llm_key_sets=json.dumps(sets))
        assert "LLM_KEY_SETS is malformed" in refusal
        assert "set 'llm.direct': missing model" in refusal
        assert _KEY_DIRECT not in refusal and _KEY_GW not in refusal

    def test_whole_sets_start(self) -> None:
        assert set(_settings(llm_key_sets=json.dumps(_SETS)).llm_sets) == {
            "llm.gw", "llm.direct",
        }

    def test_empty_is_no_llm(self) -> None:
        assert _settings(llm_key_sets="").llm_sets == {}

    def test_a_free_route_refuses_outside_development(self) -> None:
        sets = json.loads(json.dumps(_SETS))
        sets["llm.gw"]["model"] = "vendor/cheap:free"
        refusal = _refusal(llm_key_sets=json.dumps(sets))
        assert "set 'llm.gw' names a free route" in refusal
        # The pair: development may.
        assert "llm.gw" in _settings(
            app_env="development", llm_key_sets=json.dumps(sets),
        ).llm_sets


def _profile_refusal(root: Path, extra: str, sets: tuple[str, ...]) -> str:
    shutil.copytree(_FIXTURE / "templates", root / "templates")
    types = (_FIXTURE / "types.yaml").read_text(encoding="utf-8")
    (root / "types.yaml").write_text(types + extra, encoding="utf-8")
    with pytest.raises(ProfileError) as caught:
        load_profile(FileProfileSource(root), llm_set_names=sets)
    return str(caught.value)


class TestTheRoute:
    @pytest.mark.parametrize(
        ("extra", "complaint"),
        [
            # SHORTFALL: a set the deploy does not define; no sets at all.
            (
                "  m:\n    channels: [llm]\n    llm_sets: [llm.gone]\n",
                "routes to key set 'llm.gone', which LLM_KEY_SETS",
            ),
            (
                "  m:\n    channels: [llm]\n",
                "routes to 'llm' but declares no `llm_sets`",
            ),
            # EMPTY and REPEAT in the list.
            (
                "  m:\n    channels: [llm]\n    llm_sets: []\n",
                "found an empty list (routes the call nowhere)",
            ),
            (
                "  m:\n    channels: [llm]\n    llm_sets: [llm.gw, llm.gw]\n",
                "'llm.gw' is listed twice",
            ),
            # The kind's shape.
            (
                "  m:\n    channels: [llm, email]\n    llm_sets: [llm.gw]\n",
                "a model is not a person -- 'llm' goes alone",
            ),
            (
                "  m:\n    channels: [llm]\n    llm_sets: [llm.gw]\n"
                "    category: c\n",
                "declares `category`, which concerns a person",
            ),
            (
                "  m:\n    channels: [email]\n    llm_sets: [llm.gw]\n",
                "declares `llm_sets` but does not route to 'llm' alone",
            ),
            (
                "  m:\n    channels: [email]\n    answer_keep: 1h\n",
                "declares `answer_keep` but does not route to 'llm' alone",
            ),
            (
                "  m:\n    channels: [llm]\n    llm_sets: [llm.gw]\n"
                "    answer_keep: 3650d\n",
                "longer than NOTIFICATION_RETENTION_DAYS",
            ),
        ],
    )
    def test_a_broken_llm_type_refuses_startup(
        self, tmp_path: Path, extra: str, complaint: str,
    ) -> None:
        refusal = _profile_refusal(tmp_path, extra, ("llm.gw", "llm.direct"))
        assert complaint in refusal

    def test_a_template_sheet_for_llm_refuses_startup(self, tmp_path: Path) -> None:
        """llm renders nothing: a sheet naming it is a template no
        delivery reads -- refused, like a typo of a channel."""
        shutil.copytree(_FIXTURE / "templates", tmp_path / "templates")
        (tmp_path / "types.yaml").write_text(
            (_FIXTURE / "types.yaml").read_text(encoding="utf-8") + _LLM_TYPES,
        )
        sheet = tmp_path / "templates" / "en.yaml"
        sheet.write_text(
            sheet.read_text(encoding="utf-8")
            + "\nask.model:\n  llm:\n    body: \"x\"\n",
        )
        with pytest.raises(ProfileError) as caught:
            load_profile(FileProfileSource(tmp_path))
        assert "channel key 'llm' is not a channel of this service that" in str(
            caught.value,
        )

    def test_explain_names_the_sets_and_their_layer(self, provider: _Provider) -> None:
        decided = registry.explain("ask.model", "llm_sets")
        assert decided.value == ("llm.gw", "llm.direct")
        assert decided.layer is Layer.PROFILE
        kept = registry.explain("ask.model", "answer_keep")
        assert kept.value == timedelta(hours=24)
        assert kept.layer is Layer.DEFAULT


class TestIntakeByKind:
    """Which fields a request carries is its type's (P5-1)."""

    @pytest.mark.parametrize(
        ("type_", "extra", "drop", "reason"),
        [
            # An llm type without its letter (SHORTFALL).
            ("ask.model", {}, "llm", "needs the field 'llm'"),
            # An llm type with a person's address or letter (REPEAT of a
            # kind it does not have).
            (
                "ask.model",
                {"target_type": "all", "target_value": "*"},
                None,
                "may not carry target_type, target_value",
            ),
            (
                "ask.model", {"title": "T", "body": "B"}, None,
                "may not carry title, body",
            ),
            # A type for people with a model's letter.
            (
                "unit_event_in_app",
                {"target_type": "all", "target_value": "*", "title": "T",
                 "body": "B"},
                None,
                "may not carry 'llm'",
            ),
            # A type for people without title/body -- moved here from the
            # parser (tests/test_transport_events.py).
            (
                "unit_event_in_app",
                {"target_type": "all", "target_value": "*", "body": "B"},
                "llm",
                "needs title",
            ),
            (
                "unit_event_in_app",
                {"target_type": "all", "target_value": "*", "title": "T"},
                "llm",
                "needs body",
            ),
        ],
    )
    async def test_the_wrong_fields_are_rejected_under_the_key(
        self, provider: _Provider, bare_client: AsyncClient,
        type_: str, extra: dict[str, Any], drop: str | None, reason: str,
    ) -> None:
        key = _key()
        data = _request(key, type_, **extra)
        if drop is not None:
            del data[drop]
        await _send(data)
        read = await _read(bare_client, key)
        assert read["job"] is None
        (item,) = read["intake"]
        assert item["outcome"] == "rejected_at_intake"
        assert reason in item["reason"]

    @pytest.mark.parametrize(
        ("letter", "reason"),
        [
            ({"messages": []}, "llm.messages must be a non-empty list"),
            ({}, "required field 'messages' is missing"),
            ({"messages": [{"content": "x"}]}, "llm.messages[0].role"),
            ({"messages": [{"role": "user"}]}, "llm.messages[0].content"),
            (
                {"messages": [{"role": "user", "content": "x"}],
                 "params": {"model": "other", "stream": True}},
                "llm.params may not set 'model', 'stream'",
            ),
            (
                {"messages": [{"role": "user", "content": "x"}], "prompt": "x"},
                "llm has unknown field(s) 'prompt'",
            ),
        ],
    )
    async def test_the_letters_form_is_checked_never_its_text(
        self, provider: _Provider, bare_client: AsyncClient,
        letter: dict[str, Any], reason: str,
    ) -> None:
        key = _key()
        await _send(_request(key, llm=letter))
        (item,) = (await _read(bare_client, key))["intake"]
        assert reason in item["reason"]

    async def test_an_oversize_letter_is_rejected(
        self, provider: _Provider, bare_client: AsyncClient,
    ) -> None:
        key = _key()
        big = {"messages": [{"role": "user", "content": "x" * 200_001}]}
        await _send(_request(key, llm=big))
        (item,) = (await _read(bare_client, key))["intake"]
        assert "the limit is 200000" in item["reason"]

    async def test_a_replay_asks_once_and_one_letter_has_one_hash(
        self, provider: _Provider,
    ) -> None:
        """REPEAT: the same bytes under one key are one job and one call;
        the same letter under two keys is two calls with ONE hash."""
        provider.on(_GW_HOST, _ok(), _ok())
        first, second = _key(), _key()
        await _send(_request(first))
        await _send(_request(first))
        await _send(_request(second))
        await _pass()
        assert provider.calls(_GW_HOST) == 2
        one = await _call((await _job(first)).id)
        two = await _call((await _job(second)).id)
        assert one.letter_hash == two.letter_hash == hashlib.sha256(
            llm_letter_encoding(_LETTER).encode(),
        ).hexdigest()


class TestTheFallback:
    async def test_a_transport_failure_goes_to_the_next_set(
        self, provider: _Provider, bare_client: AsyncClient,
    ) -> None:
        provider.on(_GW_HOST, _status(503))
        provider.on(_DIRECT_HOST, _ok())
        key = _key()
        await _send(_request(key))
        await _pass()
        assert provider.calls(_GW_HOST) == 1
        assert provider.calls(_DIRECT_HOST) == 1
        job = await _job(key)
        assert job.status == NotificationStatus.SENT
        answer = await _answer(job.id)
        assert answer is not None and answer.answered_set == "llm.direct"

    async def test_an_answer_of_the_first_set_ends_the_call(
        self, provider: _Provider,
    ) -> None:
        """Whatever the model answered -- here a refusal in words."""
        provider.on(_GW_HOST, _ok(content=None, refusal="I can't help with that."))
        key = _key()
        await _send(_request(key))
        await _pass()
        assert provider.calls(_GW_HOST) == 1
        assert provider.calls(_DIRECT_HOST) == 0
        assert (await _job(key)).status == NotificationStatus.SENT

    async def test_the_list_exhausted_is_the_retry_rule(
        self, provider: _Provider,
    ) -> None:
        """Both sets fail -> an attempt is spent; the next attempt starts
        from the FIRST set; at the type's ceiling: transient_exhausted."""
        provider.on(_GW_HOST, _status(502), _status(502))
        provider.on(_DIRECT_HOST, _boom(httpx.ConnectError), _status(500))
        key = _key()
        await _send(_request(key))
        await _pass()
        job = await _job(key)
        delivery = await _delivery(job.id)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.attempts == 1
        assert delivery.wait_reason == "transient_backoff"
        await _pass()
        assert [r.url.host for r in provider.requests] == [
            _GW_HOST, _DIRECT_HOST, _GW_HOST, _DIRECT_HOST,
        ]
        delivery = await _delivery(job.id)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.failure_class == "transient_exhausted"
        assert (await _job(key)).status == NotificationStatus.FAILED


class TestTheKeyIsNowhere:
    async def test_not_in_the_journal_the_record_the_log_or_the_api(
        self, provider: _Provider, bare_client: AsyncClient,
    ) -> None:
        """Through a 401 (config), a 503 and a transport exception -- the
        texts that could carry it -- and a final answer."""
        provider.on(_GW_HOST, _status(401, {"error": {"code": 401,
                                                      "message": "bad key"}}))
        provider.on(_DIRECT_HOST, _boom(httpx.ReadTimeout))
        key = _key()
        await _send(_request(key, "ask.model"))
        with capture_logs() as logs:
            await _pass()
            provider.on(_GW_HOST, _status(503))
            provider.on(_DIRECT_HOST, _ok())
            await _pass()
        job = await _job(key)
        seen = _everything(
            logs,
            await _path(job.id),
            (await _delivery(job.id)).error_message,
            await _read(bare_client, key),
            await _read(bare_client, key, "/path"),
            await _read(bare_client, key, "/deliveries"),
        )
        assert _KEY_GW not in seen and _KEY_DIRECT not in seen
        # The pair: the key DID travel, in the header and only there.
        sent = provider.requests[0]
        assert sent.headers["authorization"] == f"Bearer {_KEY_GW}"
        assert _KEY_GW not in sent.content.decode()
        assert "status 401" in seen and "llm.gw" in seen


# =============================================================================
# B2 -- the adapter
# =============================================================================


class TestTheRequest:
    async def test_a_gateway_set_carries_the_routing_block(
        self, provider: _Provider,
    ) -> None:
        provider.on(_GW_HOST, _ok())
        await _send(_request(_key()))
        await _pass()
        (request,) = provider.requests
        assert str(request.url) == f"https://{_GW_HOST}/api/v1/chat/completions"
        body = json.loads(request.content)
        assert body == {
            "temperature": 0.2,
            "max_tokens": 64,
            "model": "vendor/asked-model",
            "messages": _LETTER["messages"],
            "response_format": _LETTER["response_format"],
            "provider": {"allow_fallbacks": False, "data_collection": "deny"},
        }

    async def test_a_vendor_set_carries_none_of_it(
        self, provider: _Provider,
    ) -> None:
        provider.on(_GW_HOST, _status(500))
        provider.on(_DIRECT_HOST, _ok())
        await _send(_request(_key()))
        await _pass()
        request = provider.requests[1]
        assert str(request.url) == f"https://{_DIRECT_HOST}/v1/chat/completions"
        body = json.loads(request.content)
        assert "provider" not in body
        assert set(body) == {
            "temperature", "max_tokens", "model", "messages", "response_format",
        }
        assert body["model"] == "direct-model"

    def test_the_letters_params_never_override_the_transport(self) -> None:
        """build_llm_request writes the transport's fields last."""
        key_set = LLMKeySet("s", "https://a.invalid", "k", "the-model", True)
        letter = {"messages": [{"role": "user", "content": "x"}],
                  "params": {"model": "other", "provider": {"x": 1}}}
        request = build_llm_request(letter, key_set)
        assert request["model"] == "the-model"
        assert request["provider"] == LLM_GATEWAY_ROUTING


def _response(status: int, body: Any = None, raw: bytes | None = None,
              **headers: str) -> httpx.Response:
    content = raw if raw is not None else (
        b"" if body is None else json.dumps(body).encode()
    )
    return httpx.Response(status, content=content, headers=headers)


class TestTheBoundary:
    """The grid "answer -> outcome" of the gate, row by row."""

    @pytest.mark.parametrize(
        "response",
        [
            _response(200, raw=_completion_bytes()),
            _response(200, raw=_completion_bytes(content="")),
            _response(200, raw=_completion_bytes(content=None, refusal="No.")),
            _response(200, raw=_completion_bytes(finish_reason="content_filter")),
            _response(200, raw=_completion_bytes(finish_reason="length")),
            # Structured output asked for, content not the schema's.
            _response(200, raw=_completion_bytes(content="not json at all")),
        ],
        ids=["content", "empty", "refusal", "filtered", "cut", "off-schema"],
    )
    def test_any_content_is_an_answer(self, response: httpx.Response) -> None:
        reply = interpret_llm_response("s", response)
        assert not isinstance(reply, _SetFailure)
        assert reply.body == response.text

    @pytest.mark.parametrize(
        ("response", "outcome"),
        [
            (_response(200, {"error": {"code": 401, "message": "x"}}), "configuration"),
            (_response(200, {"error": {"code": 402, "message": "x"}}), "configuration"),
            (_response(200, {"error": {"code": 429, "message": "x"}}), "rate_limited"),
            (_response(200, {"error": {"code": 502, "message": "x"}}), "transient"),
            (_response(200, {"error": {"message": "x"}}), "transient"),
            (_response(200, raw=b"<html>gateway</html>"), "transient"),
            (_response(200, [1, 2]), "transient"),
            (_response(200, {"choices": []}), "transient"),
            (_response(200, {"id": "x"}), "transient"),
            (_response(200, {"choices": [{"text": "x"}]}), "transient"),
            (_response(401), "configuration"),
            (_response(402), "configuration"),
            (_response(403), "configuration"),
            (_response(404), "configuration"),
            (_response(301), "configuration"),
            (_response(429, **{"Retry-After": "7"}), "rate_limited"),
            (_response(429), "rate_limited"),
            (_response(408), "transient"),
            (_response(409), "transient"),
            (_response(500), "transient"),
            (_response(503), "transient"),
            (_response(400), "rejected"),
            (_response(413), "rejected"),
            (_response(422), "rejected"),
        ],
    )
    def test_a_transport_failure_is_classified(
        self, response: httpx.Response, outcome: str,
    ) -> None:
        failure = interpret_llm_response("s", response)
        assert isinstance(failure, _SetFailure)
        assert failure.outcome.value == outcome

    def test_the_named_wait_is_read(self) -> None:
        failure = interpret_llm_response("s", _response(429, **{"Retry-After": "7"}))
        assert isinstance(failure, _SetFailure) and failure.retry_after == 7.0
        bare = interpret_llm_response("s", _response(429))
        assert isinstance(bare, _SetFailure) and bare.retry_after is None

    def test_the_providers_message_is_never_kept(self) -> None:
        failure = interpret_llm_response("s", _response(
            400, {"error": {"code": "context_length_exceeded",
                            "type": "invalid_request_error",
                            "message": f"your messages: {_LETTER_SENTINEL}"}},
        ))
        assert isinstance(failure, _SetFailure)
        assert _LETTER_SENTINEL not in failure.text
        # The pair: what IS kept names the failure.
        assert failure.text == (
            "status 400; error code context_length_exceeded; "
            "error type invalid_request_error"
        )

    @pytest.mark.parametrize(
        ("outcomes", "expected"),
        [
            ([("rate_limited", 9.0), ("rate_limited", 3.0)], RateLimitedError),
            ([("rate_limited", 9.0), ("rate_limited", None)], LLMTransientError),
            ([("rate_limited", 9.0), ("rejected", None)], LLMTransientError),
            ([("transient", None), ("configuration", None)], LLMTransientError),
            ([("rejected", None), ("configuration", None)], MessageRejectedError),
            ([("configuration", None), ("configuration", None)], ConfigurationError),
        ],
    )
    def test_the_fold_of_the_sets(
        self, outcomes: list[tuple[str, float | None]], expected: type[Exception],
    ) -> None:
        failures = [
            _SetFailure(f"s{i}", _SetOutcome(outcome), "status x", wait)
            for i, (outcome, wait) in enumerate(outcomes)
        ]
        folded = fold_llm_failures(failures)
        assert type(folded) is expected
        assert "set 's0'" in str(folded) and "set 's1'" in str(folded)
        if isinstance(folded, RateLimitedError):
            assert folded.retry_after == 3.0


# =============================================================================
# B3 -- the answer on the job
# =============================================================================


class TestTheAnswer:
    async def test_the_read_carries_the_raw_answer_and_who_gave_it(
        self, provider: _Provider, bare_client: AsyncClient,
    ) -> None:
        raw = _completion_bytes()
        provider.on(_GW_HOST, lambda request: httpx.Response(200, content=raw))
        key = _key()
        await _send(_request(key))
        await _pass()
        read = await _read(bare_client, key)
        job = read["job"]
        assert list(job) == list(jobs.JOB_FIELDS)
        assert job["status"] == "sent"
        assert job["target_type"] is None and job["target_value"] is None
        answer = job["answer"]
        assert list(answer) == list(jobs.ANSWER_FIELDS)
        assert answer["state"] == "kept"
        # The bytes as they came -- not a re-serialization.
        assert answer["body"] == raw.decode()
        # Who ANSWERED, not who was asked.
        assert answer["answered_by"] == {
            "set": "llm.gw", "provider": "UpstreamCo",
            "model": "vendor/answered-model",
        }
        assert answer["letter_hash"] == hashlib.sha256(
            llm_letter_encoding(_LETTER).encode(),
        ).hexdigest()
        answered = datetime.fromisoformat(answer["answered_at"])
        assert datetime.fromisoformat(answer["keep_until"]) - answered == (
            timedelta(hours=24)
        )
        (delivery,) = (await _read(bare_client, key, "/deliveries"))["items"]
        assert delivery["recipient_id"] is None
        assert delivery["channel"] == "llm"

    async def test_a_response_that_names_nothing_snapshots_nothing(
        self, provider: _Provider, bare_client: AsyncClient,
    ) -> None:
        provider.on(_GW_HOST, _ok(model=None, provider=None))
        key = _key()
        await _send(_request(key))
        await _pass()
        answered_by = (await _read(bare_client, key))["job"]["answer"]["answered_by"]
        assert answered_by == {"set": "llm.gw", "provider": None, "model": None}

    async def test_a_job_for_people_reads_answer_null(
        self, provider: _Provider, bare_client: AsyncClient,
    ) -> None:
        key = _key()
        await _send({"v": 1, "idempotency_key": key, "type": "unit_event_in_app",
                     "target_type": "all", "target_value": "*",
                     "title": "T", "body": "B"})
        job = (await _read(bare_client, key))["job"]
        assert job["answer"] is None
        assert job["target_type"] == "all"  # the pair

    async def test_an_active_job_reads_state_none(
        self, provider: _Provider, bare_client: AsyncClient,
    ) -> None:
        key = _key()
        await _send(_request(key))
        answer = (await _read(bare_client, key))["job"]["answer"]
        assert answer["state"] == "none"
        assert answer["answered_by"] is None and answer["body"] is None
        assert re.fullmatch(r"[0-9a-f]{64}", answer["letter_hash"])

    async def test_the_answer_is_read_again_within_its_term(
        self, provider: _Provider, bare_client: AsyncClient,
    ) -> None:
        provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_request(key))
        await _pass()
        assert (await _read(bare_client, key)) == (await _read(bare_client, key))

    async def test_past_its_term_the_body_goes_and_the_snapshot_stays(
        self, provider: _Provider, bare_client: AsyncClient,
    ) -> None:
        provider.on(_GW_HOST, _ok(), _ok())
        old, fresh = _key(), _key()
        await _send(_request(old, "ask.solo"))
        await _send(_request(fresh, "ask.solo"))
        await _pass()
        assert (await _read(bare_client, old))["job"]["answer"]["keep_until"]
        old_id = (await _job(old)).id
        async with get_session_factory()() as session:
            await session.execute(
                update(LLMAnswer)
                .where(LLMAnswer.notification_id == old_id)
                .values(keep_until=datetime.now(UTC) - timedelta(seconds=1))
            )
            await session.commit()
        assert await purge_expired_llm_answers() == 1
        purged = (await _read(bare_client, old))["job"]["answer"]
        assert purged["state"] == "purged"
        assert purged["body"] is None
        # The pair: who answered and to which letter stay, non-empty.
        assert purged["answered_by"]["model"] == "vendor/answered-model"
        assert re.fullmatch(r"[0-9a-f]{64}", purged["letter_hash"])
        # And the answer within its term (ask.solo: 2h) is untouched.
        kept = (await _read(bare_client, fresh))["job"]["answer"]
        assert kept["state"] == "kept" and _ANSWER_SENTINEL in kept["body"]

    async def test_the_expiry_cleanup_waits_for_the_answers_term(
        self, provider: _Provider,
    ) -> None:
        """An llm job past its expiry_at, answered: kept while its body is
        -- then deleted like any expired job."""
        provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_request(key, "ask.solo"))
        await _pass()
        job_id = (await _job(key)).id
        past = datetime.now(UTC) - timedelta(seconds=1)
        async with get_session_factory()() as session:
            await session.execute(
                update(Notification).where(Notification.id == job_id)
                .values(expiry_at=past)
            )
            await session.commit()
        assert await cleanup_expired_notifications() == 0
        async with get_session_factory()() as session:
            await session.execute(
                update(LLMAnswer).where(LLMAnswer.notification_id == job_id)
                .values(keep_until=past)
            )
            await session.commit()
        await purge_expired_llm_answers()
        assert await cleanup_expired_notifications() == 1

    async def test_an_outcome_pushes(self, provider: _Provider) -> None:
        provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_request(key))
        await _pass()
        job_id = (await _job(key)).id
        async with get_session_factory()() as session:
            pushes = (await session.execute(
                select(PushOutbox).where(PushOutbox.notification_id == job_id)
            )).scalars().all()
        assert len(pushes) == 1


class TestTheLetterGoesAtTheOutcome:
    async def test_after_the_answer(self, provider: _Provider) -> None:
        provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_request(key))
        job_id = (await _job(key)).id
        assert (await _call(job_id)).letter == _LETTER  # the pair: it WAS kept
        await _pass()
        call = await _call(job_id)
        assert call.letter is None
        assert re.fullmatch(r"[0-9a-f]{64}", call.letter_hash)

    async def test_a_job_closed_before_its_resolve(self, provider: _Provider) -> None:
        key = _key()
        await _send(_request(key))
        job_id = (await _job(key)).id
        async with get_session_factory()() as session:
            await close_notifications(
                session, Notification.id == job_id, NotificationStatus.CANCELLED,
            )
            await session.commit()
        assert (await _job(key)).status == NotificationStatus.CANCELLED
        assert (await _call(job_id)).letter is None

    async def test_a_job_expired_while_its_delivery_waits(
        self, provider: _Provider,
    ) -> None:
        provider.on(_GW_HOST, _status(503))
        provider.on(_DIRECT_HOST, _status(503))
        key = _key()
        await _send(_request(key))
        await _pass()
        job_id = (await _job(key)).id
        assert (await _call(job_id)).letter == _LETTER
        async with get_session_factory()() as session:
            await close_notifications(
                session, Notification.id == job_id, NotificationStatus.EXPIRED,
            )
            await session.commit()
        assert (await _job(key)).status == NotificationStatus.EXPIRED
        assert (await _call(job_id)).letter is None

    async def test_a_job_the_ceiling_closes_without_a_resolve(
        self, provider: _Provider, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.engine import processor

        async def _fails(session: Any, notification: Any) -> bool:
            return False

        monkeypatch.setattr(processor, "_resolve_at_ceiling", _fails)
        key = _key()
        await _send(_request(key))
        job_id = (await _job(key)).id
        async with get_session_factory()() as session:
            job = (await session.execute(
                select(Notification).where(Notification.id == job_id)
                .with_for_update()
            )).scalar_one()
            await _close_at_ceiling(session, job)
            await session.commit()
        assert (await _job(key)).status == NotificationStatus.FAILED
        assert (await _call(job_id)).letter is None


class TestTheLetterIsNowhere:
    async def test_not_in_the_log_the_journal_or_the_api(
        self, provider: _Provider, bare_client: AsyncClient,
    ) -> None:
        provider.on(_GW_HOST, _status(400, {"error": {
            "code": 400, "message": f"bad: {_LETTER_SENTINEL}"}}))
        provider.on(_DIRECT_HOST, _ok())
        key = _key()
        with capture_logs() as logs:
            await _send(_request(key))
            await _pass()
        job_id = (await _job(key)).id
        seen = _everything(
            logs,
            await _path(job_id),
            await _read(bare_client, key, "/path"),
            await _read(bare_client, key, "/deliveries"),
        )
        assert _LETTER_SENTINEL not in seen
        assert _ANSWER_SENTINEL not in seen
        # The pair: the lines and rows are there, and the answer is read.
        assert any(e["event"] == "llm_set_failed" for e in logs)
        assert "accepted" in seen
        body = (await _read(bare_client, key))["job"]["answer"]["body"]
        assert _ANSWER_SENTINEL in body


class TestATornAttempt:
    async def test_keeps_the_answer_and_never_asks_twice(
        self, provider: _Provider, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The answer is written with the journal's `accepted` row in its
        own transaction: the attempt tears after the call, the next one
        closes the delivery SENT from the journal -- and the model is not
        asked again (the IS NULL of accepted_answers)."""
        from app.engine import processor

        real = processor.rollup_notification
        torn: list[bool] = []

        async def _tears_once(*args: Any, **kwargs: Any) -> None:
            if not torn:
                torn.append(True)
                raise RuntimeError("torn after the call")
            await real(*args, **kwargs)

        monkeypatch.setattr(processor, "rollup_notification", _tears_once)
        monkeypatch.setattr(settings, "notification_retry_backoff_base_seconds", 0)
        provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_request(key))
        await _pass()
        job_id = (await _job(key)).id
        assert (await _job(key)).pipeline_attempts == 1
        assert await _answer(job_id) is not None  # kept through the tear
        await _pass()
        assert provider.calls(_GW_HOST) == 1
        assert (await _job(key)).status == NotificationStatus.SENT
        answer = await _answer(job_id)
        assert answer is not None and _ANSWER_SENTINEL in (answer.body or "")

    async def test_an_answer_that_cannot_be_stored_is_not_sent(
        self, provider: _Provider, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The database refuses the answer's transaction: the delivery is
        a transient failure, never SENT without its answer."""
        real_factory = journal.get_session_factory

        class _Refusing:
            def __init__(self) -> None:
                self._session = real_factory()()

            async def __aenter__(self) -> Any:
                session = await self._session.__aenter__()

                async def _commit() -> None:
                    raise DBAPIError("COMMIT", {}, Exception("connection lost"))

                session.commit = _commit  # type: ignore[method-assign]
                return session

            async def __aexit__(self, *exc: Any) -> None:
                await self._session.__aexit__(*exc)

        monkeypatch.setattr(journal, "get_session_factory", lambda: _Refusing)
        provider.on(_GW_HOST, _ok())
        key = _key()
        await _send(_request(key))
        await _pass()
        job_id = (await _job(key)).id
        delivery = await _delivery(job_id)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.attempts == 1
        assert "could not be stored" in (delivery.error_message or "")
        assert await _answer(job_id) is None


class TestTheAnswersLock:
    """The answer's INSERT beside the attempt's lock -- measured on real
    sessions, with lock_timeout, not by timing."""

    async def _job_id(self, provider: _Provider) -> UUID:
        key = _key()
        await _send(_request(key))
        return (await _job(key)).id

    async def _insert_answer(self, job_id: UUID) -> None:
        async with get_session_factory()() as session:
            await session.execute(text("SET LOCAL lock_timeout = '1s'"))
            now = datetime.now(UTC)
            session.add(LLMAnswer(
                notification_id=job_id, body="b", answered_set="llm.gw",
                answered_provider=None, answered_model=None,
                answered_at=now, keep_until=now,
            ))
            await session.commit()

    async def _holding(
        self, job_id: UUID, key_share: bool,
    ) -> AsyncGenerator[None, None]:
        async with get_session_factory()() as holder:
            await holder.execute(
                select(Notification).where(Notification.id == job_id)
                .with_for_update(key_share=key_share)
            )
            yield
            await holder.rollback()

    async def test_the_attempts_lock_does_not_hold_the_answer(
        self, provider: _Provider,
    ) -> None:
        job_id = await self._job_id(provider)
        async for _ in self._holding(job_id, key_share=True):
            await self._insert_answer(job_id)
        assert await _answer(job_id) is not None

    async def test_a_for_update_would(self, provider: _Provider) -> None:
        """The inversion: the measurement can see a wait -- under FOR
        UPDATE the same insert runs into its lock_timeout."""
        job_id = await self._job_id(provider)
        async for _ in self._holding(job_id, key_share=False):
            with pytest.raises(DBAPIError, match="lock timeout"):
                await self._insert_answer(job_id)


class TestTheChecks:
    """The three CHECKs of migration 0026, each with its NULL twin --
    asked of the database, not of the model."""

    async def _refused(self, check: str, statement: str, **params: Any) -> None:
        async with get_session_factory()() as session:
            with pytest.raises(DBAPIError, match=f'check constraint "{check}"'):
                await session.execute(text(statement), params)
            await session.rollback()

    async def _people_job(self) -> UUID:
        key = _key()
        await _send({"v": 1, "idempotency_key": key, "type": "unit_event_in_app",
                     "target_type": "all", "target_value": "*",
                     "title": "T", "body": "B"})
        return (await _job(key)).id

    async def test_a_job_for_people_keeps_its_letter(
        self, provider: _Provider,
    ) -> None:
        job_id = await self._people_job()
        for column in ("title", "body", "target_type", "target_value"):
            await self._refused(
                "ck_notifications_letter_by_kind",
                f"UPDATE notifications SET {column} = NULL WHERE id = :id",
                id=job_id,
            )

    async def test_an_llm_job_has_none(self, provider: _Provider) -> None:
        key = _key()
        await _send(_request(key))
        job_id = (await _job(key)).id
        for column in ("title", "body", "target_type", "target_value"):
            await self._refused(
                "ck_notifications_letter_by_kind",
                f"UPDATE notifications SET {column} = 'x' WHERE id = :id",
                id=job_id,
            )

    async def test_only_the_llm_delivery_has_no_recipient(
        self, provider: _Provider, db_session: Any,
    ) -> None:
        from tests.helpers import create_recipient

        job_id = await self._people_job()
        await self._refused(
            "ck_deliveries_recipient_by_channel",
            "INSERT INTO notification_deliveries (id, notification_id, "
            "recipient_id, channel, status, attempts, rate_limit_deferrals) "
            "VALUES (gen_random_uuid(), :id, NULL, 'telegram', 'pending', 0, 0)",
            id=job_id,
        )
        recipient = await create_recipient(db_session)
        await db_session.commit()
        await self._refused(
            "ck_deliveries_recipient_by_channel",
            "INSERT INTO notification_deliveries (id, notification_id, "
            "recipient_id, channel, status, attempts, rate_limit_deferrals) "
            "VALUES (gen_random_uuid(), :id, :r, 'llm', 'pending', 0, 0)",
            id=job_id, r=recipient.id,
        )

    async def test_the_journal_names_a_recipient_except_on_llm(
        self, provider: _Provider,
    ) -> None:
        job_id = await self._people_job()
        await self._refused(
            "ck_transitions_subject_shape",
            "INSERT INTO notification_transitions (notification_id, "
            "recipient_id, channel, subject, step, outcome, attempt) "
            "VALUES (:id, NULL, 'telegram', 'channel', 'deliver', "
            "'accepted', 1)",
            id=job_id,
        )
        await self._refused(
            "ck_transitions_subject_shape",
            "INSERT INTO notification_transitions (notification_id, "
            "recipient_id, channel, subject, step, outcome, attempt) "
            "VALUES (:id, gen_random_uuid(), 'llm', 'channel', 'deliver', "
            "'accepted', 1)",
            id=job_id,
        )


# =============================================================================
# B4 -- the timeout and the lanes
# =============================================================================


class TestTheTimeout:
    def test_email_and_llm_live_keep_the_lanes_at_30(self) -> None:
        email = {
            "email_mailgun_api_key": "unit-test-provider-key",
            "email_mailgun_domain": "mg.example.org",
            "email_from_address": "Unit <unit@example.org>",
        }
        deploy = _settings(llm_key_sets=json.dumps(_SETS), **email)
        assert deploy.notification_lane_interactive_deliver_timeout_seconds == 30
        assert deploy.llm_deliver_timeout_seconds == 120

    def test_the_llm_timeout_is_held_above_its_client(self) -> None:
        refusal = _refusal(
            llm_key_sets=json.dumps(_SETS), llm_deliver_timeout_seconds=100,
        )
        assert "LLM_DELIVER_TIMEOUT_SECONDS=100 is not above the client" in refusal
        assert "llm (114 s)" in refusal
        # The pair: without llm the number binds nothing.
        assert _settings(llm_deliver_timeout_seconds=100).llm_sets == {}

    async def test_the_call_is_bounded_by_its_own_timeout_per_set(
        self, provider: _Provider, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def _slow(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(30)
            raise AssertionError("never reached")

        monkeypatch.setattr(settings, "llm_deliver_timeout_seconds", 1)
        provider.on(_GW_HOST, _slow)
        key = _key()
        await _send(_request(key))
        started = time.monotonic()
        await _pass()
        elapsed = time.monotonic() - started
        # Two sets of one second each -- not the lane's 30.
        assert 1.8 <= elapsed < 10
        channel_rows = [
            row for row in await _path((await _job(key)).id)
            if row.subject == "channel"
        ]
        assert [row.outcome for row in channel_rows] == [ChannelAnswer.TIMEOUT]


class TestTheLaneWarning:
    def test_a_lane_mixing_both_kinds_is_named(self, provider: _Provider) -> None:
        mixes = lanes_mixing_llm()
        (mix,) = mixes
        assert mix.lane == "normal"
        assert mix.llm_types == ("ask.model", "ask.solo")
        assert "unit_event" in mix.other_types  # the fixture's, for people

    def test_separate_lanes_are_not(
        self, provider: _Provider, llm_profile_dir: Path, tmp_path: Path,
    ) -> None:
        """The pair: the same llm types in a lane of their own."""
        shutil.copytree(llm_profile_dir, tmp_path / "p")
        types = tmp_path / "p" / "types.yaml"
        types.write_text(
            types.read_text().replace(_LLM_TYPES, _LLM_TYPES.replace(
                "lane: normal", "lane: bulk",
            )),
        )
        registry.reset()
        install_profile(load_profile(FileProfileSource(tmp_path / "p")))
        assert lanes_mixing_llm() == []
        assert registry.record_of("ask.model") is not None

    async def test_the_worker_says_it_at_start(
        self, provider: _Provider, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app import worker

        async def _idle(*args: Any, **kwargs: Any) -> None:
            return None

        monkeypatch.setattr(worker, "_poll_loop", _idle)
        monkeypatch.setattr(worker, "listen_for_wakes", _idle)
        with capture_logs() as logs:
            await worker.run_worker_loop()
        (line,) = [e for e in logs if e["event"] == "lane_mixes_llm"]
        assert line["lane"] == "normal"
        assert line["llm_types"] == ["ask.model", "ask.solo"]
        # Until P5-L the hint said "a lane attempts one job at a time" --
        # true while a lane's jobs went one after another. Since, a model
        # holds one of the lane's places; the line names how many there
        # are, and the hint says when the lane is held: as many models.
        assert line["jobs"] == LANE_JOBS[Lane.NORMAL] == 4
        assert "this lane attempts 4 job(s) at a time" in line["hint"]
        assert "while 4 model(s) write, every job of these other types" in (
            line["hint"]
        )


# =============================================================================
# B5 -- the contract, both ways
# =============================================================================


def _section_11() -> str:
    start = _DOC.index("\n## 11. ")
    return _DOC[start:_DOC.index("\n## ", start + 1)]


def _table(heading: str) -> list[str]:
    block = _section_11().split(f"\n### {heading}", 1)[1]
    rows = [line for line in block.split("\n### ")[0].splitlines()
            if line.startswith("|")]
    assert rows[0].split("|")[1].strip() == "field"
    return [re.findall(r"`([^`]+)`", row.split("|")[1])[0] for row in rows[2:]]


class TestTheContract:
    def test_a_key_set_is_its_fields(self) -> None:
        assert _table("`key_set`") == list(LLM_SET_FIELDS)

    def test_the_letter_is_its_fields(self) -> None:
        assert set(_table("`llm`")) == LLM_LETTER_FIELDS

    def test_the_type_is_its_two_fields(self) -> None:
        """The llm kind's own fields, documented in order: two in P5-1,
        three since P5-2 added max_audio_bytes."""
        from app.profile.loader import RECORD_FIELDS

        documented = _table("The type")
        assert documented == ["llm_sets", "answer_keep", "max_audio_bytes"]
        assert set(documented) <= set(RECORD_FIELDS)

    def test_the_reserved_params_are_listed(self) -> None:
        start = _section_11().index("`params` may not name")
        sentence = _section_11()[start:start + 400]
        named = set(re.findall(r"`([a-z_]+)`", sentence.split("--")[0])) - {"params"}
        assert named == LLM_RESERVED_PARAMS

    def test_the_answer_states_are_the_codes(self) -> None:
        start = _DOC.index("\n### `answer`")
        block = _DOC[start:_DOC.index("\n### ", start + 1)]
        state_row = next(line for line in block.splitlines()
                         if line.startswith("| `state`"))
        named = set(re.findall(r"`([a-z_]+)`", state_row.split("|")[2])) - {
            "body", "keep_until", "status",
        }
        assert named == {
            jobs.AnswerState.NONE, jobs.AnswerState.KEPT, jobs.AnswerState.PURGED,
        }

    def test_the_gateway_block_is_the_documented_one(self) -> None:
        assert json.dumps(LLM_GATEWAY_ROUTING, separators=(", ", ": ")) in (
            " ".join(_section_11().split())
        )

    def test_the_env_example_and_the_generated_env_carry_the_key(self) -> None:
        example = (_REPO / "deploy" / ".env.example").read_text()
        assert "\nLLM_KEY_SETS=\n" in example
        assert "#LLM_DELIVER_TIMEOUT_SECONDS=120\n" in example
        assert Settings.model_fields["llm_deliver_timeout_seconds"].default == 120


# =============================================================================
# Item 3 of the gate's answer -- the letter in no log line of any channel
# =============================================================================


class TestNoLetterInTheLog:
    async def test_in_app(self, db_session: Any) -> None:
        from app.engine.formatters import InAppFormatter
        from tests.helpers import create_recipient, notification_row_fields

        recipient = await create_recipient(db_session)
        notification = Notification(
            type="unit_event_in_app", title=_LETTER_SENTINEL,
            body=_LETTER_SENTINEL, target_type="user",
            target_value=str(recipient.id), **notification_row_fields(),
        )
        db_session.add(notification)
        await db_session.flush()
        delivery = NotificationDelivery(
            notification_id=notification.id, recipient_id=recipient.id,
            channel="in_app",
        )
        db_session.add(delivery)
        await db_session.flush()
        with capture_logs() as logs:
            assert await InAppFormatter().deliver(notification, delivery, recipient)
        assert _LETTER_SENTINEL not in _everything(logs)
        assert [e["event"] for e in logs] == ["in_app_delivery"]  # the pair

    async def test_every_channel_through_the_pipeline(
        self, monkeypatch: pytest.MonkeyPatch, db_session: Any,
    ) -> None:
        """telegram, email and in_app: two jobs that cover the three,
        their title and body the sentinel."""
        from tests.helpers import configure_every_channel, create_recipient

        configure_every_channel(monkeypatch)
        sent: list[str] = []

        class _Bot:
            def __init__(self, **kwargs: Any) -> None:
                self.session = self

            async def send_message(self, **kwargs: Any) -> None:
                sent.append("telegram")

            async def close(self) -> None:
                return None

        def _mail(request: httpx.Request) -> httpx.Response:
            sent.append("email")
            return httpx.Response(200, json={"id": "<m@x>", "message": "ok"})

        formatters_module._registry = build_formatters(
            settings, lambda **kwargs: _Bot(**kwargs),
            lambda: httpx.AsyncClient(transport=httpx.MockTransport(_mail)),
        )
        recipient = await create_recipient(
            db_session, email="person@example.org",
        )
        await db_session.commit()
        for type_ in ("unit_event_telegram_in_app", "unit_event_in_app_email"):
            await _send({"v": 1, "idempotency_key": _key(), "type": type_,
                         "target_type": "user",
                         "target_value": str(recipient.id),
                         "title": _LETTER_SENTINEL, "body": _LETTER_SENTINEL})
        with capture_logs() as logs:
            from app.engine.processor import process_pending_notifications

            await process_pending_notifications()
        assert _LETTER_SENTINEL not in _everything(logs)
        assert sorted(sent) == ["email", "telegram"]  # the pair
        assert {"in_app_delivery", "email_accepted"} <= {e["event"] for e in logs}
