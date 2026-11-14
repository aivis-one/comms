# =============================================================================
# W3-1 -- /health and /ready of protocol 9.1, 9.2, 9.5 (app/api/report.py)
# =============================================================================
#
# done-when:
#   1 /health: the keys of the answer are the fields of the model, release
#     and commit from the environment, always 200; the database down --
#     200, `degraded`, `checks.db` not `ok`; `details` over 8 KB is not
#     sent (left out, as the model does);
#   2 seams: `seams.provides` is primer.yaml's `provides`; the answer has
#     the form of tests/fixtures/health.json, which CI hands to the
#     machine's own check_health and _seams (scripts/check_primer_yaml.py);
#   3 /ready: 200 {ready, []} with every check ok, 503 {not_ready, names}
#     otherwise.
# And the rules of the model on comms: every check under its timeout,
# the revision kept, a failing check said on its turn, the commit a
# refusal at start. (The contract in deploy/INTEGRATION.md was held here
# both ways until DOC-1 took /health and /ready out of the document: the
# machine reads them, the product never does.)
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 `version` instead of `release`          -> TestTheForm
#   M2 a check's exception not caught (500)     -> TestTheChecks
#   M3 `status` always `ok`                     -> TestTheChecks
#   M4 details over the limit sent whole        -> TestDetails
#   M5 a seam taken out of the answer           -> TestTheSeams, TestTheGolden
#   M6 /ready always 200                        -> TestReady
#   M7 the redis check left out of `checks`     -> TestTheChecks
#   M8 no timeout on a check                    -> TestTheChecks
#   M9 a failing check logged on every request  -> TestTheTurn
#   M13 no refusal without PRIMER_COMMIT        -> TestTheCommit
#   M14 the revision not kept after the fall    -> TestTheRevision
#   W4-2 `requires_satisfied` always true       -> TestTheSeams
# =============================================================================

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import fakeredis
import pytest
import yaml
from httpx import AsyncClient
from structlog.testing import capture_logs

import app.main as main_module
from app.api import report as report_module
from app.api.report import (
    DB,
    MIB,
    PROVIDES,
    REDIS,
    REQUIRES,
    SERVICE,
    Health,
    Report,
    commit_from_environment,
)
from app.core.config import settings
from app.core.constants import MAX_REQUEST_BODY_BYTES
from tests.helpers import redis_answering

_REPO = Path(__file__).resolve().parents[1]
_PRIMER = yaml.safe_load((_REPO / "primer.yaml").read_text(encoding="utf-8"))
_GOLDEN = json.loads((_REPO / "tests" / "fixtures" / "health.json").read_text())


@pytest.fixture(autouse=True)
def _fresh_turns() -> None:
    """A turn is per process: every test starts with no check in error."""
    main_module.report.reset_turns()


async def _failing_revision() -> str:
    raise ConnectionRefusedError("the database is away")


def _db_down(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "read_revision", _failing_revision)


def _redis_down(monkeypatch: pytest.MonkeyPatch) -> None:
    async def down() -> bool:
        raise ConnectionError("redis is away")

    monkeypatch.setattr(main_module, "redis_answers", down)


_STORAGE = "store-files:9000"
"""A product storage's address of the form the machine gives (W4-2)."""


def _storage_given(monkeypatch: pytest.MonkeyPatch) -> None:
    """The seam comms requires, as on a box: the golden answer is one
    whose seams are satisfied."""
    monkeypatch.setattr(settings, "product_storage_address", _STORAGE)


def _shape(value: Any) -> Any:
    """The keys and the JSON types of `value`, at every depth."""
    if isinstance(value, dict):
        return {key: _shape(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_shape(item) for item in value]
    return type(value).__name__


class TestTheForm:
    async def test_the_keys_are_the_models_and_the_identity_is_the_images(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)
        monkeypatch.setenv("PRIMER_RELEASE", "v4.0.0-rc.9")
        monkeypatch.setenv("PRIMER_COMMIT", "c0ffee" * 6 + "abcd")
        response = await client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert set(body) == set(Health.model_fields)
        assert "version" not in body
        assert body["release"] == "v4.0.0-rc.9"
        assert body["commit"] == "c0ffee" * 6 + "abcd"
        assert body["service"] == SERVICE
        assert body["status"] == "ok"
        assert body["checks"] == {DB: "ok", REDIS: "ok"}
        assert body["limits"] == {"max_upload_mb": 1}
        assert isinstance(body["uptime_s"], int) and body["uptime_s"] >= 0

    def test_the_fields_are_the_eleven_and_details(self) -> None:
        """The pair to the key test: the model is the protocol's, not an
        empty one any answer would match."""
        assert set(Health.model_fields) == {
            "service", "release", "commit", "alembic_head", "uptime_s",
            "status", "checks", "seams", "limits", "edition", "modules",
            "details",
        }

    def test_the_service_is_primer_yamls_name(self) -> None:
        assert _PRIMER["name"] == SERVICE

    def test_the_upload_limit_is_the_body_limit_in_mib(self) -> None:
        assert report_module.MAX_UPLOAD_MB * MIB == MAX_REQUEST_BODY_BYTES
        assert report_module.MAX_UPLOAD_MB >= 1


class TestTheGolden:
    """tests/fixtures/health.json is what CI hands to the machine's own
    checks (scripts/check_primer_yaml.py); this holds it to the code, so
    it cannot stay behind a change of the answer."""

    async def test_a_live_answer_has_its_keys_and_types(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)
        _storage_given(monkeypatch)
        live = (await client.get("/health")).json()
        assert live["status"] == "ok", "the pair: the answer the golden is"
        assert _shape(live) == _shape(_GOLDEN)
        for fixed in ("service", "seams", "limits", "edition", "modules"):
            assert live[fixed] == _GOLDEN[fixed], fixed
        assert set(live["checks"]) == set(_GOLDEN["checks"])
        assert set(live["details"]["channels"]) == set(
            _GOLDEN["details"]["channels"],
        )

    def test_the_golden_is_a_valid_answer(self) -> None:
        assert Health.model_validate(_GOLDEN).status == "ok"


class TestTheSeams:
    async def test_provides_is_primer_yamls(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)
        _storage_given(monkeypatch)
        seams = (await client.get("/health")).json()["seams"]
        assert seams["provides"] == _PRIMER["provides"] == list(PROVIDES)
        assert len(PROVIDES) == 6
        assert seams["requires_satisfied"] is True

    async def test_a_seam_is_a_name_never_its_value(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """W2-3: TELEGRAM_BOT_TOKEN is a seam -- /health names it and never
        shows it, though the process holds it in its environment and its
        settings. The token is assembled here: a literal of a real token's
        shape would trip secret scanning."""
        secret = "Hm" * 17 + "w"
        token = f"987654321:{secret}"
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", token)
        monkeypatch.setattr(settings, "telegram_bot_token", token)
        redis_answering(monkeypatch)
        resp = await client.get("/health")
        assert "TELEGRAM_BOT_TOKEN" in resp.json()["seams"]["provides"]
        assert secret not in resp.text

    def test_requires_is_primer_yamls(self) -> None:
        """The code's list is the file's -- since W4-2 the product's
        storage, the one seam whose satisfaction the answer computes.
        Was "none until W4-2", over an empty list."""
        assert list(REQUIRES) == _PRIMER["requires"] == ["PRODUCT_STORAGE_ADDRESS"]

    @pytest.mark.parametrize(
        ("address", "satisfied"),
        [(_STORAGE, True), ("", False)],
        ids=["given", "empty"],
    )
    async def test_satisfied_is_the_storage_given(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
        address: str, satisfied: bool,
    ) -> None:
        """W4-2: `requires_satisfied` is the seam of the product's
        storage given -- read off the validated settings, where a
        malformed one refused startup (tests/test_llm_audio.py
        TestTheSeam) -- and false without it; the machine refuses the
        rollout on false (`seams_verify`). The rest of the answer is the
        same either way: still 200, still `ok`."""
        redis_answering(monkeypatch)
        monkeypatch.setattr(settings, "product_storage_address", address)
        response = await client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert body["seams"]["requires_satisfied"] is satisfied
        assert body["status"] == "ok"


class TestTheChecks:
    async def test_the_database_down_is_degraded_never_a_500(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)
        _db_down(monkeypatch)
        response = await client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "degraded"
        assert body["checks"] == {DB: "error", REDIS: "ok"}

    async def test_redis_down_is_degraded(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _redis_down(monkeypatch)
        body = (await client.get("/health")).json()
        assert body["status"] == "degraded"
        assert body["checks"] == {DB: "ok", REDIS: "error"}

    async def test_a_check_that_does_not_answer_is_cut_at_its_timeout(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)
        monkeypatch.setattr(report_module, "CHECK_TIMEOUT_S", 0.2)

        async def silent() -> str:
            await asyncio.sleep(30)
            return "never"

        monkeypatch.setattr(main_module, "read_revision", silent)
        started = time.monotonic()
        body = (await client.get("/health")).json()
        assert time.monotonic() - started < 5
        assert body["checks"][DB] == "error"
        assert body["checks"][REDIS] == "ok", "the others are still measured"

    async def test_an_empty_revision_is_an_error(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)

        async def empty() -> str:
            return ""

        monkeypatch.setattr(main_module, "read_revision", empty)
        assert (await client.get("/health")).json()["checks"][DB] == "error"

    async def test_the_real_revision_is_the_databases(
        self, client: AsyncClient,
    ) -> None:
        """The check itself, on the suite's database: its revision is
        the migrations' head (the suite runs on `alembic upgrade head`)."""
        from alembic.config import Config
        from alembic.script import ScriptDirectory

        head = ScriptDirectory.from_config(
            Config(str(_REPO / "alembic.ini")),
        ).get_current_head()
        assert head
        assert await main_module.read_revision() == head


class TestTheRedisCheck:
    """The check itself (app/main.py redis_answers) -- the other tests
    replace it."""

    async def test_a_redis_that_answers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "redis_url", "redis://comms-redis:6379/0")
        monkeypatch.setattr(main_module, "_redis", fakeredis.FakeAsyncRedis())
        assert await main_module.redis_answers() is True

    async def test_a_redis_that_does_not(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        server = fakeredis.FakeServer()
        server.connected = False
        monkeypatch.setattr(settings, "redis_url", "redis://comms-redis:6379/0")
        monkeypatch.setattr(
            main_module, "_redis", fakeredis.FakeAsyncRedis(server=server),
        )
        with pytest.raises(Exception):  # noqa: B017 -- any failure is `error`
            await main_module.redis_answers()

    async def test_no_redis_url_is_an_error_not_a_refusal(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "redis_url", "")
        with capture_logs() as logs:
            body = (await client.get("/health")).json()
        assert body["checks"][REDIS] == "error"
        (line,) = [
            e for e in logs
            if e["event"] == "health_check_failed" and e["check"] == REDIS
        ]
        assert line["error"].startswith("builtins.LookupError at ")

    async def test_the_client_is_closed_with_the_process(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = fakeredis.FakeAsyncRedis()
        monkeypatch.setattr(main_module, "_redis", client)
        await main_module.close_redis()
        assert main_module._redis is None


class TestTheTurn:
    async def test_said_when_it_turns_and_again_after_ok(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)
        real = main_module.read_revision
        _db_down(monkeypatch)
        with capture_logs() as logs:
            await client.get("/health")
            await client.get("/ready")
        lines = [e for e in logs if e["event"] == "health_check_failed"]
        assert [line["check"] for line in lines] == [DB]
        assert lines[0]["error"].startswith("builtins.ConnectionRefusedError at ")
        assert "away" not in str(lines[0]), "the class and place, never a text"
        monkeypatch.setattr(main_module, "read_revision", real)
        await client.get("/health")
        _db_down(monkeypatch)
        with capture_logs() as logs:
            await client.get("/health")
        assert [e["check"] for e in logs if e["event"] == "health_check_failed"] == [DB]


class TestTheRevision:
    async def test_null_before_the_first_read_and_kept_after_the_fall(self) -> None:
        """The Report takes the code's head since W6-2b (its revision must
        be that head for `db` to be ok): this one is given the revision it
        reads, which is all the test asked of it before."""
        reads: list[str | Exception] = [
            ConnectionRefusedError("down"), "0041_head", ConnectionRefusedError("down"),
        ]

        async def revision() -> str:
            got = reads.pop(0)
            if isinstance(got, Exception):
                raise got
            return got

        async def answers() -> bool:
            return True

        report = Report(
            db=revision, head=lambda: "0041_head",
            checks={REDIS: answers}, details=lambda: {},
        )
        first = json.loads((await report.health()).body)
        assert first["alembic_head"] is None and first["checks"][DB] == "error"
        second = json.loads((await report.health()).body)
        assert second["alembic_head"] == "0041_head"
        third = json.loads((await report.health()).body)
        assert third["alembic_head"] == "0041_head"
        assert third["checks"][DB] == "error"


class TestDetails:
    async def test_the_channel_map_is_there(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)
        details = (await client.get("/health")).json()["details"]
        assert set(details) == {"channels"}
        assert details["channels"], "the pair: a map, not an empty object"

    @pytest.mark.parametrize(
        ("built", "reason"),
        [
            ({"channels": "x" * 8200}, "size"),
            ({"channels": object()}, "type"),
        ],
    )
    async def test_details_it_cannot_send_are_left_out(
        self,
        client: AsyncClient,
        monkeypatch: pytest.MonkeyPatch,
        built: dict[str, Any],
        reason: str,
    ) -> None:
        redis_answering(monkeypatch)
        monkeypatch.setattr(main_module, "details", lambda: built)
        with capture_logs() as logs:
            response = await client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert "details" not in body
        assert body["status"] == "ok", "the pair: the rest of the answer is whole"
        (line,) = [e for e in logs if e["event"] == "health_details_refused"]
        assert line["reason"] == reason

    async def test_a_builder_that_raises_leaves_details_out(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)

        def broken() -> dict[str, Any]:
            raise RuntimeError("no map")

        monkeypatch.setattr(main_module, "details", broken)
        response = await client.get("/health")
        assert response.status_code == 200
        assert "details" not in response.json()

    async def test_exactly_the_limit_goes(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)
        frame = len('{"channels":""}')
        built = {"channels": "x" * (report_module.DETAILS_LIMIT - frame)}
        assert report_module.free_size(built) == report_module.DETAILS_LIMIT
        monkeypatch.setattr(main_module, "details", lambda: built)
        assert (await client.get("/health")).json()["details"] == built


class TestReady:
    async def test_ready_with_every_check_ok(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        redis_answering(monkeypatch)
        response = await client.get("/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ready", "failing": []}

    @pytest.mark.parametrize(
        ("db", "redis", "failing"),
        [(False, True, [DB]), (True, False, [REDIS]), (False, False, [DB, REDIS])],
    )
    async def test_not_ready_names_what_fails(
        self,
        client: AsyncClient,
        monkeypatch: pytest.MonkeyPatch,
        db: bool,
        redis: bool,
        failing: list[str],
    ) -> None:
        if not db:
            _db_down(monkeypatch)
        if redis:
            redis_answering(monkeypatch)
        else:
            _redis_down(monkeypatch)
        response = await client.get("/ready")
        assert response.status_code == 503
        assert response.json() == {"status": "not_ready", "failing": failing}


class TestTheCommit:
    def test_the_commit_of_the_environment(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PRIMER_COMMIT", "abc123")
        assert commit_from_environment() == "abc123"

    @pytest.mark.parametrize("value", [None, ""])
    async def test_the_api_refuses_to_start_without_it(
        self, monkeypatch: pytest.MonkeyPatch, value: str | None,
    ) -> None:
        # The entry point's global effects before the refusal, replaced.
        monkeypatch.setattr(main_module, "setup_logging", lambda: None)
        if value is None:
            monkeypatch.delenv("PRIMER_COMMIT", raising=False)
        else:
            monkeypatch.setenv("PRIMER_COMMIT", value)
        with pytest.raises(SystemExit, match="PRIMER_COMMIT is not set"):
            async with main_module.lifespan(main_module.app):
                pass


class TestTheWiring:
    """The checks /ready and /health ask, and the healthcheck compose
    runs for the worker and the consumer.

    Was TestTheContract, which held deploy/INTEGRATION.md section 15
    ("What comms shows the machine") against the code both ways; DOC-1
    took that text out of the document (its reader is the machine, not
    the product), so the halves that read it are gone and the halves
    that read the code stay."""

    def test_the_checks_are_db_and_redis(self) -> None:
        wired = [DB, *main_module.report._checks]
        assert wired == [DB, REDIS]

    def test_the_liveness_commands_are_the_compose_files(self) -> None:
        compose = yaml.safe_load(
            (_REPO / "deploy" / "docker-compose.yml").read_text(encoding="utf-8"),
        )
        for role in ("worker", "consumer"):
            command = compose["services"][f"comms-{role}"]["healthcheck"]["test"]
            assert command[1:] == ["python", "-m", "app.core.liveness", role]
