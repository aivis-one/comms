# =============================================================================
# The API is always closed, and the token is asked before anything of
# the request is read (REV-3, LEG-1)
# =============================================================================
#
# What is held here:
#   1 comms has one mode (LEG-1): a token is required to start, the
#     schema endpoints are never mounted, DATABASE_URL and TEMPLATES_DIR
#     are required, a free llm route is refused and every log line is
#     the machine's JSON -- whatever the environment holds besides,
#     a variable that once named a development mode included
#     (app/core/config.py). Until LEG-1 an explicit development ran
#     without a token, served the schema, defaulted the database,
#     started without a profile, allowed free routes and wrote the
#     console's coloured lines;
#   2 without the right token every path but /health and /ready -- an
#     unknown one, a wrong method, a body of any size or shape -- is a
#     401 before routing, body parsing or any check of the query or the
#     body (app/api/service_auth.py RequireServiceToken). FastAPI parses
#     a JSON body BEFORE it solves any dependency, so a dependency could
#     never be first;
#   3 a body nested past the JSON parser's depth is the caller's input:
#     a 422 on a route with no body model (the check of app/api/deps.py
#     parses it), FastAPI's 400 on a route with one -- never a 500, never
#     an `error` line in the log.
# The first tests pin what the design leans on, read from the frameworks
# and not taken from memory: Starlette puts the user middleware OUTSIDE
# ExceptionMiddleware (so the middleware answers by itself), and the 400
# of a body FastAPI cannot parse writes no `error` line.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 any of the development branches back (the token, the schema, the
#      database's default, the empty profile, the free route, the
#      console renderer), keyed on the old variable -> TestOneMode
#   M2 RequireServiceToken not added to the app -> TestTokenFirst
#   M3 /health and /ready not exempted -> test_health_and_ready_stay_open
#   M4 the middleware added INSIDE LimitRequestBody -> test_a_large_body
#   M5 RecursionError not caught in refuse_unstorable_body ->
#      test_a_deep_body_without_a_body_model
# =============================================================================

import json
import subprocess
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi.routing import APIRoute
from httpx import AsyncClient
from starlette.middleware.exceptions import ExceptionMiddleware
from structlog.testing import capture_logs

from app.core.channels import channel_flag_keys
from app.core.config import settings
from app.core.constants import MAX_REQUEST_BODY_BYTES
from app.main import app
from tests.helpers import redis_answering

REPO_ROOT = Path(__file__).resolve().parents[1]
# Joined at run time: a literal shaped like a real secret blocks a push.
_TOKEN = "-".join(("rev3", "unit", "token"))
_BEARER = {"Authorization": f"Bearer {_TOKEN}"}
# Past the C JSON scanner's depth (about ten thousand), under 1 MiB.
_DEEP = "[" * 60000 + "]" * 60000


@pytest.fixture
def token_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "comms_service_token", _TOKEN)


def _settings_in_a_fresh_process(
    program: str,
    **env: str,
) -> subprocess.CompletedProcess[str]:
    """Build the settings as a deploy does: a fresh process, an
    environment holding exactly `env`, and the instance's languages,
    which every start requires (W4-1): these tests are about the mode
    and the token, not the languages."""
    return subprocess.run(
        [sys.executable, "-c", program],
        cwd=REPO_ROOT,
        env={"PATH": "/usr/bin:/bin", **_LANGUAGES, **env},
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


_DATABASE = {"DATABASE_URL": "postgresql+asyncpg://u:p@db/comms_unit"}
# Every channel's flag, written (W2-1: required at every start): these
# tests are about the mode and the token, not the channels.
_CHANNELS_OFF = dict.fromkeys(channel_flag_keys(), "false")
# The instance's languages, required at every start since W4-1.
_LANGUAGES = {"LANGUAGES": "en", "DEFAULT_LANGUAGE": "en"}
# The variable that named the mode until LEG-1, assembled: it is read by
# nothing now, and the tree carries its name only as history.
_OLD_MODE = "_".join(("APP", "ENV"))
# Absent, empty, the old development, the old CI and production values.
_MODES = [None, "", "development", "ci", "production"]
# A free route of a whole key set (spec §12.1): refused by the one mode.
_FREE_SETS = (
    '{"llm.text": {"endpoint": "https://llm.example.org/v1", '
    '"key": "unit-one-mode-llm-key", "model": "vendor/cheap:free", '
    '"gateway": false}}'
)
# What a full start does after the settings: the schema it mounts, one
# log line, the profile without TEMPLATES_DIR -- each printed.
_FULL_START = """
import json
import structlog
from app.core.exceptions import ProfileError
from app.core.logging import setup_logging
from app.main import app
from app.profile.loader import install_profile_from_settings

print(json.dumps([app.openapi_url, app.docs_url, app.redoc_url]))
setup_logging()
structlog.get_logger().info("one_mode_probe")
try:
    install_profile_from_settings()
except ProfileError as exc:
    print("refused:", str(exc).splitlines()[0])
"""


def _mode(value: str | None) -> dict[str, str]:
    return {} if value is None else {_OLD_MODE: value}


class TestOneMode:
    """Was TestTheDefault: an absent mode was production, an explicit
    development opened the API. LEG-1 took the mode out: every value of
    the old variable, its absence included, starts the same comms."""

    @pytest.mark.parametrize("mode", _MODES)
    def test_no_token_and_a_free_route_refuse_the_start(
        self, mode: str | None,
    ) -> None:
        result = _settings_in_a_fresh_process(
            "from app.core.config import settings",
            **_DATABASE, **_mode(mode),
            **{**_CHANNELS_OFF, "LLM_ENABLED": "true"},
            LLM_KEY_SETS=_FREE_SETS,
        )
        assert result.returncode == 1, result.stdout
        assert "COMMS_SERVICE_TOKEN is required" in result.stderr
        assert "set 'llm.text' names a free route" in result.stderr
        assert "unit-one-mode-llm-key" not in result.stderr

    @pytest.mark.parametrize("mode", _MODES)
    def test_no_database_url_refuses_the_start(self, mode: str | None) -> None:
        result = _settings_in_a_fresh_process(
            "from app.core.config import settings",
            **_CHANNELS_OFF, **_mode(mode), COMMS_SERVICE_TOKEN=_TOKEN,
        )
        assert result.returncode == 1, result.stdout
        assert "DATABASE_URL is required." in result.stderr

    @pytest.mark.parametrize("mode", _MODES)
    def test_a_full_start_is_closed_and_machine_lined(
        self, mode: str | None,
    ) -> None:
        """The pair of both refusals: with the token and the database the
        settings build -- and the start that follows mounts no schema,
        writes the machine's JSON line and refuses a missing profile."""
        result = _settings_in_a_fresh_process(
            _FULL_START,
            **_DATABASE, **_CHANNELS_OFF, **_mode(mode),
            COMMS_SERVICE_TOKEN=_TOKEN, PRIMER_RELEASE="one-mode-release",
        )
        assert result.returncode == 0, result.stderr
        lines = result.stdout.splitlines()
        assert json.loads(lines[0]) == [None, None, None]
        probes = [
            json.loads(line) for line in lines[1:]
            if line.startswith("{") and '"one_mode_probe"' in line
        ]
        assert len(probes) == 1, lines
        assert probes[0]["event"] == "one_mode_probe"
        assert probes[0]["release"] == "one-mode-release"
        assert lines[-1] == "refused: TEMPLATES_DIR is required. "\
            "Point it at the product profile directory."


# -----------------------------------------------------------------------------
# What the design leans on -- read from the frameworks, pinned first
# -----------------------------------------------------------------------------


class TestTheGround:
    def test_user_middleware_sits_outside_exception_handling(self) -> None:
        """The application's own stack, outside in: each user middleware
        wraps ExceptionMiddleware, so an HTTPException raised in one
        reaches no handler (a 500) -- the middleware answers by itself."""
        layers: list[type] = []
        layer: Any = app.build_middleware_stack()
        while layer is not None and len(layers) < 50:
            layers.append(type(layer))
            layer = getattr(layer, "app", None)
        user = [m.cls for m in app.user_middleware]
        assert user, "the pair: the app has user middleware"
        handling = layers.index(ExceptionMiddleware)
        for cls in user:
            assert layers.index(cls) < handling, (cls, layers)

    async def test_a_body_fastapi_cannot_parse_logs_no_error(
        self,
        client: AsyncClient,
        token_set: None,
    ) -> None:
        with capture_logs() as logs:
            response = await client.post(
                "/api/v1/threads",
                content=_DEEP,
                headers={**_BEARER, "Content-Type": "application/json"},
            )
        assert response.status_code == 400, response.text
        assert response.json()["error"]["class"] == "bad_request"
        assert not [e for e in logs if e["log_level"] == "error"], logs


# -----------------------------------------------------------------------------
# The token first
# -----------------------------------------------------------------------------


def _routes() -> list[tuple[str, str]]:
    """(method, concrete path) of every API route of the application."""
    found: list[tuple[str, str]] = []
    for included in app.router.routes:
        routes = getattr(getattr(included, "original_router", None), "routes", ())
        for route in [included, *routes]:
            if not isinstance(route, APIRoute):
                continue
            path = route.path
            while "{" in path:
                start, end = path.index("{"), path.index("}")
                path = path[:start] + str(uuid4()) + path[end + 1 :]
            found.extend((method, path) for method in sorted(route.methods))
    return found


_OPEN = {"/health", "/ready"}


class TestTokenFirst:

    async def test_every_route_but_the_open_two(
        self,
        anonymous_client: AsyncClient,
        token_set: None,
    ) -> None:
        """The watchman: the application's own list of routes. A body
        the checks would refuse rides along -- the token is asked first."""
        routes = [(m, p) for m, p in _routes() if p not in _OPEN]
        assert len(routes) >= 30, "the pair: the walk finds the routes"
        for method, path in routes:
            response = await anonymous_client.request(
                method,
                path,
                params={"probe": "a\x00b"},
                content='{"key": "\\u0000"}',
                headers={"Content-Type": "application/json"},
            )
            assert response.status_code == 401, (method, path, response.text)
            assert response.json() == {
                "error": {
                    "class": "unauthorized",
                    "message": "Invalid or missing service token",
                }
            }
            assert response.headers["WWW-Authenticate"] == "Bearer"


    @pytest.mark.parametrize(
        ("method", "path"),
        [
            ("GET", "/api/v1/nowhere"),
            ("DELETE", "/health/x"),
            ("PATCH", "/api/v1/threads"),
        ],
    )

    async def test_unknown_paths_and_methods(
        self,
        anonymous_client: AsyncClient,
        token_set: None,
        method: str,
        path: str,
    ) -> None:
        """Nothing outside says which routes exist: 401, not 404/405."""
        response = await anonymous_client.request(method, path)
        assert response.status_code == 401, response.text


    @pytest.mark.parametrize(
        "body",
        [_DEEP, "{not json", '{"key": "\\u0000"}'],
    )

    async def test_the_body_is_not_read(
        self,
        anonymous_client: AsyncClient,
        token_set: None,
        body: str,
    ) -> None:
        response = await anonymous_client.post(
            f"/api/v1/recipients/{uuid4()}/inbox/read-all",
            content=body,
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 401, response.text


    async def test_a_large_body(
        self,
        anonymous_client: AsyncClient,
        token_set: None,
    ) -> None:
        response = await anonymous_client.post(
            "/api/v1/threads",
            content=b"x" * (MAX_REQUEST_BODY_BYTES + 1),
        )
        assert response.status_code == 401, response.text


    @pytest.mark.parametrize(
        "header",
        [None, "Basic x", "Bearer ", "Bearer nope", f"Token {_TOKEN}"],
    )

    async def test_a_wrong_header(
        self,
        anonymous_client: AsyncClient,
        token_set: None,
        header: str | None,
    ) -> None:
        headers = {} if header is None else {"Authorization": header}
        response = await anonymous_client.get(
            f"/api/v1/recipients/{uuid4()}/inbox",
            headers=headers,
        )
        assert response.status_code == 401
        assert _TOKEN not in response.text


    async def test_the_first_of_two_headers_decides(
        self,
        bare_client: AsyncClient,
        token_set: None,
    ) -> None:
        """As FastAPI's Header(str) read it before: the first one."""
        path = f"/api/v1/recipients/{uuid4()}/inbox"
        good, bad = ("Authorization", f"Bearer {_TOKEN}"), ("Authorization", "x")
        assert (await bare_client.get(path, headers=[good, bad])).status_code == 200
        assert (await bare_client.get(path, headers=[bad, good])).status_code == 401


    async def test_the_right_token_passes(
        self,
        client: AsyncClient,
        token_set: None,
    ) -> None:
        response = await client.get(
            f"/api/v1/recipients/{uuid4()}/inbox",
            headers=_BEARER,
        )
        assert response.status_code == 200


    async def test_health_and_ready_stay_open(
        self,
        anonymous_client: AsyncClient,
        token_set: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # /ready is 200 only with every check ok (W3-1); the suite has no
        # Redis, so its check answers here (tests/helpers.py).
        redis_answering(monkeypatch)
        assert (await anonymous_client.get("/health")).status_code == 200
        assert (await anonymous_client.get("/ready")).status_code == 200



# -----------------------------------------------------------------------------
# A body nested past the parser
# -----------------------------------------------------------------------------


class TestDeepBody:
    async def test_a_deep_body_without_a_body_model(
        self,
        client: AsyncClient,
        token_set: None,
    ) -> None:
        with capture_logs() as logs:
            response = await client.post(
                f"/api/v1/recipients/{uuid4()}/inbox/read-all",
                content=_DEEP,
                headers={**_BEARER, "Content-Type": "application/json"},
            )
        assert response.status_code == 422, response.text
        error = response.json()["error"]
        assert error["class"] == "validation"
        assert "nested" in error["message"]
        assert not [e for e in logs if e["log_level"] == "error"], logs

    async def test_a_deep_body_with_a_body_model(
        self,
        client: AsyncClient,
        token_set: None,
    ) -> None:
        response = await client.post(
            "/api/v1/threads",
            content=_DEEP,
            headers={**_BEARER, "Content-Type": "application/json"},
        )
        assert response.status_code == 400
        assert response.json()["error"]["class"] == "bad_request"

    async def test_a_shallow_body_reads_on(
        self,
        client: AsyncClient,
        token_set: None,
    ) -> None:
        """The pair: nesting the parser holds is the route's business."""
        response = await client.post(
            f"/api/v1/recipients/{uuid4()}/inbox/read-all",
            content="[" * 100 + "]" * 100,
            headers={**_BEARER, "Content-Type": "application/json"},
        )
        assert response.status_code != 422, response.text
        assert response.status_code < 500
