"""L-1: with the option `login` off, comms is v4.0.1 (the guard).

A product installs a release of comms with its passport unchanged; with
`login` off nothing of the module may show. Three things are held, each
against v4.0.1 frozen in a fixture:

  - the application's routes (method, path) are v4.0.1's
    (tests/fixtures/routes_v4.0.1.json, taken by _PROBE below from the
    code of the tag ec2ca8d, the same as primer@6bbc1e8's);
  - primer.yaml is v4.0.1's (tests/fixtures/primer_v4.0.1.json: the tag's
    file parsed -- data, comments do not count) and the option: `login`
    in `options` and its two keys in `env`, nothing else;
  - the environment of a v4.0.1 instance plus LOGIN_ENABLED=false and
    the machine's literal in the secret starts, with v4.0.1's routes;
    the secret's value changes nothing that starts (its routes, the
    channel map of /health, every other setting).

The routes are read in a FRESH process that imports app.main under the
instance's environment, as a process of comms starts: `app` is built
once, at import. The walk goes through the routers the application
includes (FastAPI 0.141 keeps them wrapped, `original_router`).

Mutations: a route that is no v4.0.1 route (3.1), a key of `env` without
`when` (3.2), one more option (3.3) -- each red here. The twin of 3.1 --
the same route included only with LOGIN_ENABLED true -- green; since
L-2 that twin is the code (app/main.py), and with the option on the
start of the sign-in is in the table (TestTheRoutesWithLoginOn).
"""

import copy
import json
import subprocess
import sys
from functools import cache
from pathlib import Path
from typing import Any

import pytest
import yaml

from app.core.config import Settings

_REPO = Path(__file__).resolve().parents[1]
_FIXTURES = _REPO / "tests" / "fixtures"
_ROUTES_V401: list[list[str]] = json.loads(
    (_FIXTURES / "routes_v4.0.1.json").read_text(encoding="utf-8"),
)
_PRIMER_V401: dict[str, Any] = json.loads(
    (_FIXTURES / "primer_v4.0.1.json").read_text(encoding="utf-8"),
)
_PRIMER: dict[str, Any] = yaml.safe_load(
    (_REPO / "primer.yaml").read_text(encoding="utf-8"),
)

# The option and what it adds to v4.0.1's primer.yaml (form §8 of the
# contract): one name, two keys.
_LOGIN_ENV = {
    "LOGIN_ENABLED": {"source": "passport", "key": "options.login"},
    "TELEGRAM_LOGIN_CLIENT_SECRET": {"source": "asked", "when": "login"},
}
_NEW_SETTINGS = {key.lower() for key in _LOGIN_ENV}

# What the machine writes into a key of a switched-off option (the
# machine v0.1.8, pridem/stacks/options.py off() and LITERAL).
_LITERAL = "primer-switched-off"
# Assembled at run time (§0 §10): a value the test can find.
_TOKEN = "123456" + ":" + "unit-test-login-guard"
_MARK = "unit" + "-guard-" + "secret-mark"

# The instance's values of v4.0.1's settings that an option does not
# switch, and of the channel keys when the passport switches telegram on.
_VALUES = {
    "COMMS_SERVICE_TOKEN": "unit-test-service-token",
    "LANGUAGES": "en,ru",
    "DEFAULT_LANGUAGE": "en",
    "PRODUCT_STORAGE_ADDRESS": "storage:9000",
    "TELEGRAM_BOT_TOKEN": _TOKEN,
    "TELEGRAM_BOT_URL": "https://t.me/unit_guard_bot",
}
# What compose builds from keys of `env` (tests/test_primer_yaml.py
# COMPOSED): given as built.
_COMPOSED = {
    "DATABASE_URL": "postgresql+asyncpg://u:p@db/comms_unit",
    "REDIS_URL": "redis://:p@redis:6379/0",
}

# Printed by a fresh process: the routes, the channel map of /health and
# every setting but the two new ones. The fixture of the routes was taken
# by this same text on the code of v4.0.1.
_PROBE = """
import json

from app.core.config import settings
from app.main import app, details


def flat(routes, prefix=""):
    found = []
    for route in routes:
        router = getattr(route, "original_router", None)
        if router is not None:
            context = getattr(route, "include_context", None)
            inner = prefix + (getattr(context, "prefix", "") or "")
            found += flat(router.routes, inner)
            continue
        methods = sorted(getattr(route, "methods", None) or ["-"])
        found += [[method, prefix + route.path] for method in methods]
    return found


print(json.dumps({
    "routes": sorted(flat(app.routes)),
    "details": details(),
    "settings": settings.model_dump(mode="json", exclude=EXCLUDE),
}))
"""

# The passports of a v4.0.1 instance tried here: every option off, and
# the channel the module needs on.
_PASSPORTS = {"none": frozenset(), "telegram": frozenset({"telegram"})}


def _instance_env(options: frozenset[str]) -> dict[str, str]:
    """A v4.0.1 instance's environment, as compose gives it a process:
    every key of v4.0.1's `env` that is a setting, the flag of each
    option by the passport, the machine's literal in each key of a
    switched-off option."""
    env = {"PATH": "/usr/bin:/bin", "PYTHONPATH": str(_REPO), **_COMPOSED}
    for key, source in _PRIMER_V401["env"].items():
        if key.lower() not in Settings.model_fields:
            continue
        option = source.get("key", "").removeprefix("options.")
        if source["source"] == "passport" and option != source.get("key"):
            env[key] = "true" if option in options else "false"
        elif source.get("when") is not None and source["when"] not in options:
            env[key] = _LITERAL
        else:
            env[key] = _VALUES[key]
    return env


def _run(code: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=_REPO, env=env, capture_output=True, text=True, timeout=120,
        check=False,
    )


@cache
def _probe(passport: str, secret: str, login: str = "false") -> dict[str, Any]:
    """What a process of comms starts with: the v4.0.1 instance of
    `passport` plus the option's flag `login` (off unless said) and
    `secret`."""
    env = {
        **_instance_env(_PASSPORTS[passport]),
        "LOGIN_ENABLED": login,
        "TELEGRAM_LOGIN_CLIENT_SECRET": secret,
    }
    done = _run(_PROBE.replace("EXCLUDE", repr(_NEW_SETTINGS)), env)
    assert done.returncode == 0, done.stderr
    found: dict[str, Any] = json.loads(done.stdout)
    return found


class TestTheRoutes:
    @pytest.mark.parametrize("passport", sorted(_PASSPORTS))
    def test_with_login_off_the_routes_are_v401s(self, passport: str) -> None:
        """The pair: the walk sees the routes -- v4.0.1 has 34, /health
        among them."""
        routes = _probe(passport, _LITERAL)["routes"]
        assert len(_ROUTES_V401) > 30
        assert ["GET", "/health"] in _ROUTES_V401
        assert routes == _ROUTES_V401


class TestTheRoutesWithLoginOn:
    def test_the_twin_with_login_on_the_sign_in_is_there(self) -> None:
        """The twin of the guard: the same instance with the option on
        (the channel it needs on, a secret set) has v4.0.1's routes and
        the sign-in's two -- start (L-2) and complete (L-3b) --, nothing
        else: the guard above sees the flag, not the file."""
        routes = _probe("telegram", _MARK, "true")["routes"]
        assert routes == sorted([
            *_ROUTES_V401,
            ["POST", "/api/v1/login/complete"],
            ["POST", "/api/v1/login/start"],
        ])


class TestTheStart:
    @pytest.mark.parametrize("passport", sorted(_PASSPORTS))
    def test_the_secret_changes_nothing(self, passport: str) -> None:
        """With the flag false the secret is not read: the machine's
        literal and any other value start the same comms. The pair: the
        passport reaches the start -- telegram's state is its."""
        switched_off = _probe(passport, _LITERAL)
        assert _probe(passport, _MARK) == switched_off
        expected = "live" if passport == "telegram" else "not_configured"
        assert switched_off["details"]["channels"]["telegram"] == expected
        assert _MARK not in json.dumps(switched_off)

    def test_a_v401_environment_alone_is_refused(self) -> None:
        """LACK: the flag is required -- a v4.0.1 instance's environment
        without it is refused, naming it. The machine gives it from the
        passport (primer.yaml `env`)."""
        done = _run("import app.core.config", _instance_env(frozenset()))
        assert done.returncode == 1
        assert "LOGIN_ENABLED is not set" in done.stderr
        assert "Traceback" not in done.stderr


class TestThePrimerYaml:
    def test_it_is_v401s_and_the_option(self) -> None:
        """Data against data: the option and its two keys, nothing else
        -- no other key, no other option, `provides`, `requires`,
        `accepts`, the stack and the report as they were."""
        assert _PRIMER_V401["options"] and _PRIMER_V401["env"]  # the pair
        assert "login" not in _PRIMER_V401["options"]
        assert set(_LOGIN_ENV).isdisjoint(_PRIMER_V401["env"])
        expected = copy.deepcopy(_PRIMER_V401)
        expected["options"] = [*expected["options"], "login"]
        expected["env"] = {**expected["env"], **_LOGIN_ENV}
        assert expected == _PRIMER
