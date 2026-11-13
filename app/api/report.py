# =============================================================================
# COMMS Service -- what comms shows the deploy machine (protocol 9, W3-1)
# =============================================================================
#
# GET /health -- always 200, the document of protocol 9.1, from a healthy
#   and from a degraded comms alike: the machine reads it after `up`
#   (its conformance of the stack, and `seams_verify`); `status` is `ok`
#   only when every check is.
# GET /ready  -- 200 {status: ready, failing: []} when every check is ok,
#   else 503 {status: not_ready, failing: [names]} (protocol 9.2). The
#   compose healthcheck of comms-app reads it.
# Both are open: the machine and docker hold no token
# (app/api/service_auth.py OPEN_PATHS). deploy/INTEGRATION.md section 15
# is the contract.
#
# THE MODEL is primer-blank's primer_report/health.py (754f856), written
# here rather than taken: it is a directory of that repository, not a
# package; a git dependency carries no hash, and comms installs every
# package --require-hashes; and it brings a request id and a log
# formatter of its own, where comms has its own (W3-2). What holds the
# form to the machine's is one file, tests/fixtures/health.json: the suite
# holds a live answer to it by key and type (tests/test_report.py), and
# CI hands it to the machine's own check_health and _seams
# (scripts/check_primer_yaml.py, job `machine`).
#
# THE RULES OF THE MODEL, on comms:
#   - every request measures anew; the checks run AT ONCE, each under
#     CHECK_TIMEOUT_S, and one that raises or does not answer in time is
#     `error` while the others are still measured (protocol 9.5);
#   - `checks` hold what comms does not work without: the database and
#     Redis. The database check IS the read of its revision -- one query,
#     whose answer is also `alembic_head`; no revision read, `db: error`;
#   - the last revision read is kept between requests: a database that
#     falls after it was measured reports `db: error` with that revision;
#     before the first read `alembic_head` is null;
#   - a check that turns to `error` is logged once (`health_check_failed`:
#     the check, and the class and place of what stopped it -- a database
#     error's SQLSTATE too, never a text), and again only after it was
#     `ok` in between;
#   - `details` is built from memory on every /health; one that is not a
#     JSON object or is over DETAILS_LIMIT bytes is left out of the answer
#     and named in the log (`health_details_refused`).
# Which callables measure what is app/main.py's: the redis client is the
# transport's (app/transport/connection.py), a rank this package does not
# import (app/__init__.py).
# =============================================================================

import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Literal

import structlog
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    ValidationError,
    field_validator,
)

from app.core.config import settings
from app.core.constants import MAX_REQUEST_BODY_BYTES
from app.core.logging import RELEASE_VARIABLE
from app.core.sanitize import db_error_summary, pipeline_error_of

logger = structlog.get_logger()

CheckState = Literal["ok", "error"]

SERVICE = "comms"
"""The stack's name: `name` of primer.yaml, which the machine compares
`service` with (tests/test_report.py holds the two)."""

EDITION = "core"
"""A service has no editions in contract 3 -- its passport chooses none and
the machine passes no PRIMER_EDITION to a service -- and protocol 9.1 asks
for a non-empty one: comms has the one edition, as primer-blank names it."""

MODULES: tuple[str, ...] = ()
"""The modules of that edition: none. The channels a deploy has are in
`details` (its channel map)."""

PROVIDES: tuple[str, ...] = (
    "COMMS_API_URL",
    "COMMS_SERVICE_TOKEN",
    "COMMS_EVENTS_ADDRESS",
    "COMMS_EVENTS_PASSWORD",
    "TELEGRAM_BOT_URL",
)
"""`provides` of primer.yaml, in its order: what the machine checks every
seam of the product from comms against (`seams_verify`). The runtime image
does not carry primer.yaml; tests/test_report.py holds the two equal."""

REQUIRES: tuple[str, ...] = ("PRODUCT_STORAGE_ADDRESS",)
"""`requires` of primer.yaml: the product's storage (W4-2), the one seam
comms takes from the product. `requires_satisfied` is _requires_satisfied
below; tests/test_report.py holds this list to the file."""

MIB = 1024 * 1024
MAX_UPLOAD_MB = MAX_REQUEST_BODY_BYTES // MIB
"""The largest body comms takes, in the unit of `limits` (MiB, protocol
9.1): MAX_REQUEST_BODY_BYTES is a whole number of them (tests/test_report.py)."""

DB = "db"
REDIS = "redis"

CHECK_TIMEOUT_S = 2.0
"""How long one check may take before it is `error` (protocol 9.5: 1--2 s)."""

DETAILS_LIMIT = 8192
"""The most `details` may take: bytes of its compact JSON in UTF-8 -- the
measure the deploy machine checks it with (protocol 9.1)."""

COMMIT_VARIABLE = "PRIMER_COMMIT"
"""The commit the image was built from (W1-1), the `commit` of /health."""

_DETAILS: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])


def free_size(value: JsonValue) -> int:
    """The bytes of `value` as compact JSON in UTF-8."""
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return len(text.encode("utf-8"))


def commit_from_environment() -> str:
    """PRIMER_COMMIT, or the API refuses to start naming it (app/main.py
    lifespan) -- the form of app/core/logging.py release_from_environment:
    /health names the commit, and a default would say something false.
    Empty is absent."""
    commit = os.environ.get(COMMIT_VARIABLE, "")
    if not commit:
        raise SystemExit(
            f"comms: startup refused -- {COMMIT_VARIABLE} is not set: /health "
            f"names the commit (protocol 9.1). The image carries it from its "
            f"build argument (Dockerfile); a process started outside the "
            f"image sets it itself."
        )
    return commit


class _Closed(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Seams(_Closed):
    """What comms provides to others and whether its requires are met."""

    provides: list[str]
    requires_satisfied: bool


class Limits(_Closed):
    """Limits of the live application the machine compares host knobs with."""

    max_upload_mb: int = Field(ge=0)


class Health(_Closed):
    """The /health document of protocol 9.1; `details` is optional (an
    answer without it leaves the key out)."""

    service: str = Field(min_length=1)
    release: str = Field(min_length=1)
    commit: str = Field(min_length=1)
    alembic_head: str | None
    uptime_s: int = Field(ge=0, strict=True)
    status: Literal["ok", "degraded"]
    checks: dict[str, CheckState] = Field(min_length=1)
    seams: Seams
    limits: Limits
    edition: str = Field(min_length=1)
    modules: list[str]
    details: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("details")
    @classmethod
    def _bounded(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        size = free_size(value)
        if size > DETAILS_LIMIT:
            raise ValueError(f"details takes {size} bytes, over {DETAILS_LIMIT}")
        return value


class Ready(_Closed):
    """The /ready document: ready, or the names of the failing checks."""

    status: Literal["ready", "not_ready"]
    failing: list[str]


async def _within[T](call: Callable[[], Awaitable[T]]) -> T | Exception:
    """The result of `call`, or what stopped it -- a timeout is one."""
    try:
        return await asyncio.wait_for(call(), CHECK_TIMEOUT_S)
    except Exception as error:  # every failure of a check is its `error`
        return error


def _requires_satisfied() -> bool:
    """The product's storage is given and reads as an address: off the
    validated settings, where a malformed one refused startup -- so a
    seam that is written is one comms builds its body links on
    (Settings.body_link_prefix), and an empty one is not satisfied."""
    return settings.body_link_prefix is not None


def _seams() -> Seams:
    return Seams(
        provides=list(PROVIDES),
        requires_satisfied=_requires_satisfied(),
    )


class Report:
    """The two routes of protocol 9 over comms' checks.

    `db` reads the database's revision in one query and raises when there
    is none to read; `checks` are the other checks, each a coroutine
    answering True when its dependency answers. `details` builds the free
    object of /health from memory -- no database, no network. A check is a
    coroutine that can be cancelled: one that blocks the event loop is not
    held by the timeout.
    """

    def __init__(
        self,
        *,
        db: Callable[[], Awaitable[str]],
        checks: Mapping[str, Callable[[], Awaitable[bool]]],
        details: Callable[[], Mapping[str, JsonValue]],
    ) -> None:
        if DB in checks:
            raise ValueError(f"{DB!r} is the revision's check, not another")
        self._db = db
        self._checks = dict(checks)
        self._details = details
        self._last_head: str | None = None
        self._failed: set[str] = set()
        self._started = time.monotonic()
        # A wrong fixed field fails the import, not every request.
        Health.model_validate(
            {
                **self._fixed(),
                "release": "-",
                "commit": "-",
                "alembic_head": None,
                "uptime_s": 0,
                "status": "ok",
                "checks": dict.fromkeys([DB, *self._checks], "ok"),
            }
        )
        self.router = APIRouter()
        self.router.add_api_route("/health", self.health, methods=["GET"])
        self.router.add_api_route("/ready", self.ready, methods=["GET"])

    @staticmethod
    def _fixed() -> dict[str, object]:
        return {
            "service": SERVICE,
            "edition": EDITION,
            "modules": list(MODULES),
            "limits": Limits(max_upload_mb=MAX_UPLOAD_MB),
            "seams": _seams(),
        }

    def reset_turns(self) -> None:
        """Forget which checks were `error` (tests): the next failure of
        each is a turn again, and logged."""
        self._failed.clear()

    async def _measure(self) -> dict[str, CheckState]:
        """Every check once, at the same time; the revision read is kept."""
        revision, *outcomes = await asyncio.gather(
            _within(self._db), *(_within(check) for check in self._checks.values())
        )
        results: dict[str, bool | Exception | None] = {}
        if isinstance(revision, str) and revision:
            self._last_head = revision
            results[DB] = True
        else:
            results[DB] = revision if isinstance(revision, Exception) else None
        for name, outcome in zip(self._checks, outcomes, strict=True):
            results[name] = (
                outcome if isinstance(outcome, Exception) else outcome is True or None
            )
        return {name: self._state(name, outcome) for name, outcome in results.items()}

    def _state(self, name: str, outcome: bool | Exception | None) -> CheckState:
        """`ok` for True; anything else is `error`, logged on the turn."""
        if outcome is True:
            self._failed.discard(name)
            return "ok"
        if name not in self._failed:
            self._failed.add(name)
            logger.warning(
                "health_check_failed",
                check=name,
                error=(
                    (db_error_summary(outcome) or pipeline_error_of(outcome))
                    if isinstance(outcome, Exception)
                    else None
                ),
            )
        return "error"

    def _free(self) -> dict[str, JsonValue] | None:
        """`details` for this answer, or None when there is none to send."""
        try:
            built = self._details()
        except Exception as error:  # a failing builder leaves details out
            logger.warning(
                "health_details_refused", reason="error", error=pipeline_error_of(error)
            )
            return None
        try:
            value = _DETAILS.validate_python(built, strict=True)
            size = free_size(value)
        except (ValidationError, TypeError, ValueError, RecursionError):
            logger.warning("health_details_refused", reason="type")
            return None
        if size > DETAILS_LIMIT:
            logger.warning("health_details_refused", reason="size", size=size)
            return None
        return value

    async def health(self) -> JSONResponse:
        """The full document; 200 whatever the state."""
        checks = await self._measure()
        document: dict[str, object] = {
            **self._fixed(),
            "release": os.environ.get(RELEASE_VARIABLE, ""),
            "commit": os.environ.get(COMMIT_VARIABLE, ""),
            "alembic_head": self._last_head,
            "uptime_s": int(time.monotonic() - self._started),
            "status": "ok" if all(s == "ok" for s in checks.values()) else "degraded",
            "checks": checks,
        }
        details = self._free()
        if details is not None:
            document["details"] = details
        body = Health.model_validate(document).model_dump(exclude_unset=True)
        return JSONResponse(body, status_code=200)

    async def ready(self) -> JSONResponse:
        """200 when every check passes, else 503 with the failing names."""
        checks = await self._measure()
        failing = sorted(name for name, state in checks.items() if state != "ok")
        body = Ready(status="not_ready" if failing else "ready", failing=failing)
        return JSONResponse(body.model_dump(), status_code=503 if failing else 200)
