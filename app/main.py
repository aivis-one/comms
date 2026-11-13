# =============================================================================
# COMMS Service -- Application Entry Point
# =============================================================================
#
# FastAPI application: /health and /ready of protocol 9 (unversioned,
# UNAUTHENTICATED -- the deploy machine and docker hold no secret; their
# form and rules are app/api/report.py, what they measure is wired here)
# plus the internal product-facing API under /api/v1 (app/api/), every
# router guarded by the service-to-service bearer token (Phase 3b). The
# routers are the include_router block below -- that block is the list;
# deploy/INTEGRATION.md is the contract.
#
# The notification worker does NOT run inside this process (unlike the
# donors, which ticked daemons in the API lifespan). It is a separate
# process from the same image: `python -m app.worker`.
#
# HEALTH vs READY (W3-1, protocol 9.1, 9.2, 9.5):
#   /health -- always 200, the machine's document: checks `db` and
#              `redis`, the seams, the release and the commit of the
#              image, the channel map under `details`;
#   /ready  -- 503 while a check is not ok.
# The `db` check reads the database's revision (alembic_version) -- one
# query, also `alembic_head` -- as a SQLAlchemy expression, not a raw-SQL
# text() string, per the no-raw-SQL house rule. The `redis` check is a
# PING of comms' own Redis (REDIS_URL, which compose gives comms-app for
# this alone): the client is built on the first check and closed with the
# process. An empty REDIS_URL -- a development, the suite -- is `redis:
# error`, not a refused start.
# =============================================================================

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import Depends, FastAPI
from pydantic import JsonValue
from redis.asyncio import Redis
from sqlalchemy import column, select, table

from app.api.body_limit import LimitRequestBody
from app.api.changes import router as changes_router
from app.api.channels import router as channels_router
from app.api.deps import (
    refuse_repeated_query,
    refuse_unstorable_body,
    refuse_unstorable_query,
)
from app.api.errors import register_error_handlers
from app.api.inbox import router as inbox_router
from app.api.jobs import router as jobs_router
from app.api.lanes import router as lanes_router
from app.api.messaging import participants_router, sections_router
from app.api.messaging import router as messaging_router
from app.api.prefs import router as prefs_router
from app.api.recipients import router as recipients_router
from app.api.report import REDIS, Report, commit_from_environment
from app.api.request_id import RequestId
from app.api.service_auth import RequireServiceToken
from app.core.config import APP_VERSION, settings
from app.core.database import (
    ProcessRole,
    dispose_engine,
    get_engine,
    set_process_role,
)
from app.core.logging import setup_logging
from app.engine.formatters import channel_map, close_formatters, llm_set_map
from app.profile.loader import install_profile_from_settings
from app.transport.connection import redis_client

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: logging + profile on startup, cleanup on stop.

    A broken profile raises ProfileError here and the process dies
    before serving traffic -- by design (fail at startup, not on the
    delivery path).

    Each uvicorn worker is its own process and runs this before it
    serves: the role sizes its pool (app/core/database.py POOLS).
    """
    setup_logging()
    # The commit /health names: without it the API refuses to start, as
    # without its release (setup_logging) -- W3-1.
    commit_from_environment()
    set_process_role(ProcessRole.API)
    install_profile_from_settings()
    if not settings.comms_service_token:
        # Only reachable in development: any other APP_ENV refuses to
        # start without the token (app/core/config.py). Loud on purpose
        # -- an open "internal" API must be a visible choice, not a
        # silent default.
        logger.warning(
            "service_auth_disabled",
            reason="COMMS_SERVICE_TOKEN is empty",
            env=settings.app_env,
        )
    # The channel map is the operator's view of the channel rule
    # (app/core/channels.py): live / not_configured / not_implemented.
    logger.info(
        "comms_started",
        version=APP_VERSION,
        env=settings.app_env,
        channels=channel_map(settings),
        llm_sets=llm_set_map(settings),
    )
    yield
    await close_formatters()
    await close_redis()
    await dispose_engine()
    logger.info("comms_stopped")


# THE INTERACTIVE SCHEMA IS A DEVELOPMENT TOOL, NOT A PRODUCT SURFACE.
# FastAPI serves /docs, /redoc and /openapi.json as routes of their own
# (behind the service token like every path, when one is set), and what
# they publish is every route's docstring. Those docstrings
# are written for us: they carry KNOWN CEILING blocks, release markers
# and, on one route, the sentence describing a read-authz bypass --
# text for the next developer, published to anyone who asks.
#
# An integrator does not need them: deploy/INTEGRATION.md is the
# contract, and it is written for that reader. So outside an explicit
# development (an absent APP_ENV is production) the three endpoints do
# not exist at all -- openapi_url=None also
# removes /docs and /redoc, and the explicit Nones say so to the next
# reader rather than leaving it to be discovered.
def docs_urls(is_dev: bool) -> dict[str, str | None]:
    """The schema endpoints FastAPI should mount, or None for each.

    A function rather than three inline conditionals so that both
    branches can be asserted without re-importing this module under
    patched settings: the closed case is checked against the running
    app, the open one against this.
    """
    return {
        "openapi_url": "/openapi.json" if is_dev else None,
        "docs_url": "/docs" if is_dev else None,
        "redoc_url": "/redoc" if is_dev else None,
    }


_DOCS = docs_urls(settings.is_dev)

app = FastAPI(
    title="COMMS Service",
    version=APP_VERSION,
    lifespan=lifespan,
    openapi_url=_DOCS["openapi_url"],
    docs_url=_DOCS["docs_url"],
    redoc_url=_DOCS["redoc_url"],
    # Before every route: a key given twice is refused, not read as its
    # last value; a query or body string the database cannot store is a
    # 422, not the driver's 500 (app/api/deps.py).
    dependencies=[
        Depends(refuse_repeated_query),
        Depends(refuse_unstorable_query),
        Depends(refuse_unstorable_body),
    ],
)
register_error_handlers(app)
# Every request's body bounded at MAX_REQUEST_BODY_BYTES, refused 413
# before a byte past it is read or parsed (app/api/body_limit.py, PERF-3).
app.add_middleware(LimitRequestBody)
# The service token, asked before routing and before the body is read,
# counted, parsed or checked; /health and /ready alone are open
# (app/api/service_auth.py, REV-3). Outside the body limit.
app.add_middleware(RequireServiceToken)
# The request id (protocol 9.6), before anything else of comms looks at
# the request: the 401 and the 413 carry it too (app/api/request_id.py).
# Added last, the outermost.
app.add_middleware(RequestId)
app.include_router(inbox_router)
app.include_router(jobs_router)
app.include_router(changes_router)
app.include_router(prefs_router)
app.include_router(messaging_router)
app.include_router(sections_router)
app.include_router(participants_router)
app.include_router(recipients_router)
app.include_router(channels_router)
app.include_router(lanes_router)


_ALEMBIC_VERSION = table("alembic_version", column("version_num"))


async def read_revision() -> str:
    """The `db` check: the database's revision, in one query; raises when
    there is none to read (no alembic_version, an empty one)."""
    engine = get_engine()
    async with engine.connect() as conn:
        revision = await conn.scalar(select(_ALEMBIC_VERSION.c.version_num))
    if not isinstance(revision, str) or not revision:
        raise LookupError("alembic_version holds no revision")
    return revision


_redis: Redis | None = None


async def redis_answers() -> bool:
    """The `redis` check: comms' own Redis answers a PING. Raises when
    REDIS_URL is empty (the header)."""
    global _redis
    if not settings.redis_url:
        raise LookupError("REDIS_URL is not set")
    if _redis is None:
        _redis = redis_client()
    return bool(await _redis.ping())


async def close_redis() -> None:
    """Close the check's client, if a check built one."""
    global _redis
    if _redis is not None:
        client, _redis = _redis, None
        await client.aclose()


def details() -> dict[str, JsonValue]:
    """The free object of /health: the channel map -- the same map as the
    startup log (comms_started), so confirming what a deploy has needs a
    request rather than a grep. STATES ONLY, never a key's value: the
    route is unauthenticated."""
    channels: dict[str, JsonValue] = dict(channel_map(settings))
    return {"channels": channels}


# Each callable looked up when it runs, so a test replaces it by name here.
report = Report(
    db=lambda: read_revision(),
    checks={REDIS: lambda: redis_answers()},
    details=lambda: details(),
)
app.include_router(report.router)
