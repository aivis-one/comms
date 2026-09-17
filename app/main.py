# =============================================================================
# COMMS Service -- Application Entry Point
# =============================================================================
#
# Minimal FastAPI application: /health and /ready (unversioned,
# UNAUTHENTICATED -- installer/docker healthchecks carry no secret)
# plus the internal product-facing API under /api/v1 (app/api/):
# inbox (the in-app bell) and the E8 preferences facade, both guarded
# by the service-to-service bearer token (Phase 3b).
#
# The notification worker does NOT run inside this process (unlike the
# donors, which ticked daemons in the API lifespan). It is a separate
# process from the same image: `python -m app.worker`.
#
# HEALTH vs READY:
#   /health -- always 200; reports dependency status.
#   /ready  -- 503 when the database is unreachable.
#
# ORM-ONLY NOTE: the DB probe uses select(1) (a SQLAlchemy expression),
# not a raw-SQL text("SELECT 1") string, per the no-raw-SQL house rule.
# =============================================================================

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import select

from app.api.errors import register_error_handlers
from app.api.inbox import router as inbox_router
from app.api.messaging import participants_router, sections_router
from app.api.messaging import router as messaging_router
from app.api.prefs import router as prefs_router
from app.api.recipients import router as recipients_router
from app.core.config import APP_VERSION, settings
from app.core.database import dispose_engine, get_engine
from app.core.logging import setup_logging
from app.engine.formatters import channel_map, close_formatters
from app.profile.loader import install_profile_from_settings

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: logging + profile on startup, cleanup on stop.

    A broken profile raises ProfileError here and the process dies
    before serving traffic -- by design (fail at startup, not on the
    delivery path).
    """
    setup_logging()
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
    )
    yield
    await close_formatters()
    await dispose_engine()
    logger.info("comms_stopped")


# THE INTERACTIVE SCHEMA IS A DEVELOPMENT TOOL, NOT A PRODUCT SURFACE.
# FastAPI serves /docs, /redoc and /openapi.json with no authentication
# of their own -- require_service_auth guards the routers, not these --
# and what they publish is every route's docstring. Those docstrings
# are written for us: they carry KNOWN CEILING blocks, release markers
# and, on one route, the sentence describing a read-authz bypass. Five
# of the twenty-one operations carry such text today.
#
# An integrator does not need them: deploy/INTEGRATION.md is the
# contract, and it is written for that reader. So outside development
# the three endpoints do not exist at all -- openapi_url=None also
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
)
register_error_handlers(app)
app.include_router(inbox_router)
app.include_router(prefs_router)
app.include_router(messaging_router)
app.include_router(sections_router)
app.include_router(participants_router)
app.include_router(recipients_router)


async def _db_ok() -> bool:
    """Probe database connectivity via an ORM expression."""
    try:
        engine = get_engine()
        async with engine.connect() as conn:
            await conn.execute(select(1))
    except Exception:
        return False
    return True


@app.get("/health")
async def health() -> JSONResponse:
    """Health check -- always returns 200, reports DB and channels.

    THE CHANNEL MAP IS HERE AS WELL AS IN THE STARTUP LOG, and it is
    the same map (channel_map): deploy/INTEGRATION.md asks the operator
    to compare what the deploy has against what the installer meant to
    give it, and a detector reachable only by grepping a container's
    log is half a detector. One map in two places, never a second
    vocabulary for the same three states.

    STATES ONLY, NEVER A KEY VALUE: this endpoint carries no
    authentication on purpose (installer and docker healthchecks hold
    no secret), so what it may say about a channel is that the deploy
    has it, lacks it, or has no implementation of it -- and nothing
    about what was configured.
    """
    db_ok = await _db_ok()
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok" if db_ok else "degraded",
            "db": "ok" if db_ok else "error",
            "version": APP_VERSION,
            "channels": channel_map(settings),
        },
    )


@app.get("/ready")
async def ready() -> JSONResponse:
    """Readiness probe -- 503 if the database is unreachable."""
    db_ok = await _db_ok()
    return JSONResponse(
        status_code=200 if db_ok else 503,
        content={
            "status": "ok" if db_ok else "degraded",
            "db": "ok" if db_ok else "error",
        },
    )
