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
# process from the same image: `python -m app.worker`
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
from app.api.messaging import router as messaging_router
from app.api.prefs import router as prefs_router
from app.core.config import APP_VERSION, settings
from app.core.database import dispose_engine, get_engine
from app.core.logging import setup_logging
from app.engine.formatters import close_formatters
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
        # Only reachable in stub mode: real mode refuses to start
        # without the token (app/core/config.py). Loud on purpose --
        # an open "internal" API must be a visible choice, not a
        # silent default.
        logger.warning(
            "service_auth_disabled",
            reason="COMMS_SERVICE_TOKEN is empty",
            channels_mode=settings.channels_mode,
        )
    logger.info(
        "comms_started",
        version=APP_VERSION,
        env=settings.app_env,
        channels_mode=settings.channels_mode,
    )
    yield
    await close_formatters()
    await dispose_engine()
    logger.info("comms_stopped")


app = FastAPI(
    title="COMMS Service",
    version=APP_VERSION,
    lifespan=lifespan,
)
register_error_handlers(app)
app.include_router(inbox_router)
app.include_router(prefs_router)
app.include_router(messaging_router)


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
    """Health check -- always returns 200, reports DB status."""
    db_ok = await _db_ok()
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok" if db_ok else "degraded",
            "db": "ok" if db_ok else "error",
            "version": APP_VERSION,
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
