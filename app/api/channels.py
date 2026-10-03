# =============================================================================
# COMMS Service -- Channel health API (P2-4, spec §6.7)
# =============================================================================
#
#   GET /api/v1/channels/health?window_minutes=<1..1440, default 60>
#
# What each channel answered over the window, with its state from the
# same channel map /health carries (app/engine/health.py). Behind the
# service token like every /api/v1 route.
#
# WHY NOT IN /health: /health is unauthenticated on purpose and says only
# what a deploy has. Counts of what the channels did are operational data
# and belong behind the token; and /health keeps its body and its status
# code, so nothing that reads it today changes. The contract is
# deploy/INTEGRATION.md, "Channel health".
# =============================================================================

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_service_auth
from app.core.constants import HEALTH_WINDOW_DEFAULT_MINUTES
from app.core.database import get_db_reader
from app.engine.health import channel_health, health_window

router = APIRouter(
    prefix="/api/v1/channels",
    tags=["channels"],
    dependencies=[Depends(require_service_auth)],
)


@router.get("/health")
async def get_channel_health(
    window_minutes: int = Query(default=HEALTH_WINDOW_DEFAULT_MINUTES),
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, Any]:
    """Every channel's answers over the window; reads only."""
    return await channel_health(session, health_window(window_minutes))
