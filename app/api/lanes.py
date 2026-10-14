# =============================================================================
# COMMS Service -- Lane depth API (H8, spec §8.8)
# =============================================================================
#
#   GET /api/v1/lanes/depth
#
# How many jobs wait in each lane, read from the job rows now
# (app/engine/depth.py). Behind the service token like every /api/v1
# route; READ ONLY, one snapshot (app/api/jobs.py read_only_snapshot).
# Not in /health, which keeps its body and its status code (P2-4); not
# beside the channel health either: that one counts the journal over a
# window, this one the queue at an instant. The contract is
# deploy/INTEGRATION.md, "Lane depth".
# =============================================================================

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_service_auth
from app.api.jobs import read_only_snapshot
from app.engine.depth import lane_depth

router = APIRouter(
    prefix="/api/v1/lanes",
    tags=["lanes"],
    dependencies=[Depends(require_service_auth)],
)


@router.get("/depth")
async def get_lane_depth(
    session: AsyncSession = Depends(read_only_snapshot),
) -> dict[str, Any]:
    """Every lane's queue depth; reads only."""
    return await lane_depth(session)
