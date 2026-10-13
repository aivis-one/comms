# =============================================================================
# COMMS Service -- the depth of each lane's queue (H8, spec §8.8)
# =============================================================================
#
# How many jobs wait in each lane, counted from the job rows at the
# moment of the read -- no counter is stored (a stored depth would be a
# second truth that drifts from the rows). A lane no worker process
# takes (COMMS_WORKER_LANES) still has its depth: the rows are there,
# and that is exactly the backlog this is for.
#
#   ready                 jobs the lane's next pass would select now --
#                         the SAME condition (app/engine/processor.py
#                         ready_now), the batch limit aside
#   waiting               jobs still active but not ready yet: scheduled
#                         later, behind their pipeline gate, every
#                         delivery on its backoff
#   oldest_ready_seconds  how long the oldest ready job has been due
#                         (now - its scheduled_at); null with none ready
#
# A job past its expiry that the sweep has not closed yet is in neither:
# the sweep will close it, no delivery will be made.
#
# One statement that reads the ACTIVE rows only, through an index
# entered by status -- ix_notifications_status, or the lane index
# (lane, status, scheduled_at) with lane = ANY(every lane): the
# planner's pick between two ways to the same rows (measured: status).
# The finished jobs, nearly all of the table, are never read; the "a
# delivery is due" probe goes by the deliveries' job index
# (tests/test_lane_depth.py, EXPLAIN on 50 000 rows).
# =============================================================================

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, not_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.constants import NotificationStatus
from app.engine.models import Notification
from app.engine.processor import not_expired, ready_now
from app.profile.registry import Lane

_ACTIVE = (NotificationStatus.PENDING, NotificationStatus.PROCESSING)


async def lane_depth(session: AsyncSession) -> dict[str, Any]:
    """Every lane's depth, each with every key -- an empty lane is
    zeros, never a missing key."""
    now = datetime.now(UTC)
    ready = ready_now(now)
    rows = await session.execute(
        select(
            Notification.lane,
            func.count().filter(ready),
            func.count().filter(not_(ready)),
            func.min(Notification.scheduled_at).filter(ready),
        )
        .where(
            Notification.lane.in_([lane.value for lane in Lane]),
            Notification.status.in_(_ACTIVE),
            not_expired(now),
        )
        .group_by(Notification.lane)
    )
    found = {lane: (r, w, oldest) for lane, r, w, oldest in rows.all()}
    lanes: dict[str, dict[str, Any]] = {}
    for lane in Lane:
        ready_count, waiting, oldest = found.get(lane.value, (0, 0, None))
        lanes[lane.value] = {
            "ready": ready_count,
            "waiting": waiting,
            "oldest_ready_seconds": (
                None if oldest is None
                else round((now - oldest).total_seconds(), 3)
            ),
        }
    return {"lanes": lanes}
