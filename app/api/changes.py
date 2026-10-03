# =============================================================================
# COMMS Service -- What changed since a cursor (P2-3, spec §7.5)
# =============================================================================
#
#   GET /api/v1/notifications/changes?limit=<1..100, default 20>&cursor=...
#
# The keys of the jobs that changed after the cursor, oldest change
# first; the product reads each by its key (app/api/jobs.py). The
# mechanics -- the order, what is held back, the cursor's read time --
# are app/engine/changes.py; the contract is deploy/INTEGRATION.md
# section 9.
#
# READ ONLY, ONE SNAPSHOT: the route runs in the read-only REPEATABLE
# READ session of the job reads -- nothing here can write, and a page's
# rows and the keys looked up for them come from one snapshot.
# =============================================================================

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_service_auth
from app.api.jobs import read_only_snapshot
from app.api.paging import (
    PAGE_LIMIT_DEFAULT,
    ChangesCursor,
    decode_changes_cursor,
    encode_changes_cursor,
    page_limit,
)
from app.engine.changes import Position, list_changes

router = APIRouter(
    prefix="/api/v1/notifications",
    tags=["jobs"],
    dependencies=[Depends(require_service_auth)],
)

# The closed field set of an item -- the table in INTEGRATION.md §9.
CHANGE_ITEM_FIELDS = ("idempotency_key",)


@router.get("/changes")
async def read_changes(
    limit: int = Query(default=PAGE_LIMIT_DEFAULT),
    cursor: str | None = Query(default=None),
    session: AsyncSession = Depends(read_only_snapshot),
) -> dict[str, Any]:
    """A page of the jobs that changed after the cursor; reads only.
    next_cursor is never null."""
    size = page_limit(limit)
    given = decode_changes_cursor(cursor)
    found = await list_changes(
        session,
        limit=size,
        position=Position(given.xact, given.ident) if given else None,
        read_at=given.read_at if given else None,
    )
    return {
        "items": [{"idempotency_key": key} for key in found.keys],
        "next_cursor": encode_changes_cursor(ChangesCursor(
            found.position.xact, found.position.ident, found.read_at,
        )),
    }
