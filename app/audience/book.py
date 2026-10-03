# =============================================================================
# COMMS Service -- The address book as comms holds it (P2-4, spec §10.5)
# =============================================================================
#
# RECONCILIATION, NOT REPAIR. The book is synced eventually: an event
# lost or dead-lettered leaves comms' copy behind the product's, and
# nobody knows. This module lists what comms has recorded so the
# product can compare. comms decides nothing by the difference and
# changes nothing here -- the product owns the book and fixes it by the
# two write paths it already has (a newer snapshot, a deletion).
#
# WHAT A ROW SAYS: the id, the snapshot version, `active`, and whether
# the person was forgotten. NO ADDRESS, for anyone: the version names the
# content -- an equal version with other bytes is refused at write time
# (audience/sync.py, SnapshotConflictError) -- so comparing versions is
# comparing snapshots, and an address list over the wire would be a copy
# of personal data for nothing. A tombstone has no address to give
# anyway (CHECK ck_recipients_tombstone).
#
# ORDER: (created_at ASC, id ASC), keyset by ix_recipients_book
# (migration 0018). Ascending on purpose: a recipient created while the
# product walks the pages lands after its cursor and is seen; newest
# first would skip it and report it missing. A row changed after its page
# was read shows its old state until the next walk -- the listing is not
# a snapshot of the whole book.
#
# KNOWN CEILING (acknowledged by design -- P2-4, the in-flight insert):
#   1. Mechanics: created_at is now() -- the START of the inserting
#      transaction. A recipient whose transaction began before the
#      product read a page and committed after it carries a created_at
#      behind that page's cursor, and this walk does not reach it.
#   2. Status: acknowledged by design.
#   3. Backlog ref: none -- the walk is repeated by the product, and the
#      next walk reaches the row; a reconciliation is a periodic pass.
#   4. Promotion trigger (observable): a recipient that a completed walk
#      did not list although its created_at is older than the walk's
#      first request.
#   5. Agreed fix: none agreed; the candidate is a keyset over a
#      commit-ordered key (an xid8 column, as the journal has).
#   6. Rejected: clock_timestamp() for created_at (commit order is
#      still not insertion-time order); a snapshot of the whole book per
#      walk (unbounded, and stale by the time it is read).
# =============================================================================

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import Recipient


async def list_book(
    session: AsyncSession,
    *,
    limit: int,
    cursor: tuple[datetime, UUID] | None,
) -> tuple[list[dict[str, Any]], tuple[datetime, UUID] | None]:
    """One page of the address book; reads only.

    `limit` is already bounded by the route (app/api/paging.py). limit+1
    rows are fetched to learn whether a next page exists without a
    COUNT. Returns (items, next_cursor) -- next_cursor is the
    (created_at, id) of the last row given, or None on the last page.
    """
    stmt = select(
        Recipient.id,
        Recipient.created_at,
        Recipient.version,
        Recipient.active,
        Recipient.deleted_at,
    )
    if cursor is not None:
        stmt = stmt.where(tuple_(Recipient.created_at, Recipient.id) > cursor)
    stmt = stmt.order_by(Recipient.created_at, Recipient.id).limit(limit + 1)

    rows = (await session.execute(stmt)).all()
    has_more = len(rows) > limit
    rows = rows[:limit]

    items = [
        {
            "recipient_id": str(row.id),
            "version": row.version,
            "active": row.active,
            "deleted": row.deleted_at is not None,
        }
        for row in rows
    ]
    next_cursor = (rows[-1].created_at, rows[-1].id) if has_more else None
    return items, next_cursor
