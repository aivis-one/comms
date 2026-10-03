# =============================================================================
# COMMS Service -- What changed since a cursor (P2-3, spec §7.5)
# =============================================================================
#
# A product that was down asks which of its jobs changed while it was
# not listening, and then reads each one by its key (P2-2). The feed is
# the list of keys; the truth is the read (§7.1).
#
# THE ORDER IS THE JOURNAL'S (xact_id, id): the writing transaction,
# then the row's identity inside it. A cursor on `id` alone would lose a
# late commit for good: transaction A takes id 100, B takes 101 and
# commits first, a reader passes 101 -- and A's 100 lands behind it.
#
# NOTHING IN FLIGHT IS HANDED OUT. A page holds only rows whose writing
# transaction is below the xmin of the reading snapshot: every
# transaction below xmin has ended, so no row with such an xact_id can
# appear later, and the committed ones are all visible to the snapshot.
# Rows of a transaction still running when the page is read are held
# back and come with a later read. The bound is taken INSIDE the
# statement that reads the rows, so the rows and the xmin always come
# from one snapshot. The read still runs in the read-only REPEATABLE
# READ session of the job reads (app/api/jobs.py read_only_snapshot):
# the keys of a page are looked up by a second statement, and under
# READ COMMITTED retention could delete a job between the two.
#
# AN ITEM IS A JOB, NOT A ROW: its idempotency key and nothing more. A
# page is the longest run of rows after the cursor that touches at most
# `limit` jobs (and at most CHANGES_SCAN_ROWS rows); it is cut right
# before the first row of the next job, so no job is stepped over. On a
# page a key appears once; it appears again on a later page only when
# the job has rows there too -- it changed again, or its rows span more
# than one page.
#
# THE CURSOR'S read_at is the database clock of the read that handed it
# out, refreshed by every read, an empty one included: a product that
# polls keeps its cursor fresh however quiet comms is, and a cursor older
# than the retention period is refused (410 cursor_expired) rather than
# continued past changes retention may already have deleted.
# =============================================================================

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import Select, String, cast, func, literal, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.constants import CHANGES_SCAN_ROWS
from app.core.exceptions import CursorExpiredError, ValidationError
from app.engine.models import XID8, Notification, NotificationTransition

_SNAPSHOT = func.pg_current_snapshot()


@dataclass(frozen=True)
class Position:
    """(xact_id, id) of the last journal row a reader was given; (0, 0)
    before the first -- every real row is after it."""

    xact: int
    ident: int


START = Position(0, 0)


@dataclass(frozen=True)
class ChangesPage:
    keys: list[str]
    position: Position
    read_at: datetime


def _xid8(value: int) -> Any:
    """An xid8 parameter: sent as text, cast by the server -- the driver
    has no xid8 parameter type of its own."""
    return cast(literal(str(value), String), XID8)


def changes_scan(start: Position) -> Select[Any]:
    """The rows after `start` that no running transaction can still
    precede, in feed order, at most CHANGES_SCAN_ROWS of them. A
    function of its own so the suite can EXPLAIN the very statement."""
    return (
        select(
            cast(NotificationTransition.xact_id, String),
            NotificationTransition.id,
            NotificationTransition.notification_id,
        )
        .where(
            tuple_(NotificationTransition.xact_id, NotificationTransition.id)
            > tuple_(_xid8(start.xact), literal(start.ident)),
            NotificationTransition.xact_id < func.pg_snapshot_xmin(_SNAPSHOT),
        )
        .order_by(NotificationTransition.xact_id, NotificationTransition.id)
        .limit(CHANGES_SCAN_ROWS)
    )


async def list_changes(
    session: AsyncSession,
    *,
    limit: int,
    position: Position | None,
    read_at: datetime | None,
) -> ChangesPage:
    """One page of the jobs that changed after `position`; reads only.

    `position` and `read_at` come from the product's cursor, or are both
    None for a first read (from the start of the kept journal). A cursor
    from the future -- a transaction not yet assigned, a read time later
    than now -- is a ValidationError (422); one older than the
    retention period is a CursorExpiredError (410).

    KNOWN CEILING (acknowledged by design -- P2-3, the feed's delay):
      1. Mechanics: rows are handed out only below the reading
         snapshot's xmin, so one running transaction holds back every
         row written after it began -- in comms an attempt that is
         calling a channel, up to that call's timeout; anywhere in the
         cluster, any open transaction. The feed is late by the longest
         of them; nothing is lost.
      2. Status: acknowledged by design.
      3. Backlog ref: none -- the delay is the price of the no-loss
         rule, and comms' longest transaction is bounded by its own
         timeouts (app/engine/service.py _DELIVER_TIMEOUT_SECONDS).
      4. Promotion trigger (observable): a feed read that returns
         `items: []` while pg_stat_activity shows a backend_xid older
         than a minute.
      5. Agreed fix: none agreed; the candidate is to shorten the
         transactions that hold it (the attempt), not to loosen the
         rule.
      6. Rejected: handing out rows at or above xmin (a late commit of
         a smaller xact_id would be stepped over for good); a cursor on
         the row id or on time (the same loss, rejected by the handoff).

    KNOWN CEILING (acknowledged by design -- P2-3, retention under a
    live cursor):
      1. Mechanics: retention deletes a terminal job by its created_at
         (app/engine/service.py delete_terminal_notifications_batch),
         its journal rows with it (ON DELETE CASCADE). A job created
         longer ago than the retention period whose last change came
         after the product's position can be deleted before the product
         reads that change: the feed never shows it, although the
         cursor is not expired.
      2. Status: acknowledged by design.
      3. Backlog ref: none -- it takes a job that lived longer than the
         retention period (90 days by default).
      4. Promotion trigger (observable): a key the product holds as
         unfinished that the read by key answers 404, under a cursor
         that is not expired.
      5. Agreed fix: none agreed; the candidate is retention measuring
         a terminal job's age by its last transition.
      6. Rejected: a stored "deletion horizon" written by retention (a
         new stored state and a write on the retention path).
    """
    now, xmax = (await session.execute(select(
        func.now(), cast(func.pg_snapshot_xmax(_SNAPSHOT), String),
    ))).one()
    if position is not None and read_at is not None:
        if position.xact >= int(xmax) or read_at > now:
            raise ValidationError("Malformed cursor: it is from the future")
        days = settings.notification_retention_days
        if days and read_at < now - timedelta(days=days):
            raise CursorExpiredError(
                f"the cursor was read more than {days} days ago -- what "
                f"changed after it may be deleted; reconcile by key and "
                f"start again without a cursor"
            )
    start = position or START

    rows = (await session.execute(changes_scan(start))).all()

    jobs: list[UUID] = []
    seen: set[UUID] = set()
    last = start
    for xact, ident, notification_id in rows:
        if notification_id not in seen:
            if len(jobs) == limit:
                break
            jobs.append(notification_id)
            seen.add(notification_id)
        last = Position(int(xact), ident)

    keys: dict[UUID, str] = {}
    if jobs:
        keys = dict((await session.execute(
            select(Notification.id, Notification.idempotency_key)
            .where(Notification.id.in_(jobs))
        )).tuples().all())
    return ChangesPage(
        keys=[keys[job] for job in jobs], position=last, read_at=now,
    )
