"""The deliveries' status index carries the delivery id too (H12).

Revision ID: 0029_status_index_with_id
Revises: 0028_llm_audio
Create Date: 2026-10-15

BEFORE:

  ix_notification_deliveries_status   (status, notification_id) --
                 migration 0027, for the fold's probe.

  A portion's selection (app/engine/service.py deliver_notification:
  the job's waiting deliveries that are due, ORDER BY id LIMIT the
  portion) had no index that gave both the job and the order. With
  fresh statistics the planner walked the primary key in id order,
  filtering every delivery of every job until the job's: 50 000 and
  110 000 buffers a portion when the job's waiting deliveries had the
  highest ids (measured, H12 gate); with statistics from before a send
  it took the status index and sorted every waiting delivery of the job
  -- 96 buffers a portion at the start of a 5000-person send, the
  square again. Dropping the order did not help: with no ORDER BY it
  scanned the heap (656 buffers) when the whole job was waiting.

AFTER:

  ix_notification_deliveries_status   (status, notification_id, id) --
                 the same name and leading columns, so the probe and
                 every query that filters by status are served as
                 before; the portion is read off it in id order, inside
                 the job, with no sort: a handful of buffers under any
                 statistics (tests/test_portion_select.py).

EXISTING ROWS: none to change; the index is rebuilt over them. A plain
CREATE INDEX, like 0023, 0025 and 0027: writes to notification_deliveries
wait while it builds.

THE DOWNGRADE restores (status, notification_id).

Migration-owned: ix_notification_deliveries_status is listed in
app/core/schema_objects.py.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0029_status_index_with_id"
down_revision: str | None = "0028_llm_audio"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "notification_deliveries"
_STATUS = "ix_notification_deliveries_status"


def upgrade() -> None:
    op.drop_index(_STATUS, table_name=_TABLE)
    op.create_index(_STATUS, _TABLE, ["status", "notification_id", "id"])


def downgrade() -> None:
    op.drop_index(_STATUS, table_name=_TABLE)
    op.create_index(_STATUS, _TABLE, ["status", "notification_id"])
