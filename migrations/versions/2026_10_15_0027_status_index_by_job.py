"""The deliveries' status index keyed by job too (H11).

Revision ID: 0027_status_index_by_job
Revises: 0026_llm_channel
Create Date: 2026-10-15

BEFORE:

  ix_notification_deliveries_status   (status) -- from the model
  ix_notification_deliveries_pending  (notification_id) WHERE status =
                 'pending' -- migration 0025, for the fold's probe "does
                 any delivery of this job still wait?"

  The probe had two ways in, and the planner chose between them by its
  estimates. With statistics taken before a send began -- the usual
  case: a send inserts thousands of pending rows between two autovacuum
  passes -- it believed 'pending' rare and went by the status index,
  reading every waiting delivery of every job in heap order until it
  met this job's: 5000 other waiting deliveries ahead made 92 buffers a
  probe, every portion, against 3 by the job (measured, H11 gate). With
  fresh statistics the two paths were 2.61 against 3.03 -- a coin the
  CI tossed (3.4.0, CI / image).

AFTER:

  ix_notification_deliveries_status   (status, notification_id) -- the
                 same name and the same leading column, so every query
                 that filters by status is served as before; a query
                 that names the job too is bounded by it. No index on
                 status alone is left, so no plan of the probe reads
                 other jobs' waiting deliveries, whatever the planner
                 believes.
  ix_notification_deliveries_pending  dropped -- the new index answers
                 the probe by the job and the status at once; one index
                 for one question.

EXISTING ROWS: none to change; the index is rebuilt over them. A plain
CREATE INDEX, like 0023 and 0025: writes to notification_deliveries wait
while it builds.

THE DOWNGRADE restores both indexes as they were.

Migration-owned: ix_notification_deliveries_status is listed in
app/core/schema_objects.py; the model no longer declares an index on
status (app/engine/models.py).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0027_status_index_by_job"
down_revision: str | None = "0026_llm_channel"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "notification_deliveries"
_STATUS = "ix_notification_deliveries_status"
_PENDING = "ix_notification_deliveries_pending"


def upgrade() -> None:
    op.drop_index(_PENDING, table_name=_TABLE)
    op.drop_index(_STATUS, table_name=_TABLE)
    op.create_index(_STATUS, _TABLE, ["status", "notification_id"])


def downgrade() -> None:
    op.drop_index(_STATUS, table_name=_TABLE)
    op.create_index(_STATUS, _TABLE, ["status"])
    op.create_index(
        _PENDING,
        _TABLE,
        ["notification_id"],
        postgresql_where=sa.text("status = 'pending'"),
    )
