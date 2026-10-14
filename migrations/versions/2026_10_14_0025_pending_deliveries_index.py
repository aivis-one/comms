"""A job's waiting deliveries, by index (H9).

Revision ID: 0025_pending_deliveries_index
Revises: 0024_job_lane
Create Date: 2026-10-14

BEFORE: the fold asked "does any delivery of this job still wait?" only
by reading the DISTINCT status of every delivery of the job -- after
every portion of a send (H7: one delivery per portion in bulk), so a
send to N people read N rows N times.

AFTER:

  ix_notification_deliveries_pending  new -- (notification_id) WHERE
                 status = 'pending': the waiting deliveries of a job and
                 nothing else. The fold's probe (app/engine/service.py
                 rollup_notification) reads one entry while any waits,
                 however many of the job's deliveries are done and
                 however many other jobs wait. Partial on purpose, and
                 usable: the probe names 'pending' itself, the planner
                 proves the predicate from the query. Neither
                 ix_notification_deliveries_notification_id (every
                 delivery of the job, done ones first) nor
                 ix_notification_deliveries_status (every waiting
                 delivery of every job) bounds it.

EXISTING ROWS: none to change; the index is built over them.

THE DOWNGRADE drops the index; the probe falls back to the two indexes
above.

Migration-owned: ix_notification_deliveries_pending is listed in
app/core/schema_objects.py.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0025_pending_deliveries_index"
down_revision: str | None = "0024_job_lane"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX = "ix_notification_deliveries_pending"


def upgrade() -> None:
    op.create_index(
        _INDEX,
        "notification_deliveries",
        ["notification_id"],
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name="notification_deliveries")
