"""The job keeps its isolation lane, and the pick goes by lane (P4-1).

Revision ID: 0024_job_lane
Revises: 0023_expired_cleanup_index
Create Date: 2026-10-13

BEFORE: one selection picked the oldest due jobs of every kind; a bulk
backlog held the places a login code needed.

AFTER:

  notifications.lane  new, NOT NULL -- interactive | critical | normal
                 | bulk (app/profile/registry.py Lane), the type's value
                 AT INTAKE: a snapshot, like push_on. Read by the
                 worker's selection, one per lane
                 (app/engine/processor.py process_lane).

  ix_notifications_lane_status_scheduled  new -- (lane, status,
                 scheduled_at): each lane's pick, by equality on lane and
                 status and a range on scheduled_at, in pick order.
                 Not partial on purpose: the statuses reach the query
                 as bound parameters, and a partial predicate is only
                 usable while the planner can prove it from the query.

  ix_notifications_status_scheduled  dropped -- its one consumer was the
                 pick, which now leads with the lane; the expiry sweep
                 and the other status filters keep ix_notifications_status.

EXISTING ROWS take 'normal', and why: until this revision every job was
picked by the one selection that is now the normal lane's, with the
same numbers. The column is added nullable, filled, then made NOT NULL
(the 0012 order) -- no lasting server default: intake always names it.

The CHECK tests IS NOT NULL before IN (0013's rule).

THE DOWNGRADE drops the column and its index and restores the old pick
index; every job is picked by one selection again.

Migration-owned: ck_notifications_lane and
ix_notifications_lane_status_scheduled are listed in
app/core/schema_objects.py.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0024_job_lane"
down_revision: str | None = "0023_expired_cleanup_index"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "notifications",
        sa.Column("lane", sa.String(16), nullable=True),
    )
    op.execute("UPDATE notifications SET lane = 'normal'")
    op.alter_column("notifications", "lane", nullable=False)
    op.create_check_constraint(
        "ck_notifications_lane",
        "notifications",
        "lane IS NOT NULL AND "
        "lane IN ('interactive', 'critical', 'normal', 'bulk')",
    )
    op.drop_index(
        "ix_notifications_status_scheduled", table_name="notifications",
    )
    op.create_index(
        "ix_notifications_lane_status_scheduled",
        "notifications",
        ["lane", "status", "scheduled_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_notifications_lane_status_scheduled", table_name="notifications",
    )
    op.create_index(
        "ix_notifications_status_scheduled",
        "notifications",
        ["status", "scheduled_at"],
    )
    op.drop_constraint(
        "ck_notifications_lane", "notifications", type_="check",
    )
    op.drop_column("notifications", "lane")
