"""The job keeps whether it waits for the recipient's schedule (H3).

Revision ID: 0022_job_recipient_schedule
Revises: 0021_job_retry_snapshot
Create Date: 2026-10-05

BEFORE: every delivery waited for its recipient's allowed periods; a
login code sent at night waited for the morning and expired on the
way.

AFTER:

  notifications.recipient_schedule  new, NOT NULL -- respect | bypass
                 (app/profile/registry.py RecipientSchedule), the
                 type's value AT INTAKE: a snapshot, like push_on. Read
                 by the schedule gate of
                 app/engine/service.py deliver_notification only.

EXISTING ROWS take 'respect', and why: until this revision every job
waited for the schedule, so that is the rule each was accepted with in
effect. The column is added nullable, filled, then made NOT NULL (the
0012 order) -- no lasting server default: intake always names it.

The CHECK tests IS NOT NULL before IN (0013's rule).

THE DOWNGRADE drops the column; every delivery waits again.

Migration-owned: ck_notifications_recipient_schedule is listed in
app/core/schema_objects.py.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0022_job_recipient_schedule"
down_revision: str | None = "0021_job_retry_snapshot"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "notifications",
        sa.Column("recipient_schedule", sa.String(16), nullable=True),
    )
    op.execute("UPDATE notifications SET recipient_schedule = 'respect'")
    op.alter_column("notifications", "recipient_schedule", nullable=False)
    op.create_check_constraint(
        "ck_notifications_recipient_schedule",
        "notifications",
        "recipient_schedule IS NOT NULL AND "
        "recipient_schedule IN ('respect', 'bypass')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_notifications_recipient_schedule", "notifications", type_="check",
    )
    op.drop_column("notifications", "recipient_schedule")
