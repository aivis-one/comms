"""The expiry cleanup selects its batch by index (H5).

Revision ID: 0023_expired_cleanup_index
Revises: 0022_job_recipient_schedule
Create Date: 2026-10-05

BEFORE: every batch of the expiry cleanup -- which runs on EVERY worker
tick -- scanned and sorted notifications to find the expired jobs it
may delete (expiry_at IS NOT NULL AND expiry_at < now AND status IN
(...) ORDER BY expiry_at): no index served it, and status alone is not
selective.

AFTER:

  ix_notifications_expired_deletable  new -- notifications (expiry_at)
                 WHERE expiry_at IS NOT NULL AND status IN (the
                 outcomes the cleanup deletes). Only the rows the
                 cleanup can match are in it: active jobs, failed ones
                 (kept until retention) and jobs without an expiry are
                 not. Keyed by expiry_at, so a batch is read in order --
                 no sort.

THE PREDICATE IS app/engine/service.py _EXPIRED_DELETABLE_STATUSES,
spelled out: the planner uses the index only while the batch select's
status list implies it. tests/test_expired_cleanup_index.py holds the
two equal, read back from pg_indexes.

THE DOWNGRADE drops the index; the cleanup scans again.

Migration-owned: ix_notifications_expired_deletable is listed in
app/core/schema_objects.py.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0023_expired_cleanup_index"
down_revision: str | None = "0022_job_recipient_schedule"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX = "ix_notifications_expired_deletable"


def upgrade() -> None:
    op.create_index(
        _INDEX,
        "notifications",
        ["expiry_at"],
        postgresql_where=sa.text(
            "expiry_at IS NOT NULL AND status IN ('sent', 'partial_sent', "
            "'expired', 'cancelled', 'suppressed', 'no_recipients')"
        ),
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name="notifications")
