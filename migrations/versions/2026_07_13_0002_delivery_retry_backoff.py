"""delivery retry backoff -- next_retry_at + poll index

Revision ID: 0002_delivery_retry_backoff
Revises: 0001_engine_core
Create Date: 2026-07-13 06:00:00.000000

Changes (review 1.1):
  - notification_deliveries.next_retry_at (nullable, tz) -- earliest
    moment a transient retry may run; the deliver stage skips
    deliveries gated into the future. NULL = no gate.
  - Poll-index swap on notifications: the Phase 1 composite
    (scheduled_at, status) led on the range column, so in steady state
    (mostly sent/expired rows inside the range) it scanned everything;
    the status-leading (status, scheduled_at, priority) matches the
    worker poll (WHERE status IN (...) AND scheduled_at <= now) with
    far better selectivity. The old index served no other query and is
    dropped as redundant.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_delivery_retry_backoff"
down_revision: str | None = "0001_engine_core"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add retry gate column; swap the notifications poll index."""
    op.add_column(
        "notification_deliveries",
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.drop_index(
        "ix_notifications_scheduled_status",
        table_name="notifications",
    )
    op.create_index(
        "ix_notifications_status_scheduled_priority",
        "notifications",
        ["status", "scheduled_at", "priority"],
    )


def downgrade() -> None:
    """Restore the Phase 1 index shape; drop the retry gate column."""
    op.drop_index(
        "ix_notifications_status_scheduled_priority",
        table_name="notifications",
    )
    op.create_index(
        "ix_notifications_scheduled_status",
        "notifications",
        ["scheduled_at", "status"],
    )

    op.drop_column("notification_deliveries", "next_retry_at")
