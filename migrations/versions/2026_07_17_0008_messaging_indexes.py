"""messaging indexes -- activity keyset + close-notify partial (Phase 4c)

Revision ID: 0008_messaging_indexes
Revises: 0007_messaging_operators
Create Date: 2026-07-17 00:00:00.000000

Two read-path indexes on `threads`:

1. ix_threads_activity -- an EXPRESSION index on
   COALESCE(last_message_at, created_at) DESC, id DESC. It backs the
   keyset pagination of list_visible_threads (ORDER BY the same
   expression + id, cursor is a range on it) AND is a candidate for the
   auto-close idle scan (WHERE COALESCE(...) < cutoff). One index, no
   write amplification of a maintained column. Whether the planner
   actually uses it for BOTH is proven by EXPLAIN in the Phase 4c
   report -- the auto-close KNOWN CEILING is lifted ONLY on confirmed
   coverage (expression indexes are picky about the exact expression).

2. ix_threads_close_notify_pending -- a PARTIAL index over the
   close-notify flag (WHERE close_notify_pending_at IS NOT NULL). The
   consumer (app/notifier.consume_close_notifications) runs every tick;
   this keeps its scan over a normally-empty set O(pending).

Raw DDL: alembic's create_index does not express a COALESCE ordering
cleanly; the DROP mirror is symmetric.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0008_messaging_indexes"
down_revision: str | None = "0007_messaging_operators"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the activity expression index + the close-notify partial."""
    op.execute(
        "CREATE INDEX ix_threads_activity ON threads "
        "(COALESCE(last_message_at, created_at) DESC, id DESC)"
    )
    op.execute(
        "CREATE INDEX ix_threads_close_notify_pending ON threads "
        "(close_notify_pending_at) WHERE close_notify_pending_at IS NOT NULL"
    )


def downgrade() -> None:
    """Drop both indexes."""
    op.execute("DROP INDEX ix_threads_close_notify_pending")
    op.execute("DROP INDEX ix_threads_activity")
