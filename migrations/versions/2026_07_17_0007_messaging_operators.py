"""messaging operators -- assignee FK + close-notify marker (Phase 4b)

Revision ID: 0007_messaging_operators
Revises: 0006_messaging_foundation
Create Date: 2026-07-17 00:00:00.000000

Phase 4b wires the operator axis that 4a left "soft". Two schema
changes on `threads` plus a data backfill:

1. assignee -> recipients FK, nullable, ondelete=RESTRICT. assignee
   ALWAYS points into recipients (one table): a claiming agent for a
   section thread, or the pre-assigned master for a user thread. So a
   hard FK is clean here, unlike the polymorphic operator_value (no
   FK). RESTRICT for the same reason as client/sender/participant
   (0006): a recipient a thread references is not silently shredded.
   ix_threads_assignee serves the visibility query (assignee == me),
   which -- with the user-thread pre-assign below -- also covers
   user-thread visibility, so no (operator_kind, operator_value) index
   is needed.

2. close_notify_pending_at -- the 4b -> 4c handoff marker. 4b sets it
   when a SECTION thread reaches `closed`; 4c reads/sends/clears it.
   Unindexed here (4c owns that read).

3. BACKFILL: pre-assign existing user threads. 4a created user threads
   with assignee=NULL; 4b's model sets assignee=operator_value at
   creation, so existing rows are aligned here. INVARIANT assignee set
   <=> assigned_at set is honoured (assigned_at := created_at). On the
   test VPS messaging tables are empty -> a no-op, but the migration
   must be correct up/down regardless.

DOWNGRADE is SCHEMA-ONLY: drop the index, the FK and the marker
column. It does NOT revert the backfill -- a backfilled assignee is
indistinguishable from a claimed one, and leaving a populated nullable
column (exactly the 4a shape: assignee as a plain nullable UUID, no
FK) is harmless and valid.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007_messaging_operators"
down_revision: str | None = "0006_messaging_foundation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the assignee FK + close-notify marker, backfill user threads."""
    # -- close-notify marker (4b sets, 4c consumes) --
    op.add_column(
        "threads",
        sa.Column(
            "close_notify_pending_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )

    # -- assignee -> recipients FK (RESTRICT) + supporting index --
    op.create_foreign_key(
        "threads_assignee_fkey",
        "threads",
        "recipients",
        ["assignee"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index("ix_threads_assignee", "threads", ["assignee"])

    # -- backfill: pre-assign existing user threads to their master --
    # operator_value of a user thread is a validated recipient, so this
    # references valid rows; assigned_at := created_at keeps the
    # "assignee set <=> assigned_at set" invariant.
    op.execute(
        sa.text(
            "UPDATE threads "
            "SET assignee = operator_value, assigned_at = created_at "
            "WHERE operator_kind = 'user' AND assignee IS NULL"
        )
    )


def downgrade() -> None:
    """Schema-only revert (does NOT un-backfill assignee data)."""
    op.drop_index("ix_threads_assignee", table_name="threads")
    op.drop_constraint("threads_assignee_fkey", "threads", type_="foreignkey")
    op.drop_column("threads", "close_notify_pending_at")
