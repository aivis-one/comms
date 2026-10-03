"""The changes feed reads an index (P2-3).

Revision ID: 0019_changes_feed_index
Revises: 0018_channel_health_book
Create Date: 2026-10-03

BEFORE: the journal (0017) is indexed for one job's path, for
forgetting and for the channel health window (0018). "Every job that
changed after a position" -- the order (xact_id, id) from a cursor --
had no index and sorted the whole journal.

AFTER: ix_transitions_changes ON notification_transitions
(xact_id, id) INCLUDE (notification_id).

  - the key is the feed's order: the writing transaction, then the
    row's identity inside it (app/engine/service.py list_changes);
  - a page is a range scan from the cursor that stops after its scan
    budget, the upper bound (xact_id < the snapshot's xmin) inside the
    same index condition;
  - notification_id rides in the index: a page is read without the heap.

xact_id is xid8 (0017), which has a btree operator class: the row-value
comparison (xact_id, id) > (x, i) is an index condition.

A plain CREATE INDEX inside the migration's transaction (the 0016 rule):
the journal ships in the same release as this index.

Migration-owned: listed in app/core/schema_objects.py.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0019_changes_feed_index"
down_revision: str | None = "0018_channel_health_book"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_transitions_changes",
        "notification_transitions",
        ["xact_id", "id"],
        postgresql_include=["notification_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_transitions_changes", table_name="notification_transitions",
    )
