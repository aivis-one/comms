"""The inbox keyset reads an index (D1 / R7).

Revision ID: 0016_inbox_index
Revises: 0015_pipeline_outcome
Create Date: 2026-10-02

BEFORE: the inbox page (app/engine/service.py list_recipient_deliveries)
filters notification_deliveries by recipient, channel and status 'sent'
and orders by (sent_at DESC, id DESC) -- with no index over that order,
every page sorted the recipient's whole sent history (0001's
ix_notification_deliveries_inbox is (recipient_id, status, read_at):
it filters, it does not order).

AFTER: ix_deliveries_inbox_keyset ON notification_deliveries
(recipient_id, channel, sent_at DESC, id DESC) WHERE status = 'sent'.

  - channel in the key: the one caller of the inbox page passes
    in_app every time (app/api/inbox.py);
  - partial on status = 'sent': only sent deliveries are ever listed,
    and a pending or failed row never enters the index;
  - the order matches the keyset's, so a page is an index range scan
    that stops after limit + 1 rows, cursor included.

The badge (get_unread_count) is NOT this index's: read_at IS NULL is in
the key of 0001's ix_notification_deliveries_inbox, and the planner
keeps it there (EXPLAIN in the D1-2 report). Both stay; each serves
one query.

A plain CREATE INDEX inside the migration's transaction, not
CONCURRENTLY (D1-2 gate): the delivery tables on the boxes are small,
and CONCURRENTLY outside a transaction adds a failure of its own -- an
invalid index left behind, to be found and rebuilt.

Migration-owned: the index is not in the models (a partial index with a
DESC key), so it is listed in app/core/schema_objects.py, which keeps
autogenerate from offering to drop it and lets the suite assert it.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0016_inbox_index"
down_revision: str | None = "0015_pipeline_outcome"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX ix_deliveries_inbox_keyset ON notification_deliveries "
        "(recipient_id, channel, sent_at DESC, id DESC) "
        "WHERE status = 'sent'"
    )


def downgrade() -> None:
    op.execute("DROP INDEX ix_deliveries_inbox_keyset")
