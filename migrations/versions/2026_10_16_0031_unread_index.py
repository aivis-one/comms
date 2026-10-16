"""The badge's index holds unread rows only -- no waiting delivery (H15).

Revision ID: 0031_unread_index
Revises: 0030_json_null_is_null
Create Date: 2026-10-16

BEFORE: ix_notification_deliveries_inbox ON notification_deliveries
(recipient_id, status, read_at), from 0001, over EVERY delivery. Its two
callers (app/engine/service.py get_unread_count and mark_all_read) ask
for one recipient's sent, unread rows, by channel. But status is a
column of it, so a condition status = 'pending' with no recipient is
executable on it too -- a scan of the whole index for every waiting
delivery of every job. That was the path of the fold's probe CI H13
caught (Bitmap Index Scan on it, Index Cond status = 'pending', Plan
Rows 1, 10 002 entries read, 652 buffers), and a path of ready_now, the
portion select and close_waiting_deliveries whenever the planner does
not take ix_notification_deliveries_status (up to 18 825 buffers,
H14 FIX).

AFTER: ix_notification_deliveries_unread ON notification_deliveries
(recipient_id, channel) WHERE status = 'sent' AND read_at IS NULL --
exactly the badge's rows. A waiting delivery is not in it at all, so
that path does not exist, whatever the planner believes: not an
estimate, the rows are absent. Both callers filter its predicate and
pass the channel (app/api/inbox.py passes in_app every time); a read
or a waiting row never enters it. ix_notification_deliveries_inbox is
dropped. Forgetting (withdraw_recipient: a recipient's jobs, their
waiting deliveries, their error texts) reads by recipient through
ix_notification_deliveries_recipient_id (recipient_id alone, the
model's), which has no status: it gives the probe no path either.

A plain CREATE INDEX inside the migration's transaction, not
CONCURRENTLY (as 0016, 0027): the delivery table on the boxes is small;
the table is locked for writes while the index builds.

THE DOWNGRADE builds 0001's index back and drops this one.

Migration-owned: listed in app/core/schema_objects.py (a partial index;
0001's entry there is replaced by this one's).
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0031_unread_index"
down_revision: str | None = "0030_json_null_is_null"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX ix_notification_deliveries_unread "
        "ON notification_deliveries (recipient_id, channel) "
        "WHERE status = 'sent' AND read_at IS NULL"
    )
    op.execute("DROP INDEX ix_notification_deliveries_inbox")


def downgrade() -> None:
    op.execute(
        "CREATE INDEX ix_notification_deliveries_inbox "
        "ON notification_deliveries (recipient_id, status, read_at)"
    )
    op.execute("DROP INDEX ix_notification_deliveries_unread")
