"""The reverse outbox and the job's push_on (P3-1, spec §7.4, §7.6).

Revision ID: 0020_push_outbox
Revises: 0019_changes_feed_index
Create Date: 2026-10-03

BEFORE: a product learned what became of its job only by reading
(by key, or the changes since a cursor). Nothing was owed to it.

AFTER:

  notifications.push_on  new, NOT NULL -- what the type pushes back to
                 the product, AT INTAKE: outcome | outcome_and_deferral
                 | none (app/engine/constants.py PushOn). A snapshot,
                 like `channels` and `category`.
  push_outbox    new -- a push owed to the product, written in the
                 transaction of the transition that owes it
                 (app/engine/journal.py), published after the commit
                 and then deleted by the relay
                 (app/transport/push_relay.py).
    id               bigint identity -- the relay reads in this order.
    notification_id  FK -> notifications ON DELETE CASCADE: rows go
                     with their job. Nothing else: the key is looked up
                     at publish time; no status, never the letter.
    created_at       timestamptz DEFAULT now().

EXISTING ROWS take push_on = 'none', and why: before this revision no
job pushed anything; 'none' is what each of them was accepted with in
effect. The column is added nullable, filled, then made NOT NULL (the
0012 order) -- no lasting server default: intake always names it.

The CHECK tests IS NOT NULL before IN (0013's rule: a NULL operand
makes the predicate NULL, which a CHECK lets through).

THE DOWNGRADE drops the table and the column; pushes still owed are
dropped with the table -- a push is a hint, the read stays the truth.

Migration-owned: ck_notifications_push_on and ix_push_outbox_notification
are listed in app/core/schema_objects.py.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0020_push_outbox"
down_revision: str | None = "0019_changes_feed_index"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "push_outbox"


def upgrade() -> None:
    op.add_column(
        "notifications",
        sa.Column("push_on", sa.String(24), nullable=True),
    )
    op.execute("UPDATE notifications SET push_on = 'none'")
    op.alter_column("notifications", "push_on", nullable=False)
    op.create_check_constraint(
        "ck_notifications_push_on",
        "notifications",
        "push_on IS NOT NULL AND "
        "push_on IN ('outcome', 'outcome_and_deferral', 'none')",
    )

    op.create_table(
        _TABLE,
        sa.Column(
            "id", sa.BigInteger(), sa.Identity(always=True), primary_key=True,
        ),
        sa.Column(
            "notification_id", sa.Uuid(),
            sa.ForeignKey("notifications.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    # The lookup of the ON DELETE CASCADE from notifications: retention
    # deletes jobs in batches, and an outbox that grew while Redis was
    # away must not be scanned once per deleted job.
    op.create_index(
        "ix_push_outbox_notification", _TABLE, ["notification_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_push_outbox_notification", table_name=_TABLE)
    op.drop_table(_TABLE)
    op.drop_constraint(
        "ck_notifications_push_on", "notifications", type_="check",
    )
    op.drop_column("notifications", "push_on")
