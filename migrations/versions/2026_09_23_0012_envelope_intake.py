"""envelope and intake -- the key is required, the profile routes (F1.2)

Revision ID: 0012_envelope_intake
Revises: 0011_delivery_schedule
Create Date: 2026-09-23 00:00:00.000000

WHAT TURNS OVER ON `notifications`:

  idempotency_key  NULL allowed -> NOT NULL; the PARTIAL unique index
                   (WHERE idempotency_key IS NOT NULL) becomes a PLAIN
                   unique index under the same name. The path "no key"
                   is gone everywhere, so the predicate has nothing left
                   to exclude.
  fingerprint      new, NOT NULL -- the byte-level identity of the
                   request under its key; "same key, other bytes" is a
                   conflict, not a replay.
  channels         new, NOT NULL -- the channels the profile routed the
                   type to AT INTAKE. Replaces the action_data key
                   "_channels", which is removed from every row: a
                   channel inside the letter was a channel named by the
                   caller.
  correlation      new, nullable -- the product's own reference from
                   the envelope, stored untouched.
  expiry_layer     new, NOT NULL -- which layer decided expiry_at
                   (envelope / profile / default).

NEW TABLE `intake_outcomes`: requests that were NOT accepted, under
their key -- a rejection at intake or a conflict with an accepted job
(app/engine/models.py IntakeOutcome). The unique index over (key,
fingerprint, outcome) makes a replay of the same bytes record nothing
new.

BACKFILL OF EXISTING ROWS, and why each value:

  idempotency_key  'pre-0012:<id>' where NULL. Unique by construction
                   (the id is), and recognisable, which is what makes
                   the downgrade exact.
  fingerprint      a sentinel that contains a colon and dashes, so it
                   can never equal a SHA-256 hex digest. The original
                   bytes are not recoverable; a replay of a pre-0012
                   event under its old key is therefore a CONFLICT --
                   loud is the honest answer when the bytes cannot be
                   compared.
  channels         action_data->'_channels', or ["in_app"] where the key
                   was absent -- exactly what the old resolve stage
                   fell back to, so no row changes its route.
  action_data      '_channels' removed; a document that becomes {} is
                   stored as NULL, which is the form create_notification
                   started from (it wrote {"_channels": [...]} when the
                   caller passed no action_data).
  expiry_layer     'envelope' where expiry_at is set (before F1.2 the
                   caller was its only source), else 'default'.

THE DOWNGRADE RESTORES every row create_notification ever wrote:
backfilled keys go back to NULL, "_channels" goes back into
action_data, the new columns and the table are dropped and the partial
index returns. One form comes back different, and it is equivalent: a
row inserted around create_notification WITHOUT "_channels" returns
with "_channels": ["in_app"] -- the value the old resolve stage read
for it anyway. Pinned by tests/test_migration_0012.py on a non-empty
table of every form.

PLAIN ALTERs ARE SAFE HERE, and would not always be: comms tables are
young. On an accumulated notifications table the backfill UPDATEs and
the SET NOT NULL scans would hold locks for their duration -- the same
trap migration 0009 names for CREATE INDEX.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0012_envelope_intake"
down_revision: str | None = "0011_delivery_schedule"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_BACKFILL_KEY_PREFIX = "pre-0012:"
# 64 characters, not a hex digest (':' and '-' are not hex digits).
_FINGERPRINT_SENTINEL = "pre-0012:no-bytes".ljust(64, "-")


def upgrade() -> None:
    op.add_column(
        "notifications", sa.Column("fingerprint", sa.String(64), nullable=True),
    )
    op.add_column(
        "notifications",
        sa.Column("channels", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "notifications", sa.Column("correlation", sa.String(200), nullable=True),
    )
    op.add_column(
        "notifications", sa.Column("expiry_layer", sa.String(20), nullable=True),
    )

    op.execute(
        sa.text(
            "UPDATE notifications SET idempotency_key = :prefix || id::text "
            "WHERE idempotency_key IS NULL"
        ).bindparams(prefix=_BACKFILL_KEY_PREFIX)
    )
    op.execute(
        sa.text("UPDATE notifications SET fingerprint = :sentinel").bindparams(
            sentinel=_FINGERPRINT_SENTINEL,
        )
    )
    op.execute(
        "UPDATE notifications SET channels = "
        "COALESCE(action_data->'_channels', '[\"in_app\"]'::jsonb)"
    )
    op.execute(
        "UPDATE notifications SET action_data = CASE "
        "WHEN (action_data - '_channels') = '{}'::jsonb THEN NULL "
        "ELSE action_data - '_channels' END "
        "WHERE action_data ? '_channels'"
    )
    op.execute(
        "UPDATE notifications SET expiry_layer = CASE "
        "WHEN expiry_at IS NOT NULL THEN 'envelope' ELSE 'default' END"
    )

    for column in ("idempotency_key", "fingerprint", "channels", "expiry_layer"):
        op.alter_column("notifications", column, nullable=False)

    op.drop_index("uq_notifications_idempotency_key", table_name="notifications")
    op.create_index(
        "uq_notifications_idempotency_key",
        "notifications",
        ["idempotency_key"],
        unique=True,
    )

    op.create_table(
        "intake_outcomes",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("idempotency_key", sa.String(200), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("outcome", sa.String(30), nullable=False),
        sa.Column("reason", sa.String(2000), nullable=False),
        sa.Column("notification_id", sa.UUID(), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["notification_id"], ["notifications.id"], ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_intake_outcomes_key_fingerprint_outcome",
        "intake_outcomes",
        ["idempotency_key", "fingerprint", "outcome"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "uq_intake_outcomes_key_fingerprint_outcome",
        table_name="intake_outcomes",
    )
    op.drop_table("intake_outcomes")

    op.drop_index("uq_notifications_idempotency_key", table_name="notifications")
    op.alter_column("notifications", "idempotency_key", nullable=True)
    op.execute(
        sa.text(
            "UPDATE notifications SET idempotency_key = NULL "
            "WHERE idempotency_key LIKE :pattern"
        ).bindparams(pattern=_BACKFILL_KEY_PREFIX + "%")
    )
    op.create_index(
        "uq_notifications_idempotency_key",
        "notifications",
        ["idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )
    op.execute(
        "UPDATE notifications SET action_data = "
        "COALESCE(action_data, '{}'::jsonb) "
        "|| jsonb_build_object('_channels', channels)"
    )

    op.drop_column("notifications", "expiry_layer")
    op.drop_column("notifications", "correlation")
    op.drop_column("notifications", "channels")
    op.drop_column("notifications", "fingerprint")
