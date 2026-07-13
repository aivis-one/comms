"""prefs + gating -- quiet-hours columns, category_mutes, index review

Revision ID: 0003_prefs_gating
Revises: 0002_delivery_retry_backoff
Create Date: 2026-07-13 12:00:00.000000

Changes (Phase 2):
  - recipients.timezone / quiet_from / quiet_to / quiet_days --
    per-recipient quiet-hours preferences. All nullable: NULL = not
    configured. quiet_days holds ISO weekdays (1=Mon..7=Sun) of window
    STARTS; quiet_from >= quiet_to means the window crosses midnight.
  - category_mutes -- per-recipient mutes of profile-declared
    preference categories. Presence of a row = muted. Category is a
    varchar validated against the profile registry at write time (no
    DB enum: the profile grows without migrations).

INDEX REVIEW (handoff item 8) -- every read path was re-checked
against the Phase 2 predicates; conclusion: NO index changes beyond
the deliberately ordered category_mutes PK.

  1. Worker poll (processor._select_due_ids):
       WHERE status IN (pending, processing) AND scheduled_at <= now
       ORDER BY priority, scheduled_at [+ EXISTS over deliveries]
     UNCHANGED by Phase 2. Mute gating runs at resolve time -- before
     any delivery rows exist -- so it adds nothing to this query.
     Quiet hours ride the EXISTS subquery's existing next_retry_at
     gate (0002). ix_notifications_status_scheduled_priority (0002)
     still matches the leading predicate exactly. Kept as is.

  2. Deliver-stage selection and the poll's EXISTS probe:
       WHERE notification_id = :id AND status = pending
         AND (next_retry_at IS NULL OR next_retry_at <= now)
     Quiet-hours deferral WRITES next_retry_at but the predicate shape
     is the same as in 0002. Both queries enter through
     ix_notification_deliveries_notification_id; deliveries per
     notification are few (recipients x channels of ONE notification),
     so a composite (notification_id, status, next_retry_at) would not
     beat the simple FK index. Not added.

  3. Mute probe (prefs.muted_recipient_ids), the ONLY new hot query:
       WHERE category = :cat AND recipient_id IN (:resolved_ids)
     Served by the PRIMARY KEY -- ordered (recipient_id, category) ON
     PURPOSE: the probe always arrives with a concrete recipient list
     (point lookups per recipient). The reverse order (category,
     recipient_id) would optimize "who muted X" audience scans, which
     nothing runs. No secondary index needed.

  4. Preference reads/writes (prefs.get_preferences / setters):
       WHERE recipient_id = :id [AND category = :cat]
     PK-prefix on category_mutes; recipients by PK. Covered.

  5. Sync receivers (audience/sync.py): recipients by PK;
     group_memberships by full PK (group_key, recipient_id). The
     resolver's reverse lookup (all members of a group) uses the PK
     prefix; ix_group_memberships_recipient_id (0001) covers the
     delete-by-recipient side. Covered.

  6. SKIPPED needs NO DDL: notifications.status is varchar(20)
     (deliberately not a PG enum), so the new value is append-only.
     ix_notifications_status (0001) serves any future status=skipped
     housekeeping scan the same as other statuses.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_prefs_gating"
down_revision: str | None = "0002_delivery_retry_backoff"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add quiet-hours columns to recipients; create category_mutes."""
    op.add_column(
        "recipients",
        sa.Column("timezone", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "recipients",
        sa.Column("quiet_from", sa.Time(), nullable=True),
    )
    op.add_column(
        "recipients",
        sa.Column("quiet_to", sa.Time(), nullable=True),
    )
    op.add_column(
        "recipients",
        sa.Column(
            "quiet_days",
            postgresql.ARRAY(sa.SmallInteger()),
            nullable=True,
        ),
    )

    op.create_table(
        "category_mutes",
        sa.Column("recipient_id", sa.Uuid(), nullable=False),
        sa.Column("category", sa.String(length=50), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["recipient_id"],
            ["recipients.id"],
            ondelete="CASCADE",
        ),
        # PK order (recipient_id, category) matches the resolve-time
        # mute probe -- see the index review in the module docstring.
        sa.PrimaryKeyConstraint("recipient_id", "category"),
    )


def downgrade() -> None:
    """Drop category_mutes; remove quiet-hours columns."""
    op.drop_table("category_mutes")

    op.drop_column("recipients", "quiet_days")
    op.drop_column("recipients", "quiet_to")
    op.drop_column("recipients", "quiet_from")
    op.drop_column("recipients", "timezone")
