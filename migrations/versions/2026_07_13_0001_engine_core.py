"""engine core -- audience projection + notification tables

Revision ID: 0001_engine_core
Revises:
Create Date: 2026-07-13 00:00:00.000000

Changes:
  - CREATE TABLE recipients          (sync projection: product user ids)
  - CREATE TABLE group_memberships   (sync projection: opaque group keys)
  - CREATE TABLE notifications       (channel-agnostic event records)
  - CREATE TABLE notification_deliveries (per-recipient, per-channel)

Notes:
  - JSONB columns use postgresql.JSONB to match the ORM models exactly
    (the cbshome donor migration created sa.JSON while its model
    declared JSONB -- that drift is NOT reproduced here).
  - ix_notification_deliveries_inbox serves the unread-count / inbox
    queries (recipient_id, status, read_at), same intent as cbshome
    migration 0021.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_engine_core"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create audience projection and notification engine tables."""

    # -- recipients (sync projection; id = product user id) --
    op.create_table(
        "recipients",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("telegram_id", sa.BigInteger(), nullable=True),
        sa.Column("email", sa.String(320), nullable=True),
        sa.Column(
            "locale",
            sa.String(8),
            server_default="en",
            nullable=False,
        ),
        sa.Column(
            "active",
            sa.Boolean(),
            server_default="true",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_recipients_telegram_id", "recipients", ["telegram_id"])
    op.create_index("ix_recipients_active", "recipients", ["active"])

    # -- group_memberships (sync projection; opaque group keys) --
    op.create_table(
        "group_memberships",
        sa.Column("group_key", sa.String(100), nullable=False),
        sa.Column("recipient_id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["recipient_id"], ["recipients.id"], ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("group_key", "recipient_id"),
    )
    op.create_index(
        "ix_group_memberships_recipient_id",
        "group_memberships",
        ["recipient_id"],
    )

    # -- notifications --
    op.create_table(
        "notifications",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("type", sa.String(50), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("body", sa.String(5000), nullable=False),
        sa.Column("target_type", sa.String(20), nullable=False),
        sa.Column("target_value", sa.String(200), nullable=False),
        sa.Column(
            "action_data",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("priority", sa.Integer(), server_default="5", nullable=False),
        sa.Column(
            "scheduled_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expiry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status",
            sa.String(20),
            server_default="pending",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_notifications_type", "notifications", ["type"])
    op.create_index("ix_notifications_status", "notifications", ["status"])
    op.create_index(
        "ix_notifications_scheduled_status",
        "notifications",
        ["scheduled_at", "status"],
    )

    # -- notification_deliveries --
    op.create_table(
        "notification_deliveries",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("notification_id", sa.UUID(), nullable=False),
        sa.Column("recipient_id", sa.UUID(), nullable=False),
        sa.Column("channel", sa.String(20), nullable=False),
        sa.Column(
            "channel_options",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "status",
            sa.String(20),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_message", sa.String(2000), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["notification_id"], ["notifications.id"], ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["recipient_id"], ["recipients.id"], ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_notification_deliveries_notification_id",
        "notification_deliveries",
        ["notification_id"],
    )
    op.create_index(
        "ix_notification_deliveries_recipient_id",
        "notification_deliveries",
        ["recipient_id"],
    )
    op.create_index(
        "ix_notification_deliveries_status",
        "notification_deliveries",
        ["status"],
    )
    op.create_index(
        "ix_notification_deliveries_inbox",
        "notification_deliveries",
        ["recipient_id", "status", "read_at"],
    )


def downgrade() -> None:
    """Drop all Phase 1 tables (reverse order)."""
    op.drop_index(
        "ix_notification_deliveries_inbox",
        table_name="notification_deliveries",
    )
    op.drop_index(
        "ix_notification_deliveries_status",
        table_name="notification_deliveries",
    )
    op.drop_index(
        "ix_notification_deliveries_recipient_id",
        table_name="notification_deliveries",
    )
    op.drop_index(
        "ix_notification_deliveries_notification_id",
        table_name="notification_deliveries",
    )
    op.drop_table("notification_deliveries")

    op.drop_index("ix_notifications_scheduled_status", table_name="notifications")
    op.drop_index("ix_notifications_status", table_name="notifications")
    op.drop_index("ix_notifications_type", table_name="notifications")
    op.drop_table("notifications")

    op.drop_index(
        "ix_group_memberships_recipient_id", table_name="group_memberships",
    )
    op.drop_table("group_memberships")

    op.drop_index("ix_recipients_active", table_name="recipients")
    op.drop_index("ix_recipients_telegram_id", table_name="recipients")
    op.drop_table("recipients")
