"""event ingest dedup -- idempotency_key on notifications

Revision ID: 0005_event_ingest_dedup
Revises: 0004_rate_limit_deferrals
Create Date: 2026-07-15 12:00:00.000000

Phase 3c item 2: the Redis Streams transport is at-least-once, so a
replayed notification_request event must not create a second
Notification. The producer supplies an idempotency_key (frozen event
contract, 1..200 chars); this migration adds the column plus a PARTIAL
unique index (WHERE idempotency_key IS NOT NULL) -- the database is
the arbiter, the consumer merely catches the IntegrityError and acks
the duplicate.

Why a column on notifications and not a processed_events table: the
dedup fact IS the notification row -- a second table would duplicate
its lifecycle and inherit the very same retention question (see the
KNOWN CEILING marker on the column in app/engine/models.py: dedup is
reliable while the stream trim horizon < NOTIFICATION_RETENTION_DAYS).

Nullable, no server_default: every pre-3c row and every notification
created by non-stream paths keeps NULL, and NULLs never collide in a
partial unique index. Downgrade drops index then column.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_event_ingest_dedup"
down_revision: str | None = "0004_rate_limit_deferrals"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "notifications",
        sa.Column(
            "idempotency_key",
            sa.String(length=200),
            nullable=True,
        ),
    )
    op.create_index(
        "uq_notifications_idempotency_key",
        "notifications",
        ["idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_notifications_idempotency_key",
        table_name="notifications",
    )
    op.drop_column("notifications", "idempotency_key")
