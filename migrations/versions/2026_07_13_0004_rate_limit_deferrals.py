"""rate limit deferrals -- 429 budget counter on deliveries

Revision ID: 0004_rate_limit_deferrals
Revises: 0003_prefs_gating
Create Date: 2026-07-13 18:00:00.000000

Phase 2.2 item 1: a channel 429 (Telegram rate limit) defers the
delivery via next_retry_at using the server-named retry_after,
WITHOUT burning an attempt -- same pattern as quiet hours. But the
deferral must be bounded: this column counts rate-limit deferrals
per delivery, and past settings.notification_max_rate_limit_deferrals
a 429 degrades to a regular transient failure (which is finite by
the attempts budget).

Why a real column and not a JSONB key: this is runtime STATE, not
channel configuration (channel_options is input); and "how many
deliveries are throttled right now" must be answerable with a WHERE
clause. Single append-only add_column; NOT NULL with server_default
"0" backfills existing rows in place; downgrade drops it. No index:
the column is never a search predicate on the hot path (read and
written by primary-key row access inside deliver).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_rate_limit_deferrals"
down_revision: str | None = "0003_prefs_gating"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the 429-deferral budget counter to deliveries."""
    op.add_column(
        "notification_deliveries",
        sa.Column(
            "rate_limit_deferrals",
            sa.SmallInteger(),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade() -> None:
    """Drop the 429-deferral budget counter."""
    op.drop_column("notification_deliveries", "rate_limit_deferrals")
