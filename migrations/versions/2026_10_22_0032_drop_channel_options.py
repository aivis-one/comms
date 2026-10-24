"""notification_deliveries.channel_options is gone -- nobody wrote it (G1).

Revision ID: 0032_drop_channel_options
Revises: 0031_unread_index
Create Date: 2026-10-22

BEFORE: a JSONB column from 0001, read by the telegram formatter alone
(button_text, disable_preview, silent over the template sheet), with
the CHECK ck_notification_deliveries_channel_options_not_json_null from
0030. Nothing in comms ever wrote it, and no product sent it: a
per-delivery override of a field the profile already owns, always
empty. A column that is always empty is not kept (no legacy).

AFTER: the column is dropped; its CHECK goes with it (a CHECK on one
column is dropped with that column). The telegram formatter reads the
template sheet and the channel's defaults only.

0030 lists the column among JSONB_COLUMNS and stays as it is (a
migration in main is not edited): the watchman of H13
(tests/test_json_null.py) holds the models to that list minus the
columns this migration drops.

downgrade restores the column empty, with its CHECK in the form 0030
gave it.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0032_drop_channel_options"
down_revision: str | None = "0031_unread_index"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# (table, column) this migration drops -- read by tests/test_json_null.py.
DROPPED_JSONB_COLUMNS: tuple[tuple[str, str], ...] = (
    ("notification_deliveries", "channel_options"),
)


def upgrade() -> None:
    op.drop_column("notification_deliveries", "channel_options")


def downgrade() -> None:
    op.add_column(
        "notification_deliveries",
        sa.Column("channel_options", JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_check_constraint(
        "ck_notification_deliveries_channel_options_not_json_null",
        "notification_deliveries",
        "jsonb_typeof(channel_options) <> 'null'",
    )
