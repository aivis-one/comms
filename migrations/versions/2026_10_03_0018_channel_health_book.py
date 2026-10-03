"""Channel health and the address book read indexes (P2-4).

Revision ID: 0018_channel_health_book
Revises: 0017_transition_journal
Create Date: 2026-10-03

BEFORE: the journal (0017) is indexed for the path of one job and for
forgetting. A question over a window of time -- "what did each channel
answer in the last hour" -- scanned the whole table, and the journal
grows faster than any other (spec §6.8). The recipients table had no
index over an order, so a page of the address book sorted the whole book.

AFTER:
  ix_transitions_channel_window ON notification_transitions (at)
      INCLUDE (channel, outcome, failure_class)
      WHERE subject = 'channel'
    -- the channel health answer (app/engine/health.py): only channel
       rows are counted, the window is a range over `at`, and the three
       columns it groups by ride in the index, so the read never touches
       the heap for the counts.
  ix_recipients_book ON recipients (created_at, id)
    -- the address book listing (app/audience/book.py): the keyset
       order, ascending, so a page is a range scan that stops after
       limit + 1 rows.

A plain CREATE INDEX inside the migration's transaction, not
CONCURRENTLY (the 0016 rule): the journal is created by 0017, which
ships in the same release, so the index is built on a table that is
empty or nearly so; the recipients table is small.

Migration-owned: neither index is in the models (a partial index with
INCLUDE, and an index the ORM never needs to know), so both are listed
in app/core/schema_objects.py.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018_channel_health_book"
down_revision: str | None = "0017_transition_journal"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_transitions_channel_window",
        "notification_transitions",
        ["at"],
        postgresql_include=["channel", "outcome", "failure_class"],
        postgresql_where=sa.text("subject = 'channel'"),
    )
    op.create_index(
        "ix_recipients_book", "recipients", ["created_at", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_recipients_book", table_name="recipients")
    op.drop_index(
        "ix_transitions_channel_window",
        table_name="notification_transitions",
    )
