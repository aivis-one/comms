"""The client's thread list on an index of its own order (MSG-1).

Revision ID: 0034_threads_client_activity
Revises: 0033_message_variants
Create Date: 2026-10-24

BEFORE: a client's list (GET /api/v1/threads?client=) had
ix_threads_client (client) and ix_threads_activity (the global activity
order, no client) to choose from. Measured on pristine tables with
fresh statistics (tests/test_thread_list_client.py TestTheCost): a
client with 2000 threads among 40 000 cost 2127 buffers for one page of
100 -- the planner walked the global activity index and filtered by
client. The gate's rule, fixed before the measurement: over 300 buffers
-> this index.

AFTER: ix_threads_client_activity (client, COALESCE(last_message_at,
created_at) DESC, id DESC) -- the client's threads in exactly the
list's order, the keyset cursor an Index Cond, as ix_threads_activity
is for the operator. ix_threads_client stays: it serves the lookups by
client alone (participation, dedup) and dropping it is not this change.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0034_threads_client_activity"
down_revision: str | None = "0033_message_variants"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX ix_threads_client_activity ON threads "
        "(client, COALESCE(last_message_at, created_at) DESC, id DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX ix_threads_client_activity")
