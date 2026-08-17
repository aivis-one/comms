"""threads operator-user index -- the third participant role (T-51)

Revision ID: 0009_threads_operator_user_index
Revises: 0008_messaging_indexes
Create Date: 2026-08-17 00:00:00.000000

One PARTIAL index on threads(operator_value) WHERE operator_kind='user'.

WHY: "threads this participant takes part in" is an OR over three
roles -- client, assignee, and operator_value on a USER thread (the DM
operator). The first two are indexed already (ix_threads_client,
ix_threads_assignee, migration 0006); the third was not, so the OR
could only be answered by a sequential scan of `threads`, and it is
walked by every unread aggregate the product asks for (the bell, the
chat-list badges).

WHY PARTIAL: operator_value is the polymorphic half of the operator
pair -- a recipients.id for the USER form, a sections.id for the
SECTION form. Only the user form is a participant role, so indexing
the section rows would add write amplification for a lookup nobody
performs. The predicate matches the query's own
`operator_kind = 'user' AND operator_value = X` branch exactly, which
is what lets the planner use it.

PLAIN CREATE INDEX IS SAFE HERE, AND WOULD NOT ALWAYS BE. comms tables
are young (no installation has meaningful history yet), so the brief
ACCESS EXCLUSIVE-free-but-write-blocking build is a non-event. On an
ACCUMULATED database the same statement would block writes to
`threads` for the duration of the build, and the fix would be
CREATE INDEX CONCURRENTLY -- which cannot run inside alembic's
transactional DDL and therefore needs a migration written specially
for it (autocommit block), not a line changed in passing. That is a
separate decision; the trap is already tracked as its own task in the
neighbouring repository. Said here so the next person who copies this
file onto a live database knows what they are copying.

Raw op.execute: the partial predicate is expressed directly, matching
the style of 0008; the DROP mirror is symmetric.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0009_threads_operator_user_index"
down_revision: str | None = "0008_messaging_indexes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the partial index over the DM-operator participant role."""
    op.execute(
        "CREATE INDEX ix_threads_operator_user ON threads "
        "(operator_value) WHERE operator_kind = 'user'"
    )


def downgrade() -> None:
    """Drop the partial index."""
    op.execute("DROP INDEX ix_threads_operator_user")
