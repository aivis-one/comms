"""A thread's revision -- a number its every write raises (RV-1).

Revision ID: 0035_thread_revision
Revises: 0034_threads_client_activity
Create Date: 2026-10-25

BEFORE: two writes of one thread that comms both performed could reach
a product in the opposite order, and nothing in the answers said which
was the later -- `assigned_at` is NULL after a release or a retag, is
taken by the application before the write queues on the row, and is not
in the answer at all.

AFTER: `threads.revision`, an integer, 1 for every thread that exists
now and for every new one; the database raises it by 1 on every row an
UPDATE of `threads` touches -- trigger threads_revision, BEFORE UPDATE,
FOR EACH ROW, function threads_revision_next(): NEW.revision :=
OLD.revision + 1. Whatever the writer put there is replaced, so the
rule has one copy and no path -- ORM, Core, raw SQL -- forgets or sets
it. An UPDATE that matches no row fires nothing.
ck_threads_revision_positive keeps "1 at birth, only upward" against an
insert that bypasses the default.

THE FIRST TRIGGER IN COMMS. `alembic check` does not see triggers or
functions; the database is asked about them directly
(app/core/schema_objects.py MIGRATION_OWNED_TRIGGERS,
tests/test_schema_objects.py), and this migration's up / down / up is
held by tests/test_migration_0035.py.

DOWN removes everything up created: the trigger, the function, the
CHECK, the column.

ix_threads_client STAYS -- the decision of 24.10 was to drop it in the
next migration that touches `threads`, by a measurement and a rule
fixed before it (tests/test_thread_revision.py TestTheClientIndex): it
goes only if every lookup of threads by client keeps off a Seq Scan in
its generic plan, served by ix_threads_client_activity or a dedup
index, at no more than 1.25 times its buffers. The participant's unread
summary fails that: its generic plan is a Seq Scan on threads with the
index as without it (the OR of the three roles), and the per-thread
unread counts are driven by the primary key. The dedup lookups and the
foreign-key check pass. So this migration does not touch the index.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0035_thread_revision"
down_revision: str | None = "0034_threads_client_activity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE threads ADD COLUMN revision integer NOT NULL DEFAULT 1"
    )
    op.execute(
        "ALTER TABLE threads ADD CONSTRAINT ck_threads_revision_positive "
        "CHECK (revision >= 1)"
    )
    op.execute(
        "CREATE FUNCTION threads_revision_next() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN "
        "NEW.revision := OLD.revision + 1; RETURN NEW; END $$"
    )
    op.execute(
        "CREATE TRIGGER threads_revision BEFORE UPDATE ON threads "
        "FOR EACH ROW EXECUTE FUNCTION threads_revision_next()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER threads_revision ON threads")
    op.execute("DROP FUNCTION threads_revision_next()")
    op.execute(
        "ALTER TABLE threads DROP CONSTRAINT ck_threads_revision_positive"
    )
    op.execute("ALTER TABLE threads DROP COLUMN revision")
