"""The reminder cancel finds its jobs by an index on correlation (PERF-2).

Revision ID: 0041_correlation_index
Revises: 0040_template_unresolved
Create Date: 2026-11-05

BEFORE: reminder_cancel (app/engine/reminders.py cancel_reminders ->
close_notifications) selects `type IN (...) AND correlation = :c AND
status IN (active)` FOR UPDATE, and no index holds correlation. The
planner ANDs two bitmaps -- ix_notifications_type x
ix_notifications_status -- and reads every page they point at: measured
on a velo-shaped table (PERF-2 report), 2.3 ms / 1369 blocks at 100 000
jobs, 34.8 ms / 13 504 blocks at 1 000 000, for ~18 rows. It runs in the
consumer's transaction, under FOR UPDATE, and grows with the table.

NOW: ix_notifications_correlation, (correlation) WHERE correlation IS
NOT NULL. Measured on the same table: 0.04 ms / 42 blocks at 1 000 000,
the index 520 kB.
  - PARTIAL: most jobs carry no correlation (6 % of the measured
    velo-shaped flow) -- they are not in it.
  - correlation alone, not (correlation, type): a correlation is one
    practice or one booking (velo's practice_correlation /
    booking_correlation); the equality is already narrow, `type` would
    widen the key without narrowing the read. aivis sends no
    reminder_cancel.
  - correlation is written at intake and never updated, so the index
    takes nothing from HOT updates of the job row.

CONCURRENTLY, IN AN autocommit_block (0009's warning, 0037's form):
notifications on a box is not small -- velo-test held 588 MB before its
VACUUM FULL. A plain CREATE INDEX holds SHARE -- writes of notifications
wait for the whole build; CONCURRENTLY holds SHARE UPDATE EXCLUSIVE, and
no ACCESS EXCLUSIVE is taken at all. A CONCURRENTLY build that fails
leaves an INVALID index under the name, which IF NOT EXISTS would then
keep: so the block first drops whatever the name holds -- nothing, an
invalid leftover, or a valid index of a run whose stamp did not land --
and builds it again. Nothing runs before the block.

downgrade(): one transaction with the stamp (FIX TX-3, migrations/
env.py): a plain DROP INDEX, under ACCESS EXCLUSIVE for the catalog
change only.

Migration-owned (a partial index): listed in app/core/schema_objects.py.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0041_correlation_index"
down_revision: str | None = "0040_template_unresolved"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_notifications_correlation")
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_notifications_correlation "
            "ON notifications (correlation) WHERE correlation IS NOT NULL"
        )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_notifications_correlation")
