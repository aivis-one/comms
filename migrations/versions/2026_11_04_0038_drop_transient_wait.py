"""A job no longer waits in its row on a transient database failure (WK-2).

Revision ID: 0038_drop_transient_wait
Revises: 0037_pipeline_transient_since
Create Date: 2026-11-04

BEFORE (0037, TX-3): a transient failure of the database on a pipeline
step gated the job without spending an attempt, whether the database
answered or not, and the open wait had its own fields on the job.

NOW (WK-2, the consumer's rule, spec 16.3): while the database does not
answer, nothing is written -- the worker rests the job in its process;
while it answers, the failure is counted like any other
(app/engine/processor.py _record_failure). No path writes a wait into
the row any more, so its fields, CHECKs and the gate's third ground go:

  pipeline_transient_since, pipeline_transient_step,
  pipeline_transient_error -- dropped;
  ck_notifications_transient_wait_whole / _gated / _open -- dropped;
  ck_notifications_pipeline_gate -- back to 0015's definition: a gate
    only behind a counted attempt.

THE GATES OF THE OPEN WAITS. A job waiting in its row at zero attempts
holds a gate 0015's CHECK refuses; that gate had no ground but the wait,
and goes with it (as 0037's down does) -- the job becomes selectable at
once. A wait on a job with counted attempts keeps the counted failure's
gate. This is the restored CHECK's condition, not a backfill.

THE RULES OF migrations/env.py. upgrade(): every step before the block
is idempotent (DROP ... IF EXISTS; the UPDATE reads no dropped column),
the gate is added NOT VALID and VALIDATEd in an autocommit_block, under
SHARE UPDATE EXCLUSIVE. Nothing writes a zero-attempt gate between the
UPDATE and the ADD: `update` stops the worker before the migration
(deploy/comms-deploy.sh), and the new worker writes none. downgrade():
one transaction with the stamp, 0037's objects back in 0037's
definitions (built from the same constants), the gate added with its
full pass.
"""

from collections.abc import Sequence

from alembic import op

from app.engine.constants import ACTIVE_STATUSES, PipelineStep

revision: str = "0038_drop_transient_wait"
down_revision: str | None = "0037_pipeline_transient_since"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ACTIVE_SQL = ", ".join(f"'{status.value}'" for status in ACTIVE_STATUSES)
_STEPS_SQL = ", ".join(f"'{step.value}'" for step in PipelineStep)

_GATE_0015 = "pipeline_retry_at IS NULL OR pipeline_attempts > 0"
_GATE_0037 = (
    "pipeline_retry_at IS NULL OR pipeline_attempts > 0 "
    "OR pipeline_transient_since IS NOT NULL"
)

# 0037's CHECKs, put back by down.
_CHECKS_0037 = {
    "ck_notifications_transient_wait_whole": (
        "(pipeline_transient_since IS NULL AND pipeline_transient_step IS NULL "
        "AND pipeline_transient_error IS NULL) OR ("
        "pipeline_transient_since IS NOT NULL "
        f"AND pipeline_transient_step IN ({_STEPS_SQL}) "
        "AND pipeline_transient_error IS NOT NULL "
        "AND pipeline_transient_error <> '')"
    ),
    "ck_notifications_transient_wait_gated": (
        "pipeline_transient_since IS NULL OR pipeline_retry_at IS NOT NULL"
    ),
    "ck_notifications_transient_wait_open": (
        f"pipeline_transient_since IS NULL OR status IN ({_ACTIVE_SQL})"
    ),
}


def upgrade() -> None:
    for name in (*_CHECKS_0037, "ck_notifications_pipeline_gate"):
        op.execute(f"ALTER TABLE notifications DROP CONSTRAINT IF EXISTS {name}")
    # The gate of an open wait at zero attempts goes with the wait (see
    # the module's docstring).
    op.execute(
        "UPDATE notifications SET pipeline_retry_at = NULL "
        "WHERE pipeline_attempts = 0 AND pipeline_retry_at IS NOT NULL"
    )
    op.execute(
        "ALTER TABLE notifications ADD CONSTRAINT "
        f"ck_notifications_pipeline_gate CHECK ({_GATE_0015}) NOT VALID"
    )
    op.execute(
        "ALTER TABLE notifications "
        "DROP COLUMN IF EXISTS pipeline_transient_error, "
        "DROP COLUMN IF EXISTS pipeline_transient_step, "
        "DROP COLUMN IF EXISTS pipeline_transient_since"
    )
    with op.get_context().autocommit_block():
        op.execute(
            "ALTER TABLE notifications VALIDATE CONSTRAINT "
            "ck_notifications_pipeline_gate"
        )


def downgrade() -> None:
    """ONE TRANSACTION, the stamp included (migrations/env.py): no
    autocommit_block. The columns come back empty, so 0037's CHECKs hold
    every row; the wider 0037 gate holds every row 0015's did."""
    op.execute(
        "ALTER TABLE notifications "
        "ADD COLUMN pipeline_transient_since timestamptz, "
        "ADD COLUMN pipeline_transient_step varchar(10), "
        "ADD COLUMN pipeline_transient_error varchar(300)"
    )
    op.execute(
        "ALTER TABLE notifications DROP CONSTRAINT ck_notifications_pipeline_gate"
    )
    for name, definition in {
        **_CHECKS_0037,
        "ck_notifications_pipeline_gate": _GATE_0037,
    }.items():
        op.execute(
            f"ALTER TABLE notifications ADD CONSTRAINT {name} CHECK ({definition})"
        )
