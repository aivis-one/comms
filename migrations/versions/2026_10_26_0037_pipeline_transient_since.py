"""A job's own wait on a transient database failure (TX-3).

Revision ID: 0037_pipeline_transient_since
Revises: 0036_notification_email
Create Date: 2026-10-26

BEFORE: a transient failure of the database on a pipeline step spent one
of the job's attempts like any failure, and at the ceiling closed it.
TX-3 gates such a job without spending an attempt -- and the journal must
say when that WAIT began, once, not on every retry 30 s apart.

The job's pipeline_attempts / pipeline_step / pipeline_error keep ONE
meaning: the counted failures and the last of them (0015's
ck_notifications_pipeline holds exactly that; untouched here). A wait is
not a counted failure, so it has fields of its own:

  pipeline_transient_since  timestamptz -- when the open wait began;
  pipeline_transient_step   varchar(10) -- the step it waits on;
  pipeline_transient_error  varchar(300) -- the failure's class and place.

Set by the first transient failure of a wait (step and error refreshed
when they change within it), cleared together by a step that succeeds, an
ordinary failure, and the job's close (app/engine/service.py
end_transient_wait).

  ck_notifications_transient_wait_whole -- the three are empty together or
    set together, the step one of PipelineStep (one copy: built from
    app.engine.constants.PipelineStep);
  ck_notifications_transient_wait_gated -- a wait only while the job is
    gated (pipeline_retry_at set);
  ck_notifications_transient_wait_open -- a wait only on a job not closed
    yet. The gated CHECK does not hold the close: no path that closes a
    job clears pipeline_retry_at. The statuses are
    app.engine.constants.ACTIVE_STATUSES -- one copy;
  ck_notifications_pipeline_gate REPLACED -- 0015 allowed a gate only
    after a counted attempt (pipeline_attempts > 0); a gate is now owed
    to a counted attempt OR to an open transient wait. DOWN puts 0015's
    definition back.

Tests hold the database's CHECKs to the code's sets, so a change of a set
fails until a migration follows it.

DOWN removes everything up created and restores ck_notifications_pipeline_gate
as 0015 defined it.

NO PASS OVER THE TABLE UNDER ACCESS EXCLUSIVE (FIX TX-3). Each CHECK is
added NOT VALID -- the catalog only, under ACCESS EXCLUSIVE for
milliseconds -- and VALIDATEd afterwards, each in a transaction of its
own (autocommit_block), under SHARE UPDATE EXCLUSIVE, which lets reads
and writes go on: an `ADD CONSTRAINT ... CHECK` checked every row of a
table that may hold hundreds of megabytes of heap for a few thousand
live rows (velo-test: 588 MB, 11 138 rows), and stopped every query of
it meanwhile. Between the ADD and the VALIDATE a new or updated row is
checked at once; the old ones satisfy the CHECKs by construction (new
columns NULL, the new gate wider than 0015's) -- VALIDATE confirms it.

IDEMPOTENT UP TO THE BLOCK: autocommit_block commits what came before
it, and alembic_version moves to 0037 only after upgrade() returns. A
VALIDATE that fails (its lock_timeout, a lost connection) leaves the
columns and the NOT VALID constraints committed at 0036; the next run
of upgrade() -- comms-app's restart runs it -- goes through again
(IF NOT EXISTS / DROP IF EXISTS) and validates. VALIDATE of a valid
constraint is a no-op. DOWN is one transaction, its gate added with a
full pass (downgrade() says why). The rules for every later migration:
migrations/env.py's header, held by tests/test_migration_safety.py.
"""

from collections.abc import Sequence

from alembic import op

from app.engine.constants import ACTIVE_STATUSES, PipelineStep

revision: str = "0037_pipeline_transient_since"
down_revision: str | None = "0036_notification_email"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ACTIVE_SQL = ", ".join(f"'{status.value}'" for status in ACTIVE_STATUSES)
_STEPS_SQL = ", ".join(f"'{step.value}'" for step in PipelineStep)

# 0015's definition, put back by down.
_GATE_0015 = "pipeline_retry_at IS NULL OR pipeline_attempts > 0"
_GATE_0037 = (
    "pipeline_retry_at IS NULL OR pipeline_attempts > 0 "
    "OR pipeline_transient_since IS NOT NULL"
)


_CHECKS = {
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
    "ck_notifications_pipeline_gate": _GATE_0037,
}


def _add_not_valid(name: str, definition: str) -> None:
    """The catalog only: drop the constraint if a half-applied run left
    it, add it NOT VALID -- no row is read."""
    op.execute(f"ALTER TABLE notifications DROP CONSTRAINT IF EXISTS {name}")
    op.execute(
        f"ALTER TABLE notifications ADD CONSTRAINT {name} "
        f"CHECK ({definition}) NOT VALID"
    )


def _validate(*names: str) -> None:
    """Each VALIDATE in a transaction of its own, under SHARE UPDATE
    EXCLUSIVE -- reads and writes of notifications go on meanwhile."""
    with op.get_context().autocommit_block():
        for name in names:
            op.execute(f"ALTER TABLE notifications VALIDATE CONSTRAINT {name}")


def upgrade() -> None:
    op.execute(
        "ALTER TABLE notifications "
        "ADD COLUMN IF NOT EXISTS pipeline_transient_since timestamptz, "
        "ADD COLUMN IF NOT EXISTS pipeline_transient_step varchar(10), "
        "ADD COLUMN IF NOT EXISTS pipeline_transient_error varchar(300)"
    )
    for name, definition in _CHECKS.items():
        _add_not_valid(name, definition)
    _validate(*_CHECKS)


def downgrade() -> None:
    """ONE TRANSACTION, the stamp included (FIX TX-3) -- no
    autocommit_block here: alembic moves alembic_version only after
    downgrade() returns, so a block would commit the schema's undoing
    while a later step of the same run could still roll the stamp back
    -- the stamp AHEAD of the schema, which neither a second downgrade
    (its columns gone) nor an upgrade (a no-op at "head") gets out of.
    0015's gate therefore comes back with a plain ADD CONSTRAINT and its
    full pass under ACCESS EXCLUSIVE: a downgrade on a box is a rare
    manual act, where being right weighs more than being available."""
    # A wait at zero attempts holds a gate 0015's CHECK refuses: the
    # wait goes with its columns below, and its gate with it -- first,
    # and in ONE statement with the wait's own fields, or 0037's CHECKs,
    # still standing, would refuse a wait without its gate.
    op.execute(
        "UPDATE notifications SET pipeline_retry_at = NULL, "
        "pipeline_transient_since = NULL, pipeline_transient_step = NULL, "
        "pipeline_transient_error = NULL "
        "WHERE pipeline_attempts = 0 AND pipeline_transient_since IS NOT NULL"
    )
    for name in (
        "ck_notifications_transient_wait_open",
        "ck_notifications_transient_wait_gated",
        "ck_notifications_transient_wait_whole",
        "ck_notifications_pipeline_gate",
    ):
        op.execute(f"ALTER TABLE notifications DROP CONSTRAINT {name}")
    op.execute(
        "ALTER TABLE notifications ADD CONSTRAINT "
        f"ck_notifications_pipeline_gate CHECK ({_GATE_0015})"
    )
    op.execute(
        "ALTER TABLE notifications "
        "DROP COLUMN pipeline_transient_error, "
        "DROP COLUMN pipeline_transient_step, "
        "DROP COLUMN pipeline_transient_since"
    )
