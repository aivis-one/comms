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


def upgrade() -> None:
    op.execute(
        "ALTER TABLE notifications "
        "ADD COLUMN pipeline_transient_since timestamptz, "
        "ADD COLUMN pipeline_transient_step varchar(10), "
        "ADD COLUMN pipeline_transient_error varchar(300)"
    )
    op.execute(
        "ALTER TABLE notifications ADD CONSTRAINT "
        "ck_notifications_transient_wait_whole CHECK ("
        "(pipeline_transient_since IS NULL AND pipeline_transient_step IS NULL "
        "AND pipeline_transient_error IS NULL) OR ("
        "pipeline_transient_since IS NOT NULL "
        f"AND pipeline_transient_step IN ({_STEPS_SQL}) "
        "AND pipeline_transient_error IS NOT NULL "
        "AND pipeline_transient_error <> ''))"
    )
    op.execute(
        "ALTER TABLE notifications ADD CONSTRAINT "
        "ck_notifications_transient_wait_gated CHECK ("
        "pipeline_transient_since IS NULL OR pipeline_retry_at IS NOT NULL)"
    )
    op.execute(
        "ALTER TABLE notifications ADD CONSTRAINT "
        "ck_notifications_transient_wait_open CHECK ("
        f"pipeline_transient_since IS NULL OR status IN ({_ACTIVE_SQL}))"
    )
    op.execute(
        "ALTER TABLE notifications DROP CONSTRAINT ck_notifications_pipeline_gate"
    )
    op.execute(
        "ALTER TABLE notifications ADD CONSTRAINT "
        f"ck_notifications_pipeline_gate CHECK ({_GATE_0037})"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE notifications DROP CONSTRAINT ck_notifications_pipeline_gate"
    )
    op.execute(
        "ALTER TABLE notifications ADD CONSTRAINT "
        f"ck_notifications_pipeline_gate CHECK ({_GATE_0015})"
    )
    for name in (
        "ck_notifications_transient_wait_open",
        "ck_notifications_transient_wait_gated",
        "ck_notifications_transient_wait_whole",
    ):
        op.execute(f"ALTER TABLE notifications DROP CONSTRAINT {name}")
    op.execute(
        "ALTER TABLE notifications "
        "DROP COLUMN pipeline_transient_error, "
        "DROP COLUMN pipeline_transient_step, "
        "DROP COLUMN pipeline_transient_since"
    )
