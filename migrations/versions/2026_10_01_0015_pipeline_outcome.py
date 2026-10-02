"""A pipeline defect becomes an outcome (T12).

Revision ID: 0015_pipeline_outcome
Revises: 0014_resources_snapshot
Create Date: 2026-10-01

BEFORE: an exception in resolve / deliver / rollup rolled the attempt
back and left the notification exactly as it was -- selectable on the
next tick, with no outcome, ever. The selection is oldest-first and
capped, so poisoned rows numbering at least the batch size starved
every healthy notification behind them: delivery stopped whole.

AFTER, on notifications:

  pipeline_attempts   int NOT NULL DEFAULT 0 -- how many attempts of
                      the pipeline ended in an exception of comms' own.
  pipeline_retry_at   timestamptz NULL -- the gate: until then the row
                      is not selected (the pattern of next_retry_at on
                      deliveries). Set only while the job is active.
  pipeline_step       varchar(10) NULL -- where it tore: lock | resolve
                      | deliver | rollup | commit.
  pipeline_error      varchar(300) NULL -- the exception CLASS and the
                      PLACE (module:line) only. Never the exception's
                      text: a text raised from a template or a formatter
                      can carry the letter's variables, and comms keeps
                      no letter content in a record (spec §6.4).

  CHECK ck_notifications_pipeline: no failure recorded (attempts 0, step
  and error NULL) or a failure recorded (attempts > 0, step one of the
  five, error set). Every disjunct tests IS NOT NULL before IN, as in
  0013: a NULL operand would make the predicate NULL, which a CHECK
  lets through.
  CHECK ck_notifications_pipeline_gate: a gate only behind a recorded
  failure. Not tied to the status on purpose: expiry, cancellation and
  the fold close a job by paths that do not know the gate, and a gate
  left on a closed job is inert -- only active jobs are selected.

On notification_deliveries: ck_deliveries_failure_class is recreated
with one more class, `pipeline` -- the waiting deliveries of a job
whose pipeline exhausted its attempts. A defect of comms itself,
distinct from the channel's classes.

DOWNGRADE refuses while any delivery carries `pipeline`: the old CHECK
has no class for it, and picking one of the four channel classes would
be a guess.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015_pipeline_outcome"
down_revision: str | None = "0014_resources_snapshot"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_STEPS = ("lock", "resolve", "deliver", "rollup", "commit")
_FAILURE_CLASSES_BEFORE = (
    "transient_exhausted", "message_rejected", "configuration", "no_address",
)
_FAILURE_CLASSES = (*_FAILURE_CLASSES_BEFORE, "pipeline")


def _failure_class_check(classes: Sequence[str]) -> str:
    return (
        "(status = 'failed' AND failure_class IS NOT NULL AND failure_class IN ("
        + ", ".join(f"'{c}'" for c in classes)
        + ")) OR (status <> 'failed' AND failure_class IS NULL)"
    )


def upgrade() -> None:
    op.add_column(
        "notifications",
        sa.Column(
            "pipeline_attempts", sa.Integer(), nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "notifications",
        sa.Column(
            "pipeline_retry_at", sa.DateTime(timezone=True), nullable=True,
        ),
    )
    op.add_column(
        "notifications",
        sa.Column("pipeline_step", sa.String(10), nullable=True),
    )
    op.add_column(
        "notifications",
        sa.Column("pipeline_error", sa.String(300), nullable=True),
    )
    op.create_check_constraint(
        "ck_notifications_pipeline",
        "notifications",
        "(pipeline_attempts = 0 AND pipeline_step IS NULL "
        "AND pipeline_error IS NULL) OR "
        "(pipeline_attempts > 0 AND pipeline_step IS NOT NULL "
        "AND pipeline_step IN ("
        + ", ".join(f"'{s}'" for s in _STEPS)
        + ") AND pipeline_error IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_notifications_pipeline_gate",
        "notifications",
        "pipeline_retry_at IS NULL OR pipeline_attempts > 0",
    )
    op.drop_constraint(
        "ck_deliveries_failure_class", "notification_deliveries", type_="check",
    )
    op.create_check_constraint(
        "ck_deliveries_failure_class",
        "notification_deliveries",
        _failure_class_check(_FAILURE_CLASSES),
    )


def downgrade() -> None:
    bind = op.get_bind()
    count = bind.execute(sa.text(
        "SELECT count(*) FROM notification_deliveries "
        "WHERE failure_class = 'pipeline'"
    )).scalar_one()
    if count:
        raise RuntimeError(
            "migration 0015 downgrade refuses: "
            f"{count} deliveries carry failure_class 'pipeline', which the "
            "previous schema has no class for. Delete them, or stay at 0015."
        )
    op.drop_constraint(
        "ck_deliveries_failure_class", "notification_deliveries", type_="check",
    )
    op.create_check_constraint(
        "ck_deliveries_failure_class",
        "notification_deliveries",
        _failure_class_check(_FAILURE_CLASSES_BEFORE),
    )
    op.drop_constraint(
        "ck_notifications_pipeline_gate", "notifications", type_="check",
    )
    op.drop_constraint(
        "ck_notifications_pipeline", "notifications", type_="check",
    )
    for column in (
        "pipeline_error", "pipeline_step", "pipeline_retry_at",
        "pipeline_attempts",
    ):
        op.drop_column("notifications", column)
