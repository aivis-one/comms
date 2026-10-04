"""The job keeps its type's transport retry (H1, spec §9.7).

Revision ID: 0021_job_retry_snapshot
Revises: 0020_push_outbox
Create Date: 2026-10-04

BEFORE: every delivery retried by the deploy's settings
(NOTIFICATION_MAX_DELIVERY_ATTEMPTS, NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS);
the profile's retry_max_attempts and retry_backoff_seconds were parsed
and never applied.

AFTER:

  notifications.retry_max_attempts     new, NOT NULL -- the attempt
                 ceiling of the job's deliveries, AT INTAKE.
  notifications.retry_backoff_seconds  new, NOT NULL -- their backoff
                 base, AT INTAKE. The cap stays the deploy's
                 (NOTIFICATION_RETRY_BACKOFF_MAX_SECONDS).
  Both are the type's declared value or, undeclared, the setting
  (app/profile/loader.py); a snapshot, like channels, category and
  push_on.

EXISTING ROWS take the values of the settings the migrating process
sees -- the same .env the worker runs with -- and why: until this
revision every job retried by those settings, so they are the rule each
existing job was in effect accepted with. A literal (the code's
defaults) would silently change the ceiling of jobs in flight on a
deploy that overrides them. The columns are added nullable, filled,
then made NOT NULL (the 0012 order) -- no lasting server default:
intake always names them.

The CHECKs are the settings' own ranges (app/core/config.py
NUMERIC_BOUNDS: notification_max_delivery_attempts 1..100,
notification_retry_backoff_base_seconds 0..86400), held equal by
tests/test_profile_retry_fields.py. Each tests IS NOT NULL first
(0013's rule).

THE DOWNGRADE drops both columns; delivery goes back to the settings.

Migration-owned: ck_notifications_retry_max_attempts and
ck_notifications_retry_backoff_seconds are listed in
app/core/schema_objects.py.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.core.config import settings

revision: str = "0021_job_retry_snapshot"
down_revision: str | None = "0020_push_outbox"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "notifications",
        sa.Column("retry_max_attempts", sa.Integer(), nullable=True),
    )
    op.add_column(
        "notifications",
        sa.Column("retry_backoff_seconds", sa.Integer(), nullable=True),
    )
    op.get_bind().execute(
        sa.text(
            "UPDATE notifications SET retry_max_attempts = :attempts, "
            "retry_backoff_seconds = :backoff"
        ),
        {
            "attempts": settings.notification_max_delivery_attempts,
            "backoff": settings.notification_retry_backoff_base_seconds,
        },
    )
    op.alter_column("notifications", "retry_max_attempts", nullable=False)
    op.alter_column("notifications", "retry_backoff_seconds", nullable=False)
    op.create_check_constraint(
        "ck_notifications_retry_max_attempts",
        "notifications",
        "retry_max_attempts IS NOT NULL AND "
        "retry_max_attempts BETWEEN 1 AND 100",
    )
    op.create_check_constraint(
        "ck_notifications_retry_backoff_seconds",
        "notifications",
        "retry_backoff_seconds IS NOT NULL AND "
        "retry_backoff_seconds BETWEEN 0 AND 86400",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_notifications_retry_backoff_seconds", "notifications", type_="check",
    )
    op.drop_constraint(
        "ck_notifications_retry_max_attempts", "notifications", type_="check",
    )
    op.drop_column("notifications", "retry_backoff_seconds")
    op.drop_column("notifications", "retry_max_attempts")
