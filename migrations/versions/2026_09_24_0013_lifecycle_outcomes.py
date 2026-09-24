"""lifecycle and outcome taxonomy (F1.3)

Revision ID: 0013_lifecycle_outcomes
Revises: 0012_envelope_intake
Create Date: 2026-09-24 00:00:00.000000

WHAT TURNS OVER.

  notification status  SKIPPED is removed; its two facts become
                       SUPPRESSED (every recipient muted -- their
                       decision) and NO_RECIPIENTS (the audience was
                       empty -- the product's sync). CANCELLED is new.
  delivery status      SKIPPED -> SUPPRESSED; EXPIRED and CANCELLED are
                       new (the parent was closed while the delivery
                       waited).
  failure_class        new, on deliveries: WHY a failed delivery failed.
                       CHECK ck_deliveries_failure_class: set exactly
                       when status is failed, from the closed set.
  wait_reason          new, on deliveries: WHY next_retry_at is set.
                       CHECK ck_deliveries_wait_reason: set exactly when
                       next_retry_at is, from the closed set, and only
                       on a pending delivery.
  category             new, on notifications: the type's category AT
                       INTAKE, so a type removed from the profile is
                       still mute-gated.

THE RULE: A ROW THIS MIGRATION CANNOT TRANSLATE WITHOUT A GUESS IS NOT
TRANSLATED -- the upgrade refuses, naming how many rows of each kind
stand in the way. Nothing is guessed, and there is no "unclassified"
value: that would be a second format of one fact. The kinds:

  active jobs              pending / processing notifications. Their
                           category snapshot cannot be filled (the
                           profile is not available here), and NULL
                           would silently switch their mute gate off.
  skipped_without_children an empty audience and "every recipient
                           muted at resolve" end identically -- no
                           deliveries -- and the row holds no trace of
                           which it was.
  skipped_mixed_children   a SKIPPED job whose deliveries are not ALL
                           skipped: not the one shape translated below.
  expired                  before F1.3 a cancellation ALSO wrote
                           EXPIRED; which expired rows were cancels is
                           not recorded anywhere.
  failed_without_children  a FAILED job without deliveries: which path
                           wrote it is a claim about every past version
                           of comms, not a fact in the row.
  with_failed_delivery     a failed delivery's class cannot be derived
                           from anything but its text, and a class read
                           from text is exactly what this migration
                           exists to end. The whole parent goes, not
                           the delivery alone: a PARTIAL_SENT left with
                           no failed child would contradict its own
                           fold.

What to do about them is one command: `deploy/comms-deploy.sh drain`
checks these same kinds and `drain --apply` deletes them
(deploy/INTEGRATION.md, "The protocol update window"). The command runs
THIS module's queries -- _BLOCKING_KINDS to count, _DRAIN_DELETES to
delete -- so the breakdown exists once. This refusal names the counts
and the command; it does not repeat the instructions.

TRANSLATED, because the row itself says what it is:
  - delivery SKIPPED -> SUPPRESSED (the late mute is the one writer of
    a skipped delivery, and "muted" is what the value says);
  - notification SKIPPED whose deliveries are ALL skipped ->
    SUPPRESSED;
  - next_retry_at cleared on every non-pending delivery: a finished
    delivery waits for nothing (the new wait-reason CHECK requires it;
    the old code left the last gate time behind on success);
  - category NULL on every remaining row: they are all outcomes, and an
    outcome is never gated again.

THE DOWNGRADE IS NOT EXACT, and it says where:
  - SUPPRESSED and NO_RECIPIENTS both go back to SKIPPED (a
    NO_RECIPIENTS that the fold wrote for a job whose deliveries were
    cascaded away also returns as SKIPPED, not FAILED);
  - CANCELLED goes back to EXPIRED -- what the old code wrote for a
    cancel;
  - delivery EXPIRED / CANCELLED go back to PENDING, which is where the
    old code left the waiting deliveries of a closed job;
  - failure_class, wait_reason and category are dropped, and the
    next_retry_at values cleared on upgrade are not restored.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013_lifecycle_outcomes"
down_revision: str | None = "0012_envelope_intake"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FAILURE_CLASSES = (
    "transient_exhausted", "message_rejected", "configuration", "no_address",
)
_WAIT_REASONS = (
    "recipient_schedule", "provider_rate_limit", "transient_backoff",
)

# The kinds of rows the upgrade refuses on, each with the query that
# counts them. The SAME breakdown, with the same names, is the check in
# deploy/INTEGRATION.md.
_BLOCKING_KINDS: dict[str, str] = {
    "active_jobs": (
        "SELECT count(*) FROM notifications "
        "WHERE status IN ('pending', 'processing')"
    ),
    "skipped_without_children": (
        "SELECT count(*) FROM notifications n WHERE n.status = 'skipped' "
        "AND NOT EXISTS (SELECT 1 FROM notification_deliveries d "
        "WHERE d.notification_id = n.id)"
    ),
    "skipped_mixed_children": (
        "SELECT count(*) FROM notifications n WHERE n.status = 'skipped' "
        "AND EXISTS (SELECT 1 FROM notification_deliveries d "
        "WHERE d.notification_id = n.id AND d.status <> 'skipped')"
    ),
    "expired": "SELECT count(*) FROM notifications WHERE status = 'expired'",
    "failed_without_children": (
        "SELECT count(*) FROM notifications n WHERE n.status = 'failed' "
        "AND NOT EXISTS (SELECT 1 FROM notification_deliveries d "
        "WHERE d.notification_id = n.id)"
    ),
    "with_failed_delivery": (
        "SELECT count(DISTINCT d.notification_id) "
        "FROM notification_deliveries d WHERE d.status = 'failed'"
    ),
}


# What `comms-deploy.sh drain --apply` deletes, per kind -- the SAME keys
# as _BLOCKING_KINDS (tests/test_drain_source.py holds them equal). Whole
# notifications go; their deliveries follow by ON DELETE CASCADE. Every
# active job goes, not only those scheduled ahead: while this migration
# refuses, comms-app is not healthy, so the worker and the consumer do
# not run and the queue cannot drain by waiting. The product's events
# wait in the stream and are read after the window.
_DRAIN_DELETES: dict[str, str] = {
    "active_jobs": (
        "DELETE FROM notifications WHERE status IN ('pending', 'processing')"
    ),
    "skipped_without_children": (
        "DELETE FROM notifications n WHERE n.status = 'skipped' "
        "AND NOT EXISTS (SELECT 1 FROM notification_deliveries d "
        "WHERE d.notification_id = n.id)"
    ),
    "skipped_mixed_children": (
        "DELETE FROM notifications n WHERE n.status = 'skipped' "
        "AND EXISTS (SELECT 1 FROM notification_deliveries d "
        "WHERE d.notification_id = n.id AND d.status <> 'skipped')"
    ),
    "expired": "DELETE FROM notifications WHERE status = 'expired'",
    "failed_without_children": (
        "DELETE FROM notifications n WHERE n.status = 'failed' "
        "AND NOT EXISTS (SELECT 1 FROM notification_deliveries d "
        "WHERE d.notification_id = n.id)"
    ),
    "with_failed_delivery": (
        "DELETE FROM notifications WHERE id IN (SELECT notification_id "
        "FROM notification_deliveries WHERE status = 'failed')"
    ),
}


def _refuse_on_ambiguous_rows() -> None:
    bind = op.get_bind()
    counts = {
        kind: bind.execute(sa.text(query)).scalar_one()
        for kind, query in _BLOCKING_KINDS.items()
    }
    blocking = {kind: count for kind, count in counts.items() if count}
    if blocking:
        listed = ", ".join(f"{kind}={count}" for kind, count in blocking.items())
        raise RuntimeError(
            "migration 0013 refuses: these rows cannot be translated "
            f"without a guess -- {listed}. Run `deploy/comms-deploy.sh "
            "drain` (deploy/INTEGRATION.md, 'The protocol update window')."
        )


def upgrade() -> None:
    _refuse_on_ambiguous_rows()

    op.add_column(
        "notification_deliveries",
        sa.Column("failure_class", sa.String(30), nullable=True),
    )
    op.add_column(
        "notification_deliveries",
        sa.Column("wait_reason", sa.String(30), nullable=True),
    )
    op.add_column(
        "notifications", sa.Column("category", sa.String(50), nullable=True),
    )

    op.execute(
        "UPDATE notification_deliveries SET status = 'suppressed' "
        "WHERE status = 'skipped'"
    )
    op.execute(
        "UPDATE notifications SET status = 'suppressed' WHERE status = 'skipped'"
    )
    op.execute(
        "UPDATE notification_deliveries SET next_retry_at = NULL "
        "WHERE status <> 'pending'"
    )

    # Every disjunct tests IS NOT NULL explicitly before IN: a NULL
    # operand makes `x IN (...)` NULL, the whole predicate NULL, and a
    # CHECK lets NULL through as if it were true -- which is exactly
    # the "failed without a class" row this constraint exists to stop.
    op.create_check_constraint(
        "ck_deliveries_failure_class",
        "notification_deliveries",
        "(status = 'failed' AND failure_class IS NOT NULL AND failure_class IN ("
        + ", ".join(f"'{c}'" for c in _FAILURE_CLASSES)
        + ")) OR (status <> 'failed' AND failure_class IS NULL)",
    )
    op.create_check_constraint(
        "ck_deliveries_wait_reason",
        "notification_deliveries",
        "(next_retry_at IS NULL AND wait_reason IS NULL) OR "
        "(next_retry_at IS NOT NULL AND status = 'pending' "
        "AND wait_reason IS NOT NULL AND wait_reason IN ("
        + ", ".join(f"'{r}'" for r in _WAIT_REASONS)
        + "))",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_deliveries_wait_reason", "notification_deliveries", type_="check",
    )
    op.drop_constraint(
        "ck_deliveries_failure_class", "notification_deliveries", type_="check",
    )
    op.execute(
        "UPDATE notifications SET status = 'skipped' "
        "WHERE status IN ('suppressed', 'no_recipients')"
    )
    op.execute(
        "UPDATE notifications SET status = 'expired' WHERE status = 'cancelled'"
    )
    op.execute(
        "UPDATE notification_deliveries SET status = 'skipped' "
        "WHERE status = 'suppressed'"
    )
    op.execute(
        "UPDATE notification_deliveries SET status = 'pending' "
        "WHERE status IN ('expired', 'cancelled')"
    )
    op.drop_column("notifications", "category")
    op.drop_column("notification_deliveries", "wait_reason")
    op.drop_column("notification_deliveries", "failure_class")
