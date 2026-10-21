"""The llm channel: a job without an audience, its letter, its answer (P5-1).

Revision ID: 0026_llm_channel
Revises: 0025_pending_deliveries_index
Create Date: 2026-10-15

BEFORE: every job had a letter for people (title, body) and an
audience (target_type, target_value), and every delivery a recipient.

AFTER:

  notifications.title, .body, .target_type, .target_value  now NULLABLE
                 -- NULL exactly on a job of an llm type, whose letter is
                 the model's and which has no audience.
  ck_notifications_letter_by_kind  new -- channels exactly ["llm"] -> the
                 four are NULL; any other channels -> none of them is.
                 A CASE, not an equality of two booleans: an equality
                 would let a job for people lose one of the four.

  notification_deliveries.recipient_id  now NULLABLE -- NULL exactly on
                 the llm channel: the model is not a recipient.
  ck_deliveries_recipient_by_channel  new -- (recipient_id IS NULL) =
                 (channel = 'llm'); both sides are never NULL.

  ck_transitions_subject_shape  redefined -- a delivery or channel row
                 of the journal names its recipient, except on the llm
                 channel, where it never does (the delivery has none).
                 Job and gate rows keep their shape.

  llm_calls      new -- one row per llm job: the ordered key sets
                 (snapshot), the letter (cleared at the outcome), its
                 hash, how long the answer is kept (snapshot).
  llm_answers    new -- the model's answer: the raw response body (TEXT,
                 cleared at keep_until), the set, provider and model
                 that answered, when, until when.
  ix_llm_answers_keep  new -- (keep_until) WHERE body IS NOT NULL: the
                 purge's lookup of answers past their term
                 (app/engine/service.py purge_llm_answers); a purged row
                 leaves the index.

Both tables hang off the job by ON DELETE CASCADE; their primary key is
the job's id, which is also the cascade's lookup.

EXISTING ROWS satisfy both CHECKs as they are: no job has the channel
llm, so every one of them keeps its four fields and every delivery its
recipient.

THE DOWNGRADE deletes the llm jobs (with their deliveries, journal and
both new tables by the cascade) -- the columns cannot become NOT NULL
again over them -- then drops what this revision added.

Migration-owned: the two CHECKs and ix_llm_answers_keep are listed in
app/core/schema_objects.py.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0026_llm_channel"
down_revision: str | None = "0025_pending_deliveries_index"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LETTER_COLUMNS = ("title", "body", "target_type", "target_value")

# The journal's row shape (migration 0017), with the recipient rule of
# delivery and channel rows as the one varying part.
_SUBJECT_SHAPE = (
    "(subject = 'job' AND recipient_id IS NULL AND channel IS NULL) OR "
    "(subject IS NOT NULL AND subject IN ('delivery', 'channel') "
    "AND channel IS NOT NULL AND {person}) OR "
    "(subject = 'gate' AND recipient_id IS NOT NULL AND channel IS NULL)"
)


def upgrade() -> None:
    for column in _LETTER_COLUMNS:
        op.alter_column("notifications", column, nullable=True)
    op.create_check_constraint(
        "ck_notifications_letter_by_kind",
        "notifications",
        "CASE WHEN channels = '[\"llm\"]'::jsonb "
        "THEN title IS NULL AND body IS NULL "
        "AND target_type IS NULL AND target_value IS NULL "
        "ELSE title IS NOT NULL AND body IS NOT NULL "
        "AND target_type IS NOT NULL AND target_value IS NOT NULL END",
    )

    op.alter_column("notification_deliveries", "recipient_id", nullable=True)
    op.create_check_constraint(
        "ck_deliveries_recipient_by_channel",
        "notification_deliveries",
        "(recipient_id IS NULL) = (channel = 'llm')",
    )

    op.drop_constraint(
        "ck_transitions_subject_shape",
        "notification_transitions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_transitions_subject_shape",
        "notification_transitions",
        _SUBJECT_SHAPE.format(
            person="(recipient_id IS NULL) = (channel = 'llm')",
        ),
    )

    op.create_table(
        "llm_calls",
        sa.Column(
            "notification_id",
            sa.Uuid(),
            sa.ForeignKey("notifications.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("sets", postgresql.JSONB(), nullable=False),
        sa.Column("letter", postgresql.JSONB(), nullable=True),
        sa.Column("letter_hash", sa.String(64), nullable=False),
        sa.Column("answer_keep_seconds", sa.Integer(), nullable=False),
    )
    op.create_table(
        "llm_answers",
        sa.Column(
            "notification_id",
            sa.Uuid(),
            sa.ForeignKey("notifications.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("answered_set", sa.String(64), nullable=False),
        sa.Column("answered_provider", sa.String(200), nullable=True),
        sa.Column("answered_model", sa.String(200), nullable=True),
        sa.Column("answered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("keep_until", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_llm_answers_keep",
        "llm_answers",
        ["keep_until"],
        postgresql_where=sa.text("body IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_llm_answers_keep", table_name="llm_answers")
    op.drop_table("llm_answers")
    op.drop_table("llm_calls")
    op.execute(
        "DELETE FROM notifications WHERE channels = '[\"llm\"]'::jsonb"
    )
    op.drop_constraint(
        "ck_transitions_subject_shape",
        "notification_transitions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_transitions_subject_shape",
        "notification_transitions",
        _SUBJECT_SHAPE.format(person="recipient_id IS NOT NULL"),
    )
    op.drop_constraint(
        "ck_deliveries_recipient_by_channel",
        "notification_deliveries",
        type_="check",
    )
    op.alter_column("notification_deliveries", "recipient_id", nullable=False)
    op.drop_constraint(
        "ck_notifications_letter_by_kind", "notifications", type_="check",
    )
    for column in _LETTER_COLUMNS:
        op.alter_column("notifications", column, nullable=False)
