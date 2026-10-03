"""The transition journal (spec §6.4, P2-1).

Revision ID: 0017_transition_journal
Revises: 0016_inbox_index
Create Date: 2026-10-03

BEFORE: comms kept only the CURRENT state. Every next attempt
overwrote the previous one's error text; the path of a job could not be
told after the fact.

AFTER: notification_transitions -- one row per transition of a job or
of a delivery, per channel answer and per recipient a gate dropped.
Rows are only ever added (app/engine/journal.py is the one writer); the
ONE edit is forgetting (app/engine/service.py, withdraw_recipient),
which clears provider_text of the forgotten recipient's rows.

  id             bigint identity -- THE ORDER. Unique inside one
                 transaction, where timestamps tie.
  xact_id        xid8 DEFAULT pg_current_xact_id() -- the writing
                 transaction. Allocation order is not commit order (A
                 takes 10, B takes 11, B commits first): a "changed
                 since" cursor needs commit visibility, which this
                 column gives and which cannot be backfilled later.
  notification_id  FK -> notifications ON DELETE CASCADE -- the only way
                 a row is ever deleted; the job's retention is the
                 journal's.
  recipient_id   uuid NULL, NO foreign key: a recipient row is a
                 tombstone and never deleted (app/forgetting.py), and a
                 second deletion path is exactly what the journal must
                 not have.
  channel        varchar(20) NULL.
  subject        job | delivery | channel | gate.
  step           where the transition happened.
  outcome        the status after it (job, delivery), the channel's
                 answer (channel), `suppressed` (gate).
  attempt        int NOT NULL >= 0.
  wait_reason, wait_until -- both or neither.
  failure_class  varchar(30) NULL.
  category       the category a gate decided by.
  error          varchar(300) NULL -- a comms exception's CLASS and
                 PLACE, never its text.
  provider_text  varchar(2000) NULL -- a provider's answer, sanitized;
                 only on a channel row, never blank.
  at             timestamptz DEFAULT clock_timestamp() -- data, not key.

No provider_message_id, no response code column: no adapter returns
either today (ChannelFormatter.deliver -> bool); a column that is
always NULL would document a state that cannot occur.

Every CHECK tests IS NOT NULL before IN (0013's rule: a NULL operand
makes the predicate NULL, which a CHECK lets through).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.types import UserDefinedType

revision: str = "0017_transition_journal"
down_revision: str | None = "0016_inbox_index"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "notification_transitions"


class _XID8(UserDefinedType):  # type: ignore[type-arg]
    """Postgres xid8, declared here rather than imported: a migration
    in main never changes, the module a type lives in may."""

    cache_ok = True

    def get_col_spec(self, **kw: object) -> str:
        return "xid8"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column(
            "id", sa.BigInteger(), sa.Identity(always=True), primary_key=True,
        ),
        sa.Column(
            "xact_id", _XID8(), nullable=False,
            server_default=sa.text("pg_current_xact_id()"),
        ),
        sa.Column(
            "notification_id", sa.Uuid(),
            sa.ForeignKey("notifications.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("recipient_id", sa.Uuid(), nullable=True),
        sa.Column("channel", sa.String(20), nullable=True),
        sa.Column("subject", sa.String(10), nullable=False),
        sa.Column("step", sa.String(20), nullable=False),
        sa.Column("outcome", sa.String(30), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("wait_reason", sa.String(30), nullable=True),
        sa.Column("wait_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_class", sa.String(30), nullable=True),
        sa.Column("category", sa.String(50), nullable=True),
        sa.Column("error", sa.String(300), nullable=True),
        sa.Column("provider_text", sa.String(2000), nullable=True),
        sa.Column(
            "at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
    )
    # A job row names no recipient and no channel; a delivery or a
    # channel row names both; a gate row names the recipient it dropped.
    op.create_check_constraint(
        "ck_transitions_subject_shape",
        _TABLE,
        "(subject = 'job' AND recipient_id IS NULL AND channel IS NULL) OR "
        "(subject IS NOT NULL AND subject IN ('delivery', 'channel') "
        "AND recipient_id IS NOT NULL AND channel IS NOT NULL) OR "
        "(subject = 'gate' AND recipient_id IS NOT NULL AND channel IS NULL)",
    )
    op.create_check_constraint(
        "ck_transitions_wait",
        _TABLE,
        "(wait_reason IS NULL AND wait_until IS NULL) OR "
        "(wait_reason IS NOT NULL AND wait_until IS NOT NULL)",
    )
    # The provider's words belong to a channel answer only, and "no
    # words" is NULL, never a blank string.
    op.create_check_constraint(
        "ck_transitions_provider_text",
        _TABLE,
        "provider_text IS NULL OR "
        "(subject = 'channel' AND provider_text <> '')",
    )
    op.create_check_constraint(
        "ck_transitions_error_not_blank",
        _TABLE,
        "error IS NULL OR error <> ''",
    )
    op.create_check_constraint(
        "ck_transitions_attempt_not_negative",
        _TABLE,
        "attempt >= 0",
    )
    # The path of one job, in order (and the CASCADE's lookup).
    op.create_index(
        "ix_transitions_path", _TABLE, ["notification_id", "id"],
    )
    # Forgetting clears provider_text by recipient: only rows that
    # carry text are indexed.
    op.create_index(
        "ix_transitions_forgetting", _TABLE, ["recipient_id"],
        postgresql_where=sa.text("provider_text IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_transitions_forgetting", table_name=_TABLE)
    op.drop_index("ix_transitions_path", table_name=_TABLE)
    op.drop_table(_TABLE)
