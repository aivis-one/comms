"""section membership -- who serves a section (T-67)

Revision ID: 0010_section_members
Revises: 0009_threads_operator_user_index
Create Date: 2026-08-20 00:00:00.000000

The roster behind three rules that until now all said "anybody":
thread visibility for an unclaimed pool, operate/claim authz on a
section thread, and the push for a message nobody has claimed yet.

EMPTY MEANS "SERVED BY ANYONE", NOT "MISCONFIGURED". A deployment that
declares no roster keeps the behaviour it had before this table
existed, in all three places -- that is the definition the code is
written against (app/messaging/membership.py), not a migration window
to be closed later. This matters concretely: the other product on this
codebase has section threads in production and will get this table
empty, so an empty table must be indistinguishable from no table at
all in behaviour.

SHAPE COPIED FROM group_memberships, the membership table this service
already had: a composite primary key over the pair, no surrogate id, no
timestamps, and an index on the second column so the reverse lookup
("which sections does this operator serve") does not scan.

ondelete CASCADE ON BOTH SIDES, which DIVERGES from the RESTRICT that
guards every thread reference to recipients (migration 0006). The
distinction is deliberate: a thread is immortal and must never lose the
identity it names, while a membership row is a current fact -- a
deleted section has no roster to keep, and a deleted recipient serves
nothing.

NO SEPARATE INDEX ON section_id: the composite primary key
(section_id, operator_id) is a b-tree with section_id leading, so the
"roster of this section" lookup -- the one the push and the authz
checks make -- is already served by it. The extra index is on
operator_id, which the primary key cannot serve.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010_section_members"
down_revision: str | None = "0009_threads_operator_user_index"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "section_members",
        sa.Column("section_id", sa.UUID(), nullable=False),
        sa.Column("operator_id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["section_id"], ["sections.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["operator_id"], ["recipients.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("section_id", "operator_id"),
    )
    op.create_index(
        "ix_section_members_operator_id",
        "section_members",
        ["operator_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_section_members_operator_id", "section_members")
    op.drop_table("section_members")
