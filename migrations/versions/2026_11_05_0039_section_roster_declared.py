"""A section remembers that its roster was declared (API-1).

Revision ID: 0039_section_roster_declared
Revises: 0038_drop_transient_wait
Create Date: 2026-11-05

BEFORE (T-67): "a section with no declared members is served by every
operator" -- so a section the product never kept a roster for and a
section the product removed every operator from were the same state,
and the second was open to everybody.

NOW: sections.roster_declared, set by the first
section_membership_changed with `member: true` and never cleared
(app/messaging/membership.py). Declared and empty -> nobody serves the
section; undeclared -> every operator does, as before.

THE SECTIONS THAT HAVE MEMBERS NOW ARE DECLARED. Every member row was
written by a `member: true` event, so for those sections the column's
definition is already a fact of the table: the UPDATE below states it,
it does not invent history. This is the new column's condition, not a
backfill -- without it the table would hold a state the code no longer
produces (members, roster undeclared), and the first departure of the
last member after the deploy would open the section to everybody.

THE LIMIT: a section whose roster was emptied BEFORE this migration has
no member row left to read and stays undeclared -- served by every
operator, as it is today. deploy/INTEGRATION.md ("Serves the section")
names it.

THE RULES OF migrations/env.py. upgrade() has no autocommit_block: one
transaction with the stamp; the ADD COLUMN is IF NOT EXISTS and the
UPDATE can run twice. A column with a constant default is a catalog
change in Postgres 11+, no rewrite. downgrade(): one transaction.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0039_section_roster_declared"
down_revision: str | None = "0038_drop_transient_wait"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE sections ADD COLUMN IF NOT EXISTS "
        "roster_declared boolean NOT NULL DEFAULT false"
    )
    op.execute(
        "UPDATE sections SET roster_declared = true "
        "WHERE NOT roster_declared AND EXISTS ("
        "SELECT 1 FROM section_members "
        "WHERE section_members.section_id = sections.id)"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE sections DROP COLUMN roster_declared")
