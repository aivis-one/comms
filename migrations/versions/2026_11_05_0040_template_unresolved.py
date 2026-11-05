"""A channel's `accepted` row names the template paths that did not resolve (TPL-1).

Revision ID: 0040_template_unresolved
Revises: 0039_section_roster_declared
Create Date: 2026-11-05

BEFORE (G5): a field of a template whose variable the job did not carry
fell back to the product's stored text, and only a warning in comms' log
said so -- the person got the letter without its form, the product knew
nothing.

NOW: the field prints its placeholder ("{paid_amount}") and the job's
journal says which: notification_transitions.template_unresolved, the
paths of the template that did not resolve in the letter the channel
took -- on the channel's `accepted` row only, written with it in its own
transaction (app/engine/journal.py record_channel_answer), so a torn
attempt keeps it with the row the next attempt closes the delivery from.
The template's text, never a value of the letter. NULL when every field
resolved; never an empty array -- two forms of "none" are one too many.

NO PASS OVER THE TABLE UNDER ACCESS EXCLUSIVE (FIX TX-3, the rules of
migrations/env.py): the journal is the largest table there is. The
column is nullable with no default -- a catalog change. The CHECK is
added NOT VALID (the catalog only, milliseconds) and VALIDATEd in an
autocommit_block, under SHARE UPDATE EXCLUSIVE, which lets reads and
writes go on; every existing row has the column NULL and satisfies it.
Up to the block the steps are idempotent (IF NOT EXISTS, DROP ... IF
EXISTS first): a VALIDATE that fails leaves the stamp at 0039, and the
next upgrade goes through again. DOWN is one transaction with the stamp.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0040_template_unresolved"
down_revision: str | None = "0039_section_roster_declared"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CHECK = "ck_transitions_template_unresolved"
_DEFINITION = (
    "template_unresolved IS NULL OR ("
    "subject = 'channel' AND outcome = 'accepted' "
    "AND cardinality(template_unresolved) >= 1)"
)


def upgrade() -> None:
    op.execute(
        "ALTER TABLE notification_transitions "
        "ADD COLUMN IF NOT EXISTS template_unresolved text[]"
    )
    op.execute(
        f"ALTER TABLE notification_transitions DROP CONSTRAINT IF EXISTS {_CHECK}"
    )
    op.execute(
        f"ALTER TABLE notification_transitions ADD CONSTRAINT {_CHECK} "
        f"CHECK ({_DEFINITION}) NOT VALID"
    )
    with op.get_context().autocommit_block():
        op.execute(
            f"ALTER TABLE notification_transitions VALIDATE CONSTRAINT {_CHECK}"
        )


def downgrade() -> None:
    """ONE TRANSACTION, the stamp included (FIX TX-3): the column goes,
    and its CHECK with it."""
    op.execute(
        "ALTER TABLE notification_transitions DROP COLUMN template_unresolved"
    )
