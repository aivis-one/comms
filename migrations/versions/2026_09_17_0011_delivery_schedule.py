"""delivery schedule -- allowed windows replace the quiet window (R-5)

Revision ID: 0011_delivery_schedule
Revises: 0010_section_members
Create Date: 2026-09-17 00:00:00.000000

WHAT TURNS OVER. Three columns held ONE quiet window: quiet_from,
quiet_to and quiet_days, where the days were the days the window
STARTED on. They are replaced by a single JSONB column holding the
periods during which delivery IS allowed, each period belonging to its
own weekday and never crossing midnight.

WHY THE POLARITY FLIPS, and it is not a matter of taste. The old model
described "do not disturb at night", which one product's screen states
directly and the other's does not: the second shows working hours --
"when you MAY reach me" -- and had to invert times in a proxy. After
the inversion its window became nocturnal, and a nocturnal window's
"start day" is the evening BEFORE the morning it covers. The result
was measurable: with "deliver Monday 09-21" the deploy delivered
Monday 00:00-09:00, which nobody asked for, and fell silent on Tuesday
morning, which nobody asked for either. Every attempt to fix it inside
the product moved the error to the other end or silently widened the
user's choice (asking for Mon+Wed also delivered on Tue).

WHY NOT SIMPLY "SEVERAL QUIET WINDOWS", which is what the product
asked for: it removes the cardinality limit and leaves the start-day
semantics in place -- measured too, the product's own two-window
formulation still delivered Monday morning. The thing to remove was
the concept, not the count.

NO MIDNIGHT CROSSING, BY CONSTRUCTION. A period that would cross is
written as two, one in each day. That is what deletes the start-day
notion: every period is owned by the day it falls in, so the day set
means what a reader assumes it means.

MINUTES, NOT TIMES, and 1440 IS A VALID END. The old model could not
express a whole day: 00:00 -> 23:59 was the closest form and left a
one-minute hole through which a notification went out at 23:59 -- on
the very day a user had marked as silent. An end of 1440 minutes is
exactly midnight, and the hole is gone.

NO DATA MIGRATION, AND THAT IS A FACT ABOUT TODAY, NOT A DECISION.
Both deploys carry zero rows with a configured window (the second
product has not launched, the first has no users on this feature yet),
which was confirmed before this migration was written. There is
therefore nothing to convert and no conversion code to maintain. The
downgrade restores the three columns empty, for the same reason.

LEGACY IS NOT KEPT. The old columns are dropped, not left nullable
"just in case": two shapes for one fact is the state this service
refuses everywhere else.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0011_delivery_schedule"
down_revision: str | None = "0010_section_members"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "recipients",
        sa.Column(
            "allowed_windows",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.drop_column("recipients", "quiet_days")
    op.drop_column("recipients", "quiet_to")
    op.drop_column("recipients", "quiet_from")


def downgrade() -> None:
    op.add_column(
        "recipients",
        sa.Column("quiet_from", sa.Time(), nullable=True),
    )
    op.add_column(
        "recipients",
        sa.Column("quiet_to", sa.Time(), nullable=True),
    )
    op.add_column(
        "recipients",
        sa.Column(
            "quiet_days",
            postgresql.ARRAY(sa.SmallInteger()),
            nullable=True,
        ),
    )
    op.drop_column("recipients", "allowed_windows")
