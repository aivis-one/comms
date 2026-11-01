"""The product's own email lives until the job's outcome (ST-1).

Revision ID: 0038_notification_email_open
Revises: 0037_pipeline_transient_since
Create Date: 2026-11-01

BEFORE: `notifications.email` (0036) lived with the row, like title and
body, until the retention -- 90 days by default -- though only the email
channel ever reads it, at its attempts, and no answer of comms carries
it.

AFTER (spec §16.3, the owner's decision of 1 November 2026): the block is
cleared at the job's outcome, as the model's letter is
(app/engine/service.py forget_channel_letters, at every transition that
writes an outcome), and the database holds it:

  ck_notifications_email_open -- a job holds its block only while it is
    not closed: `email IS NULL OR status IN (<active>)`. The statuses are
    app.engine.constants.ACTIVE_STATUSES -- one copy, as 0037's
    ck_notifications_transient_wait_open; a test holds the database's
    CHECK to the set, so a change of the set fails until a migration
    follows it.

The jobs already closed with a block (the boxes have had the block since
3.12.0) are brought to the invariant here, once, before the VALIDATE:
`UPDATE ... SET email = NULL` of the closed jobs that hold one. That is
no second format and no transitional period -- the data is made what
the invariant says, the UPDATE is idempotent (a second run finds no row),
and no code reads the old shape.

NO PASS OVER THE TABLE UNDER ACCESS EXCLUSIVE (migrations/env.py's
header). The CHECK is added NOT VALID -- the catalog only, ACCESS
EXCLUSIVE for milliseconds, committed before the block -- so from then on
every new or updated row is checked. Then, in the autocommit_block, each
statement in a transaction of its own:
  the UPDATE -- ROW EXCLUSIVE and the locks of the rows it clears (closed
    jobs, which no attempt writes); BEFORE the block it would share a
    transaction with the ADD, and hold ACCESS EXCLUSIVE for its whole
    pass over the heap;
  the VALIDATE -- SHARE UPDATE EXCLUSIVE: reads and writes go on.
A row closed between the ADD and the UPDATE is checked at its write: the
code of this release clears the block on every outcome, and the old code
does not run meanwhile (comms-deploy.sh stop_app stops the consumer, the
worker and the app before comms-app migrates).

IDEMPOTENT UP TO THE BLOCK: a VALIDATE that fails (its lock_timeout, a
lost connection) leaves the NOT VALID constraint committed at 0037; the
next upgrade() drops it IF EXISTS, adds it again, clears again (no row)
and validates.

DOWN drops the constraint, in one transaction with the stamp. The blocks
cleared by UP are gone, and DOWN does not pretend to bring them back: a
closed job's block is read by nothing.
"""

from collections.abc import Sequence

from alembic import op

from app.engine.constants import ACTIVE_STATUSES

revision: str = "0038_notification_email_open"
down_revision: str | None = "0037_pipeline_transient_since"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ACTIVE_SQL = ", ".join(f"'{status.value}'" for status in ACTIVE_STATUSES)

CHECK_NAME = "ck_notifications_email_open"
CHECK_SQL = f"email IS NULL OR status IN ({_ACTIVE_SQL})"
# The jobs the CHECK would refuse: the one statement that brings them to
# it, written once from the CHECK's own set.
CLEAR_CLOSED_SQL = (
    "UPDATE notifications SET email = NULL "
    f"WHERE email IS NOT NULL AND status NOT IN ({_ACTIVE_SQL})"
)


def upgrade() -> None:
    op.execute(f"ALTER TABLE notifications DROP CONSTRAINT IF EXISTS {CHECK_NAME}")
    op.execute(
        f"ALTER TABLE notifications ADD CONSTRAINT {CHECK_NAME} "
        f"CHECK ({CHECK_SQL}) NOT VALID"
    )
    with op.get_context().autocommit_block():
        op.execute(CLEAR_CLOSED_SQL)
        op.execute(f"ALTER TABLE notifications VALIDATE CONSTRAINT {CHECK_NAME}")


def downgrade() -> None:
    """One transaction, the stamp included -- no autocommit_block
    (migrations/env.py's header)."""
    op.execute(f"ALTER TABLE notifications DROP CONSTRAINT {CHECK_NAME}")
