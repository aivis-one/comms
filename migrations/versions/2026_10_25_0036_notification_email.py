"""The product's own email on a job (EM-1).

Revision ID: 0036_notification_email
Revises: 0035_thread_revision
Create Date: 2026-10-25

BEFORE: the email channel composed every message from the profile's
templates and the job's title and body, and sent the provider a subject
and a text only.

AFTER: `notifications.email`, nullable JSONB -- the product's own email,
{subject, text[, html]}, kept with the job like title and body and sent
to the provider exactly as it came (app/engine/formatters.py
EmailFormatter). NULL keeps the old composition, byte for byte.

  ck_notifications_email_not_json_null -- empty is SQL NULL, never the
    JSON value null (H13, the rule of every JSONB column, migration
    0030);
  ck_notifications_email_form -- an object of exactly subject, text and
    an optional html, each a string with something besides whitespace
    in it. A JSON null is left to the CHECK above, so each refusal has
    one name. The lengths are the intake's (app/core/constants.py
    MAX_EMAIL_*), written once: a second copy of the numbers here would
    be the one left behind.

DOWN removes everything up created: both CHECKs and the column.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0036_notification_email"
down_revision: str | None = "0035_thread_revision"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The JSONB column this migration adds, in 0030's form (table, column,
# nullable) -- tests/test_json_null.py reads it beside 0030's list and
# 0032's DROPPED_JSONB_COLUMNS.
ADDED_JSONB_COLUMNS = (("notifications", "email", True),)


def upgrade() -> None:
    op.execute("ALTER TABLE notifications ADD COLUMN email jsonb")
    op.execute(
        "ALTER TABLE notifications ADD CONSTRAINT "
        "ck_notifications_email_not_json_null "
        "CHECK (jsonb_typeof(email) <> 'null')"
    )
    op.execute(
        "ALTER TABLE notifications ADD CONSTRAINT ck_notifications_email_form "
        "CHECK (email IS NULL OR jsonb_typeof(email) = 'null' OR ("
        "jsonb_typeof(email) = 'object' "
        "AND (email - ARRAY['subject', 'text', 'html']) = '{}'::jsonb "
        "AND jsonb_typeof(email -> 'subject') = 'string' "
        "AND (email ->> 'subject') ~ '\\S' "
        "AND jsonb_typeof(email -> 'text') = 'string' "
        "AND (email ->> 'text') ~ '\\S' "
        "AND (NOT (email ? 'html') OR ("
        "jsonb_typeof(email -> 'html') = 'string' "
        "AND (email ->> 'html') ~ '\\S'))))"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE notifications DROP CONSTRAINT ck_notifications_email_form"
    )
    op.execute(
        "ALTER TABLE notifications DROP CONSTRAINT "
        "ck_notifications_email_not_json_null"
    )
    op.execute("ALTER TABLE notifications DROP COLUMN email")
