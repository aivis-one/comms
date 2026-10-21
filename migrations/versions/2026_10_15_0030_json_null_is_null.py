"""An empty JSONB value is SQL NULL, never the JSON value null (H13).

Revision ID: 0030_json_null_is_null
Revises: 0029_status_index_with_id
Create Date: 2026-10-15

BEFORE: a bare JSONB column wrote Python None as the JSON value `null`
on every write form (an ORM insert or update, Core values()); only a
column left out of an INSERT was SQL NULL. A JSON `null` IS NOT NULL in
SQL, so a CHECK or a query asking "no value?" by IS NULL missed it:
ck_recipients_tombstone (allowed_windows IS NULL) refused the tombstone
of every recipient who ever had a schedule -- set, or set and cleared --
and the person could not be forgotten.

AFTER (the models declare app/core/database.py JSON_VALUE, none_as_null):

  data   every top-level JSON `null` of a NULLABLE JSONB column becomes
         SQL NULL -- one statement per column:
             notifications.action_data
             notification_deliveries.channel_options
             recipients.allowed_windows
             llm_calls.audio
             llm_calls.letter
         Only the top level: `{"a": null}` and `[null]` are values and
         stay. The two NOT NULL columns (notifications.channels,
         llm_calls.sets) have no "empty" to move a JSON null to; one
         there would make the CHECK below refuse this upgrade, by its
         name -- it is not expected (every writer of both passes a list).
  CHECK  ck_<table>_<column>_not_json_null -- jsonb_typeof(<column>) <>
         'null' -- on each of the SEVEN JSONB columns. For SQL NULL the
         expression is NULL and the CHECK passes; a JSON `null` written
         around the type (text(), JSON.NULL, a hand-made row) is refused.

No CHECK this moves a row against: the moved rows only start to satisfy
ck_recipients_tombstone, and ck_llm_calls_audio_with_letter (audio IS
NULL OR letter IS NOT NULL) has no row with an audio beside a JSON-null
letter (P5-2 wrote both as SQL NULL from the start).

THE DOWNGRADE drops the CHECKs and gives the data back as it is: which
rows were JSON `null` is not kept, and the code before this revision
reads JSON `null` and SQL NULL alike, as None -- a SQL NULL is a valid
row for it.

Migration-owned: the seven CHECKs are listed in
app/core/schema_objects.py.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0030_json_null_is_null"
down_revision: str | None = "0029_status_index_with_id"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# (table, column, nullable)
JSONB_COLUMNS: tuple[tuple[str, str, bool], ...] = (
    ("notifications", "channels", False),
    ("notifications", "action_data", True),
    ("notification_deliveries", "channel_options", True),
    ("recipients", "allowed_windows", True),
    ("llm_calls", "sets", False),
    # audio BEFORE letter: ck_llm_calls_audio_with_letter (audio IS NULL
    # OR letter IS NOT NULL) is checked on every row an UPDATE writes, so
    # the link must be empty before its letter is. (The model wrote audio
    # as SQL NULL from its first day, migration 0028; the order costs
    # nothing and holds for any row there is.)
    ("llm_calls", "audio", True),
    ("llm_calls", "letter", True),
)


def _check(table: str, column: str) -> str:
    return f"ck_{table}_{column}_not_json_null"


def upgrade() -> None:
    for table, column, nullable in JSONB_COLUMNS:
        if nullable:
            op.execute(
                f"UPDATE {table} SET {column} = NULL "
                f"WHERE {column} = 'null'::jsonb"
            )
    for table, column, _ in JSONB_COLUMNS:
        op.create_check_constraint(
            _check(table, column), table, f"jsonb_typeof({column}) <> 'null'",
        )


def downgrade() -> None:
    for table, column, _ in reversed(JSONB_COLUMNS):
        op.drop_constraint(_check(table, column), table, type_="check")
