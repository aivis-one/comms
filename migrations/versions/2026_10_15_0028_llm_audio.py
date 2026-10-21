"""The audio body of an llm letter, by link (P5-2).

Revision ID: 0028_llm_audio
Revises: 0027_status_index_by_job
Create Date: 2026-10-15

BEFORE: an llm job's letter was all it asked (llm_calls.letter), and
its answer's snapshot named who answered and the letter's hash.

AFTER:

  llm_calls.audio       new, JSONB NULL -- the link to the letter's audio
                        body as the product sent it ({"url", "format"})
                        plus the type's max_audio_bytes at intake
                        ("max_bytes"); NULL for a letter without a body,
                        and cleared at the outcome with the letter.
  ck_llm_calls_audio_with_letter  new -- audio IS NULL OR letter IS NOT
                        NULL: a signed link never outlives its letter.
  llm_answers.audio_hash  new, VARCHAR(64) NULL -- SHA-256 of the body as
                        fetched and sent; NULL when the letter had none.

EXISTING ROWS: both columns are NULL on every row there is -- no letter
had a body before this revision -- and the CHECK holds on them.

Migration-owned: ck_llm_calls_audio_with_letter is listed in
app/core/schema_objects.py.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0028_llm_audio"
down_revision: str | None = "0027_status_index_by_job"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "llm_calls", sa.Column("audio", postgresql.JSONB(), nullable=True),
    )
    op.create_check_constraint(
        "ck_llm_calls_audio_with_letter",
        "llm_calls",
        "audio IS NULL OR letter IS NOT NULL",
    )
    op.add_column(
        "llm_answers", sa.Column("audio_hash", sa.String(64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("llm_answers", "audio_hash")
    op.drop_constraint(
        "ck_llm_calls_audio_with_letter", "llm_calls", type_="check",
    )
    op.drop_column("llm_calls", "audio")
