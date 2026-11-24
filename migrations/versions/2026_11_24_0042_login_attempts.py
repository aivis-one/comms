"""The sign-in attempts of the option `login` (L-2).

Revision ID: 0042_login_attempts
Revises: 0041_correlation_index
Create Date: 2026-11-24

ONE NEW TABLE, login_attempts, and its index; nothing of another table
changes. A row is one start of a sign-in (POST /api/v1/login/start):
the FINGERPRINT of its `state` (SHA-256 hex -- the state itself is never
stored), the provider, the product's redirect_uri, the PKCE verifier,
bot_access, created_at, expires_at (10 minutes after), taken_at. The
verifier is erased when the attempt is taken; the row is deleted by the
worker's purge 24 hours after it was created (app/login/attempts.py).

  ck_login_attempts_provider             the one provider of v4.2
  ck_login_attempts_state_hash           64 lowercase hex: a fingerprint,
                                         never a state
  ck_login_attempts_verifier_until_taken the verifier is there exactly
                                         while the attempt is not taken

redirect_uri is at most 2048 characters: REDIRECT_URI_MAX of
app/login/attempts.py, written here as a number -- a migration of v4
imports nothing of `app` (tests/test_migration_chain.py).

ix_login_attempts_created_at serves the purge (created_at older than
24 hours); it is declared in the model too (app/login/models.py), so
autogenerate compares it. The CHECKs are listed in
app/core/schema_objects.py MIGRATION_OWNED_CHECKS.

ONE STEP, no autocommit_block: the table is new, it holds no row a
lock could stop; a failure rolls the whole step back with its stamp.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0042_login_attempts"
down_revision: str | None = "0041_correlation_index"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "login_attempts",
        sa.Column("state_hash", sa.String(64), primary_key=True),
        sa.Column("provider", sa.String(16), nullable=False),
        sa.Column("redirect_uri", sa.String(2048), nullable=False),
        sa.Column("code_verifier", sa.String(128), nullable=True),
        sa.Column("bot_access", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("taken_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "provider = 'telegram'", name="ck_login_attempts_provider",
        ),
        sa.CheckConstraint(
            "state_hash ~ '^[0-9a-f]{64}$'", name="ck_login_attempts_state_hash",
        ),
        sa.CheckConstraint(
            "(taken_at IS NULL) = (code_verifier IS NOT NULL)",
            name="ck_login_attempts_verifier_until_taken",
        ),
    )
    op.create_index(
        "ix_login_attempts_created_at", "login_attempts", ["created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_login_attempts_created_at", table_name="login_attempts")
    op.drop_table("login_attempts")
