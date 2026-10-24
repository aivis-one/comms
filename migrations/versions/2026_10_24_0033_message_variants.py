"""A message in another language: variants and variant requests (P5-4).

Revision ID: 0033_message_variants
Revises: 0032_drop_channel_options
Create Date: 2026-10-24

BEFORE: a thread's message existed in its author's words only; comms
had no language anywhere in messaging.

AFTER: two tables next to `messages`, both insert-only, both going
with their message (ON DELETE CASCADE -- the only way a message row
can leave, app/variants.py header):

  message_variant_requests -- "a variant of this message in this
    language was asked for, by this job": the product writes it when it
    queues the model. Several per (message, locale), one per job; the
    latest is the one read.

  message_variants -- the variant, one per (message, locale), in one of
    three forms (ck_message_variants_form): the text with the language
    the model found the message written in (another than the variant's);
    "none is needed" (no text, that language IS the variant's); or
    "declined" (no text -- the product read the answer and writes none).

A language is an opaque token of a form, 1..8 of [A-Za-z0-9_-]
(app/messaging/constants.py VARIANT_LOCALE_PATTERN), compared byte for
byte. Each table carries the caller's Idempotency-Key and its request's
digest, as `messages` does (F1.4). The job is named by its key, a
string: retention deletes jobs, the variant lives with its message.

A REQUEST REMEMBERS ITS JOB BY ID (3.8.0 -- fix): `job_id`, without a
foreign key for the same reason. Retention frees a job's key, and a
later job under the same key is ANOTHER job; the request's state is
its own job's, read by id. One request per (message, locale, job).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0033_message_variants"
down_revision: str | None = "0032_drop_channel_options"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LOCALE = "'^[A-Za-z0-9_-]{1,8}$'"


def _common() -> list[sa.Column]:
    return [
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column(
            "message_id", sa.UUID(),
            sa.ForeignKey("messages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("locale", sa.String(8), nullable=False),
        sa.Column("job_key", sa.String(200), nullable=False),
        sa.Column("idempotency_key", sa.String(200), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.func.now(), nullable=False,
        ),
    ]


def upgrade() -> None:
    op.create_table(
        "message_variant_requests",
        *_common(),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            f"locale ~ {_LOCALE}",
            name="ck_message_variant_requests_locale",
        ),
        sa.CheckConstraint(
            "btrim(job_key) <> ''",
            name="ck_message_variant_requests_job_key",
        ),
    )
    op.create_index(
        "uq_message_variant_requests_job",
        "message_variant_requests",
        ["message_id", "locale", "job_id"],
        unique=True,
    )
    op.create_index(
        "uq_message_variant_requests_idempotency_key",
        "message_variant_requests",
        ["idempotency_key"],
        unique=True,
    )

    op.create_table(
        "message_variants",
        *_common(),
        sa.Column("source_locale", sa.String(8), nullable=True),
        sa.Column("body", sa.String(5000), nullable=True),
        sa.Column("declined", sa.Boolean(), nullable=False),
        sa.Column("answered_provider", sa.String(200), nullable=True),
        sa.Column("answered_model", sa.String(200), nullable=True),
        sa.CheckConstraint(
            f"locale ~ {_LOCALE}",
            name="ck_message_variants_locale",
        ),
        sa.CheckConstraint(
            f"source_locale IS NULL OR source_locale ~ {_LOCALE}",
            name="ck_message_variants_source_locale",
        ),
        sa.CheckConstraint(
            "btrim(job_key) <> ''",
            name="ck_message_variants_job_key",
        ),
        sa.CheckConstraint(
            "body IS NULL OR btrim(body) <> ''",
            name="ck_message_variants_body",
        ),
        # Exactly one of the three forms.
        sa.CheckConstraint(
            "(NOT declined AND body IS NOT NULL AND source_locale IS NOT NULL"
            " AND source_locale <> locale)"
            " OR (NOT declined AND body IS NULL AND source_locale = locale)"
            " OR (declined AND body IS NULL"
            " AND (source_locale IS NULL OR source_locale <> locale))",
            name="ck_message_variants_form",
        ),
    )
    op.create_index(
        "uq_message_variants_message_locale",
        "message_variants",
        ["message_id", "locale"],
        unique=True,
    )
    op.create_index(
        "uq_message_variants_idempotency_key",
        "message_variants",
        ["idempotency_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_table("message_variants")
    op.drop_table("message_variant_requests")
