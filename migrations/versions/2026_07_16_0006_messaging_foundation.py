"""messaging foundation -- threads / messages / read state / sections

Revision ID: 0006_messaging_foundation
Revises: 0005_event_ingest_dedup
Create Date: 2026-07-16 00:00:00.000000

Phase 4a (arch doc §2.4): the messaging DATA layer -- four tables plus
the invariants that make thread identity stable. Every messaging
constraint and index lives HERE (not in the models' __table_args__),
following the repo practice for notifications.idempotency_key (0005):
the schema invariants are reviewable in one place.

DEDUP = the thread_id invariant, applied ONLY at creation. Two partial
unique indexes express it:
  - uq_threads_dedup_subject -- one thread per (client, operator,
    subject_ref) when a subject is present. KIND-AGNOSTIC on purpose:
    a subject IS the identity, so a dm and a ticket cannot both exist
    for the same entity, and kind is not in the key.
  - uq_threads_dedup_dm -- one eternal dm per (client, operator) when
    there is no subject.
  - a subjectless ticket matches NEITHER index -> a fresh thread every
    time, achieved by the ABSENCE of a constraint.
The application is the create-or-get caller; the database is the race
arbiter (app/messaging/threads.py catches the IntegrityError by index
name, mirroring the 3c dedup in app/transport/handlers.py).

CHECK ck_threads_subject_ref_both_or_neither -- a half subject_ref
(one column set, the other NULL) is forbidden at the DB level.

ondelete on the THREE recipient-referencing FKs -- threads.client,
messages.sender, thread_read_states.participant -- is RESTRICT, NOT the
repo's usual CASCADE. This is a deliberate deviation, called out here
so a later reviewer does not "fix" it back: a thread is immortal by
thread_id (arch doc §2.4), so deleting a recipient a conversation
references must be a deliberate product decision (anonymize vs delete,
a future feature), never a silent cascade that shreds history. CASCADE
stays correct for notification deliveries (they SHOULD die with their
parent notification) -- threads are the other case. The thread-owned
children (messages, read states) DO cascade on thread deletion; a
thread is never deleted today, so that is latent, but it is the
correct parent/child rule if one ever is.

operator_value carries NO foreign key: its target is polymorphic (a
recipient for the user form, a section for the section form), so a
single column cannot hold a hard FK. Its referent is verified in the
service on creation, both forms (app/messaging/threads.py) -- the
compensation for the missing DB-level FK.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_messaging_foundation"
down_revision: str | None = "0005_event_ingest_dedup"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the messaging tables, dedup indexes and invariants."""

    # -- sections (first-class, minimal: id / key / label) --
    op.create_table(
        "sections",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("key", sa.String(100), nullable=False),
        sa.Column("label", sa.String(200), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # UNIQUE(key): lookup + the get-or-create race arbiter. Migration-
    # owned (like notifications.idempotency_key) so every messaging
    # invariant is in one reviewable place.
    op.create_index(
        "uq_sections_key",
        "sections",
        ["key"],
        unique=True,
    )

    # -- threads --
    op.create_table(
        "threads",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("client", sa.UUID(), nullable=False),
        sa.Column("operator_kind", sa.String(20), nullable=False),
        # operator_value: polymorphic (recipient id OR section id) ->
        # NO foreign key; non-null keeps the dedup indexes clean.
        sa.Column("operator_value", sa.UUID(), nullable=False),
        # Assignment axis -- FIELDS only (claim is Phase 4b). assignee
        # is intentionally not an FK (operator axis, 4b's to wire).
        sa.Column("assignee", sa.UUID(), nullable=True),
        sa.Column("assigned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("kind", sa.String(20), nullable=False),
        sa.Column("subject_type", sa.String(100), nullable=True),
        sa.Column("subject_id", sa.String(200), nullable=True),
        sa.Column("title", sa.String(500), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=True),
        sa.Column(
            "status",
            sa.String(20),
            server_default="open",
            nullable=False,
        ),
        # Activity marker for the 4b auto-close pass; kept separate
        # from updated_at. No supporting index in 4a (see the NOTE
        # below).
        sa.Column("last_message_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        # client -> recipients: RESTRICT (history-bearing; see header).
        sa.ForeignKeyConstraint(
            ["client"], ["recipients.id"], ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        # A subject_ref is both columns or neither.
        sa.CheckConstraint(
            "(subject_type IS NULL) = (subject_id IS NULL)",
            name="ck_threads_subject_ref_both_or_neither",
        ),
    )
    op.create_index("ix_threads_client", "threads", ["client"])
    # Dedup partial unique indexes = the thread_id invariant.
    op.create_index(
        "uq_threads_dedup_subject",
        "threads",
        [
            "client",
            "operator_kind",
            "operator_value",
            "subject_type",
            "subject_id",
        ],
        unique=True,
        postgresql_where=sa.text("subject_type IS NOT NULL"),
    )
    op.create_index(
        "uq_threads_dedup_dm",
        "threads",
        ["client", "operator_kind", "operator_value"],
        unique=True,
        postgresql_where=sa.text("kind = 'dm' AND subject_type IS NULL"),
    )
    # NOTE (Phase 4b socket / KNOWN CEILING candidate): no index on
    # `status` or `last_message_at` in 4a. The auto-close pass ("silent
    # N days -> closed") will scan last_message_at, but its supporting
    # index is deferred exactly like the retention index (BL-3); 4b
    # adds it and marks the KNOWN CEILING. Adding it now would be
    # speculative -- nothing in 4a queries those columns.

    # -- messages --
    op.create_table(
        "messages",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("thread_id", sa.UUID(), nullable=False),
        sa.Column("sender", sa.UUID(), nullable=False),
        sa.Column("body", sa.String(5000), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        # thread_id -> threads: CASCADE (thread-owned child).
        sa.ForeignKeyConstraint(
            ["thread_id"], ["threads.id"], ondelete="CASCADE",
        ),
        # sender -> recipients: RESTRICT (history-bearing; see header).
        sa.ForeignKeyConstraint(
            ["sender"], ["recipients.id"], ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_messages_sender", "messages", ["sender"])
    # Serves BOTH the ordered read (created_at within a thread) and the
    # unread count (created_at > last_read_at) -- the two message reads
    # 4a actually performs. thread_id leads, so it also covers plain
    # by-thread lookups (no separate thread_id index needed).
    op.create_index(
        "ix_messages_thread_created",
        "messages",
        ["thread_id", "created_at"],
    )

    # -- thread_read_states (composite PK, like category_mutes) --
    op.create_table(
        "thread_read_states",
        sa.Column("thread_id", sa.UUID(), nullable=False),
        sa.Column("participant", sa.UUID(), nullable=False),
        sa.Column("last_read_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        # thread_id -> threads: CASCADE (thread-owned child).
        sa.ForeignKeyConstraint(
            ["thread_id"], ["threads.id"], ondelete="CASCADE",
        ),
        # participant -> recipients: RESTRICT (history-bearing; header).
        sa.ForeignKeyConstraint(
            ["participant"], ["recipients.id"], ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("thread_id", "participant"),
    )
    op.create_index(
        "ix_thread_read_states_participant",
        "thread_read_states",
        ["participant"],
    )


def downgrade() -> None:
    """Drop the messaging tables (child-first; each drop takes its
    own indexes and constraints with it)."""
    op.drop_table("thread_read_states")
    op.drop_table("messages")
    op.drop_table("threads")
    op.drop_table("sections")
