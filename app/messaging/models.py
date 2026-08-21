# =============================================================================
# COMMS Service -- Messaging Models (Phase 4a)
# =============================================================================
#
# The messaging data layer (arch doc §2.4). Product-neutral: the
# "master" is just a recipient, a "practice" is an opaque subject_ref.
#
# SCHEMA INVARIANTS LIVE IN THE MIGRATION. Following the repo practice
# (notifications.idempotency_key, migration 0005), every messaging
# constraint and index -- the two dedup partial unique indexes, the
# subject_ref both-or-neither CHECK, the section key uniqueness, the
# read/unread composite index -- is declared in migration 0006, not in
# __table_args__. The models declare columns and simple single-column
# FK indexes only; the migration is the one place to review the
# invariants.
#
# WHY operator_target IS A POLYMORPHIC PAIR (not two FK columns):
#   operator_value is a recipient id for the USER form and a section id
#   for the SECTION form -- both UUIDs, but two different tables, so a
#   single column cannot carry a hard FK. It is kept NON-NULL so the
#   dedup indexes stay clean (two NULLs never collide in Postgres, so a
#   nullable operator would quietly break "one eternal dm"). Its
#   referent is verified in the service on creation, both forms
#   (threads.create_or_get_thread) -- the compensation for the missing
#   DB-level FK.
#
# RECIPIENT REFERENCES: client / sender / participant are real FKs to
# the audience-owned recipients table (referenced BY NAME -- messaging
# does not import app.audience). ondelete is RESTRICT, not the repo's
# usual CASCADE: a thread is immortal by thread_id, so a recipient that
# a conversation references must not be shredded by a silent cascade
# (see migration 0006 for the full rationale).
# =============================================================================

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base
from app.core.mixins import TimestampMixin, UUIDMixin
from app.messaging.constants import (
    MAX_MESSAGE_BODY_LEN,
    MAX_SECTION_KEY_LEN,
    MAX_SECTION_LABEL_LEN,
    MAX_SUBJECT_ID_LEN,
    MAX_SUBJECT_TYPE_LEN,
    MAX_THREAD_TITLE_LEN,
    ThreadStatus,
)


class Section(UUIDMixin, Base):
    """A first-class support section (arch doc §2.4, v1 minimal).

    Only id / key / label. The operator<->section MEMBERSHIP is
    trivial in v1 (any agent serves every section) and belongs to
    Phase 4b -- this table exists so that a thread's
    operator_target=section:<id> can point at a real row.
    """

    __tablename__ = "sections"

    # Opaque product-facing key ("support", "billing", ...). UNIQUE is
    # enforced by uq_sections_key (migration 0006) -- kept out of the
    # model for the same reason the notifications idempotency index
    # lives only in the migration.
    key: Mapped[str] = mapped_column(
        String(MAX_SECTION_KEY_LEN),
        nullable=False,
    )

    label: Mapped[str] = mapped_column(
        String(MAX_SECTION_LABEL_LEN),
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<Section id={self.id} key={self.key!r}>"


class Thread(UUIDMixin, TimestampMixin, Base):
    """A conversation with a stable identity (arch doc §2.4).

    identity = thread_id: the dedup key is applied ONLY at creation
    (the two partial unique indexes in migration 0006), so a future
    retag (4b) preserves the thread and its history. `assignee`,
    `assigned_at`, `status` and `operator_target` are FIELDS here;
    their BEHAVIOR (claim, visibility, transitions) is Phase 4b.
    """

    __tablename__ = "threads"

    # The recipient this thread is with (VELO: the client user).
    client: Mapped[UUID] = mapped_column(
        ForeignKey("recipients.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    # operator_target = (operator_kind, operator_value). Polymorphic:
    # operator_value is a recipients.id (USER) or a sections.id
    # (SECTION). No hard FK (two target tables); non-null keeps the
    # dedup indexes clean. See module header.
    operator_kind: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )
    operator_value: Mapped[UUID] = mapped_column(
        nullable=False,
    )

    # -- Assignment axis. Phase 4b wires the operator axis it inherited
    #    "soft" from 4a. assignee ALWAYS points into recipients (one
    #    table) -- for a section thread it is the claiming agent, for a
    #    user thread it is pre-assigned to operator_value (the master) at
    #    creation -- so a hard FK is clean here, unlike the polymorphic
    #    operator_value (which has none). ondelete=RESTRICT for the same
    #    reason as client/sender/participant (see migration 0006): a
    #    recipient a thread references is not silently shredded.
    #    INVARIANT: assignee is set  <=>  assigned_at is set. --
    assignee: Mapped[UUID | None] = mapped_column(
        ForeignKey("recipients.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    assigned_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Dedup discriminator for the SUBJECTLESS case (arch doc §2.4).
    kind: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    # subject_ref = (subject_type, subject_id), OPAQUE to comms (never
    # parsed). Both-or-neither is enforced by a CHECK in migration
    # 0006.
    subject_type: Mapped[str | None] = mapped_column(
        String(MAX_SUBJECT_TYPE_LEN),
        nullable=True,
    )
    subject_id: Mapped[str | None] = mapped_column(
        String(MAX_SUBJECT_ID_LEN),
        nullable=True,
    )

    title: Mapped[str | None] = mapped_column(
        String(MAX_THREAD_TITLE_LEN),
        nullable=True,
    )
    priority: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )

    # Lifecycle status. Values LOCKED to open/resolved/closed (§2.4);
    # transitions are Phase 4b. Varchar + StrEnum value, no CHECK
    # (append-only), same shape as Notification.status.
    status: Mapped[str] = mapped_column(
        String(20),
        default=ThreadStatus.OPEN,
        server_default=ThreadStatus.OPEN.value,
        nullable=False,
    )

    # Activity marker = time of the last message. This is the socket
    # the 4b auto-close pass ("silent N days -> closed") scans; kept
    # SEPARATE from updated_at so a 4b claim/retag does not look like
    # thread activity. Its supporting index is deferred to 4b (like the
    # retention index, BL-3) -- see migration 0006.
    last_message_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Phase 4b -> 4c handoff socket. Set to the close time when a
    # SECTION thread reaches `closed` (auto-close OR manual operator
    # close -- "loud" is a property of the FORM, not the trigger);
    # NEVER set for user/DM threads ("quiet"). 4b only MARKS here; 4c
    # reads pending rows, sends the "conversation closed" notification,
    # and clears the mark. Cleared on client auto-reopen (the close was
    # voided before 4c sent). Unindexed in 4b -- 4c owns that read.
    close_notify_pending_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    def __repr__(self) -> str:
        return (
            f"<Thread id={self.id} kind={self.kind} "
            f"status={self.status} client={self.client}>"
        )


class Message(UUIDMixin, Base):
    """A single message in a thread (arch doc §2.4).

    Immutable once written (append-only history) -- created_at only, no
    updated_at, mirroring the notification rows. The "message sent"
    notification is NOT emitted from the data layer by design: that
    path lives in app/notifier.py (notify_new_message).
    """

    __tablename__ = "messages"

    thread_id: Mapped[UUID] = mapped_column(
        ForeignKey("threads.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Author of the message. RESTRICT (history-bearing) -- see the
    # module header and migration 0006.
    sender: Mapped[UUID] = mapped_column(
        ForeignKey("recipients.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    body: Mapped[str] = mapped_column(
        String(MAX_MESSAGE_BODY_LEN),
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    def __repr__(self) -> str:
        return (
            f"<Message id={self.id} thread={self.thread_id} "
            f"sender={self.sender}>"
        )


class ThreadReadState(Base):
    """Per-participant read pointer for a thread (arch doc §2.4).

    (thread_id, participant) -> last_read_at. Composite PK, no
    surrogate id -- same shape as CategoryMute. "Unread in a thread" is
    DERIVED (read_state.count_unread), never stored. The upsert is
    race-safe and monotonic (read_state.mark_read).
    """

    __tablename__ = "thread_read_states"

    thread_id: Mapped[UUID] = mapped_column(
        ForeignKey("threads.id", ondelete="CASCADE"),
        primary_key=True,
    )

    # The reader (a recipient: the client or an operator). RESTRICT --
    # see the module header and migration 0006.
    participant: Mapped[UUID] = mapped_column(
        ForeignKey("recipients.id", ondelete="RESTRICT"),
        primary_key=True,
        index=True,
    )

    # Everything up to (and including) this instant is read.
    last_read_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    def __repr__(self) -> str:
        return (
            f"<ThreadReadState thread={self.thread_id} "
            f"participant={self.participant} last_read_at={self.last_read_at}>"
        )


class SectionMember(Base):
    """An operator declared as serving a section (T-67).

    THE SHAPE IS GroupMembership's, deliberately: a composite primary
    key over the pair, no surrogate id, no timestamps, an index on the
    second column so the reverse lookup ("which sections does this
    operator serve") is answerable. comms already has one membership
    table and this is the second one of the same kind, not a second
    convention.

    WHY NOT group_memberships ITSELF, since it exists: a group key is
    an opaque product string for ADDRESSING a broadcast, while this pair
    answers "who serves this section" and is read three times inside
    messaging (visibility, operate-authz, the pool push). Riding on the
    string table would mean a cross-repository key convention with no
    foreign key, where a renamed section key orphans its roster in
    silence. Here the section is referenced, not spelled.

    ondelete DIVERGES BETWEEN THE TWO COLUMNS, on purpose:
      - section CASCADE: a deleted section has no roster to keep;
      - operator CASCADE: a deleted recipient serves nothing.
    Both differ from the RESTRICT that guards thread references, and the
    reason is that a thread is IMMORTAL and must never lose the identity
    it names, whereas a membership row is a current fact with no
    history to protect.

    EMPTY IS NOT UNCONFIGURED. A section with no rows here is served by
    ANY operator -- that is the definition, not a migration window (see
    messaging/membership.py).
    """

    __tablename__ = "section_members"

    section_id: Mapped[UUID] = mapped_column(
        ForeignKey("sections.id", ondelete="CASCADE"),
        primary_key=True,
    )

    # The serving operator, a recipient (referenced BY NAME -- messaging
    # does not import app.audience).
    operator_id: Mapped[UUID] = mapped_column(
        ForeignKey("recipients.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    def __repr__(self) -> str:
        return (
            f"<SectionMember section={self.section_id} "
            f"operator={self.operator_id}>"
        )
