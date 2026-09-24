# =============================================================================
# COMMS Service -- Notification Models
# =============================================================================
#
# Canonical merge of the cbshome (base) and velo notification models.
#
# TWO-LEVEL ARCHITECTURE:
#
# Notification:
#   Channel-agnostic event record. One row per event. Contains targeting
#   info (target_type + target_value) that the resolver expands into
#   concrete NotificationDelivery rows.
#
# NotificationDelivery:
#   One row per recipient per channel. Created by the resolver stage of
#   the pipeline. Tracks delivery attempts, status and read state.
#
# PIPELINE:
#   resolve:  Notification -> N NotificationDelivery (by target)
#   deliver:  NotificationDelivery -> ChannelFormatter -> external service
#   rollup:   NotificationDelivery statuses -> Notification.status
#
# IMMUTABILITY (enforced, not just documented):
#   Notification title/body are immutable after the row is persisted.
#   The @validates guard below raises on reassignment once the object
#   has a database identity. Only status is updated by the pipeline.
#
# MERGE NOTES (cbshome = base):
#   - delivery.read_at kept from cbshome (badge/inbox, Sprint 8.3).
#   - type widened to String(50) (velo) -- product type keys such as
#     "waitlist_spot_available" do not fit cbshome's String(30).
#   - delivery.user_id -> recipient_id, FK to the sync-projection
#     recipients table (de-domainization).
#   - created_at-only rows (cbshome); velo's TimestampMixin dropped --
#     the pipeline mutates status, but audit-grade updated_at was never
#     consumed by either donor.
#
# CASCADE:
#   NotificationDelivery.notification_id -> CASCADE delete.
#   NotificationDelivery.recipient_id -> CASCADE delete.
# =============================================================================

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
    func,
    inspect,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, validates

from app.core.constants import (
    FINGERPRINT_LEN,
    MAX_BODY_LEN,
    MAX_CATEGORY_LEN,
    MAX_CORRELATION_LEN,
    MAX_IDEMPOTENCY_KEY_LEN,
    MAX_TITLE_LEN,
    MAX_TYPE_KEY_LEN,
)
from app.core.database import Base
from app.core.mixins import UUIDMixin
from app.engine.constants import DeliveryStatus, NotificationStatus


class Notification(UUIDMixin, Base):
    """Channel-agnostic notification -- one row per event."""

    __tablename__ = "notifications"

    # Domain type key, validated against the profile registry
    # (NOT a hardcoded enum -- see app/profile/registry.py). Width is
    # the shared constant so the profile validator checks against the
    # same number the column declares (Phase 3a item 6).
    type: Mapped[str] = mapped_column(
        String(MAX_TYPE_KEY_LEN),
        nullable=False,
        index=True,
    )

    title: Mapped[str] = mapped_column(
        String(MAX_TITLE_LEN),
        nullable=False,
    )

    body: Mapped[str] = mapped_column(
        String(MAX_BODY_LEN),
        nullable=False,
    )

    target_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    # Bare value: uuid string for USER, group_key for GROUP, "*" for ALL.
    target_value: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
    )

    # The request's idempotency key (F1.2: REQUIRED on every intake
    # path). The plain unique index uq_notifications_idempotency_key
    # (migrations 0005, 0012) makes the database the arbiter of "same
    # key": a second insert fails on flush, and the service then
    # compares FINGERPRINTS to tell a replay from a conflict
    # (app/engine/service.py accept_notification). How long the key
    # keeps answering is bounded by retention -- the KNOWN CEILING on
    # `fingerprint` below says how.
    idempotency_key: Mapped[str] = mapped_column(
        String(MAX_IDEMPOTENCY_KEY_LEN),
        nullable=False,
    )

    # SHA-256 hex digest of the request's BYTES -- what "the same
    # content" means under one key. Compared, never read: equality of
    # bytes is not an interpretation of meaning (spec §5.8). Two ways
    # of computing it exist, one per kind of producer (see
    # stream_fingerprint / canonical_fingerprint in
    # app/engine/service.py for why).
    #
    # KNOWN CEILING -- dedup AND conflict detection are bounded by
    # retention.
    #   1. Mechanics: the key answers only while its notification row
    #      exists. Retention deletes the row after
    #      NOTIFICATION_RETENTION_DAYS, freeing the key: a replay that
    #      arrives after the purge is accepted as NEW -- with the same
    #      bytes it duplicates the job, with different bytes it is not
    #      reported as a conflict. With retention disabled
    #      (NOTIFICATION_RETENTION_DAYS <= 0) the window is infinite.
    #   2. Status: acknowledged by design.
    #   3. Backlog ref: none -- a relationship between two settings
    #      (stream trim horizon, retention), not code work.
    #   4. Promotion trigger (observable): the stream's trim horizon is
    #      configured near NOTIFICATION_RETENTION_DAYS, or a
    #      notification_materialized log line names a key that an
    #      earlier, already purged notification carried.
    #   5. Agreed fix shape: a dedicated processed-keys table (key +
    #      fingerprint) with its OWN retention, decoupled from
    #      notifications.
    #   6. Rejected: that table NOW -- it merely moves the same
    #      retention question to a second table; and "never purge the
    #      key" -- unbounded growth for a replay window nobody uses.
    fingerprint: Mapped[str] = mapped_column(
        String(FINGERPRINT_LEN),
        nullable=False,
    )

    # The channels the profile routed this type to AT INTAKE (F1.2): a
    # snapshot, so a restart with another profile never re-routes a job
    # that was already accepted. The channel is never named by the
    # caller -- only the profile decides.
    channels: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
    )

    # The preference category of the type AT INTAKE (F1.3): a snapshot,
    # like `channels`, so a type removed from the profile after intake
    # is still mute-gated by the category it was accepted with. NULL
    # means the type had no category (never gated).
    category: Mapped[str | None] = mapped_column(
        String(MAX_CATEGORY_LEN),
        nullable=True,
    )

    # The product's own reference from the envelope (F1.2). Stored and
    # handed back untouched; comms never interprets it. Cancellation
    # matches it by EQUALITY -- a comparison, not a reading (F1.3).
    correlation: Mapped[str | None] = mapped_column(
        String(MAX_CORRELATION_LEN),
        nullable=True,
    )

    # Deep-link intent + template variables -- the LETTER, opaque.
    # {"action": "open_thread", "params": {"thread_id": "uuid"}, ...}
    action_data: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )

    # KNOWN CEILING -- every notification carries the same priority.
    #   1. Mechanics: the processor still orders its batch by this
    #      column, but since F1.2 nothing sets it -- the priority left
    #      the request (it is not an envelope field, and comms may not
    #      read the letter), so every row holds the server default and
    #      the ordering degenerates to scheduled_at.
    #   2. Status: acknowledged by design.
    #   3. Backlog ref: phase 4 (isolation lanes), which replaces
    #      priority-in-one-queue with lanes; the inbox field that still
    #      reports it goes with the resource protocol (F1.4).
    #   4. Promotion trigger (observable): a lane consumer is written
    #      (anything reading the profile field `lane`), or the inbox
    #      resource contract is reopened.
    #   5. Agreed fix shape: drop the column, its place in the processor
    #      ordering and index, and the inbox field, together.
    #   6. Rejected: keeping priority on the wire until then -- a field
    #      the processor reads is a decision taken on a request's
    #      content, and a priority inside one queue does not isolate
    #      anything anyway (spec §8.2).
    priority: Mapped[int] = mapped_column(
        Integer,
        default=5,
        server_default="5",
        nullable=False,
    )

    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    expiry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Which layer decided expiry_at (F1.2): the envelope, the profile's
    # expires_after, or the comms default (no expiry). A registry.Layer
    # value -- read back through service.expiry_of().
    expiry_layer: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(20),
        default=NotificationStatus.PENDING,
        server_default=NotificationStatus.PENDING.value,
        nullable=False,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    @validates("title", "body")
    def _forbid_mutation(self, key: str, value: str) -> str:
        """Enforce title/body immutability after creation.

        Reassignment is allowed while the object is still transient or
        pending (construction), and forbidden once it has a database
        identity (persisted / detached).
        """
        state = inspect(self)
        if state.has_identity:
            raise ValueError(
                f"Notification.{key} is immutable after creation"
            )
        return value

    def __repr__(self) -> str:
        return (
            f"<Notification id={self.id} type={self.type} "
            f"status={self.status} target={self.target_type}:{self.target_value}>"
        )


class NotificationDelivery(UUIDMixin, Base):
    """Per-recipient, per-channel delivery record."""

    __tablename__ = "notification_deliveries"

    notification_id: Mapped[UUID] = mapped_column(
        ForeignKey("notifications.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # De-domainization: points at the sync-projection recipient
    # (= product user id), not a product-local users table.
    recipient_id: Mapped[UUID] = mapped_column(
        ForeignKey("recipients.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    channel: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    # Channel-specific options, format varies by channel:
    #   telegram: {button_text, disable_preview, silent}
    #   in_app:   {action_url, dismissable}
    channel_options: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )

    status: Mapped[str] = mapped_column(
        String(20),
        default=DeliveryStatus.PENDING,
        server_default=DeliveryStatus.PENDING.value,
        nullable=False,
        index=True,
    )

    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Read tracking for the in-app inbox/badge (cbshome Sprint 8.3).
    # NULL = unread, non-NULL = read.
    read_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    attempts: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default="0",
        nullable=False,
    )

    # Phase 2.2: how many times this delivery was deferred by a
    # channel rate limit (HTTP 429). A SEPARATE budget from attempts:
    # 429 is not a message failure, so it must not burn the retry
    # budget -- but it must be bounded (a delivery cannot defer
    # forever). Past settings.notification_max_rate_limit_deferrals a
    # 429 degrades to a regular transient failure. A real column (not
    # JSONB) on purpose: "how many deliveries are being throttled
    # right now" must be a WHERE clause, not json archaeology.
    rate_limit_deferrals: Mapped[int] = mapped_column(
        SmallInteger,
        default=0,
        server_default="0",
        nullable=False,
    )

    # Review 1.1: earliest moment the next attempt may run. NULL = no
    # gate: waiting for its turn, or finished. Set by the service on a
    # transient failure (now + base * 2**(attempts-1), capped), by the
    # recipient's schedule (next allowed period), by a 429 (provider's
    # retry_after + jitter) -- each with its wait_reason below.
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # WHY next_retry_at is set (F1.3, spec §5.5): a WaitReason value.
    # Set exactly when next_retry_at is, only on a pending delivery;
    # both are cleared when the delivery is taken into an attempt.
    # Enforced by CHECK ck_deliveries_wait_reason (migration 0013): a
    # reason without a time, or a time without a reason, cannot exist.
    wait_reason: Mapped[str | None] = mapped_column(
        String(30),
        nullable=True,
    )

    # WHY the delivery failed (F1.3, spec §5.6): a FailureClass value.
    # Non-NULL exactly when status is failed -- CHECK
    # ck_deliveries_failure_class (migration 0013): "failed without a
    # class" cannot exist. error_message carries the provider's words;
    # the CLASS is what a program reads, never the text.
    failure_class: Mapped[str | None] = mapped_column(
        String(30),
        nullable=True,
    )

    error_message: Mapped[str | None] = mapped_column(
        String(2000),
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<NotificationDelivery id={self.id} "
            f"channel={self.channel} status={self.status} "
            f"recipient={self.recipient_id}>"
        )


class IntakeOutcome(UUIDMixin, Base):
    """A request that was NOT accepted, recorded under its key (F1.2).

    Two classes, told apart by `outcome`, not by text:
      rejected_at_intake -- the envelope could not be accepted (unknown
                            type, malformed address, expiry already
                            passed, unknown field): the PRODUCT's
                            responsibility, the job was never taken;
      conflict           -- the key is taken by an accepted job whose
                            bytes differ: the product reused a key for
                            new content. The accepted job is untouched
                            and referenced by notification_id.
    A failure AFTER intake is not here: it is the accepted
    notification's own status.

    A rejection does NOT occupy the key: a product that fixes its
    request may resend it under the same key and have it accepted.

    Nothing reads these rows for the product yet -- reading by key is
    phase 2. Until then service.intake_outcomes_for() is the
    programmatic answer. The unique index on (idempotency_key,
    fingerprint, outcome) lives in migration 0012
    (app/core/schema_objects.py): a replay of the same bytes records
    nothing new.
    """

    __tablename__ = "intake_outcomes"

    idempotency_key: Mapped[str] = mapped_column(
        String(MAX_IDEMPOTENCY_KEY_LEN),
        nullable=False,
    )
    fingerprint: Mapped[str] = mapped_column(
        String(FINGERPRINT_LEN),
        nullable=False,
    )
    outcome: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
    )
    # Redacted, human-readable; classification is `outcome`.
    reason: Mapped[str] = mapped_column(
        String(2000),
        nullable=False,
    )
    # The accepted job a conflict collided with; NULL for a rejection.
    # SET NULL, not CASCADE: retention purging the job must not erase
    # the record that a conflict happened.
    notification_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("notifications.id", ondelete="SET NULL"),
        nullable=True,
    )
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
