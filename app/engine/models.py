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
    MAX_BODY_LEN,
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

    # Producer-supplied dedup key for stream-ingested notification
    # requests (Phase 3c item 2): at-least-once delivery replays are
    # collapsed by the partial unique index on this column (migration
    # 0005); NULL for notifications created by other paths (tests,
    # future internal producers) -- NULLs never collide.
    #
    # KNOWN CEILING -- dedup window is bounded by retention.
    # Mechanics: dedup is reliable only while the stream's trim
    # horizon < NOTIFICATION_RETENTION_DAYS -- retention deletes the
    # notification row, freeing its key, so a replay arriving AFTER
    # the row is purged creates a duplicate. With retention disabled
    # (NOTIFICATION_RETENTION_DAYS <= 0, 3a.1) the window is infinite
    # (safe; table growth is retention's concern, not dedup's).
    # Status: acknowledged by design (Phase 3c review, part C).
    # Backlog: covered by the marker itself, no BL entry -- config
    # relationship, not code work.
    # Unfreeze trigger: stream trim horizon configured anywhere near
    # NOTIFICATION_RETENTION_DAYS, or replay-after-purge observed.
    # Agreed fix shape: none needed while the inequality holds; if it
    # ever breaks, a dedicated processed-keys table with its OWN
    # retention decoupled from notifications.
    # Rejected: a separate processed_events table NOW -- it merely
    # moves the same retention question to a second table.
    idempotency_key: Mapped[str | None] = mapped_column(
        String(MAX_IDEMPOTENCY_KEY_LEN),
        nullable=True,
        default=None,
    )

    # Deep-link intent + template variables + internal keys ("_channels").
    # {"action": "open_practice", "params": {"practice_id": "uuid"}, ...}
    action_data: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )

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

    # Review 1.1: earliest moment the next transient retry may run.
    # NULL = no gate (fresh delivery or terminal state). Set by the
    # service on transient failure: now + base * 2**(attempts-1),
    # capped; by quiet hours (window end); by a 429 (server-named
    # retry_after + jitter).
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
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
