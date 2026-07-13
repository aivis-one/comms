# =============================================================================
# COMMS Service -- Audience Models (sync projection)
# =============================================================================
#
# The de-domainized replacement for the products' local User tables
# (arch doc §2.3, Model B). The product syncs identity and group
# membership into these tables via `user_upserted` / `group_changed`
# events (service-level receivers: app/audience/sync.py; transport is
# Phase 3); the resolver expands notification targets over them.
# Comms never reads the product database.
#
# Recipient:
#   One row per product user. **id IS the product user id** -- NOT an
#   internal surrogate (`external_id` deliberately does not exist).
#   The id-space is shared with the product, which is why integration
#   tests must draw telegram_ids from an assigned test band.
#
# GroupMembership:
#   (group_key, recipient_id) pairs. The product maps its domain
#   entities into opaque group keys (e.g. practice 42 -> "practice_42",
#   role master -> "role_master") and syncs membership. The core only
#   knows the string key.
#
# CategoryMute (Phase 2):
#   Per-recipient mutes of profile-declared preference categories.
#   Presence of a row = muted. See app/audience/prefs.py.
# =============================================================================

from datetime import datetime, time
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    SmallInteger,
    String,
    Time,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base
from app.core.mixins import TimestampMixin


class Recipient(TimestampMixin, Base):
    """A notification recipient synced from the product.

    id = product user id (shared id-space, no surrogate). Because the
    id comes from the product, there is NO app-side default -- creating
    a Recipient without an explicit id is a bug, not a convenience.
    """

    __tablename__ = "recipients"

    id: Mapped[UUID] = mapped_column(
        primary_key=True,
    )

    telegram_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        index=True,
    )

    email: Mapped[str | None] = mapped_column(
        String(320),
        nullable=True,
    )

    locale: Mapped[str] = mapped_column(
        String(8),
        nullable=False,
        default="en",
        server_default="en",
    )

    active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
        index=True,
    )

    # -- Quiet-hours preferences (Phase 2) --
    # NOT sync fields: the product does not own them (they are comms
    # preferences), so user_upserted must never touch them.

    # IANA timezone for quiet-hours math. Nullable: falls back to
    # settings.default_timezone when the recipient never set one.
    timezone: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )

    # Quiet window: local wall-clock start/end. quiet_from >= quiet_to
    # means the window crosses midnight (e.g. 22:00 -> 08:00).
    quiet_from: Mapped[time | None] = mapped_column(
        Time,
        nullable=True,
    )

    quiet_to: Mapped[time | None] = mapped_column(
        Time,
        nullable=True,
    )

    # ISO weekdays (1=Mon .. 7=Sun) on which the window STARTS. An
    # overnight window starting Friday 22:00 belongs to day 5 even
    # though it ends on Saturday. Stored sorted and de-duplicated.
    quiet_days: Mapped[list[int] | None] = mapped_column(
        ARRAY(SmallInteger),
        nullable=True,
    )

    def __repr__(self) -> str:
        return (
            f"<Recipient id={self.id} tg={self.telegram_id} "
            f"locale={self.locale} active={self.active}>"
        )


class GroupMembership(Base):
    """Membership of a recipient in a product-defined group.

    group_key is opaque to the core -- the product owns the mapping
    from its domain (roles, practices, ...) to group keys.
    """

    __tablename__ = "group_memberships"

    group_key: Mapped[str] = mapped_column(
        String(100),
        primary_key=True,
    )

    recipient_id: Mapped[UUID] = mapped_column(
        ForeignKey("recipients.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    def __repr__(self) -> str:
        return (
            f"<GroupMembership group={self.group_key} "
            f"recipient={self.recipient_id}>"
        )


class CategoryMute(Base):
    """A recipient's mute of one preference category (Phase 2).

    Row present = muted (default is "everything on", so only mutes are
    stored). Categories are profile vocabulary -- validated against
    registry.registered_categories() at write time, not by a DB enum,
    so the profile can grow without migrations.

    PK order (recipient_id, category) matches the resolver's actual
    probe: mute gating arrives with a concrete recipient list and asks
    "which of THESE muted category X" -- point lookups by recipient.
    """

    __tablename__ = "category_mutes"

    recipient_id: Mapped[UUID] = mapped_column(
        ForeignKey("recipients.id", ondelete="CASCADE"),
        primary_key=True,
    )

    category: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<CategoryMute recipient={self.recipient_id} "
            f"category={self.category}>"
        )
