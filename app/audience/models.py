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

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.core.constants import (
    FINGERPRINT_LEN,
    MAX_CATEGORY_LEN,
    MAX_EMAIL_LEN,
    MAX_GROUP_KEY_LEN,
    MAX_LOCALE_LEN,
    MAX_TIMEZONE_LEN,
)
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

    # -- The snapshot's order and identity (F1.4, spec §10.2) --
    # The product's monotonic version of this snapshot. A snapshot older
    # than the stored one is refused on BOTH write paths by ONE rule
    # (audience/sync.py apply_snapshot), so the order events arrive in
    # stops mattering. 0 = written before versions existed (migration
    # 0014); the product's first versioned snapshot (>= 1) supersedes it.
    version: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )
    # Digest of the stored snapshot's fields: an equal version with
    # other bytes is a conflict, not a replay.
    snapshot_fingerprint: Mapped[str] = mapped_column(
        String(FINGERPRINT_LEN),
        nullable=False,
    )
    # Set when the product deleted the recipient (F1.4, spec §10.4). The
    # row stays as a TOMBSTONE -- threads and messages reference it with
    # RESTRICT, and the delivery history must not cascade away -- but
    # every field that reaches the person is NULL and active is false,
    # which CHECK ck_recipients_tombstone holds in the database. A
    # tombstone is terminal: no snapshot revives it.
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    telegram_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        index=True,
    )

    email: Mapped[str | None] = mapped_column(
        String(MAX_EMAIL_LEN),
        nullable=True,
    )

    # NULL = no language (F1.4): the product said so explicitly, and the
    # renderer falls back to the deploy default. An empty string is not
    # a second way to say it -- CHECK ck_recipients_locale_not_blank.
    locale: Mapped[str | None] = mapped_column(
        String(MAX_LOCALE_LEN),
        nullable=True,
    )

    active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
        index=True,
    )

    # -- Timezone (sync field, Phase 2.1) --
    # IANA timezone for quiet-hours math. Product-owned identity
    # (e.g. VELO users.timezone) synced via user_upserted alongside
    # locale. Nullable: falls back to settings.default_timezone when
    # the product does not track it.
    timezone: Mapped[str | None] = mapped_column(
        String(MAX_TIMEZONE_LEN),
        nullable=True,
    )

    # -- Delivery schedule (R-5) --
    # Comms-owned: the recipient sets it through the prefs API, the
    # product does not know it, so user_upserted must never touch it.
    #
    # THE PERIODS DURING WHICH DELIVERY IS ALLOWED -- not the periods
    # of silence. The polarity matters: the old three columns held one
    # quiet window whose day set meant "days the window STARTS on",
    # and a product whose screen says "when you may reach me" had to
    # invert times, after which the start day landed on the evening
    # before the morning it covered. Every period here belongs to the
    # day it falls in, and none crosses midnight -- a night allowance
    # is written as two periods, one per day -- so the start-day
    # notion does not exist to be got wrong.
    #
    # SHAPE: a list of {"day": 1..7 (ISO), "from": minutes, "to":
    # minutes}, minutes counted from local midnight, sorted by (day,
    # from), never overlapping or touching within a day. `to` may be
    # 1440 (exactly midnight) so that a whole allowed day is exact;
    # the old model's closest form, 00:00 -> 23:59, left a one-minute
    # hole that delivered.
    #
    # NULL means NO RESTRICTION -- deliver at any time. An EMPTY LIST
    # is rejected at write time: it would mean "never", which is not a
    # schedule but a black hole (the deliveries defer until they
    # expire). Muting exists for "do not send me this".
    #
    # JSONB and not a table: the delivery gate already holds the
    # Recipient row (engine/service.deliver_notification), and a table
    # would add a query per delivery. The write path
    # (audience/prefs.set_schedule) is the only door, so the shape is
    # validated there.
    allowed_windows: Mapped[list[dict[str, int]] | None] = mapped_column(
        JSONB,
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
        String(MAX_GROUP_KEY_LEN),
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

    # Width is the shared constant so the profile validator checks
    # categories against the same number the column declares
    # (Phase 3a item 6).
    category: Mapped[str] = mapped_column(
        String(MAX_CATEGORY_LEN),
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
