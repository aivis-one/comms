# =============================================================================
# COMMS Service -- Audience Models (sync projection)
# =============================================================================
#
# The de-domainized replacement for the products' local User tables
# (arch doc §2.3, Model B). The product syncs identity and group
# membership into these tables via `user_upserted` / `group_changed`
# events (receivers land in Phase 2); the resolver expands notification
# targets over them. Comms never reads the product database.
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
# =============================================================================

from uuid import UUID

from sqlalchemy import BigInteger, Boolean, ForeignKey, String
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
