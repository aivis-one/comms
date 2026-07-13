# =============================================================================
# COMMS Service -- ORM Mixins
# =============================================================================
#
# Ported from the cbshome backend (canonical base), trimmed to what the
# comms models use (JSONBMixin left behind -- the engine reassigns whole
# dicts, which SQLAlchemy tracks natively).
#
# WHY MIXINS (not a custom Base)?
#   Alembic reads Base.metadata for autogenerate. Subclassing Base
#   would make mixin tables appear in metadata. Mixins avoid this.
# =============================================================================

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import UUID as SA_UUID
from sqlalchemy import DateTime, func
from sqlalchemy.orm import Mapped, mapped_column


class UUIDMixin:
    """Primary key mixin -- UUID v4, app-side generated.

    App-side uuid4() ensures the ID is available immediately after
    object creation, before flush/commit. No round-trip needed.
    """

    id: Mapped[UUID] = mapped_column(
        SA_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )


class TimestampMixin:
    """Timestamp mixin -- created_at and updated_at.

    created_at: set once by the DB on INSERT (server_default).
    updated_at: set by SQLAlchemy ORM on every UPDATE (onupdate).

    NOTE: raw SQL bypasses onupdate. Always use ORM for mutations.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
    )
