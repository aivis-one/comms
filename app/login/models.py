# =============================================================================
# COMMS Service -- the model of a sign-in attempt (L-2)
# =============================================================================
#
# The columns, the primary key and the index of the purge; the CHECKs are
# the migration's (migrations/versions/2026_11_24_0042_login_attempts.py,
# app/core/schema_objects.py MIGRATION_OWNED_CHECKS), as everywhere in
# comms. The writers of a row: the start (insert), the take (taken_at
# set, the verifier erased), the purge (delete) -- app/login/attempts.py.
# =============================================================================

from datetime import datetime

from sqlalchemy import Boolean, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.core.constants import FINGERPRINT_LEN
from app.core.database import Base


class LoginAttempt(Base):
    """One start of a sign-in, until it is taken and for 24 hours.

    No attribute is a secret in the repr: SQLAlchemy's default repr names
    the class and the object, not a column.
    """

    __tablename__ = "login_attempts"

    # SHA-256 hex of the attempt's `state`; the state itself is not kept.
    state_hash: Mapped[str] = mapped_column(
        String(FINGERPRINT_LEN), primary_key=True,
    )
    provider: Mapped[str] = mapped_column(String(16), nullable=False)
    # The product's own return address, used by the code exchange as it
    # came (REDIRECT_URI_MAX of app/login/attempts.py).
    redirect_uri: Mapped[str] = mapped_column(String(2048), nullable=False)
    # The PKCE verifier; NULL from the moment the attempt is taken.
    code_verifier: Mapped[str | None] = mapped_column(String(128), nullable=True)
    bot_access: Mapped[bool] = mapped_column(Boolean, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True,
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
    )
    taken_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
