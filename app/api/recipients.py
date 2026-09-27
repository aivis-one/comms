# =============================================================================
# COMMS Service -- Recipient sync API (synchronous upsert)
# =============================================================================
#
# WHY THIS ROUTE EXISTS. Recipients are addressed by the product's own
# user id: a message to a recipient comms has never heard of resolves
# to NOTHING. The path is worth naming precisely, because a
# post-mortem looks in the wrong place otherwise:
# engine/resolver.py::_resolve_user selects the recipient by id and
# active, an unknown id yields an empty list, and
# engine/service.py::resolve_notification then marks the notification
# NO_RECIPIENTS with `notification_no_targets` -- terminal, with NO delivery
# row created at all and no retry anywhere. A message that overtakes
# the asynchronous identity sync is therefore lost silently and for
# good. (Contrast group_changed, whose unknown recipient raises
# NotFoundError and is classified retryable -- transport/handlers.py.)
#
# The event stream cannot close that window on its own: it is
# asynchronous by construction, its ordering guarantee is per-pass
# rather than absolute, and more than one consumer may share the
# group. So the product gets a SYNCHRONOUS door: create the recipient,
# wait for the answer, then send.
#
# WHAT THIS ROUTE IS NOT. It is not a second addressing model -- comms
# still delivers only to known recipients, and this route is how they
# become known. It carries no product vocabulary: the body is the same
# six identity fields the wire contract already defines, and the
# service function underneath is the same one the Redis consumer
# calls (audience/sync.py::user_upserted, called AS-IS).
#
# SNAPSHOT DISCIPLINE (inherited from that contract, not invented
# here): all six fields are REQUIRED in the body. A null is a value --
# `timezone: null` overwrites a previously synced zone with NULL --
# while an ABSENT key is a 422, because "absent" would otherwise be
# silently read as "keep what you had", which is a partial patch
# wearing a snapshot's clothes. extra="forbid" makes a typo a 422 for
# the same reason.
#
# IDEMPOTENCY: a repeat call with identical data is a no-op update, so
# the product may retry freely, and the same event arriving later
# through the stream changes nothing. The comms-owned preference
# fields (quiet_from / quiet_to / quiet_days) are never touched by the
# upsert -- see the ownership boundary in audience/sync.py.
# =============================================================================

from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_service_auth
from app.audience import sync
from app.audience.models import Recipient
from app.core.constants import (
    MAX_EMAIL_LEN,
    MAX_LOCALE_LEN,
    MAX_SNAPSHOT_VERSION,
    MAX_TELEGRAM_ID,
    MAX_TIMEZONE_LEN,
    MIN_TELEGRAM_ID,
)
from app.core.database import get_db_session
from app.forgetting import forget_recipient

logger = structlog.get_logger()

router = APIRouter(
    prefix="/api/v1/recipients",
    tags=["recipients"],
    dependencies=[Depends(require_service_auth)],
)


class RecipientSnapshot(BaseModel):
    """PUT body: the product's VERSIONED snapshot of one recipient.

    Every field is required -- see the module header on why an absent
    key must not degrade into a default. extra="forbid" turns a
    misspelled field into a 422 instead of a silently ignored one.

    EVERY BOUND IS THE COLUMN'S OWN CONSTANT (R-2 item 1). The VALUE
    rules -- a blank string or a telegram id of 0 is not a way to say
    "no value", null is -- live in ONE place for this route and the
    user_upserted event: audience/sync.py apply_snapshot (F1.4; before,
    this model accepted an empty locale that the event refused).
    """

    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1, le=MAX_SNAPSHOT_VERSION, strict=True)
    telegram_id: int | None = Field(
        ge=MIN_TELEGRAM_ID, le=MAX_TELEGRAM_ID,
    )
    email: str | None = Field(max_length=MAX_EMAIL_LEN)
    locale: str | None = Field(max_length=MAX_LOCALE_LEN)
    timezone: str | None = Field(max_length=MAX_TIMEZONE_LEN)
    active: bool


class RecipientDeletion(BaseModel):
    """DELETE body: the version of the deletion, ordered against the
    snapshots by the same rule (audience/sync.py)."""

    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1, le=MAX_SNAPSHOT_VERSION, strict=True)


def _wire(recipient: Recipient) -> dict[str, Any]:
    """The stored snapshot as comms holds it.

    Returned rather than a bare 204 so the caller can verify what
    landed -- and deliberately WITHOUT a created/updated flag: the
    whole point of an idempotent upsert is that the caller does not
    have to care which one happened.
    """
    return {
        "recipient_id": str(recipient.id),
        "version": recipient.version,
        "telegram_id": recipient.telegram_id,
        "email": recipient.email,
        "locale": recipient.locale,
        "timezone": recipient.timezone,
        "active": recipient.active,
        "deleted": recipient.deleted_at is not None,
    }


@router.put("/{recipient_id}")
async def upsert_recipient(
    recipient_id: UUID,
    snapshot: RecipientSnapshot = Body(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Create or update one recipient, synchronously, by version.

    The product calls this before its first message to a new user, so
    that the message has somewhere to land. A snapshot older than the
    stored one is a 409 `stale_snapshot`; the stored version with other
    content a 409 `conflict`; a deleted recipient a 409
    `recipient_deleted` (app/api/errors.py).
    """

    async def _apply() -> Recipient:
        """The one mapping from body to the snapshot rule."""
        return await sync.apply_snapshot(
            session,
            recipient_id=recipient_id,
            version=snapshot.version,
            telegram_id=snapshot.telegram_id,
            email=snapshot.email,
            locale=snapshot.locale,
            timezone=snapshot.timezone,
            active=snapshot.active,
        )

    try:
        # SAVEPOINT, not bare call: two writers racing on a brand-new id
        # (this route and the stream consumer, or two product replicas)
        # can both see nothing and both insert. The loser gets an
        # IntegrityError on the primary key; the savepoint keeps the
        # request's transaction usable.
        async with session.begin_nested():
            recipient = await _apply()
    except IntegrityError:
        # The winner has committed by now. Calling the SAME rule again
        # finds its row and compares versions against it. Retried once:
        # a second collision would mean the row both exists and does
        # not.
        logger.info(
            "recipient_upsert_raced",
            recipient_id=str(recipient_id),
        )
        recipient = await _apply()

    return _wire(recipient)


@router.delete("/{recipient_id}")
async def delete_recipient(
    recipient_id: UUID,
    deletion: RecipientDeletion = Body(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Forget one recipient (app/forgetting.py). A repeat is not an
    error; an id comms never heard of becomes a tombstone too, so a
    snapshot still on its way cannot create the person afterwards."""
    await forget_recipient(
        session, recipient_id=recipient_id, version=deletion.version,
    )
    recipient = await session.get(Recipient, recipient_id)
    assert recipient is not None
    return _wire(recipient)
