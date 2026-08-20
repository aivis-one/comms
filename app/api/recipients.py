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
# SKIPPED with `notification_no_targets` -- terminal, with NO delivery
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
from pydantic import BaseModel, ConfigDict
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_service_auth
from app.audience import sync
from app.audience.models import Recipient
from app.core.database import get_db_session

logger = structlog.get_logger()

router = APIRouter(
    prefix="/api/v1/recipients",
    tags=["recipients"],
    dependencies=[Depends(require_service_auth)],
)


class RecipientSnapshot(BaseModel):
    """PUT body: the five sync-owned fields carried alongside the id.

    Every field is required -- see the module header on why an absent
    key must not degrade into a default. extra="forbid" turns a
    misspelled field into a 422 instead of a silently ignored one.
    """

    model_config = ConfigDict(extra="forbid")

    telegram_id: int | None
    email: str | None
    locale: str
    timezone: str | None
    active: bool


def _wire(recipient: Recipient) -> dict[str, Any]:
    """The stored snapshot as comms holds it.

    Returned rather than a bare 204 so the caller can verify what
    landed -- and deliberately WITHOUT a created/updated flag: the
    whole point of an idempotent upsert is that the caller does not
    have to care which one happened.
    """
    return {
        "recipient_id": str(recipient.id),
        "telegram_id": recipient.telegram_id,
        "email": recipient.email,
        "locale": recipient.locale,
        "timezone": recipient.timezone,
        "active": recipient.active,
    }


@router.put("/{recipient_id}")
async def upsert_recipient(
    recipient_id: UUID,
    snapshot: RecipientSnapshot = Body(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Create or update one recipient, synchronously.

    The product calls this before its first message to a new user, so
    that the message has somewhere to land.
    """

    async def _apply() -> Recipient:
        """The one mapping from body to sync call, used by both paths.

        A local closure rather than a kwargs dict: the dict erases the
        per-field types, and rewriting the six arguments twice would be
        two copies of the mapping for the first edit to desynchronise.
        """
        return await sync.user_upserted(
            session,
            recipient_id=recipient_id,
            telegram_id=snapshot.telegram_id,
            email=snapshot.email,
            locale=snapshot.locale,
            timezone=snapshot.timezone,
            active=snapshot.active,
        )

    try:
        # SAVEPOINT, not bare call: user_upserted looks the recipient
        # up and inserts when it finds nothing, so two writers racing
        # on a brand-new id (this route and the stream consumer, or two
        # product replicas) can both see nothing and both insert. The
        # loser gets an IntegrityError on the primary key, and without
        # the savepoint that error would poison the whole request
        # transaction rather than just its insert.
        async with session.begin_nested():
            recipient = await _apply()
    except IntegrityError:
        # The winner has committed by now -- that is what released the
        # lock and turned our insert into this error. Rolling back the
        # savepoint restored the session to its pre-insert state, so
        # calling the SAME function again now finds the row and takes
        # its update path. Retried exactly once: a second collision
        # would mean the row both exists and does not, which is not a
        # state this database can be in.
        logger.info(
            "recipient_upsert_raced",
            recipient_id=str(recipient_id),
        )
        recipient = await _apply()

    return _wire(recipient)
