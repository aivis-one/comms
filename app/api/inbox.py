# =============================================================================
# COMMS Service -- Inbox API (the in-app "bell") -- FROZEN CONTRACT
# =============================================================================
#
# Phase 3b item 2. The recipient's inbox IS their in_app delivery rows
# (the cbshome read_at pattern): each item is a NotificationDelivery
# with channel=in_app and status=sent, enriched with title / body /
# navigational action_data from the parent Notification. Reading YOUR
# OWN delivery rows -- never the notification's audience -- keeps the
# BL-1 door open (set-based resolve stays a local change).
#
# CONTRACT (Phase 6 product proxy consumes as-is):
#
#   GET  /api/v1/recipients/{rid}/inbox?limit=&cursor=
#     -> 200 {
#          "items": [
#            {
#              "id": "<delivery uuid>",
#              "type": "<notification type key>",
#              "title": "...", "body": "...",
#              "action_data": {"action": "...", "params": {...}} | null,
#              "priority": 0,
#              "sent_at": "<iso8601>",
#              "read_at": "<iso8601>" | null,
#              "created_at": "<iso8601>"
#            }, ...                      # newest-first
#          ],
#          "next_cursor": "<opaque>" | null,
#          "unread": <int>              # badge in the same round-trip
#        }
#     - limit: 1..100, default 20 (clamped, not rejected);
#     - cursor: opaque string from the previous page's next_cursor;
#       a malformed cursor -> 422;
#     - action_data is the NAVIGATIONAL INTENT ONLY (amendment A):
#       template variables never leave the service.
#
#   GET  /api/v1/recipients/{rid}/inbox/unread-count
#     -> 200 {"unread": <int>}          # cheap badge polling
#
#   POST /api/v1/recipients/{rid}/inbox/{delivery_id}/read
#     -> 200 {"unread": <int>}          # idempotent; 404 if the
#                                       # delivery is absent or not
#                                       # this recipient's
#   POST /api/v1/recipients/{rid}/inbox/read-all
#     -> 200 {"unread": <int>}          # always 0, returned anyway so
#                                       # every mutation answers with
#                                       # the fresh badge
#
# Mark-read responses compute the counter IN THE SAME transaction as
# the read_at write (Master-chat, part C): under a race (two tabs) the
# returned badge must reflect this request's own write, not a stale
# snapshot.
#
# The recipient itself is NOT existence-checked here: an unknown
# recipient_id simply has an empty inbox and a zero badge. The proxy
# only ever asks for users it has authorized, and "empty" is the
# honest answer for a recipient comms has not (yet) synced.
#
# TRUST MODEL (part of the frozen contract, review 3b): comms does NOT
# verify that recipient_id belongs to the calling end user -- there is
# no per-recipient authorization here at all. The shared service token
# authenticates the PRODUCT (arch decision 14), and the product proxy
# is the sole owner of the "user X may only read/mark user X's inbox"
# check. Phase 6 MUST substitute the recipient_id server-side from its
# own authenticated session, never accept it from the client.
# =============================================================================

import base64
import binascii
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_service_auth
from app.core.database import get_db_reader, get_db_session
from app.core.exceptions import ValidationError
from app.engine.constants import DeliveryChannel
from app.engine.service import (
    get_unread_count,
    list_recipient_deliveries,
    mark_all_read,
    mark_delivery_read,
)

router = APIRouter(
    prefix="/api/v1/recipients/{recipient_id}/inbox",
    tags=["inbox"],
    dependencies=[Depends(require_service_auth)],
)


# ---------------------------------------------------------------------------
# Cursor codec
# ---------------------------------------------------------------------------
# The wire cursor is OPAQUE by contract -- clients must echo it back,
# never parse it (the encoding may change without notice as long as a
# cursor round-trips within one page sequence). Internally it is
# base64url("{sent_at.isoformat()}|{delivery_id}") over the keyset key
# (sent_at DESC, id DESC) -- see list_recipient_deliveries.


def _encode_cursor(cursor: tuple[datetime, UUID]) -> str:
    sent_at, delivery_id = cursor
    raw = f"{sent_at.isoformat()}|{delivery_id}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _decode_cursor(value: str) -> tuple[datetime, UUID]:
    """Decode a wire cursor; any malformation -> ValidationError (422)."""
    try:
        raw = base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8")
        sent_at_text, _, id_text = raw.partition("|")
        if not id_text:
            raise ValueError("missing separator")
        return datetime.fromisoformat(sent_at_text), UUID(id_text)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValidationError(f"Malformed inbox cursor: {exc}") from exc


def _serialize_item(item: dict[str, Any]) -> dict[str, Any]:
    """Wire form of one inbox item (frozen contract, see header)."""
    return {
        "id": str(item["id"]),
        "type": item["type"],
        "title": item["title"],
        "body": item["body"],
        "action_data": item["action_data"],
        "priority": item["priority"],
        "sent_at": item["sent_at"].isoformat(),
        "read_at": (
            item["read_at"].isoformat()
            if item["read_at"] is not None
            else None
        ),
        "created_at": item["created_at"].isoformat(),
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("")
async def get_inbox(
    recipient_id: UUID,
    limit: int = Query(default=20),
    cursor: str | None = Query(default=None),
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, Any]:
    """The bell feed: unread-first? No -- newest-first, plus the badge."""
    decoded = _decode_cursor(cursor) if cursor is not None else None
    items, next_cursor = await list_recipient_deliveries(
        session,
        recipient_id,
        limit=limit,
        cursor=decoded,
        channel_filter=DeliveryChannel.IN_APP,
    )
    unread = await get_unread_count(
        session, recipient_id, channel=DeliveryChannel.IN_APP
    )
    return {
        "items": [_serialize_item(item) for item in items],
        "next_cursor": (
            _encode_cursor(next_cursor) if next_cursor is not None else None
        ),
        "unread": unread,
    }


@router.get("/unread-count")
async def unread_count(
    recipient_id: UUID,
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, int]:
    """Cheap badge polling: the unread counter alone."""
    unread = await get_unread_count(
        session, recipient_id, channel=DeliveryChannel.IN_APP
    )
    return {"unread": unread}


@router.post("/read-all")
async def read_all(
    recipient_id: UUID,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, int]:
    """Mark every unread in_app delivery read; returns the fresh badge."""
    await mark_all_read(
        session, recipient_id, channel=DeliveryChannel.IN_APP
    )
    unread = await get_unread_count(
        session, recipient_id, channel=DeliveryChannel.IN_APP
    )
    return {"unread": unread}


@router.post("/{delivery_id}/read")
async def read_one(
    recipient_id: UUID,
    delivery_id: UUID,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, int]:
    """Mark one delivery read (idempotent); returns the fresh badge.

    404 when the delivery does not exist or belongs to another
    recipient (the service layer does not distinguish, deliberately).
    The badge is computed in the SAME transaction/session as the
    read_at write, so it reflects this request's own effect.
    """
    await mark_delivery_read(session, recipient_id, delivery_id)
    unread = await get_unread_count(
        session, recipient_id, channel=DeliveryChannel.IN_APP
    )
    return {"unread": unread}
