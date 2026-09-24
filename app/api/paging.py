# =============================================================================
# COMMS Service -- One way to page (F1.4, spec §10.10)
# =============================================================================
#
# Every listing route -- the inbox, the visible threads, a thread's
# messages -- pages the same way:
#
#   request   ?limit=<1..PAGE_LIMIT_MAX, default PAGE_LIMIT_DEFAULT>
#             &cursor=<the previous page's next_cursor, opaque>
#   response  {"items": [...], "next_cursor": "<opaque>" | null}
#             (the inbox adds its `unread` badge beside them)
#
# A LIMIT OUTSIDE THE BOUNDS IS REFUSED (422, class `validation`), not
# clamped. A clamp silently changes the request: a typo of 1000 returns
# a hundred rows and says nothing -- the same class as a silent typo in
# the profile. A malformed cursor is refused the same way on every
# route.
#
# THE CURSOR IS OPAQUE by contract -- clients echo it back and never
# parse it. Internally it is base64url("{timestamp}|{uuid}") over each
# listing's keyset key (timestamp DESC, id DESC), ONE codec for all.
# =============================================================================

import base64
import binascii
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from app.core.constants import PAGE_LIMIT_DEFAULT, PAGE_LIMIT_MAX
from app.core.exceptions import ValidationError

__all__ = [
    "PAGE_LIMIT_DEFAULT",
    "decode_cursor",
    "encode_cursor",
    "page",
    "page_limit",
]


def page_limit(value: int) -> int:
    """The requested page size, or a refusal -- never a clamp."""
    if not 1 <= value <= PAGE_LIMIT_MAX:
        raise ValidationError(
            f"limit must be between 1 and {PAGE_LIMIT_MAX}, got {value}"
        )
    return value


def encode_cursor(cursor: tuple[datetime, UUID]) -> str:
    when, ident = cursor
    raw = f"{when.isoformat()}|{ident}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def decode_cursor(value: str | None) -> tuple[datetime, UUID] | None:
    """Decode a wire cursor; any malformation -> ValidationError (422)."""
    if value is None:
        return None
    try:
        raw = base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8")
        when_text, _, id_text = raw.partition("|")
        if not id_text:
            raise ValueError("missing separator")
        return datetime.fromisoformat(when_text), UUID(id_text)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValidationError(f"Malformed cursor: {exc}") from exc


def page[T](
    items: Sequence[T],
    next_cursor: tuple[datetime, UUID] | None,
    serialize: Callable[[T], dict[str, Any]],
) -> dict[str, Any]:
    """The one response shape of a listing."""
    return {
        "items": [serialize(item) for item in items],
        "next_cursor": encode_cursor(next_cursor) if next_cursor else None,
    }
