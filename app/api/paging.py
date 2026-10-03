# =============================================================================
# COMMS Service -- One way to page (F1.4, spec §10.10)
# =============================================================================
#
# Every listing route -- the inbox, the visible threads, a thread's
# messages, a job's deliveries and its path, the address book -- pages
# the same way:
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
# parse it. ONE CODEC PER KIND OF KEY, both base64url:
#   (timestamp, uuid) -- "{timestamp}|{uuid}": the inbox, the threads,
#       the messages, a job's deliveries, the address book (each
#       listing its own order);
#   a monotone sequence -- "seq:{n}": a job's path, ordered by the
#       journal's identity (P2-2; timestamps tie inside a transaction).
# Each decoder refuses the other's cursor (422): a cursor carried from
# one listing to another is malformed, not silently reinterpreted.
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
    "decode_seq_cursor",
    "encode_cursor",
    "encode_seq_cursor",
    "page",
    "page_by_seq",
    "page_limit",
]

_SEQ_PREFIX = "seq:"


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


def encode_seq_cursor(position: int) -> str:
    raw = f"{_SEQ_PREFIX}{position}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def decode_seq_cursor(value: str | None) -> int | None:
    """Decode a sequence cursor; any malformation -> ValidationError
    (422) -- including a (timestamp, uuid) cursor from another listing."""
    if value is None:
        return None
    try:
        raw = base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8")
        if not raw.startswith(_SEQ_PREFIX):
            raise ValueError("not a sequence cursor")
        digits = raw.removeprefix(_SEQ_PREFIX)
        if not (digits.isascii() and digits.isdigit()):
            raise ValueError("not a non-negative integer")
        return int(digits)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValidationError(f"Malformed cursor: {exc}") from exc


def page_by_seq[T](
    items: Sequence[T],
    next_position: int | None,
    serialize: Callable[[T], dict[str, Any]],
) -> dict[str, Any]:
    """The same response shape over a sequence-keyed listing."""
    return {
        "items": [serialize(item) for item in items],
        "next_cursor": (
            encode_seq_cursor(next_position) if next_position is not None else None
        ),
    }
