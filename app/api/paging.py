# =============================================================================
# COMMS Service -- One way to page (F1.4, spec §10.10)
# =============================================================================
#
# Every listing route -- the inbox, the visible threads, a thread's
# messages, a job's deliveries and its path, the address book, the
# changes feed -- pages the same way:
#
#   request   ?limit=<1..PAGE_LIMIT_MAX, default PAGE_LIMIT_DEFAULT>
#             &cursor=<the previous page's next_cursor, opaque>
#   response  {"items": [...], "next_cursor": "<opaque>" | null}
#             (the inbox adds its `unread` badge beside them; the
#             changes feed's next_cursor is never null -- an empty page
#             still says where to continue)
#
# A LIMIT OUTSIDE THE BOUNDS IS REFUSED (422, class `validation`), not
# clamped. A clamp silently changes the request: a typo of 1000 returns
# a hundred rows and says nothing -- the same class as a silent typo in
# the profile. A malformed cursor is refused the same way on every
# route.
#
# THE CURSOR IS OPAQUE by contract -- clients echo it back and never
# parse it. ONE CODEC PER KIND OF KEY, all base64url:
#   (timestamp, uuid) -- "{timestamp}|{uuid}": the inbox, the threads,
#       the messages, a job's deliveries, the address book (each
#       listing its own order);
#   a monotone sequence -- "seq:{n}": a job's path, ordered by the
#       journal's identity (P2-2; timestamps tie inside a transaction);
#   a commit position -- "chg:{xact}:{id}:{read_at}": the changes feed
#       (P2-3, app/engine/changes.py list_changes) -- the journal's
#       (writing transaction, identity) and the moment it was read.
# Each decoder refuses the others' cursors (422): a cursor carried from
# one listing to another is malformed, not silently reinterpreted.
#
# MALFORMED IS DECIDED HERE, NOT IN THE DATABASE (TIME-1). comms writes
# every cursor itself, so what it would never write is the caller's
# error and a 422 -- never a 500 from the driver, never a silent reading:
#   - a time without a zone: the driver would take it as the server's
#     local time and page from an instant nobody sent;
#   - a time that leaves the range once moved to UTC (9999-12-31 at
#     -05:00): the driver's own conversion overflows;
#   - an integer the bigint column cannot hold (the journal's identity
#     behind "seq:" and the changes id). The changes feed's xact needs
#     no such bound here: list_changes refuses every xact at or past the
#     snapshot's xmax as "from the future" before it reaches a query.
# A refusal names the cursor and never quotes it.
# =============================================================================

import base64
import binascii
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from app.core.constants import PAGE_LIMIT_DEFAULT, PAGE_LIMIT_MAX
from app.core.exceptions import ValidationError
from app.core.sanitize import exception_text

__all__ = [
    "PAGE_LIMIT_DEFAULT",
    "ChangesCursor",
    "decode_changes_cursor",
    "decode_cursor",
    "decode_seq_cursor",
    "encode_changes_cursor",
    "encode_cursor",
    "encode_seq_cursor",
    "page",
    "page_by_seq",
    "page_limit",
]

_SEQ_PREFIX = "seq:"
_CHANGES_PREFIX = "chg:"

# The ceiling of a bigint column: the journal's identity, which "seq:"
# and the changes id are compared with. Its decimal form has 19 digits.
_BIGINT_MAX = 2**63 - 1
_BIGINT_DIGITS = len(str(_BIGINT_MAX))


def _digits(text: str) -> str:
    """Decimal ASCII digits, nothing else (no sign), or ValueError."""
    if not (text.isascii() and text.isdigit()):
        raise ValueError("not a non-negative integer")
    return text


def _counter(text: str) -> int:
    """A non-negative decimal integer, or ValueError."""
    return int(_digits(text))


def _bigint(text: str) -> int:
    """A non-negative integer a bigint column can hold, or ValueError.
    The length is checked before int() reads the digits, so a string of
    any size costs no conversion."""
    digits = _digits(text)
    if len(digits) > _BIGINT_DIGITS or int(digits) > _BIGINT_MAX:
        raise ValueError("an integer beyond the 64-bit column's range")
    return int(digits)


def _instant(text: str) -> datetime:
    """A time with its zone, convertible to UTC, or ValueError."""
    when = datetime.fromisoformat(text)
    if when.tzinfo is None:
        raise ValueError("the time carries no zone")
    try:
        when.astimezone(UTC)
    except OverflowError:
        raise ValueError("the time leaves the range once moved to UTC") from None
    return when


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
        return _instant(when_text), UUID(id_text)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValidationError(f"Malformed cursor: {exception_text(exc)}") from exc


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
        return _bigint(raw.removeprefix(_SEQ_PREFIX))
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValidationError(f"Malformed cursor: {exception_text(exc)}") from exc


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


@dataclass(frozen=True)
class ChangesCursor:
    """A position in the changes feed and the moment it was read.

    (xact, ident) is the journal's (xact_id, id) of the last row the
    product was given -- (0, 0) before the first. read_at is the
    database clock of the read that handed the cursor out: what tells a
    cursor that outlived the retention period from one that saw nothing
    new (app/engine/changes.py list_changes).
    """

    xact: int
    ident: int
    read_at: datetime


def encode_changes_cursor(cursor: ChangesCursor) -> str:
    raw = (
        f"{_CHANGES_PREFIX}{cursor.xact}:{cursor.ident}:"
        f"{cursor.read_at.isoformat()}"
    )
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def decode_changes_cursor(value: str | None) -> ChangesCursor | None:
    """Decode a changes cursor; any malformation -> ValidationError
    (422) -- including a cursor of another listing."""
    if value is None:
        return None
    try:
        raw = base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8")
        if not raw.startswith(_CHANGES_PREFIX):
            raise ValueError("not a changes cursor")
        xact_text, ident_text, read_text = raw.removeprefix(
            _CHANGES_PREFIX,
        ).split(":", 2)
        return ChangesCursor(
            _counter(xact_text), _bigint(ident_text), _instant(read_text),
        )
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValidationError(f"Malformed cursor: {exception_text(exc)}") from exc
