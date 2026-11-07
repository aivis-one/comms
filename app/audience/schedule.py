# =============================================================================
# COMMS Service -- Delivery schedule (R-5, replaces quiet-hours math)
# =============================================================================
#
# Pure functions answering ONE question: "may this recipient be
# reached right now, and if not -- when may they be?" The delivery
# pipeline (service.deliver_notification) uses the answer to DEFER a
# delivery via the existing next_retry_at gate, never to drop it (arch
# doc S5: a schedule postpones, not suppresses).
#
# WHAT REPLACED WHAT. Until R-5 this module held ONE quiet window --
# quiet_from / quiet_to plus the weekdays the window STARTED on. That
# shape describes "do not disturb at night", which one product's
# screen states directly; the other's screen states working hours,
# inverted the times in a proxy, and thereby turned its window
# nocturnal -- and a nocturnal window's start day is the evening
# BEFORE the morning it covers. Measured consequence: "deliver Monday
# 09-21" delivered Monday 00:00-09:00 and fell silent on Tuesday
# morning. Neither end was asked for, and no fix inside the product
# worked: shifting the days moved the error to the other end, and
# unioning them widened the user's choice (asking for Mon+Wed also
# delivered on Tue).
#
# THE MODEL NOW: the periods during which delivery IS ALLOWED. Each
# period belongs to the weekday it falls in and NEVER crosses
# midnight; a night allowance is two periods, one per day. There is no
# "start day" to get wrong, no window longer than a day, and no limit
# of one period per recipient.
#
# MINUTES FROM LOCAL MIDNIGHT, and `to` may be 1440. The old model had
# no way to say "this whole day": 00:00 -> 23:59 was the closest form
# and left a one-minute hole that delivered at 23:59 on a day the user
# had marked silent. 1440 is exactly midnight and closes it.
#
# NO SCHEDULE (None) MEANS NO RESTRICTION. An empty list is refused at
# write time (audience/prefs.set_schedule): "never" is not a schedule,
# it is a black hole where deliveries defer until they expire.
#
# BOUNDARIES: start inclusive, end exclusive -- at exactly the end
# minute the recipient is already outside the period. Same convention
# the quiet window used. It is also why the write path refuses two
# touching periods (09:00-12:00 and 12:00-18:00): one continuous
# stretch has one spelling.
#
# DST. A period's start is a wall time; the search turns it into the
# instants that wall time names and compares INSTANTS with `now` (TIME-1).
#   - Autumn, the repeated hour: a wall time inside it names two
#     instants, and both are openings -- a period inside that hour opens
#     twice, harmless. The search used to compare wall times and build
#     the start with fold=0: on the hour's second pass it took the FIRST
#     instant, already past, and deferred the delivery into the past --
#     picked up at once, deferred again, a journal row each time, until
#     the second opening came. Now each instant is a candidate and the
#     earliest one after `now` wins.
#     A period need not START in that hour to open in it: one that runs
#     on into it (00:00-02:30) or ends inside it (01:00-02:15) closes on
#     the first pass and opens again the moment the clocks go back. That
#     moment is a candidate too, when a period holds it (REV-5) -- the
#     search used to know starts only, and deferred such a delivery to
#     the next week.
#   - Spring, the skipped hour: a wall time inside it names no instant.
#     It is read with fold=0 -- the offset before the jump -- and so lands
#     as far past the jump as it lay past the hour's start (02:30 ->
#     03:30). A period lying ENTIRELY inside the skipped hour (02:00-03:00)
#     is waited for at that reading of its start, where it has already
#     closed; the search run at that moment goes on to the next date that
#     has the period: a weekly schedule loses one occurrence and recovers
#     by itself. A period only partly inside opens late by what was
#     skipped (02:30-04:00 is waited for until 03:30, though 03:00 is
#     already inside it): the deferred moment shifts by at most the offset
#     delta. For a "when may we reach you" feature that error bar is
#     accepted, and so the moment the clocks jump FORWARD is no candidate
#     (only the moment they go back is, above). Nothing is logged for it
#     -- the search always finds an occurrence (_next_open).
# =============================================================================

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import structlog

from app.audience.models import Recipient
from app.core.config import settings

logger = structlog.get_logger()

# Minutes in a day. A period's end may equal it (exactly midnight); a
# start may not.
MINUTES_PER_DAY = 1440

# How far ahead the search for the next opening goes. A schedule
# repeats weekly, so a non-empty one always has an opening within
# seven days; the eighth covers the day already partly spent when the
# search starts.
_SEARCH_DAYS = 8


def is_delivery_allowed(
    now: datetime,
    *,
    tz: ZoneInfo,
    windows: list[dict[str, int]],
) -> bool:
    """True when `now` falls inside one of the allowed periods."""
    local_now = now.astimezone(tz)
    minutes = local_now.hour * 60 + local_now.minute
    iso_day = local_now.isoweekday()
    return any(
        window["day"] == iso_day
        and window["from"] <= minutes < window["to"]
        for window in windows
    )


def next_delivery_allowed_at(
    now: datetime,
    *,
    tz: ZoneInfo,
    windows: list[dict[str, int]],
) -> datetime | None:
    """UTC moment when delivery becomes allowed, or None for "now".

    None is the "go ahead" answer -- returned both when `now` is
    inside a period and when there is no schedule at all, so the
    caller tests one condition rather than two.
    """
    if not windows:
        # No schedule is no restriction. The empty list cannot come
        # from the write path (refused there); it can come from a
        # cleared schedule, and it means the same thing as None.
        return None
    if is_delivery_allowed(now, tz=tz, windows=windows):
        return None
    return _next_open(now, tz=tz, windows=windows)


def _next_open(
    now: datetime,
    *,
    tz: ZoneInfo,
    windows: list[dict[str, int]],
) -> datetime | None:
    """The earliest opening strictly after `now`, in UTC.

    An opening is a period's start -- or the moment the clocks go back
    into a period (the module header, DST). Compared as instants, never
    as wall times: on the repeated hour of autumn one wall time is two
    instants.
    """
    instant_now = now.astimezone(UTC)
    today = instant_now.astimezone(tz).date()

    openings: list[datetime] = []
    for offset in range(_SEARCH_DAYS):
        date = today + timedelta(days=offset)
        midnight = datetime.combine(date, datetime.min.time(), tzinfo=tz)
        for window in windows:
            if window["day"] == date.isoweekday():
                openings += _instants(
                    midnight + timedelta(minutes=window["from"]), tz,
                )
        back = _clocks_back(date, tz)
        if back is not None and is_delivery_allowed(back, tz=tz, windows=windows):
            openings.append(back)
    later = [opening for opening in openings if opening > instant_now]
    if later:
        return min(later)

    # Not reached while the search covers eight dates: every configured
    # weekday recurs within the next seven, and a start on a later date
    # is after `now` even when read inside a skipped hour. Kept as the
    # guard a shorter search would need -- logged rather than asserted:
    # a delivery must not die on an assertion, and returning None here
    # means "send it", the safe direction for a postponement feature.
    logger.warning(
        "schedule_has_no_opening",
        windows=windows,
        timezone=str(tz),
        searched_days=_SEARCH_DAYS,
    )
    return None


def _clocks_back(day: date, tz: ZoneInfo) -> datetime | None:
    """The UTC moment, to the minute, the clocks go back during `day`
    -- None on a day they do not.

    The offset at the next midnight is below this midnight's on such a
    day; the moment of the change is found by halving the day in UTC.
    """
    start = datetime.combine(day, datetime.min.time(), tzinfo=tz)
    end = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=tz)
    before, after = start.utcoffset(), end.utcoffset()
    if before is None or after is None or after >= before:
        return None
    low, high = start.astimezone(UTC), end.astimezone(UTC)
    while high - low > timedelta(minutes=1):
        middle = low + (high - low) / 2
        if middle.astimezone(tz).utcoffset() == before:
            low = middle
        else:
            high = middle
    return high.replace(second=0, microsecond=0)


def _instants(wall: datetime, tz: ZoneInfo) -> list[datetime]:
    """The UTC instants a local wall time names: one; two inside the
    repeated hour of autumn (fold 0 and 1); none inside the skipped hour
    of spring -- read then with fold=0, the offset before the jump (the
    module header, DST).

    A fold names an instant only when that instant, back in the zone, is
    the same wall time: inside the skipped hour neither fold comes back.
    """
    found = []
    for fold in (0, 1):
        instant = wall.replace(fold=fold).astimezone(UTC)
        if instant.astimezone(tz).replace(tzinfo=None) == wall.replace(tzinfo=None):
            found.append(instant)
    return found or [wall.replace(fold=0).astimezone(UTC)]


def recipient_deferred_until(
    recipient: Recipient,
    now: datetime,
) -> datetime | None:
    """UTC moment this recipient may be reached, or None for "now".

    None when the recipient has no schedule or `now` is inside an
    allowed period -- the delivery goes out. A datetime means the
    delivery is deferred to it.
    """
    windows = recipient.allowed_windows
    if not windows:
        return None
    tz = _resolve_tz(recipient.timezone)
    return next_delivery_allowed_at(now, tz=tz, windows=windows)


def _resolve_tz(name: str | None) -> ZoneInfo:
    """Recipient timezone with fallback to the deploy default.

    A bad stored name must not break the delivery path: log loudly,
    fall back. settings.default_timezone is validated at startup, so
    the fallback itself cannot fail.
    """
    if name is None:
        return ZoneInfo(settings.default_timezone)
    try:
        return ZoneInfo(name)
    except (KeyError, ValueError):
        logger.warning(
            "invalid_recipient_timezone",
            timezone=name,
            fallback=settings.default_timezone,
        )
        return ZoneInfo(settings.default_timezone)
