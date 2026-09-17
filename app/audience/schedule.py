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
# DST: local moments are built with fold=0; around a transition the
# deferred moment may shift by the offset delta. Two cases are named
# rather than pretended away. In autumn a period inside the repeated
# hour opens twice -- harmless. In spring a period lying ENTIRELY
# inside the skipped hour (02:00-03:00) does not open on that date;
# the search below walks on to the next date that has it, so a weekly
# schedule loses one occurrence and recovers by itself. For a "when
# may we reach you" feature that error bar is accepted, and the search
# logs when it runs out of days, so the case is observable rather than
# silent.
# =============================================================================

from datetime import UTC, datetime, timedelta
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
    """The earliest period start strictly after `now`, in UTC."""
    local_now = now.astimezone(tz)
    today = local_now.date()

    for offset in range(_SEARCH_DAYS):
        date = today + timedelta(days=offset)
        midnight = datetime.combine(date, datetime.min.time(), tzinfo=tz)
        starts = sorted(
            midnight + timedelta(minutes=window["from"])
            for window in windows
            if window["day"] == date.isoweekday()
        )
        for start in starts:
            if start > local_now:
                return start.astimezone(UTC)

    # Reachable only if a DST skip swallows the sole period of the
    # sole configured day. Logged rather than asserted: a delivery
    # must not die on an assertion, and returning None here means
    # "send it" -- the safe direction for a postponement feature.
    logger.warning(
        "schedule_has_no_opening",
        windows=windows,
        timezone=str(tz),
        searched_days=_SEARCH_DAYS,
    )
    return None


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
