# =============================================================================
# COMMS Service -- Quiet-Hours Window Math (Phase 2)
# =============================================================================
#
# Pure functions answering ONE question: "is `now` inside the
# recipient's quiet window, and if so -- when does it open?" The
# delivery pipeline (service.deliver_notification) uses the answer to
# DEFER a delivery via the existing next_retry_at gate, never to drop
# it (arch doc §5: quiet hours postpone, not suppress).
#
# WINDOW MODEL:
#   quiet_from / quiet_to -- local wall-clock times in the recipient's
#     timezone. from >= to means the window crosses midnight
#     (22:00 -> 08:00). from == to is rejected at write time
#     (prefs.set_quiet_hours), so it cannot reach this module.
#   quiet_days -- ISO weekdays (1=Mon .. 7=Sun) on which the window
#     STARTS. A Friday 22:00 -> 08:00 window covers Saturday morning
#     but belongs to day 5.
#
# ALGORITHM (next_window_open):
#   A moment can only be inside a window that started TODAY or
#   YESTERDAY (windows are < 24h by construction). Check both
#   candidate starts against quiet_days, compute each window's end
#   (same day, or +1 day for overnight), and if `now` falls in any,
#   return the LATEST end among the covering windows (adjacent
#   overnight windows can overlap a morning), converted to UTC.
#   Returns None when `now` is outside every window.
#
# BOUNDARIES: start is inclusive, end is exclusive -- at exactly
# quiet_to the delivery goes out.
#
# DST: local candidate datetimes are built with fold=0; around a DST
# transition the deferred moment may shift by the offset delta. For a
# "do not buzz at night" feature that error bar is acceptable.
# =============================================================================

from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

import structlog

from app.audience.models import Recipient
from app.core.config import settings

logger = structlog.get_logger()


def next_window_open(
    now: datetime,
    *,
    tz: ZoneInfo,
    quiet_from: time,
    quiet_to: time,
    days: list[int],
) -> datetime | None:
    """UTC end of the quiet window covering `now`, or None if outside.

    `now` must be timezone-aware (the pipeline passes UTC). `days`
    are ISO weekdays of window STARTS.
    """
    local_now = now.astimezone(tz)

    # Candidate window starts: yesterday and today (local). Windows
    # are shorter than 24h, so nothing older can still cover `now`.
    open_until: datetime | None = None
    for delta_days in (-1, 0):
        start_day = (local_now + timedelta(days=delta_days)).date()
        if start_day.isoweekday() not in days:
            continue
        start = datetime.combine(start_day, quiet_from, tzinfo=tz)
        end_day = start_day if quiet_from < quiet_to else start_day + timedelta(
            days=1
        )
        end = datetime.combine(end_day, quiet_to, tzinfo=tz)
        # Inclusive start, exclusive end.
        if start <= local_now < end and (
            open_until is None or end > open_until
        ):
            open_until = end

    if open_until is None:
        return None
    return open_until.astimezone(UTC)


def recipient_quiet_until(
    recipient: Recipient,
    now: datetime,
) -> datetime | None:
    """UTC end of the recipient's quiet window covering `now`.

    None when the recipient has no (complete) quiet-hours setup or
    `now` is outside the window. All three of quiet_from / quiet_to /
    quiet_days must be set -- prefs.set_quiet_hours enforces
    all-or-nothing, so a partial state only appears via manual DB
    edits and is treated as "not configured".
    """
    if (
        recipient.quiet_from is None
        or recipient.quiet_to is None
        or not recipient.quiet_days
    ):
        return None
    tz = _resolve_tz(recipient.timezone)
    return next_window_open(
        now,
        tz=tz,
        quiet_from=recipient.quiet_from,
        quiet_to=recipient.quiet_to,
        days=recipient.quiet_days,
    )


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
