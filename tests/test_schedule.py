# =============================================================================
# COMMS Service -- Delivery schedule math tests (R-5)
# =============================================================================
# Pure-function coverage of app/audience/schedule.py on FIXED datetimes
# (no DB, no clock).
#
# WHAT THESE TESTS REPLACED. Until R-5 this file covered a QUIET
# window -- when NOT to deliver -- keyed by the weekdays the window
# STARTED on, and every assertion in it was true of that model. Two of
# those assertions were about a behaviour that no longer exists and
# could not be carried across: "an overnight window started yesterday
# still covers this morning" and "the tail of an overnight window is
# not covered when the wrong day is its start". They were right: that
# WAS the model. It is the model that was wrong -- the start-day
# notion made "deliver Monday 09-21" deliver Monday 00:00-09:00 and
# fall silent on Tuesday morning. A period now belongs to the day it
# falls in and never crosses midnight, so "yesterday's window" has
# nothing to cover today and the assertions have nothing to assert.
#
# Everything else carried over in the new polarity: boundary
# semantics, day filtering, timezone conversion, the recipient-level
# wrapper's None handling and its timezone fallback.
#
# Calendar anchors (July 2026):
#   Fri 2026-07-10 (isoweekday 5), Sat 11 (6), Sun 12 (7), Mon 13 (1).
# =============================================================================

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.audience.schedule import (
    is_delivery_allowed,
    next_delivery_allowed_at,
    recipient_deferred_until,
)

UTC_TZ = ZoneInfo("UTC")
BERLIN = ZoneInfo("Europe/Berlin")  # CEST = UTC+2 in July.

FRI, SAT, SUN, MON = 5, 6, 7, 1


def _utc(day: int, hour: int, minute: int = 0) -> datetime:
    """Aware UTC datetime in July 2026."""
    return datetime(2026, 7, day, hour, minute, tzinfo=UTC)


def _period(day: int, start: str, end: str) -> dict[str, int]:
    """A stored period from two "HH:MM" strings."""

    def minutes(value: str) -> int:
        hours, mins = value.split(":")
        return int(hours) * 60 + int(mins)

    return {"day": day, "from": minutes(start), "to": minutes(end)}


class TestIsDeliveryAllowed:
    """The "may I send right now" half, over explicit periods."""

    def test_inside_a_period(self) -> None:
        assert is_delivery_allowed(
            _utc(10, 12), tz=UTC_TZ, windows=[_period(FRI, "08:00", "17:00")]
        )

    def test_outside_a_period(self) -> None:
        assert not is_delivery_allowed(
            _utc(10, 20), tz=UTC_TZ, windows=[_period(FRI, "08:00", "17:00")]
        )

    def test_start_is_inclusive_and_end_is_exclusive(self) -> None:
        """The boundary rule carried over from the quiet window: at the
        first minute you are in, at the last you are already out."""
        windows = [_period(FRI, "08:00", "17:00")]
        assert is_delivery_allowed(_utc(10, 8), tz=UTC_TZ, windows=windows)
        assert not is_delivery_allowed(
            _utc(10, 17), tz=UTC_TZ, windows=windows
        )

    def test_a_period_covers_only_its_own_day(self) -> None:
        """THE DEFECT THIS RELEASE EXISTS FOR, as an assertion.

        Under the old model a window could be owned by one day and
        cover another (an overnight window starting Friday covered
        Saturday morning). Now Friday's period covers Friday and
        nothing else -- the same clock hours on Saturday are outside.
        """
        windows = [_period(FRI, "08:00", "17:00")]
        assert is_delivery_allowed(_utc(10, 12), tz=UTC_TZ, windows=windows)
        assert not is_delivery_allowed(
            _utc(11, 12), tz=UTC_TZ, windows=windows
        )

    def test_a_night_allowance_is_two_periods(self) -> None:
        """The form that replaced "crosses midnight": 22:00-24:00 on
        Friday plus 00:00-02:00 on Saturday. Both ends covered, and
        each belongs to the day a reader would name."""
        windows = [
            _period(FRI, "22:00", "24:00"),
            _period(SAT, "00:00", "02:00"),
        ]
        assert is_delivery_allowed(_utc(10, 23), tz=UTC_TZ, windows=windows)
        assert is_delivery_allowed(_utc(11, 1), tz=UTC_TZ, windows=windows)
        assert not is_delivery_allowed(
            _utc(11, 3), tz=UTC_TZ, windows=windows
        )

    def test_the_last_minute_of_a_full_day_is_covered(self) -> None:
        """THE ONE-MINUTE HOLE, closed. The old model's closest form
        for a whole day was 00:00-23:59, and at 23:59 a notification
        went out on a day the user had marked silent. An end of 24:00
        is exactly midnight."""
        windows = [_period(SAT, "00:00", "24:00")]
        assert is_delivery_allowed(
            _utc(11, 23, 59), tz=UTC_TZ, windows=windows
        )

    def test_timezone_decides_which_day_it_is(self) -> None:
        """23:30 UTC on Friday is 01:30 Saturday in Berlin, so a
        Saturday period covers it and a Friday one does not."""
        assert is_delivery_allowed(
            _utc(10, 23, 30), tz=BERLIN, windows=[_period(SAT, "01:00", "03:00")]
        )
        assert not is_delivery_allowed(
            _utc(10, 23, 30), tz=BERLIN, windows=[_period(FRI, "01:00", "03:00")]
        )


class TestNextDeliveryAllowedAt:
    """The "when may I send" half: None means now."""

    def test_inside_a_period_means_now(self) -> None:
        assert (
            next_delivery_allowed_at(
                _utc(10, 12),
                tz=UTC_TZ,
                windows=[_period(FRI, "08:00", "17:00")],
            )
            is None
        )

    def test_before_the_period_defers_to_its_start(self) -> None:
        assert next_delivery_allowed_at(
            _utc(10, 6), tz=UTC_TZ, windows=[_period(FRI, "08:00", "17:00")]
        ) == _utc(10, 8)

    def test_after_the_period_defers_to_the_next_week(self) -> None:
        """A weekly schedule always has a next opening; with a single
        Friday period it is seven days out, not never."""
        assert next_delivery_allowed_at(
            _utc(10, 20), tz=UTC_TZ, windows=[_period(FRI, "08:00", "17:00")]
        ) == _utc(17, 8)

    def test_the_earliest_of_several_periods_wins(self) -> None:
        windows = [
            _period(FRI, "20:00", "22:00"),
            _period(FRI, "09:00", "12:00"),
            _period(MON, "09:00", "12:00"),
        ]
        assert next_delivery_allowed_at(
            _utc(10, 7), tz=UTC_TZ, windows=windows
        ) == _utc(10, 9)

    def test_no_schedule_means_now(self) -> None:
        """PUSTOTA: an empty list reads the same as "no schedule".
        The write path refuses to store one, but a cleared schedule
        must not mean "never deliver again"."""
        assert (
            next_delivery_allowed_at(_utc(10, 3), tz=UTC_TZ, windows=[])
            is None
        )

    def test_deferral_is_expressed_in_utc(self) -> None:
        """Berlin is UTC+2 in July: a 09:00 local opening is 07:00 UTC,
        and the pipeline stores UTC."""
        result = next_delivery_allowed_at(
            _utc(10, 3), tz=BERLIN, windows=[_period(FRI, "09:00", "12:00")]
        )
        assert result == _utc(10, 7)
        assert result is not None and result.tzinfo is UTC


class TestRecipientDeferredUntil:
    """Recipient-level wrapper: no schedule, and timezone fallback."""

    def _recipient(self, **overrides: object) -> Recipient:
        """Transient recipient (no DB) allowed Friday 08:00-17:00."""
        defaults: dict[str, object] = {
            "timezone": "UTC",
            "allowed_windows": [_period(FRI, "08:00", "17:00")],
        }
        defaults.update(overrides)
        return Recipient(**defaults)

    def test_inside_the_period_sends_now(self) -> None:
        assert recipient_deferred_until(self._recipient(), _utc(10, 12)) is None

    def test_outside_the_period_defers(self) -> None:
        assert (
            recipient_deferred_until(self._recipient(), _utc(10, 6))
            == _utc(10, 8)
        )

    @pytest.mark.parametrize("windows", [None, []])
    def test_no_schedule_sends_now(self, windows: object) -> None:
        """NEHVATKA: the absence of a schedule is not a restriction.

        The old model needed three columns to agree and treated any
        missing one as "no window" -- that guard was right about the
        shape it guarded. One column cannot be half-set, so what is
        left is the actual question: null and empty both mean send.
        """
        recipient = self._recipient(allowed_windows=windows)
        assert recipient_deferred_until(recipient, _utc(10, 3)) is None

    def test_missing_timezone_falls_back_to_default(self) -> None:
        """timezone=None uses settings.default_timezone (UTC here)."""
        recipient = self._recipient(timezone=None)
        assert recipient_deferred_until(recipient, _utc(10, 6)) == _utc(10, 8)

    def test_bad_timezone_falls_back_with_warning(self) -> None:
        """A corrupt stored timezone logs and falls back, never raises:
        a delivery must not die because a product sent a typo."""
        recipient = self._recipient(timezone="Not/AZone")
        with capture_logs() as logs:
            result = recipient_deferred_until(recipient, _utc(10, 6))
        assert result == _utc(10, 8)
        assert any(
            log["event"] == "invalid_recipient_timezone" for log in logs
        )


class TestTheRuleTheOldModelCouldNotExpress:
    """The case this release exists for, as a week of hours.

    "Deliver Monday to Friday 09:00-21:00; weekends silent" -- a
    product's screen, stated plainly. Under the quiet window it was
    inexpressible: one window with one pair of times could either say
    "21:00-09:00 on weekdays" or "all day on weekends", never both,
    and whichever it said, the day set meant "the day the window
    STARTS on", so Monday morning was delivered and Tuesday morning
    was not.

    The grid below is the whole schedule, hour by hour, compared
    against what the screen promises. It is written as a literal
    picture on purpose: a reader can check it against the sentence
    above without running anything.
    """

    WEEKDAYS = (1, 2, 3, 4, 5)

    def _schedule(self) -> list[dict[str, int]]:
        return [_period(day, "09:00", "21:00") for day in self.WEEKDAYS]

    def test_the_week_matches_the_screen(self) -> None:
        # Monday 2026-07-13 .. Sunday 2026-07-19. "#" = delivered.
        # hours 0..23; 09:00-21:00 allowed -> 9 dots, 12 hashes, 3 dots
        expected = [
            ".........############...",  # Mon
            ".........############...",  # Tue
            ".........############...",  # Wed
            ".........############...",  # Thu
            ".........############...",  # Fri
            "........................",  # Sat
            "........................",  # Sun
        ]
        assert all(len(row) == 24 for row in expected)
        windows = self._schedule()

        actual = [
            "".join(
                "#"
                if is_delivery_allowed(
                    _utc(13 + offset, hour), tz=UTC_TZ, windows=windows
                )
                else "."
                for hour in range(24)
            )
            for offset in range(7)
        ]
        assert actual == expected

    def test_monday_morning_is_silent(self) -> None:
        """The end that was broken, as its own assertion: under the old
        model this hour was DELIVERED, because the window covering it
        started on Sunday and Sunday was not in the user's set."""
        assert not is_delivery_allowed(
            _utc(13, 3), tz=UTC_TZ, windows=self._schedule()
        )

    def test_tuesday_morning_is_silent_too(self) -> None:
        """The other end, which every attempted fix inside the product
        broke in exchange: shifting the days silenced Monday morning
        and started delivering Monday night instead."""
        assert not is_delivery_allowed(
            _utc(14, 3), tz=UTC_TZ, windows=self._schedule()
        )

    def test_asking_for_two_days_does_not_deliver_on_a_third(self) -> None:
        """NON-INJECTIVITY, gone. The product's workaround for the
        start-day semantics was to send D union (D-1), and {Mon, Wed}
        then became {Sun, Mon, Tue, Wed} -- the same value {Mon, Tue,
        Wed} produces, so a user who asked for Monday and Wednesday
        got Tuesday as well. Each period now carries its own day and
        nothing collapses.
        """
        windows = [_period(1, "09:00", "21:00"), _period(3, "09:00", "21:00")]
        assert is_delivery_allowed(_utc(13, 12), tz=UTC_TZ, windows=windows)
        assert not is_delivery_allowed(
            _utc(14, 12), tz=UTC_TZ, windows=windows
        )
        assert is_delivery_allowed(_utc(15, 12), tz=UTC_TZ, windows=windows)
