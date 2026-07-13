# =============================================================================
# COMMS Service -- Quiet-hours window math tests (Phase 2 item 6)
# =============================================================================
# Pure-function coverage of app/audience/quiet_hours.py on FIXED
# datetimes (no DB, no clock): same-day and overnight windows, day
# filtering (ISO weekday of window START), boundary semantics
# (inclusive start, exclusive end), timezone conversion, and the
# recipient-level wrapper's None / bad-timezone handling.
#
# Calendar anchors (July 2026):
#   Fri 2026-07-10 (isoweekday 5), Sat 11 (6), Sun 12 (7), Mon 13 (1).
# =============================================================================

from datetime import UTC, datetime, time
from zoneinfo import ZoneInfo

from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.audience.quiet_hours import next_window_open, recipient_quiet_until

UTC_TZ = ZoneInfo("UTC")
BERLIN = ZoneInfo("Europe/Berlin")  # CEST = UTC+2 in July.


def _utc(day: int, hour: int, minute: int = 0) -> datetime:
    """Aware UTC datetime in July 2026."""
    return datetime(2026, 7, day, hour, minute, tzinfo=UTC)


class TestNextWindowOpen:
    """Window math over explicit parameters."""

    def test_inside_same_day_window(self) -> None:
        """Noon inside a 08:00-17:00 Friday window -> opens 17:00."""
        result = next_window_open(
            _utc(10, 12),
            tz=UTC_TZ,
            quiet_from=time(8, 0),
            quiet_to=time(17, 0),
            days=[5],
        )
        assert result == _utc(10, 17)

    def test_outside_same_day_window(self) -> None:
        """After the window on the right day -> None."""
        assert next_window_open(
            _utc(10, 18),
            tz=UTC_TZ,
            quiet_from=time(8, 0),
            quiet_to=time(17, 0),
            days=[5],
        ) is None

    def test_start_boundary_is_inside(self) -> None:
        """Exactly quiet_from is inside the window (inclusive)."""
        assert next_window_open(
            _utc(10, 8),
            tz=UTC_TZ,
            quiet_from=time(8, 0),
            quiet_to=time(17, 0),
            days=[5],
        ) == _utc(10, 17)

    def test_end_boundary_is_outside(self) -> None:
        """Exactly quiet_to is outside the window (exclusive)."""
        assert next_window_open(
            _utc(10, 17),
            tz=UTC_TZ,
            quiet_from=time(8, 0),
            quiet_to=time(17, 0),
            days=[5],
        ) is None

    def test_overnight_before_midnight(self) -> None:
        """23:00 inside a Fri 22:00->08:00 window -> Sat 08:00."""
        assert next_window_open(
            _utc(10, 23),
            tz=UTC_TZ,
            quiet_from=time(22, 0),
            quiet_to=time(8, 0),
            days=[5],
        ) == _utc(11, 8)

    def test_overnight_after_midnight_yesterday_start(self) -> None:
        """Sat 03:00 is covered by the window that STARTED Friday."""
        assert next_window_open(
            _utc(11, 3),
            tz=UTC_TZ,
            quiet_from=time(22, 0),
            quiet_to=time(8, 0),
            days=[5],
        ) == _utc(11, 8)

    def test_overnight_tail_not_covered_by_wrong_start_day(self) -> None:
        """Sat 03:00 with only SATURDAY starts allowed -> None.

        The Saturday window has not started yet; the covering Friday
        window is filtered out by `days`. Start-day semantics.
        """
        assert next_window_open(
            _utc(11, 3),
            tz=UTC_TZ,
            quiet_from=time(22, 0),
            quiet_to=time(8, 0),
            days=[6],
        ) is None

    def test_day_filter_excludes_other_weekdays(self) -> None:
        """Right time of day, wrong weekday -> None."""
        assert next_window_open(
            _utc(11, 12),  # Saturday noon
            tz=UTC_TZ,
            quiet_from=time(8, 0),
            quiet_to=time(17, 0),
            days=[5],  # Fridays only
        ) is None

    def test_timezone_conversion(self) -> None:
        """21:00 UTC Friday = 23:00 Berlin -> inside the Berlin-local
        22:00->08:00 window; opens Sat 08:00 Berlin = 06:00 UTC."""
        assert next_window_open(
            _utc(10, 21),
            tz=BERLIN,
            quiet_from=time(22, 0),
            quiet_to=time(8, 0),
            days=[5],
        ) == _utc(11, 6)

    def test_timezone_shifts_weekday(self) -> None:
        """23:30 UTC Friday is ALREADY Saturday in Berlin: a
        Saturday-start Berlin window covers it."""
        assert next_window_open(
            _utc(10, 23, 30),  # 01:30 Saturday in Berlin
            tz=BERLIN,
            quiet_from=time(1, 0),
            quiet_to=time(6, 0),
            days=[6],
        ) == _utc(11, 4)  # 06:00 Berlin


class TestRecipientQuietUntil:
    """Recipient-level wrapper: config completeness + tz fallback."""

    def _recipient(self, **overrides: object) -> Recipient:
        """Transient recipient (no DB) with a Friday-noon window."""
        defaults: dict[str, object] = {
            "timezone": "UTC",
            "quiet_from": time(8, 0),
            "quiet_to": time(17, 0),
            "quiet_days": [5],
        }
        defaults.update(overrides)
        return Recipient(**defaults)

    def test_configured_recipient_inside_window(self) -> None:
        """Full config + inside window -> the window end."""
        recipient = self._recipient()
        assert recipient_quiet_until(recipient, _utc(10, 12)) == _utc(10, 17)

    def test_unconfigured_fields_mean_no_window(self) -> None:
        """Any missing piece of the window config -> None."""
        for overrides in (
            {"quiet_from": None},
            {"quiet_to": None},
            {"quiet_days": None},
            {"quiet_days": []},
        ):
            recipient = self._recipient(**overrides)
            assert recipient_quiet_until(recipient, _utc(10, 12)) is None

    def test_missing_timezone_falls_back_to_default(self) -> None:
        """timezone=None uses settings.default_timezone (UTC here)."""
        recipient = self._recipient(timezone=None)
        assert recipient_quiet_until(recipient, _utc(10, 12)) == _utc(10, 17)

    def test_bad_timezone_falls_back_with_warning(self) -> None:
        """A corrupt stored timezone logs and falls back, never raises."""
        recipient = self._recipient(timezone="Not/AZone")
        with capture_logs() as logs:
            result = recipient_quiet_until(recipient, _utc(10, 12))
        assert result == _utc(10, 17)
        assert any(
            log["event"] == "invalid_recipient_timezone" for log in logs
        )
