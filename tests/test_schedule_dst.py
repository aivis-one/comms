# =============================================================================
# TIME-1 item 4 -- the delivery schedule across a change of the clocks
# =============================================================================
#
# On a real zone with transitions (Europe/Berlin -- not UTC, which has
# none; not Moscow, which has had none since 2014), on the 2026 dates:
#   spring 2026-03-29: 02:00 CET -> 03:00 CEST (01:00 UTC), 02:00-03:00 skipped;
#   autumn 2026-10-25: 03:00 CEST -> 02:00 CET (01:00 UTC), 02:00-03:00 twice.
# What is held (app/audience/schedule.py, its header's DST paragraph):
#   - autumn: a period inside the repeated hour opens twice; on the second
#     pass the deferral goes to the SECOND opening -- never into the past
#     (the defect TIME-1 found: wall times compared, the first instant
#     taken, a delivery deferred into the past and re-deferred in a loop);
#   - spring, the accepted rule kept as it is: a period wholly inside the
#     skipped hour is waited for at its start read with fold=0 and then
#     goes to the next week; a period partly inside opens at its start so
#     read (later than the clock allows -- the accepted error bar);
#   - the invariant over every 10 minutes of both days: the answer is None
#     or strictly after `now`, and at that moment the recipient may be
#     reached -- except the accepted spring case.
#
# MUTATIONS these tests were written against (each turns one red):
#   M10 starts compared as wall times again -> the second pass, the
#       invariant
#   M11 no fold=1 candidate -> test_the_second_pass_waits_for_the_second
#   M12 fold=1 taken without the existence check -> test_before_the_jump,
#       the invariant
# =============================================================================

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from app.audience.schedule import is_delivery_allowed, next_delivery_allowed_at

BERLIN = ZoneInfo("Europe/Berlin")
SUNDAY = 7


def _at(day: str, hh_mm: str) -> datetime:
    return datetime.fromisoformat(f"2026-{day}T{hh_mm}:00+00:00")


def _period(start: int, end: int) -> list[dict[str, int]]:
    return [{"day": SUNDAY, "from": start, "to": end}]


def _next(now: datetime, windows: list[dict[str, int]]) -> datetime | None:
    return next_delivery_allowed_at(now, tz=BERLIN, windows=windows)


def _allowed(now: datetime, windows: list[dict[str, int]]) -> bool:
    return is_delivery_allowed(now, tz=BERLIN, windows=windows)


# 02:30-03:00 local: inside the repeated hour of autumn, inside the
# skipped hour of spring.
_INSIDE = _period(150, 180)


class TestAutumnRepeatedHour:
    def test_the_period_opens_twice(self) -> None:
        """00:30-01:00 UTC is the first pass (CEST), 01:30-02:00 UTC the
        second (CET); between them it is closed."""
        assert _allowed(_at("10-25", "00:40"), _INSIDE)
        assert not _allowed(_at("10-25", "01:10"), _INSIDE)
        assert _allowed(_at("10-25", "01:40"), _INSIDE)

    def test_the_first_pass_waits_for_the_first_opening(self) -> None:
        assert _next(_at("10-25", "00:10"), _INSIDE) == _at("10-25", "00:30")

    def test_the_second_pass_waits_for_the_second_opening(self) -> None:
        """02:10 CET, on the second pass: the period opens at 02:30 CET,
        01:30 UTC -- not at 00:30 UTC, an hour and forty minutes ago."""
        now = _at("10-25", "01:10")
        assert _next(now, _INSIDE) == _at("10-25", "01:30")

    def test_after_both_passes_the_next_week(self) -> None:
        assert _next(_at("10-25", "02:00"), _INSIDE) == _at("11-01", "01:30")


class TestSpringSkippedHour:
    def test_wholly_inside_is_waited_for_then_skipped(self) -> None:
        """The accepted rule: 02:00-03:00 does not open on that date. Its
        start read with fold=0 is 03:00 CEST (01:00 UTC); there the
        period has closed, and the search goes to the next Sunday."""
        windows = _period(120, 180)
        first = _next(_at("03-29", "00:30"), windows)
        assert first == _at("03-29", "01:00")
        assert not _allowed(first, windows)
        assert _next(first, windows) == _at("04-05", "00:00")

    def test_before_the_jump(self) -> None:
        """00:30 CET: the start read with the offset AFTER the jump
        would be 00:00 UTC, an instant that is 01:00 CET -- not the
        period's; the search waits for 01:00 UTC."""
        windows = _period(120, 180)
        assert _next(_at("03-28", "23:30"), windows) == _at("03-29", "01:00")

    def test_partly_inside_opens_at_its_start_so_read(self) -> None:
        """02:30-04:00: waited for until 03:30 CEST (01:30 UTC), though at
        03:00 CEST it is already open -- the accepted error bar."""
        windows = _period(150, 240)
        assert _next(_at("03-29", "00:30"), windows) == _at("03-29", "01:30")
        assert _allowed(_at("03-29", "01:00"), windows)


_SHAPES = {
    "inside": _INSIDE,
    "partly_inside": _period(150, 240),
    "wholly_skipped": _period(120, 180),
    "before": _period(60, 90),
    "after": _period(600, 660),
}


# The fold=0 readings of the starts that lie in the skipped hour.
_SKIPPED_READING = {
    "inside": _at("03-29", "01:30"),
    "wholly_skipped": _at("03-29", "01:00"),
}


@pytest.mark.parametrize("day", ["03-29", "10-25"])
@pytest.mark.parametrize("shape", sorted(_SHAPES))
def test_never_into_the_past_and_open_when_due(day: str, shape: str) -> None:
    windows = _SHAPES[shape]
    start = _at(day, "00:00") - timedelta(hours=2)  # local midnight or before
    checked = 0
    for step in range(0, 26 * 60, 10):
        now = start + timedelta(minutes=step)
        answer = _next(now, windows)
        if answer is None:
            assert _allowed(now, windows), now
            continue
        checked += 1
        assert answer > now, (now, answer)
        assert answer.tzinfo is UTC
        if not _allowed(answer, windows):
            # The one accepted exception: a period wholly inside the
            # skipped hour, waited for at its fold=0 reading.
            assert day == "03-29", (now, answer)
            assert answer == _SKIPPED_READING[shape], (now, answer)
    assert checked > 50, "the pair: the walk met deferrals"
