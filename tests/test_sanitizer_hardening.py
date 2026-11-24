# =============================================================================
# The sanitizer is linear on any text and never fails on its own; the
# forms of a secret still missed are caught; the stdlib line carries no
# control character of its data; the profile refuses what the send would
# =============================================================================
#
#   - TIME: sanitize_text and is_secret_name on runs that made the
#     key's prefix quadratic ("a-"*n -- 22 s on 80 000 characters before
#     REV-6) and their neighbours. Timed by _assert_linear, the one
#     measure of the file (TEST-2): 8 times the input takes at most 22
#     times the time, from 2 500 to 20 000 characters and from 10 000 to
#     80 000 -- linear ~8, quadratic ~64, so noise has to move a ratio
#     2.5 times (measured) to turn either. The measure by doublings it
#     replaced (2n at most x3: linear ~2, quadratic ~4) had a margin of
#     1.4, and its reds were the machine's: TEST-1 (a run of a few ms),
#     FIX W6-2b #1 (the sides in different rounds), CI #371 (3.22 on a
#     linear pattern). A ratio, not a number of seconds: the machine does
#     not decide the test, and the two sides of it are measured in the
#     same rounds. The figures and the reasoning: its docstring.
#   - NEVER FAILS: a repr or a str that raises, a value nested 2000 deep
#     -> a line with <unreadable TYPE>, nothing of the value.
#   - FORMS: the roots cookie, session_id, passphrase, pgpassword,
#     secret_access_key; a (name, value) pair in a field; a DSN password
#     with both "@" and "/" -- each with its pair that is no secret.
#   - STDLIB: a line break or an escape in the message or its arguments
#     is written as its escape; the traceback stays as many lines as it
#     is; uvicorn's access record, whose format unpacks record.args by
#     position, still lays its message out. Read in the line's `msg`:
#     since W3-2 the formatter writes the machine's line (protocol 9.4),
#     and the text it wrote before is the message inside it.
#   - PROFILE: a duration of 5000 digits is a ProfileError;
#     <a href='{params}'> is refused, as Telegram refuses the repr's
#     raw quotes; <a href="{params}"> and <a href='{url}'> load.
#
# MUTATIONS these tests were written against (each turns the test
# named red):
#   H1  the prefix as SAN-1 wrote it                -> TestLinearTime,
#                                                      TestEveryPattern
#   H2  no stand-in when repr raises                -> TestNeverFails
#   H3  no depth limit                              -> TestNeverFails
#   H4  exception_text without its try              -> TestNeverFails
#   H5  the REV-6 roots gone                        -> TestForms
#   H6  a (name, value) pair not looked at          -> TestForms
#   H7  userinfo lazy, as REV-4b wrote it           -> TestForms
#   H8  the stdlib message not escaped              -> TestStdlibLine
#   H9  the traceback escaped too                   -> TestStdlibLine
#   H10 int() before the length                     -> TestProfile
#   H11 params standing as any field                -> TestProfile
# REV-9 -- every pattern linear on its own form; a Cookie header whole; a
# record's message or argument that is not a string escaped by its text:
#   N1  _URL_USERINFO_RE as REV-6 wrote it          -> TestEveryPattern
#   N2  the first "@" instead of the last           -> TestEveryPattern
#   N3  no Cookie header pass                       -> TestCookieHeader
#   N4  the Cookie header to its first ";"          -> TestCookieHeader
#   N5  an object argument not wrapped              -> TestNotAString
#   N6  numbers wrapped too                         -> TestNotAString
#   N7  a message that is not a string not escaped  -> TestNotAString
#   N8  a pattern with no inputs                    -> TestEveryPattern
# TEST-1 -- one measure of time in the file:
#   T1  an O(n^2) step in is_secret_name alone      -> TestLinearTime
# W6-2b -- a DSN's password with "?" or "#" in it:
#   Q1  the rest of the URL cut at "?" and "#" again (REV-6's
#       expression) -> TestQueryMarksInAPassword
# FIX W6-2b #1 -- the measure's sides in the same rounds (TEST-2: 8n):
#   F1  every run of n, then every run of 8n, again -> TestTheMeasure
#   F2  no bound on the ratio                       -> TestTheMeasure
# TEST-2 -- the measure with a margin over 2 (N1 and H1 above, put in
# _PATTERNS, still red):
#   B1  the bound back at 3                         -> TestEveryPattern,
#                                                      TestLinearTime
# W6-2c -- a port with a path behind it is not a password:
#   G1  no port guard (W6-2b's expression)          -> TestAPortIsNotAPassword
#   G2  a guard wider than a port, (?!\w+[/?#])     -> TestAPortIsNotAPassword,
#                                                      TestQueryMarksInAPassword
#   G3  the guard in _userinfo_redacted, after the
#       word is taken                               -> TestAPortIsNotAPassword
#   G4  any number of digits, \d+                   -> TestAPortIsNotAPassword
# =============================================================================

import decimal
import functools
import io
import json
import logging
import numbers
import re
import shutil
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import structlog
import yaml

import app.core.sanitize as sanitize_module
from app.core.exceptions import ProfileError
from app.core.logging import SanitizingFormatter, processors
from app.core.sanitize import exception_text, is_secret_name, sanitize_text
from app.profile.loader import FileProfileSource, load_profile

_SECRET = "zq" + "REV6" + "s9"
_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "profile"


class TestLinearTime:
    """Item 1: the sanitizer on the key's own runs, and is_secret_name --
    its fullmatch starts at position 0 alone and must reach the end, not
    the path of _SECRET_NAME_RE.sub that TestEveryPattern times (H1 is
    quadratic in that sub and linear in this fullmatch). H1, T1."""

    @pytest.mark.parametrize("unit", [
        "a-", "-", "a", "token-", "a_b-", "x" * 50 + "-", "a- b_", "password=x ",
    ])
    @pytest.mark.parametrize("function", [sanitize_text, is_secret_name])
    def test_eight_times_the_input_at_most_22_times_the_time(
        self, unit: str, function: Callable[[str], object],
    ) -> None:
        _assert_linear(function, unit)


def _logged(**fields: Any) -> dict[str, Any]:
    stream = io.StringIO()
    logger = structlog.wrap_logger(
        structlog.PrintLogger(file=stream),
        processors=processors(structlog.processors.JSONRenderer()),
    )
    logger.info("rev6_probe", **fields)
    parsed: dict[str, Any] = json.loads(stream.getvalue())
    return parsed


class _BadRepr:
    def __repr__(self) -> str:
        raise RuntimeError(_SECRET)


class _BadStrError(Exception):
    def __str__(self) -> str:
        raise RuntimeError(_SECRET)


class TestNeverFails:
    """Item 2. H2-H4."""

    def test_a_repr_that_raises(self) -> None:
        line = _logged(value=_BadRepr(), nested={"a": [_BadRepr()]})
        assert line["value"] == "<unreadable _BadRepr>"
        assert line["nested"] == {"a": ["<unreadable _BadRepr>"]}

    def test_a_value_nested_past_the_walk(self) -> None:
        deep: list[Any] = [_SECRET]
        for _ in range(2000):
            deep = [deep]
        line = _logged(value=deep)
        text = json.dumps(line["value"])
        assert "<unreadable list>" in text
        assert _SECRET not in text

    def test_the_pair_64_levels_are_walked(self) -> None:
        deep: Any = f"token={_SECRET}"
        for _ in range(63):
            deep = [deep]
        line = json.dumps(_logged(value=deep)["value"])
        assert "token=[redacted]" in line and "unreadable" not in line

    def test_an_exception_whose_str_raises(self) -> None:
        assert exception_text(_BadStrError()) == "<unreadable _BadStrError>"
        line = _logged(error=exception_text(_BadStrError()))
        assert line["error"] == "<unreadable _BadStrError>"


class TestForms:
    """Item 3. H5-H7."""

    @pytest.mark.parametrize("name", [
        "cookie", "Set-Cookie", "session_id", "sessionid", "passphrase",
        "PGPASSWORD", "AWS_SECRET_ACCESS_KEY", "secret-access-key",
    ])
    def test_a_name(self, name: str) -> None:
        assert is_secret_name(name)
        assert _logged(**{name: _SECRET})[name] == "[redacted]"
        assert sanitize_text(f"{name}={_SECRET}") == f"{name}=[redacted]"

    @pytest.mark.parametrize("name", [
        "cookie_policy", "cookies_enabled", "session", "session_timeout",
        "access_key_id", "passphrase_hint", "public_key", "pg_host",
        "idempotency_key",
    ])
    def test_the_pair_a_name_that_is_not(self, name: str) -> None:
        assert not is_secret_name(name)
        assert sanitize_text(f"{name}=v") == f"{name}=v"

    def test_a_pair_in_a_field(self) -> None:
        line = _logged(headers=[("x-api-key", _SECRET), ("Accept", "*/*")],
                       pair=("passwords_match", True))
        assert line["headers"] == [["x-api-key", "[redacted]"], ["Accept", "*/*"]]
        assert line["pair"] == ["passwords_match", True]

    @pytest.mark.parametrize(("text", "cleaned"), [
        (f"postgresql://comms:p@{_SECRET}/w@db/comms",
         "postgresql://comms:[redacted]@db/comms"),
        (f"postgresql://comms:a/b+{_SECRET}==@db/comms",
         "postgresql://comms:[redacted]@db/comms"),
        (f"https://u:{_SECRET}@host/path?e=a@b.c",
         "https://u:[redacted]@b.c"),
        (f"redis://:p@{_SECRET}@h:6379/0", "redis://:[redacted]@h:6379/0"),
    ])
    def test_a_dsn(self, text: str, cleaned: str) -> None:
        """The URL with an @ in its query was held cleaned to its host
        ("...@host/path?e=a@b.c"): right while the password stopped at
        "?" -- and so left a password with "?" or "#" whole. W6-2b runs
        the password to the last @ before a space; the query's @ takes
        the authority with it (app/core/sanitize.py, pattern 4's price),
        the secret is gone either way."""
        assert sanitize_text(text) == cleaned
        assert sanitize_text(cleaned) == cleaned


def _msg(line: str) -> str:
    """The message of a line SanitizingFormatter wrote (W3-2: a JSON
    object of protocol 9.4; its `msg` is the text the line was before)."""
    message: str = json.loads(line)["msg"]
    return message


def _record(
    msg: str, args: Any = None, exc: BaseException | None = None,
) -> logging.LogRecord:
    exc_info = (type(exc), exc, exc.__traceback__) if exc is not None else None
    return logging.LogRecord("x", logging.INFO, "p", 1, msg, args, exc_info)


class TestStdlibLine:
    """Item 3, the stdlib formatter. H8, H9."""

    def test_the_message_and_its_arguments(self) -> None:
        line = SanitizingFormatter().format(_record(
            "provider said %s\nINFO forged line", ("a\x1b[31mb\u2028c",)))
        assert "\n" not in line and "\x1b" not in line and "\u2028" not in line
        assert _msg(line) == (
            "provider said a\\x1b[31mb\\u2028c\\nINFO forged line"
        )

    def test_the_traceback_stays_lines(self) -> None:
        try:
            raise ValueError("boom")
        except ValueError as exc:
            line = SanitizingFormatter().format(_record("failed\nx", exc=exc))
        first, *rest = _msg(line).split("\n")
        assert first == "failed\\nx"
        assert rest and rest[0].startswith("Traceback")

    def test_uvicorn_s_access_line(self) -> None:
        """uvicorn's access record is laid out by its own format, which
        unpacks record.args by position. Until W3-2 this went through
        uvicorn's AccessFormatter, kept inside SanitizingFormatter, and
        the line read '... 200 OK'; the machine's line replaced that
        formatter -- the message is the record's own, the escape kept."""
        line = SanitizingFormatter().format(_record(
            '%s - "%s %s HTTP/%s" %d',
            ("1.2.3.4:5", "GET", "/x?a=1\nFAKE", "1.1", 200)))
        assert _msg(line) == '1.2.3.4:5 - "GET /x?a=1\\nFAKE HTTP/1.1" 200'

    def test_the_original_record_is_left_alone(self) -> None:
        record = _record("a\n%s", ("c\nd",))
        SanitizingFormatter().format(record)
        assert record.msg == "a\n%s" and record.args == ("c\nd",)


def _profile(tmp_path: Path, *, types: dict[str, Any] | None = None,
             body: str | None = None) -> Path:
    root = tmp_path / "profile"
    shutil.copytree(_FIXTURE, root)
    if types:
        path = root / "types.yaml"
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        for key, fields in types.items():
            doc["types"][key].update(fields)
        path.write_text(yaml.safe_dump(doc), encoding="utf-8")
    if body is not None:
        for locale in ("en", "ru"):
            path = root / "templates" / f"{locale}.yaml"
            doc = yaml.safe_load(path.read_text(encoding="utf-8"))
            doc["unit_event"] = {"telegram": {"title": "{title}", "body": body}}
            path.write_text(yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8")
    return root


class TestProfile:
    """Item 4. H10, H11."""

    def test_a_duration_of_5000_digits(self, tmp_path: Path) -> None:
        assert sys.get_int_max_str_digits() < 5000  # the instrument
        root = _profile(
            tmp_path, types={"unit_expiring": {"expires_after": "9" * 5000 + "d"}})
        with pytest.raises(ProfileError, match="longer than 3650d"):
            load_profile(FileProfileSource(root))

    def test_the_pair_90d(self, tmp_path: Path) -> None:
        load_profile(FileProfileSource(_profile(
            tmp_path, types={"unit_expiring": {"expires_after": "90d"}})))

    def test_params_in_single_quotes(self, tmp_path: Path) -> None:
        root = _profile(tmp_path, body="<a href='{params}'>open</a> {body}")
        with pytest.raises(ProfileError, match="empty attribute name"):
            load_profile(FileProfileSource(root))

    @pytest.mark.parametrize("body", [
        '<a href="{params}">open</a> {body}',
        "<a href='{body}'>open</a>",
        "{params} {body}",
    ])
    def test_the_pair_loads(self, tmp_path: Path, body: str) -> None:
        load_profile(FileProfileSource(_profile(tmp_path, body=body)))


# =============================================================================
# REV-9
# =============================================================================

# The inputs that break each pattern's own form: its leading characters
# repeated, and its neighbours. Keyed by the pattern's name in
# app/core/sanitize.py; test_every_pattern_has_inputs holds the keys equal
# to the patterns the module compiles, so a new pattern without inputs
# turns it red.
_UNITS: dict[str, list[str]] = {
    "_AUTH_HEADER_RE": ["authorization: ", "authorization ", "Authorization:="],
    "_BEARER_RE": ["Bearer ", "bearer", "bearer\t"],
    "_COOKIE_HEADER_RE": ["cookie: ", "set-cookie:", "Cookie:a=b; "],
    "_KEYVAL_RE": ["a-", "token-", "token=", "token: ", 'password="', "a_b-"],
    "_LLM_KEY_RE": ["sk-", "sk-a!"],
    "_LONG_HEX_RE": ["a" * 47 + "-", "a"],
    "_MAILGUN_KEY_RE": ["key-", "a" * 32 + "-"],
    "_QUERY_KEYVAL_RE": ["?token=", "&a=", "?a-"],
    "_SECRET_NAME_RE": ["a-", "token-", "a_b-"],
    "_TELEGRAM_TOKEN_RE": ["bot1234567:", "1234567:"],
    "_URL_USERINFO_RE": [
        "://a:", "://", ":@", "://a:@b", "://a:x@",
        # W6-2c: the port guard fires on every unit, and gives up after
        # five digits on every unit.
        "://a:1/", "://a:123456/",
    ],
}
_PATTERNS = {
    name: value for name, value in vars(sanitize_module).items()
    if isinstance(value, re.Pattern)
}
_MIXED = "".join(unit for units in _UNITS.values() for unit in units)


def _fastest(work: Callable[[], object], repeats: int = 1) -> float:
    """The least of 5 runs, each `repeats` calls: the run the machine
    least disturbed."""
    runs = []
    for _ in range(5):
        started = time.perf_counter()
        for _ in range(repeats):
            work()
        runs.append(time.perf_counter() - started)
    return min(runs)


def _run(work: Callable[[str], object], text: str, repeats: int) -> float:
    """One run: `repeats` calls of `work` on `text`, in seconds."""
    started = time.perf_counter()
    for _ in range(repeats):
        work(text)
    return time.perf_counter() - started


# The measure: each pair (n, 8n) in order; a run of n takes 20 ms.
_PAIRS = ((2_500, 20_000), (10_000, 80_000))
_RUN = 0.02
_BOUND = 22


def _repeats(fastest: float) -> int:
    """The calls of a run that takes 20 ms, one call `fastest` seconds at
    the least: the measure's calibration, and _Sleeps's reckoning of it."""
    return max(1, int(_RUN / max(fastest, 1e-7)) + 1)


def _assert_linear(work: Callable[[str], object], unit: str) -> None:
    """8 times the input takes at most 22 times the time (TEST-2): from
    2 500 to 20 000 characters, then from 10 000 to 80 000. Linear work
    reads ~8, quadratic ~64.

    HOW: each pair is 5 rounds of a run of n and a run of 8n, each run so
    many calls that a run of n takes 20 ms -- a ratio of two figures of a
    few milliseconds is the machine's, not the pattern's (TEST-1: 2.4 ms
    -> 9.7 ms on a slow CI, no defect). The ratio is the least run of 8n
    over the least of n.

    THE MARGIN. With a factor k between the sizes, linear reads k and
    quadratic k^2; the bound that leaves both the same margin is k^1.5,
    and the margin is sqrt(k). A margin of 2 needs k >= 4, and at 4 it is
    2 on paper alone (measured 4.23 linear, ~15 quadratic: 1.9). k = 8
    and the bound 22 leave 2.75 to linear and 2.91 to quadratic on paper;
    measured on every unit of this file, through each pattern and the
    whole sanitizer (TEST-2, the worst of each):

        pair               linear  quadratic             margin
        2 500 -> 20 000     8.66   55.2 (N1, through     2.54 / 2.51
                                   sanitize_text)
        10 000 -> 80 000    8.52   64.3 (N1)             2.58 / 2.92

    so noise must move a ratio 2.5 times, up or down, to turn a linear
    pattern red or a quadratic one green. A slope fitted over more sizes
    was rejected: the same span gives the same information at its ends,
    each size more costs time, and its margin hangs on the points the
    noise lands on -- no one figure to hold.

    THE ORDER OF THE PAIRS: the small one first, judged as it comes, so a
    quadratic run fails before 80 000 costs minutes (H1: ~70 s a call at
    80 000; red in ~22 s at the small pair). The large one keeps the
    reach to 80 000: a quadratic term small beside the linear one shows
    on long input alone.

    THE TWO SIDES OF A RATIO IN THE SAME ROUNDS (FIX W6-2b #1). Measured
    one size after the other -- every run of n, then every run of 2n -- a
    slowdown of the machine that lasted the second's runs and not the
    first's multiplied the ratio: a linear pattern read 3.09 and 3.13 on
    CI (x1.55 for a second). A slowdown longer than a round now falls on
    both sides; a shorter one is a run the least of 5 leaves out.

    WHY THE MEASURE BY DOUBLINGS WENT: 2n at most x3 sat between linear
    ~2 and quadratic ~4 with a margin of 1.5 and 1.33 (1.43 / 1.26
    measured). TEST-1 and FIX W6-2b #1 mended how it measured, and the
    stand still went red on noise: CI #371, 3.22 on _URL_USERINFO_RE,
    which doubles at 1.95-2.03. A lasting slowdown that starts inside
    round 1, between its two runs, still multiplies the ratio -- now it
    has to be x2.75 to turn a linear work red, not x1.5."""
    for n, eight_n in _PAIRS:
        small = unit * (n // len(unit) + 1)
        large = unit * (eight_n // len(unit) + 1)
        repeats = _repeats(_fastest(functools.partial(work, small)))
        runs_small: list[float] = []
        runs_large: list[float] = []
        for _ in range(5):
            runs_small.append(_run(work, small, repeats))
            runs_large.append(_run(work, large, repeats))
        earlier, took = min(runs_small), min(runs_large)
        assert took / max(earlier, 1e-6) <= _BOUND, (
            unit[:12], eight_n, earlier, took)


class _Sleeps:
    """A work whose time is set, not measured: `per_char` seconds of sleep
    for each character (or its square, `power` 2) -- 2.5 ms a call at
    2 500 characters, clear of the sleep's own overhead.

    `lasting`: slower by 4 for good from a call reckoned from the
    measure's own calibration, not from a fixed count -- a fixed count
    holds for a window of r, the calls of a run, and r hangs on how long
    the runner's sleep takes (TEST-2). The first 5 calls are the
    measure's calibration at 2 500; the work times them and takes, by
    _repeats, the r the measure takes. The slowdown starts at call
    5 + 3r + 1. In the same rounds that is the run of 20 000 of round 2:
    round 1 whole and the run of 2 500 of round 2 keep each side a fast
    run, the ratio reads 8. One size after the other it is inside the
    runs of 2 500 (after the first, 5 + r, before the last, 5 + 5r):
    every run of 20 000 slowed, the ratio about 32, over 22. Both hold
    with r reckoned one off the measure's, for r > 2. x4, not the x1.55
    seen on CI: under the margin of 2.5 a slowdown of x2 no longer tells
    the two orders apart."""

    def __init__(
        self, *, power: int = 1, lasting: bool = False, per_char: float = 1e-6,
    ) -> None:
        self.power, self.lasting, self.per_char = power, lasting, per_char
        self.calls = 0
        self.calibration: list[float] = []
        self.repeats: int | None = None
        self.slow_from: int | None = None

    def __call__(self, text: str) -> None:
        self.calls += 1
        slower = 4.0 if self.slow_from and self.calls >= self.slow_from else 1.0
        started = time.perf_counter()
        time.sleep(self.per_char * len(text) ** self.power / 10_000 ** (
            self.power - 1) * slower)
        if self.lasting and self.calls <= 5:
            self.calibration.append(time.perf_counter() - started)
            if self.calls == 5:
                self.repeats = _repeats(min(self.calibration))
                self.slow_from = 5 + 3 * self.repeats + 1


class TestTheMeasure:
    """FIX W6-2b #1, TEST-2: _assert_linear on works whose time is known.
    F1, F2."""

    def test_a_slowdown_that_lasts_is_not_the_pattern_s(self) -> None:
        """Linear, slowed by 4 for good from round 2 of the first pair on
        (_Sleeps, `lasting`): green in the same rounds, red one size after
        the other (F1). The pair: the slowdown came, and inside the first
        pair's 5 + 10r calls."""
        work = _Sleeps(lasting=True)
        _assert_linear(work, "a")
        assert work.repeats is not None and work.slow_from is not None
        assert work.slow_from <= 5 + 10 * work.repeats < work.calls

    def test_the_pair_the_guard_sees_a_square(self) -> None:
        """Quadratic: 2 500 -> 20 000 reads ~64, over 22 (F2)."""
        with pytest.raises(AssertionError):
            _assert_linear(_Sleeps(power=2), "a")


class TestEveryPattern:
    """Item 1: every pattern the module compiles, on the inputs that
    break its own form, through the pattern alone and through the whole
    sanitizer. N1, N2, N8."""

    def test_every_pattern_has_inputs(self) -> None:
        assert set(_UNITS) == set(_PATTERNS)

    @pytest.mark.parametrize(("name", "unit"), [
        (name, unit) for name, units in sorted(_UNITS.items()) for unit in units
    ] + [("_mixed", _MIXED)])
    def test_linear(self, name: str, unit: str) -> None:
        if name in _PATTERNS:
            pattern = _PATTERNS[name]
            _assert_linear(lambda text: pattern.sub("", text), unit)
        _assert_linear(sanitize_text, unit)

    @pytest.mark.parametrize("unit", ["://a:", "://"])
    def test_the_reviewer_s_inputs_under_a_second(self, unit: str) -> None:
        text = unit * (100_000 // len(unit))
        assert _fastest(lambda: sanitize_text(text)) < 1

    @pytest.mark.parametrize(("text", "cleaned"), [
        (f"postgresql://comms:p@{_SECRET}/w@db/comms",
         "postgresql://comms:[redacted]@db/comms"),
        (f"https://u:{_SECRET}@host/path?e=a@b.c",
         "https://u:[redacted]@b.c"),
        ("redis://:@h:6379/0 down", "redis://:[redacted]@h:6379/0 down"),
        ("https://host:443/path", "https://host:443/path"),
    ])
    def test_the_last_at_as_before(self, text: str, cleaned: str) -> None:
        """The query's @ case was held cleaned to its host: right while
        the password stopped at "?" (REV-9); W6-2b took that stop away,
        so the last @ before a space is the query's (test_a_dsn says
        why)."""
        assert sanitize_text(text) == cleaned
        assert sanitize_text(cleaned) == cleaned


class TestQueryMarksInAPassword:
    """W6-2b, the review's B6: a password holds "?" and "#" as any other
    character -- 3.14.0 cleaned them, REV-6 stopped the rest of the URL
    at them and left such a password whole. Q1."""

    @pytest.mark.parametrize("password", [
        "Zx9?Qw#7", "a#b", "?a", "a@b?c", "a?b#c/d@e",
    ])
    @pytest.mark.parametrize(("head", "tail"), [
        ("postgresql+asyncpg://comms:", "@db/comms"),
        ("redis://:", "@comms-redis:6379/0"),
        ("x postgresql://comms:", "@db/comms?ssl=1 next"),
    ])
    def test_the_password_goes_whole(
        self, head: str, password: str, tail: str,
    ) -> None:
        text = head + password + tail
        cleaned = sanitize_text(text)
        assert cleaned == head + "[redacted]" + tail
        assert sanitize_text(cleaned) == cleaned

    def test_the_reviewer_s_dsn(self) -> None:
        # Joined at runtime: a literal DSN with a password reads as a
        # credential to a push scanner.
        dsn = "postgresql+asyncpg://comms:" + "Zx9?" + "Qw#7" + "@db/comms"
        assert sanitize_text(dsn) == "postgresql+asyncpg://comms:[redacted]@db/comms"

    @pytest.mark.parametrize("text", [
        "https://host:443/a?b#c", "see ://a:b?c#d", "https://host/p?q=a:b",
    ])
    def test_the_pair_no_at_no_userinfo(self, text: str) -> None:
        """No @ after the colon: no userinfo, the text as it is."""
        assert sanitize_text(text) == text


class TestAPortIsNotAPassword:
    """W6-2c, the review's N2: after "host:", 1 to 5 digits and "/", "?" or
    "#" are a port and what follows it -- the address as it is, though an
    @ comes later in the word; every password W6-2b hid is still hidden.
    G1-G4."""

    @pytest.mark.parametrize("text", [
        "http://storage:9000/audio/a.ogg?user=john@example.com",
        "GET http://comms-app:8000/api/v1/recipients?email=a@b.c",
        "https://api.telegram.org:443/bot123/sendMessage?chat_id=@channel",
        "http://h:9000#to@b", "http://h:1?a@b",
    ])
    def test_the_address_as_it_is(self, text: str) -> None:
        assert sanitize_text(text) == text

    @pytest.mark.parametrize(("head", "password", "tail"), [
        ("postgresql+asyncpg://comms:", "Zx9?" + "Qw#7", "@db/comms"),
        ("postgresql://comms:", "p@ss/" + _SECRET, "@db:5432"),
        ("redis://:", "4d0c" * 12, "@comms-redis:6379/0"),
        # A port with no path is the userinfo's reading: user "h".
        ("http://h:", "9000", "@x"),
        # Six digits are no port.
        ("postgresql://u:", "123456/" + _SECRET, "@db"),
        # Digits alone end at their "@".
        ("postgresql://u:", "1234", "@db/x?y=1"),
    ])
    def test_the_pair_a_password_is_hidden(
        self, head: str, password: str, tail: str,
    ) -> None:
        cleaned = sanitize_text(head + password + tail)
        assert cleaned == head + "[redacted]" + tail
        assert sanitize_text(cleaned) == cleaned

    def test_a_url_further_in_the_word_is_looked_at(self) -> None:
        """The guard ends the match at the port, it does not take the word:
        a URL in the query of a guarded one is still cleaned."""
        text = f"http://h:9000/cb?next=https://u:{_SECRET}@h2/x"
        cleaned = sanitize_text(text)
        assert cleaned == "http://h:9000/cb?next=https://u:[redacted]@h2/x"
        assert _SECRET not in cleaned

    def test_the_price_a_password_shaped_as_a_port(self) -> None:
        """A password that starts with 1 to 5 digits and then "/", "?" or
        "#" reads as a port: left whole, the price app/core/sanitize.py
        names at pattern 4. The deploy machine mints none such
        (token_urlsafe: [A-Za-z0-9_-])."""
        text = "postgresql://u:12/" + "ab@db"
        assert sanitize_text(text) == text


class TestCookieHeader:
    """Item 2: a Cookie header's every cookie. N3, N4."""

    @pytest.mark.parametrize(("text", "cleaned"), [
        (f"Cookie: a={_SECRET}; c={_SECRET}", "Cookie: [redacted]"),
        (f"Set-Cookie: s={_SECRET}; Path=/; HttpOnly\nnext",
         "Set-Cookie: [redacted]\nnext"),
        (f"cookie:a={_SECRET};b={_SECRET}", "cookie:[redacted]"),
    ])
    def test_the_whole_value(self, text: str, cleaned: str) -> None:
        assert sanitize_text(text) == cleaned
        assert sanitize_text(cleaned) == cleaned

    @pytest.mark.parametrize(("text", "cleaned"), [
        (f"cookie={_SECRET}; b=c", "cookie=[redacted]; b=c"),
        ("cookie_policy: strict", "cookie_policy: strict"),
        ("cookies_enabled: true", "cookies_enabled: true"),
        ("no cookie here", "no cookie here"),
        ("Cookie:   ", "Cookie:   "),
    ])
    def test_the_pair_left_by_its_own_rule(self, text: str, cleaned: str) -> None:
        assert sanitize_text(text) == cleaned


class _ForgedError(Exception):
    pass


class TestNotAString:
    """Item 3: what becomes text inside the formatter is escaped by that
    text. N5-N7."""

    _FORGED = _ForgedError("bad\nINFO forged \x1b[31m")

    def _line(self, msg: Any, args: Any = None) -> str:
        return _msg(SanitizingFormatter().format(_record(msg, args)))

    def test_an_exception_argument(self) -> None:
        assert self._line("failed: %s", (self._FORGED,)) == (
            "failed: bad\\nINFO forged \\x1b[31m")

    def test_its_repr(self) -> None:
        line = self._line("failed: %r", (self._FORGED,))
        assert "\n" not in line and "\x1b" not in line
        assert line.startswith("failed: _ForgedError(")

    def test_a_message_that_is_not_a_string(self) -> None:
        assert self._line(self._FORGED) == "bad\\nINFO forged \\x1b[31m"

    def test_an_argument_whose_str_raises(self) -> None:
        assert self._line("x %s", (_BadRepr(),)) == "x <unreadable _BadRepr>"
        assert self._line("x %s", (_BadStrError(),)) == "x <unreadable _BadStrError>"

    def test_the_pair_numbers_stay_numbers(self) -> None:
        assert isinstance(decimal.Decimal("1.5"), numbers.Number)  # the instrument
        assert self._line("%d %.1f %s %s", (3, 2.5, decimal.Decimal("1.5"), True)) == (
            "3 2.5 1.5 True")

    def test_the_pair_uvicorn_and_the_traceback(self) -> None:
        """uvicorn's status stays a number for %d (W3-2: laid out by the
        record's own format, no longer by uvicorn's AccessFormatter --
        see TestStdlibLine.test_uvicorn_s_access_line)."""
        line = self._line(
            '%s - "%s %s HTTP/%s" %d', ("1.2.3.4:5", "GET", "/x", "1.1", 404))
        assert line == '1.2.3.4:5 - "GET /x HTTP/1.1" 404'
        try:
            raise ValueError("boom")
        except ValueError as exc:
            out = _msg(SanitizingFormatter().format(_record("failed: %s", (exc,), exc)))
        first, *rest = out.split("\n")
        assert first == "failed: boom" and rest[0].startswith("Traceback")

