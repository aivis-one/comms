# =============================================================================
# The sanitizer is linear on any text and never fails on its own; the
# forms of a secret still missed are caught; the stdlib line carries no
# control character of its data; the profile refuses what the send would
# =============================================================================
#
#   - TIME: the median of 5 runs, for sanitize_text and is_secret_name,
#     on runs that made the key's prefix quadratic ("a-"*n -- 22 s on
#     80 000 characters before REV-6) and their neighbours; each doubling
#     of n from 10 000 to 80 000 at most triples the time (linear ~2,
#     quadratic ~4). A ratio, not a number of seconds: the machine does
#     not decide the test.
#   - NEVER FAILS: a repr or a str that raises, a value nested 2000 deep
#     -> a line with <unreadable TYPE>, nothing of the value.
#   - FORMS: the roots cookie, session_id, passphrase, pgpassword,
#     secret_access_key; a (name, value) pair in a field; a DSN password
#     with both "@" and "/" -- each with its pair that is no secret.
#   - STDLIB: a line break or an escape in the message or its arguments
#     is written as its escape; the traceback stays as many lines as it
#     is; uvicorn's access formatter, which unpacks record.args by
#     position, still lays its line out.
#   - PROFILE: a duration of 5000 digits is a ProfileError;
#     <a href='{params}'> is refused, as Telegram refuses the repr's
#     raw quotes; <a href="{params}"> and <a href='{url}'> load.
#
# MUTATIONS these tests were written against (each turns one red):
#   H1  the prefix as SAN-1 wrote it                -> TestLinearTime
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
# =============================================================================

import io
import json
import logging
import shutil
import statistics
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import structlog
import yaml
from uvicorn.logging import AccessFormatter

from app.core.exceptions import ProfileError
from app.core.logging import SanitizingFormatter, processors
from app.core.sanitize import exception_text, is_secret_name, sanitize_text
from app.profile.loader import FileProfileSource, load_profile

_SECRET = "zq" + "REV6" + "s9"
_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "profile"


def _median_seconds(work: Callable[[], object]) -> float:
    runs = []
    for _ in range(5):
        started = time.perf_counter()
        work()
        runs.append(time.perf_counter() - started)
    return statistics.median(runs)


class TestLinearTime:
    """Item 1. H1."""

    @pytest.mark.parametrize("unit", [
        "a-", "-", "a", "token-", "a_b-", "x" * 50 + "-", "a- b_", "password=x ",
    ])
    @pytest.mark.parametrize("function", [sanitize_text, is_secret_name])
    def test_each_doubling_at_most_triples(
        self, unit: str, function: Callable[[str], object],
    ) -> None:
        # Each doubling is judged as it comes: a quadratic run fails on
        # the first, before the next sizes cost minutes.
        earlier = None
        for n in (10_000, 20_000, 40_000, 80_000):
            took = _median_seconds(lambda n=n: function(unit * (n // len(unit))))
            if earlier is not None:
                assert took / max(earlier, 1e-6) <= 3, (unit, n, earlier, took)
            earlier = took


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
         "https://u:[redacted]@host/path?e=a@b.c"),
        (f"redis://:p@{_SECRET}@h:6379/0", "redis://:[redacted]@h:6379/0"),
    ])
    def test_a_dsn(self, text: str, cleaned: str) -> None:
        assert sanitize_text(text) == cleaned
        assert sanitize_text(cleaned) == cleaned


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
        assert line == (
            "provider said a\\x1b[31mb\\u2028c\\nINFO forged line"
        )

    def test_the_traceback_stays_lines(self) -> None:
        try:
            raise ValueError("boom")
        except ValueError as exc:
            line = SanitizingFormatter().format(_record("failed\nx", exc=exc))
        first, *rest = line.split("\n")
        assert first == "failed\\nx"
        assert rest and rest[0].startswith("Traceback")

    def test_uvicorn_s_access_line(self) -> None:
        """AccessFormatter unpacks record.args by position."""
        inner = AccessFormatter('%(client_addr)s "%(request_line)s" %(status_code)s',
                                use_colors=False)
        line = SanitizingFormatter(inner).format(_record(
            '%s - "%s %s HTTP/%s" %d',
            ("1.2.3.4:5", "GET", "/x?a=1\nFAKE", "1.1", 200)))
        assert line == '1.2.3.4:5 "GET /x?a=1\\nFAKE HTTP/1.1" 200 OK'

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
