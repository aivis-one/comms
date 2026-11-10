# =============================================================================
# W3-2 -- the line of protocol 9.4, as the process writes it
# =============================================================================
#
# Item 1: every line of a comms process -- a structlog call with fields
# and without, the standard library through the root handler and through
# uvicorn's own handlers, an exception with its traceback, an exception
# nobody caught in the main thread and in another -- is ONE JSON object
# of exactly the seven fields and, when the line has any, `ctx`; the
# fields of a call are in `ctx`, sanitized. Read on the ASSEMBLED line: a
# child process runs the real setup_logging() and the output is read
# whole, every line of it (configuring logging here would outlive the
# test -- the way tests/test_log_sanitizing.py does it).
#
# Item 2: PRIMER_RELEASE is the release of every line; an entry point
# started without it, or with it empty, refuses to start naming it -- the
# worker, the consumer and the API under uvicorn, each in a child process.
#
# MUTATIONS (one run, each red here):
#   L1 the fields of a call spread on the top level   -> test_every_line_is_
#                                                        the_protocols
#   L2 sanitize_event out of the chain                -> test_a_secret_in_ctx_
#                                                        is_redacted
#   L3 SanitizingFormatter writes the message bare    -> test_every_line_is_
#                                                        the_protocols
#   L4 the traceback left out of msg                  -> test_the_traceback_is_
#                                                        in_msg
#   L5 sys.excepthook not set                         -> test_every_line_is_
#                                                        the_protocols
#   L6 a default release instead of the refusal       -> TestTheRelease
# =============================================================================

import json
import os
import re
import subprocess
import sys
from typing import Any

import pytest

from app.core.logging import LOG_CTX, LOG_FIELDS, RELEASE_VARIABLE
from tests.conftest import REPO_ROOT

_RELEASE = "w3-2-probe"
# A database password inside a DSN, glued at run time.
_PASSWORD = "pw" + "W32" + "zq7"
_DSN = f"postgresql+asyncpg://comms:{_PASSWORD}@db:5432/comms"
_TS = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z")

_CHILD = f"""
import asyncio, logging, logging.config, threading
import structlog
from uvicorn.config import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
from app.core.logging import serving, setup_logging
from app.core.sanitize import sanitized_traceback
setup_logging()
log = structlog.get_logger()
log.info("w32_fields", job="j-1", count=3, nested={{"dsn": {_DSN!r}}},
         password="p", request_id="from-the-call", ts="from-the-call")
log.info("w32_bare")
logging.getLogger("sqlalchemy.pool").warning("w32_stdlib %s", "arg")
logging.getLogger("uvicorn.access").info(
    '%s - "%s %s HTTP/%s" %d', "127.0.0.1", "GET", "/x", "1.1", 200)
try:
    raise RuntimeError("w32 boom {_DSN}")
except RuntimeError as exc:
    log.error("w32_exc_info", exc_info=exc)
    log.error("w32_exception_field", exception=sanitized_traceback(exc))
    logging.getLogger("uvicorn.error").error("w32_uvicorn_exc", exc_info=True)
with serving("rid-1"):
    log.info("w32_served")
    logging.getLogger("sqlalchemy.pool").warning("w32_served_stdlib")
log.info("w32_after")
log.info("w32_not_finite", ratio=float("nan"))
def fail():
    raise RuntimeError("w32 thread")
thread = threading.Thread(target=fail, name="w32-thread")
thread.start(); thread.join()
async def main():
    raise RuntimeError("w32 uncaught")
asyncio.run(main())
"""


def _refuse(constant: str) -> Any:
    raise ValueError(f"not JSON: {constant}")


def _parsed(output: str) -> list[dict[str, Any]]:
    """Every line of the output, each parsed as strict JSON."""
    lines = [line for line in output.splitlines() if line.strip()]
    assert lines, "the pair: the child wrote lines"
    return [json.loads(line, parse_constant=_refuse) for line in lines]


@pytest.fixture(scope="module")
def child() -> tuple[str, dict[str, dict[str, Any]]]:
    """The child's raw output and its lines by event."""
    done = subprocess.run(
        [sys.executable, "-c", _CHILD], cwd=REPO_ROOT,
        env={**os.environ, RELEASE_VARIABLE: _RELEASE},
        capture_output=True, text=True, timeout=60, check=False,
    )
    output = done.stdout + done.stderr
    lines = _parsed(output)
    return output, {line["event"]: line for line in lines}


class TestTheLine:
    """done-when 1. L1, L3, L5."""

    def test_every_line_is_the_protocols(
        self, child: tuple[str, dict[str, dict[str, Any]]],
    ) -> None:
        output, by_event = child
        for line in _parsed(output):
            assert list(line)[:7] == list(LOG_FIELDS), line
            assert set(line) - set(LOG_FIELDS) <= {LOG_CTX}, line
            assert line["service"] == "comms"
            assert line["release"] == _RELEASE
            assert _TS.fullmatch(line["ts"]), line["ts"]
            assert isinstance(line["msg"], str) and line["msg"], line
            assert line["level"] in {"debug", "info", "warning", "error", "critical"}
        # The pair: every path wrote its line, the uncaught ones too.
        assert {
            "w32_fields", "w32_bare", "sqlalchemy.pool", "uvicorn.access",
            "w32_exc_info", "w32_exception_field", "uvicorn.error",
            "unhandled_exception", "unhandled_thread_exception",
        } <= set(by_event)

    def test_the_fields_of_a_call_are_in_ctx(
        self, child: tuple[str, dict[str, dict[str, Any]]],
    ) -> None:
        line = child[1]["w32_fields"]
        assert line["msg"] == "w32_fields" and line["level"] == "info"
        # A field named as a field of the line stays in ctx: the line's
        # own request_id and ts are the line's.
        assert line["request_id"] is None
        assert line["ts"] != "from-the-call"
        assert line[LOG_CTX]["job"] == "j-1"
        assert line[LOG_CTX]["count"] == 3
        assert line[LOG_CTX]["request_id"] == "from-the-call"
        assert line[LOG_CTX]["ts"] == "from-the-call"

    def test_a_line_without_fields_has_no_ctx(
        self, child: tuple[str, dict[str, dict[str, Any]]],
    ) -> None:
        assert LOG_CTX not in child[1]["w32_bare"]
        assert LOG_CTX in child[1]["w32_fields"]  # the pair

    def test_the_standard_library_s_lines(
        self, child: tuple[str, dict[str, dict[str, Any]]],
    ) -> None:
        (stdlib,) = [
            line for line in _parsed(child[0]) if line["msg"] == "w32_stdlib arg"
        ]
        assert (stdlib["level"], stdlib["event"]) == ("warning", "sqlalchemy.pool")
        assert LOG_CTX not in stdlib
        access = child[1]["uvicorn.access"]
        assert access["msg"] == '127.0.0.1 - "GET /x HTTP/1.1" 200'

    def test_a_secret_in_ctx_is_redacted(
        self, child: tuple[str, dict[str, dict[str, Any]]],
    ) -> None:
        """L2."""
        output, by_event = child
        assert _PASSWORD not in output
        ctx = by_event["w32_fields"][LOG_CTX]
        assert ctx["password"] == "[redacted]"
        # The pair: the DSN is there, without its password.
        assert ctx["nested"] == {"dsn": _DSN.replace(_PASSWORD, "[redacted]")}

    @pytest.mark.parametrize(
        "event", ["w32_exc_info", "w32_exception_field", "uvicorn.error"],
    )
    def test_the_traceback_is_in_msg(
        self, child: tuple[str, dict[str, dict[str, Any]]], event: str,
    ) -> None:
        """L4: exc_info, an `exception=` field and a library's record."""
        line = child[1][event]
        first, *rest = line["msg"].split("\n")
        assert first == ("w32_uvicorn_exc" if event == "uvicorn.error" else event)
        assert rest and rest[0] == "Traceback (most recent call last):"
        assert "RuntimeError: w32 boom" in line["msg"]
        assert "[redacted]" in line["msg"]
        assert LOG_CTX not in line

    def test_the_uncaught(
        self, child: tuple[str, dict[str, dict[str, Any]]],
    ) -> None:
        main = child[1]["unhandled_exception"]
        assert main["level"] == "critical"
        assert "RuntimeError: w32 uncaught" in main["msg"]
        thread = child[1]["unhandled_thread_exception"]
        assert thread[LOG_CTX] == {"thread": "w32-thread"}
        assert "RuntimeError: w32 thread" in thread["msg"]

    def test_the_request_id_while_served(
        self, child: tuple[str, dict[str, dict[str, Any]]],
    ) -> None:
        by_event = child[1]
        assert by_event["w32_served"]["request_id"] == "rid-1"
        assert by_event["w32_after"]["request_id"] is None
        stdlib = [
            line for line in _parsed(child[0])
            if line["msg"] == "w32_served_stdlib"
        ]
        assert [line["request_id"] for line in stdlib] == ["rid-1"]

    def test_a_float_json_cannot_take(
        self, child: tuple[str, dict[str, dict[str, Any]]],
    ) -> None:
        assert child[1]["w32_not_finite"][LOG_CTX] == {"ratio": "nan"}


_ENTRY_POINTS = {
    "worker": [sys.executable, "-m", "app.worker"],
    "consumer": [sys.executable, "-m", "app.consumer"],
    "api": [sys.executable, "-m", "uvicorn", "app.main:app", "--port", "0"],
}


def _start(command: list[str], **env: str) -> tuple[int, str]:
    environment = {
        key: value for key, value in os.environ.items()
        if key != RELEASE_VARIABLE
    }
    done = subprocess.run(
        command, cwd=REPO_ROOT, env={**environment, **env},
        capture_output=True, text=True, timeout=60, check=False,
    )
    return done.returncode, done.stdout + done.stderr


class TestTheRelease:
    """done-when 2. L6."""

    @pytest.mark.parametrize("given", ["absent", "empty"])
    @pytest.mark.parametrize("entry", list(_ENTRY_POINTS))
    def test_without_it_no_process_starts(self, entry: str, given: str) -> None:
        env = {} if given == "absent" else {RELEASE_VARIABLE: ""}
        code, output = _start(_ENTRY_POINTS[entry], **env)
        assert code != 0, output
        assert f"{RELEASE_VARIABLE} is not set" in output, output

    def test_with_it_the_line_names_it(self) -> None:
        """The pair, on the same entry point: with the release the
        consumer gets past the logging setup and stops on its next check
        (no REDIS_URL) -- that refusal is a line of the protocol, naming
        the release."""
        code, output = _start(
            _ENTRY_POINTS["consumer"], REDIS_URL="", **{RELEASE_VARIABLE: _RELEASE},
        )
        assert code != 0
        (line,) = [
            line for line in _parsed(output)
            if line["event"] == "unhandled_exception"
        ]
        assert line["release"] == _RELEASE
        assert "REDIS_URL is required" in line["msg"]
        assert f"{RELEASE_VARIABLE} is not set" not in output
