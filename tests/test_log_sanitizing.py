# =============================================================================
# H8 R-D -- no path to the log prints a secret from an exception's text
# =============================================================================
#
# The paths: structlog (an event with exc_info, rendered by
# format_exc_info), the standard library through the root handler
# (sqlalchemy, asyncio), uvicorn's own loggers (their own handlers, no
# propagation), and an exception nobody caught (main thread -- the one
# out of asyncio.run -- and other threads). Each path is run in a CHILD
# PROCESS through the real setup_logging(), and its output read whole:
# a test that configured logging in this process would leave it changed
# for every test after it (§0 §9, the H7 fix). The pieces with no global
# state -- the processor, the formatter -- are also tested here, in
# process.
#
# MUTATIONS these tests were written against (each turns one red):
#   M3  sanitize_event out of the structlog chain     -> TestThePaths structlog
#   M4  the root handler without SanitizingFormatter  -> TestThePaths stdlib
#   M5  uvicorn's handlers not wrapped                -> TestThePaths uvicorn
#   M6  sys.excepthook / threading.excepthook not set -> TestThePaths uncaught
#   M7  a copy of sanitize_text outside app/core      -> TestOneSanitizer
#   W1  idempotency_key out of CORRELATION_FIELDS     -> TestCorrelationFields
# =============================================================================

import ast
import io
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest
import structlog

from app.core.logging import (
    CORRELATION_FIELDS,
    SanitizingFormatter,
    processors,
    sanitize_event,
)

_REPO = Path(__file__).resolve().parents[1]
_APP = _REPO / "app"
# A database password inside a DSN, glued at run time.
_PASSWORD = "pw" + "SENTINEL" + "zq7"
_DSN = f"postgresql+asyncpg://comms:{_PASSWORD}@db:5432/comms"
_HEX = "ab" * 32  # 64 hex characters: a digest, a key, or a secret
_FUNCTIONS = ("sanitize_text", "sanitize_error", "sanitized_traceback")


def _child(body: str) -> str:
    """Run `body` in a fresh interpreter after the real setup_logging();
    return everything it wrote, stdout and stderr together."""
    script = (
        "from app.core.logging import setup_logging\n"
        "setup_logging()\n" + body
    )
    done = subprocess.run(
        [sys.executable, "-c", script], cwd=_REPO, env=dict(os.environ),
        capture_output=True, text=True, timeout=60, check=False,
    )
    return done.stdout + done.stderr


def _chained() -> str:
    """Source of an exception whose text AND cause carry the secret."""
    return (
        "def boom():\n"
        "    try:\n"
        f"        raise ConnectionError('cannot reach {_DSN}')\n"
        "    except ConnectionError as cause:\n"
        f"        raise RuntimeError('pool failed for {_DSN}') from cause\n"
    )


def _redacted_with_trace(output: str) -> None:
    assert _PASSWORD not in output
    # The pair: the traceback is there, and so is the redaction mark.
    assert "Traceback (most recent call last)" in output
    assert "RuntimeError: pool failed for" in output
    assert "[redacted]" in output


class TestThePaths:
    """done-when R-D (2). M3-M6."""

    def test_structlog_with_exc_info(self) -> None:
        output = _child(
            _chained()
            + "import structlog\n"
            "try:\n"
            "    boom()\n"
            "except RuntimeError as exc:\n"
            "    structlog.get_logger().error('h8_probe', exc_info=exc)\n"
        )
        assert "h8_probe" in output
        _redacted_with_trace(output)

    def test_the_standard_library_root(self) -> None:
        output = _child(
            _chained()
            + "import logging\n"
            "try:\n"
            "    boom()\n"
            "except RuntimeError:\n"
            "    logging.getLogger('sqlalchemy.pool').error(\n"
            "        'h8_probe', exc_info=True)\n"
        )
        assert "h8_probe" in output
        _redacted_with_trace(output)

    def test_uvicorn_s_own_handlers(self) -> None:
        """uvicorn configures its loggers -- own handlers, no propagation
        -- BEFORE the app's lifespan calls setup_logging (uvicorn
        _subprocess.subprocess_started); the child does the same, in
        that order."""
        script = (
            "import logging, logging.config\n"
            "from uvicorn.config import LOGGING_CONFIG\n"
            "logging.config.dictConfig(LOGGING_CONFIG)\n"
            "from app.core.logging import setup_logging\n"
            "setup_logging()\n" + _chained()
            + "try:\n"
            "    boom()\n"
            "except RuntimeError:\n"
            "    logging.getLogger('uvicorn.error').error('h8_probe', exc_info=True)\n"
            "logging.getLogger('uvicorn.access').info(\n"
            f"    '%s - \"%s %s HTTP/%s\" %d', '127.0.0.1', 'GET',\n"
            f"    '/x?token={_PASSWORD}', '1.1', 200)\n"
        )
        done = subprocess.run(
            [sys.executable, "-c", script], cwd=_REPO, env=dict(os.environ),
            capture_output=True, text=True, timeout=60, check=False,
        )
        output = done.stdout + done.stderr
        assert "h8_probe" in output and "GET /x?token=[redacted]" in output
        _redacted_with_trace(output)

    @pytest.mark.parametrize("where", ["asyncio_run", "thread"])
    def test_an_exception_nobody_caught(self, where: str) -> None:
        raise_it = (
            "import asyncio\n"
            "async def main():\n"
            "    boom()\n"
            "asyncio.run(main())\n"
            if where == "asyncio_run" else
            "import threading\n"
            "t = threading.Thread(target=boom)\n"
            "t.start(); t.join()\n"
        )
        output = _child(_chained() + raise_it)
        event = (
            "unhandled_exception" if where == "asyncio_run"
            else "unhandled_thread_exception"
        )
        assert event in output
        _redacted_with_trace(output)


class TestThePiecesInProcess:
    """The processor and the formatter alone -- no global state."""

    def test_the_chain_renders_then_sanitizes(self) -> None:
        stream = io.StringIO()
        logger = structlog.wrap_logger(
            structlog.PrintLogger(file=stream),
            processors=processors(structlog.processors.JSONRenderer()),
        )
        try:
            raise RuntimeError(f"cannot reach {_DSN}")
        except RuntimeError as exc:
            logger.error("h8_probe", exc_info=exc, nested={"dsn": [_DSN]})
        line = json.loads(stream.getvalue())
        assert _PASSWORD not in stream.getvalue()
        assert line["exception"].startswith("Traceback")
        assert line["nested"] == {"dsn": [_DSN.replace(_PASSWORD, "[redacted]")]}

    def test_the_formatter_keeps_the_inner_shape(self) -> None:
        inner = logging.Formatter("<%(levelname)s> %(message)s")
        formatter = SanitizingFormatter(inner)
        record = logging.LogRecord(
            "x", logging.WARNING, __file__, 1, "dsn %s", (_DSN,), None,
        )
        assert formatter.format(record) == (
            f"<WARNING> dsn {_DSN.replace(_PASSWORD, '[redacted]')}"
        )


class TestCorrelationFields:
    """правка 2 of the H8 gate: the thread between the product's logs and
    comms' (spec §6.6) is kept whole -- a 64-hex idempotency key stays,
    the same hex inside an exception's text is redacted. W1."""

    def test_the_key_stays_the_text_is_redacted(self) -> None:
        event = sanitize_event(None, "info", {
            "event": "h8_probe",
            "idempotency_key": _HEX,
            "correlation": _HEX,
            "error": f"refused with {_HEX}",
        })
        assert event["idempotency_key"] == _HEX
        assert event["correlation"] == _HEX
        assert event["error"] == "refused with [redacted]"

    def test_the_list_is_the_thread_and_nothing_else(self) -> None:
        expected = {
            "idempotency_key", "correlation", "notification_id", "delivery_id",
        }
        assert expected == CORRELATION_FIELDS


class TestOneSanitizer:
    """done-when R-D (1): one implementation, in core; every caller
    imports it from there. M7."""

    def test_the_patterns_live_in_one_module(self) -> None:
        definers: list[str] = []
        importers: dict[str, str] = {}
        for path in sorted(_APP.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            name = path.relative_to(_REPO).as_posix()
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name in _FUNCTIONS:
                    definers.append(f"{name}:{node.name}")
                if isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id.endswith("_RE")
                    and "re.compile" in ast.unparse(node.value)
                    and "LONG_HEX" in t.id
                    for t in node.targets
                ):
                    definers.append(f"{name}:{node.targets[0].id}")  # type: ignore[attr-defined]
                if isinstance(node, ast.ImportFrom) and any(
                    alias.name in _FUNCTIONS
                    for alias in node.names
                ):
                    importers[name] = node.module or ""
        assert sorted(definers) == [
            "app/core/sanitize.py:_LONG_HEX_RE",
            "app/core/sanitize.py:sanitize_error",
            "app/core/sanitize.py:sanitize_text",
            "app/core/sanitize.py:sanitized_traceback",
        ]
        # The pair: the callers exist, and all of them import from core.
        assert len(importers) >= 10
        assert set(importers.values()) == {"app.core.sanitize"}
