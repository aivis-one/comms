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
# SAN-1 (the forms pattern 5 of app/core/sanitize.py missed, and values
# the processor could not walk):
#   S1  the key without its _/- prefix               -> TestPrefixedKeys
#   S2  no quote allowed after the key               -> TestQuotedKeys
#   S3  the unquoted value to the first space        -> TestValueWithSpaces
#   S4  no quoted-value branch                       -> TestValueWithSpaces
#   S5  the query value runs like any other          -> TestValueWithSpaces
#   S6  [redacted] extended like any value           -> TestValueWithSpaces
#   S7  word characters let in after the key         -> TestPrefixedKeys
#   S8  a namedtuple rebuilt from one iterable       -> TestTheProcessorNeverRaises
#   S9  no cycle guard                               -> TestTheProcessorNeverRaises
#   S10 the cycle guard global, not per path         -> TestTheProcessorNeverRaises
# =============================================================================

import ast
import io
import json
import logging
import os
import subprocess
import sys
from collections import deque, namedtuple
from pathlib import Path
from typing import Any

import pytest
import structlog

from app.core.logging import (
    CORRELATION_FIELDS,
    SanitizingFormatter,
    processors,
    sanitize_event,
)
from app.core.sanitize import sanitize_text

_REPO = Path(__file__).resolve().parents[1]
_APP = _REPO / "app"
# A database password inside a DSN, glued at run time.
_PASSWORD = "pw" + "SENTINEL" + "zq7"
_DSN = f"postgresql+asyncpg://comms:{_PASSWORD}@db:5432/comms"
_HEX = "ab" * 32  # 64 hex characters: a digest, a key, or a secret
_FUNCTIONS = ("sanitize_text", "exception_text", "sanitized_traceback")


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

    def test_the_formatter_writes_the_machine_line(self) -> None:
        """Until W3-2 the formatter wrapped the one a handler had and
        kept its shape (`<WARNING> dsn ...`); the machine's line
        (protocol 9.4) replaced that shape -- the level is a field, the
        sanitized text is the line's msg."""
        record = logging.LogRecord(
            "x", logging.WARNING, __file__, 1, "dsn %s", (_DSN,), None,
        )
        line = json.loads(SanitizingFormatter().format(record))
        assert (line["level"], line["event"]) == ("warning", "x")
        assert line["msg"] == f"dsn {_DSN.replace(_PASSWORD, '[redacted]')}"


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
            "app/core/sanitize.py:exception_text",
            "app/core/sanitize.py:sanitize_text",
            "app/core/sanitize.py:sanitized_traceback",
        ]
        # The pair: the callers exist, and all of them import from core.
        assert len(importers) >= 10
        assert set(importers.values()) == {"app.core.sanitize"}


# =============================================================================
# SAN-1 -- the key=value forms, and values the processor cannot walk
# =============================================================================

# A value in a credential's place, glued at run time.
_SECRET = "zq" + "SANone" + "7k"


def _once_and_twice(text: str) -> str:
    """sanitize_text's output, checked idempotent on the way."""
    once = sanitize_text(text)
    assert sanitize_text(once) == once
    return once


class TestPrefixedKeys:
    """Item 1: a credential word with a prefix joined by _ or - is a key
    -- the settings' names, the deploy's env names, a gateway's header,
    an OAuth pair; a word that only contains one is not. S1, S7."""

    @pytest.mark.parametrize("key", [
        "access_token", "refresh_token", "db_password", "client_secret",
        "comms_service_token", "telegram_bot_token", "email_mailgun_api_key",
        "postgres_password", "redis_password", "x-api-key", "X_API_KEY",
    ])
    @pytest.mark.parametrize("sep", ["=", ": "])
    def test_the_value_goes_the_key_stays(self, key: str, sep: str) -> None:
        cleaned = _once_and_twice(f"refused {key}{sep}{_SECRET}; retry")
        assert cleaned == f"refused {key}{sep}[redacted]; retry"

    @pytest.mark.parametrize("text", [
        "max_tokens=1000 is over the limit",
        "tokenizer: cl100k",
        "token_mismatch: 2 of 3",
        "password_changed: true",
        "passwords match",
        "AuthorizationError: no scope",
        "TokenValidationError: bad padding",
        "secrets: 3 configured",
        "a token-shaped value",
    ])
    def test_ordinary_words_pass_whole(self, text: str) -> None:
        assert sanitize_text(text) == text


class TestQuotedKeys:
    """Item 2: a key in quotes -- JSON, a dict's repr -- and its value
    in quotes; the quotes and the rest of the structure stay. S2."""

    def test_json(self) -> None:
        text = json.dumps({"password": _SECRET, "n": 1, "event": "x"})
        cleaned = _once_and_twice(text)
        assert json.loads(cleaned) == {
            "password": "[redacted]", "n": 1, "event": "x",
        }

    def test_a_dict_repr(self) -> None:
        cleaned = _once_and_twice(repr({"db_password": _SECRET, "b": 2}))
        assert ast.literal_eval(cleaned) == {"db_password": "[redacted]", "b": 2}

    def test_a_header_dict_repr(self) -> None:
        """The secret and the key only: the bearer pattern (2) runs to
        the next whitespace, and in a repr that is past the closing
        quote -- a shape of pattern 2, not of this one."""
        text = repr({"Authorization": f"Bearer {_SECRET}", "Accept": "*/*"})
        cleaned = _once_and_twice(text)
        assert _SECRET not in cleaned
        assert cleaned.startswith("{'Authorization': '[redacted]'")


class TestValueWithSpaces:
    """Item 3: a value with a space leaves no piece of itself; the text
    after its boundary stays. S3-S6."""

    def test_unquoted_runs_to_the_boundary(self) -> None:
        cleaned = _once_and_twice(f"login password=my {_SECRET} x; then 2")
        assert cleaned == "login password=[redacted]; then 2"

    def test_unquoted_runs_to_the_end_of_the_line(self) -> None:
        cleaned = _once_and_twice(f"token: my {_SECRET}\nnext line")
        assert cleaned == "token: [redacted]\nnext line"

    def test_the_query_string(self) -> None:
        cleaned = _once_and_twice(f"GET /x?a=1&password={_SECRET}&b=2")
        assert cleaned == "GET /x?a=1&password=[redacted]&b=2"

    def test_the_access_line_keeps_its_tail(self) -> None:
        cleaned = _once_and_twice(f'"GET /x?token={_SECRET} HTTP/1.1" 200')
        assert cleaned == '"GET /x?token=[redacted] HTTP/1.1" 200'

    def test_a_json_string_with_a_space_and_a_quote(self) -> None:
        text = json.dumps({"api_key": f'my "{_SECRET}" key', "ok": True})
        cleaned = _once_and_twice(text)
        assert _SECRET not in cleaned
        assert json.loads(cleaned) == {"api_key": "[redacted]", "ok": True}

    def test_a_quote_cut_short(self) -> None:
        cleaned = _once_and_twice(f'"password": "my {_SECRET}')
        assert cleaned == '"password": "[redacted]'

    def test_a_redacted_header_keeps_the_text_after_it(self) -> None:
        cleaned = _once_and_twice(f"sent Authorization: Bearer {_SECRET} upstream")
        assert cleaned == "sent Authorization=[redacted] upstream"

    def test_a_value_an_earlier_pattern_took_part_of(self) -> None:
        cleaned = _once_and_twice(f"token: Bearer {_SECRET} and more")
        assert cleaned == "token: [redacted]"

    def test_two_keys_on_one_line(self) -> None:
        cleaned = _once_and_twice(
            f"access_token=a {_SECRET}, 'client_secret': '{_SECRET} b' end")
        assert cleaned == "access_token=[redacted], 'client_secret': '[redacted]' end"

    @pytest.mark.parametrize("text", ["password=", "password=&b=2", ""])
    def test_no_value_nothing_to_take(self, text: str) -> None:
        assert sanitize_text(text) == text


_Pair = namedtuple("_Pair", "label secret")


def _logged(value: Any) -> dict[str, Any]:
    """One event with `value`, through the real chain, rendered as JSON."""
    stream = io.StringIO()
    logger = structlog.wrap_logger(
        structlog.PrintLogger(file=stream),
        processors=processors(structlog.processors.JSONRenderer()),
    )
    logger.info("san1_probe", value=value)
    line = stream.getvalue()
    assert _SECRET not in line
    parsed: dict[str, Any] = json.loads(line)
    return parsed


class TestTheProcessorNeverRaises:
    """Item 4: a value _sanitized cannot walk goes to the log as its
    repr, sanitized -- never as an exception out of the chain. S8-S10."""

    def test_a_namedtuple(self) -> None:
        line = _logged(_Pair("x", f"token={_SECRET}"))
        # The repr's own field name is a key too: the whole quoted value goes.
        assert line["value"] == "_Pair(label='x', secret='[redacted]')"

    def test_a_dict_that_holds_itself(self) -> None:
        loop: dict[str, Any] = {"dsn": f"password={_SECRET}"}
        loop["self"] = loop
        line = _logged(loop)
        assert line["value"]["dsn"] == "password=[redacted]"
        assert "{...}" in line["value"]["self"]
        assert "[redacted]" in line["value"]["self"]

    def test_a_list_that_holds_itself(self) -> None:
        loop: list[Any] = [f"token={_SECRET}"]
        loop.append(loop)
        line = _logged(loop)
        assert line["value"][0] == "token=[redacted]"
        assert "[...]" in line["value"][1]

    def test_the_same_list_twice_is_not_a_cycle(self) -> None:
        shared = [f"token={_SECRET}"]
        line = _logged({"a": shared, "b": shared})
        assert line["value"] == {
            "a": ["token=[redacted]"], "b": ["token=[redacted]"],
        }

    def test_what_it_does_not_rebuild(self) -> None:
        line = _logged([deque([f"token={_SECRET}"]), (), {}, 3, None])
        assert line["value"] == ["deque(['token=[redacted]])", [], {}, 3, None]
