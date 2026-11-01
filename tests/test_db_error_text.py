# =============================================================================
# DB-1 -- a database error reaches a log as its class, place, SQLSTATE and
# constraint: never its text, never a value
# =============================================================================
#
# Every error here is REAL -- raised by Postgres, by asyncpg, by SQLAlchemy
# on this test's own database -- and carries a SENTINEL where a person's
# data would sit: in a key (DETAIL "Key (...)=(...)"), in a row ("Failing
# row contains (...)"), in a message (22P02), in a bound parameter.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 hide_parameters off the application's engine -> TestTheFlag
#   M2 exception_text / the summary returning the text -> TestTheRule,
#      TestRealPaths
#   M3 the recognition without the driver's classes -> TestTheRule (chain)
#   M4 the structlog chain back to a bare format_exc_info -> TestExcInfo
#   M5 the resolver writing the value again -> TestTheResolver
#   M8 bound_contextvars around resolve_targets taken off -> TestTheResolver
#   M6 a new logger with str(exc) -> TestTheGuard
#   M7 SanitizingFormatter off uvicorn's handler -> TestUvicorn
# =============================================================================

import ast
import io
import logging
import logging.config
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from structlog.testing import capture_logs

from app.core.config import settings
from app.core.database import get_engine, get_session_factory
from app.core.logging import _sanitize_handlers, format_exc_info_by_the_rule
from app.core.sanitize import (
    db_error_summary,
    exception_text,
    pipeline_error_of,
    sanitized_traceback,
)
from tests.helpers import real_own_backend_terminated, real_serialization_failure

SENTINEL = "zqSENTINELxk"
_APP = Path(__file__).resolve().parents[1] / "app"


# ---------------------------------------------------------------------------
# Real errors with the sentinel where data sits
# ---------------------------------------------------------------------------


async def _on_the_app_engine(*statements: tuple[str, dict[str, Any]]) -> Exception:
    """Run statements on the APPLICATION's engine until one fails; return
    its exception (the temp table dies with the connection)."""
    async with get_engine().connect() as conn:
        await conn.execute(text(
            "CREATE TEMP TABLE zz_db1 (k text PRIMARY KEY, v text "
            "CONSTRAINT zz_db1_v CHECK (v <> 'bad'), n int)"))
        try:
            for sql, params in statements:
                await conn.execute(text(sql), params)
        except Exception as exc:
            await conn.rollback()
            return exc
    raise AssertionError("nothing failed")


async def real_unique() -> Exception:  # 23505, the key in DETAIL
    return await _on_the_app_engine(
        ("INSERT INTO zz_db1 VALUES (:k, 'ok', 1)", {"k": SENTINEL}),
        ("INSERT INTO zz_db1 VALUES (:k, 'ok', 1)", {"k": SENTINEL}),
    )


async def real_check() -> Exception:  # 23514, the row in DETAIL
    return await _on_the_app_engine(
        ("INSERT INTO zz_db1 VALUES (:k, 'bad', 1)", {"k": SENTINEL}),
    )


async def real_value_in_message() -> Exception:  # 22P02, the value in the text
    return await _on_the_app_engine(
        (f"SELECT CAST('{SENTINEL}' AS uuid)", {}),
    )


async def real_foreign_key() -> Exception:  # 23503
    return await _on_the_app_engine(
        ("CREATE TEMP TABLE zz_db1_child (k text REFERENCES zz_db1 (k))", {}),
        ("INSERT INTO zz_db1_child VALUES (:k)", {"k": SENTINEL}),
    )


async def _raw_connection() -> asyncpg.Connection:
    url = get_engine().url
    return await asyncpg.connect(
        host=url.host, port=url.port, user=url.username,
        password=url.password, database=url.database,
    )


async def real_bare_asyncpg() -> Exception:  # asyncpg's own, no SQLAlchemy
    conn = await _raw_connection()
    try:
        await conn.execute("CREATE TEMP TABLE zz_raw (v text CHECK (length(v) < 5))")
        await conn.execute(f"INSERT INTO zz_raw VALUES ('{SENTINEL}')")
    except Exception as exc:
        return exc
    finally:
        await conn.close()
    raise AssertionError("nothing failed")


async def real_client_data_error() -> Exception:  # the client's DataError
    conn = await _raw_connection()
    try:
        await conn.fetchval("SELECT $1::int", SENTINEL)
    except Exception as exc:
        return exc
    finally:
        await conn.close()
    raise AssertionError("nothing failed")


_REAL: dict[str, Callable[[], Awaitable[Exception]]] = {
    "23505": real_unique,
    "23514": real_check,
    "22P02": real_value_in_message,
    "23503": real_foreign_key,
    "bare asyncpg": real_bare_asyncpg,
    "client DataError": real_client_data_error,
}


class TestTheRule:
    @pytest.mark.parametrize("kind", sorted(_REAL))
    async def test_the_sentinel_never_survives(self, kind: str) -> None:
        exc = await _REAL[kind]()
        assert SENTINEL in "".join(str(link) for link in _links(exc))  # instrument
        summary = db_error_summary(exc)
        assert summary is not None
        assert summary.startswith(f"{type(exc).__module__}.{type(exc).__qualname__}")
        for rendered in (summary, exception_text(exc), sanitized_traceback(exc)):
            assert SENTINEL not in rendered
            assert "DETAIL" not in rendered
        trace = sanitized_traceback(exc)
        assert "File " in trace  # the frames stay

    @pytest.mark.parametrize("kind, state, constraint", [
        ("23505", "23505", "zz_db1_pkey"),
        ("23514", "23514", "zz_db1_v"),
        ("23503", "23503", "zz_db1_child_k_fkey"),
        ("22P02", "22P02", None),
    ])
    async def test_the_summary_says_what_it_is(
        self, kind: str, state: str, constraint: str | None,
    ) -> None:
        summary = db_error_summary(await _REAL[kind]())
        assert summary is not None
        assert f"SQLSTATE {state}" in summary
        if constraint:
            assert f"constraint {constraint}" in summary

    async def test_transient_ones(self) -> None:
        for exc in (await real_serialization_failure(),
                    await real_own_backend_terminated(mid_statement=False)):
            summary = db_error_summary(exc)
            assert summary is not None
            assert summary.startswith(pipeline_error_of(exc))

    async def test_the_chain_every_link(self) -> None:
        """SQLAlchemy's error -> the adapter's -> asyncpg's: three links,
        three summaries in the traceback, no text of any."""
        exc = await real_check()
        assert len(_links(exc)) >= 3
        trace = sanitized_traceback(exc)
        assert SENTINEL not in trace
        assert trace.count("SQLSTATE 23514") >= 2

    def test_the_pair_comms_own_words_stay(self) -> None:
        mine = ValueError("our own words")
        assert db_error_summary(mine) is None
        assert exception_text(mine) == "our own words"
        assert "our own words" in sanitized_traceback(_raised(mine))


def _links(exc: BaseException) -> list[BaseException]:
    seen: list[BaseException] = []
    link: BaseException | None = exc
    while link is not None and all(link is not s for s in seen):
        seen.append(link)
        orig = getattr(link, "orig", None)
        link = orig if isinstance(orig, BaseException) else (
            link.__cause__ or link.__context__)
    return seen


def _raised(exc: BaseException) -> BaseException:
    try:
        raise exc
    except BaseException as raised:
        return raised


class TestTheFlag:
    async def test_no_parameters_on_the_application_engine(self) -> None:
        exc = await real_unique()
        assert "[parameters" not in str(exc)

    async def test_the_pair_without_the_flag(self) -> None:
        engine = create_async_engine(settings.database_url)
        try:
            async with engine.connect() as conn:
                await conn.execute(text("CREATE TEMP TABLE zz_p (k text PRIMARY KEY)"))
                await conn.execute(
                    text("INSERT INTO zz_p VALUES (:k)"), {"k": SENTINEL},
                )
                with pytest.raises(Exception) as caught:
                    await conn.execute(text("INSERT INTO zz_p VALUES (:k)"),
                                       {"k": SENTINEL})
            assert "[parameters" in str(caught.value)
        finally:
            await engine.dispose()


# ---------------------------------------------------------------------------
# The logging pipeline: exc_info of any logger, uvicorn's handlers
# ---------------------------------------------------------------------------


class TestExcInfo:
    async def test_exc_info_renders_by_the_rule(self) -> None:
        exc = await real_check()
        out = format_exc_info_by_the_rule(None, "error", {"exc_info": exc})
        assert SENTINEL not in out["exception"]
        assert "SQLSTATE 23514" in out["exception"]


    async def test_through_the_real_chain(self) -> None:
        """The structlog chain itself (app/core/logging.py processors): an
        exc_info goes through the rule, not through a bare
        format_exc_info."""
        from app.core.logging import processors

        exc = await real_check()
        event: Any = {"event": "boom", "exc_info": exc}
        for processor in processors(lambda _l, _m, d: d):
            event = processor(None, "error", event)
        assert SENTINEL not in str(event)
        assert "SQLSTATE 23514" in str(event)


class TestUvicorn:
    async def test_uvicorn_error_prints_the_summary(self) -> None:
        """uvicorn.error ("Exception in ASGI application") propagates to the
        `uvicorn` logger's handler -- the one uvicorn's own config installs;
        _sanitize_handlers wraps it at the app's start."""
        from uvicorn.config import LOGGING_CONFIG

        before = {name: (lg.handlers[:], lg.propagate, lg.level)
                  for name in ("uvicorn", "uvicorn.error", "uvicorn.access")
                  for lg in [logging.getLogger(name)]}
        try:
            logging.config.dictConfig(LOGGING_CONFIG)
            _sanitize_handlers()
            stream = io.StringIO()
            (handler,) = logging.getLogger("uvicorn").handlers
            handler.setStream(stream)  # type: ignore[attr-defined]
            exc = await real_check()
            logging.getLogger("uvicorn.error").error(
                "Exception in ASGI application", exc_info=(type(exc), exc,
                                                          exc.__traceback__))
            out = stream.getvalue()
            assert "Exception in ASGI application" in out
            assert SENTINEL not in out
            assert "SQLSTATE 23514" in out
        finally:
            for name, (handlers, propagate, level) in before.items():
                lg = logging.getLogger(name)
                lg.handlers[:] = handlers
                lg.propagate = propagate
                lg.setLevel(level)


# ---------------------------------------------------------------------------
# Real paths
# ---------------------------------------------------------------------------


def _all_text(logs: list[dict[str, Any]]) -> str:
    return "\n".join(str(entry) for entry in logs)


class TestRealPaths:
    async def test_an_api_500(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        failure = await real_check()

        async def broken(*_a: Any, **_k: Any) -> Any:
            raise failure

        monkeypatch.setattr("app.api.messaging.get_or_create_section", broken)
        with capture_logs() as logs:
            resp = await client.post(
                "/api/v1/sections", json={"key": "k", "label": "L"},
            )
        assert resp.status_code == 500
        assert SENTINEL not in resp.text
        text_ = _all_text(logs)
        assert SENTINEL not in text_
        assert "SQLSTATE 23514" in text_

    async def test_the_consumer_unexpected(
        self, monkeypatch: pytest.MonkeyPatch, db_session: AsyncSession,
    ) -> None:
        import fakeredis.aioredis as fakeaioredis

        from app.transport import consumer as consumer_module
        from app.transport.consumer import StreamConsumer
        from tests.test_transport_consumer import (
            _run_until,
            _user_upserted_data,
            _xadd,
        )

        failure = await real_check()

        async def broken(*_a: Any, **_k: Any) -> Any:
            raise failure

        redis = fakeaioredis.FakeRedis()
        stream = f"comms:test:{uuid4().hex[:8]}"
        monkeypatch.setattr(settings, "comms_events_stream", stream)
        monkeypatch.setattr(settings, "consumer_block_ms", 20)
        monkeypatch.setattr(consumer_module, "handle_event", broken)
        await _xadd(redis, stream, "user_upserted", _user_upserted_data(uuid4()))

        async def dead() -> bool:
            return await redis.xlen(settings.dlq_stream) == 1

        with capture_logs() as logs:
            await _run_until(StreamConsumer(redis), dead)
        dlq = await redis.xrange(settings.dlq_stream)
        assert SENTINEL not in str(dlq)
        assert SENTINEL not in _all_text(logs)
        assert "SQLSTATE 23514" in _all_text(logs)

    async def test_the_worker_pipeline_step(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.engine import processor
        from tests.test_worker_keeps_its_work import _attempt, _job

        failure = await real_check()

        async def broken(*_a: Any, **_k: Any) -> Any:
            raise failure

        job_id = await _job()
        monkeypatch.setattr(processor, "rollup_notification", broken)
        with capture_logs() as logs:
            await _attempt(job_id)
        assert SENTINEL not in _all_text(logs)
        assert "SQLSTATE 23514" in _all_text(logs)

    async def test_the_worker_delivery(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.engine import service
        from tests.test_worker_keeps_its_work import _attempt, _job

        failure = await real_check()

        async def broken(*_a: Any, **_k: Any) -> bool:
            raise failure

        monkeypatch.setattr(service.get_formatter("in_app"), "deliver", broken)
        job_id = await _job()
        with capture_logs() as logs:
            await _attempt(job_id)
        assert SENTINEL not in _all_text(logs)
        async with get_session_factory()() as s:
            stored = await s.scalar(text(
                "SELECT string_agg(coalesce(provider_text, ''), ' ') FROM "
                "notification_transitions WHERE notification_id = :id"),
                {"id": job_id})
        assert SENTINEL not in (stored or "")

    async def test_a_maintenance_pass(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app import worker

        failure = await real_check()

        async def broken() -> int:
            raise failure

        monkeypatch.setattr(worker, "run_engine_maintenance", broken)
        with capture_logs() as logs:
            await worker.run_maintenance_pass()
        assert SENTINEL not in _all_text(logs)
        assert any(e["event"] == "maintenance_pass_error" for e in logs)


# ---------------------------------------------------------------------------
# The resolver
# ---------------------------------------------------------------------------


@contextmanager
def _capture_with_context() -> Iterator[list[dict[str, Any]]]:
    """capture_logs, with the contextvars merged in first -- as the real
    chain does (app/core/logging.py processors): a line's job id and key
    come from bound_contextvars, which capture_logs alone does not see."""
    import structlog
    from structlog.testing import LogCapture

    capture = LogCapture()
    old = structlog.get_config()
    structlog.configure(processors=[structlog.contextvars.merge_contextvars, capture])
    try:
        yield capture.entries
    finally:
        structlog.configure(**old)


class TestTheResolver:
    @pytest.mark.parametrize("value", ["person@example.org", "+79991234567"])
    async def test_no_value_and_the_jobs_envelope(self, value: str) -> None:
        """On the real path -- an attempt resolving a job whose user target
        is an email or a phone: the line holds no value and nothing derived
        from it, its length, and the job's id and idempotency key, equal to
        the job's (bound around resolve_targets in resolve_notification)."""
        import hashlib

        from tests.test_worker_keeps_its_work import _attempt, _job

        job_id = await _job()
        async with get_session_factory()() as s:
            await s.execute(text(
                "UPDATE notifications SET target_value = :v WHERE id = :id"),
                {"v": value, "id": job_id})
            key = await s.scalar(text(
                "SELECT idempotency_key FROM notifications WHERE id = :id"),
                {"id": job_id})
            await s.commit()
        with _capture_with_context() as logs:
            await _attempt(job_id)
        out = _all_text(logs)
        assert value not in out
        assert hashlib.sha256(value.encode()).hexdigest()[:8] not in out
        (line,) = [e for e in logs if e["event"] == "invalid_user_target"]
        assert line["target_length"] == len(value)
        assert line["notification_id"] == str(job_id)
        assert line["idempotency_key"] == key


# ---------------------------------------------------------------------------
# The guard -- no exception's text past the rule, anywhere in an except
# ---------------------------------------------------------------------------

# Uses of the caught exception's text that are no LOG and leave no
# process, or comms' own exceptions -- each with its reason.
_ALLOWED = {
    # except redis.ResponseError (never the database's): the text of
    # Redis's answer to XGROUP CREATE is compared, written nowhere
    ("app/transport/consumer.py", '"BUSYGROUP" not in str(exc)'),
    # except TelegramAPIError (never the database's): the provider's words
    # classify the failure; nothing of them is stored or logged from here
    ("app/engine/formatters.py", "str(exc).lower()"),
    # except _DuplicateNameError (comms' own, raised by the JSON hook while
    # reading LLM_KEY_SETS): the duplicated set NAME of the operator's own
    # configuration, into the startup refusal -- no database on that path
    ("app/core/channels.py", "exc.args[0]"),
}

_FORBIDDEN_CALLS = {"str", "repr"}


def _offences(source: str, path: str) -> list[str]:
    tree = ast.parse(source)
    found: list[str] = []
    for handler in ast.walk(tree):
        if not (isinstance(handler, ast.ExceptHandler) and handler.name):
            continue
        name = handler.name
        lines = source.splitlines()
        for node in ast.walk(ast.Module(body=handler.body, type_ignores=[])):
            hit = None
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                    and node.func.id in _FORBIDDEN_CALLS and node.args \
                    and isinstance(node.args[0], ast.Name) and node.args[0].id == name:
                hit = f"{node.func.id}({name})"
            elif isinstance(node, ast.FormattedValue) and isinstance(
                    node.value, ast.Name) and node.value.id == name:
                hit = "{" + name + "}"
            elif isinstance(node, ast.Attribute) and isinstance(
                    node.value, ast.Name) and node.value.id == name \
                    and node.attr == "args":
                hit = f"{name}.args"
            elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod) \
                    and any(isinstance(n, ast.Name) and n.id == name
                            for n in ast.walk(node.right)):
                hit = f"% {name}"
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                    and node.func.attr == "format" and any(
                        isinstance(n, ast.Name) and n.id == name
                        for a in node.args for n in ast.walk(a)):
                hit = f".format({name})"
            elif isinstance(node, ast.Attribute) and isinstance(
                    node.value, ast.Name) and node.value.id == "traceback" \
                    and node.attr.startswith("format"):
                hit = f"traceback.{node.attr}"
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                    and node.func.attr == "exception":
                hit = "logger.exception"
            elif isinstance(node, ast.keyword) and node.arg == "exc_info":
                hit = "exc_info"
            if hit is None:
                continue
            line = lines[node.lineno - 1] if hasattr(node, "lineno") else ""
            if any(path == p and form in line for p, form in _ALLOWED):
                continue
            found.append(f"{path}:{getattr(node, 'lineno', '?')}: {hit}")
    return found


class TestTheGuard:
    def test_no_exception_text_past_the_rule_in_app(self) -> None:
        found = []
        for path in sorted(_APP.rglob("*.py")):
            rel = path.relative_to(_APP.parent).as_posix()
            found += _offences(path.read_text(encoding="utf-8"), rel)
        assert found == []

    @pytest.mark.parametrize("body", [
        "try:\n    f()\nexcept Exception as x:\n    logger.error('e', error=str(x))\n",
        "try:\n    f()\nexcept Exception as x:\n    msg = str(x)\n"
        "    logger.error('e', error=msg)\n",
        "try:\n    f()\nexcept Exception as x:\n    logger.error(f'failed: {x}')\n",
        "try:\n    f()\nexcept Exception as x:\n    logger.error('e', error=repr(x))\n",
        "try:\n    f()\nexcept Exception as x:\n    logger.error('e', a=x.args)\n",
        "try:\n    f()\nexcept Exception as x:\n    logger.error('%s' % x)\n",
        "try:\n    f()\nexcept Exception as x:\n    logger.error('{}'.format(x))\n",
        "try:\n    f()\nexcept Exception as x:\n    logger.exception('e')\n",
        "try:\n    f()\nexcept Exception as x:\n    logger.error('e', exc_info=True)\n",
        "try:\n    f()\nexcept Exception as x:\n    t = traceback.format_exc()\n",
    ], ids=["str", "via-variable", "f-string", "repr", "args", "percent", "format",
            "logger.exception", "exc_info", "traceback"])
    def test_each_form_is_red(self, body: str) -> None:
        assert _offences(body, "tests/test_db_error_text.py") != []

    @pytest.mark.parametrize("body", [
        "try:\n    f()\nexcept Exception as x:\n    logger.error('e', exception="
        "sanitized_traceback(x))\n",
        "try:\n    f()\nexcept Exception as x:\n    logger.error('e', error="
        "exception_text(x))\n",
        "try:\n    f()\nexcept Exception as x:\n    logger.error('e', error="
        "pipeline_error_of(x))\n",
    ], ids=["traceback", "one-line", "class-and-place"])
    def test_the_pair_the_rules_forms_are_green(self, body: str) -> None:
        assert _offences(body, "tests/test_db_error_text.py") == []


def test_the_allowed_list_is_still_needed() -> None:
    """Every entry of _ALLOWED still matches a place in app/ -- an entry
    nothing needs any more is a hole kept open."""
    texts = {p.relative_to(_APP.parent).as_posix(): p.read_text(encoding="utf-8")
             for p in _APP.rglob("*.py")}
    for path, form in _ALLOWED:
        assert form in texts.get(path, ""), (path, form)



