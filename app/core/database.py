# =============================================================================
# COMMS Service -- Database Connection
# =============================================================================
#
# Ported as-is from the cbshome backend (canonical base).
#
# LAZY ENGINE:
#   Engine is NOT created at import time -- created on first call to
#   get_engine(). Prevents "Future attached to a different loop" errors
#   in pytest where the event loop is created after module imports.
#
# SESSIONS:
#   get_db_session() -- read-write: commits on success, rollback on error.
#   get_db_reader()  -- read-only: a READ ONLY transaction, always
#                       rolled back. A write inside it is refused by the
#                       database (SQLSTATE 25006), not by convention.
#
# RULE (P-01): NEVER call session.commit() in routers.
#   get_db_session() commits automatically after yield.
# =============================================================================

import errno
import socket
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum

import structlog
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import (
    DBAPIError,
    IntegrityError,
    InterfaceError,
    OperationalError,
)
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.core.config import settings

logger = structlog.get_logger()


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""


# THE ONE JSONB TYPE OF COMMS (H13). Python None is SQL NULL -- not the
# JSON value `null`, which is what a bare JSONB writes for an explicit
# None, by every write form (an ORM insert or update, Core values()).
# A JSON `null` IS NOT NULL in SQL, so a CHECK or a query that asks
# "no value?" by IS NULL misses it: ck_recipients_tombstone did, and a
# recipient who ever had a schedule could not be forgotten (H13). On a
# NOT NULL column it turns a None from a silent JSON `null` into the
# refusal it should be. Every JSONB column of every model uses it
# (tests/test_json_null.py holds it, by the models' metadata), and each
# carries CHECK ck_<table>_<column>_not_json_null at the database
# (migration 0030), for a writer that goes around the type.
JSON_VALUE = JSONB(none_as_null=True)


class ProcessRole(StrEnum):
    """What a comms process is, for the size of its connection pool (H7).

    Set by the process's own entry point before its first engine
    (set_process_role): app/main.py lifespan (each uvicorn worker),
    app/worker.py main, app/consumer.py main. An engine asked for with
    no role refuses -- a process nobody sized does not get a default.
    """

    API = "api"
    WORKER = "worker"
    CONSUMER = "consumer"


@dataclass(frozen=True)
class Pool:
    """A process's pool: connections kept, and the overflow above them."""

    size: int
    overflow: int

    @property
    def capacity(self) -> int:
        return self.size + self.overflow


# THE CONNECTION BUDGET (H7). Every comms process has its own pool, and
# all of them share one Postgres (comms-postgres, deploy/docker-compose.yml)
# whose max_connections is written in the compose file. Every pool at
# its full overflow, the worker's wake listener, and the connections
# outside any pool (RESERVED_CONNECTIONS) must fit it -- a pool that
# overflowed past it would fail with "too many connections" in
# whichever process asked next, under load, where it is hardest to see.
# tests/test_connection_budget.py holds the sum against the compose
# file, the uvicorn --workers count included.
#   worker 10 + 25 -- its lanes' slots each hold a journal session while
#       they call (app/engine/service.py LANE_CONCURRENCY), each of its
#       lanes' places holds an attempt's session or a pass's selection
#       (LANE_JOBS, P5-L), the maintenance loop holds one: 25 + 9 + 1,
#       the whole of it at once (tests/test_lanes.py). Its wake listener
#       holds one more, on its own engine outside this pool
#       (app/engine/wake.py).
#   api 10 + 10, per uvicorn worker -- one session per request.
#   consumer 5 + 5 -- one event at a time in one session, and the push
#       relay beside it, one session at a time (app/transport): two.
POOLS: dict[ProcessRole, Pool] = {
    ProcessRole.API: Pool(size=10, overflow=10),
    ProcessRole.WORKER: Pool(size=10, overflow=25),
    ProcessRole.CONSUMER: Pool(size=5, overflow=5),
}

# Connections outside every pool: the migration (alembic in comms-app,
# its own engine -- migrations/env.py -- running beside the old worker
# and consumer during an update), an operator's psql or pg_dump
# (comms-deploy.sh db, backup, restore), and the deploy script's own
# asyncpg probe (comms-deploy.sh drain).
RESERVED_CONNECTIONS = 3


# ---------------------------------------------------------------------------
# Lazy singletons -- created on first access, not at import time
# ---------------------------------------------------------------------------
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_role: ProcessRole | None = None


def set_process_role(role: ProcessRole) -> None:
    """Name this process's role; its next engine is sized by it."""
    global _role
    _role = role


def get_engine() -> AsyncEngine:
    """Get or create the async engine (singleton), sized by the process's
    role (POOLS). Raises when no role was set: see ProcessRole."""
    global _engine
    if _engine is None:
        if _role is None:
            raise RuntimeError(
                "no process role: the entry point must call "
                "set_process_role() before the first engine -- its pool "
                "is sized by it (app/core/database.py POOLS)"
            )
        pool = POOLS[_role]
        _engine = create_async_engine(
            settings.database_url,
            echo=False,
            pool_size=pool.size,
            max_overflow=pool.overflow,
            pool_pre_ping=True,
            pool_recycle=1800,
        )
    return _engine


async def dispose_engine() -> None:
    """Dispose engine and reset singletons. Called at shutdown and in tests."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
    _session_factory = None


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Get or create session factory bound to current engine (singleton)."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _session_factory


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Transactional read-write session: committed on success, rolled
    back on failure.

    A ROUTE TAKES IT ONLY AS `DbSession` (app/api/deps.py), never as a bare
    Depends(get_db_session). The commit is this function's code after
    `yield`, and WHEN it runs is the dependency's scope: FastAPI gives a
    yield-dependency without one the scope "request", whose exit stack
    closes after the response has been sent (fastapi/routing.py:
    `await response(...)` inside the request stack, the function stack
    already closed) -- so the product got its 200 before the commit, and
    a commit that failed was a success with no row behind it (TX-1).
    scope="function" closes the stack before the response is sent: the
    commit happens first, and a commit that fails raises into the error
    handlers (app/api/errors.py) and becomes the answer. The response
    body is serialized inside the endpoint's call, before that exit, so
    nothing in it waits on the commit (expire_on_commit=False besides).
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def get_db_reader() -> AsyncGenerator[AsyncSession, None]:
    """Read-only session: its transaction is READ ONLY, and it always
    rolls back.

    SET TRANSACTION is the transaction's first statement -- it must be,
    Postgres refuses it after any query -- so every statement the route
    runs is in a transaction the database itself keeps from writing
    (25006 read_only_sql_transaction). A reader that starts to write is
    a failure where it is written, not a write that a rollback quietly
    undoes.
    """
    factory = get_session_factory()
    session = factory()
    try:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        yield session
    finally:
        await session.rollback()
        await session.close()


# -----------------------------------------------------------------------------
# The insert race (D1 / R2, H13) -- one rule for every writer of the kind
# -----------------------------------------------------------------------------

async def once_more_on_insert_race[T](
    session: AsyncSession,
    apply: Callable[[], Awaitable[T]],
    *,
    event: str,
    **fields: str,
) -> T:
    """Run `apply` under a SAVEPOINT; on a UNIQUE VIOLATION run it once
    more, outside one.

    The SAVEPOINT keeps the caller's transaction usable after the
    loser's INSERT fails. The winner has committed by then (the loser's
    INSERT waited on its lock), so the SAME rule run again finds the
    winner's row and decides against it: a replay, a stale or a newer
    version -- exactly as without the race. Once: a second collision
    would mean the row both exists and does not, and it is raised.

    ONLY THE RACE IS RETRIED (H13): the race is two INSERTs of one key,
    and its one signature is a unique violation (SQLSTATE 23505). Any
    other IntegrityError -- a CHECK, a foreign key, a NOT NULL -- is a
    row this write must not make, raised at once: no `<event>` line (it
    is not a race, and a log saying so sent the reader of the
    tombstone's CHECK violation after a race that never was) and no
    second run that could only fail the same way. The caller's
    transaction stays usable either way: the SAVEPOINT is rolled back.

    WHERE IT LIVES, AND WHY HERE: the rule is one for every writer of
    the kind -- app/audience/sync.py and prefs.py, app/messaging
    sections.py and membership.py (G1). It stood in app/audience/sync.py
    while audience alone used it, made public there rather than copied
    (a private name imported across modules is what D1 / R7 forbids,
    tests/test_hygiene_r7.py). Messaging never imports app.audience
    (app/messaging/__init__.py, the package DAG), so the rule moved
    below both, here, rather than into a copy.
    """
    try:
        async with session.begin_nested():
            return await apply()
    except IntegrityError as exc:
        if not is_unique_violation(exc):
            raise
        logger.info(event, **fields)
        return await apply()


# SQLSTATE of a unique violation -- the insert race's one signature.
UNIQUE_VIOLATION = "23505"


# -----------------------------------------------------------------------------
# A transient database failure (TX-1) -- one rule, by code and class
# -----------------------------------------------------------------------------

# The measure: the SAME request, repeated unchanged, may succeed. By the
# SQLSTATE the driver reports and by the class of the exception -- never
# by a message's text.
_TRANSIENT_SQLSTATE_CLASSES = frozenset({
    "40",  # transaction rollback: 40001 serialization, 40P01 deadlock
    "08",  # connection exception: 08003 the connection died mid-query
})
_TRANSIENT_SQLSTATES = frozenset({
    "57P01",  # admin_shutdown -- a restart, or pg_terminate_backend
    "57P02",  # crash_shutdown
    "57P03",  # cannot_connect_now -- the server is starting
    "53300",  # too_many_connections -- an overload, and it passes
})


# The errno of a NETWORK failure on the way to the database (TX-1 FIX):
# what an OSError of the socket says when the connection could not be
# made or was lost. Each one is a state of the network or of the
# server's listener, gone on a repeat; none is a file, a right or an
# argument of ours:
#   ECONNREFUSED  -- nothing listens yet (the server restarting);
#   ECONNRESET    -- the server or a middlebox dropped the connection;
#   ECONNABORTED  -- the connection aborted on our side of the network;
#   EHOSTUNREACH  -- no route to the host now (a container not up yet);
#   EHOSTDOWN     -- the host reported down (the same, said otherwise);
#   ENETUNREACH   -- no route to the network now;
#   ENETDOWN      -- the interface down (a network being re-created);
#   ETIMEDOUT     -- the kernel's own connect timeout;
#   EADDRNOTAVAIL -- no local address or port to connect from right
#                    now (ephemeral ports exhausted, an interface
#                    still coming up).
# The first three and ETIMEDOUT usually come as ConnectionError and
# TimeoutError subclasses already -- Python maps them -- and are listed
# so a plain OSError carrying them is not missed.
_NETWORK_ERRNOS = frozenset({
    errno.ECONNREFUSED, errno.ECONNRESET, errno.ECONNABORTED,
    errno.EHOSTUNREACH, errno.EHOSTDOWN, errno.ENETUNREACH, errno.ENETDOWN,
    errno.ETIMEDOUT, errno.EADDRNOTAVAIL,
})


def _network_failure(exc: BaseException) -> bool:
    """Whether an OS-level error is a failure of the network on the way
    to the database -- by its class and its errno, never its text:
      - ConnectionError (refused, reset, aborted, a broken pipe) and
        TimeoutError (the connect timed out);
      - socket.gaierror -- the database's name did not resolve;
      - an OSError whose errno is in _NETWORK_ERRNOS;
      - THE COMBINED FAILURE OF SEVERAL ADDRESSES: asyncio's
        BaseEventLoop.create_connection (Python 3.12), when every
        address of a name refused and their texts differ, raises a new
        `OSError('Multiple exceptions: ...')` -- exactly OSError, no
        errno, the per-address errors dropped. The type is checked by
        EQUALITY, not isinstance: every subclass of OSError that means
        something else (FileNotFoundError, PermissionError, ...) is
        then outside it, and so is any OSError carrying an errno -- a
        bare OSError with no errno is what asyncio builds, and nothing
        on a route's or handle_event's path raises one otherwise. On the
        stand `localhost` resolves to ::1 and 127.0.0.1, and a refused
        connection came as this one, which the rule missed.
    NOT a network failure: any other OSError -- a file not found, a
    right refused, a directory, a would-block, an errno outside the set.
    """
    if isinstance(exc, (ConnectionError, TimeoutError, socket.gaierror)):
        return True
    if not isinstance(exc, OSError):
        return False
    if type(exc) is OSError and exc.errno is None:
        return True
    return exc.errno in _NETWORK_ERRNOS


def _sqlstate(exc: BaseException) -> str | None:
    """The SQLSTATE of a database error: the code SQLAlchemy's asyncpg
    adapter copies onto `orig` (pgcode), or the driver's own."""
    if isinstance(exc, DBAPIError):
        code = getattr(exc.orig, "pgcode", None)
    else:
        code = getattr(exc, "sqlstate", None)
    return code if isinstance(code, str) else None


def is_transient_db_error(exc: BaseException) -> bool:
    """Whether a failure is one the same request, repeated unchanged, may
    get past: the database or the way to it, not the request. ONE RULE
    (TX-1) -- the consumer retries by it (app/transport/consumer.py) and
    a route answers 503 by it (app/api/errors.py).

    TRANSIENT:
      - SQLSTATE of class 40 (a serialization failure, a deadlock) or
        class 08 (the connection gone in the middle of a statement);
      - 57P01 / 57P02 / 57P03 (the server shut down, crashed, or is
        starting -- a restart, measured as 57P0x, not as class 08) and
        53300 (too many connections);
      - the connection already gone when a statement is sent: SQLAlchemy's
        InterfaceError -- the driver's own InterfaceError, which carries
        no SQLSTATE (measured: a backend terminated while idle);
      - OperationalError: PEP 249's "not necessarily under the control of
        the programmer". asyncpg's adapter never raises one (its
        translation table has none); it stays by its definition;
      - the connection never opened or the network failed on the way:
        an OS-level error SQLAlchemy does not wrap, read by its class
        and errno (_network_failure -- the combined failure of several
        addresses included) -- and the pool's own timeout
        (sqlalchemy.exc.TimeoutError), an overload that passes.
    NOT TRANSIENT: everything else -- a constraint (class 23), bad data
    (22), a statement or a right (42), a cancelled statement (57014).
    NOT IN THE RULE: 55P03 lock_not_available. Nothing in comms can
    reach it -- no NOWAIT and no lock_timeout anywhere; SKIP LOCKED skips
    rather than fails (the TX-1 report).

    A FALSE TRANSIENT IS A BUG OF OURS, BOUNDED. The driver's
    InterfaceError is also what it raises when code misuses a
    connection ("another operation is in progress"). Retrying that
    cannot help, and does not hide it: the consumer retries it a bounded
    number of times and then dead-letters it, "retries exhausted" with
    its class and place, and a route answers 503 -- repeated, it fails
    the same way. Telling the two apart would mean reading the message.
    """
    if isinstance(exc, (InterfaceError, OperationalError, PoolTimeoutError)):
        return True
    if _network_failure(exc):
        return True
    code = _sqlstate(exc)
    if code is None:
        return False
    return code[:2] in _TRANSIENT_SQLSTATE_CLASSES or code in _TRANSIENT_SQLSTATES


def is_unique_violation(exc: IntegrityError) -> bool:
    """Whether an IntegrityError is a unique violation: by the SQLSTATE
    the driver reports (`pgcode` of the DBAPI error), never by the
    message text."""
    return getattr(exc.orig, "pgcode", None) == UNIQUE_VIOLATION


def is_unique_violation_of(exc: IntegrityError, *names: str) -> bool:
    """Whether an IntegrityError is a unique violation of one of
    `names` -- the writer's OWN unique indexes: the insert race of its
    key, and nothing else.

    By the SQLSTATE and by the name of the violated constraint as the
    driver reports it in a field, never by the message text (G5). The
    text carries the DETAIL, and the DETAIL carries VALUES: a key, a
    subject id, a whole failing row for a CHECK or a NOT NULL. A value
    that spells one of our index names would make another violation
    read as our race -- the replay would find no winner, or worse, a
    holder that has nothing to do with this error.

    WHERE THE NAME COMES FROM -- an adapter behaviour, not its API.
    `exc.orig` is SQLAlchemy's asyncpg adapter error; it copies the
    SQLSTATE (`pgcode`) but not the constraint name. The name is on the
    asyncpg exception the adapter chains as `__cause__` (`raise
    translated_error from error`, sqlalchemy/dialects/postgresql/
    asyncpg.py _handle_exception), as `constraint_name` -- filled for a
    unique INDEX as well as a UNIQUE constraint (measured on the pinned
    asyncpg 0.31.0 / SQLAlchemy 2.0.54, requirements.lock). An upgrade
    that stops chaining it turns every race into a raised error, loud,
    never into a swallowed one: each writer's own-race test on a real
    session (tests/test_race_by_constraint.py) is the sentinel.

    No name (an error without that cause, or a cause without the field)
    is not our race: the error is raised -- red is better than silently
    wrong.
    """
    if not names:
        raise ValueError("is_unique_violation_of needs at least one name")
    if not is_unique_violation(exc):
        return False
    cause = getattr(exc.orig, "__cause__", None)
    return getattr(cause, "constraint_name", None) in names
