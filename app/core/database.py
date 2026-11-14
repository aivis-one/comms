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

import asyncio
import errno
import socket
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

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
from sqlalchemy.pool import NullPool

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
#   The worker and the consumer each hold one more, outside their pools,
#   while they ask whether the database answers (database_answers,
#   PROBE_CONNECTIONS): one probe in flight per process at a time.
POOLS: dict[ProcessRole, Pool] = {
    ProcessRole.API: Pool(size=10, overflow=10),
    ProcessRole.WORKER: Pool(size=10, overflow=25),
    ProcessRole.CONSUMER: Pool(size=5, overflow=5),
}

# Connections outside every pool: the migration (alembic, its own engine
# -- migrations/env.py -- in the deploy machine's throwaway container of
# comms-app, its step `migrate`), an operator's psql or pg_dump (the
# machine's `db_dump` among them), and one spare.
RESERVED_CONNECTIONS = 3

# The probe's own connection (database_answers), outside the pool, in
# each process that asks: the worker and the consumer. One per process:
# the probe is single-flight.
PROBE_CONNECTIONS = 1


# THE DATABASE'S LIMITS (W5-3). Every connection of a comms process --
# its pool's, its probe's, the worker's wake listener's -- carries three
# limits the SERVER keeps, as startup parameters of the connection
# (server_settings: a SET at connect runs in a transaction the pool's
# reset rolls back, tests/test_lanes.py), and one the driver keeps:
#   statement_timeout -- the longest one statement may run, its waits
#       for locks included (57014);
#   lock_timeout -- the longest one statement may wait for one lock
#       (55P03); below statement_timeout, or it never fires first;
#   idle_in_transaction_session_timeout -- the longest a transaction may
#       sit between two statements; past it the server ends the session
#       and the next statement fails as a connection gone (InterfaceError);
#   command_timeout (asyncpg) -- the client's: for a server that does not
#       answer at all; TimeoutError. It is the CONNECTION's, not a
#       statement's, so it stands above the longest statement any
#       connection may run: the waits for an attempt (below).
# What each failure becomes is the one rule of transient failures below
# (is_transient_db_error), unchanged: 57014 and 55P03 are not transient
# -- a statement past three times its measure is a defect of comms, said
# loudly; the session ended and the client's timeout are.
#
# THE ATTEMPT'S HOLD. An attempt holds its job's row through the
# channel's call (app/engine/processor.py, THE LOCK RULE): its
# transaction sits idle that long, and whoever waits for the row
# (wait_for_attempts) waits that long. How long is the attempt's, set by
# each entry point after its profile (set_attempt_hold,
# app/engine/service.py longest_attempt_hold); the worker's idle limit
# and the waits are that and WAIT_MARGIN_SECONDS. The migration keeps its
# own: a lock limit, no statement limit -- migrations/env.py.


@dataclass(frozen=True)
class Limits:
    """A process's limits, in seconds. idle None: the attempt's hold and
    WAIT_MARGIN_SECONDS (the worker: its attempts sit idle in their
    transactions through the channel's call)."""

    statement: float
    lock: float
    idle: float | None


# The numbers stand above the measure (MEASURED_SECONDS) by
# MEASURE_MARGIN at least -- tests/test_db_limits.py holds it -- and are
# round: a busy box, and a path the measure did not take.
LIMITS: dict[ProcessRole, Limits] = {
    ProcessRole.API: Limits(statement=10.0, lock=5.0, idle=10.0),
    ProcessRole.WORKER: Limits(statement=30.0, lock=10.0, idle=None),
    ProcessRole.CONSUMER: Limits(statement=30.0, lock=10.0, idle=10.0),
}

# The longest legitimate work of each process on a volume shaped like
# production (tests/measure_db_limits.py SEED: 100 000 jobs, 270 000
# deliveries, 10 000 people), the worst of two measurements of three
# runs each, in seconds; measured 13 November 2026 at primer@8946b73,
# Postgres 16. The worker's is the resolve of a job to everyone (10 000
# deliveries in one transaction); the API's, the lane depth; the
# consumer's, the forgetting of a person.
MEASURED_SECONDS: dict[ProcessRole, float] = {
    ProcessRole.API: 0.108,
    ProcessRole.WORKER: 1.656,
    ProcessRole.CONSUMER: 0.040,
}
MEASURE_MARGIN = 3.0

# Above the attempt's hold: a wait for its row ends with its commit.
WAIT_MARGIN_SECONDS = 30.0

# The client's timeout above the server's longest: the server says why.
CLIENT_MARGIN_SECONDS = 10.0


# ---------------------------------------------------------------------------
# Lazy singletons -- created on first access, not at import time
# ---------------------------------------------------------------------------
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_role: ProcessRole | None = None
_attempt_hold: float | None = None


def set_process_role(role: ProcessRole) -> None:
    """Name this process's role; its next engine is sized by it."""
    global _role
    _role = role


def set_attempt_hold(seconds: float) -> None:
    """Name the longest an attempt holds its job's row (the header): the
    next engine's limits are derived from it."""
    global _attempt_hold
    _attempt_hold = seconds


def wait_seconds() -> float:
    """The longest a statement may wait for an attempt's row: its
    lock_timeout in wait_for_attempts. Raises when no entry point named
    the hold."""
    if _attempt_hold is None:
        raise RuntimeError(
            "no attempt hold: the entry point must call set_attempt_hold() "
            "after its profile, before the first engine -- the limits are "
            "derived from it (app/core/database.py LIMITS)"
        )
    return _attempt_hold + WAIT_MARGIN_SECONDS


def waiting_statement_seconds() -> float:
    """The statement_timeout of a statement that waits for an attempt's
    row (wait_for_attempts): the wait and the margin again for its own
    work -- above the wait, so one wait past the hold fails as the lock
    it is (55P03), as lock_timeout below statement_timeout does
    everywhere (LIMITS)."""
    return wait_seconds() + WAIT_MARGIN_SECONDS


def connect_args(role: ProcessRole) -> dict[str, Any]:
    """The limits of `role` as the driver takes them (the header)."""
    limits = LIMITS[role]
    wait = wait_seconds()
    idle = wait if limits.idle is None else limits.idle
    return {
        "server_settings": {
            "statement_timeout": _ms(limits.statement),
            "lock_timeout": _ms(limits.lock),
            "idle_in_transaction_session_timeout": _ms(idle),
        },
        "command_timeout": (
            max(limits.statement, waiting_statement_seconds())
            + CLIENT_MARGIN_SECONDS
        ),
    }


def named_connect_args(application_name: str) -> dict[str, Any]:
    """connect_args of this process's role, its connection named
    `application_name` in pg_stat_activity: a connection of its own
    outside the pool (the probe, the wake listener) carries the limits
    of the pool's."""
    args = connect_args(_role_set())
    args["server_settings"]["application_name"] = application_name
    return args


def _ms(seconds: float) -> str:
    return f"{round(seconds * 1000)}ms"


def _role_set() -> ProcessRole:
    if _role is None:
        raise RuntimeError(
            "no process role: the entry point must call "
            "set_process_role() before the first engine -- its pool "
            "is sized by it (app/core/database.py POOLS)"
        )
    return _role


async def wait_for_attempts(session: AsyncSession) -> None:
    """Let this transaction's statements wait for an attempt's row: a
    statement that takes a job's row FOR UPDATE and waits (expiry,
    cancellation, forgetting -- app/engine/service.py close_notifications,
    withdraw_recipient) waits for the attempt's commit, as long as the
    attempt's hold, which the process's own limits do not cover. SET
    LOCAL: the transaction's, gone at its end; never a session's SET,
    which would outlive it in the pool.

    KNOWN CEILING (W5-3) -- A WAIT FOR SEVERAL ATTEMPTS IN ONE STATEMENT.
      1. Mechanics: each wait for a lock is bounded by one attempt's hold
         and the margin (lock_timeout); the statement by that and the
         margin again (statement_timeout). A statement that locks several
         jobs in id order waits
         for each one's attempt; an attempt may take a job the statement
         has not reached yet while it waits for an earlier one (the
         attempt's lock SKIPs LOCKED rows, it does not wait), so the
         waits can follow one another, and the statement fail with 57014
         past the limit: the request is answered 500 (the API), the
         event held (the consumer), the sweep's portion rolled back (the
         worker). Nothing is half-done: the transaction rolls back.
      2. Status: acknowledged by design.
      3. Task: none -- not observed; it needs attempts of the same jobs
         one after another for longer than the margin.
      4. Unfreeze when: a 57014 in event_held, database_error_at_api_edge
         or maintenance_pass_error naming close_notifications or
         withdraw_recipient.
      5. Agreed fix: a limit of the number of attempts the statement can
         wait for (its jobs' count) times the hold -- or portions that
         lock one job at a time.
      6. Rejected: no statement_timeout on these transactions (a
         forgotten holder would hang them again -- the reason for W5-3);
         lock_timeout alone (it bounds one lock, not the statement).
    """
    statement = _ms(waiting_statement_seconds())
    await session.execute(text(f"SET LOCAL statement_timeout = '{statement}'"))
    await session.execute(text(f"SET LOCAL lock_timeout = '{_ms(wait_seconds())}'"))


def get_engine() -> AsyncEngine:
    """Get or create the async engine (singleton), sized by the process's
    role (POOLS) and bounded by its limits (LIMITS). Raises when no role
    or no attempt hold was set: see ProcessRole, set_attempt_hold."""
    global _engine
    if _engine is None:
        role = _role_set()
        pool = POOLS[role]
        _engine = create_async_engine(
            settings.database_url,
            # DB-1: no bound value in an error's text ([parameters]).
            hide_parameters=True,
            echo=False,
            pool_size=pool.size,
            max_overflow=pool.overflow,
            pool_pre_ping=True,
            pool_recycle=1800,
            # W5-3: the role's limits (LIMITS).
            connect_args=connect_args(role),
        )
    return _engine


async def dispose_engine() -> None:
    """Dispose engine and reset singletons. Called at shutdown and in tests."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
    _session_factory = None


# DOES THE DATABASE ANSWER (WK-2). ONE PROBE, ONE RULE for the worker and
# the consumer (spec 16.3, decided 1 November 2026): a transient failure
# of the database is waited for, uncounted, only while the database does
# not answer; while it answers, the failure is counted like any other.
# How long "does the database answer" may take to say no -- the WHOLE of
# it: the connection, the statement and the close.
PROBE_TIMEOUT_SECONDS = 5.0

# How the probe's connection names itself in pg_stat_activity.
PROBE_APPLICATION_NAME = "comms-db-probe"

_probe_in_flight: asyncio.Task[bool] | None = None


async def database_answers() -> bool:
    """Whether the database answers a SELECT 1 within PROBE_TIMEOUT_SECONDS,
    on a connection of the probe's own, OUTSIDE the process's pool --
    whatever the failure that made the caller ask.

    OUTSIDE THE POOL: a pool every connection of which is held -- the
    worker's lanes at their places, an event's session -- would make a
    probe from it wait for a connection and say "no" of a database that
    answers; that is a wait without a count, where the owner's rule
    wants the count. Its own connection depends on the database alone.

    BOUNDED AS A WHOLE: the timeout covers the connection's opening (TCP,
    the startup, the authentication), the statement and the close; a
    server that takes the connection and never speaks is a "no" in
    PROBE_TIMEOUT_SECONDS.

    SINGLE-FLIGHT: a caller that finds a probe in flight in this process
    (on this event loop) waits for its answer instead of opening a
    connection of its own -- a burst of failures across a lane's places
    asks the database once, and the process holds at most
    PROBE_CONNECTIONS outside its pool (the connection budget above).
    """
    global _probe_in_flight
    loop = asyncio.get_running_loop()
    probe = _probe_in_flight
    if probe is None or probe.done() or probe.get_loop() is not loop:
        probe = loop.create_task(_probe_once())
        _probe_in_flight = probe
    return await asyncio.shield(probe)


async def _probe_once() -> bool:
    """One probe: its own engine with no pool, used once, disposed."""
    engine = create_async_engine(
        settings.database_url,
        # DB-1: no bound value in an error's text ([parameters]).
        hide_parameters=True,
        poolclass=NullPool,
        connect_args=named_connect_args(PROBE_APPLICATION_NAME),
    )
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_SECONDS):
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
    finally:
        await engine.dispose()


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
    (22), a statement or a right (42), a cancelled statement (57014: the
    statement_timeout of LIMITS among others) and a lock not had in time
    (55P03: lock_timeout of LIMITS). Each limit stands at least three
    times above the measure of the work it bounds, and a wait for an
    attempt's row is lifted to the attempt's hold (wait_for_attempts):
    past them it is a defect of comms, counted and said, not a passing
    state to wait out. NOWAIT is not used; SKIP LOCKED skips rather than
    fails (the TX-1 report). The session ended by
    idle_in_transaction_session_timeout and the driver's command_timeout
    (TimeoutError, _network_failure) are transient, by the rows above.

    A FALSE TRANSIENT IS A BUG OF OURS, BOUNDED. The driver's
    InterfaceError is also what it raises when code misuses a
    connection ("another operation is in progress"). Retrying that
    cannot help, and does not hide it: the consumer retries it a bounded
    number of times and then holds the entry, its class and place said
    (event_held), and a route answers 503 -- repeated, it fails
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
