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

from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum

import structlog
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
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
    """Transactional read-write session (auto-commit on success)."""
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
