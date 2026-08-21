# =============================================================================
# COMMS Service -- pytest conftest
# =============================================================================
#
# TEST DB CONTRACT (handoff item 6):
#   - Tests run against an EMPTY database (fresh CI service container /
#     clean local db). The schema is brought up ONLY via
#     `alembic upgrade head` -- never create_all. The session-scoped
#     fixture below shells out to the same alembic CLI the VPS uses.
#   - Cross-test isolation: an autouse fixture deletes all comms rows
#     (ORM delete(), FK order) before every test.
#
# STUB PRODUCT PROFILE (handoff item 3 / Phase 2 item 1):
#   Notification types and templates come from the profile registry.
#   Since Phase 2 the test profile lives ON DISK as a real fixture
#   directory (tests/fixtures/profile: types.yaml + templates/) and is
#   loaded through the REAL loader (app/profile/loader.py) -- every
#   test run exercises the same code path the service uses at startup,
#   and the fixture YAML doubles as the reference shape for the
#   product's comms-profile/.
#
# ENVIRONMENT:
#     CHANNELS_MODE=stub   -> nothing ever leaves the process
#     TEMPLATES_DIR empty  -> startup profile load is a no-op in dev;
#                             the stub_profile fixture installs the
#                             fixture profile explicitly per test
#   DATABASE_URL must be set and must name a *_test database -- see the
#   guard below. The development default is NOT usable for tests, and
#   that is deliberate (T-78).
#
# WHY THERE IS A GUARD ON THE DATABASE NAME (T-78):
#   The autouse clean_db fixture below DELETES ALL COMMS ROWS before
#   every test -- threads, messages, recipients, section rosters. Point
#   the suite at the live database and it empties it, quietly and
#   completely, in the first second of the run.
#
#   Nothing structural prevents that from being pointed anywhere: the
#   deployed image ships tests/ AND the dev dependencies, so a bare
#   `docker compose exec comms-app pytest` inside the running stack is
#   one command away, and it would inherit the container's live
#   DATABASE_URL. An IDE run with no environment does the same thing
#   through the development default, which names the dev database.
#
#   So the contract is the database NAME and nothing else: it must end
#   in `_test`. There is no override flag and no escape variable on
#   purpose -- one would be set once "just to run it here" and stay set,
#   and a guard that can be turned off reads as a setting rather than as
#   "you are about to erase production".
# =============================================================================

from __future__ import annotations

import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import CategoryMute, GroupMembership, Recipient
from app.core.config import settings
from app.core.database import dispose_engine, get_session_factory
from app.engine.models import Notification, NotificationDelivery
from app.messaging.models import Message, Section, Thread, ThreadReadState
from app.profile.loader import FileProfileSource, install_profile, load_profile
from app.profile.registry import registry

REPO_ROOT = Path(__file__).resolve().parents[1]

# The one rule. A database whose name does not end in this is not a test
# database, whoever says otherwise.
TEST_DB_SUFFIX = "_test"


def require_test_database(database_url: str) -> str:
    """Return the target database name, or refuse to let the run start.

    Takes the URL as an argument rather than reading settings itself, so
    the rule can be exercised directly against every shape of URL a
    caller can produce (tests/test_db_guard.py) instead of only against
    whatever this machine happens to be configured with.

    A malformed URL is a REFUSAL, not a pass: an exception on the way to
    reading the name would otherwise be the one path that skips the
    check entirely.
    """
    try:
        name = make_url(database_url).database or ""
    except ArgumentError as exc:
        raise pytest.UsageError(
            f"Refusing to run the test suite: DATABASE_URL is not a URL "
            f"this suite can read a database name out of ({exc}). The "
            f"name must end with {TEST_DB_SUFFIX!r}."
        ) from exc

    if not name.endswith(TEST_DB_SUFFIX):
        raise pytest.UsageError(
            f"Refusing to run the test suite against database {name!r}: "
            f"it is not a test database (the name must end with "
            f"{TEST_DB_SUFFIX!r}). This suite DELETES ALL ROWS before "
            f"every test, so running it here would empty that database. "
            f"Provision and target 'comms_test' -- "
            f"`deploy/comms-deploy.sh test` does both -- or export "
            f"DATABASE_URL yourself pointing at a *_test database."
        )
    return name


def pytest_sessionstart(session: pytest.Session) -> None:
    """Fail-closed guard, before anything else in the run.

    A session hook rather than a fixture: it fires once, before
    collection, before apply_migrations, before clean_db, and it fires
    the same way whether the run is the whole suite, one file, or one
    test. A fixture could be skipped by selecting tests that do not use
    it; this cannot.

    Reads settings.database_url, NOT os.environ, and that is the whole
    point: with DATABASE_URL unset the settings layer substitutes the
    development default, which names the LIVE dev database. A guard
    reading raw environment would see nothing there and stay silent in
    exactly the case it exists for.
    """
    require_test_database(settings.database_url)

# ---------------------------------------------------------------------------
# Fixture product profile (loaded from disk through the real loader)
# ---------------------------------------------------------------------------

FIXTURE_PROFILE_DIR = REPO_ROOT / "tests" / "fixtures" / "profile"

# Loaded once at collection time: a broken fixture profile fails the
# whole run loudly instead of failing every test individually.
_FIXTURE_PROFILE = load_profile(FileProfileSource(FIXTURE_PROFILE_DIR))


# ---------------------------------------------------------------------------
# Schema: alembic upgrade head, once per session (never create_all)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def apply_migrations() -> None:
    """Bring the schema up via the alembic CLI -- same path as the VPS."""
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        cwd=REPO_ROOT,
    )


@pytest.fixture(scope="session", autouse=True)
async def dispose_engine_at_end() -> AsyncGenerator[None, None]:
    """Dispose the lazy engine when the test session ends."""
    yield
    await dispose_engine()


# ---------------------------------------------------------------------------
# Per-test isolation
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
async def clean_db(apply_migrations: None) -> AsyncGenerator[None, None]:
    """Delete all comms rows before each test (ORM delete, FK order)."""
    factory = get_session_factory()
    async with factory() as session:
        # Messaging (Phase 4a) -- child-first, and ALL before Recipient:
        # the client / sender / participant FKs are RESTRICT, so a
        # recipient cannot be deleted while a thread / message /
        # read-state still references it.
        await session.execute(delete(ThreadReadState))
        await session.execute(delete(Message))
        await session.execute(delete(Thread))
        await session.execute(delete(Section))
        await session.execute(delete(NotificationDelivery))
        await session.execute(delete(Notification))
        await session.execute(delete(CategoryMute))
        await session.execute(delete(GroupMembership))
        await session.execute(delete(Recipient))
        await session.commit()
    yield


@pytest.fixture(autouse=True)
def stub_profile() -> Generator[None, None, None]:
    """Install the on-disk fixture profile for every test.

    Goes through the REAL loader/installer path, so the registry state
    tests see is exactly what a production startup would produce from
    the same files.
    """
    registry.reset()
    install_profile(_FIXTURE_PROFILE)
    yield
    registry.reset()


# ---------------------------------------------------------------------------
# Common fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """AsyncSession from the app's session factory."""
    factory = get_session_factory()
    async with factory() as session:
        yield session
        await session.rollback()


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """AsyncClient driving the FastAPI app via ASGITransport."""
    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as ac:
        yield ac
