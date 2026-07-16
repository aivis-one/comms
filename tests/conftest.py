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
#   No env fiddling needed -- defaults align for tests:
#     APP_ENV=development  -> local DATABASE_URL default
#     CHANNELS_MODE=stub   -> nothing ever leaves the process
#     TEMPLATES_DIR empty  -> startup profile load is a no-op in dev;
#                             the stub_profile fixture installs the
#                             fixture profile explicitly per test
#   CI overrides DATABASE_URL via workflow env.
# =============================================================================

from __future__ import annotations

import subprocess
import sys
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import CategoryMute, GroupMembership, Recipient
from app.core.database import dispose_engine, get_session_factory
from app.engine.models import Notification, NotificationDelivery
from app.messaging.models import Message, Section, Thread, ThreadReadState
from app.profile.loader import FileProfileSource, install_profile, load_profile
from app.profile.registry import registry

REPO_ROOT = Path(__file__).resolve().parents[1]

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
