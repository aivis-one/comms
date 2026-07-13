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
# STUB PRODUCT PROFILE (handoff item 3):
#   Notification types and templates come from the profile registry.
#   In Phase 1 the profile is a stub that this conftest registers for
#   every test: a small dictionary of unit-test type keys + en/ru
#   templates. Real per-deploy profiles arrive in Phase 2.
#
# ENVIRONMENT:
#   No env fiddling needed -- defaults align for tests:
#     APP_ENV=development  -> local DATABASE_URL default
#     CHANNELS_MODE=stub   -> nothing ever leaves the process
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

from app.audience.models import GroupMembership, Recipient
from app.core.database import dispose_engine, get_session_factory
from app.engine.models import Notification, NotificationDelivery
from app.engine.registry import registry

REPO_ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# Stub product profile (Phase 1)
# ---------------------------------------------------------------------------

STUB_TYPES = (
    "unit_event",
    "unit_rem_24h",
    "unit_rem_1h",
    "unit_rem_10m",
)

STUB_TEMPLATES_EN = {
    "unit_event": {
        "telegram": {
            "title": "{title}",
            "body": "{body} [{extra}]",
        },
        "email": {
            "subject": "COMMS: {title}",
        },
    },
}

STUB_TEMPLATES_RU = {
    "unit_event": {
        "telegram": {
            "body": "RU: {body}",
        },
    },
}


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
        await session.execute(delete(NotificationDelivery))
        await session.execute(delete(Notification))
        await session.execute(delete(GroupMembership))
        await session.execute(delete(Recipient))
        await session.commit()
    yield


@pytest.fixture(autouse=True)
def stub_profile() -> Generator[None, None, None]:
    """Register the stub product profile for every test."""
    registry.reset()
    registry.register_types(STUB_TYPES)
    registry.register_templates("en", STUB_TEMPLATES_EN)
    registry.register_templates("ru", STUB_TEMPLATES_RU)
    yield
    registry.reset()


# ---------------------------------------------------------------------------
# Common fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_session() -> AsyncGenerator:  # type: ignore[type-arg]
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
