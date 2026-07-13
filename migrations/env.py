# =============================================================================
# COMMS Service -- Alembic env.py
# =============================================================================
#
# Alembic migration environment (cbshome pattern). Imports all ORM
# models so that Base.metadata contains the full schema for
# autogenerate.
#
# RULE: add imports as modules are created.
# =============================================================================

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

# ---------------------------------------------------------------------------
# Model imports -- required for Alembic autogenerate.
# ---------------------------------------------------------------------------
# Phase 1: audience sync projection
# Phase 2: category mutes (preferences)
from app.audience.models import (  # noqa: F401
    CategoryMute,
    GroupMembership,
    Recipient,
)
from app.core.config import settings
from app.core.database import Base

# Phase 1: notification engine
from app.engine.models import Notification, NotificationDelivery  # noqa: F401

# ---------------------------------------------------------------------------

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Generate SQL without connecting to DB (--sql mode)."""
    url = settings.database_url
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:  # type: ignore[no-untyped-def]
    """Run migrations with a live connection."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create async engine and run migrations."""
    connectable = create_async_engine(settings.database_url)

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
