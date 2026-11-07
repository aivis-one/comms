# =============================================================================
# COMMS Service -- Alembic env.py
# =============================================================================
#
# Alembic migration environment (cbshome pattern). Imports all ORM
# models so that Base.metadata contains the full schema for
# autogenerate.
#
# RULE: add imports as modules are created.
# RULES (FIX TX-3), held by tests/test_migration_safety.py for every
# migration after 0036 -- different for the two directions, because
# alembic moves alembic_version only after the step returns:
#   upgrade():   no check of existing rows under a lock that stops writes --
#     a CHECK or FOREIGN KEY on an existing table is added NOT VALID and
#     VALIDATEd in an autocommit_block; SET NOT NULL only after such a
#     validated CHECK (col IS NOT NULL). What precedes the block is
#     committed before the stamp, so it is IDEMPOTENT (IF NOT EXISTS,
#     DROP ... IF EXISTS first): a failure later leaves the stamp BEHIND the
#     schema, and running upgrade again goes through.
#   downgrade(): NO autocommit_block -- one transaction with the stamp. A
#     partial commit there leaves the stamp AHEAD of the schema, and nothing
#     gets out of that; a rare manual downgrade may take its full pass.
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
from app.core.schema_objects import include_object

# Phase 1: notification engine
from app.engine.models import Notification, NotificationDelivery  # noqa: F401

# Phase 4a: messaging data layer (threads / messages / read state /
# sections)
from app.messaging.models import (  # noqa: F401
    Message,
    Section,
    SectionMember,
    Thread,
    ThreadReadState,
)

# ---------------------------------------------------------------------------
#
# AUTOGENERATE FILTER (R-3). Schema invariants are declared in the
# migrations, not in __table_args__, so Base.metadata does not carry
# them and autogenerate would offer to DROP them -- ten index removals
# on a schema at head, four of them uniqueness that products depend on.
# include_object mutes exactly those objects, by name, and nothing
# else; the list and the reasoning live in app/core/schema_objects.py.
#
# It is passed to BOTH configure calls. In the offline branch it is
# inert today -- autogenerate needs a connection, so --sql mode never
# consults it -- and it is there so that the two configurations cannot
# drift apart, not because it fires.

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
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:  # type: ignore[no-untyped-def]
    """Run migrations with a live connection."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=include_object,
        # What `alembic check` compares, said here rather than left to
        # alembic's defaults (REV-5): a column's type (the default since
        # alembic 1.12) and its SERVER DEFAULT (off by default) -- a
        # model whose default drifted from the database's is a red check,
        # not a silent difference.
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create async engine and run migrations."""
    # A MIGRATION WAITS FOR A LOCK AT MOST 10 s (TX-4). Its DDL takes an
    # ACCESS EXCLUSIVE lock on a live table; queued behind a long
    # transaction, it would hold every query of that table queued behind
    # ITSELF, without a bound. With lock_timeout it fails instead (55P03),
    # its transaction rolls back whole, comms-app does not start --
    # `comms-deploy.sh update` says so, with the dump it made first --
    # and the container's restart policy tries the migration again.
    # Set on the migration's own connection (server_settings at
    # connect), not in a transaction it could leave with.
    connectable = create_async_engine(
        settings.database_url,
        # DB-1: no bound value in an error's text ([parameters]).
        hide_parameters=True,
        connect_args={"server_settings": {"lock_timeout": "10s"}},
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
