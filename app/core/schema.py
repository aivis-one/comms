# =============================================================================
# COMMS Service -- the schema a process runs on is its code's (W6-2b)
# =============================================================================
#
# The schema is the deploy machine's step `migrate`, run apart from the
# processes (primer.yaml); comms-worker and comms-consumer wait for their
# database only (deploy/docker-compose.yml). So a process can meet a
# schema that is not its code's: after a red `migrate` (the stamp behind
# the schema, tests/test_migration_chain.py) or from an image older than
# the database. Every process compares the two:
#   - the code's head -- the head of the migrations the image carries
#     (code_head), the one the machine reads with `alembic heads` in the
#     same image;
#   - the database's revision -- alembic_version (read_revision), one
#     query, a SQLAlchemy expression, not a raw-SQL text() string, per
#     the no-raw-SQL house rule.
# What each does when they differ:
#   - the API serves on: /health answers `degraded` with `checks.db`
#     `error`, /ready 503 naming `db` (app/api/report.py); `alembic_head`
#     stays the DATABASE's revision -- the machine's `migrate` compares it
#     with the image's head to decide whether to migrate;
#   - the worker and the consumer refuse to start (refuse_foreign_schema),
#     naming both: the container restarts until the schema is the code's.
# =============================================================================

from functools import cache
from pathlib import Path

import structlog
from alembic.script import ScriptDirectory
from sqlalchemy import column, select, table

from app.core.database import get_engine

logger = structlog.get_logger()

_MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"
"""The image's migrations, by an absolute path: alembic.ini's
script_location is relative to the working directory, and a process need
not start in the repository's root."""

_ALEMBIC_VERSION = table("alembic_version", column("version_num"))


@cache
def code_head() -> str:
    """The head of the migrations this code carries; read once. More heads
    than one, or none, raise: CI and the machine refuse such a tree before
    it is an image."""
    (head,) = ScriptDirectory(str(_MIGRATIONS)).get_heads()
    return head


async def read_revision() -> str:
    """The database's revision, in one query; raises when there is none to
    read (no alembic_version, an empty one)."""
    engine = get_engine()
    async with engine.connect() as conn:
        revision = await conn.scalar(select(_ALEMBIC_VERSION.c.version_num))
    if not isinstance(revision, str) or not revision:
        raise LookupError("alembic_version holds no revision")
    return revision


class ForeignSchemaError(Exception):
    """The database's revision is not the code's head: the outcome of the
    `db` check of /health and /ready, and the reason a process refuses to
    start. Its text is the two revisions, nothing of a letter."""

    def __init__(self, revision: str | None, head: str) -> None:
        self.revision = revision
        self.head = head
        super().__init__(
            f"the schema is at {revision or 'none'}, the code expects {head}",
        )


async def refuse_foreign_schema() -> None:
    """Return when the database's revision is the code's head; else end
    the process naming both (the caller closes what it built). An empty
    alembic_version is a schema at none; a database that does not answer,
    or one with no alembic_version at all, raises its own error -- the
    process ends with it either way, and its restart policy brings it
    back."""
    head = code_head()
    try:
        revision: str | None = await read_revision()
    except LookupError:
        revision = None
    if revision == head:
        return
    foreign = ForeignSchemaError(revision, head)
    logger.error("schema_foreign", schema=revision, expects=head)
    raise SystemExit(
        f"comms: startup refused -- {foreign}: the deploy machine's "
        f"`migrate` brings the schema to the code's head; a process does not "
        f"run on another."
    )
