# =============================================================================
# COMMS Service -- Messaging: Sections (Phase 4a, item 5)
# =============================================================================
#
# Minimal CRUD for the first-class Section (arch doc §2.4). Enough that
# a thread's operator_target=section:<id> points at a real row.
# Operator<->section membership is trivial in v1 (any agent serves
# every section) and is Phase 4b -- NOT built here.
#
# Callers commit (repo session rule).
# =============================================================================

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.models import Section

logger = structlog.get_logger()


async def get_section_by_key(
    session: AsyncSession,
    *,
    key: str,
) -> Section | None:
    """Return the section with this key, or None."""
    section: Section | None = await session.scalar(
        select(Section).where(Section.key == key)
    )
    return section


async def get_or_create_section(
    session: AsyncSession,
    *,
    key: str,
    label: str,
) -> Section:
    """Return the section for `key`, creating it if absent.

    Idempotent by key. The DATABASE is the arbiter under a race: the
    unique index uq_sections_key (migration 0006) turns a concurrent
    second insert into an IntegrityError, caught here and resolved to
    the row the other creator committed (same shape as the 3c dedup in
    app/transport/handlers.py). An existing section is returned as-is;
    `label` is not updated (create-or-find, not upsert).
    """
    existing = await get_section_by_key(session, key=key)
    if existing is not None:
        return existing

    section = Section(key=key, label=label)
    try:
        # SAVEPOINT so a losing race rolls back only this insert and
        # leaves the outer session usable for the caller's commit.
        async with session.begin_nested():
            session.add(section)
            await session.flush()
    except IntegrityError as exc:
        # Only OUR unique index means "someone else created it first";
        # any other integrity error is a real bug.
        if "uq_sections_key" not in str(exc.orig):
            raise
        existing = await get_section_by_key(session, key=key)
        if existing is None:
            raise
        logger.info("section_create_lost_race", key=key)
        return existing

    logger.info("section_created", section_id=str(section.id), key=key)
    return section
