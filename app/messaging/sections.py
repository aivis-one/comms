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
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import once_more_on_insert_race
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
    second insert into a unique violation, and the insert race's one
    rule (app/core/database.py once_more_on_insert_race) runs the same
    find-or-create once more -- it finds the row the other creator
    committed. Any other integrity error is raised at once. An existing
    section is returned as-is; `label` is not updated (create-or-find,
    not upsert).
    """
    section, created = await once_more_on_insert_race(
        session,
        lambda: _get_or_create_once(session, key=key, label=label),
        event="section_create_lost_race",
        key=key,
    )
    if created:
        logger.info("section_created", section_id=str(section.id), key=key)
    return section


async def _get_or_create_once(
    session: AsyncSession, *, key: str, label: str,
) -> tuple[Section, bool]:
    """The section for `key` and whether THIS call created it."""
    existing = await get_section_by_key(session, key=key)
    if existing is not None:
        return existing, False
    section = Section(key=key, label=label)
    session.add(section)
    await session.flush()
    return section, True
