# =============================================================================
# COMMS Service -- Messaging section tests (Phase 4a, item 5)
# =============================================================================
# get_or_create_section: creates, then finds; idempotent by key; label
# is not overwritten on find. get_section_by_key returns None when
# absent. Concurrent get_or_create for the same key -> one row (the
# unique index is the arbiter).
# =============================================================================

import asyncio

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.messaging.models import Section
from app.messaging.sections import get_or_create_section, get_section_by_key


class TestGetOrCreate:
    async def test_creates_then_finds_same_row(
        self, db_session: AsyncSession
    ) -> None:
        created = await get_or_create_section(
            db_session, key="support", label="Support"
        )
        again = await get_or_create_section(
            db_session, key="support", label="IGNORED LABEL"
        )
        assert again.id == created.id
        # find must not overwrite the label
        assert again.label == "Support"
        count = await db_session.scalar(
            select(func.count()).select_from(Section)
        )
        assert count == 1

    async def test_get_by_key_absent_is_none(
        self, db_session: AsyncSession
    ) -> None:
        assert await get_section_by_key(db_session, key="nope") is None


class TestGetOrCreateRace:
    async def test_concurrent_same_key_single_row(self) -> None:
        """Two concurrent creators for one key converge on one row."""
        factory = get_session_factory()

        async def worker() -> str:
            async with factory() as session:
                section = await get_or_create_section(
                    session, key="billing", label="Billing"
                )
                await session.commit()
                return str(section.id)

        id_a, id_b = await asyncio.gather(worker(), worker())
        assert id_a == id_b

        async with factory() as session:
            count = await session.scalar(
                select(func.count()).select_from(Section)
            )
        assert count == 1
