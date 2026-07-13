# =============================================================================
# COMMS Service -- Sync receiver tests (Phase 2 item 4)
# =============================================================================
# Service-level receivers for the product's `user_upserted` /
# `group_changed` events (transport is Phase 3; these functions ARE
# the contract). Covered:
#   - upsert creates / updates / never duplicates
#   - re-sync does NOT touch comms-owned preference fields
#   - active=False drops the recipient out of USER resolution
#   - group add/remove is idempotent both ways
#   - group add for an unsynced recipient is a sync-ordering error
#   - resolver visibility over synced data: USER / GROUP / ALL
# =============================================================================

from datetime import time
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import GroupMembership, Recipient
from app.audience.sync import group_changed, user_upserted
from app.core.exceptions import NotFoundError
from app.engine.constants import TargetType
from app.engine.resolver import resolve_targets
from tests.helpers import next_phase2_telegram_id


async def _recipient_count(session: AsyncSession) -> int:
    result = await session.execute(
        select(func.count()).select_from(Recipient)
    )
    return result.scalar_one()


class TestUserUpserted:
    """Identity sync: create, update, idempotency, pref ownership."""

    async def test_creates_recipient(self, db_session: AsyncSession) -> None:
        """First event for an id creates the projection row."""
        recipient_id = uuid4()
        tg = next_phase2_telegram_id()
        recipient = await user_upserted(
            db_session,
            recipient_id=recipient_id,
            telegram_id=tg,
            email="alpha@example.test",
            locale="ru",
            active=True,
        )
        assert recipient.id == recipient_id
        assert recipient.telegram_id == tg
        assert recipient.email == "alpha@example.test"
        assert recipient.locale == "ru"
        assert recipient.active is True

    async def test_updates_in_place_without_duplicates(
        self, db_session: AsyncSession,
    ) -> None:
        """A repeat event updates the same row; count stays 1."""
        recipient_id = uuid4()
        await user_upserted(
            db_session,
            recipient_id=recipient_id,
            telegram_id=next_phase2_telegram_id(),
            email=None,
            locale="en",
            active=True,
        )
        new_tg = next_phase2_telegram_id()
        updated = await user_upserted(
            db_session,
            recipient_id=recipient_id,
            telegram_id=new_tg,
            email="beta@example.test",
            locale="de",
            active=False,
        )
        assert updated.telegram_id == new_tg
        assert updated.email == "beta@example.test"
        assert updated.locale == "de"
        assert updated.active is False
        assert await _recipient_count(db_session) == 1

    async def test_resync_does_not_touch_preferences(
        self, db_session: AsyncSession,
    ) -> None:
        """Ownership boundary: sync writes only its five fields."""
        recipient_id = uuid4()
        recipient = await user_upserted(
            db_session,
            recipient_id=recipient_id,
            telegram_id=next_phase2_telegram_id(),
            email=None,
            locale="en",
            active=True,
        )
        # Comms-owned preference fields, set outside the sync path.
        recipient.timezone = "Europe/Berlin"
        recipient.quiet_from = time(22, 0)
        recipient.quiet_to = time(8, 0)
        recipient.quiet_days = [5, 6]
        await db_session.flush()

        await user_upserted(
            db_session,
            recipient_id=recipient_id,
            telegram_id=next_phase2_telegram_id(),
            email="resync@example.test",
            locale="ru",
            active=True,
        )
        assert recipient.timezone == "Europe/Berlin"
        assert recipient.quiet_from == time(22, 0)
        assert recipient.quiet_to == time(8, 0)
        assert recipient.quiet_days == [5, 6]
        # ...while the sync fields did change.
        assert recipient.locale == "ru"

    async def test_deactivated_recipient_leaves_user_resolution(
        self, db_session: AsyncSession,
    ) -> None:
        """active=False from the product hides the recipient."""
        recipient_id = uuid4()
        await user_upserted(
            db_session,
            recipient_id=recipient_id,
            telegram_id=next_phase2_telegram_id(),
            email=None,
            locale="en",
            active=True,
        )
        assert await resolve_targets(
            db_session, TargetType.USER, str(recipient_id),
        ) == [recipient_id]

        await user_upserted(
            db_session,
            recipient_id=recipient_id,
            telegram_id=next_phase2_telegram_id(),
            email=None,
            locale="en",
            active=False,
        )
        assert await resolve_targets(
            db_session, TargetType.USER, str(recipient_id),
        ) == []


class TestGroupChanged:
    """Membership sync: idempotent add/remove, ordering guard."""

    async def _synced_recipient(self, session: AsyncSession) -> Recipient:
        return await user_upserted(
            session,
            recipient_id=uuid4(),
            telegram_id=next_phase2_telegram_id(),
            email=None,
            locale="en",
            active=True,
        )

    async def test_add_is_idempotent(self, db_session: AsyncSession) -> None:
        """Replayed add events leave exactly one membership row."""
        recipient = await self._synced_recipient(db_session)
        for _ in range(2):
            await group_changed(
                db_session,
                group_key="practice_42",
                recipient_id=recipient.id,
                member=True,
            )
        count = await db_session.execute(
            select(func.count()).select_from(GroupMembership).where(
                GroupMembership.group_key == "practice_42",
            )
        )
        assert count.scalar_one() == 1

    async def test_remove_is_idempotent(
        self, db_session: AsyncSession,
    ) -> None:
        """Removing twice (or removing a non-member) is a no-op."""
        recipient = await self._synced_recipient(db_session)
        await group_changed(
            db_session,
            group_key="practice_42",
            recipient_id=recipient.id,
            member=True,
        )
        for _ in range(2):
            await group_changed(
                db_session,
                group_key="practice_42",
                recipient_id=recipient.id,
                member=False,
            )
        assert await resolve_targets(
            db_session, TargetType.GROUP, "practice_42",
        ) == []

    async def test_add_unknown_recipient_is_ordering_error(
        self, db_session: AsyncSession,
    ) -> None:
        """group_changed before user_upserted raises NotFoundError."""
        with pytest.raises(NotFoundError, match="user_upserted"):
            await group_changed(
                db_session,
                group_key="practice_42",
                recipient_id=uuid4(),
                member=True,
            )


class TestResolverSeesSyncedData:
    """Item 4 done-when: USER/GROUP/ALL resolve over synced rows."""

    async def test_user_group_all_visibility(
        self, db_session: AsyncSession,
    ) -> None:
        """One synced flow feeds all three resolver paths."""
        alpha = await user_upserted(
            db_session,
            recipient_id=uuid4(),
            telegram_id=next_phase2_telegram_id(),
            email=None,
            locale="en",
            active=True,
        )
        bravo = await user_upserted(
            db_session,
            recipient_id=uuid4(),
            telegram_id=next_phase2_telegram_id(),
            email=None,
            locale="en",
            active=True,
        )
        await group_changed(
            db_session,
            group_key="role_master",
            recipient_id=alpha.id,
            member=True,
        )

        assert await resolve_targets(
            db_session, TargetType.USER, str(bravo.id),
        ) == [bravo.id]
        assert await resolve_targets(
            db_session, TargetType.GROUP, "role_master",
        ) == [alpha.id]
        assert set(
            await resolve_targets(db_session, TargetType.ALL, "*")
        ) == {alpha.id, bravo.id}
