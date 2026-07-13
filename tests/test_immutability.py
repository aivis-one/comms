# =============================================================================
# COMMS Service -- Immutability tests
# =============================================================================
# Handoff item 2 done-when: notification title/body are IMMUTABLE after
# creation -- enforced by the model, not just documented.
# =============================================================================

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.constants import TargetType
from app.engine.models import Notification
from app.engine.service import create_notification


class TestTitleBodyImmutability:
    async def test_mutation_after_flush_raises(
        self, db_session: AsyncSession,
    ) -> None:
        """Once persisted (create_notification flushes), title/body lock."""
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="Original",
            body="Body",
            target_type=TargetType.ALL,
            target_value="*",
        )

        with pytest.raises(ValueError, match="immutable"):
            notification.title = "Changed"
        with pytest.raises(ValueError, match="immutable"):
            notification.body = "Changed"

        assert notification.title == "Original"
        assert notification.body == "Body"

    async def test_mutation_after_commit_raises(
        self, db_session: AsyncSession,
    ) -> None:
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="Original",
            body="Body",
            target_type=TargetType.ALL,
            target_value="*",
        )
        await db_session.commit()

        with pytest.raises(ValueError, match="immutable"):
            notification.title = "Changed"

    def test_construction_is_free(self) -> None:
        """Before persistence the object is still under construction."""
        notification = Notification(
            type="unit_event",
            title="Draft",
            body="Draft body",
            target_type="all",
            target_value="*",
        )
        notification.title = "Redrafted"
        notification.body = "Redrafted body"
        assert notification.title == "Redrafted"

    async def test_status_stays_mutable(
        self, db_session: AsyncSession,
    ) -> None:
        """The pipeline still owns status transitions."""
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.ALL,
            target_value="*",
        )
        await db_session.commit()
        notification.status = "processing"
        assert notification.status == "processing"
