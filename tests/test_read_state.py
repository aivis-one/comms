# =============================================================================
# COMMS Service -- In-app inbox / read-state tests (service level)
# =============================================================================
# The cbshome Sprint 8.3 read_at/badge machinery, ported at the service
# layer. The HTTP surface for the product is Phase 3 transport work.
# =============================================================================

from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundError
from app.engine.constants import DeliveryStatus, TargetType
from app.engine.models import NotificationDelivery
from app.engine.service import (
    create_notification,
    get_unread_count,
    list_recipient_deliveries,
    mark_all_read,
    mark_delivery_read,
    resolve_notification,
)
from tests.helpers import create_recipient


async def _seed_sent_deliveries(
    session: AsyncSession,
    count: int = 2,
) -> tuple:
    """Recipient with N sent deliveries + 1 pending."""
    recipient = await create_recipient(session)
    deliveries = []
    for index in range(count + 1):
        notification = await create_notification(
            session,
            type="unit_event",
            title=f"Title {index}",
            body=f"Body {index}",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["in_app"],
            action_data={"extra": f"x{index}"},
        )
        created = await resolve_notification(session, notification)
        deliveries.extend(created)

    # Mark the first N as sent, leave the last pending.
    from datetime import UTC, datetime

    for delivery in deliveries[:count]:
        delivery.status = DeliveryStatus.SENT
        delivery.sent_at = datetime.now(UTC)
    await session.flush()
    return recipient, deliveries


class TestUnreadCount:
    async def test_counts_only_unread_sent(
        self, db_session: AsyncSession,
    ) -> None:
        recipient, _ = await _seed_sent_deliveries(db_session, count=2)
        assert await get_unread_count(db_session, recipient.id) == 2

    async def test_zero_for_unknown_recipient(
        self, db_session: AsyncSession,
    ) -> None:
        assert await get_unread_count(db_session, uuid4()) == 0


class TestMarkRead:
    async def test_mark_single_read_idempotent(
        self, db_session: AsyncSession,
    ) -> None:
        recipient, deliveries = await _seed_sent_deliveries(db_session)
        target = deliveries[0]

        await mark_delivery_read(db_session, recipient.id, target.id)
        first_read_at = target.read_at
        assert first_read_at is not None
        assert await get_unread_count(db_session, recipient.id) == 1

        # Second call is a no-op.
        await mark_delivery_read(db_session, recipient.id, target.id)
        assert target.read_at == first_read_at

    async def test_foreign_delivery_not_found(
        self, db_session: AsyncSession,
    ) -> None:
        _, deliveries = await _seed_sent_deliveries(db_session)
        stranger = await create_recipient(db_session)
        with pytest.raises(NotFoundError):
            await mark_delivery_read(
                db_session, stranger.id, deliveries[0].id,
            )

    async def test_mark_all_read(self, db_session: AsyncSession) -> None:
        recipient, _ = await _seed_sent_deliveries(db_session, count=2)
        marked = await mark_all_read(db_session, recipient.id)
        assert marked == 2
        assert await get_unread_count(db_session, recipient.id) == 0


class TestListDeliveries:
    async def test_lists_only_sent_with_enrichment(
        self, db_session: AsyncSession,
    ) -> None:
        recipient, _ = await _seed_sent_deliveries(db_session, count=2)
        items, total = await list_recipient_deliveries(
            db_session, recipient.id,
        )

        assert total == 2
        assert len(items) == 2
        for item in items:
            assert item["status"] == DeliveryStatus.SENT
            assert item["title"].startswith("Title")
            assert item["body"].startswith("Body")
            assert item["type"] == "unit_event"
            # Internal "_channels" key is filtered out of action_data.
            assert item["action_data"] is not None
            assert "_channels" not in item["action_data"]
            assert item["action_data"]["extra"].startswith("x")

    async def test_channel_filter(self, db_session: AsyncSession) -> None:
        recipient, _ = await _seed_sent_deliveries(db_session, count=2)
        items, total = await list_recipient_deliveries(
            db_session, recipient.id, channel_filter="telegram",
        )
        assert total == 0
        assert items == []

    async def test_pagination(self, db_session: AsyncSession) -> None:
        recipient, _ = await _seed_sent_deliveries(db_session, count=2)
        items, total = await list_recipient_deliveries(
            db_session, recipient.id, page=2, per_page=1,
        )
        assert total == 2
        assert len(items) == 1


class TestPendingInvisible:
    async def test_pending_delivery_not_listed(
        self, db_session: AsyncSession,
    ) -> None:
        recipient, deliveries = await _seed_sent_deliveries(db_session)
        pending = [
            d for d in deliveries if d.status == DeliveryStatus.PENDING
        ]
        assert pending  # the seed leaves one pending
        items, _ = await list_recipient_deliveries(db_session, recipient.id)
        listed_ids = {item["id"] for item in items}
        assert all(d.id not in listed_ids for d in pending)


class TestOwnDataOnly:
    async def test_lists_scoped_to_recipient(
        self, db_session: AsyncSession,
    ) -> None:
        recipient_a, _ = await _seed_sent_deliveries(db_session)
        recipient_b, _ = await _seed_sent_deliveries(db_session)

        items_a, _ = await list_recipient_deliveries(
            db_session, recipient_a.id,
        )
        from sqlalchemy import select

        delivery_ids_b = {
            row[0]
            for row in (
                await db_session.execute(
                    select(NotificationDelivery.id).where(
                        NotificationDelivery.recipient_id == recipient_b.id
                    )
                )
            ).all()
        }
        assert all(item["id"] not in delivery_ids_b for item in items_a)
