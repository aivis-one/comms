# =============================================================================
# COMMS Service -- In-app inbox / read-state tests (service level)
# =============================================================================
# The cbshome Sprint 8.3 read_at/badge machinery, ported at the service
# layer. The HTTP surface for the product is Phase 3 transport work.
# =============================================================================

from typing import Any
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
) -> tuple[Any, list[Any]]:
    """Recipient with N sent deliveries + 1 pending.

    action_data carries BOTH a navigational intent and a template-
    variable key ("extra") -- the listing must return only the former
    (Phase 3b amendment A). sent_at values are made DISTINCT (one
    second apart, oldest first) so ordering/keyset assertions are
    deterministic beyond the id tie-breaker.
    """
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
            action_data={
                "action": "open_unit",
                "params": {"unit_id": str(index)},
                "extra": f"x{index}",
            },
        )
        created = await resolve_notification(session, notification)
        deliveries.extend(created)

    # Mark the first N as sent, leave the last pending.
    from datetime import UTC, datetime, timedelta

    base = datetime.now(UTC)
    for index, delivery in enumerate(deliveries[:count]):
        delivery.status = DeliveryStatus.SENT
        delivery.sent_at = base + timedelta(seconds=index)
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
        items, next_cursor = await list_recipient_deliveries(
            db_session, recipient.id,
        )

        assert next_cursor is None  # both rows fit on one page
        assert len(items) == 2
        for item in items:
            assert item["status"] == DeliveryStatus.SENT
            assert item["title"].startswith("Title")
            assert item["body"].startswith("Body")
            assert item["type"] == "unit_event"
            # Amendment A: only the navigational intent survives --
            # template variables ("extra") and internal keys never
            # leave the service.
            assert item["action_data"] == {
                "action": "open_unit",
                "params": {"unit_id": item["body"][-1]},
            }

    async def test_channel_filter(self, db_session: AsyncSession) -> None:
        recipient, _ = await _seed_sent_deliveries(db_session, count=2)
        items, next_cursor = await list_recipient_deliveries(
            db_session, recipient.id, channel_filter="telegram",
        )
        assert next_cursor is None
        assert items == []

    async def test_no_action_means_null_intent(
        self, db_session: AsyncSession,
    ) -> None:
        """action_data without an "action" key -> item not tappable."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["in_app"],
            action_data={"extra": "template variable only"},
        )
        created = await resolve_notification(db_session, notification)
        from datetime import UTC, datetime

        created[0].status = DeliveryStatus.SENT
        created[0].sent_at = datetime.now(UTC)
        await db_session.flush()

        items, _ = await list_recipient_deliveries(
            db_session, recipient.id,
        )
        assert len(items) == 1
        assert items[0]["action_data"] is None

    async def test_keyset_pagination(
        self, db_session: AsyncSession,
    ) -> None:
        """Walk 3 rows in pages of 2: newest-first, no repeats,
        cursor exhausts to None."""
        recipient, _ = await _seed_sent_deliveries(db_session, count=3)

        page1, cursor = await list_recipient_deliveries(
            db_session, recipient.id, limit=2,
        )
        assert len(page1) == 2
        assert cursor is not None
        # Newest-first: the seed sends oldest-first, so page 1 opens
        # with the LAST seeded title.
        assert page1[0]["title"] == "Title 2"
        assert page1[1]["title"] == "Title 1"

        page2, cursor2 = await list_recipient_deliveries(
            db_session, recipient.id, limit=2, cursor=cursor,
        )
        assert [item["title"] for item in page2] == ["Title 0"]
        assert cursor2 is None

        ids = [item["id"] for item in page1 + page2]
        assert len(ids) == len(set(ids))  # no row served twice

    async def test_keyset_tie_break_on_equal_sent_at(
        self, db_session: AsyncSession,
    ) -> None:
        """Equal sent_at (a fan-out batch) must not lose or repeat
        rows across the page boundary -- the id tie-breaker is what
        makes the order total."""
        recipient, deliveries = await _seed_sent_deliveries(
            db_session, count=3,
        )
        from datetime import UTC, datetime

        same_moment = datetime.now(UTC)
        for delivery in deliveries[:3]:
            delivery.sent_at = same_moment
        await db_session.flush()

        seen: list[Any] = []
        cursor = None
        while True:
            page, cursor = await list_recipient_deliveries(
                db_session, recipient.id, limit=1, cursor=cursor,
            )
            seen.extend(item["id"] for item in page)
            if cursor is None:
                break
        assert len(seen) == 3
        assert len(set(seen)) == 3


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
