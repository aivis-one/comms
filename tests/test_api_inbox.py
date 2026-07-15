# =============================================================================
# COMMS Service -- Inbox API tests (Phase 3b item 2) -- frozen contract
# =============================================================================
#
# Drives the /api/v1 inbox surface through the ASGI transport. Seeds
# COMMIT (the request handler opens its own session -- an uncommitted
# seed would be invisible to it); the autouse clean_db fixture wipes
# rows between tests. Auth is disabled by default test config (empty
# token, stub mode) -- guarded separately in test_api_auth.py.
#
# telegram_ids come from the Phase 3b band 84000-84999.
# =============================================================================

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.engine.constants import DeliveryStatus, TargetType
from app.engine.service import create_notification, resolve_notification
from tests.helpers import create_recipient, next_phase3b_telegram_id


async def _seed_inbox(
    count: int = 2,
    *,
    channels: list[str] | None = None,
    action_data: dict[str, Any] | None = None,
) -> tuple[UUID, list[UUID]]:
    """Recipient with N SENT deliveries, committed for the API to see.

    sent_at ascends one second per delivery (oldest first), so
    "newest-first" assertions are deterministic. Returns
    (recipient_id, delivery_ids in seed order).
    """
    factory = get_session_factory()
    async with factory() as session:
        recipient = await create_recipient(
            session, telegram_id=next_phase3b_telegram_id(),
        )
        delivery_ids: list[UUID] = []
        base = datetime.now(UTC)
        for index in range(count):
            notification = await create_notification(
                session,
                type="unit_event",
                title=f"Title {index}",
                body=f"Body {index}",
                target_type=TargetType.USER,
                target_value=str(recipient.id),
                channels=channels or ["in_app"],
                action_data=(
                    action_data
                    if action_data is not None
                    else {
                        "action": "open_unit",
                        "params": {"unit_id": str(index)},
                        "extra": f"template variable {index}",
                    }
                ),
            )
            created = await resolve_notification(session, notification)
            for delivery in created:
                delivery.status = DeliveryStatus.SENT
                delivery.sent_at = base + timedelta(seconds=index)
                delivery_ids.append(delivery.id)
        recipient_id = recipient.id
        await session.commit()
    return recipient_id, delivery_ids


def _inbox(recipient_id: UUID) -> str:
    return f"/api/v1/recipients/{recipient_id}/inbox"


class TestInboxFeed:
    async def test_feed_shape_and_order(self, client: AsyncClient) -> None:
        """The frozen wire form: newest-first items + cursor + badge."""
        recipient_id, _ = await _seed_inbox(count=2)

        response = await client.get(_inbox(recipient_id))
        assert response.status_code == 200
        payload = response.json()

        assert set(payload) == {"items", "next_cursor", "unread"}
        assert payload["next_cursor"] is None
        assert payload["unread"] == 2

        titles = [item["title"] for item in payload["items"]]
        assert titles == ["Title 1", "Title 0"]  # newest-first

        item = payload["items"][0]
        assert set(item) == {
            "id", "type", "title", "body", "action_data",
            "priority", "sent_at", "read_at", "created_at",
        }
        assert item["type"] == "unit_event"
        assert item["read_at"] is None
        # ISO 8601 strings parse back.
        datetime.fromisoformat(item["sent_at"])
        datetime.fromisoformat(item["created_at"])
        # Amendment A: navigational intent ONLY -- the template
        # variable ("extra") never leaves the service.
        assert item["action_data"] == {
            "action": "open_unit", "params": {"unit_id": "1"},
        }

    async def test_unknown_recipient_is_empty_not_404(
        self, client: AsyncClient,
    ) -> None:
        response = await client.get(_inbox(uuid4()))
        assert response.status_code == 200
        assert response.json() == {
            "items": [], "next_cursor": None, "unread": 0,
        }

    async def test_non_in_app_channels_invisible(
        self, client: AsyncClient,
    ) -> None:
        """The inbox is the in_app rows -- a sent telegram delivery
        appears neither in the feed nor in the badge (its read_at
        stays NULL forever and would inflate the counter)."""
        recipient_id, _ = await _seed_inbox(
            count=1, channels=["telegram"],
        )
        response = await client.get(_inbox(recipient_id))
        payload = response.json()
        assert payload["items"] == []
        assert payload["unread"] == 0

    async def test_keyset_walk_via_opaque_cursor(
        self, client: AsyncClient,
    ) -> None:
        recipient_id, _ = await _seed_inbox(count=3)

        response = await client.get(_inbox(recipient_id), params={"limit": 2})
        first = response.json()
        assert len(first["items"]) == 2
        assert first["next_cursor"] is not None

        response = await client.get(
            _inbox(recipient_id),
            params={"limit": 2, "cursor": first["next_cursor"]},
        )
        second = response.json()
        assert [item["title"] for item in second["items"]] == ["Title 0"]
        assert second["next_cursor"] is None

        ids = [i["id"] for i in first["items"] + second["items"]]
        assert len(ids) == len(set(ids))

    async def test_malformed_cursor_is_422(
        self, client: AsyncClient,
    ) -> None:
        response = await client.get(
            _inbox(uuid4()), params={"cursor": "not-a-cursor"},
        )
        assert response.status_code == 422

    async def test_limit_clamped_not_rejected(
        self, client: AsyncClient,
    ) -> None:
        recipient_id, _ = await _seed_inbox(count=2)
        for bad_limit in (0, -5, 100000):
            response = await client.get(
                _inbox(recipient_id), params={"limit": bad_limit},
            )
            assert response.status_code == 200


class TestUnreadCountEndpoint:
    async def test_counts_unread(self, client: AsyncClient) -> None:
        recipient_id, _ = await _seed_inbox(count=2)
        response = await client.get(f"{_inbox(recipient_id)}/unread-count")
        assert response.status_code == 200
        assert response.json() == {"unread": 2}

    async def test_zero_for_unknown_recipient(
        self, client: AsyncClient,
    ) -> None:
        response = await client.get(f"{_inbox(uuid4())}/unread-count")
        assert response.json() == {"unread": 0}


class TestMarkRead:
    async def test_read_one_returns_fresh_badge(
        self, client: AsyncClient,
    ) -> None:
        recipient_id, delivery_ids = await _seed_inbox(count=2)
        response = await client.post(
            f"{_inbox(recipient_id)}/{delivery_ids[0]}/read",
        )
        assert response.status_code == 200
        assert response.json() == {"unread": 1}

        # Idempotent: the second call changes nothing.
        response = await client.post(
            f"{_inbox(recipient_id)}/{delivery_ids[0]}/read",
        )
        assert response.json() == {"unread": 1}

        # read_at now populated in the feed.
        feed = (await client.get(_inbox(recipient_id))).json()
        read_flags = {
            item["id"]: item["read_at"] is not None
            for item in feed["items"]
        }
        assert read_flags[str(delivery_ids[0])] is True
        assert read_flags[str(delivery_ids[1])] is False

    async def test_foreign_delivery_is_404(
        self, client: AsyncClient,
    ) -> None:
        _, delivery_ids = await _seed_inbox(count=1)
        stranger_id, _ = await _seed_inbox(count=1)
        response = await client.post(
            f"{_inbox(stranger_id)}/{delivery_ids[0]}/read",
        )
        assert response.status_code == 404

    async def test_unknown_delivery_is_404(
        self, client: AsyncClient,
    ) -> None:
        recipient_id, _ = await _seed_inbox(count=1)
        response = await client.post(
            f"{_inbox(recipient_id)}/{uuid4()}/read",
        )
        assert response.status_code == 404

    async def test_read_all(self, client: AsyncClient) -> None:
        recipient_id, _ = await _seed_inbox(count=3)
        response = await client.post(f"{_inbox(recipient_id)}/read-all")
        assert response.status_code == 200
        assert response.json() == {"unread": 0}

        feed = (await client.get(_inbox(recipient_id))).json()
        assert all(
            item["read_at"] is not None for item in feed["items"]
        )
        assert feed["unread"] == 0

    async def test_read_all_skips_other_channels(
        self, db_session: AsyncSession, client: AsyncClient,
    ) -> None:
        """read-all touches in_app rows only: a sent telegram
        delivery keeps read_at NULL."""
        recipient_id, _ = await _seed_inbox(
            count=1, channels=["telegram", "in_app"],
        )
        await client.post(f"{_inbox(recipient_id)}/read-all")

        from sqlalchemy import select

        from app.engine.models import NotificationDelivery

        rows = (
            await db_session.execute(
                select(
                    NotificationDelivery.channel,
                    NotificationDelivery.read_at,
                ).where(NotificationDelivery.recipient_id == recipient_id)
            )
        ).all()
        by_channel: dict[str, Any] = {
            channel: read_at for channel, read_at in rows
        }
        assert by_channel["in_app"] is not None
        assert by_channel["telegram"] is None
