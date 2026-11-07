# =============================================================================
# A REST body's bool and int are taken as sent, never coerced (REV-3)
# =============================================================================
#
# Pydantic's lax mode reads "no" as False, true as 1, "1" as 1 -- a
# silent change of what the product sent, where the stream path refuses
# the same values (app/transport/events.py, app/audience/sync.py). The
# fields where coercion changes the meaning are strict
# (app/api/recipients.py, app/api/prefs.py, app/api/messaging.py):
#   RecipientSnapshot.telegram_id, .active; the values of
#   PreferencesPatch.categories; ThreadCreateIn.priority.
# A wrong type is a 422 `validation` naming the field; the right type
# reads as before.
#
# MUTATIONS these tests were written against (each turns one red):
#   M6a strict off telegram_id    -> test_snapshot_refused[telegram_id-*]
#   M6b strict off active          -> test_snapshot_refused[active-*]
#   M6c categories back to bool    -> test_categories_refused
#   M6d strict off priority        -> test_priority_refused
# =============================================================================

from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient, Response

from app.core.database import get_session_factory
from tests.helpers import (
    create_recipient,
    next_snapshot_version,
    next_telegram_id,
)


def _field_named(response: Response, field: str) -> None:
    assert response.status_code == 422, response.text
    error = response.json()["error"]
    assert error["class"] == "validation"
    assert any(field in f["loc"] for f in error["fields"]), error


def _snapshot(**fields: Any) -> dict[str, Any]:
    return {
        "version": next_snapshot_version(),
        "telegram_id": None,
        "email": None,
        "locale": None,
        "timezone": None,
        "active": True,
        **fields,
    }


async def _recipient() -> UUID:
    async with get_session_factory()() as session:
        recipient = await create_recipient(session)
        await session.commit()
    return recipient.id


class TestRecipientSnapshot:
    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("telegram_id", True),
            ("telegram_id", False),
            ("telegram_id", "1"),
            ("telegram_id", 7.0),
            ("active", "no"),
            ("active", "true"),
            ("active", 0),
            ("active", 1),
        ],
    )
    async def test_snapshot_refused(
        self,
        client: AsyncClient,
        field: str,
        value: Any,
    ) -> None:
        response = await client.put(
            f"/api/v1/recipients/{uuid4()}",
            json=_snapshot(**{field: value}),
        )
        _field_named(response, field)

    @pytest.mark.parametrize(
        ("with_telegram", "active"),
        [(False, True), (True, False)],
    )
    async def test_snapshot_reads_as_sent(
        self,
        client: AsyncClient,
        with_telegram: bool,
        active: bool,
    ) -> None:
        telegram_id = next_telegram_id() if with_telegram else None
        response = await client.put(
            f"/api/v1/recipients/{uuid4()}",
            json=_snapshot(telegram_id=telegram_id, active=active),
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert (body["telegram_id"], body["active"]) == (telegram_id, active)


class TestPreferences:
    @pytest.mark.parametrize("value", ["off", "false", 0, None])
    async def test_categories_refused(
        self,
        client: AsyncClient,
        value: Any,
    ) -> None:
        recipient = await _recipient()
        response = await client.patch(
            f"/api/v1/recipients/{recipient}/preferences",
            json={"categories": {"unit_reminder": value}},
        )
        _field_named(response, "categories")
        form = (
            await client.get(
                f"/api/v1/recipients/{recipient}/preferences",
            )
        ).json()
        assert form["categories"]["unit_reminder"] is True, "nothing muted"

    async def test_categories_read_as_sent(self, client: AsyncClient) -> None:
        recipient = await _recipient()
        response = await client.patch(
            f"/api/v1/recipients/{recipient}/preferences",
            json={"categories": {"unit_reminder": False}},
        )
        assert response.status_code == 200, response.text
        assert response.json()["categories"]["unit_reminder"] is False


class TestThreadPriority:
    async def _create(self, client: AsyncClient, priority: Any) -> Response:
        return await client.post(
            "/api/v1/threads",
            json={
                "client": str(await _recipient()),
                "operator_kind": "user",
                "operator_value": str(await _recipient()),
                "kind": "ticket",
                "priority": priority,
            },
        )

    @pytest.mark.parametrize("value", ["1", True, 1.0])
    async def test_priority_refused(self, client: AsyncClient, value: Any) -> None:
        _field_named(await self._create(client, value), "priority")

    @pytest.mark.parametrize("value", [1, None])
    async def test_priority_reads_as_sent(
        self,
        client: AsyncClient,
        value: int | None,
    ) -> None:
        response = await self._create(client, value)
        assert response.status_code == 200, response.text
        assert response.json()["priority"] == value
