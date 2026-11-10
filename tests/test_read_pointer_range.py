# =============================================================================
# A read pointer's time outside the UTC range is refused (REV-3)
# =============================================================================
#
# POST /threads/{id}/read takes `last_read_at`. A time with a zone that
# leaves the range once moved to UTC (0001-01-01T00:00:00+05:00) reached
# the driver's own conversion and came back as a 500. It is the caller's
# input: a 422 `validation` naming the field, by the rule of the cursors
# (app/api/paging.py utc_instant). A time without a zone is read as UTC,
# as before; a future one is clamped to now, as before.
#
# MUTATIONS these tests were written against (each turns one red):
#   M11 the ReadIn check out -> test_past_the_range
# =============================================================================

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from httpx import AsyncClient

from app.core.database import get_session_factory
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.threads import create_or_get_thread
from tests.helpers import create_recipient, intake_fields


async def _thread() -> tuple[UUID, UUID]:
    async with get_session_factory()() as session:
        client = await create_recipient(session)
        operator = await create_recipient(session)
        await session.commit()
        thread = await create_or_get_thread(
            session,
            **intake_fields(),
            client=client.id,
            operator_kind=OperatorKind.USER,
            operator_value=operator.id,
            kind=ThreadKind.TICKET,
        )
        await session.commit()
        return thread.id, client.id


async def _read(client: AsyncClient, when: str | None) -> tuple[int, dict]:
    thread, participant = await _thread()
    body: dict[str, object] = {"participant": str(participant)}
    if when is not None:
        body["last_read_at"] = when
    response = await client.post(f"/api/v1/threads/{thread}/read", json=body)
    return response.status_code, response.json()


@pytest.mark.parametrize(
    "when",
    ["0001-01-01T00:00:00+05:00", "9999-12-31T23:00:00-05:00"],
)
async def test_past_the_range(client: AsyncClient, when: str) -> None:
    status, body = await _read(client, when)
    assert status == 422, body
    assert body["error"]["class"] == "validation"
    assert any("last_read_at" in f["loc"] for f in body["error"]["fields"])


@pytest.mark.parametrize(
    "when",
    [
        None,
        (datetime.now(UTC) - timedelta(days=1)).isoformat(),
        (datetime.now(UTC) + timedelta(days=1)).isoformat(),
        "2026-01-01T00:00:00",
        "0001-01-02T00:00:00+00:00",
    ],
)
async def test_in_range_reads_on(client: AsyncClient, when: str | None) -> None:
    status, body = await _read(client, when)
    assert status == 200, body
    assert body == {"unread": 0}
