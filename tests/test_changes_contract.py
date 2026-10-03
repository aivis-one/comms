# =============================================================================
# P2-3 Б2 -- the contract of the changes feed, and the refusal classes
# =============================================================================
#
# deploy/INTEGRATION.md section 9 is the one full copy of the changes
# feed's contract, and section 5's table is the one list of refusal
# classes. Both are held to the code in BOTH directions.
#
# MUTATION these tests were written against:
#   M12 a field in the code without the document, or the reverse; a
#       refusal class in the code without its row, or the reverse
# =============================================================================

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi.routing import APIRoute
from httpx import AsyncClient

from app.api.changes import CHANGE_ITEM_FIELDS
from app.api.errors import ErrorClass
from app.api.paging import ChangesCursor, encode_changes_cursor
from app.core.config import settings
from app.core.database import get_session_factory
from app.engine.constants import TargetType
from app.engine.models import Notification, NotificationTransition
from tests.helpers import notification_row_fields

_DOC = (
    Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md"
).read_text(encoding="utf-8")
_PATH = "/api/v1/notifications/changes"


def _section(heading: str) -> str:
    start = _DOC.index(f"{heading}\n")
    end = re.compile(r"^## ", re.M).search(_DOC, start + len(heading))
    assert end is not None
    return _DOC[start:end.start()]


_FEED = _section("## 9. What changed since a cursor")
_RESOURCES = _section("## 5. The resource protocol (F1.4)")


def _example() -> dict[str, Any]:
    (block,) = re.findall(r"```json\n(.*?)```", _FEED, re.S)
    example: dict[str, Any] = json.loads(block)
    return example


async def _one_change() -> None:
    async with get_session_factory()() as session:
        job = Notification(
            type="unit_event", title="T", body="B",
            target_type=TargetType.USER, target_value="u",
            **notification_row_fields(),
        )
        session.add(job)
        await session.flush()
        session.add(NotificationTransition(
            notification_id=job.id, subject="job", step="resolve",
            outcome="processing", attempt=0,
        ))
        await session.commit()


def _route_params() -> set[str]:
    from app.main import app

    for route in app.routes:
        for candidate in getattr(
            getattr(route, "original_router", None), "routes", [],
        ):
            if isinstance(candidate, APIRoute) and candidate.path == _PATH:
                return {p.name for p in candidate.dependant.query_params}
    raise AssertionError("the route was not found")


class TestTheFeedContract:
    async def test_the_example_has_the_shape_of_a_real_answer(
        self, client: AsyncClient,
    ) -> None:
        await _one_change()
        real = (await client.get(_PATH)).json()
        assert real["items"], "the pair: an item was really listed"
        example = _example()
        assert set(example) == set(real)
        (documented,) = example["items"]
        assert set(documented) == set(real["items"][0])
        assert set(documented) == set(CHANGE_ITEM_FIELDS)

    def test_the_table_names_every_field_of_an_item(self) -> None:
        table = set(re.findall(r"^\| `(\w+)` \|", _FEED, re.M))
        assert table == set(CHANGE_ITEM_FIELDS)

    def test_the_query_parameters(self) -> None:
        (line,) = re.findall(rf"GET {re.escape(_PATH)}\?(\S+)", _FEED)
        documented = {pair.partition("=")[0] for pair in line.split("&")}
        assert documented == _route_params()

    def test_the_section_says_what_the_product_relies_on(self) -> None:
        """The four facts the recovery stands on, and the recipe."""
        assert "**It is never `null`**" in _FEED
        assert "**Without a\ncursor**" in _FEED
        assert "**The feed is late, not lossy.**" in _FEED
        assert "up to that\ncall's timeout" in _FEED
        assert "410\n`cursor_expired`" in _FEED
        recipe = re.findall(r"^    (\d)\. ", _FEED, re.M)
        assert recipe == ["1", "2", "3", "4", "5", "6"]


class TestTheRefusalClasses:
    def test_the_table_lists_every_class_of_the_code(self) -> None:
        rows = re.findall(r"^\| (\d{3}) \| `(\w+)` \|", _RESOURCES, re.M)
        assert len(rows) > 5, "the pair: the table is found"
        assert {cls for _, cls in rows} == {c.value for c in ErrorClass}

    async def test_cursor_expired_travels_with_the_status_of_its_row(
        self, client: AsyncClient,
    ) -> None:
        rows = dict(
            (cls, int(status))
            for status, cls in re.findall(
                r"^\| (\d{3}) \| `(\w+)` \|", _RESOURCES, re.M,
            )
        )
        old = datetime.now(UTC) - timedelta(
            days=settings.notification_retention_days + 1,
        )
        response = await client.get(_PATH, params={
            "cursor": encode_changes_cursor(ChangesCursor(0, 0, old)),
        })
        assert response.json()["error"]["class"] == ErrorClass.CURSOR_EXPIRED
        assert response.status_code == rows[ErrorClass.CURSOR_EXPIRED] == 410
