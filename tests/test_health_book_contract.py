# =============================================================================
# P2-4 Б3 -- the contract of channel health and of the address book
# =============================================================================
#
# deploy/INTEGRATION.md, section 5, "Channel health" and "Reconciling
# the address book", is the one full copy of both contracts. Every field
# and every query parameter is held to the code in BOTH directions: a
# field the code returns that the document does not name fails, and so
# does a field the document names that the code does not return.
#
# MUTATION these tests were written against:
#   M11 a field added to the code without the document, or removed from
#       the document -> every test of the touched section
# =============================================================================

import json
import re
from pathlib import Path
from typing import Any

from fastapi.routing import APIRoute
from httpx import AsyncClient

from app.core.constants import (
    HEALTH_WINDOW_DEFAULT_MINUTES,
    HEALTH_WINDOW_MAX_MINUTES,
)
from app.engine.constants import ChannelAnswer, DeliveryChannel
from app.engine.health import REFUSAL_CLASSES
from tests.helpers import create_recipient

_DOC = (
    Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md"
).read_text(encoding="utf-8")


def _section(title: str) -> str:
    """The text of one `### title` section, up to the next heading."""
    start = _DOC.index(f"### {title}\n")
    end = re.compile(r"^#{2,3} ", re.M).search(_DOC, start + 4)
    assert end is not None
    return _DOC[start:end.start()]


def _example(section: str) -> dict[str, Any]:
    (block,) = re.findall(r"```json\n(.*?)```", section, re.S)
    example: dict[str, Any] = json.loads(block)
    return example


def _table_fields(section: str) -> set[str]:
    return set(re.findall(r"^\| `(\w+)` \|", section, re.M))


def _query_params(path: str) -> set[str]:
    from app.main import app

    routes: list[APIRoute] = []
    for route in app.routes:
        candidates = getattr(getattr(route, "original_router", None), "routes", [route])
        routes.extend(r for r in candidates if isinstance(r, APIRoute))
    (route,) = [r for r in routes if r.path == path and "GET" in r.methods]
    return {param.name for param in route.dependant.query_params}


def _documented_params(section: str, path: str) -> set[str]:
    (line,) = re.findall(rf"GET {re.escape(path)}\?(\S+)", section)
    return {pair.partition("=")[0] for pair in line.split("&")}


_HEALTH = _section("Channel health")
_BOOK = _section("Reconciling the address book")


class TestChannelHealthContract:
    def test_the_sections_are_found_and_not_empty(self) -> None:
        assert "configuration_share" in _HEALTH
        assert "recipient_id" in _BOOK

    async def test_the_example_has_the_shape_of_a_real_answer(
        self, client: AsyncClient,
    ) -> None:
        real = (await client.get("/api/v1/channels/health")).json()
        example = _example(_HEALTH)
        assert set(example) == set(real)
        assert set(example["window"]) == set(real["window"])
        assert real["channels"], "the pair: channels are really listed"
        (documented,) = example["channels"].values()
        for name, entry in real["channels"].items():
            assert set(entry) == set(documented), name
            assert set(entry["by_outcome"]) == set(documented["by_outcome"])
            assert set(entry["refused_by_class"]) == set(
                documented["refused_by_class"],
            )

    def test_the_example_names_every_outcome_and_class(self) -> None:
        (documented,) = _example(_HEALTH)["channels"].values()
        assert set(documented["by_outcome"]) == {a.value for a in ChannelAnswer}
        assert set(documented["refused_by_class"]) == {
            c.value for c in REFUSAL_CLASSES
        }

    async def test_the_table_names_every_field_of_a_channel(
        self, client: AsyncClient,
    ) -> None:
        real = (await client.get("/api/v1/channels/health")).json()
        entry = next(iter(real["channels"].values()))
        assert _table_fields(_HEALTH) == set(entry)

    def test_the_listed_channels_are_the_service_channels(self) -> None:
        sentence = re.search(
            r"Every channel of the service is listed \(([^)]*)\)", _HEALTH,
        )
        assert sentence is not None
        named = set(re.findall(r"`(\w+)`", sentence.group(1)))
        assert named == {c.value for c in DeliveryChannel}

    def test_the_query_parameter_and_its_bounds(self) -> None:
        path = "/api/v1/channels/health"
        assert _documented_params(_HEALTH, path) == _query_params(path)
        assert (
            f"1..{HEALTH_WINDOW_MAX_MINUTES}, default "
            f"{HEALTH_WINDOW_DEFAULT_MINUTES}"
        ) in _HEALTH


class TestAddressBookContract:
    async def test_the_example_has_the_shape_of_a_real_answer(
        self, client: AsyncClient, db_session: Any,
    ) -> None:
        await create_recipient(db_session)
        await db_session.commit()
        real = (await client.get("/api/v1/recipients")).json()
        example = _example(_BOOK)
        assert set(example) == set(real)
        assert real["items"], "the pair: an item was really listed"
        (documented,) = example["items"]
        assert set(documented) == set(real["items"][0])

    async def test_the_table_names_every_field_of_an_item(
        self, client: AsyncClient, db_session: Any,
    ) -> None:
        await create_recipient(db_session)
        await db_session.commit()
        (item,) = (await client.get("/api/v1/recipients")).json()["items"]
        assert _table_fields(_BOOK) == set(item)

    def test_the_query_parameters(self) -> None:
        path = "/api/v1/recipients"
        assert _documented_params(_BOOK, path) == _query_params(path)
