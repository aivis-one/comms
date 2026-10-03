# =============================================================================
# P2-4 Б2 -- reconciling the address book (spec §10.5)
# =============================================================================
#
# GET /api/v1/recipients pages through the book as comms holds it: id,
# version, active, deleted -- no address -- oldest first, tombstones
# included, behind the service token; reading it changes nothing.
#
# MUTATIONS these tests were written against (each turns one red):
#   M6  require_service_auth dropped from the router  -> TestAuth
#   M7  a write on the read path                      -> TestNothingChanges
#   M9  an address in an item                         -> TestNoAddress
#   M10 newest first        -> TestPaging.test_a_recipient_created_...
# =============================================================================

import ast
import base64
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import Recipient
from app.audience.sync import snapshot_fingerprint
from app.core.config import settings
from app.core.database import Base, get_session_factory
from tests.helpers import create_recipient

_URL = "/api/v1/recipients"
_ROOT = Path(__file__).resolve().parents[1]
_TOKEN = "p2-4-book-unit-test-token"
_ITEM_KEYS = {"recipient_id", "version", "active", "deleted"}
# An address no field, status or key ever contains by itself.
_EMAIL_SENTINEL = "zq7" + "bookaddr" + "xk4@unit-test.invalid"


async def _make(count: int) -> list[UUID]:
    """`count` recipients, each in its own transaction (distinct
    created_at), oldest first."""
    ids: list[UUID] = []
    for _ in range(count):
        async with get_session_factory()() as session:
            recipient = await create_recipient(session)
            await session.commit()
            ids.append(recipient.id)
    return ids


async def _page(client: AsyncClient, **params: Any) -> dict[str, Any]:
    response = await client.get(_URL, params=params)
    assert response.status_code == 200, response.text
    body: dict[str, Any] = response.json()
    return body


async def _walk(client: AsyncClient, limit: int) -> list[str]:
    seen: list[str] = []
    cursor: str | None = None
    while True:
        params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        body = await _page(client, **params)
        seen.extend(item["recipient_id"] for item in body["items"])
        cursor = body["next_cursor"]
        if cursor is None:
            return seen


# -----------------------------------------------------------------------------
# The page shape and the walk
# -----------------------------------------------------------------------------


class TestPaging:
    async def test_an_empty_book(self, client: AsyncClient) -> None:
        assert await _page(client) == {"items": [], "next_cursor": None}

    async def test_fewer_than_a_page(self, client: AsyncClient) -> None:
        ids = await _make(2)
        body = await _page(client, limit=5)
        assert [i["recipient_id"] for i in body["items"]] == [str(x) for x in ids]
        assert body["next_cursor"] is None

    async def test_exactly_a_page_has_no_next(self, client: AsyncClient) -> None:
        """limit+1 is fetched: a full last page says so, it does not
        send the product for an empty one."""
        ids = await _make(3)
        body = await _page(client, limit=3)
        assert len(body["items"]) == len(ids) == 3
        assert body["next_cursor"] is None

    async def test_a_walk_sees_everyone_once_in_order(
        self, client: AsyncClient,
    ) -> None:
        ids = await _make(5)
        first = await _page(client, limit=2)
        assert first["next_cursor"] is not None
        assert await _walk(client, limit=2) == [str(x) for x in ids]

    async def test_ties_in_created_at_are_broken_by_id(
        self, client: AsyncClient,
    ) -> None:
        async with get_session_factory()() as session:
            made = [await create_recipient(session) for _ in range(4)]
            await session.commit()
        stamps = {r.created_at for r in made}
        assert len(stamps) == 1, "the pair: one transaction, one created_at"
        walked = await _walk(client, limit=1)
        assert walked == [str(u) for u in sorted(r.id for r in made)]

    async def test_a_recipient_created_while_paging_is_reached(
        self, client: AsyncClient,
    ) -> None:
        await _make(3)
        first = await _page(client, limit=2)
        assert first["next_cursor"] is not None
        (late,) = await _make(1)
        rest = await _page(client, limit=2, cursor=first["next_cursor"])
        assert str(late) in [i["recipient_id"] for i in rest["items"]]

    async def test_the_same_cursor_twice_is_the_same_page(
        self, client: AsyncClient,
    ) -> None:
        await _make(4)
        cursor = (await _page(client, limit=2))["next_cursor"]
        one = await _page(client, limit=2, cursor=cursor)
        two = await _page(client, limit=2, cursor=cursor)
        assert one == two
        assert one["items"], "the pair: the page is not empty"

    @pytest.mark.parametrize("limit", [0, 101])
    async def test_a_limit_out_of_bounds_is_refused(
        self, client: AsyncClient, limit: int,
    ) -> None:
        response = await client.get(_URL, params={"limit": limit})
        assert response.status_code == 422
        assert response.json()["error"]["class"] == "validation"

    @pytest.mark.parametrize(
        "cursor",
        ["not-base64!", base64.urlsafe_b64encode(b"zq7-no-separator").decode()],
    )
    async def test_a_malformed_cursor_is_refused(
        self, client: AsyncClient, cursor: str,
    ) -> None:
        response = await client.get(_URL, params={"cursor": cursor})
        assert response.status_code == 422
        assert response.json()["error"]["class"] == "validation"


# -----------------------------------------------------------------------------
# What a row says
# -----------------------------------------------------------------------------


class TestWhatARowSays:
    async def test_a_living_recipient(
        self, client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        await db_session.commit()
        (item,) = (await _page(client))["items"]
        assert item == {
            "recipient_id": str(recipient.id),
            "version": 1,
            "active": True,
            "deleted": False,
        }

    async def test_a_deactivated_recipient_is_not_deleted(
        self, client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        await create_recipient(db_session, active=False)
        await db_session.commit()
        (item,) = (await _page(client))["items"]
        assert (item["active"], item["deleted"]) == (False, False)

    async def test_a_row_written_before_versions(
        self, client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        rid = uuid4()
        db_session.add(Recipient(
            id=rid, version=0,
            snapshot_fingerprint=snapshot_fingerprint(
                telegram_id=None, email=None, locale=None,
                timezone=None, active=True,
            ),
            locale=None,
        ))
        await db_session.commit()
        (item,) = (await _page(client))["items"]
        assert (item["recipient_id"], item["version"]) == (str(rid), 0)

    async def test_a_forgotten_recipient_is_a_tombstone_without_address(
        self, client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session, email=_EMAIL_SENTINEL)
        await db_session.commit()
        before = await client.get(_URL)
        assert _EMAIL_SENTINEL not in before.text
        forgotten = await client.request(
            "DELETE", f"{_URL}/{recipient.id}", json={"version": 2},
        )
        assert forgotten.status_code == 200, forgotten.text
        response = await client.get(_URL)
        (item,) = response.json()["items"]
        assert set(item) == _ITEM_KEYS
        assert item["recipient_id"] == str(recipient.id)
        assert item["version"] == 2
        assert (item["active"], item["deleted"]) == (False, True)
        assert _EMAIL_SENTINEL not in response.text

    async def test_an_id_comms_never_heard_of_deleted_is_listed(
        self, client: AsyncClient,
    ) -> None:
        rid = uuid4()
        response = await client.request(
            "DELETE", f"{_URL}/{rid}", json={"version": 5},
        )
        assert response.status_code == 200, response.text
        (item,) = (await _page(client))["items"]
        assert item == {
            "recipient_id": str(rid), "version": 5,
            "active": False, "deleted": True,
        }


class TestNoAddress:
    async def test_no_item_carries_an_address(
        self, client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        await create_recipient(db_session, email=_EMAIL_SENTINEL)
        await db_session.commit()
        response = await client.get(_URL)
        (item,) = response.json()["items"]
        assert set(item) == _ITEM_KEYS
        assert item["recipient_id"] and item["version"] == 1, "the pair"
        assert _EMAIL_SENTINEL not in response.text


# -----------------------------------------------------------------------------
# comms changes nothing on this read
# -----------------------------------------------------------------------------

_BOOK_SOURCES = ("app/audience/book.py",)
_WRITE_CALLS = frozenset({
    "add", "add_all", "delete", "merge", "flush", "commit",
    "insert", "update", "apply_snapshot", "tombstone", "forget_recipient",
})


def _called_names(rel: str, function: str | None = None) -> set[str]:
    tree: ast.AST = ast.parse((_ROOT / rel).read_text(encoding="utf-8"))
    if function is not None:
        (tree,) = [
            n for n in ast.walk(tree)
            if isinstance(n, ast.AsyncFunctionDef) and n.name == function
        ]
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            target = node.func
            if isinstance(target, ast.Attribute):
                names.add(target.attr)
            elif isinstance(target, ast.Name):
                names.add(target.id)
    return names


async def _row_counts(session: AsyncSession) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in Base.metadata.sorted_tables:
        counts[table.name] = (
            await session.execute(select(func.count()).select_from(table))
        ).scalar_one()
    return counts


class TestNothingChanges:
    def test_the_read_model_calls_no_write(self) -> None:
        called = _called_names("app/audience/book.py")
        assert "select" in called, "the pair: the scanner sees the query"
        assert not called & _WRITE_CALLS, called & _WRITE_CALLS

    def test_the_route_calls_no_write(self) -> None:
        called = _called_names("app/api/recipients.py", "list_recipients")
        assert "list_book" in called, "the pair: the route is the one read"
        assert not called & _WRITE_CALLS, called & _WRITE_CALLS

    async def test_a_walk_changes_no_row_and_no_stamp(
        self, client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        await _make(3)
        before = await _row_counts(db_session)
        stamps = (await db_session.execute(
            select(Recipient.id, Recipient.updated_at, Recipient.version)
            .order_by(Recipient.id)
        )).all()
        assert before["recipients"] == 3, "the pair"
        await db_session.rollback()
        await _walk(client, limit=2)
        assert await _row_counts(db_session) == before
        assert (await db_session.execute(
            select(Recipient.id, Recipient.updated_at, Recipient.version)
            .order_by(Recipient.id)
        )).all() == stamps


# -----------------------------------------------------------------------------
# The same authorization as every /api/v1 route
# -----------------------------------------------------------------------------


@pytest.fixture
def auth_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "comms_service_token", _TOKEN)


class TestAuth:
    async def test_no_token_is_401(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        response = await client.get(_URL)
        assert response.status_code == 401
        assert response.json()["error"]["class"] == "unauthorized"

    async def test_a_wrong_token_is_401(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        response = await client.get(
            _URL, headers={"Authorization": "Bearer zq7-not-the-token-xk4"},
        )
        assert response.status_code == 401

    async def test_the_token_lets_it_through(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        await _make(1)
        response = await client.get(
            _URL, headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert response.status_code == 200
        assert response.json()["items"], "the pair: a real answer"
