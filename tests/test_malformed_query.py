# =============================================================================
# TIME-1 -- a malformed cursor, query parameter or time is the caller's
# input: a 422 `validation`, never comms' 500
# =============================================================================
#
# What is held here, against the code and deploy/INTEGRATION.md:
#   1 a cursor whose time carries no zone, whose time leaves the range
#     once moved to UTC, or whose integer lies outside the 64-bit column
#     is refused when it is decoded, on every route that takes one --
#     naming the cursor, never its value; a cursor comms handed out reads
#     on as before (app/api/paging.py);
#   2 a query parameter -- its value or its name -- holding a string the
#     database cannot store is refused before any route runs, naming the
#     parameter, never the value (app/api/deps.py refuse_unstorable_query,
#     the rule of app/core/storable.py). Only the NUL is reachable: the
#     query string is percent-decoded with errors="replace", so a lone
#     surrogate never arrives (Starlette QueryParams);
#   3 a schedule time is exactly HH:MM in ASCII digits -- no trailing
#     newline, no other script's digits, no 24:MM but 24:00 as an end
#     (app/api/prefs.py).
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 the zone check out of the cursor time -> TestTimestampCursor naive
#   M2 the UTC range check out -> TestTimestampCursor overflow
#   M3 the 64-bit bound out of the sequence cursor -> TestSequenceCursor
#   M4 the 64-bit bound out of the changes cursor's id -> TestChangesCursor
#   M5 the bound off by one -> the 2**63 - 1 pairs
#   M6 refuse_unstorable_query out of the app's dependencies ->
#      TestUnstorableQuery
#   M7 the check reading values only -> test_a_name_holding_nul
#   M8 _TIME_RE matched with `$` again -> "09:00\n"
#   M9 `\d` again -> the Arabic-Indic digits
#
# NOT built: a 64-bit bound on the changes cursor's xact -- the "from the
# future" check (app/engine/changes.py list_changes) refuses every xact
# at or past the snapshot's xmax first, so such a bound could not be seen
# red; test_xact_past_the_column_range holds the refusal itself.
# =============================================================================

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.routing import APIRoute
from httpx import AsyncClient, Response

from app.api.paging import encode_cursor, encode_seq_cursor
from app.core.database import get_session_factory
from app.main import app
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.threads import create_or_get_thread
from app.transport.events import parse_event
from app.transport.handlers import handle_event
from tests.helpers import create_recipient, intake_fields

_INT64_MAX = 2**63 - 1
# Values no routine text contains by itself.
_SENTINEL = "zq9" + "VALUE" + "wk2"


def _wire(raw: str) -> str:
    """A cursor in comms' wire form: base64url of its text."""
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _refused(response: Response, name: str, *values: str) -> None:
    """A 422 `validation` naming `name` and quoting none of `values`."""
    assert response.status_code == 422, response.text
    error = response.json()["error"]
    assert error["class"] == "validation"
    assert name in error["message"]
    for value in values:
        assert value not in response.text


@dataclass(frozen=True)
class _World:
    """One of each thing a listing hangs on."""

    recipient: UUID
    operator: UUID
    thread: UUID
    key: str


async def _job(target: UUID) -> str:
    """An accepted job, through the stream path, under a fresh key."""
    key = f"time-1:{uuid4()}"
    data = {
        "v": 1,
        "idempotency_key": key,
        "type": "unit_event_in_app",
        "target_type": "user",
        "target_value": str(target),
        "title": "T",
        "body": "B",
    }
    parsed = parse_event(
        {"event": "notification_request", "data": json.dumps(data)},
    )
    async with get_session_factory()() as session:
        await handle_event(session, parsed)
        await session.commit()
    return key


@pytest.fixture
async def world() -> _World:
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
            kind=ThreadKind.DM,
        )
        await session.commit()
        ids = (client.id, operator.id, thread.id)
    return _World(ids[0], ids[1], ids[2], await _job(ids[0]))


def _timestamp_routes(w: _World) -> dict[str, tuple[str, dict[str, Any]]]:
    """Every route whose cursor is (timestamp, uuid), with a context
    under which the cursor reaches the query."""
    return {
        "inbox": (f"/api/v1/recipients/{w.recipient}/inbox", {}),
        "threads_operator": ("/api/v1/threads", {"operator": str(w.operator)}),
        "threads_client": ("/api/v1/threads", {"client": str(w.recipient)}),
        "feed": (f"/api/v1/threads/{w.thread}/messages", {}),
        "recipients": ("/api/v1/recipients", {}),
        "deliveries": ("/api/v1/notifications/by-key/deliveries", {"key": w.key}),
    }


_TIMESTAMP_ROUTES = (
    "inbox",
    "threads_operator",
    "threads_client",
    "feed",
    "recipients",
    "deliveries",
)

# A time with no zone (a date alone too), and times with a zone that
# leave Python's range once moved to UTC -- the driver's conversion.
_NO_ZONE = ("2026-01-01T00:00:00", "2026-01-01")
_PAST_THE_RANGE = ("9999-12-31T23:00:00-05:00", "0001-01-01T00:30:00+14:00")


# -----------------------------------------------------------------------------
# 1 -- cursors
# -----------------------------------------------------------------------------


class TestTimestampCursor:
    @pytest.mark.parametrize("route", _TIMESTAMP_ROUTES)
    @pytest.mark.parametrize("when", _NO_ZONE + _PAST_THE_RANGE)
    async def test_refused(
        self,
        client: AsyncClient,
        world: _World,
        route: str,
        when: str,
    ) -> None:
        path, params = _timestamp_routes(world)[route]
        cursor = _wire(f"{when}|{uuid4()}")
        response = await client.get(path, params={**params, "cursor": cursor})
        _refused(response, "cursor", when, cursor)

    @pytest.mark.parametrize("route", _TIMESTAMP_ROUTES)
    async def test_a_cursor_comms_encodes_reads_on(
        self,
        client: AsyncClient,
        world: _World,
        route: str,
    ) -> None:
        path, params = _timestamp_routes(world)[route]
        cursor = encode_cursor((datetime.now(UTC), uuid4()))
        response = await client.get(path, params={**params, "cursor": cursor})
        assert response.status_code == 200, response.text

    async def test_a_handed_out_cursor_reads_on(
        self,
        client: AsyncClient,
        world: _World,
    ) -> None:
        """The round trip: next_cursor of a page opens the next one."""
        first = await client.get("/api/v1/recipients", params={"limit": 1})
        cursor = first.json()["next_cursor"]
        assert cursor is not None
        second = await client.get(
            "/api/v1/recipients",
            params={"limit": 1, "cursor": cursor},
        )
        assert second.status_code == 200
        assert second.json()["items"] != first.json()["items"]


_PATH = "/api/v1/notifications/by-key/path"
_CHANGES = "/api/v1/notifications/changes"


class TestSequenceCursor:
    @pytest.mark.parametrize("n", [str(2**63), str(2**64 + 5), "1" + "0" * 30])
    async def test_past_the_column_range(
        self,
        client: AsyncClient,
        world: _World,
        n: str,
    ) -> None:
        cursor = _wire(f"seq:{n}")
        response = await client.get(
            _PATH,
            params={"key": world.key, "cursor": cursor},
        )
        _refused(response, "cursor", n, cursor)

    async def test_negative(self, client: AsyncClient, world: _World) -> None:
        cursor = _wire("seq:-1")
        response = await client.get(
            _PATH,
            params={"key": world.key, "cursor": cursor},
        )
        _refused(response, "cursor", cursor)

    async def test_the_column_maximum_reads_on(
        self,
        client: AsyncClient,
        world: _World,
    ) -> None:
        response = await client.get(
            _PATH,
            params={
                "key": world.key,
                "cursor": encode_seq_cursor(_INT64_MAX),
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["items"] == []

    async def test_a_cursor_comms_encodes_reads_on(
        self,
        client: AsyncClient,
        world: _World,
    ) -> None:
        """Position 0 -- before the first row -- reads the job's path."""
        response = await client.get(
            _PATH,
            params={
                "key": world.key,
                "cursor": encode_seq_cursor(0),
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["items"]


def _changes(xact: int | str, ident: int | str, read_at: str | None = None) -> str:
    when = read_at if read_at is not None else datetime.now(UTC).isoformat()
    return _wire(f"chg:{xact}:{ident}:{when}")


class TestChangesCursor:
    @pytest.mark.parametrize("n", [str(2**63), "1" + "0" * 30])
    async def test_id_past_the_column_range(
        self,
        client: AsyncClient,
        n: str,
    ) -> None:
        cursor = _changes(0, n)
        response = await client.get(_CHANGES, params={"cursor": cursor})
        _refused(response, "cursor", n, cursor)

    async def test_xact_past_the_column_range(self, client: AsyncClient) -> None:
        cursor = _changes(2**63, 0)
        response = await client.get(_CHANGES, params={"cursor": cursor})
        _refused(response, "cursor", str(2**63), cursor)

    async def test_negative(self, client: AsyncClient) -> None:
        cursor = _changes(0, -1)
        response = await client.get(_CHANGES, params={"cursor": cursor})
        _refused(response, "cursor", cursor)

    @pytest.mark.parametrize("when", _NO_ZONE + _PAST_THE_RANGE)
    async def test_read_time(self, client: AsyncClient, when: str) -> None:
        cursor = _changes(0, 0, when)
        response = await client.get(_CHANGES, params={"cursor": cursor})
        _refused(response, "cursor", when, cursor)

    async def test_the_column_maximum_reads_on(self, client: AsyncClient) -> None:
        response = await client.get(
            _CHANGES,
            params={"cursor": _changes(0, _INT64_MAX)},
        )
        assert response.status_code == 200, response.text

    async def test_a_handed_out_cursor_reads_on(self, client: AsyncClient) -> None:
        first = await client.get(_CHANGES, params={"limit": 1})
        cursor = first.json()["next_cursor"]
        second = await client.get(_CHANGES, params={"limit": 1, "cursor": cursor})
        assert second.status_code == 200, second.text


# -----------------------------------------------------------------------------
# 2 -- a query string the database cannot store
# -----------------------------------------------------------------------------

_NUL_VALUE = f"{_SENTINEL}\x00"


def _string_params(w: _World) -> list[tuple[str, str, dict[str, Any]]]:
    """Every string query parameter of the GET routes: (path, name,
    the rest of a request under which it would reach the database)."""
    cursor_routes = [
        (path, "cursor", params) for path, params in _timestamp_routes(w).values()
    ] + [
        (_PATH, "cursor", {"key": w.key}),
        (_CHANGES, "cursor", {}),
    ]
    return [
        *cursor_routes,
        ("/api/v1/notifications/by-key", "key", {}),
        ("/api/v1/notifications/by-key/deliveries", "key", {}),
        (_PATH, "key", {}),
        (f"/api/v1/threads/{w.thread}/messages", "locale", {}),
        (
            "/api/v1/threads",
            "locale",
            {"operator": str(w.operator), "with_last_message": "true"},
        ),
    ]


def _get_routes() -> list[APIRoute]:
    """Every GET route of the application, the included routers' too."""
    found: list[APIRoute] = []
    for included in app.router.routes:
        routes = getattr(getattr(included, "original_router", None), "routes", ())
        for route in [included, *routes]:
            if isinstance(route, APIRoute) and "GET" in route.methods:
                found.append(route)
    return found


class TestUnstorableQuery:
    def test_every_string_parameter_is_here(self, world: _World) -> None:
        """The watchman: a string query parameter added to a GET route is
        added to the table below."""
        declared = {
            (route.path, param.name)
            for route in _get_routes()
            for param in route.dependant.query_params
            if param.field_info.annotation in (str, str | None)
        }
        assert len(declared) == 12, "the pair: the walk finds them"
        listed = {
            (
                path.replace(str(world.recipient), "{recipient_id}").replace(
                    str(world.thread), "{thread_id}"
                ),
                name,
            )
            for path, name, _ in _string_params(world)
        }
        assert listed == declared

    async def test_every_string_parameter(
        self,
        client: AsyncClient,
        world: _World,
    ) -> None:
        params = _string_params(world)
        assert len(params) == 13
        for path, name, rest in params:
            response = await client.get(path, params={**rest, name: _NUL_VALUE})
            _refused(response, f"'{name}'", _SENTINEL)

    async def test_a_plain_string_reads_on(
        self,
        client: AsyncClient,
        world: _World,
    ) -> None:
        """The pair: the same parameters, a storable value -> the
        route's own answer, not this refusal."""
        plain = {
            "key": world.key,
            "locale": "de",
        }
        for path, name, rest in _string_params(world):
            if name == "cursor":
                continue
            response = await client.get(path, params={**rest, name: plain[name]})
            assert response.status_code == 200, (path, name, response.text)

    async def test_a_name_holding_nul(
        self,
        client: AsyncClient,
        world: _World,
    ) -> None:
        response = await client.get(
            "/api/v1/recipients",
            params={_NUL_VALUE: "1"},
        )
        _refused(response, "query parameter name", _SENTINEL)

    async def test_every_get_route(self, client: AsyncClient) -> None:
        """The watchman: the application's own list of GET routes, each
        refusing a NUL in a parameter it does not even declare -- the
        check stands before every route, so a route added later is
        covered without being named here."""
        paths = [route.path for route in _get_routes()]
        assert len(paths) >= 16, "the pair: the walk finds the routes"
        for path in paths:
            concrete = path
            while "{" in concrete:
                start, end = concrete.index("{"), concrete.index("}")
                concrete = concrete[:start] + str(uuid4()) + concrete[end + 1 :]
            response = await client.get(concrete, params={"probe": _NUL_VALUE})
            _refused(response, "'probe'", _SENTINEL)


# -----------------------------------------------------------------------------
# 3 -- a schedule time
# -----------------------------------------------------------------------------


async def _schedule(
    client: AsyncClient,
    recipient: UUID,
    start: str,
    end: str,
) -> Response:
    return await client.patch(
        f"/api/v1/recipients/{recipient}/preferences",
        json={"schedule": [{"day": "mon", "from": start, "to": end}]},
    )


def _names(response: Response, field: str) -> None:
    assert response.status_code == 422, response.text
    error = response.json()["error"]
    assert error["class"] == "validation"
    assert any(f["loc"][-1] == field for f in error["fields"]), error


class TestScheduleTime:
    @pytest.mark.parametrize(
        "start",
        ["09:00\n", "0\u0669:00", "09:00 ", "24:30", ""],
    )
    async def test_from_refused(
        self,
        client: AsyncClient,
        world: _World,
        start: str,
    ) -> None:
        _names(await _schedule(client, world.recipient, start, "23:00"), "from")

    @pytest.mark.parametrize(
        "end",
        ["10:00\n", "1\u0660:00", "24:30", "\u0662\u0664:00"],
    )
    async def test_to_refused(
        self,
        client: AsyncClient,
        world: _World,
        end: str,
    ) -> None:
        _names(await _schedule(client, world.recipient, "09:00", end), "to")

    @pytest.mark.parametrize(
        ("start", "end"),
        [("09:00", "23:59"), ("00:00", "24:00"), ("19:05", "20:00")],
    )
    async def test_well_formed_reads_back(
        self,
        client: AsyncClient,
        world: _World,
        start: str,
        end: str,
    ) -> None:
        response = await _schedule(client, world.recipient, start, end)
        assert response.status_code == 200, response.text
        assert response.json()["schedule"] == [
            {"day": "mon", "from": start, "to": end},
        ]
