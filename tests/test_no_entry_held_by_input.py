# =============================================================================
# R-2 -- a value the product sent never holds the stream
# =============================================================================
#
# A product's wrong input is refused cheaply -- under the key, or dropped
# with one line -- and never HELD: a held entry holds its subjects, and
# every later event of them waits behind it for good (app/transport/
# consumer.py, WHOSE ERROR IT IS). Three values the parse let through
# held their entry until R-2; each case is the reproduction, red on
# primer@5dbfb24, and the event behind it that must now go through:
#
#   1 a user target_value past the column (MAX_TARGET_VALUE_LEN): UUID()
#     takes any number of '-', 'urn:' and 'uuid:', the INSERT then failed
#     on notifications.target_value -- the request held, and with it the
#     user_deleted of its person (TestALongTarget);
#   2 a reminder_cancel naming more distinct types than the driver takes
#     parameters (32767): the statement failed as an InterfaceError, read
#     as transient -- the cancel held, and with it every later request
#     under its correlation (TestManyTypes);
#   3 a timezone that is a directory of the zone database ("America"):
#     zoneinfo raised IsADirectoryError, which nobody caught -- the
#     user_upserted held, and with it every later event of the person;
#     PUT answered 500 (TestAZoneThatIsADirectory). Now it is an unknown
#     zone like any other: stored, said, and the schedule falls back to
#     the default zone -- on both paths alike.
#
# MUTATIONS these tests were written against (each turns one red):
#   M4 the target_value bound removed -> TestALongTarget
#   M5 the bound raised as a RuntimeError (held, not refused) ->
#      TestALongTarget, TestManyTypes
#   M6 the length bound of types removed -> TestManyTypes
#   M8 open_zone without IsADirectoryError -> TestAZoneThatIsADirectory
#   M9 sync.py back on its own except (the helper not used) -> PUT 500 /
#      the user_upserted held: TestAZoneThatIsADirectory
#   M10 _resolve_tz back on its own except ->
#      TestAZoneThatIsADirectory::test_the_schedule_falls_back
#   M11 the drop line carries the value -> TestManyTypes (the sentinel)
# =============================================================================

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from httpx import AsyncClient
from sqlalchemy import select
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.audience.schedule import _resolve_tz, open_zone
from app.core.config import settings
from app.core.constants import MAX_CANCEL_TYPES, MAX_TARGET_VALUE_LEN
from app.core.database import get_session_factory
from app.engine.models import Notification
from app.transport import consumer as consumer_module
from app.transport.consumer import StreamConsumer
from tests import test_transport_consumer as consumer_tests
from tests.helpers import next_snapshot_version
from tests.test_transport_consumer import (
    _no_pending,
    _request_data,
    _run_until,
    _user_upserted_data,
)

# A directory of the zone database: with the tzdata package (both locks
# carry it) zoneinfo opens it and raises IsADirectoryError.
_DIRECTORY_ZONE = "America"

# The driver's ceiling of parameters in one statement (asyncpg): one
# more than it, all distinct, is what held the cancel.
_DRIVER_PARAMETERS = 32767

# A value the product sent: no line of comms may carry it.
_SENTINEL = "zq" + "SENTINEL" + "88"


@pytest.fixture
def fake_redis() -> fakeaioredis.FakeRedis:
    return fakeaioredis.FakeRedis()


@pytest.fixture
def a_stream(monkeypatch: pytest.MonkeyPatch) -> str:
    name = f"comms:test:{uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", name)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    monkeypatch.setattr(consumer_module, "_BACKOFF_DELAYS", (0.01, 0.01, 0.01))
    monkeypatch.setattr(consumer_module, "_WAIT_CEILING", 0.02)
    return name


async def _xadd(
    redis: fakeaioredis.FakeRedis, stream: str, event: str, data: dict[str, Any],
) -> None:
    await redis.xadd(stream, {"event": event, "data": json.dumps(data)})


def _lines(logs: list[dict[str, Any]], event: str) -> list[dict[str, Any]]:
    return [e for e in logs if e["event"] == event]


async def _recipient(rid: UUID) -> Recipient | None:
    async with get_session_factory()() as s:
        return await s.get(Recipient, rid)


async def _forgotten(rid: UUID) -> bool:
    found = await _recipient(rid)
    return found is not None and found.deleted_at is not None


async def _job(key: str) -> Notification | None:
    async with get_session_factory()() as s:
        return await s.scalar(
            select(Notification).where(Notification.idempotency_key == key)
        )


def _padded_uuid(person: UUID, length: int) -> str:
    """The person's uuid in a form UUID() reads, `length` characters
    long: the hyphens it ignores in front."""
    text = str(person)
    return "-" * (length - len(text)) + text


class TestALongTarget:
    async def test_refused_under_the_key_and_the_person_flows(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        client: AsyncClient,
    ) -> None:
        """The request is refused under its key, naming the field; the
        user_deleted of its person behind it is applied. Until R-2 the
        request was held and the deletion waited behind it for good."""
        p = uuid4()
        data = _request_data(
            target_value=_padded_uuid(p, MAX_TARGET_VALUE_LEN + 1),
        )
        key = data["idempotency_key"]
        await _xadd(fake_redis, a_stream, "notification_request", data)
        await _xadd(fake_redis, a_stream, "user_deleted",
                    {"v": 1, "recipient_id": str(p), "version": 2})
        with capture_logs() as logs:

            async def done() -> bool:
                return await _forgotten(p) and await _no_pending(
                    fake_redis, a_stream,
                )

            await _run_until(StreamConsumer(fake_redis), done)
        assert _lines(logs, "event_held") == []
        assert await _job(key) is None
        answer = await client.get(
            "/api/v1/notifications/by-key", params={"key": key},
        )
        intake = answer.json()["intake"]
        assert [i["outcome"] for i in intake] == ["rejected_at_intake"]
        assert "'target_value' exceeds" in intake[0]["reason"]

    async def test_the_column_width_itself_is_taken(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
    ) -> None:
        """The pair: a target exactly MAX_TARGET_VALUE_LEN long is a job
        -- the bound is the column's, not narrower."""
        data = _request_data(
            target_value=_padded_uuid(uuid4(), MAX_TARGET_VALUE_LEN),
        )
        await _xadd(fake_redis, a_stream, "notification_request", data)

        async def done() -> bool:
            return await _job(data["idempotency_key"]) is not None

        await _run_until(StreamConsumer(fake_redis), done)
        job = await _job(data["idempotency_key"])
        assert job is not None
        assert job.target_value == data["target_value"]


class TestManyTypes:
    @pytest.mark.parametrize("count", [
        MAX_CANCEL_TYPES + 1, _DRIVER_PARAMETERS + 1,
    ], ids=["past-the-bound", "past-the-driver"])
    async def test_dropped_and_the_correlation_flows(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str, count: int,
    ) -> None:
        """The cancel is dropped with one line that carries no value;
        the request under its correlation behind it is a job. Until R-2
        the cancel past the driver's parameters was held as transient,
        and the request waited behind it for good."""
        c = f"c-{uuid4().hex}"
        await _xadd(fake_redis, a_stream, "reminder_cancel", {
            "v": 1, "correlation": c,
            "types": [f"{_SENTINEL}{i}" for i in range(count)],
        })
        after = {**consumer_tests.TestReminderCancel._reminder_request(c),
                 "idempotency_key": f"k-{uuid4().hex}"}
        await _xadd(fake_redis, a_stream, "notification_request", after)
        with capture_logs() as logs:

            async def done() -> bool:
                return await _job(after["idempotency_key"]) is not None \
                    and await _no_pending(fake_redis, a_stream)

            await _run_until(StreamConsumer(fake_redis), done)
        assert _lines(logs, "event_held") == []
        (line,) = _lines(logs, "event_dropped")
        assert line["kind"] == "reminder_cancel"
        assert line["stage"] == "parse"
        # The class and the place of the refusal in comms' code, not its
        # text -- the text is the parse's, and a refusal's text may quote
        # what it refused; and nothing of the entry anywhere in the line.
        assert line["reason"].startswith(
            "app.core.exceptions.ValidationError at app/transport/events.py:",
        ), line["reason"]
        assert _SENTINEL not in repr(line)

    async def test_the_bound_itself_is_taken(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
    ) -> None:
        """The pair: MAX_CANCEL_TYPES types are a cancel that runs --
        the job it names is cancelled."""
        c = f"c-{uuid4().hex}"
        request = {**consumer_tests.TestReminderCancel._reminder_request(c),
                   "idempotency_key": f"k-{uuid4().hex}"}
        await _xadd(fake_redis, a_stream, "notification_request", request)
        types = ["unit_rem_1h"] + [
            f"t{i}" for i in range(MAX_CANCEL_TYPES - 1)
        ]
        await _xadd(fake_redis, a_stream, "reminder_cancel",
                    {"v": 1, "correlation": c, "types": types})

        async def done() -> bool:
            job = await _job(request["idempotency_key"])
            return job is not None and job.status == "cancelled" \
                and await _no_pending(fake_redis, a_stream)

        await _run_until(StreamConsumer(fake_redis), done)


class TestAZoneThatIsADirectory:
    def test_the_zone_database_has_it_as_a_directory(self) -> None:
        """The same gate on the instrument: the value is the defect's
        form here -- zoneinfo itself raises IsADirectoryError on it --
        and open_zone gives no zone for it, nor for an unknown name,
        and gives one for a real zone."""
        from zoneinfo import ZoneInfo

        with pytest.raises(IsADirectoryError):
            ZoneInfo(_DIRECTORY_ZONE)
        assert open_zone(_DIRECTORY_ZONE) is None
        assert open_zone("Mars/Olympus") is None
        zone = open_zone("Europe/Berlin")
        assert zone is not None and zone.key == "Europe/Berlin"

    async def test_the_stream_stores_it_and_the_person_flows(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
    ) -> None:
        """user_upserted(q) with the zone is stored as sent, said once
        (as an unknown zone is); user_upserted(p) with it and then
        user_deleted(p) -- the deletion is applied. Until R-2 the first
        snapshot was held, and every later event of its person with it."""
        p, q = uuid4(), uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(q, timezone=_DIRECTORY_ZONE))
        await _xadd(fake_redis, a_stream, "user_upserted",
                    _user_upserted_data(p, timezone=_DIRECTORY_ZONE))
        await _xadd(fake_redis, a_stream, "user_deleted", {
            "v": 1, "recipient_id": str(p), "version": next_snapshot_version(),
        })
        with capture_logs() as logs:

            async def done() -> bool:
                return await _forgotten(p) and await _no_pending(
                    fake_redis, a_stream,
                )

            await _run_until(StreamConsumer(fake_redis), done)
        assert _lines(logs, "event_held") == []
        assert len(_lines(logs, "invalid_timezone_synced")) == 2
        stored = await _recipient(q)
        assert stored is not None
        assert stored.timezone == _DIRECTORY_ZONE

    async def test_put_stores_it_as_the_stream_does(
        self, client: AsyncClient,
    ) -> None:
        """PUT with the zone: 200, the zone stored as sent -- as for an
        unknown zone (tests/test_api_recipients.py). Until R-2: 500."""
        rid = uuid4()
        with capture_logs() as logs:
            response = await client.put(f"/api/v1/recipients/{rid}", json={
                "version": next_snapshot_version(), "telegram_id": None,
                "email": None, "locale": "en", "timezone": _DIRECTORY_ZONE,
                "active": True,
            })
        assert response.status_code == 200
        assert response.json()["timezone"] == _DIRECTORY_ZONE
        assert len(_lines(logs, "invalid_timezone_synced")) == 1
        stored = await _recipient(rid)
        assert stored is not None and stored.timezone == _DIRECTORY_ZONE

    def test_the_schedule_falls_back(self) -> None:
        """A stored directory zone reaches the schedule now that it can
        be stored: the default zone, said -- not an exception on the
        delivery path."""
        with capture_logs() as logs:
            zone = _resolve_tz(_DIRECTORY_ZONE)
        assert zone.key == settings.default_timezone
        assert len(_lines(logs, "invalid_recipient_timezone")) == 1
        assert zone.utcoffset(datetime(2026, 1, 1, tzinfo=UTC)) is not None
