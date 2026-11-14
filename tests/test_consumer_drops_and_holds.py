# =============================================================================
# W5-1 -- no dead letters: a product's bad entry is refused under its key
# or dropped with one line; comms' own failure holds the entry by subject
# =============================================================================
#
#   1 nothing of an entry is kept: after every kind of bad entry the
#     consumer's redis holds the inbound stream and nothing else, and no
#     setting of a dead-letter stream is left (TestNothingKept);
#   2 a notification_request whose key is readable is refused under the
#     key -- the product reads it by the key -- and acknowledged; the
#     same entry again is the same outcome (TestTheRefusalUnderTheKey);
#   3 any other bad entry is ONE line, event_dropped, and the ack: the
#     kind, the stage, the class and place of the refusal, the SHA-256
#     and length of the data bytes -- no value of the entry
#     (TestTheLineOfADrop);
#   4 a failure of comms holds the entry; no later event of its subject
#     is applied before it, a chain of subjects included; every other
#     subject flows; the hold is said once per entry, not once per pass
#     (TestHeldBySubject).
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 the drop XADDs the entry to f"{stream}:dlq" -> TestNothingKept
#   M2 the drop's reason the refusal's text (exception_text) ->
#      TestTheLineOfADrop::test_one_line_without_a_value
#   M3 the drop without its ack -> TestTheLineOfADrop (pending)
#   M4 RejectedNotificationRequest dropped instead of handled ->
#      TestTheRefusalUnderTheKey
#   M5 the held subjects not asked before handling -> TestHeldBySubject::
#      test_the_held_one_is_not_overtaken
#   M6 an entry held behind holds nothing it writes or reads ->
#      TestHeldBySubject::test_a_chain_of_subjects
#   M7 the held subjects not emptied at a pass -> TestHeldBySubject
#      (the entries behind never go out)
#   M8 one subject for every event -> TestHeldBySubject (the other
#      person does not flow)
#   W6-2a (writes and reads, review of 4.0.0, A1):
#   C1 an entry behind holds all it writes and reads, as one set, as
#      until W6-2a -> test_the_reviewers_case,
#      test_a_request_behind_does_not_hold_its_person
#   C3 a write not asked against the held reads ->
#      test_a_chain_of_subjects (the cancel overtakes the request)
#   C4 the reads not asked against the held writes ->
#      test_a_held_cancel_holds_the_requests_it_would_cancel
#   C5 a request held by its failure does not hold its person ->
#      test_the_reviewers_case (user_deleted(p) overtakes it)
#   M9 a person never told about held instead of dropped ->
#      TestTheLineOfADrop::test_a_person_never_told_about
#   M10 a transient failure past the budget dropped instead of held ->
#      TestHeldBySubject::test_a_transient_failure_past_the_budget
#   M11 the subject of section_membership_changed its section_key ->
#      TestHeldBySubject::test_the_subjects
#   M12 event_held said at every pass -> TestHeldBySubject::
#      test_said_once_per_entry
# =============================================================================

import asyncio
import hashlib
import json
from typing import Any
from uuid import UUID, uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from httpx import AsyncClient
from sqlalchemy import select
from structlog.testing import capture_logs

from app.audience.models import GroupMembership, Recipient
from app.core.config import Settings, settings
from app.core.database import get_session_factory
from app.engine.constants import NotificationStatus
from app.engine.models import Notification
from app.transport import consumer as consumer_module
from app.transport.consumer import StreamConsumer, subjects_of
from app.transport.events import (
    GroupChanged,
    NotificationRequest,
    RejectedNotificationRequest,
    ReminderCancel,
    SectionMembershipChanged,
    UserDeleted,
    UserUpserted,
)
from tests.helpers import real_serialization_failure
from tests.test_transport_consumer import (
    _no_pending,
    _request_data,
    _run_until,
    _user_upserted_data,
    _xadd,
)

# A value the product sent, in names and values alike: no line of comms
# may carry it.
_SENTINEL = "zq" + "SENTINEL" + "77"


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


async def _pending(redis: fakeaioredis.FakeRedis, stream: str) -> int:
    """Entries pending in the group; -1 before the group exists."""
    try:
        info = await redis.xpending(stream, settings.comms_consumer_group)
    except Exception:
        return -1
    return int(info["pending"])


async def _recipient(rid: UUID) -> bool:
    async with get_session_factory()() as s:
        found = await s.scalar(select(Recipient.id).where(Recipient.id == rid))
    return found is not None


async def _member(rid: UUID, group_key: str) -> bool:
    async with get_session_factory()() as s:
        found = await s.scalar(
            select(GroupMembership.recipient_id).where(
                GroupMembership.recipient_id == rid,
                GroupMembership.group_key == group_key,
            )
        )
    return found is not None


async def _keys(redis: fakeaioredis.FakeRedis) -> set[str]:
    return {k.decode() for k in await redis.keys("*")}


def _lines(logs: list[dict[str, Any]], event: str) -> list[dict[str, Any]]:
    return [e for e in logs if e["event"] == event]


# The bad entries of every kind, as the stream delivers them, with the
# kind and the stage their line names.
_BAD: list[tuple[str, dict[str, Any], str]] = [
    ("not-utf8", {"event": "user_deleted", "data": b"\xff\xfe" + _SENTINEL.encode()},
     "user_deleted"),
    ("no-event", {"data": json.dumps({"v": 1, _SENTINEL: _SENTINEL})}, "unknown"),
    ("unknown-event", {"event": f"zz_{_SENTINEL}", "data": "{}"}, "unknown"),
    ("broken-json", {"event": "user_deleted", "data": '{"' + _SENTINEL},
     "user_deleted"),
    ("request-without-key", {
        "event": "notification_request",
        "data": json.dumps({"v": 1, "type": "unit_event", "title": _SENTINEL}),
    }, "notification_request"),
    ("unknown-field", {
        "event": "user_deleted",
        "data": json.dumps({"v": 1, "recipient_id": str(uuid4()), "version": 1,
                            f"x_{_SENTINEL}": _SENTINEL}),
    }, "user_deleted"),
    ("version", {
        "event": "user_deleted",
        "data": json.dumps({"v": _SENTINEL, "recipient_id": str(uuid4()),
                            "version": 1}),
    }, "user_deleted"),
    ("no-data", {"event": "user_deleted"}, "user_deleted"),
]


class TestNothingKept:
    async def test_the_inbound_stream_and_nothing_else(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
    ) -> None:
        """Every kind of bad entry, then a good one: all acknowledged, and
        the consumer's redis holds the inbound stream alone -- no stream
        of what came in wrong."""
        for _name, fields, _kind in _BAD:
            await fake_redis.xadd(a_stream, fields)
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def done() -> bool:
            return await _recipient(person) and await _no_pending(fake_redis, a_stream)

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), done)
        assert await _keys(fake_redis) == {a_stream}
        # The pair: the stream is there and holds what came in.
        assert await fake_redis.xlen(a_stream) == len(_BAD) + 1
        assert len(_lines(logs, "event_dropped")) == len(_BAD)

    def test_no_setting_of_a_dead_letter_stream(self) -> None:
        names = set(Settings.model_fields)
        assert not {n for n in names if "dlq" in n}
        assert not hasattr(settings, "dlq_stream")
        # The pair: the consumer's own settings are there.
        assert {"consumer_batch_size", "consumer_block_ms"} <= names


class TestTheRefusalUnderTheKey:
    async def test_the_product_finds_it_by_the_key_and_again(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        client: AsyncClient,
    ) -> None:
        """A request whose key is readable and whose field is unknown: no
        job, the refusal under the key, read by the product's route; the
        same bytes again -- the same one outcome, nothing dropped."""
        data = _request_data(**{f"x_{_SENTINEL}": 1})
        key = data["idempotency_key"]
        fields = {"event": "notification_request", "data": json.dumps(data)}
        await fake_redis.xadd(a_stream, fields)
        with capture_logs() as logs:

            async def done() -> bool:
                return bool(_lines(logs, "event_not_accepted_recorded")) \
                    and await _no_pending(fake_redis, a_stream)

            await _run_until(StreamConsumer(fake_redis), done)
        first = await client.get("/api/v1/notifications/by-key", params={"key": key})
        assert first.status_code == 200
        body = first.json()
        assert body["job"] is None
        assert [i["outcome"] for i in body["intake"]] == ["rejected_at_intake"]

        await fake_redis.xadd(a_stream, fields)
        with capture_logs() as more:

            async def again() -> bool:
                return bool(_lines(more, "event_not_accepted_recorded")) \
                    and await _no_pending(fake_redis, a_stream)

            await _run_until(StreamConsumer(fake_redis), again)
        second = await client.get("/api/v1/notifications/by-key", params={"key": key})
        assert second.json() == body
        assert _lines(logs + more, "event_dropped") == []
        assert len(_lines(logs + more, "event_not_accepted_recorded")) == 2
        assert await _keys(fake_redis) == {a_stream}


class TestTheLineOfADrop:
    @pytest.mark.parametrize(("name", "fields", "kind"), _BAD,
                             ids=[n for n, _f, _k in _BAD])
    async def test_one_line_without_a_value(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        name: str, fields: dict[str, Any], kind: str,
    ) -> None:
        entry_id = (await fake_redis.xadd(a_stream, fields)).decode()
        with capture_logs() as logs:

            async def done() -> bool:
                return bool(_lines(logs, "event_dropped")) \
                    and await _no_pending(fake_redis, a_stream)

            await _run_until(StreamConsumer(fake_redis), done)
        (line,) = _lines(logs, "event_dropped")
        assert set(line) == {"event", "log_level", "entry_id", "kind", "stage",
                             "reason", "data_fingerprint", "data_bytes"}
        assert line["log_level"] == "warning"
        assert line["entry_id"] == entry_id
        assert line["kind"] == kind
        assert line["stage"] == "parse"
        assert line["reason"].startswith(
            "app.core.exceptions.ValidationError at app/transport/events.py:"
        )
        raw = fields.get("data")
        if raw is None:
            assert line["data_fingerprint"] is None
            assert line["data_bytes"] is None
        else:
            raw = raw if isinstance(raw, bytes) else raw.encode()
            assert line["data_fingerprint"] == hashlib.sha256(raw).hexdigest()
            assert line["data_bytes"] == len(raw)
        # No value of the entry in any line -- and the entry did hold one.
        assert _SENTINEL not in repr(logs)
        assert (_SENTINEL in repr(fields)) is (name != "no-data")

    async def test_a_person_never_told_about(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
    ) -> None:
        """group_changed for a person comms never heard of: the ordering
        retry, then the drop -- the product's, not held."""
        data = {"v": 1, "group_key": "g-" + uuid4().hex[:8],
                "recipient_id": str(uuid4()), "member": True}
        await _xadd(fake_redis, a_stream, "group_changed", data)
        with capture_logs() as logs:

            async def done() -> bool:
                return bool(_lines(logs, "event_dropped")) \
                    and await _no_pending(fake_redis, a_stream)

            await _run_until(StreamConsumer(fake_redis), done)
        (line,) = _lines(logs, "event_dropped")
        assert (line["kind"], line["stage"]) == ("group_changed", "retries")
        assert line["reason"].startswith("app.core.exceptions.NotFoundError at ")
        assert _lines(logs, "event_held") == []

    async def test_a_defect_of_the_parse_is_dropped_loudly(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Not a refusal: a defect of comms met in the parse. Never
        parsed, the entry has no subject to be held by -- dropped at
        ERROR, and the next entry is processed."""
        real = consumer_module.parse_event

        def parse(fields: dict[Any, Any]) -> Any:
            if fields.get(b"event") == b"user_deleted":
                raise RuntimeError(_SENTINEL)
            return real(fields)

        monkeypatch.setattr(consumer_module, "parse_event", parse)
        await _xadd(fake_redis, a_stream, "user_deleted",
                    {"v": 1, "recipient_id": str(uuid4()), "version": 1})
        person = uuid4()
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def done() -> bool:
            return await _recipient(person) and await _no_pending(fake_redis, a_stream)

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), done)
        (line,) = _lines(logs, "event_dropped")
        assert (line["log_level"], line["stage"]) == ("error", "parse")
        assert line["reason"].startswith("builtins.RuntimeError at ")
        assert _SENTINEL not in repr(logs)


def _failing(
    monkeypatch: pytest.MonkeyPatch, which: Any, failure: Any,
) -> tuple[list[bool], list[Any]]:
    """handle_event fails with `failure` for the events `which` accepts
    while the switch is on; the events it applied, in order."""
    real = consumer_module.handle_event
    broken = [True]
    applied: list[Any] = []

    async def handle(session: Any, event: Any) -> Any:
        if broken[0] and which(event):
            raise failure() if callable(failure) else failure
        result = await real(session, event)
        applied.append(event)
        return result

    monkeypatch.setattr(consumer_module, "handle_event", handle)
    return broken, applied


class TestHeldBySubject:
    _INTERVAL = 0.1

    @pytest.fixture(autouse=True)
    def _short_interval(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(consumer_module, "_PENDING_REDRAIN_SECONDS",
                            self._INTERVAL)

    def test_the_subjects(self) -> None:
        person, other = uuid4(), uuid4()
        # (writes, reads) since W6-2a; until it one set of subjects.
        writes_person = ({f"person:{person}"}, set())
        assert subjects_of(UserUpserted(person, 1, None, None, None, None, True)) \
            == writes_person
        assert subjects_of(UserDeleted(person, 1)) == writes_person
        assert subjects_of(GroupChanged("g", person, True)) == writes_person
        assert subjects_of(SectionMembershipChanged("s", "S", person, True)) \
            == writes_person
        assert subjects_of(ReminderCancel(["t"], "c", None, None)) \
            == ({"correlation:c"}, set())
        assert subjects_of(RejectedNotificationRequest("k", "f", "r")) \
            == ({"key:k"}, set())
        request = NotificationRequest(
            idempotency_key="k", fingerprint="f", type="t", target_type="user",
            target_value=str(other).upper(), scheduled_at=None, expiry_at=None,
            correlation="c", title=None, body=None, action_data=None, llm=None,
            email=None, llm_audio=None,
        )
        # One form of a person's id, whatever case the request sent.
        assert subjects_of(request) == (
            {"key:k"}, {"correlation:c", f"person:{other}"},
        )
        group = NotificationRequest(
            idempotency_key="k", fingerprint="f", type="t", target_type="group",
            target_value="g", scheduled_at=None, expiry_at=None, correlation=None,
            title=None, body=None, action_data=None, llm=None, email=None,
            llm_audio=None,
        )
        assert subjects_of(group) == ({"key:k"}, set())

    async def test_the_held_one_is_not_overtaken(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A person in the address book; group_changed true fails in
        comms and is held; the false behind it waits; another person
        flows. A restart, comms mended: true, then false -- the person
        is no member. Applied out of order, the true would put them
        back."""
        person, other = uuid4(), uuid4()
        group = "g-" + uuid4().hex[:8]
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))

        async def known() -> bool:
            return await _recipient(person) and await _no_pending(fake_redis, a_stream)

        await _run_until(StreamConsumer(fake_redis), known)
        broken, applied = _failing(
            monkeypatch,
            lambda e: isinstance(e, GroupChanged) and e.member,
            RuntimeError("a defect of comms"),
        )
        change = {"v": 1, "group_key": group, "recipient_id": str(person)}
        await _xadd(fake_redis, a_stream, "group_changed", {**change, "member": True})
        await _xadd(fake_redis, a_stream, "group_changed", {**change, "member": False})
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(other))

        async def other_flowed() -> bool:
            return await _recipient(other) and await _pending(fake_redis, a_stream) == 2

        with capture_logs() as logs:
            await _run_until(StreamConsumer(fake_redis), other_flowed)
        assert [type(e) for e in applied] == [UserUpserted]  # false not applied
        assert len(_lines(logs, "event_held")) == 1
        assert len(_lines(logs, "event_held_behind")) == 1

        broken[0] = False

        async def emptied() -> bool:
            return await _no_pending(fake_redis, a_stream)

        await _run_until(StreamConsumer(fake_redis), emptied)
        assert [(type(e), getattr(e, "member", None)) for e in applied[1:]] == [
            (GroupChanged, True), (GroupChanged, False),
        ]
        assert not await _member(person, group)
        assert await _keys(fake_redis) == {a_stream}

    async def test_a_chain_of_subjects(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A{person} held; B -- a reminder to the person under a
        correlation -- behind it; C, the cancel of that correlation,
        behind B though it names no person. Mended in the running
        process: A, B, C in order -- the reminder cancelled. C before B
        would have cancelled nothing."""
        person = uuid4()
        correlation = str(uuid4())
        broken, applied = _failing(
            monkeypatch, lambda e: isinstance(e, UserUpserted),
            RuntimeError("a defect of comms"),
        )
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))
        from tests.test_transport_consumer import TestReminderCancel
        reminder = TestReminderCancel._reminder_request(correlation)
        reminder["target_value"] = str(person)
        await _xadd(fake_redis, a_stream, "notification_request", reminder)
        await _xadd(fake_redis, a_stream, "reminder_cancel", {
            "v": 1, "types": ["unit_rem_1h"], "correlation": correlation,
        })
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            deadline = asyncio.get_event_loop().time() + 5
            while await _pending(fake_redis, a_stream) != 3 or len(
                    await fake_redis.xrange(a_stream)) != 3:
                assert asyncio.get_event_loop().time() < deadline
                await asyncio.sleep(0.02)
            await asyncio.sleep(self._INTERVAL * 3)  # passes go by, held
            assert applied == []
            broken[0] = False
            while not await _no_pending(fake_redis, a_stream):
                assert asyncio.get_event_loop().time() < deadline
                await asyncio.sleep(0.02)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert [type(e) for e in applied] == [
            UserUpserted, NotificationRequest, ReminderCancel,
        ]
        async with get_session_factory()() as s:
            status = await s.scalar(select(Notification.status).where(
                Notification.idempotency_key == reminder["idempotency_key"]))
        assert status == NotificationStatus.CANCELLED

    async def _in_one_run(
        self, redis: fakeaioredis.FakeRedis, stream: str,
        held: Any, broken: list[bool], *, behind: int,
    ) -> list[dict[str, Any]]:
        """Run the consumer until `behind` entries are left pending and
        everything else is read; check `held` (a sync check of the state
        while held); mend; run until nothing is pending. The log lines."""
        task = asyncio.ensure_future(StreamConsumer(redis).run())
        try:
            with capture_logs() as logs:
                deadline = asyncio.get_event_loop().time() + 5
                total = await redis.xlen(stream)
                while not (
                    await _pending(redis, stream) == behind
                    and len(_lines(logs, "event_processed"))
                    + len(_lines(logs, "event_held"))
                    + len(_lines(logs, "event_held_behind")) >= total
                ):
                    assert asyncio.get_event_loop().time() < deadline, (
                        f"pending {await _pending(redis, stream)}, "
                        f"expected {behind} held"
                    )
                    await asyncio.sleep(0.02)
                await asyncio.sleep(self._INTERVAL * 3)  # passes go by
                held()
                broken[0] = False
                while not await _no_pending(redis, stream):
                    assert asyncio.get_event_loop().time() < deadline
                    await asyncio.sleep(0.02)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        return logs

    async def test_the_reviewers_case(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One request held (to p, under lesson-1); behind it requests to
        q and r under lesson-1 and lesson-2, user_upserted(q),
        user_deleted(r), user_deleted(p). Until W6-2a the held subjects
        spread from entry to entry: pending 7, applied 0 (review of
        4.0.0, A1). Now only user_deleted(p) waits -- the held request's
        own person -- and everything else is applied; mended, the held
        request goes out, then user_deleted(p)."""
        p, q, r = uuid4(), uuid4(), uuid4()
        key = f"held-{uuid4().hex}"
        broken, applied = _failing(
            monkeypatch,
            lambda e: getattr(e, "idempotency_key", None) == key,
            RuntimeError("a defect of comms"),
        )

        def request(person: UUID, correlation: str) -> dict[str, Any]:
            return _request_data(idempotency_key=f"k-{uuid4().hex}",
                                 target_value=str(person), correlation=correlation)

        await _xadd(fake_redis, a_stream, "notification_request", {
            **request(p, "lesson-1"), "idempotency_key": key,
        })
        for person, correlation in ((q, "lesson-1"), (r, "lesson-1"),
                                    (q, "lesson-2")):
            await _xadd(fake_redis, a_stream, "notification_request",
                        request(person, correlation))
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(q))
        await _xadd(fake_redis, a_stream, "user_deleted",
                    {"v": 1, "recipient_id": str(r), "version": 2})
        await _xadd(fake_redis, a_stream, "user_deleted",
                    {"v": 1, "recipient_id": str(p), "version": 2})

        def while_held() -> None:
            kinds = [type(e).__name__ for e in applied]
            assert sorted(kinds) == sorted([
                "NotificationRequest", "NotificationRequest",
                "NotificationRequest", "UserUpserted", "UserDeleted",
            ])
            # The one deletion applied is r's: p's waits.
            (deleted,) = [e for e in applied if isinstance(e, UserDeleted)]
            assert deleted.recipient_id == r

        logs = await self._in_one_run(fake_redis, a_stream, while_held, broken,
                                      behind=2)
        assert len(_lines(logs, "event_held_behind")) == 1
        tail = applied[-2:]
        assert isinstance(tail[0], NotificationRequest)
        assert tail[0].idempotency_key == key
        assert isinstance(tail[1], UserDeleted) and tail[1].recipient_id == p

    async def test_a_held_cancel_holds_the_requests_it_would_cancel(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """reminder_cancel(c) held; a request under c behind it waits --
        applied first, the cancel would cancel a request sent after it;
        a request under another correlation flows."""
        from tests.test_transport_consumer import TestReminderCancel
        c, other = f"c-{uuid4().hex}", f"c-{uuid4().hex}"
        broken, applied = _failing(
            monkeypatch, lambda e: isinstance(e, ReminderCancel),
            RuntimeError("a defect of comms"),
        )
        await _xadd(fake_redis, a_stream, "reminder_cancel", {
            "v": 1, "types": ["unit_rem_1h"], "correlation": c,
        })
        after = {**TestReminderCancel._reminder_request(c),
                 "idempotency_key": f"k-{uuid4().hex}"}
        await _xadd(fake_redis, a_stream, "notification_request", after)
        elsewhere = {**TestReminderCancel._reminder_request(other),
                     "idempotency_key": f"k-{uuid4().hex}"}
        await _xadd(fake_redis, a_stream, "notification_request", elsewhere)

        def while_held() -> None:
            assert [e.correlation for e in applied] == [other]

        await self._in_one_run(fake_redis, a_stream, while_held, broken, behind=2)
        assert [type(e).__name__ for e in applied[1:]] == [
            "ReminderCancel", "NotificationRequest",
        ]
        async with get_session_factory()() as s:
            status = await s.scalar(select(Notification.status).where(
                Notification.idempotency_key == after["idempotency_key"]))
        # Sent after the cancel, it was not cancelled by it.
        assert status != NotificationStatus.CANCELLED

    async def test_a_request_behind_does_not_hold_its_person(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """reminder_cancel(c) held; a request to p under c waits behind
        it; user_deleted(p) does not wait behind the request -- a request
        held behind holds its correlation, not its person."""
        from tests.test_transport_consumer import TestReminderCancel
        c, p = f"c-{uuid4().hex}", uuid4()
        broken, applied = _failing(
            monkeypatch, lambda e: isinstance(e, ReminderCancel),
            RuntimeError("a defect of comms"),
        )
        await _xadd(fake_redis, a_stream, "reminder_cancel", {
            "v": 1, "types": ["unit_rem_1h"], "correlation": c,
        })
        await _xadd(fake_redis, a_stream, "notification_request", {
            **TestReminderCancel._reminder_request(c),
            "idempotency_key": f"k-{uuid4().hex}", "target_value": str(p),
        })
        await _xadd(fake_redis, a_stream, "user_deleted",
                    {"v": 1, "recipient_id": str(p), "version": 2})

        def while_held() -> None:
            assert [type(e).__name__ for e in applied] == ["UserDeleted"]

        await self._in_one_run(fake_redis, a_stream, while_held, broken, behind=2)
        assert [type(e).__name__ for e in applied[1:]] == [
            "ReminderCancel", "NotificationRequest",
        ]

    async def test_said_once_per_entry(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Held for good: met again at every pass, said once -- the held
        one and the one behind it alike; another person flows."""
        person = uuid4()
        met = [0]

        def which(event: Any) -> bool:
            if isinstance(event, UserDeleted):
                met[0] += 1
                return True
            return False

        _failing(monkeypatch, which, RuntimeError("a defect of comms"))
        await _xadd(fake_redis, a_stream, "user_deleted",
                    {"v": 1, "recipient_id": str(person), "version": 1})
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            with capture_logs() as logs:
                await asyncio.sleep(self._INTERVAL * 8)
                other = uuid4()
                await _xadd(fake_redis, a_stream, "user_upserted",
                            _user_upserted_data(other))
                deadline = asyncio.get_event_loop().time() + 5
                while not await _recipient(other):
                    assert asyncio.get_event_loop().time() < deadline
                    await asyncio.sleep(0.02)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert met[0] >= 3  # met again, pass after pass
        assert len(_lines(logs, "event_held")) == 1
        assert len(_lines(logs, "event_held_behind")) == 1
        assert not await _recipient(person)  # the upsert held behind
        assert await _pending(fake_redis, a_stream) == 2

    async def test_a_transient_failure_past_the_budget(
        self, fake_redis: fakeaioredis.FakeRedis, a_stream: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The database answers, the event keeps failing transiently: the
        budget, then held -- not dropped; its retries said on the first
        pass only; once the failure ends, applied."""
        failure = await real_serialization_failure()
        person, other = uuid4(), uuid4()
        broken, applied = _failing(
            monkeypatch,
            lambda e: isinstance(e, UserUpserted) and e.recipient_id == person,
            failure,
        )
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(person))
        await _xadd(fake_redis, a_stream, "user_upserted", _user_upserted_data(other))
        task = asyncio.ensure_future(StreamConsumer(fake_redis).run())
        try:
            with capture_logs() as logs:
                deadline = asyncio.get_event_loop().time() + 5
                while not await _recipient(other):
                    assert asyncio.get_event_loop().time() < deadline
                    await asyncio.sleep(0.02)
                await asyncio.sleep(self._INTERVAL * 4)
                assert await _pending(fake_redis, a_stream) == 1
                broken[0] = False
                while not (await _recipient(person)
                           and await _no_pending(fake_redis, a_stream)):
                    assert asyncio.get_event_loop().time() < deadline
                    await asyncio.sleep(0.02)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        (held,) = _lines(logs, "event_held")
        assert held["attempts"] == len(consumer_module._BACKOFF_DELAYS) + 1
        assert _lines(logs, "event_dropped") == []
        retries = _lines(logs, "event_retry_scheduled")
        assert len(retries) == len(consumer_module._BACKOFF_DELAYS)
        assert len(_lines(logs, "event_retries_exhausted")) == 1
        assert [e.recipient_id for e in applied] == [other, person]
