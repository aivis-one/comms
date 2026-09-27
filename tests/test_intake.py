# =============================================================================
# COMMS Service -- Envelope and intake (F1.2)
# =============================================================================
# What intake promises since F1.2, by input:
#   - the channel is the PROFILE's, per type, snapshotted at intake; it
#     cannot be named in a call in any form;
#   - the key is required everywhere; same key + same bytes -> the same
#     job, same key + other bytes -> a conflict recorded under the key,
#     never two jobs, not even under a race;
#   - a request whose key is readable but which cannot be accepted is
#     recorded under the key as rejected_at_intake -- a class distinct
#     from conflict and from a failure in the lifecycle;
#   - the expiry comes from the envelope over the profile, with its
#     layer;
#   - intake is durable: commit before XACK.
#
# THREE DOUBLE AXES -- REPEAT, EMPTY, SHORTFALL -- per input, named in
# each class docstring.
# =============================================================================

import asyncio
import inspect
import json
from dataclasses import fields
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.constants import MAX_CORRELATION_LEN
from app.core.database import get_session_factory
from app.engine.constants import IntakeOutcomeClass, NotificationStatus
from app.engine.models import IntakeOutcome, Notification
from app.engine.service import (
    Intake,
    accept_notification,
    canonical_fingerprint,
    create_notification,
    expiry_of,
    intake_outcomes_for,
    resolve_notification,
    stream_fingerprint,
)
from app.notifier import notify_new_message
from app.profile.loader import RawProfile, install_profile, parse_profile
from app.profile.registry import Layer, registry
from app.transport.consumer import StreamConsumer
from app.transport.events import (
    NotificationRequest,
    RejectedNotificationRequest,
    parse_event,
)
from app.transport.handlers import HandleResult, handle_event
from tests.helpers import create_recipient, intake_fields

_WAIT_TIMEOUT = 5.0


def _data(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "v": 1,
        "idempotency_key": f"intake-{uuid4().hex}",
        "type": "unit_event",
        "target_type": "all",
        "target_value": "*",
        "title": "T",
        "body": "B",
    }
    data.update(overrides)
    return data


def _entry(data: dict[str, Any] | str) -> dict[str, str]:
    raw = data if isinstance(data, str) else json.dumps(data)
    return {"event": "notification_request", "data": raw}


async def _handle(session: AsyncSession, data: dict[str, Any] | str) -> HandleResult:
    """Parse and handle one entry the way the consumer does."""
    return await handle_event(session, parse_event(_entry(data)))


async def _jobs(session: AsyncSession, key: str) -> list[Notification]:
    rows = await session.execute(
        select(Notification).where(Notification.idempotency_key == key)
    )
    return list(rows.scalars().all())


async def _accept(session: AsyncSession, key: str, fingerprint: str, **kw: Any) -> Any:
    fields_: dict[str, Any] = {
        "type": "unit_event_in_app",
        "title": "T",
        "body": "B",
        "target_type": "all",
        "target_value": "*",
    }
    fields_.update(kw)
    return await accept_notification(
        session, idempotency_key=key, fingerprint=fingerprint, **fields_,
    )


# -----------------------------------------------------------------------------
# Item 1 -- the channel comes from the profile
# -----------------------------------------------------------------------------


class TestChannelsFromTheProfile:
    """REPEAT: the same call for two types -> two different deliveries.
    EMPTY: a type without `channels` -> the default in_app. SHORTFALL:
    a `_channels` key smuggled into the letter is inert."""

    async def test_two_types_same_call_different_deliveries(
        self, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        channels_by_type = {}
        for type_key in ("unit_event", "unit_event_in_app_email"):
            notification = await create_notification(
                db_session,
                idempotency_key=f"k-{type_key}", fingerprint="a" * 64,
                type=type_key, title="T", body="B",
                target_type="user", target_value=str(recipient.id),
            )
            deliveries = await resolve_notification(db_session, notification)
            channels_by_type[type_key] = sorted(d.channel for d in deliveries)
        assert channels_by_type == {
            "unit_event": ["telegram"],
            "unit_event_in_app_email": ["email", "in_app"],
        }

    async def test_type_without_channels_takes_the_default(
        self, db_session: AsyncSession,
    ) -> None:
        notification = await create_notification(
            db_session, idempotency_key="k-bare", fingerprint="a" * 64,
            type="unit_bare", title="T", body="B",
            target_type="all", target_value="*",
        )
        assert notification.channels == ["in_app"]

    async def test_a_channels_key_in_the_letter_is_inert(
        self, db_session: AsyncSession,
    ) -> None:
        """The old stash key, if it ever reaches the letter, routes
        nothing: resolve reads the snapshot column only."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session, idempotency_key="k-stash", fingerprint="a" * 64,
            type="unit_event", title="T", body="B",
            target_type="user", target_value=str(recipient.id),
            action_data={"_channels": ["email"]},
        )
        deliveries = await resolve_notification(db_session, notification)
        assert [d.channel for d in deliveries] == ["telegram"]

    async def test_the_route_is_a_snapshot_taken_at_intake(
        self, db_session: AsyncSession,
    ) -> None:
        """A restart with another profile does not re-route a job that
        was already accepted."""
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session, idempotency_key="k-snap", fingerprint="a" * 64,
            type="unit_event", title="T", body="B",
            target_type="user", target_value=str(recipient.id),
        )
        doc = {"version": 2, "types": {
            "unit_event": {"category": "unit_updates", "channels": ["email"]},
            "msg.participant_message": {"category": "m"},
            "msg.support_message": {"category": "m"},
            "msg.thread_closed": {"category": "m"},
        }}
        install_profile(parse_profile(RawProfile(types=doc)), registry)
        assert registry.explain("unit_event", "channels").value == ("email",)
        deliveries = await resolve_notification(db_session, notification)
        assert [d.channel for d in deliveries] == ["telegram"]

    def test_no_call_surface_names_a_channel(self) -> None:
        """The forms a channel could be named in a call, each checked;
        each absence has its pair (the neighbouring field IS there)."""
        create_params = inspect.signature(create_notification).parameters
        assert "channels" not in create_params
        assert "type" in create_params
        request_fields = {f.name for f in fields(NotificationRequest)}
        assert "channels" not in request_fields
        assert "priority" not in request_fields
        assert "type" in request_fields
        assert "correlation" in request_fields
        rejected = parse_event(_entry(_data(channels=["in_app"])))
        assert isinstance(rejected, RejectedNotificationRequest)
        assert "unknown field(s) 'channels'" in rejected.reason


# -----------------------------------------------------------------------------
# Items 3, 4 -- the key, duplicates and conflicts
# -----------------------------------------------------------------------------


class TestKeyAndFingerprint:
    """REPEAT: same bytes; one byte of difference; a race. EMPTY: a
    missing or empty key (DLQ -- no address). SHORTFALL: a key longer
    than its column."""

    async def test_same_key_same_bytes_is_the_same_job(
        self, db_session: AsyncSession,
    ) -> None:
        data = _data()
        assert await _handle(db_session, data) is HandleResult.PROCESSED
        first = (await _jobs(db_session, data["idempotency_key"]))[0]
        assert await _handle(db_session, data) is HandleResult.DUPLICATE
        jobs = await _jobs(db_session, data["idempotency_key"])
        assert [j.id for j in jobs] == [first.id]
        assert await intake_outcomes_for(db_session, data["idempotency_key"]) == []

    async def test_one_byte_of_difference_is_a_conflict(
        self, db_session: AsyncSession,
    ) -> None:
        """The SAME meaning, serialised with one extra space: bytes are
        compared, not meaning (spec §5.8)."""
        data = _data()
        compact = json.dumps(data, separators=(",", ":"))
        spaced = compact.replace('"title":"T"', '"title": "T"', 1)
        assert len(spaced) == len(compact) + 1
        assert await _handle(db_session, compact) is HandleResult.PROCESSED
        assert await _handle(db_session, spaced) is HandleResult.CONFLICT

        (job,) = await _jobs(db_session, data["idempotency_key"])
        assert job.fingerprint == stream_fingerprint(compact.encode())
        (outcome,) = await intake_outcomes_for(db_session, data["idempotency_key"])
        assert outcome.outcome == IntakeOutcomeClass.CONFLICT
        assert outcome.notification_id == job.id
        assert outcome.fingerprint == stream_fingerprint(spaced.encode())

    async def test_a_conflict_leaves_the_job_untouched(
        self, db_session: AsyncSession,
    ) -> None:
        data = _data(title="original")
        await _handle(db_session, data)
        await _handle(db_session, {**data, "title": "replaced"})
        (job,) = await _jobs(db_session, data["idempotency_key"])
        assert job.title == "original"

    async def test_a_replayed_conflict_records_once(
        self, db_session: AsyncSession,
    ) -> None:
        data = _data()
        await _handle(db_session, data)
        other = {**data, "body": "other"}
        for _ in range(3):
            assert await _handle(db_session, other) is HandleResult.CONFLICT
        assert len(await intake_outcomes_for(db_session, data["idempotency_key"])) == 1

    async def test_two_concurrent_intakes_make_one_job(self) -> None:
        """A second insert under the key waits for the first transaction
        and then fails on the unique index: the holder is read only
        after that -- never two jobs, whatever the bytes."""
        factory = get_session_factory()
        for second_fingerprint, expected in (
            ("a" * 64, Intake.DUPLICATE),
            ("b" * 64, Intake.CONFLICT),
        ):
            key = f"race-{uuid4().hex}"
            async with factory() as first, factory() as second:
                accepted = await _accept(first, key, "a" * 64)
                racing = asyncio.ensure_future(
                    _accept(second, key, second_fingerprint),
                )
                await asyncio.sleep(0.2)
                assert not racing.done()  # blocked on the index
                await first.commit()
                answer = await asyncio.wait_for(racing, _WAIT_TIMEOUT)
                await second.commit()
            assert accepted.outcome is Intake.ACCEPTED
            assert answer.outcome is expected
            assert answer.notification.id == accepted.notification.id
            async with factory() as check:
                assert len(await _jobs(check, key)) == 1

    @pytest.mark.parametrize("key", [None, "", 7, "k" * 201])
    def test_an_unreadable_key_has_no_address(self, key: Any) -> None:
        data = _data()
        if key is None:
            del data["idempotency_key"]
        else:
            data["idempotency_key"] = key
        with pytest.raises(Exception, match="idempotency_key"):
            parse_event(_entry(data))

    async def test_the_key_column_refuses_null(
        self, db_session: AsyncSession,
    ) -> None:
        db_session.add(Notification(
            type="unit_event", title="T", body="B",
            target_type="all", target_value="*",
            fingerprint="a" * 64, channels=["in_app"], expiry_layer="default",
        ))
        with pytest.raises(IntegrityError, match="idempotency_key"):
            await db_session.flush()

    async def test_the_key_column_takes_a_key(
        self, db_session: AsyncSession,
    ) -> None:
        """The pair: the same row WITH a key flushes."""
        db_session.add(Notification(
            type="unit_event", title="T", body="B",
            target_type="all", target_value="*", idempotency_key="k",
            fingerprint="a" * 64, channels=["in_app"], expiry_layer="default",
        ))
        await db_session.flush()


# -----------------------------------------------------------------------------
# Item 6 -- rejected at intake
# -----------------------------------------------------------------------------


class TestRejectedAtIntake:
    """REPEAT: the same rejected bytes twice -> one record. EMPTY: an
    empty correlation, an empty target. SHORTFALL: an undeclared type,
    an expiry already passed."""

    @pytest.mark.parametrize("overrides,reason", [
        ({"type": "not_in_profile"}, "Unregistered notification type"),
        ({"target_type": "user", "target_value": "not-a-uuid"}, "not a valid uuid"),
        ({"target_value": ""}, "target_value"),
        ({"target_type": "planet"}, "target_type"),
        ({"correlation": ""}, "'correlation' must be a non-empty string"),
        ({"correlation": 17}, "'correlation' must be a non-empty string"),
        ({"correlation": "c" * (MAX_CORRELATION_LEN + 1)}, "exceeds 200"),
        ({"priority": 1}, "unknown field(s) 'priority'"),
        ({"expiry_at": "2020-01-01T00:00:00+00:00"}, "has already passed"),
        (
            {
                "scheduled_at": "2090-01-02T00:00:00+00:00",
                "expiry_at": "2090-01-01T00:00:00+00:00",
            },
            "is not after scheduled_at",
        ),
    ])
    async def test_each_form_is_recorded_under_the_key(
        self, db_session: AsyncSession, overrides: dict[str, Any], reason: str,
    ) -> None:
        data = _data(**overrides)
        assert await _handle(db_session, data) is HandleResult.REJECTED
        (outcome,) = await intake_outcomes_for(db_session, data["idempotency_key"])
        assert outcome.outcome == IntakeOutcomeClass.REJECTED_AT_INTAKE
        assert reason in outcome.reason
        assert outcome.notification_id is None
        assert await _jobs(db_session, data["idempotency_key"]) == []

    async def test_the_same_rejected_bytes_record_once(
        self, db_session: AsyncSession,
    ) -> None:
        data = _data(type="not_in_profile")
        for _ in range(3):
            await _handle(db_session, data)
        assert len(await intake_outcomes_for(db_session, data["idempotency_key"])) == 1

    async def test_a_rejection_does_not_occupy_the_key(
        self, db_session: AsyncSession,
    ) -> None:
        """Fixed and resent under the same key -> accepted."""
        data = _data(type="not_in_profile")
        assert await _handle(db_session, data) is HandleResult.REJECTED
        fixed = {**data, "type": "unit_event"}
        assert await _handle(db_session, fixed) is HandleResult.PROCESSED
        assert len(await _jobs(db_session, data["idempotency_key"])) == 1

    async def test_the_three_outcomes_are_told_apart_by_class(
        self, db_session: AsyncSession,
    ) -> None:
        """Rejected, conflict and a failure in the lifecycle: three
        different programmatic answers, none of them a text."""
        rejected = _data(type="not_in_profile")
        await _handle(db_session, rejected)
        taken = _data()
        await _handle(db_session, taken)
        await _handle(db_session, {**taken, "body": "other"})
        failed = _data()
        await _handle(db_session, failed)
        (job,) = await _jobs(db_session, failed["idempotency_key"])
        job.status = NotificationStatus.FAILED
        await db_session.flush()

        (r,) = await intake_outcomes_for(db_session, rejected["idempotency_key"])
        (c,) = await intake_outcomes_for(db_session, taken["idempotency_key"])
        assert r.outcome == IntakeOutcomeClass.REJECTED_AT_INTAKE
        assert c.outcome == IntakeOutcomeClass.CONFLICT
        assert await intake_outcomes_for(db_session, failed["idempotency_key"]) == []
        assert job.status == NotificationStatus.FAILED
        assert {r.outcome, c.outcome} == set(IntakeOutcomeClass)


# -----------------------------------------------------------------------------
# Item 7 -- the expiry, envelope over profile
# -----------------------------------------------------------------------------


class TestExpiryLayers:
    """REPEAT: both layers carry an expiry -> the envelope. EMPTY:
    neither -> no expiry. SHORTFALL: only the profile -> counted from
    scheduled_at."""

    async def test_envelope_wins_over_profile(self, db_session: AsyncSession) -> None:
        when = datetime.now(UTC) + timedelta(days=3)
        job = await create_notification(
            db_session, idempotency_key="e1", fingerprint="a" * 64,
            type="unit_expiring", title="T", body="B",
            target_type="all", target_value="*", expiry_at=when,
        )
        decided = expiry_of(job)
        assert (decided.value, decided.layer) == (when, Layer.ENVELOPE)
        assert decided.source == "envelope: expiry_at"

    async def test_profile_counts_from_scheduled_at(
        self, db_session: AsyncSession,
    ) -> None:
        not_before = datetime.now(UTC) + timedelta(days=1)
        job = await create_notification(
            db_session, idempotency_key="e2", fingerprint="a" * 64,
            type="unit_expiring", title="T", body="B",
            target_type="all", target_value="*", scheduled_at=not_before,
        )
        decided = expiry_of(job)
        assert decided.value == not_before + timedelta(hours=1)
        assert decided.layer is Layer.PROFILE
        assert "expires_after of 'unit_expiring'" in decided.source

    async def test_neither_layer_means_no_expiry(
        self, db_session: AsyncSession,
    ) -> None:
        job = await create_notification(
            db_session, idempotency_key="e3", fingerprint="a" * 64,
            type="unit_bare", title="T", body="B",
            target_type="all", target_value="*",
        )
        decided = expiry_of(job)
        assert (decided.value, decided.layer) == (None, Layer.DEFAULT)
        assert job.expiry_layer == "default"


# -----------------------------------------------------------------------------
# Item 2 -- the correlation is carried untouched
# -----------------------------------------------------------------------------


class TestCorrelation:
    """REPEAT: n/a. EMPTY: absent -> NULL; "" -> rejected (above).
    SHORTFALL: at the column width -> stored."""

    async def test_carried_untouched(self, db_session: AsyncSession) -> None:
        value = "ord-17/ä€ {x} $.amount"
        data = _data(correlation=value)
        await _handle(db_session, data)
        (job,) = await _jobs(db_session, data["idempotency_key"])
        assert job.correlation == value

    async def test_absent_is_null_and_the_width_fits(
        self, db_session: AsyncSession,
    ) -> None:
        absent, full = _data(), _data(correlation="c" * MAX_CORRELATION_LEN)
        await _handle(db_session, absent)
        await _handle(db_session, full)
        (absent_job,) = await _jobs(db_session, absent["idempotency_key"])
        (full_job,) = await _jobs(db_session, full["idempotency_key"])
        assert absent_job.correlation is None
        assert full_job.correlation == "c" * MAX_CORRELATION_LEN


# -----------------------------------------------------------------------------
# Amendment 4 -- two fingerprints, and one meaning through both paths
# -----------------------------------------------------------------------------


class TestTwoFingerprints:
    def test_canonical_is_order_independent(self) -> None:
        assert canonical_fingerprint({"a": 1, "b": 2}) == canonical_fingerprint(
            {"b": 2, "a": 1},
        )
        assert canonical_fingerprint({"a": 1}) != canonical_fingerprint({"a": 2})

    async def test_a_product_key_equal_to_an_internal_one_conflicts(
        self, db_session: AsyncSession,
    ) -> None:
        """The same key and the same meaning arriving through both
        paths: the digests differ by construction, so the collision is
        a recorded conflict for the product, and the chat ping stands."""
        sender = await create_recipient(db_session)
        client = await create_recipient(db_session)
        from app.messaging.constants import OperatorKind, ThreadKind
        from app.messaging.threads import create_or_get_thread, post_message

        thread = await create_or_get_thread(
            db_session,
                **intake_fields(), client=client.id, operator_kind=OperatorKind.USER,
            operator_value=sender.id, kind=ThreadKind.DM,
        )
        message = await post_message(
            db_session,
                **intake_fields(), thread_id=thread.id, sender=client.id, body="hi",
        )
        await db_session.refresh(thread)
        (ping,) = await notify_new_message(db_session, thread=thread, message=message)
        data = _data(
            idempotency_key=ping.idempotency_key,
            type=ping.type, title=ping.title, body=ping.body or "x",
        )
        assert await _handle(db_session, data) is HandleResult.CONFLICT
        assert len(await _jobs(db_session, ping.idempotency_key)) == 1


# -----------------------------------------------------------------------------
# Item 5 -- durable intake: commit before XACK
# -----------------------------------------------------------------------------


class TestDurableIntake:
    """A crash between the commit and the XACK loses nothing and
    creates nothing twice: the entry stays pending, the next start
    replays it, the key collapses the replay."""

    async def test_crash_between_commit_and_ack(
        self, monkeypatch: pytest.MonkeyPatch, db_session: AsyncSession,
    ) -> None:
        redis = fakeaioredis.FakeRedis()
        stream = f"comms:test:{uuid4().hex[:8]}"
        monkeypatch.setattr(settings, "comms_events_stream", stream)
        monkeypatch.setattr(settings, "consumer_block_ms", 20)
        data = _data()
        await redis.xadd(stream, _entry(data))

        crashing = StreamConsumer(redis)

        async def crash(entry_id: Any) -> None:
            raise RuntimeError("crash between commit and XACK")

        monkeypatch.setattr(crashing, "_ack", crash)
        with pytest.raises(RuntimeError, match="between commit and XACK"):
            await crashing.run()
        db_session.expire_all()
        assert len(await _jobs(db_session, data["idempotency_key"])) == 1
        pending = await redis.xpending_range(
            stream, settings.comms_consumer_group, min="-", max="+", count=10,
        )
        assert len(pending) == 1

        restarted = StreamConsumer(redis)
        task = asyncio.ensure_future(restarted.run())
        try:
            deadline = asyncio.get_event_loop().time() + _WAIT_TIMEOUT
            while await redis.xpending_range(
                stream, settings.comms_consumer_group, min="-", max="+",
                count=10,
            ):
                assert asyncio.get_event_loop().time() < deadline
                await asyncio.sleep(0.02)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        db_session.expire_all()
        assert len(await _jobs(db_session, data["idempotency_key"])) == 1
        assert await intake_outcomes_for(db_session, data["idempotency_key"]) == []

    async def test_a_rollback_before_commit_replays_as_one_acceptance(
        self, monkeypatch: pytest.MonkeyPatch, db_session: AsyncSession,
    ) -> None:
        """The other side: a crash BEFORE the commit writes nothing, and
        the replay is the one acceptance."""
        redis = fakeaioredis.FakeRedis()
        stream = f"comms:test:{uuid4().hex[:8]}"
        monkeypatch.setattr(settings, "comms_events_stream", stream)
        monkeypatch.setattr(settings, "consumer_block_ms", 20)
        data = _data()
        await redis.xadd(stream, _entry(data))

        from sqlalchemy.ext.asyncio import AsyncSession as _Session

        real_commit = _Session.commit
        calls = {"n": 0}

        async def failing_commit(self: _Session) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise asyncio.CancelledError
            await real_commit(self)

        monkeypatch.setattr(_Session, "commit", failing_commit)
        with pytest.raises(asyncio.CancelledError):
            await StreamConsumer(redis).run()
        db_session.expire_all()
        monkeypatch.setattr(_Session, "commit", real_commit)
        assert await _jobs(db_session, data["idempotency_key"]) == []

        task = asyncio.ensure_future(StreamConsumer(redis).run())
        try:
            deadline = asyncio.get_event_loop().time() + _WAIT_TIMEOUT
            while not await _jobs(db_session, data["idempotency_key"]):
                assert asyncio.get_event_loop().time() < deadline
                await asyncio.sleep(0.02)
                db_session.expire_all()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert len(await _jobs(db_session, data["idempotency_key"])) == 1


# -----------------------------------------------------------------------------
# Retention covers the intake records
# -----------------------------------------------------------------------------


class TestIntakeRetention:
    """The records of requests that were not accepted follow the same
    horizon as the jobs (NOTIFICATION_RETENTION_DAYS)."""

    async def test_old_records_go_fresh_ones_stay(
        self, monkeypatch: pytest.MonkeyPatch, db_session: AsyncSession,
    ) -> None:
        from app.engine.processor import cleanup_terminal_notifications

        monkeypatch.setattr(settings, "notification_retention_days", 30)
        now = datetime.now(UTC)
        for key, age in (("old", 31), ("fresh", 29)):
            db_session.add(IntakeOutcome(
                idempotency_key=key, fingerprint="a" * 64,
                outcome=IntakeOutcomeClass.REJECTED_AT_INTAKE.value,
                reason="r", received_at=now - timedelta(days=age),
            ))
        await db_session.commit()

        await cleanup_terminal_notifications()

        db_session.expire_all()
        remaining = (await db_session.execute(
            select(IntakeOutcome.idempotency_key)
        )).scalars().all()
        assert remaining == ["fresh"]
