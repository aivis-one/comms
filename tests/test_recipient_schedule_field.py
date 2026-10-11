# =============================================================================
# H3 Б1 Б3 -- a type that declares recipient_schedule: bypass is
# delivered outside the recipient's schedule; nothing else changes
# =============================================================================
#
# Through the real pipeline: a recipient whose only allowed period is
# two days ahead -- so NOW is outside it -- and a type that declares or
# does not declare the field. The value is the job's, snapshotted at
# intake (notifications.recipient_schedule, migration 0022).
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the schedule gate ignores the field     -> TestTheGate, TestLoginCode
#   M2  bypass also passes the late mute        -> TestMutes (after resolve)
#   M3  bypass also passes the resolve-time mute -> TestMutes (before)
#   M4  the field read live from the registry   -> TestSnapshot
#   M5  migration 0022 filling 'bypass'         -> test_migration_0022
#   M6  the loader taking any value             -> TestLoader
#   M10 a value in the document, not the loader -> TestTheContract
# =============================================================================

import os
import re
import shutil
import subprocess
import sys
from collections.abc import AsyncGenerator, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import CategoryMute
from app.core.database import dispose_engine, get_session_factory
from app.core.exceptions import ProfileError
from app.engine import service
from app.engine.constants import DeliveryStatus, NotificationStatus, TargetType
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import process_pending_notifications
from app.engine.service import create_notification
from app.profile.loader import (
    _FIELDS,
    RECIPIENT_SCHEDULE,
    RECORD_FIELDS,
    FileProfileSource,
    load_profile,
)
from app.profile.registry import Decided, Layer, TypeRecord, registry
from tests.helpers import create_recipient, intake_fields, notification_row_fields

_REPO = Path(__file__).resolve().parents[1]
_FIXTURE = _REPO / "tests" / "fixtures" / "profile"
_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")
_TYPE = "unit_event_in_app"     # category unit_updates; no field declared
_CATEGORY = "unit_updates"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _declare(value: str, type_key: str = _TYPE) -> None:
    record = registry.record_of(type_key)
    assert record is not None
    fields = dict(record.fields)
    fields["recipient_schedule"] = Decided(value, Layer.PROFILE, "test")
    registry.register_type(
        type_key, category=registry.category_of(type_key),
        record=TypeRecord(fields=fields),
    )


class _Spy:
    """The in_app channel, answering yes."""

    async def deliver(self, *args: Any) -> bool:
        return True


@contextmanager
def _channel() -> Iterator[None]:
    with patch("app.engine.service.get_formatter", return_value=_Spy()):
        yield


async def _outside_its_schedule(session: AsyncSession) -> UUID:
    """A recipient whose one allowed period is two days ahead: now is
    outside it, and the next opening is more than a day away."""
    recipient = await create_recipient(session)
    day = (datetime.now(UTC).isoweekday() + 1) % 7 + 1
    recipient.allowed_windows = [{"day": day, "from": 0, "to": 1}]
    recipient.timezone = "UTC"
    await session.commit()
    return recipient.id


async def _intake(
    session: AsyncSession, recipient_id: UUID, **extra: Any,
) -> UUID:
    job = await create_notification(
        session, **intake_fields(), type=_TYPE, title="T", body="B",
        target_type=TargetType.USER, target_value=str(recipient_id), **extra,
    )
    await session.commit()
    return job.id


async def _state(nid: UUID) -> tuple[str, list[NotificationDelivery]]:
    async with get_session_factory()() as session:
        job = await session.get(Notification, nid)
        assert job is not None
        deliveries = list((await session.execute(
            select(NotificationDelivery)
            .where(NotificationDelivery.notification_id == nid)
        )).scalars())
        return job.status, deliveries


# -----------------------------------------------------------------------------
# Б1 (1) -- the gate
# -----------------------------------------------------------------------------


class TestTheGate:
    async def test_bypass_goes_at_once_outside_the_schedule(
        self, db_session: AsyncSession,
    ) -> None:
        _declare("bypass")
        nid = await _intake(db_session, await _outside_its_schedule(db_session))
        with _channel():
            await process_pending_notifications()
        status, (delivery,) = await _state(nid)
        assert status == NotificationStatus.SENT
        assert delivery.status == DeliveryStatus.SENT
        assert delivery.wait_reason is None

    async def test_respect_waits_as_today(self, db_session: AsyncSession) -> None:
        """The pair: the same recipient, a type without the field."""
        nid = await _intake(db_session, await _outside_its_schedule(db_session))
        with _channel():
            await process_pending_notifications()
        status, (delivery,) = await _state(nid)
        assert status == NotificationStatus.PROCESSING
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.wait_reason == "recipient_schedule"
        assert delivery.next_retry_at is not None
        assert delivery.next_retry_at > datetime.now(UTC) + timedelta(hours=12)

    async def test_bypass_reaches_every_recipient_outside_their_schedule(
        self, db_session: AsyncSession,
    ) -> None:
        _declare("bypass")
        first = await _outside_its_schedule(db_session)
        second = await _outside_its_schedule(db_session)
        nids = [await _intake(db_session, r) for r in (first, second)]
        with _channel():
            await process_pending_notifications()
        for nid in nids:
            status, _ = await _state(nid)
            assert status == NotificationStatus.SENT


# -----------------------------------------------------------------------------
# Б1 (2) -- the login code at night
# -----------------------------------------------------------------------------


class TestLoginCode:
    @pytest.mark.parametrize("value,outcome", [
        ("bypass", NotificationStatus.SENT),
        ("respect", NotificationStatus.EXPIRED),
    ])
    async def test_a_code_with_fifteen_minutes_of_life(
        self, db_session: AsyncSession, value: str, outcome: str,
    ) -> None:
        """A code that lives 15 minutes, a person whose next allowed
        period is days away. bypass: it goes now, before its expiry.
        respect (today's rule): it is deferred past the expiry, and when
        the expiry has passed the sweep expires it unsent."""
        _declare(value)
        nid = await _intake(
            db_session, await _outside_its_schedule(db_session),
            expiry_at=datetime.now(UTC) + timedelta(minutes=15),
        )
        with _channel():
            await process_pending_notifications()
            # The quarter of an hour passes.
            await db_session.execute(
                update(Notification).where(Notification.id == nid)
                .values(expiry_at=datetime.now(UTC) - timedelta(seconds=1))
            )
            await db_session.commit()
            await process_pending_notifications()
        status, _ = await _state(nid)
        assert status == outcome


# -----------------------------------------------------------------------------
# Б1 (3) -- the mutes are not this gate
# -----------------------------------------------------------------------------


class TestMutes:
    async def test_muted_before_resolve_is_suppressed(
        self, db_session: AsyncSession,
    ) -> None:
        _declare("bypass")
        rid = await _outside_its_schedule(db_session)
        db_session.add(CategoryMute(recipient_id=rid, category=_CATEGORY))
        await db_session.commit()
        nid = await _intake(db_session, rid)
        with _channel():
            await process_pending_notifications()
        status, deliveries = await _state(nid)
        assert status == NotificationStatus.SUPPRESSED
        assert deliveries == []

    async def test_muted_after_resolve_is_suppressed(
        self, db_session: AsyncSession,
    ) -> None:
        """The late mute is checked before the schedule gate, and bypass
        does not pass it."""
        _declare("bypass")
        rid = await _outside_its_schedule(db_session)
        nid = await _intake(db_session, rid)
        async with get_session_factory()() as session:
            job = await session.get(Notification, nid)
            assert job is not None
            await service.resolve_notification(session, job)
            session.add(CategoryMute(recipient_id=rid, category=_CATEGORY))
            await session.flush()
            with _channel():
                await service.deliver_notification(session, job)
            await service.rollup_notification(session, job)
            await session.commit()
        status, (delivery,) = await _state(nid)
        assert delivery.status == DeliveryStatus.SUPPRESSED
        assert status == NotificationStatus.SUPPRESSED

    async def test_unmuted_bypass_is_sent(self, db_session: AsyncSession) -> None:
        """The pair: the same type and category, no mute -- sent."""
        _declare("bypass")
        nid = await _intake(db_session, await _outside_its_schedule(db_session))
        with _channel():
            await process_pending_notifications()
        status, _ = await _state(nid)
        assert status == NotificationStatus.SENT


# -----------------------------------------------------------------------------
# Б1 (4) -- the snapshot
# -----------------------------------------------------------------------------


class TestSnapshot:
    @pytest.mark.parametrize("value", list(RECIPIENT_SCHEDULE))
    async def test_intake_snapshots_the_declared_value(
        self, db_session: AsyncSession, value: str,
    ) -> None:
        _declare(value)
        nid = await _intake(db_session, uuid4())
        job = await db_session.get(Notification, nid)
        assert job is not None and job.recipient_schedule == value

    @pytest.mark.parametrize("at_intake,later,outcome", [
        ("bypass", "respect", NotificationStatus.SENT),
        ("respect", "bypass", NotificationStatus.PROCESSING),
    ])
    async def test_a_profile_changed_after_intake_does_not_change_the_job(
        self, db_session: AsyncSession, at_intake: str, later: str, outcome: str,
    ) -> None:
        _declare(at_intake)
        nid = await _intake(db_session, await _outside_its_schedule(db_session))
        _declare(later)
        with _channel():
            await process_pending_notifications()
        status, _ = await _state(nid)
        assert status == outcome

    async def test_a_type_removed_after_intake_keeps_its_job_s_rule(
        self, db_session: AsyncSession,
    ) -> None:
        _declare("bypass")
        nid = await _intake(db_session, await _outside_its_schedule(db_session))
        registry.reset()
        with _channel():
            await process_pending_notifications()
        status, _ = await _state(nid)
        assert status == NotificationStatus.SENT


# -----------------------------------------------------------------------------
# Б1 (5) explain, (6) the loader
# -----------------------------------------------------------------------------


class TestExplain:
    def test_a_declared_value_names_the_profile(self) -> None:
        """unit_routed declares bypass in the fixture profile."""
        decided = registry.explain("unit_routed", "recipient_schedule")
        assert (decided.value, decided.layer) == ("bypass", Layer.PROFILE)

    def test_an_undeclared_value_names_the_default(self) -> None:
        decided = registry.explain(_TYPE, "recipient_schedule")
        assert (decided.value, decided.layer, decided.source) == (
            "respect", Layer.DEFAULT,
            "comms default: recipient_schedule not declared",
        )


class TestLoader:
    def test_the_values_and_the_default(self) -> None:
        assert RECIPIENT_SCHEDULE == ("respect", "bypass")
        assert "recipient_schedule" in RECORD_FIELDS

    @pytest.mark.parametrize("value", ["respect", "bypass"])
    def test_each_value_is_taken(self, value: str) -> None:
        assert _FIELDS["recipient_schedule"].check(value) is None

    @pytest.mark.parametrize("value", ["Bypass", "", "skip", True, None, 1])
    def test_anything_else_is_refused(self, value: Any) -> None:
        problem = _FIELDS["recipient_schedule"].check(value)
        assert problem is not None
        assert "respect, bypass" in problem

    @pytest.mark.parametrize("written", ["skip", "yes", "on"])
    def test_a_profile_with_another_value_is_a_red_start(
        self, tmp_path: Path, written: str,
    ) -> None:
        """Through the real loader. `yes` and `on` are what YAML reads as
        a boolean -- the reason the field is not one."""
        copy = tmp_path / "profile"
        shutil.copytree(_FIXTURE, copy)
        types = copy / "types.yaml"
        source = types.read_text(encoding="utf-8")
        assert "    recipient_schedule: bypass\n" in source
        types.write_text(
            source.replace(
                "    recipient_schedule: bypass\n",
                f"    recipient_schedule: {written}\n",
            ),
            encoding="utf-8",
        )
        with pytest.raises(ProfileError, match="recipient_schedule"):
            load_profile(FileProfileSource(copy))


# -----------------------------------------------------------------------------
# Б1 (7) -- the column and migration 0022
# -----------------------------------------------------------------------------


class TestTheColumn:
    @pytest.mark.parametrize("value", [None, "skip", ""])
    async def test_a_value_outside_the_two_is_refused(
        self, db_session: AsyncSession, value: str | None,
    ) -> None:
        fields = notification_row_fields()
        fields["recipient_schedule"] = value
        db_session.add(Notification(
            type=_TYPE, title="T", body="B", target_type="all",
            target_value="*", **fields,
        ))
        with pytest.raises(IntegrityError, match="recipient_schedule"):
            await db_session.flush()

    @pytest.mark.parametrize("value", list(RECIPIENT_SCHEDULE))
    async def test_each_of_the_two_is_taken(
        self, db_session: AsyncSession, value: str,
    ) -> None:
        fields = notification_row_fields()
        fields["recipient_schedule"] = value
        db_session.add(Notification(
            type=_TYPE, title="T", body="B", target_type="all",
            target_value="*", **fields,
        ))
        await db_session.flush()


_BEFORE = "0021_job_retry_snapshot"
_SUBJECT = "0022_job_recipient_schedule"


def _alembic(*args: str) -> None:
    done = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=_REPO, capture_output=True, text=True, env=dict(os.environ),
    )
    assert done.returncode == 0, done.stderr


async def _migrate(*args: str) -> None:
    await dispose_engine()
    _alembic(*args)
    await dispose_engine()


async def _sql(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        result = await session.execute(text(statement), params)
        await session.commit()
        return result


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    yield
    await _sql("DELETE FROM notifications")
    await _migrate("upgrade", "head")


async def test_migration_0022(at_head_afterwards: None) -> None:
    """An existing job takes 'respect' -- the rule every job lived by --
    then NOT NULL and the CHECK; down drops the column; up again."""
    await _migrate("downgrade", _BEFORE)
    id_ = uuid4()
    await _sql(
        "INSERT INTO notifications (id, type, title, body, target_type, "
        "target_value, idempotency_key, fingerprint, channels, "
        "expiry_layer, push_on, retry_max_attempts, retry_backoff_seconds, "
        "status) VALUES (:id, 'unit_event', 'T', 'B', 'all', '*', :key, "
        ":fp, '[\"in_app\"]'::jsonb, 'default', 'none', 3, 30, 'processing')",
        id=id_, key=f"m22:{id_}", fp="a" * 64,
    )
    await _migrate("upgrade", _SUBJECT)
    assert (await _sql(
        "SELECT recipient_schedule FROM notifications WHERE id = :id", id=id_,
    )).scalar_one() == "respect"
    assert (await _sql(
        "SELECT is_nullable FROM information_schema.columns WHERE "
        "table_name = 'notifications' AND column_name = 'recipient_schedule'",
    )).scalar_one() == "NO"
    assert (await _sql(
        "SELECT count(*) FROM pg_constraint "
        "WHERE conname = 'ck_notifications_recipient_schedule'",
    )).scalar_one() == 1
    await _migrate("downgrade", _BEFORE)
    assert (await _sql(
        "SELECT count(*) FROM information_schema.columns WHERE "
        "table_name = 'notifications' AND column_name = 'recipient_schedule'",
    )).scalar_one() == 0
    await _migrate("upgrade", _SUBJECT)
    assert (await _sql(
        "SELECT recipient_schedule FROM notifications WHERE id = :id", id=id_,
    )).scalar_one() == "respect"


# -----------------------------------------------------------------------------
# Б3 -- the contract
# -----------------------------------------------------------------------------


class TestTheContract:
    def _section(self) -> str:
        start = _DOC.index("### `recipient_schedule`")
        end = _DOC.index("\n## ", start)
        return _DOC[start:end]

    def _table(self) -> dict[str, str]:
        rows = {}
        for line in self._section().splitlines():
            match = re.match(r"\| `([a-z_]+)` \| (.*) \|$", line)
            if match:
                rows[match.group(1)] = match.group(2)
        assert rows, "the value table is empty"
        return rows

    def test_the_values_are_the_loader_s_both_ways(self) -> None:
        assert set(self._table()) == set(RECIPIENT_SCHEDULE)

    def test_the_default_is_the_loader_s(self) -> None:
        defaults = [v for v, text_ in self._table().items() if "the default" in text_]
        default = _FIELDS["recipient_schedule"].default()[0]
        assert defaults == [default]

    def test_the_declaration_is_written_as_the_loader_reads_it(self) -> None:
        assert "    recipient_schedule: bypass\n" in self._section()
        assert "recipient_schedule" in RECORD_FIELDS

    def test_what_it_does_not_do_is_said(self) -> None:
        section = self._section()
        assert "it does not pass a mute" in section

    def test_the_schedule_section_points_to_it(self) -> None:
        start = _DOC.index("## 6. Preferences")
        assert "`recipient_schedule: bypass` (section 3)" in _DOC[start:]
