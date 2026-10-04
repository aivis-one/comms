# =============================================================================
# H1 Б3 -- the profile's retry_max_attempts, retry_backoff_seconds and
# max_body_chars do what they declare
# =============================================================================
#
# The ceiling and the backoff base are the job's, snapshotted at intake
# (notifications.retry_max_attempts / retry_backoff_seconds, migration
# 0021); the backoff cap stays the deploy's. max_body_chars refuses a
# longer body at intake, under the key.
#
# MUTATIONS these tests were written against (each turns one red):
#   M7  the ceiling read from the setting, not the job  -> TestCeiling
#   M8  the backoff base read from the setting          -> TestBackoff
#   M9  the body check removed                          -> TestBody
#   M10 the body check off by one (>=)                  -> TestBody pair
#   M11 the fields read live from the registry          -> TestSnapshot
#   M12 migration 0021 filling literals                 -> test_migration_0021
#   M13 the loader's upper bound removed                -> TestLoader
# =============================================================================

import json
import os
import shutil
import subprocess
import sys
from collections.abc import AsyncGenerator, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import NUMERIC_BOUNDS, settings
from app.core.constants import MAX_BODY_LEN
from app.core.database import dispose_engine, get_session_factory
from app.core.exceptions import ProfileError
from app.engine.constants import (
    DeliveryStatus,
    FailureClass,
    IntakeOutcomeClass,
    TargetType,
)
from app.engine.formatters import EmailTransientError
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import process_pending_notifications
from app.engine.service import create_notification, read_job_by_key
from app.profile.loader import (
    _FIELDS,
    FileProfileSource,
    install_profile,
    load_profile,
)
from app.profile.registry import Decided, Layer, TypeRecord, registry
from app.transport.events import parse_event
from app.transport.handlers import HandleResult, handle_event
from tests.helpers import create_recipient, intake_fields, notification_row_fields

_REPO = Path(__file__).resolve().parents[1]
_FIXTURE = _REPO / "tests" / "fixtures" / "profile"
_PLAIN = "unit_event_in_app"   # declares none of the three
_ROUTED = "unit_routed"        # declares 2 / 0 / 1000 (fixture profile)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _reinstall() -> None:
    """Load and install the fixture profile again -- so the default
    layer reads the settings as they are now, as a startup would."""
    registry.reset()
    install_profile(load_profile(FileProfileSource(_FIXTURE)))


def _declare(type_key: str, **declared: int) -> None:
    record = registry.record_of(type_key)
    assert record is not None
    fields = dict(record.fields)
    for name, value in declared.items():
        fields[name] = Decided(value, Layer.PROFILE, "test")
    registry.register_type(
        type_key, category=registry.category_of(type_key),
        record=TypeRecord(fields=fields),
    )


class _Transient:
    async def deliver(self, *args: Any) -> bool:
        raise EmailTransientError("provider error (503)")


@contextmanager
def _always_transient() -> Iterator[None]:
    with patch("app.engine.service.get_formatter", return_value=_Transient()):
        yield


async def _intake(session: AsyncSession, type_key: str, **extra: Any) -> UUID:
    recipient = await create_recipient(session)
    job = await create_notification(
        session, **intake_fields(), type=type_key, title="T", body="B",
        target_type=TargetType.USER, target_value=str(recipient.id), **extra,
    )
    await session.commit()
    return job.id


async def _delivery(nid: UUID) -> NotificationDelivery:
    async with get_session_factory()() as session:
        (delivery,) = (await session.execute(
            select(NotificationDelivery).where(
                NotificationDelivery.notification_id == nid,
            )
        )).scalars().all()
        return delivery


async def _burn(nid: UUID) -> NotificationDelivery:
    """Pass after pass until the delivery fails (backoff 0: each pass
    may retry); bounded well above any ceiling used here."""
    with _always_transient():
        for _ in range(12):
            await process_pending_notifications()
            delivery = await _delivery(nid)
            if delivery.status != DeliveryStatus.PENDING:
                return delivery
    raise AssertionError("the delivery never failed")


@pytest.fixture
def five_attempts_no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """The deploy's settings: 5 attempts, no idle wait -- and a profile
    installed under them, so the default layer is 5 / 0."""
    monkeypatch.setattr(settings, "notification_max_delivery_attempts", 5)
    monkeypatch.setattr(settings, "notification_retry_backoff_base_seconds", 0)
    _reinstall()


# -----------------------------------------------------------------------------
# Б3 (1) -- the ceiling
# -----------------------------------------------------------------------------


class TestCeiling:
    async def test_a_type_declaring_2_burns_in_2_under_a_setting_of_5(
        self, db_session: AsyncSession, five_attempts_no_wait: None,
    ) -> None:
        """Through the real loader (the fixture's unit_routed declares
        retry_max_attempts: 2) and the real pipeline."""
        nid = await _intake(db_session, _ROUTED)
        delivery = await _burn(nid)
        assert (delivery.status, delivery.failure_class, delivery.attempts) == (
            DeliveryStatus.FAILED, FailureClass.TRANSIENT_EXHAUSTED, 2,
        )

    async def test_a_type_without_it_burns_in_the_setting_s_5(
        self, db_session: AsyncSession, five_attempts_no_wait: None,
    ) -> None:
        nid = await _intake(db_session, _PLAIN)
        delivery = await _burn(nid)
        assert (delivery.status, delivery.attempts) == (DeliveryStatus.FAILED, 5)

    async def test_a_ceiling_of_1_fails_at_the_first_transient(
        self, db_session: AsyncSession, five_attempts_no_wait: None,
    ) -> None:
        _declare(_PLAIN, retry_max_attempts=1)
        nid = await _intake(db_session, _PLAIN)
        delivery = await _burn(nid)
        assert delivery.attempts == 1


# -----------------------------------------------------------------------------
# Б3 (2) -- the backoff
# -----------------------------------------------------------------------------


class TestBackoff:
    async def _gap(self, nid: UUID) -> float:
        before = datetime.now(UTC)
        with _always_transient():
            await process_pending_notifications()
        delivery = await _delivery(nid)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.next_retry_at is not None
        return (delivery.next_retry_at - before).total_seconds()

    async def test_the_type_s_base_is_the_first_gap(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "notification_retry_backoff_base_seconds", 30)
        _reinstall()
        _declare(_PLAIN, retry_backoff_seconds=40)
        nid = await _intake(db_session, _PLAIN)
        assert 39 <= await self._gap(nid) <= 42

    async def test_without_it_the_setting_s_base(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "notification_retry_backoff_base_seconds", 30)
        _reinstall()
        nid = await _intake(db_session, _PLAIN)
        assert 29 <= await self._gap(nid) <= 32

    async def test_the_cap_stays_the_deploy_s(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Declared base = the cap: the second gap would be 2x, the cap
        clips it -- the cap is not the type's."""
        monkeypatch.setattr(settings, "notification_retry_backoff_max_seconds", 100)
        _declare(_PLAIN, retry_backoff_seconds=100)
        nid = await _intake(db_session, _PLAIN)
        assert 99 <= await self._gap(nid) <= 102
        async with get_session_factory()() as session:
            await session.execute(text(
                "UPDATE notification_deliveries SET next_retry_at = now()"
            ))
            await session.commit()
        assert 99 <= await self._gap(nid) <= 102


# -----------------------------------------------------------------------------
# Б3 (3) -- the body
# -----------------------------------------------------------------------------


def _event(body: str, key: str) -> Any:
    return parse_event({
        "event": "notification_request",
        "data": json.dumps({
            "v": 1, "idempotency_key": key, "type": _ROUTED,
            "target_type": "user", "target_value": str(uuid4()),
            "title": "T", "body": body,
        }),
    })


class TestBody:
    async def test_a_body_past_the_type_s_limit_is_refused_under_its_key(
        self, db_session: AsyncSession,
    ) -> None:
        """unit_routed declares max_body_chars: 1000. 1001 characters:
        refused at intake; the product reads the refusal and its reason
        by the key; there is no job."""
        key = f"h1-body-{uuid4()}"
        assert await handle_event(db_session, _event("x" * 1001, key)) is (
            HandleResult.REJECTED
        )
        await db_session.commit()
        read = await read_job_by_key(db_session, key)
        assert read.job is None
        (row,) = read.intake
        assert row.outcome == IntakeOutcomeClass.REJECTED_AT_INTAKE
        assert "1001" in row.reason
        assert "1000" in row.reason
        assert "max_body_chars" in row.reason

    async def test_a_body_exactly_at_the_limit_is_accepted(
        self, db_session: AsyncSession,
    ) -> None:
        key = f"h1-body-{uuid4()}"
        assert await handle_event(db_session, _event("x" * 1000, key)) is (
            HandleResult.PROCESSED
        )
        await db_session.commit()
        read = await read_job_by_key(db_session, key)
        assert read.job is not None and read.intake == []

    async def test_an_undeclared_limit_is_the_column_s(
        self, db_session: AsyncSession,
    ) -> None:
        record = registry.record_of(_PLAIN)
        assert record is not None
        assert record.value("max_body_chars") == MAX_BODY_LEN
        job = await create_notification(
            db_session, **intake_fields(), type=_PLAIN, title="T",
            body="x" * MAX_BODY_LEN, target_type=TargetType.ALL,
            target_value="*",
        )
        assert len(job.body) == MAX_BODY_LEN

    async def test_an_empty_body_is_accepted(
        self, db_session: AsyncSession,
    ) -> None:
        job = await create_notification(
            db_session, **intake_fields(), type=_ROUTED, title="T", body="",
            target_type=TargetType.ALL, target_value="*",
        )
        assert job.body == ""


# -----------------------------------------------------------------------------
# Б3 (4) -- the snapshot
# -----------------------------------------------------------------------------


class TestSnapshot:
    async def test_intake_snapshots_both(
        self, db_session: AsyncSession, five_attempts_no_wait: None,
    ) -> None:
        routed = await _intake(db_session, _ROUTED)
        plain = await _intake(db_session, _PLAIN)
        for nid, expected in ((routed, (2, 0)), (plain, (5, 0))):
            job = await db_session.get(Notification, nid)
            assert job is not None
            assert (job.retry_max_attempts, job.retry_backoff_seconds) == expected

    async def test_a_profile_changed_after_intake_does_not_change_the_job(
        self, db_session: AsyncSession, five_attempts_no_wait: None,
    ) -> None:
        _declare(_PLAIN, retry_max_attempts=2)
        nid = await _intake(db_session, _PLAIN)
        _declare(_PLAIN, retry_max_attempts=7)
        delivery = await _burn(nid)
        assert delivery.attempts == 2

    async def test_a_type_removed_after_intake_keeps_its_job_s_rule(
        self, db_session: AsyncSession, five_attempts_no_wait: None,
    ) -> None:
        _declare(_PLAIN, retry_max_attempts=2)
        nid = await _intake(db_session, _PLAIN)
        registry.reset()  # no type at all now
        delivery = await _burn(nid)
        assert delivery.attempts == 2


# -----------------------------------------------------------------------------
# Б3 (5) -- explain
# -----------------------------------------------------------------------------


class TestExplain:
    @pytest.mark.parametrize("field", [
        "retry_max_attempts", "retry_backoff_seconds", "max_body_chars",
    ])
    def test_a_declared_field_names_the_profile(self, field: str) -> None:
        assert registry.explain(_ROUTED, field).layer is Layer.PROFILE

    @pytest.mark.parametrize("field,source", [
        ("retry_max_attempts", "settings: NOTIFICATION_MAX_DELIVERY_ATTEMPTS"),
        ("retry_backoff_seconds",
         "settings: NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS"),
        ("max_body_chars", "comms default: MAX_BODY_LEN"),
    ])
    def test_an_undeclared_field_names_its_default(
        self, field: str, source: str,
    ) -> None:
        decided = registry.explain(_PLAIN, field)
        assert (decided.layer, decided.source) == (Layer.DEFAULT, source)


# -----------------------------------------------------------------------------
# The loader's bounds
# -----------------------------------------------------------------------------


class TestLoader:
    def test_retry_max_attempts_has_the_setting_s_top(self) -> None:
        check = _FIELDS["retry_max_attempts"].check
        top = NUMERIC_BOUNDS["notification_max_delivery_attempts"].hi
        assert check(top) is None
        assert check(top + 1) is not None

    def test_the_backoff_base_is_not_above_the_deploy_s_cap(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "notification_retry_backoff_max_seconds", 600)
        check = _FIELDS["retry_backoff_seconds"].check
        assert check(600) is None
        problem = check(601)
        assert problem is not None
        assert "NOTIFICATION_RETRY_BACKOFF_MAX_SECONDS=600" in problem

    def test_the_backoff_base_has_the_setting_s_top(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        top = NUMERIC_BOUNDS["notification_retry_backoff_base_seconds"].hi
        monkeypatch.setattr(settings, "notification_retry_backoff_max_seconds", top)
        check = _FIELDS["retry_backoff_seconds"].check
        assert check(top) is None
        assert check(top + 1) is not None

    def test_a_profile_past_the_cap_is_a_red_start(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "notification_retry_backoff_max_seconds", 600)
        copy = tmp_path / "profile"
        shutil.copytree(_FIXTURE, copy)
        types = copy / "types.yaml"
        text_ = types.read_text(encoding="utf-8")
        assert "    retry_backoff_seconds: 0\n" in text_
        types.write_text(
            text_.replace(
                "    retry_backoff_seconds: 0\n", "    retry_backoff_seconds: 601\n",
            ),
            encoding="utf-8",
        )
        with pytest.raises(ProfileError, match="retry_backoff_seconds"):
            load_profile(FileProfileSource(copy))


# -----------------------------------------------------------------------------
# The column: CHECKs and migration 0021
# -----------------------------------------------------------------------------


class TestTheColumns:
    @pytest.mark.parametrize("field,value", [
        ("retry_max_attempts", None), ("retry_max_attempts", 0),
        ("retry_max_attempts", 101),
        ("retry_backoff_seconds", None), ("retry_backoff_seconds", -1),
        ("retry_backoff_seconds", 86_401),
    ])
    async def test_a_value_outside_the_range_is_refused(
        self, db_session: AsyncSession, field: str, value: int | None,
    ) -> None:
        fields = notification_row_fields()
        fields[field] = value
        db_session.add(Notification(
            type=_PLAIN, title="T", body="B", target_type="all",
            target_value="*", **fields,
        ))
        with pytest.raises(IntegrityError, match=field):
            await db_session.flush()

    @pytest.mark.parametrize("field,value", [
        ("retry_max_attempts", 1), ("retry_max_attempts", 100),
        ("retry_backoff_seconds", 0), ("retry_backoff_seconds", 86_400),
    ])
    async def test_the_range_s_ends_are_taken(
        self, db_session: AsyncSession, field: str, value: int,
    ) -> None:
        fields = notification_row_fields()
        fields[field] = value
        db_session.add(Notification(
            type=_PLAIN, title="T", body="B", target_type="all",
            target_value="*", **fields,
        ))
        await db_session.flush()

    @pytest.mark.parametrize("constraint,setting", [
        ("ck_notifications_retry_max_attempts",
         "notification_max_delivery_attempts"),
        ("ck_notifications_retry_backoff_seconds",
         "notification_retry_backoff_base_seconds"),
    ])
    async def test_the_check_is_the_setting_s_range(
        self, db_session: AsyncSession, constraint: str, setting: str,
    ) -> None:
        definition = (await db_session.execute(
            text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname = :name"
            ),
            {"name": constraint},
        )).scalar_one()
        bound = NUMERIC_BOUNDS[setting]
        assert f">= {bound.lo}" in definition
        assert f"<= {bound.hi}" in definition


_BEFORE = "0020_push_outbox"
_SUBJECT = "0021_job_retry_snapshot"


def _alembic(*args: str, env: dict[str, str] | None = None) -> None:
    done = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=_REPO, capture_output=True, text=True,
        env={**os.environ, **(env or {})},
    )
    assert done.returncode == 0, done.stderr


async def _migrate(*args: str, env: dict[str, str] | None = None) -> None:
    await dispose_engine()
    _alembic(*args, env=env)
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


async def test_migration_0021(at_head_afterwards: None) -> None:
    """Existing jobs take the values of the settings the MIGRATING
    process sees -- here deliberately not the code's defaults (7 / 45,
    not 3 / 30): a literal would be caught. Then NOT NULL and both
    CHECKs; down drops the columns; up again on the same row."""
    await _migrate("downgrade", _BEFORE)
    id_ = uuid4()
    await _sql(
        "INSERT INTO notifications (id, type, title, body, target_type, "
        "target_value, idempotency_key, fingerprint, channels, "
        "expiry_layer, push_on, status) VALUES (:id, 'unit_event', 'T', "
        "'B', 'all', '*', :key, :fp, '[\"in_app\"]'::jsonb, 'default', "
        "'none', 'processing')",
        id=id_, key=f"m21:{id_}", fp="a" * 64,
    )
    overridden = {
        "NOTIFICATION_MAX_DELIVERY_ATTEMPTS": "7",
        "NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS": "45",
    }
    await _migrate("upgrade", _SUBJECT, env=overridden)
    row = (await _sql(
        "SELECT retry_max_attempts, retry_backoff_seconds FROM notifications "
        "WHERE id = :id", id=id_,
    )).one()
    assert tuple(row) == (7, 45)
    assert (await _sql(
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_name = 'notifications' AND is_nullable = 'NO' "
        "AND column_name IN ('retry_max_attempts', 'retry_backoff_seconds')",
    )).scalar_one() == 2
    assert (await _sql(
        "SELECT count(*) FROM pg_constraint WHERE conname IN "
        "('ck_notifications_retry_max_attempts', "
        "'ck_notifications_retry_backoff_seconds')",
    )).scalar_one() == 2
    await _migrate("downgrade", _BEFORE)
    assert (await _sql(
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_name = 'notifications' AND column_name IN "
        "('retry_max_attempts', 'retry_backoff_seconds')",
    )).scalar_one() == 0
    await _migrate("upgrade", _SUBJECT)
    row = (await _sql(
        "SELECT retry_max_attempts, retry_backoff_seconds FROM notifications "
        "WHERE id = :id", id=id_,
    )).one()
    assert tuple(row) == (
        settings.notification_max_delivery_attempts,
        settings.notification_retry_backoff_base_seconds,
    )

