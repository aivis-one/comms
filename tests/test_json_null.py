# =============================================================================
# H13 -- an empty JSONB value is SQL NULL, never the JSON value null
# =============================================================================
#
# Every "is it empty?" here is asked of the DATABASE (`col IS NULL`), not
# of the model: the ORM reads JSON `null` and SQL NULL alike, as None,
# and that is exactly how the trap hid. The watchman walks the models'
# metadata, and its pair is that it finds the seven columns -- a walk
# over nothing would be green on any code.
# =============================================================================

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import subprocess
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import fakeredis.aioredis as fakeaioredis
import pytest
from httpx import AsyncClient
from sqlalchemy import JSON, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

import app.audience.models
import app.engine.models
import app.messaging.models  # noqa: F401 -- every model on the metadata
from app.audience.models import Recipient
from app.audience.prefs import set_schedule
from app.audience.sync import is_unique_violation, once_more_on_insert_race
from app.core.config import settings
from app.core.database import Base, dispose_engine, get_session_factory
from app.engine.models import LLMCall, Notification, NotificationDelivery
from app.transport.consumer import StreamConsumer
from tests.helpers import create_recipient, notification_row_fields

_REPO = Path(__file__).resolve().parents[1]
_MIGRATION = _REPO / "migrations" / "versions" / "2026_10_15_0030_json_null_is_null.py"
_BEFORE = "0029_status_index_with_id"
_SUBJECT = "0030_json_null_is_null"


def _migration_module() -> Any:
    spec = importlib.util.spec_from_file_location("m0030", _MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _json_columns() -> dict[tuple[str, str], Any]:
    """Every column of a JSON type on the models' metadata."""
    return {
        (table.name, column.name): column
        for table in Base.metadata.tables.values()
        for column in table.columns
        if isinstance(column.type, JSON)
    }


async def _sql(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        result = await session.execute(text(statement), params)
        await session.commit()
        return result


async def _is_null(table: str, column: str, key: str, value: Any) -> bool:
    found: bool = (await _sql(
        f"SELECT {column} IS NULL FROM {table} WHERE {key} = :v", v=value,
    )).scalar_one()
    return found


# -----------------------------------------------------------------------------
# Item 3 -- the watchman
# -----------------------------------------------------------------------------


class TestTheWatchman:
    def test_every_json_column_writes_none_as_sql_null(self) -> None:
        columns = _json_columns()
        missing = sorted(
            f"{table}.{column}"
            for (table, column), col in columns.items()
            if not col.type.none_as_null
        )
        assert missing == [], (
            f"JSONB without none_as_null (use app/core/database.py "
            f"JSON_VALUE): {missing}"
        )

    def test_it_sees_the_seven(self) -> None:
        """The pair: the walk finds the columns -- the ones the migration
        lists, nullable as it says."""
        columns = _json_columns()
        listed = {
            (table, column): nullable
            for table, column, nullable in _migration_module().JSONB_COLUMNS
        }
        assert len(columns) == 7
        assert {k: c.nullable for k, c in columns.items()} == listed

    async def test_each_has_its_check_in_the_database(self) -> None:
        names = set((await _sql(
            "SELECT conname FROM pg_constraint WHERE conname LIKE "
            "'%\\_not\\_json\\_null'",
        )).scalars())
        assert names == {
            f"ck_{table}_{column}_not_json_null"
            for table, column in _json_columns()
        }


# -----------------------------------------------------------------------------
# Item 1 -- None is SQL NULL, on the database's layer, column by column
# -----------------------------------------------------------------------------


async def _job(session: AsyncSession, **fields: Any) -> Notification:
    base: dict[str, Any] = {
        "type": "unit_event_in_app", "title": "T", "body": "B",
        "target_type": "all", "target_value": "*",
        **notification_row_fields(),
    }
    base.update(fields)
    job = Notification(**base)
    session.add(job)
    await session.flush()
    return job


async def _llm_call(session: AsyncSession, **fields: Any) -> LLMCall:
    job = await _job(
        session, type="ask.model", title=None, body=None, target_type=None,
        target_value=None, **notification_row_fields(channels=["llm"]),
    )
    base: dict[str, Any] = {
        "notification_id": job.id, "sets": ["llm.gw"],
        "letter": {"messages": []}, "letter_hash": "a" * 64,
        "answer_keep_seconds": 60,
    }
    base.update(fields)
    call = LLMCall(**base)
    session.add(call)
    await session.flush()
    return call


async def _delivery(session: AsyncSession, **fields: Any) -> NotificationDelivery:
    job = await _job(session)
    recipient = await create_recipient(session)
    delivery = NotificationDelivery(
        notification_id=job.id, recipient_id=recipient.id, channel="in_app",
        **fields,
    )
    session.add(delivery)
    await session.flush()
    return delivery


class TestNoneIsNull:
    async def test_action_data(self, db_session: AsyncSession) -> None:
        empty = await _job(db_session, action_data=None)
        valued = await _job(db_session, action_data={"action": "x"})
        await db_session.commit()
        assert await _is_null("notifications", "action_data", "id", empty.id)
        assert (await _sql(
            "SELECT action_data FROM notifications WHERE id = :v", v=valued.id,
        )).scalar_one() == {"action": "x"}
        valued.action_data = None
        await db_session.commit()
        assert await _is_null("notifications", "action_data", "id", valued.id)

    async def test_channel_options(self, db_session: AsyncSession) -> None:
        empty = await _delivery(db_session, channel_options=None)
        valued = await _delivery(db_session, channel_options={"silent": True})
        await db_session.commit()
        assert await _is_null(
            "notification_deliveries", "channel_options", "id", empty.id,
        )
        assert not await _is_null(
            "notification_deliveries", "channel_options", "id", valued.id,
        )
        valued.channel_options = None
        await db_session.commit()
        assert await _is_null(
            "notification_deliveries", "channel_options", "id", valued.id,
        )

    async def test_allowed_windows(self, db_session: AsyncSession) -> None:
        recipient = await create_recipient(db_session)
        await db_session.commit()
        await set_schedule(
            db_session, recipient.id,
            windows=[{"day": 1, "from": 0, "to": 60}],
        )
        await db_session.commit()
        assert (await _sql(
            "SELECT allowed_windows FROM recipients WHERE id = :v",
            v=recipient.id,
        )).scalar_one() == [{"day": 1, "from": 0, "to": 60}]
        await set_schedule(db_session, recipient.id, windows=None)
        await db_session.commit()
        assert await _is_null("recipients", "allowed_windows", "id", recipient.id)

    @pytest.mark.parametrize("column", ["letter", "audio"])
    async def test_the_llm_call(self, db_session: AsyncSession, column: str) -> None:
        call = await _llm_call(
            db_session, audio={"url": "u", "format": "wav", "max_bytes": 1},
        )
        await db_session.commit()
        assert not await _is_null(
            "llm_calls", column, "notification_id", call.notification_id,
        )
        call.audio = None
        if column == "letter":
            call.letter = None
        await db_session.commit()
        assert await _is_null(
            "llm_calls", column, "notification_id", call.notification_id,
        )

    async def test_none_in_a_not_null_column_is_refused_not_hidden(
        self, db_session: AsyncSession,
    ) -> None:
        """notifications.channels and llm_calls.sets: a None was a silent
        JSON `null` that passed NOT NULL; now it is the refusal."""
        with pytest.raises(IntegrityError) as caught:
            await _job(db_session, channels=None)
        assert getattr(caught.value.orig, "pgcode", None) == "23502"
        await db_session.rollback()
        with pytest.raises(IntegrityError) as caught:
            await _llm_call(db_session, sets=None)
        assert getattr(caught.value.orig, "pgcode", None) == "23502"
        await db_session.rollback()


class TestTheDatabaseRefusesJsonNull:
    """CHECK ck_<table>_<column>_not_json_null, each with its NULL twin:
    SQL NULL passes (the empty value), JSON null is refused."""

    async def _rows(self) -> dict[tuple[str, str], tuple[str, Any]]:
        async with get_session_factory()() as session:
            job = await _job(session)
            delivery = await _delivery(session)
            recipient = await create_recipient(session)
            call = await _llm_call(session)
            await session.commit()
        return {
            ("notifications", "channels"): ("id", job.id),
            ("notifications", "action_data"): ("id", job.id),
            ("notification_deliveries", "channel_options"): ("id", delivery.id),
            ("recipients", "allowed_windows"): ("id", recipient.id),
            ("llm_calls", "sets"): ("notification_id", call.notification_id),
            ("llm_calls", "letter"): ("notification_id", call.notification_id),
            ("llm_calls", "audio"): ("notification_id", call.notification_id),
        }

    async def test_every_column(self) -> None:
        rows = await self._rows()
        assert set(rows) == set(_json_columns())
        for (table, column), (key, value) in rows.items():
            async with get_session_factory()() as session:
                with pytest.raises(
                    IntegrityError,
                    match=f"ck_{table}_{column}_not_json_null",
                ):
                    await session.execute(text(
                        f"UPDATE {table} SET {column} = 'null'::jsonb "
                        f"WHERE {key} = :v"
                    ), {"v": value})
                await session.rollback()
            if _json_columns()[(table, column)].nullable and column != "letter":
                # The twin: SQL NULL is the empty value, and passes.
                await _sql(
                    f"UPDATE {table} SET {column} = NULL WHERE {key} = :v",
                    v=value,
                )
                assert await _is_null(table, column, key, value)


# -----------------------------------------------------------------------------
# Item 1 (3) / item 2 -- the migration, and the person forgotten after it
# -----------------------------------------------------------------------------


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


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    yield
    await _sql("DELETE FROM notifications")
    await _sql("DELETE FROM recipients")
    await _migrate("upgrade", "head")


async def _seed_before(session: AsyncSession) -> dict[str, Any]:
    """Rows as the code before 0030 left them: a JSON `null` beside a
    nested null and an ordinary value, in every nullable column."""
    cleared = await create_recipient(session)
    windows = await create_recipient(session)
    job_null = await _job(session)
    job_nested = await _job(session, action_data={"a": None})
    delivery = await _delivery(session)
    call = await _llm_call(session)
    await session.commit()
    for statement, params in (
        ("UPDATE recipients SET allowed_windows = 'null'::jsonb WHERE id = :v",
         {"v": cleared.id}),
        ("UPDATE recipients SET allowed_windows = "
         "'[{\"day\": 1, \"from\": 0, \"to\": 60}]'::jsonb WHERE id = :v",
         {"v": windows.id}),
        ("UPDATE notifications SET action_data = 'null'::jsonb WHERE id = :v",
         {"v": job_null.id}),
        ("UPDATE notification_deliveries SET channel_options = 'null'::jsonb "
         "WHERE id = :v", {"v": delivery.id}),
        # P5-1's cleared letter; its audio was SQL NULL from 0028 on.
        ("UPDATE llm_calls SET letter = 'null'::jsonb "
         "WHERE notification_id = :v", {"v": call.notification_id}),
    ):
        await session.execute(text(statement), params)
    await session.commit()
    return {
        "cleared": cleared.id, "windows": windows.id, "job_null": job_null.id,
        "job_nested": job_nested.id, "delivery": delivery.id,
        "call": call.notification_id,
    }


async def test_migration_0030(
    at_head_afterwards: None, bare_client: AsyncClient,
) -> None:
    await _migrate("downgrade", _BEFORE)
    async with get_session_factory()() as session:
        ids = await _seed_before(session)
    assert not await _is_null("recipients", "allowed_windows", "id", ids["cleared"])

    await _migrate("upgrade", _SUBJECT)
    assert await _is_null("recipients", "allowed_windows", "id", ids["cleared"])
    assert await _is_null("notifications", "action_data", "id", ids["job_null"])
    assert await _is_null(
        "notification_deliveries", "channel_options", "id", ids["delivery"],
    )
    assert await _is_null("llm_calls", "letter", "notification_id", ids["call"])
    # The pairs: a value and a NESTED null are not "empty", and stay.
    assert (await _sql(
        "SELECT action_data FROM notifications WHERE id = :v",
        v=ids["job_nested"],
    )).scalar_one() == {"a": None}
    assert (await _sql(
        "SELECT allowed_windows FROM recipients WHERE id = :v", v=ids["windows"],
    )).scalar_one() == [{"day": 1, "from": 0, "to": 60}]

    # Item 2: the person whose schedule was cleared BEFORE the migration
    # -- the row that held a JSON null -- is forgotten now (by the API).
    answer = await bare_client.request(
        "DELETE", f"/api/v1/recipients/{ids['cleared']}", json={"version": 2},
    )
    assert answer.status_code == 200, answer.text
    assert (await _sql(
        "SELECT deleted_at IS NOT NULL AND allowed_windows IS NULL "
        "FROM recipients WHERE id = :v", v=ids["cleared"],
    )).scalar_one()

    # Down: the CHECKs go, the data stays as SQL NULL (valid before too).
    await _migrate("downgrade", _BEFORE)
    assert (await _sql(
        "SELECT count(*) FROM pg_constraint WHERE conname LIKE "
        "'%\\_not\\_json\\_null'",
    )).scalar_one() == 0
    assert await _is_null("notifications", "action_data", "id", ids["job_null"])
    await _migrate("upgrade", _SUBJECT)
    assert (await _sql(
        "SELECT count(*) FROM pg_constraint WHERE conname LIKE "
        "'%\\_not\\_json\\_null'",
    )).scalar_one() == 7


async def test_the_migration_seeds_the_stream_case(
    at_head_afterwards: None, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Item 2 by the STREAM: a cleared schedule seeded as JSON null
    before 0030; after it, user_deleted forgets the person and the
    dead-letter stream stays empty."""
    await _migrate("downgrade", _BEFORE)
    async with get_session_factory()() as session:
        ids = await _seed_before(session)
    await _migrate("upgrade", _SUBJECT)
    dlq = await _consume_user_deleted(monkeypatch, ids["cleared"])
    assert dlq == 0
    assert (await _sql(
        "SELECT deleted_at IS NOT NULL FROM recipients WHERE id = :v",
        v=ids["cleared"],
    )).scalar_one()


async def _consume_user_deleted(
    monkeypatch: pytest.MonkeyPatch, recipient_id: UUID,
) -> int:
    """One user_deleted through the real consumer on a fake redis, until
    the entry is settled -- the person tombstoned, or the entry dead-
    lettered; the length of the dead-letter stream after it."""
    redis = fakeaioredis.FakeRedis()
    stream = f"comms:h13:{uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "comms_events_stream", stream)
    monkeypatch.setattr(settings, "consumer_block_ms", 20)
    await redis.xadd(stream, {
        "event": "user_deleted",
        "data": json.dumps({"v": 1, "recipient_id": str(recipient_id),
                            "version": 2}),
    })
    task = asyncio.ensure_future(StreamConsumer(redis).run())
    try:
        deadline = asyncio.get_event_loop().time() + 10
        while not (
            await redis.xlen(settings.dlq_stream)
            or (await _sql(
                "SELECT deleted_at IS NOT NULL FROM recipients WHERE id = :v",
                v=recipient_id,
            )).scalar_one()
        ):
            assert asyncio.get_event_loop().time() < deadline
            await asyncio.sleep(0.02)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    length: int = await redis.xlen(settings.dlq_stream)
    return length


class TestForgettingWithASchedule:
    """Item 2: before H13 the tombstone of a recipient who ever had a
    schedule failed ck_recipients_tombstone (JSON null IS NOT NULL) --
    by the stream into the dead letters, by the API a 500."""

    async def _with_schedule(self, db_session: AsyncSession) -> UUID:
        recipient = await create_recipient(db_session)
        await db_session.commit()
        await set_schedule(
            db_session, recipient.id, windows=[{"day": 2, "from": 60, "to": 120}],
        )
        await db_session.commit()
        return recipient.id

    async def test_by_the_api(
        self, db_session: AsyncSession, bare_client: AsyncClient,
    ) -> None:
        recipient_id = await self._with_schedule(db_session)
        answer = await bare_client.request(
            "DELETE", f"/api/v1/recipients/{recipient_id}", json={"version": 2},
        )
        assert answer.status_code == 200, answer.text
        assert (await _sql(
            "SELECT deleted_at IS NOT NULL AND allowed_windows IS NULL "
            "FROM recipients WHERE id = :v", v=recipient_id,
        )).scalar_one()

    async def test_by_the_stream(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recipient_id = await self._with_schedule(db_session)
        assert await _consume_user_deleted(monkeypatch, recipient_id) == 0
        assert (await _sql(
            "SELECT deleted_at IS NOT NULL FROM recipients WHERE id = :v",
            v=recipient_id,
        )).scalar_one()

    async def test_set_then_cleared(
        self, db_session: AsyncSession, bare_client: AsyncClient,
    ) -> None:
        recipient_id = await self._with_schedule(db_session)
        await set_schedule(db_session, recipient_id, windows=None)
        await db_session.commit()
        answer = await bare_client.request(
            "DELETE", f"/api/v1/recipients/{recipient_id}", json={"version": 2},
        )
        assert answer.status_code == 200, answer.text


# -----------------------------------------------------------------------------
# Gate item 1 -- the race is only the race
# -----------------------------------------------------------------------------


class TestOnlyTheRaceIsRetried:
    async def test_a_check_violation_is_raised_at_once(
        self, db_session: AsyncSession,
    ) -> None:
        calls: list[int] = []

        async def apply() -> None:
            calls.append(1)
            db_session.add(Recipient(
                id=uuid4(), version=-1, snapshot_fingerprint="f" * 64,
                active=True,
            ))
            await db_session.flush()

        with capture_logs() as logs, pytest.raises(IntegrityError) as caught:
            await once_more_on_insert_race(
                db_session, apply, event="unit_raced", recipient_id="x",
            )
        assert not is_unique_violation(caught.value)
        assert calls == [1]
        assert [e for e in logs if e["event"] == "unit_raced"] == []
        # The caller's transaction is still usable: the savepoint went.
        assert (await db_session.execute(select(1))).scalar_one() == 1

    async def test_a_unique_violation_is_the_race_and_runs_again(
        self, db_session: AsyncSession,
    ) -> None:
        """The pair: the race is still retried, and logged as one."""
        existing = await create_recipient(db_session)
        await db_session.commit()
        calls: list[int] = []

        async def apply() -> str:
            calls.append(1)
            if len(calls) == 1:
                await db_session.execute(text(
                    "INSERT INTO recipients (id, version, snapshot_fingerprint, "
                    "active) VALUES (:id, 1, :fp, true)"
                ), {"id": existing.id, "fp": "f" * 64})
            return "second run"

        with capture_logs() as logs:
            result = await once_more_on_insert_race(
                db_session, apply, event="unit_raced", recipient_id="x",
            )
        assert result == "second run" and calls == [1, 1]
        assert [e["event"] for e in logs if e["event"] == "unit_raced"] == [
            "unit_raced",
        ]


# -----------------------------------------------------------------------------
# The class of influence -- the API's answers are the same
# -----------------------------------------------------------------------------


class TestTheAnswersAreTheSame:
    async def test_a_cleared_schedule_reads_null(
        self, db_session: AsyncSession, client: AsyncClient,
    ) -> None:
        recipient = await create_recipient(db_session)
        await db_session.commit()
        await set_schedule(db_session, recipient.id, windows=None)
        await db_session.commit()
        answer = await client.get(f"/api/v1/recipients/{recipient.id}/preferences")
        assert answer.status_code == 200, answer.text
        assert answer.json()["schedule"] is None

    async def test_a_job_without_action_data_reads_null_in_the_inbox(
        self, db_session: AsyncSession, client: AsyncClient,
    ) -> None:
        """action_data absent at intake: SQL NULL in the base, `null` in
        the inbox item -- the answer it always was."""
        from datetime import UTC, datetime

        from app.engine.constants import DeliveryStatus, TargetType
        from app.engine.service import create_notification, resolve_notification
        from tests.helpers import intake_fields

        recipient = await create_recipient(db_session)
        job = await create_notification(
            db_session, **intake_fields(), type="unit_event_in_app",
            title="T", body="B", target_type=TargetType.USER,
            target_value=str(recipient.id), action_data=None,
        )
        for delivery in await resolve_notification(db_session, job):
            delivery.status = DeliveryStatus.SENT
            delivery.sent_at = datetime.now(UTC)
        await db_session.commit()
        assert await _is_null("notifications", "action_data", "id", job.id)
        answer = await client.get(f"/api/v1/recipients/{recipient.id}/inbox")
        assert answer.status_code == 200, answer.text
        (item,) = answer.json()["items"]
        assert item["action_data"] is None
        assert item["title"] == "T"  # the pair
