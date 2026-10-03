# =============================================================================
# P2-2 -- reading a job by its key (spec §6.2 / §6.3 / §6.6 / §7.1)
# =============================================================================
#
# MUTATIONS these tests were written against (each turns one red):
#   R1 the job's title in the wire form   -> TestNoLetter, TestContract
#   R2 a delivery's error_message on the wire -> TestNoLetter
#   R3 a write inside a read              -> TestReadOnly
#   R4 the router without the service token -> TestAuth
#   R5 one query per delivery             -> TestQueryCount
#   R6 a field in a form, not in the document (and back) -> TestContract
#   R7 the summary without intake outcomes -> TestGrid (rejected by key)
#   R8 the key as a path segment          -> TestKey
# =============================================================================

import itertools
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import delete, event, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api import jobs
from app.audience.models import CategoryMute
from app.core.config import settings
from app.core.database import get_engine, get_session_factory
from app.engine import processor
from app.engine.constants import (
    ChannelAnswer,
    DeliveryStatus,
    FailureClass,
    IntakeOutcomeClass,
    JournalStep,
    JournalSubject,
    NotificationStatus,
    PipelineStep,
    TargetType,
    WaitReason,
)
from app.engine.formatters import ConfigurationError, RateLimitedError
from app.engine.models import Notification, NotificationDelivery, NotificationTransition
from app.engine.processor import process_pending_notifications
from app.engine.service import close_notifications, withdraw_recipient
from app.transport.events import parse_event
from app.transport.handlers import handle_event
from tests.helpers import add_to_group, create_recipient
from tests.test_transition_journal import _channel, _poisoned_rollup, _Spy

_DOC = Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md"
_BASE = "/api/v1/notifications/by-key"

# Values no field, status or routine text ever contains by itself.
_TITLE_SENTINEL = "zq9" + "TITLE" + "wk2"
_BODY_SENTINEL = "zq9" + "BODY" + "wk2"
_PARAM_SENTINEL = "zq9" + "PARAM" + "wk2"
_ERROR_SENTINEL = "zq9" + "ERRTEXT" + "wk2"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _request(key: str, target: UUID | str, **extra: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "v": 1,
        "idempotency_key": key,
        "type": "unit_event_in_app",
        "target_type": TargetType.USER,
        "target_value": str(target),
        "title": "T",
        "body": "B",
    }
    data.update(extra)
    return data


async def _send(data: dict[str, Any]) -> str:
    """One notification_request through the stream path: parse, handle,
    commit -- the way the consumer records acceptance and refusal."""
    parsed = parse_event({"event": "notification_request", "data": json.dumps(data)})
    async with get_session_factory()() as session:
        result = await handle_event(session, parsed)
        await session.commit()
    return str(result)


async def _read(client: AsyncClient, key: str, tail: str = "", **params: Any) -> Any:
    return await client.get(f"{_BASE}{tail}", params={"key": key, **params})


async def _job_id(key: str) -> UUID:
    async with get_session_factory()() as session:
        return (
            await session.execute(
                select(Notification.id).where(Notification.idempotency_key == key)
            )
        ).scalar_one()


# This file's own telegram_id band: the shared band of tests/helpers.py
# is one counter for the whole suite, and a 100-recipient job here would
# eat a tenth of it.
_TELEGRAM_IDS = itertools.count(92_200_000)


async def _recipient(session: AsyncSession) -> Any:
    return await create_recipient(session, telegram_id=next(_TELEGRAM_IDS))


def _key() -> str:
    return f"p2-2:{uuid4()}"


# -----------------------------------------------------------------------------
# The grid: state -> form of the answer
# -----------------------------------------------------------------------------


class TestGrid:
    async def test_unknown_key_is_404_with_the_one_error_body(
        self, bare_client: AsyncClient
    ) -> None:
        for tail in ("", "/deliveries", "/path"):
            r = await _read(bare_client, _key(), tail)
            assert r.status_code == 404
            assert list(r.json()) == ["error"]
            assert r.json()["error"]["class"] == "not_found"

    async def test_rejected_at_intake_reads_with_its_reason(
        self, bare_client: AsyncClient
    ) -> None:
        key = _key()
        assert await _send(_request(key, uuid4(), type="no_such_type")) == "rejected"
        r = await _read(bare_client, key)
        assert r.status_code == 200
        body = r.json()
        assert body["job"] is None
        (item,) = body["intake"]
        assert item["outcome"] == IntakeOutcomeClass.REJECTED_AT_INTAKE
        assert "no_such_type" in item["reason"]
        assert item["notification_id"] is None
        for tail in ("/deliveries", "/path"):
            assert (await _read(bare_client, key, tail)).status_code == 404

    async def test_accepted_pending_job(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        assert await _send(_request(key, recipient.id)) == "processed"
        body = (await _read(bare_client, key)).json()
        assert body["idempotency_key"] == key
        assert body["intake"] == []
        job = body["job"]
        assert job["status"] == NotificationStatus.PENDING
        assert job["pipeline"] == {
            "attempts": 0,
            "step": None,
            "error": None,
            "retry_at": None,
        }
        assert job["deliveries"] == []
        path = (await _read(bare_client, key, "/path")).json()
        assert [(i["subject"], i["step"]) for i in path["items"]] == [
            ("job", "intake"),
        ]
        assert (await _read(bare_client, key, "/deliveries")).json() == {
            "items": [],
            "next_cursor": None,
        }

    async def test_rejection_before_and_conflict_after_acceptance(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id, type="no_such_type"))
        await _send(_request(key, recipient.id))
        assert (
            await _send(_request(key, recipient.id, body="other bytes")) == "conflict"
        )
        body = (await _read(bare_client, key)).json()
        assert [i["outcome"] for i in body["intake"]] == [
            IntakeOutcomeClass.REJECTED_AT_INTAKE,
            IntakeOutcomeClass.CONFLICT,
        ]
        assert body["intake"][1]["notification_id"] == body["job"]["id"]

    async def test_a_replay_is_not_shown(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id))
        assert await _send(_request(key, recipient.id)) == "duplicate"
        body = (await _read(bare_client, key)).json()
        assert body["intake"] == []
        assert body["job"] is not None

    async def test_conflict_whose_job_is_past_retention(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id))
        await _send(_request(key, recipient.id, body="other bytes"))
        async with get_session_factory()() as session:
            await session.execute(
                delete(Notification).where(Notification.idempotency_key == key)
            )
            await session.commit()
        body = (await _read(bare_client, key)).json()
        assert body["job"] is None
        assert [(i["outcome"], i["notification_id"]) for i in body["intake"]] == [
            (IntakeOutcomeClass.CONFLICT, None),
        ]

    async def test_waiting_on_the_schedule(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id))
        until = datetime.now(UTC) + timedelta(hours=2)
        with (
            _channel(_Spy()),
            patch("app.engine.service.recipient_deferred_until", return_value=until),
        ):
            await process_pending_notifications()
        body = (await _read(bare_client, key)).json()
        assert body["job"]["status"] == NotificationStatus.PROCESSING
        assert body["job"]["deliveries"] == [
            {"channel": "in_app", "status": "pending", "count": 1},
        ]
        (item,) = (await _read(bare_client, key, "/deliveries")).json()["items"]
        assert (item["status"], item["wait_reason"], item["next_retry_at"]) == (
            DeliveryStatus.PENDING,
            WaitReason.RECIPIENT_SCHEDULE,
            until.isoformat(),
        )
        assert item["recipient_id"] == str(recipient.id)

    async def test_sent(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id))
        with _channel(_Spy()):
            await process_pending_notifications()
        body = (await _read(bare_client, key)).json()
        assert body["job"]["status"] == NotificationStatus.SENT
        (item,) = (await _read(bare_client, key, "/deliveries")).json()["items"]
        assert item["status"] == DeliveryStatus.SENT and item["sent_at"]
        path = (await _read(bare_client, key, "/path")).json()["items"]
        assert ("channel", ChannelAnswer.ACCEPTED) in [
            (p["subject"], p["outcome"]) for p in path
        ]

    async def test_failed_with_class_and_the_providers_sanitized_words(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id))
        secret = "cd" * 20  # glued at runtime
        refusal = ConfigurationError(
            f"provider refused (401): Bearer {secret} Forbidden"
        )
        with _channel(_Spy({"in_app": refusal})):
            await process_pending_notifications()
        (item,) = (await _read(bare_client, key, "/deliveries")).json()["items"]
        assert (item["status"], item["failure_class"]) == (
            DeliveryStatus.FAILED,
            FailureClass.CONFIGURATION,
        )
        path = (await _read(bare_client, key, "/path")).json()["items"]
        (answer,) = [p for p in path if p["subject"] == "channel"]
        assert "Forbidden" in answer["provider_text"]
        assert "[redacted]" in answer["provider_text"]
        assert secret not in json.dumps(path)
        async with get_session_factory()() as session:
            stored = (
                await session.execute(
                    select(NotificationTransition.provider_text).where(
                        NotificationTransition.subject == JournalSubject.CHANNEL,
                        NotificationTransition.notification_id == await _job_id(key),
                    )
                )
            ).scalar_one()
        assert answer["provider_text"] == stored  # the journal's text, as is

    async def test_suppressed_names_the_category(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        db_session.add(CategoryMute(recipient_id=recipient.id, category="unit_updates"))
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id))
        with _channel(_Spy()):
            await process_pending_notifications()
        body = (await _read(bare_client, key)).json()
        assert body["job"]["status"] == NotificationStatus.SUPPRESSED
        path = (await _read(bare_client, key, "/path")).json()["items"]
        (gate,) = [p for p in path if p["subject"] == "gate"]
        assert (gate["recipient_id"], gate["category"]) == (
            str(recipient.id),
            "unit_updates",
        )

    async def test_behind_the_pipeline_gate(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id))
        with (
            _channel(_Spy()),
            patch.object(processor, "rollup_notification", _poisoned_rollup),
        ):
            await process_pending_notifications()
        pipeline = (await _read(bare_client, key)).json()["job"]["pipeline"]
        assert pipeline["attempts"] == 1
        assert pipeline["step"] == PipelineStep.ROLLUP
        assert pipeline["error"] and pipeline["retry_at"]
        path = (await _read(bare_client, key, "/path")).json()["items"]
        assert path[-1]["wait_reason"] == "pipeline_retry"

    @pytest.mark.parametrize(
        ("outcome", "delivery"),
        [
            (NotificationStatus.EXPIRED, DeliveryStatus.EXPIRED),
            (NotificationStatus.CANCELLED, DeliveryStatus.CANCELLED),
        ],
    )
    async def test_expired_and_cancelled(
        self,
        bare_client: AsyncClient,
        db_session: AsyncSession,
        outcome: NotificationStatus,
        delivery: DeliveryStatus,
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id))
        with (
            _channel(_Spy()),
            patch(
                "app.engine.service.recipient_deferred_until",
                return_value=datetime.now(UTC) + timedelta(hours=1),
            ),
        ):
            await process_pending_notifications()
        async with get_session_factory()() as session:
            await close_notifications(
                session, Notification.idempotency_key == key, outcome
            )
            await session.commit()
        assert (await _read(bare_client, key)).json()["job"]["status"] == outcome
        (item,) = (await _read(bare_client, key, "/deliveries")).json()["items"]
        assert item["status"] == delivery

    async def test_after_forgetting_the_providers_words_are_null(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id))
        with _channel(_Spy({"in_app": RateLimitedError(30.0, ": addr@example.test")})):
            await process_pending_notifications()
        before = (await _read(bare_client, key, "/path")).json()["items"]
        (spoke,) = [p for p in before if p["subject"] == "channel"]
        assert spoke["provider_text"]
        async with get_session_factory()() as session:
            await withdraw_recipient(session, recipient.id)
            await session.commit()
        after = (await _read(bare_client, key, "/path")).json()["items"]
        (answer,) = [p for p in after if p["subject"] == "channel"]
        assert answer["provider_text"] is None
        assert answer["outcome"] == ChannelAnswer.RATE_LIMITED  # the row stays

    async def test_a_job_born_before_the_journal(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        """A job accepted before migration 0017 has no journal rows: its
        path is empty, its summary and deliveries are whole."""
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(_request(key, recipient.id))
        with _channel(_Spy()):
            await process_pending_notifications()
        async with get_session_factory()() as session:
            await session.execute(
                delete(NotificationTransition).where(
                    NotificationTransition.notification_id == await _job_id(key)
                )
            )
            await session.commit()
        assert (await _read(bare_client, key, "/path")).json() == {
            "items": [],
            "next_cursor": None,
        }
        body = (await _read(bare_client, key)).json()
        assert body["job"]["status"] == NotificationStatus.SENT
        assert len((await _read(bare_client, key, "/deliveries")).json()["items"]) == 1


# -----------------------------------------------------------------------------
# The key: a query parameter, read back by its exact value
# -----------------------------------------------------------------------------

_ODD_KEYS = [
    "a/b?c%d e+f",
    "заказ/№1",
    "a//b/../c",
    "#frag&x=1",
    "  spaced  ",
    "x" * 200,
]


class TestKey:
    @pytest.mark.parametrize("key", _ODD_KEYS, ids=range(len(_ODD_KEYS)))
    async def test_any_key_reads_back(
        self, bare_client: AsyncClient, db_session: AsyncSession, key: str
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        await _send(_request(key, recipient.id))
        body = (await _read(bare_client, key)).json()
        assert body["idempotency_key"] == key
        assert body["job"]["id"] == str(await _job_id(key))

    async def test_an_unencoded_plus_reads_as_a_space(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        """The documented trap (INTEGRATION.md §8): the parser reads a raw
        '+' as a space; %2B is a plus."""
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = f"plus+{uuid4()}"
        await _send(_request(key, recipient.id))
        raw = await bare_client.get(f"{_BASE}?key={key}")
        assert raw.status_code == 404
        encoded = await bare_client.get(f"{_BASE}?key={key.replace('+', '%2B')}")
        assert encoded.json()["idempotency_key"] == key

    @pytest.mark.parametrize("query", ["", "?key=", "?key=" + "y" * 201])
    async def test_a_key_outside_1_200_is_refused(
        self, bare_client: AsyncClient, query: str
    ) -> None:
        for tail in ("", "/deliveries", "/path"):
            r = await bare_client.get(f"{_BASE}{tail}{query}")
            assert r.status_code == 422
            assert r.json()["error"]["class"] == "validation"


# -----------------------------------------------------------------------------
# Paging
# -----------------------------------------------------------------------------


async def _group_job(db_session: AsyncSession, size: int) -> str:
    group = f"g-{uuid4()}"
    for _ in range(size):
        recipient = await _recipient(db_session)
        await add_to_group(db_session, group, recipient.id)
    await db_session.commit()
    key = _key()
    await _send(_request(key, group, target_type=TargetType.GROUP))
    with _channel(_Spy()):
        await process_pending_notifications()
    return key


class TestPaging:
    async def test_deliveries_page_without_overlap(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        key = await _group_job(db_session, 5)
        first = (await _read(bare_client, key, "/deliveries", limit=3)).json()
        assert len(first["items"]) == 3 and first["next_cursor"]
        second = (
            await _read(
                bare_client, key, "/deliveries", limit=3, cursor=first["next_cursor"]
            )
        ).json()
        assert len(second["items"]) == 2 and second["next_cursor"] is None
        ids = [i["recipient_id"] for i in first["items"] + second["items"]]
        assert len(set(ids)) == 5

    async def test_path_pages_in_order(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        key = await _group_job(db_session, 3)
        whole = (await _read(bare_client, key, "/path", limit=100)).json()["items"]
        seen: list[dict[str, Any]] = []
        cursor = None
        while True:
            params: dict[str, Any] = {"limit": 4}
            if cursor:
                params["cursor"] = cursor
            got = (await _read(bare_client, key, "/path", **params)).json()
            seen += got["items"]
            cursor = got["next_cursor"]
            if cursor is None:
                break
        assert seen == whole and len(whole) > 4
        assert [i["at"] for i in whole] == sorted(i["at"] for i in whole)

    async def test_a_cursor_from_the_other_listing_is_refused(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        key = await _group_job(db_session, 3)
        d = (await _read(bare_client, key, "/deliveries", limit=1)).json()[
            "next_cursor"
        ]
        p = (await _read(bare_client, key, "/path", limit=1)).json()["next_cursor"]
        assert (await _read(bare_client, key, "/path", cursor=d)).status_code == 422
        assert (
            await _read(bare_client, key, "/deliveries", cursor=p)
        ).status_code == 422
        assert (await _read(bare_client, key, "/path", limit=0)).status_code == 422
        assert (await _read(bare_client, key, "/path", cursor="!!")).status_code == 422


# -----------------------------------------------------------------------------
# Never the letter
# -----------------------------------------------------------------------------


class TestNoLetter:
    async def test_no_letter_and_no_comms_error_text_in_any_answer(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        recipient = await _recipient(db_session)
        await db_session.commit()
        key = _key()
        await _send(
            _request(
                key,
                recipient.id,
                title=_TITLE_SENTINEL,
                body=_BODY_SENTINEL,
                action_data={"action": "open", "params": {"p": _PARAM_SENTINEL}},
            )
        )

        class _DefectError(RuntimeError):
            pass

        with _channel(_Spy({"in_app": _DefectError(_ERROR_SENTINEL)})):
            await process_pending_notifications()
        # The sentinels ARE in the database -- there is something to leak.
        async with get_session_factory()() as session:
            stored = (
                await session.execute(
                    select(Notification.title, NotificationDelivery.error_message)
                    .join(NotificationDelivery)
                    .where(Notification.idempotency_key == key)
                )
            ).one()
        assert stored.title == _TITLE_SENTINEL
        assert _ERROR_SENTINEL in (stored.error_message or "")
        answers = [
            (await _read(bare_client, key, tail)).text
            for tail in ("", "/deliveries", "/path")
        ]
        # The pair: the answers are there and say something.
        assert all(len(a) > 50 for a in answers)
        assert json.loads(answers[2])["items"]
        joined = "\n".join(answers)
        for sentinel in (
            _TITLE_SENTINEL,
            _BODY_SENTINEL,
            _PARAM_SENTINEL,
            _ERROR_SENTINEL,
        ):
            assert sentinel not in joined

    def test_the_sentinels_are_not_routine(self) -> None:
        for sentinel in (
            _TITLE_SENTINEL,
            _BODY_SENTINEL,
            _PARAM_SENTINEL,
            _ERROR_SENTINEL,
        ):
            assert sentinel and sentinel not in "pending processing sent failed"


# -----------------------------------------------------------------------------
# Authorization, read only, a fixed number of queries
# -----------------------------------------------------------------------------


class TestAuth:
    async def test_the_service_token_guards_every_route(
        self, bare_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        token = "tok-" + "e" * 12  # glued at runtime
        monkeypatch.setattr(settings, "comms_service_token", token)
        for tail in ("", "/deliveries", "/path"):
            refused = await bare_client.get(f"{_BASE}{tail}", params={"key": "k"})
            assert refused.status_code == 401
            assert refused.json()["error"]["class"] == "unauthorized"
            allowed = await bare_client.get(
                f"{_BASE}{tail}",
                params={"key": "k"},
                headers={"Authorization": f"Bearer {token}"},
            )
            assert allowed.status_code == 404  # past the token: the key is unknown


async def _row_counts() -> dict[str, int]:
    async with get_session_factory()() as session:
        tables = (
            (
                await session.execute(
                    text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                )
            )
            .scalars()
            .all()
        )
        counts = {}
        for table in tables:
            counts[table] = (
                await session.execute(text(f'SELECT count(*) FROM "{table}"'))
            ).scalar_one()
    return counts


class TestReadOnly:
    async def test_reading_changes_no_row(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        key = await _group_job(db_session, 3)
        before = await _row_counts()
        assert sum(before.values()) > 0  # the pair
        for tail in ("", "/deliveries", "/path"):
            assert (await _read(bare_client, key, tail)).status_code == 200
        assert await _row_counts() == before

    async def test_the_reading_session_refuses_a_write(self) -> None:
        reader = jobs.read_only_snapshot()
        session = await reader.__anext__()
        try:
            with pytest.raises(DBAPIError, match="read-only"):
                await session.execute(
                    update(Notification)
                    .values(title="x")
                    .where(Notification.title == "-")
                )
        finally:
            await reader.aclose()


@contextmanager
def _counting() -> Iterator[list[str]]:
    seen: list[str] = []

    def on_execute(*args: Any) -> None:
        seen.append(args[2])

    sync_engine = get_engine().sync_engine
    event.listen(sync_engine, "before_cursor_execute", on_execute)
    try:
        yield seen
    finally:
        event.remove(sync_engine, "before_cursor_execute", on_execute)


class TestQueryCount:
    async def test_one_and_a_hundred_deliveries_cost_the_same_queries(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        small = await _group_job(db_session, 1)
        large = await _group_job(db_session, 100)
        costs = {}
        for key in (small, large):
            per_route = []
            for tail, params in (
                ("", {}),
                ("/deliveries", {"limit": 100}),
                ("/path", {"limit": 100}),
            ):
                with _counting() as seen:
                    r = await _read(bare_client, key, tail, **params)
                assert r.status_code == 200
                per_route.append(len(seen))
            costs[key] = per_route
        assert costs[small] == costs[large]
        assert all(n >= 1 for n in costs[small])  # the pair: queries were counted
        big = (await _read(bare_client, large, "/deliveries", limit=100)).json()
        assert len(big["items"]) == 100


# -----------------------------------------------------------------------------
# The contract: INTEGRATION.md §8 against the code, both ways
# -----------------------------------------------------------------------------

_CELL = re.compile(r"`([^`]+)`")


def _section_8() -> str:
    text_ = _DOC.read_text()
    start = text_.index("\n## 8. ")
    end = text_.find("\n## ", start + 1)
    return text_[start:end]


def _tables() -> dict[str, dict[str, str]]:
    """{form: {field: value-cell}} from the `### `<form>`` headings."""
    found: dict[str, dict[str, str]] = {}
    for block in re.split(r"\n### ", _section_8())[1:]:
        heading, _, body = block.partition("\n")
        named = _CELL.findall(heading.split(" -- ", 1)[0])
        assert len(named) == 1, heading
        rows = [line for line in body.splitlines() if line.startswith("|")]
        header, _sep, *data = rows
        assert header.split("|")[1].strip() == "field"
        fields: dict[str, str] = {}
        for row in data:
            cells = row.split("|")
            (name,) = _CELL.findall(cells[1])
            assert name not in fields, f"{named[0]}.{name} twice"
            fields[name] = cells[2]
        found[named[0]] = fields
    return found


class TestContract:
    def test_every_form_equals_its_document_table(self) -> None:
        tables = _tables()
        assert set(tables) == set(jobs.FORMS)
        for form, fields in jobs.FORMS.items():
            assert list(tables[form]) == list(fields), form

    @pytest.mark.parametrize(
        ("form", "field", "values"),
        [
            ("job", "status", {s.value for s in NotificationStatus}),
            ("job", "target_type", {s.value for s in TargetType}),
            ("pipeline", "step", {s.value for s in PipelineStep}),
            ("intake_item", "outcome", {s.value for s in IntakeOutcomeClass}),
            ("delivery_item", "status", {s.value for s in DeliveryStatus}),
            ("delivery_item", "failure_class", {s.value for s in FailureClass}),
            ("delivery_item", "wait_reason", {s.value for s in WaitReason}),
            ("path_item", "subject", {s.value for s in JournalSubject}),
            ("path_item", "step", {s.value for s in JournalStep}),
        ],
    )
    def test_every_enumeration_in_the_document_is_the_codes(
        self, form: str, field: str, values: set[str]
    ) -> None:
        named = set(_CELL.findall(_tables()[form][field])) - {"null"}
        assert named == values

    def test_the_channel_answers_are_named(self) -> None:
        named = set(_CELL.findall(_tables()["path_item"]["outcome"]))
        assert {a.value for a in ChannelAnswer} <= named

    async def test_a_live_answer_carries_exactly_the_documented_fields(
        self, bare_client: AsyncClient, db_session: AsyncSession
    ) -> None:
        key = await _group_job(db_session, 2)
        body = (await _read(bare_client, key)).json()
        assert list(body) == list(jobs.SUMMARY_FIELDS)
        assert list(body["job"]) == list(jobs.JOB_FIELDS)
        assert list(body["job"]["pipeline"]) == list(jobs.PIPELINE_FIELDS)
        assert list(body["job"]["deliveries"][0]) == list(jobs.DELIVERY_COUNT_FIELDS)
        item = (await _read(bare_client, key, "/deliveries")).json()["items"][0]
        assert list(item) == list(jobs.DELIVERY_ITEM_FIELDS)
        step = (await _read(bare_client, key, "/path")).json()["items"][0]
        assert list(step) == list(jobs.PATH_ITEM_FIELDS)
