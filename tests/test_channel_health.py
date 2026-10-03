# =============================================================================
# P2-4 Б1 -- channel health over a window (spec §6.7)
# =============================================================================
#
# The second answer next to GET /health: what each channel answered in
# the last N minutes, counted from the journal's channel rows, stored
# nowhere, behind the service token; /health itself does not change.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1 the subject = 'channel' filter dropped
#                               -> TestWhatIsCounted.test_other_subjects_...
#   M2 an empty window answers 0.0
#                               -> TestEmptyWindow
#   M3 every refusal in the numerator
#                               -> TestWhatIsCounted.test_only_configuration_...
#   M4 the window's lower bound turned (at < from) or made strict
#                               -> TestWindow
#   M5 counts added to the /health body
#                               -> TestHealthUnchanged
#   M6 require_service_auth dropped from the router
#                               -> TestAuth
#   M7 a write on the read path
#                               -> TestNothingStored
#   M8 the index of migration 0018 dropped
#                               -> TestCost
#   M12 the window clamped instead of refused
#                               -> TestWindowBounds
# =============================================================================

import ast
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.constants import (
    HEALTH_WINDOW_DEFAULT_MINUTES,
    HEALTH_WINDOW_MAX_MINUTES,
)
from app.core.database import Base, get_session_factory
from app.engine import health
from app.engine.constants import (
    ChannelAnswer,
    DeliveryChannel,
    FailureClass,
    JournalSubject,
    TargetType,
)
from app.engine.formatters import (
    EmailFormatter,
    PermanentDeliveryError,
    channel_map,
)
from app.engine.health import REFUSAL_CLASSES, window_counts
from app.engine.models import Notification, NotificationTransition
from app.engine.processor import process_pending_notifications
from app.engine.service import _failure_class_of, create_notification
from tests.helpers import (
    configure_every_channel,
    create_recipient,
    intake_fields,
    notification_row_fields,
)

_URL = "/api/v1/channels/health"
_ROOT = Path(__file__).resolve().parents[1]
_TOKEN = "p2-4-health-unit-test-token"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


async def _job() -> UUID:
    """A job row the journal rows can hang on (FK)."""
    async with get_session_factory()() as session:
        notification = Notification(
            type="unit_event", title="T", body="B",
            target_type=TargetType.USER, target_value=str(uuid4()),
            **notification_row_fields(),
        )
        session.add(notification)
        await session.commit()
        return notification.id


async def _rows(
    job: UUID,
    *,
    channel: str = "email",
    outcome: str = ChannelAnswer.ACCEPTED,
    failure_class: str | None = None,
    subject: str = JournalSubject.CHANNEL,
    ago: timedelta = timedelta(minutes=1),
    count: int = 1,
) -> None:
    """`count` journal rows written `ago` before the database's now()."""
    async with get_session_factory()() as session:
        for _ in range(count):
            fields: dict[str, Any] = {
                "notification_id": job,
                "subject": subject,
                "step": "deliver",
                "outcome": outcome,
                "attempt": 1,
                "failure_class": failure_class,
            }
            if subject != JournalSubject.JOB:
                fields["recipient_id"] = uuid4()
            if subject in (JournalSubject.CHANNEL, JournalSubject.DELIVERY):
                fields["channel"] = channel
            row = NotificationTransition(**fields)
            session.add(row)
            await session.flush()
            await session.execute(
                text(
                    "UPDATE notification_transitions SET at = now() - "
                    "make_interval(secs => :s) WHERE id = :id"
                ),
                {"s": ago.total_seconds(), "id": row.id},
            )
        await session.commit()


async def _health(client: AsyncClient, **params: Any) -> dict[str, Any]:
    response = await client.get(_URL, params=params)
    assert response.status_code == 200, response.text
    body: dict[str, Any] = response.json()
    return body


# -----------------------------------------------------------------------------
# done-when (1): the aivis install, through the real pipeline
# -----------------------------------------------------------------------------


class _Provider401:
    """Mailgun as it answered at the aivis install: 401 to every send."""

    def __init__(self) -> None:
        self.calls = 0

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        return httpx.Response(401, text="Forbidden")


@contextmanager
def _email_answering_401(provider: _Provider401) -> Iterator[None]:
    formatter = EmailFormatter(
        client=httpx.AsyncClient(transport=httpx.MockTransport(provider.handle)),
        api_base_url=settings.email_api_base_url,
        api_key=settings.email_mailgun_api_key,
        domain=settings.email_mailgun_domain,
        from_address=settings.email_from_address,
    )
    with patch("app.engine.service.get_formatter", return_value=formatter):
        yield


class TestTheAivisInstall:
    async def test_keys_present_every_answer_a_configuration_refusal(
        self,
        client: AsyncClient,
        db_session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Keys set, every letter refused 401: health says 100 %
        configuration refusals while /health still says `email: live`."""
        configure_every_channel(monkeypatch)
        provider = _Provider401()
        for _ in range(3):
            recipient = await create_recipient(
                db_session, email=f"r{uuid4().hex[:8]}@unit-test.invalid",
            )
            await create_notification(
                db_session, **intake_fields(),
                type="unit_event_email", title="T", body="B",
                target_type=TargetType.USER, target_value=str(recipient.id),
            )
        await db_session.commit()
        with _email_answering_401(provider):
            await process_pending_notifications()
        assert provider.calls == 3, "the pair: the provider was really called"

        email = (await _health(client))["channels"]["email"]
        assert email["state"] == "live"
        assert email["answers"] == 3
        assert email["by_outcome"][ChannelAnswer.REFUSED] == 3
        assert email["refused_by_class"][FailureClass.CONFIGURATION] == 3
        assert email["configuration_share"] == 1.0

        shallow = await client.get("/health")
        assert shallow.status_code == 200
        assert shallow.json()["channels"]["email"] == "live"


# -----------------------------------------------------------------------------
# done-when (2): an empty window is its own answer
# -----------------------------------------------------------------------------


class TestEmptyWindow:
    async def test_no_rows_at_all_is_null_not_a_percentage(
        self, client: AsyncClient,
    ) -> None:
        body = await _health(client)
        assert set(body["channels"]) == {c.value for c in DeliveryChannel}
        for name, entry in body["channels"].items():
            assert entry["answers"] == 0, name
            assert entry["configuration_share"] is None, name
            assert set(entry["by_outcome"].values()) == {0}, name

    async def test_rows_only_before_the_window_read_as_empty(
        self, client: AsyncClient,
    ) -> None:
        job = await _job()
        await _rows(
            job, outcome=ChannelAnswer.REFUSED,
            failure_class=FailureClass.CONFIGURATION,
            ago=timedelta(minutes=61), count=2,
        )
        email = (await _health(client, window_minutes=60))["channels"]["email"]
        assert email["answers"] == 0
        assert email["configuration_share"] is None
        # The pair: the same rows ARE counted by a window that reaches them.
        wide = (await _health(client, window_minutes=120))["channels"]["email"]
        assert wide["answers"] == 2
        assert wide["configuration_share"] == 1.0

    async def test_one_channel_empty_next_to_one_that_answered(
        self, client: AsyncClient,
    ) -> None:
        await _rows(await _job(), channel="telegram", count=2)
        channels = (await _health(client))["channels"]
        assert channels["telegram"]["answers"] == 2
        assert channels["telegram"]["configuration_share"] == 0.0
        assert channels["email"]["answers"] == 0
        assert channels["email"]["configuration_share"] is None


# -----------------------------------------------------------------------------
# What is counted
# -----------------------------------------------------------------------------


class TestWhatIsCounted:
    async def test_other_subjects_are_not_channel_answers(
        self, client: AsyncClient,
    ) -> None:
        job = await _job()
        await _rows(job, subject=JournalSubject.JOB, outcome="pending")
        await _rows(job, subject=JournalSubject.DELIVERY, outcome="failed",
                    failure_class=FailureClass.CONFIGURATION)
        await _rows(job, subject=JournalSubject.GATE, outcome="suppressed")
        await _rows(job, outcome=ChannelAnswer.ACCEPTED)
        email = (await _health(client))["channels"]["email"]
        assert email["answers"] == 1
        assert email["refused_by_class"][FailureClass.CONFIGURATION] == 0
        assert email["configuration_share"] == 0.0

    async def test_only_configuration_is_in_the_numerator(
        self, client: AsyncClient,
    ) -> None:
        job = await _job()
        await _rows(job, outcome=ChannelAnswer.REFUSED,
                    failure_class=FailureClass.CONFIGURATION)
        await _rows(job, outcome=ChannelAnswer.REFUSED,
                    failure_class=FailureClass.NO_ADDRESS)
        await _rows(job, outcome=ChannelAnswer.REFUSED,
                    failure_class=FailureClass.MESSAGE_REJECTED)
        await _rows(job, outcome=ChannelAnswer.ACCEPTED)
        email = (await _health(client))["channels"]["email"]
        assert email["answers"] == 4
        assert email["by_outcome"][ChannelAnswer.REFUSED] == 3
        assert email["refused_by_class"] == {
            FailureClass.CONFIGURATION: 1,
            FailureClass.NO_ADDRESS: 1,
            FailureClass.MESSAGE_REJECTED: 1,
        }
        assert email["configuration_share"] == 0.25

    async def test_retries_count_once_each_on_purpose(
        self, client: AsyncClient,
    ) -> None:
        """k transient answers of one letter and one dead-deploy refusal:
        the channel was called k+1 times."""
        job = await _job()
        await _rows(job, outcome=ChannelAnswer.TRANSIENT, count=3)
        await _rows(job, outcome=ChannelAnswer.REFUSED,
                    failure_class=FailureClass.CONFIGURATION)
        email = (await _health(client))["channels"]["email"]
        assert email["answers"] == 4
        assert email["by_outcome"][ChannelAnswer.TRANSIENT] == 3
        assert email["configuration_share"] == 0.25

    async def test_every_outcome_has_its_key(
        self, client: AsyncClient,
    ) -> None:
        job = await _job()
        for answer in ChannelAnswer:
            await _rows(
                job, outcome=answer,
                failure_class=(
                    FailureClass.CONFIGURATION
                    if answer == ChannelAnswer.REFUSED else None
                ),
            )
        email = (await _health(client))["channels"]["email"]
        assert email["by_outcome"] == {a.value: 1 for a in ChannelAnswer}
        assert email["answers"] == len(ChannelAnswer)

    async def test_the_state_is_the_map_of_health(
        self, client: AsyncClient,
    ) -> None:
        body = await _health(client)
        shallow = (await client.get("/health")).json()["channels"]
        assert shallow, "the pair: the map is not empty"
        assert {n: e["state"] for n, e in body["channels"].items()} == shallow
        assert shallow == channel_map(settings)

    def test_refusal_classes_are_the_ones_a_refusal_can_carry(self) -> None:
        """Pinned to the one place that decides a refusal's class."""
        decided = {
            _failure_class_of(cls("x"))
            for cls in PermanentDeliveryError.__subclasses__()
        }
        assert decided, "the pair: there are refusal classes at all"
        assert decided == set(REFUSAL_CLASSES)


# -----------------------------------------------------------------------------
# The window
# -----------------------------------------------------------------------------


class TestWindow:
    async def test_a_row_inside_counts_one_outside_does_not(
        self, client: AsyncClient,
    ) -> None:
        job = await _job()
        await _rows(job, ago=timedelta(minutes=4, seconds=50))
        await _rows(job, ago=timedelta(minutes=5, seconds=30))
        email = (await _health(client, window_minutes=5))["channels"]["email"]
        assert email["answers"] == 1

    async def test_both_bounds_are_inclusive(
        self, db_session: AsyncSession,
    ) -> None:
        """The statement the route runs, with bounds set to the rows'
        own timestamps."""
        job = await _job()
        await _rows(job, ago=timedelta(minutes=10))
        await _rows(job, ago=timedelta(minutes=2))
        stamps = sorted(
            (await db_session.execute(select(NotificationTransition.at))).scalars()
        )
        assert len(stamps) == 2
        rows = (await db_session.execute(window_counts(stamps[0], stamps[1]))).all()
        assert sum(r[3] for r in rows) == 2
        rows = (await db_session.execute(
            window_counts(stamps[0] + timedelta(microseconds=1), stamps[1]),
        )).all()
        assert sum(r[3] for r in rows) == 1

    async def test_the_window_is_echoed_by_the_database_clock(
        self, client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        body = await _health(client, window_minutes=30)
        window = body["window"]
        assert window["minutes"] == 30
        start = datetime.fromisoformat(window["from"])
        end = datetime.fromisoformat(window["to"])
        assert end - start == timedelta(minutes=30)
        db_now = (await db_session.execute(select(func.now()))).scalar_one()
        assert abs((db_now - end).total_seconds()) < 60

    async def test_the_default_window(self, client: AsyncClient) -> None:
        body = await _health(client)
        assert body["window"]["minutes"] == HEALTH_WINDOW_DEFAULT_MINUTES == 60

    async def test_two_calls_in_a_row_agree(self, client: AsyncClient) -> None:
        await _rows(await _job(), count=2)
        first = (await _health(client))["channels"]
        second = (await _health(client))["channels"]
        assert first == second
        assert first["email"]["answers"] == 2


class TestWindowBounds:
    @pytest.mark.parametrize("value", [0, -1, HEALTH_WINDOW_MAX_MINUTES + 1])
    async def test_out_of_bounds_is_refused_not_clamped(
        self, client: AsyncClient, value: int,
    ) -> None:
        response = await client.get(_URL, params={"window_minutes": value})
        assert response.status_code == 422
        assert response.json()["error"]["class"] == "validation"

    @pytest.mark.parametrize("value", ["", "abc", "1.5"])
    async def test_not_an_integer_is_refused(
        self, client: AsyncClient, value: str,
    ) -> None:
        response = await client.get(f"{_URL}?window_minutes={value}")
        assert response.status_code == 422
        assert response.json()["error"]["class"] == "validation"

    @pytest.mark.parametrize("value", [1, HEALTH_WINDOW_MAX_MINUTES])
    async def test_the_bounds_themselves_are_taken(
        self, client: AsyncClient, value: int,
    ) -> None:
        body = await _health(client, window_minutes=value)
        assert body["window"]["minutes"] == value


# -----------------------------------------------------------------------------
# done-when (3): nothing is stored
# -----------------------------------------------------------------------------

_HEALTH_SOURCES = ("app/engine/health.py", "app/api/channels.py")
# Every way a write can be spelled from a module of this service.
_WRITE_CALLS = frozenset({
    "add", "add_all", "delete", "merge", "flush", "commit",
    "insert", "update", "execute_write",
})
# Names a stored health state would plausibly carry.
_STATE_FORMS = ("health", "broken", "dead", "degraded", "viable", "channel_state")


def _called_names(rel: str) -> set[str]:
    tree = ast.parse((_ROOT / rel).read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func_node = node.func
            if isinstance(func_node, ast.Attribute):
                names.add(func_node.attr)
            elif isinstance(func_node, ast.Name):
                names.add(func_node.id)
    return names


class TestNothingStored:
    @pytest.mark.parametrize("rel", _HEALTH_SOURCES)
    def test_the_read_path_calls_no_write(self, rel: str) -> None:
        called = _called_names(rel)
        assert called, "the pair: the scanner sees calls in the module"
        assert "select" in called or "channel_health" in called
        assert not called & _WRITE_CALLS, called & _WRITE_CALLS

    def test_no_table_or_column_holds_a_health_state(self) -> None:
        tables = Base.metadata.tables
        assert "notification_transitions" in tables, "the pair"
        names = [
            name
            for table in tables.values()
            for name in (table.name, *(c.name for c in table.columns))
        ]
        assert len(names) > 50, "the pair: the scan sees the schema"
        hits = [n for n in names for form in _STATE_FORMS if form in n]
        assert hits == []

    async def test_the_database_has_no_such_column_either(
        self, db_session: AsyncSession,
    ) -> None:
        names = (await db_session.execute(text(
            "SELECT table_name || '.' || column_name "
            "FROM information_schema.columns WHERE table_schema = 'public'"
        ))).scalars().all()
        assert any(n.startswith("notification_transitions.") for n in names)
        hits = [n for n in names for form in _STATE_FORMS if form in n]
        assert hits == []

    async def test_a_call_changes_no_row(
        self, client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        await _rows(await _job(), count=3)
        before = await _row_counts(db_session)
        assert before["notification_transitions"] == 3, "the pair"
        await _health(client)
        assert await _row_counts(db_session) == before


async def _row_counts(session: AsyncSession) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in Base.metadata.sorted_tables:
        counts[table.name] = (
            await session.execute(select(func.count()).select_from(table))
        ).scalar_one()
    return counts


# -----------------------------------------------------------------------------
# done-when (4): /health keeps its form and its code
# -----------------------------------------------------------------------------

_HEALTH_KEYS = {"status", "db", "version", "channels"}


class TestHealthUnchanged:
    async def test_the_body_is_the_same_before_and_after_failures(
        self, client: AsyncClient,
    ) -> None:
        before = await client.get("/health")
        job = await _job()
        await _rows(job, outcome=ChannelAnswer.REFUSED,
                    failure_class=FailureClass.CONFIGURATION, count=5)
        assert (await _health(client))["channels"]["email"]["answers"] == 5
        after = await client.get("/health")
        assert before.status_code == after.status_code == 200
        assert set(after.json()) == _HEALTH_KEYS
        assert after.json() == before.json()
        assert set(after.json()["channels"].values()) <= {
            "live", "not_configured", "not_implemented",
        }


# -----------------------------------------------------------------------------
# done-when (5): the read is an index scan at volume
# -----------------------------------------------------------------------------


class TestCost:
    async def test_the_window_reads_the_index_on_100k_rows(self) -> None:
        job = await _job()
        async with get_session_factory()() as session:
            # 100 000 channel answers spread over 30 days, plus 20 000
            # rows of other subjects that the index must not hold.
            await session.execute(text(
                "INSERT INTO notification_transitions "
                "(notification_id, recipient_id, channel, subject, step, "
                " outcome, attempt, failure_class, at) "
                "SELECT :job, gen_random_uuid(), "
                "(ARRAY['email','telegram','in_app'])[1 + i % 3], "
                "'channel', 'deliver', "
                "CASE WHEN i % 5 = 0 THEN 'refused' ELSE 'accepted' END, 1, "
                "CASE WHEN i % 5 = 0 THEN 'configuration' END, "
                "now() - make_interval(secs => i * 26) "
                "FROM generate_series(1, 100000) AS i"
            ), {"job": job})
            await session.execute(text(
                "INSERT INTO notification_transitions "
                "(notification_id, subject, step, outcome, attempt, at) "
                "SELECT :job, 'job', 'resolve', 'processing', 0, "
                "now() - make_interval(secs => i * 130) "
                "FROM generate_series(1, 20000) AS i"
            ), {"job": job})
            await session.commit()
            await session.execute(text("ANALYZE notification_transitions"))
            await session.commit()

            db_now = (await session.execute(select(func.now()))).scalar_one()
            statement = window_counts(db_now - timedelta(minutes=60), db_now)
            compiled = statement.compile(
                dialect=session.bind.dialect,  # type: ignore[union-attr]
                compile_kwargs={"literal_binds": True},
            )
            plan = "\n".join(
                (await session.execute(text(f"EXPLAIN {compiled}"))).scalars()
            )
            total = (await session.execute(
                select(func.count()).select_from(NotificationTransition)
            )).scalar_one()
        assert total == 120_000, "the pair: the volume is really there"
        print(plan)
        assert "ix_transitions_channel_window" in plan, plan
        assert "Seq Scan on notification_transitions" not in plan, plan


# -----------------------------------------------------------------------------
# done-when (6): the same authorization as every /api/v1 route
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
        response = await client.get(
            _URL, headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert response.status_code == 200
        assert response.json()["channels"], "the pair: a real answer"

    async def test_health_itself_stays_open(
        self, client: AsyncClient, auth_enabled: None,
    ) -> None:
        assert (await client.get("/health")).status_code == 200


# The read model is imported so a rename of the module fails here first.
assert health.channel_health
