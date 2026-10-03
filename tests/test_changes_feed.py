# =============================================================================
# P2-3 Б1 -- what changed since a cursor (spec §7.5)
# =============================================================================
#
# The keys of the jobs whose journal rows came after the cursor, in the
# journal's (xact_id, id) order, never a row a running transaction can
# still precede; read only; never the letter.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the cursor on the row id alone (no xact_id, no xmin)
#                                  -> TestLateCommit
#   M2  the xmin bound dropped     -> TestLateCommit, TestCommitBetweenPages
#   M4  the page not cut before the next job's first row
#                                  -> TestPaging.test_interleaved_jobs_...
#   M5  a row per item instead of a job per item
#                                  -> TestPaging.test_a_key_once_on_a_page
#   M6  the job's status / title in the item
#                                  -> TestNoLetter
#   M7  expiry off                 -> TestExpiry
#   M8  a `seq:` cursor accepted   -> TestCursorRefusals
#   M9  the limit clamped          -> TestCursorRefusals
#   M10 the token dropped          -> TestAuth
#   M11 the index dropped          -> TestCost
#   M13 a write on the read path   -> TestReadOnly
# =============================================================================

import base64
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.changes import read_changes
from app.api.jobs import read_only_snapshot
from app.api.paging import (
    ChangesCursor,
    decode_changes_cursor,
    encode_changes_cursor,
    encode_cursor,
    encode_seq_cursor,
)
from app.core.config import settings
from app.core.constants import CHANGES_SCAN_ROWS
from app.core.database import Base, get_session_factory
from app.engine.changes import START, Position, changes_scan
from app.engine.constants import TargetType
from app.engine.models import Notification, NotificationTransition
from tests.helpers import notification_row_fields

_URL = "/api/v1/notifications/changes"
_TOKEN = "p2-3-changes-unit-test-token"
# A value no field, status or key ever contains by itself.
_LETTER = "zq7" + "LETTERCHG" + "xk4"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _new_job(**overrides: Any) -> Notification:
    fields: dict[str, Any] = {
        "type": "unit_event", "title": "T", "body": "B",
        "target_type": TargetType.USER, "target_value": str(uuid4()),
        **notification_row_fields(),
    }
    fields.update(overrides)
    return Notification(**fields)


async def _jobs(count: int) -> list[tuple[UUID, str]]:
    """`count` committed jobs WITHOUT journal rows: (id, key)."""
    made: list[tuple[UUID, str]] = []
    async with get_session_factory()() as session:
        for _ in range(count):
            job = _new_job()
            session.add(job)
            await session.flush()
            made.append((job.id, job.idempotency_key))
        await session.commit()
    return made


def _row(job: UUID) -> NotificationTransition:
    return NotificationTransition(
        notification_id=job, subject="job", step="resolve",
        outcome="processing", attempt=0,
    )


async def _commit_rows(*jobs: UUID) -> None:
    """One transaction writing one row per job, in the given order."""
    async with get_session_factory()() as session:
        for job in jobs:
            session.add(_row(job))
            await session.flush()
        await session.commit()


async def _page(client: AsyncClient, **params: Any) -> dict[str, Any]:
    response = await client.get(_URL, params=params)
    assert response.status_code == 200, response.text
    body: dict[str, Any] = response.json()
    return body


def _keys(body: dict[str, Any]) -> list[str]:
    return [item["idempotency_key"] for item in body["items"]]


async def _walk(
    client: AsyncClient, cursor: str | None = None, limit: int = 20,
) -> tuple[list[str], str]:
    seen: list[str] = []
    while True:
        params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        body = await _page(client, **params)
        cursor = body["next_cursor"]
        assert cursor is not None
        if not body["items"]:
            return seen, cursor
        seen.extend(_keys(body))


async def _xact_and_id(session: AsyncSession, row: NotificationTransition) -> str:
    xact = (await session.execute(
        text("SELECT pg_current_xact_id()::text"),
    )).scalar_one()
    return f"id={row.id} xact_id={xact}"


# -----------------------------------------------------------------------------
# done-when (1): a late commit is not lost -- two real sessions
# -----------------------------------------------------------------------------


class TestLateCommit:
    async def test_the_smaller_id_committed_later_still_arrives(
        self, client: AsyncClient,
    ) -> None:
        """A takes the smaller id, B the larger; B commits first and a
        page is read; A commits after it. A's change comes on the next
        read, before B's -- nothing is stepped over."""
        (job_a, key_a), (job_b, key_b) = await _jobs(2)
        factory = get_session_factory()
        session_a = factory()
        session_b = factory()
        try:
            row_a = _row(job_a)
            session_a.add(row_a)
            await session_a.flush()
            trace_a = await _xact_and_id(session_a, row_a)

            row_b = _row(job_b)
            session_b.add(row_b)
            await session_b.flush()
            trace_b = await _xact_and_id(session_b, row_b)
            assert row_a.id < row_b.id, "the pair: A holds the smaller id"
            await session_b.commit()

            first = await _page(client)
            print(f"A: {trace_a}; B: {trace_b}; B committed; page 1: "
                  f"{_keys(first)} at {decode_changes_cursor(first['next_cursor'])}")
            assert _keys(first) == [], "B is held back while A runs"

            await session_a.commit()
            second = await _page(client, cursor=first["next_cursor"])
            print(f"A committed; page 2: {_keys(second)}")
            assert _keys(second) == [key_a, key_b]
        finally:
            await session_a.close()
            await session_b.close()

    async def test_nothing_after_the_cursor_is_lost_over_a_walk(
        self, client: AsyncClient,
    ) -> None:
        """The same race, walked to the end: every key exactly once."""
        (job_a, key_a), (job_b, key_b), (job_c, key_c) = await _jobs(3)
        await _commit_rows(job_c)
        session_a = get_session_factory()()
        try:
            session_a.add(_row(job_a))
            await session_a.flush()
            await _commit_rows(job_b)
            seen, cursor = await _walk(client, limit=1)
            assert seen == [key_c]
            await session_a.commit()
            rest, _ = await _walk(client, cursor=cursor, limit=1)
            assert rest == [key_a, key_b]
        finally:
            await session_a.close()


# -----------------------------------------------------------------------------
# done-when (2): a commit while pages are read is not lost between them
# -----------------------------------------------------------------------------


class TestCommitBetweenPages:
    async def test_a_transaction_open_across_the_pages(
        self, client: AsyncClient,
    ) -> None:
        """E is committed; L begins (its xact_id below theirs); J1 and J2
        commit. Page one gives E; page two, with L still open, gives
        nothing; L commits; the walk goes on with L, J1, J2."""
        (early, k_early), (late, k_late), (j1, k1), (j2, k2) = await _jobs(4)
        await _commit_rows(early)
        session_late = get_session_factory()()
        try:
            session_late.add(_row(late))
            await session_late.flush()
            await _commit_rows(j1)
            await _commit_rows(j2)
            first = await _page(client, limit=1)
            assert _keys(first) == [k_early]
            held = await _page(client, limit=1, cursor=first["next_cursor"])
            assert _keys(held) == [], "J1 and J2 wait for L"
            await session_late.commit()
            rest, _ = await _walk(client, cursor=held["next_cursor"], limit=1)
            assert rest == [k_late, k1, k2]
        finally:
            await session_late.close()

    async def test_a_rolled_back_transaction_leaves_nothing(
        self, client: AsyncClient,
    ) -> None:
        (job_a, _), (job_b, key_b) = await _jobs(2)
        session_a = get_session_factory()()
        try:
            session_a.add(_row(job_a))
            await session_a.flush()
            await _commit_rows(job_b)
            await session_a.rollback()
        finally:
            await session_a.close()
        seen, _ = await _walk(client)
        assert seen == [key_b]


# -----------------------------------------------------------------------------
# done-when (3), (7): the cursor; paging and repeats
# -----------------------------------------------------------------------------


class TestPaging:
    async def test_an_empty_journal_answers_a_cursor(
        self, client: AsyncClient,
    ) -> None:
        body = await _page(client)
        assert body["items"] == []
        cursor = decode_changes_cursor(body["next_cursor"])
        assert cursor is not None
        assert (cursor.xact, cursor.ident) == (START.xact, START.ident)

    async def test_nothing_new_keeps_the_position_and_refreshes_the_time(
        self, client: AsyncClient,
    ) -> None:
        (job, key), = await _jobs(1)
        await _commit_rows(job)
        first = await _page(client)
        assert _keys(first) == [key]
        again = await _page(client, cursor=first["next_cursor"])
        assert again["items"] == []
        one = decode_changes_cursor(first["next_cursor"])
        two = decode_changes_cursor(again["next_cursor"])
        assert one is not None and two is not None
        assert (two.xact, two.ident) == (one.xact, one.ident)
        assert two.read_at >= one.read_at

    async def test_the_same_cursor_twice_is_the_same_page(
        self, client: AsyncClient,
    ) -> None:
        jobs = await _jobs(3)
        await _commit_rows(*(job for job, _ in jobs))
        one = await _page(client, limit=2)
        two = await _page(client, limit=2)
        assert _keys(one) == _keys(two) == [jobs[0][1], jobs[1][1]]
        a = decode_changes_cursor(one["next_cursor"])
        b = decode_changes_cursor(two["next_cursor"])
        assert a is not None and b is not None
        assert (a.xact, a.ident) == (b.xact, b.ident)

    async def test_a_key_once_on_a_page(self, client: AsyncClient) -> None:
        (job, key), = await _jobs(1)
        await _commit_rows(*([job] * 10))
        body = await _page(client)
        assert _keys(body) == [key]

    async def test_interleaved_jobs_are_cut_before_the_next_job(
        self, client: AsyncClient,
    ) -> None:
        """Rows J1, J2, J1, J3 at limit 2: page one is J1, J2 (the
        second J1 row with them), page two J3 -- none stepped over."""
        (j1, k1), (j2, k2), (j3, k3) = await _jobs(3)
        await _commit_rows(j1, j2, j1, j3)
        first = await _page(client, limit=2)
        assert _keys(first) == [k1, k2]
        second = await _page(client, limit=2, cursor=first["next_cursor"])
        assert _keys(second) == [k3]

    async def test_a_job_that_changes_again_comes_again(
        self, client: AsyncClient,
    ) -> None:
        (job, key), = await _jobs(1)
        await _commit_rows(job)
        first = await _page(client)
        await _commit_rows(job)
        second = await _page(client, cursor=first["next_cursor"])
        assert _keys(first) == _keys(second) == [key]

    async def test_a_broadcast_longer_than_the_scan_spans_two_pages(
        self, client: AsyncClient,
    ) -> None:
        """One transaction writing more rows than one page scans: the
        key heads two pages, then the next job follows."""
        (big, key_big), (small, key_small) = await _jobs(2)
        async with get_session_factory()() as session:
            await session.execute(text(
                "INSERT INTO notification_transitions "
                "(notification_id, subject, step, outcome, attempt) "
                "SELECT :job, 'job', 'resolve', 'processing', 0 "
                "FROM generate_series(1, :n)"
            ), {"job": big, "n": CHANGES_SCAN_ROWS + 1000})
            await session.commit()
        await _commit_rows(small)
        first = await _page(client)
        assert _keys(first) == [key_big]
        second = await _page(client, cursor=first["next_cursor"])
        assert _keys(second) == [key_big, key_small]


# -----------------------------------------------------------------------------
# done-when (4): never the letter
# -----------------------------------------------------------------------------


class TestNoLetter:
    async def test_the_letter_is_not_in_the_answer(
        self, client: AsyncClient,
    ) -> None:
        async with get_session_factory()() as session:
            job = _new_job(
                title=_LETTER, body=_LETTER,
                action_data={"var": _LETTER},
            )
            session.add(job)
            await session.flush()
            key = job.idempotency_key
            session.add(_row(job.id))
            await session.commit()
        response = await client.get(_URL)
        assert response.status_code == 200
        assert _keys(response.json()) == [key], "the pair: the key is there"
        assert key
        assert _LETTER not in response.text
        assert set(response.json()["items"][0]) == {"idempotency_key"}


# -----------------------------------------------------------------------------
# done-when (5), (6): refusals
# -----------------------------------------------------------------------------


def _raw(text_value: str) -> str:
    return base64.urlsafe_b64encode(text_value.encode()).decode()


class TestCursorRefusals:
    @pytest.mark.parametrize("cursor", [
        encode_seq_cursor(5),
        encode_cursor((datetime.now(UTC), uuid4())),
        "not-base64!",
        _raw("chg:1:2"),
        _raw("chg:x:2:2026-10-03T00:00:00+00:00"),
        _raw("chg:-1:2:2026-10-03T00:00:00+00:00"),
        _raw("chg:1:2:2026-10-03T00:00:00"),
        _raw("seq:1:2:2026-10-03T00:00:00+00:00"),
    ])
    async def test_a_malformed_or_foreign_cursor_is_422(
        self, client: AsyncClient, cursor: str,
    ) -> None:
        response = await client.get(_URL, params={"cursor": cursor})
        assert response.status_code == 422
        assert set(response.json()) == {"error"}
        assert response.json()["error"]["class"] == "validation"

    async def test_a_transaction_not_yet_assigned_is_422(
        self, client: AsyncClient,
    ) -> None:
        cursor = encode_changes_cursor(
            ChangesCursor(2**62, 1, datetime.now(UTC)),
        )
        response = await client.get(_URL, params={"cursor": cursor})
        assert response.status_code == 422
        assert "future" in response.json()["error"]["message"]

    async def test_a_read_time_in_the_future_is_422(
        self, client: AsyncClient,
    ) -> None:
        cursor = encode_changes_cursor(
            ChangesCursor(0, 0, datetime.now(UTC) + timedelta(hours=1)),
        )
        response = await client.get(_URL, params={"cursor": cursor})
        assert response.status_code == 422
        assert response.json()["error"]["class"] == "validation"

    @pytest.mark.parametrize("limit", [0, 101])
    async def test_a_limit_out_of_bounds_is_refused(
        self, client: AsyncClient, limit: int,
    ) -> None:
        response = await client.get(_URL, params={"limit": limit})
        assert response.status_code == 422
        assert response.json()["error"]["class"] == "validation"


class TestExpiry:
    async def test_a_cursor_older_than_retention_is_410(
        self, client: AsyncClient,
    ) -> None:
        old = datetime.now(UTC) - timedelta(
            days=settings.notification_retention_days + 1,
        )
        assert settings.notification_retention_days > 0, "the pair"
        response = await client.get(_URL, params={
            "cursor": encode_changes_cursor(ChangesCursor(0, 0, old)),
        })
        assert response.status_code == 410
        assert set(response.json()) == {"error"}
        assert response.json()["error"]["class"] == "cursor_expired"

    async def test_a_cursor_just_inside_retention_is_read(
        self, client: AsyncClient,
    ) -> None:
        recent = datetime.now(UTC) - timedelta(
            days=settings.notification_retention_days - 1,
        )
        body = await _page(
            client, cursor=encode_changes_cursor(ChangesCursor(0, 0, recent)),
        )
        assert body["items"] == []

    async def test_retention_off_never_expires(
        self, client: AsyncClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "notification_retention_days", 0)
        ancient = datetime(2020, 1, 1, tzinfo=UTC)
        body = await _page(
            client, cursor=encode_changes_cursor(ChangesCursor(0, 0, ancient)),
        )
        assert body["items"] == []


# -----------------------------------------------------------------------------
# done-when (8): the read does not write
# -----------------------------------------------------------------------------


async def _row_counts(session: AsyncSession) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in Base.metadata.sorted_tables:
        counts[table.name] = (
            await session.execute(select(func.count()).select_from(table))
        ).scalar_one()
    return counts


class TestReadOnly:
    def test_the_route_runs_in_the_read_only_snapshot(self) -> None:
        from app.main import app

        for route in app.routes:
            for candidate in getattr(
                getattr(route, "original_router", None), "routes", [],
            ):
                if getattr(candidate, "endpoint", None) is read_changes:
                    calls = {d.call for d in candidate.dependant.dependencies}
                    assert read_only_snapshot in calls
                    return
        raise AssertionError("the route was not found")

    async def test_the_snapshot_refuses_a_write(self) -> None:
        generator = read_only_snapshot()
        session = await generator.__anext__()
        try:
            with pytest.raises(Exception, match="read-only"):
                await session.execute(text(
                    "CREATE TEMP TABLE zq7_write_probe (x int)"
                ))
        finally:
            await generator.aclose()

    async def test_a_walk_changes_no_row(
        self, client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        jobs = await _jobs(3)
        await _commit_rows(*(job for job, _ in jobs))
        before = await _row_counts(db_session)
        assert before["notification_transitions"] == 3, "the pair"
        await db_session.rollback()
        seen, _ = await _walk(client, limit=1)
        assert len(seen) == 3
        assert await _row_counts(db_session) == before


# -----------------------------------------------------------------------------
# done-when (9): the same authorization
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
        assert response.json()["next_cursor"], "the pair: a real answer"


# -----------------------------------------------------------------------------
# done-when (10): a page is an index scan at volume
# -----------------------------------------------------------------------------


class TestCost:
    async def test_a_page_reads_the_index_on_100k_rows(self) -> None:
        jobs = await _jobs(20)
        async with get_session_factory()() as session:
            for job, _ in jobs:
                # One transaction per job: twenty xact_ids, 5 000 rows each.
                await session.execute(text(
                    "INSERT INTO notification_transitions "
                    "(notification_id, subject, step, outcome, attempt) "
                    "SELECT :job, 'job', 'resolve', 'processing', 0 "
                    "FROM generate_series(1, 5000)"
                ), {"job": job})
                await session.commit()
            await session.execute(text("ANALYZE notification_transitions"))
            await session.commit()
            middle = (await session.execute(text(
                "SELECT xact_id::text, id FROM notification_transitions "
                "ORDER BY xact_id, id OFFSET 50000 LIMIT 1"
            ))).one()
            statement = changes_scan(Position(int(middle[0]), middle[1]))
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
        assert total == 100_000, "the pair: the volume is really there"
        print(plan)
        assert "ix_transitions_changes" in plan, plan
        assert "Seq Scan" not in plan, plan
        assert "Sort" not in plan, plan
