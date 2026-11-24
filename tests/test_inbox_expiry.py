# =============================================================================
# R-5 -- the inbox shows nothing whose job's expiry_at has passed
# =============================================================================
#
# Until R-1 the expiry cleanup deleted a finished job once its expiry_at
# passed, and its in_app delivery left the inbox and the unread badge
# with it (as in v4.0.1). R-1 keeps the job and its key until retention
# (tests/test_kept_until_retention.py), so the entry stayed in the inbox
# and the badge for the whole notification history -- and the entry
# carries no expiry_at, so the product cannot filter it out itself.
# R-5 reads, it does not delete: the page and the badge (every answer
# that carries it) leave out an entry whose job's expiry_at has passed;
# `read` on such an entry is 404 and `read-all` does not write it -- as
# v4.0.1 answered once the row was gone; the job, read by its key, is
# 200 until retention. deploy/INTEGRATION.md section 3a says so.
#
# Past means expiry_at < now() of the database, the start of the
# transaction: the page and its badge (one read-only transaction), a
# `read` and its badge (one transaction) see one moment. expiry_at equal
# to that moment is still shown, as the expiry sweep does not expire it
# yet (app/engine/processor.py expire_overdue_notifications).
#
# MUTATIONS these tests were written against (each turns one red):
#   MA  the condition off the page            -> TestThePage
#   MB  the condition off the badge           -> TestTheBadge, TestReadAll
#   MC  <= instead of <                       -> TestTheBoundary
#   MD  the condition off `read`              -> TestReadOne
#   ME  the condition off `read-all`          -> TestReadAll
#   MF  expiry_at >= now() without NULL       -> TestThePage (no expiry)
#   MG  the clock of Python, not the database -> TestTheBoundary
#   MH  the rule's subquery FOR UPDATE        -> TestNoLockOnTheJob
#
# THE RULE TAKES NO LOCK. read and read-all write deliveries the pipeline
# writes too; the rule only reads its job's expiry_at, which nobody
# writes after intake. Held on real sessions (TestNoLockOnTheJob): with
# the job row held FOR UPDATE -- as the expiry sweep and forgetting hold
# it (app/engine/service.py close_notifications, withdraw_recipient) --
# the badge, read and read-all end without waiting on it, observed in
# pg_stat_activity, not timed. No edge joins the lock graph, so no new
# cycle can be built.
# =============================================================================

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from httpx import AsyncClient
from sqlalchemy import select, text

from app.core.database import get_session_factory
from app.engine.constants import DeliveryChannel, DeliveryStatus, TargetType
from app.engine.models import NotificationDelivery
from app.engine.service import (
    create_notification,
    get_unread_count,
    list_recipient_deliveries,
    mark_all_read,
    mark_delivery_read,
    resolve_notification,
)
from tests.helpers import create_recipient, intake_fields

_INTEGRATION = "deploy/INTEGRATION.md"

# The expiries a seeded job may carry: none, ahead, behind.
NONE, AHEAD, PAST = "none", "ahead", "past"


async def _seed(*expiries: str) -> tuple[UUID, list[UUID], list[str]]:
    """A recipient with one SENT in_app delivery per entry of `expiries`,
    oldest first (sent_at one second apart), each job's expiry_at set
    as named -- past ones by time passing after acceptance, as intake
    refuses an expiry already behind. Committed for the API to see.
    Returns the recipient, the delivery ids and the jobs' keys, in seed
    order."""
    factory = get_session_factory()
    base = datetime.now(UTC) - timedelta(minutes=10)
    async with factory() as session:
        recipient = await create_recipient(session)
        deliveries: list[UUID] = []
        keys: list[str] = []
        jobs: list[tuple[UUID, str]] = []
        for index, expiry in enumerate(expiries):
            fields = intake_fields()
            job = await create_notification(
                session, **fields, type="unit_event_in_app",
                title=f"Title {index}", body=f"Body {index}",
                target_type=TargetType.USER, target_value=str(recipient.id),
                expiry_at=(
                    None if expiry == NONE
                    else datetime.now(UTC) + timedelta(hours=1)
                ),
            )
            (delivery,) = await resolve_notification(session, job)
            delivery.status = DeliveryStatus.SENT
            delivery.sent_at = base + timedelta(seconds=index)
            deliveries.append(delivery.id)
            keys.append(fields["idempotency_key"])
            jobs.append((job.id, expiry))
        await session.flush()
        for job_id, expiry in jobs:
            if expiry == PAST:
                await session.execute(text(
                    "UPDATE notifications SET expiry_at = now() - "
                    "interval '1 minute' WHERE id = :i"
                ), {"i": job_id})
        recipient_id = recipient.id
        await session.commit()
    return recipient_id, deliveries, keys


def _inbox(recipient: UUID) -> str:
    return f"/api/v1/recipients/{recipient}/inbox"


async def _ids(client: AsyncClient, recipient: UUID, **params: Any) -> list[str]:
    response = await client.get(_inbox(recipient), params=params)
    assert response.status_code == 200
    return [item["id"] for item in response.json()["items"]]


async def _read_at(delivery: UUID) -> datetime | None:
    async with get_session_factory()() as session:
        value: datetime | None = (await session.execute(
            select(NotificationDelivery.read_at)
            .where(NotificationDelivery.id == delivery)
        )).scalar_one()
        return value


class TestThePage:
    """MA, MF. REPEAT: expired entries between visible ones, walked by
    the cursor. EMPTY: every entry expired. The pair: no expiry and an
    expiry ahead are shown."""

    async def test_the_expired_entry_is_left_out(self, client: AsyncClient) -> None:
        recipient, (none, _past, ahead), _ = await _seed(NONE, PAST, AHEAD)
        assert await _ids(client, recipient) == [str(ahead), str(none)]

    async def test_the_cursor_walks_past_the_expired(
        self, client: AsyncClient,
    ) -> None:
        recipient, ids, _ = await _seed(NONE, PAST, AHEAD, PAST, NONE)
        seen: list[str] = []
        cursor: str | None = None
        for _ in range(5):
            params: dict[str, Any] = {"limit": 1}
            if cursor is not None:
                params["cursor"] = cursor
            response = await client.get(_inbox(recipient), params=params)
            body = response.json()
            seen += [item["id"] for item in body["items"]]
            cursor = body["next_cursor"]
            if cursor is None:
                break
        assert cursor is None
        assert seen == [str(ids[4]), str(ids[2]), str(ids[0])]

    async def test_all_expired_is_an_empty_page(self, client: AsyncClient) -> None:
        recipient, _, _ = await _seed(PAST, PAST)
        body = (await client.get(_inbox(recipient))).json()
        assert body["items"] == []
        assert body["next_cursor"] is None
        assert body["unread"] == 0


class TestTheBadge:
    """MB: every answer that carries the badge leaves the expired out."""

    async def test_the_page_and_the_counter(self, client: AsyncClient) -> None:
        recipient, _, _ = await _seed(NONE, PAST, AHEAD, PAST)
        page = (await client.get(_inbox(recipient))).json()
        assert page["unread"] == 2
        counter = await client.get(f"{_inbox(recipient)}/unread-count")
        assert counter.json() == {"unread": 2}


class TestTheJobStays:
    """Requirement 2: the job is read by its key -- 200, its delivery
    with it -- while the inbox no longer shows it (R-1 stays)."""

    async def test_by_key_200_the_inbox_without_it(
        self, client: AsyncClient,
    ) -> None:
        recipient, _, (key,) = await _seed(PAST)
        assert await _ids(client, recipient) == []
        summary = await client.get(
            "/api/v1/notifications/by-key", params={"key": key},
        )
        assert summary.status_code == 200
        assert summary.json()["job"] is not None
        rows = await client.get(
            "/api/v1/notifications/by-key/deliveries", params={"key": key},
        )
        assert rows.status_code == 200
        assert [
            (r["recipient_id"], r["channel"], r["status"])
            for r in rows.json()["items"]
        ] == [(str(recipient), "in_app", "sent")]


class TestReadOne:
    """MD: `read` on an expired entry is 404 not_found, as v4.0.1 once
    the row was gone, and writes nothing. The pair: a live one is 200."""

    async def test_expired_is_404_and_unwritten(self, client: AsyncClient) -> None:
        recipient, (live, past), _ = await _seed(NONE, PAST)
        response = await client.post(f"{_inbox(recipient)}/{past}/read")
        assert response.status_code == 404
        assert response.json()["error"]["class"] == "not_found"
        assert await _read_at(past) is None
        response = await client.post(f"{_inbox(recipient)}/{live}/read")
        assert response.status_code == 200
        assert response.json() == {"unread": 0}
        assert await _read_at(live) is not None


class TestReadAll:
    """ME, MB: `read-all` writes the live entries only; the expired keep
    read_at NULL (v4.0.1 had no row to write). EMPTY: all expired --
    nothing written."""

    async def test_live_read_expired_untouched(self, client: AsyncClient) -> None:
        recipient, (live, past), _ = await _seed(NONE, PAST)
        response = await client.post(f"{_inbox(recipient)}/read-all")
        assert response.status_code == 200
        assert response.json() == {"unread": 0}
        assert await _read_at(live) is not None
        assert await _read_at(past) is None

    async def test_all_expired_writes_nothing(self, client: AsyncClient) -> None:
        recipient, ids, _ = await _seed(PAST, PAST)
        response = await client.post(f"{_inbox(recipient)}/read-all")
        assert response.json() == {"unread": 0}
        assert [await _read_at(i) for i in ids] == [None, None]


class TestTheBoundary:
    """MC, MG: expiry_at equal to now() of the transaction is shown and
    counted -- the page and the badge built in that same transaction.
    One second behind it, it is not."""

    async def _in_one_transaction(self, offset: str) -> tuple[int, int]:
        recipient, _, _ = await _seed(AHEAD)
        async with get_session_factory()() as session:
            await session.execute(text(
                f"UPDATE notifications SET expiry_at = now() {offset} "
                "WHERE id = (SELECT notification_id FROM notification_deliveries "
                "WHERE recipient_id = :r)"
            ), {"r": recipient})
            # The row is the transaction's own: what the code reads next
            # compares it against the same now().
            assert (await session.execute(text(
                "SELECT expiry_at = now() FROM notifications WHERE id = (SELECT "
                "notification_id FROM notification_deliveries WHERE "
                "recipient_id = :r)"
            ), {"r": recipient})).scalar_one() is (offset == "")
            items, _ = await list_recipient_deliveries(
                session, recipient, channel_filter=DeliveryChannel.IN_APP,
            )
            unread = await get_unread_count(
                session, recipient, channel=DeliveryChannel.IN_APP,
            )
            await session.rollback()
        return len(items), unread

    async def test_equal_to_now_is_shown(self) -> None:
        assert await self._in_one_transaction("") == (1, 1)

    async def test_a_second_behind_is_not(self) -> None:
        assert await self._in_one_transaction("- interval '1 second'") == (0, 0)


class TestTheContract:
    """The contract and the code, both ways: section 3a says what the
    tests above hold."""

    def test_section_3a_says_it(self) -> None:
        with open(_INTEGRATION, encoding="utf-8") as handle:
            doc = handle.read()
        section = doc[doc.index("\n## 3a. "):doc.index("\n## 5. ")]
        lead = "**An inbox entry leaves with its job's `expiry_at`:**"
        assert lead in section
        paragraph = section[section.index(lead):]
        paragraph = paragraph[:paragraph.index("\n\n")]
        for named in ("`unread`", "`read-all`", "404", "by its key", "retention"):
            assert named in paragraph, named


class TestNoLockOnTheJob:
    """The rule reads the job row and locks nothing of it: with the job
    held FOR UPDATE by another transaction, the inbox's statements end
    and do not wait (pg_stat_activity, observed, not timed). The pair:
    a FOR UPDATE of the same row from the inbox's session does wait --
    the observation can see a wait."""

    async def _waits(self, work: Any) -> bool:
        recipient, (delivery,), _ = await _seed(AHEAD)
        factory = get_session_factory()
        async with factory() as holder, factory() as inbox:
            await holder.execute(text(
                "SELECT id FROM notifications WHERE id = (SELECT "
                "notification_id FROM notification_deliveries WHERE id = :d) "
                "FOR UPDATE"
            ), {"d": delivery})
            pid = (await inbox.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            task = asyncio.ensure_future(work(inbox, recipient, delivery))
            waited = False
            for _ in range(500):
                if task.done():
                    break
                async with factory() as observer:
                    kind = (await observer.execute(text(
                        "SELECT wait_event_type FROM pg_stat_activity "
                        "WHERE pid = :p AND datname = current_database()"
                    ), {"p": pid})).scalar_one_or_none()
                if kind == "Lock":
                    waited = True
                    break
                await asyncio.sleep(0.01)
            await holder.rollback()
            await task
            await inbox.rollback()
        return waited

    async def test_the_inbox_does_not_wait(self) -> None:
        async def inbox_work(session: Any, recipient: UUID, delivery: UUID) -> None:
            await get_unread_count(session, recipient, channel=DeliveryChannel.IN_APP)
            await list_recipient_deliveries(
                session, recipient, channel_filter=DeliveryChannel.IN_APP,
            )
            await mark_delivery_read(session, recipient, delivery)
            await mark_all_read(session, recipient, channel=DeliveryChannel.IN_APP)

        assert await self._waits(inbox_work) is False

    async def test_a_lock_of_the_job_would_be_seen(self) -> None:
        async def locks_the_job(session: Any, recipient: UUID, delivery: UUID) -> None:
            await session.execute(text(
                "SELECT id FROM notifications WHERE id = (SELECT "
                "notification_id FROM notification_deliveries WHERE id = :d) "
                "FOR UPDATE"
            ), {"d": delivery})

        assert await self._waits(locks_the_job) is True
