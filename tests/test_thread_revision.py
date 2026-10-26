# =============================================================================
# RV-1 -- a thread's revision: +1 on every write of its row
# =============================================================================
#
# What is held here, against the code, the schema and deploy/INTEGRATION.md
# (section 13 -- the row; section 14 -- "The thread's revision"):
#   1 the writers -- every writer of a threads row raises the revision by
#     exactly one per write, the raw-SQL path included; a conditional
#     UPDATE that matches no row, and an ORM flush with nothing to write,
#     raise nothing (the pairs); a new thread is 1;
#   2 the answers -- every path that answers a thread carries the
#     revision as the database holds it right after the call;
#   3 a race on real sessions: two writes of one thread take n+1 and n+2
#     in the order they commit -- the wait observed by pg_blocking_pids;
#   4 velo's scenario -- answers applied in reverse order by "keep it if
#     the revision is newer" land on comms' assignee;
#   5 ix_threads_client -- measured by the rule fixed before the
#     measurement;
#   6 the contract.
# The trigger's existence: tests/test_schema_objects.py; the migration's
# up / down / up: tests/test_migration_0035.py.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the trigger dropped -> TestWriters, the schema test
#   M2  the function moving nothing (NEW.revision := OLD.revision) ->
#       TestWriters, the schema test
#   M3  no eager_defaults on the mapper -> TestAnswers (status, retag,
#       create)
#   M4  no FetchedValue on the column -> TestAnswers (status, retag)
#   M5  assign answering the object it was given -> TestAnswers assign
#   M6  claim without its refresh -> TestAnswers claim
#   M7  the down step leaving the function -> test_migration_0035
#   M8  `revision` out of _thread_out or out of the contract ->
#       TestAnswers, TestTheContract
# =============================================================================

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import event, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_engine, get_session_factory
from app.core.exceptions import ClaimTakenError
from app.messaging.constants import OperatorKind, ThreadKind, ThreadStatus
from app.messaging.models import Thread
from app.messaging.operators import (
    assign_thread,
    claim_thread,
    release_thread,
    retag_thread,
)
from app.messaging.read_state import (
    participant_unread_summary,
    unread_counts_for_participant,
)
from app.messaging.status import auto_close_idle_threads_batch, set_status
from app.messaging.threads import (
    _select_by_dedup_key,
    create_or_get_thread,
    create_or_get_thread_detailed,
    post_message,
)
from app.notifier import consume_close_notifications
from tests.helpers import (
    create_recipient,
    create_section,
    intake_fields,
    next_rv1_telegram_id,
)

_REPO = Path(__file__).resolve().parent.parent
_BOUND = 10.0
_POLL = 0.02


# ---------------------------------------------------------------------------
# Building blocks -- every row committed
# ---------------------------------------------------------------------------


async def _recipient() -> UUID:
    rid = uuid4()
    async with get_session_factory()() as s:
        await create_recipient(
            s, recipient_id=rid, telegram_id=next_rv1_telegram_id()
        )
        await s.commit()
    return rid


async def _section() -> UUID:
    async with get_session_factory()() as s:
        section = await create_section(s, key=f"rv1-{uuid4().hex[:8]}")
        await s.commit()
        return section.id


async def _ticket(
    customer: UUID | None = None, *, holder: UUID | None = None
) -> UUID:
    customer = customer or await _recipient()
    async with get_session_factory()() as s:
        thread = await create_or_get_thread(
            s, **intake_fields(), client=customer,
            operator_kind=OperatorKind.SECTION, operator_value=await _section(),
            kind=ThreadKind.TICKET, subject_type="practice",
            subject_id=uuid4().hex,
        )
        if holder is not None:
            assert await claim_thread(s, thread_id=thread.id, operator=holder)
        await s.commit()
        return thread.id


async def _revision(tid: UUID) -> int:
    async with get_session_factory()() as s:
        value = await s.scalar(
            text("SELECT revision FROM threads WHERE id = :id"), {"id": tid}
        )
    assert isinstance(value, int)
    return value


async def _write(work: Callable[[AsyncSession], Awaitable[Any]]) -> Any:
    async with get_session_factory()() as s:
        result = await work(s)
        await s.commit()
        return result


# ---------------------------------------------------------------------------
# 1 -- the writers
# ---------------------------------------------------------------------------


class TestWriters:
    async def test_a_new_thread_is_one(self) -> None:
        assert await _revision(await _ticket()) == 1

    async def test_claim(self) -> None:
        tid, operator = await _ticket(), await _recipient()
        await _write(lambda s: claim_thread(s, thread_id=tid, operator=operator))
        assert await _revision(tid) == 2
        # the pair: a claim of a held thread matches no row
        other = await _recipient()
        assert not await _write(
            lambda s: claim_thread(s, thread_id=tid, operator=other)
        )
        assert await _revision(tid) == 2

    async def test_assign_and_release(self) -> None:
        tid, a, b = await _ticket(), await _recipient(), await _recipient()

        async def assign(s: AsyncSession, who: UUID) -> Any:
            return await assign_thread(
                s, thread=await s.get(Thread, tid), assignee=who
            )

        await _write(lambda s: assign(s, a))
        assert await _revision(tid) == 2
        await _write(lambda s: assign(s, a))  # already a's: nothing written
        assert await _revision(tid) == 2
        async with get_session_factory()() as s:
            await release_thread(s, thread=await s.get(Thread, tid))
            await s.commit()
        assert await _revision(tid) == 3
        async with get_session_factory()() as s:  # in the pool: nothing
            await release_thread(s, thread=await s.get(Thread, tid))
            await s.commit()
        assert await _revision(tid) == 3
        # the pair: an assign decided on a stale read matches no row
        async with get_session_factory()() as stale:
            seen = await stale.get(Thread, tid)
            await _write(lambda s: assign(s, b))
            assert await _revision(tid) == 4
            with pytest.raises(ClaimTakenError):
                await assign_thread(stale, thread=seen, assignee=a)
            await stale.rollback()
        assert await _revision(tid) == 4

    async def test_retag(self) -> None:
        tid = await _ticket()
        moved = await _section()
        await _write(lambda s: retag_thread(s, thread_id=tid, section=moved))
        assert await _revision(tid) == 2

    async def test_a_message_and_a_client_reopen_are_one_write_each(
        self,
    ) -> None:
        customer = await _recipient()
        tid = await _ticket(customer)

        async def post(s: AsyncSession) -> Any:
            return await post_message(
                s, thread_id=tid, sender=customer, body="x",
                idempotency_key=f"rv1:{uuid4()}", fingerprint="f" * 64,
            )

        await _write(post)
        assert await _revision(tid) == 2
        await _write(lambda s: set_status(
            s, thread_id=tid, target=ThreadStatus.CLOSED))
        assert await _revision(tid) == 3
        await _write(post)  # activity AND reopen: one UPDATE
        assert await _revision(tid) == 4

    async def test_status(self) -> None:
        tid = await _ticket()
        for target, expected in (
            (ThreadStatus.RESOLVED, 2),
            (ThreadStatus.RESOLVED, 2),  # X -> X: nothing written
            (ThreadStatus.CLOSED, 3),    # closed, the notice marked
            (ThreadStatus.OPEN, 4),      # reopen
        ):
            await _write(lambda s, t=target: set_status(  # type: ignore[misc]
                s, thread_id=tid, target=t))
            assert await _revision(tid) == expected, target

    async def test_auto_close(self) -> None:
        now = datetime.now(UTC)
        idle, busy = await _ticket(), await _ticket()
        async with get_session_factory()() as s:
            await s.execute(
                update(Thread).where(Thread.id == idle)
                .values(created_at=now - timedelta(days=40))
            )
            await s.commit()
        before = await _revision(idle)
        closed = await _write(lambda s: auto_close_idle_threads_batch(
            s, cutoff=now - timedelta(days=30), when=now, limit=1000))
        assert closed == 1
        assert await _revision(idle) == before + 1
        assert await _revision(busy) == 1  # the pair: not touched

    async def test_the_close_notice_pass(self) -> None:
        tid = await _ticket()
        await _write(lambda s: set_status(
            s, thread_id=tid, target=ThreadStatus.CLOSED))
        assert await _revision(tid) == 2
        assert await consume_close_notifications() == 1
        assert await _revision(tid) == 3  # the mark cleared: a write
        assert await consume_close_notifications() == 0
        assert await _revision(tid) == 3  # the pair: nothing to clear

    async def test_raw_sql_and_the_same_values(self) -> None:
        tid = await _ticket()
        async with get_session_factory()() as s:
            await s.execute(
                text("UPDATE threads SET revision = 100 WHERE id = :id"),
                {"id": tid},
            )
            await s.commit()
        assert await _revision(tid) == 2  # whatever the writer put there
        async with get_session_factory()() as s:
            await s.execute(
                text("UPDATE threads SET status = status WHERE id = :id"),
                {"id": tid},
            )
            await s.commit()
        assert await _revision(tid) == 3  # the same values: a write
        async with get_session_factory()() as s:
            await s.execute(
                text("UPDATE threads SET status = status WHERE id = :id"),
                {"id": uuid4()},
            )
            await s.commit()
        assert await _revision(tid) == 3  # no row matched: nothing

    async def test_the_check(self) -> None:
        customer = await _recipient()

        async def insert(revision: int) -> None:
            async with get_session_factory()() as s:
                await s.execute(text(
                    "INSERT INTO threads (id, client, operator_kind, "
                    "operator_value, kind, status, created_at, "
                    "idempotency_key, fingerprint, revision) VALUES "
                    "(gen_random_uuid(), :c, 'section', gen_random_uuid(), "
                    "'ticket', 'open', now(), :k, repeat('f', 64), :r)"
                ), {"c": customer, "k": f"rv1:{uuid4()}", "r": revision})
                await s.commit()

        with pytest.raises(IntegrityError, match="ck_threads_revision_positive"):
            await insert(0)
        await insert(1)  # the pair


# ---------------------------------------------------------------------------
# 2 -- the answers
# ---------------------------------------------------------------------------


def _body(customer: UUID, master: UUID) -> bytes:
    return json.dumps({
        "client": str(customer), "operator_kind": "user",
        "operator_value": str(master), "kind": "dm",
    }).encode()


async def _create(
    client: AsyncClient, body: bytes, key: str
) -> dict[str, Any]:
    resp = await client.post(
        "/api/v1/threads", content=body,
        headers={"Idempotency-Key": key, "Content-Type": "application/json"},
    )
    assert resp.status_code == 200, resp.text
    payload: dict[str, Any] = resp.json()
    return payload


class TestAnswers:
    """Each path that answers a thread: the revision in the answer is
    the revision in the database right after the call."""

    async def test_create_inserted_found_and_replayed(
        self, bare_client: AsyncClient
    ) -> None:
        customer, master = await _recipient(), await _recipient()
        body = _body(customer, master)
        inserted = await _create(bare_client, body, f"rv1:{uuid4()}")
        tid = UUID(inserted["id"])
        assert inserted["created"] is True
        assert inserted["revision"] == await _revision(tid) == 1
        # the thread moves on, then is answered again two ways
        await _write(lambda s: set_status(
            s, thread_id=tid, target=ThreadStatus.RESOLVED))
        found = await _create(bare_client, body, f"rv1:{uuid4()}")
        assert found["created"] is False
        assert found["revision"] == await _revision(tid) == 2
        replayed = await _create(bare_client, body, f"rv1:{uuid4()}")
        assert replayed["revision"] == 2

    async def test_create_the_winner_of_an_insert_race(
        self, bare_client: AsyncClient
    ) -> None:
        """Another request with this key holds the insert; this one waits
        on the key's index, loses, and answers the winner's thread."""
        customer, master = await _recipient(), await _recipient()
        body, key = _body(customer, master), f"rv1:{uuid4()}"
        fingerprint = hashlib.sha256(
            b"POST\n/api/v1/threads?\n" + body
        ).hexdigest()
        async with get_session_factory()() as early:
            thread, _ = await create_or_get_thread_detailed(
                early, client=customer, operator_kind=OperatorKind.USER,
                operator_value=master, kind=ThreadKind.DM,
                idempotency_key=key, fingerprint=fingerprint,
            )
            holder = await _pid(early)
            task = asyncio.create_task(_create(bare_client, body, key))
            await _until_waiting(holder, task)
            await early.commit()
        answer = await asyncio.wait_for(task, _BOUND)
        assert answer["id"] == str(thread.id)
        assert answer["revision"] == await _revision(thread.id) == 1

    async def test_both_lists(self, client: AsyncClient) -> None:
        customer, operator = await _recipient(), await _recipient()
        tid = await _ticket(customer, holder=operator)
        await _write(lambda s: set_status(
            s, thread_id=tid, target=ThreadStatus.RESOLVED))
        expected = await _revision(tid)
        assert expected == 3
        for reader in ({"client": str(customer)}, {"operator": str(operator)}):
            rows = (await client.get("/api/v1/threads", params=reader)).json()
            (row,) = [r for r in rows["items"] if r["id"] == str(tid)]
            assert row["revision"] == expected

    @pytest.mark.parametrize(
        "verb", ["claim", "assign", "release", "status", "retag"]
    )
    async def test_the_verbs(self, client: AsyncClient, verb: str) -> None:
        operator = await _recipient()
        holder = None if verb in ("claim", "assign") else operator
        tid = await _ticket(holder=holder)
        before = await _revision(tid)
        body: dict[str, Any] = {"operator": str(operator)}
        if verb == "assign":
            body["assignee"] = str(operator)
        if verb == "status":
            body["status"] = "resolved"
        if verb == "retag":
            body["section"] = str(await _section())
        resp = await client.post(f"/api/v1/threads/{tid}/{verb}", json=body)
        assert resp.status_code == 200, resp.text
        answer = resp.json()["thread"] if verb == "claim" else resp.json()
        assert answer["revision"] == await _revision(tid) == before + 1


# ---------------------------------------------------------------------------
# 3 -- a race on real sessions
# ---------------------------------------------------------------------------


async def _pid(session: AsyncSession) -> int:
    pid = await session.scalar(text("SELECT pg_backend_pid()"))
    assert isinstance(pid, int)
    return pid


async def _waiting_on(holder: int) -> bool:
    async with get_session_factory()() as s:
        waiting = await s.scalar(
            text(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname = current_database() "
                "AND CAST(:holder AS integer) = ANY(pg_blocking_pids(pid))"
            ),
            {"holder": holder},
        )
    return bool(waiting)


async def _until_waiting(holder: int, task: asyncio.Task[Any]) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _BOUND
    while not await _waiting_on(holder):
        assert not task.done(), (
            f"the second call ended without waiting: {task.result()!r}"
        )
        assert loop.time() < deadline, "the second call never waited"
        await asyncio.sleep(_POLL)


class TestRace:
    async def test_two_writes_take_the_next_two_numbers_in_commit_order(
        self,
    ) -> None:
        operator = await _recipient()
        tid = await _ticket()
        n = await _revision(tid)
        factory = get_session_factory()
        async with factory() as late, factory() as early:
            seen = await late.get(Thread, tid)
            first = await set_status(
                early, thread_id=tid, target=ThreadStatus.RESOLVED
            )
            holder = await _pid(early)

            async def second() -> Thread:
                assert seen is not None
                return await assign_thread(late, thread=seen, assignee=operator)

            task = asyncio.create_task(second())
            await _until_waiting(holder, task)
            await early.commit()
            assigned = await asyncio.wait_for(task, _BOUND)
            await late.commit()
        assert first.revision == n + 1
        assert assigned.revision == n + 2
        assert await _revision(tid) == n + 2


# ---------------------------------------------------------------------------
# 4 -- velo's scenario
# ---------------------------------------------------------------------------


def _keep_if_newer(copy: dict[str, Any], answer: dict[str, Any]) -> None:
    """The product's rule (section 14): keep the answer only if its
    revision is greater than the one kept."""
    if answer["revision"] > copy.get("revision", 0):
        copy.clear()
        copy.update(answer)


class TestVelosScenario:
    @pytest.mark.parametrize("second", ["assign", "release"])
    async def test_answers_in_reverse_order(
        self, client: AsyncClient, second: str
    ) -> None:
        a, b = await _recipient(), await _recipient()
        tid = await _ticket()
        first = await client.post(
            f"/api/v1/threads/{tid}/assign",
            json={"operator": str(a), "assignee": str(a)},
        )
        body = {"operator": str(a)}
        if second == "assign":
            body["assignee"] = str(b)
        later = await client.post(f"/api/v1/threads/{tid}/{second}", json=body)
        assert first.status_code == later.status_code == 200
        assert later.json()["revision"] > first.json()["revision"]

        copy: dict[str, Any] = {}
        for answer in (later.json(), first.json()):  # arrived reversed
            _keep_if_newer(copy, answer)
        async with get_session_factory()() as s:
            row = await s.get(Thread, tid)
            assert row is not None
            truth = str(row.assignee) if row.assignee else None
        assert copy["assignee"] == truth
        assert truth == (str(b) if second == "assign" else None)


# ---------------------------------------------------------------------------
# 5 -- ix_threads_client, by the rule fixed before the measurement
# ---------------------------------------------------------------------------

# THE RULE (RV-1 gate, written before the measurement, not moved after):
# ix_threads_client goes only if, for EACH of the five queries that look
# threads up by client --
#   the DM dedup lookup, the subject dedup lookup, the participant's
#   unread summary, the participant's unread counts, and the
#   foreign-key check of threads.client --
#   (a) the GENERIC plan without the index has no Seq Scan on threads,
#       the client predicate served by ix_threads_client_activity or a
#       dedup unique index; and
#   (b) EXPLAIN (ANALYZE, BUFFERS) without the index reads at most 1.25
#       times the buffers of the same query with it, on the same data.
# WHY 1.25: what dropping buys is one index entry per new thread (client
# is never updated, its updates are HOT) and the disk; the summary is
# read on every badge render, and losing more than a quarter of its
# reads costs more than the index.
_RATIO = 1.25
_HEAVY_THREADS = 2000
_OTHER_THREADS = 38_000
_OTHERS = 19
_FK_CHECK = (
    'SELECT 1 FROM ONLY "public"."threads" x WHERE $1 '
    'OPERATOR(pg_catalog.=) "client" FOR KEY SHARE OF x'
)


async def _captured(run: Callable[[], Awaitable[Any]]) -> list[tuple[str, Any]]:
    seen: list[tuple[str, Any]] = []

    def record(conn: Any, cursor: Any, statement: str, parameters: Any,
               *rest: Any) -> None:
        seen.append((statement, parameters))

    engine = get_engine().sync_engine
    event.listen(engine, "before_cursor_execute", record)
    try:
        await run()
    finally:
        event.remove(engine, "before_cursor_execute", record)
    return seen


def _of_threads(captured: list[tuple[str, Any]]) -> tuple[str, Any]:
    (found,) = [(s, p) for s, p in captured if "threads" in s and "$" in s]
    return found


async def _measure(
    statement: str, parameters: Any, *, without: bool
) -> tuple[str, int]:
    """The generic plan and the buffers of one query, with the index or
    without it -- the index dropped inside the transaction, rolled back."""
    async with get_session_factory()() as s:
        raw = await s.connection()
        if without:
            await raw.exec_driver_sql("DROP INDEX ix_threads_client")
        await raw.exec_driver_sql(
            "CREATE FUNCTION pg_temp.generic_plan(q text) RETURNS SETOF text "
            "LANGUAGE plpgsql AS $f$ BEGIN RETURN QUERY EXECUTE "
            "'EXPLAIN (GENERIC_PLAN) ' || q; END $f$"
        )
        generic = "\n".join(
            (await raw.execute(
                text("SELECT * FROM pg_temp.generic_plan(:q)"),
                {"q": statement},
            )).scalars()
        )
        plan: Any = (await raw.exec_driver_sql(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement, parameters,
        )).scalar_one()
        await s.rollback()
    top = plan[0]["Plan"]
    print("ANALYZED", " / ".join(re.findall(
        r'"Node Type": "([^"]+)"', json.dumps(plan))))
    return generic, int(top["Shared Hit Blocks"]) + int(
        top.get("Shared Read Blocks", 0)
    )


@pytest.mark.usefixtures("pristine_tables")
class TestTheClientIndex:
    async def _seed(self) -> tuple[UUID, UUID, UUID, list[UUID]]:
        heavy, operator = await _recipient(), await _recipient()
        others = [await _recipient() for _ in range(_OTHERS)]
        section = await _section()
        insert = (
            "INSERT INTO threads (id, client, operator_kind, operator_value, "
            "kind, status, last_message_at, created_at, idempotency_key, "
            "fingerprint) SELECT gen_random_uuid(), :client, 'user', "
            "gen_random_uuid(), 'dm', 'open', now() - g * interval '1 min', "
            "now() - interval '1 year', :tag || g, repeat('f', 64) "
            "FROM generate_series(1, :n) AS g"
        )
        async with get_session_factory()() as s:
            await s.execute(text(insert), {
                "client": heavy, "tag": "h:", "n": _HEAVY_THREADS - 200})
            for n, other in enumerate(others):
                await s.execute(text(insert), {
                    "client": other, "tag": f"o{n}:",
                    "n": _OTHER_THREADS // _OTHERS})
            # 200 tickets of the heavy client in one section, with a
            # subject each -- the subject dedup's population.
            await s.execute(text(
                "INSERT INTO threads (id, client, operator_kind, "
                "operator_value, kind, status, created_at, subject_type, "
                "subject_id, idempotency_key, fingerprint, assignee, "
                "assigned_at) SELECT gen_random_uuid(), :client, 'section', "
                ":section, 'ticket', 'open', now() - interval '1 year', "
                "'practice', 'p' || g, 's:' || g, repeat('f', 64), :op, now() "
                "FROM generate_series(1, 200) AS g"
            ), {"client": heavy, "section": section, "op": operator})
            # The heavy client's newest 100 threads: 30 messages each,
            # from the other side.
            await s.execute(text(
                "INSERT INTO messages (id, thread_id, sender, body, "
                "idempotency_key, fingerprint, created_at) "
                "SELECT gen_random_uuid(), t.id, :op, 'm', "
                "'seed:' || t.id || ':' || g, repeat('f', 64), "
                "now() - g * interval '1 s' "
                "FROM (SELECT id FROM threads WHERE client = :client "
                "      ORDER BY last_message_at DESC NULLS LAST LIMIT 100) t, "
                "generate_series(1, 30) AS g"
            ), {"client": heavy, "op": operator})
            dm_partner = await s.scalar(text(
                "SELECT operator_value FROM threads WHERE client = :c "
                "AND kind = 'dm' LIMIT 1"
            ), {"c": heavy})
            page = list(await s.scalars(text(
                "SELECT id FROM threads WHERE client = :c "
                "ORDER BY last_message_at DESC NULLS LAST LIMIT 100"
            ), {"c": heavy}))
            await s.commit()
        async with get_engine().connect() as connection:
            auto = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await auto.execute(text("SET default_statistics_target = 1000"))
            await auto.execute(text("ANALYZE"))
        return heavy, section, dm_partner, page

    async def test_the_rule(self) -> None:
        heavy, section, partner, page = await self._seed()
        factory = get_session_factory()

        async def dm() -> None:
            async with factory() as s:
                assert await _select_by_dedup_key(
                    s, client=heavy, operator_kind=OperatorKind.USER,
                    operator_value=partner, subject_type=None, subject_id=None,
                ) is not None

        async def subject() -> None:
            async with factory() as s:
                assert await _select_by_dedup_key(
                    s, client=heavy, operator_kind=OperatorKind.SECTION,
                    operator_value=section, subject_type="practice",
                    subject_id="p7",
                ) is not None

        async def summary() -> None:
            async with factory() as s:
                await participant_unread_summary(s, participant=heavy)

        async def counts() -> None:
            async with factory() as s:
                await unread_counts_for_participant(
                    s, participant=heavy, thread_ids=page
                )

        queries = {
            name: _of_threads(await _captured(run))
            for name, run in (
                ("dedup dm", dm), ("dedup subject", subject),
                ("unread summary", summary), ("unread counts", counts),
            )
        }
        queries["fk check"] = (_FK_CHECK, (heavy,))

        verdicts: dict[str, bool] = {}
        for name, (statement, parameters) in queries.items():
            generic_with, with_index = await _measure(
                statement, parameters, without=False
            )
            generic, without = await _measure(statement, parameters, without=True)
            # generic: a parameter left in the plan, its value never bound
            assert re.search(r"\$\d", generic), generic
            served = re.search(
                r"ix_threads_client_activity|uq_threads_dedup_", generic
            )
            seq = re.search(r"Seq Scan on threads", generic)
            ok = bool(served) and not seq and without <= _RATIO * with_index
            verdicts[name] = ok
            print(f"\nINDEX {name}: with {with_index}, without {without}, "
                  f"served={bool(served)}, seq={bool(seq)} -> {ok}\n"
                  f"WITH:\n{generic_with}\nWITHOUT:\n{generic}")
        print("VERDICTS", verdicts)
        assert set(verdicts) == {
            "dedup dm", "dedup subject", "unread summary", "unread counts",
            "fk check",
        }
        # THE DECISION FOLLOWS THE RULE, and the schema follows the
        # decision: the index is in the schema exactly while some query
        # fails the rule. Measured for RV-1 (3.12.0): the unread summary
        # fails (a) -- its generic plan is a Seq Scan on threads WITH the
        # index as without it, the OR of the three roles is not served by
        # ix_threads_client at all -- and the unread counts are driven by
        # the primary key, not by client. So the index STAYS. A planner
        # that one day passes all five turns this red: drop it then.
        async with get_session_factory()() as s:
            present = await s.scalar(text(
                "SELECT count(*) FROM pg_indexes "
                "WHERE indexname = 'ix_threads_client'"
            ))
        assert present == 1
        assert not all(verdicts.values()), verdicts
        assert verdicts["unread summary"] is False


# ---------------------------------------------------------------------------
# 6 -- the contract
# ---------------------------------------------------------------------------

_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


def _doc_section(number: int) -> str:
    start = _DOC.index(f"\n## {number}. ")
    return _DOC[start:_DOC.index("\n## ", start + 1)]


class TestTheContract:
    async def test_the_row_is_the_answer(self, bare_client: AsyncClient) -> None:
        """Both ways: the fields section 13 names for a row are exactly
        the fields an answer with a thread carries, in order."""
        documented_text = _doc_section(13).split("**A row** is the thread:", 1)[1]
        documented = re.findall(
            r"`([a-z_]+)`", documented_text.split(".\n", 1)[0]
        )
        answer = await _create(
            bare_client, _body(await _recipient(), await _recipient()),
            f"rv1:{uuid4()}",
        )
        assert documented == [key for key in answer if key != "created"]
        assert "revision" in documented

    def test_the_revision_is_explained(self) -> None:
        part = _doc_section(14).split("### The thread's revision", 1)[1]
        part = part.split("\n### ", 1)[0]
        for path in ("`POST /threads`", "`GET /threads`", "`claim`",
                     "`assign`", "`release`", "`status`", "`retag`"):
            assert path in part, path
        assert "**Keep your copy of a thread only if the answer's " \
               "`revision` is greater" in part
        assert "auto-close" in part  # comms' own writes raise it too
        assert "does not\ntouch the assignee" in part or \
               "do not\ntouch the assignee" in part
