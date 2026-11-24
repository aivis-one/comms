"""L-2: an attempt is taken once, within 10 minutes, and purged after a
day (app/login/attempts.py, app/worker.py).

THE TAKE: every outcome of the grid -- taken (the verifier, the
redirect_uri, bot_access; in the row the verifier erased, taken_at set,
committed before the take returns), unknown (never given; older than a
day, purged or not), expired, replayed (taken before, expired since or
not). THE RACE, on real sessions: a holder takes the row, two takes wait
on it (pg_stat_activity, wait_event_type 'Lock'), the holder lets go --
exactly one is taken, the other replayed. THE PURGE: rows older than a
day go, younger stay, with the notification retention off and with
`login` off, behind a gate of its own. THE TABLE's CHECKs refuse what
they forbid, each with its NULL twin.

Mutations: the take without `taken_at IS NULL` (M1), the verifier kept
(M2), the state stored in the clear (M3), the purge under the retention's
gate (M5), the day out of the classification (M6), the take without the
term (M9), the purge by expires_at (M11) -- each red here.
"""

import asyncio
import hashlib
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from app import worker
from app.core.config import settings
from app.core.database import get_session_factory
from app.login.attempts import (
    ATTEMPT_KEEP,
    NotTaken,
    StartedAttempt,
    TakenAttempt,
    code_challenge,
    purge_login_attempts,
    start_attempt,
    take_attempt,
)
from app.login.models import LoginAttempt

_RETURN = "https://product.example.org/auth/telegram/return"


async def _start(bot_access: bool = False) -> StartedAttempt:
    async with get_session_factory()() as session:
        started = await start_attempt(
            session, redirect_uri=_RETURN, bot_access=bot_access,
            client_id="123456",
        )
        await session.commit()
    return started


def _hash(state: str) -> str:
    return hashlib.sha256(state.encode()).hexdigest()


async def _row(state: str) -> LoginAttempt | None:
    async with get_session_factory()() as session:
        return await session.get(LoginAttempt, _hash(state))


async def _age(state: str, ago: timedelta) -> None:
    """The attempt as if started `ago` before now: both times moved."""
    async with get_session_factory()() as session:
        await session.execute(
            text(
                "UPDATE login_attempts SET "
                "created_at = created_at - CAST(:ago AS interval), "
                "expires_at = expires_at - CAST(:ago AS interval), "
                "taken_at = taken_at - CAST(:ago AS interval) "
                "WHERE state_hash = :h"
            ),
            {"ago": ago, "h": _hash(state)},
        )
        await session.commit()


def _challenge_of(started: StartedAttempt) -> str:
    return started.authorize_url.split("code_challenge=")[1].split("&")[0]


class TestTheTake:
    async def test_a_fresh_attempt_is_taken_and_committed(self) -> None:
        started = await _start(bot_access=True)
        taken = await take_attempt(started.state)
        assert isinstance(taken, TakenAttempt)
        # The verifier the start made: its challenge is the URL's.
        assert code_challenge(taken.code_verifier) == _challenge_of(started)
        assert taken.redirect_uri == _RETURN
        assert taken.bot_access is True
        # Committed: another session sees it taken, the verifier erased.
        row = await _row(started.state)
        assert row is not None
        assert row.taken_at is not None
        assert row.code_verifier is None

    async def test_twice_is_replayed(self) -> None:
        started = await _start()
        assert isinstance(await take_attempt(started.state), TakenAttempt)
        assert await take_attempt(started.state) is NotTaken.REPLAYED
        assert await take_attempt(started.state) is NotTaken.REPLAYED

    async def test_never_given_is_unknown(self) -> None:
        """EMPTY: a table with no row, and a table with another's."""
        assert await take_attempt("x" * 43) is NotTaken.UNKNOWN
        other = await _start()
        assert await take_attempt("y" * 43) is NotTaken.UNKNOWN
        assert (await _row(other.state)).code_verifier is not None  # type: ignore[union-attr]

    @pytest.mark.parametrize("ago", [
        timedelta(minutes=10, seconds=1), timedelta(hours=1),
        timedelta(hours=23, minutes=59),
    ], ids=["just-past", "an-hour", "a-day-less-a-minute"])
    async def test_past_its_term_is_expired(self, ago: timedelta) -> None:
        started = await _start()
        await _age(started.state, ago)
        assert await take_attempt(started.state) is NotTaken.EXPIRED
        row = await _row(started.state)
        assert row is not None and row.code_verifier is not None
        assert row.taken_at is None

    async def test_taken_then_past_its_term_is_replayed(self) -> None:
        started = await _start()
        assert isinstance(await take_attempt(started.state), TakenAttempt)
        await _age(started.state, timedelta(hours=1))
        assert await take_attempt(started.state) is NotTaken.REPLAYED

    @pytest.mark.parametrize("taken_first", [False, True], ids=["fresh", "taken"])
    async def test_older_than_a_day_is_unknown_purged_or_not(
        self, taken_first: bool,
    ) -> None:
        started = await _start()
        if taken_first:
            assert isinstance(await take_attempt(started.state), TakenAttempt)
        await _age(started.state, ATTEMPT_KEEP + timedelta(minutes=30))
        assert await _row(started.state) is not None  # not purged yet
        assert await take_attempt(started.state) is NotTaken.UNKNOWN

    async def test_purged_is_unknown(self) -> None:
        started = await _start()
        await _age(started.state, ATTEMPT_KEEP + timedelta(hours=1))
        assert await purge_login_attempts() == 1
        assert await take_attempt(started.state) is NotTaken.UNKNOWN

    def test_the_results_keep_secrets_out_of_their_repr(self) -> None:
        started = StartedAttempt(
            state="unit-state-mark", authorize_url="unit-url-mark?unit-state-mark",
            expires_at=None,  # type: ignore[arg-type]
        )
        taken = TakenAttempt(
            code_verifier="unit-verifier-mark", redirect_uri=_RETURN,
            bot_access=False,
        )
        assert "mark" not in repr(started)
        assert "unit-verifier-mark" not in repr(taken)


# -----------------------------------------------------------------------------
# The race
# -----------------------------------------------------------------------------

_WAITING = text(
    "SELECT count(*) FROM pg_stat_activity "
    "WHERE datname = current_database() AND pid <> :holder "
    "AND wait_event_type = 'Lock' AND query LIKE '%login_attempts%'"
)


async def _until_both_wait(holder: int, tasks: list[asyncio.Task[Any]]) -> None:
    """Until both takes wait on a lock -- by pg_stat_activity, never by a
    sleep's length; a take that ends first fails the test."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10.0
    while True:
        async with get_session_factory()() as session:
            waiting = await session.scalar(_WAITING, {"holder": holder})
        if waiting == 2:
            return
        done = [t.result() for t in tasks if t.done()]
        assert not done, f"a take ended without waiting: {done!r}"
        assert loop.time() < deadline, f"{waiting} of 2 takes wait"
        await asyncio.sleep(0.02)


@pytest.fixture
async def holder() -> AsyncIterator[Any]:
    async with get_session_factory()() as session:
        yield session
        await session.rollback()


class TestTheRace:
    async def test_of_two_takes_at_once_one_wins(self, holder: Any) -> None:
        started = await _start()
        pid = await holder.scalar(text("SELECT pg_backend_pid()"))
        await holder.execute(
            text(
                "SELECT 1 FROM login_attempts WHERE state_hash = :h "
                "FOR NO KEY UPDATE"
            ),
            {"h": _hash(started.state)},
        )
        tasks = [asyncio.create_task(take_attempt(started.state)) for _ in range(2)]
        await _until_both_wait(pid, tasks)
        await holder.rollback()
        results = await asyncio.gather(*tasks)

        taken = [r for r in results if isinstance(r, TakenAttempt)]
        assert len(taken) == 1, results
        assert [r for r in results if not isinstance(r, TakenAttempt)] == [
            NotTaken.REPLAYED,
        ]
        assert code_challenge(taken[0].code_verifier) == _challenge_of(started)
        row = await _row(started.state)
        assert row is not None and row.code_verifier is None


# -----------------------------------------------------------------------------
# The purge
# -----------------------------------------------------------------------------


@pytest.fixture
def gates() -> Iterator[None]:
    worker.reset_login_purge_gate()
    worker.reset_retention_gate()
    worker.reset_auto_close_gate()
    yield
    worker.reset_login_purge_gate()


async def _states() -> set[str]:
    async with get_session_factory()() as session:
        return set((await session.execute(select(LoginAttempt.state_hash))).scalars())


@pytest.mark.usefixtures("gates")
class TestThePurge:
    async def test_older_than_a_day_go_younger_stay(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """With the notification retention OFF (0) and `login` off -- the
        pass is its own."""
        monkeypatch.setattr(settings, "notification_retention_days", 0)
        assert settings.login_enabled == "false"
        old, day_less, fresh = await _start(), await _start(), await _start()
        await _age(old.state, ATTEMPT_KEEP + timedelta(minutes=1))
        await _age(day_less.state, ATTEMPT_KEEP - timedelta(minutes=1))
        await worker.run_maintenance_pass()
        assert await _states() == {_hash(day_less.state), _hash(fresh.state)}

    async def test_an_expired_attempt_is_kept_its_day(self) -> None:
        """By created_at and a day -- not by its term of 10 minutes."""
        started = await _start()
        await _age(started.state, timedelta(hours=1))
        await worker.run_maintenance_pass()
        assert await _states() == {_hash(started.state)}

    async def test_its_own_gate(self) -> None:
        """Once an interval: a row that ages past the day between two
        passes waits for the next interval; the gate reset, it goes."""
        await worker.run_maintenance_pass()
        started = await _start()
        await _age(started.state, ATTEMPT_KEEP + timedelta(minutes=1))
        await worker.run_maintenance_pass()
        assert await _states() == {_hash(started.state)}
        worker.reset_login_purge_gate()
        await worker.run_maintenance_pass()
        assert await _states() == set()


# -----------------------------------------------------------------------------
# The table's CHECKs (migration 0042), each with its NULL twin
# -----------------------------------------------------------------------------

_INSERT = text(
    "INSERT INTO login_attempts (state_hash, provider, redirect_uri, "
    "code_verifier, bot_access, created_at, expires_at, taken_at) VALUES "
    "(:h, :provider, 'https://r.example.org/', :verifier, false, now(), "
    "now() + interval '10 minutes', CASE WHEN :taken THEN now() END)"
)
_GOOD = {"h": "a" * 64, "provider": "telegram", "verifier": "v" * 64,
         "taken": False}


class TestTheChecks:
    @pytest.mark.parametrize("change", [
        {"provider": "other"},
        {"h": "A" * 64},
        {"h": "unit-state-in-the-clear-" + "x" * 19},
        {"verifier": None},  # not taken, no verifier
        {"taken": True},  # taken, the verifier kept
    ], ids=["provider", "hash-upper", "state-in-clear", "untaken-null",
            "taken-kept"])
    async def test_refused(self, change: dict[str, Any]) -> None:
        async with get_session_factory()() as session:
            with pytest.raises(IntegrityError):
                await session.execute(_INSERT, {**_GOOD, **change})

    @pytest.mark.parametrize("change", [
        {},
        {"verifier": None, "taken": True},  # taken, the verifier gone
    ], ids=["untaken", "taken"])
    async def test_the_pair_accepted(self, change: dict[str, Any]) -> None:
        async with get_session_factory()() as session:
            await session.execute(_INSERT, {**_GOOD, **change})
            await session.rollback()
