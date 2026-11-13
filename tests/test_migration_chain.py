# =============================================================================
# W6-1 -- a 3.4.0 database climbs 0025 -> head in one migrate, and again
# after a migrate that failed half way
# =============================================================================
#
# The database of a box on comms 3.4.0 (revision 0025, people's data in
# it) is brought to head by ONE `alembic upgrade head` -- the deploy
# machine's step `migrate` (primer.yaml) -- and the machine runs that step
# again when it fails. Held here on the database the suite runs on,
# brought down to 0025 and seeded, as tests/test_migration_0037.py does:
#
#   - the chain in one run: head, `alembic check` clean, the schema the same
#     as a head built from nothing, every row of every table kept by the rule
#     of each migration that changes data (_after), the count of
#     `comms count` (app/cli.py count_conversations) the same before and
#     after;
#   - a run that FAILED after any one step of the chain -- an exception at
#     the end of the step, after all its work -- and a run CUT OFF
#     (pg_terminate_backend) while it waits on a lock, at 0039 and inside
#     0041's CREATE INDEX CONCURRENTLY: the next run reaches head, the same
#     schema, the same rows;
#   - no migration after 0041 imports `app` (TestNoLaterMigrationImportsApp).
#
# WHAT A FAILURE LEAVES (alembic 1.20, runtime/migration.py): the chain runs
# in one transaction (migrations/env.py), but an autocommit_block COMMITS
# that transaction first -- every step before it, with its stamp -- and a
# new one begins after it; the stamp of a step is written after the step
# returns. So a failure after step N leaves the stamp at the revision before
# B, B the last step with a block not after N (0025 when there is none),
# with B's schema applied: the next run runs B again. _committed_stamp
# predicts it, and every failure test holds the prediction first -- the
# failure happened where the test put it.
#
# THE SEED (_SEED) is 0025's schema in the forms 3.4.0 (aaf6781) writes, by
# reading its writers -- not 3.4.0's code run here: the image the suite runs
# in carries no other tree. Every table of 0025 has rows, every nullable
# column a value somewhere (_NEVER_WRITTEN says where 3.4.0 writes none),
# and each condition a data step of the chain tests has both of its sides
# (test_the_seed_has_every_form). 0025's own CHECKs refuse a form 3.4.0
# could not write.
#
# MUTATIONS (one run, each red here, the migration reverted after):
#   M1 0034 also deletes one message           -> the climb (the rows)
#   M2 0030 without the UPDATE of recipients   -> the climb (0030's CHECK
#                                                 refuses the seed's JSON null)
#   M3 0039 without its UPDATE                 -> the climb (rule of 0039)
#   M4 0040's ADD COLUMN without IF NOT EXISTS -> the failure after 0040
#   M5 0041 without its DROP INDEX ... IF EXISTS -> the cut-off inside 0041
#                                                 (the index left invalid)
#   M6 ACTIVE_STATUSES renamed in app/engine/constants.py and its users
#      -> this file does not collect: 0037 does not import
#   M7 the injected failure raises nothing     -> every failure case
#   M8 the import scanner without `from app`   -> the scanner reads the tree
# =============================================================================

import ast
import asyncio
import json
import re
import shutil
import subprocess
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from app.cli import count_conversations
from app.core.config import settings
from app.core.database import dispose_engine, get_session_factory
from tests.test_migration_safety import uses_a_block

REPO_ROOT = Path(__file__).resolve().parents[1]
VERSIONS = REPO_ROOT / "migrations" / "versions"
_BASE = "0025_pending_deliveries_index"

# Revisions up to this number are in main; every one after it is v4's and
# imports nothing of `app` (TestNoLaterMigrationImportsApp).
_IMPORTS_CHECKED_AFTER = 41

# A process of alembic, or of the run with a failure in it: at most this
# many seconds (the suite's own timeout is 300 a test).
_PROCESS_TIMEOUT = 240


# ---------------------------------------------------------------------------
# The chain, as alembic walks it
# ---------------------------------------------------------------------------


def _chain() -> list[tuple[str, str, Path]]:
    """(revision, down_revision, file) of every step after 0025, oldest
    first -- a new step of v4 is in it by itself."""
    config = Config(str(REPO_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(REPO_ROOT / "migrations"))
    script = ScriptDirectory.from_config(config)
    steps = []
    for revision in script.walk_revisions(base=_BASE, head="heads"):
        if revision.revision == _BASE:
            continue
        assert isinstance(revision.down_revision, str), revision.revision
        steps.append(
            (revision.revision, revision.down_revision, Path(revision.path)),
        )
    return list(reversed(steps))


_CHAIN = _chain()
_HEAD = _CHAIN[-1][0]


def _committed_stamp(revision: str) -> str:
    """The stamp a failure at the end of `revision`'s step leaves: the
    revision before the last step with an autocommit_block up to it, or
    0025 (the module's header)."""
    stamp = _BASE
    for step, previous, path in _CHAIN:
        if uses_a_block(path):
            stamp = previous
        if step == revision:
            return stamp
    raise AssertionError(f"{revision} is not in the chain")


# ---------------------------------------------------------------------------
# Processes: alembic, and alembic with one step that fails
# ---------------------------------------------------------------------------


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    """A Python process in the repository: the migrations call asyncio.run
    (migrations/env.py), which must not run inside the suite's loop."""
    return subprocess.run(
        [sys.executable, *args], cwd=REPO_ROOT, capture_output=True, text=True,
        timeout=_PROCESS_TIMEOUT, check=False,
    )


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    return _run("-m", "alembic", *args)


async def _migrate(*args: str) -> None:
    await dispose_engine()
    done = await asyncio.to_thread(_alembic, *args)
    await dispose_engine()
    assert done.returncode == 0, done.stderr


# `alembic upgrade head` with the steps of another directory: the copy in
# which one step fails.
_UPGRADE_FROM = (
    "import sys\n"
    "from alembic import command\n"
    "from alembic.config import Config\n"
    "config = Config('alembic.ini')\n"
    "config.set_main_option('version_locations', sys.argv[1])\n"
    "command.upgrade(config, 'head')\n"
)


def _marker(revision: str) -> str:
    return f"W6-1 injected failure after {revision}"


def _failing_copy(revision: str, path: Path, into: Path) -> Path:
    """migrations/versions copied, the step of `revision` ending in an
    exception after all its work -- its autocommit_block included."""
    versions = into / "versions"
    versions.mkdir()
    for source in VERSIONS.glob("*.py"):
        shutil.copy(source, versions / source.name)
    with (versions / path.name).open("a", encoding="utf-8") as copy:
        copy.write(
            "\n\n_w61_upgrade = upgrade\n\n\n"
            "def upgrade() -> None:  # noqa: F811\n"
            "    _w61_upgrade()\n"
            f"    raise RuntimeError({_marker(revision)!r})\n"
        )
    return versions


async def _upgrade_failing_after(
    revision: str, path: Path, tmp_path: Path,
) -> subprocess.CompletedProcess[str]:
    versions = _failing_copy(revision, path, tmp_path)
    await dispose_engine()
    done = await asyncio.to_thread(_run, "-c", _UPGRADE_FROM, str(versions))
    await dispose_engine()
    return done


# ---------------------------------------------------------------------------
# The database: its stamp, its schema, its rows
# ---------------------------------------------------------------------------


async def _all(statement: str, **params: Any) -> list[Any]:
    async with get_session_factory()() as session:
        return list((await session.execute(text(statement), params)).all())


async def _scalar(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        return await session.scalar(text(statement), params)


async def _execute(*statements: str) -> None:
    async with get_session_factory()() as session:
        for statement in statements:
            await session.execute(text(statement))
        await session.commit()


async def _stamp() -> str:
    return str(await _scalar("SELECT version_num FROM alembic_version"))


async def _tables() -> list[str]:
    return [row[0] for row in await _all(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
        "AND table_name <> 'alembic_version' ORDER BY table_name"
    )]


async def _schema() -> dict[str, list[tuple[Any, ...]]]:
    """What the schema IS, read from the catalog: columns (no position:
    a column dropped and added back moves), constraints with their
    validation, indexes with their validity, triggers, functions."""
    return {
        "columns": sorted(tuple(r) for r in await _all(
            "SELECT table_name, column_name, data_type, is_nullable, "
            "coalesce(column_default, '') FROM information_schema.columns "
            "WHERE table_schema = 'public'")),
        "constraints": sorted(tuple(r) for r in await _all(
            "SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid), "
            "convalidated FROM pg_constraint "
            "WHERE connamespace = 'public'::regnamespace")),
        "indexes": sorted(tuple(r) for r in await _all(
            "SELECT pg_get_indexdef(i.indexrelid), i.indisvalid, i.indisready "
            "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE c.relnamespace = 'public'::regnamespace")),
        "triggers": sorted(tuple(r) for r in await _all(
            "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE NOT tgisinternal")),
        "functions": sorted(tuple(r) for r in await _all(
            "SELECT proname, prosrc FROM pg_proc "
            "WHERE pronamespace = 'public'::regnamespace")),
    }


Rows = dict[str, list[dict[str, Any]]]


async def _rows() -> Rows:
    """Every row of every table as JSON, and for each JSONB column
    `<column>#null`: whether it is SQL NULL -- to_jsonb writes SQL NULL
    and the JSON value null alike, and 0030 tells them apart."""
    found: Rows = {}
    for table in await _tables():
        jsonb = [row[0] for row in await _all(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :table "
            "AND data_type = 'jsonb' ORDER BY column_name", table=table,
        )]
        flags = ", ".join(f"'{c}#null', t.{c} IS NULL" for c in jsonb)
        row = f"to_jsonb(t) || jsonb_build_object({flags})" if jsonb else "to_jsonb(t)"
        found[table] = [
            json.loads(value) if isinstance(value, str) else value
            for (value,) in await _all(f"SELECT {row} FROM {table} t")
        ]
    return found


def _canonical(rows: Rows) -> dict[str, list[str]]:
    return {
        table: sorted(json.dumps(row, sort_keys=True) for row in table_rows)
        for table, table_rows in rows.items()
    }


def _after(before: Rows, tables_at_head: list[str]) -> Rows:
    """The rows a run of the chain leaves, by the rule of each migration
    that changes data; every other column and every other table as it was.
    A table the chain creates is empty."""
    members = {row["section_id"] for row in before["section_members"]}
    expected: Rows = {table: [] for table in tables_at_head}
    for table, rows in before.items():
        for original in rows:
            row = dict(original)
            if table == "notifications":
                # 0030: a JSON null is SQL NULL
                row["action_data#null"] = row["action_data"] is None
                # 0036: the product's own email, none on an old job
                row["email"], row["email#null"] = None, True
                # 0038: a gate at zero attempts goes (none at 0025: 0015's
                # CHECK refuses one)
                if row["pipeline_attempts"] == 0:
                    row["pipeline_retry_at"] = None
            elif table == "notification_deliveries":
                # 0032: the column is gone
                del row["channel_options"], row["channel_options#null"]
            elif table == "recipients":
                # 0030
                row["allowed_windows#null"] = row["allowed_windows"] is None
            elif table == "threads":
                # 0035: 1 for every thread that existed
                row["revision"] = 1
            elif table == "sections":
                # 0039: declared exactly where members are
                row["roster_declared"] = row["id"] in members
            elif table == "notification_transitions":
                # 0040
                row["template_unresolved"] = None
            expected[table].append(row)
    return expected


async def _count() -> tuple[int, int]:
    return await count_conversations(settings.database_url)


# ---------------------------------------------------------------------------
# The seed: revision 0025 in 3.4.0's forms (aaf6781)
# ---------------------------------------------------------------------------


def _id(n: int) -> str:
    return f"6a000000-0000-4000-8000-{n:012d}"


R1, R2, R3, R4, R5 = (_id(n) for n in range(1, 6))
N1, N2, N3, N4 = (_id(n) for n in range(11, 15))
D1, D2, D3, D4, D5 = (_id(n) for n in range(21, 26))
S1, S2 = _id(31), _id(32)
T1, T2, T3 = _id(41), _id(42), _id(43)
M1, M2, M3 = _id(51), _id(52), _id(53)
I1, I2 = _id(61), _id(62)

_F = "f" * 64
_TOMBSTONE = "tombstone".ljust(64, "-")


def _at(offset: str = "0 seconds") -> str:
    """A fixed moment, so that two seeds are the same rows byte for byte
    (TestTheConnectionCutOff compares two runs' rows)."""
    return f"(timestamptz '2026-11-01 10:00:00+00' + interval '{offset}')"


_JOB = (
    "INSERT INTO notifications (id, type, title, body, target_type, "
    "target_value, action_data, scheduled_at, expiry_at, status, created_at, "
    "idempotency_key, fingerprint, channels, correlation, expiry_layer, "
    "category, pipeline_attempts, pipeline_retry_at, pipeline_step, "
    "pipeline_error, push_on, retry_max_attempts, retry_backoff_seconds, "
    "recipient_schedule, lane) VALUES "
)
_DELIVERY = (
    "INSERT INTO notification_deliveries (id, notification_id, recipient_id, "
    "channel, status, sent_at, read_at, attempts, error_message, created_at, "
    "next_retry_at, rate_limit_deferrals, failure_class, wait_reason) VALUES "
)
# The journal's id is an identity ALWAYS and its xact_id a default:
# given here, with OVERRIDING SYSTEM VALUE, for the same reason as _at.
_TRANSITION = (
    "INSERT INTO notification_transitions (id, xact_id, notification_id, "
    "recipient_id, channel, subject, step, outcome, attempt, wait_reason, "
    "wait_until, failure_class, category, error, provider_text, at) "
    "OVERRIDING SYSTEM VALUE VALUES "
)
_THREAD = (
    "INSERT INTO threads (id, client, operator_kind, operator_value, assignee, "
    "assigned_at, kind, subject_type, subject_id, title, priority, status, "
    "last_message_at, created_at, updated_at, close_notify_pending_at, "
    "idempotency_key, fingerprint) VALUES "
)

# Each form with the 3.4.0 writer it stands for (paths at aaf6781).
_SEED: tuple[str, ...] = (
    # app/audience/sync.py apply_snapshot (created without allowed_windows:
    # SQL NULL); app/audience/prefs.py set_schedule (a list) and its
    # clearing (None through the bare JSONB type: the JSON value null, H13);
    # app/audience/sync.py tombstone (every field NULL, inactive).
    "INSERT INTO recipients (id, telegram_id, email, locale, active, created_at, "
    "updated_at, timezone, allowed_windows, version, snapshot_fingerprint, "
    "deleted_at) VALUES "
    f"('{R1}', 1001, 'r1@example.org', 'en', true, {_at()}, {_at('1 hour')}, "
    "'Europe/Berlin', '[{\"day\": 1, \"from\": 540, \"to\": 1080}]', 3, "
    f"'{_F}', NULL), "
    f"('{R2}', NULL, 'r2@example.org', 'de', true, {_at()}, {_at('2 hours')}, "
    f"NULL, 'null', 2, '{_F}', NULL), "
    f"('{R3}', 1003, NULL, NULL, true, {_at()}, NULL, NULL, NULL, 1, '{_F}', "
    "NULL), "
    f"('{R4}', NULL, NULL, NULL, false, {_at()}, {_at('3 hours')}, NULL, NULL, "
    f"2, '{_TOMBSTONE}', {_at('3 hours')}), "
    f"('{R5}', 1005, 'operator@example.org', 'en', true, {_at()}, NULL, NULL, "
    f"NULL, 1, '{_F}', NULL)",
    "INSERT INTO group_memberships (group_key, recipient_id) VALUES "
    f"('team-a', '{R1}'), ('team-a', '{R2}')",
    "INSERT INTO category_mutes (recipient_id, category, created_at) VALUES "
    f"('{R3}', 'promo', {_at()})",
    # app/engine/service.py create_notification: action_data passed always
    # -- an object, or None as the JSON value null; channels a list of the
    # profile's channels; the pipeline's gate behind counted attempts
    # (app/engine/processor.py, 0015's CHECKs).
    _JOB
    + f"('{N1}', 'unit_event_in_app', 'T1', 'B1', 'user', '{R1}', "
    "'{\"action\": \"open_booking\", \"params\": {\"booking_id\": \"b-1\"}}', "
    f"{_at()}, {_at('1 day')}, 'sent', {_at()}, 'w61:n1', '{_F}', "
    "'[\"in_app\", \"telegram\"]', 'booking:b-1', 'envelope', 'booking', 0, "
    "NULL, NULL, NULL, 'outcome', 3, 30, 'respect', 'normal'), "
    f"('{N2}', 'unit_event_telegram', 'T2', 'B2', 'group', 'team-a', 'null', "
    f"{_at()}, NULL, 'processing', {_at()}, 'w61:n2', '{_F}', '[\"telegram\"]', "
    f"NULL, 'default', NULL, 2, {_at('30 seconds')}, 'deliver', "
    "'OperationalError at deliver', 'outcome_and_deferral', 5, 0, 'bypass', "
    "'interactive'), "
    f"('{N3}', 'unit_event_email', 'T3', 'B3', 'all', '*', 'null', {_at()}, "
    f"{_at('2 days')}, 'partial_sent', {_at()}, 'w61:n3', '{_F}', "
    "'[\"email\"]', 'practice:p-7', 'profile', 'digest', 0, NULL, NULL, NULL, "
    "'none', 1, 86400, 'respect', 'bulk'), "
    f"('{N4}', 'unit_event_in_app', 'T4', 'B4', 'user', '{R5}', 'null', "
    f"{_at('1 hour')}, NULL, 'pending', {_at()}, 'w61:n4', '{_F}', "
    "'[\"in_app\"]', NULL, 'default', NULL, 0, NULL, NULL, NULL, 'outcome', 3, "
    "30, 'respect', 'critical')",
    # app/engine/service.py resolve_notification: a delivery without
    # channel_options (an attribute never set is not in the INSERT: SQL
    # NULL); its later states by the processor and the read API.
    _DELIVERY
    + f"('{D1}', '{N1}', '{R1}', 'in_app', 'sent', {_at('1 minute')}, "
    f"{_at('5 minutes')}, 1, NULL, {_at()}, NULL, 0, NULL, NULL), "
    f"('{D2}', '{N1}', '{R1}', 'telegram', 'sent', {_at('1 minute')}, NULL, 1, "
    f"NULL, {_at()}, NULL, 0, NULL, NULL), "
    f"('{D3}', '{N2}', '{R2}', 'telegram', 'pending', NULL, NULL, 0, NULL, "
    f"{_at()}, {_at('1 hour')}, 0, NULL, 'recipient_schedule'), "
    f"('{D4}', '{N3}', '{R3}', 'email', 'failed', NULL, NULL, 3, 'smtp timeout', "
    f"{_at()}, NULL, 2, 'transient_exhausted', NULL), "
    f"('{D5}', '{N3}', '{R4}', 'email', 'recipient_inactive', NULL, NULL, 0, "
    f"NULL, {_at()}, NULL, 0, NULL, NULL)",
    # app/engine/journal.py: record_job, record_delivery, record_gate,
    # record_channel_answer -- each subject in its shape.
    _TRANSITION
    + f"(1, '1001', '{N1}', NULL, NULL, 'job', 'intake', 'pending', 0, NULL, "
    f"NULL, NULL, NULL, NULL, NULL, {_at()}), "
    f"(2, '1002', '{N1}', '{R1}', 'in_app', 'delivery', 'resolve', 'pending', 0, "
    f"NULL, NULL, NULL, NULL, NULL, NULL, {_at()}), "
    f"(3, '1003', '{N1}', '{R1}', 'telegram', 'channel', 'deliver', 'accepted', "
    f"1, NULL, NULL, NULL, NULL, NULL, 'message_id 7', {_at('1 minute')}), "
    f"(4, '1004', '{N1}', NULL, NULL, 'job', 'rollup', 'sent', 0, NULL, NULL, "
    f"NULL, NULL, NULL, NULL, {_at('1 minute')}), "
    f"(5, '1005', '{N2}', NULL, NULL, 'job', 'deliver', 'processing', 2, "
    f"'pipeline_retry', {_at('30 seconds')}, NULL, NULL, "
    f"'OperationalError at deliver', NULL, {_at()}), "
    f"(6, '1006', '{N2}', '{R2}', 'telegram', 'delivery', 'resolve', 'pending', "
    f"0, 'recipient_schedule', {_at('1 hour')}, NULL, NULL, NULL, NULL, "
    f"{_at()}), "
    f"(7, '1006', '{N2}', '{R3}', NULL, 'gate', 'resolve', 'suppressed', 0, "
    f"NULL, NULL, NULL, 'promo', NULL, NULL, {_at()}), "
    f"(8, '1007', '{N3}', '{R3}', 'email', 'channel', 'deliver', 'transient', 3, "
    "NULL, NULL, 'transient_exhausted', NULL, 'TimeoutError', 'gateway timeout', "
    f"{_at('2 minutes')}), "
    f"(9, '1008', '{N3}', '{R3}', 'email', 'delivery', 'deliver', 'failed', 3, "
    f"NULL, NULL, 'transient_exhausted', 'digest', NULL, NULL, {_at('2 minutes')})",
    # app/engine/journal.py _owe_push; its id an identity ALWAYS (_TRANSITION)
    "INSERT INTO push_outbox (id, notification_id, created_at) "
    f"OVERRIDING SYSTEM VALUE VALUES (1, '{N1}', {_at()}), (2, '{N3}', {_at()})",
    # app/engine/service.py record_intake_outcome
    "INSERT INTO intake_outcomes (id, idempotency_key, fingerprint, outcome, "
    f"reason, notification_id, received_at) VALUES ('{I1}', 'w61:rejected', "
    f"'{_F}', 'rejected_at_intake', 'unknown type', NULL, {_at()}), "
    f"('{I2}', 'w61:n1', '{'c' * 64}', 'conflict', 'other bytes under the key', "
    f"'{N1}', {_at()})",
    # app/messaging/sections.py, app/messaging/membership.py
    f"INSERT INTO sections (id, key, label, created_at) VALUES ('{S1}', "
    f"'support', 'Support', {_at()}), ('{S2}', 'billing', 'Billing', {_at()})",
    f"INSERT INTO section_members (section_id, operator_id) VALUES ('{S1}', '{R5}')",
    # app/messaging/threads.py: a user thread pre-assigned, section threads
    # unassigned; app/messaging/status.py marks a closed section thread.
    _THREAD
    + f"('{T1}', '{R1}', 'user', '{R5}', '{R5}', {_at()}, 'dm', NULL, NULL, NULL, "
    f"NULL, 'open', {_at('2 minutes')}, {_at()}, NULL, NULL, 'w61:t1', '{_F}'), "
    f"('{T2}', '{R2}', 'section', '{S1}', NULL, NULL, 'ticket', 'booking', 'b-1', "
    f"'About b-1', 2, 'resolved', {_at('3 minutes')}, {_at()}, {_at('1 hour')}, "
    f"NULL, 'w61:t2', '{_F}'), "
    f"('{T3}', '{R3}', 'section', '{S2}', NULL, NULL, 'ticket', NULL, NULL, NULL, "
    f"NULL, 'closed', NULL, {_at()}, {_at('1 day')}, {_at('1 day')}, 'w61:t3', "
    f"'{_F}')",
    "INSERT INTO messages (id, thread_id, sender, body, created_at, "
    f"idempotency_key, fingerprint) VALUES ('{M1}', '{T1}', '{R1}', 'hello', "
    f"{_at('1 minute')}, 'w61:m1', '{_F}'), "
    f"('{M2}', '{T1}', '{R5}', 'hi', {_at('2 minutes')}, 'w61:m2', '{_F}'), "
    f"('{M3}', '{T2}', '{R2}', 'about b-1', {_at('3 minutes')}, 'w61:m3', "
    f"'{_F}')",
    # app/messaging/read_state.py mark_read
    "INSERT INTO thread_read_states (thread_id, participant, last_read_at, "
    f"created_at) VALUES ('{T1}', '{R1}', {_at('2 minutes')}, {_at()}), "
    f"('{T1}', '{R5}', {_at('1 minute')}, {_at()})",
)

# Nullable columns of 0025 that 3.4.0 never writes a value into -> why.
_NEVER_WRITTEN: dict[tuple[str, str], str] = {
    ("notification_deliveries", "channel_options"): (
        "no writer at aaf6781: app/engine/service.py creates a delivery "
        "without it, nothing updates it (0032 drops it)"
    ),
}


async def _seed() -> None:
    await _execute(*_SEED)


async def _seeded_at_0025() -> None:
    """The suite's database (clean, at head) down to 0025, seeded."""
    await _migrate("downgrade", _BASE)
    assert await _stamp() == _BASE
    await _seed()


@pytest.fixture
async def back_to_head() -> AsyncGenerator[None, None]:
    """The database at head again and without the seed, whatever the test
    left. A chain that cannot run again (a red test of this file) is
    rebuilt from nothing, so the rest of the suite still has its schema."""
    yield
    await dispose_engine()
    if await _stamp() != _HEAD:
        done = await asyncio.to_thread(_alembic, "upgrade", "head")
        if done.returncode != 0:
            await _execute("DROP SCHEMA public CASCADE", "CREATE SCHEMA public")
            await _migrate("upgrade", "head")
    tables = ", ".join(await _tables())
    await _execute(f"TRUNCATE {tables} CASCADE")
    await dispose_engine()


async def _held_to_the_rules(before: Rows, reference: dict[str, Any]) -> None:
    """At head: the schema of a head built from nothing, every row by the
    rule of its migration."""
    assert await _stamp() == _HEAD
    assert await _schema() == reference
    after = await _rows()
    assert _canonical(after) == _canonical(_after(before, await _tables()))


# ---------------------------------------------------------------------------
# Item 1 -- 0025 with data -> head in one run
# ---------------------------------------------------------------------------


class TestTheClimb:
    async def test_the_seed_has_every_form(self, back_to_head: None) -> None:
        """The instrument: every table of 0025 has rows, every nullable
        column a value somewhere (but _NEVER_WRITTEN), and both sides of
        each condition a data step of the chain tests."""
        await _seeded_at_0025()
        tables = await _tables()
        assert len(tables) == 13, tables
        for table in tables:
            assert await _scalar(f"SELECT count(*) FROM {table}") > 0, table
        nullable = await _all(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND is_nullable = 'YES' "
            "AND table_name <> 'alembic_version'"
        )
        assert len(nullable) > 30  # the pair: the walk reads the columns
        empty = [
            (table, column) for table, column in nullable
            if not await _scalar(f"SELECT count({column}) FROM {table}")
        ]
        assert sorted(empty) == sorted(_NEVER_WRITTEN)
        sides = {
            # 0030: a JSON null, a SQL NULL, a value
            "action_data json null": "SELECT count(*) FROM notifications "
            "WHERE action_data = 'null'::jsonb",
            "action_data object": "SELECT count(*) FROM notifications "
            "WHERE jsonb_typeof(action_data) = 'object'",
            "windows json null": "SELECT count(*) FROM recipients "
            "WHERE allowed_windows = 'null'::jsonb",
            "windows sql null": "SELECT count(*) FROM recipients "
            "WHERE allowed_windows IS NULL",
            "windows array": "SELECT count(*) FROM recipients "
            "WHERE jsonb_typeof(allowed_windows) = 'array'",
            # 0038: a gate behind counted attempts, none without
            "gated": "SELECT count(*) FROM notifications "
            "WHERE pipeline_retry_at IS NOT NULL AND pipeline_attempts > 0",
            "not gated": "SELECT count(*) FROM notifications "
            "WHERE pipeline_retry_at IS NULL AND pipeline_attempts = 0",
            # 0039: a section with members, one without
            "section with members": "SELECT count(*) FROM sections s "
            "WHERE EXISTS (SELECT 1 FROM section_members m "
            "WHERE m.section_id = s.id)",
            "section without": "SELECT count(*) FROM sections s "
            "WHERE NOT EXISTS (SELECT 1 FROM section_members m "
            "WHERE m.section_id = s.id)",
            # 0041, 0034, 0031: both sides of each index's predicate
            "correlation": "SELECT count(*) FROM notifications "
            "WHERE correlation IS NOT NULL",
            "no correlation": "SELECT count(*) FROM notifications "
            "WHERE correlation IS NULL",
            "last message": "SELECT count(*) FROM threads "
            "WHERE last_message_at IS NOT NULL",
            "no message": "SELECT count(*) FROM threads "
            "WHERE last_message_at IS NULL",
            "sent unread": "SELECT count(*) FROM notification_deliveries "
            "WHERE status = 'sent' AND read_at IS NULL",
            "sent read": "SELECT count(*) FROM notification_deliveries "
            "WHERE status = 'sent' AND read_at IS NOT NULL",
        }
        missing = [name for name, query in sides.items() if not await _scalar(query)]
        assert missing == []

    async def test_0025_with_data_climbs_to_head_in_one_run(
        self, back_to_head: None,
    ) -> None:
        reference = await _schema()
        await _seeded_at_0025()
        before = await _rows()
        counted = await _count()
        assert counted == (3, 3)  # the pair: `comms count` reads 0025

        await _migrate("upgrade", "head")

        await _held_to_the_rules(before, reference)
        assert await _count() == counted
        await dispose_engine()
        checked = await asyncio.to_thread(_alembic, "check")
        assert checked.returncode == 0, checked.stdout + checked.stderr
        # Run again at head: nothing to do, nothing changed.
        await _migrate("upgrade", "head")
        await _held_to_the_rules(before, reference)

    async def test_an_empty_0025_climbs_to_head(self, back_to_head: None) -> None:
        reference = await _schema()
        await _migrate("downgrade", _BASE)
        assert await _count() == (0, 0)
        await _migrate("upgrade", "head")
        assert await _stamp() == _HEAD
        assert await _schema() == reference
        assert all(rows == [] for rows in (await _rows()).values())


# ---------------------------------------------------------------------------
# Item 2 -- a run that failed half way, and the next run
# ---------------------------------------------------------------------------


class TestTheRunAgain:
    def test_the_chain_is_the_steps_after_0025(self) -> None:
        """The pair of the parametrization: the walk sees the chain, the
        steps with a block among it."""
        names = [revision for revision, _, _ in _CHAIN]
        assert names[0] == "0026_llm_channel"
        assert len(names) >= 16
        assert [r for r, _, p in _CHAIN if uses_a_block(p)][:4] == [
            "0037_pipeline_transient_since", "0038_drop_transient_wait",
            "0040_template_unresolved", "0041_correlation_index",
        ]

    @pytest.mark.parametrize(
        ("revision", "path"), [(r, p) for r, _, p in _CHAIN],
        ids=[r for r, _, _ in _CHAIN],
    )
    async def test_a_run_that_failed_after_a_step_goes_through_again(
        self, back_to_head: None, tmp_path: Path, revision: str, path: Path,
    ) -> None:
        reference = await _schema()
        await _seeded_at_0025()
        before = await _rows()
        counted = await _count()

        failed = await _upgrade_failing_after(revision, path, tmp_path)
        assert failed.returncode != 0
        assert _marker(revision) in failed.stderr, failed.stderr[-2000:]
        assert await _stamp() == _committed_stamp(revision)

        await _migrate("upgrade", "head")
        await _held_to_the_rules(before, reference)
        assert await _count() == counted


# The migration process and the test's own lock holders: separate
# connections, outside the suite's engine (it is disposed around every run).
def _holder_engine() -> Any:
    return create_async_engine(
        settings.database_url, hide_parameters=True, poolclass=NullPool,
    )


_WAITING = text(
    "SELECT pid FROM pg_stat_activity WHERE datname = current_database() "
    "AND pid <> pg_backend_pid() AND wait_event_type = 'Lock' "
    "AND query LIKE :statement"
)


async def _waiting_pid(
    observer: AsyncConnection, process: asyncio.subprocess.Process, statement: str,
) -> int:
    """The pid of the migration once it waits on a lock in `statement`
    (a LIKE pattern) -- by pg_stat_activity, not by a clock."""
    for _ in range(3000):
        pids = (await observer.execute(_WAITING, {"statement": statement})).all()
        await observer.rollback()
        if pids:
            return int(pids[0][0])
        if process.returncode is not None:
            break
        await asyncio.sleep(0.02)
    _, err = await process.communicate()
    raise AssertionError(f"the migration never waited in {statement}: {err!r}")


async def _start_upgrade() -> asyncio.subprocess.Process:
    await dispose_engine()
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", "alembic", "upgrade", "head",
        cwd=REPO_ROOT, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def _cut_off(
    observer: AsyncConnection, process: asyncio.subprocess.Process, pid: int,
) -> None:
    """The migration's connection terminated by the server, the process
    left to fail on it."""
    assert (await observer.execute(
        text("SELECT pg_terminate_backend(:pid)"), {"pid": pid},
    )).scalar()
    await observer.rollback()
    _, err = await asyncio.wait_for(process.communicate(), _PROCESS_TIMEOUT)
    assert process.returncode != 0, err


async def _transient_columns() -> int:
    return int(await _scalar(
        "SELECT count(*) FROM information_schema.columns WHERE table_name = "
        "'notifications' AND column_name LIKE 'pipeline_transient_%'"
    ))


class TestTheConnectionCutOff:
    async def test_cut_off_waiting_at_0039_is_the_failure_after_0038(
        self, back_to_head: None, tmp_path: Path,
    ) -> None:
        """Cut off while 0039 waits for `sections` (the test holds it):
        0037's and 0038's blocks are committed, the stamp is 0037 -- the
        state an exception at the end of 0038 leaves, schema and rows
        alike; the next run reaches head."""
        reference = await _schema()
        _, step_0038 = next((r, p) for r, _, p in _CHAIN
                            if r == "0038_drop_transient_wait")
        await _seeded_at_0025()
        failed = await _upgrade_failing_after(
            "0038_drop_transient_wait", step_0038, tmp_path)
        assert failed.returncode != 0
        by_exception = (await _stamp(), await _schema(), _canonical(await _rows()))
        await _migrate("upgrade", "head")
        await _execute(f"TRUNCATE {', '.join(await _tables())} CASCADE")

        await _seeded_at_0025()
        before = await _rows()
        counted = await _count()
        engine = _holder_engine()
        try:
            async with engine.connect() as holder, engine.connect() as observer:
                await holder.execute(text("LOCK TABLE sections IN ACCESS SHARE MODE"))
                process = await _start_upgrade()
                pid = await _waiting_pid(observer, process, "ALTER TABLE sections%")
                await _cut_off(observer, process, pid)
                await holder.rollback()
        finally:
            await engine.dispose()

        # the schema ahead of its stamp: 0038 dropped what 0037 stands for
        assert await _stamp() == _committed_stamp("0038_drop_transient_wait")
        assert await _transient_columns() == 0
        assert (await _stamp(), await _schema(), _canonical(await _rows())) == (
            by_exception)

        await _migrate("upgrade", "head")
        await _held_to_the_rules(before, reference)
        assert await _count() == counted

    async def test_cut_off_inside_the_concurrent_build_of_0041(
        self, back_to_head: None,
    ) -> None:
        """0041's CREATE INDEX CONCURRENTLY commits the index to the
        catalog, then waits for a writer of notifications (the test holds
        ROW EXCLUSIVE): cut off there, the index is left INVALID under its
        name, the stamp 0040. The next run drops it and builds it valid."""
        reference = await _schema()
        await _seeded_at_0025()
        before = await _rows()
        counted = await _count()
        engine = _holder_engine()
        try:
            async with (
                engine.connect() as parking,
                engine.connect() as writer,
                engine.connect() as observer,
            ):
                # Parked at 0039 first: the writer's lock taken before would
                # stop 0026's ALTER of notifications instead.
                await parking.execute(text("LOCK TABLE sections IN ACCESS SHARE MODE"))
                process = await _start_upgrade()
                await _waiting_pid(observer, process, "ALTER TABLE sections%")
                await writer.execute(
                    text("LOCK TABLE notifications IN ROW EXCLUSIVE MODE"))
                await parking.rollback()
                pid = await _waiting_pid(
                    observer, process, "CREATE INDEX CONCURRENTLY%")
                valid = (await observer.execute(text(
                    "SELECT indisvalid FROM pg_index WHERE indexrelid = "
                    "to_regclass('ix_notifications_correlation')"))).all()
                await observer.rollback()
                assert valid == [(False,)]  # the build's catalog row, invalid
                await _cut_off(observer, process, pid)
                await writer.rollback()
        finally:
            await engine.dispose()

        assert await _stamp() == "0040_template_unresolved"
        assert await _scalar(
            "SELECT indisvalid FROM pg_index WHERE indexrelid = "
            "to_regclass('ix_notifications_correlation')") is False

        await _migrate("upgrade", "head")
        await _held_to_the_rules(before, reference)
        assert await _count() == counted


# ---------------------------------------------------------------------------
# Item 4 -- no migration after 0041 imports `app`
# ---------------------------------------------------------------------------


def imports_of_app(source: str) -> list[str]:
    """Every way `source` imports the application: `import app[...]`,
    `from app[...] import`, importlib.import_module("app...") and
    __import__("app...") -- by the syntax tree, so a word in a docstring
    or a comment is not one."""

    def is_app(name: str | None) -> bool:
        return name is not None and (name == "app" or name.startswith("app."))

    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            found += [f"import {a.name}" for a in node.names if is_app(a.name)]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and is_app(
            node.module,
        ):
            found.append(f"from {node.module} import")
        elif isinstance(node, ast.Call) and node.args:
            func = node.func
            name = (
                func.id if isinstance(func, ast.Name)
                else func.attr if isinstance(func, ast.Attribute) else ""
            )
            first = node.args[0]
            if (
                name in ("import_module", "__import__")
                and isinstance(first, ast.Constant)
                and isinstance(first.value, str) and is_app(first.value)
            ):
                found.append(f"{name}({first.value!r})")
    return found


def _later_migrations() -> list[Path]:
    later = []
    for path in sorted(VERSIONS.glob("*.py")):
        number = re.search(r"_(\d{4})_", path.name)
        assert number, path.name
        if int(number.group(1)) > _IMPORTS_CHECKED_AFTER:
            later.append(path)
    return later


class TestNoLaterMigrationImportsApp:
    """A migration of v4 is run by whatever code `app` holds when it runs:
    one that imports `app` changes with that code (0037 and 0038 import
    app.engine.constants; held by the climb above). After 0041, none does."""

    def test_none_does(self) -> None:
        found = {
            path.name: imports_of_app(path.read_text(encoding="utf-8"))
            for path in _later_migrations()
        }
        assert {name: hits for name, hits in found.items() if hits} == {}

    def test_the_scanner_reads_the_tree(self) -> None:
        """The pair: the migrations that do import are found."""
        hits = {
            re.search(r"_(\d{4})_", path.name).group(1):  # type: ignore[union-attr]
            imports_of_app(path.read_text(encoding="utf-8"))
            for path in sorted(VERSIONS.glob("*.py"))
        }
        assert {number for number, found in hits.items() if found} == {
            "0021", "0037", "0038",
        }

    @pytest.mark.parametrize("body", [
        "import app",
        "import app.engine.constants",
        "import app.engine.constants as constants",
        "from app.engine.constants import PipelineStep",
        "from app import cli",
        "import importlib\nimportlib.import_module('app.engine.constants')",
        "from importlib import import_module\nimport_module('app')",
        "__import__('app.engine')",
        "def upgrade():\n    from app.engine.constants import PipelineStep",
    ])
    def test_every_form_is_found(self, body: str) -> None:
        assert imports_of_app(body) != []

    @pytest.mark.parametrize("body", [
        "from alembic import op",
        "import apparatus",
        "from application import x",
        "import importlib\nimportlib.import_module('apple')",
        '"""from app.engine import constants -- prose, not code."""',
        "# import app",
    ])
    def test_the_pair_what_is_not_an_import_of_app(self, body: str) -> None:
        assert imports_of_app(body) == []
