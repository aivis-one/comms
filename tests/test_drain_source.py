# =============================================================================
# COMMS Service -- The drain verb and its one source (F1.5)
# =============================================================================
# `deploy/comms-deploy.sh drain` runs migration 0013's OWN queries. This
# file holds that true and runs the command's driver -- the python it
# feeds to a one-off container on the box -- against this suite's
# database, put back at revision 0011 (where a box that ran `main`
# stays after 0013 refuses: all migrations are one transaction).
#
# What is NOT exercised here, because no docker daemon runs in the
# suite: the compose plumbing around the driver (stop, dump, `run
# --rm`). That half is checked by `bash -n` and by reading; the box run
# is the owner's.
# =============================================================================

import importlib.util
import os
import re
import subprocess
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from types import ModuleType
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text

from app.core.database import dispose_engine, get_session_factory

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "deploy" / "comms-deploy.sh"
DOC = REPO_ROOT / "deploy" / "INTEGRATION.md"
MIGRATION = next((REPO_ROOT / "migrations" / "versions").glob("*_0013_*.py"))


def _m0013() -> ModuleType:
    spec = importlib.util.spec_from_file_location("m0013_under_test", MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _driver_source() -> str:
    """The python the script feeds to the container, as written there."""
    body = SCRIPT.read_text(encoding="utf-8")
    match = re.search(r"<<'PY'\n(.*?)\nPY\n", body, re.S)
    assert match, "the driver heredoc is gone from comms-deploy.sh"
    return match.group(1)


# -----------------------------------------------------------------------------
# One source for the breakdown
# -----------------------------------------------------------------------------


def _refusal_with_one_row(module: ModuleType) -> None:
    """Run the migration's refusal against a stub connection that counts
    one row of every kind -- the text, without a database."""

    class _Result:
        def scalar_one(self) -> int:
            return 1

    class _Bind:
        def execute(self, _statement: Any) -> _Result:
            return _Result()

    original = module.op.get_bind
    module.op.get_bind = lambda: _Bind()  # type: ignore[attr-defined]
    try:
        module._refuse_on_ambiguous_rows()
    finally:
        module.op.get_bind = original  # type: ignore[attr-defined]


class TestOneSource:
    def test_every_counted_kind_has_its_deletion(self) -> None:
        module = _m0013()
        assert set(module._DRAIN_DELETES) == set(module._BLOCKING_KINDS)
        assert len(module._BLOCKING_KINDS) == 6

    def test_the_driver_reads_the_migration_not_a_copy(self) -> None:
        driver = _driver_source()
        assert "_BLOCKING_KINDS" in driver and "_DRAIN_DELETES" in driver
        assert "*_0013_*.py" in driver
        # No SQL of the breakdown lives in the script itself.
        script = SCRIPT.read_text(encoding="utf-8")
        assert "DELETE FROM notifications" not in script
        assert "status = 'skipped'" not in script

    def test_the_refusal_names_the_verb(self) -> None:
        """Asserted on the refusal as the operator reads it, not on how
        the source happens to wrap it: the first version matched the
        line break inside the string literal and broke on the first
        sentence added before it (F1.6)."""
        module = _m0013()
        with pytest.raises(RuntimeError) as excinfo:
            _refusal_with_one_row(module)
        refusal = str(excinfo.value)
        assert "`deploy/comms-deploy.sh drain`" in refusal
        assert "The protocol update window" in refusal


class TestTheDocument:
    def _window(self) -> str:
        doc = DOC.read_text(encoding="utf-8")
        start = doc.index("## The protocol update window")
        return doc[start : doc.index("\n## ", start + 1)]

    def test_the_step_carries_no_raw_command(self) -> None:
        window = self._window()
        for raw in ("docker exec", "psql", "DELETE FROM", "SELECT"):
            assert raw not in window, raw
        # The pair: the verbs ARE there.
        for verb in (
            "**`update`**",
            "**`drain`**",
            "**`drain --apply`**",
            "**`start`**",
        ):
            assert verb in window, verb

    def test_every_command_of_the_window_exists_in_the_script(self) -> None:
        verbs = _verbs(SCRIPT.read_text(encoding="utf-8"))
        for verb in ("update", "drain", "start"):
            assert verb in verbs, verb


# -----------------------------------------------------------------------------
# The dispatcher rule the products parse by
# -----------------------------------------------------------------------------


def _verbs(script: str) -> set[str]:
    """velo's svc_verbs, transcribed: the labels of the column-zero case."""
    labels: set[str] = set()
    inblock = False
    for line in script.splitlines():
        if re.match(r"^case\s", line):
            inblock = True
            continue
        if inblock and line.startswith("esac"):
            inblock = False
            continue
        if inblock:
            match = re.match(r"^\s*([a-z][a-z0-9|_-]*)\)", line)
            if match:
                labels.update(p for p in match.group(1).split("|") if p)
    return labels


def test_the_verbs_are_the_old_ones_plus_drain() -> None:
    before = {
        "install",
        "update",
        "start",
        "stop",
        "restart",
        "logs",
        "db",
        "test",
        "status",
    }
    assert _verbs(SCRIPT.read_text(encoding="utf-8")) == before | {"drain"}


def test_the_dispatcher_is_the_only_case_at_column_zero() -> None:
    """The rule itself, not only today's labels: velo's parser skips a
    label starting with '-', so a nested `case` of flags at column zero
    would pass the verb test by luck -- and the next nested case with a
    label like `dump)` would leak. Exactly one `case` sits at column
    zero, and it is the dispatcher (mutation D3 found the gap)."""
    lines = SCRIPT.read_text(encoding="utf-8").splitlines()
    column_zero = [line for line in lines if re.match(r"^case\s", line)]
    assert column_zero == ['case "${1:-}" in']


def test_nested_labels_are_not_verbs() -> None:
    verbs = _verbs(SCRIPT.read_text(encoding="utf-8"))
    for nested in ("dump", "restore", "migrate", "apply", "--apply"):
        assert nested not in verbs


# -----------------------------------------------------------------------------
# The driver, run against this suite's database at revision 0011
# -----------------------------------------------------------------------------


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def _drive(mode: str, cwd: Path = REPO_ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-", mode],
        input=_driver_source(),
        cwd=cwd,
        capture_output=True,
        text=True,
        env=dict(os.environ),
    )


async def _migrate(*args: str) -> None:
    await dispose_engine()
    done = _alembic(*args)
    await dispose_engine()
    assert done.returncode == 0, done.stderr


async def _sql(statement: str, **params: Any) -> None:
    async with get_session_factory()() as session:
        await session.execute(text(statement), params)
        await session.commit()


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    yield
    await _sql("DELETE FROM notifications")
    await _migrate("upgrade", "head")


async def _seed_every_kind_at_0011() -> dict[str, str]:
    rid = uuid4()
    await _sql(
        "INSERT INTO recipients (id, telegram_id, email, locale, timezone, "
        "active) VALUES (:r, NULL, NULL, 'en', NULL, true)",
        r=rid,
    )

    async def job(status: str) -> str:
        nid = uuid4()
        await _sql(
            "INSERT INTO notifications (id, type, title, body, target_type, "
            "target_value, status) VALUES (:n, 'unit_event', 'T', 'B', 'all', "
            "'*', :s)",
            n=nid,
            s=status,
        )
        return str(nid)

    async def child(nid: str, status: str) -> None:
        await _sql(
            "INSERT INTO notification_deliveries (id, notification_id, "
            "recipient_id, channel, status) VALUES (:d, :n, :r, 'in_app', :s)",
            d=uuid4(),
            n=nid,
            r=rid,
            s=status,
        )

    await job("pending")  # active_jobs
    await job("skipped")  # skipped_without_children
    mixed = await job("skipped")  # skipped_mixed_children
    await child(mixed, "skipped")
    await child(mixed, "sent")
    await job("expired")  # expired
    await job("failed")  # failed_without_children
    partial = await job("partial_sent")  # with_failed_delivery
    await child(partial, "sent")
    await child(partial, "failed")
    kept = await job("sent")  # sent history -- must stay
    await child(kept, "sent")
    return {"kept": kept}


async def test_drain_on_0011_then_the_update_passes(
    at_head_afterwards: None,
) -> None:
    await _sql("DELETE FROM notifications")
    await _migrate("downgrade", "0011_delivery_schedule")
    seeded = await _seed_every_kind_at_0011()

    # The red start the window begins with: 0013 refuses, and the base
    # stays at 0011 (one transaction), not at 0012.
    refused = _alembic("upgrade", "head")
    assert refused.returncode != 0
    assert "comms-deploy.sh drain" in refused.stderr
    await dispose_engine()

    check = _drive("check")
    assert check.returncode == 1, check.stdout + check.stderr
    assert "schema at 0011_delivery_schedule" in check.stdout
    for kind in _m0013()._BLOCKING_KINDS:
        assert f"  {kind}=1" in check.stdout, kind

    applied = _drive("apply")
    assert applied.returncode == 0, applied.stdout + applied.stderr
    for kind in _m0013()._BLOCKING_KINDS:
        assert f"  {kind}: 1 -> 0" in applied.stdout, kind

    again = _drive("check")
    assert again.returncode == 0 and "=1" not in again.stdout
    repeat = _drive("apply")  # a second run: no action
    assert repeat.returncode == 0

    await _migrate("upgrade", "head")  # step 4: the migration passes
    async with get_session_factory()() as session:
        status = await session.scalar(
            text("SELECT status FROM notifications WHERE id = :n"),
            {"n": seeded["kept"]},
        )
    assert status == "sent"


async def test_drain_refuses_past_0013() -> None:
    refused = _drive("check")
    assert refused.returncode == 2
    assert "at or past 0013" in refused.stdout


def test_drain_refuses_an_image_without_0013(tmp_path: Path) -> None:
    (tmp_path / "migrations" / "versions").mkdir(parents=True)
    refused = _drive("check", cwd=tmp_path)
    assert refused.returncode == 2
    assert "no migration 0013" in refused.stdout
