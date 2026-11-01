# =============================================================================
# FIX TX-3 -- no migration checks existing rows under a lock that stops
# writes (the rule in migrations/env.py's header)
# =============================================================================
#
# Every migration after 0036 (those already in main are history, not
# checked) is RUN against a recording `op` -- upgrade() and downgrade() --
# so SQL built by f-strings, loops and helpers is seen as it is sent:
#   (a) ADD CONSTRAINT ... CHECK on a table the file did not create: NOT
#       VALID, and VALIDATE CONSTRAINT of that name inside an
#       autocommit_block of the same migration;
#   (b) a FOREIGN KEY the same way (ADD CONSTRAINT ... FOREIGN KEY,
#       op.create_foreign_key); ADD COLUMN ... REFERENCES cannot be NOT
#       VALID and is refused -- add the column, then the key;
#   (c) SET NOT NULL / op.alter_column(nullable=False) only after a
#       validated CHECK (col IS NOT NULL) in the same migration: Postgres
#       12+ then skips its own pass.
# A table created in the same file has no existing rows: exempt.
# Those rules are upgrade()'s. downgrade() has its own: NO autocommit_block
# -- alembic moves alembic_version after the step returns, and a partial
# commit in a downgrade leaves the stamp ahead of the schema. And what
# upgrade() sends BEFORE its block must be idempotent (IF NOT EXISTS, a
# constraint dropped IF EXISTS first): a failure later leaves the stamp
# behind the schema, and upgrade runs again (migrations/env.py's header).
#
# MUTATIONS: a CHECK of 0037 added without NOT VALID, or its VALIDATE out
# of the autocommit block -> TestEveryMigration red.
# =============================================================================

import importlib.util
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

VERSIONS = Path(__file__).resolve().parents[1] / "migrations" / "versions"
_HISTORY_UNTIL = 36  # in main: 0036 and before are not checked


@dataclass
class _Event:
    kind: str  # "sql" | "check" | "fk" | "not_null" | "table" | "fk_column"
    text: str = ""
    name: str = ""
    table: str = ""
    column: str = ""
    not_valid: bool = False
    in_block: bool = False


@dataclass
class _Recorder:
    events: list[_Event] = field(default_factory=list)
    in_block: bool = False
    blocks: int = 0

    # --- what a migration calls -------------------------------------------
    def execute(self, sql: Any, *_a: Any, **_k: Any) -> None:
        self.events.append(_Event("sql", text=" ".join(str(sql).split()),
                                  in_block=self.in_block))

    def get_context(self) -> "_Recorder":
        return self

    @contextmanager
    def autocommit_block(self) -> Iterator[None]:
        self.blocks += 1
        self.in_block = True
        try:
            yield
        finally:
            self.in_block = False

    def create_check_constraint(self, name: str, table: str, condition: Any,
                                **kw: Any) -> None:
        self.events.append(_Event(
            "check", name=name, table=table, text=str(condition),
            not_valid=bool(kw.get("postgresql_not_valid"))))

    def create_foreign_key(self, name: str, table: str, *_a: Any, **kw: Any) -> None:
        self.events.append(_Event(
            "fk", name=name, table=table,
            not_valid=bool(kw.get("postgresql_not_valid"))))

    def alter_column(self, table: str, column: str, **kw: Any) -> None:
        if kw.get("nullable") is False:
            self.events.append(_Event("not_null", table=table, column=column))

    def create_table(self, table: str, *_a: Any, **_k: Any) -> None:
        self.events.append(_Event("table", table=table))

    def add_column(self, table: str, column: Any, **_k: Any) -> None:
        if getattr(column, "foreign_keys", None):
            self.events.append(_Event("fk_column", table=table,
                                      column=getattr(column, "name", "")))

    def __getattr__(self, name: str) -> Any:
        return lambda *a, **k: None  # drop_*, create_index, ...


_CHECK = re.compile(
    r"ALTER TABLE (?:ONLY )?\"?(\w+)\"? .*?ADD CONSTRAINT \"?(\w+)\"? CHECK \((.*)\)",
    re.I)
_FK = re.compile(
    r"ALTER TABLE (?:ONLY )?\"?(\w+)\"? .*?ADD CONSTRAINT \"?(\w+)\"? FOREIGN KEY",
    re.I)
_FK_COLUMN = re.compile(r"ALTER TABLE \"?(\w+)\"? .*ADD COLUMN .*REFERENCES", re.I)
_NOT_NULL = re.compile(
    r"ALTER TABLE \"?(\w+)\"? .*ALTER COLUMN \"?(\w+)\"? SET NOT NULL", re.I)
_VALIDATE = re.compile(r"VALIDATE CONSTRAINT \"?(\w+)\"?", re.I)
_CREATE = re.compile(r"CREATE TABLE (?:IF NOT EXISTS )?\"?(\w+)\"?", re.I)


def _violations(events: list[_Event]) -> list[str]:
    created = {e.table for e in events if e.kind == "table"}
    created |= {m.group(1) for e in events if e.kind == "sql"
                for m in [_CREATE.search(e.text)] if m}
    validated_in_block = {
        m.group(1) for e in events if e.kind == "sql" and e.in_block
        for m in _VALIDATE.finditer(e.text)
    }
    found: list[str] = []
    not_null_checks: dict[tuple[str, str], str] = {}
    for e in events:
        if e.kind == "sql":
            check, fk = _CHECK.search(e.text), _FK.search(e.text)
            for match, what in ((check, "CHECK"), (fk, "FOREIGN KEY")):
                if not match or match.group(1) in created:
                    continue
                table, name = match.group(1), match.group(2)
                if "NOT VALID" not in e.text.upper():
                    found.append(f"{what} {name} on {table} without NOT VALID")
                elif name not in validated_in_block:
                    found.append(f"{what} {name} on {table} never VALIDATEd "
                                 f"in an autocommit_block")
                elif what == "CHECK":
                    column = re.fullmatch(r"\(?\s*\"?(\w+)\"? IS NOT NULL\s*\)?",
                                          match.group(3).strip(), re.I)
                    if column:
                        not_null_checks[(table, column.group(1))] = name
            column_fk = _FK_COLUMN.search(e.text)
            if column_fk and column_fk.group(1) not in created:
                found.append(f"ADD COLUMN ... REFERENCES on {column_fk.group(1)}")
            not_null = _NOT_NULL.search(e.text)
            if not_null and not_null.group(1) not in created and (
                    (not_null.group(1), not_null.group(2)) not in not_null_checks):
                found.append(f"SET NOT NULL {not_null.group(1)}.{not_null.group(2)} "
                             f"without a validated CHECK first")
        elif e.kind in ("check", "fk") and e.table not in created:
            what = "CHECK" if e.kind == "check" else "FOREIGN KEY"
            if not e.not_valid:
                found.append(f"{what} {e.name} on {e.table} without NOT VALID")
            elif e.name not in validated_in_block:
                found.append(f"{what} {e.name} on {e.table} never VALIDATEd "
                             f"in an autocommit_block")
            elif e.kind == "check":
                column = re.fullmatch(r"\s*\"?(\w+)\"? IS NOT NULL\s*", e.text, re.I)
                if column:
                    not_null_checks[(e.table, column.group(1))] = e.name
        elif e.kind == "fk_column" and e.table not in created:
            found.append(f"a column with a FOREIGN KEY added to {e.table}")
        elif e.kind == "not_null" and e.table not in created and (
                (e.table, e.column) not in not_null_checks):
            found.append(f"SET NOT NULL {e.table}.{e.column} without a "
                         f"validated CHECK first")
    return found


_IDEMPOTENT = (
    re.compile(r"ADD COLUMN IF NOT EXISTS", re.I),
    re.compile(r"DROP (?:CONSTRAINT|COLUMN|INDEX|TABLE) IF EXISTS", re.I),
    re.compile(r"CREATE (?:UNIQUE )?(?:INDEX|TABLE) IF NOT EXISTS", re.I),
)
_NOT_IDEMPOTENT = re.compile(
    r"ADD COLUMN (?!IF NOT EXISTS)|CREATE (?:UNIQUE )?(?:INDEX|TABLE) "
    r"(?!IF NOT EXISTS)|DROP (?:COLUMN|INDEX|TABLE) (?!IF EXISTS)", re.I)


def _not_idempotent_before_block(events: list[_Event]) -> list[str]:
    """In an upgrade() with an autocommit_block: every statement before
    it can run twice. An ADD CONSTRAINT counts when the same name was
    dropped IF EXISTS just before."""
    found = []
    dropped: set[str] = set()
    for e in events:
        if e.in_block:
            break
        if e.kind != "sql":
            if e.kind in ("check", "fk") and e.name in dropped:
                continue
            found.append(f"{e.kind} {e.name or e.table} via op before the block")
            continue
        drop = re.search(r"DROP CONSTRAINT IF EXISTS \"?(\w+)", e.text, re.I)
        if drop:
            dropped.add(drop.group(1))
        add = re.search(r"ADD CONSTRAINT \"?(\w+)", e.text, re.I)
        if add and add.group(1) not in dropped:
            found.append(f"ADD CONSTRAINT {add.group(1)} not dropped IF EXISTS first")
        if _NOT_IDEMPOTENT.search(e.text):
            found.append(f"not idempotent: {e.text[:80]}")
    return found


def _load(path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _record(path: Path, step: str) -> _Recorder:
    module = _load(path)
    recorder = _Recorder()
    module.op = recorder
    getattr(module, step)()
    return recorder


def _run(path: Path) -> list[str]:
    found = []
    up = _record(path, "upgrade")
    found += [f"upgrade: {v}" for v in _violations(up.events)]
    if up.blocks:
        found += [f"upgrade: {v}" for v in _not_idempotent_before_block(up.events)]
    down = _record(path, "downgrade")
    if down.blocks:
        found.append("downgrade: an autocommit_block -- the stamp would run "
                     "ahead of the schema")
    return found


def uses_a_block(path: Path) -> bool:
    """Whether upgrade() commits part of itself (an autocommit_block) --
    the migrations tests/test_migration_0037.py re-runs after a stamp
    left behind."""
    return _record(path, "upgrade").blocks > 0


def _later_migrations() -> list[Path]:
    later = []
    for path in sorted(VERSIONS.glob("*.py")):
        match = re.search(r"_(\d{4})_", path.name)
        if match and int(match.group(1)) > _HISTORY_UNTIL:
            later.append(path)
    return later


class TestEveryMigration:
    def test_there_is_one_to_check(self) -> None:
        assert [p.name for p in _later_migrations()]  # 0037 today

    @pytest.mark.parametrize("path", _later_migrations(), ids=lambda p: p.name)
    def test_no_pass_under_a_write_stopping_lock(self, path: Path) -> None:
        assert _run(path) == []


_HEADER = "from alembic import op\nimport sqlalchemy as sa\n\n"


def _synthetic(tmp_path: Path, body: str, down: str = "    pass") -> list[str]:
    path = tmp_path / "2026_01_01_9999_synthetic.py"
    path.write_text(_HEADER + body + "\n\ndef downgrade():\n" + down + "\n")
    return _run(path)


class TestTheGuardSeesEveryForm:
    @pytest.mark.parametrize("body", [
        'def upgrade():\n    op.execute("ALTER TABLE notifications ADD CONSTRAINT '
        'ck_x CHECK (a > 0)")',
        'def upgrade():\n    op.execute("ALTER TABLE notifications ADD CONSTRAINT '
        'ck_x CHECK (a > 0) NOT VALID")\n    op.execute("ALTER TABLE notifications '
        'VALIDATE CONSTRAINT ck_x")',  # validated, but not in the block
        'def upgrade():\n    op.create_check_constraint("ck_x", "recipients", '
        '"a > 0")',
        'def upgrade():\n    op.execute("ALTER TABLE threads ADD CONSTRAINT fk_x '
        'FOREIGN KEY (a) REFERENCES recipients (id)")',
        'def upgrade():\n    op.create_foreign_key("fk_x", "threads", "recipients", '
        '["a"], ["id"])',
        'def upgrade():\n    op.execute("ALTER TABLE threads ADD COLUMN a uuid '
        'REFERENCES recipients (id)")',
        'def upgrade():\n    op.add_column("threads", sa.Column("a", sa.Uuid, '
        'sa.ForeignKey("recipients.id")))',
        'def upgrade():\n    op.execute("ALTER TABLE notifications ALTER COLUMN a '
        'SET NOT NULL")',
        'def upgrade():\n    op.alter_column("notifications", "a", nullable=False)',
    ], ids=["check-sql", "check-validate-outside", "check-op", "fk-sql", "fk-op",
            "fk-column-sql", "fk-column-op", "not-null-sql", "not-null-op"])
    def test_each_form_without_its_safety_is_red(
        self, tmp_path: Path, body: str,
    ) -> None:
        assert _synthetic(tmp_path, body) != []

    @pytest.mark.parametrize("body", [
        'def upgrade():\n    op.execute("ALTER TABLE notifications DROP CONSTRAINT '
        'IF EXISTS ck_x")\n    op.execute("ALTER TABLE notifications ADD CONSTRAINT '
        'ck_x CHECK (a > 0) NOT VALID")\n    with op.get_context().'
        'autocommit_block():\n        op.execute("ALTER TABLE notifications '
        'VALIDATE CONSTRAINT ck_x")',
        'def upgrade():\n    op.execute("ALTER TABLE threads DROP CONSTRAINT IF '
        'EXISTS fk_x")\n    op.create_foreign_key("fk_x", "threads", "recipients", '
        '["a"], ["id"], postgresql_not_valid=True)\n    with op.get_context().'
        'autocommit_block():\n        op.execute("ALTER TABLE threads VALIDATE '
        'CONSTRAINT fk_x")',
        'def upgrade():\n    op.execute("ALTER TABLE notifications DROP CONSTRAINT '
        'IF EXISTS ck_a")\n    op.execute("ALTER TABLE notifications ADD CONSTRAINT '
        'ck_a CHECK (a IS NOT NULL) NOT VALID")\n    with op.get_context().'
        'autocommit_block():\n        op.execute("ALTER TABLE notifications '
        'VALIDATE CONSTRAINT ck_a")\n    op.execute("ALTER TABLE notifications '
        'ALTER COLUMN a SET NOT NULL")',
        'def upgrade():\n    op.execute("CREATE TABLE fresh (a int)")\n    '
        'op.execute("ALTER TABLE fresh ADD CONSTRAINT ck_x CHECK (a > 0)")',
        'def upgrade():\n    op.create_table("fresh")\n    '
        'op.create_check_constraint("ck_x", "fresh", "a > 0")',
    ], ids=["check-safe", "fk-safe", "not-null-safe", "new-table-sql",
            "new-table-op"])
    def test_the_pair_each_safe_form_is_green(
        self, tmp_path: Path, body: str,
    ) -> None:
        assert _synthetic(tmp_path, body) == []


class TestTheTwoDirections:
    def test_a_block_in_downgrade_is_red(self, tmp_path: Path) -> None:
        down = ('    with op.get_context().autocommit_block():\n'
                '        op.execute("ALTER TABLE notifications VALIDATE '
                'CONSTRAINT ck_x")')
        assert _synthetic(tmp_path, "def upgrade():\n    pass", down) == [
            "downgrade: an autocommit_block -- the stamp would run ahead of "
            "the schema"]

    def test_a_plain_check_in_downgrade_is_green(self, tmp_path: Path) -> None:
        """downgrade() is one transaction: a full pass is its price."""
        down = ('    op.execute("ALTER TABLE notifications ADD CONSTRAINT ck_x '
                'CHECK (a > 0)")')
        assert _synthetic(tmp_path, "def upgrade():\n    pass", down) == []

    def test_a_step_before_the_block_that_cannot_run_twice_is_red(
        self, tmp_path: Path,
    ) -> None:
        body = ('def upgrade():\n    op.execute("ALTER TABLE notifications ADD '
                'COLUMN a int")\n    with op.get_context().autocommit_block():'
                '\n        pass')
        assert any("not idempotent" in v for v in _synthetic(tmp_path, body))

    def test_the_pair_an_idempotent_step_before_the_block(
        self, tmp_path: Path,
    ) -> None:
        body = ('def upgrade():\n    op.execute("ALTER TABLE notifications ADD '
                'COLUMN IF NOT EXISTS a int")\n    with op.get_context().'
                'autocommit_block():\n        pass')
        assert _synthetic(tmp_path, body) == []
