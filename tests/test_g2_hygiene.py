# =============================================================================
# G2 -- hygiene: texts that lie, the type record in the contract, the
# `test` verb on a box, statements found by their shape
# =============================================================================
#
#   1 every reference to a file -- and to a name in it -- resolves
#   2 deploy/INTEGRATION.md section 3 holds the type record, field by
#     field, and the loader holds the table (both ways)
#   3 `comms-deploy.sh test` runs the suite on comms-app's image with
#     the checkout mounted and CI's variables only
#   4 no test finds a SQL statement by a substring; tests/helpers.py
#     sql_shape is the one reader, with its own twins
#
# (Items 5 and 6 are prose and a premise: tests/helpers.py's header,
# tests/test_h9_release.py _sends_ids and its premise check.)
#
# MUTATIONS (one run, every one red here):
#   M1 a reference to app/engine/service.py list_changes back -> test_every_
#                                                                 reference_resolves
#   M2 the scanner without the line-break form          -> test_the_scanner_
#                                                           sees_every_form
#   M3 the lane default changed in the loader           -> test_the_defaults_
#                                                           are_the_loaders
#   M4 a row removed from the table                     -> test_the_rows_are_
#                                                           the_record
#   M5 cmd_test back to `exec comms-app python -m pytest` -> TestTheTestVerb
#   M6 a test finds a statement by "ORDER BY" in s       -> test_no_test_
#                                                           searches_sql_by_substring
#   M7 sql_shape takes the first FROM at any depth       -> TestSqlShape
#   M8 (tests/test_h9_release.py) the greater id given   -> the premise check
#      to the other send                                    there
#   FIX (3.7.0, a box's suite saw the live worker's listener):
#   M9 a pg_stat_activity query without datname          -> test_every_
#                                                           activity_query_is_
#                                                           this_databases
# =============================================================================

import ast
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest

from app.core.config import NUMERIC_BOUNDS
from app.core.constants import MAX_BODY_LEN, MAX_CATEGORY_LEN, MSG_TYPE_KEYS
from app.core.exceptions import ProfileError
from app.profile import loader
from app.profile.loader import (
    CHANNEL_NAMES,
    LANES,
    PUSH_ON,
    RECIPIENT_SCHEDULE,
    RECORD_FIELDS,
    SCHEMA_VERSION,
    RawProfile,
    parse_profile,
)
from tests.helpers import sql_shape

_REPO = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Item 1 -- every reference resolves
# ---------------------------------------------------------------------------

_ROOTS = ("app", "tests", "scripts", "deploy", ".github")
_APP_PACKAGES = "audience|engine|api|core|messaging|profile|transport"
_REFERENCE = re.compile(
    r"(?<![\w/.-])("
    r"(?:app|tests|scripts|deploy|\.github|migrations)/[\w./-]*[\w-]"
    r"\.(?:py|md|sh|yml|yaml)"
    rf"|(?:{_APP_PACKAGES})/[\w./-]*[\w-]\.py"
    r")"
    # a name after it: a space, a comma or `::`, maybe a line break
    # into the next comment line, maybe a backquote
    r"(?:(?:[ \t]*(?:,|::|:)?(?:[ \t]*\n[ \t]*#?)?[ \t]*`?)"
    r"([a-z_]*[a-z][a-z0-9_]*)(?![A-Za-z0-9])(\(\))?)?"
)

# References that do not resolve, on purpose -- (file, reference) -> why.
NOT_A_REFERENCE: dict[tuple[str, str], str] = {
    ("app/profile/__init__.py", "engine/profile.py"):
        "history: where the loader lived before the move",
    ("app/profile/__init__.py", "app/profile/profile.py"):
        "the name the move did NOT take (it would stutter)",
    ("tests/test_lane_depth.py", "app/x.py"):
        "a made-up place inside a fixture's pipeline_error value",
}


def _defined(path: Path, cache: dict[Path, set[str]]) -> set[str]:
    if path not in cache:
        names: set[str] = set()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
            ):
                names.add(node.name)
            elif isinstance(node, ast.Assign):
                names |= {t.id for t in node.targets if isinstance(t, ast.Name)}
            elif isinstance(node, ast.AnnAssign) and isinstance(
                node.target, ast.Name,
            ):
                names.add(node.target.id)
        cache[path] = names
    return cache[path]


def scan_references(
    text: str, source: str, repo: Path = _REPO,
) -> list[tuple[str, int, str, str | None]]:
    """Every reference in `text` that does not resolve: (source, line,
    reference, the name that is not there -- or None: no such file).

    A name is checked when it is snake_case or written as a call:
    single lowercase words after a path are prose too often to read."""
    cache: dict[Path, set[str]] = {}
    broken: list[tuple[str, int, str, str | None]] = []
    for match in _REFERENCE.finditer(text):
        reference, name, call = match.groups()
        line = text.count("\n", 0, match.start()) + 1
        head = reference.split("/")[0]
        target = (
            repo / reference if head in (*_ROOTS, "migrations")
            else repo / "app" / reference
        )
        if not target.exists():
            broken.append((source, line, reference, None))
        elif (
            name and target.suffix == ".py" and ("_" in name or call)
            and name not in _defined(target, cache)
        ):
            broken.append((source, line, reference, name))
    return broken


def _sources() -> list[Path]:
    """Every text the watchman reads -- but this file: its references
    are the scanner's own fixtures, broken on purpose."""
    return [
        path
        for root in _ROOTS
        for path in sorted((_REPO / root).rglob("*"))
        if path.is_file()
        and path.suffix in (".py", ".md", ".sh", ".yml", ".yaml")
        and "__pycache__" not in path.parts
        and path != Path(__file__).resolve()
    ]


class TestEveryReferenceResolves:
    def test_every_reference_resolves(self) -> None:
        """Both ways: a broken reference is red, and so is a listed
        exception that no longer occurs. The pair: the walk reads the
        tree (hundreds of sources, the known exceptions found)."""
        sources = _sources()
        assert len(sources) > 100
        broken = [
            hit for path in sources
            for hit in scan_references(
                path.read_text(encoding="utf-8"),
                str(path.relative_to(_REPO)),
            )
        ]
        listed = {(source, reference) for source, _, reference, _ in broken}
        unexplained = [
            hit for hit in broken if (hit[0], hit[2]) not in NOT_A_REFERENCE
        ]
        assert unexplained == [], f"references that do not resolve: {unexplained}"
        assert set(NOT_A_REFERENCE) == listed, "a listed exception is gone"

    def test_the_scanner_sees_every_form(self, tmp_path: Path) -> None:
        """The instrument's own twin, on a tree of its own."""
        (tmp_path / "app" / "engine").mkdir(parents=True)
        (tmp_path / "app" / "engine" / "real.py").write_text(
            "def real_name():\n    pass\nCONSTANT = 1\n",
        )
        text = "\n".join([
            "# see app/engine/real.py real_name -- resolves",           # 1
            "# see app/engine/real.py ghost_name -- not there",         # 2
            "# see app/engine/gone.py -- no such file",                 # 3
            "# see engine/real.py::ghost_two, the app-relative form",   # 4
            "# see (app/engine/real.py,",                               # 5
            "#      ghost_three) across a line break",                  # 6
            "# see app/engine/real.py THE HEADER -- a heading word",    # 7
            "# see app/engine/real.py holds -- a prose word",           # 8
            "# see app/engine/real.py render() -- a call",              # 9
        ])
        assert scan_references(text, "s", tmp_path) == [
            ("s", 2, "app/engine/real.py", "ghost_name"),
            ("s", 3, "app/engine/gone.py", None),
            ("s", 4, "engine/real.py", "ghost_two"),
            ("s", 5, "app/engine/real.py", "ghost_three"),
            ("s", 9, "app/engine/real.py", "render"),
        ]


# ---------------------------------------------------------------------------
# Item 2 -- the type record, field by field
# ---------------------------------------------------------------------------

_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


def _record_rows() -> dict[str, list[str]]:
    section = _DOC[_DOC.index("\n## 3. "):_DOC.index("\n## 3a. ")]
    block = section.split("\n### The type record -- every field", 1)[1]
    block = block.split("\n### ", 1)[0]
    lines = [line for line in block.splitlines() if line.startswith("|")]
    header = [cell.strip() for cell in lines[0].split("|")[1:-1]]
    assert header == ["field", "value", "when not declared", "what it does"]
    rows: dict[str, list[str]] = {}
    for line in lines[2:]:
        cells = [cell.strip() for cell in line.split("|")[1:-1]]
        (name,) = re.findall(r"^`([a-z_]+)`$", cells[0])
        assert name not in rows, f"{name} twice in the table"
        rows[name] = cells
    return rows


def _ticked(cell: str) -> set[str]:
    return set(re.findall(r"`([^`]+)`", cell))


class TestTheTypeRecordTable:
    def test_the_rows_are_the_record(self) -> None:
        """Every field of the loader has its row, and no row is not a
        field. The pair: there are rows."""
        rows = _record_rows()
        assert len(rows) == len(RECORD_FIELDS) > 0
        assert set(rows) == set(RECORD_FIELDS)

    def test_the_defaults_are_the_loaders(self) -> None:
        """'when not declared' is what the loader hands out: the value,
        or the setting it reads it from."""
        rows = _record_rows()
        for name in RECORD_FIELDS:
            value, source = loader._FIELDS[name].default()
            cell = rows[name][2]
            if source.startswith("settings: "):
                assert f"`{source.removeprefix('settings: ')}`" in cell, name
            elif value in (None, ()):
                assert cell == "none", name
            elif isinstance(value, tuple):
                assert cell == f"`[{', '.join(value)}]`", name
            elif name == "answer_keep":
                assert cell == f"`{loader._DEFAULT_ANSWER_KEEP}`", name
            else:
                assert cell == f"`{value}`", name

    def test_the_closed_sets_are_the_codes(self) -> None:
        rows = _record_rows()
        assert _ticked(rows["lane"][1]) == set(LANES)
        assert _ticked(rows["push_on"][1]) == set(PUSH_ON)
        assert _ticked(rows["recipient_schedule"][1]) == set(RECIPIENT_SCHEDULE)
        assert _ticked(rows["channels"][1]) == set(CHANNEL_NAMES)

    def test_the_bounds_are_the_codes(self) -> None:
        rows = _record_rows()
        attempts = NUMERIC_BOUNDS["notification_max_delivery_attempts"]
        backoff = NUMERIC_BOUNDS["notification_retry_backoff_base_seconds"]
        assert f"{attempts.lo}..{attempts.hi}" in rows["retry_max_attempts"][1]
        assert f"{backoff.lo}..{backoff.hi}" in rows["retry_backoff_seconds"][1]
        assert f"1..{MAX_BODY_LEN}" in rows["max_body_chars"][1]
        assert f"1..{loader.MAX_LLM_AUDIO_BYTES}" in rows["max_audio_bytes"][1]
        assert f"at most {MAX_CATEGORY_LEN} characters" in rows["category"][1]
        for unit in loader._DURATION_UNITS:
            assert f"`{unit}`" in rows["expires_after"][1]

    def test_the_kinds_are_what_the_loader_refuses(self) -> None:
        """Which fields only an llm type takes, and which an llm type
        refuses, asked of the loader -- declared on a type for people
        and on an llm type in turn -- and compared with the marks."""
        samples: dict[str, Any] = {
            "category": "c", "lane": "bulk", "push_on": "outcome",
            "recipient_schedule": "bypass", "expires_after": "10m",
            "retry_max_attempts": 2, "retry_backoff_seconds": 1,
            "answer_keep": "1h", "max_body_chars": 10, "max_audio_bytes": 10,
        }
        rows = _record_rows()

        def accepted(record: dict[str, Any]) -> bool:
            types = {key: {"category": "msg_cat"} for key in MSG_TYPE_KEYS}
            types["t"] = record
            raw = RawProfile(types={"version": SCHEMA_VERSION, "types": types})
            try:
                parse_profile(raw, None, ("llm.text",))
            except ProfileError:
                return False
            return True

        person = {"channels": ["telegram"]}
        model = {"channels": ["llm"], "llm_sets": ["llm.text"]}
        assert accepted(person) and accepted(model)  # the pair
        assert not accepted({**person, "llm_sets": ["llm.text"]})
        assert "(the llm kind only, required there)" in rows["llm_sets"][1]
        for name, sample in samples.items():
            on_person = accepted({**person, name: sample})
            on_model = accepted({**model, name: sample})
            assert on_person or on_model, name
            assert ("(the llm kind only" in rows[name][1]) == (not on_person), name
            assert ("(not on an llm type)" in rows[name][1]) == (not on_model), name


# ---------------------------------------------------------------------------
# Item 3 -- `comms-deploy.sh test`
# ---------------------------------------------------------------------------

_SCRIPT = _REPO / "deploy" / "comms-deploy.sh"


def _function(name: str) -> str:
    text = _SCRIPT.read_text(encoding="utf-8")
    match = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text, re.S | re.M)
    assert match, f"{name} not found at column zero"
    return match.group(0)


_STUBS = r'''
RED= GREEN= CYAN= YELLOW= NC=
cd_compose() { :; }
load_env() { POSTGRES_DB=comms; POSTGRES_USER=comms; }
compose() {
    echo "compose $*" >> "$CALLS"
    case "$1 $2 $3" in
        "exec -T comms-app")
            echo "postgresql+asyncpg://comms:pw@comms-postgres:5432/comms" ;;
        "ps -q comms-app") [ "$APP_UP" = 1 ] && echo cid-1234 ;;
    esac
    return 0
}
docker() {
    echo "docker $*" >> "$CALLS"
    case "$1" in
        inspect) echo sha256:image-of-the-app ;;
        run) shift; printf '%s\n' "$@" > "$RUN_ARGS" ;;
    esac
    return 0
}
COMPOSE_CMD=compose
'''


def _run_test_verb(
    tmp_path: Path, *, app_up: bool = True, checkout: bool = True,
) -> tuple[subprocess.CompletedProcess[str], list[str], str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    if checkout:
        (repo / "pyproject.toml").write_text("")
        (repo / "tests").mkdir()
    calls, run_args = tmp_path / "calls", tmp_path / "run_args"
    calls.touch()
    program = "\n".join([
        "set -u",
        f"CALLS='{calls}' RUN_ARGS='{run_args}' APP_UP={int(app_up)}",
        f"REPO_DIR='{repo}'",
        _STUBS,
        _function("cmd_test"),
        "cmd_test",
    ])
    done = subprocess.run(
        ["bash", "-c", program], capture_output=True, text=True, timeout=30,
    )
    args = run_args.read_text().splitlines() if run_args.exists() else []
    return done, args, calls.read_text()


class TestTheTestVerb:
    def test_the_suite_runs_on_the_apps_image_with_the_checkout(
        self, tmp_path: Path,
    ) -> None:
        done, args, calls = _run_test_verb(tmp_path)
        assert done.returncode == 0, done.stdout + done.stderr
        repo = tmp_path / "repo"
        assert args[0] == "--rm"
        assert args[1:3] == ["--network", "container:cid-1234"]
        assert args[3:5] == ["-v", f"{repo}:/app:ro"]
        assert [a for a in args if a.startswith(("APP_ENV=", "DATABASE_URL=",
                                                 "COMMS_SERVICE_TOKEN="))] == [
            "APP_ENV=ci",
            "DATABASE_URL=postgresql+asyncpg://comms:pw@comms-postgres:5432/"
            "comms_test",
            "COMMS_SERVICE_TOKEN=comms-test-not-a-secret-token",
        ]
        # CI's variables and nothing else: three -e, no env file.
        assert args.count("-e") == 3 and "--env-file" not in args
        assert args[args.index("sha256:image-of-the-app"):] == [
            "sha256:image-of-the-app", "python", "-m", "pytest", "-q", "-p",
            "no:cacheprovider",
        ]
        # The pair: the suite does not run inside the live app.
        assert "compose exec -T comms-app python" not in calls
        assert "compose exec -T comms-postgres psql" in calls

    def test_no_running_app_is_refused(self, tmp_path: Path) -> None:
        done, args, _ = _run_test_verb(tmp_path, app_up=False)
        assert done.returncode == 1
        assert "comms-app is not running" in done.stdout
        assert args == []

    def test_no_checkout_is_refused(self, tmp_path: Path) -> None:
        done, args, _ = _run_test_verb(tmp_path, checkout=False)
        assert done.returncode == 1
        assert "no checkout at" in done.stdout
        assert args == []


# ---------------------------------------------------------------------------
# Item 4 -- statements are found by their shape
# ---------------------------------------------------------------------------

_SQL_WORDS = re.compile(
    r"\b(SELECT|EXISTS|FROM [a-z_]+|ORDER BY|FOR UPDATE|FOR NO KEY UPDATE|"
    r"SKIP LOCKED|LIMIT|WHERE|JOIN|UPDATE [a-z_]+|DELETE|INSERT INTO|EXPLAIN|"
    r"[a-z_]+\.[a-z_]+ =)"
)

# Substring tests of SQL-looking text that are NOT a search for a
# statement -- (file, function) -> what the text is.
NOT_A_STATEMENT_SEARCH: dict[tuple[str, str], str] = {
    ("tests/helpers.py", "sql_shape"):
        "the helper itself, over the clauses it has already split",
    ("tests/test_drain_source.py",
     "test_the_driver_reads_the_migration_not_a_copy"):
        "the source text of a shell script, not a statement sent",
    ("tests/test_migration_0012.py", "test_round_trip_on_every_form_of_row"):
        "an index definition read from pg_indexes -- the schema's layer",
    ("tests/test_transport_consumer.py", "test_every_retry_log_is_redacted"):
        "the text of a log entry",
}


def scan_sql_searches(source: str, path: str) -> set[tuple[str, str]]:
    """(path, function) of every `"<SQL words>" in x` / `not in x` and
    every `.startswith("<SQL words>")` in `source`."""
    tree = ast.parse(source)
    found: set[tuple[str, str]] = set()

    def sql(node: ast.AST) -> bool:
        return (
            isinstance(node, ast.Constant) and isinstance(node.value, str)
            and bool(_SQL_WORDS.search(node.value))
        )

    def visit(node: ast.AST, scope: str) -> None:
        for child in ast.iter_child_nodes(node):
            inner = scope
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                inner = child.name
            if (
                isinstance(child, ast.Compare)
                and any(isinstance(op, (ast.In, ast.NotIn)) for op in child.ops)
                and sql(child.left)
            ):
                found.add((path, inner))
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute)
                and child.func.attr in ("startswith", "endswith")
                and any(sql(arg) for arg in child.args)
            ):
                found.add((path, inner))
            visit(child, inner)

    visit(tree, "<module>")
    return found


class TestStatementsByShape:
    def test_no_test_searches_sql_by_substring(self) -> None:
        """Both ways, like the reference watchman. The pair: the helper's
        own reading is found, so the scan reads the files."""
        found: set[tuple[str, str]] = set()
        for path in sorted((_REPO / "tests").glob("*.py")):
            if path.name == Path(__file__).name:
                continue
            found |= scan_sql_searches(
                path.read_text(encoding="utf-8"),
                str(path.relative_to(_REPO)),
            )
        assert ("tests/helpers.py", "sql_shape") in found
        assert sorted(found - set(NOT_A_STATEMENT_SEARCH)) == []
        assert set(NOT_A_STATEMENT_SEARCH) <= found

    def test_the_scanner_sees_every_form(self) -> None:
        source = '''
def picks(seen):
    a = [s for s in seen if "ORDER BY" in s]
    b = [s for s in seen if "FROM notifications" not in s]
    c = [s for s in seen if s.startswith("SELECT notification_deliveries.id")]
    d = [s for s in seen if "hello" in s]
    return a, b, c, d

def other(seen):
    return [s for s in seen if "plain words" in s]
'''
        assert scan_sql_searches(source, "m.py") == {("m.py", "picks")}


class TestSqlShape:
    def test_the_top_level_not_the_subquery(self) -> None:
        shape = sql_shape(
            "SELECT notifications.id FROM notifications WHERE "
            "notifications.lane = $1 AND EXISTS (SELECT 1 FROM "
            "notification_deliveries WHERE x ORDER BY y LIMIT 1) "
            "FOR NO KEY UPDATE SKIP LOCKED"
        )
        assert (shape.verb, shape.table) == ("SELECT", "notifications")
        assert shape.order_by == "" and shape.limit is False
        assert shape.lock == "FOR NO KEY UPDATE SKIP LOCKED"
        assert "notifications.lane = $1" in shape.where
        assert shape.sources == {"notifications", "notification_deliveries"}
        # A subquery's FROM BEFORE the statement's own: the top level wins.
        shape = sql_shape(
            "SELECT (SELECT count(*) FROM notification_deliveries) AS n "
            "FROM notifications ORDER BY notifications.id"
        )
        assert (shape.table, shape.order_by) == (
            "notifications", "notifications.id",
        )

    def test_quotes_hide_keywords(self) -> None:
        shape = sql_shape(
            "SELECT 'FROM nowhere ORDER BY x' FROM recipients ORDER BY id"
        )
        assert (shape.table, shape.order_by) == ("recipients", "id")

    @pytest.mark.parametrize(
        ("statement", "verb", "table"),
        [
            ("DELETE FROM notifications WHERE id = 1", "DELETE FROM",
             "notifications"),
            ("UPDATE recipients SET locale = 'en'", "UPDATE", "recipients"),
            ("INSERT INTO llm_calls (a) VALUES (1)", "INSERT INTO", "llm_calls"),
            ("EXPLAIN (ANALYZE) SELECT 1", "EXPLAIN", None),
            ("SELECT recipients.id FROM recipients FOR UPDATE", "SELECT",
             "recipients"),
        ],
    )
    def test_the_verb_and_the_table(
        self, statement: str, verb: str, table: str | None,
    ) -> None:
        shape = sql_shape(statement)
        assert (shape.verb, shape.table) == (verb, table)

    def test_a_sqlalchemy_statement(self) -> None:
        """str() of a statement object is read the same way."""
        from sqlalchemy import select

        from app.engine.models import Notification

        shape = sql_shape(
            select(Notification.id).order_by(Notification.created_at).limit(1),
        )
        assert (shape.verb, shape.table, shape.limit) == (
            "SELECT", "notifications", True,
        )
        assert shape.order_by == "notifications.created_at"


# ---------------------------------------------------------------------------
# FIX (3.7.0) -- the suite looks at its own database only
# ---------------------------------------------------------------------------
#
# On a box `comms-deploy.sh test` runs the suite against <db>_test on
# the server where the live comms-worker works in <db>. A query to
# pg_stat_activity / pg_locks without the database sees that worker's
# sessions: listener_pid() found the live wake listener (15 failures on
# both boxes), and a pid taken there could be terminated. In CI no live
# worker is beside the suite, so CI could not see it.

_ACTIVITY = re.compile(r"\b(pg_stat_activity|pg_locks)\b")
_OWN_DATABASE = "datname = current_database()"


# A query that looks at ANOTHER database on purpose -- (file, function)
# -> why. It filters by datname too, only not by its own.
OTHER_DATABASE_ON_PURPOSE: dict[tuple[str, str], str] = {
    ("tests/test_lane_wake.py",
     "test_another_databases_listener_is_neither_seen_nor_killed"):
        "the pair of the shared-server test: it proves the other "
        "database's listener is there, by its datname",
}


def scan_activity_queries(
    source: str, path: str,
) -> list[tuple[str, str, int, bool]]:
    """(path, function, line, limited to this database) of every SQL
    string that reads pg_stat_activity or pg_locks. Adjacent literals
    are one string to the parser, so a query split over lines is read
    whole; docstrings are prose and are not read."""
    tree = ast.parse(source)
    docs: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            first = node.body[0] if node.body else None
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                docs.add(id(first.value))
    found: list[tuple[str, str, int, bool]] = []

    def visit(node: ast.AST, scope: str) -> None:
        for child in ast.iter_child_nodes(node):
            inner = scope
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                inner = child.name
            if (
                isinstance(child, ast.Constant) and isinstance(child.value, str)
                and id(child) not in docs and _ACTIVITY.search(child.value)
            ):
                found.append(
                    (path, inner, child.lineno, _OWN_DATABASE in child.value),
                )
            visit(child, inner)

    visit(tree, "<module>")
    return found


class TestOwnDatabaseOnly:
    def test_every_activity_query_is_this_databases(self) -> None:
        """Every query of pg_stat_activity / pg_locks in tests/ carries
        datname = current_database(). The pair: the scan finds the known
        queries -- the wake listener's among them."""
        found = [
            hit for path in sorted((_REPO / "tests").glob("*.py"))
            if path.name != Path(__file__).name
            for hit in scan_activity_queries(
                path.read_text(encoding="utf-8"),
                str(path.relative_to(_REPO)),
            )
        ]
        files = {path for path, _, _, _ in found}
        assert "tests/test_lane_wake.py" in files and len(found) >= 10
        unlimited = [
            (path, scope, line) for path, scope, line, own in found
            if not own and (path, scope) not in OTHER_DATABASE_ON_PURPOSE
        ]
        assert unlimited == [], f"not limited to this database: {unlimited}"
        on_purpose = {(path, scope) for path, scope, _, own in found if not own}
        assert on_purpose == set(OTHER_DATABASE_ON_PURPOSE)

    def test_the_scanner_sees_a_split_query(self) -> None:
        source = """
def a():
    return text(
        "SELECT pid FROM pg_stat_activity "
        "WHERE application_name = :name"
    )

def b():
    return text(
        "SELECT pid FROM pg_stat_activity "
        "WHERE pid = :pid AND datname = current_database()"
    )

def c():
    \"\"\"pg_stat_activity in a docstring is prose.\"\"\"
"""
        assert [
            (scope, own) for _, scope, _, own in scan_activity_queries(source, "m.py")
        ] == [("a", False), ("b", True)]

