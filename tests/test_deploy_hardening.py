# =============================================================================
# TX-4 -- deploy and CI: the dump before a migration, an honest restore,
# closed dumps and secrets, Postgres off the shared network, CI with
# rights and pins, a lean image, a bounded migration lock, the lock file
# held to pyproject's ranges.
# =============================================================================
#
# The shell: the functions of deploy/comms-deploy.sh run as they are,
# under `bash -c`; docker compose and docker are shell FUNCTIONS declared
# in the same command line (as tests/test_drain_shell.py) -- they log
# every call and answer as the scenario says. The restore's honesty is
# held on a REAL psql against a scratch database of this test's own.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  update not checking the dump      -> TestUpdate
#   M2  an empty dump accepted            -> TestUpdate
#   M3  restore without ON_ERROR_STOP     -> TestRestore (real psql)
#   M4  dump_db without umask 077         -> TestDumpRights
#   M5  env_file back on comms-postgres   -> TestCompose
#   M6  comms-postgres back on aivis-shared -> TestCompose
#   M7  a `uses:` by tag                  -> TestCi
#   M8  no lock_timeout in migrations/env.py -> TestMigrationLockTimeout
#   M9  update's app recreate without --no-deps -> TestUpdate
#   M10 the lock's version out of pyproject's range -> TestTheLockSatisfies
#   M11 a checkout without persist-credentials: false -> TestCi (REV-5)
#   M12 a pip install the lock's hashes do not cover (`-e .`, a bare
#       pin) -> TestCi (REV-5)
# =============================================================================

import asyncio
import os
import re
import shutil
import stat
import subprocess
import sys
import time
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml
from packaging.requirements import Requirement
from packaging.version import Version
from sqlalchemy import text

from app.core.database import dispose_engine, get_engine

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deploy" / "comms-deploy.sh"
COMPOSE = ROOT / "deploy" / "docker-compose.yml"
SELF = "./comms-deploy.sh"


def _function(name: str) -> str:
    text_ = SCRIPT.read_text(encoding="utf-8")
    match = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text_, re.S | re.M)
    assert match, f"{name} not found at column zero"
    return match.group(0)


# docker compose and docker, as shell functions: every call logged to
# $LOG; the answers set by the scenario's variables.
_STUBS = r'''
compose() {
    echo "compose $*" >> "$LOG"
    case "$1" in
        build|stop) return "${STOP_RC:-0}" ;;
        up) return "${UP_RC:-0}" ;;
        exec)
            shift
            [ "$1" = "-T" ] && shift
            shift
            case "$1" in
                pg_dump)
                    [ "${DUMP_RC:-0}" = 0 ] || return "$DUMP_RC"
                    [ -n "${DUMP_EMPTY:-}" ] || echo "-- a dump"
                    ;;
                psql)
                    shift
                    if [ -n "${REAL_PSQL:-}" ]; then
                        local args=()
                        while [ $# -gt 0 ]; do
                            case "$1" in
                                -U|-d) shift 2 ;;
                                *) args+=("$1"); shift ;;
                            esac
                        done
                        PGPASSWORD="$REAL_PASS" psql -h "$REAL_HOST" -p "$REAL_PORT" \
                            -U "$REAL_USER" -d "$REAL_DB" "${args[@]}"
                        return $?
                    fi
                    cat > /dev/null
                    echo "${SCHEMA:-f}"
                    return "${PSQL_RC:-0}"
                    ;;
            esac
            ;;
    esac
}
docker() {
    echo "docker $*" >> "$LOG"
    if [ "$1" = "inspect" ]; then echo "${HEALTH:-healthy}"; fi
}
git() { echo "git $*" >> "$LOG"; return 0; }
sleep() { :; }
'''

_FUNCTIONS = (
    "health_wait_seconds", "wait_healthy", "wait_for_app",
    "converge_datastores", "dump_db", "schema_exists", "recreate_app",
    "stop_app", "start_old_app",
    "cmd_update", "cmd_install", "cmd_db", "cmd_restart",
)


def _run(
    tmp_path: Path, verb: str, *args: str, answer: str = "", **env: str,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    log = tmp_path / "calls.log"
    log.write_text("")
    backups = tmp_path / "backups"
    lines = [
        "set -u",
        "RED= GREEN= YELLOW= CYAN= NC=",
        f'BACKUP_DIR="{backups}"',
        f'REPO_DIR="{tmp_path}"',
        f'LOG="{log}"',
        "POSTGRES_USER=comms POSTGRES_DB=comms",
        "COMPOSE_CMD=compose NETWORK_NAME=aivis-shared",
        *(f"{k}={v!r}" for k, v in env.items()),
        "cd_compose() { :; }", "load_env() { :; }", "ensure_env_link() { :; }",
        "check_overlay_dir() { :; }", "show_app_log_tail() { :; }",
        "ensure_network() { :; }", "generate_env() { :; }",
        "seed_profile() { :; }", "handover_token() { :; }",
        _STUBS,
        *(_function(name) for name in _FUNCTIONS),
        f'{verb} "$@"',
    ]
    done = subprocess.run(
        ["bash", "-c", "\n".join(lines), SELF, *args],
        input=answer, capture_output=True, text=True, timeout=60,
    )
    return done, log.read_text().splitlines()


def _index(calls: list[str], start: str) -> int:
    return next(i for i, c in enumerate(calls) if c.startswith(start))


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


# ---------------------------------------------------------------------------
# update and install -- the dump before the migration
# ---------------------------------------------------------------------------


class TestUpdate:
    def test_the_order(self, tmp_path: Path) -> None:
        """Datastores converged (never forced) -> dumped -> the three app
        containers recreated with --no-deps (the migration runs there)."""
        done, calls = _run(tmp_path, "cmd_update")
        assert done.returncode == 0, done.stdout + done.stderr
        converge = _index(calls, "compose up -d comms-postgres comms-redis")
        dump = _index(calls, "compose exec -T comms-postgres pg_dump")
        app = _index(calls, "compose up -d --force-recreate --no-deps "
                            "comms-app comms-worker comms-consumer")
        assert converge < dump < app
        assert "--force-recreate" not in calls[converge]
        (dumped,) = (tmp_path / "backups").glob("comms-preupdate-*.sql")
        assert _mode(dumped) == 0o600
        assert _mode(tmp_path / "backups") == 0o700

    @pytest.mark.parametrize("failure", [{"DUMP_RC": "1"}, {"DUMP_EMPTY": "1"}],
                             ids=["code", "empty"])
    def test_no_dump_no_migration(
        self, tmp_path: Path, failure: dict[str, str],
    ) -> None:
        done, calls = _run(tmp_path, "cmd_update", **failure)
        assert done.returncode == 1
        assert "✗ no dump -- no migration" in done.stdout
        assert not any("--force-recreate" in c for c in calls)
        assert list((tmp_path / "backups").glob("*.sql")) == []

    def test_unhealthy_datastores_stop_it_before_the_dump(
        self, tmp_path: Path,
    ) -> None:
        done, calls = _run(tmp_path, "cmd_update", HEALTH="starting",
                           COMMS_HEALTH_WAIT_SECONDS="4")
        assert done.returncode == 1
        assert "not healthy within 4s" in done.stdout
        assert not any("pg_dump" in c for c in calls)


class TestInstall:
    def test_a_new_box_is_not_dumped(self, tmp_path: Path) -> None:
        done, calls = _run(tmp_path, "cmd_install", SCHEMA="f")
        assert done.returncode == 0, done.stdout + done.stderr
        assert not any("pg_dump" in c for c in calls)
        assert any(c == "compose up -d" for c in calls)

    def test_a_box_with_a_schema_is_dumped_first(self, tmp_path: Path) -> None:
        done, calls = _run(tmp_path, "cmd_install", SCHEMA="t")
        assert done.returncode == 0, done.stdout + done.stderr
        assert _index(calls, "compose exec -T comms-postgres pg_dump") < calls.index(
            "compose up -d")
        assert list((tmp_path / "backups").glob("comms-preinstall-*.sql"))

    def test_a_failed_dump_starts_nothing(self, tmp_path: Path) -> None:
        done, calls = _run(tmp_path, "cmd_install", SCHEMA="t", DUMP_RC="1")
        assert done.returncode == 1
        assert "compose up -d" not in calls

    def test_an_unanswered_schema_check_starts_nothing(self, tmp_path: Path) -> None:
        done, calls = _run(tmp_path, "cmd_install", PSQL_RC="1")
        assert done.returncode == 1
        assert "compose up -d" not in calls


# ---------------------------------------------------------------------------
# the dumps' rights
# ---------------------------------------------------------------------------


class TestDumpRights:
    def test_db_dump_closes_the_old_ones_too(self, tmp_path: Path) -> None:
        backups = tmp_path / "backups"
        backups.mkdir(mode=0o755)
        old = backups / "comms-20260101-000000.sql"
        old.write_text("old")
        old.chmod(0o644)
        done, _ = _run(tmp_path, "cmd_db", "dump")
        assert done.returncode == 0, done.stdout + done.stderr
        assert _mode(backups) == 0o700
        for dump in backups.glob("*.sql"):
            assert _mode(dump) == 0o600, dump

    def test_every_dump_goes_through_dump_db(self) -> None:
        """One copy: pg_dump is written in the script once, in dump_db."""
        body = SCRIPT.read_text(encoding="utf-8")
        assert len(re.findall(r"\bpg_dump -U", body)) == 1
        assert "pg_dump -U" in _function("dump_db")
        for verb in ("cmd_db", "cmd_update", "cmd_install", "cmd_drain"):
            assert "dump_db " in _function(verb), verb


# ---------------------------------------------------------------------------
# restore -- honest, with the application stopped
# ---------------------------------------------------------------------------


class TestRestoreOrder:
    @pytest.mark.parametrize("ok", [True, False], ids=["restored", "failed"])
    def test_stop_restore_start(self, tmp_path: Path, ok: bool) -> None:
        dump = tmp_path / "dump.sql"
        dump.write_text("SELECT 1;\n")
        done, calls = _run(tmp_path, "cmd_db", "restore", str(dump),
                           answer="yes\n", PSQL_RC="0" if ok else "3")
        stop = _index(calls, "compose stop comms-app comms-worker comms-consumer")
        restore = _index(calls, "compose exec -T comms-postgres psql")
        start = _index(calls, "compose up -d --force-recreate --no-deps")
        assert stop < restore < start  # restarted either way
        assert "ON_ERROR_STOP=1" in calls[restore]
        assert "--single-transaction" in calls[restore]
        if ok:
            assert done.returncode == 0
            assert "✓ Restored" in done.stdout
        else:
            assert done.returncode == 1
            assert "✗ restore failed" in done.stdout
            assert "✓ Restored" not in done.stdout


_SCRATCH = "comms_restore_scratch_test"


@pytest.fixture
def scratch_db() -> Any:
    url = get_engine().url
    params = {
        "REAL_HOST": url.host or "localhost", "REAL_PORT": str(url.port or 5432),
        "REAL_USER": url.username or "comms", "REAL_PASS": url.password or "",
        "REAL_DB": _SCRATCH,
    }

    def admin(sql: str) -> None:
        subprocess.run(
            ["psql", "-h", params["REAL_HOST"], "-p", params["REAL_PORT"],
             "-U", params["REAL_USER"], "-d", "postgres", "-c", sql],
            env={**os.environ, "PGPASSWORD": params["REAL_PASS"]},
            check=True, capture_output=True,
        )

    admin(f"DROP DATABASE IF EXISTS {_SCRATCH} WITH (FORCE)")
    admin(f"CREATE DATABASE {_SCRATCH}")
    yield params
    admin(f"DROP DATABASE IF EXISTS {_SCRATCH} WITH (FORCE)")


def _scratch_rows(params: dict[str, str]) -> str:
    done = subprocess.run(
        ["psql", "-h", params["REAL_HOST"], "-p", params["REAL_PORT"],
         "-U", params["REAL_USER"], "-d", _SCRATCH, "-tAc",
         "SELECT coalesce(string_agg(v, ',' ORDER BY v), '') FROM keep"],
        env={**os.environ, "PGPASSWORD": params["REAL_PASS"]},
        capture_output=True, text=True,
    )
    return done.stdout.strip() if done.returncode == 0 else f"ERR {done.stderr}"


@pytest.mark.skipif(
    shutil.which("psql") is None,
    reason="the psql client is not in this environment (comms' image has "
           "none); CI's test job runs it",
)
class TestRestoreHonesty:
    """A REAL psql against a scratch database: the dump goes into an
    empty schema in one transaction, under ON_ERROR_STOP."""

    def _seed(self, params: dict[str, str]) -> None:
        subprocess.run(
            ["psql", "-h", params["REAL_HOST"], "-p", params["REAL_PORT"],
             "-U", params["REAL_USER"], "-d", _SCRATCH, "-c",
             "CREATE TABLE keep (v text); INSERT INTO keep VALUES ('before');"],
            env={**os.environ, "PGPASSWORD": params["REAL_PASS"]},
            check=True, capture_output=True,
        )

    def test_an_error_in_the_dump_changes_nothing(
        self, tmp_path: Path, scratch_db: dict[str, str],
    ) -> None:
        self._seed(scratch_db)
        dump = tmp_path / "broken.sql"
        dump.write_text(
            "CREATE TABLE keep (v text);\nINSERT INTO keep VALUES ('after');\n"
            "THIS IS NOT SQL;\n"
        )
        done, _ = _run(tmp_path, "cmd_db", "restore", str(dump), answer="yes\n",
                       REAL_PSQL="1", **scratch_db)
        assert done.returncode == 1
        assert "✗ restore failed" in done.stdout
        assert "✓ Restored" not in done.stdout
        assert _scratch_rows(scratch_db) == "before"  # rolled back whole

    def test_the_pair_a_good_dump_replaces_the_database(
        self, tmp_path: Path, scratch_db: dict[str, str],
    ) -> None:
        self._seed(scratch_db)
        dump = tmp_path / "good.sql"
        dump.write_text(
            "CREATE TABLE keep (v text);\nINSERT INTO keep VALUES ('after');\n"
        )
        done, _ = _run(tmp_path, "cmd_db", "restore", str(dump), answer="yes\n",
                       REAL_PSQL="1", **scratch_db)
        assert done.returncode == 0, done.stdout + done.stderr
        assert "✓ Restored" in done.stdout
        assert _scratch_rows(scratch_db) == "after"  # no 'before': overwritten


# ---------------------------------------------------------------------------
# compose -- secrets and networks
# ---------------------------------------------------------------------------


def _compose() -> dict[str, Any]:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


class TestCompose:
    def test_the_datastores_get_their_own_variables_only(self) -> None:
        services = _compose()["services"]
        assert "env_file" not in services["comms-postgres"]
        assert "env_file" not in services["comms-redis"]
        assert set(services["comms-postgres"]["environment"]) == {
            "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"}
        assert set(services["comms-redis"]["environment"]) == {"REDIS_PASSWORD"}

    def test_the_redis_password_is_no_argument_and_redis_is_pid_1(self) -> None:
        command = _compose()["services"]["comms-redis"]["command"]
        assert command[:2] == ["sh", "-c"]
        script = command[2]
        assert "|" not in script  # no pipeline: exec in the shell itself
        last = script.split("&&")[-1].strip()
        assert last.startswith("exec redis-server /tmp/redis.conf")
        assert "--requirepass" not in script
        assert "REDIS_PASSWORD" not in last  # the server's argv: none
        assert "umask 077" in script

    def test_postgres_is_off_the_shared_network(self) -> None:
        data = _compose()
        services = data["services"]
        assert services["comms-postgres"]["networks"] == ["comms-internal"]
        for name in ("comms-app", "comms-worker", "comms-consumer", "comms-redis"):
            assert set(services[name]["networks"]) == {"aivis-shared",
                                                       "comms-internal"}, name
        assert data["networks"]["comms-internal"] == {
            "name": "comms-internal", "internal": True}
        assert data["networks"]["aivis-shared"] == {"external": True}


# ---------------------------------------------------------------------------
# CI -- rights and pins
# ---------------------------------------------------------------------------

_WORKFLOWS = sorted((ROOT / ".github" / "workflows").glob("*.yml"))


def _uses_lines(text_: str) -> list[str]:
    return [line.strip() for line in text_.splitlines()
            if re.match(r"\s*(-\s+)?uses:", line)]


def _checkouts(data: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        step for job in data["jobs"].values() for step in job["steps"]
        if str(step.get("uses", "")).startswith("actions/checkout@")
    ]


def _pip_installs(data: dict[str, Any]) -> list[str]:
    """Every `pip install` command line of every step's `run`."""
    return [
        line.strip()
        for job in data["jobs"].values() for step in job["steps"]
        for line in str(step.get("run", "")).splitlines()
        if re.match(r"\s*(python3? -m )?pip install\b", line)
    ]


# What a hash-checked install looks like: the lock itself (every line of
# it carries its hashes -- tests/test_delivery_contract.py), or a file
# installed under --require-hashes (a stanza of the lock).
_HASHED = re.compile(
    r"(python3? -m )?pip install (-r requirements\.lock|--require-hashes -r \S+)"
)


def _unhashed(installs: list[str]) -> list[str]:
    return [line for line in installs if not _HASHED.fullmatch(line)]


def _jobs_without_a_limit(data: dict[str, Any]) -> list[str]:
    return [
        name for name, job in data["jobs"].items()
        if not (isinstance(job.get("timeout-minutes"), int)
                and job["timeout-minutes"] > 0)
    ]


class TestCi:
    @pytest.mark.parametrize("workflow", _WORKFLOWS, ids=lambda p: p.name)
    def test_every_job_has_its_permissions(self, workflow: Path) -> None:
        data = yaml.safe_load(workflow.read_text(encoding="utf-8"))
        assert data.get("permissions") == {}, "the token gets nothing by default"
        for name, job in data["jobs"].items():
            assert "permissions" in job, name

    @pytest.mark.parametrize("workflow", _WORKFLOWS, ids=lambda p: p.name)
    def test_every_action_is_pinned_by_sha(self, workflow: Path) -> None:
        lines = _uses_lines(workflow.read_text(encoding="utf-8"))
        assert lines
        for line in lines:
            assert re.search(r"@[0-9a-f]{40}\s+#\s*v\d", line), line

    @pytest.mark.parametrize("workflow", _WORKFLOWS, ids=lambda p: p.name)
    def test_every_job_has_a_time_limit(self, workflow: Path) -> None:
        """A hang is red (FIX review 3.13.0, the second): without a limit
        a job that hangs waits GitHub's six hours."""
        data = yaml.safe_load(workflow.read_text(encoding="utf-8"))
        assert _jobs_without_a_limit(data) == []

    def test_the_guard_sees_a_job_without_a_limit(self) -> None:
        data = {"jobs": {"a": {"timeout-minutes": 40}, "b": {}, "c": {
            "timeout-minutes": 0}}}
        assert _jobs_without_a_limit(data) == ["b", "c"]

    def test_the_guard_sees_a_tag(self) -> None:
        line = _uses_lines("      - uses: actions/checkout@v7\n")[0]
        assert not re.search(r"@[0-9a-f]{40}\s+#\s*v\d", line)

    @pytest.mark.parametrize("workflow", _WORKFLOWS, ids=lambda p: p.name)
    def test_no_checkout_keeps_the_token(self, workflow: Path) -> None:
        """REV-5: actions/checkout writes the job's token into
        .git/config unless told not to, and the checkout is mounted into
        the containers the suite runs in."""
        data = yaml.safe_load(workflow.read_text(encoding="utf-8"))
        checkouts = _checkouts(data)
        assert checkouts, "the pair: there are checkouts to read"
        for step in checkouts:
            assert (step.get("with") or {}).get("persist-credentials") is False

    @pytest.mark.parametrize("workflow", _WORKFLOWS, ids=lambda p: p.name)
    def test_every_install_is_hash_checked(self, workflow: Path) -> None:
        """REV-5: no package reaches CI that the lock's hashes do not
        name -- not the project built by `pip install -e .` (its
        setuptools came unpinned), not a bare `pyyaml==` pin."""
        data = yaml.safe_load(workflow.read_text(encoding="utf-8"))
        installs = _pip_installs(data)
        assert installs, "the pair: there are installs to read"
        assert _unhashed(installs) == []

    def test_the_guard_sees_an_unhashed_install(self) -> None:
        installs = [
            "pip install -r requirements.lock",
            'pip install --require-hashes -r "$RUNNER_TEMP/pyyaml.txt"',
            "pip install -e . --no-deps",
            "pip install pyyaml==6.0.3",
            "python -m pip install -r requirements.lock somepackage",
        ]
        assert _unhashed(installs) == installs[2:]


# ---------------------------------------------------------------------------
# the image
# ---------------------------------------------------------------------------


class TestImage:
    def test_no_tests_and_no_unpinned_pip(self) -> None:
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        code = [line for line in dockerfile.splitlines()
                if not line.lstrip().startswith("#")]
        assert not any(re.match(r"\s*COPY\s+tests", line) for line in code)
        assert not any("--upgrade pip" in line for line in code)

    def test_the_context_keeps_what_the_build_copies(self) -> None:
        ignored = [line.strip() for line in
                   (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
                   if line.strip() and not line.startswith("#")]
        assert "tests/" in ignored
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        sources = []
        for line in dockerfile.splitlines():
            match = re.match(r"\s*COPY\s+(?!--from)(.+)\s+\S+$", line)
            if match:
                sources += match.group(1).split()
        assert sources
        for source in sources:
            top = source.rstrip("/").split("/")[0]
            assert not any(pattern.rstrip("/") == top for pattern in ignored), source


# ---------------------------------------------------------------------------
# every run of the image reads only what the image has, or a mount
# (TX-4 FIX)
# ---------------------------------------------------------------------------


def _image_paths() -> set[str]:
    """What the image carries: the TARGETS of the runtime stage's COPY
    lines (a white list), resolved against its WORKDIR -- the Dockerfile
    parsed, one copy. .dockerignore is a black list of the build
    context and says nothing of what the image holds."""
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    runtime = dockerfile.rsplit("\nFROM ", 1)[1]
    workdir = "/"
    found = set()
    for line in runtime.splitlines():
        line = line.strip()
        if line.startswith("WORKDIR "):
            # the directory exists, empty: a path IN it is the COPYs'
            workdir = line.split(None, 1)[1]
        elif line.startswith("COPY "):
            parts = [p for p in line.split()[1:] if not p.startswith("--")]
            *sources, target = parts
            base = target if target.startswith("/") else f"{workdir}/{target}"
            base = re.sub(r"/\./|/\.$", "/", base).replace("//", "/")
            for source in sources:
                if base.endswith("/") and not source.endswith("/"):
                    found.add(base + source.rsplit("/", 1)[-1])
                else:
                    found.add(base.rstrip("/"))
    return found


def _image_runs(text_: str) -> list[str]:
    """Every `docker run` of comms' image in a workflow, one string each,
    from `docker run` to the image name and the rest of its line."""
    runs = []
    for step in re.findall(r"run: \|\n((?:\s{10,}.*\n)+)", text_):
        joined = re.sub(r"\\\n\s*", " ", step)
        runs += [m.group(0) for m in
                 re.finditer(r"docker run [^\n]*?comms:ci[^\n]*", joined)]
    return runs


def _unreachable(run: str, image: set[str]) -> list[str]:
    """The /app paths `run` reads that are neither in the image nor under
    a directory mounted in the same run."""
    mounts = [m.split(":")[1] for m in
              re.findall(r'-v\s+"?([^"\s]+)"?', run)]
    without_mounts = re.sub(r'-v\s+"?[^"\s]+"?', " ", run)
    missing = []
    for path in re.findall(r"/app(?:/[\w./-]*)?", without_mounts):
        path = path.rstrip("/.") or "/app"
        covered = [m for m in mounts if path == m or path.startswith(m + "/")]
        held = [p for p in image if path == p or path.startswith(p + "/")]
        if path == "/app":
            held = ["/app"]  # the WORKDIR itself
        if not covered and not held:
            missing.append(path)
    return missing


_CI = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")


class TestImageRuns:
    def test_every_path_is_in_the_image_or_mounted(self) -> None:
        image = _image_paths()
        assert {"/app/app", "/app/migrations", "/app/alembic.ini"} <= image
        assert not any(p.startswith("/app/tests") for p in image)
        runs = _image_runs(_CI)
        assert len(runs) == 2  # the start, and the suite
        for run in runs:
            assert _unreachable(run, image) == [], run

    def test_the_guard_sees_a_path_the_image_lacks(self) -> None:
        run = ("docker run -d --name x -e TEMPLATES_DIR=/app/tests/fixtures/"
               "profile comms:ci")
        assert _unreachable(run, _image_paths()) == ["/app/tests/fixtures/profile"]
        mounted = ('docker run -v "$PWD:/app:ro" -e TEMPLATES_DIR=/app/tests/x '
                   "comms:ci")
        assert _unreachable(mounted, _image_paths()) == []

    def test_the_start_reads_the_profile_where_a_box_does(self) -> None:
        (start,) = [r for r in _image_runs(_CI) if "TEMPLATES_DIR" in r]
        ci_dir = re.search(r"-e TEMPLATES_DIR=(\S+)", start)
        assert ci_dir is not None
        services = _compose()["services"]
        box = {services[n]["environment"]["TEMPLATES_DIR"]
               for n in ("comms-app", "comms-worker", "comms-consumer")}
        assert box == {ci_dir.group(1)}


# ---------------------------------------------------------------------------
# a migration waits for a lock 10 s at most
# ---------------------------------------------------------------------------

_BEFORE = "0036_notification_email"


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=ROOT, capture_output=True, text=True, timeout=120,
    )


class TestMigrationLockTimeout:
    async def test_a_held_table_fails_the_migration_in_bound(self) -> None:
        await dispose_engine()
        assert _alembic("downgrade", _BEFORE).returncode == 0
        try:
            engine = get_engine()
            async with engine.connect() as holder:
                await holder.execute(text(
                    "LOCK TABLE notifications IN ACCESS SHARE MODE"))
                started = time.monotonic()
                done = await asyncio.to_thread(_alembic, "upgrade", "head")
                elapsed = time.monotonic() - started
                await holder.rollback()
            assert done.returncode != 0
            assert "lock timeout" in done.stderr or "LockNotAvailable" in done.stderr
            assert elapsed < 40, elapsed  # bounded: ~10 s, not forever
        finally:
            await dispose_engine()
            again = _alembic("upgrade", "head")
            assert again.returncode == 0, again.stderr
            await dispose_engine()


# ---------------------------------------------------------------------------
# the lock satisfies pyproject's ranges
# ---------------------------------------------------------------------------

_PYPROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
_LOCK = (ROOT / "requirements.lock").read_text(encoding="utf-8")


def _locked() -> dict[str, Version]:
    found = {}
    for line in _LOCK.splitlines():
        match = re.match(r"^([A-Za-z0-9_.\-]+)==([^\s\\;]+)", line.strip())
        if match:
            found[match.group(1).lower().replace("_", "-")] = Version(match.group(2))
    return found


def _declared() -> list[Requirement]:
    project = _PYPROJECT["project"]
    specs = list(project["dependencies"]) + list(
        project["optional-dependencies"]["dev"])
    return [Requirement(spec) for spec in specs]


def _unsatisfied(declared: list[Requirement], locked: dict[str, Version]) -> list[str]:
    return [
        str(req) for req in declared
        if req.name.lower().replace("_", "-") in locked
        and locked[req.name.lower().replace("_", "-")] not in req.specifier
    ]


class TestTheLockSatisfies:
    """Decision D2 (the owner): a Dependabot pull request that raises a
    range in pyproject is not regenerated into the lock automatically --
    it is closed by hand. This keeps it from being merged: the version
    the lock pins must lie in every range pyproject declares."""

    def test_every_locked_version_is_in_its_range(self) -> None:
        assert _unsatisfied(_declared(), _locked()) == []

    def test_the_guard_sees_a_drift(self) -> None:
        drifted = [Requirement("fastapi>=999.0")]
        assert _unsatisfied(drifted, _locked()) == ["fastapi>=999.0"]


# ---------------------------------------------------------------------------
# a hung test is red (FIX review 3.13.0, the second)
# ---------------------------------------------------------------------------


class TestAHangIsRed:
    def test_the_image_gets_the_timeout_from_the_lock(self) -> None:
        """The suite on a box (comms-deploy.sh test) and CI's image job run
        on the image's venv: pytest-timeout reaches it through the lock."""
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        assert re.search(r"pip install [^\n]*-r requirements\.lock", dockerfile)
        assert re.search(r"^pytest-timeout==", _LOCK, re.M)
        options = _PYPROJECT["tool"]["pytest"]["ini_options"]
        assert options["timeout"] == 300
        assert options["timeout_method"] == "signal"

    @pytest.mark.parametrize("body, red", [
        ("import asyncio\n\nasync def test_x():\n    await asyncio.Event().wait()\n",
         True),
        ("async def test_x():\n    pass\n", False),
    ], ids=["hangs", "returns"])
    def test_a_hanging_test_is_red_not_a_hang(
        self, tmp_path: Path, body: str, red: bool,
    ) -> None:
        test = tmp_path / "test_probe.py"
        test.write_text(body)
        started = time.monotonic()
        done = subprocess.run(
            # No flag on the cache plugin (G3 forbids switching it off,
            # FIX review 3.13.0, the third): the subprocess's rootdir is
            # tmp_path -- the checkout's pyproject.toml is no ancestor of
            # it -- so its cache lands in tmp_path/.pytest_cache, asserted
            # below, and nothing is written into the checkout.
            [sys.executable, "-m", "pytest", "-q",
             "-o", "asyncio_mode=auto", "--timeout=2",
             "-o", "timeout_method=signal", str(test)],
            cwd=tmp_path, capture_output=True, text=True, timeout=120,
        )
        assert time.monotonic() - started < 60
        if red:
            assert done.returncode != 0
            assert "Timeout" in done.stdout
            # the subprocess's cache: in the test's own temporary directory
            assert (tmp_path / ".pytest_cache" / "v" / "cache" / "lastfailed").is_file()
        else:
            assert done.returncode == 0, done.stdout
