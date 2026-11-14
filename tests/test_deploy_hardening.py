# =============================================================================
# TX-4 -- deploy and CI: Postgres off every network but comms' own, the
# datastores with their own variables only, CI with rights and pins, a
# lean image, a bounded migration lock, the lock file held to pyproject's
# ranges.
# =============================================================================
#
# The dump before a migration, the honest restore and the closed dumps
# were the old installer's and went with it in
# W1-2: the schema is the deploy machine's step `migrate`, and its dumps
# are the machine's (`db_dump`).
#
# MUTATIONS these tests were written against (each turns one red; M1-M4
# and M9 held the old installer and went with it):
#   M5  env_file back on comms-postgres   -> TestCompose
#   M6  comms-postgres back on a network beside comms-internal -> TestCompose
#   M7  a `uses:` by tag                  -> TestCi
#   M8  no lock_timeout in migrations/env.py -> TestMigrationLockTimeout
#   M10 the lock's version out of pyproject's range -> TestTheLockSatisfies
#   M11 a checkout without persist-credentials: false -> TestCi (REV-5)
#   M12 a pip install the lock's hashes do not cover (`-e .`, a bare
#       pin) -> TestCi (REV-5)
#   M13 the suite run with the checkout mounted again -> TestImageRuns
#   M14 tests/ back in .dockerignore      -> TestImage (W1-1)
#   M15 .env out of .dockerignore         -> TestImage (W1-1)
#   W1-2: the Redis password from a key that is not the seam's
#         -> TestCompose
# =============================================================================

import asyncio
import re
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
COMPOSE = ROOT / "deploy" / "docker-compose.yml"

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

    def test_the_datastores_take_the_machines_keys(self) -> None:
        """The password of each datastore is the key of primer.yaml `env`
        comms builds its own URL from (tests/test_primer_yaml.py
        COMPOSED): one value, two places, never two values."""
        services = _compose()["services"]
        assert services["comms-postgres"]["environment"] == {
            "POSTGRES_USER": "comms",
            "POSTGRES_PASSWORD": "${POSTGRES_PASSWORD:?}",
            "POSTGRES_DB": "comms",
        }
        assert services["comms-redis"]["environment"] == {
            "REDIS_PASSWORD": "${COMMS_EVENTS_PASSWORD:?}",
        }

    def test_no_service_reads_an_env_file(self) -> None:
        services = _compose()["services"]
        assert sorted(services) == [
            "comms-app", "comms-consumer", "comms-postgres", "comms-redis",
            "comms-worker",
        ]
        assert [name for name, body in services.items() if "env_file" in body] == []

    def test_postgres_is_on_comms_network_only(self) -> None:
        """No network towards the product in the file (the machine's
        override adds it to comms-app and comms-redis); the database is
        on comms-internal alone, which has no way out."""
        data = _compose()
        services = data["services"]
        assert services["comms-postgres"]["networks"] == ["comms-internal"]
        assert services["comms-redis"]["networks"] == ["comms-internal"]
        for name in ("comms-app", "comms-worker", "comms-consumer"):
            assert services[name]["networks"] == ["default", "comms-internal"], name
        assert data["networks"] == {"comms-internal": {"internal": True}}


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


# What a hash-checked install looks like: a file installed under
# --require-hashes, written out -- a lock, or a stanza of one. Until W1-1
# `pip install -r requirements.lock` passed as well, pip switching into
# hash-checking mode by itself while the file carries hashes; the flag
# is now required, so a lock that lost its hashes fails the install.
_HASHED = re.compile(r"(python3? -m )?pip install --require-hashes -r \S+")


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
            "pip install --require-hashes -r requirements-dev.lock",
            'pip install --require-hashes -r "$RUNNER_TEMP/pyyaml.txt"',
            "pip install -r requirements.lock",
            "pip install -e . --no-deps",
            "pip install pyyaml==6.0.3",
            "python -m pip install --require-hashes -r requirements.lock somepackage",
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
        ignored = _ignored()
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


    def test_the_context_holds_the_tree_but_secrets_and_history(self) -> None:
        """W1-1: the test stage copies the context entire and the suite
        reads the whole tree, so no file of it is ignored -- M14; and
        what must never reach an image stays out: the history and every
        .env -- M15. TX-4 kept tests/ and the rest out of the context;
        the runtime stage's white list of COPY lines does that now."""
        ignored = _ignored()
        tree = _tree_files(ROOT)
        # The pair: the walk sees what the suite reads.
        assert {"tests/conftest.py", "deploy/INTEGRATION.md",
                ".github/workflows/ci.yml", "scripts/check_domain_literals.py",
                ".gitignore", ".dockerignore", "Dockerfile",
                "requirements-dev.lock"} <= tree
        assert [path for path in sorted(tree) if _in(path, ignored)] == []
        for path in (".git/config", ".env", ".env.local", "deploy/.env",
                     "app/.env"):
            assert _in(path, ignored), path

    def test_the_guard_sees_an_ignored_file(self) -> None:
        assert _in("tests/conftest.py", ["tests/"])
        assert _in("deploy/.env", ["**/.env"])
        assert not _in("deploy/.env.example", ["**/.env", ".env.*"])
        assert _in("deploy/INTEGRATION.md", ["*.md"]) is False
        assert _in("README.md", ["*.md"])


def _ignored() -> list[str]:
    """.dockerignore's patterns; the file has no negation, and one would
    be refused here rather than misread."""
    patterns = [line.strip() for line in
                (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.startswith("#")]
    assert not any(p.startswith("!") for p in patterns), patterns
    return patterns


def _in(path: str, patterns: list[str]) -> bool:
    """Whether .dockerignore's `patterns` keep `path` (relative, posix)
    out of the context: a pattern is anchored at the root, matches the
    path or a directory above it, and `**/` matches at any depth."""
    from fnmatch import fnmatchcase

    parts = path.split("/")
    prefixes = ["/".join(parts[:n]) for n in range(1, len(parts) + 1)]
    for pattern in patterns:
        pattern = pattern.rstrip("/")
        if pattern.startswith("**/"):
            tail = pattern[3:]
            if any(fnmatchcase(part, tail) for part in parts):
                return True
        elif any(
            fnmatchcase(prefix, pattern) and prefix.count("/") == pattern.count("/")
            for prefix in prefixes
        ):
            return True
    return False


def _tree_files(root: Path) -> set[str]:
    """Every file of the tree, relative: not .git, not the caches, not a
    .env (what the context leaves out on purpose -- asserted on its own
    in the test above)."""
    skipped = {".git", "__pycache__", ".pytest_cache", ".mypy_cache",
               ".ruff_cache"}
    found = set()
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if skipped & set(relative.parts) or not path.is_file():
            continue
        if relative.suffix == ".pyc" or relative.name == ".env" or (
            len(relative.parts) == 1 and relative.name.startswith(".env.")
        ):
            continue
        found.add(relative.as_posix())
    return found


# ---------------------------------------------------------------------------
# every run of the image reads only what the image has, or a mount
# (TX-4 FIX)
# ---------------------------------------------------------------------------


def _image_paths(stage: str = "runtime") -> set[str]:
    """What a stage carries: the TARGETS of its COPY lines (a white list),
    resolved against its WORKDIR -- the Dockerfile parsed, one copy.
    .dockerignore is a black list of the build context and says nothing
    of what the image holds. The stage by its NAME (W1-1): until then
    the last FROM was read, which was the runtime of two stages."""
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    blocks = re.split(r"^FROM\s+\S+\s+AS\s+(\S+)\s*$", dockerfile, flags=re.M)
    named = dict(zip(blocks[1::2], blocks[2::2], strict=True))
    workdir = "/"
    found = set()
    for line in named[stage].splitlines():
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
                if source == ".":
                    # the whole context, into the target directory
                    found.add(base.rstrip("/") or "/")
                elif base.endswith("/") and not source.endswith("/"):
                    found.add(base + source.rsplit("/", 1)[-1])
                else:
                    found.add(base.rstrip("/"))
    return found


# The image tag CI gives each stage it builds (W1-1).
_TAGS = {"comms:ci": "runtime", "comms:test": "test"}


def _image_runs(text_: str) -> list[tuple[str, str]]:
    """Every `docker run` of comms' image in a workflow, one string each,
    from `docker run` to the image name and the rest of its line -- with
    the stage its tag names."""
    runs = []
    for step in re.findall(r"run: \|\n((?:\s{10,}.*\n)+)", text_):
        joined = re.sub(r"\\\n\s*", " ", step)
        runs += [(_TAGS[m.group(1)], m.group(0)) for m in
                 re.finditer(r"docker run [^\n]*?(comms:(?:ci|test))\b[^\n]*",
                             joined)]
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
        assert "/app" in _image_paths("test")  # the whole tree (W1-1)
        runs = _image_runs(_CI)
        # Until W1-1 two runs, both of the runtime: the start and the
        # suite. W1-1: each stage three -- what it carries (pip list), the
        # machine's arguments, and the start (runtime) or the suite (test).
        # W2-2: the runtime two more -- the `comms` command of the working
        # image, `comms derive bot-id` of a well-formed token and of a
        # malformed one. FIX W3-1: the runtime one more -- the migration
        # before the start (test_the_start_is_migrated_first_by_primer_yamls_command).
        # W4-1: the runtime one more -- the profile's validator, as the
        # machine runs it (test_the_profile_is_validated_as_the_machine_does).
        assert sorted(stage for stage, _ in runs) == ["runtime"] * 7 + ["test"] * 3
        derives = [
            stage for stage, run in runs
            if re.search(r"comms:ci\s+comms derive bot-id\b", run)
        ]
        assert derives == ["runtime"] * 2
        for stage, run in runs:
            assert _unreachable(run, _image_paths(stage)) == [], run

    def test_the_suite_runs_in_the_test_stage_unmounted(self) -> None:
        """W1-1, M13: the suite runs in the stage the machine runs it in,
        on that stage's own copy of the tree -- a checkout mounted over
        /app would prove the checkout, not the stage."""
        (stage, suite), = [(s, r) for s, r in _image_runs(_CI) if "pytest" in r]
        assert stage == "test"
        assert not re.search(r"-v\s", suite), suite
        assert "comms:test sh -c" in suite

    def test_the_guard_sees_a_path_the_image_lacks(self) -> None:
        run = ("docker run -d --name x -e TEMPLATES_DIR=/app/tests/fixtures/"
               "profile comms:ci")
        assert _unreachable(run, _image_paths()) == ["/app/tests/fixtures/profile"]
        mounted = ('docker run -v "$PWD:/app:ro" -e TEMPLATES_DIR=/app/tests/x '
                   "comms:ci")
        assert _unreachable(mounted, _image_paths()) == []

    def test_the_start_is_migrated_first_by_primer_yamls_command(self) -> None:
        """FIX W3-1: /ready's `db` check reads the schema's revision, and
        the API's start migrates nothing (W1-2) -- the machine runs
        primer.yaml's `migrate` before `up`. The start step does the same:
        the migration, in the runtime image, with the command read from
        primer.yaml, before the service is started."""
        step = _CI.split("- name: The image starts (GET /ready)", 1)[1]
        step = step.split("\n      - name:", 1)[0]
        assert "['stacks']['comms']['migrate']" in step
        assert "open('primer.yaml')" in step
        migrate = step.index("comms:ci $migrate")
        start = step.index("docker run -d --name comms-app")
        assert migrate < start
        (run,) = [r for _, r in _image_runs(_CI) if "comms:ci $migrate" in r]
        assert "docker run --rm" in run, "a throwaway container"
        stacks = yaml.safe_load((ROOT / "primer.yaml").read_text(encoding="utf-8"))
        assert stacks["stacks"]["comms"]["migrate"] == "alembic upgrade head"

    def test_the_start_reads_the_profile_where_a_box_does(self) -> None:
        """Until W4-1 the path was the compose file's constant, /profile.
        Now the machine gives it: the copy of the kind `profile` at
        /primer/data/<kind> (protocol 4.5, the machine's CONTAINER_DATA),
        under the variable of primer.yaml `accepts.profile` -- and the
        compose file sets it nowhere."""
        (start,) = [
            r for _, r in _image_runs(_CI)
            if "TEMPLATES_DIR" in r and "--name comms-app" in r
        ]
        ci_dir = re.search(r"-e TEMPLATES_DIR=(\S+)", start)
        assert ci_dir is not None
        primer = yaml.safe_load((ROOT / "primer.yaml").read_text(encoding="utf-8"))
        (kind,) = [k for k, a in primer["accepts"].items()
                   if a["env"] == "TEMPLATES_DIR"]
        assert ci_dir.group(1) == f"/primer/data/{kind}"
        services = _compose()["services"]
        assert not [n for n in ("comms-app", "comms-worker", "comms-consumer")
                    if "TEMPLATES_DIR" in services[n]["environment"]]

    def test_the_profile_is_validated_as_the_machine_does(self) -> None:
        """W4-1: the image's validator of the profile, run as the machine
        runs it (pridem steps/services.py DataCopy): the command of
        primer.yaml `accepts.profile` read from the file, <dir> the copy's
        path, no network, no capability, nothing of the instance but the
        path -- in the runtime image, the one the machine builds."""
        step = _CI.split("- name: The image has the comms command", 1)[1]
        step = step.split("\n      - name:", 1)[0]
        assert "['accepts']['profile']['validate']" in step
        (stage, run), = [
            (s, r) for s, r in _image_runs(_CI) if "${validate//<dir>/" in r
        ]
        assert stage == "runtime"
        assert "--network none" in run and "--cap-drop ALL" in run
        assert "-e TEMPLATES_DIR=/primer/data/profile" in run
        assert ":/primer/data/profile:ro" in run
        assert not re.search(r"-e (?!TEMPLATES_DIR=)", run), run


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
# Both locks (W1-1): the runtime's and the dev one.
_LOCKS = {
    name: (ROOT / name).read_text(encoding="utf-8")
    for name in ("requirements.lock", "requirements-dev.lock")
}


def _locked(lock: str = "requirements-dev.lock") -> dict[str, Version]:
    found = {}
    for line in _LOCKS[lock].splitlines():
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

    @pytest.mark.parametrize("lock", list(_LOCKS))
    def test_every_locked_version_is_in_its_range(self, lock: str) -> None:
        """Each lock against every range it pins -- the runtime lock
        carries `dependencies` only, so the dev lock is what holds the
        ranges of `dev` (W1-1)."""
        locked = _locked(lock)
        assert locked  # the pair: the file reads as pins
        assert _unsatisfied(_declared(), locked) == []

    def test_the_guard_sees_a_drift(self) -> None:
        drifted = [Requirement("fastapi>=999.0")]
        assert _unsatisfied(drifted, _locked()) == ["fastapi>=999.0"]


# ---------------------------------------------------------------------------
# a hung test is red (FIX review 3.13.0, the second)
# ---------------------------------------------------------------------------


class TestAHangIsRed:
    def test_the_image_gets_the_timeout_from_the_lock(self) -> None:
        """The suite runs in the test stage (CI's image job, the deploy
        machine): pytest-timeout reaches it through the dev lock that
        stage installs. Until W1-1 the runtime image ran the suite and
        took it from the one lock; the runtime carries no test tool now
        (tests/test_image_stages.py)."""
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        assert re.search(
            r"pip install [^\n]*--require-hashes -r requirements-dev\.lock",
            dockerfile,
        )
        assert re.search(r"^pytest-timeout==", _LOCKS["requirements-dev.lock"], re.M)
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
