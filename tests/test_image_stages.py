# =============================================================================
# W1-1 -- the image in three stages: builder, test, runtime
# =============================================================================
#
# Read from the Dockerfile, the two locks and CI's workflow, not from a
# built image: the sandbox that writes this has no docker, and a build
# proves nothing a parse of these files does not -- what an image carries
# is decided here. The built images are checked too, by CI itself (job
# `image`, part 1: the runtime's `pip list` is the runtime lock exactly,
# the test stage's holds the dev lock, both hand the machine's arguments
# to the process).
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  a dev package's stanza added to requirements.lock  -> no dev in runtime
#   M2  the builder installs requirements-dev.lock          -> no dev in runtime
#   M3  a `pip install` RUN in the runtime stage            -> no dev in runtime
#   M4  the test stage without its dev install              -> dev in test
#   M8  --require-hashes dropped from one install           -> hash-checked
#   M10 an ARG without its ENV (either stage)               -> the arguments
#   M11 a --build-arg dropped from one CI build             -> CI passes them
#   M12 runtime not the last stage / a stage renamed        -> the stages
#   M17 an ENV dropped from the test stage                  -> test as runtime
# =============================================================================

import re
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[1]
_DOCKERFILE = (_REPO / "Dockerfile").read_text(encoding="utf-8")
_CI = yaml.safe_load(
    (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"),
)
_PYPROJECT = tomllib.loads((_REPO / "pyproject.toml").read_text(encoding="utf-8"))
_RUNTIME_LOCK = "requirements.lock"
_DEV_LOCK = "requirements-dev.lock"
_ARGUMENTS = ("PRIMER_RELEASE", "PRIMER_COMMIT")

_FROM = re.compile(r"^FROM\s+(?P<base>\S+)\s+AS\s+(?P<name>\S+)\s*$", re.I)


def stages(text: str) -> dict[str, list[str]]:
    """Stage name -> its instructions in order: comments and blank lines
    dropped, continuation lines joined into one."""
    joined = re.sub(r"[ \t]*\\\n\s*", " ", text)
    found: dict[str, list[str]] = {}
    current: list[str] | None = None
    for raw in joined.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _FROM.match(line)
        if match:
            assert match["name"] not in found, f"stage {match['name']} twice"
            current = found.setdefault(match["name"], [])
        elif current is not None:
            current.append(line)
    return found


def _names(specs: list[str]) -> set[str]:
    return {
        re.split(r"[><=!~\[ ]", spec, maxsplit=1)[0].strip().lower().replace("_", "-")
        for spec in specs
    }


def _dev_only() -> set[str]:
    """The packages of `dev` that `dependencies` does not also declare
    (httpx is both: the email channel's client and the API test client)."""
    project = _PYPROJECT["project"]
    return _names(project["optional-dependencies"]["dev"]) - _names(
        project["dependencies"],
    )


def _locked(lock: str) -> set[str]:
    text = (_REPO / lock).read_text(encoding="utf-8")
    return {
        m.group(1).lower().replace("_", "-")
        for m in re.finditer(r"^([A-Za-z0-9_.\-]+)==", text, re.M)
    }


def installs(steps: list[str]) -> list[str]:
    """The lock file of every `pip install` of a stage, in order."""
    found = []
    for step in steps:
        if step.startswith("RUN ") and "pip install" in step:
            match = re.search(r"pip install .*?-r (\S+)", step)
            assert match is not None, step
            found.append(match.group(1))
    return found


def unhashed(text: str) -> list[str]:
    """Every `pip install` of the Dockerfile without --require-hashes
    written out."""
    return [
        step for steps in stages(text).values() for step in steps
        if step.startswith("RUN ") and "pip install" in step
        and "--require-hashes" not in step
    ]


def envs(steps: list[str]) -> dict[str, str]:
    found = {}
    for step in steps:
        if step.startswith("ENV "):
            name, _, value = step[4:].partition("=")
            found[name.strip()] = value.strip()
    return found


def unpassed(steps: list[str]) -> list[str]:
    """The machine's arguments a stage does not hand to its process: an
    ARG, then an ENV of the same name made of it."""
    args = {step.split()[1] for step in steps if step.startswith("ARG ")}
    env = envs(steps)
    return [
        name for name in _ARGUMENTS
        if name not in args
        or env.get(name) not in (f"${{{name}}}", f"${name}")
    ]


_STAGES = stages(_DOCKERFILE)


class TestTheStages:
    """M12. The machine builds `test` by its name and the service by
    building without --target -- which gives the LAST stage."""

    def test_three_stages_by_name_runtime_last(self) -> None:
        assert list(_STAGES) == ["builder", "test", "runtime"]

    def test_every_install_is_hash_checked(self) -> None:
        """M8. The flag written out: pip switches into hash-checking mode
        by itself only while the file carries hashes."""
        found = [lock for steps in _STAGES.values() for lock in installs(steps)]
        assert found == [_RUNTIME_LOCK, _DEV_LOCK]  # the pair: two installs
        assert unhashed(_DOCKERFILE) == []

    def test_the_guard_sees_an_unhashed_install(self) -> None:
        text = (
            "FROM x AS builder\n"
            "RUN pip install --require-hashes -r requirements.lock\n"
            "FROM x AS runtime\n"
            "RUN /opt/venv/bin/pip install \\\n    -r requirements.lock\n"
        )
        assert unhashed(text) == ["RUN /opt/venv/bin/pip install -r requirements.lock"]


class TestNoDevInTheRuntime:
    """done-when 1: the runtime image carries no package of `dev`, the
    test stage carries them all. M1-M4."""

    def test_the_runtime_venv_is_the_runtime_lock(self) -> None:
        """M2, M3: the runtime installs nothing itself -- its venv is the
        builder's, and the builder installs the runtime lock only."""
        assert installs(_STAGES["runtime"]) == []
        assert "COPY --from=builder /opt/venv /opt/venv" in _STAGES["runtime"]
        assert installs(_STAGES["builder"]) == [_RUNTIME_LOCK]

    def test_the_runtime_lock_has_no_dev_package(self) -> None:
        """M1. With the pair: the dev-only packages exist and the dev lock
        pins every one of them."""
        dev_only = _dev_only()
        assert {"pytest", "ruff", "mypy", "fakeredis"} <= dev_only
        assert dev_only <= _locked(_DEV_LOCK)
        assert dev_only & _locked(_RUNTIME_LOCK) == set()

    def test_the_test_stage_adds_the_dev_lock(self) -> None:
        """M4."""
        steps = _STAGES["test"]
        assert "COPY --from=builder /opt/venv /opt/venv" in steps
        assert installs(steps) == [_DEV_LOCK]


class TestTheMachinesArguments:
    """done-when 3: PRIMER_RELEASE and PRIMER_COMMIT, ARG and ENV of each
    stage that runs a process of comms; CI builds with both. M10, M11."""

    @pytest.mark.parametrize("stage", ["test", "runtime"])
    def test_the_stage_hands_both_to_the_process(self, stage: str) -> None:
        assert unpassed(_STAGES[stage]) == []

    def test_the_guard_sees_an_arg_without_its_env(self) -> None:
        steps = [
            "ARG PRIMER_RELEASE", "ARG PRIMER_COMMIT",
            "ENV PRIMER_RELEASE=${PRIMER_RELEASE}", "ENV PRIMER_COMMIT=sha",
        ]
        assert unpassed(steps) == ["PRIMER_COMMIT"]
        assert unpassed(steps[1:]) == ["PRIMER_RELEASE", "PRIMER_COMMIT"]

    def test_ci_builds_both_stages_with_both(self) -> None:
        """M11. Every build of the image job passes both arguments, from
        the job's environment, which takes them from the run."""
        job: dict[str, Any] = _CI["jobs"]["image"]
        assert job["env"]["PRIMER_RELEASE"] == "${{ github.ref_name }}"
        assert job["env"]["PRIMER_COMMIT"] == "${{ github.sha }}"
        builds = [
            build
            for step in job["steps"]
            for build in re.findall(
                r"docker build [^\n]*",
                re.sub(r"\\\n\s*", " ", str(step.get("run", ""))),
            )
        ]
        # The pair: the two builds, by name and by default.
        assert ["--target test" in build for build in builds] == [True, False]
        assert not any("--target runtime" in build for build in builds)
        for build in builds:
            for name in _ARGUMENTS:
                assert f'--build-arg {name}="${name}"' in build, build


class TestTheTestStageIsTheRuntimesEnvironment:
    """The suite runs in the test stage and says something of the
    runtime only if the two run alike: user, directory, environment.
    M17."""

    def test_same_user_directory_and_environment(self) -> None:
        test, runtime = _STAGES["test"], _STAGES["runtime"]

        def kept(steps: list[str], kind: str) -> list[str]:
            return [step for step in steps if step.startswith(f"{kind} ")]

        assert kept(test, "USER") == kept(runtime, "USER") == ["USER comms"]
        assert kept(test, "WORKDIR") == kept(runtime, "WORKDIR") == ["WORKDIR /app"]
        assert envs(test) == envs(runtime)
        assert {"PATH", "PYTHONPATH", *_ARGUMENTS} <= envs(runtime).keys()
