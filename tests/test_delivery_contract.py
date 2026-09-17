# =============================================================================
# COMMS Service -- what the delivery promises outside the code
# =============================================================================
#
# Three facts that live in files nobody imports -- the version number, the
# dependency lock, the environment templates -- and that an integrator or an
# on-call engineer reads before they ever read a module. Each of them is a
# claim made in one place and consumed in another, which is the shape that
# rots silently: nothing fails when they drift, the failure arrives later
# and somewhere else.
# =============================================================================

import re
import tomllib
from pathlib import Path

import pytest

from app.core.config import APP_VERSION

_ROOT = Path(__file__).resolve().parent.parent
_PYPROJECT = tomllib.loads((_ROOT / "pyproject.toml").read_text())
_LOCK = (_ROOT / "requirements.lock").read_text()


def _requirement_name(spec: str) -> str:
    """The distribution name out of a requirement string."""
    return re.split(r"[><=!~\[ ]", spec, maxsplit=1)[0].strip().lower()


def _declared_dependencies() -> set[str]:
    project = _PYPROJECT["project"]
    specs = list(project["dependencies"])
    specs += list(project["optional-dependencies"]["dev"])
    return {_requirement_name(spec).replace("_", "-") for spec in specs}


def _locked_distributions() -> set[str]:
    names = set()
    for line in _LOCK.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "==" not in line:
            continue
        names.add(line.split("==", 1)[0].strip().lower().replace("_", "-"))
    return names


class TestTheVersionIsOneNumber:
    """The number an incident is opened against.

    It stood at 0.1.0 through every release up to v1.3.0, so "which
    version of comms is deployed" had no answer from outside the box --
    /health reported the same string on every one of them.
    """

    def test_the_two_sources_agree(self) -> None:
        """Two files carry it, so they are pinned to each other: raising
        one alone is exactly the mistake that produced 0.1.0-forever."""
        assert _PYPROJECT["project"]["version"] == APP_VERSION

    def test_the_version_is_not_the_placeholder(self) -> None:
        """The pair to the agreement above: two sources agreeing on
        0.1.0 would satisfy it and mean nothing. A released version has
        a major, and 0.1.0 is what the template shipped with.
        """
        assert APP_VERSION != "0.1.0"
        assert re.fullmatch(r"\d+\.\d+\.\d+", APP_VERSION)
        assert int(APP_VERSION.split(".")[0]) >= 1


class TestTheLockCoversWhatIsDeclared:
    """The image and CI install from requirements.lock, not from the
    ranges in pyproject. A dependency added to pyproject and not
    regenerated into the lock does not fail the build -- it fails at
    import, in the container, later.
    """

    @pytest.mark.parametrize("name", sorted(_declared_dependencies()))
    def test_every_declared_dependency_is_pinned(self, name: str) -> None:
        assert name in _locked_distributions(), (
            f"{name} is declared in pyproject and absent from "
            f"requirements.lock -- regenerate the lock in this commit "
            f"(the command is in the lock's own header)"
        )

    @pytest.mark.parametrize("name", sorted(_declared_dependencies()))
    def test_every_pinned_line_carries_a_hash(self, name: str) -> None:
        """A version pin says WHICH release; a hash says which ARTIFACT.

        The guard exists because losing the hashes costs nothing
        visible: regenerate without --generate-hashes and the file
        still installs, still pins every version, and quietly stops
        checking what was downloaded. Nothing fails, so nothing tells
        anyone -- the same shape as a comment that points at a file
        that no longer exists.
        """
        lines = _LOCK.splitlines()
        for position, line in enumerate(lines):
            head = line.split("==")[0].strip().lower().replace("_", "-")
            if "==" in line and not line.startswith("#") and head == name:
                rest = "\n".join(lines[position:position + 40])
                assert "--hash=sha256:" in rest.split("\n")[1], (
                    f"{name} is pinned without a hash -- the lock was "
                    f"regenerated without --generate-hashes (the command "
                    f"is in the lock's own header)"
                )
                return
        pytest.fail(f"{name} is not in the lock at all")

    def test_the_hashes_cover_more_than_one_artifact(self) -> None:
        """THE PAIR to the line-by-line check: a file where every
        package carries exactly one hash would satisfy it and would
        pin comms to one platform's wheel -- an image built on another
        architecture could then not be installed at all. Packages with
        compiled wheels carry a hash per artifact.
        """
        hashes = _LOCK.count("--hash=sha256:")
        assert hashes > len(_locked_distributions()) * 2

    def test_everything_in_the_lock_is_pinned_exactly(self) -> None:
        """A lock with a range in it is not a lock. The pair to the
        coverage test above: coverage is satisfied by a file that
        merely names the packages."""
        loose = [
            line.strip()
            for line in _LOCK.splitlines()
            if line.strip()
            and not line.startswith("#")
            and not line.startswith(" ")
            and not line.strip().startswith("--hash")
            and "==" not in line
        ]
        assert loose == []
        assert len(_locked_distributions()) > len(_declared_dependencies())


class TestTheTemplatesCarryTheContract:
    """The stream and the group are half of an agreement.

    A relay pointing at a name nobody reads is not an error on either
    side: it writes into a stream with no consumer, and the
    notifications simply never arrive. The names are defaulted, so the
    deploy works without them -- which is exactly why they have to be
    written down, or the other side has to read our source to learn
    what to match.
    """

    @pytest.mark.parametrize(
        "template", ["deploy/.env.example", ".env.example"],
    )
    @pytest.mark.parametrize(
        "key", ["COMMS_EVENTS_STREAM", "COMMS_CONSUMER_GROUP"],
    )
    def test_the_stream_contract_is_in_the_template(
        self, template: str, key: str
    ) -> None:
        text = (_ROOT / template).read_text()
        assert re.search(rf"^{key}=", text, re.M), (
            f"{key} is not a key line in {template} -- an integrator "
            f"cannot match a name that is only in our source"
        )

    def test_the_deploy_template_carries_the_credentials_too(self) -> None:
        """The pair: the contract names are useless in a template that
        cannot bring the service up at all. This is the half a review
        reported as missing -- it is present, and this test is what
        keeps the report true next time."""
        text = (_ROOT / "deploy/.env.example").read_text()
        for key in ("COMMS_SERVICE_TOKEN", "REDIS_URL", "DATABASE_URL"):
            assert re.search(rf"^{key}=", text, re.M), key

    def test_the_documented_names_are_the_defaults(self) -> None:
        """The template and the code must say the same thing. A
        template that documents a name the service does not default to
        would send every integrator to a stream we do not read."""
        from app.core.config import Settings

        defaults = Settings.model_fields
        text = (_ROOT / "deploy/.env.example").read_text()
        for key, field in (
            ("COMMS_EVENTS_STREAM", "comms_events_stream"),
            ("COMMS_CONSUMER_GROUP", "comms_consumer_group"),
        ):
            documented = re.search(rf"^{key}=(.*)$", text, re.M)
            assert documented is not None
            assert documented.group(1).strip() == defaults[field].default
