# =============================================================================
# H10 -- every container image comms names is pinned by digest
# =============================================================================
#
# Where comms names an image: the Dockerfile's FROM (both stages), the
# compose file's `image:` (comms-postgres, comms-redis). CI names none of
# its own: it reads Postgres from the compose file. Each pin is
# `name:tag@sha256:<64 hex>` -- the tag for the reader and for Dependabot,
# the digest deciding. The digests' VALUES are not held here: Dependabot
# changes them, by pull request; what is held is that each one is there.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the digest dropped from one compose image         -> TestThePins
#   M2  the digest dropped from one Dockerfile stage      -> TestThePins
#   M3  a `services:` image back in CI                    -> TestCiNamesNone
#   M4  a digest one hex short                            -> TestThePins
#   M5  the docker-compose ecosystem out of dependabot.yml -> TestDependabot
# =============================================================================

import re
from pathlib import Path
from typing import Any

import yaml

_REPO = Path(__file__).resolve().parents[1]
_DOCKERFILE = _REPO / "Dockerfile"
_COMPOSE = _REPO / "deploy" / "docker-compose.yml"
_WORKFLOWS = _REPO / ".github" / "workflows"
_DEPENDABOT = _REPO / ".github" / "dependabot.yml"

_PIN = re.compile(r"^[a-z0-9][a-z0-9._/-]*:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$")
_FROM = re.compile(r"^FROM\s+(?P<ref>\S+)(?:\s+AS\s+(?P<stage>\S+))?\s*$", re.I | re.M)


def _dockerfile_images() -> list[str]:
    """Every FROM that names an image -- a FROM of an earlier stage's
    name is not one."""
    stages: set[str] = set()
    images: list[str] = []
    for match in _FROM.finditer(_DOCKERFILE.read_text(encoding="utf-8")):
        if match["ref"] not in stages:
            images.append(match["ref"])
        if match["stage"]:
            stages.add(match["stage"])
    return images


def _compose_images() -> dict[str, str]:
    services: dict[str, Any] = yaml.safe_load(
        _COMPOSE.read_text(encoding="utf-8"),
    )["services"]
    return {
        name: service["image"] for name, service in services.items()
        if "image" in service
    }


class TestThePins:
    """done-when 1 (2). M1, M2, M4."""

    def test_every_image_carries_a_digest(self) -> None:
        named = [
            *(f"Dockerfile: {ref}" for ref in _dockerfile_images()),
            *(f"compose {name}: {ref}" for name, ref in _compose_images().items()),
        ]
        # The pair: the walk finds them -- two stages, two services.
        assert len(named) == 4, named
        unpinned = [entry for entry in named if not _PIN.match(entry.split(": ", 1)[1])]
        assert unpinned == []

    def test_both_stages_build_on_one_image(self) -> None:
        images = _dockerfile_images()
        assert len(images) == 2
        assert len(set(images)) == 1, images

    def test_the_lines_the_boxes_stay_on(self) -> None:
        """The tags -- the lines Dependabot keeps within: Python 3.12,
        Postgres 16, Redis 7."""
        tags = {
            ref.split("@")[0]
            for ref in [*_dockerfile_images(), *_compose_images().values()]
        }
        assert tags == {"python:3.12-slim", "postgres:16", "redis:7-alpine"}


class TestCiNamesNone:
    """CI runs the images the compose file pins and names none itself --
    a `services:` or `container:` image would be a pin no Dependabot
    ecosystem keeps (H10 gate 0.1). M3."""

    def test_no_image_of_its_own(self) -> None:
        names = {ref.split(":")[0] for ref in _compose_images().values()}
        names |= {ref.split(":")[0] for ref in _dockerfile_images()}
        workflows = sorted(_WORKFLOWS.glob("*.yml"))
        assert workflows  # the pair: there is CI to read
        for path in workflows:
            text = path.read_text(encoding="utf-8")
            jobs: dict[str, Any] = yaml.safe_load(text)["jobs"]
            for name, job in jobs.items():
                assert "services" not in job, f"{path.name}:{name}"
                assert "container" not in job, f"{path.name}:{name}"
            for image in names:
                assert not re.search(rf"\b{re.escape(image)}:\w", text), (
                    f"{path.name} names {image} with a tag"
                )

    def test_ci_reads_postgres_from_the_compose_file(self) -> None:
        text = (_WORKFLOWS / "ci.yml").read_text(encoding="utf-8")
        reads = re.findall(
            r"yaml\.safe_load\(open\('deploy/docker-compose\.yml'\)\)"
            r"\['services'\]\['comms-postgres'\]\['image'\]",
            text,
        )
        assert len(reads) == 2  # the suite's job and the image's job


class TestDependabot:
    """done-when 2: every place with an image is kept by an ecosystem,
    into `test`, major lines (and Python's minor) left to a decision.
    M5."""

    @staticmethod
    def _updates() -> dict[str, dict[str, Any]]:
        config: dict[str, Any] = yaml.safe_load(
            _DEPENDABOT.read_text(encoding="utf-8"),
        )
        return {entry["package-ecosystem"]: entry for entry in config["updates"]}

    def test_both_image_files_are_kept(self) -> None:
        updates = self._updates()
        directories = (
            updates["docker"]["directory"], updates["docker-compose"]["directory"],
        )
        assert directories == ("/", "/deploy")
        assert _DOCKERFILE.parent == _REPO
        assert _COMPOSE.parent == _REPO / "deploy"
        for ecosystem in ("docker", "docker-compose"):
            entry = updates[ecosystem]
            assert entry["target-branch"] == "test"
            assert {
                "dependency-name": "*",
                "update-types": ["version-update:semver-major"],
            } in entry["ignore"]

    def test_python_stays_on_its_line(self) -> None:
        assert {
            "dependency-name": "python",
            "update-types": ["version-update:semver-minor"],
        } in self._updates()["docker"]["ignore"]
