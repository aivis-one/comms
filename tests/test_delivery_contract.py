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

import json
import re
import tomllib
from pathlib import Path
from typing import Any

import pytest

from app.api.prefs import PeriodIn, PreferencesPatch
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


# -----------------------------------------------------------------------------
# The preferences contract: one full copy, held to the code
# -----------------------------------------------------------------------------


_DOC = (_ROOT / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")
_PREFS_SOURCE = (_ROOT / "app" / "api" / "prefs.py").read_text(encoding="utf-8")

# The 1.x schedule, in every form it was written: one quiet window as an
# object {from, to, days} whose `days` were the days it STARTED on,
# crossing midnight when from > to, with all three fields required.
_REMOVED_MODEL_FORMS = (
    '"days"', "`days`", "START", "overnight", "from > to",
    "all three fields", "{from, to, days}", "FROZEN",
)


def _preferences_section() -> str:
    start = _DOC.index("## 6. Preferences")
    return _DOC[start:_DOC.index("\n## ", start + 1)]


def _json_examples() -> list[dict[str, Any]]:
    blocks = re.findall(r"```json\n(.*?)\n```", _preferences_section(), re.S)
    return [json.loads(block) for block in blocks]


def _prefs_header() -> str:
    """The leading comment block of app/api/prefs.py."""
    lines = []
    for line in _PREFS_SOURCE.splitlines():
        if not line.startswith("#"):
            break
        lines.append(line)
    return "\n".join(lines)


def _period_fields() -> set[str]:
    return {
        field.alias or name for name, field in PeriodIn.model_fields.items()
    }


class TestThePreferencesContract:
    """The contract of the settings screen lives in ONE full copy --
    deploy/INTEGRATION.md, "6. Preferences" -- and the header of
    app/api/prefs.py points at it. Before 3.0.0 the header was the only
    copy, and it described the 1.x schedule (one quiet window, `days` as
    start days, crossing midnight) years after the code had turned it
    into a list of ALLOWED periods: a connector written from it would
    have stored the opposite of the person's choice."""

    def test_the_get_example_has_the_keys_get_returns(self) -> None:
        get_example, _ = _json_examples()
        assert set(get_example) == {"categories", "schedule", "timezone"}

    async def test_the_get_example_matches_a_real_answer(
        self, client: Any, db_session: Any,
    ) -> None:
        """What GET REALLY returns -- the facade's own keys and the
        period's own keys -- is what the document shows."""
        from tests.helpers import create_recipient

        recipient = await create_recipient(db_session)
        await db_session.commit()
        get_example, _ = _json_examples()
        url = f"/api/v1/recipients/{recipient.id}/preferences"
        written = await client.patch(
            url, json={"schedule": get_example["schedule"]},
        )
        assert written.status_code == 200, written.text
        answer = (await client.get(url)).json()
        assert set(answer) == set(get_example)
        assert answer["schedule"], "the pair: a schedule was really stored"
        for period in answer["schedule"]:
            assert set(period) == set(get_example["schedule"][0])

    def test_the_patch_example_has_the_writable_parts(self) -> None:
        _, patch_example = _json_examples()
        assert set(patch_example) == set(PreferencesPatch.model_fields)

    def test_a_period_has_the_fields_the_model_takes(self) -> None:
        get_example, patch_example = _json_examples()
        for period in [*get_example["schedule"], *patch_example["schedule"]]:
            assert set(period) == _period_fields()
        table = re.findall(r"^\| `(\w+)` \|", _preferences_section(), re.M)
        assert set(table) == _period_fields()

    @pytest.mark.parametrize("form", _REMOVED_MODEL_FORMS)
    def test_the_removed_model_is_gone(self, form: str) -> None:
        assert form not in _preferences_section(), form
        assert form not in _prefs_header(), form

    def test_the_new_model_is_there(self) -> None:
        """The pair: the section describes the list of allowed periods,
        and the header points at the section instead of copying it."""
        section = _preferences_section()
        assert "list of ALLOWED periods" in section
        assert '"day"' in section
        assert "6. Preferences" in _prefs_header()
        assert "GET /api" not in _prefs_header()
