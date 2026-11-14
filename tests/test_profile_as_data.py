# =============================================================================
# The profile is data comms takes; `comms validate --profile` (W4-1)
# =============================================================================
#
# Under Primer's machine the product's profile comes as data comms takes
# (form D of protocol 4.5, primer.yaml `accepts.profile`): the machine
# copies the product's directory and runs `comms validate --profile <dir>`
# on it in a throwaway container of the new image, with no network and
# nothing of the instance but the path -- before the migration. The
# validator is the loader of the start (app/profile/loader.py), read
# without the settings, which refuse a process with no instance.
#
#   - the fixture profile is good: exit 0 -- run as the machine runs it,
#     the image's launcher in a process whose environment is the path and
#     nothing else;
#   - a bad one: exit 1 and EVERY problem, one line each, named by file:
#     a type missing in a language of the profile, a templates file that
#     is not YAML, a template for a type types.yaml does not declare --
#     never the first alone; a types.yaml that is not YAML, alone;
#   - the instance not known: every language of the profile is checked as
#     the default an instance may have; what only the instance can say --
#     that DEFAULT_LANGUAGE has templates -- is the start's;
#   - the contract (deploy/INTEGRATION.md section 3) against the code.
#
# MUTATIONS these tests were written against (each turns one red):
#   V1  validate stops at the first finding       -> TestEveryProblem
#   V2  validate imports the settings              -> TestAsTheMachineRunsIt
#   V3  exit 0 with findings                       -> TestEveryProblem
#   V4  the languages' completeness not checked    -> TestEveryProblem
#   V5  only the deploy's default checked, the
#       instance not known                         -> TestNoInstance
#   V6  the default's templates not asked at start -> TestTheStart
#   V7  the contract's command not primer.yaml's   -> TestTheContract
# =============================================================================

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from app.cli import main
from app.core.config import settings
from app.core.exceptions import ProfileError
from app.profile.loader import (
    FileProfileSource,
    ProfileDeploy,
    _deploy,
    load_profile,
    validate_profile,
)
from tests.conftest import FIXTURE_PROFILE_DIR

_REPO = Path(__file__).resolve().parents[1]
_LAUNCHER = _REPO / "bin" / "comms"
_PRIMER = yaml.safe_load((_REPO / "primer.yaml").read_text(encoding="utf-8"))
# Where the machine puts the copy of a kind in the container (its
# CONTAINER_DATA, protocol 4.5).
_MACHINE_PATH = "/primer/data/profile"


def _copy(tmp_path: Path) -> Path:
    root = tmp_path / "profile"
    shutil.copytree(FIXTURE_PROFILE_DIR, root)
    return root


def _rewrite(root: Path, locale: str, change: dict[str, object]) -> None:
    """templates/<locale>.yaml with `change` applied to its types (a None
    value removes the type), written back without the fixture's aliases."""
    path = root / "templates" / f"{locale}.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    for type_key, sheet in change.items():
        if sheet is None:
            doc.pop(type_key, None)
        else:
            doc[type_key] = sheet
    path.write_text(yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8")


def _launch(profile: Path) -> subprocess.CompletedProcess[str]:
    """`comms validate --profile <dir>` by the image's launcher, as the
    machine runs it: the profile's path under TEMPLATES_DIR and nothing
    else of the instance -- no DATABASE_URL, no token, no flag."""
    return subprocess.run(
        [sys.executable, str(_LAUNCHER), "validate", "--profile", str(profile)],
        env={
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": str(_REPO),
            "TEMPLATES_DIR": str(profile),
        },
        capture_output=True, text=True, timeout=60, check=False,
    )


class TestAsTheMachineRunsIt:
    """V2: a validator that built the settings would refuse every profile
    in the machine's container (`DATABASE_URL is required`)."""

    def test_the_fixture_profile_is_good(self) -> None:
        done = _launch(FIXTURE_PROFILE_DIR)
        assert (done.returncode, done.stderr) == (0, ""), done.stderr
        assert done.stdout == (
            f"comms validate: the profile at {FIXTURE_PROFILE_DIR} is good\n"
        )

    def test_the_pair_a_bad_one_is_refused_in_the_same_container(
        self, tmp_path: Path,
    ) -> None:
        root = _copy(tmp_path)
        _rewrite(root, "ru", {"unit_presented": None})
        done = _launch(root)
        assert done.returncode == 1
        assert done.stdout == ""
        assert "templates/ru.yaml: unit_presented has no sheet in ru" in done.stderr


class TestEveryProblem:
    """V1, V3, V4: every problem, one line each, named by its file."""

    def test_three_problems_three_lines(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        root = _copy(tmp_path)
        _rewrite(root, "ru", {"unit_presented": None})
        _rewrite(root, "en", {"unit_evnet": {"telegram": {"body": "{body}"}}})
        (root / "templates" / "de.yaml").write_text(
            "unit_event: {telegram: [\n", encoding="utf-8",
        )
        assert main(["validate", "--profile", str(root)]) == 1
        out, err = capsys.readouterr()
        assert out == ""
        lines = err.splitlines()
        assert all(line.startswith("comms validate: ") for line in lines), err
        named = [
            "templates/ru.yaml: unit_presented has no sheet in ru and has one in en",
            "templates/de.yaml: Invalid YAML",
            "templates/en.yaml: template for type 'unit_evnet'",
        ]
        for words in named:
            assert len([line for line in lines if words in line]) == 1, (words, err)
        # The typo is a type of its own in en alone: en has it, ru not.
        assert "unit_evnet has no sheet in ru" in err
        assert len(lines) == 4, err

    def test_a_types_file_that_is_not_yaml_is_alone(self, tmp_path: Path) -> None:
        root = _copy(tmp_path)
        (root / "types.yaml").write_text("types: [\n", encoding="utf-8")
        _rewrite(root, "ru", {"unit_presented": None})
        findings = validate_profile(root)
        assert len(findings) == 1
        assert "Invalid YAML" in findings[0] and "types.yaml" in findings[0]

    def test_a_directory_that_is_not_there(self, tmp_path: Path) -> None:
        findings = validate_profile(tmp_path / "nowhere")
        assert len(findings) == 1
        assert "Profile directory does not exist" in findings[0]

    def test_the_pair_the_fixture_has_none(self) -> None:
        assert validate_profile(FIXTURE_PROFILE_DIR) == ()


# The en title opens what the en body closes; ru has a title of its own
# that opens a tag and no body. A ru recipient gets the ru title over the
# en body where en is the default -- whole -- and over the stored body
# where ru is -- "<b>" never closed.
_OPENS_IN_RU = {
    "en": {"unit_event": {"telegram": {"title": "<b>{title}", "body": "{body}</b>"}}},
    "ru": {"unit_event": {"telegram": {"title": "<b>{title}"}}},
}


class TestNoInstance:
    """V5: with no instance every language of the profile is checked as
    the default; the start checks the instance's own."""

    def test_a_text_broken_under_one_default_is_refused(self, tmp_path: Path) -> None:
        root = _copy(tmp_path)
        for locale, change in _OPENS_IN_RU.items():
            _rewrite(root, locale, change)
        findings = validate_profile(root)
        assert len(findings) == 1, findings
        assert (
            "templates/ru.yaml: unit_event.telegram.title+body for a ru"
            in findings[0]
        )
        assert "Telegram would not parse" in findings[0]

    def test_the_pair_the_start_on_en_loads_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = _copy(tmp_path)
        for locale, change in _OPENS_IN_RU.items():
            _rewrite(root, locale, change)
        monkeypatch.setattr(settings, "default_language", "en")
        load_profile(FileProfileSource(root))

    def test_the_deploy_is_the_settings_again_after(self) -> None:
        validate_profile(FIXTURE_PROFILE_DIR)
        assert _deploy() == ProfileDeploy(
            default_language=settings.default_language,
            max_delivery_attempts=settings.notification_max_delivery_attempts,
            backoff_base_seconds=settings.notification_retry_backoff_base_seconds,
            backoff_max_seconds=settings.notification_retry_backoff_max_seconds,
            retention_days=settings.notification_retention_days,
        )


class TestTheStart:
    """V6: the start knows the instance -- its default language has
    templates when the profile has any."""

    def test_a_default_the_profile_has_no_texts_in_is_refused(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "default_language", "uk")
        with pytest.raises(ProfileError) as caught:
            load_profile(FileProfileSource(FIXTURE_PROFILE_DIR))
        refusal = str(caught.value)
        assert "templates/uk.yaml: the instance's default language uk" in refusal
        assert "(DEFAULT_LANGUAGE) has no templates" in refusal
        assert "the profile's languages are en, ru" in refusal

    def test_the_pair_a_default_it_has_loads(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "default_language", "ru")
        load_profile(FileProfileSource(FIXTURE_PROFILE_DIR))

    def test_the_same_profile_is_good_with_no_instance(self) -> None:
        """uk is a default only an instance can choose: the validator
        does not know it and does not refuse the profile for it."""
        assert validate_profile(FIXTURE_PROFILE_DIR) == ()

    def test_the_start_holds_the_languages_too(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = _copy(tmp_path)
        _rewrite(root, "ru", {"unit_presented": None})
        monkeypatch.setattr(settings, "default_language", "en")
        with pytest.raises(ProfileError, match="unit_presented has no sheet in ru"):
            load_profile(FileProfileSource(root))


_INTEGRATION = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")
_SECTION_3 = _INTEGRATION[
    _INTEGRATION.index("## 3. The product profile"):
    _INTEGRATION.index("### The type record -- every field")
]


class TestTheContract:
    """V7: deploy/INTEGRATION.md section 3 against the code."""

    def test_the_kind_and_its_command_are_primer_yamls(self) -> None:
        accepted = _PRIMER["accepts"]["profile"]
        assert (
            f'profile: {{ validate: "{accepted["validate"]}", '
            f'env: {accepted["env"]} }}'
        ) in _SECTION_3
        assert "dependencies.comms.data:\n{profile:" in _SECTION_3
        assert f"`{_MACHINE_PATH}`" in _SECTION_3

    def test_the_ceiling_it_names_is_in_the_compose_file(self) -> None:
        compose = (_REPO / "deploy" / "docker-compose.yml").read_text(encoding="utf-8")
        assert (
            "KNOWN CEILING (acknowledged by design -- the profile on comms-app alone)"
            in compose
        )
        assert "the KNOWN CEILING in comms' `deploy/docker-compose.yml`" in _SECTION_3

    def test_the_languages_are_the_settings(self) -> None:
        section = _INTEGRATION[
            _INTEGRATION.index("### Languages -- `LANGUAGES` and `DEFAULT_LANGUAGE`"):
            _INTEGRATION.index("### The type record -- every field")
        ]
        rows = [
            line.split("|")[1].strip().strip("`")
            for line in section.splitlines()
            if line.startswith("| `")
        ]
        assert rows == ["LANGUAGES", "DEFAULT_LANGUAGE"]
        for name in rows:
            assert hasattr(settings, name.lower()), name
        assert "byte for byte" in section and "`en-GB` and `EN` are no `en`" in section
