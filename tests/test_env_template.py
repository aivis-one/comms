# =============================================================================
# COMMS Service -- the deploy's environment template is every knob (G4)
# =============================================================================
#
# deploy/.env.example is the ONE template (the root copy drifted from it
# and is gone). It is held to the code both ways:
#
#   forward  every setting of app/core/config.py is a variable line,
#            except what docker-compose.yml sets itself for the comms
#            processes (`environment:`, read as YAML) -- a value in .env
#            would be overridden there, and that fact lives once, in
#            compose;
#   reverse  every variable line is a setting, or a variable comms'
#            compose file or installer reads, each found in that file in
#            the form it is read;
#   values   a line commented out, and an operator's line, holds the
#            setting's default (an example only for a setting whose
#            default is "unset", and the comment says so);
#   writers  the `# by:` line above each variable; the ones `install`
#            writes are exactly the ones generate_env writes.
#
# Mutations (built once, by hand):
#   M12 a variable line taken out           -> test_every_setting_is_there
#   M13 BOGUS_VAR= added                    -> test_every_variable_is_read
#   M14 #NOTIFICATION_RETENTION_DAYS=91     -> test_the_values_are_the_defaults
# =============================================================================

import re
from pathlib import Path

import yaml

from app.core.config import Settings

_REPO = Path(__file__).resolve().parents[1]
_TEMPLATE = (_REPO / "deploy" / ".env.example").read_text(encoding="utf-8")
_COMPOSE = (_REPO / "deploy" / "docker-compose.yml").read_text(encoding="utf-8")
_INSTALLER = (_REPO / "deploy" / "comms-deploy.sh").read_text(encoding="utf-8")

_LINE = re.compile(r"^(#?)([A-Z][A-Z0-9_]+)=(.*)$")
_WRITERS = {"install", "install, then a product installer",
            "a product installer", "operator"}

# Variables of the template that are not settings, and where each is
# read: by the compose file (interpolation, or the image of a service
# that takes ./.env) or by the installer.
_NOT_SETTINGS = {
    "POSTGRES_USER": "compose",
    "POSTGRES_DB": "compose",
    "REDIS_PASSWORD": "compose",
    "PROFILE_DIR": "compose",
    "PROFILE_OVERLAY_DIR": "compose",
    "POSTGRES_PASSWORD": "installer",
    "PRODUCT_ENV_PATH": "installer",
}


def _variables(text: str) -> list[tuple[bool, str, str, int]]:
    """(commented, name, value, line index) of every variable line."""
    found = []
    for index, line in enumerate(text.splitlines()):
        match = _LINE.match(line)
        if match:
            found.append((bool(match.group(1)), match.group(2), match.group(3), index))
    return found


def _compose_environment(text: str) -> set[str]:
    """The keys every service of a compose file sets in `environment:`,
    read as YAML -- a name in a comment is not one."""
    names: set[str] = set()
    for service in (yaml.safe_load(text).get("services") or {}).values():
        environment = service.get("environment") or {}
        if isinstance(environment, list):
            names |= {item.split("=", 1)[0] for item in environment}
        else:
            names |= set(environment)
    return names


def _settings() -> set[str]:
    return {name.upper() for name in Settings.model_fields}


def _generate_env() -> str:
    match = re.search(r"^generate_env\(\) \{\n.*?^\}\n", _INSTALLER, re.S | re.M)
    assert match, "generate_env not found at column zero"
    return match.group(0)


def _code_lines(text: str) -> str:
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )


class TestBothWays:
    def test_compose_sets_what_the_template_leaves_out(self) -> None:
        """The pair of the subtraction: it is not empty, and it holds
        both template directories."""
        subtracted = _compose_environment(_COMPOSE)
        assert {"TEMPLATES_DIR", "TEMPLATES_OVERLAY_DIR"} <= subtracted
        assert subtracted <= _settings()

    def test_a_name_in_a_compose_comment_is_not_subtracted(self) -> None:
        text = (
            "services:\n"
            "  a:\n"
            "    # LOG_LEVEL: DEBUG\n"
            "    environment:\n"
            "      TEMPLATES_DIR: /profile\n"
            "  b:\n"
            "    environment:\n"
            "      - DLQ_MAXLEN=5\n"
        )
        assert _compose_environment(text) == {"TEMPLATES_DIR", "DLQ_MAXLEN"}

    def test_every_setting_is_there(self) -> None:
        expected = _settings() - _compose_environment(_COMPOSE)
        written = {name for _c, name, _v, _i in _variables(_TEMPLATE)}
        assert expected - written == set()
        # The pair: the walk compared settings, the knobs a review named
        # among them.
        assert len(expected) == 44
        assert {
            "NOTIFICATION_RETENTION_DAYS",
            "DLQ_MAXLEN",
            "NOTIFICATION_LANE_NORMAL_BATCH_SIZE",
        } <= expected & written

    def test_nothing_compose_sets_is_written(self) -> None:
        written = {name for _c, name, _v, _i in _variables(_TEMPLATE)}
        assert written & _compose_environment(_COMPOSE) == set()

    def test_every_variable_is_read(self) -> None:
        compose = _code_lines(_COMPOSE)
        installer = _code_lines(_INSTALLER)
        for _c, name, _v, _i in _variables(_TEMPLATE):
            if name in _settings():
                continue
            where = _NOT_SETTINGS.get(name)
            assert where is not None, f"{name} is read by nobody"
            if where == "compose":
                assert re.search(
                    rf"\$\{{{name}\b|\$\$\{{{name}\}}|\$\${name}\b", compose,
                ), name
            else:
                assert re.search(rf"\b{name}\b", installer), name
        assert all(
            name in {n for _c, n, _v, _i in _variables(_TEMPLATE)}
            for name in _NOT_SETTINGS
        )

    def test_every_variable_is_written_once(self) -> None:
        names = [name for _c, name, _v, _i in _variables(_TEMPLATE)]
        assert len(names) == len(set(names)) == 51


class TestValues:
    def test_the_values_are_the_defaults(self) -> None:
        lines = _TEMPLATE.splitlines()
        checked = 0
        for commented, name, value, index in _variables(_TEMPLATE):
            operator = lines[index - 1] == "# by: operator"
            if not (commented or operator) or name not in _settings():
                continue
            default = Settings.model_fields[name.lower()].default
            if default is None:
                # "Unset" has no value to write: the line is an example,
                # and its comment says so.
                block = "\n".join(lines[max(0, index - 8):index])
                assert "example, not the default" in block, name
                continue
            assert value == str(default), name
            checked += 1
        assert checked >= 30


class TestWriters:
    def test_every_variable_names_its_writer(self) -> None:
        lines = _TEMPLATE.splitlines()
        for _c, name, _v, index in _variables(_TEMPLATE):
            marker = lines[index - 1]
            assert marker.startswith("# by: "), name
            assert marker[len("# by: "):] in _WRITERS, name

    def test_install_writes_what_the_template_says(self) -> None:
        lines = _TEMPLATE.splitlines()
        marked = {
            name for _c, name, _v, index in _variables(_TEMPLATE)
            if lines[index - 1][len("# by: "):].split(",")[0] == "install"
        }
        written = set(re.findall(r"^([A-Z][A-Z0-9_]+)=", _generate_env(), re.M))
        assert marked == written
        assert len(written) > 15
