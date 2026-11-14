# =============================================================================
# COMMS Service -- the deploy's environment reference is every knob (G4)
# =============================================================================
#
# deploy/.env.example is the ONE reference of the deploy's variables (the
# root copy drifted from it and is gone). No file is made from it since
# W1-2: Primer's deploy machine renders the instance's values by
# primer.yaml `env`, and deploy/docker-compose.yml hands them to the comms
# processes. It is held to the code both ways, and to the compose file
# and primer.yaml:
#
#   forward  every setting of app/core/config.py is a variable line;
#   reverse  every variable line is a setting, or a variable comms'
#            compose file reads, found there in the form it is read;
#   compose  every name compose substitutes from the environment
#            (${NAME}, ${NAME:-x}, ${NAME-x}, ${NAME:+x}, ${NAME:?x},
#            $NAME; $$ is an escape, not a substitution) is a variable
#            line (review 3.10.0: until the fix a new ${NAME} in compose
#            passed every test above);
#   values   a line by `default` holds the setting's default (an example
#            only for a setting whose default is "unset", and the comment
#            says so); a line by `compose` holds what compose sets;
#   writers  the `# by:` line above each variable: `primer.yaml` are
#            exactly the names compose substitutes, each a key of
#            primer.yaml `env`; `compose` are exactly the names compose
#            sets; `machine` are exactly the variables of the kinds of
#            data primer.yaml `accepts` (W4-1), which the machine's own
#            override sets. Until W1-2 the writers were the old installer's, held
#            to its generate_env, and compose's own values were left out
#            of the file; the installer is gone, and a value compose sets
#            is written as compose sets it, held here.
#
# Mutations (built once, by hand):
#   M12 a variable line taken out           -> test_every_setting_is_there
#   M13 BOGUS_VAR= added                    -> test_every_variable_is_read
#   M14 #NOTIFICATION_RETENTION_DAYS=91     -> test_the_values_are_the_defaults
#   M17 ${EXTRA_ASSETS_DIR} into compose    -> test_every_name_compose_
#                                              substitutes_is_there
#   M18 $EXTRA_ASSETS_DIR into compose      -> the same
#   W1-2 the DATABASE_URL line not compose's -> test_what_compose_sets_is_
#                                              written_as_compose_sets_it
# =============================================================================

import re
from pathlib import Path

import pytest
import yaml

from app.core.config import Settings

_REPO = Path(__file__).resolve().parents[1]
_TEMPLATE = (_REPO / "deploy" / ".env.example").read_text(encoding="utf-8")
_COMPOSE = (_REPO / "deploy" / "docker-compose.yml").read_text(encoding="utf-8")
_PRIMER = yaml.safe_load((_REPO / "primer.yaml").read_text(encoding="utf-8"))

_LINE = re.compile(r"^(#?)([A-Z][A-Z0-9_]+)=(.*)$")
_WRITERS = {"primer.yaml", "compose", "machine", "default"}

# Variables of the template that are not settings: keys of primer.yaml
# `env` the compose file reads to build a setting or to give a datastore
# its own variable.
_NOT_SETTINGS = {"POSTGRES_PASSWORD", "COMMS_EVENTS_PASSWORD"}


def _variables(text: str) -> list[tuple[bool, str, str, int]]:
    """(commented, name, value, line index) of every variable line."""
    found = []
    for index, line in enumerate(text.splitlines()):
        match = _LINE.match(line)
        if match:
            found.append((bool(match.group(1)), match.group(2), match.group(3), index))
    return found


def _by(writer: str) -> dict[str, str]:
    """name -> value of every variable line whose `# by:` is `writer`."""
    lines = _TEMPLATE.splitlines()
    return {
        name: value for _c, name, value, index in _variables(_TEMPLATE)
        if lines[index - 1] == f"# by: {writer}"
    }


def _compose_values(text: str) -> dict[str, set[str]]:
    """What the comms processes of a compose file -- the services built
    from the image -- SET in `environment:`, read as YAML (a name in a
    comment is not one): name -> every value it takes. The datastores'
    own variables are theirs, not settings of comms.

    A key that only passes its own variable through
    (`COMMS_SERVICE_TOKEN: ${COMMS_SERVICE_TOKEN:?}`) is not set by
    compose: the value is the machine's. TX-4 gave comms-postgres and
    comms-redis their own variables; W1-2 made every comms process take
    its values this way, the file has no env_file."""
    values: dict[str, set[str]] = {}
    for service in (yaml.safe_load(text).get("services") or {}).values():
        if "build" not in service:
            continue
        environment = service.get("environment") or {}
        if isinstance(environment, list):
            pairs = [[*item.split("=", 1), ""] for item in environment]
            items = {pair[0]: pair[1] for pair in pairs}
        else:
            items = dict(environment)
        for name, value in items.items():
            if re.fullmatch(rf"\$\{{{name}(?::?[-?][^}}]*)?\}}", str(value or "")):
                continue  # passed through, not set here
            values.setdefault(name, set()).add(str(value))
    return values


def _compose_environment(text: str) -> set[str]:
    return set(_compose_values(text))


def _settings() -> set[str]:
    return {name.upper() for name in Settings.model_fields}


# A name compose substitutes: `$` and a name, braced or bare (compose's
# own name pattern). `$$` is the escape for a literal `$` -- read first,
# so `$${NAME}` and `$$NAME` are never a substitution.
_SUBSTITUTION = re.compile(
    r"\$\$|\$\{([A-Za-z_][A-Za-z0-9_]*)|\$([A-Za-z_][A-Za-z0-9_]*)",
)


def _compose_substituted(text: str) -> set[str]:
    """Every name compose takes from the environment in `text`, in every
    form of substitution, nested defaults included (`${A:-${B}}`)."""
    return {
        match.group(1) or match.group(2)
        for match in _SUBSTITUTION.finditer(text)
        if match.group(0) != "$$"
    }


def _code_lines(text: str) -> str:
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )


class TestBothWays:
    def test_compose_sets_what_it_builds(self) -> None:
        """The pair of the compose rows: compose sets values, the two URLs
        it builds among them, all of them settings. Until W4-1 the
        profile's mount point was one of them; the machine sets it now
        (TestWriters.test_the_machines_own_are_the_accepted_kinds)."""
        set_here = _compose_environment(_COMPOSE)
        assert {"DATABASE_URL", "REDIS_URL"} <= set_here
        assert "TEMPLATES_DIR" not in set_here
        assert set_here <= _settings()

    def test_a_name_in_a_compose_comment_is_not_set(self) -> None:
        text = (
            "services:\n"
            "  a:\n"
            "    build: .\n"
            "    # LOG_LEVEL: DEBUG\n"
            "    environment:\n"
            "      TEMPLATES_DIR: /profile\n"
            "      COMMS_SERVICE_TOKEN: ${COMMS_SERVICE_TOKEN:?}\n"
            "  b:\n"
            "    build: .\n"
            "    environment:\n"
            "      - CONSUMER_BATCH_SIZE=5\n"
            "  store:\n"
            "    environment:\n"
            "      POSTGRES_DB: comms\n"
        )
        assert _compose_values(text) == {
            "TEMPLATES_DIR": {"/profile"}, "CONSUMER_BATCH_SIZE": {"5"},
        }

    def test_every_setting_is_there(self) -> None:
        """Until W1-2 the settings compose sets were subtracted (a value
        in the generated .env would have been overridden); no .env is
        generated now, and they are written as compose writes them."""
        written = {name for _c, name, _v, _i in _variables(_TEMPLATE)}
        assert _settings() - written == set()
        # The pair: the walk compared settings, the knobs a review named
        # among them. 50 since W1-2 (48 written before, and the two URLs);
        # 49 since W5-1, DLQ_MAXLEN gone with the dead letters; 50 since
        # W4-1, the default-locale setting gone, LANGUAGES and DEFAULT_LANGUAGE new.
        assert len(_settings()) == 50
        assert {
            "NOTIFICATION_RETENTION_DAYS",
            "CONSUMER_BATCH_SIZE",
            "NOTIFICATION_LANE_NORMAL_BATCH_SIZE",
        } <= _settings() & written

    def test_what_compose_sets_is_written_as_compose_sets_it(self) -> None:
        values = _compose_values(_COMPOSE)
        assert values  # the pair: compose sets something
        for name, given in values.items():
            assert len(given) == 1, f"{name}: two values over the services"
        assert _by("compose") == {
            name: value for name, (value,) in values.items()
        }

    def test_every_variable_is_read(self) -> None:
        compose = _code_lines(_COMPOSE)
        for _c, name, _v, _i in _variables(_TEMPLATE):
            if name in _settings():
                continue
            assert name in _NOT_SETTINGS, f"{name} is read by nobody"
            assert re.search(rf"\$\{{{name}\b", compose), name
        written = {n for _c, n, _v, _i in _variables(_TEMPLATE)}
        assert written >= _NOT_SETTINGS

    def test_every_name_compose_substitutes_is_there(self) -> None:
        substituted = _compose_substituted(_code_lines(_COMPOSE))
        written = {name for _c, name, _v, _i in _variables(_TEMPLATE)}
        assert substituted - written == set()
        # The pair: the scan found names, the two compose builds the URLs
        # from among them.
        assert {"POSTGRES_PASSWORD", "COMMS_EVENTS_PASSWORD"} <= substituted

    @pytest.mark.parametrize(
        ("text", "names"),
        [
            ("x: ${NAME}", {"NAME"}),
            ("x: ${NAME:-default}", {"NAME"}),
            ("x: ${NAME-default}", {"NAME"}),
            ("x: ${NAME:+alternative}", {"NAME"}),
            ("x: ${NAME:?it is required}", {"NAME"}),
            ("x: $NAME/sub", {"NAME"}),
            ("x: ${A:-${B}}", {"A", "B"}),
            ("x: $${NAME}", set()),
            ("x: $$NAME", set()),
            ("x: $$$NAME", {"NAME"}),
            ("# x: ${NAME}\n  # $NAME", set()),
        ],
        ids=[
            "braced", "default_if_unset_or_empty", "default_if_unset",
            "alternative", "required", "bare", "nested",
            "escaped_braced", "escaped_bare", "escape_then_bare", "comment",
        ],
    )
    def test_every_form_of_substitution(self, text: str, names: set[str]) -> None:
        assert _compose_substituted(_code_lines(text)) == names

    def test_every_variable_is_written_once(self) -> None:
        names = [name for _c, name, _v, _i in _variables(_TEMPLATE)]
        # 54 since W2-1: the three channel flags; 55 since W2-1b: the
        # next service token; 52 since W1-2: the installer's seven
        # variables gone, DATABASE_URL and REDIS_URL kept, the two URLs'
        # keys (POSTGRES_PASSWORD kept, COMMS_EVENTS_PASSWORD new), the
        # two template directories written; 51 since W5-1, DLQ_MAXLEN gone
        # with the dead letters; 52 since W4-1, the default-locale setting gone,
        # LANGUAGES and DEFAULT_LANGUAGE new.
        assert len(names) == len(set(names)) == 52


class TestValues:
    def test_the_values_are_the_defaults(self) -> None:
        lines = _TEMPLATE.splitlines()
        checked = 0
        for _commented, name, value, index in _variables(_TEMPLATE):
            if lines[index - 1] != "# by: default":
                continue
            assert name in _settings(), name
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

    def test_the_machines_are_what_compose_takes_from_it(self) -> None:
        """`primer.yaml` lines: exactly the names compose substitutes,
        each a key of primer.yaml `env` (the machine refuses any other in
        compose: scripts/check_primer_yaml.py). Until W1-2 the installer's
        lines were held to its generate_env here."""
        marked = set(_by("primer.yaml"))
        assert marked == _compose_substituted(_code_lines(_COMPOSE))
        assert marked <= set(_PRIMER["env"])
        assert len(marked) > 10

    def test_the_machines_own_are_the_accepted_kinds(self) -> None:
        """`machine` lines (W4-1): exactly the variables of the kinds of
        data comms takes, each set by the machine to the copy's path
        /primer/data/<kind> (protocol 4.5) -- a setting, and neither a
        key of `env` nor set by compose."""
        accepted = {
            kind["env"]: f"/primer/data/{name}"
            for name, kind in _PRIMER["accepts"].items()
        }
        assert accepted  # the pair: comms takes data
        assert _by("machine") == accepted
        assert set(accepted) <= _settings()
        assert set(accepted).isdisjoint(_PRIMER["env"])
        assert set(accepted).isdisjoint(_compose_environment(_COMPOSE))
