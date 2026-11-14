"""primer.yaml against comms' code (W2-2).

`env` holds exactly the values of an instance that comms reads -- held
both ways here, against the code's own registries: the settings
(app/core/config.py Settings), the channels (app/core/channels.py) and
the `comms` command (app/cli.py). What the machine's loader says of the
file is asked of the machine itself, in CI's job `machine`
(scripts/check_primer_yaml.py): the machine is not in comms' locks.

And the contract: the seams comms gives the product, `provides`, and the
one it takes, `requires` (W4-2), are the ones deploy/INTEGRATION.md
section 15 describes -- both ways.

And the hand-over (W1-2): with no env_file, a value reaches a comms
process only through `environment:` of deploy/docker-compose.yml -- so
every key of `env` that is a setting is passed to every process built
from the image, and the two values compose builds (COMPOSED) are built
from their keys. The machine checks that every reference is a key of
`env`; it does not check that a key is referenced at all.
"""

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from app import cli
from app.core.channels import CHANNEL_SPECS
from app.core.config import Settings

_REPO = Path(__file__).resolve().parents[1]
_PRIMER: dict[str, Any] = yaml.safe_load(
    (_REPO / "primer.yaml").read_text(encoding="utf-8"),
)
_ENV: dict[str, dict[str, Any]] = _PRIMER["env"]
_PROVIDES: list[str] = _PRIMER["provides"]
_REQUIRES: list[str] = _PRIMER["requires"]
_ACCEPTS: dict[str, dict[str, str]] = _PRIMER["accepts"]
# The variable of each kind of data comms takes: the machine's, never a
# key of `env` (S.accepts-env, W4-1).
_ACCEPTED_ENV: set[str] = {kind["env"] for kind in _ACCEPTS.values()}

# Every setting by its variable, with its default in code.
_DEFAULTS: dict[str, Any] = {
    name.upper(): field.default for name, field in Settings.model_fields.items()
}

# A setting whose default is None or "" has no value of its own: the
# instance must give it, or it switches something off. Each one is a key
# of `env`, or here -- with the reason it is not.
NOT_IN_ENV: dict[str, str] = {
    "DATABASE_URL": "built by compose from POSTGRES_PASSWORD (W1-2)",
    "REDIS_URL": "built by compose from COMMS_EVENTS_PASSWORD (W1-2)",
    "TEMPLATES_OVERLAY_DIR": (
        "the machine has no kind of data for the overlay: empty, no overlay"
    ),
    "COMMS_SERVICE_TOKEN_NEXT": (
        "the machine has no change of key yet (Primer's letter of 12.11)"
    ),
    "COMMS_WORKER_LANES": "unset is every lane: a default, not an absence",
}

# The two values comms reads that compose builds from keys of `env`: the
# key each is built from (tests/test_primer_yaml.py TestTheHandOver).
COMPOSED: dict[str, str] = {
    "DATABASE_URL": "POSTGRES_PASSWORD",
    "REDIS_URL": "COMMS_EVENTS_PASSWORD",
}

# A setting with a default in code is the product's constant and not a key
# of `env` (contract 3 rule 1) -- but for these, a fact of the instance.
DEFAULTED_IN_ENV: dict[str, str] = {
    "EMAIL_MAILGUN_REGION": (
        "the region of the instance's provider account: a deploy on the "
        "other region has no other way to say so (gate W2-2)"
    ),
}


def _unset_default(value: Any) -> bool:
    return value is None or value == ""


class TestEnvIsWhatCommsReads:
    def test_every_key_is_read_or_given(self) -> None:
        """A key of `env` is a setting, a variable of the `comms` command,
        a seam comms gives, or what compose builds a setting from."""
        known = (
            set(_DEFAULTS) | set(cli.READS) | set(_PROVIDES) | set(COMPOSED.values())
        )
        assert _ENV  # the pair: there are keys to read
        assert sorted(set(_ENV) - known) == []

    def test_no_key_is_a_setting_with_a_default(self) -> None:
        defaulted = {
            key for key in _ENV
            if key in _DEFAULTS and not _unset_default(_DEFAULTS[key])
        }
        assert defaulted == set(DEFAULTED_IN_ENV)
        # The pair: the rule sees defaults -- most settings have one.
        assert sum(not _unset_default(v) for v in _DEFAULTS.values()) > 30

    def test_every_value_comms_needs_is_in_env_or_said_why(self) -> None:
        needed = {key for key, value in _DEFAULTS.items() if _unset_default(value)}
        assert "TELEGRAM_ENABLED" in needed  # the pair: the flags are among them
        assert sorted(
            needed - set(_ENV) - set(NOT_IN_ENV) - _ACCEPTED_ENV,
        ) == []

    def test_the_exceptions_are_still_exceptions(self) -> None:
        """A row of NOT_IN_ENV is a setting with no default of its own and
        not a key of `env`; one that is no longer either is gone from here."""
        for key in NOT_IN_ENV:
            assert key in _DEFAULTS and _unset_default(_DEFAULTS[key]), key
            assert key not in _ENV, key
        for key in DEFAULTED_IN_ENV:
            assert key in _DEFAULTS and not _unset_default(_DEFAULTS[key]), key

    def test_what_compose_builds_from_is_minted(self) -> None:
        for setting, key in COMPOSED.items():
            assert setting in NOT_IN_ENV
            assert _ENV[key] == {"source": "minted"}, key


_CHANNELS = [spec for spec in CHANNEL_SPECS if spec.flag is not None]


class TestTheProfileIsData:
    """W4-1: the profile comes as data comms takes (form D of protocol
    4.5): `accepts.profile`, checked by `comms validate --profile <dir>`
    and given under TEMPLATES_DIR, the setting comms reads -- never a key
    of `env`, which the machine refuses (S.accepts-env). Mutations: the
    kind removed, TEMPLATES_DIR back in `env` or among NOT_IN_ENV."""

    def test_the_profile_is_accepted_under_the_setting_comms_reads(self) -> None:
        assert set(_ACCEPTS) == {"profile"}
        assert _ACCEPTS["profile"]["env"] == "TEMPLATES_DIR"
        assert "templates_dir" in Settings.model_fields
        assert _unset_default(_DEFAULTS["TEMPLATES_DIR"])

    def test_the_path_is_not_a_key_of_env(self) -> None:
        assert _ACCEPTED_ENV  # the pair: there is a variable to keep out
        assert _ACCEPTED_ENV.isdisjoint(_ENV)
        assert _ACCEPTED_ENV.isdisjoint(NOT_IN_ENV)

    def test_the_validator_is_the_comms_command(self) -> None:
        """The command the machine runs, <dir> its copy's path, is the
        `comms` command of the image (bin/comms -> app/cli.py main), and
        its parser takes it as written."""
        words = _ACCEPTS["profile"]["validate"].split()
        assert words[:2] == ["comms", "validate"]
        assert "<dir>" in words
        parsed = cli._parser().parse_args(
            [w.replace("<dir>", "/primer/data/profile") for w in words[1:]],
        )
        assert (parsed.command, parsed.profile) == ("validate", "/primer/data/profile")

    def test_the_languages_come_from_the_passport(self) -> None:
        """W4-1: comms has no languages of its own (protocol 5.2.2)."""
        assert _ENV["LANGUAGES"] == {"source": "passport", "key": "languages"}
        assert _ENV["DEFAULT_LANGUAGE"] == {
            "source": "passport", "key": "default_language",
        }


class TestChannels:
    def test_the_options_are_the_switchable_channels(self) -> None:
        assert _CHANNELS  # the pair
        assert sorted(_PRIMER["options"]) == sorted(spec.name for spec in _CHANNELS)

    @pytest.mark.parametrize("spec", _CHANNELS, ids=lambda spec: spec.name)
    def test_the_flag_comes_from_the_passport(self, spec: Any) -> None:
        assert _ENV[spec.flag] == {"source": "passport", "key": f"options.{spec.name}"}

    @pytest.mark.parametrize("spec", _CHANNELS, ids=lambda spec: spec.name)
    def test_every_key_of_the_channel_is_under_its_option(self, spec: Any) -> None:
        keys = [*spec.keys, *spec.optional_validators]
        assert keys  # the pair: the channel has keys
        for key in keys:
            if key in _ENV:
                assert _ENV[key].get("when") == spec.name, key
        assert [key for key in spec.keys if key not in _ENV] == []

    def test_the_commands_variables_are_telegrams(self) -> None:
        for key in cli.READS:
            assert _ENV[key].get("when") == "telegram", key

    def test_every_key_under_an_option_is_its_channels(self) -> None:
        """The other way: a key under `when: <channel>` is a key of that
        channel, or a variable of the command that derives for it."""
        for key, source in _ENV.items():
            when = source.get("when")
            if when is None:
                continue
            (spec,) = [spec for spec in _CHANNELS if spec.name == when]
            assert key in (*spec.keys, *spec.optional_validators, *cli.READS), key


class TestDerived:
    def test_every_derived_value_has_its_command(self) -> None:
        derived = {k: v for k, v in _ENV.items() if v["source"] == "derived"}
        assert derived == {
            "TELEGRAM_BOT_URL": {
                "source": "derived", "run": "comms derive bot-url", "when": "telegram",
            },
        }
        for source in derived.values():
            program, verb, name = source["run"].split()
            assert (program, verb) == ("comms", "derive")
            assert name in cli._VALUES

    def test_the_bot_id_waits_for_the_machine(self) -> None:
        """The machine refuses a derived value shorter than 16 characters;
        a bot id is 9-10 digits. The command exists, the key does not."""
        assert "bot-id" in cli._VALUES
        assert "TELEGRAM_BOT_ID" not in _ENV
        assert "TELEGRAM_BOT_ID" not in _PROVIDES


# ---------------------------------------------------------------------------
# the contract: deploy/INTEGRATION.md section 15
# ---------------------------------------------------------------------------

_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


def _section(number: str) -> str:
    start = _DOC.index(f"\n## {number}. ")
    end = _DOC.find("\n## ", start + 1)
    return _DOC[start:end]


def _block(section: str, lead: str) -> str:
    """The paragraph of `section` that opens with `lead`, through the
    table after it: up to the next paragraph in bold."""
    start = section.index(lead)
    end = section.find("\n**", start + len(lead))
    return section[start:] if end < 0 else section[start:end]


def _seam_rows(text: str) -> list[str]:
    """The first column of the seams table: | `NAME` | what it is |."""
    return re.findall(r"^\| `([A-Z][A-Z0-9_]*)` \|", text, flags=re.M)


_GIVES = "**The seams: what comms gives the product.**"
_TAKES = "**The seam comms takes from the product.**"


class TestTheContract:
    def test_the_seams_described_are_the_seams_given(self) -> None:
        """Was read off the whole section; since W4-2 the section has a
        second table, of the seam comms takes, so each table is read
        under its own paragraph."""
        rows = _seam_rows(_block(_section("15"), _GIVES))
        assert rows  # the pair: the table is read
        assert len(rows) == len(set(rows))
        assert sorted(rows) == sorted(_PROVIDES)

    def test_the_seam_described_is_the_seam_taken(self) -> None:
        rows = _seam_rows(_block(_section("15"), _TAKES))
        assert rows == _REQUIRES == ["PRODUCT_STORAGE_ADDRESS"]

    def test_the_seam_taken_is_a_seam_from_the_product(self) -> None:
        """W4-2, both ways: every name of `requires` is a key of `env` from
        the product, under no option -- the machine checks `requires`
        whatever the options (its half_seams) -- and every such key of
        `env` is in `requires`."""
        assert _ENV["PRODUCT_STORAGE_ADDRESS"] == {"source": "seam", "from": "product"}
        seams = sorted(key for key, value in _ENV.items() if value["source"] == "seam")
        assert seams == sorted(_REQUIRES)

    def test_every_seam_is_a_key_of_env(self) -> None:
        assert _PROVIDES
        assert [name for name in _PROVIDES if name not in _ENV] == []

    def test_the_bot_token_is_a_seam_for_the_transition(self) -> None:
        """W2-3: the token is given for the product's Mini App sign-in until
        Telegram's public key; the row says so and points at the marker,
        which stands at the name in primer.yaml (one full copy)."""
        (row,) = [
            line for line in _section("15").splitlines()
            if line.startswith("| `TELEGRAM_BOT_TOKEN` |")
        ]
        assert "public key" in row
        assert "the seam is removed" in row
        assert "KNOWN CEILING at this name in `primer.yaml`" in row
        raw = (_REPO / "primer.yaml").read_text(encoding="utf-8")
        marker = raw.index("# KNOWN CEILING (acknowledged by design -- the bot's")
        name = raw.index("  - TELEGRAM_BOT_TOKEN\n")
        assert raw.index("provides:") < marker < name
        assert raw.count("KNOWN CEILING") == 1

    def test_the_reader_sees_why_the_bot_id_waits(self) -> None:
        section = _section("15")
        assert "`TELEGRAM_BOT_ID` waits for the machine" in section
        assert "shorter than 16 characters" in section

    def test_the_rows_reader_sees_every_form(self) -> None:
        text = "| `A_B` | x |\n|---|---|\n| `C` | y |\n| not `D` | z |\n"
        assert _seam_rows(text) == ["A_B", "C"]


# ---------------------------------------------------------------------------
# the hand-over: deploy/docker-compose.yml passes what `env` gives (W1-2)
# ---------------------------------------------------------------------------

_COMPOSE: dict[str, Any] = yaml.safe_load(
    (_REPO / "deploy" / "docker-compose.yml").read_text(encoding="utf-8"),
)
_PROCESSES = {
    name: service for name, service in _COMPOSE["services"].items()
    if "build" in service
}
# The services that read a COMPOSED value: Redis is the consumer's, and the
# API's for the `redis` check of /health and /ready (W3-1, app/main.py);
# never the worker's.
_READS_COMPOSED = {
    "DATABASE_URL": {"comms-app", "comms-worker", "comms-consumer"},
    "REDIS_URL": {"comms-app", "comms-consumer"},
}


class TestTheHandOver:
    def test_every_setting_of_env_reaches_every_process(self) -> None:
        """A key of `env` that is a setting, passed as itself, `:?`, to
        each process built from the image. The pair: there are three
        processes and settings among the keys."""
        assert sorted(_PROCESSES) == ["comms-app", "comms-consumer", "comms-worker"]
        settings = [key for key in _ENV if key in _DEFAULTS]
        assert "COMMS_SERVICE_TOKEN" in settings and "TELEGRAM_ENABLED" in settings
        for name, service in _PROCESSES.items():
            environment = service["environment"]
            missing = [k for k in settings if environment.get(k) != f"${{{k}:?}}"]
            assert missing == [], name

    def test_nothing_else_of_env_is_passed(self) -> None:
        """The other way: a key of `env` that is no setting (a seam comms
        gives, a variable of `comms derive`) reaches no process."""
        others = [key for key in _ENV if key not in _DEFAULTS]
        assert "COMMS_API_URL" in others  # the pair
        for name, service in _PROCESSES.items():
            assert [k for k in others if k in service["environment"]] == [], name

    def test_what_compose_builds_is_built_from_its_key(self) -> None:
        assert set(_READS_COMPOSED) == set(COMPOSED)
        for setting, key in COMPOSED.items():
            for name, service in _PROCESSES.items():
                value = service["environment"].get(setting)
                if name not in _READS_COMPOSED[setting]:
                    assert value is None, (name, setting)
                    continue
                assert isinstance(value, str), (name, setting)
                assert re.findall(r"\$\{([A-Z0-9_]+)", value) == [key], (name, value)
