"""No secret of the instance in the repr of the settings (SEC-1).

pydantic prints every field of a model in its repr and str, so a field
whose value is a secret, or holds one, is declared repr=False
(app/core/config.py Settings) -- one line of debugging away from a log
otherwise.

Which fields those are is read from DATA, not from anyone's memory:
  - a key of primer.yaml `env` whose source is `asked` (the operator's
    credential) or `minted` (the machine's) and that is a setting;
  - a setting compose builds from such a key (COMPOSED of
    tests/test_primer_yaml.py, held there against compose and `minted`);
  - a setting comms reads that is not a key of `env` at all
    (NOT_IN_ENV of tests/test_primer_yaml.py): each row of it is
    classified below, secret or not, with the reason -- a new row nobody
    classified is red.
A new secret setting declared without repr=False is red without an edit
of this file; so is repr=False on a setting that holds no secret.
"""

from typing import Any

from app.core.config import Settings
from tests.test_primer_yaml import _ENV, COMPOSED, NOT_IN_ENV

# The sources of `env` whose value is a secret.
SECRET_SOURCES = frozenset({"asked", "minted"})

# Rows of NOT_IN_ENV that are not built by compose: secret or not, why.
SECRET_OUTSIDE_ENV: dict[str, str] = {
    "COMMS_SERVICE_TOKEN_NEXT": (
        "the next service token: a secret like the current one, kept out "
        "of `env` only until the machine changes keys"
    ),
}
PLAIN_OUTSIDE_ENV: dict[str, str] = {
    "TEMPLATES_OVERLAY_DIR": "a path inside the container",
    "COMMS_WORKER_LANES": "names of lanes",
}

# The secret settings known at SEC-1: the rule must find at least these,
# or it finds nothing and every check below is green for nothing.
KNOWN_SECRETS = frozenset({
    "database_url",
    "redis_url",
    "comms_service_token",
    "comms_service_token_next",
    "telegram_login_client_secret",
    "telegram_bot_token",
    "email_mailgun_api_key",
    "llm_key_sets",
})

# A setting with no secret, given a mark of its own: the repr prints it.
_PLAIN_FIELD = "email_mailgun_domain"


def _secret_keys() -> set[str]:
    return {
        key for key, source in _ENV.items()
        if source["source"] in SECRET_SOURCES
    }


def secret_fields() -> set[str]:
    """Every setting whose value is a secret or holds one, by the data."""
    keys = _secret_keys()
    fields = {key.lower() for key in keys if key.lower() in Settings.model_fields}
    fields |= {
        setting.lower() for setting, key in COMPOSED.items() if key in keys
    }
    fields |= {setting.lower() for setting in SECRET_OUTSIDE_ENV}
    return fields


def _hidden_fields() -> set[str]:
    return {
        name for name, field in Settings.model_fields.items()
        if field.repr is False
    }


class TestTheRuleIsReadFromData:
    def test_every_secret_key_is_a_setting_the_rule_sees(self) -> None:
        """A secret key that is neither a setting nor built into one by
        compose reaches comms some way the rule cannot see."""
        for key in _secret_keys():
            assert (
                key.lower() in Settings.model_fields
                or key in COMPOSED.values()
            ), key

    def test_every_value_outside_env_is_classified_once(self) -> None:
        groups = (set(COMPOSED), set(SECRET_OUTSIDE_ENV), set(PLAIN_OUTSIDE_ENV))
        for key in NOT_IN_ENV:
            assert sum(key in group for group in groups) == 1, key
        assert set().union(*groups) == set(NOT_IN_ENV)

    def test_the_hidden_fields_are_exactly_the_secret_ones(self) -> None:
        assert _hidden_fields() == secret_fields()

    def test_the_rule_finds_the_known_secrets(self) -> None:
        assert secret_fields() >= KNOWN_SECRETS


def _mark(name: str) -> str:
    # Glued at run time: no literal in the shape of a key.
    return "-".join(("sec1", name, "mark"))


def _value(name: str) -> str:
    """A value holding the field's mark; a URL holds it as its password."""
    if name == "database_url":
        return "".join(
            ("postgresql+asyncpg://comms:", _mark(name), "@localhost:5432/comms")
        )
    if name == "redis_url":
        return "".join(("redis://:", _mark(name), "@localhost:6379/0"))
    return _mark(name)


def _marked() -> tuple[Settings, dict[str, str]]:
    """Settings with every secret field marked; the rest given explicitly,
    so the environment does not decide whether they are valid."""
    values = {name: _value(name) for name in secret_fields()}
    values[_PLAIN_FIELD] = _mark(_PLAIN_FIELD)
    given: dict[str, Any] = {
        "telegram_enabled": "false",
        "email_enabled": "false",
        "llm_enabled": "false",
        "login_enabled": "false",
        "languages": "en,ru",
        "default_language": "en",
        **values,
    }
    return Settings(**given), values


class TestTheRepr:
    def test_no_secret_in_repr_or_str(self) -> None:
        settings, _ = _marked()
        for name in secret_fields():
            assert _mark(name) not in repr(settings), name
            assert _mark(name) not in str(settings), name

    def test_a_plain_field_is_printed(self) -> None:
        """The pair of the test above: the repr prints fields at all."""
        settings, _ = _marked()
        assert _mark(_PLAIN_FIELD) in repr(settings)
        assert _mark(_PLAIN_FIELD) in str(settings)

    def test_the_values_are_still_read(self) -> None:
        """repr=False hides a value from the repr only: every reader of
        the field, and model_dump(), gets it whole."""
        settings, values = _marked()
        dumped = settings.model_dump()
        for name in secret_fields():
            assert getattr(settings, name) == values[name], name
            assert dumped[name] == values[name], name
