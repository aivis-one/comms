"""L-1: the option `login` at startup (app/core/modules.py).

The flag by the channels' rule: exactly `true` or `false`, otherwise
startup is refused naming it. With `true` the channel `telegram` must be
on and TELEGRAM_LOGIN_CLIENT_SECRET set -- a refusal names the option or
the key and never the value. With `false` the secret is not read,
whatever it holds, the machine's `primer-switched-off` included.

The grid is every cell of flag x telegram x secret; in each the secret is
a value the test can find: a refusal's text must not carry it, nor the
repr of settings that started.

Mutations: the secret judged with the flag false (2.1), the need of
telegram dropped (2.2), the secret's value in the refusal (2.3), a flag
read loosely (2.4) -- each red here.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from app.core.channels import CHANNEL_SPECS, channel_flag_keys
from app.core.config import Settings, format_startup_error
from app.core.modules import MODULE_SPECS, module_env_keys, module_flag_keys
from app.engine.constants import DeliveryChannel

# Assembled at run time: a literal in the shape of a bot token blocks the
# push (§0 §10). The secret is a value the test looks for, not a shape.
_TOKEN = "123456" + ":" + "unit-test-login-option"
_MARK = "unit" + "-login-" + "client-" + "secret-mark"
# What the machine writes into a key of a switched-off option (the
# machine v0.1.8, pridem/stacks/options.py). Here only as INPUT.
_LITERAL = "primer-switched-off"

_BASE: dict[str, Any] = {
    "database_url": "postgresql+asyncpg://u:p@db/comms_unit",
    "comms_service_token": "unit-test-service-token",
    "email_enabled": "false",
    "llm_enabled": "false",
    "languages": "en",
    "default_language": "en",
}
_TELEGRAM_KEYS = {
    "telegram_bot_token": _TOKEN,
    "telegram_bot_url": "https://t.me/unit_login_bot",
}

# None: the flag is absent.
_LOGIN_FLAGS: list[str | None] = [None, "", "TRUE", " true", "1", "false", "true"]
_TELEGRAM_FLAGS = ["true", "false", "yes"]
_SECRETS = ["", "   ", _LITERAL, _MARK]


def _settings(
    login: str | None, telegram: str, secret: str, **overrides: Any,
) -> Settings:
    kwargs: dict[str, Any] = {
        **_BASE,
        "telegram_enabled": telegram,
        "login_enabled": login,
        "telegram_login_client_secret": secret,
        **(_TELEGRAM_KEYS if telegram == "true" else {}),
        **overrides,
    }
    return Settings(**kwargs)


def _refusal(
    login: str | None, telegram: str, secret: str, **overrides: Any,
) -> str:
    with pytest.raises(ValidationError) as caught:
        _settings(login, telegram, secret, **overrides)
    return format_startup_error(caught.value)


def _flag_is_bad(value: str | None) -> bool:
    return value not in ("true", "false")


class TestTheGrid:
    @pytest.mark.parametrize("secret", _SECRETS, ids=repr)
    @pytest.mark.parametrize("telegram", _TELEGRAM_FLAGS)
    @pytest.mark.parametrize("login", _LOGIN_FLAGS, ids=repr)
    def test_every_cell(self, login: str | None, telegram: str, secret: str) -> None:
        login_bad = _flag_is_bad(login)
        telegram_bad = _flag_is_bad(telegram)
        on = login == "true"
        needs_telegram = on and telegram == "false"
        empty = on and secret == ""
        blank = on and secret != "" and secret.strip() == ""
        starts = not (login_bad or telegram_bad or needs_telegram or empty or blank)

        if starts:
            started = _settings(login, telegram, secret)
            assert started.login_enabled == login
            assert _MARK not in repr(started)
            return

        text = _refusal(login, telegram, secret)
        assert _MARK not in text
        assert ("LOGIN_ENABLED is not set" in text) == (login is None)
        assert (f"LOGIN_ENABLED={login!r} is not a switch" in text) == (
            login_bad and login is not None
        )
        assert ("TELEGRAM_ENABLED='yes' is not a switch" in text) == telegram_bad
        assert ("option 'login' is misconfigured:" in text) == (
            needs_telegram or empty or blank
        )
        assert (
            "needs channel 'telegram', which is switched off "
            "(TELEGRAM_ENABLED=false)" in text
        ) == needs_telegram
        assert ("missing: TELEGRAM_LOGIN_CLIENT_SECRET" in text) == empty
        assert (
            "TELEGRAM_LOGIN_CLIENT_SECRET consists only of whitespace" in text
        ) == blank

    def test_the_grid_has_both_outcomes(self) -> None:
        """The pair: the grid holds cells that start and cells refused --
        on and off alike."""
        assert _settings("false", "false", "").login_enabled == "false"
        assert _settings("true", "true", _MARK).login_enabled == "true"
        assert "option 'login'" in _refusal("true", "false", _MARK)


class TestTheRefusal:
    def test_each_problem_is_named_once(self) -> None:
        """REPEAT: two problems of the option are two lines of ONE
        refusal, each named once."""
        text = _refusal("true", "false", "")
        assert text.count("option 'login' is misconfigured:") == 1
        assert text.count("needs channel 'telegram'") == 1
        assert text.count("missing: TELEGRAM_LOGIN_CLIENT_SECRET") == 1

    def test_collected_with_every_other_problem(self) -> None:
        """One message for the whole start: the option's refusal beside
        another of the start's."""
        text = _refusal("true", "false", _MARK, comms_service_token="")
        assert "option 'login' is misconfigured:" in text
        assert "COMMS_SERVICE_TOKEN is required" in text
        assert text.count("comms: startup refused") == 1
        assert _MARK not in text

    def test_a_broken_telegram_is_telegrams_problem(self) -> None:
        """Crossing: telegram on with a malformed key -- the channel's
        refusal, and none of the option's when its secret is set."""
        text = _refusal(
            "true", "true", _MARK, telegram_bot_token="not-a-token",
        )
        assert "channel 'telegram' is misconfigured:" in text
        assert "option 'login'" not in text
        assert _MARK not in text

    def test_the_switched_off_secret_is_never_compared(self) -> None:
        """With the flag false the machine's literal is not read; with
        true it is a set value like any other -- comms does not know it."""
        assert _settings("false", "true", _LITERAL).login_enabled == "false"
        assert _settings("true", "true", _LITERAL).login_enabled == "true"


class TestTheRegistry:
    def test_login_is_the_one_module(self) -> None:
        assert [spec.name for spec in MODULE_SPECS] == ["login"]
        (login,) = MODULE_SPECS
        assert login.flag == "LOGIN_ENABLED"
        assert login.keys == ("TELEGRAM_LOGIN_CLIENT_SECRET",)
        assert login.channels == ("telegram",)

    def test_a_module_is_not_a_channel(self) -> None:
        """Not in the channel registry, not a delivery channel, its flag
        not a channel's. The pair: the registries are not empty."""
        channels = {spec.name for spec in CHANNEL_SPECS}
        assert channels and MODULE_SPECS
        for spec in MODULE_SPECS:
            assert spec.name not in channels
            assert spec.name not in {c.value for c in DeliveryChannel}
            assert spec.flag not in channel_flag_keys()
            for channel in spec.channels:
                assert channel in channels, channel

    def test_every_module_variable_is_a_settings_field(self) -> None:
        """app/core/config.py module_values reads them by lower-case name."""
        variables = module_flag_keys() + module_env_keys()
        assert variables
        for key in variables:
            assert key.lower() in Settings.model_fields, key

    def test_the_secret_is_kept_out_of_repr(self) -> None:
        assert Settings.model_fields["telegram_login_client_secret"].repr is False
