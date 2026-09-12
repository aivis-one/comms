# =============================================================================
# COMMS Service -- Startup config validation tests (Phase 3b items 4 + 1)
# =============================================================================
#
# A deploy with a PARTIAL telegram key set, or without the service token
# outside development, must FAIL AT STARTUP -- not degrade on the
# delivery path (empty bot URL -> BUTTON_URL_INVALID storm of permanent
# FAILED deliveries; empty service token -> an open "internal" API).
#
# R-0: until then both rules were gated by the "real" position of the
# global channel switch, and its "stub" position was exempt from them.
# The switch is gone: the telegram rule now follows the channel's own
# key set (an EMPTY set is a legal deploy without telegram), and the
# token rule follows APP_ENV, the same
# axis as DATABASE_URL. The full matrix of key-set states lives in
# test_channel_key_sets.py; this file keeps the original cases.
#
# Settings are constructed directly with _env_file=None so the
# validation under test sees exactly the kwargs given, not whatever
# .env happens to lie around. APP_ENV and DATABASE_URL are pinned too:
# the environment the suite runs in must not decide these tests.
# =============================================================================

from typing import Any

import pytest

from app.core.config import Settings

# Everything a production deploy WITH telegram needs to boot; tests
# knock fields out one at a time.
_FULL_OK = {
    "app_env": "production",
    "database_url": "postgresql+asyncpg://u:p@db/comms_unit",
    "telegram_bot_token": "123456:test-token",
    "telegram_bot_url": "https://t.me/unit_test_bot",
    "comms_service_token": "unit-test-service-token",
}


def _settings(**overrides: str) -> Settings:
    kwargs: dict[str, Any] = {"_env_file": None, **_FULL_OK, **overrides}
    return Settings(**kwargs)


class TestStartupValidation:
    def test_full_config_boots(self) -> None:
        """R-0: asserted the switch read "real"; the switch is gone, so
        the successor asserts what the full config actually carries."""
        settings = _settings()
        assert settings.telegram_bot_token == "123456:test-token"
        assert settings.telegram_bot_url == "https://t.me/unit_test_bot"

    def test_without_bot_token_fails(self) -> None:
        with pytest.raises(ValueError, match="TELEGRAM_BOT_TOKEN"):
            _settings(telegram_bot_token="")

    def test_without_bot_url_fails(self) -> None:
        """The 3a flag 7.3 case: an empty base URL would turn every
        deep-link button into BUTTON_URL_INVALID -> permanent FAILED."""
        with pytest.raises(ValueError, match="TELEGRAM_BOT_URL"):
            _settings(telegram_bot_url="")

    def test_without_service_token_fails(self) -> None:
        with pytest.raises(ValueError, match="COMMS_SERVICE_TOKEN"):
            _settings(comms_service_token="")


class TestEmptyTelegramIsLegal:
    def test_boots_with_no_telegram_and_no_token_in_development(
        self,
    ) -> None:
        """R-0: was "stub mode boots with nothing configured" and
        asserted the switch read "stub". What stays true: an empty
        telegram set boots anywhere (a deploy without telegram), and an
        empty service token boots in development. What the switch hid:
        outside development the token is required -- pinned above."""
        settings = _settings(
            app_env="development",
            telegram_bot_token="",
            telegram_bot_url="",
            comms_service_token="",
        )
        assert settings.telegram_bot_token == ""
        assert settings.telegram_bot_url == ""
        assert settings.comms_service_token == ""
