# =============================================================================
# COMMS Service -- Real-mode config validation tests (Phase 3b item 4 + 1)
# =============================================================================
#
# CHANNELS_MODE=real must FAIL AT STARTUP when telegram credentials or
# the service token are missing -- not degrade on the delivery path
# (empty bot URL -> BUTTON_URL_INVALID storm of permanent FAILED
# deliveries; empty service token -> an open "internal" API).
# Stub mode stays exempt: tests/CI run without credentials by design.
#
# Settings are constructed directly with _env_file=None so the
# validation under test sees exactly the kwargs given, not whatever
# .env happens to lie around.
# =============================================================================

from typing import Any

import pytest

from app.core.config import Settings

# Everything a REAL-mode config needs to boot; tests knock fields out
# one at a time.
_REAL_OK = {
    "channels_mode": "real",
    "telegram_bot_token": "123456:test-token",
    "telegram_bot_url": "https://t.me/unit_test_bot",
    "comms_service_token": "unit-test-service-token",
}


def _settings(**overrides: str) -> Settings:
    kwargs: dict[str, Any] = {"_env_file": None, **_REAL_OK, **overrides}
    return Settings(**kwargs)


class TestRealModeStartupValidation:
    def test_real_with_full_config_boots(self) -> None:
        settings = _settings()
        assert settings.channels_mode == "real"

    def test_real_without_bot_token_fails(self) -> None:
        with pytest.raises(ValueError, match="TELEGRAM_BOT_TOKEN"):
            _settings(telegram_bot_token="")

    def test_real_without_bot_url_fails(self) -> None:
        """The 3a flag 7.3 case: an empty base URL would turn every
        deep-link button into BUTTON_URL_INVALID -> permanent FAILED."""
        with pytest.raises(ValueError, match="TELEGRAM_BOT_URL"):
            _settings(telegram_bot_url="")

    def test_real_without_service_token_fails(self) -> None:
        with pytest.raises(ValueError, match="COMMS_SERVICE_TOKEN"):
            _settings(comms_service_token="")


class TestStubModeUnaffected:
    def test_stub_boots_with_nothing_configured(self) -> None:
        kwargs: dict[str, Any] = {
            "_env_file": None,
            "channels_mode": "stub",
            "telegram_bot_token": "",
            "telegram_bot_url": "",
            "comms_service_token": "",
        }
        settings = Settings(**kwargs)
        assert settings.channels_mode == "stub"
