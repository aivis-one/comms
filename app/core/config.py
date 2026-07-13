# =============================================================================
# COMMS Service -- Application Configuration
# =============================================================================
#
# All settings loaded from environment variables (or .env file).
# Pydantic-settings validates types and applies defaults automatically.
#
# DEPLOY MODEL (see Comms-Service-Architecture.md):
#   One comms deploy per product. DATABASE_URL points at a dedicated
#   database + role inside the PRODUCT's Postgres. Channel credentials
#   (bot token) are shared with the product and injected via env.
#
# CHANNELS_MODE:
#   "stub" (default) -- every channel resolves to StubFormatter; nothing
#                       leaves the process. Used by tests and CI.
#   "real"           -- channels with configured credentials use real
#                       formatters (Phase 1: telegram only; email/push/
#                       in_app remain stubs until later phases).
#
# DEFAULT_LOCALE:
#   Per-deploy default used as the template-rendering fallback language
#   (recipient locale -> default_locale -> stored title/body).
# =============================================================================

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Single source of truth for API version.
# Import as: from app.core.config import APP_VERSION, settings
APP_VERSION = "0.1.0"

# Valid structlog log levels.
_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

# Valid channel modes.
_VALID_CHANNELS_MODES = {"stub", "real"}


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # -- Application --
    app_env: str = "development"
    log_level: str = "INFO"

    # -- Database (dedicated comms database inside the product Postgres) --
    database_url: str = ""

    # -- Channels --
    # "stub" keeps every channel local (tests/CI); "real" enables
    # configured channels (Phase 1: telegram).
    channels_mode: str = "stub"

    # -- Telegram (shared bot with the product, see arch doc §7) --
    telegram_bot_token: str = ""
    # Base URL for deep-link buttons, e.g. "https://t.me/velo_testbot".
    # Consumed by TelegramFormatter.format_deep_link (ported from VELO).
    telegram_bot_url: str = ""

    # -- Localization --
    # Per-deploy default locale; template fallback language.
    default_locale: str = "en"

    # -- Notification engine --
    notification_poll_interval_seconds: int = 5
    notification_max_backoff_seconds: int = 60
    notification_max_delivery_attempts: int = 3

    # -- Computed properties --

    @property
    def is_dev(self) -> bool:
        """True when running in development mode."""
        return self.app_env == "development"

    # -- Validation --

    @model_validator(mode="after")
    def _apply_env_defaults_and_validate(self) -> "Settings":
        """Apply development defaults and validate values.

        Development: provides a working local DATABASE_URL so the
        service starts without a .env. Any other env requires an
        explicit DATABASE_URL.
        """
        if not self.database_url:
            if self.is_dev:
                self.database_url = (
                    "postgresql+asyncpg://comms:comms@localhost:5432/comms"
                )
            else:
                raise ValueError(
                    "DATABASE_URL is required outside development. "
                    "Set it in the .env file."
                )

        if self.log_level.upper() not in _VALID_LOG_LEVELS:
            raise ValueError(
                f"Invalid LOG_LEVEL: {self.log_level}. "
                f"Valid: {', '.join(sorted(_VALID_LOG_LEVELS))}"
            )

        if self.channels_mode not in _VALID_CHANNELS_MODES:
            raise ValueError(
                f"Invalid CHANNELS_MODE: {self.channels_mode}. "
                f"Valid: {', '.join(sorted(_VALID_CHANNELS_MODES))}"
            )

        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
