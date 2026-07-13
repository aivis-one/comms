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

from zoneinfo import ZoneInfo

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
    # Fallback IANA timezone for recipients without an explicit one --
    # quiet-hours window math needs a timezone for everybody. Validated
    # at startup (a bad name must not surface on the delivery path).
    default_timezone: str = "UTC"

    # -- Product profile (per-deploy data: types + templates) --
    # Directory with the product profile: types.yaml + templates/.
    # On the VPS it arrives as a bind-mount of the product repo's
    # comms-profile/ (mount mechanics are Phase 5); tests point it at
    # the fixture directory in this repo. Empty is tolerated ONLY in
    # development (the Phase 1 stub-profile behavior for tests) --
    # see app/engine/profile.py: install_profile_from_settings.
    templates_dir: str = ""

    # -- Notification engine --
    notification_poll_interval_seconds: int = 5
    notification_max_backoff_seconds: int = 60
    notification_max_delivery_attempts: int = 3
    # Max notifications picked per worker batch; the tail is picked up
    # on the next tick (review 1.1: unbounded backlog fetch).
    notification_batch_size: int = 50
    # Per-delivery retry backoff: base * 2**(attempts-1), capped.
    # Defaults give ~30s/60s between the three attempts -- comparable
    # to the cbshome donor's 1-minute worker tick, where retries were
    # meaningful (review 1.1: retries burned within seconds).
    notification_retry_backoff_base_seconds: int = 30
    # Double duty (Phase 2.3): also caps the honored 429 retry_after
    # -- "no failure-driven gate exceeds this" is one policy knob.
    notification_retry_backoff_max_seconds: int = 600

    # Phase 2.2: how many channel rate-limit (429) deferrals a single
    # delivery gets before a 429 degrades to a regular transient
    # failure. Bounds the deferral loop: past the budget the attempts
    # budget takes over, which is finite. With typical Telegram
    # retry_after values (3-30s) the default buys minutes of honest
    # waiting -- far beyond any realistic burst at current scale.
    notification_max_rate_limit_deferrals: int = 10

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

        try:
            ZoneInfo(self.default_timezone)
        except (KeyError, ValueError) as exc:
            raise ValueError(
                f"Invalid DEFAULT_TIMEZONE: {self.default_timezone!r}. "
                f"Must be an IANA timezone name (e.g. 'UTC', "
                f"'Europe/Berlin')."
            ) from exc

        if self.notification_max_rate_limit_deferrals < 0:
            raise ValueError(
                "NOTIFICATION_MAX_RATE_LIMIT_DEFERRALS must be >= 0 "
                "(0 disables 429 deferrals: every 429 is a regular "
                "transient failure)."
            )

        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
