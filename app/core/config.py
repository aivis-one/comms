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

    # -- Event transport, push side (Phase 3c) --
    # The consumer process (`python -m app.consumer`) reads the
    # product's event stream via a Redis consumer group. Nobody else
    # needs Redis (API/worker are DB-only), so an empty redis_url is
    # validated at CONSUMER startup (app/consumer.py), not here --
    # deliberately not coupled to channels_mode.
    # Names below are FROZEN CONTRACT surface (Phase 3c item 7): the
    # product's outbox relay XADDs into comms_events_stream; the DLQ
    # is derived as f"{comms_events_stream}:dlq" (see dlq_stream).
    redis_url: str = ""
    comms_events_stream: str = "comms:events"
    comms_consumer_group: str = "comms"
    # STABLE consumer name (not hostname/pid): after a restart the
    # same name re-reads its own pending entries (XREADGROUP "0"), so
    # unacked messages replay without XAUTOCLAIM machinery. One
    # consumer per deploy by design.
    comms_consumer_name: str = "comms-1"
    # XREADGROUP batch size / block timeout, and the cap on the DLQ
    # stream length (approximate MAXLEN trimming).
    consumer_batch_size: int = 32
    consumer_block_ms: int = 5000
    dlq_maxlen: int = 10000

    @property
    def dlq_stream(self) -> str:
        """Dead-letter stream name, derived from the main stream."""
        return f"{self.comms_events_stream}:dlq"

    # -- Service-to-service authorization (Phase 3b item 1) --
    # The comms API is INTERNAL (arch decision 14): only the product
    # backend calls it, over the shared Docker network, presenting this
    # shared secret as "Authorization: Bearer <token>". Verified by the
    # FastAPI dependency in app/api/deps.py. NEVER logged (same
    # principle as the formatter's secret sanitizer).
    # Empty token: startup ERROR in real mode (an unauthenticated
    # "internal" API is effectively open), loud warning + auth disabled
    # in stub mode (local dev / tests).
    comms_service_token: str = ""

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
    # see app/profile/loader.py: install_profile_from_settings.
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
    notification_retry_backoff_max_seconds: int = 600

    # Phase 2.3: TRUST limit on the channel-named 429 wait -- a
    # SEPARATE knob from our backoff policy on purpose. backoff_max
    # is OUR retry policy; this is how far we trust the SERVER's
    # word. Deliberately generous (an hour): the cap is protection
    # from absurdity (ms-vs-s mixups, buggy servers), not a working
    # mode -- capping must stay exceptional, so that capped=true in
    # the deferral log remains an ALARM ("we overrode the server
    # that rate-limits us" -- the road to bot bans if routine), not
    # noise. Telegram legitimately asks for 1000-3000s on serious
    # flood waits; those must be honored, not capped.
    # NOTE (Phase 3a fix D): the proportional 429 jitter (up to +50%,
    # see _RATE_LIMIT_JITTER_MAX_FRACTION in app/engine/service.py)
    # rides ON TOP of the capped value and is one-sided (never earlier
    # than the server asked) -- the effective wait ceiling is
    # cap x 1.5, not cap.
    notification_max_retry_after_seconds: int = 3600

    # Phase 2.2: how many channel rate-limit (429) deferrals a single
    # delivery gets before a 429 degrades to a regular transient
    # failure. Bounds the deferral loop: past the budget the attempts
    # budget takes over, which is finite. With typical Telegram
    # retry_after values (3-30s) the default buys minutes of honest
    # waiting -- far beyond any realistic burst at current scale.
    notification_max_rate_limit_deferrals: int = 10

    # Phase 3a item 5 (+3a.1): retention of TERMINAL notifications
    # (SENT / PARTIAL_SENT / FAILED / SKIPPED / EXPIRED) -- rows older
    # than this are deleted
    # in batches by the worker's retention pass, deliveries follow by
    # FK cascade. Age is measured on created_at.
    # SEMANTICS (fix I): <= 0 means retention is DISABLED -- never
    # "delete everything now". A stray RETENTION_DAYS=0 in env must
    # not become an irreversible wipe of the whole history; disabling
    # is loud (worker startup log).
    notification_retention_days: int = 90

    # Phase 3a fix H: the retention pass runs on its OWN slow cadence,
    # not on the 5s worker tick -- the batched DELETE scans without an
    # index (acknowledged, BL-3) and days-granular retention gains
    # nothing from second-granular scheduling. Per-process monotonic
    # gate in app/engine/worker.py. Strictly > 0 (fix I): 0 is a
    # config error at startup, NOT "every tick" -- someone writing 0
    # to mean "off" must not get the hottest possible cadence; "off"
    # is NOTIFICATION_RETENTION_DAYS <= 0.
    notification_retention_interval_seconds: int = 3600

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

        # Phase 3b item 4 (3a flag 7.3): real mode with missing telegram
        # credentials must die AT STARTUP, not on the delivery path --
        # an empty bot URL turns every deep-link button into a
        # BUTTON_URL_INVALID storm of permanent FAILED deliveries, and
        # an empty token fails every send. Stub mode stays exempt
        # (tests/CI run without credentials by design).
        if self.channels_mode == "real":
            if not self.telegram_bot_token:
                raise ValueError(
                    "TELEGRAM_BOT_TOKEN is required when "
                    "CHANNELS_MODE=real: without it every telegram "
                    "send fails. Set it in the .env file."
                )
            if not self.telegram_bot_url:
                raise ValueError(
                    "TELEGRAM_BOT_URL is required when "
                    "CHANNELS_MODE=real: deep-link buttons would be "
                    "built from an empty base and every buttoned "
                    "delivery would permanently fail with "
                    "BUTTON_URL_INVALID. Set it in the .env file."
                )
            # Phase 3b item 1: an "internal" API without its shared
            # secret is effectively open -- same fail-at-startup
            # philosophy. In stub mode an empty token merely disables
            # auth (loud warning at startup, see app/main.py).
            if not self.comms_service_token:
                raise ValueError(
                    "COMMS_SERVICE_TOKEN is required when "
                    "CHANNELS_MODE=real: the comms API is internal "
                    "(arch decision 14) and must not run open. Set "
                    "it in the .env file."
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

        if self.notification_max_retry_after_seconds <= 0:
            raise ValueError(
                "NOTIFICATION_MAX_RETRY_AFTER_SECONDS must be > 0 "
                "(it bounds how long a channel-named 429 wait is "
                "honored; 0 would turn every deferral into an "
                "immediate re-poll)."
            )

        if self.notification_retention_interval_seconds <= 0:
            raise ValueError(
                "NOTIFICATION_RETENTION_INTERVAL_SECONDS must be > 0. "
                "To disable retention set "
                "NOTIFICATION_RETENTION_DAYS to 0 or a negative "
                "value; interval 0 does NOT mean 'every tick'."
            )

        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
