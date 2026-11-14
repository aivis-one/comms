# =============================================================================
# COMMS Service -- the numeric bounds of the settings (W4-1)
# =============================================================================
#
# THE ONE TABLE OF NUMERIC BOUNDS lives here, not in app/core/config.py,
# and with it the defaults of the four settings a product profile is
# checked against: this module builds no settings. The profile's loader
# (app/profile/loader.py) is run by `comms validate --profile` in a
# throwaway container of the machine's that has the profile's path and
# nothing else of the instance (protocol 4.5) -- app/core/config.py
# refuses such a process at its import, so nothing the loader needs may
# live only there. app/core/config.py imports both from here: the bound
# a setting is held to and the bound a profile's field is held to stay
# one number.
# =============================================================================

from dataclasses import dataclass

# The settings' defaults a profile is checked against (app/profile/
# loader.py ProfileDeploy): the defaults of the Settings fields of the
# same names are these. None of the four is a key of primer.yaml `env`,
# so on a box each is its default -- the number `comms validate` uses.
NOTIFICATION_MAX_DELIVERY_ATTEMPTS = 3
NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS = 30
NOTIFICATION_RETRY_BACKOFF_MAX_SECONDS = 600
NOTIFICATION_RETENTION_DAYS = 90

@dataclass(frozen=True)
class Bound:
    """The closed range a numeric setting may take, and why."""

    lo: int
    hi: int
    why: str


# The four lanes' batch sizes share one reason (P4-1).
_LANE_BATCH_WHY = (
    "jobs one pass of the lane selects; 0 does NOT switch the lane off "
    "-- 'off' is leaving the lane out of COMMS_WORKER_LANES; above 1000 "
    "one pass holds that many jobs in memory"
)

# The four lanes' deliver timeouts share one reason (P4-3).
_LANE_DELIVER_TIMEOUT_WHY = (
    "seconds one channel call of the lane may take before it is "
    "abandoned as a timeout; 0 abandons every call; above 600 an attempt "
    "holds its job's row and a pooled connection for ten minutes per "
    "call; and not at or below the client timeout of a live channel "
    "(checked by app/core/config.py)"
)

# THE ONE TABLE OF NUMERIC BOUNDS (D1 / R3). Every int field of
# Settings has a row -- a test walks the fields against this table, so
# a new knob without a bound fails the suite. A value outside its row
# refuses startup naming the variable: a service that came up on a
# wrong number is worse than one that refused and said which (spec
# §17.7). No clamping, no warning-instead, no default-instead.
NUMERIC_BOUNDS: dict[str, Bound] = {
    "consumer_batch_size": Bound(
        1, 1000, "XREADGROUP COUNT: 0 reads nothing; above 1000 one read "
        "holds that many envelopes in memory at once",
    ),
    "consumer_block_ms": Bound(
        1, 60_000, "XREADGROUP BLOCK: 0 blocks FOREVER, so the consumer "
        "never wakes to see a shutdown; above a minute it answers one late",
    ),
    "changes_stream_maxlen": Bound(
        1, 10_000_000, "XADD MAXLEN of the push stream: 0 trims every "
        "push as it is written -- a stream no listener can read",
    ),
    "notification_poll_interval_seconds": Bound(
        1, 3600, "0 is a busy loop on the database; above an hour a "
        "scheduled_at is honored an hour late",
    ),
    "notification_max_backoff_seconds": Bound(
        1, 3600, "the idle worker's longest sleep; not below the poll "
        "interval (checked by app/core/config.py)",
    ),
    "notification_max_delivery_attempts": Bound(
        1, 100, "0 attempts sends nothing and never fails; the profile's "
        "retry_max_attempts refuses below 1 for the same reason",
    ),
    "notification_max_pipeline_attempts": Bound(
        1, 100, "0 would close a job without one attempt; T12",
    ),
    "notification_lane_interactive_batch_size": Bound(
        1, 1000, _LANE_BATCH_WHY,
    ),
    "notification_lane_critical_batch_size": Bound(1, 1000, _LANE_BATCH_WHY),
    "notification_lane_normal_batch_size": Bound(1, 1000, _LANE_BATCH_WHY),
    "notification_lane_bulk_batch_size": Bound(1, 1000, _LANE_BATCH_WHY),
    "notification_lane_interactive_deliver_timeout_seconds": Bound(
        1, 600, _LANE_DELIVER_TIMEOUT_WHY,
    ),
    "notification_lane_critical_deliver_timeout_seconds": Bound(
        1, 600, _LANE_DELIVER_TIMEOUT_WHY,
    ),
    "notification_lane_normal_deliver_timeout_seconds": Bound(
        1, 600, _LANE_DELIVER_TIMEOUT_WHY,
    ),
    "notification_lane_bulk_deliver_timeout_seconds": Bound(
        1, 600, _LANE_DELIVER_TIMEOUT_WHY,
    ),
    "notification_retry_backoff_base_seconds": Bound(
        0, 86_400, "0 = no idle wait between attempts (the profile's "
        "retry_backoff_seconds allows 0 too); above a day a retry "
        "outlives any letter",
    ),
    "notification_retry_backoff_max_seconds": Bound(
        0, 86_400, "the backoff cap; not below the base (checked by "
        "app/core/config.py)",
    ),
    "notification_max_retry_after_seconds": Bound(
        1, 86_400, "0 turns every 429 deferral into an immediate re-poll",
    ),
    "notification_max_rate_limit_deferrals": Bound(
        0, 1000, "0 = every 429 is a regular transient failure",
    ),
    "notification_retention_days": Bound(
        0, 3650, "0 DISABLES retention -- never 'delete everything'; a "
        "negative number has no meaning",
    ),
    "notification_retention_interval_seconds": Bound(
        1, 86_400, "0 does NOT mean 'every tick'; 'off' is "
        "NOTIFICATION_RETENTION_DAYS=0",
    ),
    "thread_auto_close_days": Bound(
        0, 3650, "0 DISABLES auto-close -- never a mass close; a negative "
        "number has no meaning",
    ),
    "thread_auto_close_interval_seconds": Bound(
        1, 86_400, "0 does NOT mean 'every tick'; 'off' is "
        "THREAD_AUTO_CLOSE_DAYS=0",
    ),
    "llm_deliver_timeout_seconds": Bound(
        1, 600, "seconds one call of the llm channel to ONE key set may "
        "take; a job's call over its whole list of sets is bounded by "
        "this times the number of sets; above 600 an attempt holds its "
        "job's row for ten minutes per set; and not at or below the llm "
        "client's own timeout when the channel is live (checked by "
        "app/core/config.py)",
    ),
}
