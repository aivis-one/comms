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
# CHANNELS:
#   A channel is decided by its own key set and by nothing else -- see
#   app/core/channels.py for the rule. Empty set: the deploy has no
#   such channel. Full set: the channel is live. Partial set or a
#   malformed value: startup is refused with a message naming the
#   channel and the keys (load_settings below).
#
# DEFAULT_LOCALE:
#   Per-deploy default used as the template-rendering fallback language
#   (recipient locale -> default_locale -> stored title/body).
# =============================================================================

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.core.channels import (
    CHANNEL_SPECS,
    ChannelState,
    channel_env_keys,
    channel_optional_keys,
    evaluate_channels,
)
from app.core.lanes import Lane

# Single source of truth for API version.
# Import as: from app.core.config import APP_VERSION, settings
#
# IT IS THE NUMBER THE DEPLOY REPORTS. /health and the startup log both
# carry it, and the first question when an incident is opened in
# someone else's stack is which version of comms is running there. It
# stood at 0.1.0 through every release up to v1.3.0, which made that
# question unanswerable from the outside. Raise it with the tag, in the
# same commit; pyproject carries the same number and a test pins the
# two together, so neither can move alone.
#
# WHAT THE NUMBER PROMISES, because the products read it as an
# instruction. A MAJOR bump means the contract changed and the caller
# must change with it. A patch bump would have said "safe, do nothing",
# which is the loudest kind of lie this service can tell.
#
# 3.0.0 is the PROTOCOL turning over (phase 1): jobs and resources speak
# a language 2.0.0 does not -- an envelope with a required key and the
# channel chosen by the profile, outcomes as an enumeration, one error
# body with a class, one way to page, a versioned recipient snapshot
# that can be forgotten, idempotency keys on the calls that create, and
# no `priority`. A product built against 2.0.0 cannot talk to it
# unchanged, and under one number the question "which protocol runs
# there" would have no answer. The tag is `v3.0.0`, set by the owner on
# the merge commit. (2.0.0 was the schedule turning over: the
# preferences `schedule` key became a list of allowed periods.)
#
# MINOR bumps add and narrow nothing: 3.1.0 is D1 (bounds on every
# number, a pipeline defect becomes an outcome, closed membership
# events); 3.2.0 is phase 2 (the transition journal, a letter the
# channel took is never sent twice, reading a job by its key, what
# changed since a cursor, channel health, the address book); 3.3.0 is
# phase 3 (the push stream: a type that declares push_on owes its
# product the job's key, after the commit, at least once); 3.4.0 is
# phase 4 (isolation lanes: each with its own batch, slots and deliver
# timeout held above its channels' clients, intake waking its lane, the
# lanes a worker takes chosen by the deploy) with H7 (row locks in one
# order, a push outliving the expiry cleanup, one portion of deliveries
# per attempt, connection pools by process) and H8 (comms' own errors
# kept as class and place, every log line sanitized, the lanes' depth).
# The tag goes on the merge commit, after the merge -- never ahead of it.
APP_VERSION = "3.4.0"

# Valid structlog log levels.
_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

# Provider base addresses per region. Here and nowhere else: this file
# is the single exemption of the domain-literal fence. Keys are the
# closed set validated in app/core/channels.py (EMAIL_REGIONS) -- a test
# pins the two sides together.
_EMAIL_API_BASE_URLS = {
    "eu": "https://api.eu.mailgun.net",
    "us": "https://api.mailgun.net",
}


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
    "(checked below)"
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
    "dlq_maxlen": Bound(
        1, 10_000_000, "XADD MAXLEN: 0 trims every entry -- a dead-letter "
        "queue that keeps nothing",
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
        "interval (checked below)",
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
        0, 86_400, "the backoff cap; not below the base (checked below)",
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
}

# Pairs where one setting must not fall below another: (low, high).
_ORDERED_PAIRS: tuple[tuple[str, str], ...] = (
    ("notification_poll_interval_seconds", "notification_max_backoff_seconds"),
    (
        "notification_retry_backoff_base_seconds",
        "notification_retry_backoff_max_seconds",
    ),
)


def numeric_problems(values: Mapping[str, int]) -> list[str]:
    """Every bound and order violated by `values`, each naming its
    variable -- collected, so a deploy fixes all of them in one pass."""
    problems = [
        f"{name.upper()}={values[name]} is outside {bound.lo}..{bound.hi}: "
        f"{bound.why}."
        for name, bound in NUMERIC_BOUNDS.items()
        if not bound.lo <= values[name] <= bound.hi
    ]
    problems.extend(
        f"{high.upper()}={values[high]} is below "
        f"{low.upper()}={values[low]}."
        for low, high in _ORDERED_PAIRS
        if values[high] < values[low]
    )
    return problems


def deliver_timeout_problems(
    timeouts: Mapping[Lane, int], live: Iterable[str],
) -> list[str]:
    """Every lane whose deliver timeout is not above the client timeout
    of a live channel, each naming its variable and the channels (P4-3,
    spec §8.5).

    WHY THE RULE: the lane's timeout (app/engine/service.py
    _deliver_single) abandons a channel call from OUTSIDE, as a bare
    TimeoutError. A call that times out inside its client instead tells
    "never reached" from "reached, no answer" -- which is what decides
    whether a retry can duplicate the letter. A lane at or below its
    client's total takes that apart without a word; so it refuses
    startup, never a clamp. WHICH CHANNELS: the ones live on this
    deploy -- a channel that is not live never calls a client (its
    formatter refuses every delivery), and the lane of a job does not
    fix its channels anyway (the profile decides both, per type).
    Where the client total is not a hard bound: KNOWN CEILING (client
    totals) in app/core/channels.py.
    """
    clients = [
        spec for spec in CHANNEL_SPECS
        if spec.name in set(live) and spec.client_timeouts
    ]
    problems: list[str] = []
    for lane, timeout in timeouts.items():
        below = [spec for spec in clients if timeout <= spec.client_timeout_total]
        if not below:
            continue
        floor = math.floor(max(spec.client_timeout_total for spec in below)) + 1
        named = ", ".join(
            f"{spec.name} ({spec.client_timeout_total:g} s)" for spec in below
        )
        problems.append(
            f"{lane_deliver_timeout_variable(lane)}={timeout} is not above "
            f"the client timeout of the live channel {named}: a call must "
            f"time out inside its client first, where 'never reached' and "
            f"'reached, no answer' stay apart. Set it to {floor} or more."
        )
    return problems


def lane_deliver_timeout_variable(lane: Lane) -> str:
    """The environment variable of `lane`'s deliver timeout."""
    return f"NOTIFICATION_LANE_{lane.value.upper()}_DELIVER_TIMEOUT_SECONDS"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # -- Application --
    app_env: str = "development"
    log_level: str = "INFO"

    # -- Database (dedicated comms database inside the product Postgres) --
    database_url: str = ""

    # -- Event transport, push side (Phase 3c) --
    # The consumer process (`python -m app.consumer`) reads the
    # product's event stream via a Redis consumer group. Nobody else
    # needs Redis (API/worker are DB-only), so an empty redis_url is
    # validated at CONSUMER startup (app/consumer.py), not here --
    # deliberately not coupled to any delivery channel.
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

    # -- Event transport, the reverse direction (P3-1, spec §7) --
    # comms XADDs the key of a job that changed into this stream of its
    # own redis (the relay in the consumer process,
    # app/transport/push_relay.py); the product listens. The NAME is
    # derived from the inbound stream, like the DLQ's -- it can be
    # neither the inbound stream nor the DLQ, so comms never reads its
    # own pushes -- and is FROZEN CONTRACT surface (deploy/INTEGRATION.md
    # section 10). The cap is approximate MAXLEN trimming: a product
    # away for longer than the stream holds reconciles by the changes
    # feed (section 9), which is why a push may be lost and a result
    # may not.
    changes_stream_maxlen: int = 100_000

    @property
    def changes_stream(self) -> str:
        """The push stream's name, derived from the inbound stream."""
        return f"{self.comms_events_stream}:changes"

    # -- Service-to-service authorization (Phase 3b item 1) --
    # The comms API is INTERNAL (arch decision 14): only the product
    # backend calls it, over the shared Docker network, presenting this
    # shared secret as "Authorization: Bearer <token>". Verified by the
    # FastAPI dependency in app/api/deps.py. NEVER logged (same
    # principle as the formatter's secret sanitizer).
    # Empty token: startup ERROR outside development (an
    # unauthenticated "internal" API is effectively open) -- the same
    # axis as DATABASE_URL. In development an empty token disables auth
    # with a loud startup warning (app/main.py).
    comms_service_token: str = ""

    # -- Telegram (shared bot with the product, see arch doc §7) --
    # A channel key set (app/core/channels.py): both empty = this
    # deploy has no telegram; both set = live; anything else refuses
    # startup.
    telegram_bot_token: str = ""
    # Base URL for deep-link buttons, e.g. "https://t.me/<product_bot>".
    # Consumed by TelegramFormatter.format_deep_link (ported from VELO).
    telegram_bot_url: str = ""

    # -- Email (a channel key set, app/core/channels.py) --
    # All three empty = this deploy has no email; all three set = live;
    # anything else refuses startup. The sender has NO default on
    # purpose: it is a product fact, and a default would make a partial
    # set look complete.
    email_mailgun_api_key: str = ""
    email_mailgun_domain: str = ""
    # A bare address, or the display form "Name <address>" -- a signed
    # sender is materially better for deliverability, which is the
    # reason a provider is used at all.
    email_from_address: str = ""
    # NOT part of the key set (it has a default), but its shape IS
    # checked when spelled out: a closed set of two, so "eu-west" or
    # "europe" refuses startup instead of failing every send.
    email_mailgun_region: str = "eu"

    @property
    def email_api_base_url(self) -> str:
        """Provider base address for this deploy's region.

        THE ONE PLACE these live: the domain-literal fence (arch doc
        §2.6 / decision 13) whitelists this file and nothing else --
        external addresses belong to configuration, never to logic. The
        region is validated as a closed set before this is read, so the
        lookup cannot miss.
        """
        return _EMAIL_API_BASE_URLS[self.email_mailgun_region]

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
    # T12: how many attempts of the PIPELINE (resolve / deliver /
    # rollup, not a channel call) may end in an exception of comms' own
    # before the job gets its outcome -- FAILED, its waiting deliveries
    # closed with failure class `pipeline`. Between attempts the job
    # waits behind the same backoff as a delivery
    # (NOTIFICATION_RETRY_BACKOFF_*), so it never starves the healthy
    # jobs behind it. A separate knob from the delivery attempts: the
    # channel being down and comms being broken are different facts.
    notification_max_pipeline_attempts: int = 3
    # -- Lanes (P4-1, spec §8) --
    # The lanes THIS worker process takes, comma-separated
    # (app/core/lanes.py Lane values). Unset: all four. One image,
    # any set: a second container of the same image may take only
    # `critical` (spec §8.6, phase 7) -- the union over a deploy's
    # worker processes must cover every lane (deploy/INTEGRATION.md).
    # Written but empty, an empty item, an unknown lane or a repeated
    # one refuses startup (worker_lane_problem). Validated here, with
    # every other knob only the worker reads: every entrypoint refuses
    # alike on one .env.
    comms_worker_lanes: str | None = None

    # Jobs one pass of each lane selects, oldest due first; the tail is
    # picked up on the lane's next pass (review 1.1). Each lane selects
    # its own jobs only, so a backlog of one never takes another's
    # places. normal keeps the one number every job had before lanes
    # (50); interactive and critical are a person or a payment at a
    # time; bulk is many jobs that may wait, bounded so that one pass
    # of it stays short. Their delivery concurrency is a constant next
    # to the pool it is counted against (app/engine/service.py
    # LANE_CONCURRENCY).
    notification_lane_interactive_batch_size: int = 10
    notification_lane_critical_batch_size: int = 10
    notification_lane_normal_batch_size: int = 50
    notification_lane_bulk_batch_size: int = 20
    # Seconds one channel call of each lane may take before the attempt
    # abandons it as a timeout (P4-3, spec §8.5) -- the call only, not
    # the wait for one of the lane's slots. 30 in every lane: the one
    # timeout every call had before lanes, so normal behaves as it did.
    # None shorter: startup holds each above the client timeout of
    # every live channel (deliver_timeout_problems), 29 for telegram
    # and email (app/core/channels.py), so a short lane would time out
    # nothing earlier. None longer: a live channel's call ends inside
    # its client first, so a longer lane would only wait longer where a
    # client overruns its own total (KNOWN CEILING (client totals),
    # app/core/channels.py).
    notification_lane_interactive_deliver_timeout_seconds: int = 30
    notification_lane_critical_deliver_timeout_seconds: int = 30
    notification_lane_normal_deliver_timeout_seconds: int = 30
    notification_lane_bulk_deliver_timeout_seconds: int = 30
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
    # (every outcome: sent, partial_sent, failed, expired, cancelled,
    # suppressed, no_recipients) -- rows older
    # than this are deleted
    # in batches by the worker's retention pass, deliveries follow by
    # FK cascade. Age is measured on created_at.
    # SEMANTICS (fix I): 0 means retention is DISABLED -- never
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
    # is NOTIFICATION_RETENTION_DAYS=0.
    notification_retention_interval_seconds: int = 3600

    # Phase 4b item 6: auto-close of idle THREADS. A thread that has
    # been silent for this many days is moved to `closed` (a status,
    # NOT a delete -- a thread is immortal by thread_id, and a closed
    # thread revives on the next client message). Silence is measured
    # on COALESCE(last_message_at, created_at), so a thread that never
    # got a message ages from creation.
    # SEMANTICS (mirrors retention, fix I): 0 means auto-close is
    # DISABLED -- never a mass close. Disabling is loud (worker startup
    # log). Default 30.
    thread_auto_close_days: int = 30

    # The auto-close pass runs on its OWN slow cadence (sibling of the
    # retention pass), NOT on the worker tick and NOT sharing the
    # retention interval -- two unrelated maintenance frequencies must
    # not be coupled through one knob. Per-process monotonic gate in
    # app/worker.py. Strictly > 0: 0 is a config error at startup, NOT
    # "every tick" -- "off" is THREAD_AUTO_CLOSE_DAYS <= 0. Days-
    # granular, so an hour is plenty.
    thread_auto_close_interval_seconds: int = 3600

    # -- Computed properties --

    @property
    def is_dev(self) -> bool:
        """True when running in development mode."""
        return self.app_env == "development"

    @property
    def worker_lanes(self) -> tuple[Lane, ...]:
        """The lanes this worker process takes, in Lane order.

        Read after validation: a value worker_lane_problem refuses never
        reaches here (startup was refused).
        """
        if self.comms_worker_lanes is None:
            return tuple(Lane)
        named = {item.strip() for item in self.comms_worker_lanes.split(",")}
        return tuple(lane for lane in Lane if lane.value in named)

    def lane_deliver_timeout(self, lane: Lane) -> int:
        """Seconds one channel call of `lane` may take (P4-3)."""
        value: int = getattr(
            self, f"notification_lane_{lane.value}_deliver_timeout_seconds",
        )
        return value

    def lane_batch_size(self, lane: Lane) -> int:
        """Jobs one pass of `lane` selects."""
        batch_size: int = getattr(
            self, f"notification_lane_{lane.value}_batch_size",
        )
        return batch_size

    # -- Validation --

    @model_validator(mode="before")
    @classmethod
    def _empty_defaulted_key_means_unwritten(cls, values: Any) -> Any:
        """An empty DEFAULTED channel key reads as a key nobody wrote.

        THE RULE, stated once for every defaulted key there will ever
        be: such a key has no empty state. "Unset" and "set to the
        default" are indistinguishable by construction -- which is
        exactly why these keys are kept out of the set that decides a
        channel (app/core/channels.py) -- so an empty value cannot mean
        anything other than "not written". The keys come from that same
        registry, so a defaulted key added later obeys the rule without
        anyone remembering to; each is a field of this class under its
        lower-case name (pinned by a test).

        WHY HERE AND NOT IN evaluate_channels: that function already
        treats an empty defaulted value as absent and passes it without
        a complaint. Its verdict was never the problem -- the VALUE
        was. `EMAIL_MAILGUN_REGION=` in a deploy's env makes
        pydantic-settings store "" (the key even lands in
        model_fields_set), startup is allowed, and the empty string
        then reaches the region -> base-address lookup below as a bare
        KeyError inside the WORKER, while the API -- which never reads
        the region -- keeps reporting the channel live. Dropping the
        key here instead lets pydantic apply the declared default
        through its own machinery, so model_fields_set stays truthful
        for whoever later builds logic on "was this spelled out".

        WHITESPACE IS NOT EMPTY: "   " was written, and it still
        refuses startup naming the key (app/core/channels.py). And only
        the keys of channel_optional_keys() are normalized: other
        defaulted fields of this class mean something else when emptied
        (an empty LOG_LEVEL is a refusal today), and turning an
        existing refusal into an acceptance is a different question
        from this one.
        """
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        for key in channel_optional_keys():
            if normalized.get(key.lower()) == "":
                del normalized[key.lower()]
        return normalized

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

        # Credentials: every problem is collected before raising, so an
        # integrator fixes all of them in one pass, not one per restart.
        problems: list[str] = []

        # Phase 3b item 4 (3a flag 7.3), generalized: a channel with a
        # PARTIAL key set must die AT STARTUP, not on the delivery path
        # -- an empty bot URL next to a real token turns every deep-link
        # button into a BUTTON_URL_INVALID storm of permanent FAILED
        # deliveries. An EMPTY set is legal: the deploy has no channel.
        # Every declared key is a field of this class under its
        # lower-case name (pinned by a test).
        channel_states, channel_problems = evaluate_channels({
            key: getattr(self, key.lower())
            for key in channel_env_keys() + channel_optional_keys()
        })
        problems.extend(channel_problems)

        # P4-1: the worker's lanes -- a set that takes nothing, or names
        # something that is not a lane, must not start a worker that
        # then silently delivers less than it was meant to.
        lane_problem = worker_lane_problem(self.comms_worker_lanes)
        if lane_problem is not None:
            problems.append(lane_problem)

        # Phase 3b item 1: an "internal" API without its shared secret
        # is effectively open -- required outside development, the same
        # axis as DATABASE_URL above.
        if not self.is_dev and not self.comms_service_token:
            problems.append(
                "COMMS_SERVICE_TOKEN is required outside development: "
                "the comms API is internal (arch decision 14) and must "
                "not run open."
            )

        if problems:
            raise ValueError("\n".join(problems))

        try:
            ZoneInfo(self.default_timezone)
        except (KeyError, ValueError) as exc:
            raise ValueError(
                f"Invalid DEFAULT_TIMEZONE: {self.default_timezone!r}. "
                f"Must be an IANA timezone name (e.g. 'UTC', "
                f"'Europe/Berlin')."
            ) from exc

        # Numeric bounds: the one table above (D1 / R3).
        numeric = numeric_problems(
            {name: getattr(self, name) for name in NUMERIC_BOUNDS}
        )
        # P4-3: and above the clients of the live channels. Reached only
        # with a valid key set, so every channel has its state. A lane
        # already outside its bound is not named twice.
        in_bounds: dict[Lane, int] = {}
        for lane in Lane:
            timeout = self.lane_deliver_timeout(lane)
            bound = NUMERIC_BOUNDS[
                f"notification_lane_{lane.value}_deliver_timeout_seconds"
            ]
            if bound.lo <= timeout <= bound.hi:
                in_bounds[lane] = timeout
        numeric.extend(deliver_timeout_problems(
            in_bounds,
            [
                name for name, state in channel_states.items()
                if state == ChannelState.LIVE
            ],
        ))
        if numeric:
            raise ValueError("\n".join(numeric))

        return self

    # extra="ignore" is a DECISION, not an oversight: a deploy's env is
    # shared with installers that write keys this service does not read
    # (including keys it used to read), and an unknown key must not
    # refuse startup. Its consequence for channel keys is marked as a
    # KNOWN CEILING in app/core/channels.py (evaluate_channels).
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


def worker_lane_problem(value: str | None) -> str | None:
    """What is wrong with COMMS_WORKER_LANES, or None (P4-1).

    Unset (None) is all four lanes and is never a problem. Written, it
    is a comma-separated list of Lane values; spaces around an item are
    not part of it. Every problem is named in one line, so a deploy
    fixes them in one pass:
      - nothing but separators and spaces -- a worker that takes no lane
        delivers nothing; leaving the variable out takes every lane;
      - an empty item between commas;
      - an item that is not a lane (exact spelling, the set is closed);
      - a lane listed twice.
    """
    if value is None:
        return None
    lanes = ", ".join(lane.value for lane in Lane)
    items = [item.strip() for item in value.split(",")]
    if not any(items):
        return (
            f"COMMS_WORKER_LANES={value!r} names no lane: a worker that "
            f"takes no lane delivers nothing. Leave it unset to take "
            f"every lane ({lanes})."
        )
    found: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not item:
            found.append("an empty item between commas")
        elif item not in {lane.value for lane in Lane}:
            found.append(f"{item!r} is not a lane")
        elif item in seen:
            found.append(f"{item!r} is listed twice")
        else:
            seen.add(item)
    if not found:
        return None
    return (
        f"COMMS_WORKER_LANES={value!r}: {'; '.join(found)}. "
        f"Lanes: {lanes}."
    )


def format_startup_error(exc: ValidationError) -> str:
    """Render a settings ValidationError as a message for a human.

    Our own validator raises ValueError with ready-made text; pydantic
    wraps it ("Value error, ...") and adds a location. Unwrap ours,
    keep pydantic's own wording (with the field name) for type errors.
    """
    lines: list[str] = []
    for error in exc.errors():
        ctx_error = (error.get("ctx") or {}).get("error")
        if error["type"] == "value_error" and ctx_error is not None:
            lines.append(str(ctx_error))
        else:
            loc = ".".join(str(part) for part in error["loc"]).upper()
            lines.append(f"{loc}: {error['msg']}")
    body = "\n".join(lines)
    return (
        "comms: startup refused -- the configuration is invalid.\n"
        f"{body}\n"
        "Fix the .env of this deploy and start again."
    )


def load_settings() -> Settings:
    """Build the settings, or exit with a readable message.

    Settings are built at import -- before logging is configured -- so
    a raw pydantic traceback would be the only output. SystemExit with a
    string prints exactly that string to stderr and exits with status 1,
    without a traceback. Every entrypoint (API, worker, consumer,
    migrations) imports this module, so all of them refuse alike.
    """
    try:
        return Settings()
    except ValidationError as exc:
        raise SystemExit(format_startup_error(exc)) from None


settings = load_settings()
