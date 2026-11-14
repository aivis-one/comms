# =============================================================================
# COMMS Service -- Application Configuration
# =============================================================================
#
# All settings loaded from environment variables -- on a box the deploy
# machine gives them (primer.yaml and deploy/docker-compose.yml); there is
# no settings file.
# Pydantic-settings validates types and applies defaults automatically.
#
# DEPLOY MODEL (see Comms-Service-Architecture.md):
#   One comms deploy per product. DATABASE_URL points at a dedicated
#   database + role inside the PRODUCT's Postgres. Channel credentials
#   (bot token) are shared with the product and injected via env.
#
# CHANNELS:
#   A channel is decided by its flag (<CHANNEL>_ENABLED) and by nothing
#   else -- see app/core/channels.py for the rule. false: the deploy has
#   no such channel, and its keys are not read. true: every key of the
#   channel is required and checked. No flag, or any other value:
#   startup is refused naming the flag (load_settings below).
#
# LANGUAGES / DEFAULT_LANGUAGE (W4-1):
#   The instance's languages and its default, from the passport
#   (primer.yaml `env`, protocol 5.2.2). Both required, no default in
#   code: a start without them is refused naming them (language_problems
#   below). A text is looked up in the recipient's language, then in
#   DEFAULT_LANGUAGE, then the stored title/body
#   (app/engine/template_engine.py _locale_chain). LANGUAGES itself is
#   read here only -- the recipient's language is looked up in the
#   profile directly, never in it.
# =============================================================================

import math
import re
from collections.abc import Iterable, Mapping
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import Field, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.core.bounds import (
    NOTIFICATION_MAX_DELIVERY_ATTEMPTS,
    NOTIFICATION_RETENTION_DAYS,
    NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS,
    NOTIFICATION_RETRY_BACKOFF_MAX_SECONDS,
    NUMERIC_BOUNDS,
)
from app.core.channels import (
    CHANNEL_SPECS,
    ChannelSpec,
    ChannelState,
    LLMKeySet,
    channel_env_keys,
    channel_flag_keys,
    channel_optional_keys,
    channel_spec,
    evaluate_channels,
    parse_llm_key_sets,
    storage_address_problem,
)
from app.core.lanes import Lane

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
    # A channel with its own deliver timeout (P5-1: llm) is not bounded
    # by the lanes, so it does not bound them either:
    # own_deliver_timeout_problems holds it to its own number.
    clients = [
        spec for spec in CHANNEL_SPECS
        if spec.name in set(live)
        and spec.client_timeouts
        and not spec.own_deliver_timeout
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


def own_deliver_timeout_problems(
    timeout: int, variable: str, spec: ChannelSpec, live: Iterable[str],
) -> list[str]:
    """The rule of deliver_timeout_problems for a channel that carries
    its own deliver timeout (P5-1): the timeout must be above the
    channel's client total, for the same reason a lane's must -- a call
    has to time out inside its client first. Only when the channel is
    live: an absent channel calls no client."""
    if spec.name not in set(live) or timeout > spec.client_timeout_total:
        return []
    floor = math.floor(spec.client_timeout_total) + 1
    return [
        f"{variable}={timeout} is not above the client timeout of the live "
        f"channel {spec.name} ({spec.client_timeout_total:g} s): a call "
        f"must time out inside its client first, where 'never reached' "
        f"and 'reached, no answer' stay apart. Set it to {floor} or more."
    ]


def lane_deliver_timeout_variable(lane: Lane) -> str:
    """The environment variable of `lane`'s deliver timeout."""
    return f"NOTIFICATION_LANE_{lane.value.upper()}_DELIVER_TIMEOUT_SECONDS"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # -- Application --
    # ONE MODE (LEG-1): comms has no development or production of its
    # own. The token is required to start, the schema endpoints are
    # never mounted (app/main.py), every log line is the machine's JSON
    # (app/core/logging.py).
    log_level: str = "INFO"

    # -- Database (dedicated comms database inside the product Postgres) --
    database_url: str = ""

    # -- Event transport, push side (Phase 3c) --
    # The consumer process (`python -m app.consumer`) reads the
    # product's event stream via a Redis consumer group. Nobody else
    # works with Redis -- the API only checks that it answers, for
    # /health and /ready (W3-1, app/main.py), and an empty redis_url is
    # that check's `error`; the worker is DB-only -- so an empty
    # redis_url is refused at CONSUMER startup (app/consumer.py), not
    # here -- deliberately not coupled to any delivery channel.
    # Names below are FROZEN CONTRACT surface (Phase 3c item 7): the
    # product's outbox relay XADDs into comms_events_stream.
    redis_url: str = ""
    comms_events_stream: str = "comms:events"
    comms_consumer_group: str = "comms"
    # STABLE consumer name (not hostname/pid): after a restart the
    # same name re-reads its own pending entries (XREADGROUP "0"), so
    # unacked messages replay without XAUTOCLAIM machinery. One
    # consumer per deploy by design.
    comms_consumer_name: str = "comms-1"
    # XREADGROUP batch size / block timeout. An entry comms cannot take
    # is refused or dropped and kept nowhere (W5-1,
    # app/transport/consumer.py WHOSE ERROR IT IS): no stream of its own.
    consumer_batch_size: int = 32
    consumer_block_ms: int = 5000

    # -- Event transport, the reverse direction (P3-1, spec §7) --
    # comms XADDs the key of a job that changed into this stream of its
    # own redis (the relay in the consumer process,
    # app/transport/push_relay.py); the product listens. The NAME is
    # derived from the inbound stream -- it cannot be the inbound stream,
    # so comms never reads its own pushes -- and is FROZEN CONTRACT
    # surface (deploy/INTEGRATION.md section 10). The cap is approximate
    # MAXLEN trimming: a product away for longer than the stream holds
    # reconciles by the changes feed (section 9), which is why a push may
    # be lost and a result may not.
    changes_stream_maxlen: int = 100_000

    @property
    def changes_stream(self) -> str:
        """The push stream's name, derived from the inbound stream."""
        return f"{self.comms_events_stream}:changes"

    # -- Service-to-service authorization (Phase 3b item 1) --
    # The comms API is INTERNAL (arch decision 14): only the product
    # backend calls it, over the shared Docker network, presenting this
    # shared secret as "Authorization: Bearer <token>". Verified before
    # anything of the request is read (app/api/service_auth.py). NEVER
    # logged (same principle as the formatter's secret sanitizer).
    # Empty token: startup ERROR (an unauthenticated "internal" API is
    # effectively open) -- the same axis as DATABASE_URL.
    #
    # TWO TOKENS, for a change of key without downtime (Primer protocol
    # 5.4, owner's decision 01.11): the current one and the next one. A
    # request passes with either; the next one is optional ("" or unset:
    # only the current one passes) and never stands alone -- set without
    # the current one, startup is refused: an integrator who set a
    # token must not get an open API.
    # The next one may equal the current one (a repeated step of the
    # machine): one key, compared twice. Which tokens are in force is
    # said in ONE place, service_tokens below.
    #
    # repr=False on both: the settings object's repr never carries a
    # token (pydantic prints every field by default).
    comms_service_token: str = Field(default="", repr=False)
    comms_service_token_next: str = Field(default="", repr=False)

    # -- Channel flags (app/core/channels.py) --
    # Exactly "true" or "false", no default: an absent flag, or any
    # other value, refuses startup naming it. Kept as text, not as a
    # bool, on purpose: pydantic would read "yes", "1" or "on" as true,
    # and the refusal is collected with every other problem below
    # rather than cut short by a type error. A switched-off channel's
    # keys are never read -- the properties below that parse keys ask
    # the flag first.
    telegram_enabled: str | None = None
    email_enabled: str | None = None
    llm_enabled: str | None = None

    # -- Telegram (shared bot with the product, see arch doc §7) --
    # The channel's keys (app/core/channels.py): with TELEGRAM_ENABLED
    # true both are required; with false neither is read.
    telegram_bot_token: str = ""
    # Base URL for deep-link buttons, e.g. "https://t.me/<product_bot>".
    # Consumed by TelegramFormatter.format_deep_link (ported from VELO).
    telegram_bot_url: str = ""

    # -- Email (the channel's keys, app/core/channels.py) --
    # With EMAIL_ENABLED true all three are required; with false none is
    # read. The sender has NO default on purpose: it is a product fact,
    # and a default would make a partial set look complete.
    email_mailgun_api_key: str = ""
    email_mailgun_domain: str = ""
    # A bare address, or the display form "Name <address>" -- a signed
    # sender is materially better for deliverability, which is the
    # reason a provider is used at all.
    email_from_address: str = ""
    # NOT a required key (it has a default), but its shape IS checked
    # when the channel is on and the value spelled out: a closed set of
    # two, so "eu-west" or "europe" refuses startup instead of failing
    # every send.
    email_mailgun_region: str = "eu"

    # -- LLM (the channel's key, app/core/channels.py; P5-1) --
    # One JSON object of named key sets -- {"llm.text": {"endpoint":
    # ..., "key": ..., "model": ..., "gateway": true|false}}. With
    # LLM_ENABLED true it is required, whole and well-formed, or startup
    # is refused naming the set and the field; with false it is not read.
    llm_key_sets: str = ""
    # Seconds one call to ONE set may take; the job's call over its list
    # of sets is bounded by this times the length of the list
    # (app/engine/service.py). Not a lane's: a model writes for tens of
    # seconds, and the lanes' 30 must stay what it is for email.
    llm_deliver_timeout_seconds: int = 120

    # -- The product's storage (W4-2, app/core/channels.py) --
    # `<host>:<port>` of the point of the product's object store, the seam
    # PRODUCT_STORAGE_ADDRESS the machine gives comms from the product
    # (primer.yaml `requires`). A body link is fetched only from
    # `http://<it>/...`. NOT a key of the llm channel: the seam is not
    # under an option -- the machine gives it, and requires it, whatever
    # the instance's options -- and /health answers for it with the llm
    # channel off too (app/api/report.py). Written, it must have its form
    # always, or startup is refused; empty = this deploy has no
    # product storage, every body link is refused, and /health says the
    # seam is not satisfied.
    product_storage_address: str = ""

    @property
    def body_link_prefix(self) -> str | None:
        """`http://<PRODUCT_STORAGE_ADDRESS>/`: what a body link must
        begin with (app/core/channels.py body_link_allowed); None when the
        deploy has no product storage. Built HERE -- the one file the
        domain-literal fence allows a scheme in. Read only off validated
        settings: a malformed address refused startup."""
        if self.product_storage_address == "":
            return None
        return f"http://{self.product_storage_address}/"

    @property
    def llm_sets(self) -> dict[str, LLMKeySet]:
        """This deploy's llm key sets by name, in declaration order; {}
        when the llm channel is switched off -- its key is then not read.
        Only ever read on validated settings: with the channel on, a
        malformed value refused startup, so nothing is lost here."""
        if self.llm_enabled != "true":
            return {}
        sets, _ = parse_llm_key_sets(self.llm_key_sets)
        return sets

    @property
    def channel_states(self) -> dict[str, ChannelState]:
        """The state of every implemented channel on this deploy -- the
        one reading of the flags and keys every reader shares (the
        channel map of app/engine/formatters.py, the startup log, GET
        /health). Read only off validated settings: a broken flag or key
        set never got this far, so every channel has its state."""
        states, _ = evaluate_channels(channel_values(self))
        return states

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

    # -- Languages (W4-1): the instance's, from the passport --
    # LANGUAGES: codes joined by commas, `en,ru,uk` (the machine's form,
    # protocol 5.2.2). DEFAULT_LANGUAGE: one of them, the language a text
    # falls back to. Required, no default (language_problems).
    languages: str = ""
    default_language: str = ""
    # Fallback IANA timezone for recipients without an explicit one --
    # quiet-hours window math needs a timezone for everybody. Validated
    # at startup (a bad name must not surface on the delivery path).
    default_timezone: str = "UTC"

    # -- Product profile (per-deploy data: types + templates) --
    # Directory with the product profile: types.yaml + templates/.
    # On a box the machine gives it: the product's directory, copied and
    # bound read-only, its path under this variable (form D, primer.yaml
    # `accepts.profile`); tests point it at the fixture directory in
    # this repo. Empty refuses the start of every process that reads
    # the profile -- see app/profile/loader.py:
    # install_profile_from_settings.
    templates_dir: str = ""
    # Directory with the TEXT OVERLAY (PO-1): {locale}.yaml files of the
    # shape of templates/{locale}.yaml, each text replacing the base text
    # at the same locale, type, channel and field. On a box it has no
    # mount -- the machine has no kind of data for it -- and stays empty
    # (deploy/INTEGRATION.md section 3). Empty = no overlay, the base
    # texts as they are; set to a directory that does not exist = the
    # service does not start. See
    # app/profile/loader.py, THE TEXT OVERLAY.
    templates_overlay_dir: str = ""

    # -- Notification engine --
    notification_poll_interval_seconds: int = 5
    notification_max_backoff_seconds: int = 60
    notification_max_delivery_attempts: int = NOTIFICATION_MAX_DELIVERY_ATTEMPTS
    # T12: how many attempts of the PIPELINE (resolve / deliver /
    # rollup, not a channel call) may end in an exception of comms' own
    # IN A ROW before the job gets its outcome -- FAILED, its waiting
    # deliveries closed with failure class `pipeline`. An attempt that
    # commits sets the count back to 0 (REV-2, app/engine/processor.py
    # _attempt_job): failures scattered over a long job never add up
    # to the ceiling. Between attempts the job waits behind the same
    # backoff as a delivery (NOTIFICATION_RETRY_BACKOFF_*), so it never
    # starves the healthy jobs behind it. A separate knob from the
    # delivery attempts: the channel being down and comms being broken
    # are different facts.
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
    # alike on one environment.
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
    notification_retry_backoff_base_seconds: int = (
        NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS
    )
    notification_retry_backoff_max_seconds: int = (
        NOTIFICATION_RETRY_BACKOFF_MAX_SECONDS
    )

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
    notification_retention_days: int = NOTIFICATION_RETENTION_DAYS

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
    # NOT a delete -- the thread stays, only the product deletes one
    # (TK-2), and a closed thread revives on the next client message).
    # Silence is measured on COALESCE(last_message_at, created_at), so
    # a thread that never got a message ages from creation.
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
    def service_tokens(self) -> tuple[str, ...]:
        """The service tokens a request may present: the current one,
        then the next one when set. An empty value is not a token and
        is never in the list -- so an empty presented token matches
        nothing. Read by app/api/deps.py require_service_auth, which
        compares the presented token with every one of them."""
        return tuple(
            token
            for token in (self.comms_service_token, self.comms_service_token_next)
            if token
        )

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
    def _validate(self) -> "Settings":
        """Validate values; DATABASE_URL first, which has no default."""
        if not self.database_url:
            raise ValueError("DATABASE_URL is required.")

        if self.log_level.upper() not in _VALID_LOG_LEVELS:
            raise ValueError(
                f"Invalid LOG_LEVEL: {self.log_level}. "
                f"Valid: {', '.join(sorted(_VALID_LOG_LEVELS))}"
            )

        # Credentials: every problem is collected before raising, so an
        # integrator fixes all of them in one pass, not one per restart.
        problems: list[str] = []

        # Phase 3b item 4 (3a flag 7.3), generalized: a switched-on
        # channel with an incomplete key set must die AT STARTUP, not on
        # the delivery path -- an empty bot URL next to a real token
        # turns every deep-link button into a BUTTON_URL_INVALID storm of
        # permanent FAILED deliveries. A switched-off channel is legal
        # and is not read (W2-1). Every flag and declared key is a field
        # of this class under its lower-case name (pinned by a test).
        channel_states, channel_problems = evaluate_channels(
            channel_values(self),
        )
        problems.extend(channel_problems)
        # W4-1: the instance's languages, as the passport gives them.
        problems.extend(language_problems(self.languages, self.default_language))
        problems.extend(self._free_route_problems())

        # P4-1: the worker's lanes -- a set that takes nothing, or names
        # something that is not a lane, must not start a worker that
        # then silently delivers less than it was meant to.
        lane_problem = worker_lane_problem(self.comms_worker_lanes)
        if lane_problem is not None:
            problems.append(lane_problem)

        # Phase 3b item 1: an "internal" API without its shared secret
        # is effectively open -- required, the same axis as DATABASE_URL
        # above.
        if not self.comms_service_token:
            problems.append(
                "COMMS_SERVICE_TOKEN is required: "
                "the comms API is internal (arch decision 14) and must "
                "not run open."
            )
        # W4-2: a seam the machine gave in a form comms cannot read is a
        # defect of the box -- a red start, never a deploy that refuses
        # every voice in silence. Empty is no storage, not a defect.
        if self.product_storage_address != "":
            storage_problem = storage_address_problem(self.product_storage_address)
            if storage_problem is not None:
                problems.append(storage_problem)
        # W2-1b: the next token rides beside a current one, never alone
        # -- named on its own line, beside the missing current one.
        if self.comms_service_token_next and not self.comms_service_token:
            problems.append(
                "COMMS_SERVICE_TOKEN_NEXT is set without COMMS_SERVICE_TOKEN: "
                "the next token is accepted beside a current one, never "
                "alone."
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

        # Numeric bounds: the one table, app/core/bounds.py (D1 / R3).
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
        live = [
            name for name, state in channel_states.items()
            if state == ChannelState.LIVE
        ]
        numeric.extend(deliver_timeout_problems(in_bounds, live))
        # P5-1: a channel with its own deliver timeout -- the same rule,
        # its own number. One outside its bound is not named twice.
        llm_bound = NUMERIC_BOUNDS["llm_deliver_timeout_seconds"]
        if llm_bound.lo <= self.llm_deliver_timeout_seconds <= llm_bound.hi:
            numeric.extend(own_deliver_timeout_problems(
                self.llm_deliver_timeout_seconds,
                "LLM_DELIVER_TIMEOUT_SECONDS",
                channel_spec("llm"),
                live,
            ))
        if numeric:
            raise ValueError("\n".join(numeric))

        return self

    def _free_route_problems(self) -> list[str]:
        """No free model route (spec §12.1, the third gateway condition:
        no free routes, debugging included). A gateway marks a free
        route by the suffix `:free` of the model id. Read only off
        well-formed sets: a malformed value is refused by the channel
        rule already, naming its set; and never off a switched-off
        channel, whose key is not read."""
        if self.llm_enabled != "true":
            return []
        if self.llm_key_sets == "":
            return []
        sets, _ = parse_llm_key_sets(self.llm_key_sets)
        return [
            f"LLM_KEY_SETS: set {name!r} names a free route (a model id "
            f"ending in ':free'); free routes are refused"
            for name, key_set in sets.items()
            if key_set.model.endswith(":free")
        ]

    # extra="ignore" is a DECISION, not an oversight: a deploy's env is
    # shared with installers that write keys this service does not read
    # (including keys it used to read), and an unknown key must not
    # refuse startup. A misspelt channel variable is still caught: a
    # misspelt flag is an absent flag, and a misspelt key of a
    # switched-on channel is a missing key -- both refuse startup.
    # No env_file (LEG-1): the environment is the one source -- on a box
    # the machine's, in CI the job's; a file of settings beside it would
    # be a second, read only where one happens to lie.
    model_config = SettingsConfigDict(extra="ignore")


def channel_values(source: Settings) -> dict[str, str | None]:
    """Every channel flag and key of `source` by its variable name --
    what app/core/channels.py evaluate_channels reads. Every one is a
    field of Settings under its lower-case name (pinned by a test)."""
    return {
        key: getattr(source, key.lower())
        for key in (
            channel_flag_keys() + channel_env_keys() + channel_optional_keys()
        )
    }


LANGUAGE_CODE = re.compile(r"[a-z]{2,3}")
"""A language as the passport names it (protocol 5.2.2): the primary
subtag alone, lower case -- `en`, `uk`, `haw`; never a region."""


def language_problems(languages: str, default: str) -> list[str]:
    """What is wrong with LANGUAGES and DEFAULT_LANGUAGE, each naming
    its variable (W4-1). The machine renders both from the passport and
    holds them to this form; comms holds them again, at its own start:
    a start on a value it cannot read is a red start, never a fallback.

    LANGUAGES: codes joined by commas, no spaces, at least one, each
    once. DEFAULT_LANGUAGE: one code, among LANGUAGES. A recipient's
    language is not held to either: it is looked up in the profile as
    written (app/engine/template_engine.py).
    """
    problems: list[str] = []
    codes = languages.split(",") if languages else []
    if not codes:
        problems.append(
            "LANGUAGES is not set: the instance's languages, codes joined "
            "by commas (`en,ru`), from the passport."
        )
    bad = sorted({code for code in codes if not LANGUAGE_CODE.fullmatch(code)})
    if bad:
        problems.append(
            f"LANGUAGES={languages!r}: {', '.join(repr(code) for code in bad)} "
            f"is not a language code -- two or three lower-case letters, no "
            f"region, no spaces, joined by commas."
        )
    twice = sorted({code for code in codes if codes.count(code) > 1})
    if twice:
        problems.append(
            f"LANGUAGES={languages!r}: {', '.join(twice)} is named twice."
        )
    if not default:
        problems.append(
            "DEFAULT_LANGUAGE is not set: the language a text falls back "
            "to, one of LANGUAGES, from the passport."
        )
    elif not LANGUAGE_CODE.fullmatch(default):
        problems.append(
            f"DEFAULT_LANGUAGE={default!r} is not a language code -- two or "
            f"three lower-case letters, no region."
        )
    elif codes and not bad and default not in codes:
        problems.append(
            f"DEFAULT_LANGUAGE={default!r} is not among LANGUAGES={languages!r}."
        )
    return problems


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
        "Fix the variables named above and start again."
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
