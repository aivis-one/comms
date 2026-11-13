# =============================================================================
# W2-1 -- a channel is an option of the instance: its flag decides
# =============================================================================
#
# Under the Primer's machine a channel of comms is an option of the
# instance, switched by the passport; comms learns of it only by the
# flag <CHANNEL>_ENABLED, and the keys of a switched-off option hold a
# literal of the machine that comms never reads (app/core/channels.py).
#
#   item 1  the flag: exactly true or false, required; true -> the key
#           set is required and checked; false -> nothing of the channel
#           is read, whatever it holds
#   item 2  a profile type routed into a switched-off channel loads
#   item 3  its delivery there is the outcome "channel unavailable"
#           (failed, configuration), the job's other channels going on --
#           on the real database, through the existing path
#   item 4  deploy/INTEGRATION.md and deploy/.env.example say so, held
#           to the code both ways
#
# THREE DOUBLE AXES. REPEAT: every channel, every broken flag in one
# refusal, two types into one switched-off channel. EMPTY: the flag
# written empty, every key empty with the flag on, a job whose only
# channel is off. SHORTFALL: the flag absent, one key short, an llm
# type whose key sets are not defined.
#
# MUTATIONS (one run, every one red here):
#   M1 evaluate_channels decides by the keys, the flag ignored
#                                         -> TestTheFlag false+literal,
#                                            true+empty
#   M2 the flag read as pydantic reads a bool (yes, 1, True)
#                                         -> test_a_value_not_a_switch_...
#   M3 an absent flag is false            -> test_an_absent_flag_...
#   M4 a switched-off channel's optional keys still checked
#                                         -> test_false_with_malformed_...
#   M5 a route check into a switched-off channel back on the startup
#      path                               -> TestTheProfile
#   M6 llm_set_names=() with llm off      -> test_an_llm_type_loads_...
#   M7 intake refuses a type with a switched-off channel
#                                         -> TestTheDelivery
#   M8 llm_sets without the flag guard    -> test_false_with_a_full_...
#   M9 the body host checked with llm off -> test_an_llm_job_with_audio_...
#   M10 _free_route_problems without the guard
#                                         -> test_a_free_route_is_not_...
#   M11 a flag renamed in the document; the reason text changed in the
#       code                              -> TestTheContract
#   M12 a key holding the literal read as empty with the flag on
#                                         -> test_true_with_the_literal_...
# =============================================================================

import json
import re
import shutil
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.channels import (
    CHANNEL_SPECS,
    FLAG_VALUES,
    ChannelState,
    channel_flag_keys,
    channel_spec,
    switched_off_reason,
)
from app.core.config import Settings, format_startup_error, settings
from app.core.exceptions import ProfileError
from app.engine import formatters as formatters_module
from app.engine.constants import (
    ChannelAnswer,
    DeliveryChannel,
    DeliveryStatus,
    FailureClass,
    JournalSubject,
    NotificationStatus,
    TargetType,
)
from app.engine.formatters import (
    ConfigurationError,
    UnavailableChannelFormatter,
    build_formatters,
    channel_map,
)
from app.engine.models import (
    LLMCall,
    Notification,
    NotificationDelivery,
    NotificationTransition,
)
from app.engine.processor import process_pending_notifications
from app.engine.service import create_notification
from app.profile.loader import install_profile_from_settings
from app.profile.registry import registry
from tests.helpers import configure_every_channel, create_recipient, intake_fields

_REPO = Path(__file__).resolve().parents[1]
_FIXTURE = _REPO / "tests" / "fixtures" / "profile"
_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")
_TEMPLATE = (_REPO / "deploy" / ".env.example").read_text(encoding="utf-8")

# What the machine writes into the keys of a switched-off option
# (Primer protocol 5.2.3). Here only as INPUT: comms never names it.
_LITERAL = "primer-switched-off"

# Assembled at run time: a literal in the shape of a bot token blocks
# the push (§0 §10).
_TOKEN = "123456" + ":" + "unit-test-channel-flags"
_LLM_SETS = json.dumps({"llm.text": {
    "endpoint": "https://llm.example.org/v1", "key": "unit-flags-llm-key",
    "model": "unit-model", "gateway": False,
}})

# Every channel with a flag: its full, well-formed key set and its
# spelled-out defaulted keys, by variable name.
_FULL: dict[str, dict[str, str]] = {
    "telegram": {
        "TELEGRAM_BOT_TOKEN": _TOKEN,
        "TELEGRAM_BOT_URL": "https://t.me/unit_flags_bot",
    },
    "email": {
        "EMAIL_MAILGUN_API_KEY": "unit-flags-provider-key",
        "EMAIL_MAILGUN_DOMAIN": "mg.example.org",
        "EMAIL_FROM_ADDRESS": "Unit <unit@example.org>",
        "EMAIL_MAILGUN_REGION": "eu",
    },
    "llm": {
        "LLM_KEY_SETS": _LLM_SETS,
        "LLM_BODY_HOSTS": "store.internal:9000",
    },
}
# A malformed value for one key of each channel, and one for a defaulted
# key where the channel has one.
_MALFORMED: dict[str, dict[str, str]] = {
    "telegram": {"TELEGRAM_BOT_TOKEN": "not-a-token"},
    "email": {"EMAIL_FROM_ADDRESS": "noreply", "EMAIL_MAILGUN_REGION": "europe"},
    "llm": {"LLM_KEY_SETS": "{", "LLM_BODY_HOSTS": "store"},
}
_FLAGGED = [spec.name for spec in CHANNEL_SPECS if spec.flag is not None]


def _kwargs(values: dict[str, str]) -> dict[str, str]:
    return {name.lower(): value for name, value in values.items()}


def _settings(
    channel: str, flag: str | None, keys: dict[str, str] | None = None,
) -> Settings:
    """A production deploy: `channel` with `flag` (None: absent) and
    `keys`, every other channel switched off."""
    flags: dict[str, Any] = dict.fromkeys(channel_flag_keys(), "false")
    flags[channel_spec(channel).flag or ""] = flag
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        app_env="production",
        database_url="postgresql+asyncpg://u:p@db/comms_unit",
        comms_service_token="unit-test-service-token",
        **_kwargs(flags),
        **_kwargs(keys or {}),
    )


def _refusal(
    channel: str, flag: str | None, keys: dict[str, str] | None = None,
) -> str:
    with pytest.raises(ValidationError) as caught:
        _settings(channel, flag, keys)
    return format_startup_error(caught.value)


# -----------------------------------------------------------------------------
# item 1 -- the flag
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("channel", _FLAGGED)
class TestTheFlag:
    def test_true_with_the_full_set_is_live(self, channel: str) -> None:
        source = _settings(channel, "true", _FULL[channel])
        assert channel_map(source)[channel] == ChannelState.LIVE

    def test_true_with_every_key_empty_refuses_naming_them(
        self, channel: str,
    ) -> None:
        """EMPTY: the deploy without the channel was this, until W2-1."""
        spec = channel_spec(channel)
        refusal = _refusal(channel, "true", dict.fromkeys(spec.keys, ""))
        assert f"channel '{channel}' is misconfigured" in refusal
        assert f"missing: {', '.join(spec.keys)}" in refusal
        assert f"or set {spec.flag}=false" in refusal

    def test_true_with_one_key_short_refuses_naming_it(
        self, channel: str,
    ) -> None:
        """SHORTFALL: each key in turn, the others whole."""
        for key in channel_spec(channel).keys:
            refusal = _refusal(channel, "true", {**_FULL[channel], key: ""})
            assert f"missing: {key} " in refusal, key

    def test_true_with_a_malformed_key_refuses_naming_it(
        self, channel: str,
    ) -> None:
        for key, value in _MALFORMED[channel].items():
            refusal = _refusal(channel, "true", {**_FULL[channel], key: value})
            assert f"{key} is malformed" in refusal, key

    def test_true_with_the_literal_is_a_malformed_key(
        self, channel: str,
    ) -> None:
        """M12: the literal is not known here -- with the channel on it
        is a value like any other, refused as malformed, never read as
        "not set"."""
        keys = dict.fromkeys(_FULL[channel], _LITERAL)
        refusal = _refusal(channel, "true", keys)
        assert f"channel '{channel}' is misconfigured" in refusal
        assert "is malformed" in refusal
        assert "missing:" not in refusal

    def test_false_with_the_literal_starts_switched_off(
        self, channel: str,
    ) -> None:
        """What the machine writes for a switched-off option: every key,
        the defaulted ones too, the literal. The pair: the same keys with
        the flag on are refused (the test above)."""
        source = _settings(channel, "false", dict.fromkeys(_FULL[channel], _LITERAL))
        assert channel_map(source)[channel] == ChannelState.NOT_CONFIGURED
        assert channel_map(source)["in_app"] == ChannelState.LIVE

    def test_false_with_malformed_keys_starts(self, channel: str) -> None:
        """M4: the defaulted keys of a switched-off channel are not
        checked either."""
        source = _settings(
            channel, "false", {**_FULL[channel], **_MALFORMED[channel]},
        )
        assert channel_map(source)[channel] == ChannelState.NOT_CONFIGURED

    def test_false_with_a_full_set_is_still_off_and_nothing_is_read(
        self, channel: str,
    ) -> None:
        """M8: the properties that parse a key ask the flag first, and no
        client of the channel is built."""
        source = _settings(channel, "false", _FULL[channel])
        assert channel_map(source)[channel] == ChannelState.NOT_CONFIGURED
        assert source.llm_sets == {}
        assert source.llm_body_host_set == frozenset()
        built: list[str] = []
        registry_ = build_formatters(
            source,
            lambda **kwargs: built.append("bot"),  # type: ignore[arg-type,return-value,func-returns-value]
            lambda: built.append("http"),
            lambda: built.append("llm"),
            lambda: built.append("body"),
        )
        assert built == []
        assert registry_.closers == []
        unavailable = (
            registry_.model if channel == DeliveryChannel.LLM
            else registry_.formatters[channel]
        )
        assert isinstance(unavailable, UnavailableChannelFormatter)

    def test_an_absent_flag_refuses_naming_it(self, channel: str) -> None:
        """M3, SHORTFALL: no flag is not false -- a red start."""
        spec = channel_spec(channel)
        refusal = _refusal(channel, None, _FULL[channel])
        assert f"{spec.flag} is not set" in refusal
        assert "exactly true or false" in refusal

    @pytest.mark.parametrize(
        "value", ["", "yes", "1", "True", "FALSE", " true", "false ", "on"],
    )
    def test_a_value_not_a_switch_refuses_naming_it(
        self, channel: str, value: str,
    ) -> None:
        """M2: exactly true or false, nothing pydantic would read as a
        bool, no case or space forgiven."""
        spec = channel_spec(channel)
        refusal = _refusal(channel, value, _FULL[channel])
        assert f"{spec.flag}={value!r} is not a switch" in refusal

    def test_a_broken_flag_does_not_judge_the_keys(self, channel: str) -> None:
        """Whether the keys are wanted is not known: only the flag."""
        keys = dict.fromkeys(channel_spec(channel).keys, "")
        refusal = _refusal(channel, "yes", keys)
        assert "is not a switch" in refusal
        assert "missing:" not in refusal


class TestEveryFlag:
    def test_every_broken_flag_is_named_in_one_refusal(self) -> None:
        """REPEAT: all of them, one pass."""
        with pytest.raises(ValidationError) as caught:
            Settings(
                _env_file=None,  # type: ignore[call-arg]
                app_env="production",
                database_url="postgresql+asyncpg://u:p@db/comms_unit",
                comms_service_token="unit-test-service-token",
                telegram_enabled=None, email_enabled="yes", llm_enabled="",
            )
        refusal = format_startup_error(caught.value)
        assert "TELEGRAM_ENABLED is not set" in refusal
        assert "EMAIL_ENABLED='yes' is not a switch" in refusal
        assert "LLM_ENABLED='' is not a switch" in refusal
        assert "Traceback" not in refusal

    def test_the_flag_comes_from_the_environment(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The process's own path: the variable as it is written."""
        base: dict[str, Any] = {
            "_env_file": None, "app_env": "production",
            "database_url": "postgresql+asyncpg://u:p@db/comms_unit",
            "comms_service_token": "unit-test-service-token",
        }
        monkeypatch.setenv("EMAIL_ENABLED", "yes")
        with pytest.raises(ValidationError, match="EMAIL_ENABLED='yes'"):
            Settings(**base)
        monkeypatch.delenv("EMAIL_ENABLED")
        with pytest.raises(ValidationError, match="EMAIL_ENABLED is not set"):
            Settings(**base)
        monkeypatch.setenv("EMAIL_ENABLED", "false")
        assert channel_map(Settings(**base))["email"] == ChannelState.NOT_CONFIGURED

    def test_in_app_has_no_flag_and_is_always_on(self) -> None:
        assert channel_spec("in_app").flag is None
        assert set(channel_flag_keys()) == {
            "TELEGRAM_ENABLED", "EMAIL_ENABLED", "LLM_ENABLED",
        }
        assert FLAG_VALUES == ("true", "false")

    def test_a_free_route_is_not_read_while_llm_is_off(self) -> None:
        """M10: the free-route refusal reads LLM_KEY_SETS, and a
        switched-off channel's key is not read. The pair: on, refused."""
        free = json.dumps({"llm.text": {
            "endpoint": "https://llm.example.org/v1", "key": "k",
            "model": "vendor/cheap:free", "gateway": True,
        }})
        assert _settings("llm", "false", {"LLM_KEY_SETS": free}).llm_sets == {}
        assert "names a free route" in _refusal(
            "llm", "true", {"LLM_KEY_SETS": free},
        )


# -----------------------------------------------------------------------------
# item 2 -- the profile takes a route into a switched-off channel
# -----------------------------------------------------------------------------

_LLM_TYPES = """
  ask.off:
    channels: [llm]
    llm_sets: [llm.text]
    lane: normal
    max_audio_bytes: 1000
    retry_max_attempts: 2
    retry_backoff_seconds: 0
  ask.other:
    channels: [llm]
    llm_sets: [llm.other]
    lane: normal
"""


def _profile(root: Path) -> Path:
    """The fixture profile -- [in_app, email] and [telegram, email]
    types among others -- with two llm types."""
    shutil.copytree(_FIXTURE / "templates", root / "templates")
    types = (_FIXTURE / "types.yaml").read_text(encoding="utf-8")
    (root / "types.yaml").write_text(types + _LLM_TYPES, encoding="utf-8")
    return root


@pytest.fixture
def startup_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = _profile(tmp_path / "profile")
    monkeypatch.setattr(settings, "templates_dir", str(root))
    monkeypatch.setattr(settings, "templates_overlay_dir", "")
    for flag in channel_flag_keys():
        monkeypatch.setattr(settings, flag.lower(), "false")
    return root


class TestTheProfile:
    def test_types_into_switched_off_channels_load(
        self, startup_profile: Path,
    ) -> None:
        """M5. REPEAT: several types into each switched-off channel."""
        registry.reset()
        assert install_profile_from_settings() is True
        assert channel_map(settings)["email"] == ChannelState.NOT_CONFIGURED
        for key in ("unit_event_in_app_email", "unit_event_telegram_email"):
            record = registry.record_of(key)
            assert record is not None, key
            assert "email" in record.value("channels")

    def test_an_llm_type_loads_with_llm_off(self, startup_profile: Path) -> None:
        """M6: its key sets are not checked -- LLM_KEY_SETS is not read.
        SHORTFALL: ask.other names a set no deploy defines."""
        registry.reset()
        assert install_profile_from_settings() is True
        assert registry.record_of("ask.off") is not None
        assert registry.record_of("ask.other") is not None

    def test_with_llm_on_the_sets_are_checked(
        self, startup_profile: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The pair: the same profile on a deploy with llm on is refused
        for the set it does not define, and only for it."""
        monkeypatch.setattr(settings, "llm_enabled", "true")
        monkeypatch.setattr(settings, "llm_key_sets", _LLM_SETS)
        with pytest.raises(ProfileError) as caught:
            install_profile_from_settings()
        assert "routes to key set 'llm.other'" in str(caught.value)
        assert "'llm.text'" not in str(caught.value).split("defined:")[0]

    def test_with_every_channel_on_it_loads_too(
        self, startup_profile: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        configure_every_channel(monkeypatch)
        registry.reset()
        assert install_profile_from_settings() is True
        assert channel_map(settings)["email"] == ChannelState.LIVE


# -----------------------------------------------------------------------------
# item 3 -- the delivery: the outcome "channel unavailable", the rest goes
# -----------------------------------------------------------------------------


def _email_off() -> Settings:
    """The deploy: email switched off with the machine's literal in its
    keys, every other channel off too."""
    return _settings("email", "false", dict.fromkeys(_FULL["email"], _LITERAL))


async def _deliveries(
    session: AsyncSession, nid: Any,
) -> dict[str, NotificationDelivery]:
    rows = await session.execute(
        select(NotificationDelivery)
        .where(NotificationDelivery.notification_id == nid)
        .execution_options(populate_existing=True)
    )
    return {d.channel: d for d in rows.scalars()}


class TestTheDelivery:
    async def test_in_app_goes_email_is_the_outcome(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """M7: the job is accepted, its in_app delivery sent, its email
        delivery failed `configuration` with the flag named -- through
        the existing path, its channel row in the journal."""
        monkeypatch.setattr(
            formatters_module, "_registry",
            build_formatters(_email_off(), lambda **kwargs: None),
        )
        recipient = await create_recipient(db_session, email="person@example.org")
        notification = await create_notification(
            db_session, **intake_fields(), type="unit_event_in_app_email",
            title="T", body="B", target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        nid = notification.id
        await db_session.commit()

        assert await process_pending_notifications() == 1

        by_channel = await _deliveries(db_session, nid)
        assert set(by_channel) == {"in_app", "email"}
        assert by_channel["in_app"].status == DeliveryStatus.SENT
        email = by_channel["email"]
        assert email.status == DeliveryStatus.FAILED
        assert email.failure_class == FailureClass.CONFIGURATION
        assert email.attempts == 0
        assert email.error_message == (
            "channel 'email' is not available on this deploy: "
            + switched_off_reason("email")
        )
        assert "EMAIL_ENABLED=false" in email.error_message
        job = await db_session.get(Notification, nid, populate_existing=True)
        assert job is not None
        assert job.status == NotificationStatus.PARTIAL_SENT
        rows = (await db_session.execute(
            select(NotificationTransition).where(
                NotificationTransition.notification_id == nid,
                NotificationTransition.subject == JournalSubject.CHANNEL,
            )
        )).scalars().all()
        by_row = {r.channel: r for r in rows}
        assert by_row["email"].outcome == ChannelAnswer.REFUSED
        assert by_row["email"].failure_class == FailureClass.CONFIGURATION
        assert by_row["in_app"].outcome == ChannelAnswer.ACCEPTED

    async def test_a_person_without_an_address_meets_the_same_outcome(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """SHORTFALL: the channel being off is said before the address
        is looked at -- `configuration`, not `no_address`."""
        monkeypatch.setattr(
            formatters_module, "_registry",
            build_formatters(_email_off(), lambda **kwargs: None),
        )
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session, **intake_fields(), type="unit_event_in_app_email",
            title="T", body="B", target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        nid = notification.id
        await db_session.commit()

        await process_pending_notifications()

        by_channel = await _deliveries(db_session, nid)
        assert by_channel["in_app"].status == DeliveryStatus.SENT
        assert by_channel["email"].status == DeliveryStatus.FAILED
        assert by_channel["email"].failure_class == FailureClass.CONFIGURATION

    async def test_a_job_whose_only_channel_is_off_fails(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """EMPTY: nothing of the job can go -- failed, not a success.
        REPEAT: two people, two refusals."""
        monkeypatch.setattr(
            formatters_module, "_registry",
            build_formatters(_email_off(), lambda **kwargs: None),
        )
        for n in range(2):
            await create_recipient(db_session, email=f"p{n}@example.org")
        notification = await create_notification(
            db_session, **intake_fields(), type="unit_event_email",
            title="T", body="B", target_type=TargetType.ALL,
            target_value="*",
        )
        nid = notification.id
        await db_session.commit()

        await process_pending_notifications()

        rows = (await db_session.execute(
            select(NotificationDelivery)
            .where(NotificationDelivery.notification_id == nid)
            .execution_options(populate_existing=True)
        )).scalars().all()
        assert len(rows) == 2
        assert {(r.status, r.failure_class) for r in rows} == {
            (DeliveryStatus.FAILED, FailureClass.CONFIGURATION),
        }
        job = await db_session.get(Notification, nid, populate_existing=True)
        assert job is not None
        assert job.status == NotificationStatus.FAILED

    async def test_an_llm_job_with_audio_is_accepted_and_is_the_outcome(
        self, db_session: AsyncSession, tmp_path: Path,
    ) -> None:
        """M9: with llm off the body's host is not checked -- the list
        that would judge it is not read -- and the job ends in the same
        outcome, its letter and link gone with it."""
        from app.profile.loader import (
            FileProfileSource,
            install_profile,
            load_profile,
        )

        registry.reset()
        install_profile(load_profile(FileProfileSource(_profile(tmp_path / "p"))))
        assert settings.llm_enabled == "false"
        assert settings.llm_body_host_set == frozenset()
        notification = await create_notification(
            db_session, **intake_fields(), type="ask.off",
            title=None, body=None, target_type=None, target_value=None,
            llm={"messages": [{"role": "user", "content": "x"}]},
            llm_audio={"url": "http://elsewhere.internal:9000/a.wav",
                       "format": "wav"},
        )
        nid = notification.id
        await db_session.commit()

        await process_pending_notifications()

        (delivery,) = (await _deliveries(db_session, nid)).values()
        assert delivery.channel == DeliveryChannel.LLM
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.failure_class == FailureClass.CONFIGURATION
        assert "LLM_ENABLED=false" in (delivery.error_message or "")
        call = await db_session.get(LLMCall, nid, populate_existing=True)
        assert call is not None
        assert call.letter is None and call.audio is None


# -----------------------------------------------------------------------------
# item 4 -- the contract, both ways
# -----------------------------------------------------------------------------


def _flag_table() -> dict[str, str]:
    """channel -> flag, as deploy/INTEGRATION.md's table lists them."""
    start = _DOC.index("| channel | flag |\n")
    table = _DOC[start:_DOC.index("\n\n", start)]
    return dict(re.findall(r"^\| `([a-z_]+)` \| `([A-Z_]+)` \|$", table, re.M))


class TestTheContract:
    def test_the_table_is_the_specs(self) -> None:
        """M11, both ways: every flag of the code in the table under its
        channel, and nothing else there."""
        table = _flag_table()
        assert table, "the pair: the table is read"
        assert table == {
            spec.name: spec.flag for spec in CHANNEL_SPECS if spec.flag
        }
        assert "`in_app` has no flag" in " ".join(_DOC.split())

    def test_every_flag_the_document_names_is_one(self) -> None:
        named = set(re.findall(r"\b([A-Z]+_ENABLED)\b", _DOC))
        assert named == set(channel_flag_keys())

    def test_the_values_are_the_codes(self) -> None:
        line = _DOC[_DOC.index("The value is exactly"):]
        line = line[:line.index(".")]
        assert tuple(re.findall(r"`([a-z]+)`", line)) == FLAG_VALUES

    async def test_the_outcome_text_is_the_built_one(self) -> None:
        """Read by a person, so held by the value the code builds: the
        refusal the switched-off email actually raises."""
        built = build_formatters(_email_off(), lambda **kwargs: None)
        formatter = built.formatters[DeliveryChannel.EMAIL]
        with pytest.raises(ConfigurationError) as caught:
            await formatter.deliver(
                Notification(type="unit_event", title="T", body="B",
                             target_type="user", target_value="*"),
                NotificationDelivery(channel="email"),
                None,  # type: ignore[arg-type]
            )
        assert f"\n    {caught.value}\n" in _DOC

    def test_the_env_template_carries_every_flag(self) -> None:
        """Until W1-2 the writer was "a product installer" (its second
        pass); the flag is now the passport's option, a key of
        primer.yaml `env` (tests/test_primer_yaml.py TestChannels)."""
        for flag in channel_flag_keys():
            assert re.search(
                rf"^# by: primer.yaml\n{flag}=(true|false)$",
                _TEMPLATE, re.M,
            ), flag

    def test_the_code_never_names_the_literal(self) -> None:
        """The protocol: code learns of an option only by the flag. The
        pair: the walk reads the code, and this file does use it."""
        sources = list((_REPO / "app").rglob("*.py"))
        assert len(sources) > 50
        assert not [
            p for p in sources if _LITERAL in p.read_text(encoding="utf-8")
        ]
        assert _LITERAL in Path(__file__).read_text(encoding="utf-8")
