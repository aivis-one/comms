# =============================================================================
# D1 / R3 + R4 -- every number has a bound; the membership events are
# closed.
# =============================================================================
#
# MUTATIONS these tests were written against (each turns one red):
#   M7 a row removed from NUMERIC_BOUNDS -> test_every_int_field_has_a_bound
#   M8 a bound moved by one              -> TestEachBound (the twin at the
#                                           edge passes, one past refuses)
#   M9 _closed removed from either membership parser
#                                        -> TestClosedMembershipEvents
# =============================================================================

import json
from typing import Any
from uuid import uuid4

import pytest

from app.core.config import NUMERIC_BOUNDS, Settings, settings
from app.core.exceptions import ValidationError
from app.profile.loader import _FIELDS
from app.transport.events import (
    GroupChanged,
    SectionMembershipChanged,
    parse_event,
)

_FULL_OK = {
    "app_env": "production",
    "database_url": "postgresql+asyncpg://u:p@db/comms_unit",
    "telegram_bot_token": "123456:test-token",
    "telegram_bot_url": "https://t.me/unit_test_bot",
    "comms_service_token": "unit-test-service-token",
}

# The partner a value at an edge needs so that the ORDER checks stay
# satisfied: testing one bound must not trip a different rule.
_PARTNER_AT = {
    "notification_poll_interval_seconds": (
        "notification_max_backoff_seconds", "hi",
    ),
    "notification_max_backoff_seconds": (
        "notification_poll_interval_seconds", "lo",
    ),
    "notification_retry_backoff_base_seconds": (
        "notification_retry_backoff_max_seconds", "hi",
    ),
    "notification_retry_backoff_max_seconds": (
        "notification_retry_backoff_base_seconds", "lo",
    ),
}


def _settings(**overrides: Any) -> Settings:
    kwargs: dict[str, Any] = {"_env_file": None, **_FULL_OK, **overrides}
    return Settings(**kwargs)


def _at(name: str, value: int) -> dict[str, int]:
    overrides = {name: value}
    if name in _PARTNER_AT:
        partner, edge = _PARTNER_AT[name]
        bound = NUMERIC_BOUNDS[partner]
        overrides[partner] = bound.hi if edge == "hi" else bound.lo
    return overrides


class TestTheTable:
    def test_every_int_field_has_a_bound(self) -> None:
        """No numeric knob without a bound -- and no row for a knob that
        does not exist. M7. The pair: the set is not empty. 17 since
        P3-1 added the push stream's cap (changes_stream_maxlen)."""
        ints = {
            name
            for name, field in Settings.model_fields.items()
            if field.annotation is int
        }
        assert len(ints) == 17, (
            "17 numeric parameters: D1's 15, T12's, P3-1's"
        )
        assert ints == set(NUMERIC_BOUNDS)

    def test_the_ranges_are_the_published_ones(self) -> None:
        """The ranges, written out: the list the owner checks the boxes'
        .env against before the merge (D1 report). Pinned HERE, not read
        from the table -- the twins below take their edges from the
        table, so a wrong number in it would move them along (M8)."""
        assert {
            name: (bound.lo, bound.hi) for name, bound in NUMERIC_BOUNDS.items()
        } == {
            "consumer_batch_size": (1, 1000),
            "consumer_block_ms": (1, 60_000),
            "dlq_maxlen": (1, 10_000_000),
            "changes_stream_maxlen": (1, 10_000_000),
            "notification_poll_interval_seconds": (1, 3600),
            "notification_max_backoff_seconds": (1, 3600),
            "notification_max_delivery_attempts": (1, 100),
            "notification_max_pipeline_attempts": (1, 100),
            "notification_batch_size": (1, 1000),
            "notification_retry_backoff_base_seconds": (0, 86_400),
            "notification_retry_backoff_max_seconds": (0, 86_400),
            "notification_max_retry_after_seconds": (1, 86_400),
            "notification_max_rate_limit_deferrals": (0, 1000),
            "notification_retention_days": (0, 3650),
            "notification_retention_interval_seconds": (1, 86_400),
            "thread_auto_close_days": (0, 3650),
            "thread_auto_close_interval_seconds": (1, 86_400),
        }

    def test_every_row_has_an_order_and_a_reason(self) -> None:
        for name, bound in NUMERIC_BOUNDS.items():
            assert bound.lo <= bound.hi, name
            assert bound.why.strip(), name

    def test_the_defaults_are_inside_their_bounds(self) -> None:
        settings = _settings()
        for name, bound in NUMERIC_BOUNDS.items():
            assert bound.lo <= getattr(settings, name) <= bound.hi, name


class TestEachBound:
    @pytest.mark.parametrize("name", sorted(NUMERIC_BOUNDS))
    def test_on_the_bound_passes(self, name: str) -> None:
        bound = NUMERIC_BOUNDS[name]
        assert getattr(_settings(**_at(name, bound.lo)), name) == bound.lo
        assert getattr(_settings(**_at(name, bound.hi)), name) == bound.hi

    @pytest.mark.parametrize("name", sorted(NUMERIC_BOUNDS))
    def test_past_the_bound_refuses_startup_naming_the_variable(
        self, name: str,
    ) -> None:
        bound = NUMERIC_BOUNDS[name]
        for value in (bound.lo - 1, bound.hi + 1):
            with pytest.raises(ValueError) as caught:
                _settings(**_at(name, value))
            message = str(caught.value)
            assert f"{name.upper()}={value} is outside" in message


class TestNamedCases:
    def test_zero_delivery_attempts_refuses_startup_by_name(self) -> None:
        """done-when (2)."""
        with pytest.raises(ValueError, match="NOTIFICATION_MAX_DELIVERY_ATTEMPTS=0"):
            _settings(notification_max_delivery_attempts=0)

    def test_a_negative_retention_is_a_refusal_not_disabled(self) -> None:
        """The one value that used to be accepted and now refuses: "off"
        is 0 (the variable list in the D1 report)."""
        with pytest.raises(ValueError, match="NOTIFICATION_RETENTION_DAYS=-1"):
            _settings(notification_retention_days=-1)
        assert _settings(notification_retention_days=0).notification_retention_days == 0

    def test_order_is_checked(self) -> None:
        with pytest.raises(
            ValueError,
            match="NOTIFICATION_MAX_BACKOFF_SECONDS=4 is below "
            "NOTIFICATION_POLL_INTERVAL_SECONDS=5",
        ):
            _settings(notification_max_backoff_seconds=4)
        with pytest.raises(
            ValueError,
            match="NOTIFICATION_RETRY_BACKOFF_MAX_SECONDS=29 is below",
        ):
            _settings(notification_retry_backoff_max_seconds=29)

    def test_every_violation_is_named_in_one_pass(self) -> None:
        with pytest.raises(ValueError) as caught:
            _settings(notification_batch_size=0, dlq_maxlen=0)
        message = str(caught.value)
        assert "NOTIFICATION_BATCH_SIZE=0" in message
        assert "DLQ_MAXLEN=0" in message


class TestTheProfileDefaultLayer:
    """done-when: the default layer cannot hand out a value that would be
    refused if the profile declared it."""

    @pytest.mark.parametrize(
        ("field", "setting"),
        [
            ("retry_max_attempts", "notification_max_delivery_attempts"),
            ("retry_backoff_seconds", "notification_retry_backoff_base_seconds"),
        ],
    )
    def test_every_value_the_setting_can_take_the_profile_accepts(
        self, field: str, setting: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Since H1 a declared backoff base must also not exceed the
        deploy's cap (NOTIFICATION_RETRY_BACKOFF_MAX_SECONDS) -- and the
        SETTING can reach its own top only when the cap is there too
        (the ordered pair base <= max). So the cap is raised to its top
        here, the one deploy in which the setting takes bound.hi; the
        claim -- the default layer hands out nothing a declaration would
        be refused for -- is unchanged. Since H1 the profile's range
        also has the setting's top: one above it is refused."""
        monkeypatch.setattr(
            settings, "notification_retry_backoff_max_seconds",
            NUMERIC_BOUNDS["notification_retry_backoff_max_seconds"].hi,
        )
        bound = NUMERIC_BOUNDS[setting]
        check = _FIELDS[field].check
        assert check(bound.lo) is None
        assert check(bound.hi) is None
        # The pair: the check is not vacuous -- one below is refused,
        # and one above.
        assert check(bound.lo - 1) is not None
        assert check(bound.hi + 1) is not None


def _event(event: str, data: dict[str, Any]) -> Any:
    return parse_event({"event": event, "data": json.dumps({"v": 1, **data})})


_GROUP = {"group_key": "g1", "recipient_id": str(uuid4()), "member": True}
_SECTION = {
    "section_key": "support", "section_label": "Support",
    "operator_id": str(uuid4()), "member": True,
}


class TestClosedMembershipEvents:
    """R4 done-when (1), in BOTH events. M9."""

    @pytest.mark.parametrize(
        ("event", "data"),
        [("group_changed", _GROUP), ("section_membership_changed", _SECTION)],
    )
    def test_an_extra_field_refuses_naming_it(
        self, event: str, data: dict[str, Any],
    ) -> None:
        with pytest.raises(ValidationError) as caught:
            _event(event, {**data, "zq7extra": 1})
        message = str(caught.value)
        assert "unknown field(s) 'zq7extra'" in message
        assert message.startswith(f"{event}:")

    def test_the_regular_sets_are_accepted(self) -> None:
        """The pair: the exact field sets the products send (D1 report,
        impact table) parse."""
        assert isinstance(_event("group_changed", _GROUP), GroupChanged)
        assert isinstance(
            _event("section_membership_changed", _SECTION),
            SectionMembershipChanged,
        )

    @pytest.mark.parametrize(
        ("event", "data", "missing"),
        [
            ("group_changed", _GROUP, "member"),
            ("section_membership_changed", _SECTION, "section_label"),
        ],
    )
    def test_a_missing_field_still_refuses(
        self, event: str, data: dict[str, Any], missing: str,
    ) -> None:
        """Closing the set did not weaken the required fields."""
        short = {k: v for k, v in data.items() if k != missing}
        with pytest.raises(ValidationError, match=missing):
            _event(event, short)
