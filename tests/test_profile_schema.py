# =============================================================================
# COMMS Service -- Strict versioned profile schema (F1.1)
# =============================================================================
# What app/profile/loader.py promises since F1.1, by input:
#   - the schema version: declared, exactly the current one; without
#     one, older, newer, not an integer -> three distinct refusals
#     plus the form one, each naming both versions; a wrong version is
#     reported alone;
#   - the top level and the type record are CLOSED: an unknown key
#     refuses by name with the allowed names; x- keys are tolerated,
#     fenced and dropped;
#   - every field has a fixed type (enumerations, lists from
#     enumerations, integers, a duration literal); no field takes an
#     expression;
#   - defaults are layered and explainable programmatically; the retry
#     defaults are a reference to settings, not a copy;
#   - routes are cross-checked against the deploy's channel states;
#   - every violation is reported in ONE refusal, in a fixed order;
#   - keys declared twice in YAML refuse instead of the last one
#     silently winning.
#
# THREE DOUBLE AXES per input: REPEAT, EMPTY, SHORTFALL -- see the
# class docstrings. Nothing here touches the database.
# =============================================================================

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from app.core.channels import ChannelState
from app.core.config import Settings, settings
from app.core.constants import MAX_BODY_LEN, MSG_TYPE_KEYS
from app.core.exceptions import ProfileError
from app.profile import loader as loader_module
from app.profile.loader import (
    CHANNEL_NAMES,
    LANES,
    PUSH_ON,
    RECORD_FIELDS,
    SCHEMA_VERSION,
    FileProfileSource,
    RawProfile,
    install_profile,
    install_profile_from_settings,
    load_profile,
    parse_profile,
)
from app.profile.registry import Layer, ProfileRegistry
from tests.helpers import configure_every_channel

REPO_ROOT = Path(__file__).resolve().parents[1]

# The chat-baseline types every valid profile must carry.
_MSG_TYPES: dict[str, Any] = {
    key: {"category": "msg_cat"} for key in MSG_TYPE_KEYS
}

# A deploy with only in_app live (what the suite runs with).
_ONLY_IN_APP = {
    "in_app": ChannelState.LIVE,
    "telegram": ChannelState.NOT_CONFIGURED,
    "email": ChannelState.NOT_CONFIGURED,
}
_ALL_LIVE = dict.fromkeys(CHANNEL_NAMES, ChannelState.LIVE)


def _doc(types: dict[str, Any] | None = None, **top: Any) -> dict[str, Any]:
    """A version-2 types document with the chat baseline added."""
    doc: dict[str, Any] = {"version": SCHEMA_VERSION}
    doc["types"] = {**_MSG_TYPES, **(types or {})}
    doc.update(top)
    return doc


def _parse(doc: Any, states: dict[str, ChannelState] | None = None) -> Any:
    return parse_profile(RawProfile(types=doc), states)


def _refusal(doc: Any, states: dict[str, ChannelState] | None = None) -> str:
    with pytest.raises(ProfileError) as excinfo:
        _parse(doc, states)
    return str(excinfo.value)


def _write(root: Path, types_yaml: str, **templates: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "types.yaml").write_text(types_yaml, encoding="utf-8")
    (root / "templates").mkdir(exist_ok=True)
    for locale, content in templates.items():
        (root / "templates" / f"{locale}.yaml").write_text(
            content, encoding="utf-8",
        )
    return root


_MSG_YAML = "".join(
    f"  {key}:\n    category: msg_cat\n" for key in MSG_TYPE_KEYS
)


# -----------------------------------------------------------------------------
# Item 1 -- the schema version
# -----------------------------------------------------------------------------


class TestSchemaVersion:
    """REPEAT: `version` twice (TestDuplicateKeys). EMPTY: an empty
    file, `version:` with no value. SHORTFALL: no `version` key."""

    def test_current_version_loads(self) -> None:
        assert set(_parse(_doc()).types) == set(MSG_TYPE_KEYS)

    def test_no_version_names_the_required_one(self) -> None:
        doc = _doc()
        del doc["version"]
        message = _refusal(doc)
        assert "declares no schema version (declared: none)" in message
        assert "requires `version: 2`" in message

    def test_older_version_names_both(self) -> None:
        message = _refusal({**_doc(), "version": 1})
        assert "declares schema version 1, which is older" in message
        assert "requires `version: 2`" in message

    def test_newer_version_names_both(self) -> None:
        message = _refusal({**_doc(), "version": 3})
        assert "declares schema version 3, which is newer" in message
        assert "requires `version: 2`" in message

    def test_the_three_version_refusals_differ(self) -> None:
        """Three states, three texts -- none is a copy of another."""
        missing = _doc()
        del missing["version"]
        texts = {
            _refusal(missing),
            _refusal({**_doc(), "version": 1}),
            _refusal({**_doc(), "version": 3}),
        }
        assert len(texts) == 3

    @pytest.mark.parametrize(
        "declared", ["2", 2.0, True, None, [2], "v2"],
    )
    def test_non_integer_version_refused(self, declared: Any) -> None:
        """`true` is an int in Python and 2.0 == 2: both must still be
        refused, or a bool or a float would pass for the version."""
        message = _refusal({**_doc(), "version": declared})
        assert "schema version must be an integer" in message
        assert f"(declared: {declared!r})" in message
        assert "requires `version: 2`" in message

    def test_empty_file_refused_for_no_version(self, tmp_path: Path) -> None:
        root = _write(tmp_path, "# nothing yet\n")
        with pytest.raises(ProfileError, match="empty: it declares no schema"):
            load_profile(FileProfileSource(root))

    def test_version_one_document_names_the_migration(self) -> None:
        """The pre-F1.1 flat document (type keys at the top level) is
        refused for its missing version, with the move spelled out --
        not judged key by key as if it were version 2."""
        message = _refusal(dict(_MSG_TYPES))
        assert "version-1 format" in message
        assert "move them under `types:`" in message
        assert "unknown top-level key" not in message

    def test_wrong_version_is_reported_alone(self) -> None:
        """The short-circuit (item 6 exception): with a wrong version,
        the unknown key next to it is NOT reported."""
        doc = {**_doc({"t": {"chanels": ["in_app"]}}), "version": 3}
        doc["typse"] = {}
        message = _refusal(doc)
        assert "newer" in message
        assert "typse" not in message
        assert "chanels" not in message

    def test_no_branch_accepts_another_version(self) -> None:
        """Every integer but the current one refuses."""
        for declared in (-1, 0, 1, 3, 10):
            with pytest.raises(ProfileError):
                _parse({**_doc(), "version": declared})


# -----------------------------------------------------------------------------
# Item 3 -- unknown keys refuse; x- is tolerated
# -----------------------------------------------------------------------------


class TestClosedKeys:
    """REPEAT: `x-a` twice (TestDuplicateKeys). EMPTY: `x-` with no
    tail. SHORTFALL: no `types` section at all."""

    def test_misspelt_field_refused_by_name(self) -> None:
        message = _refusal(_doc({"t": {"chanels": ["in_app"]}}))
        assert "type 't': unknown field 'chanels'" in message
        assert "expected one of: " + ", ".join(RECORD_FIELDS) in message

    def test_misspelt_field_does_not_fall_back_to_default(self) -> None:
        """The very defect §9.4 names: the typo must not load and route
        by the default channels."""
        with pytest.raises(ProfileError):
            _parse(_doc({"t": {"chanels": ["telegram"]}}), _ALL_LIVE)

    def test_unknown_top_level_key_refused(self) -> None:
        message = _refusal(_doc(typse={}))
        assert "unknown top-level key 'typse'" in message
        assert "expected only: types, version" in message

    def test_non_string_keys_refused(self) -> None:
        """YAML `1:` is an int key -- never a field name, never `version`."""
        doc = _doc({"t": {1: "x"}})
        doc[7] = 1
        message = _refusal(doc)
        assert "type 't': unknown field 1;" in message
        assert "unknown top-level key 7;" in message

    def test_extension_keys_tolerated_and_inert(self) -> None:
        """An x- key at both levels loads, and the record it sits in is
        identical to the record without it."""
        plain = _parse(_doc({"t": {"lane": "bulk"}}))
        extended = _parse(_doc(
            {"t": {"lane": "bulk", "x-anything": {"a": [1, 2]}}},
            **{"x-meta": "note"},
        ))
        assert extended.types["t"] == plain.types["t"]
        assert "x-anything" not in extended.types["t"].fields

    @pytest.mark.parametrize("key", ["x-", "X-foo", "x_foo", "xfoo"])
    def test_near_extension_keys_refused(self, key: str) -> None:
        message = _refusal(_doc({"t": {key: 1}}))
        assert f"unknown field {key!r}" in message

    def test_extension_values_are_still_fenced(self) -> None:
        message = _refusal(_doc(**{"x-link": "see https://example.com"}))
        assert "x-link contains an external domain literal" in message

    def test_missing_types_section_refused(self) -> None:
        message = _refusal({"version": SCHEMA_VERSION})
        assert "no `types` section" in message

    def test_storage_class_is_not_a_field(self) -> None:
        """Mandatory fix 1: the field arrives with answer storage."""
        assert "storage_class" not in RECORD_FIELDS
        message = _refusal(_doc({"t": {"storage_class": "hot"}}))
        assert "unknown field 'storage_class'" in message


# -----------------------------------------------------------------------------
# Item 2 -- every field has a fixed type
# -----------------------------------------------------------------------------


class TestFieldTypes:
    """Per field -- REPEAT: a channel listed twice. EMPTY: [], "", 0,
    null. SHORTFALL: a value outside the closed set, a name that is not
    a channel."""

    def test_the_field_set_is_exactly_this(self) -> None:
        assert RECORD_FIELDS == (
            "category",
            "channels",
            "expires_after",
            "lane",
            "max_body_chars",
            "push_on",
            "retry_backoff_seconds",
            "retry_max_attempts",
        )

    def test_every_field_declared_lands_on_the_profile_layer(self) -> None:
        record = _parse(_doc({"t": {
            "category": "c",
            "channels": ["in_app", "telegram"],
            "lane": "critical",
            "push_on": "outcome_and_deferral",
            "expires_after": "7d",
            "retry_max_attempts": 5,
            "retry_backoff_seconds": 0,
            "max_body_chars": 100,
        }})).types["t"]
        assert record.value("channels") == ("in_app", "telegram")
        assert record.value("expires_after") == timedelta(days=7)
        assert record.value("retry_backoff_seconds") == 0
        assert {d.layer for d in record.fields.values()} == {Layer.PROFILE}

    @pytest.mark.parametrize("field", [f for f in RECORD_FIELDS if f != "category"])
    @pytest.mark.parametrize(
        "expression",
        ["$.amount", "amount > 1000 ? telegram : email", "{% if x %}a{% endif %}"],
    )
    def test_no_field_takes_an_expression(
        self, field: str, expression: str,
    ) -> None:
        """No field accepts a string that could be evaluated. The one
        free string, `category`, is an opaque key compared for
        equality and never interpreted (see the next test)."""
        with pytest.raises(ProfileError, match=f"`{field}` found"):
            _parse(_doc({"t": {field: expression}}))

    def test_category_is_opaque(self) -> None:
        record = _parse(_doc({"t": {"category": "$.amount"}})).types["t"]
        assert record.value("category") == "$.amount"

    def test_lane_outside_the_set_lists_the_set(self) -> None:
        message = _refusal(_doc({"t": {"lane": "fast"}}))
        assert "`lane` found 'fast' (str); expected one of: " in message
        assert "interactive, critical, normal, bulk" in message
        assert LANES == ("interactive", "critical", "normal", "bulk")

    @pytest.mark.parametrize("value", ["Normal", "", None, 1, ["normal"]])
    def test_lane_forms_refused(self, value: Any) -> None:
        with pytest.raises(ProfileError, match="`lane` found"):
            _parse(_doc({"t": {"lane": value}}))

    def test_push_on_is_one_of_three(self) -> None:
        assert PUSH_ON == ("outcome", "outcome_and_deferral", "none")
        message = _refusal(_doc({"t": {"push_on": "deferral"}}))
        assert "expected one of: outcome, outcome_and_deferral, none" in message

    @pytest.mark.parametrize("value", [["outcome"], [], "", None])
    def test_push_on_is_not_a_list(self, value: Any) -> None:
        """Mandatory fix 4: a closed set of three declarations, not a
        subset -- and not a subset of the channels."""
        with pytest.raises(ProfileError, match="`push_on` found"):
            _parse(_doc({"t": {"push_on": value}}))

    def test_push_on_is_independent_of_channels(self) -> None:
        """No "subset of channels" check in any form: push_on loads
        whatever the channels are."""
        for value in PUSH_ON:
            record = _parse(
                _doc({"t": {"channels": ["email"], "push_on": value}}),
            ).types["t"]
            assert record.value("push_on") == value

    @pytest.mark.parametrize(
        ("value", "complaint"),
        [
            ([], "found an empty list"),
            (["in_app", "in_app"], "'in_app' is listed twice"),
            (["telgram"], "'telgram' (str) is not a channel"),
            ("in_app", "found 'in_app' (str)"),
            ([None], "an empty value (null) is not a channel"),
            (None, "found an empty value (null)"),
        ],
    )
    def test_channels_forms(self, value: Any, complaint: str) -> None:
        message = _refusal(_doc({"t": {"channels": value}}))
        assert complaint in message
        assert "from: email, in_app, telegram" in message

    @pytest.mark.parametrize(
        ("literal", "expected"),
        [("30s", timedelta(seconds=30)), ("15m", timedelta(minutes=15)),
         ("24h", timedelta(hours=24)), ("1d", timedelta(days=1))],
    )
    def test_duration_literals(self, literal: str, expected: timedelta) -> None:
        record = _parse(_doc({"t": {"expires_after": literal}})).types["t"]
        assert record.value("expires_after") == expected

    @pytest.mark.parametrize(
        "value", ["15", 900, "0m", "-5m", "1h30m", "1.5h", "15 m", "15M", "", None],
    )
    def test_duration_forms_refused(self, value: Any) -> None:
        message = _refusal(_doc({"t": {"expires_after": value}}))
        assert "expected a duration literal" in message

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("retry_max_attempts", 0),
            ("retry_max_attempts", True),
            ("retry_max_attempts", "3"),
            ("retry_max_attempts", 2.0),
            ("retry_backoff_seconds", -1),
            ("retry_backoff_seconds", False),
            ("max_body_chars", 0),
            ("max_body_chars", MAX_BODY_LEN + 1),
            ("max_body_chars", None),
        ],
    )
    def test_integer_forms_refused(self, field: str, value: Any) -> None:
        message = _refusal(_doc({"t": {field: value}}))
        assert f"`{field}` found" in message
        assert "expected an integer" in message

    def test_integer_boundaries_pass(self) -> None:
        record = _parse(_doc({"t": {
            "retry_max_attempts": 1,
            "retry_backoff_seconds": 0,
            "max_body_chars": MAX_BODY_LEN,
        }})).types["t"]
        assert record.value("max_body_chars") == MAX_BODY_LEN

    @pytest.mark.parametrize("category", ["", 5, None, "c" * 51])
    def test_category_forms_refused(self, category: Any) -> None:
        with pytest.raises(ProfileError, match="`category` found"):
            _parse(_doc({"t": {"category": category}}))

    def test_record_that_is_not_a_mapping_refused(self) -> None:
        message = _refusal(_doc({"t": ["in_app"]}))
        assert "type 't': the record must be a mapping" in message


# -----------------------------------------------------------------------------
# Item 5 -- layered defaults, explainable
# -----------------------------------------------------------------------------


class TestLayers:
    """REPEAT: the same field declared in two types keeps two answers.
    EMPTY: a bare key -> every field from the default layer.
    SHORTFALL: an unknown type or field -> LookupError, not "default"."""

    def _installed(self, types: dict[str, Any]) -> ProfileRegistry:
        target = ProfileRegistry()
        install_profile(_parse(_doc(types)), target)
        return target

    def test_bare_key_takes_every_default(self) -> None:
        target = self._installed({"t": None})
        for field in RECORD_FIELDS:
            assert target.explain("t", field).layer is Layer.DEFAULT

    def test_lane_default_is_normal_and_says_why(self) -> None:
        decided = self._installed({"t": {}}).explain("t", "lane")
        assert decided.value == "normal"
        assert decided.layer is Layer.DEFAULT
        assert decided.source == "comms default: lane not declared"

    def test_channels_default_is_in_app(self) -> None:
        decided = self._installed({"t": {}}).explain("t", "channels")
        assert decided.value == ("in_app",)
        assert decided.layer is Layer.DEFAULT

    def test_overridden_field_answers_profile(self) -> None:
        target = self._installed({"t": {"lane": "bulk"}, "u": {}})
        decided = target.explain("t", "lane")
        assert (decided.value, decided.layer) == ("bulk", Layer.PROFILE)
        assert decided.source == "types.yaml: type 't'.lane"
        # The neighbour keeps its own answer (REPEAT across types).
        assert target.explain("u", "lane").layer is Layer.DEFAULT

    def test_retry_defaults_are_a_reference_to_settings(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Mandatory fix 5: the value follows the setting and names it
        as the source -- there is no second number to drift."""
        monkeypatch.setattr(settings, "notification_max_delivery_attempts", 7)
        monkeypatch.setattr(
            settings, "notification_retry_backoff_base_seconds", 11,
        )
        target = self._installed({"t": {}})
        attempts = target.explain("t", "retry_max_attempts")
        backoff = target.explain("t", "retry_backoff_seconds")
        assert (attempts.value, attempts.layer) == (7, Layer.DEFAULT)
        assert attempts.source == "settings: NOTIFICATION_MAX_DELIVERY_ATTEMPTS"
        assert backoff.value == 11
        assert backoff.source == (
            "settings: NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS"
        )

    def test_unknown_type_is_not_explained_as_default(self) -> None:
        with pytest.raises(LookupError, match="'ghost' has no profile record"):
            self._installed({}).explain("ghost", "lane")

    def test_unknown_field_is_not_explained_as_default(self) -> None:
        with pytest.raises(LookupError, match="'chanels' is not a field"):
            self._installed({"t": {}}).explain("t", "chanels")

    def test_type_registered_without_a_profile_has_no_record(self) -> None:
        target = ProfileRegistry()
        target.register_type("bare")
        assert target.record_of("bare") is None
        with pytest.raises(LookupError):
            target.explain("bare", "lane")

    def test_reset_clears_records(self) -> None:
        target = self._installed({"t": {}})
        target.reset()
        assert target.record_of("t") is None


# -----------------------------------------------------------------------------
# Item 4 -- routes against the deploy
# -----------------------------------------------------------------------------


class TestRoutes:
    """REPEAT: two types into one missing channel -> two lines. EMPTY:
    a channel whose keys are all empty (not_configured). SHORTFALL: a
    partial key set -- refused by Settings, before the loader."""

    def test_route_outside_the_registry_refused(self) -> None:
        message = _refusal(_doc({"t": {"channels": ["sms"]}}), _ALL_LIVE)
        assert "type 't': `channels` found ['sms']" in message
        assert "'sms' (str) is not a channel of this service" in message

    def test_route_into_not_configured_channel_refused(self) -> None:
        message = _refusal(
            _doc({"t": {"channels": ["in_app", "telegram"]}}), _ONLY_IN_APP,
        )
        assert "type 't' routes to channel 'telegram' (decided by: profile)" in (
            message
        )
        assert "every key of 'telegram' is empty on this deploy" in message
        assert "'in_app'" not in message.split("routes to channel")[1][:12]

    def test_in_app_with_an_empty_declared_key_set_passes(self) -> None:
        """in_app has no declared keys and is live by definition."""
        record = _parse(
            _doc({"t": {"channels": ["in_app"]}}), _ONLY_IN_APP,
        ).types["t"]
        assert record.value("channels") == ("in_app",)

    def test_default_route_passes_on_a_deploy_without_externals(self) -> None:
        profile = _parse(_doc({"t": {}}), _ONLY_IN_APP)
        assert profile.types["t"].value("channels") == ("in_app",)

    def test_live_channel_passes(self) -> None:
        live = {**_ONLY_IN_APP, "email": ChannelState.LIVE}
        record = _parse(_doc({"t": {"channels": ["email"]}}), live).types["t"]
        assert record.value("channels") == ("email",)

    def test_every_offending_route_is_named(self) -> None:
        message = _refusal(
            _doc({"a": {"channels": ["telegram"]}, "b": {"channels": ["email"]}}),
            _ONLY_IN_APP,
        )
        assert "(2 problems)" in message
        assert "type 'a' routes to channel 'telegram'" in message
        assert "type 'b' routes to channel 'email'" in message

    def test_partial_key_set_is_refused_before_the_loader(self) -> None:
        """The existing rule (app/core/channels.py via Settings) covers
        it; the loader has no second check. Settings refusing is what
        makes the state unreachable there."""
        with pytest.raises(ValueError, match="channel 'telegram' is misconfigured"):
            Settings(
                _env_file=None,
                app_env="production",
                database_url="postgresql+asyncpg://u:p@db/comms_unit",
                comms_service_token="unit-test-service-token",
                telegram_bot_token="123456:unit-test-bot-token",
                telegram_bot_url="",
            )

    def test_startup_path_cross_checks_the_real_deploy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """install_profile_from_settings reads this deploy's states:
        the suite blanks telegram, so a telegram route refuses; the
        same profile loads once telegram's keys are set."""
        root = _write(
            tmp_path,
            "version: 2\ntypes:\n" + _MSG_YAML
            + "  t:\n    channels: [telegram]\n",
        )
        monkeypatch.setattr(settings, "templates_dir", str(root))
        with pytest.raises(ProfileError, match="routes to channel 'telegram'"):
            install_profile_from_settings()
        monkeypatch.setattr(
            settings, "telegram_bot_token", "123456:unit-test-bot-token",
        )
        monkeypatch.setattr(
            settings, "telegram_bot_url", "https://t.me/unit_test_bot",
        )
        assert install_profile_from_settings() is True

    def test_route_check_reuses_the_channel_rule(self) -> None:
        """The deploy's states come from evaluate_channels itself, over
        the same keys the startup channel map reads."""
        from app.engine.formatters import channel_map

        states = loader_module.deploy_channel_states()
        assert {k: v.value for k, v in states.items()} == {
            k: v for k, v in channel_map(settings).items()
            if v != ChannelState.NOT_IMPLEMENTED.value
        }


# -----------------------------------------------------------------------------
# Item 6 -- every violation in one refusal, in a fixed order
# -----------------------------------------------------------------------------


class TestCollectedRefusal:
    """REPEAT: the same violation reached twice is listed once. EMPTY:
    no violation -> no refusal. SHORTFALL: violations of different
    checks (schema, baseline, routes, templates) share one refusal."""

    _THREE = {
        "a": {"lane": "fast"},
        "b": {"chanels": ["in_app"]},
        "c": {"retry_max_attempts": 0},
    }

    def test_three_violations_in_three_types_one_refusal(self) -> None:
        message = _refusal(_doc(self._THREE))
        assert "(3 problems)" in message
        assert "type 'a': `lane` found 'fast'" in message
        assert "type 'b': unknown field 'chanels'" in message
        assert "type 'c': `retry_max_attempts` found 0" in message

    def test_order_does_not_depend_on_the_source_order(self) -> None:
        forward = _refusal(_doc(self._THREE))
        backward = _refusal(_doc(dict(reversed(list(self._THREE.items())))))
        assert forward == backward
        lines = [line for line in forward.splitlines() if line.startswith("  - ")]
        assert lines == sorted(lines)

    def test_different_checks_share_one_refusal(self) -> None:
        doc = _doc({"t": {"lane": "fast", "channels": ["telegram"]}})
        del doc["types"]["msg.thread_closed"]
        templates = {"en": {"ghost": {"telegram": {"body": "x"}}}}
        with pytest.raises(ProfileError) as excinfo:
            parse_profile(RawProfile(types=doc, templates_by_locale=templates),
                          _ONLY_IN_APP)
        message = str(excinfo.value)
        assert "(4 problems)" in message
        assert "`lane` found 'fast'" in message
        assert "'msg.thread_closed' is not declared" in message
        assert "routes to channel 'telegram'" in message
        assert "template for type 'ghost'" in message

    def test_one_violation_says_problem(self) -> None:
        assert "(1 problem)" in _refusal(_doc({"t": {"lane": "fast"}}))

    def test_clean_profile_raises_nothing(self) -> None:
        assert "t" in _parse(_doc({"t": {"lane": "bulk"}}), _ALL_LIVE).types


# -----------------------------------------------------------------------------
# F8 -- keys declared twice in YAML
# -----------------------------------------------------------------------------


class TestDuplicateKeys:
    """REPEAT is the subject. EMPTY: a key repeated with an empty
    value. SHORTFALL: a merge override (`<<`) is not a repeat."""

    def test_type_declared_twice_refused(self, tmp_path: Path) -> None:
        root = _write(
            tmp_path,
            "version: 2\ntypes:\n" + _MSG_YAML
            + "  t:\n    lane: bulk\n  t:\n    lane: critical\n",
        )
        with pytest.raises(ProfileError) as excinfo:
            load_profile(FileProfileSource(root))
        message = str(excinfo.value)
        assert "types.yaml: key 't' at line" in message
        assert "declared a second time" in message

    def test_field_declared_twice_refused(self, tmp_path: Path) -> None:
        root = _write(
            tmp_path,
            "version: 2\ntypes:\n" + _MSG_YAML
            + "  t:\n    category: a\n    category:\n",
        )
        with pytest.raises(ProfileError, match="key 'category' at line"):
            load_profile(FileProfileSource(root))

    def test_version_declared_twice_refused(self, tmp_path: Path) -> None:
        root = _write(tmp_path, "version: 1\nversion: 2\ntypes:\n" + _MSG_YAML)
        with pytest.raises(ProfileError, match="key 'version' at line 2"):
            load_profile(FileProfileSource(root))

    def test_duplicates_in_several_files_share_one_refusal(
        self, tmp_path: Path,
    ) -> None:
        root = _write(
            tmp_path,
            "version: 2\nx-a: 1\nx-a: 2\ntypes:\n" + _MSG_YAML,
            en="msg.thread_closed:\n  telegram:\n    body: a\n    body: b\n",
        )
        with pytest.raises(ProfileError) as excinfo:
            load_profile(FileProfileSource(root))
        message = str(excinfo.value)
        assert "(2 problems)" in message
        assert "types.yaml: key 'x-a'" in message
        assert "templates/en.yaml: key 'body'" in message

    def test_merge_override_is_not_a_duplicate(self, tmp_path: Path) -> None:
        root = _write(
            tmp_path,
            "version: 2\nx-base: &base\n  lane: bulk\n  push_on: none\n"
            "types:\n" + _MSG_YAML
            + "  t:\n    <<: *base\n    lane: critical\n",
        )
        record = load_profile(FileProfileSource(root)).types["t"]
        assert record.value("lane") == "critical"
        assert record.value("push_on") == "none"
        assert record.fields["push_on"].layer is Layer.PROFILE


# -----------------------------------------------------------------------------
# Templates -- type and channel keys are closed too
# -----------------------------------------------------------------------------


class TestTemplateKeys:
    """REPEAT: TestDuplicateKeys. EMPTY: an empty file (warning, as
    before). SHORTFALL: a template for an undeclared type."""

    def test_unknown_channel_key_refused(self) -> None:
        templates = {"en": {"msg.thread_closed": {"telegramm": {"body": "x"}}}}
        with pytest.raises(ProfileError) as excinfo:
            parse_profile(RawProfile(types=_doc(), templates_by_locale=templates))
        message = str(excinfo.value)
        assert "channel key 'telegramm' is not a channel" in message
        assert "expected one of: email, in_app, telegram" in message

    def test_known_channel_keys_pass(self) -> None:
        templates = {"en": {"msg.thread_closed": {
            name: {"body": "x"} for name in CHANNEL_NAMES
        }}}
        profile = parse_profile(
            RawProfile(types=_doc(), templates_by_locale=templates),
        )
        assert set(profile.templates["en"]["msg.thread_closed"]) == set(
            CHANNEL_NAMES,
        )


# -----------------------------------------------------------------------------
# The profiles in this repository pass the schema
# -----------------------------------------------------------------------------


class TestShippedProfiles:
    def test_smoke_profile_loads_on_a_deploy_without_externals(self) -> None:
        profile = load_profile(
            FileProfileSource(REPO_ROOT / "deploy" / "smoke-profile"),
            _ONLY_IN_APP,
        )
        assert set(profile.types) == set(MSG_TYPE_KEYS)

    def test_fixture_profile_loads_against_a_full_deploy(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Was ..._against_the_suite_deploy: the fixture routed nothing
        but in_app, so the suite's own deploy (externals blanked) could
        load it. Since F1.2 the fixture routes types to telegram and
        email -- the channel is the profile's -- so it is loaded on a
        deploy that has every channel. What it pinned still holds: the
        reference record is decided by the profile, field by field."""
        configure_every_channel(monkeypatch)
        profile = load_profile(
            FileProfileSource(REPO_ROOT / "tests" / "fixtures" / "profile"),
            loader_module.deploy_channel_states(),
        )
        record = profile.types["unit_routed"]
        layers = {name: d.layer for name, d in record.fields.items()}
        assert layers.pop("category") is Layer.DEFAULT
        assert set(layers.values()) == {Layer.PROFILE}


# -----------------------------------------------------------------------------
# Item 3 / item 7 -- what the loader's own text says
# -----------------------------------------------------------------------------


class TestLoaderText:
    """The header is read by reviewers, not only by people: it must
    carry the boundary, and no trace of the removed tolerance."""

    _SOURCE = (REPO_ROOT / "app" / "profile" / "loader.py").read_text(
        encoding="utf-8",
    )

    def test_no_additive_contract_statement(self) -> None:
        assert "additive" not in self._SOURCE.lower()
        # The pair: the strict rule IS stated (not an empty header).
        assert "refuses startup by name" in self._SOURCE

    def test_template_boundary_is_stated_with_the_reason(self) -> None:
        assert "TEMPLATES ARE A SUBSTITUTION LANGUAGE" in self._SOURCE
        assert "(the Jinja class, {% if %}) is REJECTED" in self._SOURCE
        assert "no conditions, no" in self._SOURCE

    def test_storage_class_line_is_present(self) -> None:
        assert "STORAGE CLASS is not a field yet" in self._SOURCE
