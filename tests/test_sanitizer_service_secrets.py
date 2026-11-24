# =============================================================================
# R-4 -- every secret of the service is a name the sanitizer knows
# =============================================================================
#
# SEC-1 held the repr of the settings to the secrets read from DATA
# (tests/test_settings_secrets.py); the sanitizer kept a list written
# from memory, and two secrets of the service passed it whole:
#   - COMMS_SERVICE_TOKEN_NEXT -- its name ends in `next`, not in a
#     credential word;
#   - LLM_KEY_SETS -- its name ends in `sets`, and its value is a JSON
#     object of sets whose `key` is any string the operator was given,
#     not only the `sk-` shape; even under a known name, an unquoted
#     value was cut at its first `,` `]` or `}`, and the JSON after it
#     stayed.
# Here:
#   - TestTheTwoForms: both secrets, in every form the current token is
#     hidden in, and the value of LLM_KEY_SETS as the environment holds it
#     (app/core/channels.py parse_llm_key_sets: an object of sets);
#   - TestTheGuard: every secret of the instance, by the data SEC-1 reads,
#     written as NAME=<value>, comes out without its value -- a new secret
#     whose name the sanitizer does not know is red without an edit here;
#   - TestNotASecret: names and fields that only look like one stay whole.
#
# MUTATIONS these tests were written against (each turns the test
# named red):
#   M1  comms_service_token_next out of _KEY        -> TestTheTwoForms,
#                                                      TestTheGuard
#   M2  llm_key_sets out of _KEY                    -> TestTheTwoForms,
#                                                      TestTheGuard
#   M3  no branch for a value opening with [ or {   -> TestTheTwoForms
#   M4  no _JSON_KEY_RE pass                        -> TestTheTwoForms
#   M5  _JSON_KEY_RE with no quote before `key`     -> TestNotASecret
#   M6  any name segment after a credential word    -> TestNotASecret
#   M8  a new `asked` key in primer.yaml (a copy)   -> TestTheGuard
#   M9  the [ { branch before [redacted]            -> TestTheTwoForms
#   M10 the guard's mark in a shape a pattern knows -> TestTheGuard
# (M7, the key's prefix quadratic: tests/test_sanitizer_hardening.py.)
# =============================================================================

import io
import json
from typing import Any

import pytest
import structlog

from app.core.channels import parse_llm_key_sets
from app.core.logging import processors
from app.core.sanitize import is_secret_name, sanitize_text
from tests.test_primer_yaml import _ENV, COMPOSED
from tests.test_settings_secrets import SECRET_OUTSIDE_ENV, SECRET_SOURCES

# A value in a secret's place, glued at run time: no literal in the shape
# of a key (§0 §10), and no shape any pattern of the sanitizer knows.
_SECRET = "zq" + "Rfour" + "s9"


def _once(text: str) -> str:
    """sanitize_text's output, checked idempotent on the way."""
    once = sanitize_text(text)
    assert sanitize_text(once) == once
    return once


def _logged(**fields: Any) -> str:
    """One event with `fields`, through the real chain, rendered as JSON."""
    stream = io.StringIO()
    logger = structlog.wrap_logger(
        structlog.PrintLogger(file=stream),
        processors=processors(structlog.processors.JSONRenderer()),
    )
    logger.info("r4_probe", **fields)
    return stream.getvalue()


def _sets(*keys: str) -> str:
    """LLM_KEY_SETS as the environment holds it: one object, a set by its
    name, each with its key (not of the sk- shape)."""
    return json.dumps({
        f"llm.s{n}": {
            "endpoint": "https://llm.example.org/v1", "gateway": False,
            "key": key, "model": "unit-model",
        }
        for n, key in enumerate(keys)
    })


# =============================================================================
# item 1 -- the two forms
# =============================================================================

_TOKEN_FORMS = [
    "{n}={v}",
    "{n}: {v}",
    "export {n}={v} now",
    '"{n}": "{v}"',
    "'{n}': '{v}'",
    "GET /x?{n}={v}&b=2",
]


class TestTheTwoForms:
    @pytest.mark.parametrize("form", _TOKEN_FORMS)
    @pytest.mark.parametrize(
        "name", ["COMMS_SERVICE_TOKEN", "COMMS_SERVICE_TOKEN_NEXT",
                 "comms_service_token_next"],
    )
    def test_the_next_token_as_the_current_one(self, name: str, form: str) -> None:
        """The current token is the pair: the form is one the sanitizer
        hides at all."""
        cleaned = _once(form.format(n=name, v=_SECRET))
        assert _SECRET not in cleaned
        assert name in cleaned and "[redacted]" in cleaned

    @pytest.mark.parametrize(
        "name", ["COMMS_SERVICE_TOKEN_NEXT", "LLM_KEY_SETS", "llm_key_sets"],
    )
    def test_a_field_of_the_log_by_its_name(self, name: str) -> None:
        assert is_secret_name(name)
        line = json.loads(_logged(**{name: _SECRET}))
        assert line[name] == "[redacted]"

    def test_the_sets_as_the_environment_holds_them(self) -> None:
        value = _sets(_SECRET, _SECRET[::-1])
        assert parse_llm_key_sets(value)[1] == []  # the pair: a valid value
        cleaned = _once(f"LLM_KEY_SETS={value}\nnext line")
        assert cleaned == "LLM_KEY_SETS=[redacted]\nnext line"

    @pytest.mark.parametrize(("text", "cleaned"), [
        ("LLM_KEY_SETS='{v}' end", "LLM_KEY_SETS='[redacted]' end"),
        ("llm_key_sets: {v}", "llm_key_sets: [redacted]"),
        ("LLM_KEY_SETS={{}}", "LLM_KEY_SETS=[redacted]"),
    ])
    def test_the_sets_in_other_forms(self, text: str, cleaned: str) -> None:
        assert _once(text.format(v=_sets(_SECRET))) == cleaned

    def test_the_sets_as_a_json_field(self) -> None:
        text = json.dumps({"LLM_KEY_SETS": _sets(_SECRET), "n": 1})
        cleaned = _once(text)
        assert json.loads(cleaned) == {"LLM_KEY_SETS": "[redacted]", "n": 1}

    @pytest.mark.parametrize("text", [
        # The rows of the finding, as the reviewer wrote them.
        f'LLM_KEY_SETS=[{{"name":"a","key":"{_SECRET}"}}]',
        f'{{"name":"a","key":"{_SECRET}"}}',
        # A value the parse refuses -- the one that reaches a refusal:
        # the key not a JSON string, the name of the set the secret.
        f'LLM_KEY_SETS={{"llm.text": {{"key": {_SECRET}, "model": "m"}}}}',
        f'LLM_KEY_SETS={{"{_SECRET}": {{"model": "m"}}}}',
    ])
    def test_the_findings_rows(self, text: str) -> None:
        cleaned = _once(text)
        assert _SECRET not in cleaned
        assert "[redacted]" in cleaned

    def test_a_key_without_its_name(self) -> None:
        """A set's key wherever the JSON or a dict's repr stands without
        the variable's name; the rest of the structure stays."""
        value = json.loads(_sets(_SECRET, _SECRET[::-1]))
        cleaned = _once(json.dumps(value))
        assert _SECRET not in cleaned and _SECRET[::-1] not in cleaned
        assert json.loads(cleaned)["llm.s1"] == {
            "endpoint": "https://llm.example.org/v1", "gateway": False,
            "key": "[redacted]", "model": "unit-model",
        }
        cleaned = _once(repr(value))
        assert _SECRET not in cleaned and _SECRET[::-1] not in cleaned
        assert "'key': '[redacted]'" in cleaned

    @pytest.mark.parametrize(("text", "cleaned"), [
        (f'"key": "{_SECRET}', '"key": "[redacted]'),
        (f'"key": "{_SECRET}\n"x": 1', '"key": "[redacted]\n"x": 1'),
        ('"key": ""', '"key": "[redacted]"'),
        (f'LLM_KEY_SETS={{"a": {{"key": "{_SECRET}', "LLM_KEY_SETS=[redacted]"),
    ])
    def test_cut_short_and_empty(self, text: str, cleaned: str) -> None:
        assert _once(text) == cleaned

    def test_a_value_opening_with_a_bracket_under_any_name(self) -> None:
        """The class, not the one name: a value of JSON under a known name
        was cut at its first `,` and left the rest."""
        text = f'refused secret={{"a": 1, "b": "{_SECRET}"}}'
        assert _once(text) == "refused secret=[redacted]"
        text = f"token=[1, {_SECRET}]"
        assert _once(text) == "token=[redacted]"

    def test_a_redacted_value_is_not_extended(self) -> None:
        """[redacted] is the first branch: the text after it stays."""
        text = f"sent Authorization: Bearer {_SECRET} upstream"
        assert _once(text) == "sent Authorization=[redacted] upstream"
        assert _once("LLM_KEY_SETS=[redacted] then") == "LLM_KEY_SETS=[redacted] then"

    @pytest.mark.parametrize(
        "text", ["LLM_KEY_SETS=", "COMMS_SERVICE_TOKEN_NEXT=", "LLM_KEY_SETS= "],
    )
    def test_no_value_nothing_to_take(self, text: str) -> None:
        assert sanitize_text(text) == text


# =============================================================================
# item 2 -- the guard, from the data SEC-1 reads
# =============================================================================

# The secrets known at R-4: the rule must find at least these, or it
# finds nothing and every check below is green for nothing.
KNOWN_SECRET_NAMES = frozenset({
    "POSTGRES_PASSWORD", "COMMS_EVENTS_PASSWORD", "COMMS_SERVICE_TOKEN",
    "TELEGRAM_BOT_TOKEN", "EMAIL_MAILGUN_API_KEY", "LLM_KEY_SETS",
    "TELEGRAM_LOGIN_CLIENT_SECRET", "DATABASE_URL", "REDIS_URL",
    "COMMS_SERVICE_TOKEN_NEXT",
})


def secret_names() -> set[str]:
    """Every variable of the instance whose value is a secret or holds
    one: the keys of primer.yaml `env` the operator or the machine gives
    (SECRET_SOURCES), the values compose builds from them (COMPOSED), and
    the secrets outside `env` (SECRET_OUTSIDE_ENV) -- whether or not comms
    reads them as a setting."""
    names = {key for key, source in _ENV.items()
             if source["source"] in SECRET_SOURCES}
    names |= {setting for setting, key in COMPOSED.items() if key in names}
    names |= set(SECRET_OUTSIDE_ENV)
    return names


def _mark(name: str) -> str:
    return "-".join(("r4", name.lower(), "mark"))


def _value(name: str) -> str:
    """The variable's value with its mark: a value compose builds is a URL
    holding the mark as its password; the sets are their JSON, the mark
    as a set's name and as its key; any other is the mark."""
    mark = _mark(name)
    if name in COMPOSED:
        return "".join(("db://comms:", mark, "@localhost:5432/comms"))
    if name == "LLM_KEY_SETS":
        return json.dumps({mark: {"endpoint": "https://llm.example.org/v1",
                                  "gateway": False, "key": mark, "model": "m"}})
    return mark


class TestTheGuard:
    def test_the_rule_finds_the_known_secrets(self) -> None:
        assert secret_names() >= KNOWN_SECRET_NAMES

    @pytest.mark.parametrize("name", sorted(secret_names()))
    def test_every_secret_goes(self, name: str) -> None:
        for text in (f"{name}={_value(name)}", f"{name.lower()}={_value(name)}"):
            cleaned = _once(text)
            assert _mark(name) not in cleaned, cleaned
            assert cleaned.startswith(text.split("=")[0])
        assert _mark(name) not in _logged(**{name: _value(name)})

    def test_the_pair_a_name_the_sanitizer_does_not_know(self) -> None:
        """The mark is no shape a pattern knows: a name the sanitizer does
        not know keeps it -- the checks above are its name's work."""
        name = "COMMS_WIDGET_SEED"
        assert sanitize_text(f"{name}={_mark(name)}") == f"{name}={_mark(name)}"


# =============================================================================
# item 4 -- what only looks like a secret stays
# =============================================================================


class TestNotASecret:
    @pytest.mark.parametrize("name", [
        "next_cursor", "keys", "idempotency_key", "correlation_key",
        "key_sets", "token_next", "token_status", "comms_service_token_next_at",
        "llm_key_sets_count", "llm_sets",
    ])
    def test_a_name(self, name: str) -> None:
        assert not is_secret_name(name)
        assert sanitize_text(f"{name}=v1, x") == f"{name}=v1, x"
        assert sanitize_text(f'"{name}": "v1"') == f'"{name}": "v1"'
        assert json.loads(_logged(**{name: "v1"}))[name] == "v1"

    def test_the_bare_key_as_a_name(self) -> None:
        """`key` alone names no secret: a field `key` of a log (a
        section's, app/messaging/sections.py) and key=... stay; only a
        quoted "key" with a string -- a set's key in JSON -- goes
        (TestTheTwoForms)."""
        assert not is_secret_name("key")
        assert sanitize_text("key=v1, x") == "key=v1, x"
        assert json.loads(_logged(key="v1"))["key"] == "v1"

    @pytest.mark.parametrize("text", [
        '{"idempotency_key": "k-1", "monkey": "m", "keys": "a"}',
        "{'key_id': 'k', 'keyword': 'w'}",
        '"key": 7',
        "GET /api/v1/notifications/by-key?key=k-1 HTTP/1.1",
        "COMMS_SERVICE_TOKEN_NEXT is set without COMMS_SERVICE_TOKEN",
        "LLM_KEY_SETS on this deploy does not define the set",
    ])
    def test_a_text(self, text: str) -> None:
        assert sanitize_text(text) == text

    def test_the_shape_the_parse_names(self) -> None:
        """The complaint that shows the form of a set, as the code builds
        it: `"key": ...` is no value."""
        complaint = parse_llm_key_sets("[]")[1][0]
        assert '"key": ...' in complaint
        assert sanitize_text(complaint) == complaint
