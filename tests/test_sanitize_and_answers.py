# =============================================================================
# A secret leaves the log by no field name and no tail; a provider's
# answer read for what it is (a redirect of the email API, a policy's 403
# at the model gateway, a pool that never sent)
# =============================================================================
#
#   - A field of a log event, or a key of a dict inside one, whose NAME is
#     a credential's (app/core/sanitize.py is_secret_name -- the key of
#     the key=value pattern, the whole name) shows [redacted] whatever
#     its value; the name stays. Through the real structlog chain.
#   - The key=value pattern runs before the shapes: a value one of them
#     would start ("sk-...", 48 hex) is taken whole, with no tail.
#   - A DSN password with "/" (base64) or "@" in it; secret_key=,
#     private_key:, credentials= -- each with its pair that is no secret.
#   - Email: a 3xx is the channel ("email_channel_not_viable", once).
#   - Llm: a 403 that declines the request on a policy (metadata.reasons,
#     metadata.patterns, a policy error_type) is this request's
#     (message_rejected); any other 403 the set's configuration.
#   - A PoolTimeout never reached the provider: not "may have arrived".
#
# MUTATIONS these tests were written against (each turns one red):
#   S1  field names not looked at                   -> TestFieldNames
#   S2  names looked at on the event only           -> TestFieldNames
#   S3  the shapes before the pair again            -> TestNoTail
#   S4  userinfo's password stops at "/" again      -> TestNewForms
#   S5  the new key roots gone                      -> TestNewForms
#   S6  an email redirect one message again         -> TestEmailRedirect
#   S7  every 403 the set's configuration           -> TestPolicy403
#   S8  a spent budget read as a policy             -> TestPolicy403
#   S9  "Pool" back among "may have arrived"        -> TestPoolTimeout
# =============================================================================

import io
import json
from typing import Any
from uuid import UUID

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.core.channels import LLMKeySet
from app.core.logging import processors
from app.core.sanitize import sanitize_text
from app.engine.constants import DeliveryChannel
from app.engine.formatters import (
    ConfigurationError,
    EmailFormatter,
    LLMFormatter,
    LLMTransientError,
    MessageRejectedError,
)
from app.engine.models import Notification, NotificationDelivery

_SECRET = "zq" + "REV4b" + "s7"


def _logged(**fields: Any) -> dict[str, Any]:
    stream = io.StringIO()
    logger = structlog.wrap_logger(
        structlog.PrintLogger(file=stream),
        processors=processors(structlog.processors.JSONRenderer()),
    )
    logger.info("rev4b_probe", **fields)
    line = stream.getvalue()
    parsed: dict[str, Any] = json.loads(line)
    return parsed


def _once(text: str) -> str:
    cleaned = sanitize_text(text)
    assert sanitize_text(cleaned) == cleaned
    return cleaned


class TestFieldNames:
    """Item 1. S1, S2."""

    def test_a_credential_name_hides_its_value(self) -> None:
        line = _logged(
            password=_SECRET,
            headers={"x-api-key": _SECRET, "Accept": "*/*"},
            config={"db": {"secret_key": _SECRET, "host": "db"}},
            credentials={"user": "u", "pass": _SECRET},
            Authorization=f"Basic {_SECRET}",
        )
        assert _SECRET not in json.dumps(line)
        assert line["password"] == "[redacted]"
        assert line["headers"] == {"x-api-key": "[redacted]", "Accept": "*/*"}
        assert line["config"] == {"db": {"secret_key": "[redacted]", "host": "db"}}
        assert line["credentials"] == "[redacted]"
        assert line["Authorization"] == "[redacted]"

    def test_the_pair_names_that_only_look_like_one(self) -> None:
        line = _logged(
            passwords_match=True, max_tokens=1000, token_count=3,
            idempotency_key="k-1", password_changed="yes",
        )
        assert (
            line["passwords_match"], line["max_tokens"], line["token_count"],
            line["idempotency_key"], line["password_changed"],
        ) == (True, 1000, 3, "k-1", "yes")


class TestNoTail:
    """Item 2. S3."""

    @pytest.mark.parametrize(("text", "cleaned"), [
        ("password=sk-" + "A" * 30 + ".tail-" + _SECRET, "password=[redacted]"),
        ("token=" + "a" * 48 + "-" + _SECRET, "token=[redacted]"),
        ('{"api_key": "sk-' + "B" * 24 + " " + _SECRET + '"}',
         '{"api_key": "[redacted]"}'),
        ("password=sk-" + "C" * 24 + " " + _SECRET + "; next",
         "password=[redacted]; next"),
    ])
    def test_the_value_goes_whole(self, text: str, cleaned: str) -> None:
        assert _once(text) == cleaned

    @pytest.mark.parametrize(("text", "cleaned"), [
        ("sk-" + "D" * 30, "[redacted]"),
        ("a" * 48, "[redacted]"),
        ("invalid Authorization: Bearer X rejected",
         "invalid Authorization=[redacted] rejected"),
        ("token: Bearer X and more", "token: [redacted]"),
    ])
    def test_the_pair_the_shapes_and_the_header_as_before(
        self, text: str, cleaned: str,
    ) -> None:
        assert _once(text) == cleaned


class TestNewForms:
    """Item 3. S4, S5."""

    @pytest.mark.parametrize(("text", "cleaned"), [
        ("postgresql://comms:a/b+" + _SECRET + "==@db/comms",
         "postgresql://comms:[redacted]@db/comms"),
        ("redis://:p@" + _SECRET + "@h:6379/0", "redis://:[redacted]@h:6379/0"),
        ("https://u:" + _SECRET + "@host/path?e=a@b.c",
         "https://u:[redacted]@host/path?e=a@b.c"),
        ("secret_key=" + _SECRET, "secret_key=[redacted]"),
        ("aws_secret_key=" + _SECRET, "aws_secret_key=[redacted]"),
        ("private_key: " + _SECRET, "private_key: [redacted]"),
        ("credentials=" + _SECRET, "credentials=[redacted]"),
        ("credential=" + _SECRET, "credential=[redacted]"),
    ])
    def test_taken(self, text: str, cleaned: str) -> None:
        assert _once(text) == cleaned

    @pytest.mark.parametrize("text", [
        "https://host:443/path", "http://host/x?a=b@c", "user@example.com",
        "idempotency_key=k-1", "correlation_key=c-2", "public_key=pk",
        "credential_type=basic", "no credentials were given",
        "secret_keys rotate", "keyboard: en",
    ])
    def test_the_pair_left_as_it_is(self, text: str) -> None:
        assert sanitize_text(text) == text


# -----------------------------------------------------------------------------
# The providers
# -----------------------------------------------------------------------------


def _email_formatter(status: int, **response: Any) -> EmailFormatter:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, **response)

    return EmailFormatter(
        client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        api_base_url="https://api.mailgun.test", api_key="unit-" + "not-a-key",
        domain="mg.example.test", from_address="comms@mg.example.test",
    )


def _letter() -> tuple[Notification, NotificationDelivery, Recipient]:
    return (
        Notification(
            id=UUID(int=1), type="unit_event_email", lane="normal", title="T",
            body="B", target_type="user", target_value="*", action_data=None,
        ),
        NotificationDelivery(
            id=UUID(int=2), recipient_id=UUID(int=3), channel=DeliveryChannel.EMAIL,
        ),
        Recipient(email="person@example.test", locale="en", active=True),
    )


class TestEmailRedirect:
    """Item 4, email. S6."""

    @pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
    async def test_a_redirect_is_the_channel(self, status: int) -> None:
        formatter = _email_formatter(
            status, headers={"location": "https://elsewhere.test/"})
        with capture_logs() as logs:
            for _ in range(2):
                with pytest.raises(ConfigurationError):
                    await formatter.deliver(*_letter())
        loud = [e for e in logs if e["event"] == "email_channel_not_viable"]
        assert len(loud) == 1 and loud[0]["status"] == status

    async def test_the_pair_a_410_is_one_message(self) -> None:
        with pytest.raises(MessageRejectedError):
            await _email_formatter(410).deliver(*_letter())


_SET = LLMKeySet(name="one", endpoint="https://llm.test/v1",
                 key="unit-" + "not-a-key", model="m", gateway=True)
_LETTER = {"messages": [{"role": "user", "content": "hi"}]}


async def _ask(status: int, body: Any = None) -> BaseException | None:
    def handle(request: httpx.Request) -> httpx.Response:
        if body is None:
            return httpx.Response(status)
        return httpx.Response(status, json=body)

    formatter = LLMFormatter(
        client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        sets={"one": _SET},
    )
    try:
        await formatter.ask(_LETTER, ["one"])
    except Exception as exc:
        return exc
    return None


def _error(**metadata: Any) -> dict[str, Any]:
    return {"error": {"code": 403, "message": "blocked", "metadata": metadata}}


class TestPolicy403:
    """Item 4, llm: by the KEYS of the provider's metadata, the flagged
    text never read. S7, S8."""

    @pytest.mark.parametrize("metadata", [
        {"reasons": ["violence"], "flagged_input": _SECRET,
         "provider_name": "p", "model_slug": "m"},
        {"patterns": ["ignore all previous instructions"]},
        {"error_type": "content_policy_violation", "provider_code": "SAFETY"},
        {"error_type": "refusal"},
    ])
    async def test_a_policy_declines_the_request(
        self, metadata: dict[str, Any],
    ) -> None:
        outcome = await _ask(403, _error(**metadata))
        assert isinstance(outcome, MessageRejectedError)
        assert _SECRET not in str(outcome)

    async def test_inside_a_200_too(self) -> None:
        outcome = await _ask(200, _error(reasons=["x"], flagged_input=_SECRET))
        assert isinstance(outcome, MessageRejectedError)

    @pytest.mark.parametrize("body", [
        None,
        {"error": {"code": 403, "message": "forbidden"}},
        _error(limit_source="openrouter_guardrail_budget", limit_usd=50),
        _error(error_type="permission_denied"),
    ])
    async def test_the_pair_any_other_403_is_the_set(self, body: Any) -> None:
        assert isinstance(await _ask(403, body), ConfigurationError)


class TestPoolTimeout:
    """Item 4: a pool timeout got no connection -- nothing was sent. S9."""

    async def test_email_not_sent(self) -> None:
        def handle(request: httpx.Request) -> httpx.Response:
            raise httpx.PoolTimeout("no connection")

        formatter = EmailFormatter(
            client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
            api_base_url="https://api.mailgun.test",
            api_key="unit-" + "not-a-key", domain="mg.example.test",
            from_address="comms@mg.example.test",
        )
        with capture_logs() as logs, pytest.raises(httpx.PoolTimeout):
            await formatter.deliver(*_letter())
        (failure,) = [e for e in logs if e["event"] == "email_request_failed"]
        assert failure["sent_unknown"] is False

    async def test_llm_not_sent_and_retried(self) -> None:
        def handle(request: httpx.Request) -> httpx.Response:
            raise httpx.PoolTimeout("no connection")

        formatter = LLMFormatter(
            client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
            sets={"one": _SET},
        )
        with pytest.raises(LLMTransientError) as caught:
            await formatter.ask(_LETTER, ["one"])
        assert "PoolTimeout" in str(caught.value)
        assert "may have reached" not in str(caught.value)

    async def test_the_pair_a_read_timeout_may_have_reached(self) -> None:
        def handle(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("too slow")

        formatter = LLMFormatter(
            client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
            sets={"one": _SET},
        )
        with pytest.raises(LLMTransientError) as caught:
            await formatter.ask(_LETTER, ["one"])
        assert "may have reached" in str(caught.value)
