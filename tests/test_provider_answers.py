# =============================================================================
# CH-2 -- the provider's answer read right: email and llm
# =============================================================================
#
# Email (EmailFormatter._interpret), against what Mailgun answers:
#   - an unknown domain is a 404 with "Domain not found" in its JSON
#     message (mailgun-go mocks/mock_domains.go): the CHANNEL, like the
#     sender-fault 400; a 404 that names no sender stays ONE message;
#   - a 2xx is the provider's acceptance, whichever 2xx (RFC 9110 15.3);
#   - a 408 is "the request did not arrive whole -- repeat it" (RFC 9110
#     15.5.9): a retry, no message was created.
# Llm (LLMFormatter._ask_one): the provider's answer is read as a stream
# with a ceiling (MAX_LLM_ANSWER_BYTES); an answer above it is the set's
# transport failure, read no further.
#
# MUTATIONS these tests were written against (each turns one red):
#   E1  sender markers on 400 only                  -> TestEmailDomain
#   E2  sender markers on any 4xx                   -> TestEmailDomain
#   E3  accepted back to {200, 202}                 -> TestEmailAccepted
#   E4  408 not transient                           -> TestEmailRetry
#   E5  any 404 is the channel                      -> TestEmailDomain
#   E6  "channel not viable" on every message       -> TestEmailDomain
#   L1  the answer read whole                       -> TestLlmCeiling
#   L2  the ceiling compared with >=                -> TestLlmCeiling
#   L3  Content-Length not looked at                -> TestLlmCeiling
#   L4  an answer over the ceiling a rejection      -> TestLlmCeiling
#   L5  Content-Encoding kept on the rebuilt answer -> TestLlmCeiling
#   L6  the document's number apart from the code   -> TestTheContract
# =============================================================================

import gzip
import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.core.channels import LLMKeySet
from app.core.constants import MAX_LLM_ANSWER_BYTES
from app.engine.constants import DeliveryChannel, DeliveryStatus, FailureClass
from app.engine.formatters import (
    ConfigurationError,
    EmailFormatter,
    EmailTransientError,
    LLMFormatter,
    LLMReply,
    LLMTransientError,
    MessageRejectedError,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.service import (
    create_notification,
    deliver_notification,
    resolve_notification,
)
from tests.helpers import create_recipient, intake_fields

_DOC = (Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md").read_text(
    encoding="utf-8")


# -----------------------------------------------------------------------------
# Email
# -----------------------------------------------------------------------------


class _Mailgun:
    """The provider, answering every POST with one status and body."""

    def __init__(
        self, status: int, body: Any = None, *, raw: str | None = None,
    ) -> None:
        self.status, self.body, self.raw = status, body, raw
        self.requests = 0

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests += 1
        if self.raw is not None:
            return httpx.Response(self.status, text=self.raw)
        if self.body is None:
            return httpx.Response(self.status)
        return httpx.Response(self.status, json=self.body)

    def formatter(self) -> EmailFormatter:
        return EmailFormatter(
            client=httpx.AsyncClient(transport=httpx.MockTransport(self.handle)),
            api_base_url="https://api.mailgun.test",
            api_key="unit-" + "not-a-key",
            domain="mg.example.test",
            from_address="Comms <noreply@mg.example.test>",
        )


async def _email(
    session: AsyncSession, formatter: EmailFormatter,
) -> NotificationDelivery:
    """One email job through the real service step."""
    recipient = await create_recipient(session, email="person@example.test")
    notification = await create_notification(
        session, **intake_fields(), type="unit_event_email", title="T", body="B",
        target_type="user", target_value=str(recipient.id),
    )
    (delivery,) = await resolve_notification(session, notification)
    with patch("app.engine.service.get_formatter", return_value=formatter):
        await deliver_notification(session, notification)
    await session.refresh(delivery)
    return delivery


async def _raised(provider: _Mailgun) -> BaseException | bool:
    """What the formatter does with the answer, no database: True, or the
    exception."""
    notification = Notification(
        id=UUID(int=1), type="unit_event_email", lane="normal", title="T",
        body="B", target_type="user", target_value="*", action_data=None,
    )
    delivery = NotificationDelivery(
        id=UUID(int=2), recipient_id=UUID(int=3), channel=DeliveryChannel.EMAIL)
    recipient = Recipient(email="person@example.test", locale="en", active=True)
    try:
        return await provider.formatter().deliver(notification, delivery, recipient)
    except Exception as exc:
        return exc


_DOMAIN_NOT_FOUND = {"message": "Domain not found: mg.example.test"}


class TestEmailDomain:
    """Item 1: Mailgun's unknown domain (404 with the marker in its JSON)
    is the channel; a 404 that names no sender is one message; a 400
    with a marker as before. E1, E2, E5, E6."""

    async def test_domain_not_found_is_the_channel(
        self, db_session: AsyncSession,
    ) -> None:
        formatter = _Mailgun(404, _DOMAIN_NOT_FOUND).formatter()
        with capture_logs() as logs:
            first = await _email(db_session, formatter)
            second = await _email(db_session, formatter)
        for delivery in (first, second):
            assert delivery.status == DeliveryStatus.FAILED
            assert delivery.failure_class == FailureClass.CONFIGURATION
            assert delivery.next_retry_at is None
        loud = [e for e in logs if e["event"] == "email_channel_not_viable"]
        assert len(loud) == 1
        assert loud[0]["status"] == 404

    @pytest.mark.parametrize(("status", "body", "raw"), [
        (404, None, None),
        (404, {"message": "Not Found"}, None),
        (404, None, "<html>Domain not found</html>"),
        (410, _DOMAIN_NOT_FOUND, None),
        (413, {"message": "request size exceeds 52.4MiB limit"}, None),
    ], ids=["404-empty", "404-no-marker", "404-html", "410-marker", "413"])
    async def test_the_pair_one_message(
        self, status: int, body: Any, raw: str | None,
    ) -> None:
        assert isinstance(await _raised(_Mailgun(status, body, raw=raw)),
                          MessageRejectedError)

    async def test_the_pair_a_400_with_a_marker_as_before(self) -> None:
        body = {"message": "Domain mg.example.test is not allowed to send: "
                           "Free accounts are for test purposes only."}
        assert isinstance(await _raised(_Mailgun(400, body)), ConfigurationError)


class TestEmailAccepted:
    """Item 2: any 2xx is the provider's acceptance. E3."""

    @pytest.mark.parametrize(
        "status", [200, 201, 202, 203, 204, 205, 206, 207, 208, 226])
    async def test_every_2xx_is_accepted(self, status: int) -> None:
        body = None if status in (204, 205) else {
            "id": "<1@mg.example.test>", "message": "Queued. Thank you."}
        with capture_logs() as logs:
            assert await _raised(_Mailgun(status, body)) is True
        (accepted,) = [e for e in logs if e["event"] == "email_accepted"]
        assert accepted["status"] == status

    async def test_the_record_says_sent(self, db_session: AsyncSession) -> None:
        delivery = await _email(db_session, _Mailgun(201, {"id": "<1@x>"}).formatter())
        assert delivery.status == DeliveryStatus.SENT


class TestEmailRetry:
    """Item 3: 408 -- the provider did not get the request whole -- is a
    retry; other 4xx stay as they were. E4."""

    async def test_408_is_transient(self, db_session: AsyncSession) -> None:
        provider = _Mailgun(408, raw="Request Timeout")
        assert isinstance(await _raised(provider), EmailTransientError)
        delivery = await _email(db_session, provider.formatter())
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.failure_class is None
        assert delivery.next_retry_at is not None


# -----------------------------------------------------------------------------
# Llm
# -----------------------------------------------------------------------------

_SET = LLMKeySet(name="one", endpoint="https://llm.test/v1", key="unit-" + "not-a-key",
                 model="m", gateway=False)
_SET_TWO = LLMKeySet(name="two", endpoint="https://llm2.test/v1",
                     key="unit-" + "not-a-key", model="m", gateway=False)
_LETTER = {"messages": [{"role": "user", "content": "hi"}]}
_BLOCK = 64 * 1024


def _completion(size: int) -> bytes:
    """A chat completion of exactly `size` bytes."""
    head = b'{"choices":[{"message":{"role":"assistant","content":"'
    tail = b'"}}],"model":"m"}'
    fill = size - len(head) - len(tail)
    assert fill >= 0
    return head + b"x" * fill + tail


class _Counted(httpx.AsyncByteStream):
    """A body that counts what the reader took from it."""

    def __init__(self, body: bytes) -> None:
        self.body, self.taken = body, 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for start in range(0, len(self.body), _BLOCK):
            chunk = self.body[start:start + _BLOCK]
            self.taken += len(chunk)
            yield chunk

    async def aclose(self) -> None:
        return None


class _Model:
    """The provider: one answer per endpoint."""

    def __init__(self, answers: dict[str, tuple[bytes, dict[str, str]]]) -> None:
        self.answers = answers
        self.streams: dict[str, _Counted] = {}

    def handle(self, request: httpx.Request) -> httpx.Response:
        body, headers = self.answers[request.url.host]
        stream = self.streams[request.url.host] = _Counted(body)
        return httpx.Response(200, headers=headers, stream=stream)

    def formatter(self) -> LLMFormatter:
        return LLMFormatter(
            client=httpx.AsyncClient(transport=httpx.MockTransport(self.handle)),
            sets={"one": _SET, "two": _SET_TWO},
        )


async def _ask(model: _Model, *sets: str) -> LLMReply | BaseException:
    try:
        return await model.formatter().ask(_LETTER, list(sets or ("one",)))
    except Exception as exc:
        return exc


class TestLlmCeiling:
    """Item 4: the answer is read with a ceiling; above it the set failed
    as a transport (the next set is asked, the call is retried), read no
    further. L1-L5."""

    async def test_one_over_is_transient_read_no_further(self) -> None:
        model = _Model({"llm.test": (_completion(MAX_LLM_ANSWER_BYTES + 1), {})})
        outcome = await _ask(model)
        assert isinstance(outcome, LLMTransientError)
        assert f"larger than {MAX_LLM_ANSWER_BYTES} bytes" in str(outcome)
        assert model.streams["llm.test"].taken <= MAX_LLM_ANSWER_BYTES + _BLOCK

    async def test_exactly_the_ceiling_is_an_answer(self) -> None:
        body = _completion(MAX_LLM_ANSWER_BYTES)
        outcome = await _ask(_Model({"llm.test": (body, {})}))
        assert isinstance(outcome, LLMReply)
        assert outcome.body == body.decode()

    async def test_a_declared_length_over_is_not_read(self) -> None:
        big = _completion(MAX_LLM_ANSWER_BYTES + 1)
        model = _Model({"llm.test": (big, {"content-length": str(len(big))})})
        outcome = await _ask(model)
        assert isinstance(outcome, LLMTransientError)
        assert model.streams["llm.test"].taken == 0

    async def test_a_lying_length_is_counted_anyway(self) -> None:
        model = _Model({"llm.test": (
            _completion(MAX_LLM_ANSWER_BYTES + _BLOCK * 3), {"content-length": "10"})})
        outcome = await _ask(model)
        assert isinstance(outcome, LLMTransientError)
        assert model.streams["llm.test"].taken <= MAX_LLM_ANSWER_BYTES + _BLOCK

    async def test_the_next_set_is_asked(self) -> None:
        model = _Model({
            "llm.test": (_completion(MAX_LLM_ANSWER_BYTES + 1), {}),
            "llm2.test": (_completion(1000), {}),
        })
        outcome = await _ask(model, "one", "two")
        assert isinstance(outcome, LLMReply)
        assert outcome.set_name == "two"

    async def test_a_compressed_answer_is_read_once(self) -> None:
        body = _completion(5000)
        model = _Model({"llm.test": (
            gzip.compress(body), {"content-encoding": "gzip"})})
        outcome = await _ask(model)
        assert isinstance(outcome, LLMReply)
        assert outcome.body == body.decode()

    async def test_an_error_status_is_read_as_before(self) -> None:
        def handle(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json={"error": {"code": 401}})

        formatter = LLMFormatter(
            client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
            sets={"one": _SET},
        )
        with pytest.raises(ConfigurationError):
            await formatter.ask(_LETTER, ["one"])


class TestTheContract:
    """deploy/INTEGRATION.md names the ceiling with the code's number, and
    the email outcomes this change made. L6."""

    def test_the_llm_ceiling(self) -> None:
        assert re.search(
            rf"an answer larger than {MAX_LLM_ANSWER_BYTES} bytes", _DOC)

    def test_the_email_answers(self) -> None:
        (paragraph,) = re.findall(
            r"\*\*What the provider's answer means\.\*\*.*?(?:\n\n|\Z)",
            _DOC, flags=re.S,
        )
        for words in ("any `2xx`", "`404`", "`408`"):
            assert words in paragraph, words
