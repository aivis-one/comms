# =============================================================================
# COMMS Service -- The email channel (R-1)
# =============================================================================
#
# Email becomes a channel under the SAME rule as every other one
# (app/core/channels.py): its key set decides, and nothing else does.
# What is specific to email and pinned here:
#
#   - the key set, including the DEFAULTED region -- which does NOT
#     make an empty set partial, and whose shape is still checked;
#   - the sender's two legal forms, and the refusal text naming the
#     expected one;
#   - subject and body: the channel reads its OWN profile fields, with
#     a three-step subject fallback and a body fallback that never
#     reaches into another channel's templates;
#   - the three failure classes, decided in the formatter because the
#     shared retry policy (app/engine/service.py) is not touched;
#   - loudness on STATE: the first configuration-class refusal says the
#     channel is not viable, the rest are ordinary lines;
#   - the recipient address: three unusable shapes, all terminal.
#
# NOTHING HERE GOES TO THE NETWORK: every live channel is built with an
# injected client on httpx.MockTransport, and conftest's two tripwires
# (aiogram transport, httpx transport) fail any test that slips out.
# =============================================================================

from typing import Any
from unittest.mock import patch
from uuid import UUID

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.core.channels import EMAIL_REGIONS, ChannelState, evaluate_channels
from app.core.config import _EMAIL_API_BASE_URLS, Settings
from app.engine.constants import (
    DeliveryChannel,
    DeliveryStatus,
    NotificationStatus,
    TargetType,
)
from app.engine.formatters import (
    EmailFormatter,
    EmailTransientError,
    PermanentDeliveryError,
    RateLimitedError,
    UnavailableChannelFormatter,
    build_formatters,
    channel_map,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import process_pending_notifications
from app.engine.service import create_notification
from app.profile.registry import registry
from tests.helpers import create_recipient

_KEY = "key-0123456789"
_DOMAIN = "mail.example.test"
_SENDER = "noreply@mail.example.test"

_WITH_EMAIL: dict[str, Any] = {
    "app_env": "production",
    "database_url": "postgresql+asyncpg://u:p@db/comms_unit",
    "comms_service_token": "unit-test-service-token",
    "email_mailgun_api_key": _KEY,
    "email_mailgun_domain": _DOMAIN,
    "email_from_address": _SENDER,
}


def _settings(**overrides: str) -> Settings:
    kwargs: dict[str, Any] = {"_env_file": None, **_WITH_EMAIL, **overrides}
    return Settings(**kwargs)


def _keys(**overrides: str) -> dict[str, str]:
    """The email key set as the parser sees it."""
    values = {
        "EMAIL_MAILGUN_API_KEY": _KEY,
        "EMAIL_MAILGUN_DOMAIN": _DOMAIN,
        "EMAIL_FROM_ADDRESS": _SENDER,
    }
    values.update(overrides)
    return values


def _problem(values: dict[str, str]) -> str:
    _, problems = evaluate_channels(values)
    assert problems, "expected the key set to be refused"
    return "\n".join(problems)


class _Provider:
    """A provider stand-in: records requests, replies as instructed."""

    def __init__(
        self,
        status: int = 200,
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        raises: Exception | None = None,
        body: str | None = None,
    ) -> None:
        self.status = status
        self.payload = (
            payload if payload is not None else {"id": "<20260912.1@mg.test>"}
        )
        self.headers = headers or {}
        self.raises = raises
        self.body = body
        self.requests: list[httpx.Request] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.raises is not None:
            raise self.raises
        if self.body is not None:
            return httpx.Response(
                self.status, text=self.body, headers=self.headers,
            )
        return httpx.Response(
            self.status, json=self.payload, headers=self.headers,
        )

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(self.handle),
        )

    def fields(self) -> dict[str, str]:
        """The form fields of the single request that was made."""
        (request,) = self.requests
        content = request.content.decode()
        fields: dict[str, str] = {}
        for pair in content.split("&"):
            key, _, value = pair.partition("=")
            from urllib.parse import unquote_plus

            fields[unquote_plus(key)] = unquote_plus(value)
        return fields


def _formatter(provider: _Provider, **overrides: str) -> EmailFormatter:
    source = _settings(**overrides)
    return EmailFormatter(
        client=provider.client(),
        api_base_url=source.email_api_base_url,
        api_key=source.email_mailgun_api_key,
        domain=source.email_mailgun_domain,
        from_address=source.email_from_address,
    )


def _notification(**overrides: Any) -> Notification:
    defaults: dict[str, Any] = {
        "id": UUID(int=1),
        "type": "unit_event",
        "title": "Hello",
        "body": "World",
        "target_type": "user",
        "target_value": "*",
        "action_data": None,
    }
    defaults.update(overrides)
    return Notification(**defaults)


def _delivery(**overrides: Any) -> NotificationDelivery:
    defaults: dict[str, Any] = {
        "id": UUID(int=2),
        "recipient_id": UUID(int=3),
        "channel": DeliveryChannel.EMAIL,
        "channel_options": None,
    }
    defaults.update(overrides)
    return NotificationDelivery(**defaults)


def _recipient(**overrides: Any) -> Recipient:
    defaults: dict[str, Any] = {
        "email": "user@example.test",
        "locale": "en",
        "active": True,
    }
    defaults.update(overrides)
    return Recipient(**defaults)


async def _send(
    provider: _Provider, recipient: Recipient | None = None, **overrides: Any
) -> bool:
    formatter = _formatter(provider)
    return await formatter.deliver(
        _notification(**overrides),
        _delivery(),
        recipient or _recipient(),
    )


# ---------------------------------------------------------------------------
# The key set
# ---------------------------------------------------------------------------


class TestEmailKeySet:
    def test_all_empty_is_a_deploy_without_email(self) -> None:
        states, problems = evaluate_channels({})
        assert problems == []
        assert states["email"] == ChannelState.NOT_CONFIGURED

    def test_all_set_is_live(self) -> None:
        states, problems = evaluate_channels(_keys())
        assert problems == []
        assert states["email"] == ChannelState.LIVE

    @pytest.mark.parametrize(
        "missing",
        [
            "EMAIL_MAILGUN_API_KEY",
            "EMAIL_MAILGUN_DOMAIN",
            "EMAIL_FROM_ADDRESS",
        ],
    )
    def test_partial_set_names_exactly_the_missing_key(
        self, missing: str,
    ) -> None:
        problem = _problem(_keys(**{missing: ""}))
        assert f"missing: {missing}" in problem
        for other in ("EMAIL_MAILGUN_API_KEY", "EMAIL_MAILGUN_DOMAIN"):
            if other != missing:
                assert f"missing: {other}" not in problem

    @pytest.mark.parametrize("value", [" ", "\t"])
    def test_whitespace_only_is_garbage_not_empty(self, value: str) -> None:
        problem = _problem(_keys(EMAIL_MAILGUN_API_KEY=value))
        assert "EMAIL_MAILGUN_API_KEY consists only of whitespace" in problem

    def test_no_key_value_reaches_the_refusal_text(self) -> None:
        """Keys carry secrets: the text names the key, never its value."""
        problem = _problem(_keys(EMAIL_MAILGUN_DOMAIN=""))
        assert _KEY not in problem


class TestRegionIsDefaultedNotDeciding:
    def test_absent_region_does_not_make_an_empty_set_partial(self) -> None:
        """The trap this test exists for: a defaulted key counted as
        part of the set would make EVERY deploy without email look
        partial -- and refuse to start. That would take down the
        products that have no email at all."""
        states, problems = evaluate_channels({})
        assert problems == []
        assert states["email"] == ChannelState.NOT_CONFIGURED

    def test_default_is_a_known_region(self) -> None:
        assert _settings().email_mailgun_region in EMAIL_REGIONS

    @pytest.mark.parametrize("region", sorted(EMAIL_REGIONS))
    def test_every_known_region_resolves_to_an_address(
        self, region: str,
    ) -> None:
        source = _settings(email_mailgun_region=region)
        assert source.email_api_base_url == _EMAIL_API_BASE_URLS[region]

    def test_the_two_sides_of_the_closed_set_agree(self) -> None:
        """The names live in channels.py, the addresses in config.py --
        the fence allows addresses in exactly one file. Nothing keeps
        the two lists in step except this assertion."""
        assert set(_EMAIL_API_BASE_URLS) == set(EMAIL_REGIONS)

    @pytest.mark.parametrize(
        "region", ["eu-west", "europe", "EU", "us-east-1", " eu"],
    )
    def test_unknown_region_refuses_startup(self, region: str) -> None:
        with pytest.raises(ValueError, match="EMAIL_MAILGUN_REGION"):
            _settings(email_mailgun_region=region)

    def test_unknown_region_refuses_even_without_the_channel(self) -> None:
        """A typo in a defaulted value on a deploy that has NO email is
        still this deploy's typo: ignoring it would teach the integrator
        that the key does not exist."""
        problem = _problem({"EMAIL_MAILGUN_REGION": "europe"})
        assert "EMAIL_MAILGUN_REGION is malformed" in problem

    def test_the_refusal_names_the_allowed_values(self) -> None:
        problem = _problem({"EMAIL_MAILGUN_REGION": "europe"})
        for region in EMAIL_REGIONS:
            assert region in problem


class TestSenderForm:
    @pytest.mark.parametrize(
        "value",
        [
            "noreply@mail.example.test",
            "Support <noreply@mail.example.test>",
            "A B C <a@b.co>",
        ],
    )
    def test_both_legal_forms_are_accepted(self, value: str) -> None:
        states, problems = evaluate_channels(
            _keys(EMAIL_FROM_ADDRESS=value),
        )
        assert problems == []
        assert states["email"] == ChannelState.LIVE

    @pytest.mark.parametrize(
        "value",
        [
            "noreply",                       # no @
            "noreply@",                      # nothing after @
            "@mail.example.test",            # nothing before @
            "a@b@c.test",                    # two @
            "noreply@localhost",             # no dot in the domain
            "no reply@mail.example.test",    # space
            "<a@b.co>",                      # empty display name
            "Name <a@b.co",                  # unclosed
            "Name a@b.co>",                  # unopened
            "Name <a@b.co> x",               # trailing junk
        ],
    )
    def test_malformed_sender_refuses(self, value: str) -> None:
        problem = _problem(_keys(EMAIL_FROM_ADDRESS=value))
        assert "EMAIL_FROM_ADDRESS is malformed" in problem

    @pytest.mark.parametrize(
        "value",
        [
            "a@b.co\nBcc: attacker@evil.test",
            "a@b.co\r\nBcc: attacker@evil.test",
            "Name\r\n <a@b.co>",
            "Name <a@b.co>\n",
        ],
    )
    def test_line_breaks_refuse_wherever_they_sit(self, value: str) -> None:
        """CR/LF is checked over the WHOLE value before any parsing, so
        the refusal cannot depend on how the angle brackets split."""
        problem = _problem(_keys(EMAIL_FROM_ADDRESS=value))
        assert "line break" in problem

    def test_the_refusal_names_the_expected_form(self) -> None:
        """A refusal on a value the provider itself would have accepted
        has to say what shape it wanted -- otherwise the integrator is
        told only that they are wrong."""
        problem = _problem(_keys(EMAIL_FROM_ADDRESS="noreply"))
        assert "Name <address>" in problem


class TestStartupWithEmail:
    def test_full_set_makes_the_channel_live(self) -> None:
        assert channel_map(_settings())["email"] == ChannelState.LIVE

    def test_empty_set_leaves_the_service_running_without_email(
        self,
    ) -> None:
        source = _settings(
            email_mailgun_api_key="",
            email_mailgun_domain="",
            email_from_address="",
        )
        assert channel_map(source)["email"] == ChannelState.NOT_CONFIGURED

    def test_unconfigured_email_resolves_to_the_refusal(self) -> None:
        source = _settings(
            email_mailgun_api_key="",
            email_mailgun_domain="",
            email_from_address="",
        )
        built = build_formatters(
            source, _fake_bot_factory, _Provider().client,
        )
        assert isinstance(
            built.formatters[DeliveryChannel.EMAIL],
            UnavailableChannelFormatter,
        )

    def test_a_live_channel_registers_its_client_for_closing(self) -> None:
        built = build_formatters(
            _settings(), _fake_bot_factory, _Provider().client,
        )
        assert [name for name, _ in built.closers] == ["email_http_client"]

    def test_the_client_is_built_once_not_per_message(self) -> None:
        """A fresh TLS handshake per email would eat the delivery-time
        budget; the client is created once, at registry build."""
        built: list[int] = []
        provider = _Provider()

        def _factory() -> httpx.AsyncClient:
            built.append(1)
            return provider.client()

        build_formatters(_settings(), _fake_bot_factory, _factory)
        assert built == [1]


def _fake_bot_factory(**kwargs: Any) -> Any:
    raise AssertionError("telegram must not be built in these tests")


# ---------------------------------------------------------------------------
# Subject and body
# ---------------------------------------------------------------------------


class TestSubjectAndBody:
    async def test_subject_comes_from_the_channels_own_template(
        self,
    ) -> None:
        """The fixture profile declares unit_event.email.subject --
        email reads its OWN field, exactly like telegram reads its own.
        No new profile key, no channel option."""
        provider = _Provider()
        assert await _send(provider, title="Hello") is True
        assert provider.fields()["subject"] == "COMMS: Hello"

    async def test_body_falls_back_to_the_stored_body(self) -> None:
        """The fixture declares no email BODY template, only a subject:
        a profile that does not spell out every field stays valid."""
        provider = _Provider()
        await _send(provider, body="World")
        assert provider.fields()["text"] == "World"

    async def test_body_never_borrows_another_channels_template(
        self,
    ) -> None:
        """The telegram body for this type is "{body} [{extra}]" and
        carries markup elsewhere -- reaching into it would deliver raw
        characters in a text email."""
        provider = _Provider()
        await _send(provider, body="World")
        assert provider.fields()["text"] == "World"
        assert "[" not in provider.fields()["text"]

    async def test_subject_falls_back_to_the_title(self) -> None:
        provider = _Provider()
        with patch(
            "app.engine.formatters.render",
            side_effect=lambda **kwargs: None,
        ):
            await _send(provider, title="Reset your password")
        assert provider.fields()["subject"] == "Reset your password"

    async def test_subject_fallback_is_not_derived_from_the_body(
        self,
    ) -> None:
        """A confirmation body opens with the code, and the subject
        shows in the lock-screen preview: the code must not travel
        there."""
        provider = _Provider()
        with patch(
            "app.engine.formatters.render",
            side_effect=lambda **kwargs: None,
        ):
            await _send(
                provider, title="Confirm your address", body="482913 is...",
            )
        assert "482913" not in provider.fields()["subject"]

    @pytest.mark.parametrize("title", ["   ", "\t", " \n "])
    async def test_blank_title_falls_through_to_the_type_key(
        self, title: str,
    ) -> None:
        """A blank title IS reachable (ingest rejects "" but accepts
        whitespace), and an empty subject looks like spam -- which hurts
        the deliverability this channel exists for."""
        provider = _Provider()
        with patch(
            "app.engine.formatters.render",
            side_effect=lambda **kwargs: None,
        ):
            await _send(provider, title=title)
        assert provider.fields()["subject"] == "unit_event"

    @pytest.mark.parametrize(
        "title",
        [
            "Hi\r\nBcc: attacker@evil.test",
            "Hi\nX-Injected: 1",
            "Hi\t\tthere",
        ],
    )
    async def test_subject_is_normalised(self, title: str) -> None:
        """The subject is rendered from producer-supplied values -- the
        same trust boundary HTML escaping guards on the telegram side."""
        provider = _Provider()
        with patch(
            "app.engine.formatters.render",
            side_effect=lambda **kwargs: None,
        ):
            await _send(provider, title=title)
        subject = provider.fields()["subject"]
        assert "\n" not in subject and "\r" not in subject
        assert "  " not in subject

    async def test_the_locale_chain_is_the_one_every_channel_uses(
        self,
    ) -> None:
        """A locale with no templates of its own falls back to the
        deploy default -- the fixture declares "en" only."""
        provider = _Provider()
        await _send(provider, recipient=_recipient(locale="ru"))
        assert provider.fields()["subject"] == "COMMS: Hello"

    async def test_the_request_carries_this_deploys_sender_and_domain(
        self,
    ) -> None:
        provider = _Provider()
        await _send(provider)
        (request,) = provider.requests
        assert str(request.url).endswith(f"/v3/{_DOMAIN}/messages")
        assert provider.fields()["from"] == _SENDER
        assert provider.fields()["to"] == "user@example.test"


# ---------------------------------------------------------------------------
# The three failure classes
# ---------------------------------------------------------------------------


class TestFailureClasses:
    async def test_accepted_is_a_success(self) -> None:
        assert await _send(_Provider(status=200)) is True
        assert await _send(_Provider(status=202)) is True

    @pytest.mark.parametrize("status", [500, 502, 503])
    async def test_provider_errors_are_transient(self, status: int) -> None:
        with pytest.raises(EmailTransientError):
            await _send(_Provider(status=status))

    async def test_network_failure_is_transient(self) -> None:
        with pytest.raises(httpx.ConnectError):
            await _send(
                _Provider(raises=httpx.ConnectError("no route")),
            )

    async def test_rate_limit_with_a_named_wait_is_deferred(self) -> None:
        """429 is not a message failure: the provider's own wait is
        honoured through the existing deferral mechanism rather than a
        second cap of our own."""
        provider = _Provider(status=429, headers={"Retry-After": "90"})
        with pytest.raises(RateLimitedError) as caught:
            await _send(provider)
        assert caught.value.retry_after == 90.0

    @pytest.mark.parametrize("header", [{}, {"Retry-After": "not-a-number"}])
    async def test_rate_limit_without_a_named_wait_is_transient(
        self, header: dict[str, str],
    ) -> None:
        """No usable wait -> the ordinary attempts budget, which is
        finite. Guessing a duration would not be."""
        with pytest.raises(EmailTransientError):
            await _send(_Provider(status=429, headers=header))

    async def test_recipient_400_fails_one_message(self) -> None:
        provider = _Provider(
            status=400, payload={"message": "to parameter is not a valid"},
        )
        with pytest.raises(PermanentDeliveryError):
            await _send(provider)

    @pytest.mark.parametrize("status", [401, 402, 403])
    async def test_credentials_and_account_are_permanent(
        self, status: int,
    ) -> None:
        """402 is an account out of credit, not a message fault: quietly
        retrying it is pointless."""
        with pytest.raises(PermanentDeliveryError):
            await _send(_Provider(status=status, payload={"message": "no"}))

    @pytest.mark.parametrize(
        "message",
        [
            "Domain not found: mail.example.test",
            "The domain is not verified",
            "Free accounts are for test purposes only",
            "from address is not allowed",
        ],
    )
    async def test_sender_side_400_is_read_from_the_body(
        self, message: str,
    ) -> None:
        """400 is two different failures under one status: on the
        recipient it is one message, on the SENDER it is the channel.
        The provider's own text is what tells them apart."""
        provider = _Provider(status=400, payload={"message": message})
        formatter = _formatter(provider)
        with capture_logs() as logs, pytest.raises(PermanentDeliveryError):
            await formatter.deliver(
                _notification(), _delivery(), _recipient(),
            )
        assert any(e["event"] == "email_channel_not_viable" for e in logs)

    async def test_unparsable_body_stays_conservative(self) -> None:
        """No JSON to read: fail ONE message and log the body. Calling a
        live channel dead on a reply we could not parse is the more
        expensive mistake."""
        provider = _Provider(status=400, body="<html>gateway</html>")
        formatter = _formatter(provider)
        with capture_logs() as logs, pytest.raises(PermanentDeliveryError):
            await formatter.deliver(
                _notification(), _delivery(), _recipient(),
            )
        assert not any(
            e["event"] == "email_channel_not_viable" for e in logs
        )
        assert any(e["event"] == "email_rejected" for e in logs)


class TestLoudnessIsPerState:
    async def test_first_configuration_failure_is_a_line_of_its_own(
        self,
    ) -> None:
        """A dead channel during a thousand registrations must not be a
        thousand identical lines: the FIRST refusal names the channel as
        not viable, the rest are ordinary per-message lines."""
        provider = _Provider(status=401, payload={"message": "Forbidden"})
        formatter = _formatter(provider)

        events: list[list[str]] = []
        for _ in range(3):
            with capture_logs() as logs, pytest.raises(
                PermanentDeliveryError,
            ):
                await formatter.deliver(
                    _notification(), _delivery(), _recipient(),
                )
            events.append([entry["event"] for entry in logs])

        assert events[0].count("email_channel_not_viable") == 1
        assert all(
            "email_channel_not_viable" not in later for later in events[1:]
        )
        assert all("email_rejected" in attempt for attempt in events)

    async def test_the_flag_does_not_gate_delivery(self) -> None:
        """It controls log volume and nothing else -- the channel is
        never marked broken, and a later success is still a success."""
        provider = _Provider(status=401, payload={"message": "Forbidden"})
        formatter = _formatter(provider)
        with pytest.raises(PermanentDeliveryError):
            await formatter.deliver(
                _notification(), _delivery(), _recipient(),
            )

        healthy = _Provider()
        formatter._client = healthy.client()
        assert await formatter.deliver(
            _notification(), _delivery(), _recipient(),
        ) is True


class TestForensicHandle:
    async def test_the_provider_id_is_logged_on_success(self) -> None:
        provider = _Provider(payload={"id": "<abc@mg.test>"})
        formatter = _formatter(provider)
        with capture_logs() as logs:
            await formatter.deliver(
                _notification(), _delivery(), _recipient(),
            )
        accepted = [e for e in logs if e["event"] == "email_accepted"]
        assert accepted[0]["provider_message_id"] == "<abc@mg.test>"

    async def test_the_provider_id_is_logged_on_refusal_too(self) -> None:
        """An id that only shows up on success is missing exactly when
        something went wrong."""
        provider = _Provider(
            status=400, payload={"id": "<def@mg.test>", "message": "bad to"},
        )
        formatter = _formatter(provider)
        with capture_logs() as logs, pytest.raises(PermanentDeliveryError):
            await formatter.deliver(
                _notification(), _delivery(), _recipient(),
            )
        rejected = [e for e in logs if e["event"] == "email_rejected"]
        assert rejected[0]["provider_message_id"] == "<def@mg.test>"

    async def test_a_missing_id_does_not_break_the_send(self) -> None:
        provider = _Provider(payload={"message": "Queued"})
        formatter = _formatter(provider)
        with capture_logs() as logs:
            assert await formatter.deliver(
                _notification(), _delivery(), _recipient(),
            ) is True
        accepted = [e for e in logs if e["event"] == "email_accepted"]
        assert accepted[0]["provider_message_id"] is None

    async def test_no_recipient_address_reaches_the_log(self) -> None:
        """comms logs carry no personal data, and this channel does not
        start the precedent: recipient_id leads to the address through
        the database, the provider id leads to the message in the
        provider's dashboard."""
        provider = _Provider()
        formatter = _formatter(provider)
        with capture_logs() as logs:
            await formatter.deliver(
                _notification(), _delivery(), _recipient(),
            )
        assert not any("user@example.test" in str(entry) for entry in logs)

    async def test_a_read_timeout_is_marked_as_possibly_sent(self) -> None:
        """The provider offers no idempotency on this endpoint, so a
        duplicate cannot be PREVENTED. What can be done is telling the
        two timeouts apart afterwards: a connect failure never produced
        a message, a read timeout may have."""
        provider = _Provider(raises=httpx.ReadTimeout("too slow"))
        formatter = _formatter(provider)
        with capture_logs() as logs, pytest.raises(httpx.ReadTimeout):
            await formatter.deliver(
                _notification(), _delivery(), _recipient(),
            )
        (failure,) = [
            e for e in logs if e["event"] == "email_request_failed"
        ]
        assert failure["sent_unknown"] is True

    async def test_a_connect_failure_is_marked_as_not_sent(self) -> None:
        provider = _Provider(raises=httpx.ConnectError("no route"))
        formatter = _formatter(provider)
        with capture_logs() as logs, pytest.raises(httpx.ConnectError):
            await formatter.deliver(
                _notification(), _delivery(), _recipient(),
            )
        (failure,) = [
            e for e in logs if e["event"] == "email_request_failed"
        ]
        assert failure["sent_unknown"] is False


class TestRepeatOnTheSendPath:
    async def test_one_deliver_call_makes_one_request(self) -> None:
        provider = _Provider()
        await _send(provider)
        assert len(provider.requests) == 1

    async def test_a_retry_sends_an_identical_message(self) -> None:
        """Duplicates cannot be prevented here, and this is what makes
        that tolerable: subject and body are rendered from the STORED
        title/body, so a retry re-sends an identical message -- the same
        code twice, never two different ones."""
        provider = _Provider()
        formatter = _formatter(provider)
        notification = _notification(title="Confirm", body="482913 is...")
        for _ in range(2):
            await formatter.deliver(
                notification, _delivery(), _recipient(),
            )
        first, second = (
            httpx.QueryParams(r.content.decode()) for r in provider.requests
        )
        assert first["subject"] == second["subject"]
        assert first["text"] == second["text"]


# ---------------------------------------------------------------------------
# The recipient address
# ---------------------------------------------------------------------------


class TestRecipientAddress:
    @pytest.mark.parametrize("value", [None, "", "   "])
    async def test_missing_address_is_terminal_before_the_provider(
        self, value: str | None,
    ) -> None:
        """The snapshot may gain an address later -- retrying anyway
        would keep the notification alive to its attempt budget and eat
        the delivery-time budget of recipients who have one. A later
        address is a reason for a NEW notification, not for reviving
        this one."""
        provider = _Provider()
        with pytest.raises(PermanentDeliveryError, match="email address"):
            await _send(provider, recipient=_recipient(email=value))
        assert provider.requests == []

    @pytest.mark.parametrize(
        "value",
        ["not-an-address", "@example.test", "user@", "a b@example.test"],
    )
    async def test_broken_address_is_terminal(self, value: str) -> None:
        provider = _Provider()
        with pytest.raises(PermanentDeliveryError):
            await _send(provider, recipient=_recipient(email=value))
        assert provider.requests == []

    async def test_the_refusal_does_not_echo_the_address(self) -> None:
        """An address is personal data even when it is unusable: the
        error names the DEFECT, never the value. (The first version of
        this test fed a VALID address and passed for the wrong reason --
        nothing was raised at all.)"""
        # No @ at all -- rejected by this formatter. A double-@ value
        # is deliberately NOT rejected here: it is a syntax the provider
        # itself judges, and it lands in the 400 class.
        broken = "secret-user-no-at-sign"
        with pytest.raises(PermanentDeliveryError) as caught:
            await _send(_Provider(), recipient=_recipient(email=broken))
        assert "secret-user" not in str(caught.value)
        assert "not a usable mailbox" in str(caught.value)

    async def test_a_usable_address_is_trimmed_and_sent(self) -> None:
        provider = _Provider()
        await _send(provider, recipient=_recipient(email=" a@b.test "))
        assert provider.fields()["to"] == "a@b.test"


# ---------------------------------------------------------------------------
# End to end through the worker
# ---------------------------------------------------------------------------


async def _deliveries(
    notification_id: UUID,
) -> list[NotificationDelivery]:
    from app.core.database import get_session_factory

    factory = get_session_factory()
    async with factory() as session:
        rows = await session.execute(
            select(NotificationDelivery)
            .where(NotificationDelivery.notification_id == notification_id)
            .order_by(NotificationDelivery.channel)
        )
        return list(rows.scalars().all())


async def _status(notification_id: UUID) -> str:
    from app.core.database import get_session_factory

    factory = get_session_factory()
    async with factory() as session:
        row = await session.execute(
            select(Notification).where(Notification.id == notification_id)
        )
        return str(row.scalar_one().status)


class TestThroughThePipeline:
    async def test_a_live_channel_delivers(
        self, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        recipient.email = "pipeline@example.test"
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["email"],
        )
        await db_session.commit()

        provider = _Provider()
        with patch(
            "app.engine.service.get_formatter",
            return_value=_formatter(provider),
        ):
            await process_pending_notifications()

        (delivery,) = await _deliveries(notification.id)
        assert delivery.status == DeliveryStatus.SENT
        assert len(provider.requests) == 1

    async def test_missing_address_fails_without_spending_an_attempt(
        self, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        recipient.email = None
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["email"],
        )
        await db_session.commit()

        with patch(
            "app.engine.service.get_formatter",
            return_value=_formatter(_Provider()),
        ):
            await process_pending_notifications()

        (delivery,) = await _deliveries(notification.id)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.attempts == 0
        assert await _status(notification.id) == NotificationStatus.FAILED

    async def test_a_neighbour_in_the_same_pass_still_arrives(
        self, db_session: AsyncSession,
    ) -> None:
        """The refusal is raised by the FORMATTER, inside the per-
        delivery try: it closes one delivery and leaves the pass alone.
        app/engine/service.py is not touched by this change."""
        recipient = await create_recipient(db_session)
        recipient.email = None
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["in_app", "email"],
        )
        await db_session.commit()

        with patch(
            "app.engine.service.get_formatter",
            side_effect=lambda channel: (
                _formatter(_Provider())
                if channel == DeliveryChannel.EMAIL
                else __import__(
                    "app.engine.formatters", fromlist=["InAppFormatter"],
                ).InAppFormatter()
            ),
        ):
            await process_pending_notifications()

        by_channel = {d.channel: d for d in await _deliveries(notification.id)}
        assert by_channel[DeliveryChannel.IN_APP].status == (
            DeliveryStatus.SENT
        )
        assert by_channel[DeliveryChannel.EMAIL].status == (
            DeliveryStatus.FAILED
        )
        assert await _status(notification.id) == (
            NotificationStatus.PARTIAL_SENT
        )


# ---------------------------------------------------------------------------
# The consumers
# ---------------------------------------------------------------------------


class TestConsumersAreUntouched:
    def test_a_deploy_with_telegram_only_has_no_email(self) -> None:
        """Slice for the product that runs telegram and no email: its
        key set for email is empty, so the channel is simply absent and
        nothing about its telegram changes."""
        source = Settings(
            _env_file=None,  # type: ignore[call-arg]
            app_env="production",
            database_url="postgresql+asyncpg://u:p@db/comms_unit",
            comms_service_token="unit-test-service-token",
            telegram_bot_token="8123456789:AA-token",
            telegram_bot_url="https://telegram.me/some_bot",
        )
        mapped = channel_map(source)
        assert mapped["telegram"] == ChannelState.LIVE
        assert mapped["email"] == ChannelState.NOT_CONFIGURED

    def test_an_email_template_does_not_pull_the_channel_in(self) -> None:
        """Declaring templates for a channel says nothing about whether
        a notification uses it: the channel list comes from the request
        and from nowhere else."""
        assert registry.get_template(
            "en", "unit_event", "email", "subject",
        ) is not None

    async def test_a_declared_template_adds_no_delivery(
        self, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        notification = await create_notification(
            db_session,
            type="unit_event",
            title="T",
            body="B",
            target_type=TargetType.USER,
            target_value=str(recipient.id),
            channels=["in_app"],
        )
        await db_session.commit()

        await process_pending_notifications()

        channels = {d.channel for d in await _deliveries(notification.id)}
        assert channels == {DeliveryChannel.IN_APP}
