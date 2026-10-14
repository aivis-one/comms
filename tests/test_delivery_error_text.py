# =============================================================================
# H8 R-C -- what a delivery keeps of a failed call: the journal's rule
# =============================================================================
#
# error_message holds the provider's answer, sanitized, or -- for comms'
# own exception -- its class and place; never comms' text, which can
# carry the letter. The rule is ONE function (app/engine/journal.py
# answer_of + answer_text), read by the delivery and the journal alike.
# The texts comms puts into its OWN refusals (a deep link it cannot
# build, a channel the deploy lacks) count as answers of the call too,
# so they carry no value of the letter: a guard reads every such raise.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  the general branch writes sanitize_error(exc) again
#                                 -> TestCommsOwnException
#   M2  the delivery's text taken around answer_text
#                                 -> TestOneRule
#   G1  a deep-link refusal interpolates startapp again
#                                 -> TestTheGuard (both tests)
# =============================================================================

import ast
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest
from sqlalchemy import select, text

from app.core.database import get_session_factory
from app.core.lanes import Lane
from app.engine import journal
from app.engine.constants import FailureClass, JournalSubject, TargetType
from app.engine.formatters import (
    ConfigurationError,
    EmailTransientError,
    MessageRejectedError,
    NoAddressError,
    RateLimitedError,
    TelegramFormatter,
)
from app.engine.models import NotificationDelivery, NotificationTransition
from app.engine.processor import process_lane
from app.engine.service import create_notification
from tests.helpers import create_recipient, intake_fields
from tests.test_formatters import BOT_URL, _FakeBot

_APP = Path(__file__).resolve().parents[1] / "app"
# What a letter variable looks like when it ends up in an exception text.
_VARIABLE = "zq8" + "LETTERVALUE" + "wk1"
# A bot token, glued at run time (a literal in its shape blocks the push).
_SECRET = "123456789" + ":" + "AAH" + "sentinel" * 4


class _Raises:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    async def deliver(self, *args: Any) -> bool:
        raise self.exc


async def _failed_once(exc: Exception) -> tuple[UUID, UUID]:
    """A normal job whose one in_app call raises `exc`; one pass."""
    async with get_session_factory()() as session:
        recipient = await create_recipient(session)
        job = await create_notification(
            session, **intake_fields(), type="unit_event_in_app",
            title="T", body="B", target_type=TargetType.USER,
            target_value=str(recipient.id),
        )
        await session.commit()
        nid = job.id
    with patch("app.engine.service.get_formatter", return_value=_Raises(exc)):
        await process_lane(Lane.NORMAL)
    async with get_session_factory()() as session:
        did = (await session.execute(
            select(NotificationDelivery.id)
            .where(NotificationDelivery.notification_id == nid)
        )).scalar_one()
    return nid, did


async def _rows_as_json(nid: UUID, did: UUID) -> str:
    async with get_session_factory()() as session:
        job = (await session.execute(text(
            "SELECT row_to_json(n)::text FROM notifications n WHERE id = :i"
        ), {"i": nid})).scalar_one()
        delivery = (await session.execute(text(
            "SELECT row_to_json(d)::text FROM notification_deliveries d "
            "WHERE id = :i"
        ), {"i": did})).scalar_one()
    assert job and delivery
    return job + "\n" + delivery


async def _texts(did: UUID) -> tuple[str | None, str | None, str | None]:
    """The delivery's error_message and its journal channel row's
    provider_text and error."""
    async with get_session_factory()() as session:
        delivery = await session.get(NotificationDelivery, did)
        assert delivery is not None
        provider_text, error = (await session.execute(
            select(NotificationTransition.provider_text, NotificationTransition.error)
            .where(
                NotificationTransition.notification_id == delivery.notification_id,
                NotificationTransition.recipient_id == delivery.recipient_id,
                NotificationTransition.channel == delivery.channel,
                NotificationTransition.subject == JournalSubject.CHANNEL,
            )
        )).one()
        message = delivery.error_message
    return message, provider_text, error


class TestCommsOwnException:
    """done-when R-C (1). M1."""

    async def test_no_letter_value_in_either_row(self) -> None:
        nid, did = await _failed_once(
            RuntimeError(f"render failed for {{name}}={_VARIABLE}"),
        )
        rows = await _rows_as_json(nid, did)
        assert _VARIABLE not in rows
        message, _, error = await _texts(did)
        # The pair: the class and the place are there, and non-empty.
        assert message and message.startswith("builtins.RuntimeError at app/")
        assert message == error


class TestProviderAnswer:
    """done-when R-C (2): the provider's words are kept, sanitized."""

    async def test_the_secret_redacted_the_words_kept(self) -> None:
        _, did = await _failed_once(
            RateLimitedError(5.0, f" -- flood {_SECRET} slow down"),
        )
        message, provider_text, _ = await _texts(did)
        assert message and _SECRET not in message
        assert "[redacted]" in message and "slow down" in message
        assert message == provider_text

    async def test_a_provider_answer_without_words_says_its_class(self) -> None:
        _, did = await _failed_once(EmailTransientError(""))
        message, provider_text, _ = await _texts(did)
        assert provider_text is None
        assert message == "app.engine.formatters.EmailTransientError"


class TestOneRule:
    """done-when R-C (3): one function, two readers -- the journal's row
    and the delivery's text agree, for an answer and for an exception of
    comms. M2."""

    @pytest.mark.parametrize(
        "exc",
        [
            MessageRejectedError("provider rejected the message (400)"),
            RuntimeError(f"formatter bug {_VARIABLE}"),
        ],
        ids=["provider", "comms"],
    )
    async def test_the_delivery_reads_the_journal_rule(self, exc: Exception) -> None:
        _, did = await _failed_once(exc)
        message, provider_text, error = await _texts(did)
        assert message is not None
        assert message == (provider_text or error)
        rule = journal.answer_of(
            exc,
            FailureClass.MESSAGE_REJECTED
            if isinstance(exc, MessageRejectedError) else None,
        )
        assert (rule.provider_text is None) == isinstance(exc, RuntimeError)


# -----------------------------------------------------------------------------
# The guard: comms' own refusals carry no value of the letter
# -----------------------------------------------------------------------------

_REFUSALS = {
    "ConfigurationError", "NoAddressError", "MessageRejectedError",
    "RateLimitedError", "EmailTransientError",
}
# What a refusal's text may interpolate: the provider's words (the
# caught exception, its status, its reason), counts and limits, and the
# deploy's own facts. A name of the letter -- an action, its parameters,
# the startapp built from them, a variable -- is none of these.
_ALLOWED_NAMES = {"exc", "status", "reason", "retry_after", "_STARTAPP_MAX_LEN"}
_ALLOWED_CALLS = {"len", "_reason_tail", "_clean_reason", "_address_defect", "float"}
_ALLOWED_ATTRIBUTES = {"self._channel", "self._reason"}


def _refusal_raises() -> list[tuple[str, int, ast.Call]]:
    found: list[tuple[str, int, ast.Call]] = []
    for path in sorted(_APP.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Raise)
                and isinstance(node.exc, ast.Call)
                and isinstance(node.exc.func, ast.Name)
                and node.exc.func.id in _REFUSALS
            ):
                found.append((path.name, node.lineno, node.exc))
    return found


def _interpolated(call: ast.Call) -> list[ast.expr]:
    values: list[ast.expr] = []
    for argument in call.args:
        for node in ast.walk(argument):
            if isinstance(node, ast.FormattedValue):
                values.append(node.value)
    return values


def _allowed(value: ast.expr) -> bool:
    if isinstance(value, ast.Name):
        return value.id in _ALLOWED_NAMES
    if isinstance(value, ast.Attribute):
        return ast.unparse(value) in _ALLOWED_ATTRIBUTES
    if isinstance(value, ast.Call) and isinstance(value.func, ast.Name):
        return value.func.id in _ALLOWED_CALLS
    return False


class TestTheGuard:
    """правка 1 of the H8 gate. G1."""

    def test_every_refusal_interpolates_only_what_is_allowed(self) -> None:
        raises = _refusal_raises()
        # The pair: the scanner finds the refusals (fourteen today).
        assert len(raises) >= 10
        offending = [
            f"{name}:{line}: {ast.unparse(value)}"
            for name, line, call in raises
            for value in _interpolated(call)
            if not _allowed(value)
        ]
        assert offending == []

    @pytest.mark.parametrize(
        "action_data",
        [
            {"action": _VARIABLE, "params": {_VARIABLE: _VARIABLE, "b": "c"}},
            {"action": "open", "params": {"id": _VARIABLE * 8}},
            {"action": "open", "params": {"id": _VARIABLE + " !"}},
        ],
        ids=["two_params", "too_long", "bad_characters"],
    )
    def test_a_deep_link_refusal_names_no_value(
        self, action_data: dict[str, Any],
    ) -> None:
        """By behaviour too: each deep-link refusal, driven with the
        letter's values, says what is wrong without repeating them."""
        formatter = TelegramFormatter(bot=_FakeBot(), bot_url=BOT_URL)
        with pytest.raises(MessageRejectedError) as caught:
            formatter.format_deep_link(action_data)
        assert _VARIABLE not in str(caught.value)
        assert str(caught.value).startswith("deep link:")

    def test_the_other_refusals_of_comms_are_covered(self) -> None:
        """The classes the guard reads are the channel refusals there
        are -- a new one added to formatters is a new name here."""
        assert {cls.__name__ for cls in (
            ConfigurationError, NoAddressError, MessageRejectedError,
            RateLimitedError, EmailTransientError,
        )} == _REFUSALS
