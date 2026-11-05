# =============================================================================
# CH-1 -- a Telegram letter Telegram will never take is refused at once
# =============================================================================
#
# Two halves:
#   - Telegram's own answers that no retry can fix ("message is too
#     long", "text must be non-empty", "text is too long") are a
#     message_rejected failure on the FIRST attempt. The answers come
#     through aiogram's own response check (BaseSession.check_response)
#     with the Bot API's JSON -- the exception is the one aiogram builds;
#   - the letter that is certain not to fit (or to be empty) is refused
#     before Telegram is called: the formatter counts a LOWER BOUND of
#     what Telegram counts (app/engine/formatters.py, the telegram letter
#     limits), so it never refuses a letter Telegram would take.
#
# The delivery is run through the real service step (deliver_notification)
# on the database: the record says failed, the attempt count, no retry.
#
# MUTATIONS these tests were written against (each turns one red):
#   C1-C3  each answer's substring out of _PERMANENT_ERRORS -> TestAnswers
#   C4  the character limit 5000                    -> TestBeforeTheCall
#   C5  counted before the markup is read           -> TestBeforeTheCall
#   C6  counted in UTF-16 units                     -> TestBeforeTheCall
#   C7  \r not dropped                              -> TestTheCount
#   C8  entities not unescaped                      -> TestTheCount
#   C9  no strip                                    -> TestTheCount
#   C10 a tag ends at the first >, quotes or not    -> TestTheCount
#   C11 no empty check                              -> TestBeforeTheCall
#   C12 no byte check                               -> TestBeforeTheCall
#   C13 the byte check off by one (>=)              -> TestBeforeTheCall
#   C14 the refusal quoting the letter              -> TestBeforeTheCall
#   C15 the document's numbers apart from the code  -> TestTheContract
# =============================================================================

import json
import re
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from aiogram import Bot
from aiogram.client.session.base import BaseSession
from aiogram.exceptions import TelegramNetworkError
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.constants import DeliveryStatus, FailureClass
from app.engine.formatters import (
    TELEGRAM_MAX_TEXT_BYTES,
    TELEGRAM_MAX_TEXT_CHARS,
    TelegramFormatter,
    telegram_text_length,
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
# A bot token's shape, glued at run time (a literal in it blocks the push).
_TOKEN = str(424242) + ":" + "T" * 35
_SENT = {
    "ok": True,
    "result": {"message_id": 1, "date": 0, "chat": {"id": 1, "type": "private"}},
}


def _refused(description: str) -> dict[str, Any]:
    return {"ok": False, "error_code": 400, "description": description}


class _BotApi(BaseSession):
    """The HTTP side of aiogram, answering with the Bot API's JSON: the
    answer goes through aiogram's own check_response, so the exception
    is the one aiogram raises for it."""

    def __init__(
        self, status: int = 200, answer: dict[str, Any] = _SENT,
        *, network_error: bool = False,
    ) -> None:
        super().__init__()
        self.status, self.answer = status, answer
        self.network_error = network_error
        self.texts: list[str] = []

    async def close(self) -> None:
        return None

    # BaseSession's own signature, timeout included.
    async def make_request(
        self, bot: Bot, method: Any, timeout: int | None = None,  # noqa: ASYNC109
    ) -> Any:
        self.texts.append(method.text)
        if self.network_error:
            raise TelegramNetworkError(method=method, message="connection reset")
        response = self.check_response(
            bot=bot, method=method, status_code=self.status,
            content=json.dumps(self.answer),
        )
        return response.result

    async def stream_content(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


async def _deliver(
    session: AsyncSession, api: _BotApi, *, title: str = "T", body: str = "B",
) -> NotificationDelivery:
    """One job of unit_plain (no template: the stored title in <b>, the
    stored body escaped) through the real service step."""
    recipient = await create_recipient(session)
    notification: Notification = await create_notification(
        session, **intake_fields(), type="unit_plain", title=title, body=body,
        target_type="user", target_value=str(recipient.id),
    )
    (delivery,) = await resolve_notification(session, notification)
    formatter = TelegramFormatter(bot=Bot(token=_TOKEN, session=api),
                                  bot_url="https://t.me/unit_bot")
    with patch("app.engine.service.get_formatter", return_value=formatter):
        await deliver_notification(session, notification)
    await session.refresh(delivery)
    return delivery


def _body_for(chars: int) -> str:
    """A body that makes the letter exactly `chars` characters once its
    markup is read: "<b>T</b>\\n\\n" is three ("T\\n\\n")."""
    return "x" * (chars - 3)


class TestAnswers:
    """Item 1: Telegram's answers that no retry fixes -> message_rejected
    on the first attempt, the reason in the record. The pair: a network
    error stays a retry. C1-C3."""

    @pytest.mark.parametrize("description", [
        "Bad Request: message is too long",
        "Bad Request: text must be non-empty",
        "Bad Request: text is too long",
    ])
    async def test_rejected_at_once(
        self, db_session: AsyncSession, description: str,
    ) -> None:
        api = _BotApi(400, _refused(description))
        delivery = await _deliver(db_session, api)
        assert len(api.texts) == 1  # Telegram was asked once
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.failure_class == FailureClass.MESSAGE_REJECTED
        assert delivery.next_retry_at is None
        assert delivery.error_message is not None
        assert description.split(": ", 1)[1] in delivery.error_message

    async def test_the_pair_a_network_error_is_retried(
        self, db_session: AsyncSession,
    ) -> None:
        api = _BotApi(network_error=True)
        delivery = await _deliver(db_session, api)
        assert len(api.texts) == 1
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.failure_class is None
        assert delivery.next_retry_at is not None

    async def test_the_pair_a_bad_request_of_another_kind_is_retried(
        self, db_session: AsyncSession,
    ) -> None:
        api = _BotApi(400, _refused("Bad Request: something else"))
        delivery = await _deliver(db_session, api)
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.failure_class is None
        assert delivery.next_retry_at is not None


class TestBeforeTheCall:
    """Item 2: a letter certain not to fit, or certain to be empty, is
    refused with Telegram not called; the refusal names a size and a
    limit, never the letter. C4-C6, C11-C14."""

    async def test_one_over_is_refused_without_a_call(
        self, db_session: AsyncSession,
    ) -> None:
        api = _BotApi()
        delivery = await _deliver(
            db_session, api, body=_body_for(TELEGRAM_MAX_TEXT_CHARS + 1))
        assert api.texts == []
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.failure_class == FailureClass.MESSAGE_REJECTED
        assert delivery.next_retry_at is None
        assert delivery.error_message == (
            "telegram: the text is at least 4097 characters once its markup "
            "is read, the limit is 4096"
        )

    async def test_exactly_the_limit_is_sent(self, db_session: AsyncSession) -> None:
        api = _BotApi()
        delivery = await _deliver(
            db_session, api, body=_body_for(TELEGRAM_MAX_TEXT_CHARS))
        assert len(api.texts) == 1
        assert len(api.texts[0]) > TELEGRAM_MAX_TEXT_CHARS  # <b></b> is in it
        assert delivery.status == DeliveryStatus.SENT

    async def test_markup_is_not_counted(self, db_session: AsyncSession) -> None:
        """Every & is &amp; as sent (five characters) and one once read."""
        api = _BotApi()
        delivery = await _deliver(
            db_session, api, body="&" * (TELEGRAM_MAX_TEXT_CHARS - 3))
        assert len(api.texts) == 1
        assert delivery.status == DeliveryStatus.SENT

    async def test_an_emoji_is_one_character(self, db_session: AsyncSession) -> None:
        """Telegram counts code points (TDLib utf8_length), not UTF-16."""
        emoji = "\U0001F44D"
        api = _BotApi()
        delivery = await _deliver(
            db_session, api, body=emoji * (TELEGRAM_MAX_TEXT_CHARS - 3))
        assert delivery.status == DeliveryStatus.SENT
        over = await _deliver(
            db_session, _BotApi(), body=emoji * (TELEGRAM_MAX_TEXT_CHARS - 2))
        assert over.status == DeliveryStatus.FAILED

    async def test_an_empty_text_is_refused_without_a_call(
        self, db_session: AsyncSession,
    ) -> None:
        api = _BotApi()
        delivery = await _deliver(db_session, api, title="  ", body=" \n ")
        assert api.texts == []
        assert delivery.failure_class == FailureClass.MESSAGE_REJECTED
        assert delivery.error_message == (
            "telegram: the text is empty once its markup is read")

    async def test_the_pair_a_word_is_not_empty(self, db_session: AsyncSession) -> None:
        api = _BotApi()
        delivery = await _deliver(db_session, api, title=" ", body="ok")
        assert delivery.status == DeliveryStatus.SENT

    @pytest.mark.parametrize(("size", "sent"), [
        (TELEGRAM_MAX_TEXT_BYTES, True),
        (TELEGRAM_MAX_TEXT_BYTES + 1, False),
    ])
    async def test_the_byte_limit_as_sent(
        self, db_session: AsyncSession, size: int, sent: bool,
    ) -> None:
        """Markup is bytes as sent and nothing once read: a template link
        whose address fills the bytes, its count far under the limit."""
        api = _BotApi()
        delivery = await _with_title(db_session, api, _link_of(size))
        assert telegram_text_length(_sent_of(size)) < TELEGRAM_MAX_TEXT_CHARS
        if sent:
            assert [len(t.encode()) for t in api.texts] == [size]  # the instrument
            assert delivery.status == DeliveryStatus.SENT
        else:
            assert api.texts == []
            assert delivery.failure_class == FailureClass.MESSAGE_REJECTED
            assert delivery.error_message == (
                f"telegram: the text is {size} bytes as sent, "
                f"the limit is {TELEGRAM_MAX_TEXT_BYTES}"
            )

    async def test_the_refusal_names_no_letter(self, db_session: AsyncSession) -> None:
        word = "zq" + "LETTER" + "k7"
        api = _BotApi()
        body = (word + " ") * ((TELEGRAM_MAX_TEXT_CHARS // len(word + " ")) + 1)
        delivery = await _deliver(db_session, api, title=word, body=body)
        assert api.texts == []
        assert delivery.error_message is not None
        assert word not in delivery.error_message


_LINK_HEAD, _LINK_TAIL = '<a href="https://x.test/', '">T</a>'
_SEAM = "\n\nB"  # the stored body "B" after the rendered title


def _link_of(size: int) -> str:
    """A rendered title -- a link -- that makes the letter `size` bytes."""
    fill = size - len((_LINK_HEAD + _LINK_TAIL + _SEAM).encode())
    return f"{_LINK_HEAD}{'p' * fill}{_LINK_TAIL}"


def _sent_of(size: int) -> str:
    return _link_of(size) + _SEAM


async def _with_title(
    session: AsyncSession, api: _BotApi, title: str,
) -> NotificationDelivery:
    """_deliver with the title as a template renders it (trusted markup,
    verbatim); every other field falls back to the stored text."""
    def render(**kwargs: Any) -> str | None:
        return title if kwargs["field"] == "title" else None

    with patch("app.engine.formatters.render", side_effect=render):
        return await _deliver(session, api)


class TestTheCount:
    """The lower bound itself, on texts the stored path cannot build:
    template markup, a quoted > in an attribute, what Telegram drops,
    the edges it trims. C7-C10."""

    @pytest.mark.parametrize(("text", "count"), [
        ("<b>bold</b> <i>it</i>", 7),
        ('<a href="https://x.test/?a=1&amp;b=2">link</a>', 4),
        ('<a href="x>y">link</a>', 4),
        ("a&amp;b&lt;c&#33;", 6),
        ("a\r\nb", 3),
        ("a\u2028b", 2),
        ("  \n text \n  ", 4),
        ("a\tb", 3),
        ("\U0001F44D\U0001F44D", 2),
    ])
    def test_counts_as_telegram_at_most(self, text: str, count: int) -> None:
        assert telegram_text_length(text) == count


class TestTheContract:
    """deploy/INTEGRATION.md names the two limits with the code's own
    numbers -- the paragraph and the constants move together. C15."""

    def test_the_numbers(self) -> None:
        (paragraph,) = re.findall(
            r"\*\*A telegram letter has Telegram's limits\.\*\*.*?(?:\n\n|\Z)",
            _DOC, flags=re.S,
        )
        assert f"{TELEGRAM_MAX_TEXT_CHARS} characters" in paragraph
        assert f"{TELEGRAM_MAX_TEXT_BYTES} bytes" in paragraph
        assert "`failed`, `message_rejected`" in paragraph
