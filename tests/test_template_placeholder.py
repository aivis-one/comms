# =============================================================================
# TPL-1 -- the placeholder in the letter, the mark in the job, the template
# fields the loader refuses
# =============================================================================
#
# The rendering grid itself is tests/test_template_missing_variable.py.
# Here:
#   - the telegram text is HTML: a placeholder is escaped, and a value
#     inside a JSON object reaches its path escaped (the objects were
#     stringified whole before -- a path into one never resolved in a
#     telegram title or body, a defect G5's fallback hid);
#   - the job's path, read by key, names the paths that did not resolve
#     on the channel's `accepted` row, and nothing else of the letter;
#   - the CHECK that holds the column's shape, with its NULL twin;
#   - the loader refuses an attribute in a path, a field inside a spec,
#     a width or precision above the limit; the renderer refuses the
#     same past the loader.
#
# MUTATIONS these tests were written against (each turns one red):
#   T1  render back to None on an unresolved path  -> TestThePath (and the grid)
#   T2  null as None                               -> the grid
#   T3  the placeholder not escaped in telegram    -> TestTelegramMarkup
#   T4  objects stringified whole again            -> TestTelegramMarkup
#   T5  a type without a template prints a hole    -> TestThePath pair
#   T6  the mark not written                       -> TestThePath
#   T7  a value of the letter in the mark          -> TestThePath
#   T8  the CHECK without its subject condition    -> TestTheCheck
#   T9  the attribute rule off                     -> TestTheLoader
#   T10 the width rule off                         -> TestTheLoader
#   T11 the limit off by one                       -> TestTheLoader
#   T12 a field inside a spec allowed              -> TestTheLoader
#   T13 the renderer without the width rule        -> TestTheRendererPastTheLoader
#   T14 the button and the subject back to their fallback
#                                                  -> test_template_missing_variable
# =============================================================================

import json
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.engine.constants import ChannelAnswer, JournalStep, JournalSubject
from app.engine.formatters import EmailFormatter, TelegramFormatter
from app.engine.models import Notification, NotificationTransition
from app.engine.processor import process_pending_notifications
from app.engine.service import create_notification
from app.engine.template_engine import render
from app.profile.loader import _leaf_complaint
from app.profile.registry import registry
from app.profile.template_fields import MAX_TEMPLATE_SPEC_WIDTH
from app.transport.events import parse_event
from app.transport.handlers import handle_event
from tests.helpers import create_recipient, intake_fields

_BASE = "/api/v1/notifications/by-key"
# A value of the letter: it must reach no mark and no log line.
_SENTINEL = "zq" + "LETTER" + "v7"


class _Bot:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def send_message(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return SimpleNamespace(message_id=1)


def _telegram() -> tuple[TelegramFormatter, _Bot]:
    bot = _Bot()
    return TelegramFormatter(bot=bot, bot_url="https://t.me/unit_bot"), bot  # type: ignore[arg-type]


def _email() -> tuple[EmailFormatter, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"id": "<tpl1@mg.test>"})

    formatter = EmailFormatter(
        client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        api_base_url="https://api.mailgun.test",
        api_key="unit-" + "not-a-key",
        domain="mg.test",
        from_address="comms@mg.test",
    )
    return formatter, requests


@contextmanager
def _channel(formatter: Any) -> Iterator[None]:
    with patch("app.engine.service.get_formatter", return_value=formatter):
        yield


async def _send(key: str, type_: str, target: UUID, **action_data: Any) -> None:
    data = {
        "v": 1, "idempotency_key": key, "type": type_, "target_type": "user",
        "target_value": str(target), "title": "Hello", "body": "World",
        **({"action_data": action_data} if action_data else {}),
    }
    parsed = parse_event({"event": "notification_request", "data": json.dumps(data)})
    async with get_session_factory()() as session:
        await handle_event(session, parsed)
        await session.commit()


async def _path(client: AsyncClient, key: str) -> list[dict[str, Any]]:
    response = await client.get(f"{_BASE}/path", params={"key": key})
    assert response.status_code == 200
    items: list[dict[str, Any]] = response.json()["items"]
    return items


def _accepted(path: list[dict[str, Any]]) -> dict[str, Any]:
    (row,) = [
        p for p in path
        if (p["subject"], p["outcome"]) == ("channel", ChannelAnswer.ACCEPTED)
    ]
    return row


class TestThePath:
    """Item 2: by key, the channel's `accepted` row names the paths that
    did not resolve; every other row, and a letter with every field
    resolved, carries null. T1, T5-T7."""

    async def test_telegram_names_the_missing_variable(
        self, bare_client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        await db_session.commit()
        key = f"tpl1:{uuid4()}"
        await _send(key, "unit_event", recipient.id, other=_SENTINEL)
        formatter, bot = _telegram()
        with _channel(formatter):
            await process_pending_notifications()
        (call,) = bot.calls
        assert call["text"] == "Hello\n\nWorld [{extra}]"
        path = await _path(bare_client, key)
        assert _accepted(path)["template_unresolved"] == ["extra"]
        assert all(
            p["template_unresolved"] is None for p in path if p is not _accepted(path)
        )
        assert _SENTINEL not in json.dumps(path)

    async def test_email_names_the_missing_paths(
        self, bare_client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        registry.register_templates("en", {"unit_event_email": {"email": {
            "subject": "{params[name]} {title}", "body": "{titel} {params[name]}",
        }}})
        recipient = await create_recipient(db_session, email="p@example.test")
        await db_session.commit()
        key = f"tpl1:{uuid4()}"
        await _send(
            key, "unit_event_email", recipient.id,
            action="open", params={"x": _SENTINEL},
        )
        formatter, requests = _email()
        with _channel(formatter):
            await process_pending_notifications()
        (request,) = requests
        fields = dict(httpx.QueryParams(request.content.decode()))
        assert fields["subject"] == "{params[name]} Hello"
        assert fields["text"] == "{titel} {params[name]}"
        path = await _path(bare_client, key)
        assert _accepted(path)["template_unresolved"] == ["params[name]", "titel"]
        assert _SENTINEL not in json.dumps(path)

    async def test_the_pair_every_field_resolved(
        self, bare_client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        await db_session.commit()
        key = f"tpl1:{uuid4()}"
        await _send(key, "unit_event", recipient.id, extra="x")
        formatter, bot = _telegram()
        with _channel(formatter):
            await process_pending_notifications()
        assert bot.calls[0]["text"] == "Hello\n\nWorld [x]"
        assert _accepted(await _path(bare_client, key))["template_unresolved"] is None

    async def test_the_pair_a_type_without_a_template_takes_the_stored_text(
        self, bare_client: AsyncClient, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        await db_session.commit()
        key = f"tpl1:{uuid4()}"
        await _send(key, "unit_plain", recipient.id)
        formatter, bot = _telegram()
        with _channel(formatter):
            await process_pending_notifications()
        assert bot.calls[0]["text"] == "<b>Hello</b>\n\nWorld"
        assert _accepted(await _path(bare_client, key))["template_unresolved"] is None


class TestTelegramMarkup:
    """The telegram text is HTML: the placeholder is escaped, a value
    inside a JSON object reaches its path escaped. T3, T4."""

    async def _text(self, template: str, **action_data: Any) -> str:
        registry.register_templates(
            "en", {"unit_event": {"telegram": {"title": "T", "body": template}}},
        )
        formatter, bot = _telegram()
        job = Notification(
            id=uuid4(), type="unit_event", lane="normal", title="Hello",
            body="World", target_type="user", target_value="*",
            action_data=action_data or None,
        )
        recipient = SimpleNamespace(telegram_id=80999, locale="en")
        await formatter.deliver(job, SimpleNamespace(), recipient)  # type: ignore[arg-type]
        text: str = bot.calls[0]["text"]
        return text

    async def test_the_placeholder_is_escaped(self) -> None:
        assert await self._text("a {<i>x}") == "T\n\na {&lt;i&gt;x}"

    async def test_a_path_into_an_object_resolves_escaped(self) -> None:
        """`params` -- the one object the intake lets in -- reaches its
        path, escaped; it printed the whole object's repr before, so the
        path did not resolve."""
        text = await self._text("{params[name]}", params={"name": "<Zo> & co"})
        assert text == "T\n\n&lt;Zo&gt; &amp; co"

    async def test_the_pair_a_number_keeps_its_spec(self) -> None:
        assert await self._text("{n:,.2f}", n=1234.5) == "T\n\n1,234.50"


class TestTheCheck:
    """ck_transitions_template_unresolved, with its NULL twin. T8."""

    async def _insert(self, session: AsyncSession, **row: Any) -> None:
        recipient = await create_recipient(session)
        job = await create_notification(
            session, **intake_fields(), type="unit_event", title="T", body="B",
            target_type="user", target_value=str(recipient.id),
        )
        session.add(NotificationTransition(
            notification_id=job.id, recipient_id=recipient.id, channel="telegram",
            step=JournalStep.DELIVER, attempt=1, **row,
        ))
        await session.flush()

    @pytest.mark.parametrize(("subject", "outcome", "value"), [
        (JournalSubject.CHANNEL, ChannelAnswer.ACCEPTED, ["x"]),
        (JournalSubject.CHANNEL, ChannelAnswer.ACCEPTED, None),
        (JournalSubject.CHANNEL, ChannelAnswer.REFUSED, None),
        (JournalSubject.DELIVERY, "sent", None),
    ])
    async def test_allowed(
        self, db_session: AsyncSession, subject: str, outcome: str, value: Any,
    ) -> None:
        await self._insert(
            db_session, subject=subject, outcome=outcome, template_unresolved=value,
        )

    @pytest.mark.parametrize(("subject", "outcome", "value"), [
        (JournalSubject.CHANNEL, ChannelAnswer.ACCEPTED, []),
        (JournalSubject.CHANNEL, ChannelAnswer.REFUSED, ["x"]),
        (JournalSubject.DELIVERY, "sent", ["x"]),
    ])
    async def test_refused(
        self, db_session: AsyncSession, subject: str, outcome: str, value: Any,
    ) -> None:
        with pytest.raises(IntegrityError) as caught:
            await self._insert(
                db_session, subject=subject, outcome=outcome,
                template_unresolved=value,
            )
        assert "ck_transitions_template_unresolved" in str(caught.value.orig)

    async def test_the_instrument_the_table_has_the_column(
        self, db_session: AsyncSession,
    ) -> None:
        rows = await db_session.execute(
            select(NotificationTransition.template_unresolved).limit(1))
        assert rows is not None


class TestTheLoader:
    """Item 3: refused at startup, each with a sentence that says why;
    the pairs pass. T9-T12."""

    @pytest.mark.parametrize(("template", "words"), [
        ("{x.__class__}", "attribute access"),
        ("{x[a].__class__.__mro__}", "attribute access"),
        ("{user.name}", "attribute access"),
        ("{x:99999999}", "above 4096"),
        ("{x:.99999999}", "above 4096"),
        ("{x:>4097}", "above 4096"),
        ("{x:04097}", "above 4096"),
        ("{a:{w}}", "inside the format spec"),
        ("{a:>{w}.{p}}", "inside the format spec"),
    ])
    def test_refused(self, template: str, words: str) -> None:
        complaint = _leaf_complaint("body", template)
        assert complaint is not None and words in complaint, complaint

    @pytest.mark.parametrize("template", [
        "{user[name]}", "{amount:,.2f}", "{x:>10}", "{x:4096}", "{x:.4096}",
        "{x!r:>5}", "plain text", "{title} {body}",
    ])
    def test_the_pair_passes(self, template: str) -> None:
        assert _leaf_complaint("body", template) is None

    def test_the_limit_is_the_documented_one(self) -> None:
        assert MAX_TEMPLATE_SPEC_WIDTH == 4096


class TestTheRendererPastTheLoader:
    """A template registered past the loader: the renderer refuses what
    the loader would, and never builds the string. T13."""

    @pytest.mark.parametrize(("template", "variables"), [
        ("{x:99999999}", {"x": "v"}),
        ("{a:{w}}", {"a": "v", "w": "99999999"}),
        ("{x.__class__}", {"x": "v"}),
    ])
    def test_the_field_is_none(self, template: str, variables: dict[str, Any]) -> None:
        registry.register_templates(
            "en", {"unit_plain": {"telegram": {"body": template}}})
        assert render("unit_plain", "telegram", "body", "en", variables) is None
