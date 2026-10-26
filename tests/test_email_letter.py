# =============================================================================
# EM-1 -- the product's own email: `email: {subject, text[, html]}`
# =============================================================================
#
# What is held here, against the code and deploy/INTEGRATION.md (section
# 7 -- the request's `email` row; section 3a -- "Your own email"):
#   1 the intake -- the grid of the block's states, each accepted or
#     refused under the key with a reason naming the field and never the
#     value; the limits at the border; the block refused on a type
#     without email among its channels; the same key with the same block
#     a duplicate, with another a conflict; the pair: no block, as before;
#   2 a string the database will not take (a NUL, a lone surrogate) in
#     ANY string of ANY event -- a request refused under the key, the
#     field named; in the key itself, the unreadable key's path; every
#     other event a malformation, the dead letter at once (EM-1 FIX);
#   3 the delivery on the real path of the engine with the provider's
#     transport faked: the provider gets subject / text / html byte for
#     byte, nothing of the template or of title / body, no html key when
#     there is none; the pair: no block, the request exactly as before;
#     every recipient of a group the same block; a muted one none;
#   4 the letter does not leak: not into the dead letter (the G4 grid,
#     tests/test_dlq_no_letter.py), not into any row but the job's, not
#     into a log line, not into an error when the provider refuses;
#   5 the contract.
#
# MUTATIONS these tests were written against (each turns one red):
#   M1  `email` in the envelope, not the letter -> the G4 grid and its
#       split (tests/test_dlq_no_letter.py)
#   M2  deliver ignores the block -> TestTheDelivery
#   M3  html sent as "" when there is none -> TestTheDelivery
#   M4  the block's subject through _clean_subject -> TestTheDelivery
#       (two spaces and a line break)
#   M5  the "email among the channels" rule taken out -> TestTheIntake
#   M6  a limit off by one -> TestTheIntake (the borders)
#   M7  create_notification not keeping the block -> TestTheDelivery
#   M8  a limit in the contract other than the constant -> TestTheContract
#   M9  a refusal quoting the value -> TestTheIntake (the reasons)
#   M10 the rule of the string the database will not take (EM-1 FIX):
#       without the surrogate -> the surrogate rows of TestUnstorable;
#       taken out of the other events -> their rows; taken out of the
#       request -> its rows (the request a database error, a dead letter)
# =============================================================================

import json
import re
from pathlib import Path
from typing import Any
from unittest.mock import patch
from urllib.parse import parse_qsl
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import CategoryMute
from app.core.constants import (
    MAX_EMAIL_HTML_LEN,
    MAX_EMAIL_SUBJECT_LEN,
    MAX_EMAIL_TEXT_LEN,
)
from app.core.database import get_session_factory
from app.core.exceptions import ValidationError
from app.engine.constants import DeliveryStatus
from app.engine.models import Notification
from app.engine.processor import process_pending_notifications
from app.engine.service import intake_outcomes_for
from app.transport.events import EMAIL_LETTER_LIMITS, parse_event
from app.transport.handlers import HandleResult, handle_event
from tests.helpers import create_recipient, next_em1_telegram_id
from tests.test_email_channel import _deliveries, _formatter, _Provider

# The llm channel's fixture: the profile with an llm type (ask.model)
# installed, for the two cases that need one.
from tests.test_llm_channel import llm_profile_dir, provider  # noqa: F401

_REPO = Path(__file__).resolve().parent.parent
_S = "SENTINEL-EM1"
_LIMITS = {
    "subject": MAX_EMAIL_SUBJECT_LEN,
    "text": MAX_EMAIL_TEXT_LEN,
    "html": MAX_EMAIL_HTML_LEN,
}


def _block(**overrides: Any) -> dict[str, Any]:
    block: dict[str, Any] = {
        "subject": f"subject {_S}",
        "text": f"text {_S}",
        "html": f"<p>html {_S}</p>",
    }
    block.update(overrides)
    return {k: v for k, v in block.items() if v is not _DROP}


_DROP = object()


def _request(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "v": 1,
        "idempotency_key": f"em1-{uuid4().hex}",
        "type": "unit_event_email",
        "target_type": "all",
        "target_value": "*",
        "title": "T",
        "body": "B",
        "email": _block(),
    }
    data.update(overrides)
    return {k: v for k, v in data.items() if v is not _DROP}


async def _handle(session: AsyncSession, data: dict[str, Any]) -> HandleResult:
    entry = {"event": "notification_request", "data": json.dumps(data)}
    return await handle_event(session, parse_event(entry))


async def _reason(session: AsyncSession, key: str) -> str:
    (outcome,) = await intake_outcomes_for(session, key)
    return str(outcome.reason)


async def _job(session: AsyncSession, key: str) -> Notification:
    job = (await session.execute(
        select(Notification).where(Notification.idempotency_key == key)
    )).scalar_one()
    return job


# ---------------------------------------------------------------------------
# 1 -- the intake
# ---------------------------------------------------------------------------


class TestTheIntake:
    async def test_accepted_and_kept(self, db_session: AsyncSession) -> None:
        data = _request()
        assert await _handle(db_session, data) is HandleResult.PROCESSED
        job = await _job(db_session, data["idempotency_key"])
        assert job.email == data["email"]
        assert (job.title, job.body) == ("T", "B")  # the other channels'

    @pytest.mark.parametrize("html", [_DROP, None])
    async def test_no_html(self, db_session: AsyncSession, html: Any) -> None:
        data = _request(email=_block(html=html))
        assert await _handle(db_session, data) is HandleResult.PROCESSED
        job = await _job(db_session, data["idempotency_key"])
        assert job.email == {"subject": f"subject {_S}", "text": f"text {_S}"}

    @pytest.mark.parametrize("block", [_DROP, None])
    async def test_no_block_as_before(
        self, db_session: AsyncSession, block: Any
    ) -> None:
        """The pair of every refusal below."""
        data = _request(email=block)
        assert await _handle(db_session, data) is HandleResult.PROCESSED
        assert (await _job(db_session, data["idempotency_key"])).email is None

    @pytest.mark.parametrize(("block", "field"), [
        ("a string", "email"),
        (["subject"], "email"),
        (7, "email"),
        (True, "email"),
        ({}, "subject"),
        (_block(subject=_DROP), "subject"),
        (_block(text=_DROP), "text"),
        (_block(subject=None), "subject"),
        (_block(extra="x"), "extra"),
        (_block(subject=""), "subject"),
        (_block(subject="   "), "subject"),
        (_block(subject=5), "subject"),
        (_block(text=""), "text"),
        (_block(text=" \n\t "), "text"),
        (_block(text=["t"]), "text"),
        (_block(text=[_S]), "text"),  # a refused value carrying the letter
        (_block(html=""), "html"),
        (_block(html="   "), "html"),
        (_block(html=1), "html"),
    ])
    async def test_refused_under_the_key(
        self, db_session: AsyncSession, block: Any, field: str
    ) -> None:
        data = _request(email=block)
        assert await _handle(db_session, data) is HandleResult.REJECTED
        reason = await _reason(db_session, data["idempotency_key"])
        assert field in reason
        assert _S not in reason  # the field, never the value
        assert await _count(db_session, data["idempotency_key"]) == 0

    @pytest.mark.parametrize("name", ["subject", "text", "html"])
    async def test_the_borders(self, db_session: AsyncSession, name: str) -> None:
        limit = _LIMITS[name]
        at = _request(email=_block(**{name: "x" * limit}))
        over = _request(email=_block(**{name: "x" * (limit + 1)}))
        assert await _handle(db_session, at) is HandleResult.PROCESSED
        assert await _handle(db_session, over) is HandleResult.REJECTED
        reason = await _reason(db_session, over["idempotency_key"])
        assert f"email.{name} exceeds {limit}" in reason

    @pytest.mark.parametrize("type_", ["unit_event_in_app", "unit_event"])
    async def test_a_type_without_email(
        self, db_session: AsyncSession, type_: str
    ) -> None:
        data = _request(type=type_)
        assert await _handle(db_session, data) is HandleResult.REJECTED
        reason = await _reason(db_session, data["idempotency_key"])
        assert "has no email channel" in reason
        # the pair: the same type without the block is accepted
        plain = _request(type=type_, email=_DROP)
        assert await _handle(db_session, plain) is HandleResult.PROCESSED

    @pytest.mark.usefixtures("provider")
    async def test_an_llm_type(self, db_session: AsyncSession) -> None:
        data = {
            "v": 1, "idempotency_key": f"em1-{uuid4().hex}",
            "type": "ask.model",
            "llm": {"messages": [{"role": "user", "content": "q"}]},
            "email": _block(),
        }
        assert await _handle(db_session, data) is HandleResult.REJECTED
        reason = await _reason(db_session, data["idempotency_key"])
        assert "has no email channel" in reason

    async def test_title_and_body_stay_required(
        self, db_session: AsyncSession
    ) -> None:
        data = _request(title=_DROP, body=_DROP)
        assert await _handle(db_session, data) is HandleResult.REJECTED
        reason = await _reason(db_session, data["idempotency_key"])
        assert "title" in reason and "body" in reason

    async def test_a_repeat(self, db_session: AsyncSession) -> None:
        data = _request()
        assert await _handle(db_session, data) is HandleResult.PROCESSED
        assert await _handle(db_session, data) is HandleResult.DUPLICATE
        other = dict(data, email=_block(text="another text"))
        assert await _handle(db_session, other) is HandleResult.CONFLICT
        assert (await _job(db_session, data["idempotency_key"])).email == (
            data["email"]
        )

    async def test_a_line_break_in_the_subject_is_kept(
        self, db_session: AsyncSession
    ) -> None:
        subject = "two  spaces\r\nand a break"
        data = _request(email=_block(subject=subject))
        assert await _handle(db_session, data) is HandleResult.PROCESSED
        job = await _job(db_session, data["idempotency_key"])
        assert job.email is not None
        assert job.email["subject"] == subject


async def _count(session: AsyncSession, key: str) -> int:
    return len((await session.execute(
        select(Notification.id).where(Notification.idempotency_key == key)
    )).all())


# ---------------------------------------------------------------------------
# 2 -- a string the database will not take, in any event (EM-1 FIX)
# ---------------------------------------------------------------------------

# The two kinds of the class: a NUL, and a lone surrogate (valid JSON,
# a Python string, no UTF-8 for it).
_BAD = {"nul": "\x00", "surrogate": "\ud800"}
_UNSTORABLE_SAID = "is a string the database cannot store"


def _clean(reason: str) -> None:
    """The reason is itself written down -- a refusal under the key, a
    dead letter -- so it carries neither kind."""
    assert "\x00" not in reason
    reason.encode("utf-8")


@pytest.mark.parametrize("bad", sorted(_BAD))
class TestUnstorable:
    @pytest.mark.parametrize(("field", "overrides"), [
        ("email", lambda b: {"email": _block(subject=f"a{b}b")}),
        ("email", lambda b: {"email": _block(text=f"a{b}b")}),
        ("email", lambda b: {"email": _block(html=f"<p>{b}</p>")}),
        ("title", lambda b: {"title": f"T{b}"}),
        ("body", lambda b: {"body": f"B{b}"}),
        ("action_data", lambda b: {"action_data": {"params": {"p": f"x{b}"}}}),
        ("action_data", lambda b: {"action_data": {f"var{b}": "x"}}),
        ("correlation", lambda b: {"correlation": f"c{b}"}),
        ("type", lambda b: {"type": f"unit{b}event"}),
        ("target_value", lambda b: {"target_value": f"*{b}"}),
        ("scheduled_at", lambda b: {"scheduled_at": f"2030{b}"}),
    ])
    async def test_a_request_refused_under_the_key(
        self, db_session: AsyncSession, bad: str, field: str, overrides: Any,
    ) -> None:
        data = _request(**overrides(_BAD[bad]))
        assert await _handle(db_session, data) is HandleResult.REJECTED
        reason = await _reason(db_session, data["idempotency_key"])
        assert f"field {field!r} {_UNSTORABLE_SAID}" in reason
        _clean(reason)

    @pytest.mark.usefixtures("provider")
    async def test_in_an_llm_letter(self, db_session: AsyncSession, bad: str) -> None:
        data = {
            "v": 1, "idempotency_key": f"em1-{uuid4().hex}",
            "type": "ask.model",
            "llm": {"messages": [{"role": "user", "content": f"q{_BAD[bad]}"}]},
        }
        assert await _handle(db_session, data) is HandleResult.REJECTED
        reason = await _reason(db_session, data["idempotency_key"])
        assert f"field 'llm' {_UNSTORABLE_SAID}" in reason
        _clean(reason)

    async def test_in_a_field_name(self, db_session: AsyncSession, bad: str) -> None:
        data = _request()
        data[f"tit{_BAD[bad]}le"] = "x"
        assert await _handle(db_session, data) is HandleResult.REJECTED
        reason = await _reason(db_session, data["idempotency_key"])
        assert f"a field name {_UNSTORABLE_SAID}" in reason
        _clean(reason)

    def test_in_the_key_the_key_is_unreadable(self, bad: str) -> None:
        """Nothing to record a refusal under: parse_event raises -- the
        dead letter's path (spec §6.3, tests/test_dlq_no_letter.py)."""
        data = _request(idempotency_key=f"key{_BAD[bad]}")
        with pytest.raises(ValidationError, match="idempotency_key") as raised:
            parse_event({"event": "notification_request",
                         "data": json.dumps(data)})
        _clean(str(raised.value))

    @pytest.mark.parametrize(("event", "field"), [
        ("user_upserted", "locale"),
        ("user_upserted", "email"),
        ("user_deleted", "recipient_id"),
        ("group_changed", "group_key"),
        ("section_membership_changed", "section_label"),
        ("reminder_cancel", "correlation"),
        ("reminder_cancel", "types"),
    ])
    def test_every_other_event(self, bad: str, event: str, field: str) -> None:
        """A malformation of the event: parse_event raises -- the dead
        letter at once (tests/test_dlq_no_letter.py TestUnstorable) --
        naming the field. Never a database error and its retries."""
        data = _other_event(event)
        value = data[field]
        data[field] = (
            [f"{value[0]}{_BAD[bad]}"] if isinstance(value, list)
            else f"{value}{_BAD[bad]}"
        )
        with pytest.raises(ValidationError) as raised:
            parse_event({"event": event, "data": json.dumps(data)})
        assert f"field {field!r} {_UNSTORABLE_SAID}" in str(raised.value)
        _clean(str(raised.value))

    @pytest.mark.parametrize("event", sorted(
        ["user_upserted", "user_deleted", "group_changed",
         "section_membership_changed", "reminder_cancel"],
    ))
    def test_a_field_name_of_every_other_event(
        self, bad: str, event: str
    ) -> None:
        data = _other_event(event)
        data[f"no{_BAD[bad]}te"] = "x"
        with pytest.raises(ValidationError) as raised:
            parse_event({"event": event, "data": json.dumps(data)})
        assert f"a field name {_UNSTORABLE_SAID}" in str(raised.value)
        _clean(str(raised.value))


class TestStorable:
    """The pair: the same fields without either kind are accepted."""

    async def test_a_request(self, db_session: AsyncSession) -> None:
        data = _request(
            action_data={"params": {"p": "x"}, "var": "y"},
            correlation="c", email=_block(subject="a b"),
        )
        assert await _handle(db_session, data) is HandleResult.PROCESSED

    @pytest.mark.parametrize("event", sorted(
        ["user_upserted", "user_deleted", "group_changed",
         "section_membership_changed", "reminder_cancel"],
    ))
    def test_every_other_event(self, event: str) -> None:
        parse_event({"event": event, "data": json.dumps(_other_event(event))})


def _other_event(event: str) -> dict[str, Any]:
    """A well-formed payload of each event other than the request."""
    rid = str(uuid4())
    return {
        "user_upserted": {
            "v": 1, "recipient_id": rid, "version": 1, "telegram_id": None,
            "email": "one@example.test", "locale": "en",
            "timezone": "Europe/Berlin", "active": True,
        },
        "user_deleted": {"v": 1, "recipient_id": rid, "version": 2},
        "group_changed": {
            "v": 1, "group_key": "g1", "recipient_id": rid, "member": True,
        },
        "section_membership_changed": {
            "v": 1, "section_key": "support", "section_label": "Support",
            "operator_id": rid, "member": True,
        },
        "reminder_cancel": {
            "v": 1, "types": ["unit_rem_1h"], "correlation": "c-1",
        },
    }[event]


# ---------------------------------------------------------------------------
# 3 -- the delivery, on the real path of the engine
# ---------------------------------------------------------------------------


async def _person(session: AsyncSession, email: str) -> UUID:
    rid = uuid4()
    recipient = await create_recipient(
        session, recipient_id=rid, telegram_id=next_em1_telegram_id(),
    )
    recipient.email = email
    return rid


def _pairs(mailgun: _Provider) -> list[list[tuple[str, str]]]:
    return [
        parse_qsl(request.content.decode(), keep_blank_values=True)
        for request in mailgun.requests
    ]


async def _deliver(
    session: AsyncSession, data: dict[str, Any], mailgun: _Provider
) -> UUID:
    assert await _handle(session, data) is HandleResult.PROCESSED
    job = await _job(session, data["idempotency_key"])
    await session.commit()
    with patch(
        "app.engine.service.get_formatter",
        return_value=_formatter(mailgun),
    ):
        await process_pending_notifications()
    return job.id


_TRACKING = [
    ("o:tracking", "no"), ("o:tracking-clicks", "no"),
    ("o:tracking-opens", "no"),
]


class TestTheDelivery:
    async def test_the_block_as_it_came(self, db_session: AsyncSession) -> None:
        await _person(db_session, "one@example.test")
        subject = f"two  spaces\nand a break {_S}"
        data = _request(
            title=f"T-{_S}-title", body=f"B-{_S}-body",
            email=_block(subject=subject),
        )
        mailgun = _Provider()
        job_id = await _deliver(db_session, data, mailgun)
        (fields,) = [dict(p) for p in _pairs(mailgun)]
        assert fields["subject"] == subject  # not cleaned, not templated
        assert fields["text"] == f"text {_S}"
        assert fields["html"] == f"<p>html {_S}</p>"
        everything = json.dumps(fields)
        assert "COMMS:" not in everything      # the profile's template
        assert "-title" not in everything      # the job's title
        assert "-body" not in everything       # the job's body
        assert set(fields) == {
            "from", "to", "subject", "text", "html",
            "o:tracking", "o:tracking-clicks", "o:tracking-opens",
        }
        (delivery,) = await _deliveries(job_id)
        assert delivery.status == DeliveryStatus.SENT

    async def test_no_html_no_key(self, db_session: AsyncSession) -> None:
        await _person(db_session, "one@example.test")
        mailgun = _Provider()
        await _deliver(db_session, _request(email=_block(html=_DROP)), mailgun)
        (fields,) = [dict(p) for p in _pairs(mailgun)]
        assert "html" not in fields
        assert fields["text"] == f"text {_S}"

    async def test_without_the_block_exactly_as_before(
        self, db_session: AsyncSession
    ) -> None:
        """The pair: the request to the provider, field by field and in
        order, as the composition before EM-1 made it -- the profile's
        subject template over the title, the stored body."""
        await _person(db_session, "one@example.test")
        mailgun = _Provider()
        await _deliver(db_session, _request(email=_DROP), mailgun)
        (pairs,) = _pairs(mailgun)
        assert pairs == [
            ("from", _formatter(_Provider())._from_address),
            ("to", "one@example.test"),
            ("subject", "COMMS: T"),
            ("text", "B"),
            *_TRACKING,
        ]

    async def test_every_recipient_the_same_block(
        self, db_session: AsyncSession
    ) -> None:
        await _person(db_session, "one@example.test")
        await _person(db_session, "two@example.test")
        mailgun = _Provider()
        await _deliver(db_session, _request(), mailgun)
        sent = [dict(p) for p in _pairs(mailgun)]
        assert sorted(f["to"] for f in sent) == [
            "one@example.test", "two@example.test",
        ]
        for fields in sent:
            assert (fields["subject"], fields["text"], fields["html"]) == (
                f"subject {_S}", f"text {_S}", f"<p>html {_S}</p>",
            )

    async def test_a_muted_recipient_gets_none(
        self, db_session: AsyncSession
    ) -> None:
        muted = await _person(db_session, "muted@example.test")
        await _person(db_session, "open@example.test")
        db_session.add(CategoryMute(recipient_id=muted, category="unit_updates"))
        mailgun = _Provider()
        await _deliver(db_session, _request(), mailgun)
        assert [dict(p)["to"] for p in _pairs(mailgun)] == ["open@example.test"]


# ---------------------------------------------------------------------------
# 4 -- the letter does not leak
# ---------------------------------------------------------------------------

_TABLES_WITHOUT_THE_LETTER = (
    "notification_deliveries", "notification_transitions", "intake_outcomes",
    "push_outbox",
)


class TestNoLeak:
    async def test_a_refusing_provider(self, db_session: AsyncSession) -> None:
        await _person(db_session, "one@example.test")
        mailgun = _Provider(status=400, payload={"message": "bad recipient"})
        with capture_logs() as logs:
            job_id = await _deliver(db_session, _request(), mailgun)
        (delivery,) = await _deliveries(job_id)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.error_message  # the pair: the error is there
        assert _S not in delivery.error_message
        assert _S not in json.dumps(logs, default=str)
        async with get_session_factory()() as s:
            for table in _TABLES_WITHOUT_THE_LETTER:
                rows = (await s.execute(
                    text(f"SELECT row_to_json(t)::text FROM {table} t")
                )).scalars().all()
                assert all(_S not in row for row in rows), table
            transitions = (await s.execute(
                text("SELECT count(*) FROM notification_transitions")
            )).scalar_one()
        assert transitions  # the pair: the journal was written


# ---------------------------------------------------------------------------
# 5 -- the contract
# ---------------------------------------------------------------------------

_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


class TestTheContract:
    def test_the_row_of_section_7(self) -> None:
        row = next(line for line in _DOC.splitlines()
                   if line.startswith("| `email` | optional | letter |"))
        documented = {
            name: int(limit)
            for name, limit in re.findall(r"`([a-z]+)` \(1\.\.(\d+)", row)
        }
        assert documented == dict(EMAIL_LETTER_LIMITS) == _LIMITS
        assert "`null` is no block" in row

    def test_title_and_body_and_the_unstorable_string(self) -> None:
        start = _DOC.index("### `notification_request` -- a job")
        part = _DOC[start:_DOC.index("\n### ", start + 1)]
        assert "**`title` and `body` stay required with an `email` " \
               "block.**" in part
        assert "the email\ndoes not use them" in part
        nul = part.split("**A string the database cannot store", 1)[1]
        nul = nul.split("\n\n", 1)[0]
        for said in ("a NUL character", "a lone surrogate",
                     "in any\nevent is rejected at intake",
                     "rejected under its\nkey",
                     "makes the key unreadable", "dead-letter stream at once"):
            assert said in nul, said
        for event in ("user_upserted", "user_deleted", "group_changed",
                      "section_membership_changed", "reminder_cancel"):
            assert f"`{event}`" in nul, event

    def test_your_own_email(self) -> None:
        part = _DOC.split("**Your own email.**", 1)[1].split("\n\n", 1)[0]
        for said in ("exactly these", "the HTML is not sanitized",
                     "a subject with a\nline break goes as it is",
                     "One block\nper job", "byte\nfor byte as before"):
            assert said in part, said
