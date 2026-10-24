# =============================================================================
# P5-4 -- a message in another language: variants, requests, the feed
# =============================================================================
#
# What is held (app/variants.py header; deploy/INTEGRATION.md section 12):
#   B1 the two records -- written, replayed, refused by form, by
#      reference, by conflict; the original's bytes never change; the
#      provider and model copied from the job's answer;
#   B2 the feed in a reader's language -- every row of the order of
#      decision, in both directions; without `locale` byte for byte as
#      before; the same page with and without it;
#   B3 a variant lives and goes with its message -- the database's
#      cascades, the only deletion paths there are; forgetting keeps it;
#   B4 the contract against the code, both ways;
#   plus the insert races on real sessions and the cost of the page's
#   three reads on pristine tables with fresh statistics.
#
# MUTATIONS (one run, every one red here):
#   M1  a variant written over messages.body        -> TestTheOriginal
#   M2  a second variant replaces the first         -> test_a_second_variant_
#                                                      is_a_conflict
#   M3  every job status -> preparing               -> test_the_order_row_6
#   M4  nothing recorded -> not_needed              -> test_the_order_row_7
#   M5  the source_locale rule of row 4 removed     -> test_the_order_row_4
#   M6  the first request decides, not the latest   -> test_the_latest_request_
#                                                      decides
#   M7  the feed without locale carries the state   -> TestWithoutLocale
#   M8  ON DELETE CASCADE removed (0033)            -> TestHowLongItLives
#   M9  provider/model taken from the request body  -> test_provider_and_model_
#                                                      are_the_answers
#   M10 the unique violation not caught             -> TestTheInsertRace
#   M11 declined read as not_needed                 -> test_the_order_row_2,
#                                                      test_a_model_that_
#                                                      refused_in_words
#   3.8.0 -- fix:
#   M12 the feed reads the job by key, not by id    -> TestAKeyOutlivesItsJob
#   M13 a status removed from _BY_JOB_STATUS        -> TestTheStatusTable
#   M14 [...] instead of .get in decide_state       -> (killed only with M12:
#                                                      read by id, the status
#                                                      is in the table)
# =============================================================================

import asyncio
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import delete, event, select, text

from app.api.messaging import (
    FEED_VARIANT_FIELDS,
    VARIANT_FIELDS,
    VARIANT_REQUEST_FIELDS,
    VariantIn,
    VariantRequestIn,
)
from app.core.database import get_engine, get_session_factory
from app.core.exceptions import VariantExistsError
from app.engine.constants import NotificationStatus
from app.engine.models import LLMAnswer, LLMCall, Notification
from app.forgetting import forget_recipient
from app.messaging.constants import VariantState
from app.messaging.models import (
    Message,
    MessageVariant,
    MessageVariantRequest,
    Thread,
)
from app.variants import reader_states, write_variant
from tests.helpers import (
    create_recipient,
    next_phase4c_telegram_id,
    notification_row_fields,
    sql_shape,
)

_REPO = Path(__file__).resolve().parents[1]
_BOUND = 10.0
# Sentinels: not a substring of anything comms writes.
_ORIGINAL = "Ich kann mich seit gestern nicht einloggen -- quokka-5513"
_TEXT_EN = "I can't log in since yesterday -- quokka-5513-en"
_PROVIDER, _MODEL = "provider-ibis-77", "model-ibis-77"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


async def _recipient() -> UUID:
    async with get_session_factory()() as session:
        rid = uuid4()
        await create_recipient(
            session, recipient_id=rid, telegram_id=next_phase4c_telegram_id(),
        )
        await session.commit()
    return rid


async def _conversation(
    client: AsyncClient, direction: str = "client_to_operator",
) -> tuple[str, str, UUID]:
    """A DM thread and one message in it: (thread id, message id, its
    sender). The direction says who wrote: the client to the operator,
    or the operator to the client -- one mechanism either way."""
    person, operator = await _recipient(), await _recipient()
    created = await client.post("/api/v1/threads", json={
        "client": str(person), "operator_kind": "user",
        "operator_value": str(operator), "kind": "dm",
    })
    assert created.status_code == 200, created.text
    thread = created.json()["id"]
    sender = person if direction == "client_to_operator" else operator
    posted = await client.post(
        f"/api/v1/threads/{thread}/messages",
        json={"sender": str(sender), "body": _ORIGINAL},
    )
    assert posted.status_code == 200, posted.text
    return thread, posted.json()["id"], sender


async def _job(
    status: str = NotificationStatus.SENT,
    *,
    answered: bool = True,
    llm: bool = True,
    answer_body: str | None = '{"text": "...", "source": "de"}',
) -> str:
    """A job, by its key: llm (a call row) or not, answered or not."""
    key = f"job:{uuid4()}"
    async with get_session_factory()() as session:
        fields = notification_row_fields(channels=["llm"] if llm else ["in_app"])
        fields["idempotency_key"] = key
        job = Notification(
            type="ask.model" if llm else "unit_event",
            title=None if llm else "t", body=None if llm else "b",
            target_type=None if llm else "user",
            target_value=None if llm else "*",
            status=status, **fields,
        )
        session.add(job)
        await session.flush()
        if llm:
            session.add(LLMCall(
                notification_id=job.id, sets=["llm.gw"], letter=None,
                letter_hash="a" * 64, answer_keep_seconds=60,
            ))
            if answered:
                now = datetime.now(UTC)
                session.add(LLMAnswer(
                    notification_id=job.id, body=answer_body,
                    answered_set="llm.gw", answered_provider=_PROVIDER,
                    answered_model=_MODEL, answered_at=now,
                    keep_until=now + timedelta(days=1),
                ))
        await session.commit()
    return key


def _key() -> dict[str, str]:
    return {"Idempotency-Key": f"test:{uuid4()}"}


async def _request(
    client: AsyncClient, thread: str, message: str, locale: str, job_key: str,
    headers: dict[str, str] | None = None,
) -> Any:
    return await client.post(
        f"/api/v1/threads/{thread}/messages/{message}/variant-requests",
        json={"locale": locale, "job_key": job_key}, headers=headers or _key(),
    )


async def _variant(
    client: AsyncClient, thread: str, message: str, body: dict[str, Any],
    headers: dict[str, str] | None = None,
) -> Any:
    return await client.post(
        f"/api/v1/threads/{thread}/messages/{message}/variants",
        json=body, headers=headers or _key(),
    )


def _text(locale: str, job_key: str, source: str = "de") -> dict[str, Any]:
    return {"locale": locale, "job_key": job_key, "source_locale": source,
            "body": _TEXT_EN}


async def _feed(client: AsyncClient, thread: str, locale: str | None) -> Any:
    query = f"?locale={locale}" if locale is not None else ""
    return await client.get(f"/api/v1/threads/{thread}/messages{query}")


async def _state(client: AsyncClient, thread: str, locale: str) -> str:
    response = await _feed(client, thread, locale)
    assert response.status_code == 200, response.text
    (item,) = response.json()["items"]
    state: str = item["variant_state"]
    return state


_DIRECTIONS = ["client_to_operator", "operator_to_client"]


# ---------------------------------------------------------------------------
# B1 -- the records
# ---------------------------------------------------------------------------


class TestTheRequest:
    async def test_written_and_replayed(self, client: AsyncClient) -> None:
        thread, message, _ = await _conversation(client)
        job = await _job(NotificationStatus.PENDING, answered=False)
        headers = _key()
        first = await _request(client, thread, message, "en", job, headers)
        assert first.status_code == 200, first.text
        assert set(first.json()) == set(VARIANT_REQUEST_FIELDS)
        assert (first.json()["locale"], first.json()["job_key"]) == ("en", job)
        again = await _request(client, thread, message, "en", job, headers)
        assert again.json() == first.json()
        other = await _request(client, thread, message, "fr", job, headers)
        assert (other.status_code, other.json()["error"]["class"]) == (
            409, "conflict",
        )

    async def test_the_same_job_twice_under_another_key(
        self, client: AsyncClient,
    ) -> None:
        thread, message, _ = await _conversation(client)
        job = await _job(NotificationStatus.PENDING, answered=False)
        assert (await _request(client, thread, message, "en", job)).status_code == 200
        second = await _request(client, thread, message, "en", job)
        assert (second.status_code, second.json()["error"]["class"]) == (
            409, "conflict",
        )
        # The pair: another job for the same language is a new request.
        newer = await _job(NotificationStatus.PENDING, answered=False)
        assert (await _request(client, thread, message, "en", newer)).status_code == 200

    @pytest.mark.parametrize(
        ("what", "expected"),
        [("no_job", 422), ("not_llm", 422), ("other_thread", 404),
         ("no_thread", 404), ("no_message", 404)],
    )
    async def test_refused_by_reference(
        self, client: AsyncClient, what: str, expected: int,
    ) -> None:
        thread, message, _ = await _conversation(client)
        job = await _job(NotificationStatus.PENDING, answered=False)
        if what == "no_job":
            job = f"job:{uuid4()}"
        elif what == "not_llm":
            job = await _job(NotificationStatus.SENT, llm=False)
        elif what == "other_thread":
            thread = (await _conversation(client))[0]
        elif what == "no_thread":
            thread = str(uuid4())
        elif what == "no_message":
            message = str(uuid4())
        response = await _request(client, thread, message, "en", job)
        assert response.status_code == expected, response.text

    @pytest.mark.parametrize(
        "body",
        [
            {"locale": "", "job_key": "k"},
            {"locale": "toolong12", "job_key": "k"},
            {"locale": "en US", "job_key": "k"},
            {"locale": "\u0440\u0443", "job_key": "k"},
            {"locale": "en", "job_key": "   "},
            {"locale": "en", "job_key": "k" * 201},
            {"locale": "en"},
            {"locale": "en", "job_key": "k", "extra": 1},
        ],
    )
    async def test_refused_by_form(
        self, client: AsyncClient, body: dict[str, Any],
    ) -> None:
        thread, message, _ = await _conversation(client)
        response = await client.post(
            f"/api/v1/threads/{thread}/messages/{message}/variant-requests",
            json=body, headers=_key(),
        )
        assert response.status_code == 422, response.text

    async def test_no_key_header(self, client: Any, bare_client: Any) -> None:
        thread, message, _ = await _conversation(client)
        response = await bare_client.post(
            f"/api/v1/threads/{thread}/messages/{message}/variant-requests",
            json={"locale": "en", "job_key": "k"},
        )
        assert response.status_code == 422


class TestTheVariant:
    @pytest.mark.parametrize(
        ("form", "extra"),
        [
            ("text", {"source_locale": "de", "body": _TEXT_EN}),
            ("not_needed", {"source_locale": "en"}),
            ("declined", {"declined": True}),
            ("declined_with_source", {"declined": True, "source_locale": "de"}),
        ],
    )
    async def test_each_form_is_written(
        self, client: AsyncClient, form: str, extra: dict[str, Any],
    ) -> None:
        thread, message, _ = await _conversation(client)
        job = await _job()
        response = await _variant(
            client, thread, message, {"locale": "en", "job_key": job, **extra},
        )
        assert response.status_code == 200, response.text
        out = response.json()
        assert set(out) == set(VARIANT_FIELDS)
        assert out["body"] == extra.get("body")
        assert out["declined"] is extra.get("declined", False)
        assert out["source_locale"] == extra.get("source_locale")

    async def test_provider_and_model_are_the_answers(
        self, client: AsyncClient,
    ) -> None:
        """Copied from the job's answer, never sent: a body naming them
        is refused (closed body)."""
        thread, message, _ = await _conversation(client)
        job = await _job()
        out = (await _variant(client, thread, message, _text("en", job))).json()
        assert (out["answered_provider"], out["answered_model"]) == (
            _PROVIDER, _MODEL,
        )
        refused = await _variant(client, thread, message, {
            **_text("fr", job), "answered_provider": "mine",
        })
        assert refused.status_code == 422

    @pytest.mark.parametrize(
        "extra",
        [
            {},                                                  # no source
            {"source_locale": "de"},                             # no body
            {"source_locale": "en", "body": "x"},                # same + body
            {"declined": True, "body": "x"},                     # declined + body
            {"declined": True, "source_locale": "en"},           # declined + same
            {"source_locale": "de", "body": ""},                 # empty
            {"source_locale": "de", "body": "   "},              # blank
            {"source_locale": "de", "body": "x" * 5001},         # too long
            {"source_locale": "d e", "body": "x"},               # source form
            {"declined": "yes"},                                 # not a bool
        ],
    )
    async def test_refused_by_form(
        self, client: AsyncClient, extra: dict[str, Any],
    ) -> None:
        thread, message, _ = await _conversation(client)
        job = await _job()
        response = await _variant(
            client, thread, message, {"locale": "en", "job_key": job, **extra},
        )
        assert response.status_code == 422, response.text

    async def test_a_job_without_an_answer(self, client: AsyncClient) -> None:
        thread, message, _ = await _conversation(client)
        job = await _job(NotificationStatus.PROCESSING, answered=False)
        response = await _variant(client, thread, message, _text("en", job))
        assert response.status_code == 422
        assert "has no answer" in response.json()["error"]["message"]

    async def test_a_second_variant_is_a_conflict(
        self, client: AsyncClient,
    ) -> None:
        thread, message, _ = await _conversation(client)
        job = await _job()
        headers = _key()
        first = await _variant(client, thread, message, _text("en", job), headers)
        assert first.status_code == 200
        assert (await _variant(
            client, thread, message, _text("en", job), headers,
        )).json() == first.json()
        second = await _variant(client, thread, message, {
            **_text("en", job), "body": "another text",
        })
        assert (second.status_code, second.json()["error"]["class"]) == (
            409, "conflict",
        )
        stored = (await _feed(client, thread, "en")).json()["items"][0]
        assert stored["variant"]["body"] == _TEXT_EN
        reused = await _variant(client, thread, message, _text("fr", job), headers)
        assert reused.status_code == 409


class TestTheOriginal:
    async def test_no_write_changes_the_message(self, client: AsyncClient) -> None:
        """The message row, byte for byte, before and after every write
        of both records in every form."""
        thread, message, _ = await _conversation(client)

        async def row() -> tuple[Any, ...]:
            async with get_session_factory()() as session:
                m = await session.get(Message, UUID(message))
                assert m is not None
                return (m.thread_id, m.sender, m.body, m.idempotency_key,
                        m.fingerprint, m.created_at)

        before = await row()
        job = await _job()
        await _request(client, thread, message, "en", job)
        await _variant(client, thread, message, _text("en", job))
        await _variant(client, thread, message,
                       {"locale": "de", "job_key": job, "source_locale": "de"})
        await _variant(client, thread, message,
                       {"locale": "ar", "job_key": job, "declined": True})
        assert await row() == before
        assert before[2] == _ORIGINAL
        (item,) = (await _feed(client, thread, "en")).json()["items"]
        assert item["body"] == _ORIGINAL
        assert item["variant"]["body"] == _TEXT_EN


# ---------------------------------------------------------------------------
# B2 -- the feed in a reader's language: the order of decision
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("direction", _DIRECTIONS)
class TestTheOrder:
    async def test_the_order_row_1(self, client: AsyncClient, direction: str) -> None:
        thread, message, _ = await _conversation(client, direction)
        await _variant(client, thread, message, _text("en", await _job()))
        response = (await _feed(client, thread, "en")).json()["items"][0]
        assert response["variant_state"] == VariantState.READY
        assert set(response["variant"]) == set(FEED_VARIANT_FIELDS)
        assert response["variant"]["source_locale"] == "de"

    async def test_the_order_row_2(self, client: AsyncClient, direction: str) -> None:
        thread, message, _ = await _conversation(client, direction)
        await _request(client, thread, message, "en",
                       await _job(NotificationStatus.PENDING, answered=False))
        await _variant(client, thread, message,
                       {"locale": "en", "job_key": await _job(), "declined": True})
        assert await _state(client, thread, "en") == VariantState.IMPOSSIBLE

    async def test_the_order_row_3(self, client: AsyncClient, direction: str) -> None:
        thread, message, _ = await _conversation(client, direction)
        await _request(client, thread, message, "en",
                       await _job(NotificationStatus.FAILED, answered=False))
        await _variant(client, thread, message,
                       {"locale": "en", "job_key": await _job(),
                        "source_locale": "en"})
        assert await _state(client, thread, "en") == VariantState.NOT_NEEDED

    async def test_the_order_row_4(self, client: AsyncClient, direction: str) -> None:
        """Another variant found the message written in German: German
        needs none -- even with a failed request for it."""
        thread, message, _ = await _conversation(client, direction)
        await _variant(client, thread, message, _text("en", await _job()))
        await _request(client, thread, message, "de",
                       await _job(NotificationStatus.FAILED, answered=False))
        assert await _state(client, thread, "de") == VariantState.NOT_NEEDED

    @pytest.mark.parametrize("status", ["pending", "processing", "sent"])
    async def test_the_order_row_5(
        self, client: AsyncClient, direction: str, status: str,
    ) -> None:
        thread, message, _ = await _conversation(client, direction)
        await _request(client, thread, message, "en",
                       await _job(status, answered=status == "sent"))
        assert await _state(client, thread, "en") == VariantState.PREPARING

    @pytest.mark.parametrize("status", ["failed", "expired", "cancelled", "gone"])
    async def test_the_order_row_6(
        self, client: AsyncClient, direction: str, status: str,
    ) -> None:
        thread, message, _ = await _conversation(client, direction)
        job = await _job(
            "failed" if status == "gone" else status, answered=False,
        )
        await _request(client, thread, message, "en", job)
        if status == "gone":
            async with get_session_factory()() as session:
                await session.execute(
                    delete(Notification).where(Notification.idempotency_key == job)
                )
                await session.commit()
        assert await _state(client, thread, "en") == VariantState.IMPOSSIBLE

    async def test_the_order_row_7(self, client: AsyncClient, direction: str) -> None:
        """Nothing recorded for the language. The pair: another language
        has a variant, so the feed did read the message's records."""
        thread, message, _ = await _conversation(client, direction)
        await _variant(client, thread, message, _text("fr", await _job(),
                                                      source="it"))
        assert await _state(client, thread, "en") == VariantState.NOT_REQUESTED
        assert await _state(client, thread, "fr") == VariantState.READY

    async def test_the_latest_request_decides(
        self, client: AsyncClient, direction: str,
    ) -> None:
        thread, message, _ = await _conversation(client, direction)
        await _request(client, thread, message, "en",
                       await _job(NotificationStatus.FAILED, answered=False))
        assert await _state(client, thread, "en") == VariantState.IMPOSSIBLE
        await _request(client, thread, message, "en",
                       await _job(NotificationStatus.PENDING, answered=False))
        assert await _state(client, thread, "en") == VariantState.PREPARING
        await _request(client, thread, message, "en",
                       await _job(NotificationStatus.EXPIRED, answered=False))
        assert await _state(client, thread, "en") == VariantState.IMPOSSIBLE

    async def test_a_model_that_refused_in_words(
        self, client: AsyncClient, direction: str,
    ) -> None:
        """The job is `sent`: the model answered -- in words, refusing.
        comms reads none of it: PREPARING, until the product writes
        `declined` -> IMPOSSIBLE (the content's source of impossible)."""
        thread, message, _ = await _conversation(client, direction)
        job = await _job(answer_body='{"error": "I cannot translate this"}')
        await _request(client, thread, message, "en", job)
        assert await _state(client, thread, "en") == VariantState.PREPARING
        await _variant(client, thread, message,
                       {"locale": "en", "job_key": job, "declined": True})
        assert await _state(client, thread, "en") == VariantState.IMPOSSIBLE


class TestAKeyOutlivesItsJob:
    """3.8.0 -- fix: a request names its job by KEY, and retention frees
    the key. A later job under the same key is another job; the
    request's state is its own job's -- gone -> IMPOSSIBLE -- whatever
    the key names now. Reproduced by review over HTTP: a person-kind
    job there gave KeyError 'partial_sent' (a 500 on ?locale=), a new
    llm job brought the finished request back to PREPARING."""

    async def _request_whose_job_retention_deleted(
        self, client: AsyncClient,
    ) -> tuple[str, str]:
        thread, message, _ = await _conversation(client)
        job = await _job(NotificationStatus.FAILED, answered=False)
        assert (await _request(client, thread, message, "en", job)).status_code == 200
        async with get_session_factory()() as session:
            await session.execute(
                delete(Notification).where(Notification.idempotency_key == job)
            )
            await session.commit()
        return thread, job

    async def _job_under(
        self, key: str, *, llm: bool, status: str,
    ) -> None:
        async with get_session_factory()() as session:
            fields = notification_row_fields(
                channels=["llm"] if llm else ["in_app"],
            )
            fields["idempotency_key"] = key
            job = Notification(
                type="ask.model" if llm else "unit_event",
                title=None if llm else "t", body=None if llm else "b",
                target_type=None if llm else "user",
                target_value=None if llm else "*",
                status=status, **fields,
            )
            session.add(job)
            await session.flush()
            if llm:
                session.add(LLMCall(
                    notification_id=job.id, sets=["llm.gw"], letter=None,
                    letter_hash="a" * 64, answer_keep_seconds=60,
                ))
            await session.commit()

    async def test_the_job_gone(self, client: AsyncClient) -> None:
        thread, _ = await self._request_whose_job_retention_deleted(client)
        assert await _state(client, thread, "en") == VariantState.IMPOSSIBLE

    async def test_a_person_job_under_the_same_key(
        self, client: AsyncClient,
    ) -> None:
        """A status no llm job has -- and no 500."""
        thread, key = await self._request_whose_job_retention_deleted(client)
        await self._job_under(key, llm=False, status=NotificationStatus.PARTIAL_SENT)
        assert await _state(client, thread, "en") == VariantState.IMPOSSIBLE

    async def test_a_new_llm_job_under_the_same_key(
        self, client: AsyncClient,
    ) -> None:
        """The old request stays what its own job made it. The pair: a
        request for the NEW job -- the same key -- is a new request, and
        it is the one that prepares."""
        thread, key = await self._request_whose_job_retention_deleted(client)
        await self._job_under(key, llm=True, status=NotificationStatus.PENDING)
        assert await _state(client, thread, "en") == VariantState.IMPOSSIBLE
        message = (await _feed(client, thread, None)).json()["items"][0]["id"]
        renewed = await _request(client, thread, message, "en", key)
        assert renewed.status_code == 200, renewed.text
        assert await _state(client, thread, "en") == VariantState.PREPARING


class TestTheStatusTable:
    def test_every_status_an_llm_job_can_have(self) -> None:
        """_BY_JOB_STATUS names every status an llm job can reach: all
        of NotificationStatus but the three only a job for people can
        end in -- an llm job has ONE delivery without a recipient, so
        app/engine/service.py _fold never gives it partial_sent (two
        deliveries), suppressed or no_recipients (a person muted or
        gone). The .get default in the feed is for a status this test
        did not foresee; this test is where it shows."""
        from app.variants import _BY_JOB_STATUS

        assert set(_BY_JOB_STATUS) == set(NotificationStatus) - {
            NotificationStatus.PARTIAL_SENT,
            NotificationStatus.SUPPRESSED,
            NotificationStatus.NO_RECIPIENTS,
        }


class TestWithoutLocale:
    async def test_the_feed_is_what_it_was(self, client: AsyncClient) -> None:
        """Without `locale`: the shape the feed always had, and the same
        bytes after variants and requests are written."""
        thread, message, sender = await _conversation(client)
        before = await _feed(client, thread, None)
        body = before.json()
        assert set(body) == {"items", "next_cursor"}
        (item,) = body["items"]
        assert set(item) == {"id", "thread_id", "sender", "body", "created_at"}
        assert (item["id"], item["sender"], item["body"]) == (
            message, str(sender), _ORIGINAL,
        )
        job = await _job()
        await _request(client, thread, message, "en", job)
        await _variant(client, thread, message, _text("en", job))
        after = await _feed(client, thread, None)
        assert after.content == before.content

    async def test_the_same_page_with_and_without(
        self, client: AsyncClient,
    ) -> None:
        thread, _, sender = await _conversation(client)
        for n in range(4):
            await client.post(f"/api/v1/threads/{thread}/messages",
                              json={"sender": str(sender), "body": f"m{n}"})
        plain = (await client.get(
            f"/api/v1/threads/{thread}/messages?limit=2")).json()
        local = (await client.get(
            f"/api/v1/threads/{thread}/messages?limit=2&locale=en")).json()
        assert [i["id"] for i in plain["items"]] == [i["id"] for i in local["items"]]
        assert plain["next_cursor"] == local["next_cursor"] is not None
        assert all(i["variant_state"] == "not_requested" for i in local["items"])

    @pytest.mark.parametrize("locale", ["", "toolong12", "en%20US"])
    async def test_a_locale_of_another_form(
        self, client: AsyncClient, locale: str,
    ) -> None:
        thread, _, _ = await _conversation(client)
        response = await client.get(
            f"/api/v1/threads/{thread}/messages?locale={locale}")
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# B3 -- how long it lives
# ---------------------------------------------------------------------------


async def _counts(message: str) -> tuple[int, int]:
    async with get_session_factory()() as session:
        variants = len((await session.scalars(select(MessageVariant).where(
            MessageVariant.message_id == UUID(message)))).all())
        requests = len((await session.scalars(select(MessageVariantRequest).where(
            MessageVariantRequest.message_id == UUID(message)))).all())
    return variants, requests


class TestHowLongItLives:
    """No path of comms deletes a message or a thread (app/variants.py
    header): the database's cascades are the paths there are, and a
    variant goes with its message on each."""

    async def _written(self, client: AsyncClient) -> tuple[str, str, UUID]:
        thread, message, sender = await _conversation(client)
        job = await _job()
        await _request(client, thread, message, "en", job)
        await _variant(client, thread, message, _text("en", job))
        assert await _counts(message) == (1, 1)  # the pair
        return thread, message, sender

    async def test_a_deleted_message_takes_them(self, client: AsyncClient) -> None:
        _, message, _ = await self._written(client)
        async with get_session_factory()() as session:
            await session.execute(delete(Message).where(Message.id == UUID(message)))
            await session.commit()
        assert await _counts(message) == (0, 0)

    async def test_a_deleted_thread_takes_them(self, client: AsyncClient) -> None:
        thread, message, _ = await self._written(client)
        async with get_session_factory()() as session:
            await session.execute(delete(Thread).where(Thread.id == UUID(thread)))
            await session.commit()
        assert await _counts(message) == (0, 0)

    async def test_forgetting_the_sender_keeps_them(
        self, client: AsyncClient,
    ) -> None:
        """As it keeps the message's body: the product's decision."""
        thread, message, sender = await self._written(client)
        async with get_session_factory()() as session:
            assert await forget_recipient(
                session, recipient_id=sender, version=99,
            ) is True
            await session.commit()
        assert await _counts(message) == (1, 1)
        (item,) = (await _feed(client, thread, "en")).json()["items"]
        assert (item["body"], item["variant"]["body"]) == (_ORIGINAL, _TEXT_EN)


# ---------------------------------------------------------------------------
# The insert race -- real sessions
# ---------------------------------------------------------------------------


async def _pid(session: Any) -> int:
    pid: int = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
    return pid


async def _until_waiting(pid: list[int]) -> None:
    for _ in range(int(_BOUND / 0.02)):
        if pid:
            async with get_session_factory()() as session:
                if (await session.execute(text(
                    "SELECT count(*) FROM pg_stat_activity WHERE pid = :pid "
                    "AND wait_event_type = 'Lock' "
                    "AND datname = current_database()"
                ), {"pid": pid[0]})).scalar_one():
                    return
        await asyncio.sleep(0.02)
    raise AssertionError("the second writer never waited on a lock")


class TestTheInsertRace:
    @pytest.mark.parametrize("same_key", [True, False])
    async def test_two_writers_of_one_language(
        self, client: AsyncClient, same_key: bool,
    ) -> None:
        """The first writer's insert is uncommitted; the second finds no
        variant, inserts, waits on the unique index, and when the first
        commits: the same key -> the first's row; another key -> 409.
        One row either way."""
        thread, message, _ = await _conversation(client)
        job = await _job()
        factory = get_session_factory()
        first_key = f"race:{uuid4()}"
        second_key = first_key if same_key else f"race:{uuid4()}"
        common = {"thread_id": UUID(thread), "message_id": UUID(message),
                  "locale": "en", "job_key": job, "source_locale": "de",
                  "body": _TEXT_EN, "declined": False, "fingerprint": "f" * 64}
        async with factory() as first:
            mine = await write_variant(first, key=first_key, **common)
            pid: list[int] = []

            async def second() -> Any:
                async with factory() as session:
                    pid.append(await _pid(session))
                    row = await write_variant(session, key=second_key, **common)
                    await session.commit()
                    return row.id

            task = asyncio.create_task(second())
            await _until_waiting(pid)
            await first.commit()
            if same_key:
                assert await asyncio.wait_for(task, _BOUND) == mine.id
            else:
                with pytest.raises(VariantExistsError):
                    await asyncio.wait_for(task, _BOUND)
        assert await _counts(message) == (1, 0)


# ---------------------------------------------------------------------------
# The cost of the page's three reads -- pristine tables, fresh statistics
# ---------------------------------------------------------------------------

_MESSAGES = 20_000
_PAGE = 100
# Per page of 100: about three buffers per message for each read by a
# unique index -- the bound is the page's, whatever the tables hold.
_BUFFERS = 450


@pytest.mark.usefixtures("pristine_tables")
class TestTheCostOfThePage:
    async def _seed(self, client: AsyncClient) -> tuple[str, list[UUID]]:
        thread, message, sender = await _conversation(client)
        template = await _job(NotificationStatus.PENDING, answered=False)
        async with get_session_factory()() as session:
            await session.execute(text(
                "INSERT INTO messages (id, thread_id, sender, body, "
                "idempotency_key, fingerprint, created_at) "
                "SELECT gen_random_uuid(), :thread, :sender, 'm' || g, "
                "'seed:m:' || g, repeat('f', 64), now() - g * interval '1 s' "
                "FROM generate_series(1, :n) AS g"
            ), {"thread": UUID(thread), "sender": sender, "n": _MESSAGES})
            columns = [c for c in (await session.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'notifications' AND table_schema = 'public' "
                "ORDER BY ordinal_position"
            ))).scalars() if c not in ("id", "idempotency_key")]
            listed = ", ".join(columns)
            await session.execute(text(
                f"INSERT INTO notifications (id, idempotency_key, {listed}) "
                f"SELECT gen_random_uuid(), 'seed:job:' || m.idempotency_key, "
                f"{', '.join('t.' + c for c in columns)} "
                f"FROM messages m, notifications t "
                f"WHERE t.idempotency_key = :template AND m.thread_id = :thread"
            ), {"template": template, "thread": UUID(thread)})
            for locale in ("en", "fr"):
                await session.execute(text(
                    "INSERT INTO message_variants (id, message_id, locale, "
                    "source_locale, body, declined, job_key, idempotency_key, "
                    f"fingerprint) SELECT gen_random_uuid(), id, '{locale}', "
                    "'de', 'v', false, 'seed:job:' || idempotency_key, "
                    f"'seed:v:{locale}:' || id, repeat('f', 64) "
                    "FROM messages WHERE thread_id = :thread"
                ), {"thread": UUID(thread)})
            await session.execute(text(
                "INSERT INTO message_variant_requests (id, message_id, locale, "
                "job_key, job_id, idempotency_key, fingerprint) "
                "SELECT gen_random_uuid(), m.id, 'ar', n.idempotency_key, n.id, "
                "'seed:r:' || m.id, repeat('f', 64) FROM messages m "
                "JOIN notifications n "
                "ON n.idempotency_key = 'seed:job:' || m.idempotency_key "
                "WHERE m.thread_id = :thread"
            ), {"thread": UUID(thread)})
            await session.commit()
        async with get_engine().connect() as connection:
            autocommit = await connection.execution_options(
                isolation_level="AUTOCOMMIT",
            )
            await autocommit.execute(text("SET default_statistics_target = 1000"))
            await autocommit.execute(text("ANALYZE"))
        async with get_session_factory()() as session:
            page = list((await session.scalars(
                select(Message.id).where(Message.thread_id == UUID(thread))
                .order_by(Message.created_at.desc(), Message.id.desc())
                .limit(_PAGE)
            )).all())
        assert message  # the conversation's own message is among the rows
        return thread, page

    @pytest.mark.parametrize("locale", ["ar", "en"])
    async def test_three_reads_bounded_by_the_page(
        self, client: AsyncClient, locale: str,
    ) -> None:
        """ar: every message has a request (the jobs read runs);
        en: every message has a variant. Each read is an index scan of
        a handful of buffers per message, never a scan of the table."""
        _, page = await self._seed(client)
        seen: list[tuple[str, Any]] = []

        def record(conn: Any, cursor: Any, statement: str, parameters: Any,
                   *rest: Any) -> None:
            seen.append((statement, parameters))

        engine = get_engine().sync_engine
        event.listen(engine, "before_cursor_execute", record)
        try:
            async with get_session_factory()() as session:
                states = await reader_states(session, page, locale)
        finally:
            event.remove(engine, "before_cursor_execute", record)
        expected = (VariantState.PREPARING if locale == "ar"
                    else VariantState.READY)
        assert {state for state, _ in states.values()} == {expected}
        tables = [sql_shape(s).table for s, _ in seen]
        assert sorted(tables) == sorted(
            ["message_variants", "message_variant_requests"]
            + (["notifications"] if locale == "ar" else [])
        )
        for statement, parameters in seen:
            async with get_session_factory()() as session:
                raw = await session.connection()
                plan = (await raw.exec_driver_sql(
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement,
                    parameters,
                )).scalar_one()
                await session.rollback()
            top = plan[0]["Plan"]
            read = top["Shared Hit Blocks"] + top.get("Shared Read Blocks", 0)
            dump = json.dumps(plan)
            assert '"Node Type": "Seq Scan"' not in dump, dump
            assert read <= _BUFFERS, dump


# ---------------------------------------------------------------------------
# B4 -- the contract against the code
# ---------------------------------------------------------------------------

_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")


def _section_12() -> str:
    start = _DOC.index("\n## 12. ")
    return _DOC[start:_DOC.index("\n## ", start + 1)]


def _table_fields(after: str) -> list[str]:
    block = _section_12().split(after, 1)[1]
    rows = []
    for line in block.splitlines():
        if line.startswith("| `"):
            rows.append(re.findall(r"`([^`]+)`", line.split("|")[1])[0])
        elif rows and not line.startswith("|"):
            break
    return rows


def _answer_fields(after: str) -> list[str]:
    block = _section_12().split(after, 1)[1]
    sentence = " ".join(block.split("The answer carries", 1)[1].split(".")[0].split())
    return re.findall(r"`([a-z_]+)`", sentence)


class TestTheContract:
    def test_the_request_body_and_answer(self) -> None:
        heading = "`POST /api/v1/threads/{tid}/messages/{mid}/variant-requests`"
        assert _table_fields(heading) == list(VariantRequestIn.model_fields)
        assert _answer_fields(heading) == list(VARIANT_REQUEST_FIELDS)

    def test_the_variant_body_and_answer(self) -> None:
        heading = "`POST /api/v1/threads/{tid}/messages/{mid}/variants`"
        assert _table_fields(heading) == list(VariantIn.model_fields)
        assert _answer_fields(heading) == list(VARIANT_FIELDS)

    def test_the_feed_keys_and_states(self) -> None:
        section = _section_12()
        variant_row = next(
            line for line in section.splitlines() if line.startswith("| `variant` |")
        )
        fields = variant_row.split("|")[2].split("`ready`:", 1)[1].split(";")[0]
        assert re.findall(r"`([a-z_]+)`", fields) == list(FEED_VARIANT_FIELDS)
        order = [
            line for line in section.split("decided in this order", 1)[1]
            .splitlines() if re.match(r"\| \d \|", line)
        ]
        states = [re.findall(r"`([a-z_]+)`", row.split("|")[3])[0] for row in order]
        assert states == [
            "ready", "impossible", "not_needed", "not_needed", "preparing",
            "impossible", "not_requested",
        ]
        assert set(states) == {state.value for state in VariantState}

    def test_the_routes_are_the_apps(self) -> None:
        from app.main import app

        paths = set(app.openapi()["paths"])
        for route in ("/api/v1/threads/{thread_id}/messages/{message_id}"
                      "/variant-requests",
                      "/api/v1/threads/{thread_id}/messages/{message_id}/variants"):
            assert route in paths
        assert "?locale=" in _section_12()
        assert "locale" in [
            p["name"] for p in app.openapi()["paths"][
                "/api/v1/threads/{thread_id}/messages"]["get"]["parameters"]
        ]
