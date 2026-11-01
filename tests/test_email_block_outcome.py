# =============================================================================
# COMMS Service -- the product's own email lives until the job's outcome
# (ST-1)
# =============================================================================
#
# The `email` block of a job (EM-1) is read by the email channel at its
# attempts and by nothing else; the owner's decision of 1 November 2026
# (spec §16.3): it goes at the job's outcome, as the model's letter does,
# while title, body and action_data stay until the retention (the in_app
# inbox reads them). Held here:
#
#   1  every transition that writes an outcome -- the five writes of an
#      outcome status (app/engine/service.py forget_channel_letters):
#      the resolve's NO_RECIPIENTS and SUPPRESSED, the fold, the direct
#      close of a job not resolved yet, the ceiling's close of a job whose
#      resolve failed -- and the close of a resolved job, which folds: a
#      job with a block -> its outcome -> the block is gone, title, body
#      and action_data are there (TestEveryOutcome);
#   2  the pairs: a job not closed keeps its block; a delivery retried
#      before the outcome reads it -- the provider gets the block on the
#      retry -- and the outcome then takes it (TestBeforeTheOutcome);
#   3  the database: a job at an outcome with a block is refused by
#      ck_notifications_email_open; with NULL it is not (the CHECK's
#      twin); the CHECK's statuses are ACTIVE_STATUSES (TestTheDatabase);
#   4  the contract says the block lives until the outcome
#      (TestTheContract).
#
# Mutations (built once, by hand):
#   M4a-e  one of the five writes without forget_channel_letters
#          -> TestEveryOutcome: the database refuses that close
#   M5     forget_channel_letters without its status guard
#          -> TestBeforeTheOutcome
#   M6     the CHECK built from a literal missing a status
#          -> TestTheDatabase.test_the_checks_set_is_the_codes
# =============================================================================

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import CategoryMute
from app.core.database import get_session_factory
from app.engine import processor, service
from app.engine.constants import (
    ACTIVE_STATUSES,
    DeliveryStatus,
    NotificationStatus,
    TargetType,
)
from app.engine.models import Notification, NotificationDelivery
from tests.helpers import create_recipient, intake_fields, next_tx1_telegram_id
from tests.test_email_channel import _formatter, _Provider
from tests.test_email_letter import _deliver, _pairs, _person, _request

_DOC = (Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md").read_text(
    encoding="utf-8",
)
_BLOCK = {"subject": "the subject", "text": "the text", "html": "<p>the html</p>"}
_ACTION = {"action": "open", "params": {"id": "1"}}
_TYPE = "unit_event_in_app_email"


async def _job(target: UUID | None = None) -> UUID:
    """A job with a block, for one person (`target`, or a new one in
    the book), committed -- PENDING."""
    async with get_session_factory()() as s:
        if target is None:
            target = uuid4()
            await create_recipient(s, recipient_id=target,
                                   telegram_id=next_tx1_telegram_id())
            await s.commit()
        job = await service.create_notification(
            s, **intake_fields(), type=_TYPE, title="T", body="B",
            action_data=_ACTION, email=dict(_BLOCK),
            target_type=TargetType.USER, target_value=str(target),
        )
        await s.commit()
        return job.id


async def _locked(s: AsyncSession, job_id: UUID) -> Notification:
    return (await s.execute(
        select(Notification).where(Notification.id == job_id)
        .with_for_update().execution_options(populate_existing=True)
    )).scalar_one()


async def _row(job_id: UUID) -> Notification:
    async with get_session_factory()() as s:
        job = await s.get(Notification, job_id)
        assert job is not None
        return job


def _assert_closed_without_block(job: Notification, status: str) -> None:
    assert job.status == status
    assert job.email is None
    # The pair: the letter the inbox reads is there.
    assert (job.title, job.body, job.action_data) == ("T", "B", _ACTION)


async def _resolved(job_id: UUID) -> None:
    async with get_session_factory()() as s:
        await service.resolve_notification(s, await _locked(s, job_id))
        await s.commit()


class TestEveryOutcome:
    async def test_no_recipients(self) -> None:
        job_id = await _job(target=uuid4())  # nobody in the book
        await _resolved(job_id)
        _assert_closed_without_block(await _row(job_id),
                                     NotificationStatus.NO_RECIPIENTS)

    async def test_suppressed(self) -> None:
        rid = uuid4()
        async with get_session_factory()() as s:
            await create_recipient(s, recipient_id=rid,
                                   telegram_id=next_tx1_telegram_id())
            s.add(CategoryMute(recipient_id=rid, category="unit_updates"))
            await s.commit()
        job_id = await _job(target=rid)
        await _resolved(job_id)
        _assert_closed_without_block(await _row(job_id),
                                     NotificationStatus.SUPPRESSED)

    async def test_the_fold(self) -> None:
        job_id = await _job()
        await _resolved(job_id)
        assert (await _row(job_id)).email == _BLOCK
        async with get_session_factory()() as s:
            await s.execute(update(NotificationDelivery)
                            .where(NotificationDelivery.notification_id == job_id)
                            .values(status=DeliveryStatus.SENT))
            await service.rollup_notification(s, await _locked(s, job_id))
            await s.commit()
        _assert_closed_without_block(await _row(job_id), NotificationStatus.SENT)

    async def test_the_close_of_a_job_not_resolved(self) -> None:
        job_id = await _job()
        async with get_session_factory()() as s:
            closed = await service.close_notifications(
                s, Notification.id == job_id, NotificationStatus.CANCELLED,
            )
            await s.commit()
        assert closed == 1
        _assert_closed_without_block(await _row(job_id),
                                     NotificationStatus.CANCELLED)

    async def test_the_close_of_a_resolved_job_folds(self) -> None:
        job_id = await _job()
        await _resolved(job_id)
        async with get_session_factory()() as s:
            await service.close_notifications(
                s, Notification.id == job_id, NotificationStatus.EXPIRED,
            )
            await s.commit()
        _assert_closed_without_block(await _row(job_id), NotificationStatus.EXPIRED)

    async def test_the_ceiling_without_a_resolve(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def fails(session: Any, notification: Any) -> bool:
            return False

        monkeypatch.setattr(processor, "_resolve_at_ceiling", fails)
        job_id = await _job()
        async with get_session_factory()() as s:
            await processor._close_at_ceiling(s, await _locked(s, job_id))
            await s.commit()
        _assert_closed_without_block(await _row(job_id), NotificationStatus.FAILED)


class TestBeforeTheOutcome:
    async def test_a_job_not_closed_keeps_it(self) -> None:
        job_id = await _job()
        assert (await _row(job_id)).email == _BLOCK
        await _resolved(job_id)
        job = await _row(job_id)
        assert job.status == NotificationStatus.PROCESSING
        assert job.email == _BLOCK

    async def test_forget_on_an_active_job_is_a_no_op(self) -> None:
        job_id = await _job()
        async with get_session_factory()() as s:
            job = await _locked(s, job_id)
            await service.forget_channel_letters(s, job)
            await s.commit()
        assert (await _row(job_id)).email == _BLOCK

    async def test_a_retry_reads_it_and_the_outcome_takes_it(
        self, db_session: AsyncSession,
    ) -> None:
        """The first attempt fails at the provider (a 500: a retry); the
        job stays open with its block; the retry sends the block as it
        came, and the outcome then takes it."""
        await _person(db_session, "retry@example.test")
        data = _request(target_type="all", target_value="*")
        failing = _Provider(status=500, payload={"message": "down"})
        job_id = await _deliver(db_session, data, failing)
        job = await _row(job_id)
        assert job.status == NotificationStatus.PROCESSING
        assert job.email == data["email"]
        async with get_session_factory()() as s:
            await s.execute(update(NotificationDelivery)
                            .where(NotificationDelivery.notification_id == job_id)
                            .values(next_retry_at=datetime.now(UTC)
                                    - timedelta(minutes=1)))
            await s.commit()
        sending = _Provider()
        with patch("app.engine.service.get_formatter",
                   return_value=_formatter(sending)):
            await processor.process_pending_notifications()
        (fields,) = [dict(p) for p in _pairs(sending)]
        assert fields["subject"] == data["email"]["subject"]
        assert fields["text"] == data["email"]["text"]
        job = await _row(job_id)
        assert job.status == NotificationStatus.SENT
        assert job.email is None


class TestTheDatabase:
    async def test_a_closed_job_with_its_block_is_refused(self) -> None:
        job_id = await _job()
        with pytest.raises(IntegrityError, match="ck_notifications_email_open"):
            async with get_session_factory()() as s:
                await s.execute(update(Notification).where(Notification.id == job_id)
                                .values(status=NotificationStatus.EXPIRED))
                await s.commit()

    async def test_the_twin_a_closed_job_without_it(self) -> None:
        """The CHECK's NULL twin: the same close without a block passes."""
        job_id = await _job()
        async with get_session_factory()() as s:
            await s.execute(update(Notification).where(Notification.id == job_id)
                            .values(status=NotificationStatus.EXPIRED, email=None))
            await s.commit()
        job = await _row(job_id)
        assert (job.status, job.email) == (NotificationStatus.EXPIRED, None)

    async def test_the_checks_set_is_the_codes(self) -> None:
        """One copy: the CHECK's statuses are ACTIVE_STATUSES."""
        async with get_session_factory()() as s:
            definition = await s.scalar(text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname = 'ck_notifications_email_open' AND convalidated"
            ))
        assert definition is not None
        found = set(re.findall(r"'([a-z_]+)'::character varying", definition))
        assert found == {status.value for status in ACTIVE_STATUSES}
        assert "email IS NULL" in definition


class TestTheContract:
    def test_the_block_lives_until_the_outcome(self) -> None:
        text_ = " ".join(_DOC.split())
        assert "**The block lives until the job's outcome:**" in text_
        section = " ".join(
            _DOC[_DOC.index("## How long comms keeps things"):].split(),
        )
        assert ("**Your own email** (the `email` block, section 3a): until the "
                "job's outcome") in section
        assert ("A job's `title`, `body` and `action_data` live as long: the "
                "`in_app` inbox reads them.") in section

    def test_the_points_are_the_codes(self) -> None:
        """Every write of an outcome status in app/engine is followed by
        forget_channel_letters in the same block -- the count the
        contract's promise rests on, read from the code."""
        root = Path(__file__).resolve().parents[1] / "app" / "engine"
        writes = 0
        for path in (root / "service.py", root / "processor.py"):
            lines = path.read_text(encoding="utf-8").splitlines()
            for number, line in enumerate(lines):
                match = re.search(r"notification\.status = (\S+)", line)
                if match is None or match.group(1).endswith("PROCESSING"):
                    continue
                writes += 1
                following = "\n".join(lines[number:number + 5])
                assert "await forget_channel_letters(session, notification)" in (
                    following
                ), f"{path.name}:{number + 1}"
        assert writes == 5
