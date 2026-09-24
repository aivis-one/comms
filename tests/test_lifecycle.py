# =============================================================================
# COMMS Service -- Lifecycle and outcome taxonomy (F1.3)
# =============================================================================
# accepted -> queued (with a reason) -> in flight -> outcome, where the
# outcome is an enumeration, never a text (spec §5.5, §5.6):
#   - a failed delivery carries one of four classes, decided by the
#     channel exception's type in one place;
#   - a waiting delivery carries its reason; the reason leaves with the
#     wait;
#   - the job's outcome is THE FOLD of its deliveries, with suppressed
#     deliveries counted as neither;
#   - expiry and cancellation go through the deliveries; cancellation
#     matches the envelope correlation, never the letter;
#   - the provider's words reach the record on every deferral, and an
#     exception without text writes its type.
#
# THREE DOUBLE AXES per input -- in each class docstring. The mutation
# list these tests answer was written before them (see the report).
# =============================================================================

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.audience.models import Recipient
from app.audience.prefs import set_category_muted
from app.core.database import get_session_factory
from app.engine.constants import (
    DeliveryStatus,
    FailureClass,
    NotificationStatus,
    WaitReason,
)
from app.engine.formatters import (
    ConfigurationError,
    MessageRejectedError,
    NoAddressError,
    PermanentDeliveryError,
    RateLimitedError,
    build_variables,
)
from app.engine.models import Notification, NotificationDelivery
from app.engine.processor import process_pending_notifications
from app.engine.reminders import cancel_reminders
from app.engine.service import (
    close_notifications,
    create_notification,
    deliver_notification,
    resolve_notification,
    rollup_notification,
)
from app.profile.loader import RawProfile, install_profile, parse_profile
from app.profile.registry import registry
from tests.helpers import create_recipient, intake_fields

_TOKEN = "123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw"


class _Raising:
    """A formatter that raises the given exception on every call."""

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc
        self.calls = 0

    async def deliver(self, *args: Any, **kwargs: Any) -> bool:
        self.calls += 1
        raise self.exc


class _Succeeding:
    async def deliver(self, *args: Any, **kwargs: Any) -> bool:
        return True


async def _job(
    session: AsyncSession,
    *,
    type_key: str = "unit_event",
    recipients: int = 1,
    correlation: str | None = None,
    scheduled_at: datetime | None = None,
    expiry_at: datetime | None = None,
) -> tuple[Notification, list[NotificationDelivery]]:
    """An accepted, RESOLVED job for fresh recipients."""
    people = [await create_recipient(session) for _ in range(recipients)]
    notification = await create_notification(
        session, **intake_fields(), type=type_key, title="T", body="B",
        target_type="user" if recipients == 1 else "all",
        target_value=str(people[0].id) if recipients == 1 else "*",
        correlation=correlation, scheduled_at=scheduled_at,
        expiry_at=expiry_at,
    )
    deliveries = await resolve_notification(session, notification)
    return notification, deliveries


async def _deliver_with(
    session: AsyncSession, notification: Notification, formatter: Any,
) -> None:
    with patch("app.engine.service.get_formatter", return_value=formatter):
        await deliver_notification(session, notification)


# -----------------------------------------------------------------------------
# Item 1 -- the failure class
# -----------------------------------------------------------------------------


class TestFailureClass:
    """REPEAT: the same exception twice -> the same class. EMPTY: a
    transient failure -> no class. SHORTFALL: a failed row without a
    class -> refused by the database."""

    @pytest.mark.parametrize(("exc", "expected"), [
        (ConfigurationError("dead"), FailureClass.CONFIGURATION),
        (MessageRejectedError("blocked"), FailureClass.MESSAGE_REJECTED),
        (NoAddressError("no telegram_id"), FailureClass.NO_ADDRESS),
    ])
    async def test_each_channel_exception_has_its_class(
        self, db_session: AsyncSession, exc: Exception, expected: FailureClass,
    ) -> None:
        notification, (delivery,) = await _job(db_session)
        await _deliver_with(db_session, notification, _Raising(exc))
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.failure_class == expected
        assert delivery.attempts == 0

    def test_the_base_class_cannot_be_raised(self) -> None:
        with pytest.raises(TypeError, match="abstract"):
            PermanentDeliveryError("no class")

    async def test_configuration_is_loud(self, db_session: AsyncSession) -> None:
        notification, _ = await _job(db_session)
        with capture_logs() as logs:
            await _deliver_with(
                db_session, notification, _Raising(ConfigurationError("x")),
            )
        (entry,) = [e for e in logs if e["event"] == "delivery_permanent_failure"]
        assert entry["log_level"] == "error"
        assert entry["failure_class"] == "configuration"

    async def test_message_rejection_is_not_loud(
        self, db_session: AsyncSession,
    ) -> None:
        """The pair: only a dead channel is an error."""
        notification, _ = await _job(db_session)
        with capture_logs() as logs:
            await _deliver_with(
                db_session, notification, _Raising(MessageRejectedError("x")),
            )
        (entry,) = [e for e in logs if e["event"] == "delivery_permanent_failure"]
        assert entry["log_level"] == "warning"

    async def test_exhausted_transient_attempts_are_classed(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.core.config import settings

        monkeypatch.setattr(settings, "notification_max_delivery_attempts", 1)
        notification, (delivery,) = await _job(db_session)
        await _deliver_with(db_session, notification, _Raising(RuntimeError("x")))
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.failure_class == FailureClass.TRANSIENT_EXHAUSTED

    async def test_a_transient_failure_has_no_class(
        self, db_session: AsyncSession,
    ) -> None:
        notification, (delivery,) = await _job(db_session)
        await _deliver_with(db_session, notification, _Raising(RuntimeError("x")))
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.failure_class is None

    async def test_failed_without_a_class_is_refused(
        self, db_session: AsyncSession,
    ) -> None:
        _, (delivery,) = await _job(db_session)
        delivery.status = DeliveryStatus.FAILED
        with pytest.raises(IntegrityError, match="ck_deliveries_failure_class"):
            await db_session.flush()

    async def test_a_class_without_failed_is_refused(
        self, db_session: AsyncSession,
    ) -> None:
        _, (delivery,) = await _job(db_session)
        delivery.failure_class = FailureClass.NO_ADDRESS
        with pytest.raises(IntegrityError, match="ck_deliveries_failure_class"):
            await db_session.flush()


class TestDeadTelegramToken:
    """Amendment 5: a rejected bot token is a dead channel from the first
    attempt, and the attempt budget is not spent on it."""

    async def test_unauthorized_is_configuration_at_once(
        self, db_session: AsyncSession,
    ) -> None:
        from types import SimpleNamespace

        from aiogram.exceptions import TelegramUnauthorizedError

        from app.engine.formatters import TelegramFormatter

        class _Bot:
            async def send_message(self, *a: Any, **k: Any) -> None:
                raise TelegramUnauthorizedError(
                    method=SimpleNamespace(),  # type: ignore[arg-type]
                    message="Unauthorized",
                )

        formatter = TelegramFormatter(bot=_Bot(), bot_url="https://t.me/unit_bot")  # type: ignore[arg-type]
        notification, (delivery,) = await _job(db_session)
        await _deliver_with(db_session, notification, formatter)
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.failure_class == FailureClass.CONFIGURATION
        assert delivery.attempts == 0
        assert delivery.next_retry_at is None


# -----------------------------------------------------------------------------
# Item 3 -- the wait reason
# -----------------------------------------------------------------------------


class TestWaitReason:
    """REPEAT: two deferrals -> the last reason. EMPTY: waiting for the
    queue -> no reason and no time. SHORTFALL: a reason without a time
    -> refused by the database."""

    async def test_recipient_schedule(self, db_session: AsyncSession) -> None:
        later = datetime.now(UTC) + timedelta(hours=3)
        notification, (delivery,) = await _job(db_session)
        with patch("app.engine.service.recipient_deferred_until", return_value=later):
            await _deliver_with(db_session, notification, _Succeeding())
        assert delivery.status == DeliveryStatus.PENDING
        assert delivery.wait_reason == WaitReason.RECIPIENT_SCHEDULE
        assert delivery.next_retry_at == later

    async def test_provider_rate_limit(self, db_session: AsyncSession) -> None:
        notification, (delivery,) = await _job(db_session)
        await _deliver_with(
            db_session, notification, _Raising(RateLimitedError(5.0, " -- slow")),
        )
        assert delivery.wait_reason == WaitReason.PROVIDER_RATE_LIMIT
        assert delivery.next_retry_at is not None
        assert delivery.attempts == 0

    async def test_transient_backoff(self, db_session: AsyncSession) -> None:
        notification, (delivery,) = await _job(db_session)
        await _deliver_with(db_session, notification, _Raising(RuntimeError("x")))
        assert delivery.wait_reason == WaitReason.TRANSIENT_BACKOFF
        assert delivery.attempts == 1

    async def test_a_fresh_delivery_waits_for_the_queue(
        self, db_session: AsyncSession,
    ) -> None:
        _, (delivery,) = await _job(db_session)
        assert (delivery.wait_reason, delivery.next_retry_at) == (None, None)

    async def test_the_reason_leaves_with_the_wait(
        self, db_session: AsyncSession,
    ) -> None:
        """Backoff first, then the gate opens and the attempt succeeds:
        the finished delivery carries neither the time nor the reason."""
        notification, (delivery,) = await _job(db_session)
        await _deliver_with(db_session, notification, _Raising(RuntimeError("x")))
        assert delivery.wait_reason == WaitReason.TRANSIENT_BACKOFF
        delivery.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)
        await db_session.flush()
        await _deliver_with(db_session, notification, _Succeeding())
        assert delivery.status == DeliveryStatus.SENT
        assert (delivery.wait_reason, delivery.next_retry_at) == (None, None)

    async def test_the_last_deferral_names_the_reason(
        self, db_session: AsyncSession,
    ) -> None:
        notification, (delivery,) = await _job(db_session)
        await _deliver_with(db_session, notification, _Raising(RuntimeError("x")))
        delivery.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)
        await db_session.flush()
        later = datetime.now(UTC) + timedelta(hours=3)
        with patch("app.engine.service.recipient_deferred_until", return_value=later):
            await _deliver_with(db_session, notification, _Succeeding())
        assert delivery.wait_reason == WaitReason.RECIPIENT_SCHEDULE

    async def test_a_reason_without_a_time_is_refused(
        self, db_session: AsyncSession,
    ) -> None:
        _, (delivery,) = await _job(db_session)
        delivery.wait_reason = WaitReason.TRANSIENT_BACKOFF
        with pytest.raises(IntegrityError, match="ck_deliveries_wait_reason"):
            await db_session.flush()


# -----------------------------------------------------------------------------
# Items 2, 5 -- suppressed, no recipients, THE FOLD
# -----------------------------------------------------------------------------


class TestTheFold:
    """REPEAT: every delivery in one outcome, for each outcome. EMPTY:
    no deliveries. SHORTFALL: one delivery still waiting."""

    async def _fold_of(
        self, session: AsyncSession, outcomes: list[str],
    ) -> str:
        notification, deliveries = await _job(session, recipients=len(outcomes))
        for delivery, status in zip(deliveries, outcomes, strict=True):
            delivery.status = status
            if status == DeliveryStatus.FAILED:
                delivery.failure_class = FailureClass.MESSAGE_REJECTED
        await session.flush()
        await rollup_notification(session, notification)
        return notification.status

    @pytest.mark.parametrize(("outcomes", "expected"), [
        (["sent", "sent"], "sent"),
        (["failed", "failed"], "failed"),
        (["expired", "expired"], "expired"),
        (["cancelled", "cancelled"], "cancelled"),
        (["suppressed", "suppressed"], "suppressed"),
        (["sent", "failed"], "partial_sent"),
        (["sent", "expired"], "partial_sent"),
        (["sent", "cancelled"], "partial_sent"),
        (["sent", "suppressed"], "sent"),
        (["failed", "expired"], "failed"),
        (["expired", "cancelled"], "expired"),
        (["failed", "suppressed"], "failed"),
        (["sent", "pending"], "processing"),
    ])
    async def test_rule(
        self, db_session: AsyncSession, outcomes: list[str], expected: str,
    ) -> None:
        assert await self._fold_of(db_session, outcomes) == expected

    async def test_every_outcome_at_once(self, db_session: AsyncSession) -> None:
        """Children in all outcomes simultaneously -> PARTIAL_SENT."""
        assert await self._fold_of(db_session, [
            "sent", "failed", "suppressed", "expired", "cancelled",
        ]) == "partial_sent"

    async def test_all_suppressed_is_not_a_failure(
        self, db_session: AsyncSession,
    ) -> None:
        status = await self._fold_of(db_session, ["suppressed"] * 3)
        assert status == NotificationStatus.SUPPRESSED
        assert status != NotificationStatus.FAILED

    async def test_children_cascaded_away_is_no_recipients(
        self, db_session: AsyncSession,
    ) -> None:
        notification, (delivery,) = await _job(db_session)
        await db_session.execute(
            delete(Recipient).where(Recipient.id == delivery.recipient_id)
        )
        await db_session.flush()
        await rollup_notification(db_session, notification)
        assert notification.status == NotificationStatus.NO_RECIPIENTS

    async def test_empty_audience_and_all_muted_differ(
        self, db_session: AsyncSession,
    ) -> None:
        empty = await create_notification(
            db_session, **intake_fields(), type="unit_event", title="T",
            body="B", target_type="group", target_value="nobody_here",
        )
        await resolve_notification(db_session, empty)
        person = await create_recipient(db_session)
        await set_category_muted(db_session, person.id, "unit_updates", True)
        muted = await create_notification(
            db_session, **intake_fields(), type="unit_event", title="T",
            body="B", target_type="user", target_value=str(person.id),
        )
        await resolve_notification(db_session, muted)
        assert empty.status == NotificationStatus.NO_RECIPIENTS
        assert muted.status == NotificationStatus.SUPPRESSED

    async def test_an_outcome_is_not_re_decided(
        self, db_session: AsyncSession,
    ) -> None:
        notification, (delivery,) = await _job(db_session)
        notification.status = NotificationStatus.CANCELLED
        delivery.status = DeliveryStatus.SENT
        await db_session.flush()
        await rollup_notification(db_session, notification)
        assert notification.status == NotificationStatus.CANCELLED


# -----------------------------------------------------------------------------
# Items 4, 5 -- expiry and cancellation through the deliveries
# -----------------------------------------------------------------------------


class TestClosing:
    """REPEAT: a second cancel -> zero rows. EMPTY: a job with no
    correlation -> never matched. SHORTFALL: another correlation,
    another type -> untouched."""

    async def test_cancel_before_resolve(self, db_session: AsyncSession) -> None:
        person = await create_recipient(db_session)
        job = await create_notification(
            db_session, **intake_fields(), type="unit_rem_1h", title="T",
            body="B", target_type="user", target_value=str(person.id),
            correlation="booking:1",
            scheduled_at=datetime.now(UTC) + timedelta(hours=1),
        )
        assert await cancel_reminders(
            db_session, types={"unit_rem_1h"}, correlation="booking:1",
        ) == 1
        assert job.status == NotificationStatus.CANCELLED

    async def test_cancel_while_waiting_on_the_schedule(
        self, db_session: AsyncSession,
    ) -> None:
        """Resolved and held by the recipient's schedule: before F1.3
        only PENDING jobs matched, so this one could not be cancelled."""
        notification, (delivery,) = await _job(
            db_session, type_key="unit_rem_1h", correlation="booking:2",
        )
        later = datetime.now(UTC) + timedelta(hours=3)
        with patch("app.engine.service.recipient_deferred_until", return_value=later):
            await _deliver_with(db_session, notification, _Succeeding())
        assert notification.status == NotificationStatus.PROCESSING
        await cancel_reminders(
            db_session, types={"unit_rem_1h"}, correlation="booking:2",
        )
        await db_session.refresh(delivery)
        assert delivery.status == DeliveryStatus.CANCELLED
        assert (delivery.wait_reason, delivery.next_retry_at) == (None, None)
        assert notification.status == NotificationStatus.CANCELLED

    async def test_cancel_after_an_outcome_is_a_no_op(
        self, db_session: AsyncSession,
    ) -> None:
        notification, _ = await _job(
            db_session, type_key="unit_rem_1h", correlation="booking:3",
        )
        await _deliver_with(db_session, notification, _Succeeding())
        await rollup_notification(db_session, notification)
        assert notification.status == NotificationStatus.SENT
        for _ in range(2):
            assert await cancel_reminders(
                db_session, types={"unit_rem_1h"}, correlation="booking:3",
            ) == 0
        assert notification.status == NotificationStatus.SENT

    async def test_cancel_matches_the_envelope_only(
        self, db_session: AsyncSession,
    ) -> None:
        """A job whose LETTER carries the value but whose envelope does
        not is not matched; another type is not matched either."""
        person = await create_recipient(db_session)
        letter_only = await create_notification(
            db_session, **intake_fields(), type="unit_rem_1h", title="T",
            body="B", target_type="user", target_value=str(person.id),
            action_data={"booking_id": "booking:4"},
        )
        other_type = await create_notification(
            db_session, **intake_fields(), type="unit_rem_24h", title="T",
            body="B", target_type="user", target_value=str(person.id),
            correlation="booking:4",
        )
        match = await create_notification(
            db_session, **intake_fields(), type="unit_rem_1h", title="T",
            body="B", target_type="user", target_value=str(person.id),
            correlation="booking:4",
        )
        assert await cancel_reminders(
            db_session, types={"unit_rem_1h"}, correlation="booking:4",
        ) == 1
        assert match.status == NotificationStatus.CANCELLED
        assert letter_only.status == NotificationStatus.PENDING
        assert other_type.status == NotificationStatus.PENDING

    async def test_expiry_goes_through_the_deliveries(
        self, db_session: AsyncSession,
    ) -> None:
        """Half sent before the deadline -> PARTIAL_SENT, and the
        waiting delivery has its own outcome (it used to stay PENDING)."""
        notification, deliveries = await _job(
            db_session, recipients=2,
            expiry_at=datetime.now(UTC) + timedelta(hours=1),
        )
        deliveries[0].status = DeliveryStatus.SENT
        notification.expiry_at = datetime.now(UTC) - timedelta(seconds=1)
        job_id = notification.id
        await db_session.commit()
        await process_pending_notifications()
        db_session.expire_all()
        fresh = (await db_session.execute(
            select(Notification).where(Notification.id == job_id)
        )).scalar_one()
        rows = (await db_session.execute(
            select(NotificationDelivery.status).where(
                NotificationDelivery.notification_id == job_id,
            )
        )).scalars().all()
        assert fresh.status == NotificationStatus.PARTIAL_SENT
        assert sorted(rows) == ["expired", "sent"]

    async def test_cancel_in_flight_waits_for_the_attempt(self) -> None:
        """T9: the attempt holds the job's row lock; the cancel waits
        for it and closes what is still waiting afterwards."""
        factory = get_session_factory()
        async with factory() as setup:
            notification, _ = await _job(
                setup, type_key="unit_rem_1h", recipients=2,
                correlation="booking:5",
            )
            job_id = notification.id
            await setup.commit()
        async with factory() as worker, factory() as canceller:
            locked = (await worker.execute(
                select(Notification).where(Notification.id == job_id)
                .with_for_update()
            )).scalar_one()
            racing = asyncio.ensure_future(cancel_reminders(
                canceller, types={"unit_rem_1h"}, correlation="booking:5",
            ))
            await asyncio.sleep(0.2)
            assert not racing.done()  # blocked on the row lock
            first = (await worker.execute(
                select(NotificationDelivery)
                .where(NotificationDelivery.notification_id == locked.id)
                .order_by(NotificationDelivery.id).limit(1)
            )).scalar_one()
            first.status = DeliveryStatus.SENT
            await worker.commit()
            assert await asyncio.wait_for(racing, 5) == 1
            await canceller.commit()
        async with factory() as check:
            job = await check.get(Notification, job_id)
            assert job is not None
            assert job.status == NotificationStatus.PARTIAL_SENT

    async def test_close_does_not_touch_finished_jobs(
        self, db_session: AsyncSession,
    ) -> None:
        notification, _ = await _job(db_session)
        notification.status = NotificationStatus.FAILED
        await db_session.flush()
        assert await close_notifications(
            db_session, Notification.id == notification.id,
            NotificationStatus.EXPIRED,
        ) == 0


# -----------------------------------------------------------------------------
# Item 7 -- the category snapshot
# -----------------------------------------------------------------------------


class TestCategorySnapshot:
    async def test_a_removed_type_is_still_mute_gated(
        self, db_session: AsyncSession,
    ) -> None:
        notification, (delivery,) = await _job(db_session)
        assert notification.category == "unit_updates"
        await set_category_muted(
            db_session, delivery.recipient_id, "unit_updates", True,
        )
        # The type is removed from the profile after intake.
        registry.reset()
        install_profile(parse_profile(RawProfile(types={"version": 2, "types": {
            "msg.participant_message": {"category": "m"},
            "msg.support_message": {"category": "m"},
            "msg.thread_closed": {"category": "m"},
        }})), registry)
        assert registry.category_of("unit_event") is None
        await _deliver_with(db_session, notification, _Succeeding())
        assert delivery.status == DeliveryStatus.SUPPRESSED


# -----------------------------------------------------------------------------
# Item 6 -- the provider's words and a non-empty text
# -----------------------------------------------------------------------------


class TestErrorText:
    async def test_provider_words_on_every_deferral(
        self, db_session: AsyncSession,
    ) -> None:
        notification, (delivery,) = await _job(db_session)
        for words in ("first wait", "second wait"):
            await _deliver_with(db_session, notification, _Raising(
                RateLimitedError(5.0, f" -- {words}"),
            ))
            assert words in (delivery.error_message or "")
            delivery.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)
            await db_session.flush()

    async def test_no_secret_in_the_deferral_words(
        self, db_session: AsyncSession,
    ) -> None:
        notification, (delivery,) = await _job(db_session)
        await _deliver_with(db_session, notification, _Raising(
            RateLimitedError(5.0, f" -- flood {_TOKEN} slow down"),
        ))
        assert _TOKEN not in (delivery.error_message or "")
        assert "slow down" in (delivery.error_message or "")

    @pytest.mark.parametrize("exc", [KeyError(), RuntimeError()])
    async def test_an_exception_without_text_writes_its_type(
        self, db_session: AsyncSession, exc: Exception,
    ) -> None:
        notification, (delivery,) = await _job(db_session)
        await _deliver_with(db_session, notification, _Raising(exc))
        assert delivery.error_message == type(exc).__name__


# -----------------------------------------------------------------------------
# Item 4 -- the underscore reservation is gone everywhere
# -----------------------------------------------------------------------------


def test_an_underscore_variable_renders() -> None:
    notification = Notification(
        type="unit_event", title="T", body="B", target_type="all",
        target_value="*", action_data={"_ref": "R-17", "plain": "p"},
    )
    variables = build_variables(notification)
    assert variables["_ref"] == "R-17"
    assert variables["plain"] == "p"


# -----------------------------------------------------------------------------
# The database refuses the invariants directly, too (not only the ORM)
# -----------------------------------------------------------------------------


async def test_the_checks_hold_for_raw_sql(db_session: AsyncSession) -> None:
    _, (delivery,) = await _job(db_session)
    await db_session.commit()
    with pytest.raises(IntegrityError, match="ck_deliveries_wait_reason"):
        await db_session.execute(text(
            "UPDATE notification_deliveries SET status = 'sent', "
            "next_retry_at = now(), wait_reason = 'transient_backoff' "
            "WHERE id = :id"
        ), {"id": delivery.id})
    await db_session.rollback()

