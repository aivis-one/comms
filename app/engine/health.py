# =============================================================================
# COMMS Service -- Channel health over a window (P2-4, spec §6.7)
# =============================================================================
#
# THE SECOND ANSWER. GET /health says what a deploy HAS: every channel's
# key set (live / not_configured / not_implemented). That answer is true
# and was not enough -- at the aivis install it said `email: live` while
# every letter died on a 401. This module gives the other half: what each
# channel DID in the last N minutes, counted from the channel answers the
# transition journal already records (app/engine/journal.py).
#
# DERIVED, NEVER STORED. Nothing here writes; there is no "channel
# broken" flag and no table or column of channel health (decision R-1a, spec §6.7
# rejected the stored mark). Every answer is a count over journal rows.
#
# WHAT IS COUNTED: rows with subject `channel` only -- one row per call
# a channel answered. Job, delivery and gate rows are transitions of
# comms, not answers of a channel. RETRIES COUNT, ON PURPOSE: each row is
# a call the channel answered. A refusal by configuration is permanent
# and never retried (one row per delivery), so the headline figure's
# numerator is not inflated; transient retries grow the denominator,
# which is what the channel really did.
#
# THE HEADLINE FIGURE: configuration_share = refusals of class
# `configuration` / all answers. No answers in the window -> null, never
# 0 (that would read "healthy") and never 1 (that would read "dead").
#
# THE WINDOW is measured by the DATABASE clock (now() of the reading
# transaction), the clock that wrote `at`, and is closed at both ends:
# [now - minutes, now].
# =============================================================================

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.constants import HEALTH_WINDOW_MAX_MINUTES
from app.core.exceptions import ValidationError
from app.engine.constants import (
    ChannelAnswer,
    DeliveryChannel,
    FailureClass,
    JournalSubject,
)
from app.engine.formatters import channel_map
from app.engine.models import NotificationTransition

# The classes a channel refusal can carry: exactly the classes
# app/engine/service.py _failure_class_of maps a PermanentDeliveryError
# to (a test pins the two together). TRANSIENT_EXHAUSTED and PIPELINE
# are decided after the channel answered, on the delivery -- never on a
# channel row.
REFUSAL_CLASSES: tuple[FailureClass, ...] = (
    FailureClass.CONFIGURATION,
    FailureClass.MESSAGE_REJECTED,
    FailureClass.NO_ADDRESS,
)


def health_window(value: int) -> int:
    """The requested window in minutes, or a refusal -- never a clamp."""
    if not 1 <= value <= HEALTH_WINDOW_MAX_MINUTES:
        raise ValidationError(
            f"window_minutes must be between 1 and "
            f"{HEALTH_WINDOW_MAX_MINUTES}, got {value}"
        )
    return value


def _zero_outcomes() -> dict[str, int]:
    return {answer.value: 0 for answer in ChannelAnswer}


def _zero_classes() -> dict[str, int]:
    return {cls.value: 0 for cls in REFUSAL_CLASSES}


@dataclass
class _Counts:
    """One channel's answers in the window. Every key is present from
    the start, so "no such answer" is a 0 and never a missing key."""

    answers: int = 0
    by_outcome: dict[str, int] = field(default_factory=_zero_outcomes)
    refused_by_class: dict[str, int] = field(default_factory=_zero_classes)

    def wire(self, state: str) -> dict[str, Any]:
        configuration = self.refused_by_class[FailureClass.CONFIGURATION]
        return {
            "state": state,
            "answers": self.answers,
            "by_outcome": dict(self.by_outcome),
            "refused_by_class": dict(self.refused_by_class),
            "configuration_share": (
                configuration / self.answers if self.answers else None
            ),
        }


def window_counts(window_from: datetime, window_to: datetime) -> Select[Any]:
    """The one aggregate: channel answers in [window_from, window_to],
    grouped by (channel, outcome, failure_class). A function of its own
    so the suite can EXPLAIN the very statement the route runs."""
    return (
        select(
            NotificationTransition.channel,
            NotificationTransition.outcome,
            NotificationTransition.failure_class,
            func.count(),
        )
        .where(
            NotificationTransition.subject == JournalSubject.CHANNEL,
            NotificationTransition.at >= window_from,
            NotificationTransition.at <= window_to,
        )
        .group_by(
            NotificationTransition.channel,
            NotificationTransition.outcome,
            NotificationTransition.failure_class,
        )
    )


async def channel_health(
    session: AsyncSession, window_minutes: int,
) -> dict[str, Any]:
    """Every channel's answers over the last `window_minutes`.

    Reads only. One aggregate over the journal's channel rows in the
    window, grouped by (channel, outcome, failure_class) -- a range scan
    of ix_transitions_channel_window (migration 0018).

    KNOWN CEILING (acknowledged by design -- P2-4, the retention window):
      1. Mechanics: retention deletes a terminal job by its created_at
         (app/engine/service.py delete_terminal_notifications_batch),
         and its journal rows go with it (ON DELETE CASCADE, migration
         0017). A job created before the retention cutoff whose channel
         answered inside the window loses those answers at the next
         retention pass, and the count is short by them. It takes a
         window that reaches back near the retention period: one day of
         window against the one-day minimum retention, or a delivery
         that waited (schedule, backoff, 429) across the cutoff.
      2. Status: acknowledged by design.
      3. Backlog ref: none -- the default retention is 90 days against
         a window of at most one day (app/core/constants.py); the gap
         opens only on a deploy that set retention to a day or two.
      4. Promotion trigger (observable): the same past window asked
         twice, before and after a retention pass, gives two different
         `answers` for one channel.
      5. Agreed fix: retention measures a terminal job's age by the
         moment it became terminal rather than by its creation.
      6. Rejected: journal rows that outlive their job (a second
         deletion path for the journal, which 0017 forbids); a window
         ceiling below the retention period (does not help -- the job
         is old, not the answer).
    """
    db_now: datetime = (
        await session.execute(select(func.now()))
    ).scalar_one()
    window_from = db_now - timedelta(minutes=window_minutes)

    rows = await session.execute(window_counts(window_from, db_now))

    counts = {channel.value: _Counts() for channel in DeliveryChannel}
    for channel, outcome, failure_class, count in rows.all():
        # Every value comes from the one writer (app/engine/journal.py)
        # and its enums: a key outside them is a defect of comms and
        # fails loudly here rather than vanishing from the count.
        entry = counts[channel]
        entry.answers += count
        entry.by_outcome[outcome] += count
        if outcome == ChannelAnswer.REFUSED:
            entry.refused_by_class[failure_class] += count

    states = channel_map(settings)
    return {
        "window": {
            "minutes": window_minutes,
            "from": window_from.isoformat(),
            "to": db_now.isoformat(),
        },
        "channels": {
            name: entry.wire(states[name]) for name, entry in counts.items()
        },
    }
