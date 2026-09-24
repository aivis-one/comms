# =============================================================================
# COMMS Service -- Preferences API (the settings-screen facade)
# =============================================================================
#
# Preferences live in TWO homes (arch §2.5): category mutes in the
# category_mutes table, the delivery schedule in the allowed_windows
# column on the recipient. This facade hides both behind ONE object
# shaped for a product's settings screen, so the product proxy passes it
# through without re-assembly or re-conversion.
#
# THE CONTRACT -- the GET and PATCH forms, the fields of a schedule
# period and every rule of a write -- is written out in full in ONE
# place: deploy/INTEGRATION.md, "6. Preferences". It is not repeated
# here; tests/test_delivery_contract.py holds that section to the
# models below and to what GET really returns. The error body and
# paging are the protocol's (INTEGRATION.md, "5. The resource
# protocol"); who may read whose preferences is the product proxy's
# check (INTEGRATION.md, "What comms takes on trust").
#
# DAY-CODE CONVERSION (arch decision (a)): comms stores ISO weekdays
# 1..7 but the wire speaks the codes mon..sun -- converted HERE, inside
# comms, both directions. The product receives the finished form; if
# every client re-converted, this would be half a facade.
# =============================================================================

import re
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_service_auth
from app.audience.prefs import (
    RecipientPreferences,
    get_preferences,
    set_category_muted,
    set_schedule,
)
from app.core.database import get_db_reader, get_db_session
from app.core.exceptions import ValidationError
from app.profile.registry import registry

router = APIRouter(
    prefix="/api/v1/recipients/{recipient_id}/preferences",
    tags=["preferences"],
    dependencies=[Depends(require_service_auth)],
)


# ---------------------------------------------------------------------------
# Day-code conversion (ISO 1..7 <-> E8 mon..sun), comms-internal
# ---------------------------------------------------------------------------

# Index i holds the code for ISO weekday i+1. Single source of truth
# for both directions.
_DAY_CODES = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_CODE_TO_ISO = {code: iso for iso, code in enumerate(_DAY_CODES, start=1)}


def _iso_days_to_codes(days: tuple[int, ...]) -> list[str]:
    """ISO weekday ints (stored sorted) -> E8 codes in mon..sun order."""
    return [_DAY_CODES[iso - 1] for iso in days]


def _codes_to_iso_days(codes: list[str]) -> list[int]:
    """E8 codes -> ISO weekday ints; unknown code -> ValidationError."""
    days: list[int] = []
    for code in codes:
        iso = _CODE_TO_ISO.get(code)
        if iso is None:
            raise ValidationError(
                f"Unknown day code: {code!r}. "
                f"Valid: {', '.join(_DAY_CODES)}"
            )
        days.append(iso)
    return days


# ---------------------------------------------------------------------------
# Wire models
# ---------------------------------------------------------------------------


_TIME_RE = re.compile(r"^([01]\d|2[0-4]):([0-5]\d)$")


def _minutes(value: str, field: str) -> int:
    """"HH:MM" -> minutes from midnight; 24:00 is valid as an END.

    Minute granularity is the contract's: a boundary of 22:00:30 would
    survive a write and come back as "22:00" -- a silently unstable
    round-trip. So the wire form is exactly HH:MM and nothing finer.

    24:00 exists for one reason: a period running to the end of the
    day. The old model's closest form was 23:59, which left a
    one-minute hole delivering on a day the user had marked silent.
    24:xx is not a time, and a period is rejected later if it starts
    at 24:00 (a start has nothing after it).
    """
    match = _TIME_RE.match(value)
    if match is None:
        raise ValueError(
            f"{field} must be HH:MM between 00:00 and 24:00, got {value!r}"
        )
    hours, minutes = int(match.group(1)), int(match.group(2))
    if hours == 24 and minutes != 0:
        raise ValueError(f"{field}: 24:00 is the only hour-24 value")
    return hours * 60 + minutes


def _hhmm(minutes: int) -> str:
    """Minutes from midnight -> "HH:MM"; 1440 -> "24:00"."""
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


class PeriodIn(BaseModel):
    """One period during which delivery is ALLOWED.

    THE POLARITY IS THE POINT (R-5). This used to be one quiet window
    -- when NOT to deliver -- whose day set meant "the days the window
    STARTS on". A product whose screen says "when you may reach me"
    had to invert it; the inverted window became nocturnal, its start
    day landed on the evening before the morning it covered, and the
    deploy delivered at hours nobody asked for. A period now says when
    delivery IS allowed, belongs to the day it falls in, and never
    crosses midnight -- a night allowance is two periods, one per day.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    day: str
    # "from" is a Python keyword -> alias.
    from_: str = Field(alias="from")
    to: str

    @field_validator("day")
    @classmethod
    def _known_day(cls, value: str) -> str:
        if value not in _CODE_TO_ISO:
            raise ValueError(
                f"Unknown day code: {value!r}. "
                f"Valid: {', '.join(_DAY_CODES)}"
            )
        return value

    @field_validator("from_", "to")
    @classmethod
    def _wire_time(cls, value: str) -> str:
        _minutes(value, "time")
        return value

    def stored(self) -> dict[str, int]:
        """The shape the store and the gate speak: ISO day and minutes."""
        return {
            "day": _CODE_TO_ISO[self.day],
            "from": _minutes(self.from_, "from"),
            "to": _minutes(self.to, "to"),
        }


class PreferencesPatch(BaseModel):
    """PATCH body: both parts optional, unknown keys rejected.

    extra="forbid" is what makes timezone (and any typo) a 422 -- the
    read-only field is not silently dropped.
    """

    model_config = ConfigDict(extra="forbid")

    categories: dict[str, bool] | None = None
    # None is meaningful (clear the schedule) -- presence is checked
    # via model_fields_set, not via the value. A LIST, not an object:
    # a recipient has as many allowed periods as their week needs, and
    # the single-window shape is exactly what made "Mon-Fri 09-21 plus
    # silent weekends" inexpressible.
    schedule: list[PeriodIn] | None = None


# ---------------------------------------------------------------------------
# Facade assembly
# ---------------------------------------------------------------------------


def _facade_form(prefs: RecipientPreferences) -> dict[str, Any]:
    """Assemble the E8 form from the two preference homes."""
    categories = {
        category: category not in prefs.muted_categories
        for category in sorted(registry.registered_categories())
    }

    schedule: list[dict[str, str]] | None = None
    if prefs.allowed_windows is not None:
        # No partial-state guard like the old window needed: a
        # schedule is ONE column now, so "half a schedule" is not a
        # state the store can be in. The invariant that had to be
        # checked there -- three columns that must agree -- stopped
        # existing together with the three columns.
        schedule = [
            {
                "day": _DAY_CODES[window["day"] - 1],
                "from": _hhmm(window["from"]),
                "to": _hhmm(window["to"]),
            }
            for window in prefs.allowed_windows
        ]

    return {
        "categories": categories,
        "schedule": schedule,
        "timezone": prefs.timezone,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("")
async def get_preferences_form(
    recipient_id: UUID,
    session: AsyncSession = Depends(get_db_reader),
) -> dict[str, Any]:
    """The E8 form: category toggles + schedule + read-only timezone."""
    prefs = await get_preferences(session, recipient_id)
    return _facade_form(prefs)


@router.patch("")
async def patch_preferences_form(
    recipient_id: UUID,
    patch: PreferencesPatch = Body(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Partial write: listed toggles change, schedule replaces whole.

    Returns the full updated form: writing then reading is a fixed
    point (INTEGRATION.md, "6. Preferences").
    """
    if patch.categories is not None:
        # Toggle semantics: true = enabled = NOT muted. Validation of
        # the category against the profile happens in the service
        # layer (unknown -> ValidationError -> 422); the whole PATCH
        # is one transaction, so a bad category rolls back the valid
        # toggles before it -- no partially applied writes.
        for category, enabled in patch.categories.items():
            await set_category_muted(
                session, recipient_id, category, muted=not enabled
            )

    if "schedule" in patch.model_fields_set:
        await set_schedule(
            session,
            recipient_id,
            windows=(
                None
                if patch.schedule is None
                else [period.stored() for period in patch.schedule]
            ),
        )

    prefs = await get_preferences(session, recipient_id)
    return _facade_form(prefs)
