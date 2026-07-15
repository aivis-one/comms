# =============================================================================
# COMMS Service -- Preferences API (E8-shaped facade) -- FROZEN CONTRACT
# =============================================================================
#
# Phase 3b item 3. Preferences live in TWO homes (arch §2.5): category
# mutes in the category_mutes table, the quiet-hours schedule in
# quiet_* columns on the recipient. This facade hides both behind ONE
# object shaped for the product's settings screen (VELO E8:
# master_notifications) -- the Phase 6 proxy passes it through without
# re-assembly or re-conversion.
#
# CONTRACT (Phase 6 product proxy consumes as-is):
#
#   GET  /api/v1/recipients/{rid}/preferences
#     -> 200 {
#          "categories": {"<category>": <bool>, ...},
#              # one key per category the loaded profile DECLARES
#              # (types.yaml `category` fields); true = enabled
#              # (NOT muted). Emitted alphabetically -- display order
#              # is product UI knowledge, not ours. Mutes for
#              # categories the profile no longer declares are
#              # omitted (they still gate nothing: no type maps to
#              # them).
#          "schedule": {
#            "from": "22:00", "to": "08:00",     # local wall clock,
#                                                # HH:MM; from > to
#                                                # means overnight
#            "days": ["mon", "fri"]              # window START days,
#                                                # mon..sun order
#          } | null,                             # null = no window
#          "timezone": "<IANA name>" | null
#              # READ-ONLY: sync-owned (product identity, arch §2.5).
#              # Shown so the UI can caption the schedule; changing it
#              # is a PRODUCT feature that syncs back into comms.
#        }
#     -> 404 for a recipient comms has not synced (unlike the inbox,
#        preferences of a nonexistent recipient are not "empty" --
#        there is no row to hang them on).
#
#   PATCH /api/v1/recipients/{rid}/preferences
#     body: {
#       "categories": {"<category>": <bool>, ...},   # PARTIAL: only
#                                                    # listed toggles
#                                                    # change; unknown
#                                                    # category -> 422
#       "schedule": {...} | null                     # FULL REPLACE
#                                                    # when present
#                                                    # (all three
#                                                    # fields
#                                                    # required);
#                                                    # null clears;
#                                                    # OMITTED leaves
#                                                    # untouched
#     }
#     -> 200 <the full GET form>       # round-trip: writing then
#                                      # reading is a fixed point
#     Unknown body keys (including "timezone" -- read-only by design)
#     are rejected with 422, not ignored: silently swallowing a field
#     the client believed it set is how settings screens lie.
#
# DAY-CODE CONVERSION (arch decision (a)): comms stores ISO weekdays
# 1..7 but the wire form speaks E8 codes mon..sun -- converted HERE,
# inside comms, both directions. The product receives the finished E8
# form; if every client re-converted, this would be half a facade.
#
# TRUST MODEL (part of the frozen contract, review 3b): comms does NOT
# verify that recipient_id belongs to the calling end user -- there is
# no per-recipient authorization here at all. The shared service token
# authenticates the PRODUCT (arch decision 14), and the product proxy
# is the sole owner of the "user X may only read/write user X's
# preferences" check. Phase 6 MUST substitute the recipient_id
# server-side from its own authenticated session, never accept it
# from the client.
# =============================================================================

from datetime import time
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
    set_quiet_hours,
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


class ScheduleIn(BaseModel):
    """The quiet-hours window as the wire sees it (full replace)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    # "from" is a Python keyword -> alias.
    from_: time = Field(alias="from")
    to: time
    days: list[str]

    @field_validator("from_", "to")
    @classmethod
    def _minute_granularity(cls, value: time) -> time:
        """Reject sub-minute precision.

        The wire form is HH:MM; a window boundary of 22:00:30 would
        survive the write but come back as "22:00" -- a silently
        unstable round-trip on a frozen contract. Minutes are the
        contract's granularity, so anything finer is a client error.
        """
        if value.second or value.microsecond:
            raise ValueError(
                "schedule times use minute granularity (HH:MM); "
                "seconds are not allowed"
            )
        return value


class PreferencesPatch(BaseModel):
    """PATCH body: both parts optional, unknown keys rejected.

    extra="forbid" is what makes timezone (and any typo) a 422 -- the
    read-only field is not silently dropped.
    """

    model_config = ConfigDict(extra="forbid")

    categories: dict[str, bool] | None = None
    # None is meaningful (clear the window) -- presence is checked via
    # model_fields_set, not via the value.
    schedule: ScheduleIn | None = None


# ---------------------------------------------------------------------------
# Facade assembly
# ---------------------------------------------------------------------------


def _facade_form(prefs: RecipientPreferences) -> dict[str, Any]:
    """Assemble the E8 form from the two preference homes."""
    categories = {
        category: category not in prefs.muted_categories
        for category in sorted(registry.registered_categories())
    }

    schedule: dict[str, Any] | None = None
    if prefs.quiet_from is not None:
        # set_quiet_hours is all-or-nothing: quiet_from set implies
        # quiet_to and quiet_days set. An explicit check, not an
        # assert (Phase 2.3 precedent): asserts vanish under
        # `python -O`, and a half-window (only reachable by manual DB
        # surgery) would then surface as an opaque strftime(None)
        # traceback. RuntimeError, NOT ValidationError: this is store
        # corruption, not client input -- the caller must see a 500,
        # not be told its request was wrong.
        if prefs.quiet_to is None or prefs.quiet_days is None:
            raise RuntimeError(
                f"Recipient {prefs.recipient_id} has a partial "
                f"quiet-hours window (quiet_from set, quiet_to/days "
                f"missing) -- the store violates the all-or-nothing "
                f"invariant; fix the row"
            )
        schedule = {
            "from": prefs.quiet_from.strftime("%H:%M"),
            "to": prefs.quiet_to.strftime("%H:%M"),
            "days": _iso_days_to_codes(prefs.quiet_days),
        }

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

    Returns the full updated form (round-trip stability is part of
    the frozen contract).
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
        if patch.schedule is None:
            await set_quiet_hours(
                session,
                recipient_id,
                quiet_from=None,
                quiet_to=None,
                days=None,
            )
        else:
            await set_quiet_hours(
                session,
                recipient_id,
                quiet_from=patch.schedule.from_,
                quiet_to=patch.schedule.to,
                days=_codes_to_iso_days(patch.schedule.days),
            )

    prefs = await get_preferences(session, recipient_id)
    return _facade_form(prefs)
