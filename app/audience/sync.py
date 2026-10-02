# =============================================================================
# COMMS Service -- Sync Receivers (audience projection, Phase 2)
# =============================================================================
#
# Service-level receivers that keep the audience projection
# (recipients / group_memberships) in step with the product. The
# product emits `user_upserted` and `group_changed` events; a
# transport (HTTP / Redis Streams, Phase 3) will call these functions.
# Until then they are the contract -- tests call them directly.
#
# EVENT CONTRACT (what the transport must deliver):
#   user_upserted: {recipient_id, telegram_id, email, locale,
#                   timezone?, active}
#     -- full snapshot of the SIX sync-owned identity fields; the
#        product is the source of truth for them and every event
#        carries all of them (no partial patches). `timezone` is
#        optional in the payload: a product that does not track it
#        maps the absence to None, and comms falls back to
#        DEFAULT_TIMEZONE at quiet-hours computation time.
#   group_changed: {group_key, recipient_id, member}
#     -- member=True adds the pair, member=False removes it.
#
# IDEMPOTENCY: both receivers are safe to replay (at-least-once
# transports re-deliver). Re-applying the same event is a no-op.
#
# OWNERSHIP BOUNDARY: user_upserted writes ONLY the six identity
# fields above. Snapshot semantics apply to all of them: timezone=None
# in the event OVERWRITES a previously synced value with NULL ("None
# means keep" would be a partial patch smuggled into a snapshot
# contract). The comms-owned preference fields on Recipient
# (quiet_from / quiet_to / quiet_days) are never touched -- a re-sync
# must not wipe a recipient's settings. That is why this is a
# field-by-field upsert and not a session.merge.
#
# POISON-PILL RULE: a bad event never JAMS the stream -- it is refused
# and acknowledged (a malformed one dead-letters, a stale or deleted one
# is logged with its class). What is refused since F1.4: a blank string
# or a zero standing for "no value" (THE SNAPSHOT RULE below). An
# unknown but non-blank timezone is still stored as-is with a loud
# warning here (early signal) and degrades to DEFAULT_TIMEZONE at
# computation time (app/audience/schedule.py).
# =============================================================================

import hashlib
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import GroupMembership, Recipient
from app.core.config import settings
from app.core.exceptions import (
    NotFoundError,
    RecipientDeletedError,
    SnapshotConflictError,
    StaleSnapshotError,
    ValidationError,
)

logger = structlog.get_logger()


def _warn_if_unknown_timezone(
    recipient_id: UUID,
    timezone: str | None,
) -> None:
    """Early signal for an unresolvable synced timezone.

    Never rejects (poison-pill rule): the value is stored as-is and
    quiet-hours math degrades to DEFAULT_TIMEZONE with its own warning
    at computation time. This one fires at intake so the problem shows
    up in logs when the sync happens, not when a delivery is due.
    """
    if timezone is None:
        return
    try:
        ZoneInfo(timezone)
    except (KeyError, ValueError):
        logger.warning(
            "invalid_timezone_synced",
            recipient_id=str(recipient_id),
            timezone=timezone,
            fallback=settings.default_timezone,
        )


# -----------------------------------------------------------------------------
# The snapshot rule (F1.4, spec §10.1-10.2) -- ONE place, both write paths
# -----------------------------------------------------------------------------
#
# PUT /api/v1/recipients/{id} and the user_upserted event both land here,
# and so do their deletions (forget, app/forgetting.py). The rule:
#
#   recipient deleted             -> RecipientDeletedError, any version
#                                    (the tombstone is terminal);
#   version <  stored             -> StaleSnapshotError (a late event
#                                    must not roll the book back);
#   version == stored, same bytes -> a replay: nothing is written;
#   version == stored, other bytes-> SnapshotConflictError;
#   version >  stored             -> applied;
#   no row yet                    -> created with this version.
#
# ONE RULE FOR "NO VALUE": telegram_id, email, locale and timezone are a
# value or an explicit None. A blank string (or a telegram id of 0) is
# refused here, for both paths, because before F1.4 it was a second way
# to say "no value" -- accepted on one path, refused on the other, read
# as absence by the renderers. The database holds the same rule
# (CHECK ck_recipients_*_not_blank / _telegram_id_not_zero, 0014).


def snapshot_fingerprint(
    *,
    telegram_id: int | None,
    email: str | None,
    locale: str | None,
    timezone: str | None,
    active: bool,
) -> str:
    """Digest of the snapshot's fields, canonically encoded."""
    encoded = json.dumps(
        {
            "telegram_id": telegram_id,
            "email": email,
            "locale": locale,
            "timezone": timezone,
            "active": active,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _refuse_sentinels(
    telegram_id: int | None,
    email: str | None,
    locale: str | None,
    timezone: str | None,
) -> None:
    if telegram_id == 0:
        raise ValidationError(
            "telegram_id 0 is not a Telegram chat: send null for no chat"
        )
    for name, value in (("email", email), ("locale", locale),
                        ("timezone", timezone)):
        if value is not None and not value.strip():
            raise ValidationError(
                f"{name} is blank: send null for no {name}"
            )


def _order(recipient: Recipient, version: int, fingerprint: str) -> bool:
    """Where an incoming version stands against the stored row.

    Returns True when the incoming write must be APPLIED, False when it
    is a replay of the stored snapshot; raises for everything else.
    """
    if recipient.deleted_at is not None:
        raise RecipientDeletedError(
            f"recipient {recipient.id} was deleted; a returning person "
            f"gets a new id in the product"
        )
    if version < recipient.version:
        raise StaleSnapshotError(
            f"recipient {recipient.id}: version {version} is older than "
            f"the stored {recipient.version}"
        )
    if version == recipient.version:
        if fingerprint == recipient.snapshot_fingerprint:
            return False
        raise SnapshotConflictError(
            f"recipient {recipient.id}: version {version} is stored with "
            f"other content; a new snapshot needs a new version"
        )
    return True


async def _apply_snapshot_once(
    session: AsyncSession,
    *,
    recipient_id: UUID,
    version: int,
    telegram_id: int | None,
    email: str | None,
    locale: str | None,
    timezone: str | None,
    active: bool,
) -> Recipient:
    """Apply one recipient snapshot by THE SNAPSHOT RULE (above).

    recipient_id is the PRODUCT user id (shared id-space, Model B).
    Writes only the sync-owned identity fields; the comms-owned delivery
    schedule survives re-syncs untouched. Every field is a required
    parameter on purpose: the transport maps "absent" to None
    explicitly (snapshot discipline), never through a default.

    The row is read FOR UPDATE, so two writers of one recipient (the
    PUT route and the consumer) compare against the same stored version
    one after the other, never both against the old one.
    """
    if type(version) is not int or version < 1:
        raise ValidationError(
            f"version must be an integer >= 1, got {version!r}"
        )
    _refuse_sentinels(telegram_id, email, locale, timezone)
    _warn_if_unknown_timezone(recipient_id, timezone)
    fingerprint = snapshot_fingerprint(
        telegram_id=telegram_id, email=email, locale=locale,
        timezone=timezone, active=active,
    )

    recipient = await session.get(Recipient, recipient_id, with_for_update=True)
    if recipient is None:
        recipient = Recipient(
            id=recipient_id,
            version=version,
            snapshot_fingerprint=fingerprint,
            telegram_id=telegram_id,
            email=email,
            locale=locale,
            timezone=timezone,
            active=active,
        )
        session.add(recipient)
        await session.flush()
        logger.info(
            "recipient_created",
            recipient_id=str(recipient_id),
            version=version,
            active=active,
        )
        return recipient

    if not _order(recipient, version, fingerprint):
        logger.info(
            "recipient_snapshot_replayed",
            recipient_id=str(recipient_id),
            version=version,
        )
        return recipient

    # Field-by-field on purpose: ONLY the product-owned identity
    # fields; the delivery schedule stays untouched.
    recipient.version = version
    recipient.snapshot_fingerprint = fingerprint
    recipient.telegram_id = telegram_id
    recipient.email = email
    recipient.locale = locale
    recipient.timezone = timezone
    recipient.active = active
    await session.flush()
    logger.info(
        "recipient_updated",
        recipient_id=str(recipient_id),
        version=version,
        active=active,
    )
    return recipient


async def _tombstone_once(
    session: AsyncSession, *, recipient_id: UUID, version: int,
) -> tuple[Recipient, bool]:
    """Turn the recipient into a tombstone by the snapshot rule's order.

    Returns (recipient, forgotten_now). A recipient comms never heard
    of becomes a tombstone too -- otherwise a snapshot still on its way
    would create, after the deletion, a person the product no longer
    has. A repeated deletion is not an error: (row, False).

    Only the recipient row is touched here; what else is forgotten (the
    audience rows, the messaging rows, the deliveries) is orchestrated
    in app/forgetting.py, which may import every layer.
    """
    if type(version) is not int or version < 1:
        raise ValidationError(
            f"version must be an integer >= 1, got {version!r}"
        )
    recipient = await session.get(Recipient, recipient_id, with_for_update=True)
    now = datetime.now(UTC)
    if recipient is None:
        recipient = Recipient(
            id=recipient_id, version=version,
            snapshot_fingerprint=_TOMBSTONE_FINGERPRINT,
            telegram_id=None, email=None, locale=None, timezone=None,
            active=False, deleted_at=now,
        )
        session.add(recipient)
        await session.flush()
        return recipient, True
    if recipient.deleted_at is not None:
        return recipient, False
    if version < recipient.version:
        raise StaleSnapshotError(
            f"recipient {recipient.id}: deletion version {version} is "
            f"older than the stored {recipient.version}"
        )
    if version == recipient.version:
        raise SnapshotConflictError(
            f"recipient {recipient.id}: version {version} is stored with "
            f"a snapshot; a deletion needs a new version"
        )
    recipient.version = version
    recipient.snapshot_fingerprint = _TOMBSTONE_FINGERPRINT
    recipient.telegram_id = None
    recipient.email = None
    recipient.locale = None
    recipient.timezone = None
    recipient.allowed_windows = None
    recipient.active = False
    recipient.deleted_at = now
    await session.flush()
    return recipient, True


# The fingerprint a tombstone carries: never equal to a snapshot's.
_TOMBSTONE_FINGERPRINT = "tombstone".ljust(64, "-")


async def _group_changed_once(
    session: AsyncSession,
    *,
    group_key: str,
    recipient_id: UUID,
    member: bool,
) -> None:
    """Apply a `group_changed` sync event (add/remove membership).

    member=True ensures the (group_key, recipient_id) pair exists;
    member=False ensures it does not. Idempotent both ways. The
    recipient must have been synced first (user_upserted precedes
    group_changed in the product's event order); an unknown recipient
    is a sync-ordering bug and raises NotFoundError rather than
    silently creating a half-empty row.
    """
    if member:
        recipient = await session.get(Recipient, recipient_id)
        if recipient is None:
            raise NotFoundError(
                f"Cannot add unknown recipient {recipient_id} to group "
                f"{group_key!r}: user_upserted must precede group_changed"
            )
        if recipient.deleted_at is not None:
            # A late membership must not re-attach a forgotten person.
            raise RecipientDeletedError(
                f"recipient {recipient_id} was deleted; it joins no group"
            )
        existing = await session.scalar(
            select(GroupMembership).where(
                GroupMembership.group_key == group_key,
                GroupMembership.recipient_id == recipient_id,
            )
        )
        if existing is not None:
            return
        session.add(
            GroupMembership(group_key=group_key, recipient_id=recipient_id)
        )
        await session.flush()
        logger.info(
            "group_member_added",
            group_key=group_key,
            recipient_id=str(recipient_id),
        )
        return

    existing = await session.scalar(
        select(GroupMembership).where(
            GroupMembership.group_key == group_key,
            GroupMembership.recipient_id == recipient_id,
        )
    )
    if existing is None:
        return
    await session.delete(existing)
    await session.flush()
    logger.info(
        "group_member_removed",
        group_key=group_key,
        recipient_id=str(recipient_id),
    )


# -- The insert race (D1 / R2) ------------------------------------------------
#
# Each of the three writes below reads the row, finds nothing and
# inserts it. Two writers on a brand-new id -- the PUT route and the
# stream, two product replicas, two events of one burst -- can both find
# nothing; the loser's INSERT then fails on the primary key. Before D1
# only the PUT route survived that (it held its own SAVEPOINT); on the
# stream the IntegrityError reached the consumer's catch-all and the
# event went to the dead-letter stream. The rule now lives here, once,
# for every caller -- and for every write that inserts a key two
# writers can reach, not only a new recipient id: the category mute of
# app/audience/prefs.py is the fourth (D1-3).

async def once_more_on_insert_race[T](
    session: AsyncSession,
    apply: Callable[[], Awaitable[T]],
    *,
    event: str,
    **fields: str,
) -> T:
    """Run `apply` under a SAVEPOINT; on an IntegrityError run it once
    more, outside one.

    The SAVEPOINT keeps the caller's transaction usable after the
    loser's INSERT fails. The winner has committed by then (the loser's
    INSERT waited on its lock), so the SAME rule run again finds the
    winner's row and decides against it: a replay, a stale or a newer
    version -- exactly as without the race. Once: a second collision
    would mean the row both exists and does not, and it is raised. Any
    IntegrityError is retried, not only the key's: one that is not the
    race fails the second run the same way and is raised then.

    WHY THE NAME IS PUBLIC: the rule is one for every writer of the
    kind, and a second module needs it (app/audience/prefs.py,
    set_category_muted). A private name imported across modules is what
    D1 / R7 forbids (tests/test_hygiene_r7.py); the precedent there is
    to make the name public where it lives, not to copy the rule or to
    move it to a module of its own.
    """
    try:
        async with session.begin_nested():
            return await apply()
    except IntegrityError:
        logger.info(event, **fields)
        return await apply()


async def apply_snapshot(
    session: AsyncSession,
    *,
    recipient_id: UUID,
    version: int,
    telegram_id: int | None,
    email: str | None,
    locale: str | None,
    timezone: str | None,
    active: bool,
) -> Recipient:
    """Apply one recipient snapshot (_apply_snapshot_once), surviving
    the insert race."""
    return await once_more_on_insert_race(
        session,
        lambda: _apply_snapshot_once(
            session,
            recipient_id=recipient_id,
            version=version,
            telegram_id=telegram_id,
            email=email,
            locale=locale,
            timezone=timezone,
            active=active,
        ),
        # The event name the PUT route logged before D1: kept, it is an
        # operational contract.
        event="recipient_upsert_raced",
        recipient_id=str(recipient_id),
    )


async def tombstone(
    session: AsyncSession, *, recipient_id: UUID, version: int,
) -> tuple[Recipient, bool]:
    """Tombstone a recipient (_tombstone_once), surviving the insert
    race -- a deletion of an id comms never saw inserts its tombstone."""
    return await once_more_on_insert_race(
        session,
        lambda: _tombstone_once(
            session, recipient_id=recipient_id, version=version,
        ),
        event="recipient_tombstone_raced",
        recipient_id=str(recipient_id),
    )


async def group_changed(
    session: AsyncSession,
    *,
    group_key: str,
    recipient_id: UUID,
    member: bool,
) -> None:
    """Apply a membership change (_group_changed_once), surviving the
    insert race of two `member: true` for one pair."""
    await once_more_on_insert_race(
        session,
        lambda: _group_changed_once(
            session, group_key=group_key, recipient_id=recipient_id,
            member=member,
        ),
        event="group_member_add_raced",
        group_key=group_key,
        recipient_id=str(recipient_id),
    )

