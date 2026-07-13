# =============================================================================
# COMMS Service -- Notification Resolver
# =============================================================================
#
# Ported from the cbshome resolver (canonical base: resolve_targets +
# _RESOLVERS registry), de-domainized to run over the sync projection:
#
#   BEFORE (both donors): SELECT id FROM users WHERE ...   (product table)
#   AFTER  (comms):       recipients / group_memberships    (own tables)
#
# TARGET FORMATS (bare values -- see merge note below):
#   USER  -> target_value = "<uuid>"       single active recipient
#   GROUP -> target_value = "<group_key>"  active members of the group
#   ALL   -> target_value = "*"            all active recipients
#
# MERGE NOTE (target_value encoding):
#   cbshome double-encoded the type into the value ("user:<uuid>",
#   "role:<r>") even though target_type is a separate column; velo used
#   bare values. The canonical core keeps cbshome's registry STRUCTURE
#   and velo's bare-value ENCODING -- the prefix carried no information
#   the target_type column does not already carry.
#
# EXTENSIBILITY:
#   New target types register via register_resolver(). Each resolver
#   receives (session, target_value) and returns list[UUID].
# =============================================================================

from collections.abc import Awaitable, Callable
from uuid import UUID

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import GroupMembership, Recipient
from app.engine.constants import TargetType

logger = structlog.get_logger()

# A resolver expands a target_value into concrete recipient UUIDs.
Resolver = Callable[[AsyncSession, str], Awaitable[list[UUID]]]


async def resolve_targets(
    session: AsyncSession,
    target_type: str,
    target_value: str,
) -> list[UUID]:
    """Resolve a notification target into concrete recipient UUIDs.

    Args:
        session: Active DB session.
        target_type: One of TargetType values (or a registered custom).
        target_value: Target specifier (format depends on target_type).

    Returns:
        List of recipient UUIDs to receive the notification.
    """
    resolver = _RESOLVERS.get(target_type)
    if resolver is None:
        logger.warning(
            "unknown_target_type",
            target_type=target_type,
            target_value=target_value,
        )
        return []

    recipient_ids = await resolver(session, target_value)

    logger.info(
        "targets_resolved",
        target_type=target_type,
        target_value=target_value,
        count=len(recipient_ids),
    )
    return recipient_ids


# ---------------------------------------------------------------------------
# Individual resolvers
# ---------------------------------------------------------------------------


async def _resolve_user(
    session: AsyncSession,
    target_value: str,
) -> list[UUID]:
    """Resolve a bare uuid -> single recipient if active."""
    try:
        recipient_id = UUID(target_value)
    except ValueError:
        logger.warning("invalid_user_target", target_value=target_value)
        return []

    stmt = select(Recipient.id).where(
        Recipient.id == recipient_id,
        Recipient.active.is_(True),
    )
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    return [row] if row else []


async def _resolve_group(
    session: AsyncSession,
    target_value: str,
) -> list[UUID]:
    """Resolve a group_key -> all active members of the group."""
    if not target_value:
        logger.warning("invalid_group_target", target_value=target_value)
        return []

    stmt = (
        select(GroupMembership.recipient_id)
        .join(Recipient, GroupMembership.recipient_id == Recipient.id)
        .where(
            GroupMembership.group_key == target_value,
            Recipient.active.is_(True),
        )
    )
    result = await session.execute(stmt)
    return [row[0] for row in result.all()]


async def _resolve_all(
    session: AsyncSession,
    target_value: str,
) -> list[UUID]:
    """Resolve '*' -> all active recipients."""
    stmt = select(Recipient.id).where(
        Recipient.active.is_(True),
    )
    result = await session.execute(stmt)
    return [row[0] for row in result.all()]


# ---------------------------------------------------------------------------
# Resolver registry -- new target types register here
# ---------------------------------------------------------------------------

_RESOLVERS: dict[str, Resolver] = {
    TargetType.USER: _resolve_user,
    TargetType.GROUP: _resolve_group,
    TargetType.ALL: _resolve_all,
}


def register_resolver(target_type: str, resolver: Resolver) -> None:
    """Register a resolver for a custom target type (extension point)."""
    _RESOLVERS[target_type] = resolver
