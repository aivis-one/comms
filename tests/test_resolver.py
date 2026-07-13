# =============================================================================
# COMMS Service -- Resolver tests
# =============================================================================
# Handoff item 6: resolve (user/group/all) over recipients +
# group_memberships. Bare target_value encoding.
# =============================================================================

from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.constants import TargetType
from app.engine.resolver import resolve_targets
from tests.helpers import add_to_group, create_recipient


class TestResolveUser:
    """USER: target_value is a bare product user uuid."""

    async def test_active_user_resolves(
        self, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session)
        ids = await resolve_targets(
            db_session, TargetType.USER, str(recipient.id),
        )
        assert ids == [recipient.id]

    async def test_inactive_user_excluded(
        self, db_session: AsyncSession,
    ) -> None:
        recipient = await create_recipient(db_session, active=False)
        ids = await resolve_targets(
            db_session, TargetType.USER, str(recipient.id),
        )
        assert ids == []

    async def test_unknown_user_empty(self, db_session: AsyncSession) -> None:
        ids = await resolve_targets(
            db_session, TargetType.USER, str(uuid4()),
        )
        assert ids == []

    async def test_invalid_uuid_empty(self, db_session: AsyncSession) -> None:
        ids = await resolve_targets(
            db_session, TargetType.USER, "not-a-uuid",
        )
        assert ids == []


class TestResolveGroup:
    """GROUP: target_value is an opaque product group key."""

    async def test_group_members_resolve(
        self, db_session: AsyncSession,
    ) -> None:
        member_a = await create_recipient(db_session)
        member_b = await create_recipient(db_session)
        outsider = await create_recipient(db_session)
        await add_to_group(db_session, "practice_42", member_a.id)
        await add_to_group(db_session, "practice_42", member_b.id)
        await add_to_group(db_session, "role_master", outsider.id)

        ids = await resolve_targets(
            db_session, TargetType.GROUP, "practice_42",
        )
        assert set(ids) == {member_a.id, member_b.id}

    async def test_inactive_member_excluded(
        self, db_session: AsyncSession,
    ) -> None:
        active = await create_recipient(db_session)
        inactive = await create_recipient(db_session, active=False)
        await add_to_group(db_session, "role_master", active.id)
        await add_to_group(db_session, "role_master", inactive.id)

        ids = await resolve_targets(
            db_session, TargetType.GROUP, "role_master",
        )
        assert ids == [active.id]

    async def test_unknown_group_empty(
        self, db_session: AsyncSession,
    ) -> None:
        await create_recipient(db_session)
        ids = await resolve_targets(
            db_session, TargetType.GROUP, "no_such_group",
        )
        assert ids == []

    async def test_empty_group_key_empty(
        self, db_session: AsyncSession,
    ) -> None:
        ids = await resolve_targets(db_session, TargetType.GROUP, "")
        assert ids == []


class TestResolveAll:
    """ALL: every active recipient; target_value ignored."""

    async def test_all_active_only(self, db_session: AsyncSession) -> None:
        active_a = await create_recipient(db_session)
        active_b = await create_recipient(db_session)
        await create_recipient(db_session, active=False)

        ids = await resolve_targets(db_session, TargetType.ALL, "*")
        assert set(ids) == {active_a.id, active_b.id}


class TestUnknownTargetType:
    """Unknown target types resolve to nothing (logged warning)."""

    async def test_unknown_type_empty(self, db_session: AsyncSession) -> None:
        await create_recipient(db_session)
        ids = await resolve_targets(db_session, "practice", "whatever")
        assert ids == []
