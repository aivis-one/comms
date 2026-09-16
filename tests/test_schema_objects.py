# =============================================================================
# COMMS Service -- Migration-owned schema objects (R-3)
# =============================================================================
#
# Two lists, two questions, two kinds of test here.
#
#   THE FILTER LIST says "autogenerate must not offer to drop this".
#   Tested by running the real comparison with the real filter and
#   demanding a clean diff -- plus, in the same breath, demanding that
#   a stray object and a missing migration still show up. A filter that
#   silenced everything would satisfy the first half alone.
#
#   THE EXISTENCE LIST says "the database must carry this". Tested
#   against the live catalogs, because the filter makes autogenerate
#   silent about these objects IN BOTH DIRECTIONS: after it, a
#   migration that drops uq_threads_dedup_dm leaves `alembic check`
#   green. This file is what notices instead.
#
# The suite's schema is built by `alembic upgrade head` (conftest), so
# the database these tests inspect is the one a deploy gets.
# =============================================================================

from typing import Any

import pytest
from alembic.autogenerate import produce_migrations
from alembic.migration import MigrationContext
from sqlalchemy import Column, Integer, MetaData, Table, text
from sqlalchemy.engine import Connection

from app.core.database import Base, get_engine
from app.core.schema_objects import (
    MIGRATION_OWNED_CHECKS,
    MIGRATION_OWNED_INDEXES,
    MIGRATION_OWNED_INVARIANTS,
    include_object,
)

pytestmark = pytest.mark.usefixtures("apply_migrations")


def _diffs(connection: Connection, metadata: MetaData) -> list[Any]:
    """The upgrade operations autogenerate would write right now.

    Configured exactly as migrations/env.py configures it -- same
    filter object, imported rather than restated, so the test cannot
    pass against a filter the migration environment does not use.
    """
    context = MigrationContext.configure(
        connection,
        opts={
            "target_metadata": metadata,
            "include_object": include_object,
        },
    )
    return list(produce_migrations(context, metadata).upgrade_ops.as_diffs())


def _op_names(diffs: list[Any], op: str) -> set[str]:
    return {
        getattr(entry[1], "name", "")
        for entry in diffs
        if entry[0] == op
    }


class TestMigrationOwnedInvariantsExist:
    """Every invariant the migrations own is in the database.

    This is the pair to the filter, not an extra: the filter buys a
    readable autogenerate diff with the comparison's silence about
    these objects, and silence is only affordable if something else is
    watching. Nothing else was.
    """

    @pytest.mark.parametrize("name", sorted(MIGRATION_OWNED_INDEXES))
    async def test_the_index_is_in_the_schema_as_declared(
        self, name: str
    ) -> None:
        """A NAME IS NOT AN INVARIANT -- the definition is.

        This test used to ask only whether an index of that name
        existed, and it was right about the half it checked: an index
        that is gone cannot hold anything. What it missed is that an
        index can be present and hollow. Recreated under the same name
        without UNIQUE it passes a by-name check, passes `alembic
        check`, and lets two eternal DMs exist for one pair -- measured,
        not supposed. So both facts are asserted: the catalog flag that
        died in that experiment, and the rendered definition that
        carries the predicate and the column list.
        """
        expected = MIGRATION_OWNED_INDEXES[name]
        async with get_engine().connect() as connection:
            row = (
                await connection.execute(
                    text(
                        "SELECT x.indisunique, i.indexdef "
                        "FROM pg_indexes i "
                        "JOIN pg_class c ON c.relname = i.indexname "
                        "JOIN pg_index x ON x.indexrelid = c.oid "
                        "WHERE i.schemaname = 'public' "
                        "AND i.indexname = :name"
                    ),
                    {"name": name},
                )
            ).first()

        assert row is not None, (
            f"{name} is missing from the schema -- an invariant declared "
            f"in a migration is gone"
        )
        unique, definition = row
        assert unique == expected.unique, (
            f"{name} exists but its uniqueness changed: the schema says "
            f"unique={unique}, the invariant says {expected.unique}"
        )
        assert definition == expected.definition, (
            f"{name} exists but is defined differently:\n"
            f"  schema:    {definition}\n"
            f"  invariant: {expected.definition}\n"
            f"A legitimate change updates app/core/schema_objects.py in "
            f"the same commit as the migration. A Postgres major upgrade "
            f"can also reword this text -- then compare by eye and repin."
        )

    @pytest.mark.parametrize("name", sorted(MIGRATION_OWNED_CHECKS))
    async def test_the_check_constraint_is_in_the_schema(
        self, name: str
    ) -> None:
        """Asked of the catalog, not of autogenerate -- which is the
        whole reason a CHECK can sit in the existence list while being
        pointless in the filter list: alembic never compares CHECK
        constraints, so nothing but this test would notice one going
        missing.
        """
        async with get_engine().connect() as connection:
            found = await connection.scalar(
                text(
                    "SELECT conname FROM pg_constraint "
                    "WHERE connamespace = 'public'::regnamespace "
                    "AND contype = 'c' AND conname = :name"
                ),
                {"name": name},
            )
        assert found == name


class TestTheListsDescribeReality:
    """The lists are claims about the schema, and claims rot."""

    @pytest.mark.parametrize("name", sorted(MIGRATION_OWNED_INDEXES))
    def test_a_muted_index_is_absent_from_the_metadata(
        self, name: str
    ) -> None:
        """POVTOR: the same object claimed in two places.

        If a later change moves one of these into __table_args__, the
        metadata starts carrying it and autogenerate can compare it
        honestly -- at which point the entry here is no longer a
        reason, it is a gag. The list must shrink when the metadata
        grows.
        """
        declared = {
            index.name
            for table in Base.metadata.tables.values()
            for index in table.indexes
        }
        assert name not in declared

    def test_the_existence_list_is_the_wider_one(self) -> None:
        """The two lists differ by exactly the objects autogenerate
        does not compare -- today, the CHECK. Stated as a relation
        rather than as a count, so the assertion survives the next
        invariant being added to both.

        The index list is a mapping now (name -> definition), so the
        relation is over its KEYS; the old form compared two sets and
        was right while both were sets.
        """
        both_lists = frozenset(MIGRATION_OWNED_INDEXES) | MIGRATION_OWNED_CHECKS
        assert both_lists == MIGRATION_OWNED_INVARIANTS
        assert not (frozenset(MIGRATION_OWNED_INDEXES) & MIGRATION_OWNED_CHECKS)
        assert MIGRATION_OWNED_CHECKS

    @pytest.mark.parametrize("name", sorted(MIGRATION_OWNED_INDEXES))
    def test_the_two_pinned_facts_agree(self, name: str) -> None:
        """POVTOR: one fact written twice.

        `unique` and the first words of `definition` say the same
        thing, and a pin edited in one place only would claim an index
        is unique while pinning a definition that does not create it
        that way. The redundancy is deliberate -- the flag survives a
        Postgres rewording, the text catches a changed predicate -- but
        redundancy that is allowed to disagree is worse than either
        half alone.
        """
        shape = MIGRATION_OWNED_INDEXES[name]
        assert shape.definition.startswith(
            "CREATE UNIQUE INDEX " if shape.unique else "CREATE INDEX "
        )
        assert f" {name} " in shape.definition


class TestAutogenerateAgainstHead:
    """What the command prints, run rather than described."""

    async def test_nothing_is_proposed_on_a_schema_at_head(self) -> None:
        """The item itself: no drop of an invariant, and no other
        operation either -- a diff that is clean except for noise is
        not clean, because the noise is what hides the one line the
        reader came for.
        """
        async with get_engine().connect() as connection:
            diffs = await connection.run_sync(_diffs, Base.metadata)
        assert diffs == []

    async def test_a_model_change_without_a_migration_is_reported(
        self,
    ) -> None:
        """THE PAIR to the clean diff, and the reason the filter is not
        a mute button: a column that exists in the models and not in
        the database must still surface. Without this, a filter that
        returned False for everything would pass the test above.

        The extra column is added to a REFLECTED copy of one table, so
        the real Base.metadata is never mutated and the check cannot
        leak into another test.
        """
        probe = MetaData()

        def _reflect_with_extra_column(connection: Connection) -> list[Any]:
            sections = Table("sections", probe, autoload_with=connection)
            sections.append_column(Column("probe_column", Integer()))
            return _diffs(connection, probe)

        async with get_engine().connect() as connection:
            diffs = await connection.run_sync(_reflect_with_extra_column)

        added = {
            entry[3].name
            for entry in diffs
            if entry[0] == "add_column" and entry[2] == "sections"
        }
        assert added == {"probe_column"}

    async def test_an_unlisted_index_is_still_reported(self) -> None:
        """PUSTOTA: an object the list does not name.

        This is why the list is by name and not "skip everything the
        metadata does not reference" -- a stray index IS drift, and the
        broad filter would have swallowed it along with the ten. The
        index is created and dropped here rather than assumed, because
        the claim is about the filter's behaviour on a real reflected
        object.
        """
        engine = get_engine()
        async with engine.begin() as connection:
            await connection.execute(
                text("CREATE INDEX ix_probe_stray ON sections (created_at)")
            )
        try:
            async with engine.connect() as connection:
                diffs = await connection.run_sync(_diffs, Base.metadata)
            assert _op_names(diffs, "remove_index") == {"ix_probe_stray"}
        finally:
            async with engine.begin() as connection:
                await connection.execute(text("DROP INDEX ix_probe_stray"))

    async def test_the_filter_only_mutes_what_it_reflects(self) -> None:
        """NEHVATKA: the filter is asked about an object that is NOT
        reflected -- i.e. one arriving from the metadata side.

        A muted name must not be muted in that direction: an index of
        that name appearing in __table_args__ is a real change and
        belongs in the diff. Checked on the filter directly, because
        producing that state through the schema would mean editing the
        models from a test.
        """
        name = sorted(MIGRATION_OWNED_INDEXES)[0]
        assert include_object(None, name, "index", True, None) is False
        assert include_object(None, name, "index", False, None) is True
        assert include_object(None, name, "table", True, None) is True
        assert include_object(None, "ix_not_listed", "index", True, None)
