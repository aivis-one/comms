# =============================================================================
# COMMS Service -- Schema objects the MIGRATIONS own (R-3)
# =============================================================================
#
# WHY THIS MODULE EXISTS. Schema invariants live in the migration, not
# in __table_args__ -- a decision with its reason written down
# (app/messaging/models.py header, migration 0006): one reviewable file
# holds every invariant. The price of that decision is that
# Base.metadata does not know about them, and `alembic revision
# --autogenerate` compares the database against the metadata: an object
# it cannot find in the metadata reads as surplus in the database, so
# it proposes a DROP. On a schema at head that was ten `remove_index`
# operations, four of them the uniqueness that keeps replayed
# notifications from duplicating and threads from splitting in two.
#
# The first thing a new team does is add a column and run autogenerate.
# Their one `add_column` would arrive with ten drops around it.
#
# TWO LISTS, BECAUSE THERE ARE TWO QUESTIONS.
#
#   MIGRATION_OWNED_INDEXES answers "do not propose dropping this".
#   It is consumed by include_object below and covers exactly the
#   objects autogenerate compares and would otherwise offer to remove.
#
#   MIGRATION_OWNED_INVARIANTS answers "this MUST be in the database".
#   It is consumed by the suite, which asks the database directly, and
#   it covers one object more -- see the CHECK note below.
#
# THE LISTS ARE NAMED, NOT A BLANKET. "Skip every index the metadata
# does not reference" would also silence a genuinely stray index, and a
# stray index IS drift worth reporting. An object absent from the list
# is reported, which is the whole point: adding a migration-only object
# without listing it here turns CI red rather than quiet.
#
# WHAT THE FILTER COSTS, stated where the filter is: autogenerate now
# says nothing about these objects IN EITHER DIRECTION. If a later
# migration drops uq_threads_dedup_dm or redefines its partial WHERE,
# `alembic check` stays green. That silence is exactly why the second
# list exists and why the suite asserts existence against the live
# schema instead of trusting the comparison.
# =============================================================================

from typing import Any

from sqlalchemy.schema import SchemaItem

# Declared in migrations, absent from Base.metadata, and compared by
# autogenerate -- which is why each one needs muting by name.
MIGRATION_OWNED_INDEXES = frozenset({
    # -- Uniqueness that is an invariant, not an optimization --
    # Replay of a stream event collapses onto one notification.
    "uq_notifications_idempotency_key",
    # "One eternal DM per pair" -- partial unique index.
    "uq_threads_dedup_dm",
    # "One thread per subject" -- partial unique index.
    "uq_threads_dedup_subject",
    # Arbitrates the race when two callers create the same section;
    # the service catches the IntegrityError BY THIS NAME.
    "uq_sections_key",
    # -- Read-path indexes --
    "ix_messages_thread_created",
    "ix_notification_deliveries_inbox",
    "ix_notifications_status_scheduled_priority",
    "ix_threads_activity",
    "ix_threads_close_notify_pending",
    "ix_threads_operator_user",
})

# Migration-owned CHECK constraints. NOT in the filter list above, and
# the absence is deliberate: alembic does not compare CHECK constraints
# at all, so include_object is never called for one and an entry there
# would guard against something that cannot happen. If a future alembic
# starts comparing them, `alembic check` in CI turns red and names the
# object itself -- the detector covers that blind spot on its own.
#
# It IS in the existence list, because that test asks the database
# rather than the comparison, and so does not care what alembic
# compares. Migration 0006 lists this CHECK in the same docstring as
# the indexes: its author treated it as an invariant of equal standing,
# and today nothing whatsoever would notice if a migration dropped it.
MIGRATION_OWNED_CHECKS = frozenset({
    # A half subject_ref (one column set, the other NULL) is forbidden.
    "ck_threads_subject_ref_both_or_neither",
})

# Everything the schema must carry although the metadata never mentions
# it. The suite walks this set against the live catalogs.
MIGRATION_OWNED_INVARIANTS = MIGRATION_OWNED_INDEXES | MIGRATION_OWNED_CHECKS


def include_object(
    object_: SchemaItem,
    name: str | None,
    type_: str,
    reflected: bool,
    compare_to: Any,
) -> bool:
    """Autogenerate filter: hide migration-owned objects from the diff.

    Only REFLECTED objects are muted -- that is, objects read out of the
    database. An object of the same name arriving from the metadata
    would mean somebody moved the declaration into __table_args__, and
    that belongs in the diff rather than in this silence.
    """
    return not (
        type_ == "index"
        and reflected
        and name in MIGRATION_OWNED_INDEXES
    )
