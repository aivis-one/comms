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
# A NAME IS NOT AN INVARIANT -- ITS DEFINITION IS. The existence list
# pins each index's definition, not just its name, because the two come
# apart: an index recreated under the same name WITHOUT `UNIQUE` passes
# a by-name check, passes `alembic check`, and lets two eternal DMs
# exist for one pair. That was measured, not imagined -- the invariant
# was dead and both detectors were green. Two facts are pinned per
# index: the catalog's `indisunique` flag, which is what died in that
# experiment and which Postgres reports identically across versions,
# and the rendered definition, which is what catches a changed
# predicate or column list. A major Postgres upgrade may reformat the
# rendered text: then the definition half goes red, the uniqueness half
# stays green, and a person looks at the diff -- which is the intended
# outcome, not a malfunction.
#
# A LEGITIMATE CHANGE updates the line here in the same commit as the
# migration, exactly as a column width is updated together with its
# constant.
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

from dataclasses import dataclass
from typing import Any

from sqlalchemy.schema import SchemaItem


@dataclass(frozen=True)
class IndexShape:
    """What an index must BE, beyond being present under its name.

    unique:     pg_index.indisunique. Stable wording across Postgres
                versions, and the property that silently died when the
                index was recreated without UNIQUE.
    definition: pg_indexes.indexdef, as Postgres renders it. Catches a
                changed predicate, column list or order -- the ways an
                index stays unique and stops meaning what it meant.
    """

    unique: bool
    definition: str

# Declared in migrations, absent from Base.metadata, and compared by
# autogenerate -- which is why each one needs muting by name.
MIGRATION_OWNED_INDEXES: dict[str, IndexShape] = {
    # -- Uniqueness that is an invariant, not an optimization --
    # One key, one accepted job (F1.2: plain, the key is NOT NULL).
    # Replay of a request collapses onto it; other bytes are a conflict.
    "uq_notifications_idempotency_key": IndexShape(
        unique=True,
        definition=(
            "CREATE UNIQUE INDEX uq_notifications_idempotency_key "
            "ON public.notifications USING btree (idempotency_key)"
        ),
    ),
    # A replay of the same rejected or conflicting bytes records once.
    "uq_intake_outcomes_key_fingerprint_outcome": IndexShape(
        unique=True,
        definition=(
            "CREATE UNIQUE INDEX uq_intake_outcomes_key_fingerprint_outcome "
            "ON public.intake_outcomes USING btree "
            "(idempotency_key, fingerprint, outcome)"
        ),
    ),
    # "One eternal DM per pair" -- partial unique index.
    "uq_threads_dedup_dm": IndexShape(
        unique=True,
        definition=(
            "CREATE UNIQUE INDEX uq_threads_dedup_dm ON public.threads "
            "USING btree (client, operator_kind, operator_value) "
            "WHERE (((kind)::text = 'dm'::text) "
            "AND (subject_type IS NULL))"
        ),
    ),
    # "One thread per subject" -- partial unique index.
    "uq_threads_dedup_subject": IndexShape(
        unique=True,
        definition=(
            "CREATE UNIQUE INDEX uq_threads_dedup_subject "
            "ON public.threads USING btree "
            "(client, operator_kind, operator_value, subject_type, "
            "subject_id) WHERE (subject_type IS NOT NULL)"
        ),
    ),
    # Arbitrates the race when two callers create the same section;
    # the service catches the IntegrityError BY THIS NAME.
    "uq_sections_key": IndexShape(
        unique=True,
        definition=(
            "CREATE UNIQUE INDEX uq_sections_key ON public.sections "
            "USING btree (key)"
        ),
    ),
    # -- Read-path indexes --
    "ix_messages_thread_created": IndexShape(
        unique=False,
        definition=(
            "CREATE INDEX ix_messages_thread_created "
            "ON public.messages USING btree (thread_id, created_at)"
        ),
    ),
    "ix_notification_deliveries_inbox": IndexShape(
        unique=False,
        definition=(
            "CREATE INDEX ix_notification_deliveries_inbox "
            "ON public.notification_deliveries USING btree "
            "(recipient_id, status, read_at)"
        ),
    ),
    # The processor's pick order (F1.4: priority left the ordering and
    # the table; the index was recreated without it).
    "ix_notifications_status_scheduled": IndexShape(
        unique=False,
        definition=(
            "CREATE INDEX ix_notifications_status_scheduled "
            "ON public.notifications USING btree (status, scheduled_at)"
        ),
    ),
    # A repeated resource call answers with the row it created (F1.4).
    "uq_messages_idempotency_key": IndexShape(
        unique=True,
        definition=(
            "CREATE UNIQUE INDEX uq_messages_idempotency_key "
            "ON public.messages USING btree (idempotency_key)"
        ),
    ),
    "uq_threads_idempotency_key": IndexShape(
        unique=True,
        definition=(
            "CREATE UNIQUE INDEX uq_threads_idempotency_key "
            "ON public.threads USING btree (idempotency_key)"
        ),
    ),
    "ix_threads_activity": IndexShape(
        unique=False,
        definition=(
            "CREATE INDEX ix_threads_activity ON public.threads "
            "USING btree (COALESCE(last_message_at, created_at) DESC, "
            "id DESC)"
        ),
    ),
    "ix_threads_close_notify_pending": IndexShape(
        unique=False,
        definition=(
            "CREATE INDEX ix_threads_close_notify_pending "
            "ON public.threads USING btree (close_notify_pending_at) "
            "WHERE (close_notify_pending_at IS NOT NULL)"
        ),
    ),
    "ix_threads_operator_user": IndexShape(
        unique=False,
        definition=(
            "CREATE INDEX ix_threads_operator_user ON public.threads "
            "USING btree (operator_value) "
            "WHERE ((operator_kind)::text = 'user'::text)"
        ),
    ),
}

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
    # A failed delivery without a failure class cannot exist (F1.3).
    "ck_deliveries_failure_class",
    # A wait reason without a time (or the reverse) cannot exist (F1.3).
    "ck_deliveries_wait_reason",
    # "No value" is NULL, never a blank string or a zero (F1.4).
    "ck_recipients_locale_not_blank",
    "ck_recipients_email_not_blank",
    "ck_recipients_timezone_not_blank",
    "ck_recipients_telegram_id_not_zero",
    "ck_recipients_version_not_negative",
    # A tombstone keeps nothing that reaches the person (F1.4).
    "ck_recipients_tombstone",
})

# Everything the schema must carry although the metadata never mentions
# it. The suite walks this set against the live catalogs.
MIGRATION_OWNED_INVARIANTS = (
    frozenset(MIGRATION_OWNED_INDEXES) | MIGRATION_OWNED_CHECKS
)


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
