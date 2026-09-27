"""snapshot version, forgetting, resource keys, priority removed (F1.4)

Revision ID: 0014_resources_snapshot
Revises: 0013_lifecycle_outcomes
Create Date: 2026-09-24 12:00:00.000000

WHAT TURNS OVER.

  recipients.version            new, NOT NULL -- the product's monotonic
                                snapshot version; older snapshots are
                                refused on both write paths.
  recipients.snapshot_fingerprint new, NOT NULL -- an equal version with
                                other bytes is a conflict.
  recipients.deleted_at         new -- the product deleted the person;
                                the row stays as a TOMBSTONE.
  recipients.locale             becomes NULLABLE: "no language" is an
                                explicit NULL, never "".
  messages / threads            idempotency_key + fingerprint, NOT NULL,
                                each under a unique index: a repeated
                                resource call answers with the row it
                                created.
  notifications.priority        DROPPED, with its place in the processor
                                ordering and the index that carried it
                                (the F1.2 marker's promotion trigger --
                                "the inbox resource contract is
                                reopened" -- is this migration). The
                                index is recreated without the column.

ONE RULE FOR "NO VALUE" (spec §10.1: an absent value is an explicit
null). Before, a blank string or a zero said "no value" on at least one
of the two write paths, and the readers agreed (`locale or default`,
`if not telegram_id`). Every such sentinel becomes NULL here, and a CHECK
per column keeps it from coming back:

  locale, email, timezone   blank (empty or whitespace only) -> NULL;
                            CHECK <column> IS NULL OR btrim(<column>) <> ''
  telegram_id               0 -> NULL; CHECK telegram_id IS NULL OR <> 0
                            (no Telegram chat has id 0)

Each translation is unambiguous: the readers already read the sentinel
as absence, so the row means the same thing after as before.

THE TOMBSTONE IS HELD BY THE DATABASE (CHECK ck_recipients_tombstone): a
row with deleted_at set has every reaching field NULL -- telegram_id,
email, locale, timezone, the delivery schedule -- and active false.
Forgetting is then not a convention of the code that wrote it.

BACKFILL, and why each value:
  version               0 -- written before versions existed; the wire
                        requires >= 1, so the product's first versioned
                        snapshot always supersedes it.
  snapshot_fingerprint  a sentinel that is not a hex digest: it is never
                        compared (an equal-version comparison needs
                        version 0 on the wire, which is refused).
  idempotency_key       'pre-0014:<id>' on messages and threads -- unique
                        by construction, recognisable for the downgrade.
  fingerprint           the same non-digest sentinel: a replay of a
                        pre-0014 call under its old key cannot exist (no
                        such key was ever sent).

NO DRAIN beyond 0013: every translation above is decided by the row.

THE DOWNGRADE IS NOT EXACT, and it says where:
  - locale NULL goes back to '' (the old form of "no language"); email,
    timezone and telegram_id stay NULL (the old columns allowed NULL);
  - a tombstone becomes an inactive recipient without fields;
  - a delivery `recipient_inactive` goes back to `suppressed`, the one
    old status the fold also counted as neither sent nor failed;
  - priority comes back as the server default 5 on every row.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014_resources_snapshot"
down_revision: str | None = "0013_lifecycle_outcomes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SENTINEL = "pre-0014:no-bytes".ljust(64, "-")
_KEY_PREFIX = "pre-0014:"
_BLANKABLE = ("locale", "email", "timezone")


def upgrade() -> None:
    # -- recipients: version, identity, tombstone --
    op.add_column("recipients", sa.Column("version", sa.BigInteger(), nullable=True))
    op.add_column(
        "recipients", sa.Column("snapshot_fingerprint", sa.String(64), nullable=True),
    )
    op.add_column(
        "recipients",
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute("UPDATE recipients SET version = 0")
    op.execute(
        sa.text("UPDATE recipients SET snapshot_fingerprint = :s").bindparams(
            s=_SENTINEL,
        )
    )
    op.alter_column("recipients", "version", nullable=False)
    op.alter_column("recipients", "snapshot_fingerprint", nullable=False)

    # -- one rule for "no value" --
    op.alter_column("recipients", "locale", nullable=True, server_default=None)
    for column in _BLANKABLE:
        op.execute(
            f"UPDATE recipients SET {column} = NULL "
            f"WHERE {column} IS NOT NULL AND btrim({column}) = ''"
        )
        op.create_check_constraint(
            f"ck_recipients_{column}_not_blank",
            "recipients",
            f"{column} IS NULL OR btrim({column}) <> ''",
        )
    op.execute("UPDATE recipients SET telegram_id = NULL WHERE telegram_id = 0")
    op.create_check_constraint(
        "ck_recipients_telegram_id_not_zero",
        "recipients",
        "telegram_id IS NULL OR telegram_id <> 0",
    )
    op.create_check_constraint(
        "ck_recipients_version_not_negative", "recipients", "version >= 0",
    )
    op.create_check_constraint(
        "ck_recipients_tombstone",
        "recipients",
        "deleted_at IS NULL OR (telegram_id IS NULL AND email IS NULL "
        "AND locale IS NULL AND timezone IS NULL "
        "AND allowed_windows IS NULL AND active = false)",
    )

    # -- idempotency of the resource calls that create --
    for table in ("messages", "threads"):
        op.add_column(
            table, sa.Column("idempotency_key", sa.String(200), nullable=True),
        )
        op.add_column(table, sa.Column("fingerprint", sa.String(64), nullable=True))
        op.execute(
            sa.text(
                f"UPDATE {table} SET idempotency_key = :p || id::text, "
                f"fingerprint = :s"
            ).bindparams(p=_KEY_PREFIX, s=_SENTINEL)
        )
        op.alter_column(table, "idempotency_key", nullable=False)
        op.alter_column(table, "fingerprint", nullable=False)
        op.create_index(
            f"uq_{table}_idempotency_key", table, ["idempotency_key"], unique=True,
        )

    # -- priority, with the index that carried it --
    op.drop_index(
        "ix_notifications_status_scheduled_priority", table_name="notifications",
    )
    op.drop_column("notifications", "priority")
    op.create_index(
        "ix_notifications_status_scheduled",
        "notifications",
        ["status", "scheduled_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_notifications_status_scheduled", table_name="notifications")
    op.add_column(
        "notifications",
        sa.Column("priority", sa.Integer(), server_default="5", nullable=False),
    )
    op.create_index(
        "ix_notifications_status_scheduled_priority",
        "notifications",
        ["status", "scheduled_at", "priority"],
    )
    op.execute(
        "UPDATE notification_deliveries SET status = 'suppressed' "
        "WHERE status = 'recipient_inactive'"
    )

    for table in ("threads", "messages"):
        op.drop_index(f"uq_{table}_idempotency_key", table_name=table)
        op.drop_column(table, "fingerprint")
        op.drop_column(table, "idempotency_key")

    for name in (
        "ck_recipients_tombstone",
        "ck_recipients_version_not_negative",
        "ck_recipients_telegram_id_not_zero",
        *(f"ck_recipients_{c}_not_blank" for c in _BLANKABLE),
    ):
        op.drop_constraint(name, "recipients", type_="check")
    op.execute("UPDATE recipients SET locale = '' WHERE locale IS NULL")
    op.alter_column("recipients", "locale", nullable=False, server_default="en")
    op.drop_column("recipients", "deleted_at")
    op.drop_column("recipients", "snapshot_fingerprint")
    op.drop_column("recipients", "version")
