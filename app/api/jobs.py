# =============================================================================
# COMMS Service -- Reading a job by its key (P2-2, spec §6.2 / §6.6 / §7.1)
# =============================================================================
#
# The product's last link: its own record -> the idempotency key -> the
# job in comms -> the steps. Reading is the source of truth (§7.1); a
# later push only says "go read".
#
#   GET /api/v1/notifications/by-key?key=...             -- the summary
#   GET /api/v1/notifications/by-key/deliveries?key=...  -- a page
#   GET /api/v1/notifications/by-key/path?key=...        -- a page
#
# THE KEY IS A QUERY PARAMETER, not a path segment: a product chooses
# its keys (up to 200 characters, any characters), and a path segment
# does not survive proxies and clients -- "a//b" is merged, "a/../b" is
# normalized, "%2F" is decoded differently by each hop. A query
# parameter carries any key under standard percent-encoding; the one
# trap -- an unencoded "+" reads as a space -- is the encoder's to
# avoid, and deploy/INTEGRATION.md section 8 says so.
#
# READ ONLY. Every route runs in a read-only REPEATABLE READ
# transaction: nothing here can write, and the summary's queries see
# one snapshot. A fixed number of queries per read, not one per
# delivery.
#
# THE WIRE FORM IS A CLOSED FIELD SET per form (FORMS below), and
# deploy/INTEGRATION.md section 8 carries one table per form; a test
# holds the two to each other both ways. NEVER the letter: no title,
# body, action_data, channel_options, error_message. A provider's words
# come from the journal only -- sanitized, and cleared by forgetting.
#
# Adding a field is not a break (unknown fields are ignored by
# contract): phase 5 adds the model's answer to `job` that way.
# =============================================================================

from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_service_auth
from app.api.paging import (
    PAGE_LIMIT_DEFAULT,
    decode_cursor,
    decode_seq_cursor,
    page,
    page_by_seq,
    page_limit,
)
from app.core.constants import MAX_IDEMPOTENCY_KEY_LEN
from app.core.database import get_session_factory
from app.engine.models import (
    IntakeOutcome,
    Notification,
    NotificationDelivery,
    NotificationTransition,
)
from app.engine.service import (
    job_id_by_key,
    list_job_deliveries,
    list_job_path,
    read_job_by_key,
)

router = APIRouter(
    prefix="/api/v1/notifications/by-key",
    tags=["jobs"],
    dependencies=[Depends(require_service_auth)],
)

# -----------------------------------------------------------------------------
# The closed field sets -- one per form, each a table in INTEGRATION.md §8
# -----------------------------------------------------------------------------

SUMMARY_FIELDS = ("idempotency_key", "intake", "job")
INTAKE_ITEM_FIELDS = ("outcome", "reason", "received_at", "notification_id")
JOB_FIELDS = (
    "id", "type", "category", "target_type", "target_value", "correlation",
    "status", "created_at", "scheduled_at", "expiry_at", "pipeline",
    "deliveries",
)
PIPELINE_FIELDS = ("attempts", "step", "error", "retry_at")
DELIVERY_COUNT_FIELDS = ("channel", "status", "count")
DELIVERY_ITEM_FIELDS = (
    "recipient_id", "channel", "status", "attempts", "failure_class",
    "wait_reason", "next_retry_at", "sent_at", "read_at", "created_at",
)
PATH_ITEM_FIELDS = (
    "at", "subject", "step", "outcome", "recipient_id", "channel",
    "attempt", "wait_reason", "wait_until", "failure_class", "category",
    "error", "provider_text",
)

FORMS: dict[str, tuple[str, ...]] = {
    "summary": SUMMARY_FIELDS,
    "intake_item": INTAKE_ITEM_FIELDS,
    "job": JOB_FIELDS,
    "pipeline": PIPELINE_FIELDS,
    "delivery_count": DELIVERY_COUNT_FIELDS,
    "delivery_item": DELIVERY_ITEM_FIELDS,
    "path_item": PATH_ITEM_FIELDS,
}


def _closed(fields: tuple[str, ...], values: dict[str, Any]) -> dict[str, Any]:
    """The wire dict of one form: exactly its fields, in its order. A
    value for a field outside the set, or a field without a value, is a
    defect here -- not a quiet extra or a quiet gap on the wire."""
    if set(values) != set(fields):
        raise RuntimeError(
            f"wire form drifted: extra {sorted(set(values) - set(fields))}, "
            f"missing {sorted(set(fields) - set(values))}"
        )
    return {name: values[name] for name in fields}


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _str(value: UUID | None) -> str | None:
    return str(value) if value is not None else None


def _intake_item(row: IntakeOutcome) -> dict[str, Any]:
    return _closed(INTAKE_ITEM_FIELDS, {
        "outcome": row.outcome,
        "reason": row.reason,
        "received_at": _iso(row.received_at),
        "notification_id": _str(row.notification_id),
    })


def _job(job: Notification, counts: list[tuple[str, str, int]]) -> dict[str, Any]:
    return _closed(JOB_FIELDS, {
        "id": str(job.id),
        "type": job.type,
        "category": job.category,
        "target_type": job.target_type,
        "target_value": job.target_value,
        "correlation": job.correlation,
        "status": job.status,
        "created_at": _iso(job.created_at),
        "scheduled_at": _iso(job.scheduled_at),
        "expiry_at": _iso(job.expiry_at),
        "pipeline": _closed(PIPELINE_FIELDS, {
            "attempts": job.pipeline_attempts,
            "step": job.pipeline_step,
            "error": job.pipeline_error,
            "retry_at": _iso(job.pipeline_retry_at),
        }),
        "deliveries": [
            _closed(DELIVERY_COUNT_FIELDS, {
                "channel": channel, "status": status, "count": count,
            })
            for channel, status, count in counts
        ],
    })


def _delivery_item(row: NotificationDelivery) -> dict[str, Any]:
    return _closed(DELIVERY_ITEM_FIELDS, {
        "recipient_id": str(row.recipient_id),
        "channel": row.channel,
        "status": row.status,
        "attempts": row.attempts,
        "failure_class": row.failure_class,
        "wait_reason": row.wait_reason,
        "next_retry_at": _iso(row.next_retry_at),
        "sent_at": _iso(row.sent_at),
        "read_at": _iso(row.read_at),
        "created_at": _iso(row.created_at),
    })


def _path_item(row: NotificationTransition) -> dict[str, Any]:
    return _closed(PATH_ITEM_FIELDS, {
        "at": _iso(row.at),
        "subject": row.subject,
        "step": row.step,
        "outcome": row.outcome,
        "recipient_id": _str(row.recipient_id),
        "channel": row.channel,
        "attempt": row.attempt,
        "wait_reason": row.wait_reason,
        "wait_until": _iso(row.wait_until),
        "failure_class": row.failure_class,
        "category": row.category,
        "error": row.error,
        "provider_text": row.provider_text,
    })


# -----------------------------------------------------------------------------
# The session: read only, one snapshot
# -----------------------------------------------------------------------------


async def read_only_snapshot() -> AsyncGenerator[AsyncSession, None]:
    """A session whose transaction is READ ONLY and REPEATABLE READ; it
    is always rolled back. The options are the transaction's, not the
    pooled connection's: the next session gets the defaults back."""
    session = get_session_factory()()
    try:
        await session.connection(execution_options={
            "postgresql_readonly": True,
            "isolation_level": "REPEATABLE READ",
        })
        yield session
    finally:
        await session.rollback()
        await session.close()


def _key_param() -> Any:
    return Query(min_length=1, max_length=MAX_IDEMPOTENCY_KEY_LEN)


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------


@router.get("")
async def read_by_key(
    key: str = _key_param(),
    session: AsyncSession = Depends(read_only_snapshot),
) -> dict[str, Any]:
    """The summary: the key's intake outcomes, its job (or null), the
    job's deliveries counted by channel and status. 404 when the key
    answers nothing at all."""
    read = await read_job_by_key(session, key)
    return _closed(SUMMARY_FIELDS, {
        "idempotency_key": key,
        "intake": [_intake_item(row) for row in read.intake],
        "job": (
            _job(read.job, read.delivery_counts) if read.job is not None else None
        ),
    })


@router.get("/deliveries")
async def read_deliveries(
    key: str = _key_param(),
    limit: int = Query(default=PAGE_LIMIT_DEFAULT),
    cursor: str | None = Query(default=None),
    session: AsyncSession = Depends(read_only_snapshot),
) -> dict[str, Any]:
    """A page of the job's deliveries, oldest first."""
    size = page_limit(limit)
    position = decode_cursor(cursor)
    notification_id = await job_id_by_key(session, key)
    rows, next_cursor = await list_job_deliveries(
        session, notification_id, limit=size, cursor=position,
    )
    return page(rows, next_cursor, _delivery_item)


@router.get("/path")
async def read_path(
    key: str = _key_param(),
    limit: int = Query(default=PAGE_LIMIT_DEFAULT),
    cursor: str | None = Query(default=None),
    session: AsyncSession = Depends(read_only_snapshot),
) -> dict[str, Any]:
    """A page of the job's path through the journal, in order."""
    size = page_limit(limit)
    position = decode_seq_cursor(cursor)
    notification_id = await job_id_by_key(session, key)
    rows, next_position = await list_job_path(
        session, notification_id, limit=size, cursor=position,
    )
    return page_by_seq(rows, next_position, _path_item)

