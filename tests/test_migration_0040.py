# =============================================================================
# Migration 0040 -- notification_transitions.template_unresolved, up, down
# and up again
# =============================================================================
#
# The column and its CHECK (ck_transitions_template_unresolved) are there
# at 0040, gone at 0039, there again at 0040 -- the CHECK VALIDATED (it is
# added NOT VALID and validated in an autocommit block, FIX TX-3). A
# journal row written at 0039 -- by an explicit INSERT of 0039's columns,
# never the current model, which carries 0040's -- survives both ways and
# reads NULL at 0040.
#
# MUTATION: the downgrade not dropping the column (or the CHECK) turns
# test_up_down_up red.
# =============================================================================

from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from app.core.database import get_session_factory
from tests.test_migration_0039 import _migrate

_0039 = "0039_section_roster_declared"
_0040 = "0040_template_unresolved"
_CHECK = "ck_transitions_template_unresolved"


@pytest.fixture
async def at_head_afterwards() -> AsyncGenerator[None, None]:
    yield
    await _migrate("upgrade", "head")


async def _scalar(statement: str, **params: Any) -> Any:
    async with get_session_factory()() as session:
        return await session.scalar(text(statement), params)


async def _shape() -> tuple[Any, Any]:
    """(the column's type, the CHECK's `convalidated`) -- None if absent."""
    column = await _scalar(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'notification_transitions' "
        "AND column_name = 'template_unresolved'"
    )
    check = await _scalar(
        "SELECT convalidated FROM pg_constraint WHERE conname = :name",
        name=_CHECK,
    )
    return column, check


async def _journal_row_at_0039() -> UUID:
    """A job and its intake row in the form of 0039 (a job row: no
    recipient, no channel -- ck_transitions_subject_shape)."""
    job_id = uuid4()
    async with get_session_factory()() as session:
        await session.execute(
            text(
                "INSERT INTO notifications (id, type, title, body, "
                "target_type, target_value, idempotency_key, fingerprint, "
                "channels, expiry_layer, push_on, retry_max_attempts, "
                "retry_backoff_seconds, recipient_schedule, lane) VALUES "
                "(:id, 'unit_event_in_app', 'T', 'B', 'all', '*', :key, :fp, "
                "'[\"in_app\"]'::jsonb, 'default', 'none', 3, 0, 'respect', "
                "'normal')"
            ),
            {"id": job_id, "key": f"m0040:{job_id}", "fp": "a" * 64},
        )
        await session.execute(
            text(
                "INSERT INTO notification_transitions (notification_id, "
                "subject, step, outcome, attempt) VALUES "
                "(:id, 'job', 'intake', 'accepted', 0)"
            ),
            {"id": job_id},
        )
        await session.commit()
    return job_id


async def test_up_down_up(at_head_afterwards: None) -> None:
    await _migrate("downgrade", _0039)
    job_id = await _journal_row_at_0039()
    assert await _shape() == (None, None)
    for _ in range(2):
        await _migrate("upgrade", _0040)
        assert await _shape() == ("ARRAY", True)
        assert await _scalar(
            "SELECT count(*) FROM notification_transitions "
            "WHERE notification_id = :id AND template_unresolved IS NULL",
            id=job_id,
        ) == 1
        await _migrate("downgrade", _0039)
        assert await _shape() == (None, None)
        assert await _scalar(
            "SELECT count(*) FROM notification_transitions "
            "WHERE notification_id = :id", id=job_id,
        ) == 1
    await _migrate("upgrade", _0040)
    assert await _shape() == ("ARRAY", True)
