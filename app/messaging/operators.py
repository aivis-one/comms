# =============================================================================
# COMMS Service -- Messaging: Operators (Phase 4b, items 1-4)
# =============================================================================
#
# Operator BEHAVIOR over the 4a data layer: resolve, claim, visibility,
# retag. The operator target is the polymorphic pair from 4a
# (operator_kind, operator_value) -- see app/messaging/models.py.
#
# BL-1 DISCIPLINE: resolve_operator returns a DESCRIPTOR (OperatorScope),
# never a materialized recipient list. Visibility is expressed as a
# QUERY (list_visible_threads) -- the set of recipients serving a
# section is never produced here.
#
# NOT here (Phase 4c): write-authz (who may claim / retag / post),
# message -> notification, the msg_* gate. 4b decides SCOPE (who can
# SEE, and the atomic ownership of a claim); it does not gate writes.
# Callers commit.
# =============================================================================

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import structlog
from sqlalchemy import (
    ColumnElement,
    and_,
    func,
    or_,
    select,
    tuple_,
    update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundError, ValidationError
from app.messaging.constants import OperatorKind
from app.messaging.models import Section, Thread
from app.messaging.threads import _is_dedup_violation, _recipient_exists

logger = structlog.get_logger()

# Keyset page bounds for list_visible_threads -- mirror the inbox
# (INBOX_MAX_PAGE_SIZE): clamp, do not reject an out-of-range limit.
VISIBLE_THREADS_MAX_PAGE_SIZE = 100
VISIBLE_THREADS_DEFAULT_PAGE_SIZE = 20


@dataclass(frozen=True)
class OperatorScope:
    """A thread's resolved operator target (arch doc §2.4).

    A DESCRIPTOR, not a recipient list (BL-1). Downstream code turns it
    into query predicates (visibility) or an ownership check (claim);
    the set of agents serving a section is never materialized here.

    kind == USER    -> `value` is the single responsible recipient
                       (the master; pre-assigned as the thread's
                       assignee at creation).
    kind == SECTION -> `value` is the section id. v1 membership is
                       TRIVIAL: every agent serves every section, so
                       "does operator X serve this scope" is always
                       true for a section (rich membership is starred).
    """

    kind: OperatorKind
    value: UUID

    @property
    def is_claimable(self) -> bool:
        """Section threads are claimed by an agent; a user thread's
        operator is fixed by the form and never claimed."""
        return self.kind is OperatorKind.SECTION


def resolve_operator(
    operator_kind: OperatorKind,
    operator_value: UUID,
) -> OperatorScope:
    """Resolve a thread's operator pair into an OperatorScope.

    Pure over the pair (no DB): the referent was validated at creation
    (4a `_require_operator_referent`). This is the single seam that
    "resolves" an operator -- retag re-runs it after a section/subject
    change, and it is where the BL-1 no-materialized-list rule lives.
    """
    return OperatorScope(kind=operator_kind, value=operator_value)


# ---------------------------------------------------------------------------
# Write-authz predicates (Phase 4c item 4) -- role IN the thread, not
# identity (identity is the product proxy's concern, arch decision 14).
# These catch the role violations comms can see from thread state. v1
# section membership is TRIVIAL (OperatorScope): any agent serves any
# section, so a section thread's operator verbs are open to any actor
# the proxy vouches for; a user thread is served ONLY by its master
# (assignee), so a foreign actor is rejected. Rich (starred) membership
# tightens the section case in Phase 7.
# ---------------------------------------------------------------------------
def can_post_message(thread: Thread, actor: UUID) -> bool:
    """A message may be posted by the PARTICIPANT (client) or the
    SERVING operator (assignee)."""
    return actor == thread.client or actor == thread.assignee


def can_claim(thread: Thread) -> bool:
    """Only a SECTION thread is claimable; a user thread's operator is
    fixed by the form and is never claimed."""
    scope = resolve_operator(
        OperatorKind(thread.operator_kind), thread.operator_value
    )
    return scope.is_claimable


def can_operate(thread: Thread, actor: UUID) -> bool:
    """status / retag: the SERVING operator (assignee), OR any agent on a
    SECTION thread (v1 trivial membership). A user thread is served only
    by its master (its assignee)."""
    if actor == thread.assignee:
        return True
    scope = resolve_operator(
        OperatorKind(thread.operator_kind), thread.operator_value
    )
    return scope.kind is OperatorKind.SECTION


async def claim_thread(
    session: AsyncSession,
    *,
    thread_id: UUID,
    operator: UUID,
    when: datetime | None = None,
) -> bool:
    """Atomically claim an unassigned thread for `operator`.

    Conditional UPDATE `assignee = me WHERE id = ? AND assignee IS
    NULL` + rowcount -- exactly one of two concurrent claimers wins;
    the loser sees rowcount 0. No special-case for user threads: D1(i)
    pre-assigns their assignee at creation, so `assignee IS NULL` never
    matches a user thread and the guard rejects it for free.

    Returns True if THIS call claimed the thread; False if it was
    already assigned. Raises NotFoundError if the thread does not exist
    OR the operator referent does not exist.
    """
    # Validate the operator referent in code: the assignee FK (RESTRICT)
    # would otherwise surface a raw IntegrityError -> 500 on a bad
    # operator; a missing referent is a clean 404 (symmetric with how
    # create_or_get_thread validates its client / operator referents).
    if not await _recipient_exists(session, operator):
        raise NotFoundError(f"operator recipient {operator} does not exist")

    now = when if when is not None else datetime.now(UTC)
    result = await session.execute(
        update(Thread)
        .where(Thread.id == thread_id, Thread.assignee.is_(None))
        .values(assignee=operator, assigned_at=now)
    )
    rowcount: int = result.rowcount  # type: ignore[attr-defined]
    if rowcount == 1:
        logger.info(
            "thread_claimed",
            thread_id=str(thread_id),
            operator=str(operator),
        )
        return True

    # rowcount 0 -> already assigned, OR no such thread. Threads are
    # immortal (never deleted), so this get is not racy; it only
    # disambiguates the two failure modes.
    thread = await session.get(Thread, thread_id)
    if thread is None:
        raise NotFoundError(f"thread {thread_id} does not exist")
    return False


def participation_clause(participant: UUID) -> ColumnElement[bool]:
    """Threads `participant` TAKES PART IN -- all three participant roles.

        participates(me) = client == me
                           OR assignee == me
                           OR (user thread AND operator_value == me)

    NOT THE SAME PREDICATE AS list_visible_threads, and the difference
    is the point of this docstring. Visibility answers "may I read
    it"; participation answers "is this conversation mine". They part
    company on the UNCLAIMED SECTION THREAD: it is in every agent's
    POOL (visible, assignee IS NULL), but nobody takes part in it yet
    -- client is the customer, assignee is empty, and operator_value is
    a SECTION id, not a recipient. So a pool row is visible-not-
    participating, and every T-51 aggregate treats it as "not mine":
    absent from the summary, absent from the batch counts, no `unread`
    key on the list row. That is a real production state, not a corner
    case -- the product's operator list is read with
    is_supervisor=False, so pool rows are what an operator normally
    sees. Supervisor widening produces the same state by a second
    route (foreign threads), not the only one.

    A DESCRIPTOR-FREE predicate (BL-1 discipline): a clause, never a
    materialized set of threads or of agents.

    # KNOWN CEILING (third OR branch is currently redundant;
    # acknowledged by design):
    #   1. Mechanics: `operator_kind == user AND operator_value == me`
    #      adds no row that `assignee == me` would not already add,
    #      because a user thread is PRE-ASSIGNED assignee =
    #      operator_value at creation (D1(i), threads.py) and nothing
    #      can part them: retag rejects the user form outright and
    #      claim requires assignee IS NULL, which a user thread never
    #      has.
    #   2. Status: acknowledged by design.
    #   3. Backlog ref: none -- this is a property of the thread
    #      constructor, not a deferred item.
    #   4. Promotion trigger: a path appears that sets assignee and
    #      operator_value to different recipients on a user thread (a
    #      master hand-off / re-assignment being the obvious one).
    #   5. Agreed fix: none needed -- the branch already covers it; the
    #      trigger only means the branch stops being redundant.
    #   6. Rejected: dropping the branch as dead code. The invariant
    #      that makes it redundant lives in a CONSTRUCTOR, not in the
    #      schema, so nothing would fail loudly when it stops holding
    #      -- the DM operator's badge would simply go dark. It also
    #      keeps ix_threads_operator_user in play: the planner does not
    #      know the invariant and prices the OR branch on its own.
    """
    return or_(
        Thread.client == participant,
        Thread.assignee == participant,
        and_(
            Thread.operator_kind == OperatorKind.USER,
            Thread.operator_value == participant,
        ),
    )


async def list_visible_threads(
    session: AsyncSession,
    *,
    operator: UUID,
    is_supervisor: bool = False,
    limit: int = VISIBLE_THREADS_DEFAULT_PAGE_SIZE,
    cursor: tuple[datetime, UUID] | None = None,
) -> tuple[Sequence[Thread], tuple[datetime, UUID] | None]:
    """Threads an operator may SEE, most-recently-active first.

    The set-level form of the OperatorScope rule (resolve_operator):
        visible(me) = assignee == me
                      OR (section thread AND unassigned [AND I serve it])
      - assignee == me covers BOTH a user thread I master (D1(i)
        pre-assign) and a section thread I have claimed;
      - an unassigned section thread is in every agent's pool (v1
        membership is trivial); a foreign user thread (mastered by
        someone else, assignee != me) is therefore invisible.
      - v1 membership slot: rich operator<->section membership (starred)
        adds `AND <section> IN (my_sections)` to the section clause.

    NOTE (P-2, supervisor read-only marker): a supervisor sees EVERYTHING, but this
    read-only property is NOT enforced by the data layer -- is_supervisor
    only WIDENS the read. The write-ban (a supervisor must not mutate)
    rides with write-authz in Phase 4c (§6d), the same seam as the
    sender write-authz assumption; 4b never gates writes.

    Keyset pagination (Phase 4c item 5): ordered by
    COALESCE(last_message_at, created_at) DESC, id DESC -- the EXACT
    ix_threads_activity order (EXPLAIN-confirmed Index Scan) and the
    same keyset shape as the inbox. `limit` clamps to 1..100 (default
    20, never rejected); `cursor` is the (activity, id) of the last row
    already seen and the next page is WHERE (activity, id) < cursor.
    The id tiebreak is DESC (was id ASC in 4b) to line up with the
    index and the cursor codec. Returns (threads, next_cursor);
    next_cursor is None on the final page. The opaque base64 wire form
    of the cursor lives at the API edge (parallel to the inbox codec).
    """
    limit = max(1, min(limit, VISIBLE_THREADS_MAX_PAGE_SIZE))
    activity = func.coalesce(Thread.last_message_at, Thread.created_at)

    stmt = select(Thread)
    if not is_supervisor:
        stmt = stmt.where(
            or_(
                Thread.assignee == operator,
                and_(
                    Thread.operator_kind == OperatorKind.SECTION,
                    Thread.assignee.is_(None),
                ),
            )
        )
    if cursor is not None:
        # Row-value comparison against the (activity, id) cursor: the
        # next page is strictly "older" in the DESC order. The
        # expression index carries this as an Index Cond, not a filter.
        stmt = stmt.where(tuple_(activity, Thread.id) < cursor)

    stmt = stmt.order_by(activity.desc(), Thread.id.desc()).limit(limit + 1)
    rows = list(await session.scalars(stmt))

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor: tuple[datetime, UUID] | None = None
    if has_more and page:
        last = page[-1]
        next_cursor = (last.last_message_at or last.created_at, last.id)
    return page, next_cursor


async def retag_thread(
    session: AsyncSession,
    *,
    thread_id: UUID,
    section: UUID,
    subject_type: str | None = None,
    subject_id: str | None = None,
) -> Thread:
    """Retag a SECTION thread to a new section and/or subject_ref.

    Mutability by operator form (arch doc §2.4): a `user` thread's axes
    are FROZEN (retag rejected); a `section` thread is retag-capable.
    Sets (operator_value=section, subject_ref), re-resolves the operator
    and RESETS assignee (the thread returns to the pool, must be
    re-claimed). The thread and its history are preserved: thread_id is
    stable and the dedup key is NOT recomputed -- threads are NEVER
    merged. The partial unique indexes still guard, so a retag that
    would duplicate an existing (client, operator, subject) is REJECTED
    (one thread per entity), not merged.

    # KNOWN CEILING (P-1 -- section lifecycle; acknowledged by design):
    #   1. Mechanics: operator_value carries NO foreign key (it is the
    #      polymorphic half of the operator pair -- a recipient for
    #      user, a section for section), so deleting a `sections` row is
    #      NOT blocked by the database; a section thread would then
    #      point at a vanished section.
    #   2. Status: acknowledged by design.
    #   3. Backlog ref: BL-5 (section lifecycle -- delete / move).
    #   4. Promotion trigger: a section-deletion path is introduced, OR
    #      cbshome onboards section operators (Phase 7) -- until then
    #      sections have no consumer in v1 (VELO is user-form) and no
    #      deletion path exists in the code.
    #   5. Agreed fix: reassign-then-delete -- retag every thread off a
    #      section onto another/archive section, THEN delete it.
    #   6. Rejected: a hard FK on operator_value (would break the
    #      polymorphic pair -- a nullable two-FK design forces NULL into
    #      the dedup key, and two NULLs never collide -> "one eternal
    #      dm" quietly breaks); a silent orphan / cascade (would break
    #      thread immortality by thread_id).
    """
    thread = await session.get(Thread, thread_id)
    if thread is None:
        raise NotFoundError(f"thread {thread_id} does not exist")
    if thread.operator_kind != OperatorKind.SECTION:
        raise ValidationError(
            "user thread axes are frozen; retag is only allowed on "
            "section threads"
        )
    if (subject_type is None) != (subject_id is None):
        raise ValidationError(
            "subject_ref must be both-or-neither: "
            f"subject_type={subject_type!r} subject_id={subject_id!r}"
        )
    if await session.get(Section, section) is None:
        raise NotFoundError(f"section {section} does not exist")

    # Apply the new tag; re-resolve the operator, reset assignee.
    new_scope = resolve_operator(OperatorKind.SECTION, section)
    thread.operator_value = new_scope.value
    thread.subject_type = subject_type
    thread.subject_id = subject_id
    thread.assignee = None
    thread.assigned_at = None
    try:
        # SAVEPOINT: a dedup collision rolls back only this UPDATE and
        # leaves the outer session usable for the caller's rollback.
        async with session.begin_nested():
            await session.flush()
    except IntegrityError as exc:
        if not _is_dedup_violation(exc):
            raise
        raise ValidationError(
            "retag would duplicate an existing thread for this entity "
            "(one thread per entity); threads are never merged"
        ) from exc

    logger.info(
        "thread_retagged",
        thread_id=str(thread_id),
        section=str(section),
    )
    return thread
