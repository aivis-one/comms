# =============================================================================
# COMMS Service -- Test Helpers
# =============================================================================
#
# TEST-BAND RULE (handoff / dispatch plan §5):
#   recipient.id = product user id -- the id-space is SHARED with the
#   product. Every test telegram_id therefore comes from a band of the
#   comms suite, one allocator per band below, each refusing to
#   overflow it: 80xxx (Phase 1), 81xxx (Phase 2), 82xxx (Phase 3a),
#   84xxx (Phase 3b), 85xxx (Phase 3c), 86xxx-88xxx (Phase 4a-4c), and
#   the small bands from 92100 on (seam / task bands). The constants
#   below are the list; this paragraph only names the shape.
#
#   83xxx and 89xxx belong to VELO's own suites -- never use them
#   here.
#
#   ONE COUNTER PER BAND FOR THE WHOLE SUITE, and that is safe (G2): a
#   counter runs on across the tests of one process, but no two tests
#   ever see each other's ids -- the autouse clean_db fixture
#   (tests/conftest.py) deletes every comms row before each test, and
#   recipients.telegram_id has no unique index (ix_recipients_telegram_id
#   is a plain one), so an id handed to an earlier test is gone, not
#   taken. What the shared counter can do is run out: a band's range
#   serves the whole process, and exhausting it raises RuntimeError
#   loudly -- never a silent reuse.
# =============================================================================

import itertools
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.audience.models import GroupMembership, Recipient
from app.audience.sync import apply_snapshot, snapshot_fingerprint
from app.core.config import settings
from app.messaging.models import Section

TELEGRAM_ID_BAND_START = 80000
TELEGRAM_ID_BAND_END = 80999

_telegram_id_counter = itertools.count(TELEGRAM_ID_BAND_START)


def next_telegram_id() -> int:
    """Next telegram_id from the comms Phase 1 band (80000-80999)."""
    tid = next(_telegram_id_counter)
    if tid > TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 80000-80999 exhausted"
        )
    return tid


PHASE2_TELEGRAM_ID_BAND_START = 81000
PHASE2_TELEGRAM_ID_BAND_END = 81999

_phase2_telegram_id_counter = itertools.count(PHASE2_TELEGRAM_ID_BAND_START)


def next_phase2_telegram_id() -> int:
    """Next telegram_id from the comms Phase 2 band (81000-81999)."""
    tid = next(_phase2_telegram_id_counter)
    if tid > PHASE2_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 81000-81999 exhausted"
        )
    return tid


PHASE3A_TELEGRAM_ID_BAND_START = 82000
PHASE3A_TELEGRAM_ID_BAND_END = 82999

_phase3a_telegram_id_counter = itertools.count(PHASE3A_TELEGRAM_ID_BAND_START)


def next_phase3a_telegram_id() -> int:
    """Next telegram_id from the comms Phase 3a band (82000-82999)."""
    tid = next(_phase3a_telegram_id_counter)
    if tid > PHASE3A_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 82000-82999 exhausted"
        )
    return tid


PHASE3C_TELEGRAM_ID_BAND_START = 85000
PHASE3C_TELEGRAM_ID_BAND_END = 85999

_phase3c_telegram_id_counter = itertools.count(PHASE3C_TELEGRAM_ID_BAND_START)


def next_phase3c_telegram_id() -> int:
    """Next telegram_id from the comms Phase 3c band (85000-85999)."""
    tid = next(_phase3c_telegram_id_counter)
    if tid > PHASE3C_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 85000-85999 exhausted"
        )
    return tid


PHASE3B_TELEGRAM_ID_BAND_START = 84000
PHASE3B_TELEGRAM_ID_BAND_END = 84999

_phase3b_telegram_id_counter = itertools.count(PHASE3B_TELEGRAM_ID_BAND_START)


def next_phase3b_telegram_id() -> int:
    """Next telegram_id from the comms Phase 3b band (84000-84999).

    83000-83999 is SKIPPED on purpose: VELO facts live there (see the
    band registry in the dispatch plan; discovered in Phase 1).
    """
    tid = next(_phase3b_telegram_id_counter)
    if tid > PHASE3B_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 84000-84999 exhausted"
        )
    return tid


PHASE4A_TELEGRAM_ID_BAND_START = 86000
PHASE4A_TELEGRAM_ID_BAND_END = 86999

_phase4a_telegram_id_counter = itertools.count(PHASE4A_TELEGRAM_ID_BAND_START)


def next_phase4a_telegram_id() -> int:
    """Next telegram_id from the comms Phase 4a band (86000-86999).

    85000-85999 belongs to Phase 3c; 83xxx / 89xxx are VELO's -- never
    reused here.
    """
    tid = next(_phase4a_telegram_id_counter)
    if tid > PHASE4A_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 86000-86999 exhausted"
        )
    return tid


PHASE4B_TELEGRAM_ID_BAND_START = 87000
PHASE4B_TELEGRAM_ID_BAND_END = 87999

_phase4b_telegram_id_counter = itertools.count(PHASE4B_TELEGRAM_ID_BAND_START)


def next_phase4b_telegram_id() -> int:
    """Next telegram_id from the comms Phase 4b band (87000-87999)."""
    tid = next(_phase4b_telegram_id_counter)
    if tid > PHASE4B_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 87000-87999 exhausted"
        )
    return tid


PHASE4C_TELEGRAM_ID_BAND_START = 88000
PHASE4C_TELEGRAM_ID_BAND_END = 88999

_phase4c_telegram_id_counter = itertools.count(PHASE4C_TELEGRAM_ID_BAND_START)


def next_phase4c_telegram_id() -> int:
    """Next telegram_id from the comms Phase 4c band (88000-88999)."""
    tid = next(_phase4c_telegram_id_counter)
    if tid > PHASE4C_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 88000-88999 exhausted"
        )
    return tid


SEAM_T2_TELEGRAM_ID_BAND_START = 92100
SEAM_T2_TELEGRAM_ID_BAND_END = 92139

_seam_t2_telegram_id_counter = itertools.count(SEAM_T2_TELEGRAM_ID_BAND_START)


def next_seam_t2_telegram_id() -> int:
    """Next telegram_id from the comms seam-T2 band (92100-92139).

    A NARROW band, unlike the per-phase thousands above: the seam adds
    a handful of actors, not a module.
    """
    tid = next(_seam_t2_telegram_id_counter)
    if tid > SEAM_T2_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 92100-92139 exhausted"
        )
    return tid


T51_TELEGRAM_ID_BAND_START = 92140
T51_TELEGRAM_ID_BAND_END = 92179

_t51_telegram_id_counter = itertools.count(T51_TELEGRAM_ID_BAND_START)


def next_t51_telegram_id() -> int:
    """Next telegram_id from the T-51 unread-aggregates band.

    Narrow, like the seam-T2 band above: the unread contracts add a
    handful of actors (a client, a master, an agent, a supervisor),
    not a module.
    """
    tid = next(_t51_telegram_id_counter)
    if tid > T51_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 92140-92179 exhausted"
        )
    return tid


T64_TELEGRAM_ID_BAND_START = 92180
T64_TELEGRAM_ID_BAND_END = 92199

_t64_telegram_id_counter = itertools.count(T64_TELEGRAM_ID_BAND_START)


def next_t64_telegram_id() -> int:
    """Next telegram_id from the T-64 recipient-upsert band.

    NARROWER THAN THE BAND THE HANDOFF ASSIGNED, on purpose. T-64 was
    issued 92140-92199, but 92140-92179 is already held by the T-51
    allocator right above -- the registry double-booked the lower half.
    Rather than hand out ids a sibling suite also hands out, this
    allocator takes only the free remainder. Twenty ids is ample: these
    tests need a handful of recipients, not a module.
    """
    tid = next(_t64_telegram_id_counter)
    if tid > T64_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 92180-92199 exhausted"
        )
    return tid


T67_TELEGRAM_ID_BAND_START = 92200
T67_TELEGRAM_ID_BAND_END = 92259

_t67_telegram_id_counter = itertools.count(T67_TELEGRAM_ID_BAND_START)


def next_t67_telegram_id() -> int:
    """Next telegram_id from the T-67 section-membership band.

    92200-92259, checked against every allocator above before being
    taken: the highest previously held id is 92199 (T-64), so this band
    starts one past the end of the occupied space rather than where a
    document says it should. The registry has double-booked before --
    see the T-64 allocator's own note.
    """
    tid = next(_t67_telegram_id_counter)
    if tid > T67_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 92200-92259 exhausted"
        )
    return tid


TK1_TELEGRAM_ID_BAND_START = 92260
TK1_TELEGRAM_ID_BAND_END = 92659

_tk1_telegram_id_counter = itertools.count(TK1_TELEGRAM_ID_BAND_START)


def next_tk1_telegram_id() -> int:
    """Next telegram_id from the TK-1 band (thread hand-over and reopen).

    92260-92659, one past the highest id held above (92259, T-67); no
    literal in tests/ or app/ sits in it.
    """
    tid = next(_tk1_telegram_id_counter)
    if tid > TK1_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 92260-92659 exhausted"
        )
    return tid


TK2_TELEGRAM_ID_BAND_START = 92660
TK2_TELEGRAM_ID_BAND_END = 93059

_tk2_telegram_id_counter = itertools.count(TK2_TELEGRAM_ID_BAND_START)


def next_tk2_telegram_id() -> int:
    """Next telegram_id from the TK-2 band (deleting a thread).

    92660-93059, one past the highest id held above (92659, TK-1); no
    literal in tests/ or app/ sits in it.
    """
    tid = next(_tk2_telegram_id_counter)
    if tid > TK2_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 92660-93059 exhausted"
        )
    return tid


RV1_TELEGRAM_ID_BAND_START = 93060
RV1_TELEGRAM_ID_BAND_END = 93459

_rv1_telegram_id_counter = itertools.count(RV1_TELEGRAM_ID_BAND_START)


def next_rv1_telegram_id() -> int:
    """Next telegram_id from the RV-1 band (a thread's revision).

    93060-93459, one past the highest id held above (93059, TK-2); no
    literal in tests/ or app/ sits in it.
    """
    tid = next(_rv1_telegram_id_counter)
    if tid > RV1_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 93060-93459 exhausted"
        )
    return tid


EM1_TELEGRAM_ID_BAND_START = 93460
EM1_TELEGRAM_ID_BAND_END = 93859

_em1_telegram_id_counter = itertools.count(EM1_TELEGRAM_ID_BAND_START)


def next_em1_telegram_id() -> int:
    """Next telegram_id from the EM-1 band (the product's own email).

    93460-93859, one past the highest id held above (93459, RV-1); no
    literal in tests/ or app/ sits in it.
    """
    tid = next(_em1_telegram_id_counter)
    if tid > EM1_TELEGRAM_ID_BAND_END:
        raise RuntimeError(
            "comms test telegram_id band 93460-93859 exhausted"
        )
    return tid


async def create_recipient(
    session: AsyncSession,
    *,
    recipient_id: UUID | None = None,
    telegram_id: int | None = None,
    email: str | None = None,
    locale: str = "en",
    active: bool = True,
) -> Recipient:
    """Create a recipient row (id = simulated product user id)."""
    resolved_tg = telegram_id if telegram_id is not None else next_telegram_id()
    recipient = Recipient(
        id=recipient_id or uuid4(),
        # A row written straight into the table stands for a snapshot
        # the product sent at version 1 (F1.4: every row has one).
        version=1,
        snapshot_fingerprint=snapshot_fingerprint(
            telegram_id=resolved_tg, email=email, locale=locale,
            timezone=None, active=active,
        ),
        telegram_id=resolved_tg,
        email=email,
        locale=locale,
        active=active,
    )
    session.add(recipient)
    await session.flush()
    return recipient


async def add_to_group(
    session: AsyncSession,
    group_key: str,
    recipient_id: UUID,
) -> GroupMembership:
    """Add a recipient to an opaque product group."""
    membership = GroupMembership(
        group_key=group_key,
        recipient_id=recipient_id,
    )
    session.add(membership)
    await session.flush()
    return membership


async def create_section(
    session: AsyncSession,
    *,
    key: str,
    label: str | None = None,
) -> Section:
    """Create a messaging Section row (Phase 4a)."""
    section = Section(key=key, label=label if label is not None else key)
    session.add(section)
    await session.flush()
    return section


# -- Intake (F1.2) -------------------------------------------------------------
# Every job carries an idempotency key and a fingerprint. Tests that
# create notifications directly (not through intake) get a FRESH key per
# call -- two direct creates never collide -- and one fixed fingerprint:
# the key alone decides, the fingerprint only matters to intake tests,
# which build their own.
TEST_FINGERPRINT = "f" * 64


def intake_fields() -> dict[str, str]:
    """A fresh idempotency key and the test fingerprint."""
    return {"idempotency_key": f"test:{uuid4()}", "fingerprint": TEST_FINGERPRINT}


def notification_row_fields(channels: list[str] | None = None) -> dict[str, object]:
    """The NOT NULL intake columns for a Notification built DIRECTLY
    (tests that construct rows around intake, F1.2). A fresh key, the
    test fingerprint, the channels the row is for (in_app unless said),
    the default expiry layer, the default push_on, the default
    recipient_schedule, the default lane and the default transport
    retry -- what intake would have written for a type that declares
    none of them (P3-1, H1, H3, P4-1)."""
    return {
        **intake_fields(),
        "channels": channels if channels is not None else ["in_app"],
        "expiry_layer": "default",
        "push_on": "none",
        "recipient_schedule": "respect",
        "lane": "normal",
        "retry_max_attempts": settings.notification_max_delivery_attempts,
        "retry_backoff_seconds": (
            settings.notification_retry_backoff_base_seconds
        ),
    }


def configure_every_channel(monkeypatch: object) -> None:
    """Make every external channel LIVE on the suite's settings.

    The suite runs with every external channel's keys blanked
    (tests/conftest.py), and since F1.1 the startup path refuses a
    route into a channel the deploy did not configure. The fixture
    profile routes types to telegram and email (F1.2: the channel is
    the profile's), so a test that loads it through the STARTUP path
    must stand on a deploy that has them.
    """
    from app.core.config import settings

    for name, value in {
        "telegram_bot_token": "123456:unit-test-bot-token",
        "telegram_bot_url": "https://t.me/unit_test_bot",
        "email_mailgun_api_key": "key-unit-test",
        "email_mailgun_domain": "mg.unit-test.invalid",
        "email_from_address": "comms@unit-test.invalid",
    }.items():
        monkeypatch.setattr(settings, name, value)  # type: ignore[attr-defined]



# -- Snapshots (F1.4) ----------------------------------------------------------
# A snapshot carries the product's monotonic version. Tests that apply a
# sequence of snapshots to one recipient -- the way a product changes a
# person over time -- take the next version from one counter, so each
# call supersedes the one before it, as the product's would.
_snapshot_versions = itertools.count(1)


def next_snapshot_version() -> int:
    return next(_snapshot_versions)


async def upsert_snapshot(session: AsyncSession, **fields: Any) -> Recipient:
    """apply_snapshot with the next version (see above)."""
    return await apply_snapshot(
        session, version=next_snapshot_version(), **fields,
    )


# -----------------------------------------------------------------------------
# A statement's SHAPE -- how a test finds the statement it is about (G2)
# -----------------------------------------------------------------------------
#
# Tests that capture SQL (before_cursor_execute, pg_stat_activity) pick
# the statement they are about out of everything the code sent. They
# used to pick it by a substring -- "FROM notifications" in s, "ORDER BY"
# in s, a leading "SELECT notification_deliveries.id" -- and a substring
# cannot tell the top-level FROM from a subquery's, or an ORDER BY of
# the statement from one inside an EXISTS. sql_shape reads the clauses
# AT THE TOP LEVEL (outside parentheses and quotes) once, here, and a
# test matches fields of the shape.
#
# THE FRAGILITY, NAMED: the shape is read from the TEXT SQLAlchemy
# compiles (or Postgres reports in pg_stat_activity.query) -- its
# keyword spelling, `FROM t` / `ORDER BY a, b` layout, upper case. A
# SQLAlchemy release that changes that layout breaks the tests that
# find statements, all at once and here, in one place to change; it
# breaks no test silently, because a test that finds no statement (or
# two) fails on its own unpacking. tests/test_g2_hygiene.py holds that
# no test searches SQL by substring outside this helper.

_TOP_CLAUSES = re.compile(
    r"\b(SELECT|UPDATE|DELETE FROM|INSERT INTO|EXPLAIN|WITH|FROM|WHERE|"
    r"GROUP BY|HAVING|ORDER BY|LIMIT|OFFSET|RETURNING|SET|VALUES|"
    r"FOR NO KEY UPDATE|FOR UPDATE|FOR KEY SHARE|FOR SHARE)\b"
)
_SOURCES = re.compile(
    r"\b(?:FROM|JOIN|UPDATE|DELETE FROM|INSERT INTO)\s+([a-z_][a-z0-9_]*)"
)


@dataclass(frozen=True)
class SqlShape:
    """The top-level clauses of one SQL statement.

    verb    -- the first keyword: SELECT, UPDATE, DELETE FROM,
               INSERT INTO, EXPLAIN or WITH;
    columns -- a SELECT's own select list;
    table   -- the first table of the top-level FROM (the target of an
               UPDATE, DELETE or INSERT), or None;
    where, order_by -- the top-level clause's text, "" when absent;
    limit   -- whether the statement itself has a LIMIT;
    lock    -- "FOR UPDATE", "FOR NO KEY UPDATE", ... with " SKIP
               LOCKED" when so, or None;
    sources -- every table named after FROM / JOIN at any depth.
    """

    verb: str
    columns: str
    table: str | None
    where: str
    order_by: str
    limit: bool
    lock: str | None
    sources: frozenset[str]

    def compares(self, column: str) -> bool:
        """Whether the top-level WHERE compares `column` (table.column)
        with =, IN or an ordering operator."""
        return bool(re.search(
            rf"(?<![\w.]){re.escape(column)} (?:=|!=|<>|<=|>=|<|>|IN\b)",
            self.where,
        ))


def _top_level(text: str) -> list[tuple[int, str]]:
    """(position, keyword) of every clause keyword outside parentheses
    and quotes."""
    depth = 0
    quote: str | None = None
    outside: list[bool] = []
    for char in text:
        if quote is not None:
            outside.append(False)
            if char == quote:
                quote = None
            continue
        if char in "'\"":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        outside.append(depth == 0 and quote is None and char not in "()")
    return [
        (m.start(), m.group(1)) for m in _TOP_CLAUSES.finditer(text)
        if outside[m.start()]
    ]


def sql_shape(statement: Any) -> SqlShape:
    """The shape of a statement: SQL text, or a SQLAlchemy statement
    (compiled with its default dialect by str())."""
    text = " ".join(str(statement).split())
    marks = _top_level(text)
    clauses: dict[str, str] = {}
    for index, (start, keyword) in enumerate(marks):
        end = marks[index + 1][0] if index + 1 < len(marks) else len(text)
        clauses.setdefault(keyword, text[start + len(keyword):end].strip())
    verb = marks[0][1] if marks and marks[0][0] == 0 else ""
    if verb in ("UPDATE", "DELETE FROM", "INSERT INTO"):
        target = clauses.get(verb, "")
        table = target.split()[0] if target else None
    else:
        source = clauses.get("FROM", "")
        table = source.split()[0].rstrip(",") if source else None
    lock = next(
        (k for k in ("FOR NO KEY UPDATE", "FOR UPDATE", "FOR KEY SHARE",
                     "FOR SHARE") if k in clauses),
        None,
    )
    if lock is not None and clauses[lock].startswith("SKIP LOCKED"):
        lock = f"{lock} SKIP LOCKED"
    return SqlShape(
        verb=verb,
        columns=clauses.get("SELECT", "") if verb == "SELECT" else "",
        table=table,
        where=clauses.get("WHERE", ""),
        order_by=clauses.get("ORDER BY", ""),
        limit="LIMIT" in clauses,
        lock=lock,
        sources=frozenset(_SOURCES.findall(text)),
    )
