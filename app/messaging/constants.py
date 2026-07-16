# =============================================================================
# COMMS Service -- Messaging Constants (Phase 4a)
# =============================================================================
#
# Infrastructure enums and column widths for the messaging data layer
# (arch doc §2.4). Mirrors the engine convention (app/engine/
# constants.py): the lifecycle enums are enum.StrEnum used as the
# VALUE, while the column itself stays a varchar -- appending a member
# needs no DDL, exactly like NotificationStatus / DeliveryStatus.
#
# WIDTHS live local to the messaging package. They are consumed by ONE
# package today; the moment a second needs them (the messaging
# HTTP-API in 4c) they get promoted to app/core/constants.py -- the
# same rule the engine widths follow.
# =============================================================================

import enum


class ThreadKind(enum.StrEnum):
    """How a thread dedups on creation (arch doc §2.4).

    DM     -- subjectless: one eternal thread per (client, operator).
    TICKET -- subjectless: a fresh thread every time.

    A thread carrying a subject_ref dedups on the subject regardless of
    kind (one thread per entity), so kind only decides the SUBJECTLESS
    case. Enforcement is two partial unique indexes on `threads`
    (migration 0006); see threads.create_or_get_thread.
    """

    DM = "dm"
    TICKET = "ticket"


class ThreadStatus(enum.StrEnum):
    """Thread lifecycle status (arch doc §2.4; the set is LOCKED there).

    open     -- active, awaiting a reply (the creation default).
    resolved -- operator considers it handled, the window is still live.
    closed   -- terminal (NOT a delete: a thread is immortal by
                thread_id).

    Phase 4a defines the values ONLY. The transitions -- manual
    resolved/closed, the client-message -> open auto-reopen, and the
    "silent N days -> closed" auto-close -- are Phase 4b. Varchar
    column, append-only: a future status needs no DDL (same as
    NotificationStatus).
    """

    OPEN = "open"
    RESOLVED = "resolved"
    CLOSED = "closed"


class OperatorKind(enum.StrEnum):
    """The form of a thread's operator_target (arch doc §2.4).

    USER    -- operator_value is a recipient id (VELO: the master).
    SECTION -- operator_value is a section id (a support desk).

    The form drives 4b behavior (user: frozen axes / immutable
    assignee; section: claimable / retag-capable) -- NOT built in 4a.
    Here it is a plain discriminator paired with operator_value.
    """

    USER = "user"
    SECTION = "section"


# ---------------------------------------------------------------------------
# Column widths (messaging-local; promote to core when 4c needs them).
# The model uses these; migration 0006 uses the matching literals; a
# consistency check in tests/test_messaging_models.py keeps them equal.
# ---------------------------------------------------------------------------

# sections.key -- opaque product-facing section identifier ("support",
# "billing", ...). Same width class as group_memberships.group_key.
MAX_SECTION_KEY_LEN = 100

# sections.label -- human-readable section name for operator UIs.
MAX_SECTION_LABEL_LEN = 200

# threads.subject_type -- opaque product entity type ("practice", ...).
# Comms never parses it (arch doc §2.4: subject_ref is opaque).
MAX_SUBJECT_TYPE_LEN = 100

# threads.subject_id -- opaque product entity id. String, NOT UUID: the
# product owns the id space and it is not necessarily a uuid.
MAX_SUBJECT_ID_LEN = 200

# threads.title -- optional human title for the thread.
MAX_THREAD_TITLE_LEN = 500

# messages.body -- message text. Numerically equal to notifications.body
# (MAX_BODY_LEN) but kept separate on purpose: coupling messaging's
# limit to the notification-body limit would be accidental.
MAX_MESSAGE_BODY_LEN = 5000
