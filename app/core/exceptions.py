# =============================================================================
# COMMS Service -- Exceptions
# =============================================================================
#
# Minimal service-level exception hierarchy for Phase 1 (engine core).
# The donors (cbshome/velo) use HTTP-flavored exceptions mapped to
# responses by an app-level handler; comms grows that surface in
# Phase 3 (transport / HTTP-API). Until then the engine raises these
# plain exceptions and callers (tests, future API layer) handle them.
# =============================================================================


class CommsError(Exception):
    """Base class for all comms service errors."""


class ValidationError(CommsError):
    """Invalid input (unknown notification type, bad channel, etc.)."""


class NotFoundError(CommsError):
    """Requested entity does not exist (or belongs to someone else)."""


class AuthorizationError(CommsError):
    """Actor is not permitted to perform this write on this thread.

    Phase 4c write-authz (item 4): the check is by ROLE IN THE THREAD
    (participant / serving operator / pool agent), NOT by identity --
    identity is the product proxy's concern (arch decision 14). Mapped
    to HTTP 403 by the API error handler.
    """


class ProfileError(CommsError):
    """Product profile failed to load or validate.

    Raised at startup by app/profile/loader.py -- a broken profile
    (bad YAML, malformed tree, invalid format spec) must kill the
    service before it takes traffic, not surface on the delivery path.
    """


class ConflictError(CommsError):
    """The request is well-formed but contradicts what is recorded.

    ABSTRACT family (F1.4): the subclass names WHICH contradiction, and
    the API maps each to its own error class under HTTP 409
    (app/api/errors.py). On the event stream a conflict is not a broken
    event -- it is a late or repeated one -- so the consumer logs its
    class and acknowledges it, without a dead letter.
    """

    def __init__(self, *args: object) -> None:
        if type(self) is ConflictError:
            raise TypeError("ConflictError is abstract: raise a subclass")
        super().__init__(*args)


class StaleSnapshotError(ConflictError):
    """A recipient snapshot (or deletion) older than the stored one."""


class SnapshotConflictError(ConflictError):
    """A snapshot with the stored version but other content."""


class RecipientDeletedError(ConflictError):
    """The recipient was deleted by the product: the tombstone is
    terminal, no snapshot and no membership revives it."""


class IdempotencyConflictError(ConflictError):
    """The Idempotency-Key is taken by a request with other content."""


class ClaimTakenError(ConflictError):
    """The thread is already assigned to another operator."""


def conflict_class(exc: ConflictError) -> str:
    """The wire name of a conflict's class -- ONE mapping, shared by the
    API error body (app/api/errors.py) and the consumer's log line."""
    return _CONFLICT_CLASSES[type(exc)]


_CONFLICT_CLASSES: dict[type[ConflictError], str] = {
    StaleSnapshotError: "stale_snapshot",
    SnapshotConflictError: "conflict",
    RecipientDeletedError: "recipient_deleted",
    IdempotencyConflictError: "conflict",
    ClaimTakenError: "conflict",
}
