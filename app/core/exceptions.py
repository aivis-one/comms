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
