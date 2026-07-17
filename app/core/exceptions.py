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
