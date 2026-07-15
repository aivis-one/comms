# =============================================================================
# COMMS Service -- HTTP API Package (Phase 3b, pull side)
# =============================================================================
#
# The product-facing HTTP surface. INTERNAL by design (arch decision
# 14): the product backend proxies it (one public API surface, one
# initData authorization on the product side); comms itself never
# faces the frontend. Authorization is service-to-service via a shared
# bearer token (deps.py) on top of network isolation.
#
# Sits at the TOP of the package DAG:
#
#   core <- profile <- {engine, audience} <- api
#   engine -> audience
#
# FROZEN CONTRACTS (Phase 6 consumes them as-is):
#   inbox.py -- the in-app "bell": inbox feed / unread badge /
#               mark-read, keyset-paginated.
#   prefs.py -- the E8-shaped preferences facade: category toggles +
#               quiet-hours schedule as ONE object, timezone read-only.
#
# Versioned under /api/v1 -- freezing a contract without a version in
# the path would make the first breaking change a rename of every
# route. /health and /ready stay in app/main.py, unversioned and
# UNAUTHENTICATED (installer/docker healthchecks carry no secret).
# =============================================================================
