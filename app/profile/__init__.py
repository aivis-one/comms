# =============================================================================
# COMMS Service -- Profile Package
# =============================================================================
#
# The per-deploy product profile concern: the in-memory registry of
# notification types / categories / templates (registry.py) plus the
# disk loader and startup validator (loader.py).
#
# Extracted from app/engine in Phase 3b (item 0) to break the package
# cycle that would otherwise appear with the HTTP api package:
# audience/prefs validates categories against the registry, and the
# engine consumes the registry too -- so the profile concern must sit
# BELOW both. Resulting package DAG (arrows = imports):
#
#   core <- profile <- {engine, audience} <- api
#   engine -> audience
#
# No behavior change: a mechanical move of registry.py and
# engine/profile.py (renamed loader.py -- app/profile/profile.py would
# stutter).
# =============================================================================
