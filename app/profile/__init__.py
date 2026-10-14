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
# BELOW both. Its rank in the package graph: 1, above core only (the
# whole table: app/__init__.py, held by tests/test_h9_release.py).
#
# No behavior change: a mechanical move of registry.py and
# engine/profile.py (renamed loader.py -- app/profile/profile.py would
# stutter).
# =============================================================================
