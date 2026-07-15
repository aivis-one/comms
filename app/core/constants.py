# =============================================================================
# COMMS Service -- Shared Schema Constants (Phase 3a, item 6)
# =============================================================================
#
# Single source of truth for string-column widths that are consumed by
# MORE THAN ONE package:
#
#   MAX_TYPE_KEY_LEN  -- notifications.type        (app/engine/models.py)
#                        + profile type-key check  (app/profile/loader.py)
#   MAX_CATEGORY_LEN  -- category_mutes.category   (app/audience/models.py)
#                        + profile category check  (app/profile/loader.py)
#
# WHY HERE (and not introspected from the models, as in Phase 2):
#   The profile validator used to read these widths off the mapped
#   columns at import time. That was honest, but it made profile/loader
#   import audience/models while audience/prefs already imports
#   profile/registry -- the two packages became MUTUALLY dependent.
#   Constants in core (the shared bottom layer: core imports neither
#   engine nor audience) break the cycle; the "constant matches the
#   actual column width" honesty moves to a unit test, where
#   cross-package imports are free (tests/test_profile_loader.py).
#
# CHANGING A WIDTH:
#   Widen the constant AND ship the alembic migration that alters the
#   column in the same change; the consistency test fails otherwise.
# =============================================================================

# Width of notifications.type. Product type keys such as
# "waitlist_spot_available" made the donors' String(30) too tight;
# 50 was chosen in Phase 1 (see app/engine/models.py header).
MAX_TYPE_KEY_LEN = 50

# Width of category_mutes.category (preference category keys,
# family-granular: e.g. "reminder" covers reminder_24h/1h/10min).
MAX_CATEGORY_LEN = 50
