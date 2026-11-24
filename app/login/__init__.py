# =============================================================================
# COMMS Service -- the sign-in module (the option `login`, v4.2)
# =============================================================================
#
# The sign-in through Telegram a product asks comms for: comms records an
# attempt and hands the browser's way to Telegram (app/login/attempts.py,
# app/api/login.py); the attempt is taken once. Switched by LOGIN_ENABLED
# (app/core/modules.py); with it off no route of the module exists. comms
# keeps no personal data of a sign-in, and decides nothing for a product:
# whom it lets in is the product's.
#
# Rank 2 of the package graph (app/__init__.py): it imports core only.
# =============================================================================
