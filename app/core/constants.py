# =============================================================================
# COMMS Service -- Shared Schema Constants (Phase 3a, item 6)
# =============================================================================
#
# Single source of truth for cross-package constants. Two families
# live here (Release-Hardening added the second):
#
#   - string-column widths consumed by more than one package;
#   - the comms-native chat type keys (MSG_TYPE_*) and the external-
#     domain patterns (DOMAIN_LITERAL_PATTERNS), each consumed by two
#     parties that must never import one another (see their blocks).
#
# Column widths:
#
#   MAX_TYPE_KEY_LEN  -- notifications.type        (app/engine/models.py)
#                        + the profile loader's key check
#                        (app/profile/loader.py) + ingest validation of
#                        the notification_request event
#                        (app/transport/events.py)
#   MAX_CATEGORY_LEN  -- category_mutes.category   (app/audience/models.py)
#                        + profile category check  (app/profile/loader.py)
#   MAX_TITLE_LEN / MAX_BODY_LEN -- notifications.title / .body
#                        (app/engine/models.py) + ingest validation
#                        (app/transport/events.py)
#   MAX_IDEMPOTENCY_KEY_LEN / MAX_CORRELATION_LEN / FINGERPRINT_LEN --
#                        notifications.idempotency_key / .correlation /
#                        .fingerprint (app/engine/models.py) + the
#                        intake_outcomes table + ingest validation of
#                        the notification_request event
#                        (app/transport/events.py)
#   MAX_EMAIL_LEN / MAX_LOCALE_LEN / MAX_TIMEZONE_LEN /
#   MIN_TELEGRAM_ID / MAX_TELEGRAM_ID -- the recipient snapshot
#                        (app/audience/models.py) + the synchronous
#                        upsert (app/api/recipients.py) + the
#                        user_upserted envelope (app/transport/events.py)
#   MAX_GROUP_KEY_LEN -- group_memberships.group_key
#                        (app/audience/models.py) + the group_changed
#                        envelope (app/transport/events.py)
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

import re

# Width of notifications.type. Product type keys such as
# "waitlist_spot_available" made the donors' String(30) too tight;
# 50 was chosen in Phase 1 (see app/engine/models.py header).
MAX_TYPE_KEY_LEN = 50

# Width of category_mutes.category (preference category keys,
# family-granular: e.g. "reminder" covers reminder_24h/1h/10min).
MAX_CATEGORY_LEN = 50

# Widths of notifications.title / notifications.body (Phase 1 values,
# promoted to shared constants in Phase 3c when stream ingest became
# a second consumer -- an over-long title must be rejected at the
# stream boundary with a clear error, not surface as a DB DataError).
MAX_TITLE_LEN = 500
MAX_BODY_LEN = 5000

# Width of notifications.idempotency_key -- the producer-supplied
# dedup key of a stream-ingested notification request (Phase 3c).
# 200 fits any sane producer key (outbox row uuid, composite string);
# part of the FROZEN event contract: 1..200 chars.
MAX_IDEMPOTENCY_KEY_LEN = 200

# ONE page-size bound for every listing route (F1.4, app/api/paging.py):
# out of 1..PAGE_LIMIT_MAX is refused, never clamped.
PAGE_LIMIT_MAX = 100
PAGE_LIMIT_DEFAULT = 20

# recipients.version is a BigInteger: the snapshot version's ceiling is
# the column's (F1.4). The floor is 1 -- 0 marks rows written before
# versions existed (migration 0014).
MAX_SNAPSHOT_VERSION = 2**63 - 1

# Width of notifications.correlation -- the product's own reference on
# the envelope (F1.2), stored untouched and never interpreted. Two
# consumers: the column (app/engine/models.py) and the envelope parser
# (app/transport/events.py).
MAX_CORRELATION_LEN = 200

# notifications.fingerprint -- a SHA-256 hex digest (F1.2), the
# byte-level identity of a request under its idempotency key.
FINGERPRINT_LEN = 64

# -----------------------------------------------------------------------------
# Recipient snapshot widths (R-2 item 1)
# -----------------------------------------------------------------------------
# THREE consumers each, which is why they live here and not next to
# the columns: the column itself (app/audience/models.py), the
# synchronous HTTP surface (app/api/recipients.py) and the event
# envelope (app/transport/events.py). Before this, each of the three
# carried its own number, and two of them disagreed with the column --
# an over-long locale passed the envelope and died on the INSERT, which
# is the defect class this release closes. A width that only one place
# knows is a width the other places will get wrong.
#
# CHANGING ONE: widen the constant AND ship the alembic migration in
# the same change -- the consistency test pins constant == column.

# recipients.email -- the RFC 5321 maximum for a whole address
# (64 local + @ + 255 domain).
MAX_EMAIL_LEN = 320

# recipients.locale -- a language tag, "en" / "pt-BR" class of value.
MAX_LOCALE_LEN = 8

# recipients.timezone -- an IANA zone name ("Europe/Berlin").
MAX_TIMEZONE_LEN = 64

# group_memberships.group_key -- opaque product-facing audience key
# ("masters", "admins"). Same width class as sections.key.
MAX_GROUP_KEY_LEN = 100

# recipients.telegram_id is a BigInteger, and the bound IS the column's
# range: comms does not own the id space (the platform does), so any
# narrower window would be a policy invented here. Out of range the
# value is not a large id, it is a broken one -- and without this bound
# it reaches the INSERT and comes back as a database error.
MIN_TELEGRAM_ID = -(2**63)
MAX_TELEGRAM_ID = 2**63 - 1


# -----------------------------------------------------------------------------
# Comms-native chat notification type keys (arch doc #15: chat is a
# BASELINE capability)
# -----------------------------------------------------------------------------
# app/notifier.py emits these three types unconditionally -- chat is a
# core comms capability, not a product feature. A type with no
# category bypasses the mute gate (§2.5), which would make chat
# notifications unmutable; therefore EVERY product profile must
# register all three with a non-empty category, enforced at startup
# by app/profile/loader.py.
#
# WHY HERE (Release-Hardening, mandatory fix 2): the loader validator
# and the notifier need the same three keys. Importing the notifier
# from profile/ would invert the layers; duplicating the tuple would
# drift when a fourth chat type appears. core is the shared bottom
# layer both already depend on (same reasoning as the column widths
# above).
MSG_TYPE_PARTICIPANT_MESSAGE = "msg.participant_message"
MSG_TYPE_SUPPORT_MESSAGE = "msg.support_message"
MSG_TYPE_THREAD_CLOSED = "msg.thread_closed"

# The full baseline set the profile loader validates against. Append
# here when comms grows a fourth chat type -- the loader and the
# notifier pick it up from this one place.
MSG_TYPE_KEYS = (
    MSG_TYPE_PARTICIPANT_MESSAGE,
    MSG_TYPE_SUPPORT_MESSAGE,
    MSG_TYPE_THREAD_CLOSED,
)


# -----------------------------------------------------------------------------
# External-domain literal patterns (arch doc §2.6 / decision 13)
# -----------------------------------------------------------------------------
# Domains live in ENV, never in data and never in code; URLs are
# assembled at the edge. Two consumers share these patterns:
#
#   - scripts/check_domain_literals.py -- the CI fence over app/ CODE;
#   - app/profile/loader.py -- the startup fence over profile DATA
#     (types.yaml / templates/*.yaml), Release-Hardening item 3b.
#
# WHY HERE (and not imported from the script): the Docker image ships
# app/ but NOT scripts/ -- the loader importing the script would break
# container startup. The patterns live in app; the script imports them
# from here (single source, packaging-safe direction).
#
# A real scheme is required on purpose (the secret sanitizer's
# "://"-only regex must not trip the code fence); the bare Telegram
# hosts cover the incident class that appears WITHOUT a scheme in
# message text. NOTE: these pattern SOURCE strings do not match
# themselves ("https?://" is not matched by the compiled https?://).
DOMAIN_LITERAL_PATTERNS = (
    re.compile(r"https?://", re.IGNORECASE),
    re.compile(r"\b(?:t\.me|telegram\.(?:org|me))\b", re.IGNORECASE),
)
