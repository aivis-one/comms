# =============================================================================
# COMMS Service -- Engine Constants
# =============================================================================
#
# Infrastructure enums for the two-level notification architecture,
# merged from the cbshome (base) and velo copies:
#   Notification         -- channel-agnostic event record
#   NotificationDelivery -- per-recipient, per-channel delivery record
#
# DE-DOMAINIZATION (the key difference from both donors):
#   There is NO NotificationType enum here. Notification types are
#   domain vocabulary -- they are registered by the per-deploy product
#   profile (app/profile/registry.py). The engine validates types
#   against the registry, never against a hardcoded enum.
#
#   TargetType is likewise generic: USER / GROUP / ALL. The donors'
#   domain targets map onto it product-side:
#     cbshome role:<r>      -> GROUP "role_<r>"
#     velo    practice:<id> -> GROUP "practice_<id>"
#
# NOTIFICATION STATUS LIFECYCLE (spec §5.5; F1.3):
#   pending -> no_recipients | suppressed | expired | cancelled
#   pending -> processing -> THE FOLD of the deliveries
#                            (app/engine/service.py): sent |
#                            partial_sent | failed | expired |
#                            cancelled | suppressed | no_recipients
#
#   NO_RECIPIENTS and SUPPRESSED are the two facts one SKIPPED used to
#   hide: an empty audience (fix the sync) and every recipient muted
#   (their decision). Neither is a fault (FAILED would drown real
#   alerts) nor a delivery (SENT would hide a broken sync).
#
# DELIVERY STATUS LIFECYCLE (F1.3):
#   pending -> sent | failed (+ FailureClass) | suppressed | expired
#            | cancelled | recipient_inactive
#   pending -> pending with a WaitReason (schedule, 429, backoff)
#
#   SUPPRESSED -- the recipient muted the category while the delivery
#   waited: not a fault, not a send. RECIPIENT_INACTIVE -- the product
#   deactivated or deleted the recipient while it waited (F1.4).
#   EXPIRED / CANCELLED -- the parent was closed while this delivery
#   waited.
# =============================================================================

import enum


class IntakeOutcomeClass(enum.StrEnum):
    """Why a request was not accepted (intake_outcomes.outcome, F1.2)."""

    # The envelope could not be accepted -- the product's side.
    REJECTED_AT_INTAKE = "rejected_at_intake"
    # The key is taken by an accepted job with different bytes.
    CONFLICT = "conflict"


class NotificationStatus(enum.StrEnum):
    """Notification lifecycle status (spec §5.5).

    accepted -> queued -> in flight -> outcome. Active: PENDING (not
    resolved yet), PROCESSING (resolved, deliveries on their way). The
    rest are outcomes; the parent's outcome is the FOLD of its
    deliveries (app/engine/service.py rollup_notification), except the
    three that are decided before any delivery exists.
    """

    PENDING = "pending"
    PROCESSING = "processing"
    SENT = "sent"
    PARTIAL_SENT = "partial_sent"
    FAILED = "failed"
    EXPIRED = "expired"
    # The product (or its reminder_cancel) cancelled the job (F1.3).
    CANCELLED = "cancelled"
    # Every recipient muted the category: the recipients' decision, NOT
    # a failure -- nothing to fix (F1.3; was half of SKIPPED).
    SUPPRESSED = "suppressed"
    # The audience was empty: fix the product's sync (F1.3; was the
    # other half of SKIPPED).
    NO_RECIPIENTS = "no_recipients"


class DeliveryStatus(enum.StrEnum):
    """Per-recipient, per-channel delivery status.

    "In flight" is not a status: an attempt runs inside one transaction
    under the notification's row lock and is never committed half-way,
    so nothing outside can observe it.
    """

    PENDING = "pending"
    SENT = "sent"
    # Always with a FailureClass (CHECK ck_deliveries_failure_class).
    FAILED = "failed"
    # The recipient muted the category (late mute, re-checked at
    # deliver time): the recipient's decision, not a failure.
    SUPPRESSED = "suppressed"
    # The parent expired / was cancelled while this delivery waited.
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    # The product deactivated or deleted the recipient between resolve
    # and send (F1.4): not a failure (nothing to fix) and not a mute
    # (the product decided, not the recipient). Which of the two is on
    # the recipient row (recipients.deleted_at).
    RECIPIENT_INACTIVE = "recipient_inactive"


class FailureClass(enum.StrEnum):
    """Why a delivery failed -- four outcomes, four actions (spec §5.6).

    Set together with DeliveryStatus.FAILED and only then; one channel
    exception maps to exactly one class, in one place
    (app/engine/service.py _deliver_single).
    """

    # The channel was unavailable for the whole attempt budget.
    TRANSIENT_EXHAUSTED = "transient_exhausted"
    # This message will not arrive; others go (recipient closed the
    # channel, malformed letter for this channel, provider rejected it).
    MESSAGE_REJECTED = "message_rejected"
    # The channel is dead on this deploy -- fix the deploy. LOUD.
    CONFIGURATION = "configuration"
    # The recipient has no address in this channel -- fix the sync.
    NO_ADDRESS = "no_address"


class WaitReason(enum.StrEnum):
    """Why a pending delivery waits (spec §5.5), next to next_retry_at.

    Set exactly when next_retry_at is set (CHECK
    ck_deliveries_wait_reason); both are cleared when the delivery is
    taken into an attempt. A pending delivery with neither is waiting
    for its turn in the queue.
    """

    # Outside the recipient's allowed delivery periods.
    RECIPIENT_SCHEDULE = "recipient_schedule"
    # The provider named a wait (HTTP 429).
    PROVIDER_RATE_LIMIT = "provider_rate_limit"
    # comms' own backoff after a transient failure -- neither the
    # recipient nor the provider named it.
    TRANSIENT_BACKOFF = "transient_backoff"


class DeliveryChannel(enum.StrEnum):
    """Supported delivery channels (infrastructure, not domain).

    New channel = new formatter + new member here; the pipeline
    itself does not change (see formatters registry).
    """

    TELEGRAM = "telegram"
    EMAIL = "email"
    PUSH = "push"
    IN_APP = "in_app"


class TargetType(enum.StrEnum):
    """How to resolve notification recipients (generic, sync-projection).

    USER  -- target_value is a bare product user id (uuid string).
    GROUP -- target_value is an opaque group_key synced by the product.
    ALL   -- every active recipient; target_value is ignored ("*" by
             convention).
    """

    USER = "user"
    GROUP = "group"
    ALL = "all"
