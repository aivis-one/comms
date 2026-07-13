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
#   profile (app/engine/registry.py). The engine validates types
#   against the registry, never against a hardcoded enum.
#
#   TargetType is likewise generic: USER / GROUP / ALL. The donors'
#   domain targets map onto it product-side:
#     cbshome role:<r>      -> GROUP "role_<r>"
#     velo    practice:<id> -> GROUP "practice_<id>"
#
# NOTIFICATION STATUS LIFECYCLE (cbshome base, incl. PARTIAL_SENT):
#   pending -> processing -> sent | partial_sent | failed | expired
#
# DELIVERY STATUS LIFECYCLE:
#   pending -> sent | failed
# =============================================================================

import enum


class NotificationStatus(enum.StrEnum):
    """Notification lifecycle status."""

    PENDING = "pending"
    PROCESSING = "processing"
    SENT = "sent"
    PARTIAL_SENT = "partial_sent"
    FAILED = "failed"
    EXPIRED = "expired"


class DeliveryStatus(enum.StrEnum):
    """Per-recipient, per-channel delivery status."""

    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


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
