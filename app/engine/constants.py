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
# NOTIFICATION STATUS LIFECYCLE (cbshome base, incl. PARTIAL_SENT;
# SKIPPED added in Phase 2):
#   pending -> processing -> sent | partial_sent | failed | expired
#                                 | skipped
#
#   SKIPPED (terminal) -- the pipeline ran fine but there was nobody
#   to deliver to: the audience resolved empty, or every resolved
#   recipient has the notification's category muted. Neither a fault
#   (FAILED would drown real alerts) nor a delivery (SENT would hide
#   a broken sync).
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
    # Terminal: pipeline OK, nobody to deliver to (empty audience or
    # all recipients muted the category). Introduced in Phase 2; the
    # status column is varchar, so no DDL is needed (append-only).
    SKIPPED = "skipped"


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
