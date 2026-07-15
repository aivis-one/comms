# =============================================================================
# COMMS Service -- Event Transport Package (Phase 3c, push side)
# =============================================================================
#
# The durable inbound side: a Redis Streams consumer group reading the
# product's event stream and feeding the existing pipeline.
#
#   events.py   -- FROZEN wire contract: envelope + the three event
#                  schemas, parsing and validation (terminal errors).
#   handlers.py -- dispatch of parsed events onto the existing service
#                  functions (create_notification / user_upserted /
#                  group_changed), dedup via the DB unique index.
#   consumer.py -- the XREADGROUP loop: group bootstrap, pending drain
#                  on startup, retry-with-backoff for ordering lag,
#                  poison-pill -> DLQ, XACK discipline.
#
# Package DAG position: transport sits at the top, next to api --
#
#   core <- profile <- {engine, audience} <- {api, transport}
#   engine -> audience
#
# Model B split (arch): the PRODUCT owns the domain and maps domain
# events to notification types / group targets in its own outbox; the
# events here are already comms-shaped. The notification_request event
# carries an AUDIENCE DESCRIPTOR (target_type + target_value), never a
# recipient list -- resolve stays inside comms (the BL-1 door).
# =============================================================================
