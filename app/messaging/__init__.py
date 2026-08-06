# =============================================================================
# COMMS Service -- Messaging Package (Phase 4a)
# =============================================================================
#
# The messaging DATA layer (arch doc §2.4): threads, messages, read
# state and a minimal first-class Section. Phase 4a lays the foundation
# only --
#   - models.py      -- Thread / Message / ThreadReadState / Section
#   - constants.py   -- ThreadKind / ThreadStatus / OperatorKind + widths
#   - sections.py    -- Section create-or-get / lookup by key
#   - threads.py     -- create-or-get thread (dedup) + post message
#   - read_state.py  -- read-pointer upsert + unread count
#
# PACKAGE DAG (arrows = imports):  core <- messaging
#   Messaging references the audience-owned `recipients` table by NAME
#   (string ForeignKey / a lightweight table() probe), and never
#   imports app.audience models -- so it does not sit below audience.
#
# FENCE (Phase 4a): fields, not behavior. Operator resolution, claim,
# visibility, supervisor, retag and STATUS TRANSITIONS landed in 4b;
# the message -> notification path, the msg_* gate and the messaging
# HTTP-API landed in 4c and live OUTSIDE this package (app/notifier.py,
# app/api/messaging.py) -- this package stays data-only. An outgoing
# product callback was considered for the same seam and REJECTED (the
# product learns "a thread started" from the `created` flag on its own
# create call -- ID-10, 2026-08-04). Deferred seams that remain are
# marked in-code (KNOWN CEILING convention).
# =============================================================================
