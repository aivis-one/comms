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
# visibility, supervisor, retag and STATUS TRANSITIONS are Phase 4b;
# the message -> notification path, the msg_* gate, the callback client
# and the messaging HTTP-API are Phase 4c. Every seam left for them is
# marked in-code (KNOWN CEILING convention).
# =============================================================================
