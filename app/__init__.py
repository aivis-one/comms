# =============================================================================
# COMMS Service -- the package graph
# =============================================================================
#
# Every import in app/ goes DOWN this table, never up nor across a rank
# (H9). tests/test_h9_release.py holds it over every import of every
# module, in every form; a package or top module that is not in the
# table fails it until it is given a rank.
#
#   rank 0  core                         settings, database, sanitizer,
#                                        the closed sets (channels, lanes)
#   rank 1  profile                      the product's types and templates
#   rank 2  audience, messaging          recipients and groups; threads
#   rank 3  engine                       intake, pipeline, journal
#   rank 4  forgetting, notifier,        across engine, audience, messaging
#           variants
#   rank 5  api, transport               HTTP in, the stream in and out
#   rank 6  main, worker, consumer       the processes
#
# engine -> audience (3 -> 2), the edge the old headers named apart, is
# a step down like any other; nothing goes sideways or up.
# =============================================================================
