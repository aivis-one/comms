# =============================================================================
# COMMS Service -- the isolation lanes (spec §8.2-8.3)
# =============================================================================
#
# A closed set comms offers, like its channels (app/core/channels.py):
# it lives in core because core reads it -- the settings bound and
# validate each lane's batch size, deliver timeout and the lanes a
# worker process takes (app/core/config.py) -- and core imports nothing
# above itself (H9; the package graph is held by tests/test_h9_release.py).
# Every reader imports it from HERE; the profile, which maps a type to a
# lane, is one reader among them.
# =============================================================================

from enum import StrEnum


class Lane(StrEnum):
    """The isolation lane of a type (spec §8.2-8.3): the values of the
    profile field `lane`, in the order the schema publishes them
    (app/profile/loader.py LANES). A closed set comms offers; the
    product picks one per type, and a type that declares none goes to
    `normal`. A job keeps the lane it was accepted with
    (notifications.lane); the worker runs one loop per lane of its
    process (app/worker.py), each selecting only its own lane with its
    own budget (app/engine/processor.py process_lane) -- isolation, not
    priority: no lane is ever picked before another.
    """

    # Someone is waiting for the answer right now.
    INTERACTIVE = "interactive"
    # A failure costs money: capacity no other lane can take.
    CRITICAL = "critical"
    # Notifications -- the default, and the behavior before lanes.
    NORMAL = "normal"
    # Large sends that may wait.
    BULK = "bulk"
