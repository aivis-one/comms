# =============================================================================
# COMMS Service -- the liveness of the worker and the consumer (W3-1)
# =============================================================================
#
# A LOOP THAT STOPPED TURNING IS NOT ALIVE. The compose healthcheck of
# comms-worker and comms-consumer was "PID 1 is this process"
# (/proc/1/cmdline): true of a loop stuck on an await for ever, and of an
# event loop a blocking call holds. This module is what replaces it.
#
# THE PROMISE. Each long-lived loop of a process runs inside `watched`,
# with its BOUND: the longest a step of that loop may take. A step says
# so with `step()` -- the loop promises its next step within its bound --
# and a sleep with `rest(seconds)` -- the next step within the sleep and
# the bound. Every step re-arms the promise of the loop of the CURRENT
# TASK: a ContextVar, read where the step is made -- deep in a pass
# (a lane's place taken, app/engine/processor.py) as well as in the loop.
# Outside `watched` both calls do nothing.
#
# THE PULSE. `keep_alive` runs beside the loops (app/worker.py,
# app/consumer.py _main): every TICK_SECONDS it touches the pulse file --
# only while at least one loop is watched and no promise has run out. A
# loop past its promise, or an event loop that does not run the ticker
# (a blocking call), leaves the file to age. The file is removed when
# the ticker starts: the container's layer survives a restart, and an old
# file must not speak for a new process.
#
# THE CHECK. `python -m app.core.liveness <role>` -- the compose
# healthcheck: exit 0 when the pulse file of the role was touched within
# STALE_SECONDS, 1 otherwise, 2 for a role it does not know. It imports
# nothing of comms' settings: it runs in the container's environment every
# interval and needs no database, no network and no configuration.
#
# THE BOUNDS are the loops' own (app/worker.py lane_step_bound,
# app/transport/consumer.py, app/transport/push_relay.py), each with
# STEP_MARGIN_SECONDS for the database steps around the bounded calls.
# A statement is bounded by its process's statement_timeout
# (app/core/database.py LIMITS, W5-3), and the margin holds one at its
# limit -- tests/test_db_limits.py holds the two together, not one
# source: this module imports nothing of comms (THE CHECK). A step of the
# database longer than the margin is red -- a hang, which is what this is
# for.
#
# Not watched: the worker's wake listener (app/engine/wake.py). It waits
# for notifications for as long as there are none, by design; a lost wake
# costs a lane its interval, never a job, and the lanes are watched.
# =============================================================================

import asyncio
import contextlib
import sys
import time
from collections.abc import Iterator
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path

import structlog

logger = structlog.get_logger()

TICK_SECONDS = 5.0
"""How often the ticker touches the pulse file while every loop is alive."""

STALE_SECONDS = 30.0
"""The oldest pulse the check takes as alive: six ticks."""

STEP_MARGIN_SECONDS = 60.0
"""The database's part of a step, added to every loop's bound (header)."""

ROLES = ("worker", "consumer")
"""The processes with a pulse; the API answers /ready instead."""


def pulse_path(role: str) -> Path:
    """The pulse file of `role`, in the container's own /tmp."""
    return Path("/tmp") / f"comms-{role}.alive"


@dataclass
class _Watch:
    """One loop: its bound and the moment its promise runs out."""

    name: str
    bound: float
    deadline: float


_WATCHES: dict[str, _Watch] = {}
_CURRENT: ContextVar[_Watch | None] = ContextVar("comms_liveness", default=None)


@contextlib.contextmanager
def watched(name: str, bound: float) -> Iterator[None]:
    """Run the body as the loop `name`, which promises a step every
    `bound` seconds; the first promise starts now."""
    watch = _Watch(name=name, bound=bound, deadline=time.monotonic() + bound)
    _WATCHES[name] = watch
    token = _CURRENT.set(watch)
    try:
        yield
    finally:
        _CURRENT.reset(token)
        if _WATCHES.get(name) is watch:
            del _WATCHES[name]


def step() -> None:
    """The current loop made a step: its next one within its bound."""
    watch = _CURRENT.get()
    if watch is not None:
        watch.deadline = time.monotonic() + watch.bound


def rest(seconds: float) -> None:
    """The current loop sleeps `seconds`: its next step within the sleep
    and its bound."""
    watch = _CURRENT.get()
    if watch is not None:
        watch.deadline = time.monotonic() + seconds + watch.bound


def late(now: float) -> list[str]:
    """The loops whose promise ran out before `now`, by name."""
    return sorted(name for name, watch in _WATCHES.items() if watch.deadline < now)


def _remove(path: Path) -> None:
    """Remove the pulse file, if there is one. Synchronous on purpose, as
    `_touch`: a few bytes of metadata in the container's own /tmp, which a
    thread would only make slower."""
    with contextlib.suppress(FileNotFoundError):
        path.unlink()


def _touch(path: Path) -> None:
    """Touch the pulse file (see `_remove`)."""
    path.touch()


async def keep_alive(path: Path, tick: float = TICK_SECONDS) -> None:
    """Touch `path` every `tick` while some loop is watched and none is
    late; runs until cancelled. A turn into lateness is logged once, with
    the loops' names, and again only after the loops were alive between;
    a file that cannot be touched likewise (its class only)."""
    _remove(path)
    said_late = said_unwritable = False
    while True:
        behind = late(time.monotonic())
        if behind:
            if not said_late:
                logger.error("liveness_stalled", loops=behind)
                said_late = True
        elif _WATCHES:
            said_late = False
            try:
                _touch(path)
            except OSError as exc:
                if not said_unwritable:
                    logger.error("liveness_unwritable", error=type(exc).__name__)
                    said_unwritable = True
            else:
                said_unwritable = False
        await asyncio.sleep(tick)


def is_alive(path: Path, stale: float = STALE_SECONDS) -> bool:
    """`path` exists and was touched within `stale` seconds."""
    try:
        touched = path.stat().st_mtime
    except FileNotFoundError:
        return False
    return time.time() - touched <= stale


def main(argv: list[str]) -> int:
    """The healthcheck: 0 alive, 1 not, 2 an unknown role."""
    if len(argv) != 1 or argv[0] not in ROLES:
        roles = "|".join(ROLES)
        print(f"usage: python -m app.core.liveness {{{roles}}}", file=sys.stderr)
        return 2
    return 0 if is_alive(pulse_path(argv[0])) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
