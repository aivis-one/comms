# =============================================================================
# COMMS Service -- Consumer Entrypoint (Phase 3c item 1)
# =============================================================================
#
# Separate consumer process: same Docker image, different command --
# the third sibling next to the API and the worker:
#
#   API:      uvicorn app.main:app --host 0.0.0.0 --port 8000
#   Worker:   python -m app.worker
#   Consumer: python -m app.consumer
#
# Startup validation lives HERE, not in Settings: the consumer is the
# only process that needs Redis (API and worker are DB-only), so an
# empty REDIS_URL must kill the CONSUMER at boot -- and only it.
#
# TWO LOOPS, ONE PROCESS (P3-1): the stream consumer reads the product's
# events; the push relay (app/transport/push_relay.py) publishes the
# pushes comms owes the product into comms' own redis. The relay lives
# here because this is the process that already holds redis. It never
# ends on its own -- a failed tick is retried -- so a loop that ends
# is the consumer's: its exception, as before, ends the process (the
# container restarts it), and the relay is cancelled with it.
#
# The profile is installed at startup: ingest validates notification
# types against the registry (via create_notification), so a consumer
# without a profile would dead-letter every request.
#
# Handles SIGTERM/SIGINT by cancelling both loop tasks. An entry caught
# mid-flight rolls back UNACKED (cancellation interrupts inner
# awaits); the next start's pending drain replays it -- at-least-once
# holds by replay, not by graceful completion (review 3c.1).
# =============================================================================

import asyncio
import signal

import structlog

from app.core.config import settings
from app.core.database import dispose_engine
from app.core.logging import setup_logging
from app.engine.formatters import sanitized_traceback
from app.profile.loader import install_profile_from_settings
from app.transport.consumer import run_consumer_loop
from app.transport.push_relay import run_push_relay_loop

logger = structlog.get_logger()

# The two loops of the process, in the order _main starts them.
_LOOP_NAMES = ("consumer", "push_relay")


async def _main() -> None:
    """Run the consumer loop and the push relay with graceful shutdown
    on signals; the first loop to end ends both, and its exception is
    raised once the other is cancelled and the engine disposed."""
    loop = asyncio.get_running_loop()
    tasks = (
        asyncio.ensure_future(run_consumer_loop()),
        asyncio.ensure_future(run_push_relay_loop()),
    )

    def _request_shutdown() -> None:
        logger.info("consumer_shutdown_requested")
        for task in tasks:
            task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown)

    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in tasks:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)

    await dispose_engine()
    for name, result in zip(_LOOP_NAMES, results, strict=True):
        if isinstance(result, BaseException) and not isinstance(
            result, asyncio.CancelledError,
        ):
            # The one line the container log keeps for "why did it
            # stop" -- a redis gone silent past the socket timeout
            # (app/transport/connection.py) or refusing; the restart
            # policy brings the process back (H1).
            logger.error(
                "consumer_loop_failed",
                loop=name,
                exception=sanitized_traceback(result),
            )
            raise result


def main() -> None:
    """Console entrypoint: `python -m app.consumer`."""
    setup_logging()
    if not settings.redis_url:
        # Fail-at-startup, same philosophy as the startup config
        # validation (app/core/config.py): a consumer without Redis is
        # a no-op pretending to be a process.
        raise RuntimeError(
            "REDIS_URL is required to run the consumer: it reads the "
            "product's event stream. Set it in the .env file."
        )
    install_profile_from_settings()
    asyncio.run(_main())


if __name__ == "__main__":
    main()
