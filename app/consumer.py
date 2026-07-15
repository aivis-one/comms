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
# The profile is installed at startup: ingest validates notification
# types against the registry (via create_notification), so a consumer
# without a profile would dead-letter every request.
#
# Handles SIGTERM/SIGINT by cancelling the loop task. An entry caught
# mid-flight rolls back UNACKED (cancellation interrupts inner
# awaits); the next start's pending drain replays it -- at-least-once
# holds by replay, not by graceful completion (review 3c.1).
# =============================================================================

import asyncio
import contextlib
import signal

import structlog

from app.core.config import settings
from app.core.database import dispose_engine
from app.core.logging import setup_logging
from app.profile.loader import install_profile_from_settings
from app.transport.consumer import run_consumer_loop

logger = structlog.get_logger()


async def _main() -> None:
    """Run the consumer loop with graceful shutdown on signals."""
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(run_consumer_loop())

    def _request_shutdown() -> None:
        logger.info("consumer_shutdown_requested")
        task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown)

    with contextlib.suppress(asyncio.CancelledError):
        await task

    await dispose_engine()


def main() -> None:
    """Console entrypoint: `python -m app.consumer`."""
    setup_logging()
    if not settings.redis_url:
        # Fail-at-startup, same philosophy as the real-mode config
        # validation: a consumer without Redis is a no-op pretending
        # to be a process.
        raise RuntimeError(
            "REDIS_URL is required to run the consumer: it reads the "
            "product's event stream. Set it in the .env file."
        )
    install_profile_from_settings()
    asyncio.run(_main())


if __name__ == "__main__":
    main()
