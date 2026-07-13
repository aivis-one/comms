# =============================================================================
# COMMS Service -- Worker Entrypoint
# =============================================================================
#
# Separate worker process: same Docker image as the API, different
# command (handoff item 1):
#
#   API:    uvicorn app.main:app --host 0.0.0.0 --port 8000
#   Worker: python -m app.worker
#
# Handles SIGTERM/SIGINT by cancelling the loop task so the current
# batch finishes its per-notification transaction cleanly.
# =============================================================================

import asyncio
import contextlib
import signal

import structlog

from app.core.database import dispose_engine
from app.core.logging import setup_logging
from app.engine.formatters import close_formatters
from app.engine.worker import run_worker_loop

logger = structlog.get_logger()


async def _main() -> None:
    """Run the worker loop with graceful shutdown on signals."""
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(run_worker_loop())

    def _request_shutdown() -> None:
        logger.info("worker_shutdown_requested")
        task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown)

    with contextlib.suppress(asyncio.CancelledError):
        await task

    # Review 1.1: close the aiogram session before dropping the engine.
    await close_formatters()
    await dispose_engine()


def main() -> None:
    """Console entrypoint: `python -m app.worker`."""
    setup_logging()
    asyncio.run(_main())


if __name__ == "__main__":
    main()
