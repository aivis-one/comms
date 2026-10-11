# =============================================================================
# COMMS Service -- The redis client of the consumer process (H1)
# =============================================================================
#
# ONE PLACE builds a redis client: the stream consumer
# (app/transport/consumer.py) and the push relay
# (app/transport/push_relay.py) both take theirs from redis_client().
#
# THE SOCKET TIMEOUT. Without one, a redis that accepted the connection
# and then went silent stops the reader forever -- no error, no log
# line. With one, silence becomes redis.exceptions.TimeoutError, which
# each loop already handles as "redis is away": the relay logs it and
# retries on its next tick; the consumer's loop ends, the process logs
# it and exits, and the container restarts it (app/consumer.py) --
# exactly what a refused connection already does.
#
# THE VALUE is derived, never configured: CONSUMER_BLOCK_MS plus
# REDIS_TIMEOUT_MARGIN_SECONDS. redis-py bounds every read by the
# socket timeout, the blocking XREADGROUP included, so a timeout at or
# below the block would turn every empty wait into an error; derived,
# it is longer than the block by construction. The connect timeout
# follows the socket timeout (redis-py's default when none is given).
# =============================================================================

from redis.asyncio import Redis

from app.core import constants
from app.core.config import settings


def redis_socket_timeout() -> float:
    """Seconds a redis client waits for any answer: the consumer's
    blocking read plus the margin -- always longer than the block."""
    return settings.consumer_block_ms / 1000 + constants.REDIS_TIMEOUT_MARGIN_SECONDS


def redis_client() -> Redis:
    """A redis client on REDIS_URL with the socket timeout."""
    client: Redis = Redis.from_url(
        settings.redis_url, socket_timeout=redis_socket_timeout(),
    )
    return client
