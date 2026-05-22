"""Redis-backed cache for zeep WSDL/XSD fetches.

zeep's cache interface (`zeep.cache.Base`) is synchronous — it is invoked
during WSDL parsing, which happens inside `AsyncClient.__init__()` via a
synchronous `httpx.Client`. We therefore use the synchronous redis client
here, distinct from the `redis.asyncio` client used elsewhere in the app.

Keys are namespaced under `zeep:wsdl:` and stored with a configurable TTL.
The default 24h TTL is intentional: WSDLs change rarely and a stale entry
just means we re-fetch a bit later than necessary.
"""
import logging

import redis
from zeep.cache import Base

from app import settings

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 24 * 60 * 60
KEY_PREFIX = "zeep:wsdl:"


class RedisZeepCache(Base):
    """zeep cache backend that stores WSDL/XSD bytes in Redis."""

    def __init__(self, redis_client: redis.Redis, ttl_seconds: int = DEFAULT_TTL_SECONDS, key_prefix: str = KEY_PREFIX):
        self._redis = redis_client
        self._ttl = ttl_seconds
        self._prefix = key_prefix

    def _key(self, url: str) -> str:
        return f"{self._prefix}{url}"

    def add(self, url: str, content: bytes) -> None:
        try:
            self._redis.setex(self._key(url), self._ttl, content)
        except redis.RedisError as exc:
            # Cache failures must never break the SOAP call — log and continue.
            logger.warning("RedisZeepCache add failed for %s: %s", url, exc)

    def get(self, url: str):
        try:
            return self._redis.get(self._key(url))
        except redis.RedisError as exc:
            logger.warning("RedisZeepCache get failed for %s: %s", url, exc)
            return None


_default_cache: RedisZeepCache | None = None


def get_default_cache() -> RedisZeepCache:
    """Module-level singleton — one Redis connection pool per process."""
    global _default_cache
    if _default_cache is None:
        client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_STATE_DB,
        )
        _default_cache = RedisZeepCache(client)
    return _default_cache
