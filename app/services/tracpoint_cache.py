"""Redis-backed TTL cache for Tracpoint API responses.

Currently used for the asset roster, which rarely changes but is fetched
on every incremental pull cycle. Caching it cuts the steady-state
`getAllAssets` call rate from ~190/day per integration to ~24/day at the
default 1-hour TTL.

Set `TRACPOINT_ASSET_CACHE_TTL` (seconds) to override the default.
"""
import json
import logging
import os
from typing import Any

import redis.asyncio as redis
import stamina

from app import settings
from app.services.client import TracpointClient

logger = logging.getLogger(__name__)

DEFAULT_ASSET_TTL_SECONDS = int(os.environ.get("TRACPOINT_ASSET_CACHE_TTL", 60 * 60))
ASSET_KEY_PREFIX = "tracpoint:assets:"


class TracpointAssetCache:
    """Per-integration cache for the Tracpoint asset roster."""

    def __init__(self, redis_client: redis.Redis | None = None, ttl_seconds: int = DEFAULT_ASSET_TTL_SECONDS):
        self._redis = redis_client or redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_STATE_DB,
        )
        self._ttl = ttl_seconds

    def _key(self, integration_id: str) -> str:
        return f"{ASSET_KEY_PREFIX}{integration_id}"

    async def get(self, integration_id: str) -> list[dict[str, Any]] | None:
        try:
            async for attempt in stamina.retry_context(on=redis.RedisError, attempts=3, wait_initial=0.5, wait_max=5, wait_jitter=1.0):
                with attempt:
                    raw = await self._redis.get(self._key(integration_id))
        except redis.RedisError as exc:
            logger.warning("TracpointAssetCache.get failed for %s: %s", integration_id, exc)
            return None
        return json.loads(raw) if raw else None

    async def set(self, integration_id: str, assets: list[dict[str, Any]]) -> None:
        try:
            async for attempt in stamina.retry_context(on=redis.RedisError, attempts=3, wait_initial=0.5, wait_max=5, wait_jitter=1.0):
                with attempt:
                    await self._redis.setex(
                        self._key(integration_id),
                        self._ttl,
                        json.dumps(assets, default=str),
                    )
        except redis.RedisError as exc:
            logger.warning("TracpointAssetCache.set failed for %s: %s", integration_id, exc)


_default_cache: TracpointAssetCache | None = None


def get_default_asset_cache() -> TracpointAssetCache:
    global _default_cache
    if _default_cache is None:
        _default_cache = TracpointAssetCache()
    return _default_cache


async def fetch_assets_cached(
    client: TracpointClient,
    integration_id: str,
    cache: TracpointAssetCache | None = None,
) -> list[dict[str, Any]]:
    """Return the integration's asset roster, hitting Tracpoint only on cache miss."""
    cache = cache or get_default_asset_cache()
    cached = await cache.get(integration_id)
    if cached is not None:
        return cached
    fresh = await client.fetch_all_assets()
    await cache.set(integration_id, fresh)
    return fresh
