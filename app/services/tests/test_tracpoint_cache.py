import json

import pytest

from app.services.tracpoint_cache import (
    ASSET_KEY_PREFIX,
    TracpointAssetCache,
    fetch_assets_cached,
)


@pytest.fixture
def fake_redis(mocker):
    """Async-mock wrapper for redis.asyncio.Redis with get / setex."""
    client = mocker.MagicMock()
    client.get = mocker.AsyncMock(return_value=None)
    client.setex = mocker.AsyncMock(return_value=True)
    return client


@pytest.mark.asyncio
async def test_get_returns_none_when_empty(fake_redis):
    cache = TracpointAssetCache(redis_client=fake_redis)

    assert await cache.get("integration-1") is None
    fake_redis.get.assert_awaited_once_with(f"{ASSET_KEY_PREFIX}integration-1")


@pytest.mark.asyncio
async def test_get_decodes_cached_payload(fake_redis):
    fake_redis.get.return_value = json.dumps([{"assetId": 1}]).encode()
    cache = TracpointAssetCache(redis_client=fake_redis)

    assert await cache.get("integration-1") == [{"assetId": 1}]


@pytest.mark.asyncio
async def test_set_writes_with_configured_ttl(fake_redis):
    cache = TracpointAssetCache(redis_client=fake_redis, ttl_seconds=120)

    await cache.set("integration-1", [{"assetId": 1}])

    fake_redis.setex.assert_awaited_once()
    key, ttl, payload = fake_redis.setex.await_args.args
    assert key == f"{ASSET_KEY_PREFIX}integration-1"
    assert ttl == 120
    assert json.loads(payload) == [{"assetId": 1}]


@pytest.mark.asyncio
async def test_fetch_assets_cached_returns_cache_hit_without_calling_client(mocker):
    cached = [{"assetId": 1, "displayName": "Land Rover"}]
    cache = mocker.MagicMock()
    cache.get = mocker.AsyncMock(return_value=cached)
    cache.set = mocker.AsyncMock()
    client = mocker.MagicMock()
    client.fetch_all_assets = mocker.AsyncMock()

    result = await fetch_assets_cached(client, "integration-1", cache=cache)

    assert result == cached
    client.fetch_all_assets.assert_not_called()
    cache.set.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_assets_cached_populates_cache_on_miss(mocker):
    fresh = [{"assetId": 42, "displayName": "Truck"}]
    cache = mocker.MagicMock()
    cache.get = mocker.AsyncMock(return_value=None)
    cache.set = mocker.AsyncMock()
    client = mocker.MagicMock()
    client.fetch_all_assets = mocker.AsyncMock(return_value=fresh)

    result = await fetch_assets_cached(client, "integration-1", cache=cache)

    assert result == fresh
    client.fetch_all_assets.assert_awaited_once()
    cache.set.assert_awaited_once_with("integration-1", fresh)
