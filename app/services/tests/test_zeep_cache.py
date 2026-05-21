import pytest
import redis

from app.services.zeep_cache import KEY_PREFIX, RedisZeepCache


def test_add_writes_with_ttl(mocker):
    redis_client = mocker.MagicMock(spec=redis.Redis)
    cache = RedisZeepCache(redis_client, ttl_seconds=60)

    cache.add("http://example.com/foo.wsdl", b"<wsdl>")

    redis_client.setex.assert_called_once_with(
        f"{KEY_PREFIX}http://example.com/foo.wsdl",
        60,
        b"<wsdl>",
    )


def test_get_returns_cached_bytes(mocker):
    redis_client = mocker.MagicMock(spec=redis.Redis)
    redis_client.get.return_value = b"<wsdl>"
    cache = RedisZeepCache(redis_client)

    result = cache.get("http://example.com/foo.wsdl")

    assert result == b"<wsdl>"
    redis_client.get.assert_called_once_with(f"{KEY_PREFIX}http://example.com/foo.wsdl")


def test_get_returns_none_on_miss(mocker):
    redis_client = mocker.MagicMock(spec=redis.Redis)
    redis_client.get.return_value = None
    cache = RedisZeepCache(redis_client)

    assert cache.get("http://example.com/foo.wsdl") is None


def test_add_swallows_redis_errors(mocker):
    redis_client = mocker.MagicMock(spec=redis.Redis)
    redis_client.setex.side_effect = redis.RedisError("connection refused")
    cache = RedisZeepCache(redis_client)

    # Must not raise — cache failures should never break the SOAP path.
    cache.add("http://example.com/foo.wsdl", b"<wsdl>")


def test_get_swallows_redis_errors(mocker):
    redis_client = mocker.MagicMock(spec=redis.Redis)
    redis_client.get.side_effect = redis.RedisError("connection refused")
    cache = RedisZeepCache(redis_client)

    assert cache.get("http://example.com/foo.wsdl") is None
