import json
import os
import time
from importlib import import_module
from typing import Any

_REDIS_CLIENT = None
_REDIS_UNAVAILABLE_UNTIL = 0.0
_REDIS_BACKOFF_SEC = 5.0


def _now() -> float:
    return time.monotonic()


async def _get_redis() -> Any | None:
    global _REDIS_CLIENT, _REDIS_UNAVAILABLE_UNTIL

    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT
    if _REDIS_UNAVAILABLE_UNTIL > _now():
        return None

    try:
        redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
        redis_from_url = import_module("redis.asyncio").from_url
        client = redis_from_url(redis_url, encoding="utf-8", decode_responses=True)
        await client.ping()
        _REDIS_CLIENT = client
        return _REDIS_CLIENT
    except Exception:
        _REDIS_UNAVAILABLE_UNTIL = _now() + _REDIS_BACKOFF_SEC
        _REDIS_CLIENT = None
        return None


def cache_key(prefix: str, *parts: Any) -> str:
    tail = ":".join(str(p) for p in parts)
    return f"{prefix}:{tail}" if tail else prefix


async def cache_get(key: str) -> Any | None:
    client = await _get_redis()
    if client is None:
        return None
    try:
        raw = await client.get(key)
        if raw is None:
            return None
        return json.loads(raw)
    except Exception:
        return None


async def cache_set(key: str, value: Any, ttl_sec: float) -> bool:
    client = await _get_redis()
    if client is None:
        return False
    try:
        ttl = max(1, int(ttl_sec))
        await client.set(key, json.dumps(value, ensure_ascii=False), ex=ttl)
        return True
    except Exception:
        return False


async def cache_delete(key: str) -> None:
    client = await _get_redis()
    if client is None:
        return
    try:
        await client.delete(key)
    except Exception:
        return


async def cache_setnx(key: str, value: Any, ttl_sec: float) -> bool:
    client = await _get_redis()
    if client is None:
        return False
    try:
        ttl = max(1, int(ttl_sec))
        return bool(await client.set(key, json.dumps(value, ensure_ascii=False), ex=ttl, nx=True))
    except Exception:
        return False


async def cache_incr(key: str, ttl_sec: float) -> int:
    client = await _get_redis()
    if client is None:
        return 1
    try:
        value = await client.incr(key)
        if value == 1:
            await client.expire(key, max(1, int(ttl_sec)))
        return int(value)
    except Exception:
        return 1


async def cache_delete_pattern(pattern: str) -> int:
    client = await _get_redis()
    if client is None:
        return 0
    deleted = 0
    try:
        async for key in client.scan_iter(match=pattern, count=200):
            deleted += int(await client.delete(key))
    except Exception:
        return deleted
    return deleted
