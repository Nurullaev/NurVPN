import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from config import REMNAWAVE_LOGIN, REMNAWAVE_PASSWORD, REMNAWAVE_TOKEN_LOGIN_ENABLED
from core.cache_config import (
    REMNAWAVE_MAX_CONCURRENCY,
    REMNAWAVE_ACTION_TIMEOUT_SEC,
    REMNAWAVE_PROFILE_CACHE_TTL_SEC,
    REMNAWAVE_PROFILE_ERROR_CACHE_TTL_SEC,
    REMNAWAVE_PROFILE_TIMEOUT_SEC,
    REMNAWAVE_SERVER_CACHE_TTL_SEC,
)
from core.executor import run_io
from core.redis_cache import cache_delete_pattern, cache_get, cache_key, cache_set
from database import get_servers
from logger import logger
from panels.remnawave import RemnawaveAPI

_remnawave_semaphore = asyncio.Semaphore(REMNAWAVE_MAX_CONCURRENCY)


async def _fetch_profile_http_only(api_url: str, client_id: str) -> dict[str, Any] | None:
    """Только HTTP к панели: логин + устройства + юзер. Без кэша и без resolve. Вызывается из потока."""
    api = RemnawaveAPI(api_url)
    try:
        logged_in = True
        if not REMNAWAVE_TOKEN_LOGIN_ENABLED:
            logged_in = await asyncio.wait_for(
                api.login(REMNAWAVE_LOGIN, REMNAWAVE_PASSWORD),
                timeout=REMNAWAVE_PROFILE_TIMEOUT_SEC,
            )
        if not logged_in:
            return None
        devices = await asyncio.wait_for(
            api.get_user_hwid_devices(client_id),
            timeout=REMNAWAVE_PROFILE_TIMEOUT_SEC,
        )
        user_data = await asyncio.wait_for(
            api.get_user_by_uuid(client_id),
            timeout=REMNAWAVE_PROFILE_TIMEOUT_SEC,
        )
        hwid_count = len(devices or [])
        used_gb = None
        traffic_limit_bytes = None
        hwid_device_limit = None
        if user_data:
            user_traffic = user_data.get("userTraffic", {})
            used_bytes = user_traffic.get("usedTrafficBytes", 0)
            used_gb = round(used_bytes / 1073741824, 1)
            traffic_limit_bytes = user_data.get("trafficLimitBytes")
            hwid_device_limit = user_data.get("hwidDeviceLimit")
        return {
            "api_url": api_url,
            "hwid_count": hwid_count,
            "used_gb": used_gb,
            "traffic_limit_bytes": traffic_limit_bytes,
            "hwid_device_limit": hwid_device_limit,
        }
    except (asyncio.TimeoutError, Exception):
        return None
    finally:
        if hasattr(api, "aclose"):
            try:
                await api.aclose()
            except Exception:
                pass


def _run_profile_http_in_thread(api_url: str, client_id: str) -> dict[str, Any] | None:
    """Синхронная обёртка: свой event loop в потоке, чтобы не блокировать основной цикл бота."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(_fetch_profile_http_only(api_url, client_id))
    finally:
        loop.close()


def _run_with_api_in_thread(
    api_url: str,
    operation: Callable[[RemnawaveAPI], Awaitable[Any]],
    timeout_sec: float,
) -> Any:
    """Синхронная обёртка: логин + operation(api) в отдельном event loop в потоке."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    api = RemnawaveAPI(api_url)
    try:
        logged_in = True
        if not REMNAWAVE_TOKEN_LOGIN_ENABLED:
            logged_in = loop.run_until_complete(
                asyncio.wait_for(api.login(REMNAWAVE_LOGIN, REMNAWAVE_PASSWORD), timeout=timeout_sec)
            )
        if not logged_in:
            return None
        coro = operation(api)
        return loop.run_until_complete(asyncio.wait_for(coro, timeout=timeout_sec))
    except (asyncio.TimeoutError, Exception):
        return None
    finally:
        if hasattr(api, "aclose"):
            try:
                loop.run_until_complete(api.aclose())
            except Exception:
                pass
        loop.close()


def invalidate_remnawave_profile_cache(*, api_url: str | None = None, client_id: str | None = None) -> None:
    """Invalidate cached Remnawave profiles by api_url/client_id (or both)."""
    import asyncio

    async def _invalidate_async() -> None:
        if api_url is None and client_id is None:
            await cache_delete_pattern("remna_profile:*")
            return
        if api_url is not None and client_id is not None:
            await cache_delete_pattern(f"remna_profile:{api_url}:{client_id}")
            return
        if api_url is not None:
            await cache_delete_pattern(f"remna_profile:{api_url}:*")
            return
        await cache_delete_pattern(f"remna_profile:*:{client_id}")

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_invalidate_async())
    except RuntimeError:
        return


async def resolve_remnawave_api_url(
    session: AsyncSession,
    server_ref: str,
    *,
    fallback_any: bool = False,
) -> str | None:
    ckey = cache_key("remna_server", str(server_ref), int(bool(fallback_any)))
    cached_api_url = await cache_get(ckey)
    if isinstance(cached_api_url, str) or cached_api_url is None:
        if cached_api_url is not None:
            return cached_api_url

    servers = await get_servers(session)
    ref = str(server_ref)
    remna_server = None

    cluster_servers = servers.get(ref) or servers.get(str(ref)) or []
    remna_server = next((srv for srv in cluster_servers if srv.get("panel_type") == "remnawave"), None)

    if remna_server is None:
        for cluster_name, cluster in servers.items():
            for srv in cluster:
                if (srv.get("server_name") == ref or str(cluster_name) == ref) and srv.get("panel_type") == "remnawave":
                    remna_server = srv
                    break
            if remna_server:
                break

    if remna_server is None and fallback_any:
        remna_server = next((srv for cluster in servers.values() for srv in cluster if srv.get("panel_type") == "remnawave"), None)

    api_url = remna_server.get("api_url") if remna_server else None
    await cache_set(ckey, api_url, REMNAWAVE_SERVER_CACHE_TTL_SEC)
    return api_url


async def get_remnawave_profile(
    session: AsyncSession,
    server_ref: str,
    client_id: str,
    *,
    fallback_any: bool = False,
) -> dict[str, Any] | None:
    api_url = await resolve_remnawave_api_url(session, server_ref, fallback_any=fallback_any)
    if not api_url:
        return None

    pkey = cache_key("remna_profile", api_url, client_id)
    cached_profile = await cache_get(pkey)
    if isinstance(cached_profile, dict) or cached_profile is None:
        if cached_profile is not None:
            return cached_profile

    async with _remnawave_semaphore:
        profile = await run_io(_run_profile_http_in_thread, api_url, client_id)
        if profile is None:
            logger.warning(f"[Remnawave] Таймаут или ошибка профиля для client_id={client_id}")

    ttl = REMNAWAVE_PROFILE_CACHE_TTL_SEC if profile else REMNAWAVE_PROFILE_ERROR_CACHE_TTL_SEC
    await cache_set(pkey, profile, ttl)
    return profile


async def invalidate_remnawave_profile(
    session: AsyncSession,
    server_ref: str,
    client_id: str,
    *,
    fallback_any: bool = False,
) -> None:
    api_url = await resolve_remnawave_api_url(session, server_ref, fallback_any=fallback_any)
    if api_url:
        invalidate_remnawave_profile_cache(api_url=api_url, client_id=client_id)
    else:
        invalidate_remnawave_profile_cache(client_id=client_id)


async def with_remnawave_api(
    session: AsyncSession,
    server_ref: str,
    operation: Callable[[RemnawaveAPI], Awaitable[Any]],
    *,
    fallback_any: bool = False,
    timeout_sec: float = REMNAWAVE_ACTION_TIMEOUT_SEC,
) -> Any | None:
    api_url = await resolve_remnawave_api_url(session, server_ref, fallback_any=fallback_any)
    if not api_url:
        return None

    async with _remnawave_semaphore:
        result = await run_io(_run_with_api_in_thread, api_url, operation, timeout_sec)
        if result is None:
            logger.warning(f"[Remnawave] Таймаут или ошибка операции для server_ref={server_ref}")
        return result
