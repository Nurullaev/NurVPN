import asyncio
import atexit
import signal
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Callable, TypeVar

from logger import logger

T = TypeVar("T")

_thread_pool: ThreadPoolExecutor | None = None
_process_pool: ProcessPoolExecutor | None = None


def _atexit_shutdown_pools() -> None:
    """Очистка пулов при выходе из процесса (в т.ч. по atexit), уменьшает предупреждения resource_tracker."""
    shutdown_process_pool()
    shutdown_thread_pool()


class _IgnoreSIGINTProcess(multiprocessing.Process):
    """Процесс, игнорирующий SIGINT в воркере, чтобы Ctrl+C не обрывал queue.get() с трейсбеком."""

    def run(self) -> None:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        super().run()


def get_thread_pool() -> ThreadPoolExecutor:
    """Возвращает общий пул потоков (создаёт при первом вызове)."""
    global _thread_pool
    if _thread_pool is None:
        from config import EXECUTOR_POOL_SIZE
        size = max(1, int(EXECUTOR_POOL_SIZE))
        _thread_pool = ThreadPoolExecutor(max_workers=size, thread_name_prefix="bot-thread")
        logger.debug("[Executor] Пул потоков: {} воркеров", size)
    return _thread_pool


def shutdown_thread_pool() -> None:
    """Останавливает пул потоков (вызывать при shutdown приложения)."""
    global _thread_pool
    if _thread_pool is not None:
        _thread_pool.shutdown(wait=True)
        _thread_pool = None
        logger.debug("[Executor] Пул потоков остановлен")


def get_process_pool() -> ProcessPoolExecutor:
    """
    Возвращает пул процессов для тяжёлых задач (бэкап и т.д.).
    Задачи выполняются в отдельных процессах и могут использовать другие ядра CPU.
    """
    global _process_pool
    if _process_pool is None:
        from config import PROCESS_POOL_SIZE
        size = max(1, min(int(PROCESS_POOL_SIZE), multiprocessing.cpu_count() or 4))
        ctx = multiprocessing.get_context("spawn")
        ctx.Process = _IgnoreSIGINTProcess
        _process_pool = ProcessPoolExecutor(max_workers=size, mp_context=ctx)
        atexit.register(_atexit_shutdown_pools)
        logger.debug("[Executor] Пул процессов: {} воркеров", size)
    return _process_pool


def shutdown_process_pool() -> None:
    """Останавливает пул процессов (вызывать при shutdown приложения)."""
    global _process_pool
    if _process_pool is not None:
        try:
            atexit.unregister(_atexit_shutdown_pools)
        except Exception:
            pass
        _process_pool.shutdown(wait=True)
        _process_pool = None
        logger.debug("[Executor] Пул процессов остановлен")


async def run_io(fn: Callable[..., T], *args: object) -> T:
    """Выполняет fn(*args) в пуле потоков (I/O). Один вызов для всех блокирующих операций."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(get_thread_pool(), lambda: fn(*args))


async def run_cpu(fn: Callable[..., T], *args: object) -> T:
    """Выполняет fn(*args) в пуле процессов (CPU). fn — функция уровня модуля (для pickle)."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(get_process_pool(), fn, *args)
