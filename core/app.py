import re
import sys
import warnings

warnings.filterwarnings(
    "ignore",
    message=r'.*Field "model_custom_emoji_id" in UniqueGiftColors has conflict with protected namespace',
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r".*resource_tracker: There appear to be \d+ leaked semaphore",
    category=UserWarning,
    module="multiprocessing.resource_tracker",
)


def _sanitize_traceback(text: str) -> str:
    """Убирает обфусцированные hex-последовательности и длинные мусорные строки из вывода исключений."""
    if not text:
        return text
    cleaned = re.sub(r"(?:\\x[0-9a-fA-F]{2}){20,}", "[...]", text)
    cleaned = re.sub(r"\\x[0-9a-fA-F]{2}", "", cleaned)
    lines = cleaned.split("\n")
    result = []
    max_line_len = 120
    for line in lines:
        if len(line) > max_line_len:
            if re.match(r"^[\s\^\*]+$", line):
                result.append("  [...]")
            else:
                result.append(line[:max_line_len] + "...")
        else:
            result.append(line)
    return "\n".join(result).strip()


def _excepthook(etype, value, tb):
    import traceback
    lines = traceback.format_exception(etype, value, tb)
    sys.stderr.write(_sanitize_traceback("".join(lines)) + "\n")
    sys.stderr.flush()


import traceback as _tb

_orig_format_exception = _tb.format_exception
_orig_format_exc = _tb.format_exc


def _patched_format_exception(*args, **kwargs):
    result = _orig_format_exception(*args, **kwargs)
    return [_sanitize_traceback("".join(result))]


def _patched_format_exc(limit=None, chain=True):
    return _sanitize_traceback(_orig_format_exc(limit=limit, chain=chain))


_tb.format_exception = _patched_format_exception
_tb.format_exc = _patched_format_exc
sys.excepthook = _excepthook

import asyncio
import multiprocessing
import os
import signal
import subprocess
import sys

import uvicorn
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from bot import bot, dp
from config import (
    API_ENABLE,
    API_HOST,
    API_LOGGING,
    API_PORT,
    BACKUP_TIME,
    PING_TIME,
    PROCESS_POOL_SIZE,
    PROVIDERS_ENABLED,
    RUN_MODE,
    SUB_PATH,
    WEBAPP_HOST,
    WEBAPP_PORT,
    WEBHOOK_PATH,
    WEBHOOK_URL,
    WEBHOOK_SECRET_TOKEN,
)
from core.bootstrap import bootstrap
from database import async_session_maker, cancel_expired_pending_payments, init_db
from handlers import router
from handlers.admin.stats.stats_handler import send_daily_stats_report
from handlers.fallback_router import fallback_router
from handlers.keys.subscriptions import handle_subscription
from handlers.notifications.general_notifications import periodic_notifications
from handlers.payments.cryptobot.webhook import cryptobot_webhook
from handlers.payments.freekassa.freekassa_pay import freekassa_webhook
from handlers.payments.gift import validate_client_code
from handlers.payments.providers import get_providers
from handlers.payments.robokassa.webhook import robokassa_webhook
from handlers.payments.tribute.handlers import TRIBUTE_SECRET
from handlers.payments.tribute.webhook import tribute_webhook
from handlers.payments.yookassa.handlers import MAIN_SECRET
from handlers.payments.yookassa.webhook import yookassa_webhook
from handlers.payments.yoomoney.webhook import yoomoney_webhook
from hooks.hooks import run_hooks
from logger import logger
from middlewares import register_middleware
from middlewares.webhook_guard import telegram_webhook_guard_middleware
from servers import check_servers
from web import register_web_routes


def _is_dev_mode():
    args = set(sys.argv[1:])
    return "-dev" in args or "--dev" in args


def _run_mode() -> str:
    mode = (RUN_MODE or "full").strip().lower()
    if mode not in {"full", "core"}:
        return "full"
    return mode


def _is_full_mode() -> bool:
    return _run_mode() == "full"


async def _periodic_database_backup():
    from utils.backup import backup_database

    logger.info("[Backup] Периодический бэкап БД запущен, интервал {} с", BACKUP_TIME)
    while True:
        err = await backup_database()
        if err:
            logger.error("[Backup] Ошибка: {}", err)
        await asyncio.sleep(BACKUP_TIME)


async def _run_api():
    config = uvicorn.Config(
        "api.main:app",
        host=API_HOST,
        port=API_PORT,
        log_level="info" if API_LOGGING else "critical",
        lifespan="off",
    )
    server = uvicorn.Server(config)
    try:
        await server.serve()
    except (asyncio.CancelledError, KeyboardInterrupt):
        return
    except Exception as e:
        logger.error("[API] Остановка с ошибкой: {}", e)


async def _start_common_jobs(app: web.Application | None, mode: str):
    from core.executor import get_process_pool, get_thread_pool
    get_thread_pool()
    get_process_pool()
    await init_db()
    await bootstrap()

    try:
        async with async_session_maker() as session:
            await cancel_expired_pending_payments(session)
    except Exception as e:
        logger.error("[PaymentsSweep] Ошибка при старте: {}", e)

    await run_hooks("startup", bot=bot, dp=dp, app=app, mode=mode, sessionmaker=async_session_maker)


    worker_separate = os.environ.get("NOTIFICATION_WORKER_SEPARATE", "").strip() == "1"
    if not worker_separate:
        asyncio.create_task(periodic_notifications(bot, sessionmaker=async_session_maker))
        if BACKUP_TIME > 0:
            asyncio.create_task(_periodic_database_backup())
        if PING_TIME > 0:
            asyncio.create_task(check_servers())

    async def scheduled_stats_report():
        async with async_session_maker() as session:
            await send_daily_stats_report(session)

    async def sweep_stale_payments_job():
        async with async_session_maker() as session:
            await cancel_expired_pending_payments(session)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(scheduled_stats_report, CronTrigger(hour=0, minute=0, timezone="Europe/Moscow"))
    scheduler.add_job(sweep_stale_payments_job, CronTrigger(minute=0, timezone="Europe/Moscow"))
    scheduler.start()


async def _on_startup(app):
    if TRIBUTE_SECRET != "ACCESS-KEY-B4TN-92QX-L7ME":
        return

    mode = _run_mode()

    if _is_full_mode():
        await bot.set_webhook(
            WEBHOOK_URL,
            secret_token=WEBHOOK_SECRET_TOKEN,
            drop_pending_updates=True,
        )

    await _start_common_jobs(app, mode)


async def _on_shutdown(app: web.Application):
    _stop_notification_worker_process()
    try:
        await run_hooks("shutdown", bot=bot, dp=dp, app=app)
    except Exception:
        pass
    from core.executor import shutdown_process_pool, shutdown_thread_pool
    shutdown_thread_pool()
    shutdown_process_pool()

    if _is_full_mode():
        try:
            await bot.delete_webhook()
        except Exception:
            pass

    for task in asyncio.all_tasks():
        task.cancel()
    try:
        await asyncio.gather(*asyncio.all_tasks(), return_exceptions=True)
    except Exception as e:
        logger.error("Ошибка при завершении работы: {}", e)


async def _dev_loop():
    from core.executor import get_process_pool, get_thread_pool, shutdown_process_pool, shutdown_thread_pool
    get_thread_pool()
    get_process_pool()
    logger.info("Запуск в режиме разработки (DEV)")

    if _is_full_mode():
        try:
            await bot.delete_webhook()
        except Exception:
            pass

    await init_db()
    await bootstrap()
    await run_hooks("startup", bot=bot, dp=dp, app=None, mode="dev", sessionmaker=async_session_maker)

    try:
        async with async_session_maker() as session:
            await cancel_expired_pending_payments(session)
    except Exception as e:
        logger.error("[PaymentsSweep] Ошибка при старте (dev): {}", e)

    tasks = []
    worker_separate = os.environ.get("NOTIFICATION_WORKER_SEPARATE", "").strip() == "1"
    if not worker_separate:
        tasks.append(asyncio.create_task(periodic_notifications(bot, sessionmaker=async_session_maker)))
        if PING_TIME > 0:
            tasks.append(asyncio.create_task(check_servers()))
        if BACKUP_TIME > 0:
            tasks.append(asyncio.create_task(_periodic_database_backup()))

    if API_ENABLE:
        logger.info("[DEV] Запуск API")
        cmd = [
            sys.executable,
            "-m",
            "uvicorn",
            "api.main:app",
            "--host",
            API_HOST,
            "--port",
            str(API_PORT),
            "--reload",
        ]
        if not API_LOGGING:
            cmd += ["--log-level", "critical"]
        subprocess.Popen(cmd)

    if _is_full_mode():
        register_middleware(dp, sessionmaker=async_session_maker)
        dp.include_router(router)
        dp.include_router(fallback_router)
        await dp.start_polling(bot)
    else:
        await asyncio.Event().wait()

    try:
        await run_hooks("shutdown", bot=bot, dp=dp, app=None)
    except Exception:
        pass
    _stop_notification_worker_process()
    shutdown_thread_pool()
    shutdown_process_pool()

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def _prod_loop():
    logger.info("Запуск в production")
    app = web.Application(
        middlewares=[
            telegram_webhook_guard_middleware(WEBHOOK_PATH, WEBHOOK_SECRET_TOKEN),
        ]
    )
    app["sessionmaker"] = async_session_maker
    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)

    providers = get_providers(PROVIDERS_ENABLED)
    if providers.get("YOOKASSA", {}).get("enabled"):
        app.router.add_post("/yookassa/webhook", yookassa_webhook)
    if providers.get("YOOMONEY", {}).get("enabled"):
        app.router.add_post("/yoomoney/webhook", yoomoney_webhook)
    if providers.get("CRYPTOBOT", {}).get("enabled"):
        app.router.add_post("/cryptobot/webhook", cryptobot_webhook)
    if providers.get("ROBOKASSA", {}).get("enabled"):
        app.router.add_post("/robokassa/webhook", robokassa_webhook)
    if providers.get("FREEKASSA", {}).get("enabled"):
        app.router.add_get("/freekassa/webhook", freekassa_webhook)
    if providers.get("TRIBUTE", {}).get("enabled"):
        app.router.add_post("/tribute/webhook", tribute_webhook)

    app.router.add_get(f"{SUB_PATH}{{email}}/{{tg_id}}", handle_subscription)
    await register_web_routes(app.router)

    if _is_full_mode():
        register_middleware(dp, sessionmaker=async_session_maker)
        dp.include_router(router)
        dp.include_router(fallback_router)
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=WEBAPP_HOST, port=WEBAPP_PORT)
    await site.start()

    if API_ENABLE:
        asyncio.create_task(_run_api())

    if _is_full_mode():
        logger.info("Webhook URL: {}", WEBHOOK_URL)
    else:
        logger.info("[Core] Telegram отключён, только backend")

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    try:
        await stop_event.wait()
    finally:
        _stop_notification_worker_process()
        from core.executor import shutdown_process_pool, shutdown_thread_pool
        shutdown_thread_pool()
        shutdown_process_pool()
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in pending:
            try:
                t.cancel()
            except Exception:
                pass
        await asyncio.gather(*pending, return_exceptions=True)


_notification_worker_process: multiprocessing.Process | None = None


def _run_notification_worker():
    """Точка входа для процесса-воркера уведомлений (отдельное ядро)."""
    asyncio.run(_notification_worker_loop())


async def _notification_worker_loop():
    """Отдельный процесс: только уведомления, бэкап, пинг. Свой event loop → своё ядро CPU."""
    from core.executor import get_process_pool, get_thread_pool, shutdown_process_pool, shutdown_thread_pool

    logger.info("[Worker] Воркер уведомлений запущен, PID: {}", os.getpid())
    get_thread_pool()
    get_process_pool()
    await init_db()
    await bootstrap()
    await run_hooks("startup", bot=bot, dp=dp, app=None, mode="prod", sessionmaker=async_session_maker)

    try:
        async with async_session_maker() as session:
            await cancel_expired_pending_payments(session)
    except Exception as e:
        logger.error("[PaymentsSweep] Ошибка при старте воркера: {}", e)

    tasks = []
    tasks.append(asyncio.create_task(periodic_notifications(bot, sessionmaker=async_session_maker)))
    if BACKUP_TIME > 0:
        tasks.append(asyncio.create_task(_periodic_database_backup()))
    if PING_TIME > 0:
        tasks.append(asyncio.create_task(check_servers()))

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            break
    try:
        await stop_event.wait()
    finally:
        try:
            await run_hooks("shutdown", bot=bot, dp=dp, app=None)
        except Exception:
            pass
        shutdown_thread_pool()
        shutdown_process_pool()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _guard_integrity() -> bool:
    is_valid = await validate_client_code()
    if not is_valid:
        print("❌ Бот не активирован. Проверьте ваш клиентский код.")
        sys.exit(1)
    expected_hash = "SOLO-ACCESS-KEY-B4YOGTN-H^WL-UV11FH"
    if MAIN_SECRET != expected_hash:
        logger.error("[Integrity] Нарушена целостность файлов. Обновитесь с полной заменой папки.")
        return False
    return True


def _stop_notification_worker_process():
    global _notification_worker_process
    if _notification_worker_process is not None and _notification_worker_process.is_alive():
        _notification_worker_process.terminate()
        _notification_worker_process.join(timeout=5)
        if _notification_worker_process.is_alive():
            _notification_worker_process.kill()
        _notification_worker_process = None


def run_app():
    async def runner():
        global _notification_worker_process
        ok = await _guard_integrity()
        if not ok:
            return

        if os.environ.get("NOTIFICATION_WORKER", "").strip() == "1":
            await _notification_worker_loop()
            return

        if (PROCESS_POOL_SIZE or 0) > 0:
            ctx = multiprocessing.get_context("spawn")
            _notification_worker_process = ctx.Process(
                target=_run_notification_worker,
                name="notification_worker",
                daemon=True,
            )
            _notification_worker_process.start()
            os.environ["NOTIFICATION_WORKER_SEPARATE"] = "1"
            logger.info("[Worker] Воркер уведомлений в отдельном процессе, PID: {}", _notification_worker_process.pid)
        if _is_dev_mode():
            await _dev_loop()
        else:
            await _prod_loop()

    try:
        asyncio.run(runner())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except Exception as e:
        logger.error("Ошибка при запуске приложения: {}", e)

