import os
import subprocess
import sys
import traceback

from tempfile import NamedTemporaryFile

from aiogram import Bot, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from config import DB_NAME, DB_PASSWORD, DB_USER, PG_HOST, PG_PORT
from core.executor import run_io
from filters.admin import IsAdminFilter
from logger import logger

from . import router
from .keyboard import AdminPanelCallback, build_back_to_db_menu, build_database_kb, build_export_db_sources_kb


def sync_restore_database(
    tmp_path: str,
    db_name: str,
    db_user: str,
    db_password: str,
    pg_host: str,
    pg_port: str,
) -> tuple[bool, str]:
    """Восстановление БД из файла. Вызывать через run_io()."""
    is_custom_dump = False
    with open(tmp_path, "rb") as f:
        if f.read(5) == b"PGDMP":
            is_custom_dump = True

    try:
        subprocess.run(
            [
                "sudo", "-u", "postgres", "psql", "-d", "postgres",
                "-c",
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}' AND pid <> pg_backend_pid();",
            ],
            check=True,
        )
        subprocess.run(
            ["sudo", "-u", "postgres", "psql", "-d", "postgres", "-c", f"DROP DATABASE IF EXISTS {db_name};"],
            check=True,
        )
        subprocess.run(
            ["sudo", "-u", "postgres", "psql", "-d", "postgres", "-c", f"CREATE DATABASE {db_name} OWNER {db_user};"],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        return False, (e.stderr or e.stdout or str(e))

    os.environ["PGPASSWORD"] = db_password
    try:
        if is_custom_dump:
            result = subprocess.run(
                [
                    "pg_restore", f"--dbname={db_name}", "-U", db_user,
                    "-h", pg_host, "-p", pg_port, "--no-owner", "--exit-on-error", tmp_path,
                ],
                capture_output=True,
                text=True,
            )
        else:
            result = subprocess.run(
                ["psql", "-U", db_user, "-h", pg_host, "-p", pg_port, "-d", db_name, "-f", tmp_path],
                capture_output=True,
                text=True,
            )
        return result.returncode == 0, result.stderr or ""
    finally:
        del os.environ["PGPASSWORD"]


class DatabaseState(StatesGroup):
    waiting_for_backup_file = State()


@router.callback_query(AdminPanelCallback.filter(F.action == "database"), IsAdminFilter())
async def handle_database_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        text="🗄 <b>Управление базой данных</b>",
        reply_markup=build_database_kb(),
    )


@router.callback_query(AdminPanelCallback.filter(F.action == "restore_db"), IsAdminFilter())
async def prompt_restore_db(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📂 Отправьте файл резервной копии (.sql), чтобы восстановить базу данных.\n"
        "⚠️ Все текущие данные будут перезаписаны.",
        reply_markup=build_back_to_db_menu(),
    )
    await state.set_state(DatabaseState.waiting_for_backup_file)


@router.message(DatabaseState.waiting_for_backup_file, IsAdminFilter())
async def restore_database(message: Message, state: FSMContext, bot: Bot):
    document = message.document

    if not document or not document.file_name.endswith(".sql"):
        await message.answer("❌ Пожалуйста, отправьте файл с расширением .sql.")
        return

    try:
        with NamedTemporaryFile(delete=False, suffix=".sql") as tmp_file:
            tmp_path = tmp_file.name

        await bot.download(document, destination=tmp_path)
        logger.info("[Restore] Файл получен: {}", tmp_path)

        success, err_msg = await run_io(
            sync_restore_database,
            tmp_path,
            DB_NAME,
            DB_USER,
            DB_PASSWORD,
            PG_HOST,
            PG_PORT,
        )

        if not success:
            logger.error("[Restore] Ошибка: {}", err_msg)
            await message.answer(
                f"❌ Ошибка при восстановлении базы данных:\n<pre>{err_msg}</pre>",
            )
            return

        logger.info("[Restore] База восстановлена")
        await message.answer(
            "✅ База данных восстановлена.",
            reply_markup=build_back_to_db_menu(),
        )
        logger.info("[Restore] Завершение для перезапуска")
        await state.clear()
        sys.exit(0)

    except Exception as e:
        logger.exception(f"[Restore] Непредвиденная ошибка: {e}")
        await message.answer(
            f"❌ Произошла ошибка:\n<pre>{traceback.format_exc()}</pre>",
        )
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


@router.callback_query(AdminPanelCallback.filter(F.action == "export_db"), IsAdminFilter())
async def handle_export_db(callback: CallbackQuery):
    await callback.message.edit_text(
        "📤 Выберите панель, с которой требуется получить данные:\n\n"
        "<i>Подтянутся подписки с панели и будут сохранены в базу данных бота.</i>",
        reply_markup=build_export_db_sources_kb(),
    )


@router.callback_query(AdminPanelCallback.filter(F.action == "back_to_db_menu"), IsAdminFilter())
async def back_to_database_menu(callback: CallbackQuery):
    await callback.message.edit_text("📦 Управление базой данных:", reply_markup=build_database_kb())
