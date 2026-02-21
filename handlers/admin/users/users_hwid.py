from aiogram import F, Router, types
from aiogram.types import CallbackQuery
from sqlalchemy.ext.asyncio import AsyncSession

from panels.remnawave_runtime import (
    invalidate_remnawave_profile,
    resolve_remnawave_api_url,
    with_remnawave_api,
)
from database import get_client_id_by_email
from filters.admin import IsAdminFilter

from .keyboard import AdminUserEditorCallback, build_editor_kb, build_hwid_menu_kb


router = Router()


@router.callback_query(
    AdminUserEditorCallback.filter(F.action == "users_hwid_menu"),
    IsAdminFilter(),
)
async def handle_hwid_menu(
    callback_query: CallbackQuery,
    callback_data: AdminUserEditorCallback,
    session: AsyncSession,
):
    email = callback_data.data
    tg_id = callback_data.tg_id

    client_id = await get_client_id_by_email(session, email)
    if not client_id:
        await callback_query.message.edit_text("🚫 Не удалось найти client_id по email.")
        return

    remna_api_url = await resolve_remnawave_api_url(session, "", fallback_any=True)
    if not remna_api_url:
        await callback_query.message.edit_text(
            "🚫 Нет доступного сервера Remnawave.",
            reply_markup=build_editor_kb(tg_id),
        )
        return

    async def _fetch_info_and_devices(api):
        user_info = await api.get_user_by_uuid(client_id)
        devices = await api.get_user_hwid_devices(client_id)
        return user_info, devices

    result = await with_remnawave_api(session, "", _fetch_info_and_devices, fallback_any=True, timeout_sec=8.0)
    if result is None:
        await callback_query.message.edit_text("❌ Ошибка авторизации в Remnawave.")
        return

    user_info, devices = result

    status_emoji = "🟢"
    status_text = "Онлайн"
    online_at_str = "—"
    first_connected_str = "—"
    last_node_uuid = "—"

    if not user_info:
        status_emoji = "⚪️"
        status_text = "Не найден"
    else:
        is_online = bool(user_info.get("isOnline"))
        status_emoji = "🟢" if is_online else "⚪️"
        status_text = "Онлайн" if is_online else "Офлайн"

        online_at = user_info.get("onlineAt")
        if online_at:
            online_at_str = online_at[:19].replace("T", " ")

        first_connected_at = user_info.get("firstConnectedAt")
        if first_connected_at:
            first_connected_str = first_connected_at[:19].replace("T", " ")

        last_node_uuid_val = user_info.get("lastConnectedNodeUuid")
        if last_node_uuid_val:
            last_node_uuid = last_node_uuid_val

    if not devices:
        text = (
            "💻 <b>HWID устройства</b>\n\n"
            f"{status_emoji} <b>Статус:</b> {status_text}\n"
            f"└ 🕓 <b>Онлайн был:</b> {online_at_str}\n"
            f"└ 🚀 <b>Первое подключение:</b> {first_connected_str}\n"
            f"└ 🛰 <b>Нода последнего подключения:</b> {last_node_uuid}\n\n"
            "🔌 Нет привязанных устройств."
        )
    else:
        text = (
            "💻 <b>HWID устройства</b>\n\n"
            f"{status_emoji} <b>Статус:</b> {status_text}\n"
            f"└ 🕓 <b>Онлайн был:</b> {online_at_str}\n"
            f"└ 🚀 <b>Первое подключение:</b> {first_connected_str}\n"
            f"└ 🛰 <b>Нода последнего подключения:</b> {last_node_uuid}\n\n"
            f"🔗 Привязано устройств: <b>{len(devices)}</b>\n\n"
        )
        for idx, device in enumerate(devices, 1):
            created = device.get("createdAt", "")[:19].replace("T", " ")
            updated = device.get("updatedAt", "")[:19].replace("T", " ")
            text += (
                f"<b>{idx}.</b> <code>{device.get('hwid')}</code>\n"
                f"└ 📱 <b>Модель:</b> {device.get('deviceModel') or '—'}\n"
                f"└ 🧠 <b>Платформа:</b> {device.get('platform') or '—'} / {device.get('osVersion') or '—'}\n"
                f"└ 🌐 <b>User-Agent:</b> {device.get('userAgent') or '—'}\n"
                f"└ 🕓 <b>Создано:</b> {created}\n"
                f"└ 🔄 <b>Обновлено:</b> {updated}\n\n"
            )

    await callback_query.message.edit_text(text, reply_markup=build_hwid_menu_kb(email, tg_id))


@router.callback_query(
    AdminUserEditorCallback.filter(F.action == "users_hwid_reset"),
    IsAdminFilter(),
)
async def handle_hwid_reset(
    callback_query: CallbackQuery,
    callback_data: AdminUserEditorCallback,
    session: AsyncSession,
):
    email = callback_data.data
    tg_id = callback_data.tg_id

    client_id = await get_client_id_by_email(session, email)
    if not client_id:
        await callback_query.message.edit_text("🚫 Не удалось найти client_id по email.")
        return

    remna_api_url = await resolve_remnawave_api_url(session, "", fallback_any=True)
    if not remna_api_url:
        await callback_query.message.edit_text(
            "🚫 Нет доступного сервера Remnawave.",
            reply_markup=build_editor_kb(tg_id),
        )
        return

    async def _reset_devices(api):
        devices = await api.get_user_hwid_devices(client_id)
        if not devices:
            return 0, 0
        deleted = 0
        for device in devices:
            if await api.delete_user_hwid_device(client_id, device["hwid"]):
                deleted += 1
        return len(devices), deleted

    reset_result = await with_remnawave_api(session, "", _reset_devices, fallback_any=True, timeout_sec=12.0)
    if reset_result is None:
        await callback_query.message.edit_text("❌ Ошибка авторизации в Remnawave.")
        return

    total, deleted = reset_result
    await invalidate_remnawave_profile(
        session,
        "",
        str(client_id),
        fallback_any=True,
    )
    if total == 0:
        await callback_query.message.edit_text(
            "ℹ️ У пользователя нет привязанных устройств.",
            reply_markup=build_editor_kb(tg_id, True),
        )
        return

    await callback_query.message.edit_text(
        f"✅ Удалено HWID-устройств: <b>{deleted}</b> из <b>{total}</b>.",
        reply_markup=build_editor_kb(tg_id, True),
    )
