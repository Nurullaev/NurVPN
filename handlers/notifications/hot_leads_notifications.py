from aiogram import Bot
from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy.ext.asyncio import AsyncSession

from config import DISCOUNT_ACTIVE_HOURS, HOT_LEAD_INTERVAL_HOURS
from core.bootstrap import NOTIFICATIONS_CONFIG
from database import (
    add_notification,
    check_notification_time_bulk,
    get_hot_lead_notification_flags,
    get_hot_leads,
)
from database.tariffs import get_tariffs
from handlers.buttons import MAIN_MENU
from handlers.notifications.notify_kb import build_hot_lead_kb
from handlers.notifications.notify_utils import send_notification
from handlers.texts import (
    HOT_LEAD_FINAL_MESSAGE,
    HOT_LEAD_LOST_OPPORTUNITY,
    HOT_LEAD_MESSAGE,
)
from logger import logger


async def notify_hot_leads(bot: Bot, session: AsyncSession):
    logger.info("Запуск уведомлений для горячих лидов.")

    hot_lead_interval_hours = int(NOTIFICATIONS_CONFIG.get("HOT_LEADS_INTERVAL_HOURS", HOT_LEAD_INTERVAL_HOURS))
    discount_active_hours = int(NOTIFICATIONS_CONFIG.get("DISCOUNT_ACTIVE_HOURS", DISCOUNT_ACTIVE_HOURS))

    try:
        leads = await get_hot_leads(session)
        if not leads:
            logger.info("Нет горячих лидов для уведомлений.")
            return

        flags = await get_hot_lead_notification_flags(session, leads)
        can_send_after_step1 = await check_notification_time_bulk(
            session, [(tid, "hot_lead_step_1") for tid in leads], hot_lead_interval_hours
        )
        step2_expired_can_send = await check_notification_time_bulk(
            session, [(tid, "hot_lead_step_2") for tid in leads], discount_active_hours
        )
        can_send_after_step2 = await check_notification_time_bulk(
            session, [(tid, "hot_lead_step_2") for tid in leads], hot_lead_interval_hours
        )

        discount_tariffs = await get_tariffs(session, group_code="discounts")
        active_discount_tariffs = [t for t in discount_tariffs if t.get("is_active")]
        discount_max_tariffs = await get_tariffs(session, group_code="discounts_max")
        active_discount_max_tariffs = [t for t in discount_max_tariffs if t.get("is_active")]

        notified = 0

        for tg_id in leads:
            step_flags = flags.get(tg_id, set())
            has_step_1 = "hot_lead_step_1" in step_flags
            has_step_2 = "hot_lead_step_2" in step_flags
            has_step_3 = "hot_lead_step_3" in step_flags
            has_expired_notification = "hot_lead_step_2_expired" in step_flags

            if not has_step_1:
                await add_notification(session, tg_id, "hot_lead_step_1")
                logger.info(f"[HOT LEAD] Шаг 1 — зафиксировано без отправки: {tg_id}")
                continue

            if not has_step_2:
                if (tg_id, "hot_lead_step_1") not in can_send_after_step1:
                    continue
                if not active_discount_tariffs:
                    logger.warning(
                        f"[HOT LEAD] Пропуск шага 2 для {tg_id}: нет активных тарифов со скидкой (discounts)"
                    )
                    continue
                keyboard = build_hot_lead_kb()
                result = await send_notification(bot, tg_id, None, HOT_LEAD_MESSAGE, keyboard)
                if result:
                    await add_notification(session, tg_id, "hot_lead_step_2")
                    logger.info(f"Шаг 2 — отправлено первое уведомление: {tg_id}")
                    notified += 1
                continue

            if not has_step_3 and not has_expired_notification:
                if (tg_id, "hot_lead_step_2") in step2_expired_can_send:
                    builder = InlineKeyboardBuilder()
                    builder.row(InlineKeyboardButton(text=MAIN_MENU, callback_data="profile"))
                    result = await send_notification(
                        bot,
                        tg_id,
                        None,
                        HOT_LEAD_LOST_OPPORTUNITY,
                        builder.as_markup(),
                    )
                    if result:
                        await add_notification(session, tg_id, "hot_lead_step_2_expired")
                        logger.info(f"📭 Скидка упущена — отправлено уведомление: {tg_id}")
                continue

            if not has_step_3:
                if (tg_id, "hot_lead_step_2") not in can_send_after_step2:
                    continue
                if not active_discount_max_tariffs:
                    logger.warning(
                        f"[HOT LEAD] Пропуск шага 3 для {tg_id}: нет активных тарифов с максимальной скидкой (discounts_max)"
                    )
                    continue
                keyboard = build_hot_lead_kb(final=True)
                result = await send_notification(bot, tg_id, None, HOT_LEAD_FINAL_MESSAGE, keyboard)
                if result:
                    await add_notification(session, tg_id, "hot_lead_step_3")
                    logger.info(f"⚡ Шаг 3 — отправлено финальное уведомление: {tg_id}")
                    notified += 1

        logger.info(f"Уведомления завершены. Отправлено: {notified}")

    except Exception as e:
        logger.error(f"❌ Ошибка в notify_hot_leads: {e}")
