from collections.abc import Awaitable, Callable
from typing import Any

from aiogram import BaseMiddleware
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, Message, Update
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import CHANNEL_EXISTS, CHANNEL_ID, CHANNEL_REQUIRED, CHANNEL_URL
from core.bootstrap import MODES_CONFIG
from core.cache_config import (
    SUBSCRIPTION_CACHE_SUBSCRIBED_TTL_SEC,
    SUBSCRIPTION_CACHE_UNSUBSCRIBED_TTL_SEC,
)
from core.redis_cache import cache_delete, cache_get, cache_key, cache_set
from handlers.buttons import SUB_CHANELL, SUB_CHANELL_DONE
from handlers.texts import SUBSCRIPTION_REQUIRED_MSG
from handlers.utils import edit_or_send_message
from logger import logger


class SubscriptionMiddleware(BaseMiddleware):
    def __init__(self) -> None:
        self._subscribed_ttl = SUBSCRIPTION_CACHE_SUBSCRIBED_TTL_SEC
        self._unsubscribed_ttl = SUBSCRIPTION_CACHE_UNSUBSCRIBED_TTL_SEC

    async def __call__(
        self,
        handler: Callable[[Update, dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: dict[str, Any],
    ) -> Any:
        channel_check_enabled = bool(MODES_CONFIG.get("CHANNEL_CHECK_ENABLED", CHANNEL_REQUIRED))
        if not CHANNEL_EXISTS or not channel_check_enabled:
            return await handler(event, data)

        tg_id = None
        message = None
        from_user = None

        if event.message:
            if event.message.chat.type != ChatType.PRIVATE:
                return await handler(event, data)
            if not event.message.from_user:
                return await handler(event, data)
            if event.message.from_user.is_bot:
                return await handler(event, data)

            tg_id = event.message.from_user.id
            message = event.message
            from_user = event.message.from_user
        elif event.callback_query:
            if event.callback_query.message and event.callback_query.message.chat.type != ChatType.PRIVATE:
                return await handler(event, data)
            if not event.callback_query.from_user:
                return await handler(event, data)
            if event.callback_query.from_user.is_bot:
                return await handler(event, data)

            tg_id = event.callback_query.from_user.id
            message = event.callback_query.message
            from_user = event.callback_query.from_user
        else:
            return await handler(event, data)

        cached_status = await self._get_cached_status(tg_id)
        if cached_status is False:
            logger.info(f"[SubMiddleware] Пользователь {tg_id} не подписан (cache)")
            await self._store_user_state(data, message, from_user)
            return await self._ask_to_subscribe(message)
        if cached_status is True:
            return await handler(event, data)

        bot = data.get("bot")
        if bot is None:
            return await handler(event, data)

        try:
            member = await bot.get_chat_member(CHANNEL_ID, tg_id)
            is_subscribed = member.status in ("member", "administrator", "creator")
            await self._cache_status(tg_id, is_subscribed)
            if not is_subscribed:
                logger.info(f"[SubMiddleware] Пользователь {tg_id} не подписан")
                await self._store_user_state(data, message, from_user)
                return await self._ask_to_subscribe(message)
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            logger.warning(f"[SubMiddleware] Ошибка проверки подписки {tg_id}, пропускаем: {e}")
            return await handler(event, data)

        return await handler(event, data)

    async def _get_cached_status(self, tg_id: int) -> bool | None:
        subscribed_key = cache_key("subscribed", tg_id)
        unsubscribed_key = cache_key("unsubscribed", tg_id)
        if await cache_get(subscribed_key) is not None:
            return True
        if await cache_get(unsubscribed_key) is not None:
            return False
        return None

    async def _cache_status(self, tg_id: int, is_subscribed: bool) -> None:
        subscribed_key = cache_key("subscribed", tg_id)
        unsubscribed_key = cache_key("unsubscribed", tg_id)
        if is_subscribed:
            await cache_delete(unsubscribed_key)
            await cache_set(subscribed_key, 1, self._subscribed_ttl)
        else:
            await cache_delete(subscribed_key)
            await cache_set(unsubscribed_key, 1, self._unsubscribed_ttl)

    async def _store_user_state(self, data: dict, message: Message, from_user):
        state: FSMContext = data.get("state")
        if not state or not from_user or from_user.is_bot:
            return

        state_data = await state.get_data()
        if "original_text" in state_data and "user_data" in state_data:
            return

        original_text = message.text or message.caption
        user_data = {
            "tg_id": from_user.id,
            "username": from_user.username,
            "first_name": from_user.first_name,
            "last_name": from_user.last_name,
            "language_code": from_user.language_code,
            "is_bot": from_user.is_bot,
        }

        await state.update_data(
            original_text=original_text,
            user_data=user_data,
        )

    async def _ask_to_subscribe(self, message: Message):
        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text=SUB_CHANELL, url=CHANNEL_URL))
        builder.row(InlineKeyboardButton(text=SUB_CHANELL_DONE, callback_data="check_subscription"))

        await edit_or_send_message(
            target_message=message,
            text=SUBSCRIPTION_REQUIRED_MSG,
            reply_markup=builder.as_markup(),
        )
