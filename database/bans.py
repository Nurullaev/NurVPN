from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import BlockedUser
from logger import logger


async def create_blocked_user(session: AsyncSession, tg_id: int):
    stmt = insert(BlockedUser).values(tg_id=tg_id).on_conflict_do_nothing(index_elements=[BlockedUser.tg_id])
    await session.execute(stmt)
    await session.commit()


async def save_blocked_user_ids(session: AsyncSession, tg_ids: list[int]) -> None:
    """Вставка списка tg_id в таблицу BlockedUser. Вызывать только из основного event loop."""
    if not tg_ids:
        return
    values = [{"tg_id": tg_id} for tg_id in tg_ids]
    stmt = insert(BlockedUser).values(values).on_conflict_do_nothing(index_elements=[BlockedUser.tg_id])
    await session.execute(stmt)
    await session.commit()
    logger.info(f"📝 Добавлено {len(tg_ids)} пользователей в blocked_users")
