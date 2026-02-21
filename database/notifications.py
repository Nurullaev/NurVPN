from datetime import datetime, timedelta

from sqlalchemy import and_, delete, func, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from config import DISCOUNT_ACTIVE_HOURS
from core.bootstrap import NOTIFICATIONS_CONFIG
from database.models import Key, Notification, User
from logger import logger


async def add_notification(session: AsyncSession, tg_id: int, notification_type: str):
    try:
        stmt = (
            insert(Notification)
            .values(
                tg_id=tg_id,
                notification_type=notification_type,
                last_notification_time=datetime.utcnow(),
            )
            .on_conflict_do_update(
                index_elements=[Notification.tg_id, Notification.notification_type],
                set_={"last_notification_time": datetime.utcnow()},
            )
        )
        await session.execute(stmt)
        await session.commit()
        logger.info(f"✅ Добавлено уведомление {notification_type} для пользователя {tg_id}")
    except SQLAlchemyError as e:
        logger.error(f"❌ Ошибка при добавлении уведомления: {e}")
        await session.rollback()
        raise


async def delete_notification(session: AsyncSession, tg_id: int, notification_type: str):
    await session.execute(
        delete(Notification).where(
            Notification.tg_id == tg_id,
            Notification.notification_type == notification_type,
        )
    )
    await session.commit()
    logger.debug(f"🗑 Уведомление {notification_type} для пользователя {tg_id} удалено")


async def check_notification_time(session: AsyncSession, tg_id: int, notification_type: str, hours: int = 12) -> bool:
    stmt = select(Notification.last_notification_time).where(
        Notification.tg_id == tg_id, Notification.notification_type == notification_type
    )
    result = await session.execute(stmt)
    last_time = result.scalar_one_or_none()
    if not last_time:
        return True
    return datetime.utcnow() - last_time > timedelta(hours=hours)


async def check_notification_time_bulk(
    session: AsyncSession,
    items: list[tuple[int, str]],
    hours: int,
) -> set[tuple[int, str]]:
    """
    За один запрос определяет, кому из (tg_id, notification_type) можно слать уведомление
    (прошло больше hours с последней отправки или не слали никогда).
    Возвращает множество пар (tg_id, notification_type), которым можно слать.
    """
    if not items:
        return set()
    now = datetime.utcnow()
    threshold = now - timedelta(hours=hours)
    stmt = select(
        Notification.tg_id,
        Notification.notification_type,
        Notification.last_notification_time,
    ).where(tuple_(Notification.tg_id, Notification.notification_type).in_(items))
    result = await session.execute(stmt)
    rows = result.all()
    can_notify = set()
    found = set()
    for row in rows:
        found.add((row.tg_id, row.notification_type))
        if row.last_notification_time is None or row.last_notification_time < threshold:
            can_notify.add((row.tg_id, row.notification_type))
    for pair in items:
        if pair not in found:
            can_notify.add(pair)
    return can_notify


async def get_last_notification_time(session: AsyncSession, tg_id: int, notification_type: str) -> int | None:
    stmt = select(Notification.last_notification_time).where(
        Notification.tg_id == tg_id, Notification.notification_type == notification_type
    )
    result = await session.execute(stmt)
    ts = result.scalar_one_or_none()
    if ts:
        return int(ts.timestamp() * 1000)
    return None


async def check_hot_lead_discount(session: AsyncSession, tg_id: int) -> dict:
    try:
        result = await session.execute(
            select(Notification.notification_type, Notification.last_notification_time)
            .where(Notification.tg_id == tg_id)
            .where(Notification.notification_type.in_(["hot_lead_step_2", "hot_lead_step_3"]))
            .order_by(Notification.last_notification_time.desc())
            .limit(1)
        )

        row = result.first()
        if not row:
            return {"available": False}

        notification_type, last_time = row

        hours = int(NOTIFICATIONS_CONFIG.get("DISCOUNT_ACTIVE_HOURS", DISCOUNT_ACTIVE_HOURS))

        expires_at = last_time + timedelta(hours=hours)
        current_time = datetime.utcnow()

        if current_time > expires_at:
            return {"available": False}

        tariff_group = "discounts" if notification_type == "hot_lead_step_2" else "discounts_max"

        return {
            "available": True,
            "type": notification_type,
            "tariff_group": tariff_group,
            "expires_at": expires_at,
        }

    except Exception as e:
        logger.error(f"❌ Ошибка при проверке скидки горячего лида для {tg_id}: {e}")
        await session.rollback()
        return {"available": False}


async def check_notifications_bulk(
    session: AsyncSession,
    notification_type: str,
    hours: int,
    tg_ids: list[int] = None,
    emails: list[str] = None,
) -> list[dict]:
    from sqlalchemy import select

    from database.models import BlockedUser, Notification

    try:
        now = datetime.utcnow()
        subq_last_notification = (
            select(Notification.tg_id, func.max(Notification.last_notification_time).label("last_notification_time"))
            .where(Notification.notification_type == notification_type)
            .group_by(Notification.tg_id)
            .subquery()
        )

        stmt = (
            select(
                User.tg_id,
                Key.email,
                User.username,
                User.first_name,
                User.last_name,
                subq_last_notification.c.last_notification_time,
            )
            .outerjoin(Key, Key.tg_id == User.tg_id)
            .outerjoin(subq_last_notification, subq_last_notification.c.tg_id == User.tg_id)
        )

        if notification_type == "inactive_trial":
            stmt = stmt.where(
                and_(
                    User.trial.in_([0, -1]),
                    ~User.tg_id.in_(select(BlockedUser.tg_id)),
                    ~User.tg_id.in_(select(Key.tg_id.distinct())),
                )
            )

        if tg_ids:
            stmt = stmt.where(User.tg_id.in_(tg_ids))
        if emails:
            stmt = stmt.where(Key.email.in_(emails))

        result = await session.execute(stmt)
        users = []

        for row in result:
            last_time = row.last_notification_time
            can_notify = not last_time or (now - last_time > timedelta(hours=hours))

            if can_notify:
                users.append({
                    "tg_id": row.tg_id,
                    "email": row.email,
                    "username": row.username,
                    "first_name": row.first_name,
                    "last_name": row.last_name,
                    "last_notification_time": int(last_time.timestamp() * 1000) if last_time else None,
                })

        logger.info(f"Найдено {len(users)} пользователей, готовых к уведомлению типа {notification_type}")
        return users

    except Exception as e:
        logger.error(f"Ошибка при массовой проверке уведомлений типа {notification_type}: {e}")
        await session.rollback()
        return []
