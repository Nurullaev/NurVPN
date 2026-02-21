import asyncio

from fastapi import Depends, HTTPException, Path
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.depends import get_session, verify_identity_admin
from api.v2.schemas import UserBase, UserResponse, UserUpdate
from api.v2.base_crud import generate_crud_router
from database import async_session_maker, delete_user_data, get_servers
from database.models import Key, User
from handlers.keys.operations import delete_key_from_cluster
from logger import logger

router = generate_crud_router(
    model=User,
    schema_response=UserResponse,
    schema_create=UserBase,
    schema_update=UserUpdate,
    identifier_field="tg_id",
    enabled_methods=["get_all", "get_one", "create", "update"],
)


@router.delete("/{tg_id}", response_model=dict)
async def delete_user(
    tg_id: int = Path(..., description="Telegram ID пользователя"),
    identity=Depends(verify_identity_admin),
    session: AsyncSession = Depends(get_session),
):
    """Удаляет пользователя и его ключи на серверах."""
    try:
        result = await session.execute(select(Key.email, Key.client_id).where(Key.tg_id == tg_id))
        key_records = result.all()

        async with async_session_maker() as s:
            servers = await get_servers(session=s)
        cluster_ids = list(servers.keys())

        async def _delete_one(cluster_id: str, email: str, client_id: str):
            async with async_session_maker() as s:
                await delete_key_from_cluster(cluster_id, email, client_id, s)

        try:
            tasks = [
                _delete_one(cluster_id, email, client_id)
                for email, client_id in key_records
                for cluster_id in cluster_ids
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"[DELETE] Ошибка при удалении ключей с серверов для пользователя {tg_id}: {e}")

        await delete_user_data(session, tg_id)
        return {"detail": f"Пользователь {tg_id} и его ключи успешно удалены."}
    except Exception as e:
        logger.error(f"[DELETE] Ошибка при удалении пользователя {tg_id}: {e}")
        raise HTTPException(status_code=500, detail="Ошибка при удалении пользователя")
