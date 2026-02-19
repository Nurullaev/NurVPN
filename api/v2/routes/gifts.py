from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.depends import get_session, verify_identity_admin
from api.v2.base_crud import generate_crud_router
from api.v2.schemas import GiftBase, GiftResponse, GiftUpdate, GiftUsageResponse
from database.models import Gift, GiftUsage

router = APIRouter()

gift_router = generate_crud_router(
    model=Gift,
    schema_response=GiftResponse,
    schema_create=GiftBase,
    schema_update=GiftUpdate,
    identifier_field="gift_id",
    parameter_name="gift_id",
    enabled_methods=["get_all", "get_one", "create", "update"],
)
router.include_router(gift_router, prefix="", tags=["Gifts"])


@router.get("/by_tg_id/{tg_id}", response_model=list[GiftResponse], tags=["Gifts"])
async def get_gifts_by_tg_id(
    tg_id: int = Path(...),
    identity=Depends(verify_identity_admin),
    session: AsyncSession = Depends(get_session),
):
    """Список подарков по tg_id отправителя."""
    result = await session.execute(select(Gift).where(Gift.sender_tg_id == tg_id))
    gifts = result.scalars().all()
    if not gifts:
        raise HTTPException(status_code=404, detail="Gifts not found")
    return gifts


gift_usage_router = generate_crud_router(
    model=GiftUsage,
    schema_response=GiftUsageResponse,
    schema_create=None,
    schema_update=None,
    identifier_field="gift_id",
    enabled_methods=["get_all", "get_one", "delete"],
)
router.include_router(gift_usage_router, prefix="/usages", tags=["Gifts", "GiftUsages"])


@router.delete("/{gift_id}", response_model=dict, tags=["Gifts"])
async def delete_gift_with_usages(
    gift_id: str = Path(..., description="ID подарка"),
    identity=Depends(verify_identity_admin),
    session: AsyncSession = Depends(get_session),
):
    """Удаляет подарок и все его использования."""
    result = await session.execute(select(Gift).where(Gift.gift_id == gift_id))
    gift = result.scalar_one_or_none()
    if not gift:
        raise HTTPException(status_code=404, detail="Gift not found")
    await session.execute(delete(GiftUsage).where(GiftUsage.gift_id == gift_id))
    await session.delete(gift)
    await session.commit()
    return {"message": "Подарок и связанные использования удалены"}
