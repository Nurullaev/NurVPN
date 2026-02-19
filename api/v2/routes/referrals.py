from fastapi import Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.depends import get_session, verify_identity_admin
from api.v2.base_crud import generate_crud_router
from api.v2.schemas import ReferralResponse
from database.models import Referral

router = generate_crud_router(
    model=Referral,
    schema_response=ReferralResponse,
    schema_create=None,
    schema_update=None,
    identifier_field="referrer_tg_id",
    parameter_name="referrer_tg_id",
    enabled_methods=["get_all", "get_one", "get_all_by_field"],
)


@router.delete("/one")
async def delete_one_referral(
    referrer_tg_id: int = Query(..., description="ID пригласившего"),
    referred_tg_id: int = Query(..., description="ID приглашённого"),
    identity=Depends(verify_identity_admin),
    session: AsyncSession = Depends(get_session),
):
    """Удаляет одну связь реферала по паре referrer/referred."""
    result = await session.execute(
        select(Referral).where(Referral.referrer_tg_id == referrer_tg_id, Referral.referred_tg_id == referred_tg_id)
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Referral not found")
    await session.delete(obj)
    await session.commit()
    return {"status": "deleted_one"}
