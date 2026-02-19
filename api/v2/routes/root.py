from fastapi import APIRouter

router = APIRouter(tags=["Root"])


@router.get("/api", include_in_schema=False)
async def root():
    return {"message": "SoloBot API v2", "docs": "/api/docs"}


@router.get("/api/version", include_in_schema=True)
async def version():
    return {"version": 2, "api": "v2"}
