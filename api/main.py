from time import perf_counter

from fastapi import FastAPI, Request

from config import API_LOGGING, API_VERSION
from logger import logger

if API_VERSION == 1:
    from api.v1 import router as api_router, VERSION as API_DOC_VERSION
else:
    from api.v2 import router as api_router, VERSION as API_DOC_VERSION

app = FastAPI(
    title=f"SoloBot API (Alpha) — API v{API_DOC_VERSION}",
    version=API_DOC_VERSION,
    description=f"Версия API: **v{API_DOC_VERSION}**.",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)


@app.middleware("http")
async def api_access_log_middleware(request: Request, call_next):
    if not API_LOGGING:
        return await call_next(request)

    started = perf_counter()
    response = await call_next(request)
    duration_ms = int((perf_counter() - started) * 1000)
    client_ip = request.client.host if request.client else "-"
    path_qs = request.url.path
    if request.url.query:
        path_qs = f"{path_qs}?{request.url.query}"

    logger.info(f'[API] {client_ip} "{request.method} {path_qs}" {response.status_code} {duration_ms}ms')
    return response


app.include_router(api_router)
