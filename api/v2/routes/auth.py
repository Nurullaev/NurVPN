from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.depends import get_session, verify_identity_token
from api.v2.schemas.identities import (
    IdentityResponse,
    LinkTelegramRequest,
    LoginRequest,
    LoginResponse,
    LoginTelegramRequest,
    RegisterByEmailRequest,
    RegisterResponse,
)
from config import API_TOKEN_TTL_DAYS, API_TOKEN
from database import identities as idb
from utils.telegram_login import verify_telegram_login

router = APIRouter(prefix="/auth", tags=["Auth"])
TOKEN_TTL_HINT = "бессрочно" if API_TOKEN_TTL_DAYS is None else f"{API_TOKEN_TTL_DAYS} дн."
TELEGRAM_LOGIN_MAX_AGE = 86400  # 24 часа


@router.post("/register", response_model=RegisterResponse)
async def register_by_email(
    body: RegisterByEmailRequest,
    session: AsyncSession = Depends(get_session),
):
    (
        """Регистрация по почте и паролю: создаётся идентичность, выдаётся токен. Срок действия токена: """
        + TOKEN_TTL_HINT
        + "."
    )
    email = body.email.strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email обязателен")
    if not body.password or len(body.password) < 8:
        raise HTTPException(status_code=400, detail="Пароль минимум 8 символов")
    existing = await idb.get_identity_by_email(session, email)
    if existing:
        raise HTTPException(status_code=409, detail="Идентичность с таким email уже существует")
    identity, token = await idb.create_identity_with_token(session, email=email, password=body.password)
    return RegisterResponse(identity_id=identity.id, token=token)


@router.post("/login", response_model=LoginResponse)
async def login(
    body: LoginRequest,
    session: AsyncSession = Depends(get_session),
):
    """Вход по email и паролю. Возвращает identity_id и новый токен. Срок действия токена: """ + TOKEN_TTL_HINT + "."
    email = body.email.strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email обязателен")
    result = await idb.login_by_email(session, email, body.password)
    if not result:
        raise HTTPException(status_code=401, detail="Неверный email или пароль")
    identity, token = result
    return LoginResponse(identity_id=identity.id, token=token)


@router.post("/login-telegram", response_model=LoginResponse)
async def login_telegram(
    body: LoginTelegramRequest,
    session: AsyncSession = Depends(get_session),
):
    (
        """Вход через Telegram Login Widget (кнопка на сайте). По tg_id находим или создаём Identity, выдаём токен. Срок действия токена: """
        + TOKEN_TTL_HINT
        + "."
    )
    payload = body.model_dump(mode="json")
    if not verify_telegram_login(payload, API_TOKEN, max_age_seconds=TELEGRAM_LOGIN_MAX_AGE):
        raise HTTPException(status_code=401, detail="Неверная подпись или устаревшие данные от Telegram")
    identity = await idb.get_or_create_identity_for_tg(session, body.id)
    token = await idb.issue_token_for_identity(session, identity)
    return LoginResponse(identity_id=identity.id, token=token)


@router.post("/link-telegram", response_model=IdentityResponse)
async def link_telegram(
    body: LinkTelegramRequest,
    session: AsyncSession = Depends(get_session),
    identity=Depends(verify_identity_token),
):
    """Привязывает Telegram (tg_id) к текущей идентичности. Требуется X-Identity-Id и X-Token."""
    result = await idb.attach_telegram(session, identity.id, body.tg_id)
    if not result:
        raise HTTPException(
            status_code=409,
            detail="Этот Telegram уже привязан к другой идентичности",
        )
    return IdentityResponse.model_validate(result)


@router.get("/me", response_model=IdentityResponse)
async def me(
    identity=Depends(verify_identity_token),
):
    """Текущая идентичность по заголовкам X-Identity-Id и X-Token."""
    return IdentityResponse.model_validate(identity)
