import hashlib
import secrets
from datetime import datetime, timedelta

import bcrypt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import API_TOKEN_TTL_DAYS
from core.executor import run_cpu, run_io
from database.models import Admin, Identity, User


_BCRYPT_MAX_PASSWORD_BYTES = 72
_BCRYPT_ROUNDS = 12


def _password_bytes(password: str) -> bytes:
    """Пароль в байтах, не длиннее 72 байт (ограничение bcrypt)."""
    raw = password.encode("utf-8")
    if len(raw) > _BCRYPT_MAX_PASSWORD_BYTES:
        return raw[:_BCRYPT_MAX_PASSWORD_BYTES]
    return raw


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def hash_password(password: str) -> str:
    """Хеш пароля через bcrypt (соль уникальна на каждый пароль)."""
    salt = bcrypt.gensalt(rounds=_BCRYPT_ROUNDS)
    return bcrypt.hashpw(_password_bytes(password), salt).decode("ascii")


def check_password(password: str, password_hash: str | None) -> bool:
    if not password_hash:
        return False
    try:
        return bcrypt.checkpw(_password_bytes(password), password_hash.encode("ascii"))
    except Exception:
        return False


def generate_token() -> str:
    return secrets.token_urlsafe(32)


async def create_identity(
    session: AsyncSession,
    email: str | None = None,
    tg_id: int | None = None,
) -> Identity:
    """Создаёт идентичность; можно задать email и/или tg_id."""
    identity = Identity(email=email.strip().lower() if email else None, tg_id=tg_id)
    session.add(identity)
    await session.flush()
    if tg_id:
        await session.execute(User.__table__.update().where(User.tg_id == tg_id).values(identity_id=identity.id))
    await session.commit()
    await session.refresh(identity)
    return identity


async def get_identity_by_id(session: AsyncSession, identity_id: str) -> Identity | None:
    """Возвращает идентичность по id."""
    result = await session.execute(select(Identity).where(Identity.id == identity_id))
    return result.scalar_one_or_none()


async def get_identity_by_email(session: AsyncSession, email: str) -> Identity | None:
    """Возвращает идентичность по email."""
    if not email or not email.strip():
        return None
    result = await session.execute(select(Identity).where(Identity.email == email.strip().lower()))
    return result.scalar_one_or_none()


async def get_identity_by_tg_id(session: AsyncSession, tg_id: int) -> Identity | None:
    """Возвращает идентичность по tg_id."""
    result = await session.execute(select(Identity).where(Identity.tg_id == tg_id))
    return result.scalar_one_or_none()


async def get_identity_by_token_hash(session: AsyncSession, token_hash: str) -> Identity | None:
    """Возвращает идентичность по хешу API-токена."""
    result = await session.execute(select(Identity).where(Identity.api_token_hash == token_hash))
    return result.scalar_one_or_none()


async def issue_token_for_identity(session: AsyncSession, identity: Identity) -> str:
    """Генерирует токен, сохраняет хеш и token_issued_at в identity, возвращает токен (показать один раз)."""
    token = generate_token()
    identity.api_token_hash = await run_io(hash_token, token)
    identity.token_issued_at = datetime.utcnow()
    await session.commit()
    await session.refresh(identity)
    return token


def _is_token_expired(identity: Identity) -> bool:
    """Проверяет, истёк ли срок действия токена (если задан API_TOKEN_TTL_DAYS)."""
    if API_TOKEN_TTL_DAYS is None or identity.token_issued_at is None:
        return False
    expiry = identity.token_issued_at + timedelta(days=API_TOKEN_TTL_DAYS)
    return datetime.utcnow() >= expiry


async def create_identity_with_token(
    session: AsyncSession,
    email: str | None = None,
    password: str | None = None,
    tg_id: int | None = None,
) -> tuple[Identity, str]:
    """Создаёт идентичность и выдаёт API-токен. При регистрации по почте передать email и password."""
    identity = await create_identity(session, email=email, tg_id=tg_id)
    if password:
        identity.password_hash = await run_cpu(hash_password, password)
        await session.commit()
        await session.refresh(identity)
    token = await issue_token_for_identity(session, identity)
    return identity, token


async def verify_identity_token(session: AsyncSession, identity_id: str, token: str) -> Identity | None:
    """Проверяет пару identity_id + token и срок действия токена; возвращает Identity или None."""
    identity = await get_identity_by_id(session, identity_id)
    if not identity or not identity.api_token_hash:
        return None
    token_hash = await run_io(hash_token, token)
    if token_hash != identity.api_token_hash:
        return None
    if _is_token_expired(identity):
        return None
    return identity


async def login_by_email(session: AsyncSession, email: str, password: str) -> tuple[Identity, str] | None:
    """Вход по email и паролю: проверяет пароль, выдаёт новый токен; возвращает (identity, token) или None."""
    identity = await get_identity_by_email(session, email)
    if not identity:
        return None
    if not await run_cpu(check_password, password, identity.password_hash):
        return None
    token = await issue_token_for_identity(session, identity)
    return identity, token


async def resolve_tg_id(session: AsyncSession, identity_id: str) -> int | None:
    """По identity_id возвращает tg_id, если привязан."""
    identity = await get_identity_by_id(session, identity_id)
    return identity.tg_id if identity else None


async def attach_email(session: AsyncSession, identity_id: str, email: str) -> Identity | None:
    """Привязывает email к идентичности."""
    identity = await get_identity_by_id(session, identity_id)
    if not identity:
        return None
    email_clean = email.strip().lower() if email else None
    if not email_clean:
        return identity
    existing = await get_identity_by_email(session, email_clean)
    if existing and existing.id != identity_id:
        return None
    identity.email = email_clean
    await session.commit()
    await session.refresh(identity)
    return identity


async def attach_telegram(session: AsyncSession, identity_id: str, tg_id: int) -> Identity | None:
    """Привязывает Telegram (tg_id) к идентичности и связывает User с identity. Если tg_id в admins — выставляет is_admin."""
    identity = await get_identity_by_id(session, identity_id)
    if not identity:
        return None
    existing = await get_identity_by_tg_id(session, tg_id)
    if existing and existing.id != identity_id:
        return None
    identity.tg_id = tg_id
    admin_row = await session.execute(select(Admin).where(Admin.tg_id == tg_id))
    if admin_row.scalar_one_or_none():
        identity.is_admin = True
    await session.execute(User.__table__.update().where(User.tg_id == tg_id).values(identity_id=identity_id))
    await session.commit()
    await session.refresh(identity)
    return identity


async def get_or_create_identity_for_tg(session: AsyncSession, tg_id: int) -> Identity:
    """Для tg_id возвращает существующую идентичность или создаёт новую и привязывает User."""
    identity = await get_identity_by_tg_id(session, tg_id)
    if identity:
        return identity
    identity = Identity(tg_id=tg_id)
    session.add(identity)
    await session.flush()
    await session.execute(User.__table__.update().where(User.tg_id == tg_id).values(identity_id=identity.id))
    await session.commit()
    await session.refresh(identity)
    return identity
