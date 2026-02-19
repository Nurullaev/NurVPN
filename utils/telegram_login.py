import hashlib
import hmac
import time


def verify_telegram_login(
    payload: dict,
    bot_token: str,
    *,
    max_age_seconds: int = 86400,
) -> bool:
    """
    Проверяет подпись и свежесть данных от Telegram Login Widget.
    """
    if not payload or not bot_token:
        return False
    received_hash = payload.get("hash")
    if not received_hash:
        return False
    auth_date = payload.get("auth_date")
    if auth_date is None:
        return False
    try:
        if int(auth_date) < time.time() - max_age_seconds:
            return False
    except (TypeError, ValueError):
        return False

    check_parts = sorted((k, v) for k, v in payload.items() if k != "hash" and v is not None)
    data_check_string = "\n".join(f"{k}={v}" for k, v in check_parts)

    secret_key = hashlib.sha256(bot_token.encode()).digest()
    computed = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    return hmac.compare_digest(computed, received_hash)
