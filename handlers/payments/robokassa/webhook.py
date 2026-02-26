from aiohttp import web

from core.webhook_abuse import (
    get_webhook_client_ip,
    is_webhook_ip_blocked,
    record_webhook_signature_failure,
)
from database import (
    add_payment,
    async_session_maker,
    get_payment_by_payment_id,
    invalidate_payment_cache,
    update_balance,
    update_payment_status,
)
from handlers.payments.utils import send_payment_success_notification
from logger import logger

from .service import check_payment_signature


async def robokassa_webhook(request: web.Request):
    try:
        ip = get_webhook_client_ip(request)
        if await is_webhook_ip_blocked(ip):
            return web.Response(status=429)
        params = await request.post()
        if not check_payment_signature(params):
            await record_webhook_signature_failure(ip)
            return web.Response(status=400)

        amount_raw = params.get("OutSum")
        inv_id = params.get("InvId")
        shp_id = params.get("Shp_id") or params.get("shp_id") or params.get("id")
        shp_pid = params.get("Shp_pid") or params.get("shp_pid") or params.get("pid")

        if not amount_raw or not inv_id or not shp_id or not shp_pid:
            return web.Response(status=400)

        tg_id = int(shp_id)
        amount = float(amount_raw)

        async with async_session_maker() as session:
            payment = await get_payment_by_payment_id(session, shp_pid)
            if payment:
                if payment.get("status") == "success":
                    return web.Response(text=f"OK{inv_id}")
                if payment.get("id") is not None:
                    ok = await update_payment_status(
                        session=session, internal_id=int(payment["id"]), new_status="success"
                    )
                    if not ok:
                        return web.Response(status=500)
                else:
                    await add_payment(
                        session=session,
                        tg_id=tg_id,
                        amount=amount,
                        payment_system="robokassa",
                        status="success",
                        currency="RUB",
                        payment_id=shp_pid,
                        metadata=None,
                    )
            else:
                await add_payment(
                    session=session,
                    tg_id=tg_id,
                    amount=amount,
                    payment_system="robokassa",
                    status="success",
                    currency="RUB",
                    payment_id=shp_pid,
                    metadata=None,
                )

            await update_balance(session, tg_id, amount)
            await send_payment_success_notification(tg_id, amount, session)
            await invalidate_payment_cache(shp_pid)

        return web.Response(text=f"OK{inv_id}")
    except Exception as e:
        logger.error(f"Error processing ROBOKASSA webhook: {e}")
        return web.Response(status=500)
