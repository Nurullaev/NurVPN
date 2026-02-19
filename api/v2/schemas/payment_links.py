from typing import Any

from pydantic import BaseModel, Field


class PaymentLinkCreateRequest(BaseModel):
    tg_id: int | None = Field(None, description="Telegram ID пользователя (если не задан identity_id)")
    identity_id: str | None = Field(None, description="ID идентичности; tg_id будет взят из привязки")
    amount: int | float = Field(..., gt=0, description="Сумма оплаты")
    currency: str = Field(default="RUB", description="Валюта (например RUB)")
    provider_id: str = Field(
        ...,
        description="Идентификатор кассы: ROBOKASSA, FREEKASSA, YOOKASSA, YOOMONEY, KASSAI_CARDS, KASSAI_SBP, HELEKET и др.",
    )
    success_url: str | None = Field(None, description="URL перенаправления после успешной оплаты")
    failure_url: str | None = Field(None, description="URL перенаправления после неуспешной оплаты")
    metadata: dict[str, Any] | None = Field(None, description="Дополнительные данные")


class PaymentLinkCreateResponse(BaseModel):
    success: bool
    payment_id: str | None = None
    payment_url: str | None = None
    error: str | None = None
