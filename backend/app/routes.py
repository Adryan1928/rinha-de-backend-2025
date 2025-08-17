from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from schemas import PaymentSchema, ProcessorSummarySchema
import os
import asyncio
from services import call_processor_health, call_processor_summary, purge_payments, payments_summary_service, enqueue_payment
from typing import Optional

PROCESSOR_DEFAULT_URL = os.getenv("PROCESSOR_DEFAULT_URL", "http://localhost:8001")
PROCESSOR_FALLBACK_URL = os.getenv("PROCESSOR_FALLBACK_URL", "http://localhost:8002")
PROCESSOR_TOKEN = os.getenv("PROCESSOR_TOKEN", "123")

MESSAGE = [
    {
        "message": "Pagamento recebido com sucesso!",
        "status": "carregando"
    },
    {
        "message": "Erro ao processar pagamento.",
        "status": "finalizado"
    },
    {
        "message": "Pagamento realizado com sucesso.",
        "status": "finalizado"
    }
]

router = APIRouter(prefix="")

@router.post("/payments")
async def create_payment(payment: PaymentSchema):
    await enqueue_payment(payment)
    return {"message": "Pagamento recebido e será processado em breve."}


@router.get("/payments-summary")
async def payments_summary(from_date: Optional[str] = Query(None, alias="from"), to_date: Optional[str] = Query(None, alias="to")):
    try:

        default_summary, fallback_summary = await asyncio.gather(
            payments_summary_service(is_default=True, from_date=from_date, to_date=to_date),
            payments_summary_service(is_default=False, from_date=from_date, to_date=to_date)
        )

        return {
            "default": ProcessorSummarySchema.model_validate(default_summary),
            "fallback": ProcessorSummarySchema.model_validate(fallback_summary)
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Falha ao obter summary: {e}")

@router.get("/health")
async def health():
    return {"ok": True}

@router.get("/health-processors")
async def health_check_processors():
    """
    Dispara uma chamada para cada processor (default e fallback) em paralelo.
    Retorna status e corpo de cada um, para debug.
    """
    
    try:
        default_res, fallback_res = await asyncio.gather(
            call_processor_health(PROCESSOR_DEFAULT_URL),
            call_processor_health(PROCESSOR_FALLBACK_URL),
        )
        return {"default": default_res, "fallback": fallback_res}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Falha ao chamar processors: {e}")

@router.get("/payments-summary-processors")
async def payments_summary_processors():
    """
    Faz uma requisição em cada processor e ver como está o summary de cada um
    """

    try:
        default_res, fallback_res = await asyncio.gather(
            call_processor_summary(PROCESSOR_DEFAULT_URL),
            call_processor_summary(PROCESSOR_FALLBACK_URL),
        )
        return {"default": default_res, "fallback": fallback_res}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Falha ao chamar processors: {e}")


@router.post("/admin/purge-payments")
async def purge_payments_router():
    try:
        default_res, fallback_res = await asyncio.gather(
            purge_payments(PROCESSOR_DEFAULT_URL),
            purge_payments(PROCESSOR_FALLBACK_URL),
        )

        return {"default": default_res, "fallback": fallback_res}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Falha ao purgar pagamentos: {e}")