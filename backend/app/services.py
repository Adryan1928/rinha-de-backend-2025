from schemas import PaymentSchema, ProcessorSummarySchema
from typing import Any, Dict
import httpx
import os
from datetime import datetime, timezone
from database import redis_client
from fastapi import HTTPException
import json
from typing import Optional, List, Dict
from datetime import datetime

PROCESSOR_TOKEN = os.getenv("PROCESSOR_TOKEN", "123")

async def call_processor(base_url: str, payload: PaymentSchema, is_default: bool = True) -> Dict[str, Any]:
    url = f"{base_url}/payments"
    requestedAt = datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds")

    data = {
        **payload.model_dump(mode="json"),
        "requestedAt": requestedAt.replace("+00:00", "Z")
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(url, json=data)
        content_type = resp.headers.get("content-type", "")
        body = (
            (await resp.aread()).decode("utf-8", errors="replace")
            if "application/json" not in content_type.lower()
            else resp.json()
        )

        if (resp.status_code == 200):
            key = "default" if is_default else "fallback"
        
            summary = await redis_client.get(key)
            if summary:
                summary = json.loads(summary)
            else:
                summary = ProcessorSummarySchema(totalRequests=0, totalAmount=0).model_dump()

            summary["totalRequests"] += 1
            summary["totalAmount"] += data["amount"]

            await redis_client.set(key, json.dumps(summary))

            await redis_client.hset(
                f"payment:{data['correlationId']}",
                mapping={
                    "amount": str(data["amount"]),
                    "processor": key,
                    "requestedAt": requestedAt
                }
            )
            dt = datetime.fromisoformat(requestedAt)
            score = int(dt.timestamp())
            zset_key = f"payments:{key}"
            await redis_client.zadd(zset_key, {data['correlationId']: score})


        return {"url": url, "status_code": resp.status_code, "body": body}

async def get_payments_by_period(processor: str, from_date: Optional[str] = None, to_date: Optional[str] = None) -> List[Dict]:

    zset_key = f"payments:{processor}"

    # Se não for passado, usa -inf / +inf
    if from_date:
        start = int(datetime.fromisoformat(from_date.replace("Z", "+00:00")).timestamp())
    else:
        start = "-inf"

    if to_date:
        end = int(datetime.fromisoformat(to_date.replace("Z", "+00:00")).timestamp())
    else:
        end = "+inf"

    ids = await redis_client.zrangebyscore(zset_key, start, end)

    payments = [await redis_client.hgetall(f"payment:{pid}") for pid in ids]

    # Converte bytes → str
    payments = [
        {k: v for k, v in p.items()}
        for p in payments
    ]



    return payments


async def payments_summary_service(is_default=True, from_date: Optional[str] = None, to_date: Optional[str] = None):
    try:
        if is_default:
            key = "default"
        else:
            key = "fallback"


        if (from_date and to_date):
            payments = await get_payments_by_period(key, from_date, to_date)

            total_requests = len(payments)
            print(payments)
            total_amount = sum(float(p.get("amount", 0)) for p in payments)

            summary = ProcessorSummarySchema(
                totalRequests=total_requests,
                totalAmount=total_amount
            )
            
        else:
            summary = await redis_client.get(key)
            summary = json.loads(summary) if summary else ProcessorSummarySchema()


        return summary
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Falha ao obter summary: {e}")

async def call_processor_health(base_url: str) -> Dict[str, Any]:
    url = f"{base_url}/payments/service-health"

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(url)
        content_type = resp.headers.get("content-type", "")
        body = (
            (await resp.aread()).decode("utf-8", errors="replace")
            if "application/json" not in content_type.lower()
            else resp.json()
        )
        return {"url": url, "status_code": resp.status_code, "body": body}

async def call_processor_summary(base_url: str) -> Dict[str, Any]:
    url = f"{base_url}/admin/payments-summary"
    headers = {
        "X-Rinha-Token": f"{PROCESSOR_TOKEN}"
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(url, headers=headers)
        content_type = resp.headers.get("content-type", "")
        body = (
            (await resp.aread()).decode("utf-8", errors="replace")
            if "application/json" not in content_type.lower()
            else resp.json()
        )
        return {"url": url, "status_code": resp.status_code, "body": body}


async def purge_payments(base_url):

    try:
        await redis_client.flushdb()
        url = f"{base_url}/admin/purge-payments"
        headers = {
            "X-Rinha-Token": f"{PROCESSOR_TOKEN}"
        }

        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(url, headers=headers)
            content_type = resp.headers.get("content-type", "")
            body = (
                resp.aread().decode("utf-8", errors="replace")
                if "application/json" not in content_type.lower()
                else resp.json()
            )

            return {"url": url, "status_code": resp.status_code, "body": body}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao purgar pagamentos: {e}")