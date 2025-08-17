from schemas import PaymentSchema, ProcessorSummarySchema
from typing import Any, Dict
import httpx
import os
from datetime import datetime, timezone
from database import redis_client
from fastapi import HTTPException, BackgroundTasks
import json
from typing import Optional, List, Dict
from datetime import datetime
import uuid
import asyncio

PROCESSOR_DEFAULT_URL = os.getenv("PROCESSOR_DEFAULT_URL", "http://payment-processor-default:8080")
PROCESSOR_FALLBACK_URL = os.getenv("PROCESSOR_FALLBACK_URL", "http://payment-processor-fallback:8080")
PROCESSOR_TOKEN = os.getenv("PROCESSOR_TOKEN", "123")

QUEUE_KEY = "payments_queue"

async def enqueue_payment(payload: PaymentSchema, is_default: bool = True):
    data = {
        **payload.model_dump(mode="json"),
        "requestedAt": datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "is_default": is_default
    }
    await redis_client.rpush(QUEUE_KEY, json.dumps(data))

async def choose_processor():
    default, fallback = await asyncio.gather(redis_client.hgetall(f"{HEALTH_KEY}:default"),
    redis_client.hgetall(f"{HEALTH_KEY}:fallback"))

    def parse(info):
        return {
            "failing": info.get("failing") == "true",
            "minResponseTime": int(info.get("minResponseTime", "9999"))
        }

    default = parse(default)
    fallback = parse(fallback)

    candidates = []
    if not default["failing"]:
        candidates.append(("default", PROCESSOR_DEFAULT_URL, default["minResponseTime"]))
    if not fallback["failing"]:
        candidates.append(("fallback", PROCESSOR_FALLBACK_URL, fallback["minResponseTime"]))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[2])
    return candidates


async def process_payment_with_fallback(payload: dict):
    try:
        chosen = await choose_processor()
        if not chosen and not chosen[0]:
            return False
    except Exception as e:
        return False

    
    try:
        name, url, _ = chosen[0]

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(PROCESSOR_DEFAULT_URL + "/payments", json=payload)
            if 200 <= resp.status_code < 300:
                await save_payment(payload, is_default=True)
                return True
            else:
                raise Exception("Default processor falhou")
    except Exception:
        if not chosen[2]:
            return False
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(PROCESSOR_FALLBACK_URL + "/payments", json=payload)
                if 200 <= resp.status_code < 300:
                    await save_payment(payload, is_default=False)
                    return True
        except Exception as e:
            print(f"Falha total no pagamento: {e}")
    return False

async def worker():
    while True:
        item = await redis_client.blpop(QUEUE_KEY, timeout=0.1)
        if item:
            _, payload_json = item
            payload = json.loads(payload_json)
            await process_payment_with_fallback(payload)
        else:
            await asyncio.sleep(0.1)



async def save_payment(data: dict, is_default: bool):
    key = "default" if is_default else "fallback"
    correlation_id = data.get("correlationId") or str(uuid.uuid4())
    amount = float(data.get("amount", 0))

    dt = datetime.fromisoformat(data["requestedAt"].replace("Z","+00:00"))

    await asyncio.gather(
        redis_client.hset(f"payment:{correlation_id}", mapping={
            "amount": str(amount),
            "processor": key,
            "requestedAt": data["requestedAt"]
        }),
        redis_client.zadd(f"payments:{key}", {correlation_id: int(dt.timestamp())})
    )

async def get_payments_by_period(processor: str, from_date: Optional[str] = None, to_date: Optional[str] = None) -> List[Dict]:

    zset_key = f"payments:{processor}"

    if from_date:
        start = int(datetime.fromisoformat(from_date.replace("Z", "+00:00")).timestamp())
    else:
        start = "-inf"

    if to_date:
        end = int(datetime.fromisoformat(to_date.replace("Z", "+00:00")).timestamp())
    else:
        end = "+inf"

    ids = await redis_client.zrangebyscore(zset_key, start, end)

    payments = await asyncio.gather(
        *[redis_client.hgetall(f"payment:{pid}") for pid in ids]
    )

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


        payments = await get_payments_by_period(key, from_date, to_date)

        total_requests = len(payments)
        total_amount = sum(float(p.get("amount", 0)) for p in payments)

        summary = ProcessorSummarySchema(
            totalRequests=total_requests,
            totalAmount=total_amount
        )


        return summary
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Falha ao obter summary: {e}")
    

HEALTH_KEY = "processors_health"

async def update_processor_health():
    while True:
        for name, url in [("default", PROCESSOR_DEFAULT_URL), ("fallback", PROCESSOR_FALLBACK_URL)]:
            try:
                health = await call_processor_health(url)
                if health["status_code"] == 200 and not health["body"].get("failing", True):
                    status = {
                        "failing": "false",
                        "minResponseTime": str(health["body"].get("minResponseTime", 9999))
                    }
                else:
                    status = {"failing": "true", "minResponseTime": "9999"}
            except Exception:
                status = {"failing": "true", "minResponseTime": "9999"}

            # salva cada processor como um hash dentro da chave principal
            await redis_client.hset(f"{HEALTH_KEY}:{name}", mapping=status)

        await asyncio.sleep(5)


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