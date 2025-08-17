from schemas import PaymentSchema, ProcessorSummarySchema
from typing import Any, Dict
import httpx
import os
from datetime import datetime, timezone
from database import redis_client
from fastapi import HTTPException
import json

PROCESSOR_TOKEN = os.getenv("PROCESSOR_TOKEN", "123")

async def call_processor(base_url: str, payload: PaymentSchema, is_default: bool = True) -> Dict[str, Any]:
    url = f"{base_url}/payments"

    data = {
        **payload.model_dump(mode="json"),
        "requestedAt": datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
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
            fallback = {}
            default = {}
            if is_default:
                default = redis_client.get("default")
                if default:
                    default = json.loads(default)
                else:
                    default = ProcessorSummarySchema(totalRequests=0, totalAmount=0).model_dump()

                default["totalRequests"] += 1
                default["totalAmount"] += data["amount"]

                redis_client.set("default", json.dumps(default))
            else:
                fallback = redis_client.get("fallback")
                if fallback:
                    fallback = json.loads(fallback)
                else:
                    fallback = ProcessorSummarySchema(totalRequests=0, totalAmount=0).model_dump()

                fallback["totalRequests"] += 1
                fallback["totalAmount"] += data["amount"]

                redis_client.set("fallback", json.dumps(fallback))
        return {"url": url, "status_code": resp.status_code, "body": body}

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
        redis_client.delete("default")
        redis_client.delete("fallback")
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