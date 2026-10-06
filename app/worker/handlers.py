import asyncio
import random
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


@dataclass
class JobContext:
    job_id: uuid.UUID
    report_progress: Callable[[int], Awaitable[Any]]


class PermanentJobError(Exception):
    """Failure that retrying can't fix (e.g., invalid payload) -> FAILED immediately."""


class SimulatedFailure(Exception):
    pass


async def email_job(payload: dict, ctx: JobContext) -> dict:
    await asyncio.sleep(random.uniform(1, 3))
    return {"message_id": f"msg_{uuid.uuid4().hex[:12]}", "to": payload.get("to")}


async def webhook_job(payload: dict, ctx: JobContext) -> dict:
    await asyncio.sleep(random.uniform(1, 2))
    # 20% random failure; "simulate_failure": true forces it (for demos/tests)
    if payload.get("simulate_failure") or random.random() < 0.2:
        raise SimulatedFailure("Webhook endpoint returned HTTP 503 (simulated)")
    return {"status_code": 200, "url": payload.get("url")}


async def report_job(payload: dict, ctx: JobContext) -> dict:
    await asyncio.sleep(random.uniform(3, 5))
    return {"file_url": f"https://storage.example.com/reports/{ctx.job_id}.pdf"}


async def batch_job(payload: dict, ctx: JobContext) -> dict:
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise PermanentJobError("payload.items must be a non-empty list")
    if len(items) > 200:  # 200 * 0.5s = 100s, safely under the 120s batch timeout
        raise PermanentJobError("Batch too large: max 200 items")
    for i, _item in enumerate(items, start=1):
        await asyncio.sleep(0.5)
        await ctx.report_progress(int(i * 100 / len(items)))
    return {"total": len(items), "processed": len(items), "summary": f"Processed {len(items)} items"}


HANDLERS: dict[str, Callable[[dict, JobContext], Awaitable[dict]]] = {
    "email": email_job,
    "webhook": webhook_job,
    "report": report_job,
    "batch": batch_job,
}

JOB_TIMEOUTS: dict[str, float] = {"email": 10, "webhook": 10, "report": 15, "batch": 120}