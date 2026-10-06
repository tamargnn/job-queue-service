import uuid
from datetime import datetime

from redis.asyncio import Redis

from app.config import settings

QUEUE_KEY = "jobs:pending"

redis_client = Redis.from_url(settings.redis_url, decode_responses=True)


def _score(priority: int, created_at: datetime) -> float:
    # Higher priority first; within the same priority, older jobs first (FIFO).
    # Based on created_at (not "now") so re-enqueueing keeps the original position.
    return priority * 10**13 - int(created_at.timestamp() * 1000)


async def enqueue(job_id: uuid.UUID, priority: int, created_at: datetime) -> None:
    # nx=True: if already queued, do nothing (don't reorder, no duplicates)
    await redis_client.zadd(QUEUE_KEY, {str(job_id): _score(priority, created_at)}, nx=True)


async def pop_job(timeout: int = 1) -> uuid.UUID | None:
    # Atomic blocking pop of the highest score - two workers never get the same item
    item = await redis_client.bzpopmax(QUEUE_KEY, timeout=timeout)
    return uuid.UUID(item[1]) if item else None  # item = (key, member, score)


async def remove(job_id: uuid.UUID) -> None:
    await redis_client.zrem(QUEUE_KEY, str(job_id))


async def queue_size() -> int:
    return await redis_client.zcard(QUEUE_KEY)