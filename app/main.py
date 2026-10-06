from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy import text

from app.config import settings
from app.db import engine
from app.api.routes import router as jobs_router

app = FastAPI(title="Job Queue Service")

app.include_router(jobs_router) 

@app.get("/health")
async def health() -> dict:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    redis = Redis.from_url(settings.redis_url)
    await redis.ping()
    await redis.aclose()
    return {"status": "ok", "db": "ok", "redis": "ok"}

