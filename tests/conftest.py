import os

# Must run BEFORE importing app modules: settings and the DB engine are created at import time.
# Separate DB and Redis DB index, so running workers never touch test data.
os.environ["DATABASE_URL"] = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://user:password@postgres:5432/job_queue_test"
)
os.environ["REDIS_URL"] = os.environ.get("TEST_REDIS_URL", "redis://redis:6379/15")

import asyncpg  # noqa: E402
import pytest_asyncio  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import text  # noqa: E402

ADMIN_DSN = "postgresql://user:password@postgres:5432/postgres"


async def _create_test_database() -> None:
    conn = await asyncpg.connect(ADMIN_DSN)
    try:
        if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = 'job_queue_test'"):
            await conn.execute("CREATE DATABASE job_queue_test")
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="session", loop_scope="session", autouse=True)
async def setup_database():
    await _create_test_database()
    from app.db import engine
    from app.models import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)  # fresh schema every test run
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_state():
    """Each test starts with an empty DB and an empty queue."""
    from app.db import engine
    from app.queue import redis_client

    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE jobs, job_logs CASCADE"))
    await redis_client.flushdb()
    yield


@pytest_asyncio.fixture(loop_scope="session")
async def client():
    from app.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c