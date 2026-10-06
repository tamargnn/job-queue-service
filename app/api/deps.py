from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession

from app.db import SessionLocal


async def get_session() -> AsyncIterator[AsyncSession]:
    # One session per HTTP request, closed automatically afterwards
    async with SessionLocal() as session:
        yield session