import asyncio

from app.db import engine
from app.models import Base


async def main() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
    print("Database tables created")


if __name__ == "__main__":
    asyncio.run(main())