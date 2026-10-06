from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,  # detect dead connections (e.g., after Postgres restart)
)

# expire_on_commit=False: by default SQLAlchemy "expires" objects after commit,
# so reading job.status afterwards triggers a lazy DB reload -> crashes in async
# (MissingGreenlet). Keeping attributes loaded avoids that.
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)