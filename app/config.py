from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Defaults are for running locally (outside Docker).
    # Inside Docker, the env vars from docker-compose override these.
    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/job_queue"
    redis_url: str = "redis://localhost:6379/0"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()