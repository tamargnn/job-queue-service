import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from app.models import JobStatus

JobTypeName = Literal["email", "webhook", "report", "batch"]


class JobCreate(BaseModel):
    job_type: JobTypeName
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=0, le=10, description="Higher = more urgent")
    max_attempts: int = Field(default=3, ge=1, le=10)
    # Must include a timezone, e.g. "2026-10-07T10:00:00Z" or "...+03:00"
    run_at: AwareDatetime | None = None


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)  # build from SQLAlchemy objects

    id: uuid.UUID
    job_type: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    attempts: int
    max_attempts: int
    progress: int
    result: dict[str, Any] | None
    error_message: str | None
    idempotency_key: str | None
    run_at: datetime
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class JobList(BaseModel):
    items: list[JobOut]
    limit: int
    offset: int