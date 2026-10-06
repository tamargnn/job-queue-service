from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------- Enums ----------

class JobStatus(StrEnum):
    SCHEDULED = "scheduled"    # waiting for run_at (user-scheduled OR retry backoff)
    PENDING = "pending"        # ready to be picked up
    PROCESSING = "processing"  # claimed by a worker (has an active lease)
    COMPLETED = "completed"
    FAILED = "failed"          # permanently failed (attempts exhausted)
    CANCELLED = "cancelled"


TERMINAL_STATUSES = {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
CANCELLABLE_STATUSES = {JobStatus.PENDING, JobStatus.SCHEDULED}


class LogLevel(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


def _str_enum(enum_cls: type[StrEnum], name: str) -> SAEnum:
    """VARCHAR + CHECK constraint, storing the enum VALUE ('pending'), not NAME ('PENDING')."""
    return SAEnum(
        enum_cls,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=20,
        values_callable=lambda e: [m.value for m in e],
        validate_strings=True,
    )


# ---------- Job ----------

class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)

    # What to run
    job_type: Mapped[str] = mapped_column(String(50), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )

    # State
    status: Mapped[JobStatus] = mapped_column(
        _str_enum(JobStatus, "job_status"), nullable=False, default=JobStatus.PENDING
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")

    # Retries
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3, server_default="3")

    # Scheduling: "do not run before". Used for user scheduling AND retry backoff.
    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Lease (crash recovery): who owns the job and until when
    locked_by: Mapped[str | None] = mapped_column(String(100))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Output
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    error_message: Mapped[str | None] = mapped_column(Text)
    error_details: Mapped[dict[str, Any] | None] = mapped_column(JSONB)  # type, traceback, attempt

    # Duplicate prevention (DB-enforced, see API step for ON CONFLICT usage)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True)

    # Timestamps (always timezone-aware, UTC)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))  # any terminal state

    # lazy="raise": in async SQLAlchemy, implicit lazy loading crashes (MissingGreenlet).
    # Forcing explicit loading (selectinload) makes that bug impossible to miss.
    logs: Mapped[list[JobLog]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="JobLog.created_at",
        lazy="raise",
    )

    __table_args__ = (
        CheckConstraint("priority BETWEEN 0 AND 10", name="ck_jobs_priority_range"),
        CheckConstraint("progress BETWEEN 0 AND 100", name="ck_jobs_progress_range"),
        CheckConstraint("attempts >= 0 AND max_attempts >= 1", name="ck_jobs_attempts"),
        # List endpoint: filter by status/type, newest first
        Index("ix_jobs_status_type_created", "status", "job_type", "created_at"),
        # Scheduler + reconciler: find jobs that are due
        Index(
            "ix_jobs_due",
            "run_at",
            postgresql_where=text("status IN ('scheduled', 'pending')"),
        ),
        # Reaper: find expired leases
        Index(
            "ix_jobs_expired_leases",
            "lease_expires_at",
            postgresql_where=text("status = 'processing'"),
        ),
    )


# ---------- JobLog ----------

class JobLog(Base):
    __tablename__ = "job_logs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    level: Mapped[LogLevel] = mapped_column(_str_enum(LogLevel, "log_level"), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    # Python attribute can't be named `metadata`: it's reserved by SQLAlchemy's Base.
    # The DB column is still called "metadata".
    extra: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    job: Mapped[Job] = relationship(back_populates="logs")