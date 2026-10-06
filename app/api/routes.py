import logging
from redis.exceptions import RedisError
from app.queue import enqueue

import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session
from app.models import Job, JobStatus
from app.schemas import JobCreate, JobList, JobOut

log = logging.getLogger("api")

router = APIRouter(prefix="/jobs", tags=["jobs"])

SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.post("", response_model=JobOut, status_code=status.HTTP_201_CREATED)
async def submit_job(
    body: JobCreate,
    response: Response,
    session: SessionDep,
    idempotency_key: Annotated[str | None, Header(max_length=255)] = None,
) -> Job:
    now = datetime.now(UTC)
    run_at = body.run_at or now
    initial_status = JobStatus.SCHEDULED if run_at > now else JobStatus.PENDING

    stmt = insert(Job).values(
        id=uuid.uuid4(),
        job_type=body.job_type,
        payload=body.payload,
        priority=body.priority,
        max_attempts=body.max_attempts,
        status=initial_status,
        run_at=run_at,
        idempotency_key=idempotency_key,
    )
    if idempotency_key:
        # Atomic duplicate prevention: the UNIQUE index decides, not our code.
        # NEVER replace this with "SELECT first, then INSERT" - that is a race condition.
        stmt = stmt.on_conflict_do_nothing(index_elements=[Job.idempotency_key])

    job = (await session.execute(stmt.returning(Job))).scalar_one_or_none()

    if job is None:
        # Conflict: a job with this key already exists -> return it, don't create a new one
        existing = await session.scalar(select(Job).where(Job.idempotency_key == idempotency_key))
        response.status_code = status.HTTP_200_OK
        return existing

    
    await session.commit()
    if job.status == JobStatus.PENDING:  # scheduled jobs are enqueued later by the scheduler
        try:
            await enqueue(job.id, job.priority, job.created_at)
        except RedisError:
            # Job is safely committed in the DB; the reconciler will enqueue it.
            log.warning("Enqueue failed, reconciler will retry", extra={"job_id": str(job.id)})
    
    return job


@router.get("/{job_id}", response_model=JobOut)
async def get_job(job_id: uuid.UUID, session: SessionDep) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("", response_model=JobList)
async def list_jobs(
    session: SessionDep,
    status_filter: Annotated[JobStatus | None, Query(alias="status")] = None,
    job_type: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict:
    query = select(Job).order_by(Job.created_at.desc()).limit(limit).offset(offset)
    if status_filter:
        query = query.where(Job.status == status_filter)
    if job_type:
        query = query.where(Job.job_type == job_type)
    jobs = (await session.scalars(query)).all()
    return {"items": jobs, "limit": limit, "offset": offset}