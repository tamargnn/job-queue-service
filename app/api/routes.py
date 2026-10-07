import logging
from redis.exceptions import RedisError
from app.queue import enqueue, remove

import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session
from app.models import CANCELLABLE_STATUSES, Job, JobStatus
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


async def _get_or_404(session: AsyncSession, job_id: uuid.UUID) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post("/{job_id}/cancel", response_model=JobOut)
async def cancel_job(job_id: uuid.UUID, session: SessionDep) -> Job:
    # Conditional update: races safely with a worker claiming the same job (row lock decides)
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status.in_(CANCELLABLE_STATUSES))
        .values(status=JobStatus.CANCELLED, completed_at=func.now())
        .returning(Job)
        .execution_options(synchronize_session=False)
    )
    job = (await session.execute(stmt)).scalar_one_or_none()
    if job is None:
        existing = await _get_or_404(session, job_id)
        raise HTTPException(status_code=409, detail=f"Cannot cancel job in status '{existing.status}'")
    await session.commit()
    try:
        await remove(job.id)  # cleanup only; if it fails, the worker's claim will skip it anyway
    except RedisError:
        log.warning("Failed to remove cancelled job from queue", extra={"job_id": str(job.id)})
    return job


@router.post("/{job_id}/retry", response_model=JobOut)
async def retry_job(job_id: uuid.UUID, session: SessionDep) -> Job:
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.FAILED)
        .values(
            status=JobStatus.PENDING, attempts=0, run_at=func.now(), progress=0,
            error_message=None, error_details=None, result=None,
            started_at=None, completed_at=None,
        )
        .returning(Job)
        .execution_options(synchronize_session=False)
    )
    job = (await session.execute(stmt)).scalar_one_or_none()
    if job is None:
        existing = await _get_or_404(session, job_id)
        raise HTTPException(status_code=409, detail=f"Only failed jobs can be retried (status: '{existing.status}')")
    await session.commit()
    try:
        await enqueue(job.id, job.priority, job.created_at)
    except RedisError:
        log.warning("Enqueue failed, reconciler will retry", extra={"job_id": str(job.id)})
    return job