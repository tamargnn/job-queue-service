import asyncio
import logging
from datetime import timedelta

from sqlalchemy import func, select, update

from app.config import settings
from app.db import SessionLocal
from app.job_log import record_event
from app.models import Job, JobStatus, LogLevel
from app.queue import enqueue

log = logging.getLogger("maintenance")
BATCH = 100


async def _update_batch(where: tuple, values: dict) -> list:
    """Update up to BATCH matching rows. SKIP LOCKED: if several workers run maintenance
    at once, each grabs different rows instead of blocking or double-processing."""
    ids = select(Job.id).where(*where).limit(BATCH).with_for_update(skip_locked=True)
    stmt = (
        update(Job)
        .where(Job.id.in_(ids), *where)
        .values(**values)
        .returning(Job.id, Job.priority, Job.created_at)
        .execution_options(synchronize_session=False)
    )
    async with SessionLocal() as session, session.begin():
        return (await session.execute(stmt)).all()


async def promote_due_jobs() -> None:
    """SCHEDULED -> PENDING when run_at arrives (user-scheduled jobs AND retry backoff)."""
    rows = await _update_batch(
        (Job.status == JobStatus.SCHEDULED, Job.run_at <= func.now()),
        {"status": JobStatus.PENDING},
    )
    for job_id, priority, created_at in rows:
        await enqueue(job_id, priority, created_at)
    if rows:
        log.info(f"Promoted {len(rows)} scheduled jobs")


async def reap_expired_leases() -> None:
    """Recover jobs whose worker died (lease expired without heartbeat)."""
    expired = (Job.status == JobStatus.PROCESSING, Job.lease_expires_at < func.now())
    released = {"locked_by": None, "lease_expires_at": None}

    # Poison protection: attempts are counted at claim time, so a job that keeps
    # crashing workers eventually runs out of attempts and is marked FAILED.
    dead = await _update_batch(
        (*expired, Job.attempts >= Job.max_attempts),
        {**released, "status": JobStatus.FAILED, "completed_at": func.now(),
         "error_message": "Worker lost (lease expired); max attempts reached"},
    )
    retry = await _update_batch(
        (*expired, Job.attempts < Job.max_attempts),
        {**released, "status": JobStatus.PENDING,
         "error_message": "Worker lost (lease expired); re-queued"},
    )
    for job_id, _priority, _created_at in dead:
        await record_event(job_id, LogLevel.ERROR, "Worker lost (lease expired); max attempts reached, job failed")
    for job_id, priority, created_at in retry:
        await record_event(job_id, LogLevel.WARNING, "Worker lost (lease expired); job re-queued")
        await enqueue(job_id, priority, created_at)
    if dead or retry:
        log.warning(f"Reaper: {len(retry)} re-queued, {len(dead)} failed permanently")


async def reconcile_queue() -> None:
    """Safety net for the DB/Redis dual-write: PENDING in DB but missing from Redis
    (API crashed after commit, Redis restarted, worker died between pop and claim)."""
    stmt = (
        select(Job.id, Job.priority, Job.created_at)
        .where(Job.status == JobStatus.PENDING, Job.updated_at < func.now() - timedelta(seconds=30))
        .limit(1000)
    )
    async with SessionLocal() as session:
        rows = (await session.execute(stmt)).all()
    for job_id, priority, created_at in rows:
        await enqueue(job_id, priority, created_at)  # NX: no-op if already queued


async def maintenance_loop(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    last_reconcile = 0.0
    while not stop.is_set():
        try:
            await promote_due_jobs()
            await reap_expired_leases()
            if loop.time() - last_reconcile >= settings.reconcile_interval_seconds:
                await reconcile_queue()
                last_reconcile = loop.time()
        except Exception:
            log.exception("Maintenance cycle failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.maintenance_interval_seconds)
        except TimeoutError:
            pass