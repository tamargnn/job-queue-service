import asyncio
import logging
import os
import signal
import socket
import traceback
import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import func, update

from app.config import settings
from app.db import SessionLocal, engine
from app.logging_config import configure_logging
from app.models import Job, JobStatus
from app.queue import pop_job, redis_client
from app.worker.handlers import HANDLERS, JOB_TIMEOUTS, JobContext, PermanentJobError
from app.worker.maintenance import maintenance_loop
from app.worker.retry import backoff_seconds

WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"
log = logging.getLogger("worker")


def _lease_expiry():
    return func.now() + timedelta(seconds=settings.lease_seconds)


async def claim(job_id: uuid.UUID) -> Job | None:
    """Atomic claim: succeeds only if the job is still PENDING. The DB decides, not Redis."""
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.PENDING)
        .values(
            status=JobStatus.PROCESSING,
            attempts=Job.attempts + 1,  # counted at claim time -> crashes count as attempts
            locked_by=WORKER_ID,
            lease_expires_at=_lease_expiry(),
            started_at=func.now(),
        )
        .returning(Job)
        .execution_options(synchronize_session=False)
    )
    async with SessionLocal() as session, session.begin():
        return (await session.execute(stmt)).scalar_one_or_none()


async def update_owned(job_id: uuid.UUID, **values: Any) -> bool:
    """Fencing: update only if THIS worker still owns the job.
    Returns False if the lease was lost (reaper gave the job to someone else)."""
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.locked_by == WORKER_ID, Job.status == JobStatus.PROCESSING)
        .values(**values)
        .returning(Job.id)
        .execution_options(synchronize_session=False)
    )
    async with SessionLocal() as session, session.begin():
        return (await session.execute(stmt)).scalar_one_or_none() is not None


async def fail(job: Job, error: str, details: dict | None, retryable: bool = True) -> None:
    released = {"locked_by": None, "lease_expires_at": None,
                "error_message": error, "error_details": details}
    if retryable and job.attempts < job.max_attempts:
        delay = backoff_seconds(job.attempts)
        ok = await update_owned(job.id, **released, status=JobStatus.SCHEDULED,
                                run_at=func.now() + timedelta(seconds=delay))
        log.warning(f"Job failed, retry in {delay:.0f}s: {error}",
                    extra={"job_id": str(job.id), "attempt": job.attempts})
    else:
        ok = await update_owned(job.id, **released, status=JobStatus.FAILED, completed_at=func.now())
        log.error(f"Job failed permanently: {error}",
                  extra={"job_id": str(job.id), "attempt": job.attempts})
    if not ok:
        log.warning("Lease lost before failure could be recorded", extra={"job_id": str(job.id)})


async def heartbeat(job_id: uuid.UUID, work: asyncio.Task, lease_lost: asyncio.Event) -> None:
    while True:
        await asyncio.sleep(settings.heartbeat_seconds)
        try:
            if not await update_owned(job_id, lease_expires_at=_lease_expiry()):
                lease_lost.set()
                work.cancel()  # someone else owns the job now - stop working on it
                return
        except Exception:
            log.exception("Heartbeat failed (will retry)", extra={"job_id": str(job_id)})


async def process(job_id: uuid.UUID) -> None:
    job = await claim(job_id)
    if job is None:
        log.info("Job not claimable (cancelled or taken), skipping", extra={"job_id": str(job_id)})
        return

    ctx_log = {"job_id": str(job.id), "job_type": job.job_type,
               "attempt": job.attempts, "worker_id": WORKER_ID}
    log.info("Job started", extra=ctx_log)

    handler = HANDLERS.get(job.job_type)
    if handler is None:
        await fail(job, f"Unknown job type: {job.job_type}", None, retryable=False)
        return

    ctx = JobContext(job_id=job.id, report_progress=lambda p: update_owned(job.id, progress=p))
    timeout = JOB_TIMEOUTS.get(job.job_type, 60)
    work = asyncio.create_task(asyncio.wait_for(handler(job.payload, ctx), timeout))
    lease_lost = asyncio.Event()
    hb = asyncio.create_task(heartbeat(job.id, work, lease_lost))

    try:
        result = await work
        ok = await update_owned(job.id, status=JobStatus.COMPLETED, result=result, progress=100,
                                completed_at=func.now(), locked_by=None, lease_expires_at=None)
        if ok:
            log.info("Job completed", extra=ctx_log)
        else:
            log.warning("Lease lost before completion; result discarded", extra=ctx_log)
    except asyncio.CancelledError:
        if not lease_lost.is_set():
            raise
        log.warning("Lease lost during execution; job abandoned", extra=ctx_log)
    except TimeoutError:
        await fail(job, f"Job timed out after {timeout}s", {"type": "TimeoutError"})
    except PermanentJobError as e:
        await fail(job, str(e), {"type": "PermanentJobError"}, retryable=False)
    except Exception as e:
        await fail(job, str(e) or type(e).__name__,
                   {"type": type(e).__name__, "traceback": traceback.format_exc()})
    finally:
        hb.cancel()


async def main() -> None:
    configure_logging()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)  # graceful: stop taking NEW jobs

    log.info("Worker started", extra={"worker_id": WORKER_ID})
    maintenance = asyncio.create_task(maintenance_loop(stop))

    while not stop.is_set():
        try:
            job_id = await pop_job(timeout=1)  # 1s timeout keeps us responsive to shutdown
        except Exception:
            log.exception("Queue error")
            await asyncio.sleep(1)
            continue
        if job_id is None:
            continue
        try:
            await process(job_id)  # current job always finishes before we check `stop` again
        except Exception:
            log.exception("Unexpected error processing job", extra={"job_id": str(job_id)})

    log.info("Shutdown: current job finished, exiting", extra={"worker_id": WORKER_ID})
    await maintenance
    await redis_client.aclose()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())