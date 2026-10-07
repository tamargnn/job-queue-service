import logging
import uuid
from typing import Any

from app.db import SessionLocal
from app.models import JobLog, LogLevel

log = logging.getLogger("job_log")


async def record_event(
    job_id: uuid.UUID, level: LogLevel, message: str, extra: dict[str, Any] | None = None
) -> None:
    """Persist a per-job audit event (what happened to this job, visible via the API).

    Best effort by design: it runs in its own transaction, and a failure here is logged
    and swallowed - an audit entry must never fail or retry the job itself.
    """
    try:
        async with SessionLocal() as session, session.begin():
            session.add(JobLog(job_id=job_id, level=level, message=message, extra=extra))
    except Exception:
        log.exception("Failed to write job log entry", extra={"job_id": str(job_id)})
