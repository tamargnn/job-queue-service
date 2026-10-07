from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import func, select

from app.api.routes import router as jobs_router
from app.db import SessionLocal
from app.logging_config import configure_logging
from app.models import Job
from app.queue import queue_size

configure_logging()

app = FastAPI(title="Job Queue Service")
app.include_router(jobs_router)


@app.get("/health")
async def health() -> JSONResponse:
    body: dict = {"status": "ok"}
    try:
        async with SessionLocal() as session:
            rows = (await session.execute(select(Job.status, func.count()).group_by(Job.status))).all()
        body["db"] = "ok"
        body["jobs_by_status"] = {str(status): count for status, count in rows}
    except Exception as e:
        body["status"], body["db"] = "degraded", f"error: {type(e).__name__}"
    try:
        body["queue_depth"] = await queue_size()
        body["redis"] = "ok"
    except Exception as e:
        body["status"], body["redis"] = "degraded", f"error: {type(e).__name__}"
    # 503 lets load balancers / docker healthchecks detect an unhealthy instance
    return JSONResponse(body, status_code=200 if body["status"] == "ok" else 503)