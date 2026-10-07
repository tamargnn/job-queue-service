"""
Integration tests for the job queue.

Run (fully automated, no manual steps):
    docker compose run --rm api pytest -v

Tests run against real Postgres and Redis, isolated from the running system
(separate DB 'job_queue_test' and Redis DB 15 - see conftest.py).
Worker functions are called directly, and time is "fast-forwarded" by editing
run_at / lease_expires_at instead of sleeping.
"""
import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select, update

from app.config import settings
from app.db import SessionLocal
from app.models import Job, JobStatus
from app.queue import enqueue, pop_job, queue_size, redis_client
from app.worker import handlers
from app.worker.main import claim, process, update_owned
from app.worker.maintenance import promote_due_jobs, reap_expired_leases, reconcile_queue
from app.worker.retry import backoff_seconds

pytestmark = pytest.mark.asyncio(loop_scope="session")


# ======================================================================
# Helpers
# ======================================================================

def future_iso(**delta) -> str:
    return (datetime.now(UTC) + timedelta(**delta)).isoformat()


async def submit(client, **body) -> dict:
    r = await client.post("/jobs", json=body)
    assert r.status_code == 201, r.text
    return r.json()


async def submit_id(client, **body) -> uuid.UUID:
    return uuid.UUID((await submit(client, **body))["id"])


async def get_db_job(job_id) -> Job:
    async with SessionLocal() as session:
        return await session.get(Job, uuid.UUID(str(job_id)))


async def set_fields(job_id, **values) -> None:
    async with SessionLocal() as session, session.begin():
        await session.execute(update(Job).where(Job.id == uuid.UUID(str(job_id))).values(**values))


async def count_jobs() -> int:
    async with SessionLocal() as session:
        return await session.scalar(select(func.count()).select_from(Job))


async def make_failed_job(client) -> uuid.UUID:
    """A webhook job that always fails, with a single attempt -> FAILED after one run."""
    job_id = await submit_id(client, job_type="webhook", payload={"simulate_failure": True}, max_attempts=1)
    assert await pop_job() == job_id
    await process(job_id)
    assert (await get_db_job(job_id)).status == JobStatus.FAILED
    return job_id


# ======================================================================
# TEST 1 - Submit and retrieve a job
# Job:   report job
# Tests: POST returns 201 with correct defaults; GET returns the same job;
#        unknown ID returns 404
# ======================================================================
async def test_01_submit_and_retrieve_job(client):
    job = await submit(client, job_type="report", payload={"month": "2026-09"}, priority=3)
    assert job["status"] == "pending"
    assert job["priority"] == 3
    assert job["attempts"] == 0 and job["max_attempts"] == 3
    assert job["progress"] == 0 and job["result"] is None

    r = await client.get(f"/jobs/{job['id']}")
    assert r.status_code == 200
    assert r.json()["id"] == job["id"]
    assert r.json()["payload"] == {"month": "2026-09"}

    assert (await client.get(f"/jobs/{uuid.uuid4()}")).status_code == 404
    assert (await client.get("/jobs/not-a-uuid")).status_code == 422


# ======================================================================
# TEST 2 - Invalid submissions are rejected (8 cases)
# Job:   various malformed requests
# Tests: every invalid input returns 422 and nothing is written to the DB
# ======================================================================
@pytest.mark.parametrize(
    "body, headers",
    [
        ({"payload": {}}, None),
        ({"job_type": "unknown"}, None),
        ({"job_type": "email", "priority": 11}, None),
        ({"job_type": "email", "priority": -1}, None),
        ({"job_type": "email", "max_attempts": 0}, None),
        ({"job_type": "email", "run_at": "2030-01-01T10:00:00"}, None),
        ({"job_type": "email", "payload": "not-an-object"}, None),
        ({"job_type": "email"}, {"Idempotency-Key": "x" * 256}),
    ],
    ids=[
        "missing_job_type", "unknown_job_type", "priority_too_high", "priority_negative",
        "zero_max_attempts", "run_at_without_timezone", "payload_not_object", "idempotency_key_too_long",
    ],
)
async def test_02_invalid_submission_rejected(client, body, headers):
    r = await client.post("/jobs", json=body, headers=headers or {})
    assert r.status_code == 422
    assert await count_jobs() == 0
    assert await queue_size() == 0


# ======================================================================
# TEST 3 - Initial status depends on run_at
# Job:   email jobs with future / past / no run_at
# Tests: future -> SCHEDULED and NOT queued; past or none -> PENDING and queued
# ======================================================================
async def test_03_initial_status_depends_on_run_at(client):
    scheduled = await submit(client, job_type="email", run_at=future_iso(hours=1))
    assert scheduled["status"] == "scheduled"
    assert await queue_size() == 0  # scheduled jobs wait in the DB, not in the queue

    past = await submit(client, job_type="email", run_at=future_iso(minutes=-5))
    assert past["status"] == "pending"

    no_run_at = await submit(client, job_type="email")
    assert no_run_at["status"] == "pending"
    assert await queue_size() == 2


# ======================================================================
# TEST 4 - List jobs: filters and pagination
# Job:   3 email + 1 report + 1 scheduled email
# Tests: filter by status, by type, both combined; limit/offset paging;
#        newest first; invalid query params rejected
# ======================================================================
async def test_04_list_filters_and_pagination(client):
    for _ in range(3):
        await submit(client, job_type="email")
    report = await submit(client, job_type="report")
    scheduled = await submit(client, job_type="email", run_at=future_iso(hours=1))

    async def list_ids(**params) -> list[str]:
        r = await client.get("/jobs", params=params)
        assert r.status_code == 200
        return [j["id"] for j in r.json()["items"]]

    assert len(await list_ids(job_type="email")) == 4
    assert await list_ids(job_type="report") == [report["id"]]
    assert await list_ids(status="scheduled", job_type="email") == [scheduled["id"]]
    assert await list_ids(status="completed") == []

    page1 = await list_ids(limit=2, offset=0)
    page2 = await list_ids(limit=2, offset=2)
    page3 = await list_ids(limit=2, offset=4)
    assert page1[0] == scheduled["id"]  # newest first
    assert len(page1) == 2 and len(page2) == 2 and len(page3) == 1
    assert len(set(page1 + page2 + page3)) == 5  # no overlap between pages

    for bad in ({"limit": 0}, {"limit": 201}, {"offset": -1}, {"status": "bogus"}):
        assert (await client.get("/jobs", params=bad)).status_code == 422


# ======================================================================
# TEST 5 - Job completion flow
# Job:   email job (simulated send, 1-3s)
# Tests: worker takes the job from the queue, runs it, stores the result
#        and releases the lease
# ======================================================================
async def test_05_job_completion_flow(client):
    job_id = await submit_id(client, job_type="email", payload={"to": "a@example.com"})

    assert await pop_job() == job_id
    await process(job_id)

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.COMPLETED
    assert db_job.result["message_id"].startswith("msg_")
    assert db_job.result["to"] == "a@example.com"
    assert db_job.progress == 100 and db_job.attempts == 1
    assert db_job.started_at is not None and db_job.completed_at is not None
    assert db_job.locked_by is None and db_job.lease_expires_at is None
    assert await queue_size() == 0


# ======================================================================
# TEST 6 - Batch job reports progress while running
# Job:   batch job with 4 items (0.5s each)
# Tests: intermediate progress (25/50/75) is visible during execution,
#        final progress is 100 and the summary is stored
# ======================================================================
async def test_06_batch_job_reports_progress(client):
    job_id = await submit_id(client, job_type="batch", payload={"items": [1, 2, 3, 4]})
    assert await pop_job() == job_id

    task = asyncio.create_task(process(job_id))
    seen = set()
    while not task.done():
        seen.add((await get_db_job(job_id)).progress)
        await asyncio.sleep(0.1)
    await task

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.COMPLETED
    assert db_job.progress == 100
    assert db_job.result == {"total": 4, "processed": 4, "summary": "Processed 4 items"}
    assert seen & {25, 50, 75}, f"no intermediate progress observed: {seen}"


# ======================================================================
# TEST 7 - Report and webhook jobs return their results
# Job:   report job (3-5s) + webhook job (random failure disabled)
# Tests: each job type produces its expected result shape
# ======================================================================
async def test_07_report_and_webhook_results(client, monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.99)  # disable the random 20% failure

    report_id = await submit_id(client, job_type="report")
    webhook_id = await submit_id(client, job_type="webhook", payload={"url": "https://example.com/hook"})
    for _ in range(2):
        await process(await pop_job())

    report = await get_db_job(report_id)
    assert report.status == JobStatus.COMPLETED
    assert report.result["file_url"].endswith(f"{report_id}.pdf")

    webhook = await get_db_job(webhook_id)
    assert webhook.status == JobStatus.COMPLETED
    assert webhook.result == {"status_code": 200, "url": "https://example.com/hook"}


# ======================================================================
# TEST 8 - Failure, retries with backoff, then permanent failure
# Job:   webhook job forced to fail on every attempt (3 attempts)
# Tests: attempt 1 fails -> retry in ~30s; attempt 2 fails -> retry in ~120s;
#        attempt 3 fails -> FAILED permanently, nothing left in the queue
# ======================================================================
async def test_08_failure_retries_with_backoff_then_fails_permanently(client):
    job_id = await submit_id(client, job_type="webhook", payload={"simulate_failure": True})
    expected_delay = {1: 30, 2: 120}

    for attempt in (1, 2):
        assert await pop_job() == job_id
        await process(job_id)

        db_job = await get_db_job(job_id)
        assert db_job.status == JobStatus.SCHEDULED
        assert db_job.attempts == attempt
        assert "503" in db_job.error_message
        assert db_job.error_details["type"] == "SimulatedFailure"
        remaining = (db_job.run_at - datetime.now(UTC)).total_seconds()
        assert expected_delay[attempt] * 0.85 < remaining < expected_delay[attempt] * 1.15

        # Fast-forward time instead of sleeping: make the job due, then run the scheduler
        await set_fields(job_id, run_at=func.now())
        await promote_due_jobs()
        assert (await get_db_job(job_id)).status == JobStatus.PENDING

    assert await pop_job() == job_id
    await process(job_id)

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.FAILED
    assert db_job.attempts == 3
    assert db_job.completed_at is not None
    assert await queue_size() == 0


# ======================================================================
# TEST 9 - Backoff delay calculation
# Job:   none (pure function)
# Tests: ~30s after attempt 1, ~120s after attempt 2, jitter stays within +-10%
# ======================================================================
async def test_09_backoff_delays():
    for _ in range(100):  # jitter is random, so sample many times
        assert 27 <= backoff_seconds(1) <= 33
        assert 108 <= backoff_seconds(2) <= 132


# ======================================================================
# TEST 10 - Transient failure, then success on retry
# Job:   webhook job whose handler fails once, then succeeds
# Tests: job is retried and ends COMPLETED with attempts=2
# ======================================================================
async def test_10_transient_failure_then_success(client, monkeypatch):
    calls = {"n": 0}

    async def flaky(payload, ctx):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("temporary network error")
        return {"ok": True}

    monkeypatch.setitem(handlers.HANDLERS, "webhook", flaky)
    job_id = await submit_id(client, job_type="webhook")

    await process(await pop_job())
    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.SCHEDULED and db_job.attempts == 1
    assert db_job.error_message == "temporary network error"

    await set_fields(job_id, run_at=func.now())
    await promote_due_jobs()
    await process(await pop_job())

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.COMPLETED
    assert db_job.attempts == 2
    assert db_job.result == {"ok": True}


# ======================================================================
# TEST 11 - Permanent errors are not retried (3 cases)
# Job:   batch job with an invalid payload
# Tests: invalid input fails immediately after 1 attempt, even though
#        attempts remain - retrying can't fix bad data
# ======================================================================
@pytest.mark.parametrize(
    "items", [[], "not-a-list", list(range(201))], ids=["empty", "not_a_list", "too_many_items"]
)
async def test_11_permanent_error_is_not_retried(client, items):
    job_id = await submit_id(client, job_type="batch", payload={"items": items})
    await process(await pop_job())

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.FAILED
    assert db_job.attempts == 1 and db_job.max_attempts == 3
    assert db_job.error_details["type"] == "PermanentJobError"
    assert db_job.completed_at is not None
    assert await queue_size() == 0


# ======================================================================
# TEST 12 - Job timeout is enforced
# Job:   email job whose handler hangs forever (timeout set to 0.3s)
# Tests: the worker stops waiting after the timeout and schedules a retry
# ======================================================================
async def test_12_job_timeout_is_enforced(client, monkeypatch):
    async def hangs(payload, ctx):
        await asyncio.sleep(60)

    monkeypatch.setitem(handlers.HANDLERS, "email", hangs)
    monkeypatch.setitem(handlers.JOB_TIMEOUTS, "email", 0.3)
    job_id = await submit_id(client, job_type="email")

    loop = asyncio.get_running_loop()
    start = loop.time()
    await process(await pop_job())
    assert loop.time() - start < 5  # did not wait for the 60s handler

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.SCHEDULED  # a timeout is retryable
    assert "timed out" in db_job.error_message
    assert db_job.error_details["type"] == "TimeoutError"


# ======================================================================
# TEST 13 - max_attempts=1 fails permanently on the first failure
# Job:   webhook job forced to fail, max_attempts=1
# Tests: no retry is scheduled
# ======================================================================
async def test_13_single_attempt_job_fails_immediately(client):
    job_id = await make_failed_job(client)
    db_job = await get_db_job(job_id)
    assert db_job.attempts == 1
    assert db_job.run_at <= datetime.now(UTC)  # no future retry was scheduled
    assert await queue_size() == 0


# ======================================================================
# TEST 14 - Unknown job type found in the DB
# Job:   job inserted directly into the DB with an unregistered type
#        (e.g., a type removed from the code while jobs were still queued)
# Tests: the worker fails it permanently instead of crashing or retrying
# ======================================================================
async def test_14_unknown_job_type_fails_permanently():
    async with SessionLocal() as session, session.begin():
        job = Job(job_type="legacy_type", payload={}, status=JobStatus.PENDING)
        session.add(job)
    await process(job.id)

    db_job = await get_db_job(job.id)
    assert db_job.status == JobStatus.FAILED
    assert db_job.attempts == 1
    assert "Unknown job type" in db_job.error_message


# ======================================================================
# TEST 15 - Manual retry of a failed job
# Job:   failed webhook job + completed email job
# Tests: retry resets the job and re-queues it; only FAILED jobs can be
#        retried (409 otherwise); unknown ID returns 404
# ======================================================================
async def test_15_manual_retry_of_failed_job(client):
    done_id = await submit_id(client, job_type="email")
    await process(await pop_job())
    assert (await client.post(f"/jobs/{done_id}/retry")).status_code == 409
    assert (await client.post(f"/jobs/{uuid.uuid4()}/retry")).status_code == 404

    failed_id = await make_failed_job(client)
    r = await client.post(f"/jobs/{failed_id}/retry")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "pending"
    assert data["attempts"] == 0 and data["progress"] == 0
    assert data["error_message"] is None and data["result"] is None
    assert data["completed_at"] is None

    assert (await client.post(f"/jobs/{failed_id}/retry")).status_code == 409  # now pending
    assert await queue_size() == 1
    assert await pop_job() == failed_id


# ======================================================================
# TEST 16 - Cancel a scheduled job
# Job:   email job scheduled 1 hour ahead
# Tests: cancel succeeds; cancelling again returns 409; when its time
#        arrives, the scheduler does NOT bring it back
# ======================================================================
async def test_16_cancel_scheduled_job(client):
    job_id = await submit_id(client, job_type="email", run_at=future_iso(hours=1))

    r = await client.post(f"/jobs/{job_id}/cancel")
    assert r.status_code == 200
    assert r.json()["status"] == "cancelled" and r.json()["completed_at"] is not None

    r = await client.post(f"/jobs/{job_id}/cancel")
    assert r.status_code == 409 and "cancelled" in r.json()["detail"]

    await set_fields(job_id, run_at=func.now())
    await promote_due_jobs()
    assert (await get_db_job(job_id)).status == JobStatus.CANCELLED
    assert await queue_size() == 0


# ======================================================================
# TEST 17 - Cancel a pending job before a worker picks it up
# Job:   email job waiting in the queue
# Tests: it is removed from the queue, can't be claimed, and even a stale
#        copy of its ID left in Redis is skipped by the worker
# ======================================================================
async def test_17_cancel_pending_job(client):
    job_id = await submit_id(client, job_type="email")
    assert await queue_size() == 1

    assert (await client.post(f"/jobs/{job_id}/cancel")).status_code == 200
    assert await queue_size() == 0
    assert await claim(job_id) is None

    # Redis is only a hint: re-insert the ID and let the worker try anyway
    db_job = await get_db_job(job_id)
    await enqueue(db_job.id, db_job.priority, db_job.created_at)
    await process(await pop_job())

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.CANCELLED
    assert db_job.attempts == 0  # never ran


# ======================================================================
# TEST 18 - Running and finished jobs can't be cancelled
# Job:   processing / completed / failed jobs
# Tests: cancel returns 409 and the state is unchanged; unknown ID -> 404
# ======================================================================
async def test_18_cannot_cancel_running_or_finished_jobs(client):
    running_id = await submit_id(client, job_type="email")
    assert await pop_job() == running_id
    await claim(running_id)
    assert (await client.post(f"/jobs/{running_id}/cancel")).status_code == 409
    assert (await get_db_job(running_id)).status == JobStatus.PROCESSING

    done_id = await submit_id(client, job_type="email")
    assert await pop_job() == done_id
    await process(done_id)
    assert (await client.post(f"/jobs/{done_id}/cancel")).status_code == 409
    assert (await get_db_job(done_id)).status == JobStatus.COMPLETED

    failed_id = await make_failed_job(client)
    assert (await client.post(f"/jobs/{failed_id}/cancel")).status_code == 409

    assert (await client.post(f"/jobs/{uuid.uuid4()}/cancel")).status_code == 404


# ======================================================================
# TEST 19 - Race: cancel and worker claim at the exact same moment
# Job:   20 email jobs, each cancelled and claimed concurrently
# Tests: every time exactly one side wins, and the DB state matches
#        the winner (never both, never neither)
# ======================================================================
async def test_19_cancel_and_claim_race_has_exactly_one_winner(client):
    outcomes = set()
    for _ in range(20):
        job_id = await submit_id(client, job_type="email")
        cancel_resp, claimed = await asyncio.gather(
            client.post(f"/jobs/{job_id}/cancel"), claim(job_id)
        )
        db_job = await get_db_job(job_id)
        if cancel_resp.status_code == 200:
            assert claimed is None
            assert db_job.status == JobStatus.CANCELLED
            outcomes.add("cancel_won")
        else:
            assert cancel_resp.status_code == 409
            assert claimed is not None
            assert db_job.status == JobStatus.PROCESSING
            outcomes.add("claim_won")
    assert outcomes  # at least one round ran


# ======================================================================
# TEST 20 - Idempotency under concurrent requests
# Job:   10 identical email submissions with the same Idempotency-Key,
#        sent at the same time
# Tests: exactly one job is created and queued; one 201, nine 200;
#        a different key creates a different job
# ======================================================================
async def test_20_idempotency_under_concurrent_requests(client):
    body = {"job_type": "email", "payload": {"to": "a@example.com"}}
    headers = {"Idempotency-Key": "order-123"}

    responses = await asyncio.gather(
        *(client.post("/jobs", json=body, headers=headers) for _ in range(10))
    )
    assert len({r.json()["id"] for r in responses}) == 1
    assert sorted(r.status_code for r in responses) == [200] * 9 + [201]
    assert await count_jobs() == 1
    assert await queue_size() == 1

    other = await client.post("/jobs", json=body, headers={"Idempotency-Key": "order-456"})
    assert other.status_code == 201
    assert other.json()["id"] != responses[0].json()["id"]


# ======================================================================
# TEST 21 - Resubmitting after the job finished returns the original
# Job:   email job with an Idempotency-Key, completed, then resubmitted
#        with the same key and a DIFFERENT payload
# Tests: returns 200 with the original completed job; the job is NOT
#        run again and the original payload is kept
# ======================================================================
async def test_21_idempotent_resubmit_does_not_rerun_job(client):
    headers = {"Idempotency-Key": "welcome-email-42"}
    first = await client.post("/jobs", json={"job_type": "email", "payload": {"to": "a@example.com"}}, headers=headers)
    assert first.status_code == 201
    await process(await pop_job())

    second = await client.post("/jobs", json={"job_type": "email", "payload": {"to": "b@example.com"}}, headers=headers)
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["status"] == "completed"
    assert second.json()["payload"] == {"to": "a@example.com"}
    assert await queue_size() == 0
    assert await count_jobs() == 1


# ======================================================================
# TEST 22 - Requests without an Idempotency-Key are never deduplicated
# Job:   two identical email submissions without a key
# Tests: two separate jobs are created (identical requests may be
#        intentional, e.g., the same daily report)
# ======================================================================
async def test_22_no_idempotency_key_creates_separate_jobs(client):
    body = {"job_type": "email", "payload": {"to": "a@example.com"}}
    first = await submit(client, **body)
    second = await submit(client, **body)
    assert first["id"] != second["id"]
    assert await count_jobs() == 2
    assert await queue_size() == 2


# ======================================================================
# TEST 23 - Priority ordering with FIFO tie-break
# Job:   4 email jobs: priority 1, 9, 5, 5
# Tests: higher priority first; same priority -> oldest first
# ======================================================================
async def test_23_priority_ordering(client):
    ids = {}
    for name, priority in [("low", 1), ("high", 9), ("mid_first", 5), ("mid_second", 5)]:
        ids[name] = await submit_id(client, job_type="email", priority=priority)
        await asyncio.sleep(0.01)  # distinct created_at for the FIFO tie-break

    order = [await pop_job() for _ in range(4)]
    assert order == [ids["high"], ids["mid_first"], ids["mid_second"], ids["low"]]


# ======================================================================
# TEST 24 - Scheduled jobs don't run before their time
# Job:   2 email jobs scheduled 1 hour ahead (priority 1 and 9)
# Tests: the scheduler leaves them alone until due; once due they are
#        queued, and priority is still respected
# ======================================================================
async def test_24_scheduled_jobs_wait_until_due(client):
    low = await submit_id(client, job_type="email", priority=1, run_at=future_iso(hours=1))
    high = await submit_id(client, job_type="email", priority=9, run_at=future_iso(hours=1))

    await promote_due_jobs()
    assert (await get_db_job(low)).status == JobStatus.SCHEDULED
    assert await queue_size() == 0

    for job_id in (low, high):
        await set_fields(job_id, run_at=func.now())
    await promote_due_jobs()

    assert (await get_db_job(low)).status == JobStatus.PENDING
    assert await pop_job() == high
    assert await pop_job() == low


# ======================================================================
# TEST 25 - Duplicate pickup: 10 concurrent claims of the same job
# Job:   one email job
# Tests: exactly one claim succeeds and attempts is incremented once -
#        the DB conditional update is the real guard, not Redis
# ======================================================================
async def test_25_concurrent_claims_have_single_winner(client):
    job_id = await submit_id(client, job_type="email")
    assert await pop_job() == job_id

    results = await asyncio.gather(*(claim(job_id) for _ in range(10)))
    assert len([r for r in results if r is not None]) == 1
    assert (await get_db_job(job_id)).attempts == 1


# ======================================================================
# TEST 26 - Multiple workers drain the queue, each job claimed once
# Job:   20 email jobs, 5 concurrent simulated workers
# Tests: every job is claimed exactly once - no job lost, none duplicated
# ======================================================================
async def test_26_multiple_workers_claim_each_job_exactly_once(client):
    ids = {await submit_id(client, job_type="email") for _ in range(20)}
    claimed: list[uuid.UUID] = []

    async def worker():
        while (job_id := await pop_job(timeout=1)) is not None:
            job = await claim(job_id)
            if job is not None:
                claimed.append(job.id)

    await asyncio.gather(*(worker() for _ in range(5)))
    assert len(claimed) == 20
    assert set(claimed) == ids


# ======================================================================
# TEST 27 - Crashed worker: the reaper recovers the job + fencing
# Job:   email job claimed by a worker that "crashes" (lease expires)
# Tests: reaper returns it to PENDING and re-queues it; the crashed run
#        counts as an attempt; the old ("zombie") worker can't write anymore
# ======================================================================
async def test_27_crashed_worker_job_is_recovered_by_reaper(client):
    job_id = await submit_id(client, job_type="email")
    assert await pop_job() == job_id
    assert (await claim(job_id)).status == JobStatus.PROCESSING

    await set_fields(job_id, lease_expires_at=func.now() - timedelta(seconds=1))
    await reap_expired_leases()

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.PENDING
    assert db_job.locked_by is None
    assert db_job.attempts == 1
    assert "lease expired" in db_job.error_message
    assert await pop_job() == job_id

    assert await update_owned(job_id, status=JobStatus.COMPLETED) is False


# ======================================================================
# TEST 28 - Poison message: a job that keeps crashing workers
# Job:   email job with max_attempts=1 whose worker crashes
# Tests: when attempts are exhausted the reaper marks it FAILED
#        instead of re-queuing it forever
# ======================================================================
async def test_28_poison_job_fails_after_max_attempts(client):
    job_id = await submit_id(client, job_type="email", max_attempts=1)
    assert await pop_job() == job_id
    await claim(job_id)

    await set_fields(job_id, lease_expires_at=func.now() - timedelta(seconds=1))
    await reap_expired_leases()

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.FAILED
    assert "max attempts" in db_job.error_message
    assert db_job.completed_at is not None
    assert await queue_size() == 0


# ======================================================================
# TEST 29 - An active lease is never reaped
# Job:   email job claimed by a healthy worker
# Tests: the reaper leaves jobs with a valid lease alone
# ======================================================================
async def test_29_active_lease_is_not_reaped(client):
    job_id = await submit_id(client, job_type="email")
    assert await pop_job() == job_id
    claimed = await claim(job_id)

    await reap_expired_leases()

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.PROCESSING
    assert db_job.locked_by == claimed.locked_by
    assert await queue_size() == 0


# ======================================================================
# TEST 30 - Heartbeat keeps a slow (but alive) job from being stolen
# Job:   email job that runs 2.5s, while the lease is only 1s
# Tests: the heartbeat keeps extending the lease, so the reaper (running
#        constantly) never takes the job; it completes with attempts=1
# ======================================================================
async def test_30_heartbeat_keeps_slow_job_alive(client, monkeypatch):
    monkeypatch.setattr(settings, "lease_seconds", 1)
    monkeypatch.setattr(settings, "heartbeat_seconds", 0.2)

    async def slow(payload, ctx):
        await asyncio.sleep(2.5)
        return {"done": True}

    monkeypatch.setitem(handlers.HANDLERS, "email", slow)
    job_id = await submit_id(client, job_type="email")
    assert await pop_job() == job_id

    task = asyncio.create_task(process(job_id))
    while not task.done():
        await reap_expired_leases()
        await asyncio.sleep(0.2)
    await task

    db_job = await get_db_job(job_id)
    assert db_job.status == JobStatus.COMPLETED
    assert db_job.attempts == 1


# ======================================================================
# TEST 31 - A worker that lost its lease abandons the job ("zombie")
# Job:   slow email job (5s); mid-run the job is taken over by another worker
# Tests: the heartbeat detects the lost lease, the worker stops early,
#        and its result is never written over the new owner's job
# ======================================================================
async def test_31_worker_abandons_job_after_losing_lease(client, monkeypatch):
    monkeypatch.setattr(settings, "heartbeat_seconds", 0.1)

    async def slow(payload, ctx):
        await asyncio.sleep(5)
        return {"should": "never be saved"}

    monkeypatch.setitem(handlers.HANDLERS, "email", slow)
    job_id = await submit_id(client, job_type="email")
    assert await pop_job() == job_id

    task = asyncio.create_task(process(job_id))
    await asyncio.sleep(0.3)
    await set_fields(job_id, locked_by="worker-b")  # another worker now owns the job

    await asyncio.wait_for(task, timeout=2)  # stops well before the handler's 5s

    db_job = await get_db_job(job_id)
    assert db_job.locked_by == "worker-b"
    assert db_job.status == JobStatus.PROCESSING
    assert db_job.result is None


# ======================================================================
# TEST 32 - Reconciler restores jobs missing from Redis
# Job:   pending email job whose queue entry was lost (Redis restart, or
#        the API crashed between DB commit and enqueue)
# Tests: recent jobs are left alone; older ones are re-queued exactly once
#        (running the reconciler twice doesn't create duplicates)
# ======================================================================
async def test_32_reconciler_restores_lost_queue_entries(client):
    job_id = await submit_id(client, job_type="email")
    await redis_client.flushdb()  # Redis lost its data
    assert await queue_size() == 0

    await reconcile_queue()
    assert await queue_size() == 0  # too recent - may still be mid-submission

    await set_fields(job_id, updated_at=func.now() - timedelta(minutes=1))
    await reconcile_queue()
    await reconcile_queue()
    assert await queue_size() == 1
    assert await pop_job() == job_id


# ======================================================================
# TEST 33 - Health endpoint with queue statistics
# Job:   1 pending + 1 scheduled email job
# Tests: reports ok status, DB/Redis health, job counts and queue depth
# ======================================================================
async def test_33_health_reports_queue_stats(client):
    await submit(client, job_type="email")
    await submit(client, job_type="email", run_at=future_iso(hours=1))

    r = await client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok" and data["db"] == "ok" and data["redis"] == "ok"
    assert data["queue_depth"] == 1
    assert data["jobs_by_status"] == {"pending": 1, "scheduled": 1}