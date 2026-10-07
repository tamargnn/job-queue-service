# Job Queue Service

A distributed background job processing system built with **FastAPI**, **PostgreSQL**, **Redis** and **Docker**.

Clients submit jobs through a REST API. Jobs are persisted in PostgreSQL (the source of truth), queued by priority in Redis, and executed by one or more independent worker processes with retries, exponential backoff, scheduling, cancellation, idempotency and crash recovery.

## Quick Start

**Requirements:** Docker Desktop (with Docker Compose v2).

```bash
docker compose up --build
```

This starts:

| Service    | Role                                                            |
|------------|-----------------------------------------------------------------|
| `postgres` | Job state (source of truth)                                     |
| `redis`    | Priority queue of job IDs                                       |
| `migrate`  | One-shot: creates the DB tables, then exits                     |
| `api`      | REST API on http://localhost:8000                               |
| `worker`   | Background worker (runs jobs + maintenance: scheduler, reaper, reconciler) |

- **Interactive API docs (Swagger UI):** http://localhost:8000/docs
- **Health check:** http://localhost:8000/health

Run several workers in parallel:

```bash
docker compose up --build --scale worker=3
```

Stop everything:

```bash
docker compose down        # keep data
docker compose down -v     # also delete the database volume
```

## Running the Tests

```bash
docker compose run --rm api pytest -v
```

- **54 automated integration tests** (43 test functions, some parametrized), fully automated - no manual steps. Takes about 50 seconds.
- Tests run against **real PostgreSQL and Redis**, in an isolated database (`job_queue_test`) and Redis DB index (`15`), so they never interfere with a running system.
- Worker functions are called directly, and time is "fast-forwarded" (by editing `run_at` / `lease_expires_at`) instead of sleeping, so retry and crash scenarios run in seconds. Where timing matters, tests poll for a condition instead of sleeping a fixed time.
- The worker loop (`worker_loop`) is tested directly for graceful shutdown; only the OS signal wiring in `main()` is not covered by automated tests.

Coverage includes: submission and validation (including boundaries), completion flow, batch progress, retries with backoff, permanent vs transient failures, timeouts, manual retry, cancellation (including during retry backoff), **cancel-vs-claim race**, **concurrent idempotent submissions**, priority and FIFO ordering, scheduled jobs, **concurrent claims of the same job**, multiple workers, **crash recovery (reaper + fencing)**, poison messages, heartbeat, lease loss, queue reconciliation, **Redis outages during submit/cancel/retry**, **graceful shutdown**, **structured JSON logs**, the **per-job event log**, and health stats (healthy and degraded).

## Submitting a Test Job

**curl (bash / macOS / Linux):**

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-123" \
  -d '{"job_type": "email", "payload": {"to": "user@example.com", "subject": "Hello"}, "priority": 5}'
```

**PowerShell (Windows):**

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8000/jobs `
  -ContentType "application/json" `
  -Headers @{ "Idempotency-Key" = "order-123" } `
  -Body '{"job_type": "email", "payload": {"to": "user@example.com", "subject": "Hello"}, "priority": 5}'
```

Or use the Swagger UI at http://localhost:8000/docs.

Check the job status (use the `id` from the response):

```bash
curl http://localhost:8000/jobs/<job_id>
```

See what happened to the job (every attempt, failure, retry and state change):

```bash
curl http://localhost:8000/jobs/<job_id>/logs
```

### More examples

```jsonc
// Webhook that always fails - demonstrates retries with backoff (~30s, then ~120s, then FAILED)
{"job_type": "webhook", "payload": {"url": "https://example.com/hook", "simulate_failure": true}}

// Batch job with progress tracking (0.5s per item, max 200 items)
{"job_type": "batch", "payload": {"items": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}}

// Scheduled job (must include a timezone)
{"job_type": "report", "payload": {"month": "2026-09"}, "run_at": "2030-01-01T10:00:00Z"}
```

## API

| Method | Path                     | Description                                                   |
|--------|--------------------------|---------------------------------------------------------------|
| POST   | `/jobs`                  | Submit a job. Optional `Idempotency-Key` header. `201` = created, `200` = existing job returned for that key |
| GET    | `/jobs/{id}`             | Job status, result, error, progress                           |
| GET    | `/jobs/{id}/logs`        | Event history of a job (submitted, attempt started/failed, retry scheduled, completed, cancelled, crash recovery), oldest first |
| GET    | `/jobs`                  | List jobs. Filters: `status`, `job_type`. Paging: `limit` (1-200), `offset`. Newest first |
| POST   | `/jobs/{id}/cancel`      | Cancel a `pending` or `scheduled` job (`409` otherwise)       |
| POST   | `/jobs/{id}/retry`       | Retry a `failed` job with a fresh set of attempts (`409` otherwise) |
| GET    | `/health`                | DB/Redis health, job counts by status, queue depth (`503` if degraded) |

### Job types (mock implementations)

| Type      | Behavior                                                        |
|-----------|-----------------------------------------------------------------|
| `email`   | Sleeps 1-3s, returns a mock message ID                          |
| `webhook` | Sleeps 1-2s, 80% success / 20% simulated failure (`simulate_failure: true` forces failure) |
| `report`  | Sleeps 3-5s, returns a mock file URL                            |
| `batch`   | Processes `payload.items` (0.5s each), updates progress %, returns a summary |

### Job lifecycle

```
SCHEDULED --(run_at reached)--> PENDING --(claimed)--> PROCESSING --> COMPLETED
    ^                              |                        |
    |                         (cancel)                      +--> FAILED --(manual retry)--> PENDING
    |                              v                        |
    |                          CANCELLED                    |
    +------------(failed, attempts left: backoff)-----------+
```

## Architecture Overview

```
                 ┌──────────────┐  1. INSERT job (source of truth)   ┌──────────────┐
  Client ──────▶ │     API      │ ──────────────────────────────────▶│  PostgreSQL  │
                 │  (FastAPI)   │  2. ZADD job id (priority score)   │  jobs table  │
                 └──────┬───────┘ ─────────────┐                     └──────▲───────┘
                                               ▼                            │
                                       ┌──────────────┐                     │ 4. conditional UPDATE
                                       │    Redis     │  3. BZPOPMAX        │    (claim / complete /
                                       │ sorted set   │ ──────────────┐     │     fail, fenced by lease)
                                       └──────────────┘               ▼     │
                                                              ┌──────────────┴─┐
                                                              │   Worker(s)    │
                                                              │ + maintenance  │
                                                              └────────────────┘
```

**Core principle: PostgreSQL is the source of truth, Redis is a fast delivery hint.**

1. **API** validates the request and inserts the job into PostgreSQL. Idempotency is enforced by a unique index (`INSERT ... ON CONFLICT DO NOTHING`). After commit, the job ID is added to a Redis sorted set scored by priority (FIFO within the same priority).
2. **Worker** blocks on `BZPOPMAX` (atomic: two workers never pop the same entry), then **claims** the job in PostgreSQL with a conditional `UPDATE ... WHERE status='pending'`. If 0 rows change (cancelled, or already claimed), it skips. This DB check is the real duplicate guard.
3. While running, the worker holds a **lease** (`lease_expires_at`) that a **heartbeat** extends. All of the worker's writes are **fenced** (`WHERE locked_by = me`), so a worker that lost its lease can't overwrite anything.
4. **Maintenance loop** (in every worker, safe to run concurrently via `FOR UPDATE SKIP LOCKED`):
   - **Scheduler** - promotes due `SCHEDULED` jobs (user-scheduled and retry backoff) to `PENDING` and queues them.
   - **Reaper** - recovers jobs whose lease expired (worker crashed); marks them `FAILED` if attempts are exhausted (poison protection).
   - **Reconciler** - re-queues `PENDING` jobs missing from Redis (API crash between commit and enqueue, Redis restart).

Design decisions and trade-offs are documented in [DECISIONS.md](DECISIONS.md).

## Project Structure

```
app/
├── main.py              # FastAPI app + /health
├── config.py            # Settings (env vars)
├── db.py                # Async SQLAlchemy engine/session
├── models.py            # Job and JobLog tables
├── schemas.py           # Request/response models
├── queue.py             # Redis priority queue
├── job_log.py           # Per-job event log (job_logs table), best effort
├── init_db.py           # Table creation (run by the `migrate` service)
├── logging_config.py    # Structured JSON logging
├── api/
│   ├── routes.py        # /jobs endpoints
│   └── deps.py          # DB session dependency
└── worker/
    ├── main.py          # Worker loop (worker_loop): claim, execute, heartbeat, complete/fail; graceful shutdown
    ├── handlers.py      # Mock job implementations + timeouts
    ├── retry.py         # Exponential backoff with jitter
    └── maintenance.py   # Scheduler, reaper, reconciler
tests/
├── conftest.py          # Isolated test DB/Redis, fixtures
└── test_jobs.py         # 54 test cases (43 functions), integration tests
```

## Configuration

Set via environment variables (defaults in `app/config.py`, overridden in `docker-compose.yml`):

| Variable                        | Default | Description                              |
|---------------------------------|---------|------------------------------------------|
| `DATABASE_URL`                  | -       | PostgreSQL URL (asyncpg)                 |
| `REDIS_URL`                     | -       | Redis URL                                |
| `LEASE_SECONDS`                 | 30      | How long a claim is valid without a heartbeat |
| `HEARTBEAT_SECONDS`             | 10      | Lease extension interval                 |
| `MAINTENANCE_INTERVAL_SECONDS`  | 2       | Scheduler/reaper interval                |
| `RECONCILE_INTERVAL_SECONDS`    | 30      | Reconciler interval                      |

## Tech Stack

Python 3.11 · FastAPI · SQLAlchemy 2.0 (async, asyncpg) · PostgreSQL 15 · Redis 7 · pytest + pytest-asyncio · Docker Compose
