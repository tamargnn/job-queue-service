# Job Queue Service

A distributed background job processing system built with **FastAPI**, **PostgreSQL**, **Redis** and **Docker**.

Clients submit jobs through a REST API. Jobs are persisted in PostgreSQL (the source of truth), queued by priority in Redis, and executed by one or more independent worker processes with retries, exponential backoff, scheduling, cancellation, idempotency and crash recovery.

## Quick Start

**Requirements:** Docker Desktop (with Docker Compose v2). Ports `8000` (API), `5432` (PostgreSQL) and `6379` (Redis) must be free on your machine.

```bash
docker compose up --build
```

The first build downloads images and installs dependencies, so it can take a few minutes. The system is ready when the logs show `Worker started` and the API answers at http://localhost:8000/health.

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

Stopping a worker (`docker compose stop worker` or `down`) is graceful: it finishes the job it is running before exiting, which can take up to 130 seconds for a long batch job (see `stop_grace_period` in `docker-compose.yml`).

### Trying a worker shutdown and a worker crash

Submit a long job first, so it is still running when you stop the worker (a batch of 40 items takes about 20 seconds):

```bash
curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"job_type": "batch", "payload": {"items": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40]}}'
```

| What you do | What happens |
|-------------|--------------|
| `docker compose stop worker` (graceful) | The worker finishes the running job, takes no new ones, then exits. The job ends `completed`. Docker only force-kills after 130 seconds. |
| `docker compose kill worker` (hard kill, simulates a crash) | The job stays `processing` until its lease expires (up to 30 seconds). Then the reaper moves it back to `pending` (or `failed` if attempts are exhausted) and another worker runs it as attempt 2. The `/jobs/<id>/logs` endpoint shows `Worker lost (lease expired); job re-queued`. |

**Stopping only one worker** when several are running (`--scale worker=3`): `docker compose stop worker` and `docker compose kill worker` act on all replicas. To target one, use its container name (list them with `docker compose ps`; they are named `<project-folder>-worker-1`, `-2`, `-3`, e.g. `job-queue-worker-2`):

```bash
docker stop job-queue-worker-2     # graceful: finishes its current job first
docker kill job-queue-worker-2     # hard kill: simulates a crash of this worker only
docker start job-queue-worker-2    # bring it back
docker logs -f job-queue-worker-2  # follow the logs of this worker only
```

The reaper runs inside the workers, and the compose file has no restart policy. So after a hard kill, **start a worker again** (`docker compose start worker`), or run several workers (`--scale worker=3`) so a surviving one recovers the job. With no worker running, the job simply waits.

## Running the Tests

```bash
docker compose run --rm --build api pytest -v
```

This starts PostgreSQL and Redis automatically if they are not already running, so you don't need `docker compose up` first. It also works while the system is running. `--build` makes sure the tests run against your latest code.

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

Check the job status (use the `id` from the response). A simple email job completes after 1-3 seconds:

```bash
curl http://localhost:8000/jobs/<job_id>
```

See what happened to the job (every attempt, failure, retry and state change):

```bash
curl http://localhost:8000/jobs/<job_id>/logs
```

On Windows PowerShell, `curl` is an alias for `Invoke-WebRequest`; use `Invoke-RestMethod http://localhost:8000/jobs/<job_id>` (or `curl.exe`) instead.

Re-sending the same request with the same `Idempotency-Key` returns the existing job (`200`) instead of creating a new one (`201`). Use a different key, or no key, to create another job.

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
                      ┌────── attempt failed, attempts left: retry after backoff ─────┐
                      │                    no run_at │                                │
                      ▼                              ▼                                │
 future run_at  ┌────────────┐  run_at reached ┌────────────┐ worker claims it  ┌────────────┐
 ─────────────▶ │ SCHEDULED  │ ──────────────▶ │  PENDING   │ ────────────────▶ │ PROCESSING │
                └────────────┘                 └────────────┘                   └────────────┘
                      │ cancel             cancel │     ▲                             │
                      └─────────────┬─────────────┘     │                     ┌───────┴───────┐
                                    ▼                   │             success ▼               ▼ exhausted
                              ┌────────────┐            │               ┌────────────┐  ┌────────────┐
                              │ CANCELLED  │            │               │ COMPLETED  │  │   FAILED   │
                              └────────────┘            │               └────────────┘  └────────────┘
                                                        │         manual retry                │
                                                        └─────────────────────────────────────┘
```

- A job submitted with a future `run_at` starts as `SCHEDULED`; otherwise it starts as `PENDING`.
- A failed attempt with attempts left goes back to `SCHEDULED` with a backoff delay (~30s, then ~120s), then to `PENDING` when due.
- A job becomes `FAILED` when its attempts are exhausted or the error is not retryable (e.g. invalid payload).
- `PENDING` and `SCHEDULED` jobs can be cancelled (-> `CANCELLED`); only `FAILED` jobs can be manually retried.
- If a worker crashes, its job stays `PROCESSING` until the lease expires (up to 30s); the reaper then returns it to `PENDING`, or marks it `FAILED` if attempts are exhausted.

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
5. **Graceful shutdown** - on `SIGTERM`/`SIGINT` (e.g. `docker compose stop worker`) the worker stops taking new jobs, **finishes the job it is currently running**, then exits. Docker waits up to 130 seconds (`stop_grace_period`, longer than the 120s batch timeout) before force-killing. If a worker is killed anyway, the reaper recovers its job (step 4).

Design decisions and trade-offs are documented in [DECISIONS.md](DECISIONS.md).

## Project Structure

```
job-queue/
├── README.md                # This file: how to run, test, submit a job, architecture
├── DECISIONS.md             # Design decisions and trade-offs
├── AI_USAGE.md              # How AI tools were used
├── docker-compose.yml       # postgres, redis, migrate, api, worker
├── Dockerfile               # Image shared by migrate, api, worker and the tests
├── requirements.txt         # Python dependencies (app + tests)
├── pytest.ini               # pytest config (async mode, test path)
├── .dockerignore            # Files excluded from the Docker build context
├── .gitignore               # Files excluded from Git
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app + /health
│   ├── config.py            # Settings (env vars)
│   ├── db.py                # Async SQLAlchemy engine/session
│   ├── models.py            # Job and JobLog tables
│   ├── schemas.py           # Request/response models
│   ├── queue.py             # Redis priority queue
│   ├── job_log.py           # Per-job event log (job_logs table), best effort
│   ├── init_db.py           # Table creation (run by the `migrate` service)
│   ├── logging_config.py    # Structured JSON logging
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes.py        # /jobs endpoints
│   │   └── deps.py          # DB session dependency
│   └── worker/
│       ├── __init__.py
│       ├── main.py          # Worker loop (worker_loop): claim, execute, heartbeat, complete/fail; graceful shutdown
│       ├── handlers.py      # Mock job implementations + timeouts
│       ├── retry.py         # Exponential backoff with jitter
│       └── maintenance.py   # Scheduler, reaper, reconciler
└── tests/
    ├── __init__.py
    ├── conftest.py          # Isolated test DB/Redis, fixtures
    └── test_jobs.py         # 54 test cases (43 functions), integration tests
```

## Configuration

Set via environment variables (defaults in `app/config.py`, overridden in `docker-compose.yml`):

| Variable                        | Default | Description                              |
|---------------------------------|---------|------------------------------------------|
| `DATABASE_URL`                  | `postgresql+asyncpg://user:password@localhost:5432/job_queue` | PostgreSQL URL (asyncpg). Default is for running outside Docker; `docker-compose.yml` points it at the `postgres` service |
| `REDIS_URL`                     | `redis://localhost:6379/0` | Redis URL. Same note as above           |
| `LEASE_SECONDS`                 | 30      | How long a claim is valid without a heartbeat |
| `HEARTBEAT_SECONDS`             | 10      | Lease extension interval                 |
| `MAINTENANCE_INTERVAL_SECONDS`  | 2       | Scheduler/reaper interval                |
| `RECONCILE_INTERVAL_SECONDS`    | 30      | Reconciler interval                      |

## Tech Stack

Python 3.11 · FastAPI · SQLAlchemy 2.0 (async, asyncpg) · PostgreSQL 15 · Redis 7 · pytest + pytest-asyncio · Docker Compose
