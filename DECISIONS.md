# Design Decisions

**Guiding principle:** PostgreSQL is the source of truth; Redis is a fast delivery hint. Every correctness-critical decision (who runs a job, whether it finished, whether it can be cancelled) is made in PostgreSQL with a conditional `UPDATE`. If Redis loses data or contains duplicates, the system stays correct.

## 1. Job Pickup Strategy

**Approach chosen:** Two layers - an atomic Redis pop for fast delivery, plus a conditional database update as the real guard (a compare-and-set).

1. Workers block on `BZPOPMAX` on a Redis sorted set. The pop is atomic, so two workers never receive the same entry.
2. The worker then claims the job:
   ```sql
   UPDATE jobs SET status='processing', attempts=attempts+1, locked_by=:worker,
                   lease_expires_at=now()+30s, started_at=now()
   WHERE id=:id AND status='pending'
   RETURNING *;
   ```
   If 0 rows are returned (job was cancelled, or another worker already claimed it after a duplicate enqueue), the worker skips it.

**Why:**
- The Redis pop alone is not enough: a job ID can legitimately appear again (the reconciler re-enqueues, a retry re-enqueues), and Redis can lose data. The DB update makes duplicates harmless.
- Under PostgreSQL's default `READ COMMITTED`, row-lock acquisition is atomic: if two transactions update the same row at once, one waits, and after the first commits, PostgreSQL **re-evaluates the `WHERE` clause against the new row version**. So the conditional update acts as an atomic compare-and-set - no lost updates, no "check-then-update" race.
- Row locks are held only for single-statement transactions (milliseconds). Long-term ownership is a **lease stored as data**, not a held database lock, so a running job does not pin a DB connection or keep a long transaction open (the classic source of lock contention).
- No deadlocks: claim and cancel each touch a single row. Batch maintenance operations use `FOR UPDATE SKIP LOCKED`, so concurrent workers skip each other's rows instead of waiting.

**Trade-offs:**
- **Gained:** low-latency blocking pickup (no DB polling), priority ordering in Redis, correctness that doesn't depend on Redis.
- **Gave up:** two systems to operate, and a dual-write between DB and Redis (an API crash after commit but before enqueue leaves a job only in the DB). Mitigated by the reconciler (see Additional Decisions).
- **Alternative considered:** PostgreSQL only (`SELECT ... FOR UPDATE SKIP LOCKED` as the queue). Simpler, one system, no dual-write - a very reasonable choice at this scale. I chose Redis because the task asks for a queue technology and because blocking pops avoid constant DB polling as workers scale.

Verified by tests: 10 concurrent claims of one job -> exactly one winner (test 25); 5 workers draining 20 jobs -> each claimed exactly once (test 26); cancel and claim at the same moment -> exactly one wins, 20 rounds with random timing (test 19).

---

## 2. Worker Crash Recovery

**Approach chosen:** Lease + heartbeat + reaper, with fencing.

- On claim, the worker gets a **30-second lease** (`lease_expires_at`).
- A **heartbeat** task extends the lease every **10 seconds** while the job runs.
- A **reaper** (part of the maintenance loop, every 2s) finds `status='processing' AND lease_expires_at < now()`.
- **Fencing:** every write by a worker (heartbeat, progress, complete, fail) includes `WHERE locked_by = :me AND status = 'processing'`.
- **Attempts are counted at claim time**, so a crash counts as an attempt.

**Why:**
- A lease distinguishes "slow but alive" from "dead": a 10-minute job keeps its lease as long as it heartbeats, unlike a fixed "stuck after N minutes" timeout.
- Fencing handles the "zombie" worker - one that didn't die but stalled (GC pause, network partition). Its lease expires, another worker takes the job, and when the zombie wakes up its writes match 0 rows; its heartbeat notices the lost lease and cancels the local execution.
- Counting attempts at claim time provides **poison-message protection**: a job that crashes the worker every time would otherwise loop forever (crash -> reap -> crash). With attempts counted, the reaper marks it `FAILED` once attempts are exhausted.
- All timestamps come from the **database clock** (`now()`), never the worker's clock, so clock skew between machines can't expire leases early or late.

**What happens if a worker crashes mid-job:**
1. The worker process dies; heartbeats stop. The job stays `processing` with a lease in the future.
2. Within ≤30s the lease expires.
3. The reaper (running in any surviving or restarted worker) finds the expired lease:
   - attempts remaining -> status `pending`, lease cleared, `error_message` records the recovery, ID re-queued in Redis;
   - attempts exhausted -> status `failed`.
4. Another worker claims it normally (attempt N+1).
5. If the "crashed" worker was actually a zombie and wakes up, its result is discarded by fencing.

**Delivery guarantee:** this is **at-least-once**. A job can run twice (e.g., the worker finished the side effect but crashed before recording completion). Exactly-once is not achievable in a distributed system; real handlers must be idempotent (e.g., pass an idempotency key to the email provider).

**Graceful shutdown:** on `SIGTERM` the worker stops taking new jobs and finishes the current one. `stop_grace_period` is **130s**, deliberately longer than the longest job timeout (batch, 120s), so any job allowed to run is allowed to finish. A job killed anyway (hardware failure, `SIGKILL`) is recovered by the reaper. Trade-off: slower deploys/restarts (up to 130s per worker).

The loop is a separate function (`worker_loop(stop)`), and the signal handler only sets the `stop` event. This keeps the shutdown logic testable without sending real signals: test 39 stops the worker mid-job and checks that the running job completes while a second queued job is not started; test 40 checks that an idle worker exits within about a second. What is **not** automatically tested is the OS signal registration itself in `main()` (verified manually with `docker compose stop worker`, see `AI_USAGE.md`).

Verified by tests 27-31 (reaper + fencing, poison job, active lease untouched, heartbeat keeps a slow job alive, worker abandons a job after losing its lease).

---

## 3. Priority Queue Implementation

**Approach chosen:** Redis sorted set (`ZSET`) with `BZPOPMAX`.

```
score = priority * 10^13 - created_at_ms
```

- Higher priority -> higher score -> popped first.
- Same priority -> older job has a higher score -> **FIFO**.
- The score is based on `created_at` (not "now"), and enqueueing uses `ZADD NX`, so re-enqueueing (reconciler, reaper) never changes a job's position and never creates duplicates (a sorted set has unique members).
- Priority range is 0-10 (enforced by a DB `CHECK` constraint and API validation). The score stays well within the exact-integer range of a double.

**Why:**
- A Redis list (`LPUSH`/`BRPOP`) is FIFO only and can't order by priority. Multiple lists per priority would need polling across lists.
- `BZPOPMAX` is atomic and blocking: no polling, no race between "read top" and "remove".

**Known limitation:** strict priority can **starve** low-priority jobs under sustained high-priority load. With more time I'd add aging (raise effective priority with wait time).

---

## 4. Retry Backoff Strategy

**Approach chosen:** Exponential backoff with jitter. A failed attempt with attempts remaining moves the job back to `SCHEDULED` with `run_at = now() + delay`; the same scheduler that handles user-scheduled jobs re-queues it when due. One code path serves both features.

**Timing (max_attempts = 3, configurable 1-10 per job):**

| Attempt | When                                    |
|---------|-----------------------------------------|
| 1       | Immediately                             |
| 2       | ~30s after attempt 1 fails              |
| 3       | ~120s after attempt 2 fails             |
| -       | Attempt 3 fails -> `FAILED` permanently |

Formula: `delay = 30 * 4^(attempt-1)`, with **±10% jitter** so many jobs that failed together (e.g., an outage of the webhook target) don't all retry at the same instant (thundering herd).

**Retryable vs non-retryable:**
- Retryable: any exception, including timeouts (`asyncio.wait_for` per job type: email/webhook 10s, report 15s, batch 120s).
- Non-retryable (fail immediately): `PermanentJobError` (invalid payload - retrying can't fix bad data) and unknown job types.
- **Manual retry** (`POST /jobs/{id}/retry`) resets `attempts` to 0 - a manual retry usually follows a fix, so the job gets a full fresh set of attempts.

---

## 5. One Thing I Would Do Differently With More Time

**Make long-running jobs resumable (checkpointing).** Today a batch job that crashes at item 121 of 200 restarts from item 1 on the next attempt. I'd persist the last processed index and resume from it.

Checkpointing reduces duplicate work but can't eliminate it: a crash between processing an item and saving the checkpoint still reprocesses that item. So it must be paired with **per-item idempotency** in real handlers (e.g., a per-item idempotency key sent downstream). The system's guarantee remains at-least-once.

**Other simplifications I made (honestly):**
- **Schema migrations:** tables are created with `create_all` by a one-shot `migrate` service. Production would use Alembic.
- **Job events are best effort.** Key lifecycle events are written to the `job_logs` table (see decision G), but in a separate transaction from the state change. A crash between the two can leave a state change without its log entry (or the reverse). Making them atomic means writing the log row in the same transaction as every state update. I chose not to, so a logging problem can never fail a job. The authoritative state is always the `jobs` row.
- **Dead letter queue** is DB-based: permanently failed jobs remain queryable (`GET /jobs?status=failed`) and can be retried manually. No separate DLQ stream.
- **Cancelling a running job** is not supported (only `pending`/`scheduled`, as specified).
- **Idempotency keys** are kept indefinitely (satisfies "at least 24h"). No TTL cleanup, and no payload fingerprint: reusing a key with a different payload returns the original job instead of `409` (Stripe-style).
- **Redis** runs without auth or persistence (acceptable because Redis is rebuildable from the DB via the reconciler; production would configure auth, `maxmemory` and AOF).
- **Priority aging** (see section 3).

---

## Additional Decisions

### A. Idempotency
- Key is sent in the `Idempotency-Key` header (the Stripe / IETF draft convention - it's request metadata, not job data). Optional: only the client knows whether two identical requests are a duplicate or intentional (e.g., the same daily report). Auto-deriving a key from the payload would wrongly block legitimate repeats.
- Enforced by a **unique index** with `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING`. If nothing is returned, the existing job is fetched and returned with `200` (`201` for a new job).
- **Rejected approach:** `SELECT` by key, then `INSERT` if missing - a race condition: two concurrent requests both see "not found" and both insert. With `ON CONFLICT`, a concurrent request waits on the unique index until the first transaction commits, then does nothing.
- PostgreSQL allows multiple `NULL`s in a unique index, so jobs without a key never conflict.
- Verified: 10 concurrent identical requests -> exactly one job, one `201`, nine `200` (test 20).

### B. Scheduled jobs
- A future `run_at` (timezone required - naive datetimes are rejected with `422` because they're ambiguous) creates the job as `SCHEDULED`, stored only in the DB.
- The scheduler promotes due jobs (`run_at <= now()`) to `PENDING` and enqueues them. Priority still applies once they're due.

### C. DB/Redis dual-write and the reconciler
- The API commits to the DB first, then enqueues. The reverse order could deliver a job to a worker before it exists in the DB.
- If the enqueue fails or the API crashes in between, the job is safe in the DB. The **reconciler** (every 30s) re-enqueues `PENDING` jobs not updated in the last 30s; `ZADD NX` makes this a no-op for jobs already queued. It also covers a Redis restart and a worker dying between pop and claim.
- The API treats Redis as optional for correctness: if Redis is down, submit and manual retry still succeed (the job is `PENDING` in the DB and the reconciler queues it later), and cancel still succeeds (the DB decides; a stale Redis entry is skipped by the worker's conditional claim). Verified by tests 37 and 38, and `/health` returns `503` when Redis or the database is down (test 34).
- Trade-off: while Redis is down, new jobs wait for the reconciler (up to ~30s after Redis returns) instead of being picked up instantly.

### D. Schema choices
- **Status:** `VARCHAR` + `CHECK` constraint instead of a native PostgreSQL `ENUM` (native enums are hard to change; values can't be removed). Stored as lowercase values.
- **`job_type`:** plain string validated by the API against the handler registry, so adding a job type doesn't require a DB migration.
- **JSONB** for `payload`, `result`, `error_details`: each job type has a different shape.
- **UUID** primary keys: not guessable/enumerable, safe to generate in any process.
- **Partial indexes** for the hot queries (`run_at` where scheduled/pending; `lease_expires_at` where processing): they stay small no matter how many completed jobs accumulate.
- **Startup race avoided:** tables are created by a single `migrate` service; the API and workers wait for it to complete (`service_completed_successfully`) instead of both running `create_all` concurrently.

### E. Concurrency model
- Async Python (asyncio + asyncpg). One job at a time per worker process; scale by adding workers (`--scale worker=N`). Asyncio makes heartbeat, timeout (`asyncio.wait_for`) and shutdown handling simple concurrent tasks - a stuck thread can't be cancelled in Python, a stuck coroutine can.

### F. Testing strategy
- Integration tests against real PostgreSQL and Redis (the interesting bugs - races, fencing, duplicate claims - only exist against a real database; mocks would hide them). Mocks are used only to simulate a failure on purpose (Redis or the database being down).
- Isolated test DB and Redis index, so running workers can't pick up test jobs.
- Time is fast-forwarded by editing `run_at` / `lease_expires_at`, so retry and crash-recovery scenarios are deterministic and fast. Where a test must wait for a real concurrent event (a job being claimed), it polls for the condition instead of sleeping a fixed time, to avoid flaky tests on slow machines.
- Race tests (cancel vs claim) check the *invariant* (exactly one winner, and the DB state matches it), not which side wins, because the winner is non-deterministic. The deterministic orderings are covered separately (tests 17 and 18).
- **Known gaps:** the tests call `process()` and `worker_loop()` directly, so the real process entry point (`main()`, signal registration, the Docker `stop_grace_period`) is verified only manually. Tests run on one event loop, so they prove correctness under concurrent *connections*, not under several separate OS processes. The correctness argument still holds across processes because it rests on PostgreSQL's row locking, not on anything in Python, but a multi-container run (`--scale worker=N`) is the real end-to-end check.

### G. Job event log (`job_logs`)
- Each job has a history of events (submitted, attempt started, attempt failed + retry scheduled, completed, failed permanently, cancelled, manually retried, lease lost / re-queued by the reaper), readable via `GET /jobs/{id}/logs`. The assignment lists this as optional; I added it because "why did my job fail three times?" is the first question an operator asks, and the `jobs` row only keeps the latest error.
- Written by `app/job_log.py` in a separate, best-effort transaction (see section 5). Structured JSON logs on stdout remain the place for operational logging; `job_logs` is the per-job audit trail.
- Verified by tests 42 and 43; JSON log fields (`job_id`, `job_type`, `attempt`, `worker_id`) by test 41.
