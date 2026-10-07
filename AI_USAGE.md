# AI Tool Usage

## Tools I Used

- **Claude (Anthropic)** - used as an architecture mentor in a chat: discussing design options, drafting code component by component, explaining concepts, and challenging/defending decisions.
- **Cursor IDE** - editing and organizing the project files, integrated terminal for Docker and Git. Its AI assistant was also used near the end as an independent reviewer: I gave it the assignment PDF and the test file and asked whether the tests really cover the requirements. It then helped me close the gaps it found (see "What I Had to Fix", item 4).


## What Helped Most

**1. Reasoning about concurrency precisely.** The most valuable part was working through the race conditions one by one: why "check-then-insert" breaks idempotency and `INSERT ... ON CONFLICT` doesn't; why a conditional `UPDATE ... WHERE status='pending'` behaves as an atomic compare-and-set; why every worker write needs fencing (`WHERE locked_by = me`) to survive a "zombie" worker; and why attempts must be counted at claim time to stop poison messages from looping. Understanding that lock contention typically comes from long-held locks, not from the row-lock mechanism itself, shaped the design: single-statement transactions, leases stored as data instead of held locks, and `SKIP LOCKED` for batch maintenance.

**2. Getting productive fast in an unfamiliar stack.** Docker Compose (healthchecks, `depends_on` conditions, a one-shot migration service, service-name networking), async SQLAlchemy 2.0, and an integration-test setup with an isolated database. Without AI this would have taken far longer than the deadline allowed.

## What I Had to Fix

**1. Graceful shutdown vs. job timeouts were inconsistent.** The initial setup used a 30s `stop_grace_period` while the batch job timeout was 120s. I found this in manual testing: I submitted a 280-item batch (~140s), ran `docker compose stop worker`, and the worker was killed after exactly 30s with progress stuck at 43%. The AI's first proposal was only to cap the batch size. That fixed a real second problem (a 280-item batch could never finish within its own 120s timeout anyway), but not the shutdown issue. I pushed back that the grace period must exceed the longest job timeout, otherwise "graceful" shutdown silently turns into a crash for any long job. The final design does both: max 200 items per batch, and `stop_grace_period: 130s` (longer than the 120s max timeout), documented with its trade-off (slower deploys).

**2. Test coverage was far too thin.** The AI initially proposed one test per required feature (6 tests, plus 2 bonus). In my view, one happy-path test per feature misses exactly the bugs that matter in a distributed system. I asked for edge cases, especially around concurrency and crash recovery, and reviewed the scenarios. The first round produced 33 tests / 42 cases (the final suite is larger, see item 4), including races that a sequential test can never catch: cancel vs. claim at the same instant (20 rounds), 10 concurrent submissions with the same idempotency key, 10 concurrent claims of the same job, a heartbeat keeping a slow job alive while the reaper runs constantly, and a worker abandoning a job after losing its lease.

**3. An imprecise answer about lock contention.** I asked what happens when two operations try to lock the same row at the same moment. The first answer explained blocking and deadlocks, which wasn't my question. I pressed on it and got to the precise answer: row-lock acquisition in PostgreSQL is atomic (only one transaction can hold it), and under `READ COMMITTED` the waiting `UPDATE` re-evaluates its `WHERE` clause against the newly committed row version, which is what prevents a lost update. Lesson: AI explanations of concurrency can sound right while answering a slightly different question.

**4. Passing tests that didn't prove everything they claimed.** The 42 cases all passed, but when I reviewed the suite against the assignment's requirement list (with a second AI session as reviewer), it had real gaps: graceful shutdown, structured JSON logging, the Redis-outage fallbacks in the API, and the degraded `/health` (503) were implemented but never tested; the `job_logs` table existed but nothing wrote to it; two tests (heartbeat, lease loss) depended on fixed `sleep` timing and could be flaky on a slow machine; and the cancel-vs-claim race test ended with `assert outcomes`, which passes even if only one side ever wins, so the final check proved nothing. Fixes: the worker loop was split out into `worker_loop(stop)` so shutdown can be tested without real signals; the new tests (34-43) cover the gaps above; the flaky tests now poll for a condition instead of sleeping; the race test now adds random timing jitter and cross-checks the aggregate DB state against the counted winners. Lesson: "all tests green" is not the same as "requirements covered" - map each requirement to a test explicitly.

## Pitfalls I Checked For

The assignment notes that AI often gives subtly wrong distributed-systems advice. These are well-known incorrect patterns that I deliberately checked the code against, and how the final code avoids each:

| Pitfall | How the code avoids it |
|---|---|
| Idempotency via `SELECT` then `INSERT` (race) | Unique index + `INSERT ... ON CONFLICT DO NOTHING` |
| Redis list (`BRPOP`) as a priority queue | Sorted set + `BZPOPMAX` |
| Running a popped job without a DB check | Conditional claim `WHERE status='pending'` |
| Completing a job without checking ownership | Fencing on every worker write (`locked_by`) |
| Reaper that re-queues forever | Attempts counted at claim; reaper fails exhausted jobs |
| Re-enqueue that reorders or duplicates jobs | Score from `created_at` + `ZADD NX` |
| Using worker clocks for leases | All timestamps from the DB clock (`now()`) |
| API and workers both running `create_all` at startup | Single one-shot `migrate` service |
| `depends_on` without healthchecks | `condition: service_healthy` / `service_completed_successfully` |
| SQLAlchemy model attribute named `metadata` | Reserved name; attribute is `extra`, column is `metadata` |
| SQLAlchemy storing enum names (`PENDING`) instead of values | `values_callable` stores `pending` |
| pytest-asyncio creating a new event loop per test (breaks shared async engine/Redis client) | Session-scoped loop, pinned pytest-asyncio version |
| Tests sharing the DB with running workers | Isolated test DB and Redis index |
| Fixed `sleep()` in tests (flaky) / assertions that can't fail | Polling helper (`wait_until`); race test cross-checks aggregate DB state |
| Best-effort audit log that fails or retries the job itself | `record_event` runs in its own transaction and swallows its own errors |

## What AI Struggled With

- **Runtime behavior it couldn't observe.** The grace-period inconsistency only surfaced when I actually ran the system and killed a worker mid-job. AI-designed timeouts and settings looked consistent on paper but weren't. Manual testing of failure scenarios was essential.
- **Environment-specific issues on Windows.** PowerShell's `curl` alias (it's `Invoke-WebRequest`, which prompts a security warning), creating dotfiles like `.dockerignore`, Git not being installed, and Docker rebuild/refresh workflows required several back-and-forth iterations.
- **Answering the exact question.** On subtle topics (locking, idempotency semantics), first answers were sometimes correct but aimed at an adjacent question, and needed follow-up to get to the precise point.
