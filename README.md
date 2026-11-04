# Orqestra

Distributed AI workload execution platform: concurrent workers, fault-tolerant task
processing, and cost-aware AI orchestration.

Orqestra is a portfolio-grade engineering project. Its first proving workload is
verification of up to 50,000 **synthetic** invoices and purchase orders; invoices are a
demonstration scenario, not the product. The goal is a reliable, reproducible,
well-tested foundation that demonstrates distributed-systems engineering, not an
invoice SaaS.

## Status

Day 1 (repository foundation) was merged in PR #1 and passed GitHub Actions CI. Day 2
Step 1 (the PostgreSQL persistence foundation) was merged in PR #2; both CI lanes, including
the PostgreSQL integration lane, passed on GitHub for the pull request and for `main` after
the merge. Day 2 Step 2A (transactional job submission and job and task reads) was merged
in PR #3, with both CI lanes passing for the pull request and for `main`. Day 2 Step 2B
(the transactional attempt lifecycle and derived job status) was merged in PR #4, with both
CI lanes passing for the pull request and for `main`. Day 3A Slice 1 (PostgreSQL task
discovery) was merged in PR #5, with both CI lanes passing for `main` (run 40209662634).
Day 3A Slice 2 (attempt leases, execution deadlines, heartbeats and recovery operations,
with migration 0002) was merged in PR #6, with both CI lanes passing for `main` (run
40260902718). Day 3A Slice 3 (bounded cancellation grace periods, with migration 0003)
was merged in PR #7 (merge commit `d83afeb`), with both CI lanes passing for the pull
request (run 40271111779) and for `main` after the merge (run 40271850302). Day 3B Slice
1A (job-kind-filtered claims) was merged in PR #8 (merge commit `46acdd9`), with both CI
lanes passing for the pull request (run 40397331066) and for `main` (run 40397973812).
Day 3B Slice 1B-A (worker contracts: the handler protocol and registry, handler-outcome
mapping and validated worker settings) was merged in PR #9 (merge commit `823530a`), with
both CI lanes passing for the pull request (run 40408021385) and for `main` (run
40408731149). Day 3B Slice 1B-B1 (the first single-slot worker runtime, with a local
watchdog) was merged in PR #10 (merge commit `d498698`), with both CI lanes passing for the
pull request (run 40487174691) and for `main` (run 40488034469). Day 3B Slice B2a-1
(reconciling a completion whose answer was lost) was merged in PR #11 (merge commit
`278cb74`), with both CI lanes passing for the pull request (run 40520647309) and for `main`
(run 40524368913). Day 3B Slice B2a-2 (cooperative cancellation) was merged in PR #12
(merge commit `308bb0a`), with both CI lanes passing for the pull request (run
40598713849) and for `main` (run 40599609791). Day 3B Slice B2b (graceful, bounded
shutdown) was merged in PR #13 (merge commit `155d319`), with both CI lanes passing for
the pull request (run 40669564020) and for `main` (run 40671619033). A recovery
correction for workers frozen inside a database transaction (DEC-050) was merged in PR #14
(merge commit `0eab699`), with both CI lanes passing for `main` (run 40713618712). Day 3B
Slice B3 (`orqestra worker run`, DEC-051) was merged in PR #15 (merge commit `71fab72`),
with both CI lanes passing for the pull request (run 40760433076) and for `main` (run
40761239383). Day 3C (deterministic task partitioning, DEC-052) was merged in PR #16
(merge commit `da5b4b3`), with both CI lanes passing for the pull request (run
40770878527) and for `main` (run 40771746389). Day 4A durable retry scheduling (DEC-053,
migration 0004) is implemented on PR #17. Its original GitHub Actions run passed both CI
lanes on head `e4aa59b` (run 41538196569); the PR's Checks tab records later runs.
There is a command that runs a worker with diagnostic handlers only, and an invoice batch
handler that no command registers; there is no service, deployment or production workload
registration. No benchmark has been run. The planned column lists work that is not
implemented; do not read it as a claim about shipped behaviour.

| Area | Implemented | Planned (not started) |
| --- | --- | --- |
| Package and toolchain | `uv` lockfile, Ruff, mypy strict, pytest with coverage, `make check` and `make verify` | — |
| Domain contracts | `TaskState` and `AttemptState` with explicit transition tables; `TransientError` / `PermanentError` taxonomy; failure classification; retry and cancellation policy functions, including for revoked attempts and cancellation grace periods; `LeasePolicy` and the lease, deadline and revocation rules; `JobSubmission`, `TaskSpec`, `Job`, `Task` and `Attempt` records; typed attempt outcomes; derived job status rules; deterministic partitioning of an ordered item list into fixed-size batch tasks (DEC-052); `RetryPolicy` and a bounded exponential retry delay with deterministic jitter (DEC-053, PR #17) | — |
| Configuration and logging | Validated `ORQESTRA_*` settings with secret redaction; required, validated `ORQESTRA_WORKER_*` timing settings with no defaults; single-line JSON logs with job/task/attempt IDs | — |
| CLI | `orqestra version`, `orqestra health`, `orqestra migrate`, `orqestra worker run` with built-in diagnostic handlers only (DEC-051) | job submission and status |
| Persistence | PostgreSQL schema for jobs, tasks and attempts with named constraints and an ownership-generation (fencing) guard; forward-only migration runner with checksums, advisory locking and rejection of transaction control; atomic job submission with idempotency keys; job, task and attempt reads; starting an identified task's attempt; discovering and atomically claiming the next queued task with `FOR UPDATE SKIP LOCKED`; generation-checked completion that rejects stale and duplicate results; bounded retries; cooperative cancellation writes; derived job progress; attempt leases and non-renewable execution deadlines set at claim; generation-fenced lease renewal; revocation of expired attempts; bounded cancellation grace periods; quiescent-upgrade migration 0002, additive migration 0003 and additive migration 0004 (`tasks.available_at`, PR #17); concurrency tests between separate connections | — |
| Queue and workers | PostgreSQL is the queue: task discovery without a broker, filtered by job kind (DEC-033); the operations a worker runtime needs to renew leases, recover expired attempts and enforce cancellation grace periods (DEC-034 … DEC-041); worker contracts: the handler protocol, the job-kind handler registry, handler-outcome mapping and validated worker settings (DEC-042 … DEC-044); a first single-slot worker runtime: claims, a handler thread, heartbeats, recovery sweeps while idle or busy, completion, and a local watchdog that ends the process when ownership cannot be established or a database call stalls (DEC-045, DEC-046); reconciliation of a completion whose answer was lost, from PostgreSQL's evidence (DEC-047); cooperative cancellation: a handler is told of a confirmed cancellation request through its read-only signal (DEC-048); graceful, bounded shutdown on SIGTERM or SIGINT: no new work, the running task drained within `shutdown_timeout`, otherwise exit 9 (DEC-049); durable retry scheduling: a requeued task waits a bounded, jittered delay, stored in PostgreSQL, before any claim may take it (DEC-053, PR #17); independent single-slot processes and no Orqestra-owned service (DEC-054, owner-approved direction); subprocess integration coverage for a handler exceeding its execution deadline, worker fencing, and `deadline_exceeded` recovery (PR #18) | — |
| Invoice rules and AI adapters | Strict normalization of a synthetic invoice and two arithmetic checks; a provider contract with a deterministic mock; pure cost-aware route choice; a batch handler built by a factory and not registered with `orqestra worker run` (DEC-055, Proposed) | a live provider adapter; a usage and cost ledger |
| Aggregation, cost ledger, API | An in-process summary of an invoice job's stored item results, read from one consistent snapshot, with anomaly reporting and a strict completeness rule (DEC-056, Proposed) | a usage and cost ledger; an API (framework undecided) |
| CI | Fast lane (format, lint, type check, unit tests) and PostgreSQL `verify` lane, both passing on GitHub since PR #2 | security scans, load lane |
| Containers | — | Docker Compose for PostgreSQL (local development only) |

## Quickstart

Requires [uv](https://docs.astral.sh/uv/) and, for persistence, PostgreSQL 16. Python
3.12 comes from `.python-version`; a uv-managed interpreter is recommended.

```sh
uv sync --locked --managed-python   # install locked dependencies into .venv
make check                          # format, lint, mypy strict, unit tests
uv run orqestra version
uv run orqestra health              # loads configuration, prints JSON; secrets are redacted
```

Configuration is read from `ORQESTRA_*` environment variables and, in development,
from a local `.env` file. See `.env.example` for every setting.

### Database

Orqestra stores jobs, tasks and attempts in PostgreSQL. `orqestra migrate` applies the
packaged schema migrations to `ORQESTRA_DATABASE_URL`; it is safe to re-run and never
creates, drops or resets a database.

```sh
createdb orqestra
export ORQESTRA_DATABASE_URL=postgresql:///orqestra
uv run orqestra migrate
# {"status": "ok", "applied": ["0001_initial_schema", "0002_attempt_leases", "0003_cancellation_grace", "0004_task_availability", "0005_provider_usage"]}
uv run orqestra migrate             # again: {"status": "ok", "applied": []}
```

### Submitting jobs

Job submission is a Python interface for now; there is no CLI command or HTTP API for it
yet. `submit_job` stores a job and all of its tasks in one transaction, so either the
whole submission is stored or none of it is.

```python
import psycopg

from orqestra.domain.jobs import JobSubmission, TaskSpec
from orqestra.persistence.jobs import get_task, list_tasks, submit_job

submission = JobSubmission(
    kind="invoice_verification",
    tasks=(
        TaskSpec("invoice-1", {"invoice_id": "INV-1"}, max_attempts=3),
        TaskSpec("invoice-2", {"invoice_id": "INV-2"}, max_attempts=3),
    ),
    idempotency_key="batch-2026-10-25",
)
with psycopg.connect("postgresql:///orqestra", autocommit=True) as conn:
    result = submit_job(conn, submission)
    tasks = list_tasks(conn, result.job.id)  # ordered by task_key
    print(result.created, [t.state for t in tasks], get_task(conn, tasks[0].id))
```

With an `idempotency_key`, repeating an identical submission returns the original job
with `created=False` and stores nothing new, including when the copies arrive
concurrently. Reusing the key for different content raises `IdempotencyConflictError`.
"Identical" means the same `kind` and the same tasks by `task_key`, each with an equal
payload (PostgreSQL `jsonb` equality) and retry limit; task order does not matter.
Without a key, every call creates a new job. Invalid values (a blank `kind` or
`task_key`, a retry limit outside 1–100, a NUL character or a NaN in a payload) raise
the database's own error, which names the violated constraint where there is one, and
nothing is stored.

`submit_job` commits its own transaction, so the connection must not be inside one: it
raises `RuntimeError` if the caller has a transaction open. On a connection that is not in
autocommit mode, reads leave an implicit transaction open; commit or roll back before
submitting again.

### Partitioning items into tasks

`partition` (DEC-052, local only) turns an ordered list of JSON items into the tasks of
one job, in contiguous batches of `batch_size`:

```python
from orqestra.domain.jobs import JobSubmission
from orqestra.domain.partitioning import partition

tasks = partition(items, batch_size=100, max_attempts=3)  # 100 is an example, not advice
result = submit_job(conn, JobSubmission("invoice_verification", tasks, idempotency_key="run-42"))
```

- **Order and repeats.** Batches are contiguous slices in input order; the last may be
  shorter. Repeated items are kept, never deduplicated.
- **Keys.** `"<key_prefix>-<index>"` (prefix `"batch"` by default), with the index
  zero-padded to the width of the last one (`batch-00` … `batch-10`), so `list_tasks`
  returns the batches in order. The prefix must not be blank or contain NUL.
- **Payloads.** `{"offset": <index of the batch's first item>, "items": [...]}`. Items are
  not copied: treat them as read-only, like any payload.
- **Determinism.** The same items and settings always give equal tasks, so a resubmission
  with the same idempotency key replays. PostgreSQL compares the resulting tasks, not the
  settings: a different `batch_size` that yields the same tasks is still a replay, and
  one that yields different tasks raises `IdempotencyConflictError`.
- **Refusals.** No items, a `str` or `bytes` instead of a list, or a `batch_size` that is
  not an integer of at least 1 (`True` and `2.0` included) raise `ValueError` or
  `TypeError`. `max_attempts` is passed through; the database enforces 1–100.
- **Not decided yet.** There is no production batch size, and no limit on the number of
  items, the batch size or a payload's size; these wait for measurement (BS-05). A job's
  batches are claimed in no particular order (DEC-033).

### Running attempts

A worker claims the next queued task (or starts a task whose ID it already knows) with a
lease policy, does the work outside any database transaction while renewing the lease, and
reports the outcome with the attempt's generation. The single-slot worker runtime runs
these operations in a loop: it claims, renews leases on a schedule and sweeps for expired
attempts (see [Worker runtime](#worker-runtime-first-slice) and
[Running a worker](#running-a-worker)). There is no production worker service yet. The
example below shows the operations themselves.

```python
from datetime import timedelta
from typing import Any

import psycopg

from orqestra.domain.jobs import AttemptFailed, AttemptOutcome, AttemptSucceeded, JsonValue, Task
from orqestra.domain.leases import LeasePolicy
from orqestra.domain.retry import RetryPolicy
from orqestra.persistence.jobs import get_job_progress
from orqestra.persistence.lifecycle import complete_attempt, start_attempt

# Example values only: Orqestra has no default lease or retry policy.
POLICY = LeasePolicy(lease=timedelta(seconds=30), execution_timeout=timedelta(minutes=10))
RETRY = RetryPolicy(base_delay=timedelta(seconds=1), max_delay=timedelta(minutes=5))


def verify(payload: JsonValue) -> JsonValue:
    return {"verified": True}  # the task's work: runs outside any database transaction


def run(conn: psycopg.Connection[Any], task: Task) -> None:
    attempt = start_attempt(conn, task.id, worker_id="worker-1", policy=POLICY)  # queued -> running
    outcome: AttemptOutcome
    try:
        outcome = AttemptSucceeded(verify(task.payload))
    except Exception as exc:
        outcome = AttemptFailed.from_exception(exc)
    done = complete_attempt(
        conn, attempt.id, generation=attempt.attempt_number, outcome=outcome, retry_policy=RETRY
    )
    print(done.task.state, get_job_progress(conn, task.job_id))
```

- `start_attempt` moves a queued task to `running`, advances its generation
  (`attempt_count`) and creates attempt *N* for generation *N*, in one transaction.
  Concurrent callers get exactly one attempt; the others get `TaskNotStartableError`. A
  queued task whose retry time has not come yet raises `TaskNotEligibleError`, a subclass
  carrying that `available_at`, and changes nothing (see [Retry backoff](#retry-backoff)).
- Every claim takes a required `LeasePolicy(lease, execution_timeout)`: positive
  `timedelta`s, with the lease no longer than the timeout. The attempt gets a lease and a
  non-renewable deadline, both measured from one claim instant on the database clock, read
  after the task lock is held, so waiting for that lock does not shorten the lease.
- `claim_next_attempt(conn, worker_id, kinds=..., policy=...)` finds the next queued,
  eligible task whose job kind is one of `kinds` and claims it the same way, returning its
  attempt, or `None` when nothing of those kinds is claimable right now. `kinds` is required: a
  non-empty collection of strings, matched exactly (no case folding, trimming or
  wildcards). A bare string, an empty collection, a kind of only spaces or a kind with a
  NUL character is refused before anything is sent to the database; there is no
  unfiltered claim. "Blank" follows the database rule for job kinds (`btrim`, which trims
  spaces only), so a kind containing a tab or a newline is valid and must be named exactly.
  Tasks of other kinds are
  never locked or claimed: they stay queued with their attempt budget intact and do not
  hold up the kinds a claimant supports. It locks
  with `FOR UPDATE SKIP LOCKED`, so it never waits for another claimant or a cancellation,
  and concurrent claimants always get distinct tasks. Tasks are preferred by
  `created_at`, then `id`. This is not strict FIFO: tasks locked by others are skipped, and
  tasks of one job, which share `created_at`, are taken in `id` order.
- `complete_attempt` takes effect only for the task's current, still-running generation:
  - A newer generation raises `StaleAttemptError`.
  - A repeated completion raises `AttemptFinishedError`.

  Neither changes any record. The next task state comes from the lifecycle policy:
  - A success is final, and its result becomes the task's result.
  - A transient failure requeues the task while attempts remain, eligible again only
    after its retry delay, computed from the required `retry_policy`.
  - A permanent or unclassified failure, or exhausted retries, fail the task.
- `renew_lease(conn, attempt_id, generation=..., worker_id=..., policy=...)` is the
  heartbeat: it extends the lease by `policy.lease` from now, never past the deadline and
  never shorter. It locks only the attempt row. It rejects a wrong generation
  (`StaleAttemptError`), another worker (`WorkerMismatchError`), a revoked attempt
  (`AttemptRevokedError`), a finished one (`AttemptFinishedError`), an expired lease
  (`LeaseExpiredError`, with the reason) and a task whose cancellation was requested
  (`CancellationRequestedError`, with the end of the grace period). A worker that sees
  any of these must stop; after a cancellation request it should complete the attempt as
  cancelled before its lease ends.
- `revoke_next_expired(conn, retry_policy=...)` revokes at most one running attempt whose
  lease has expired and records why: `deadline_exceeded` when the lease had reached its
  deadline; otherwise `cancellation_grace_expired` when cancellation was requested while
  the lease still held; otherwise `lease_expired`. A grace expiry cancels the task
  whatever retry budget is left. The other reasons move the task like a transient
  failure: requeued, with a retry delay, while attempts remain, cancelled instead if a
  request is pending, failed once the budget is used up. It returns `None` when it
  revoked nothing. Ownership ends when the revocation commits: a completion that commits
  first still counts, and afterwards the old worker's completion raises
  `AttemptRevokedError` (or `StaleAttemptError` once the task was
  claimed again).
- `request_cancellation(conn, task_id, grace=...)` takes a required, positive grace period
  (a `timedelta`; there is no default). It cancels a queued task at once. On a running
  task it records the request and, if the attempt's lease still holds, sets that lease to
  end one grace period after the request, never past the deadline. This can shorten a
  long lease or lengthen one about to expire. From then on heartbeats are refused. Within
  the grace period the attempt may still complete: a success stands, and a transient
  failure cancels instead of requeueing. Once the grace period has run out, recovery
  revokes the attempt and cancels the task. A repeated request changes nothing and does
  not extend the grace period.
- A succeeded task's result may be JSON `null`; check `task.state` before reading
  `task.result`.
- Job status is derived from task states: `queued` (every task queued), `running`,
  `succeeded` (every task succeeded), `failed` (all finished, at least one failed) or
  `cancelled` (all finished, none failed, at least one cancelled).

### Retry backoff

A requeued task waits before it may run again (DEC-053). The wait is stored in
PostgreSQL, so it holds across worker crashes, restarts and every worker:

- Every task has `available_at`, the database time from which it may be claimed. A new
  task is eligible at once.
- A requeue sets it: a transient failure with attempts left, or a `lease_expired` or
  `deadline_exceeded` revocation with attempts left and no cancellation request. The
  task becomes eligible at the database clock at the requeue (read after the locks) plus
  `retry_delay(policy, task_id, n)`, where *n* is the ended attempt's generation.
- The delay is `min(max_delay, base_delay * 2**(n-1))`, scaled by a jitter factor in
  `[0.5, 1]` taken from a SHA-256 digest of the task's ID and *n*. The same task and
  attempt always get the same delay, in any process; different tasks spread out. It is
  never shorter than half the capped value and never longer than `max_delay`.
- Nothing else moves it: a success, a permanent or unclassified failure, exhausted
  retries and any cancellation (including a grace expiry) schedule no retry. A
  cancellation request on a task waiting out its delay cancels it at once, like any
  queued task.
- Both claims check `available_at <= statement_timestamp()` inside the atomic claim, so
  the boundary is inclusive and a refused claim changes nothing. Among eligible tasks the
  order is unchanged (`created_at`, then `id`), so once its delay has passed a retried
  task goes ahead of newer ones.
- `RetryPolicy(base_delay, max_delay)` has no defaults: both are positive, representable
  `timedelta`s and `base_delay` cannot exceed `max_delay`. `complete_attempt` and
  `revoke_next_expired` require one; a worker builds it from two required settings.
  **Every worker must use the same values. Nothing checks this:** the process that
  records the requeue (the worker completing the attempt, or any worker whose recovery
  sweep revokes it) decides the delay.
- `available_at` is not part of the `Task` record; it is a column for diagnostics.

### Task handlers

A worker runs these handlers (see "Worker runtime" below).

```python
from orqestra.domain.jobs import JsonValue
from orqestra.worker.handlers import HandlerCancelled, HandlerRegistry, TaskContext


def verify_invoice(context: TaskContext, payload: JsonValue) -> JsonValue:
    if context.cancellation.is_set():  # set once the worker has seen a cancellation request
        raise HandlerCancelled
    return {"verified": True}  # any JSON value, including None (JSON null)


registry = HandlerRegistry({"invoice_verification": verify_invoice})
# claim_next_attempt(conn, worker_id, kinds=registry.kinds, policy=...)
```

- A handler is a synchronous function of a `TaskContext` (task and attempt IDs, attempt
  number, job kind and a cancellation signal) and the task's payload. It gets no database
  connection. It may raise `TransientError` or `PermanentError`. The signal is a
  `CancellationSignal`: a read-only view of a `threading.Event` that only the worker holds
  and sets, after it has seen a committed cancellation request. A context refuses the
  event itself, and the view can report and wait but cannot set, clear or swap the event.
  Python cannot stop deliberate introspection, so the database stays the authority: an
  attempt is recorded as cancelled only after a committed request.
- `HandlerRegistry` maps exact job kinds to handlers. It needs at least one kind, checks
  kinds with the claim's own rule, refuses non-callable, coroutine and generator
  functions, is immutable, and has no fallback for other kinds. `registry.kinds` is what
  a worker passes to `claim_next_attempt`.
- `run_handler(handler, context, payload)` turns what a handler did into one outcome,
  without writing anything:
  - a valid JSON result: `AttemptSucceeded` holding a checked copy (new lists and dicts,
    the same keys, order and scalar values), equal to the result as JSON but not the
    handler's object, so changing that object afterwards cannot change the outcome;
  - an invalid result (anything but `None`, `bool`, `int`, finite `float`, `str`, `list`
    and `dict` with `str` keys, exact types; a NUL character or unpaired surrogate in a
    string; a cycle; nesting too deep to check): an unclassified `AttemptFailed` with
    `error_type` `orqestra.worker.handlers.InvalidHandlerResult`. A returned coroutine or
    generator is reported, never run; an unstarted coroutine is closed, which runs none
    of its code and prevents a "never awaited" warning;
  - an exception: `AttemptFailed.from_exception`, as before; `HandlerCancelled` is
    `AttemptCancelled` only if the cancellation signal was set;
  - `SystemExit`, `KeyboardInterrupt` or any other `BaseException` that is not an
    `Exception`: a `ControlPlaneSignal`, for which no outcome may be recorded (DEC-009).

  NUL characters and unpaired surrogates in failure text are stored as escapes
  (`\u0000`, `\ud800`). Database errors are not handled here: they happen when the
  outcome is recorded.
- `WorkerSettings` reads the worker's timing settings from `ORQESTRA_WORKER_*` variables
  (durations as ISO 8601, such as `PT30S`). Every value is required; there are no
  defaults. It checks the lease rules, that a database call ends before the next
  heartbeat is due, that `heartbeat_interval + db_operation_timeout + fence_margin` is
  shorter than the lease, and that `lock_timeout` is at least 1 ms and, in the whole
  milliseconds PostgreSQL uses (rounded up), strictly shorter than
  `db_operation_timeout`; so `db_operation_timeout` is at least 2 ms. `retry_base_delay`
  and `retry_max_delay` form its `retry_policy` (base no longer than max). The worker will use
  the existing `ORQESTRA_DATABASE_URL`.

### Worker runtime (first slice)

`Worker` runs one task at a time in one process. `orqestra worker run` starts one (see
[Running a worker](#running-a-worker)); from Python:

```python
import functools

from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.shutdown import run_until_signalled

settings = WorkerSettings()  # every ORQESTRA_WORKER_* value is required
worker = Worker(
    registry=registry,  # a HandlerRegistry, as above
    settings=settings,
    connect=functools.partial(connect_supervisor, database_url, settings),
)
run_until_signalled(worker)  # on the main thread; returns after SIGTERM or SIGINT
```

Without signals, `worker.run()` works until `worker.request_stop()` is called from
any thread.

- **Supervisor and handler thread.** The calling thread is the supervisor. It alone
  uses one autocommit connection. It claims a task of a supported kind
  (`kinds=registry.kinds`), reads its payload, and runs its handler on a separate thread
  with no database connection. It never claims again until that task is settled.
- **The supervisor's server-side bounds** (DEC-050), in whole milliseconds rounded up:
  - `statement_timeout` = `db_operation_timeout`;
  - `lock_timeout` = `lock_timeout`, shorter: a supervisor statement waiting for a row
    lock ordinarily gets `LockNotAvailable` (`55P03`) and the existing path retries it
    (an unconfirmed heartbeat, a completion retried, a failed sweep or claim) instead of
    being ended by the watchdog with exit 4. This is not a timing guarantee: the
    watchdog bounds the whole call, so earlier statements, the network and scheduling can
    use up the difference, and the watchdog may still end the process first;
  - `idle_in_transaction_session_timeout` = `db_operation_timeout`: if the worker is
    frozen, killed or cut off between two statements of a transaction, PostgreSQL ends
    its session, rolls the transaction back and releases its locks, so recovery can
    revoke its attempt.

  Both are needed. Before them, a worker frozen inside its heartbeat transaction kept
  its attempt's row lock indefinitely, and every worker that tried to recover that
  attempt waited for the lock until its own watchdog ended it (exit 4).
- **Schedule.** While a handler runs, the supervisor renews the lease every
  `heartbeat_interval` and runs recovery sweeps (at most `recovery_batch_limit`
  revocations every `recovery_interval`), idle or busy. A sweep step starts only if it
  can take its full `db_operation_timeout` and still end before the next heartbeat is
  due, so sweeps never delay a heartbeat. Idle, it claims every `poll_interval`.
- **Local fence.** A watchdog thread ends the process (`os._exit`) when either guard
  expires:
  - **ownership:** the attempt's lease, mapped to the local clock from the claim so it
    is never later than the real lease end, minus `fence_margin`. It moves later only
    after a *confirmed* renewal, earlier when a cancellation request shortens the lease,
    and is disarmed by the handler thread when the handler's work ends.
  - **operation:** each supervisor database call must end within
    `db_operation_timeout`. A stalled call (a frozen server or network still answers
    TCP, so `statement_timeout` never fires) ends the process instead of hanging it.

  The supervisor also ends the process itself when a heartbeat shows the lease can no
  longer be renewed (`LeaseExpiredError`, `AttemptRevokedError`, `StaleAttemptError`,
  `WorkerMismatchError`, `AttemptFinishedError`) while the handler still runs.
- **Cooperative cancellation** (DEC-048). Each attempt has its own cancellation event,
  unset when its handler starts. The handler can only read it, through
  `context.cancellation` (`is_set()`, `wait(timeout)`). When a heartbeat raises
  `CancellationRequestedError`, which means a request has *committed*, the supervisor:
  1. stops renewing;
  2. moves the fence to the end of the grace period;
  3. only then sets the event.

  A handler that sees it and raises `HandlerCancelled` ends the attempt and the task as
  cancelled. Within the grace period a success still stands, a transient failure cancels
  the task, and a permanent failure fails it (DEC-020). A handler that ignores the signal
  is fenced at the end of the grace period, and recovery then cancels the task. Nothing
  else sets the event: not an unconfirmed heartbeat, a lost connection, or lost
  ownership. A `HandlerCancelled` raised without a confirmed request remains an
  unclassified failure.

  **Notice arrives only with a heartbeat, so it is not guaranteed before the grace period
  ends.** A grace shorter than the time to the next heartbeat ends the worker (fenced)
  with no notice, and so does a database outage.
- **Recording outcomes.** The handler's outcome is recorded with `complete_attempt`. A
  completion still counts after the lease expired, until recovery commits.
  - An outcome rejected because ownership ended (`AttemptRevokedError`,
    `StaleAttemptError`) is discarded.
  - Database errors are never handler failures. A rolled-back retryable error
    (`40001`, `40P01`, `55P03`, `57014`) or a failed connect is retried until the
    attempt's deadline. Any other rejection of a valid outcome ends the process.
  - **A completion whose answer is lost** (the connection broke, so it may or may not
    have committed) is **reconciled** (DEC-047). The worker keeps the handler's outcome,
    reconnects, and reads the attempt and its task in one statement
    (`read_completion_evidence`):

    | Evidence | Meaning | Action |
    | --- | --- | --- |
    | it ended with exactly this outcome | the completion committed | settle |
    | it is still running | nothing committed | write the same outcome again |
    | it was revoked | recovery won | discard the outcome |
    | anything else | an invariant broke | end the process (exit 7) |

    - **Exact matching:** results are compared with PostgreSQL's `jsonb` equality, so
      `1e308` matches the number `jsonb` stores and `true` does not match `1`. Failures
      must have exactly the same kind, type and message.
    - **Re-read on rejection:** a rewrite that is rejected is decided by reading the
      evidence again, never by the error alone.
    - **The handler never runs again** for that attempt, and the worker then carries on.
    - **Bounded:** reads and rewrites that fail are retried, spaced by
      `db_operation_timeout`, until the attempt's execution deadline. If it is still
      unresolved then, the process ends (exit 6).
- **Graceful shutdown** (DEC-049).
  - **Stop requests.** `request_stop()` sets a flag and writes one byte to the
    worker's wake pipe; it takes no lock. `run_until_signalled` (POSIX, main thread
    only) turns the first SIGTERM or SIGINT it processes into `request_stop()`; any
    later one ends the process at once (exit 9, `shutdown_forced`). It refuses to
    start if another wakeup fd is set or a previous handler cannot be restored, and
    restores the previous handlers and wakeup fd however `run` ends.
  - **The latch.** The supervisor latches a request at its next step: the wake pipe
    ends any wait, and a step that makes several database calls (a claim and its task
    and job reads; an evidence read and its rewrite) looks for a stop after each one, so
    the lag is at most the one database call in flight. From the latch on it sends
    no claim, starts no handler and starts no sweep. The latch, not the request, is the
    linearization point: a handler that started after a request was made but before
    the latch was already running, and it is drained like any other.
  - **Draining.** A task held at the latch keeps its heartbeats, completion and
    reconciliation. Once it is settled (recorded, reconciled, or discarded because
    recovery revoked it), `run` returns and the process exits 0. The watchdog's drain
    guard, armed at `latch + shutdown_timeout`, ends the process with exit 9 otherwise,
    even if the supervisor is blocked in a database call. With `shutdown_timeout = 0`
    an idle worker stops at once and a busy one exits 9 at once.
  - **Nothing is fabricated.** Shutdown never sets the cancellation event, never
    records a failure or a cancellation, and writes nothing. An abandoned attempt
    stays `running` until its lease ends; recovery then revokes it (`lease_expired`),
    using one attempt of its budget, so a task on its last attempt fails.
  - **A claim that never started.** A claim whose answer arrives after the latch is
    not started. The worker still exits 0, and logs `stopped_with_unstarted_claim`
    with the attempt and task IDs and the lease end; a claim whose answer was lost is
    logged as `stopped_with_uncertain_claim`. **Exit 0 means every handler the worker
    started has ended as PostgreSQL confirmed; it does not mean no attempt of this
    worker is still `running`.**
  - **Never a false success.** `run` returns only through this controlled stop. An
    exception while a handler runs ends the process (exit 3); `SystemExit` with a
    success code is raised as `UncontrolledExitError`, so any other end is nonzero.
- **Exit codes:**

  | Code | Meaning |
  | --- | --- |
  | 0 | Controlled stop: every started handler ended as PostgreSQL confirmed (see above) |
  | 1 | `orqestra worker run` refused to start (one JSON line on stdout), or an unexpected exception in the supervisor (Python's own exit, with a traceback) |
  | 2 | `orqestra worker run` was called without `--kind` (argparse's usage error) |
  | 3 | Fenced: ownership could not be established |
  | 4 | Stalled: a database call outlived its bound |
  | 5 | Handler aborted: a control-plane signal, nothing recorded |
  | 6 | Whether a completion committed could not be established by the attempt's deadline |
  | 7 | Defect |
  | 8 | Outcome unrecorded before the deadline |
  | 9 | Shutdown: the task was not settled by the drain deadline, or a second signal came |

  Each exit writes one JSON line with its `event`, best effort: if stderr is closed or a
  broken pipe, the process still exits with its code. Worker events are logged as JSON
  with `worker_id`, `task_id`, `attempt_id` and `event`.

### Running a worker

`orqestra worker run` starts one single-slot worker on the main thread and runs it until
SIGTERM or SIGINT (DEC-051):

```sh
uv run orqestra worker run --kind diagnostic.echo --kind diagnostic.wait
```

Each invocation is one worker process with one handler slot. To run N workers, start N
separate invocations with the same database and `ORQESTRA_WORKER_*` settings. Orqestra
does not supervise or restart them; the operator owns restart policy and must account for
the exit codes above.

- **Kinds.** `--kind` is required and may be repeated. Each one must name a handler in
  the fixed, code-reviewed registry, matched exactly; nothing is imported dynamically.
  There is no production workload yet, so the only handlers are two diagnostics for
  development and tests:
  - `diagnostic.echo` returns its payload unchanged;
  - `diagnostic.wait` takes exactly `{"seconds": n}`, where `n` is a finite,
    non-negative number (not a boolean) no longer than `execution_timeout`. It waits on
    its cancellation signal, returns `{"waited_seconds": n}`, or ends cancelled if told
    of a committed request. Any other payload is a permanent failure.
- **Configuration.** The `ORQESTRA_*` settings, including `ORQESTRA_DATABASE_URL`, and
  all twelve required `ORQESTRA_WORKER_*` settings, with no defaults. The values below are
  test examples only, not recommendations (DEC-034):

  ```sh
  ORQESTRA_WORKER_LEASE=PT4S
  ORQESTRA_WORKER_EXECUTION_TIMEOUT=PT5M
  ORQESTRA_WORKER_HEARTBEAT_INTERVAL=PT1S
  ORQESTRA_WORKER_DB_OPERATION_TIMEOUT=PT0.8S
  ORQESTRA_WORKER_LOCK_TIMEOUT=PT0.3S
  ORQESTRA_WORKER_FENCE_MARGIN=PT1S
  ORQESTRA_WORKER_POLL_INTERVAL=PT0.1S
  ORQESTRA_WORKER_RECOVERY_INTERVAL=PT1S
  ORQESTRA_WORKER_RECOVERY_BATCH_LIMIT=5
  ORQESTRA_WORKER_SHUTDOWN_TIMEOUT=PT1S
  ORQESTRA_WORKER_RETRY_BASE_DELAY=PT0.1S
  ORQESTRA_WORKER_RETRY_MAX_DELAY=PT0.5S
  ```

  Every worker sharing a database must use the same two retry values; nothing checks this.
- **Startup checks, in order.** Each refusal prints one JSON line on stdout and exits 1,
  before any worker exists, so nothing is claimed:
  1. the settings validate (errors list each field's location and message, never its
     input);
  2. every `--kind` is a built-in handler, and none is `diagnostic.*` when
     `ORQESTRA_ENVIRONMENT=production`;
  3. `ORQESTRA_DATABASE_URL` is set and parses (a malformed one is reported
     generically);
  4. the database is reachable, and its schema is current. The check is read-only (one
     `READ ONLY` transaction of `SELECT`s, no migration lock) and requires the database's
     migration history to equal the packaged migrations exactly: an uninitialized, older,
     newer, renamed or edited history is refused. Database errors are reported by type
     and SQLSTATE only.

  There is no retry at startup: a database that is down is a refused start. Once
  running, the worker keeps its normal behaviour during outages.
- **Running.** After the checks it runs exactly as `run_until_signalled` does: exit 0
  after a controlled stop, the worker's exit codes otherwise (see above).
- **Submitting work** is the Python `submit_job` interface; there is no submission
  command.
- **Production restriction.** `diagnostic.*` kinds are refused only when
  `ORQESTRA_ENVIRONMENT` is `production`. An unset `ORQESTRA_ENVIRONMENT` defaults to
  `development`, so a production deployment must set it.

### Invoice verification (synthetic)

`orqestra.workloads.invoice` turns each synthetic invoice into one of four statuses. The
handler takes the partitioner's batch payload and is built by a factory; it is not
registered with `orqestra worker run`, and no migration, API or CLI change goes with it.
The schema and the two rules are a synthetic demonstration contract (DEC-055, Proposed),
not an accounting standard.

```python
from datetime import timedelta

from orqestra.ai.mock import MockProvider
from orqestra.ai.routing import RouteOption
from orqestra.workloads.invoice.handler import JOB_KIND, invoice_verification_handler
from orqestra.workloads.invoice.verify import REQUIRED_CAPABILITY

routes = [RouteOption("route-a", frozenset({REQUIRED_CAPABILITY}), estimated_cost_units=3)]
handler = invoice_verification_handler(
    {"route-a": MockProvider(["5"])}, routes, timeout=timedelta(seconds=30)
)
# HandlerRegistry({JOB_KIND: handler}); payload {"offset": 0, "items": [invoice, ...]}
```

- **Input.** Each item is an object with exactly `invoice_id`, `lines` (a non-empty list of
  exactly `line_id`, `quantity` and `unit_amount`) and `total`. Identifiers are 1 to 64
  characters of `[A-Za-z0-9._-]`, and `line_id` values are unique. An amount is an integer
  below 10^64 in unspecified minor units, never a `bool` or float, given as an `int`, as an
  ASCII digit string (`[0-9]{1,64}`), or as an *ambiguous* string: any other non-blank,
  printable string of up to 64 characters. There is no tax, currency, rounding or vendor rule.
- **Statuses.**
  - `malformed`: the item does not fit the schema (including an empty or blank amount).
  - `rejected`: a rule failed. `quantity_positive` fails on a zero quantity;
    `total_matches_lines` fails when the total is not the sum of quantity times unit amount.
  - `verified`: every rule passed.
  - `needs_review`: an ambiguous amount could not be settled: no route was eligible
    (`no_eligible_route`), or the provider's reply was not acceptable
    (`provider_response_invalid`).
- **Deterministic first, provider only for ambiguity.** The checks run before anything else.
  A `malformed` or `rejected` item never reaches a provider. Otherwise, if an amount is an
  ambiguous string, one route is chosen for the invoice and its provider is asked, once per
  ambiguous amount, to read just that string. The reply must be a digit string
  (`[0-9]{1,64}`); the same parser and the same rules then decide the status, and the result
  is marked `provider_assisted`. A provider never states a verdict. A provider could still
  return a plausible number that makes the arithmetic hold, so treat `provider_assisted`
  results as less trustworthy. A provider sees only the string, its field kind and the route,
  never an identifier or a path.
- **Routing.** `choose_route` is pure. A route is eligible if it declares `interpret_amount`,
  has an integer estimate (`None` is never read as zero) and the estimate does not exceed the
  optional `max_cost_units`. The lowest `(estimate, route_id)` wins, whatever the order of
  the routes. Estimates are opaque, non-negative integers supplied by the caller: there is no
  currency, price table or vendor assumption, and an estimate is not accounting.
- **Stored result.** `{"offset": n, "results": [...]}`, one result per item:
  `{"index", "status", "provider_assisted", "findings": [{"rule", "verdict", "path"}],
  "issues": [{"code", "path"}], "reason", "routing": {"route_id"} | null}`. Paths are
  positions such as `lines[2].unit_amount`. No raw value, identifier, provider text,
  reported usage, estimate or call count is stored. `routing` says which route the
  decision chose, for the attempt that completed; it is an audit, not usage accounting.
- **Failures and retry.** A payload that is not `{"offset": int, "items": list}` is a
  permanent failure (`batch_payload_invalid`). A provider error fails the whole batch task
  with a fixed code: `timeout`, `rate_limited` and `unavailable` are transient and retried
  with the durable backoff; `rejected` and `unsupported_request` are permanent. Any other
  provider exception, or a reply that is not a `ProviderResponse`, is a permanent
  `provider_protocol_violation`; the provider's own text is never stored. A retry runs the
  whole batch again, so calls that already succeeded are repeated.
- **Job status.** `malformed`, `rejected` and `needs_review` are item results. The task
  still succeeds, so a job in which every invoice was rejected still derives as `succeeded`
  (DEC-031). Aggregation must read the item results.
- **Cancellation.** Checked before each item and before each provider call; a call already
  in flight is not interrupted.
- **Tests offline.** The provider is a scripted mock, and the new unit tests run with a
  guard that makes any connection or name lookup fail. No SDK, credential or network call
  is involved.

### Invoice job summary

A job's status says nothing about its invoices: a job in which every invoice was rejected
is still `succeeded`. `summarize_invoice_job` reads the stored item results and reports
what happened to each item (DEC-056, Proposed). It is a Python interface for now, with no
command, API, table or migration.

```python
import psycopg

from orqestra.aggregation.invoice import summarize_invoice_job

with psycopg.connect("postgresql:///orqestra", autocommit=True) as conn:
    summary = summarize_invoice_job(conn, job_id)  # None if there is no such job
    if summary is not None and summary.complete:
        print(summary.verified, summary.rejected, summary.needs_review, summary.malformed)
    elif summary is not None:
        print(summary.job_status, summary.unprocessed_items, summary.anomaly_samples)
```

- **One snapshot.** The job row and all of its tasks are read in a single
  `REPEATABLE READ, READ ONLY` transaction, so a completion committed meanwhile is not seen
  and the counts describe one coherent past state. The read takes no row locks and does not
  block workers. Tasks are streamed through a server-side cursor in chunks (`fetch_size`),
  and only a bounded description of each payload is fetched, never the payload itself.
  `observed_at` is approximate: the database time of the read's first query. A long read
  holds back vacuum for as long as it lasts.
- **What is counted.** An item is counted as `verified`, `rejected`, `needs_review` or
  `malformed` only if its task succeeded, the stored result matches the task's payload
  (same `offset`, one entry per item), and the entry is valid, inside the task's own range
  and claimed once. Result indices are absolute; the offset is never added.
- **Where the rest goes.**
  - `unprocessed_items`: items of tasks that are queued, running, failed or cancelled.
  - `invalid_items`: items of a batch whose result does not match its payload, and
    indices claimed by an unusable or duplicated entry.
  - `missing_items`: indices of a matching result that no entry claims.
  - `processed + unprocessed + missing + invalid` equals `expected_items`, the sum of the
    payload lengths; it is `None` if any payload is not a batch.
- **Anomalies.** Problems in the data (a payload that is not a batch, a gap or overlap
  between batches, a result that is `null` or the wrong shape, a duplicate, out-of-range or
  unusable entry) are counted by a closed set of codes, with up to `MAX_SAMPLES` samples
  of the code, task key and index. They never contain a value from an invoice, and none is
  raised as an error.
- **`complete`.** True only for a succeeded job with at least one item, every item
  processed, and no anomaly. **Counts are per-task tallies, not a final job tally
  unless `complete` is true.** A complete summary does not change later if the engine's contracts
  hold; the schema does not enforce that, and a summary that is not complete is a
  point-in-time observation.
- **Not a spend figure.** `provider_assisted_items` counts items a provider helped read,
  not provider calls or cost. A retried batch repeats calls, and no usage is stored.
- **Errors.** `None` for a missing job; `WrongJobKindError` for another kind of job (no
  task is read); `ValueError` for a job without tasks; `RuntimeError` if the connection is
  already inside a transaction. A database error propagates and no partial summary is
  returned.

### Verification

```sh
make check      # fast lane: no services; unit-test coverage is reported, not enforced

createdb orqestra_test              # once: dedicated, disposable, local
export ORQESTRA_TEST_DATABASE_URL=postgresql:///orqestra_test
make verify     # full lane: + PostgreSQL integration tests, >=80% combined coverage, package check
```

`make check` alone does not verify persistence. `make verify` is required for persistence
changes and fails if the test database is not configured. Integration tests only run
against a local database named `orqestra_test` and work in throwaway schemas inside it.

### Known limitations

- `orqestra worker run` is a command, not a service: nothing restarts, supervises or
  deploys it, and it can run only the built-in diagnostic handlers. A task whose worker dies is
  recovered only while some worker is running: any live worker sweeps, idle or busy.
  There is no guaranteed recovery delay: it depends on live workers, their sweep
  interval and batch limit, and the backlog of expired attempts.
- Revoking an attempt ends its database ownership only, and does not cancel or un-bill an
  external AI call already made. Usage must be attributed to the revoked attempt when cost
  tracking exists.
- Invoice verification has no live provider and no usage ledger. Provider-reported usage is
  discarded, and the stored `routing` shows only which route was chosen. A provider error
  repeats the whole batch on retry, so billable calls could repeat; deterministic request
  ids would let a real adapter deduplicate them, and no exactly-once claim is made.
- The invoice job summary is in process only: no command or API exposes it, and it
  summarizes one job per call. Its snapshot read grows with the number of tasks and the
  size of their stored results, and holds back vacuum while it lasts; the cost of the
  ordering, the effect of the fetch size and the behaviour under concurrent completions
  are measured by tests that no default lane runs, and no threshold or batch-size limit is
  decided. Counts are per-task tallies, not a final job tally unless `complete`.
- **The worker's local fence is limited.**
  - It cannot stop a handler that holds the GIL in native code: the watchdog thread then
    cannot run either.
  - It cannot stop processes a handler started and detached.
  - It assumes the local and database clocks run at the same rate, with `fence_margin`
    absorbing small differences. A step forward in the database clock can end the real
    lease before the local fence.
  - It has been shown under the tested conditions only: a stalled proxy, refused
    connections, a cancellation request, and a lease ended by the database.
- **The worker runtime is a first slice.**
  - **Cancellation notice:** a handler is told of a request only at a heartbeat. With a
    grace period shorter than the time to the next heartbeat, or during a database
    outage, it is fenced without notice. Handlers that hold the GIL cannot react, and
    nothing interrupts a handler that ignores the signal before the fence.
  - **Unresolved completions:** a completion whose answer was lost, and which cannot be
    reconciled before the attempt's deadline (for example, the database stays
    unreachable), ends the process; recovery or the committed completion then decides.
    The worker does not renew the lease while reconciling, so a long outage can let
    recovery revoke the attempt first, discarding a finished outcome.
  - **Lost claims:** a claim whose answer was lost, or whose task could not then be
    read, is never started and is recovered when its lease expires, using one attempt
    of its budget. The worker claims nothing else until that lease has certainly ended
    (a full `lease` after a lost answer). So a worker process runs at most one handler at
    a time and never holds two attempts whose leases are both valid; but the abandoned
    attempt can still read `running` in the database, beside the worker's next one, until
    recovery revokes it.
  - **Shutdown timing is conditional.** From a signal, the process ends within about
    `db_operation_timeout + shutdown_timeout`, plus exit time, only while the
    watchdog and main threads can run: a handler holding the GIL delays the signal
    handler and the watchdog alike. SIGKILL, and an orchestrator whose kill grace is
    shorter than this, end the process like a crash. Processes a handler started and
    detached survive. The worker's wake pipe (`select.poll`) and the signal helper are
    POSIX only.
  - **Shutdown costs budget when it abandons work.** An attempt left by exit 9, or a
    claim that never started, uses one attempt and is recovered only after its lease
    ends, by a live worker. Handlers are not told about shutdown.
- Partitioning (DEC-052) has no production batch size and no size limits: a whole job,
  however many batches, is still one submission transaction, and replaying it compares
  every task. Bounds wait for the large-scale benchmark (BS-05).
- There are no default or maximum lease values: callers choose them. Operational bounds
  and per-job-kind policies are not decided yet. `WorkerSettings` requires every timing
  value and has no defaults.
- `run_handler` cannot interrupt a handler. The existing `Settings.worker_concurrency`
  (default 4) is validated but not read by anything; the worker is single-slot.
- Handler results are checked more strictly than `jsonb` requires: only exact built-in
  types are accepted (a `tuple`, a `str` enum or a `dict` subclass is refused rather than
  converted), and nesting deeper than about 1,000 levels is refused. Results and failure
  messages have no size limit yet.
- **Lock and idle-transaction bounds (DEC-050) are bounds, not a recovery-time promise.**
  - They protect worker supervisor sessions only. Other clients (a cancellation request
    in progress, a migration, a person in `psql`) can still hold task or attempt locks
    for as long as their sessions allow; workers then retry, and a worker whose heartbeat
    cannot get its lock for longer than its lease fences itself.
  - `idle_in_transaction_session_timeout` ends any supervisor session paused between two
    statements of a transaction for `db_operation_timeout`, whatever the cause. A
    supervisor that is running cannot pause that long inside its transaction without its
    operation guard ending the process first; a paused process (SIGSTOP, a suspended VM
    or container, a debugger) loses its session and its transaction rolls back.
  - Timing still depends on scheduling and the network: the settings relationships do not
    guarantee that a delay cannot outlast the watchdog's deadline.
- A heartbeat proves the worker is alive, not that its work progresses: a stuck handler
  keeps renewing its lease until the deadline, and only then is revoked.
- A database outage longer than the lease makes every live attempt revocable once the
  database returns, and each revocation uses one attempt of its task's budget.
- A retry delay is the earliest time a requeued task may run, not a schedule: it runs
  when a live worker supporting its kind next polls after that time, and behind any
  backlog. A retried task keeps its original `created_at`, so once eligible it is claimed
  ahead of newer tasks. Each claim scans and sorts the queued tasks, including those still
  waiting out a delay; there is no index for it yet (deferred until it is measured).
  `None` from `claim_next_attempt` or `revoke_next_expired` means only that nothing was
  claimed or revoked by that call.
- Retry policy consistency is not enforced: each process uses its own settings, and a
  worker configured differently schedules different delays for the tasks it requeues.
  There are no production retry values (DEC-034), no per-job-kind policy, and the delay
  does not depend on the error (for example, a rate limit's `Retry-After`). A database
  outage longer than the lease still revokes every live attempt when it returns; their
  retries are spread by the jitter only.
- A task whose job kind no claimant supports stays `queued` indefinitely: nothing reports
  or expires it yet. Each claim now also reads the job of the queued tasks it considers;
  how PostgreSQL plans that, and what unsupported backlogs cost, has not been measured.
- The claim reads a job's kind without locking the job, which relies on kinds never
  changing. No code path updates a job's kind, but the database does not forbid it.
- Cancellation is cooperative and bounded by the grace period. A worker learns of a
  committed request only when a heartbeat reports it; it then tells its handler through
  the read-only cancellation signal (DEC-048). Notice is not guaranteed before the grace
  period ends, for example during a database outage or when the grace is shorter than the
  time to the next heartbeat. Nothing can forcibly interrupt a handler that ignores the
  signal: the worker is fenced at the end of the grace period, and the attempt is revoked
  only when a live worker's recovery sweep runs. There are no default or maximum grace
  values, and the heartbeat interval must be shorter than any grace used for a worker to
  hear in time. There is no job-level cancellation.
- A request resets a valid lease to one grace period, so if the worker is already dead,
  recovery waits for the grace period instead of the lease it had left; a grace longer
  than that lease delays recovery.
- Execution is at-least-once only where workers are running. Running workers claim the
  kinds they support and recover expired attempts, and generation fencing records only
  the current generation's result. A task makes no progress while no running worker
  supports its kind or no worker is running at all, so eventual completion is not
  guaranteed. Neither execution nor external effects are exactly-once: a handler may run
  again after recovery.
- Migration 0002 needs a quiescent database: stop workers and claimers, let running
  attempts finish, migrate, then deploy. It refuses while any task or attempt is running
  or any session is using those tables. Code from before 0002 cannot run against the new
  schema: it cannot read revoked attempts, and its claims are rejected.
- Migration 0003 is additive and may run while attempts are running, but it fails at once
  while any session uses `attempts`. Code from before 0003 must not run once the new code
  can write `cancellation_grace_expired`: it cannot read that reason, and its heartbeats
  ignore cancellation. Stop all application processes, apply 0003, then start only
  Slice 3 code; draining first is preferable. The database does not enforce this order.
- Migration 0004 is additive and may run while attempts are running, but it fails at once
  while any session uses `tasks`. Code from before 0004 ignores `available_at` and would
  retry requeued tasks at once. Stop all application processes, apply 0004, then start
  only Day 4A code with one shared retry policy. `orqestra worker run` refuses to start
  against any other migration history, but a worker already running is not stopped, and
  direct callers of the lifecycle functions are not checked.
- A job whose started tasks were all requeued reads as `queued`; one failed task makes a
  whole job `failed` (the per-state counts give the detail).
- Idempotency keys are global (not scoped per job kind or client) and never expire.
  Keys match exactly, including case and whitespace. Payload equality follows `jsonb`:
  object key order is ignored, numbers compare by value (`1` equals `1.0`) and strings
  compare exactly, without Unicode normalisation.
  Replay detection relies on submission columns never being updated; no code path
  updates them, but the database does not forbid it.
- A submission is one transaction regardless of its size; large submissions have not
  been measured. Tasks are listed by `task_key` in code-point order; the order in which
  they were submitted is not stored.
- Cross-table invariants (for example, a running task has exactly one running attempt
  whose number equals `attempt_count`) are maintained by the lifecycle operations and
  checked by the integration tests, not enforced by the schema.
- Migrations are forward-only and transactional; statements that cannot run in a
  transaction, such as `CREATE INDEX CONCURRENTLY`, are rejected.

## Engineering process

- GitHub is the source of truth. A design that is discussed but not recorded in the
  repository is a proposal, not an accepted decision.
- Material choices are recorded as WHAT / WHY / RUNNER-UP entries in
  [`docs/decisions.md`](docs/decisions.md); cross-cutting ones become ADRs in
  [`docs/adr/`](docs/adr/). Entries stay **Proposed** until the owner accepts them.
- Short-lived branches, Conventional Commits, one scoped issue per feature, and the
  pull request template's verification section filled with commands actually run.
- Contributor guidance, scope and engineering standards: [`docs/development.md`](docs/development.md).
