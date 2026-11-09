# Orqestra

Distributed workload execution platform: concurrent workers, fault-tolerant task
processing, cost-aware provider orchestration.

Orqestra is a portfolio-grade engineering project. Its first proving workload is
verification of **synthetic** invoices. 50,000 invoices is the evidence target, not a
demonstrated result, and purchase-order matching is not implemented. Invoices are a
demonstration scenario, not the product. The goal is a reliable, reproducible,
well-tested foundation that demonstrates distributed-systems engineering, not an
invoice SaaS.

## Status

PostgreSQL is both the queue and the authoritative state, and independent single-slot
worker processes claim, run and complete tasks from it (DEC-054). Jobs are submitted through
the Python `submit_job` interface or `orqestra job submit`. `orqestra worker run` runs the
built-in diagnostic handlers only. The synthetic invoice workload has a batch handler, a job
summary and a provider usage ledger, but no shipped command registers the invoice kind, and
no test makes a live provider call (DEC-062). There is no service, deployment or production
workload. No measurement campaign has been run, so this README states no throughput or
latency figures. The history of each change is in Git and in the
[decision log](docs/decisions.md). The planned column lists work that is not
implemented; do not read it as a claim about shipped behaviour.

| Area | Implemented | Planned (not started) |
| --- | --- | --- |
| Package and toolchain | `uv` lockfile, Ruff, mypy strict, pytest with coverage, `make check` and `make verify` | — |
| Domain contracts | `TaskState` and `AttemptState` with explicit transition tables; `TransientError` / `PermanentError` taxonomy; failure classification; retry and cancellation policy functions, including for revoked attempts and cancellation grace periods; `LeasePolicy` and the lease, deadline and revocation rules; `JobSubmission`, `TaskSpec`, `Job`, `Task` and `Attempt` records; typed attempt outcomes; derived job status rules; deterministic partitioning of an ordered item list into fixed-size batch tasks (DEC-052); `RetryPolicy` and a bounded exponential retry delay with deterministic jitter (DEC-053, PR #17) | — |
| Configuration and logging | Validated `ORQESTRA_*` settings with secret redaction; required, validated `ORQESTRA_WORKER_*` timing settings with no defaults; single-line JSON logs with job/task/attempt IDs | — |
| CLI | `orqestra version`, `orqestra health`, `orqestra migrate`, `orqestra worker run` with built-in diagnostic handlers only (DEC-051); `orqestra job submit` from a JSON file of tasks and `orqestra job status` (DEC-064, Accepted); `orqestra job summary` and `orqestra job usage` for an invoice job, with an optional call budget check (DEC-066, Accepted) | — |
| Persistence | PostgreSQL schema for jobs, tasks and attempts with named constraints and an ownership-generation (fencing) guard; forward-only migration runner with checksums, advisory locking and rejection of transaction control; atomic job submission with idempotency keys; job, task and attempt reads; starting an identified task's attempt; discovering and atomically claiming the next queued task with `FOR UPDATE SKIP LOCKED`; generation-checked completion that rejects stale and duplicate results; bounded retries; cooperative cancellation writes; derived job progress; attempt leases and non-renewable execution deadlines set at claim; generation-fenced lease renewal; revocation of expired attempts; bounded cancellation grace periods; quiescent-upgrade migration 0002, additive migration 0003 and additive migration 0004 (`tasks.available_at`, PR #17); concurrency tests between separate connections | — |
| Queue and workers | PostgreSQL is the queue: task discovery without a broker, filtered by job kind (DEC-033); the operations a worker runtime needs to renew leases, recover expired attempts and enforce cancellation grace periods (DEC-034 … DEC-041); worker contracts: the handler protocol, the job-kind handler registry, handler-outcome mapping and validated worker settings (DEC-042 … DEC-044); a first single-slot worker runtime: claims, a handler thread, heartbeats, recovery sweeps while idle or busy, completion, and a local watchdog that ends the process when ownership cannot be established or a database call stalls (DEC-045, DEC-046); reconciliation of a completion whose answer was lost, from PostgreSQL's evidence (DEC-047); cooperative cancellation: a handler is told of a confirmed cancellation request through its read-only signal (DEC-048); graceful, bounded shutdown on SIGTERM or SIGINT: no new work, the running task drained within `shutdown_timeout`, otherwise exit 9 (DEC-049); durable retry scheduling: a requeued task waits a bounded, jittered delay, stored in PostgreSQL, before any claim may take it (DEC-053, PR #17); independent single-slot processes and no Orqestra-owned service (DEC-054, Accepted); subprocess integration coverage for a handler exceeding its execution deadline, worker fencing, and `deadline_exceeded` recovery (PR #18) | — |
| Invoice rules and provider adapters | Strict normalization of a synthetic invoice and two arithmetic checks; a provider contract with a deterministic mock; pure cost-aware route choice; a batch handler built by a factory and not registered with `orqestra worker run` (DEC-055, Accepted); a required per-attempt provider call cap, and a call recorder interface with an in-memory, non-durable implementation (DEC-057, Accepted); an OpenRouter adapter over an injected HTTP transport, which nothing wires in (DEC-060, Accepted) | wiring a live provider into a worker; registering the invoice handler with a worker and its usage sink |
| Aggregation and usage ledger | An in-process summary of an invoice job's stored item results, read from one consistent snapshot, with anomaly reporting and a strict completeness rule (DEC-056, Accepted); an append-only provider usage ledger (`orqestra.persistence.provider_usage`, migration 0005), with an optional usage closure at completion and a recovery tier map (DEC-058, Accepted); a durable worker usage sink that writes a kind's provider calls to it, each intent committed before its call, and wires that kind's closure and T3 tier (DEC-059, Accepted); an in-process provider usage report of the same job from its ledger, read from the same kind of snapshot, with exact per-unit totals, per-attempt finality and a cross-check against the summary (DEC-061, Accepted) | prices, currencies or cost accounting; no inbound API is planned (DEC-062) |
| Benchmarks | An unpackaged harness in `benchmarks/` ([Running the benchmarks](#running-the-benchmarks)): seeded synthetic workloads, a guarded `orqestra_bench` database, single cells and named ladders with a correctness gate, claim cost against queue depth, failure drills d1–d4 and a generator for the tables of `docs/performance.md`; no result is committed yet | — |
| CI | Fast lane (format, lint, type check, unit tests) and PostgreSQL `verify` lane, both passing on GitHub since PR #2 | security scans, load lane |
| Containers | Docker Compose for PostgreSQL, local development and integration tests only ([guide](docs/development.md#local-postgresql-with-docker-compose)) | — |

## Architecture

[docs/architecture.md](docs/architecture.md) shows how jobs, tasks, attempts, workers and
the usage ledger fit together around PostgreSQL. The reasons behind each rule are in the
[decision log](docs/decisions.md) and the [ADRs](docs/adr/).

## Quickstart

Requires [uv](https://docs.astral.sh/uv/) and, for persistence, PostgreSQL 16. Python
3.12 comes from `.python-version`; a uv-managed interpreter is recommended.

```sh
uv sync --locked --managed-python   # install locked dependencies into .venv
make check                          # format, lint, mypy strict, unit tests
uv run orqestra version
uv run orqestra health              # loads configuration, prints JSON; secrets are redacted
```

Configuration is read from `ORQESTRA_*` environment variables and from a `.env` file in
the current directory, if one exists. The file is read in every environment, production
included; a variable set in the environment takes precedence over it. `.env.example` lists
every setting.

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

Without a PostgreSQL server on the host, use the Docker Compose server instead
([guide](docs/development.md#local-postgresql-with-docker-compose)): `docker compose up -d --wait`,
then `export ORQESTRA_DATABASE_URL=postgresql://orqestra_local@127.0.0.1:5433/orqestra_local`.

### Submitting jobs

Jobs are submitted through the Python interface or the CLI (`orqestra job submit`, below);
there is no HTTP API (DEC-062). `submit_job` stores a job and all of its tasks in one
transaction, so either the whole submission is stored or none of it is.

```python
import psycopg

from orqestra.domain.jobs import JobSubmission, TaskSpec
from orqestra.persistence.jobs import get_task, list_tasks, submit_job

invoice_1 = {
    "invoice_id": "INV-1",
    "lines": [{"line_id": "L1", "quantity": 2, "unit_amount": 150}],
    "total": 300,
}
invoice_2 = {
    "invoice_id": "INV-2",
    "lines": [{"line_id": "L1", "quantity": 1, "unit_amount": 75}],
    "total": 75,
}
submission = JobSubmission(
    kind="invoice_verification",
    tasks=(
        TaskSpec("batch-0", {"offset": 0, "items": [invoice_1]}, max_attempts=3),
        TaskSpec("batch-1", {"offset": 1, "items": [invoice_2]}, max_attempts=3),
    ),
    idempotency_key="batch-2026-10-25",
)
with psycopg.connect("postgresql:///orqestra", autocommit=True) as conn:
    result = submit_job(conn, submission)
    tasks = list_tasks(conn, result.job.id)  # ordered by task_key
    print(result.created, [t.state for t in tasks], get_task(conn, tasks[0].id))
```

Each payload is a DEC-052 batch, the shape `partition` produces (see
[Partitioning items into tasks](#partitioning-items-into-tasks)). This is a
submission-only example: no shipped worker runs `invoice_verification`, so the job stays
queued.

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

From the command line (DEC-064), `orqestra job submit` sends a JSON file of tasks through
the same `submit_job`, and `orqestra job status` reads a job's derived status. Both use
`ORQESTRA_DATABASE_URL`, refuse a schema that is not current (run `orqestra migrate`), and
print one JSON line on stdout.

```sh
cat > tasks.json <<'JSON'
[
  {"task_key": "batch-0", "max_attempts": 2, "payload": {"offset": 0, "items": [
    {"invoice_id": "INV-1", "lines": [{"line_id": "L1", "quantity": 2, "unit_amount": 150}], "total": 300}]}},
  {"task_key": "batch-1", "payload": {"offset": 1, "items": [
    {"invoice_id": "INV-2", "lines": [{"line_id": "L1", "quantity": 1, "unit_amount": 75}], "total": 75}]}}
]
JSON
uv run orqestra job submit --kind invoice_verification --idempotency-key batch-2026-11-06 \
    --max-attempts 3 tasks.json
# {"status": "ok", "job_id": "…", "kind": "invoice_verification", "idempotency_key": "batch-2026-11-06", "created": true, "created_at": "…"}
uv run orqestra job status <job_id>
# {"status": "ok", "job_id": "…", "kind": "invoice_verification", "idempotency_key": "batch-2026-11-06", "created_at": "…", "job_status": "queued", "task_counts": {"queued": 2, "running": 0, "succeeded": 0, "failed": 0, "cancelled": 0}}
```

As above, the payloads are DEC-052 batches and the example is submission-only: no shipped
worker runs `invoice_verification`. [A complete run](#a-complete-run-with-a-diagnostic-worker)
below submits a job that `orqestra worker run` does process.

- **The file** is a UTF-8 JSON array of task objects with exactly `task_key` (a string),
  `payload` (any JSON value) and, optionally, `max_attempts`; `-` reads it from standard
  input. A task without `max_attempts` takes `--max-attempts`; there is no default, so a
  task with neither is refused. Unknown fields, repeated keys in an object, numbers that
  are not finite (`NaN`, `Infinity`, or a literal such as `1e400` that overflows a double)
  and strings or object keys that cannot be encoded as UTF-8 (a lone surrogate such as
  `"\ud800"`) are refused before anything is sent to the database, and so are `--kind` and
  `--idempotency-key` values that are not UTF-8. Numbers with a fraction or an exponent are
  read as doubles, as Python's `json` reads them. Everything else is
  `submit_job`'s own validation, including the idempotency rules above: resubmitting the
  same tasks with the same `--idempotency-key` prints the original job with
  `"created": false`.
- **Any non-blank kind** is accepted, including one no worker runs (the invoice kind is not
  registered with `orqestra worker run`, DEC-062); such a job stays queued.
- **Exit codes:** 0 on success; 1 with a `{"status": "error", …}` line for a refusal
  (invalid settings, listed under `errors`; otherwise one `error` message for an invalid
  file, an idempotency conflict, an unknown job id, a schema that is not current or a
  database error); 2 for argparse's usage errors, such as a malformed `JOB_ID`. A
  connection string that is not UTF-8 is reported as invalid by every command. A database error is
  reported by its type, SQLSTATE and violated constraint only, never with the connection
  string or the submitted values.
- **Timeouts:** like `orqestra migrate`, the commands set no timeout of their own. Bound the
  connection attempt with `connect_timeout` in the URL or `PGCONNECT_TIMEOUT`.

For an `invoice_verification` job, two more commands print what the library reads, as one
JSON line each (DEC-066). `orqestra job summary JOB_ID` prints `summarize_invoice_job`: the
derived status, task counts, item outcome counts, `complete`, and anomaly counts with up to
20 samples (a code, a task key and an index). `orqestra job usage JOB_ID` prints
`report_job_usage`: finality, call counts, reported amounts per exact unit (as decimal
strings), counts per attempt state, exposure and anomalies. Neither prints a payload, a
result or an invoice value.

```sh
uv run orqestra job summary <job_id>
# {"status": "ok", "job_id": "…", "kind": "invoice_verification", "observed_at": "…", "job_status": "succeeded", …, "complete": true, …}
uv run orqestra job usage <job_id> --max-provider-calls-per-attempt 16
# {"status": "ok", …, "finality": "closed", "calls": 3, …, "budget": {"cap": 16, "cap_source": "operator", "max_attempts_total": 6, "bound": 96, "calls": 3, "finality": "closed", "verdict": "within", "unknown_reason": null}}
```

- **`--max-provider-calls-per-attempt N`** adds `budget`, the call budget check of DEC-065,
  read in the same snapshot as the report: `bound` is `N` times the sum of the tasks'
  `max_attempts`. `N` is your assumption about the cap the job's workers ran with; it is
  not stored, so the check holds only if they all used it. `within` needs a `closed`
  report, `exceeded` means more calls are recorded than the bound allows, and anything
  else is `unknown`, with the finality as `unknown_reason`. `N` must be an integer from 0
  to 2,147,483,647; anything else is refused before the database is reached.
- **Refusals** exit 1 like the commands above: an unknown job id, a job of another kind
  (named by its id only), a schema that is not current and database errors.

### A complete run with a diagnostic worker

This runs a job end to end with the shipped commands: two `diagnostic.echo` tasks, one
worker that processes them, and a controlled stop. It needs `ORQESTRA_DATABASE_URL` set to a
migrated database (see [Database](#database)) and the environment from `uv sync`.

```sh
cat > echo-tasks.json <<'JSON'
[
  {"task_key": "echo-1", "payload": {"message": "hello"}},
  {"task_key": "echo-2", "payload": {"message": "world"}}
]
JSON
JOB_ID=$(uv run orqestra job submit --kind diagnostic.echo --max-attempts 3 echo-tasks.json \
    | uv run python -c 'import json, sys; print(json.load(sys.stdin)["job_id"])')

# Test values only, not recommendations (DEC-034), as in "Running a worker" below.
export ORQESTRA_WORKER_LEASE=PT4S ORQESTRA_WORKER_EXECUTION_TIMEOUT=PT5M \
    ORQESTRA_WORKER_HEARTBEAT_INTERVAL=PT1S ORQESTRA_WORKER_DB_OPERATION_TIMEOUT=PT0.8S \
    ORQESTRA_WORKER_LOCK_TIMEOUT=PT0.3S ORQESTRA_WORKER_FENCE_MARGIN=PT1S \
    ORQESTRA_WORKER_POLL_INTERVAL=PT0.1S ORQESTRA_WORKER_RECOVERY_INTERVAL=PT1S \
    ORQESTRA_WORKER_RECOVERY_BATCH_LIMIT=5 ORQESTRA_WORKER_SHUTDOWN_TIMEOUT=PT1S \
    ORQESTRA_WORKER_RETRY_BASE_DELAY=PT0.1S ORQESTRA_WORKER_RETRY_MAX_DELAY=PT0.5S

.venv/bin/orqestra worker run --kind diagnostic.echo 2> worker.log &   # one worker process
WORKER=$!
for _ in $(seq 60); do   # wait for both tasks, at most about a minute
  uv run orqestra job status "$JOB_ID" | grep -q '"job_status": "succeeded"' && break
  sleep 1
done
kill -TERM "$WORKER"   # a controlled stop: no new work, then exit 0
wait "$WORKER"; echo "worker exit code: $?"
uv run orqestra job status "$JOB_ID"
# worker exit code: 0
# {"status": "ok", "job_id": "…", "kind": "diagnostic.echo", "idempotency_key": null, "created_at": "…", "job_status": "succeeded", "task_counts": {"queued": 0, "running": 0, "succeeded": 2, "failed": 0, "cancelled": 0}}
```

The worker runs as `.venv/bin/orqestra`, not through `uv run`, so `kill` signals the worker
itself. It logs JSON lines to `worker.log`, ending with `shutdown_requested` and
`worker_stopped`, and exits 0 ([Running a worker](#running-a-worker)). Each echo task's
stored result is its payload.

### Partitioning items into tasks

`partition` (DEC-052, local only) turns an ordered list of JSON items into the tasks of
one job, in contiguous batches of `batch_size`:

```python
from orqestra.domain.jobs import JobSubmission
from orqestra.domain.partitioning import partition
from orqestra.persistence.jobs import submit_job

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
  items, the batch size or a payload's size; these wait for measurement with the benchmark
  harness ([Running the benchmarks](#running-the-benchmarks), BS-05). A job's
  batches are claimed in no particular order (DEC-033).

### Running attempts

A worker claims the next queued task (or starts a task whose ID it already knows) with a
lease policy, does the work outside any database transaction while renewing the lease, and
reports the outcome with the attempt's generation. The single-slot worker runtime runs
these operations in a loop: it claims, renews leases on a schedule and sweeps for expired
attempts (see [Worker runtime](#worker-runtime) and
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
  Lease, deadline and retry times are exact in any session TimeZone: the library decodes them in
  UTC inside its own transactions (DEC-067). Records read outside them, such as `get_attempt` or
  `get_job`, keep the session's zone.
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
  failure cancels instead of requeueing while attempts remain, and fails the task on its
  last attempt. Once the grace period has run out, recovery revokes the attempt and
  cancels the task; if the deadline came first, the revocation is `deadline_exceeded` and
  follows the transient-failure rule instead (DEC-040). A repeated request changes nothing
  and does not extend the grace period.
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

### Worker runtime

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
  due, so sweeps never delay a heartbeat. Idle, it claims every `poll_interval`. With a
  usage sink, the provider usage records a handler waits on are written by the same rule,
  before sweeps ([Provider usage sink](#provider-usage-sink)).
- **Local fence.** A watchdog thread ends the process (`os._exit`) when either guard
  expires:
  - **ownership:** the attempt's lease, mapped to the local clock from the claim so it
    is never later than the real lease end, minus `fence_margin`. It moves only on a
    *confirmed* heartbeat answer: later after a renewal, and to the end of the grace
    period, earlier or later, after a confirmed cancellation request (DEC-068). It is
    disarmed by the handler thread when the handler's work ends.
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
  2. moves the fence to the end of the grace period (earlier or later than the lease);
  3. only then sets the event.

  A handler that sees it and raises `HandlerCancelled` ends the attempt and the task as
  cancelled. Within the grace period a success still stands, a transient failure cancels
  the task while attempts remain and fails it on the last attempt, and a permanent failure
  fails it (DEC-020, DEC-038). A handler that ignores the signal is fenced at the end of
  the grace period, and recovery then cancels the task; when the deadline falls inside the
  grace period, recovery records `deadline_exceeded` instead, and a task with no attempts
  left fails (DEC-040). Nothing
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
  | 8 | Outcome, or a provider call's usage record (DEC-059), unrecorded before the deadline; for usage records, a backstop behind the fence (exit 3), which ends the process first |
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
  1. the settings validate (errors list each field's location, message and type, without
     pydantic's `input` field; a validator's message can still name the parsed value it
     checks);
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
- **Submitting work** uses `orqestra job submit` or the Python `submit_job` interface (see
  [Submitting jobs](#submitting-jobs)).
- **Production restriction.** `diagnostic.*` kinds are refused only when
  `ORQESTRA_ENVIRONMENT` is `production`. An unset `ORQESTRA_ENVIRONMENT` defaults to
  `development`, so a production deployment must set it.

### Invoice verification (synthetic)

`orqestra.workloads.invoice` turns each synthetic invoice into one of four statuses. The
handler takes the partitioner's batch payload and is built by a factory; it is not
registered with `orqestra worker run`, and no migration, API or CLI change goes with it.
The schema and the two rules are a synthetic demonstration contract (DEC-055, Accepted),
not an accounting standard.

```python
from datetime import timedelta

from orqestra.calls.mock import MockProvider
from orqestra.calls.routing import RouteOption
from orqestra.workloads.invoice.handler import JOB_KIND, invoice_verification_handler
from orqestra.workloads.invoice.verify import REQUIRED_CAPABILITY

routes = [RouteOption("route-a", frozenset({REQUIRED_CAPABILITY}), estimated_cost_units=3)]
handler = invoice_verification_handler(
    {"route-a": MockProvider(["5"])},
    routes,
    timeout=timedelta(seconds=30),
    max_provider_calls_per_attempt=10,  # required; 0 allows deterministic results only
    non_billable_routes=frozenset({"route-a"}),  # only the offline mock may be marked
)
# HandlerRegistry({JOB_KIND: handler}); payload {"offset": 0, "items": [invoice, ...]}
```

Any other provider is billable and needs a recorder, for example
`recorder=InMemoryCallRecorder()` from `orqestra.calls.recording`; without one the factory
raises `ValueError`. Existing callers must now pass `max_provider_calls_per_attempt`,
and, for the mock, either mark it or pass a recorder: the factory signature is not
backward compatible. No command registers the handler, so only direct callers change.

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
    (`no_eligible_route`), the invoice's calls would exceed the remaining per-attempt cap
    (`provider_call_cap_reached`), or the provider's reply was not acceptable
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
- **Call cap.** `max_provider_calls_per_attempt` is required: an integer from 0 to
  2^31 − 1, never a `bool`. One `handle()` run is one attempt, and its cap spans every
  item and route of the batch. Before an invoice's first call, if its calls would exceed
  the remaining per-attempt cap, it is `needs_review` (`provider_call_cap_reached`) with
  `routing` naming the chosen route, and no call is made. The check is per invoice, so a
  later invoice that needs fewer calls can still be called. No call is ever spent on an
  invoice the cap would leave undecided. The factory does not check that the cap times
  (per-call timeout plus the recorder's write time) fits in the worker's
  `execution_timeout − fence_margin`. That needs the worker's settings and the durable
  recorder's bounds, so it is left to the worker's wiring, and cap validation is not
  complete yet (DEC-057).
- **Call recording.** With a `recorder` (a `ProviderCallRecorder`), every call's intent is
  recorded before the call, including a marked mock's and a cached repeat's. Each call
  gets its own `call_seq`: 0, 1, 2, … within the attempt. An intent carries the attempt
  id, the item's absolute index, the route, the capability, the field kind and the
  route's estimate. A result is recorded only when the outcome is known: `answered` with
  the provider's own `usage`, if it reported one, or `failed` with its code and no usage.
  A call has only its intent when the provider raises something that is not a provider
  error, returns something that is not a `ProviderResponse`, or is interrupted by a
  `BaseException`; when `record_result` fails; or when the process is interrupted before
  the result (see the table below and Known limitations). No record carries a raw string,
  a request id, the reply's value or `reported_cost_units`. Intents use the usage ledger's
  ranges: an intent whose `item_index` is above 2^31 − 1 or whose estimate is above
  2^63 − 1 raises `ValueError` before `record_intent`, so the task fails and no call is
  made. Here the route id comes from a checked `RouteOption`, which uses the ledger's
  identifier pattern, and the capability is the constant `interpret_amount`, so the
  ledger accepts every intent made by verification once a durable recorder adds the
  provider. An intent built directly only checks that those two are strings; a durable
  recorder that refuses one raises in `record_intent`, before the call. A result's
  numbering and outcome use the ledger's range and values too, but its `usage` is bounded
  only by `ProviderUsage` (up to 64 characters, any number of decimal places), which is
  looser than the ledger's amount range (below 1E+20, at most 18 decimal places). That is
  known only after the call, so a durable recorder that refuses such an amount leaves a
  billed call unresolved. The worker's durable sink does exactly that: it writes no result
  for such an amount, and the task goes on ([Provider usage sink](#provider-usage-sink)).
  A cancellation stops before the next call's intent, so it never leaves an unresolved
  intent, and the calls already made keep their results. The records do not name the
  provider, job, task or prompt version: a durable recorder adds those from its own
  wiring, and `orqestra.calls` never depends on the worker.

  ```mermaid
  flowchart LR
    C{cancelled?} -- no --> I[record_intent] --> P[provider.interpret] --> R[record_result] --> V[judge the reply]
    C -- yes --> X[HandlerCancelled]
    I -- raises --> F[task fails; no call made]
    P -- anything else --> U[no result: unresolved]
  ```

  | What happened | Recorded | Then |
  | --- | --- | --- |
  | A `ProviderResponse`, valid or not | `answered`, with its `usage` | the reply is judged as before |
  | `ProviderTransientError` / `ProviderPermanentError` | `failed`, with its code | the same code is raised as a fresh error |
  | Any other exception, or a return that is not a `ProviderResponse` | intent only (unresolved) | `provider_protocol_violation` |
  | A `BaseException` (a shutdown signal) | intent only (unresolved) | the signal propagates |
  | The intent is outside the ledger's ranges | nothing; no call is made | `ValueError` fails the task |
  | `record_intent` raises | nothing; no call is made | the recorder's error fails the task |
  | `record_result` raises | intent only (unresolved) | the recorder's error fails the task |
  | A reported `usage` outside the ledger's amount range | in memory: `answered`, with its `usage`; the worker's sink: intent only (unresolved) | the reply is judged as before; the sink never raises for it |
  | Cancellation before a call | nothing for that call; earlier calls keep their results | `HandlerCancelled` |
- **Stored result.** `{"offset": n, "results": [...]}`, one result per item:
  `{"index", "status", "provider_assisted", "findings": [{"rule", "verdict", "path"}],
  "issues": [{"code", "path"}], "reason", "routing": {"route_id"} | null}`. Paths are
  positions such as `lines[2].unit_amount`. No raw value, identifier, provider text,
  reported usage, estimate or call count is stored in the result; the shape is
  unchanged by the cap and the recorder, which add only a `reason` value. `routing` says
  which route the decision chose, for the attempt that completed; it is an audit, not
  usage accounting.
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
what happened to each item (DEC-056, Accepted). It is a Python interface, and
`orqestra job summary` prints it as one JSON line (DEC-066, see
[Submitting jobs](#submitting-jobs)). It adds no API, table or migration.

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
  not provider calls or cost. A retried batch repeats calls; recorded calls are reported
  by the provider usage report below.
- **Errors.** `None` for a missing job; `WrongJobKindError` for another kind of job (no
  task is read); `ValueError` for a job without tasks; `RuntimeError` if the connection is
  already inside a transaction. A database error propagates and no partial summary is
  returned.

### Provider usage closure

An attempt's provider usage capture can be closed in the same transaction that ends the
attempt, so the closure commits or rolls back with the transition (DEC-058, Accepted).
Both arguments are optional; without them nothing is written to the ledger. A worker with a
`UsageSink` passes both for the sink's kinds, and only for them
([Provider usage sink](#provider-usage-sink)).

```python
from orqestra.persistence.lifecycle import complete_attempt, revoke_next_expired
from orqestra.persistence.provider_usage import (
    CaptureState,
    CaptureTier,
    ClosedBy,
    UsageClosure,
    capture_of,
    read_attempt_usage,
)

closure = UsageClosure(
    attempt_id=attempt.id,
    capture=CaptureState.COMPLETE,
    closed_by=ClosedBy.WORKER,
    capture_tier=CaptureTier.T3,
    call_count=calls_made,
)
complete_attempt(
    conn,
    attempt.id,
    generation=attempt.attempt_number,
    outcome=outcome,
    retry_policy=retry_policy,
    usage_closure=closure,
)
# Safe only for a kind whose handlers record every call through a durable recorder,
# such as the worker's UsageSink (see "Which kinds may be mapped" below).
revoke_next_expired(conn, retry_policy=retry_policy, usage_capture={"metered": CaptureTier.T3})
usage = read_attempt_usage(conn, attempt.id)
print(capture_of(usage))  # open, closed, lower_bound or not_captured
```

- **Worker.** The closure is written last, after the attempt and the task, on the
  completion's own cursor and under its locks. Every outcome closes, a requeued transient
  failure included. A repeated identical closure is a replay.
- **Recovery.** `usage_capture` maps job kinds to the tier their handlers record at. A
  revoked attempt of a mapped kind is closed by `recovery`: at T3, `complete` with the
  number of intents when every intent has a result (`complete(0)` when there are none),
  otherwise `incomplete`; at T1 and T2 always `incomplete`. An unmapped kind gets no
  closure. Map a kind to T3 only if its handlers record every intent before calling the
  provider: that is the one way to over-claim.
- **Late facts.** An intent waits behind the lifecycle's lock on its attempt and then
  records the state that committed, so a call recorded after the attempt ended is flagged
  late and was never counted by its closure. `record_late_usage` records a result and says
  whether a closure already existed. Closures never change: later facts leave the attempt
  `lower_bound`.
- **Reading it.** `capture_of` is pure. `closed` needs a complete closure that counts
  exactly the calls read, each with a result. `not_captured` means nothing was recorded,
  never "zero calls".
- **Which kinds may be mapped.** `InMemoryCallRecorder` writes nothing to the ledger, so
  a closure cannot see the calls it holds. Map a kind to T3, or close its attempts as
  `complete`, only if every handler of that kind records every call through a durable
  recorder: the worker's `UsageSink` (DEC-059) is the only one. Otherwise either would
  claim `complete(0)` for calls that were made. No packaged worker registers
  `invoice_verification`; only the unpackaged benchmark workers (`benchmarks/worker.py`,
  `benchmarks/faults.py`) run it, behind a `UsageSink` that maps it to T3. Anywhere else it
  stays unmapped; its attempts read `not_captured`, or `lower_bound` with an `incomplete`
  closure, never `closed`.

| Situation | What happens |
|---|---|
| A complete worker closure counts the wrong number of calls, or one has no result | `CaptureNotCompleteError`; nothing is recorded, the attempt still runs, and it may complete again with an `incomplete` closure |
| A different closure is already stored for the attempt | Completion: `UsageReplayConflictError`, nothing recorded. Recovery: the stored closure is kept and the revocation commits |
| Another session's closure for the attempt is not yet committed | The closure insert waits, up to `lock_timeout` on a worker session. Then: a replay, a conflict (completion only), or its own insert if the other rolls back |
| The completion is fenced (stale, revoked or already finished) | The usual error; no closure is written |
| The connection is lost, or any statement fails, before commit | Neither the transition nor the closure is recorded |
| A call is recorded after the closure | Accepted and flagged late; the closure is unchanged and the attempt reads as `lower_bound` |

### Provider usage sink

A worker can write its handlers' provider calls to the usage ledger as they happen
(DEC-059, Accepted). The `UsageSink` in `orqestra.worker.usage` is the durable
`ProviderCallRecorder` of DEC-057; the worker's supervisor, still the only database
client, writes every record.

```python
from datetime import timedelta

from orqestra.worker.handlers import HandlerRegistry
from orqestra.worker.runtime import Worker
from orqestra.worker.usage import UsageSink, check_provider_call_budget

sink = UsageSink(
    ["metered"],  # job kinds whose calls are captured, each at tier T3
    provider_for_route={"route-a": "provider-a"},
    model_for_route={"route-a": "model-a"},  # optional
    prompt_version="v1",  # optional
)
check_provider_call_budget(  # optional, pure: refuses a cap that cannot fit
    settings, max_provider_calls_per_attempt=20, provider_call_timeout=timedelta(seconds=5)
)
handler = make_handler(recorder=sink.recorder)  # your factory: every call goes through it
worker = Worker(
    registry=HandlerRegistry({"metered": handler}),
    settings=settings,
    connect=connect,
    usage=sink,
)
```

- **Flow.** For each call the handler's `record_intent` posts the intent and waits; the
  supervisor writes it in one guarded call and answers only once it has committed, so the
  call is made only then. After the call, `record_result` posts the result and waits
  until it has committed too. `ProviderCalls` posts each result before its next intent.
  The sink does not enforce that order: it checks only that `call_seq` starts at 0 and
  goes up by 1. A result still outstanding makes the worker's closure `incomplete`, so
  `complete(n)` is never a guess.
- **Scheduling.** A due heartbeat comes first. A record waits while the next heartbeat is
  due within `db_operation_timeout`, as sweeps do, and once renewals have stopped after a
  cancellation it never waits. Records come before sweeps and are written while a
  stopping worker drains.
- **Closures.** The completion closes the attempt's capture by `worker` at T3:
  `complete(n)` when every confirmed intent has its confirmed result, otherwise
  `incomplete`, in the completion's own transaction (DEC-058). Sweeps pass
  `usage_capture={kind: t3}` for the sink's kinds, so a revoked attempt of those kinds is
  closed by recovery at T3. Every worker of a fleet must use the same kind-to-tier map;
  nothing checks this.
- **The T3 contract.** Every handler registered for a sink kind must record every call
  through `sink.recorder`. A handler that calls a provider some other way makes both
  closures claim `complete` for fewer calls than were made. Nothing enforces this yet;
  checking it at registration is a required future step.
- **Cancellation.** An intent that is still waiting when the supervisor learns of a
  committed cancellation request is withdrawn: no row and no call, and the handler gets
  `HandlerCancelled`. An intent that was acknowledged is a call in flight: it goes
  ahead, and its result is always written, never withdrawn.
- **Invalid records** raise `ValueError` in the handler before anything is posted: a
  route the sink does not map, a record for another attempt, a `call_seq` that does not
  start at 0 and go up by 1, or a value the ledger refuses. The task fails and no call is
  made (DEC-057). A second record while one waits is a `RuntimeError`.

| What happens to a record | What the worker does |
| --- | --- |
| A retryable error (`40001`, `40P01`, `55P03`, `57014`) or a failed connect | Retries every `db_operation_timeout` until the attempt's local deadline |
| The answer is lost (the write may have committed) | Closes the connection and replays the same keyed write at once, then every `db_operation_timeout`; a replay is matched, never doubled |
| An intent cannot be confirmed while the handler waits | In practice the ownership fence ends the process first (exit 3, `ownership_deadline_passed`) and no call is made; exit 8 (`usage_intent_unrecorded`) at the deadline is the supervisor's backstop |
| A result cannot be confirmed while the handler waits | In practice the ownership fence ends the process first (exit 3); the call stays unresolved and recovery closes the capture `incomplete` at T3; exit 8 (`usage_result_unrecorded`) at the deadline is the supervisor's backstop |
| The database rejects it, or `NotFoundError`, `CallIntentNotFoundError`, `UsageReplayConflictError`, a `RuntimeError`, or a first intent finds a row already there | Exit 7 (`usage_write_rejected`) |
| The intent is stored late (the attempt had stopped running) | Exit 3 (`usage_intent_late`), no call; the row stays, an unresolved call that may overcount |
| Ownership is lost | Exit 3, as before |
| The reported amount is 1E+20 or more, or has more than 18 decimal places | No result row, nothing clamped or zeroed; `usage_result_unrepresentable` with ids only; the handler goes on and the closure is `incomplete` |
| A complete closure is refused at completion (`CaptureNotCompleteError`, `UsageReplayConflictError`) | Exit 7 (`completion_refused`) |

None of these is ever the handler's outcome. While a handler waits, the ownership fence
stays armed before the attempt's deadline D (the lease is capped at D), so in practice
it ends the process first. While heartbeats succeed or are not due, the supervisor's
exit 8 comes at worst at D plus four times `db_operation_timeout` (a retry due at D may
still wait for a step in progress and a heartbeat, lose a write's answer and fail its
immediate replay). Heartbeats that keep failing are retried within one bound, which
holds usage writes back as it holds sweeps, so exit 8 may then never come. If the
supervisor never answers, the handler's wait ends at D plus five times
`db_operation_timeout` (`usage_handoff_bound` plus two more) with a never-classified
signal (exit 5): strictly later than the four-bound worst case, and the end when
failing heartbeats hold writes back and the fence has not ended the process first. `usage_handoff_bound` and
`check_provider_call_budget` are limits for the case with no failures; retries and
replays are bounded by the attempt's deadline and by the fence, not by these limits.
Diagnostics carry the worker, task and attempt ids, `call_seq`, the exception type and
the SQLSTATE only: never an amount, a unit, a prompt or provider text.

- **Overhead.** Each call waits for two serialized supervisor round trips, plus at most
  one step in progress or a heartbeat. When PostgreSQL is slow, calls are delayed, never
  made unrecorded. Sweeps may be postponed while records wait.
- **Limits.** `usage_handoff_bound(settings)` is the longest one record may wait.
  `check_provider_call_budget` refuses a cap unless `cap × (provider_call_timeout + 2 ×
  usage_handoff_bound) ≤ execution_timeout − fence_margin`. It is a pure check that the
  wiring may call; nothing calls it for you, and `WorkerSettings` has no new field.
- **Measured, not asserted.** `tests/integration/test_worker_usage_performance.py`
  (marked `performance`) prints the per-call cost of the sink beside the in-memory
  recorder and direct ledger writes; see [the development guide](docs/development.md).

### OpenRouter adapter (not wired in)

`orqestra.providers.openrouter.OpenRouterProvider` implements the `Provider` contract with
one non-streaming request to OpenRouter's chat completions endpoint (DEC-060, Accepted).
**Nothing uses it yet.** No worker, handler or command imports `orqestra.providers`, no
test makes a live call, and live calls need separate authorization.

```python
from pydantic import SecretStr

from orqestra.providers.openrouter import OpenRouterConfig, OpenRouterProvider
from orqestra.providers.stdlib_transport import HttpsTransport

config = OpenRouterConfig(
    credential=SecretStr("..."),  # any object with get_secret_value()
    route_models={"route-a": "vendor/model"},  # explicit: there is no default model
)
provider = OpenRouterProvider(config, HttpsTransport())
report = provider.call(request)  # a CallReport; provider.interpret(request) is report.response
```

- **Configuration.** `OpenRouterConfig` has no defaults. The credential must be 1 to 512
  visible ASCII characters, so it cannot carry a space or a line break into a header, and
  it is shown as `***` in `repr`. `route_models` maps route ids (the routing rule) to model
  slugs (`[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}`, a project rule); it must not be empty, and
  it is copied into an immutable mapping. `Settings` has an optional
  `ORQESTRA_OPENROUTER_API_KEY` (`SecretStr`, `***` in `orqestra health`) that nothing
  reads yet.
- **Before any I/O.** A capability other than `interpret_amount`, a route without a model,
  or a request whose fields have the wrong types is `unsupported_request`, and the
  transport is never called.
- **One request.** The body is exactly `model`, `messages` (a fixed system prompt carrying
  `PROMPT_VERSION`, and a user message with the canonical JSON of `field_kind` and `raw`)
  and `"stream": false`. The headers are exactly `Authorization` (Bearer), `Content-Type`
  and `Accept`; the transport adds only `Host` and `Content-Length`. There is one POST
  per call and no retry.
- **Transport.** The adapter is given an `HttpTransport`, so every test uses a fake one.
  `HttpsTransport` (`http.client` and `ssl`, the only module that may open a socket)
  posts to the fixed host `openrouter.ai`, port 443 and path `/api/v1/chat/completions`.
  It follows no redirect, reads no proxy setting from the environment and prints no debug
  output. It closes the connection on every path.
- **Deadline.** `request.timeout` is the budget, measured on a monotonic clock. The time
  left is checked before the POST and before every body read. The transport also uses it
  as the connect timeout, and sets it as the socket timeout before sending, before reading
  the response head and before each read. With no time left the step does not start.
- **Response.** A status other than 200 is mapped from the status alone and its body is
  never read. A 200 body is read up to 256 KiB and must be strict UTF-8 JSON: one object,
  no duplicate keys, no `NaN` or `Infinity`, numbers kept exact. Then:
  - an embedded `error` is mapped by its `code`, like a status;
  - otherwise `id` (1 to 128 visible ASCII characters), `model` (the slug rule) and
    exactly one choice are required, with `finish_reason` `"stop"` and string content;
  - the value is the content with surrounding whitespace removed, and the verifier still
    decides whether it is an amount.
- **Usage.** `usage.cost` becomes `ProviderUsage(amount, "openrouter_credits")`, its value
  and scale kept in fixed-point text (`1.50` stays `1.50`, `1.5e-3` becomes `0.0015`).
  The amount must also be recordable in the usage ledger: finite, unsigned (`-0`
  included), at most 18 decimal places, and below 10^20. Token counts must be integers
  from 0 to 2^63-1. Absent usage is `None`, never zero.
- **Nothing leaks.** The package logs nothing. Every error carries a fixed code, and is
  raised with no `__cause__` or `__context__`, so no credential, prompt, remote message or
  standard-library message travels with it. Standard-library messages matter: an illegal
  header value would be quoted in one.

| Outcome | Raised |
| --- | --- |
| Status 400 | `ProviderPermanentError(unsupported_request)` |
| Status 401, 402 or 403 (402 even with `Retry-After`) | `ProviderPermanentError(rejected)` |
| Status 408, the deadline spent, or a socket timeout | `ProviderTransientError(timeout)` |
| Status 429 | `ProviderTransientError(rate_limited)` |
| Status 502 or 503, or a connection, TLS or HTTP protocol failure | `ProviderTransientError(unavailable)` |
| Any other status, including 3xx, other 2xx, 500 and 504 | `OpenRouterProtocolError`, defect `unknown_status` |
| A 200 with an embedded error | The error for its `code` above; an unlisted code or a malformed envelope is `malformed_error` |
| A 200 body over 256 KiB | `body_too_large` |
| Not one strict JSON object | `malformed_json` |
| A missing or invalid `id`, `model` or `choices` | `invalid_schema` |
| `finish_reason` not `"stop"`, or content that is not a string | `unsupported_content` |
| Usage or cost outside the rules above | `invalid_usage` |

`OpenRouterProtocolError` is a `PermanentError` whose text is
`provider_protocol_violation`, the same text the invoice verifier uses for a provider that
breaks the contract.

**Limits.**
- The standard library has no timeout for name lookup (`getaddrinfo`). Every other
  blocking step (connecting, including the TLS handshake; sending; reading the response
  head; each body read) is given the time left when that step starts, and the time is
  checked again only before the next step. Reading the response head (the status line,
  up to 100 header lines, and any `100 Continue` responses) and reading one body chunk
  can each wait on the socket several times, and sending writes the head and the body
  separately. So a slow server can make one call overrun its budget by more than one
  wait.
- A handler thread cannot be interrupted, and cancellation is checked only between
  provider calls. The hard bound on a call is therefore the worker's watchdog and fence
  (DEC-045, DEC-048). If the worker is fenced mid-call, the call stays unresolved in the
  usage ledger.
- A failure after the request was sent may still have been billed. It is reported as
  `unavailable`, and whatever wires the adapter in must treat it as an ambiguous outcome.
- Status 500 and 504 are not retried. Strict parsing may reject a valid future response
  shape.
- The usage ledger's failure codes have no protocol-violation code yet. Recording that
  outcome is left to the change that wires the adapter in.

### Provider usage report

`report_job_usage` reads an invoice job's provider usage ledger (migration 0005) and reports
what providers said they used, next to the unchanged summary (DEC-061, Accepted). It is a
Python interface, and `orqestra job usage` prints it as one JSON line, with the call budget
check on request (DEC-066, see [Submitting jobs](#submitting-jobs)). It adds no API, table
or migration. It reports provider-declared figures only: there are no prices, currencies or
cost.

```python
import psycopg

from orqestra.aggregation.usage import UsageFinality, report_job_usage

with psycopg.connect("postgresql:///orqestra", autocommit=True) as conn:
    report = report_job_usage(conn, job_id)  # None if there is no such job
    if report is not None:
        for unit, total in report.reported_totals.items():
            print(unit, total.amount, total.calls)
        print(report.finality is UsageFinality.CLOSED, report.unresolved)
```

```mermaid
flowchart LR
    S["one REPEATABLE READ, READ ONLY snapshot"] --> J[job row]
    J --> T["usage totals, summed in SQL"]
    T --> A["attempt usage stream"]
    A --> K["task stream"]
    K --> B["UsageReportBuilder (pure)"]
    B --> R[JobUsageReport]
```

- **One snapshot.** The job row, the exact totals, one row per attempt and the task rows
  are read in one `REPEATABLE READ, READ ONLY` transaction, as for the summary, so every
  figure describes the same past state. A call or result committed meanwhile is not seen.
  The read takes no row locks, and holds back vacuum while it lasts.
- **Exact, per unit.** Amounts are summed by PostgreSQL in `numeric`, never in Python, so a
  38-digit amount is not rounded. A unit is the provider's exact string: `credits`,
  `Credits` and `µ-credits` are three units, and nothing is converted. Each total says how
  many calls reported it. A failed call that reported usage counts;
  `settled_without_reported_usage` counts answered or failed calls that reported none.
- **Attribution.** `by_attempt_state` splits the calls and totals by the current state of
  the attempt that made them, so the usage of revoked and retried attempts stays visible.
  `calls == answered + failed + unresolved`, and the per-state figures add up to the job's.
  A call is `unresolved` while it has no result; a late result moves it to answered or
  failed. `late_intents` and `late_results` count facts recorded after their attempt
  stopped running (advisory).
- **Finality.** Each attempt is `OPEN` (running), `CLOSED` (a complete closure that this
  snapshot confirms), `LOWER_BOUND` (an incomplete closure, a finished attempt with calls
  and no closure, or a complete closure that a later intent contradicts) or `NOT_CAPTURED`
  (finished with nothing recorded). The job's `finality` is `OPEN` while the job or any
  attempt is unfinished, `CLOSED` only when every attempt is `CLOSED`, and `LOWER_BOUND`
  otherwise. `CLOSED` is a statement about this snapshot: the ledger accepts late writes.
- **True zero versus not captured.** `CLOSED` with no call is a true zero. A job whose
  attempts recorded nothing and closed nothing has no calls too, but it is `LOWER_BOUND`
  with `NOT_CAPTURED` attempts: absent capture is never read as zero.
- **Exposure.** `estimated_exposure_units` sums the route estimates of unresolved calls, in
  the same opaque units as routing. It is never added to reported amounts, and a call
  without an estimate is counted in `unresolved_without_estimate`, never as zero.
- **Cross-check.** For each succeeded task whose succeeded attempt is `CLOSED`, the
  report compares the items its result marks `provider_assisted` (by the summary's rules)
  with that attempt's answered calls. Fewer answers than assisted items is the anomaly
  `assisted_exceeds_answered`; a task whose attempt is not `CLOSED` is counted in
  `assisted_unchecked_tasks`. `assisted_items` equals the summary's
  `provider_assisted_items` for the same database state.
- **Anomalies.** A closed set of codes (`closure_contradicted`,
  `assisted_exceeds_answered`), with up to `MAX_SAMPLES` samples of the code, task key and
  attempt number. They never contain a value from the data, and none is raised.
- **Errors.** As for the summary: `None` for a missing job; `WrongJobKindError` for another
  kind of job (nothing else is read); `ValueError` for a job without tasks; `RuntimeError`
  if the connection is already inside a transaction. A database error propagates and no
  partial report is returned.

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
With the Docker Compose server, create the test database with
`docker compose exec postgres createdb -U orqestra_local orqestra_test` and set
`ORQESTRA_TEST_DATABASE_URL=postgresql://orqestra_local@127.0.0.1:5433/orqestra_test`
([guide](docs/development.md#local-postgresql-with-docker-compose)).

### Running the benchmarks

`benchmarks/` is a measurement harness kept beside the package: it is not part of `orqestra`
and not in its wheel, so it runs only from a clone, as `uv run python -m benchmarks.<module>`.
This README quotes none of its results.

| Module | What it does |
| --- | --- |
| `benchmarks.run` | One cell: a profile (`echo`, `wait` or `invoice`), a size, a batch size and a worker count. It starts real worker processes, submits the job with `orqestra job submit`, waits, applies the correctness gate and writes one result file |
| `benchmarks.ladder` | A named ladder of cells (`warm-up`, `scale`, `overhead`, `handler-bound`, `batch`, `large`), run in interleaved order with stop rules |
| `benchmarks.claims` | The cost of one claim against the depth of the queue (DEC-033, DEC-062) |
| `benchmarks.drills` | Failure drills `d1` to `d4`: killed, frozen and cut-off workers, a database restart, and poison inputs |
| `benchmarks.report` | The generated tables of `docs/performance.md`, rendered from the committed result files |

- **The database.** Every module refuses to run unless `ORQESTRA_BENCH_DATABASE_URL` names a
  local database called `orqestra_bench`, reached over a Unix socket or the loopback network
  and checked against the server's own answer (`benchmarks/database.py`). The variable is
  read from the process environment only, not from `.env`. Each run works in a throwaway
  schema (`orq_bench_…`) that it drops at the end, unless `--keep-schema` is given. On a
  local server:

  ```sh
  createdb orqestra_bench
  export ORQESTRA_BENCH_DATABASE_URL=postgresql:///orqestra_bench
  ```

- **A clean tree.** A result must name the commit it measured, so `run`, `ladder`, `claims`
  and `drills` refuse a working tree with uncommitted changes (`benchmarks/results/` is left
  out of that check). `--allow-dirty` runs anyway: its results are marked `dirty_allowed`,
  must never be committed, and stay out of the report's tables.
- **Output.** Result files go to `benchmarks/results/` (drills to
  `benchmarks/results/drills/`); raw per-task rows and worker logs go to
  `build/bench/<run_id>/`, which git ignores.
- **The `invoice` profile** runs the unpackaged `python -m benchmarks.worker`: invoice
  verification behind a `UsageSink`, answered by the offline `SeparatorReader` double, which
  makes no network call.
- **The database restart drill, `d3b`,** runs `docker compose -p orqestra-bench restart
  postgres`. It is refused unless `--allow-db-restart` is given, `COMPOSE_PROJECT_NAME` is
  `orqestra-bench`, and that project's container is the server the URL reaches (the same
  `system_identifier`). Run the benchmark database in that Compose project for it:

  ```sh
  export COMPOSE_PROJECT_NAME=orqestra-bench
  docker compose up -d --wait         # the compose.yaml server, as its own project and volume
  docker compose exec postgres createdb -U orqestra_local orqestra_bench
  export ORQESTRA_BENCH_DATABASE_URL=postgresql://orqestra_local@127.0.0.1:5433/orqestra_bench
  uv run python -m benchmarks.drills d3b --allow-db-restart
  ```

  It binds the same host port as the development server (`compose.yaml`), so only one of the
  two can run at a time.
- **The report.** `uv run python -m benchmarks.report --write` replaces the text between the
  two markers of `docs/performance.md` and never creates that file; `--check` exits 1 when
  the tables are out of date, and `--print`, the default, writes them to standard output.
- **BS-05.** Some decision entries and `partition`'s docstring call this large-scale
  measurement BS-05: the batch size, size limits and operating values wait for it. The
  `batch` and `large` ladders run it; no value has been decided from them.
- **In CI.** `tests/integration/test_bench_smoke_pg.py` is part of `make verify`: it runs
  small `invoice` and `echo` cells and the `d1` and `d3a` drills on a throwaway schema of the
  integration database, and asserts the correctness gate only, never a timing.

### Known limitations

- `orqestra worker run` is a command, not a service: nothing restarts, supervises or
  deploys it, and it can run only the built-in diagnostic handlers. A task whose worker dies is
  recovered only while some worker is running: any live worker sweeps, idle or busy.
  There is no guaranteed recovery delay: it depends on live workers, their sweep
  interval and batch limit, and the backlog of expired attempts. It is never shorter than
  the attempt's current lease: an attempt becomes revocable only when that lease ends
  (normally a full lease after its last renewal, sooner after a cancellation request's
  grace, DEC-040), and only a live worker's sweep revokes it.
- Revoking an attempt ends its database ownership only, and does not cancel or un-bill an
  external provider call already made. Calls recorded in the usage ledger stay attributed to the
  revoked attempt through its `attempt_id`. Recovery closes an attempt's usage capture only
  for kinds given a tier in `usage_capture`; an unmapped kind with no recorded calls reads
  `not_captured`, never "zero", and an unrecorded call is not known to the ledger. A
  worker passes a closure and a tier map only for the kinds of its `UsageSink` (DEC-059).
- The OpenRouter adapter is not wired in; its own limits are listed in
  [OpenRouter adapter](#openrouter-adapter-not-wired-in).
- Invoice verification has no live provider and no packaged worker: `orqestra worker run`
  builds its worker without a `UsageSink`. Only unpackaged workers wire the ledger: the
  benchmark workers (`benchmarks/worker.py`, `benchmarks/faults.py`) register invoice
  verification behind a `UsageSink` at tier T3, and the process tests' worker
  (`tests/integration/worker_process.py`) does so for its `metered` test kind. With
  `InMemoryCallRecorder` calls stay in memory. The worker's `UsageSink` is durable, but nothing checks that a sink kind's
  handlers record every call through it (the T3 contract), or that every worker of a fleet
  uses the same tier map. The ledger, and so the usage report, holds only what is
  recorded through `orqestra.persistence.provider_usage`. The stored `routing` shows only
  which route was chosen. A provider error
  repeats the whole batch on retry, so billable calls could repeat (each attempt keeps
  its own cap); deterministic request ids would let a real adapter deduplicate them, and
  no exactly-once claim is made.
- A provider call can be left unresolved: an intent with no result. This happens when
  the provider raises something that is not a provider error, returns something that is
  not a `ProviderResponse`, the process is interrupted, or the result is not written (the
  worker's sink writes none for a reported amount outside the ledger's range). With
  another recorder, a recorder failure is a handler failure: an unclassified error that
  is not retried; with the worker's sink, a ledger failure ends the process instead. The
  cap is checked against the worker's execution timeout only by
  `check_provider_call_budget`, which nothing calls for you (DEC-059).
- The invoice job summary is read in process or with `orqestra job summary` (DEC-066), one
  job per call; no API exposes it. Its snapshot read grows with the number of tasks and the
  size of their stored results, and holds back vacuum while it lasts; the cost of the
  ordering, the effect of the fetch size and the behaviour under concurrent completions
  are measured by tests that no default lane runs, and no threshold or batch-size limit is
  decided. Counts are per-task tallies, not a final job tally unless `complete`.
- The provider usage report is read in process or with `orqestra job usage` (DEC-066), one
  job per call; no API exposes it. Its read
  grows with the number of tasks, attempts and ledger rows; it is measured at 50,000
  invoices by a test that no default lane runs, and no threshold is decided. Tokens and
  closure tiers are not totalled.
- Every integration test and benchmark run uses one machine and one local PostgreSQL server
  (in Docker for the Compose setup and in CI), reached over a Unix socket or the loopback
  network: the integration suite refuses any
  other target (`check_test_target` in `tests/integration/postgres.py`), and so does the
  benchmark database guard (`benchmarks/database.py`). There is no multi-host result.
- `orqestra worker run` runs the diagnostic handlers only (`KINDS` in
  `src/orqestra/worker/builtin.py`), so the shipped command never runs the invoice handler,
  and no invoice result says anything about it.
- The offline provider double for benchmarks, `SeparatorReader` (`benchmarks/provider.py`),
  answers at once and makes no network call, so nothing here measures a real provider's
  latency, cost or reliability.
- There is no purchase-order matching: the invoice rules check each invoice on its own
  (`src/orqestra/workloads/invoice/verify.py`).
- Usage is totalled per exact unit that a provider reports, such as `tokens`, with no
  price, currency or money figure (DEC-062).
- The call budget check (`--max-provider-calls-per-attempt`, DEC-065) assumes that every
  worker of the job used that cap; the cap is not stored, so nothing confirms it.
- **The worker's local fence is limited.**
  - It cannot stop a handler that holds the GIL in native code: the watchdog thread then
    cannot run either.
  - It cannot stop processes a handler started and detached.
  - It assumes the local and database clocks run at the same rate, with `fence_margin`
    absorbing small differences. A step forward in the database clock can end the real
    lease before the local fence.
  - It has been shown under the tested conditions only: a stalled proxy, refused
    connections, a cancellation request, and a lease ended by the database.
- **Worker runtime limits.**
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
  every task. Bounds wait for the large-scale benchmark (BS-05, see
  [Running the benchmarks](#running-the-benchmarks)).
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
- There is no rate limiting or backpressure: nothing limits how fast workers claim tasks or
  call a provider, or slows submission under load. A provider's `429` is the transient
  `rate_limited` code (DEC-055), so its batch is retried after the ordinary retry delay,
  which ignores `Retry-After`.
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
  attempts finish, migrate, then deploy. It refuses while any task or attempt is running,
  and fails at once while another transaction holds any lock on `attempts`, or a lock on
  `tasks` stronger than a plain read's (a write, or `SELECT … FOR UPDATE` or `FOR SHARE`);
  a transaction that has only read `tasks` does not stop it. Code from before 0002 cannot
  run against the new schema: it cannot read revoked attempts, and its claims are
  rejected.
- Migration 0003 is additive and may run while attempts are running, but it fails at once
  while any session uses `attempts`. Code from before 0003 must not run once the new code
  can write `cancellation_grace_expired`: it cannot read that reason, and its heartbeats
  ignore cancellation. Stop all application processes, apply 0003, then start only
  code that includes migration 0003; draining first is preferable. The database does not enforce this order.
- Migration 0004 is additive and may run while attempts are running, but it fails at once
  while any session uses `tasks`. Code from before 0004 ignores `available_at` and would
  retry requeued tasks at once. Stop all application processes, apply 0004, then start
  only code that includes migration 0004, with one shared retry policy. `orqestra worker run` refuses to start
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

## Licence

Orqestra is not licensed: all rights reserved. A licence is deferred until publication
(DEC-014).
