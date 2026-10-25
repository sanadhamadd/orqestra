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
(the transactional attempt lifecycle and derived job status) is implemented and verified
locally only; it has not yet run in GitHub CI. No benchmark has been run. Nothing in the planned column exists yet; do not read it as a claim about
shipped behaviour.

| Area | Implemented | Planned (not started) |
| --- | --- | --- |
| Package and toolchain | `uv` lockfile, Ruff, mypy strict, pytest with coverage, `make check` and `make verify` | — |
| Domain contracts | `TaskState` and `AttemptState` with explicit transition tables; `TransientError` / `PermanentError` taxonomy; failure classification; retry and cancellation policy functions; `JobSubmission`, `TaskSpec`, `Job`, `Task` and `Attempt` records; typed attempt outcomes; derived job status rules | — |
| Configuration and logging | Validated `ORQESTRA_*` settings with secret redaction; single-line JSON logs with job/task/attempt IDs | — |
| CLI | `orqestra version`, `orqestra health`, `orqestra migrate` | job submission and status |
| Persistence | PostgreSQL schema for jobs, tasks and attempts with named constraints and an ownership-generation (fencing) guard; forward-only migration runner with checksums, advisory locking and rejection of transaction control; atomic job submission with idempotency keys; job, task and attempt reads; starting an identified task's attempt; generation-checked completion that rejects stale and duplicate results; bounded retries; cooperative cancellation writes; derived job progress; concurrency tests between separate connections | — |
| Queue and workers | — | discovering claimable tasks, leases, heartbeats, crash recovery, bounded cancellation grace period, worker loop (Days 3–4); queue technology undecided |
| Invoice rules and AI adapters | — | deterministic checks, mocked provider adapter, cost-aware routing (Day 5) |
| Aggregation, cost ledger, API | — | Days 6–7; API framework undecided |
| CI | Fast lane (format, lint, type check, unit tests) and PostgreSQL `verify` lane, both passing on GitHub since PR #2 | security scans, load lane |
| Containers | — | Docker Compose for PostgreSQL and workers |

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
uv run orqestra migrate             # {"status": "ok", "applied": ["0001_initial_schema"]}
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

### Running attempts

A worker that already knows a task's ID starts an attempt, does the work outside any
database transaction, and reports the outcome with the attempt's generation. Finding
claimable tasks, leases and heartbeats are not implemented yet.

```python
from typing import Any

import psycopg

from orqestra.domain.jobs import AttemptFailed, AttemptOutcome, AttemptSucceeded, JsonValue, Task
from orqestra.persistence.jobs import get_job_progress
from orqestra.persistence.lifecycle import complete_attempt, start_attempt


def verify(payload: JsonValue) -> JsonValue:
    return {"verified": True}  # the task's work: runs outside any database transaction


def run(conn: psycopg.Connection[Any], task: Task) -> None:
    attempt = start_attempt(conn, task.id, worker_id="worker-1")  # queued -> running
    outcome: AttemptOutcome
    try:
        outcome = AttemptSucceeded(verify(task.payload))
    except Exception as exc:
        outcome = AttemptFailed.from_exception(exc)
    done = complete_attempt(conn, attempt.id, generation=attempt.attempt_number, outcome=outcome)
    print(done.task.state, get_job_progress(conn, task.job_id))
```

- `start_attempt` moves a queued task to `running`, advances its generation
  (`attempt_count`) and creates attempt *N* for generation *N*, in one transaction.
  Concurrent callers get exactly one attempt; the others get `TaskNotStartableError`.
- `complete_attempt` takes effect only for the task's current, still-running generation:
  - A newer generation raises `StaleAttemptError`.
  - A repeated completion raises `AttemptFinishedError`.

  Neither changes any record. The next task state comes from the lifecycle policy:
  - A success is final, and its result becomes the task's result.
  - A transient failure requeues the task while attempts remain.
  - A permanent or unclassified failure, or exhausted retries, fail the task.
- `request_cancellation` cancels a queued task at once. On a running task it records a
  request: a later success still stands, and a transient failure then cancels instead of
  requeueing.
- A succeeded task's result may be JSON `null`; check `task.state` before reading
  `task.result`.
- Job status is derived from task states: `queued` (every task queued), `running`,
  `succeeded` (every task succeeded), `failed` (all finished, at least one failed) or
  `cancelled` (all finished, none failed, at least one cancelled).

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

- Attempts start only for a task ID the caller already has; nothing discovers claimable
  tasks yet. There are no leases or heartbeats, and nothing revokes ownership of a running
  attempt, so a task whose worker dies stays `running` until crash recovery exists (Day 3).
  For the same reason, the stale-generation tests simulate recovery with direct SQL.
- Cancellation is cooperative and has no grace period, and there is no job-level
  cancellation.
- At-least-once execution is not provided yet. A task may run in several attempts, and
  generation fencing records only the current generation's result, but nothing delivers
  tasks automatically: task discovery, leases, a worker loop and crash recovery are future
  work.
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
