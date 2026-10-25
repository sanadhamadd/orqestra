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
the merge. Day 2 Step 2A (transactional job submission and job and task reads) is
implemented and verified locally only; it has not yet run in GitHub CI. No benchmark has
been run. Nothing in the planned column exists yet; do not read it as a claim about
shipped behaviour.

| Area | Implemented | Planned (not started) |
| --- | --- | --- |
| Package and toolchain | `uv` lockfile, Ruff, mypy strict, pytest with coverage, `make check` and `make verify` | — |
| Domain contracts | `TaskState` and `AttemptState` with explicit transition tables; `TransientError` / `PermanentError` taxonomy; failure classification; retry and cancellation policy functions; `JobSubmission`, `TaskSpec`, `Job` and `Task` records | `Attempt` model (Day 2, Step 2) |
| Configuration and logging | Validated `ORQESTRA_*` settings with secret redaction; single-line JSON logs with job/task/attempt IDs | — |
| CLI | `orqestra version`, `orqestra health`, `orqestra migrate` | job submission and status |
| Persistence | PostgreSQL schema for jobs, tasks and attempts with named constraints and an ownership-generation (fencing) guard; forward-only migration runner with checksums, advisory locking and rejection of transaction control; atomic job submission with idempotency keys; job and task reads | attempt start and completion, cancellation writes, derived job status, worker concurrency tests (Day 2, Step 2) |
| Queue and workers | — | claiming, leases, heartbeats, crash recovery, bounded cancellation grace period (Days 3–4); queue technology undecided |
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

- Jobs can be submitted and read, but nothing claims, completes or cancels tasks yet.
  Stale-result rejection, at-least-once execution, retries, crash recovery and
  cancellation are not implemented or proven, and job status is not derived yet.
- Idempotency keys are global (not scoped per job kind or client) and never expire.
  Keys match exactly, including case and whitespace. Payload equality follows `jsonb`:
  object key order is ignored, numbers compare by value (`1` equals `1.0`) and strings
  compare exactly, without Unicode normalisation.
  Replay detection relies on submission columns never being updated; no code path
  updates them, but the database does not forbid it.
- A submission is one transaction regardless of its size; large submissions have not
  been measured. Tasks are listed by `task_key` in code-point order; the order in which
  they were submitted is not stored.
- The fencing guard keeps `tasks.attempt_count` monotonic; rejecting stale results also
  needs the conditional writes planned for Step 2.
- Cross-table invariants (for example, a running task has exactly one running attempt
  whose number equals `attempt_count`) are left to Step 2's transactional operations.
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
