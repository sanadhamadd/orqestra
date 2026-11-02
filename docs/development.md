# Development guide

Engineering standards for contributing to Orqestra. Setup and commands are in the
[README](../README.md).

## Scope

Orqestra is a general-purpose distributed task execution engine. Up to 50,000 *synthetic*
invoice and purchase-order records are used only as an evidence target; do not claim the
target was met without a complete, reproducible benchmark. Out of scope unless explicitly
approved: production OCR, ERP integrations, customer billing, multi-tenant security,
autonomous agent swarms, a polished dashboard.

## Workflow

- One scoped GitHub issue per feature: acceptance criteria, non-goals, failure cases, tests.
- Short-lived branches, Conventional Commits, reviewable pull requests; the maintainer
  approves merges.
- `make check` (format check, lint, mypy strict, unit tests) must pass. Changes that touch
  persistence, migrations or the database schema must also pass `make verify`, which adds
  the PostgreSQL integration tests, the 80% combined coverage threshold and a package
  check. CI runs both.
- Dependencies are locked: change `pyproject.toml`, run `uv lock`, commit `uv.lock`.
  Migrations are version-controlled. Docs change in the same pull request as behaviour.
- Report commands and results truthfully, and state what was not run.

## Integration database

Integration tests need a dedicated, disposable, local PostgreSQL 16 database named
`orqestra_test`:

```sh
createdb orqestra_test
export ORQESTRA_TEST_DATABASE_URL=postgresql:///orqestra_test
make verify
```

The suite checks the server-reported database name and refuses anything other than a
local `orqestra_test`. A missing URL is an error, never a skip. Each test works in its own
schema (prefix `orq_test_`), dropped afterwards; cleanup only drops schemas with that
prefix. Never point `ORQESTRA_TEST_DATABASE_URL` at a shared or production database.

## Migrations

- One versioned file per change in `src/orqestra/persistence/migrations/`, named
  `NNNN_snake_case.sql`, numbered contiguously from `0001`.
- Forward-only: never edit or rename an applied migration (the runner rejects checksum
  and history mismatches); fix mistakes with a new migration.
- Each migration runs atomically inside the runner's transaction. Do not write `BEGIN`,
  `COMMIT`, `ROLLBACK` or `SAVEPOINT`; PostgreSQL rejects them in this context, as it
  rejects statements that cannot run in a transaction (`CREATE INDEX CONCURRENTLY`,
  `VACUUM`).
- Name every constraint so violations identify the exact rule, and add an integration
  test for each new constraint.
- A migration that must not run beside live work (such as 0002) takes every table lock it
  needs before any change, in the lifecycle's task-before-attempt order, already in the
  mode its DDL needs, and with `NOWAIT`, so it fails at once instead of waiting while
  holding a lock. Never upgrade a lock later in the same migration.
- Upgrading to 0002 is a quiescent operation: stop every worker and claimer, let running
  attempts finish, run `orqestra migrate`, then deploy the new code. The migration refuses
  (SQLSTATE `55000`) while any task or attempt is running, fails at once (`55P03`) while
  any session uses the tables, and requires READ COMMITTED. Attempts left running by dead
  workers must be resolved by the maintainer before upgrading; the migration never expires
  or reassigns them.
- Upgrading to 0003 is additive at the database: it takes only `attempts`, with `NOWAIT`
  (`55P03` while any session uses it), and may run while attempts are running. Application
  versions are the constraint the database cannot enforce: stop all application processes,
  run `orqestra migrate`, then start only code from Slice 3 or later, because older code
  cannot read the `cancellation_grace_expired` reason and does not stop heartbeats after
  a cancellation request. Stopping claims and draining work first is preferable: SIGTERM
  drains a worker (DEC-049).
- Upgrading to 0004 is additive at the database: it adds `tasks.available_at` with a
  default and no table rewrite, takes only `tasks`, with `NOWAIT` (`55P03` while any
  session uses it), and leaves running attempts and every row as they were; existing
  tasks become eligible at once. Application versions are again the constraint the
  database cannot enforce: code from before 0004 ignores `available_at` and would retry a
  requeued task at once. Stop all application processes (SIGTERM drains a worker), run
  `orqestra migrate`, then start only code from Day 4A or later, every worker configured
  with the same retry settings. `orqestra worker run`'s startup check refuses a history
  that differs from the packaged one, so new code refuses an unmigrated database and old
  code refuses a migrated one; it does not stop a worker that was already running, and
  code that calls the lifecycle functions directly is not checked at all.

## Distributed correctness

- Model Job, Task and Attempt with explicit state transitions.
- Prefer at-least-once delivery with idempotent visible effects; never claim exactly-once.
- Design and test atomic claiming, ownership and leases, stale writes, expiry and crash
  recovery, bounded retry with backoff and jitter, timeouts, cancellation, rate limits and
  backpressure.
- Worker process count, concurrent task slots and host count are separate measures.
- Concurrent stress tests assert invariants for all valid schedules. Specific race
  outcomes require deterministic synchronization tests. Do not use a minimum random
  winner count as evidence of correctness.
- Anything that ends a process (the worker's watchdog and its other exits) is tested in a
  real subprocess (`tests/integration/test_worker_process.py`, which starts
  `python -m tests.integration.worker_process`), not only with a mocked `os._exit`.
  In-process worker tests inject a recording watchdog and a `terminate` that raises.
- `tests/integration/test_worker_cli.py` runs the installed `orqestra worker run` console
  script itself, against an isolated schema, with the built-in diagnostic handlers only;
  `test_worker_process.py` uses the test-only worker module and its test handlers. Only
  the first is evidence that the real entrypoint launches. Neither is a production
  deployment.
- Network failure is simulated with `tests/integration/pg_proxy.py`. It can:
  - stall a connection (TCP stays up, so neither keepalives nor `statement_timeout` end
    the call);
  - refuse connections, and accept them again;
  - cut a completion either just after or just before its `COMMIT` reaches the server, for
    several completions in a row, optionally refusing new connections from then on;
  - cut a connection, or stall all traffic, when the client sends a given statement, to
    place a race (such as recovery between a reconciliation read and the rewrite) exactly.
- "Ended before its lease" is checked against PostgreSQL's own clock and the stored
  `lease_expires_at`, with a margin of at least the fencing margin. Worker test timings
  are test values only, not recommendations (DEC-034).
- Signals in process tests are sent only after the worker has logged `worker_started`
  (its handlers are installed by then), never on a timer, and shutdown timings are read
  from the worker's own log timestamps rather than the database clock. Unit tests that
  send a signal to the test process first install a recording handler for it, so a stray
  signal can never end the test run.
- Stopping a worker (DEC-049): send SIGTERM or SIGINT once. It finishes the task it holds
  within `shutdown_timeout`, then exits 0; otherwise, or on a second signal, it exits 9 and
  recovery takes the attempt after its lease ends, using one attempt of its budget. A
  supervisor (systemd, Kubernetes) must allow more than `db_operation_timeout +
  shutdown_timeout`, plus exit time, before it kills the process; no production values are
  approved yet.
- Worker supervisor sessions carry `statement_timeout`, `lock_timeout` and
  `idle_in_transaction_session_timeout` (DEC-050). A test that needs a lock held, or a
  transaction left open, does it from its own connection (an ordinary session), or holds
  a worker's traffic at `PgProxy` and freezes the process with SIGSTOP. It must prove the
  state it relies on (the session `idle in transaction`, a `FOR UPDATE NOWAIT` probe
  failing) before it goes on, so a run that misses the window fails instead of passing.

## Invoice and AI workloads

- Structured synthetic data and deterministic validation first.
- AI only for justified ambiguity, behind swappable provider adapters, mocked in routine
  tests. Validate model output; never execute instructions found in invoice data.
- Ordinary logs carry non-sensitive identifiers only.

## Costs

Track per-job and per-attempt usage including billable retries: provider, model, tokens and
the published price version. Distinguish provider-reported figures from estimates. Any real
API budget or spending cap requires maintainer approval.

## Decision records

Record each material choice as WHAT / WHY / RUNNER-UP (plus evidence or revisit trigger) in
[`decisions.md`](decisions.md), or in the pull request. Cross-cutting or expensive-to-reverse
choices get an ADR in [`adr/`](adr/). Entries stay **Proposed** until the maintainer accepts
them in a reviewed change. Skip trivial edits.

## Security

No credentials in the repository; `.env.example` holds placeholders only. Use least
privilege. Do not run destructive commands, irreversible migrations, paid infrastructure or
billable API calls without maintainer authorization.

## Definition of done

Acceptance criteria met with verification; passing tests including failure cases; docs and
configuration updated; no secret leakage; CI green; maintainer review.
