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
Step 1 (the PostgreSQL persistence foundation) is implemented and verified locally only;
its CI lane has not yet run on GitHub. No benchmark has been run. Nothing in the planned
column exists yet; do not read it as a claim about shipped behaviour.

| Area | Implemented | Planned (not started) |
| --- | --- | --- |
| Package and toolchain | `uv` lockfile, Ruff, mypy strict, pytest with coverage, `make check` and `make verify` | — |
| Domain contracts | `TaskState` and `AttemptState` with explicit transition tables; `TransientError` / `PermanentError` taxonomy; failure classification; retry and cancellation policy functions | `Job`, `Task`, `Attempt` models (Day 2, Step 2) |
| Configuration and logging | Validated `ORQESTRA_*` settings with secret redaction; single-line JSON logs with job/task/attempt IDs | — |
| CLI | `orqestra version`, `orqestra health`, `orqestra migrate` | job submission and status |
| Persistence | PostgreSQL schema for jobs, tasks and attempts with named constraints and an ownership-generation (fencing) guard; forward-only migration runner with checksums, advisory locking and rejection of transaction control | transactional operations: job submission, attempt start and completion, cancellation writes, derived job status, concurrency tests (Day 2, Step 2) |
| Queue and workers | — | claiming, leases, heartbeats, crash recovery, bounded cancellation grace period (Days 3–4); queue technology undecided |
| Invoice rules and AI adapters | — | deterministic checks, mocked provider adapter, cost-aware routing (Day 5) |
| Aggregation, cost ledger, API | — | Days 6–7; API framework undecided |
| CI | Fast lane (format, lint, type check, unit tests) passed on GitHub for Day 1; PostgreSQL `verify` lane added, not yet run on GitHub | security scans, load lane |
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

- Nothing submits, claims, completes or cancels tasks yet. The schema stores and
  constrains the records, but stale-result rejection, at-least-once execution, retries,
  crash recovery and cancellation are not implemented or proven.
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
