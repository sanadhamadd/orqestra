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

## Distributed correctness

- Model Job, Task and Attempt with explicit state transitions.
- Prefer at-least-once delivery with idempotent visible effects; never claim exactly-once.
- Design and test atomic claiming, ownership and leases, stale writes, expiry and crash
  recovery, bounded retry with backoff and jitter, timeouts, cancellation, rate limits and
  backpressure.
- Worker process count, concurrent task slots and host count are separate measures.

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
