# Orqestra

Distributed AI workload execution platform: concurrent workers, fault-tolerant task
processing, and cost-aware AI orchestration.

Orqestra is a portfolio-grade engineering project. Its first proving workload is
verification of up to 50,000 **synthetic** invoices and purchase orders; invoices are a
demonstration scenario, not the product. The goal is a reliable, reproducible,
well-tested foundation that demonstrates distributed-systems engineering, not an
invoice SaaS.

## Status

Nothing in the planned column exists yet. No benchmark has been run. Verification so far
is local only (`make check` on one machine); the GitHub Actions workflow has not yet run.
Do not read the architecture below as a claim about shipped behaviour.

| Area | Implemented (verified by local checks only) | Planned (not started) |
| --- | --- | --- |
| Package and toolchain | `uv` lockfile, Ruff, mypy strict, pytest with coverage, `make check` | — |
| Domain contracts | `TaskState` lifecycle with an explicit transition table; `TransientError` / `PermanentError` taxonomy | `Job`, `Task`, `Attempt`, `Result` models and `JobState` (Day 2) |
| Configuration and logging | Validated `ORQESTRA_*` settings with secret redaction; single-line JSON logs with job/task/attempt IDs | — |
| CLI | `orqestra version`, `orqestra health` | job submission and status |
| Persistence | — | PostgreSQL schema, migrations, repositories (Day 2) |
| Queue and workers | — | queue contract, leases, retries, crash recovery (Days 3–4); technology undecided |
| Invoice rules and AI adapters | — | deterministic checks, mocked provider adapter, cost-aware routing (Day 5) |
| Aggregation, cost ledger, API | — | Days 6–7; API framework undecided |
| CI | Workflow file written (format, lint, type check, unit tests); not yet executed on GitHub | integration lane with PostgreSQL (Day 2), security scans, load lane |
| Containers | — | Docker Compose for PostgreSQL and workers |

## Quickstart

Requires [uv](https://docs.astral.sh/uv/). Python 3.12 is installed by uv from
`.python-version`.

```sh
uv sync --frozen            # install locked dependencies into .venv
make check                  # format check, lint, type check, unit tests with coverage
uv run orqestra version
uv run orqestra health      # loads configuration, prints JSON; secrets are redacted
```

Configuration is read from `ORQESTRA_*` environment variables and, in development,
from a local `.env` file. See `.env.example` for every setting.

## Engineering process

- GitHub is the source of truth. A design that is discussed but not recorded in the
  repository is a proposal, not an accepted decision.
- Material choices are recorded as WHAT / WHY / RUNNER-UP entries in
  [`docs/decisions.md`](docs/decisions.md); cross-cutting ones become ADRs in
  [`docs/adr/`](docs/adr/). Entries stay **Proposed** until the owner accepts them.
- Short-lived branches, Conventional Commits, one scoped issue per feature, and the
  pull request template's verification section filled with commands actually run.
- Contributor guidance, scope and engineering standards: [`docs/development.md`](docs/development.md).
