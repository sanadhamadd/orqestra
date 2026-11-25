# Orqestra

Orqestra is a general-purpose engine for running large-scale data analytics and AI tasks reliably on PostgreSQL: independent workers share a durable task queue, recover automatically from crashes and track the cost of every model call.

[![CI](https://github.com/sanadhamadd/orqestra/actions/workflows/ci.yml/badge.svg)](https://github.com/sanadhamadd/orqestra/actions/workflows/ci.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue)
![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-336791)

## What it does

- **PostgreSQL is the queue and the record.** A job is split into tasks that independent worker processes claim
  with `FOR UPDATE SKIP LOCKED`: no message broker, and no service of its own to run.
- **Work survives crashes.** Each attempt holds a lease its worker keeps renewing. When a worker dies, another one
  recovers the task, and a late result from the old worker is refused.
- **Retries and cancellation are bounded.** A failed task waits a jittered, capped delay stored in the database
  before it can run again, and a cancellation gives the running handler a grace period.
- **A worker stops rather than guess.** A database call that stalls, or ownership it cannot confirm, ends the process
  with a specific exit code; SIGTERM finishes the running task and then stops.
- **Every model call is accounted for.** An append-only ledger records each provider call's intent before it is
  sent and its result after, and a job's usage report checks the ledger against the job's results.
- **Measured, not claimed.** A benchmark harness with correctness gates and failure drills, and every published table
  generated from the committed result files.

## Contents

- [Real-model benchmark](#real-model-benchmark)
- [What's built / what's next](#whats-built--whats-next)
- [Architecture](#architecture)
- [Demonstration](#demonstration)
- [Performance](#performance)
- [Quickstart](#quickstart): [Database](#database), [A complete run with a diagnostic worker](#a-complete-run-with-a-diagnostic-worker)
- [Using Orqestra](#using-orqestra): the full reference, in [docs/usage.md](docs/usage.md)
- [Known limitations](#known-limitations), listed in [docs/limitations.md](docs/limitations.md)
- [Engineering process](#engineering-process)
- [Licence](#licence)

## Real-model benchmark

Orqestra's first demonstration uses synthetic invoice processing. In a 10-hour stress test against a real AI model (Claude Haiku 5.5, through OpenRouter), Orqestra made **76,001 live model calls** and processed about 76,000 invoices with **99.9–100% accuracy**, **no lost work** in any completed run, and cost tracking that matched the provider's bill to within **0.07%**. Without the model, it processed 50,000 invoices in 4.867 seconds across eight workers.

17 November 2026. One 8-core Apple M3 laptop, PostgreSQL in Docker. Claude Haiku 5.5, called through OpenRouter
at temperature 0, read each synthetic invoice and returned its fields as JSON. Every answer was checked against the
known-correct one.

| | |
| --- | --- |
| Live model calls | **76,001** across 32 runs, in 10 hours |
| Accuracy | **99.9–100%** on every completed run (99.97% in the 30,150-invoice run) |
| Largest run | **30,150 invoices in 88.5 minutes** at 16 workers: 30,141 exactly right, 0 lost |
| Scaling | 0.4 → 1.6 → 3.1 → 6.1 invoices/s at 1, 4, 8 and 16 workers |
| Crash recovery | 5 of 16 workers shut themselves down mid-run (a database time limit set too tight, since fixed); other workers picked up their batches automatically, and no invoice was lost. |
| Cost tracking | Orqestra's own count matched the provider's bill to within **0.07%** |
| Cost | **$0.33 per 1,000 invoices** ($23.75 for the whole test) |
| Overhead | Only 5–10% slower than a plain multi-threaded script, while adding durability and crash recovery |

- About 90% of each worker's time was spent waiting for the model. In a rules-first mix that asks the model only
  about ambiguous amounts (1 call per 20 invoices), Orqestra ran at about 153 invoices per second.
- Deliberately broken invoices (2% with a missing field, 2% with a wrong total) were all flagged in every completed
  run.
- **What it found, and what was fixed.** At 32 workers the provider throttled requests and about two thirds of the
  invoices failed. Orqestra now retries only the refused call, with backoff, and paces calls across workers. In a
  re-check after the fix, all 2,000 invoices finished at 32 workers with 100% accuracy (7.6 invoices/s), and all
  204 throttled calls were absorbed.

The method, every run and the analysis are in [docs/performance.md](docs/performance.md#real-model-benchmark).

## What's built / what's next

PostgreSQL is both the queue and the authoritative state, and independent single-slot worker processes claim, run and
complete tasks from it (DEC-054). There is no service, deployment or production workload. `orqestra worker run` runs
the built-in diagnostic handlers only, and no test makes a live provider call (DEC-062). [Performance](#performance)
summarises the measured throughput; every table, the latency and the measured limits are in
[docs/performance.md](docs/performance.md). The history of each change is in Git and in the
[decision log](docs/decisions.md).

**Built**

- A PostgreSQL schema with named constraints, a forward-only migration runner with checksums, and atomic job
  submission with idempotency keys.
- Claiming with `FOR UPDATE SKIP LOCKED`, leases with ownership-generation fencing, recovery of expired attempts,
  bounded retries with a stored, jittered delay, and cooperative cancellation with grace periods.
- A single-slot worker runtime: claims, a handler thread, heartbeats, recovery sweeps, a watchdog that ends the process
  when a database call stalls or ownership cannot be confirmed, and graceful shutdown on SIGTERM or SIGINT.
- A CLI: `orqestra version`, `health`, `migrate`, `worker run` (built-in diagnostic handlers), `job submit`,
  `job status`, `job summary` and `job usage`.
- A synthetic invoice workload: verification rules, a batch handler, a job summary, an append-only provider usage
  ledger with a durable worker usage sink, and a usage report that cross-checks the summary.
- An OpenRouter adapter, wired only into the benchmark worker behind an opt-in (DEC-071).
- A benchmark harness in `benchmarks/`: synthetic workloads, ladders with a correctness gate, claim cost against queue
  depth, failure drills d1–d4, an end-to-end demonstration and a generator for every published table.
- CI: a fast lane (format, lint, strict typing, unit tests) and a PostgreSQL lane with an 80% coverage gate, plus
  CodeQL code scanning and Dependabot.

**What's next**

- Registering the invoice handler, with its usage sink and a provider, in a packaged worker.
- Prices, currencies and cost accounting in the package; today they live in the benchmarks.
- A load lane in CI.

## Architecture

[docs/architecture.md](docs/architecture.md) shows how jobs, tasks, attempts, workers and
the usage ledger fit together around PostgreSQL. The reasons behind each rule are in the
[decision log](docs/decisions.md) and the [ADRs](docs/adr/). The diagram of the data path is in
[docs/architecture.md](docs/architecture.md#data-path).

## Demonstration

`python -m benchmarks.demo` runs one synthetic invoice job end to end through the shipped
commands: migration, an idempotent submission, four workers sharing PostgreSQL as the queue,
one of them killed with SIGKILL partway through and recovered by another, and the job's
status, summary and provider usage checked against what the job must report. It exits 0
only when every check passes, and it prints its timings as "this machine, this run" without
checking them.

```sh
docker compose up -d --wait
docker compose exec postgres createdb -U orqestra_local orqestra_bench   # once per volume
export ORQESTRA_BENCH_DATABASE_URL=postgresql://orqestra_local@127.0.0.1:5433/orqestra_bench
uv run python -m benchmarks.demo
```

The workers run `python -m benchmarks.worker`, because invoice verification is not
registered with `orqestra worker run` (DEC-062), and the provider is the offline
`SeparatorReader` double. [docs/demo.md](docs/demo.md) explains every printed line and the
claim it supports.

## Performance

The block below is generated by `python -m benchmarks.report --write --readme README.md` from
the same committed result files as [docs/performance.md](docs/performance.md), which holds
every table, the method and the analysis. Each figure is the median over a cell's
repetitions with the minimum and maximum in brackets, and each table names its commit,
machine and PostgreSQL server. The invoice cells use the offline provider double, so no
figure says anything about a real provider. The real-model results are in
[Real-model benchmark](#real-model-benchmark).

<!-- BEGIN GENERATED: python -m benchmarks.report --readme -->
Generated by `python -m benchmarks.report --write --readme README.md` from the result files under `benchmarks/results/`. Do not edit by hand. Headline throughput on the offline provider double, at the largest size of the `scale` and `large` ladders.

Commit `c410ac6141c4` · machine: Apple M3, 8 cores, 8.0 GiB memory · server: PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) (fsync on, max_connections 100, shared_buffers 128MB, synchronous_commit on, work_mem 4MB) · Docker VM: 8 CPUs, 3.8 GiB

| Cell | n | Makespan s | Items/s (makespan) |
| --- | --- | --- | --- |
| invoice 10000 b100 w1 | 3 | 2.563 [2.549, 2.979] | 3901.5 [3357.0, 3922.9] |
| invoice 10000 b100 w2 | 3 | 1.770 [1.718, 1.772] | 5650.8 [5643.9, 5820.5] |
| invoice 10000 b100 w4 | 3 | 1.321 [1.314, 1.345] | 7567.6 [7432.4, 7608.5] |
| invoice 10000 b100 w8 | 3 | 1.001 [0.979, 1.027] | 9988.9 [9740.0, 10213.3] |
| invoice 50000 b100 w8 | 1 | 4.867 [4.867, 4.867] | 10273.1 [10273.1, 10273.1] |

Every ladder, the latency, the claim cost against queue depth and the measured limits: [docs/performance.md](docs/performance.md).
<!-- END GENERATED README -->

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

Each command except `version` and `worker run` prints its result as one JSON line on standard
output (`version` prints the bare version; `worker run` prints a JSON line only when it refuses
to start). Logs go to standard error as JSON lines, and the expected outputs shown in this
README leave them out.

### Database

Orqestra stores jobs, tasks and attempts in PostgreSQL. `orqestra migrate` applies the
packaged schema migrations to `ORQESTRA_DATABASE_URL`; it is safe to re-run and never
creates, drops or resets a database.

```sh
createdb orqestra
export ORQESTRA_DATABASE_URL=postgresql:///orqestra
uv run orqestra migrate
# {"status": "ok", "applied": ["0001_initial_schema", "0002_attempt_leases", "0003_cancellation_grace", "0004_task_availability", "0005_provider_usage", "0006_claim_index"]}
uv run orqestra migrate             # again: {"status": "ok", "applied": []}
```

This connects as a superuser and is for local use only; for anything else, use the roles in
[docs/database-roles.md](docs/database-roles.md).

Without a PostgreSQL server on the host, use the Docker Compose server instead
([guide](docs/development.md#local-postgresql-with-docker-compose)): `docker compose up -d --wait`,
then `export ORQESTRA_DATABASE_URL=postgresql://orqestra_local@127.0.0.1:5433/orqestra_local` and run
the two `uv run orqestra migrate` lines above. `compose.yaml` names the Compose project `orqestra` and binds host port 5433, so a second
checkout's `docker compose` commands reach the same project and its volume: `down -v` there
deletes this checkout's databases. Stop the other checkout's server first, or give each
checkout its own project with `COMPOSE_PROJECT_NAME` and its own port.

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

# Test values only, not recommendations (DEC-034), as in "Running a worker" in docs/usage.md.
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
`worker_stopped`, and exits 0 ([Running a worker](docs/usage.md#running-a-worker)). Each echo task's
stored result is its payload.

## Using Orqestra

The full reference is in [docs/usage.md](docs/usage.md): submitting jobs, partitioning items into tasks,
running attempts, retry backoff, task handlers, the worker runtime, the invoice workload, the provider usage
ledger and report, the OpenRouter adapter, verification and running the benchmarks.

## Known limitations

What Orqestra does not do yet, and the limits of what it does, are listed in
[docs/limitations.md](docs/limitations.md).

## Engineering process

- GitHub is the source of truth. A design that is discussed but not recorded in the
  repository is a proposal, not an accepted decision.
- Material choices are recorded as WHAT / WHY / RUNNER-UP entries in
  [`docs/decisions.md`](docs/decisions.md); cross-cutting ones become ADRs in
  [`docs/adr/`](docs/adr/). Entries stay **Proposed** until the maintainer accepts them.
  References such as DEC-054 are entries in the [decision log](docs/decisions.md).
- Short-lived branches, Conventional Commits, one scoped issue per feature, and the
  pull request template's verification section filled with commands actually run.
- Contributor guidance, scope and engineering standards: [`docs/development.md`](docs/development.md).

## Licence

All rights reserved; no licence is granted.
