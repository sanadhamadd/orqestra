# Orqestra

Orqestra is a Python-based distributed AI workload execution platform built with PostgreSQL. It is designed to coordinate and execute work reliably by breaking large jobs into smaller tasks and distributing them across independent workers. Rather than assigning fixed workloads to each worker, Orqestra allows workers to pick up new tasks as they become available, naturally balancing work across the system. It also tracks execution, manages retries, and recovers from failures while protecting results from outdated or duplicate execution.

The broader vision is to build a general-purpose execution engine for computational and AI-assisted workflows, including background automation, parallel processing, and large-scale data analytics. Whether coordinating thousands of independent operations or processing datasets that require consistent, trustworthy results, Orqestra focuses on the infrastructure that makes these workflows reliable and scalable.

Its first demonstration uses synthetic invoice processing. In a 10-hour stress test against a real AI model (Claude Haiku 5.5, through OpenRouter), Orqestra made **76,001 live model calls** and processed about 76,000 invoices with **99.9–100% accuracy**, **no lost work**, and cost tracking that matched the provider's bill to within **0.07%**. Without the model, it processed 50,000 invoices in 4.867 seconds across eight workers. See [Real-model benchmark](#real-model-benchmark) below.

## Real-model benchmark

17 November 2026. One 8-core Apple M3 laptop, PostgreSQL in Docker. Claude Haiku 5.5, called through OpenRouter
at temperature 0, read each synthetic invoice and returned its fields as JSON. Every answer was checked against the
known-correct one.

| | |
| --- | --- |
| Live model calls | **76,001** across 32 runs, in 10 hours |
| Accuracy | **99.9–100%** on every completed run (99.97% in the 30,150-invoice run) |
| Largest run | **30,150 invoices in 88.5 minutes** at 16 workers: 30,141 exactly right, 0 lost |
| Scaling | 0.4 → 1.6 → 3.1 → 6.1 invoices/s at 1, 4, 8 and 16 workers |
| Crash recovery | 5 of 16 workers stopped mid-run; their work was picked up automatically, with nothing lost |
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

## Status

PostgreSQL is both the queue and the authoritative state, and independent single-slot
worker processes claim, run and complete tasks from it (DEC-054). Jobs are submitted through
the Python `submit_job` interface or `orqestra job submit`. `orqestra worker run` runs the
built-in diagnostic handlers only. The synthetic invoice workload has a batch handler, a job
summary and a provider usage ledger, but no shipped command registers the invoice kind, and
no test makes a live provider call (DEC-062). There is no service, deployment or production
workload. Measured throughput, latency and limits are in [Performance](#performance),
generated from the committed result files, with every table in
[docs/performance.md](docs/performance.md). The history of each change is in Git and in the
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
| Invoice rules and provider adapters | Strict normalization of a synthetic invoice and two arithmetic checks; a provider contract with a deterministic mock; pure cost-aware route choice; a batch handler built by a factory and not registered with `orqestra worker run` (DEC-055, Accepted); a required per-attempt provider call cap, and a call recorder interface with an in-memory, non-durable implementation (DEC-057, Accepted); an OpenRouter adapter over an injected HTTP transport (DEC-060, Accepted), wired only into the benchmark worker behind an opt-in and never into `orqestra worker run` (DEC-071, Accepted) | wiring a provider into a packaged worker; registering the invoice handler with a worker and its usage sink |
| Aggregation and usage ledger | An in-process summary of an invoice job's stored item results, read from one consistent snapshot, with anomaly reporting and a strict completeness rule (DEC-056, Accepted); an append-only provider usage ledger (`orqestra.persistence.provider_usage`, migration 0005), with an optional usage closure at completion and a recovery tier map (DEC-058, Accepted); a durable worker usage sink that writes a kind's provider calls to it, each intent committed before its call, and wires that kind's closure and T3 tier (DEC-059, Accepted); an in-process provider usage report of the same job from its ledger, read from the same kind of snapshot, with exact per-unit totals, per-attempt finality and a cross-check against the summary (DEC-061, Accepted) | prices, currencies or cost accounting; no inbound API is planned (DEC-062) |
| Benchmarks | An unpackaged harness in `benchmarks/` ([Running the benchmarks](docs/usage.md#running-the-benchmarks)): seeded synthetic workloads, a guarded `orqestra_bench` database, single cells and named ladders with a correctness gate, claim cost against queue depth, failure drills d1–d4, a generator for the tables of `docs/performance.md` and of [Performance](#performance), and an end-to-end demonstration ([Demonstration](#demonstration)) | — |
| CI | Fast lane (format, lint, type check, unit tests) and PostgreSQL `verify` lane, both passing on GitHub since PR #2 | security scans, load lane |
| Containers | Docker Compose for PostgreSQL, local development and integration tests only ([guide](docs/development.md#local-postgresql-with-docker-compose)) | — |

## Architecture

[docs/architecture.md](docs/architecture.md) shows how jobs, tasks, attempts, workers and
the usage ledger fit together around PostgreSQL. The reasons behind each rule are in the
[decision log](docs/decisions.md) and the [ADRs](docs/adr/).

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
  [`docs/adr/`](docs/adr/). Entries stay **Proposed** until the owner accepts them.
- Short-lived branches, Conventional Commits, one scoped issue per feature, and the
  pull request template's verification section filled with commands actually run.
- Contributor guidance, scope and engineering standards: [`docs/development.md`](docs/development.md).

## Licence

Orqestra is not licensed: all rights reserved. A licence is deferred until publication
(DEC-014).
