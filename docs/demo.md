# Demonstration

`python -m benchmarks.demo` runs one invoice job end to end through the shipped commands,
kills a worker while the job runs, and checks every result against what the job must report.
It prints one line per step. This page explains each line and the claim it supports.

## What it shows

- The benchmark database guard: the run uses only a local database named `orqestra_bench`,
  in a fresh run schema that `orqestra migrate` migrates.
- Seeded synthetic invoices, written as a task file, with the outcome the oracle in
  `benchmarks/generate.py` expects and the provable call bound of DEC-065, both printed before
  anything is submitted.
- Idempotent submission: `orqestra job submit` twice with one idempotency key creates one job.
- Independent workers sharing PostgreSQL as the queue, one of which is killed with SIGKILL
  once 30 % of the tasks have succeeded; another worker's recovery sweep revokes its attempt,
  the task runs again, and a replacement worker is started.
- `orqestra job status`, `job summary` and `job usage` read back the job, and their output is
  checked against the oracle and the call bound.
- Makespan, throughput and recovery delay, each labelled "this machine, this run".

## What it does not show

- A real provider. The provider is `SeparatorReader` (`benchmarks/provider.py`), an offline
  double that answers at once with a synthetic `tokens` figure. Nothing here says anything
  about a real provider's latency, cost or reliability.
- More than one machine: the workers and PostgreSQL run on the same host.
- Any timing claim. Timings are printed and recorded, and no check uses them.

## Why the benchmark worker

Invoice verification is not registered with `orqestra worker run`, which runs the built-in
diagnostic handlers only (DEC-062; README, "Known limitations"). The demo therefore starts
`python -m benchmarks.worker`, which runs `invoice_verification_handler` behind a durable
`UsageSink` at capture tier T3 and checks the call budget against its execution timeout before
it connects (`benchmarks/worker.py`). It is not packaged and is registered nowhere else.

## Setup

The demo needs a clone with `uv sync` done, and a local PostgreSQL database named
`orqestra_bench`. On a server on the host:

```sh
createdb orqestra_bench
export ORQESTRA_BENCH_DATABASE_URL=postgresql:///orqestra_bench
uv run python -m benchmarks.demo
```

With the Docker Compose server (`compose.yaml`):

```sh
docker compose up -d --wait
docker compose exec postgres createdb -U orqestra_local orqestra_bench   # once per volume
export ORQESTRA_BENCH_DATABASE_URL=postgresql://orqestra_local@127.0.0.1:5433/orqestra_bench
uv run python -m benchmarks.demo
```

The working tree must have no uncommitted changes, so the result names the commit it ran,
unless `--allow-dirty` is given. The README's
[Running the benchmarks](../README.md#running-the-benchmarks) describes the database guard and
the rest of the harness.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `--invoices` | 5,000 | synthetic invoices to generate |
| `--seed` | 20261107 | the generator's seed; it changes the bytes, never an expected status |
| `--batch` | 100 | invoices per task |
| `--workers` | 4 | worker processes at the start; at least 2, and fewer than the batches |
| `--stop-after` | 1,800 | a bound on a hung run, in seconds; never an expectation |
| `--keep-schema` | off | keep the run schema instead of dropping it |
| `--out-dir` | `build/bench/demo/` | where the result record is written |
| `--allow-dirty` | off | run on a tree with uncommitted changes; the record is marked |
| `--db-container` | none | accepted, as by the other benchmark commands; the demo samples no container |
| `--note` | empty | free text recorded with the environment |

## Every printed line

Each line starts with its tag. Examples show the shape only; `…` stands for values of the run.

| Tag | What it shows | Where it comes from | Check it feeds |
| --- | --- | --- | --- |
| `database` | `orqestra_bench` and the host or socket reached; never the URL | `connect_verified` (`benchmarks/database.py`) | the run refuses to start otherwise |
| `schema` | the run schema `orq_bench_…_demo` created empty, and at the end dropped | `create_run_schema`, `drop_run_schema` | — |
| `migrate` | the migrations `orqestra migrate` applied | the command's JSON line | `migrated` |
| `invoices` | the count, seed, batch size, tasks and `max_attempts` | `generate_invoices`, `build_tasks` | — |
| `expected` | verified, rejected, needs_review and malformed items, and provider calls, as the oracle expects | `expected_outcome` | `summary`, `cli_summary`, `usage`, `cli_usage` |
| `bound` | the most provider calls the job can make: cap × the sum of `max_attempts` | `call_bound` (DEC-065) | `cli_usage` |
| `submit` | `orqestra job submit`'s JSON line, with `"created": true` | the command | `submit_created` |
| `resubmit` | the same command again: `"created": false` and the same job id | the command | `resubmit_same_job` |
| `workers` | the benchmark workers started, by pid | `DrillFleet` (`benchmarks/drills.py`) | `worker_exits` |
| `kill` | the worker killed with SIGKILL, the attempt it held, and how many tasks had succeeded | `KillDrill` | `injected` |
| `revoked` | the killed attempt revoked as `lease_expired` once its lease ended, by another worker's sweep | the `attempts` row | `revocation_seen`, `victims_revoked_lease_expired`, `victims_closed_by_recovery_t3` |
| `replaced` | the task's next attempt and the worker that started it | the `attempts` row | `replacement_seen`, `one_succeeded_attempt` |
| `restarted` | a replacement worker started after the replacement attempt was seen | `DrillFleet.start` | `worker_restarted` |
| `status` | `orqestra job status`: the derived status and the task counts, and the attempts by state | the command; `failure_figures` | `cli_status`, `terminal`, `nothing_running` |
| `summary` | `orqestra job summary`: item counts, `complete` and `integrity_ok` | the command | `cli_summary`, `summary` |
| `usage` | `orqestra job usage --max-provider-calls-per-attempt 16`: finality, calls, verdict and bound | the command | `cli_usage`, `usage`, `victims_capture` |
| `makespan` | from the job's creation to its last task's end | `timing_figures` (`benchmarks/metrics.py`) | none |
| `throughput` | invoices per second over the makespan | `timing_figures` | none |
| `recovery` | the replacement attempt's start minus the killed attempt's lease end | `KillDrill.measured` | none |
| `result` | the result record's path and the run's raw directory | `write_result` | — |
| `wall time` | the whole command, from its start to the verdict | the harness clock | none |
| `verdict` | `PASS` with the number of checks, or `FAIL` with the failed check names | every check | — |

- **Why `kill` waits for a call boundary.** A worker killed between a provider call's intent
  and its result leaves a call no one answered, so recovery closes the attempt `incomplete`,
  the usage report reads `lower_bound` and the budget verdict `unknown`. The demo kills only
  a worker whose attempt has no such call, so the report can read `closed` and `within`. Drill
  d1 (`python -m benchmarks.drills d1`) also covers a kill during a call.
- **Which attempt is killed.** Every worker is stopped first; once no statement any of them
  sent is still running on the server, the oldest running attempt whose worker was stopped is
  chosen, and the other workers continue.
- **The revocation needs a live worker.** Recovery is a sweep that any running worker makes,
  so the demo needs at least two workers.
- **`usage` counts the killed attempt's calls too.** The checks compare the succeeded
  attempts' calls and tokens with the oracle; the job's total also holds the calls the killed
  attempt made before it died, and the verdict compares that total with the bound.
- **Makespan includes the start.** It counts from the job's creation, which comes before the
  workers start, and it includes the killed attempt's lease and the retry delay.
- **No figure is a check.** `makespan`, `throughput`, `recovery` and `wall time` describe this
  machine and this run only; they are recorded and never asserted.

## Exit codes

- 0: every check passed and the job finished.
- 1: a refusal (`refused: …` on standard error: no database URL, the wrong database, a dirty
  tree, invalid options), an abort (a step the rest depends on failed; `verdict FAIL: aborted:
  …`), or any failed check (`verdict FAIL:` names them).
- 2: argparse's usage errors.

## Files

- `build/bench/<run_id>/demo/`: `tasks.json`, each command's JSON output (`*.json`) and
  standard error (`*.log`), and each worker's log (`worker-N.log`).
- The result record under `build/bench/demo/`. Git ignores `build/`, and
  `python -m benchmarks.report` never reads it, so a demo run never enters the performance
  tables.
- The run schema is dropped at the end unless `--keep-schema` is given.
