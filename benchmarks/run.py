"""One benchmark cell: start workers, submit, wait, collect, gate, and build the result.

``run_cell(schema_url, cell, ...)`` takes a ready, migrated schema, so the CI
smoke test can hand it a schema of the integration suite's ``orqestra_test``
database and neither database guard is weakened. ``python -m benchmarks.run``
creates and drops a run schema in the ``orqestra_bench`` database itself
(``benchmarks.database``) and writes the result file.

A cell runs one profile:

=========  ==============================================  ======================
profile    worker process                                  task payload
=========  ==============================================  ======================
``echo``   ``orqestra worker run --kind diagnostic.echo``  ``{"n": <index>}``
``wait``   ``orqestra worker run --kind diagnostic.wait``  ``{"seconds": 0.05}``
``invoice`` ``python -m benchmarks.worker``                a DEC-052 invoice batch
=========  ==============================================  ======================

The job is submitted through ``orqestra job submit`` from a task file, every
task with ``max_attempts`` 3. Workers start first and are waited for until
each has logged ``worker_started`` (its signal handlers are installed by
then), so a SIGTERM at the end always finds a worker that can stop gracefully.

**The correctness gate** (plan 2.6) decides whether the cell counts. A cell
that fails any check is ``failed_correctness`` and reports no throughput:

1. ``terminal``: every task is terminal and the job has the expected status;
2. ``one_succeeded_attempt``, ``no_double_running``, ``nothing_running`` and
   ``failed_tasks``: the SQL invariants of the attempt history;
3. ``worker_exits``: every worker exits 0 after SIGTERM;
4. ``results_exact`` (``echo``, ``wait``): every stored result is the handler's
   exact answer, checked in SQL;
5. ``summary`` (``invoice``): ``summarize_invoice_job`` against the oracle;
6. ``usage`` (``invoice``): ``report_job_usage`` and the call budget check
   against the oracle.

The summary and usage reads raise ``WrongJobKindError`` for any other kind of
job, so their checks refuse a non-invoice profile before reading anything.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from enum import StrEnum
from functools import partial
from pathlib import Path
from typing import IO, Any

import psycopg

from benchmarks import metrics
from benchmarks.database import (
    UnsafeBenchDatabaseError,
    connect_verified,
    create_run_schema,
    drop_run_schema,
    require_bench_database_url,
    run_schema_name,
    schema_url,
    unreachable_message,
)
from benchmarks.generate import (
    DEFAULT_SEED,
    Expected,
    build_tasks,
    encode_tasks,
    expected_outcome,
    generate_invoices,
)
from benchmarks.result import (
    RAW_DIR,
    RESULTS_DIR,
    ROOT,
    DirtyTreeError,
    Status,
    WaitEnded,
    code_state,
    environment,
    now,
    validate_result,
    write_result,
)
from benchmarks.worker import DEFAULT_CALL_TIMEOUT, DEFAULT_CAP, parse_duration
from orqestra.aggregation.budget import BudgetVerdict, read_job_call_budget
from orqestra.aggregation.invoice import summarize_invoice_job
from orqestra.aggregation.usage import UsageFinality
from orqestra.domain.jobs import JobStatus, JsonValue
from orqestra.domain.states import AttemptState, TaskState
from orqestra.persistence.jobs import get_job_progress
from orqestra.persistence.migrate import migrate
from orqestra.worker.builtin import ECHO, WAIT
from orqestra.workloads.invoice.handler import JOB_KIND

MAX_ATTEMPTS = 3
WAIT_SECONDS = 0.05
ORQESTRA = Path(sys.executable).with_name("orqestra")  # the installed console script
STARTUP_TIMEOUT = 60.0
POLL = 0.2
# A backend publishes its statistics at most once a second, so the counters are read this
# long after the job ended (metrics module docstring).
STATS_SETTLE = 2.0
TOKENS = "tokens"

# The measurement timing of plan 2.4: test values recorded in every result, not
# recommendations (DEC-034). They pass WorkerSettings' relationships: 1 s < 2 s;
# 2 + 1 + 2 = 5 s < 10 s; 500 ms < 1,000 ms.
BENCH_WORKER_SETTINGS: Mapping[str, str] = {
    "ORQESTRA_WORKER_LEASE": "PT10S",
    "ORQESTRA_WORKER_EXECUTION_TIMEOUT": "PT2M",
    "ORQESTRA_WORKER_HEARTBEAT_INTERVAL": "PT2S",
    "ORQESTRA_WORKER_DB_OPERATION_TIMEOUT": "PT1S",
    "ORQESTRA_WORKER_LOCK_TIMEOUT": "PT0.5S",
    "ORQESTRA_WORKER_FENCE_MARGIN": "PT2S",
    "ORQESTRA_WORKER_POLL_INTERVAL": "PT0.05S",
    "ORQESTRA_WORKER_RECOVERY_INTERVAL": "PT2S",
    "ORQESTRA_WORKER_RECOVERY_BATCH_LIMIT": "10",
    "ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": "PT5S",
    "ORQESTRA_WORKER_RETRY_BASE_DELAY": "PT0.5S",
    "ORQESTRA_WORKER_RETRY_MAX_DELAY": "PT5S",
}


class Profile(StrEnum):
    ECHO = "echo"
    WAIT = "wait"
    INVOICE = "invoice"


KIND_OF = {Profile.ECHO: ECHO, Profile.WAIT: WAIT, Profile.INVOICE: JOB_KIND}


@dataclass(frozen=True, slots=True)
class Cell:
    """One point of a ladder. ``batch`` is ``None`` for the profiles that do not batch."""

    profile: Profile
    size: int
    batch: int | None
    workers: int
    repetition: int = 1
    seed: int = DEFAULT_SEED

    def __post_init__(self) -> None:
        for name in ("size", "workers", "repetition"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                msg = f"{name} must be a positive integer"
                raise ValueError(msg)
        if (self.profile is Profile.INVOICE) != (self.batch is not None):
            msg = "only the invoice profile has a batch size, and it needs one"
            raise ValueError(msg)

    @property
    def name(self) -> str:
        """A short identifier, usable in a schema name: ``inv1000b100w4r1``."""
        return (
            f"{self.profile.value[:3]}{self.size}b{self.batch or 0}"
            f"w{self.workers}r{self.repetition}"
        )


@dataclass(frozen=True, slots=True)
class Workload:
    """What a cell submits, and what a correct run must report about it."""

    tasks: list[dict[str, JsonValue]]
    items: int
    expected: Expected | None


def build_workload(cell: Cell) -> Workload:
    if cell.profile is Profile.INVOICE:
        assert cell.batch is not None
        invoices = generate_invoices(cell.size, cell.seed)
        tasks = build_tasks(invoices, cell.batch, MAX_ATTEMPTS)
        expected = expected_outcome(invoices, cell.batch, DEFAULT_CAP)
        return Workload(tasks, cell.size, expected)
    payload: Callable[[int], JsonValue] = (
        (lambda index: {"n": index})
        if cell.profile is Profile.ECHO
        else (lambda _: {"seconds": WAIT_SECONDS})
    )
    tasks = [
        {"task_key": f"task-{index:07d}", "payload": payload(index), "max_attempts": MAX_ATTEMPTS}
        for index in range(cell.size)
    ]
    return Workload(tasks, cell.size, None)


# --- worker processes --------------------------------------------------------------------


def worker_command(profile: Profile) -> list[str]:
    if profile is Profile.INVOICE:
        return [
            sys.executable,
            "-m",
            "benchmarks.worker",
            "--cap",
            str(DEFAULT_CAP),
            "--call-timeout",
            DEFAULT_CALL_TIMEOUT,
        ]
    return [str(ORQESTRA), "worker", "run", "--kind", KIND_OF[profile]]


def process_environment(url: str, worker_settings: Mapping[str, str]) -> dict[str, str]:
    """This process's environment without any ``ORQESTRA_*`` value, plus the cell's own."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("ORQESTRA_")}
    return env | dict(worker_settings) | {"ORQESTRA_DATABASE_URL": url}


@dataclass(slots=True)
class WorkerHandle:
    process: subprocess.Popen[bytes]
    log_path: Path
    log: IO[bytes]
    exited: dict[str, Any] | None = None  # its accounting, once it has been reaped

    def running(self) -> bool:
        """Whether the worker still runs; reaps it, keeping its accounting, once it has exited."""
        if self.exited is None:
            self.exited = metrics.poll_exit(self.process)
        return self.exited is None


@dataclass(slots=True)
class Fleet:
    """The cell's worker processes. Each logs to its own file, so no pipe can fill."""

    profile: Profile
    env: dict[str, str]
    log_dir: Path
    workers: list[WorkerHandle] = field(default_factory=list)
    started: int = 0

    def start(self) -> WorkerHandle:
        """Start one more worker. Every worker runs from ``ROOT``, where the harness lives."""
        self.started += 1
        path = self.log_dir / f"worker-{self.started}.log"
        log = path.open("wb")
        process = subprocess.Popen(  # a fixed command
            worker_command(self.profile),
            cwd=ROOT,
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
        )
        handle = WorkerHandle(process, path, log)
        self.workers.append(handle)
        return handle

    def wait_started(self, timeout: float = STARTUP_TIMEOUT) -> None:
        """Wait until every running worker has logged ``worker_started``; raise otherwise."""
        deadline = time.monotonic() + timeout
        pending = list(self.workers)
        while pending:
            for handle in list(pending):
                if _logged(handle.log_path, "worker_started"):
                    pending.remove(handle)
                elif not handle.running():
                    msg = f"a worker exited before it started; see {handle.log_path}"
                    raise RuntimeError(msg)
            if pending and time.monotonic() >= deadline:
                msg = "a worker did not start in time"
                raise RuntimeError(msg)
            time.sleep(0.05)

    def alive(self) -> int:
        return sum(1 for handle in self.workers if handle.running())

    def stop(self, timeout: float) -> list[dict[str, Any]]:
        """SIGTERM every worker still running, then reap each one; their accounting, in order."""
        for handle in self.workers:
            if handle.running():
                # never Popen.send_signal: it polls first (see metrics.poll_exit)
                os.kill(handle.process.pid, signal.SIGTERM)
        for handle in self.workers:
            if handle.exited is None:
                handle.exited = metrics.reap(handle.process, timeout)
            handle.log.close()
        return [handle.exited for handle in self.workers if handle.exited is not None]


def _logged(path: Path, event: str) -> bool:
    for line in path.read_bytes().splitlines():
        if line.startswith(b"{"):
            try:
                if json.loads(line).get("event") == event:
                    return True
            except ValueError:
                continue
    return False


def submit(
    url: str, kind: str, tasks: Sequence[JsonValue], directory: Path
) -> tuple[uuid.UUID, dict[str, Any]]:
    """Submit ``tasks`` through ``orqestra job submit``; the job id and the submission figures."""
    data = encode_tasks(tasks)
    path = directory / "tasks.json"
    path.write_bytes(data)
    began = time.monotonic()
    completed = subprocess.run(  # a fixed command
        [str(ORQESTRA), "job", "submit", "--kind", kind, str(path)],
        cwd=directory,
        env=process_environment(url, {}),
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    seconds = time.monotonic() - began
    (directory / "submit.log").write_text(completed.stderr)
    output = json.loads(completed.stdout) if completed.stdout.strip() else {}
    if completed.returncode != 0 or output.get("status") != "ok":
        msg = f"orqestra job submit failed with exit {completed.returncode}: {output}"
        raise RuntimeError(msg)
    figures = {"seconds": seconds, "tasks": len(tasks), "file_bytes": len(data)}
    return uuid.UUID(output["job_id"]), figures


# --- the correctness gate ----------------------------------------------------------------


def _check(name: str, passed: bool, detail: Any = None) -> dict[str, Any]:
    return {"name": name, "passed": passed, "detail": detail}


_ONE_SUCCEEDED_ATTEMPT = """
SELECT count(*) FROM tasks t WHERE t.job_id = %s AND t.state = 'succeeded' AND (
    (SELECT count(*) FROM attempts a WHERE a.task_id = t.id AND a.state = 'succeeded') <> 1
    OR NOT EXISTS (SELECT FROM attempts a WHERE a.task_id = t.id AND a.state = 'succeeded'
                   AND a.attempt_number = t.attempt_count))
"""
_DOUBLE_RUNNING = """
SELECT count(*) FROM (SELECT a.task_id FROM attempts a JOIN tasks t ON t.id = a.task_id
  WHERE t.job_id = %s AND a.state = 'running' GROUP BY a.task_id HAVING count(*) > 1) s
"""
_RUNNING = """
SELECT (SELECT count(*) FROM tasks WHERE job_id = %s AND state = 'running')
     + (SELECT count(*) FROM attempts a JOIN tasks t ON t.id = a.task_id
          WHERE t.job_id = %s AND a.state = 'running')
"""
_FAILED_KEYS = "SELECT task_key FROM tasks WHERE job_id = %s AND state = 'failed' ORDER BY 1"
_RESULT_MISMATCH = {
    Profile.ECHO: (
        "SELECT count(*) FROM tasks WHERE job_id = %s AND state = 'succeeded' "
        "AND result IS DISTINCT FROM payload"
    ),
    Profile.WAIT: (
        "SELECT count(*) FROM tasks WHERE job_id = %s AND state = 'succeeded' "
        "AND result IS DISTINCT FROM jsonb_build_object('waited_seconds', payload -> 'seconds')"
    ),
}


def _count(conn: metrics.Conn, query: str, *params: object) -> int:
    row = conn.execute(query, params).fetchone()
    return int(row[0]) if row else 0


def _require_invoice(profile: Profile) -> None:
    if profile is not Profile.INVOICE:
        msg = f"summary and usage checks apply only to the invoice profile, not {profile.value}"
        raise ValueError(msg)


def summary_check(
    conn: metrics.Conn, job_id: uuid.UUID, profile: Profile, expected: Expected
) -> dict[str, Any]:
    """Gate item 5: the job's invoice summary against the oracle (plan 2.6)."""
    _require_invoice(profile)
    summary = summarize_invoice_job(conn, job_id)
    if summary is None:
        return _check("summary", False, "no such job")
    outcome = {
        "verified": summary.verified,
        "rejected": summary.rejected,
        "needs_review": summary.needs_review,
        "malformed": summary.malformed,
        "provider_assisted_items": summary.provider_assisted_items,
    }
    oracle = {
        "verified": expected.verified,
        "rejected": expected.rejected,
        "needs_review": expected.needs_review,
        "malformed": expected.malformed,
        "provider_assisted_items": expected.provider_assisted,
    }
    anomalies = {code.value: count for code, count in summary.anomalies.items() if count}
    detail: dict[str, Any] = {"outcome": outcome, "oracle": oracle, "anomalies": anomalies}
    passed = outcome == oracle
    if expected.invalid_payload_tasks:
        passed = passed and summary.expected_items is None
        passed = passed and anomalies == {"payload_invalid": expected.invalid_payload_tasks}
    elif expected.failed_batch_items:
        unaccounted = summary.unprocessed_items + summary.missing_items + summary.invalid_items
        detail["unaccounted_items"] = unaccounted
        passed = passed and summary.job_status is JobStatus.FAILED and not summary.complete
        passed = passed and summary.integrity_ok and unaccounted == expected.failed_batch_items
    else:
        detail["complete"] = summary.complete
        passed = passed and summary.complete and summary.integrity_ok
    return _check("summary", passed, detail)


def usage_check(
    conn: metrics.Conn,
    job_id: uuid.UUID,
    profile: Profile,
    expected: Expected,
    *,
    clean: bool,
) -> dict[str, Any]:
    """Gate item 6: the usage report and call budget against the oracle (plan 2.6).

    The succeeded attempts' calls and tokens are exact in every run. ``clean``
    means no task needed a second attempt, so the job totals must equal them
    too, with ``CLOSED`` finality and a ``within`` budget verdict.
    """
    _require_invoice(profile)
    read = read_job_call_budget(conn, job_id, cap=expected.cap)
    if read is None:
        return _check("usage", False, "no such job")
    report, budget = read
    succeeded = report.by_attempt_state[AttemptState.SUCCEEDED]
    succeeded_tokens = _tokens(succeeded.reported_totals)
    detail: dict[str, Any] = {
        "succeeded_calls": succeeded.calls,
        "succeeded_tokens": str(succeeded_tokens),
        "oracle_calls": expected.calls,
        "oracle_tokens": expected.tokens,
        "calls": report.calls,
        "tokens": str(_tokens(report.reported_totals)),
        "finality": report.finality.value,
        "budget_verdict": budget.verdict.value,
        "budget_bound": budget.bound,
    }
    passed = succeeded.calls == expected.calls and succeeded_tokens == expected.tokens
    if clean:
        passed = (
            passed
            and report.calls == expected.calls
            and _tokens(report.reported_totals) == expected.tokens
            and report.finality is UsageFinality.CLOSED
            and budget.verdict is BudgetVerdict.WITHIN
        )
    return _check("usage", passed, detail)


def _tokens(totals: Mapping[str, Any]) -> Decimal:
    total = totals.get(TOKENS)
    return Decimal(0) if total is None else Decimal(total.amount)


def run_gate(
    conn: metrics.Conn,
    job_id: uuid.UUID,
    profile: Profile,
    expected: Expected | None,
    exit_codes: Sequence[int],
    *,
    expected_status: JobStatus = JobStatus.SUCCEEDED,
    expected_failed: Sequence[str] = (),
    expected_exit_codes: Sequence[int] | None = None,
) -> list[dict[str, Any]]:
    """Every gate check of plan 2.6 that applies to ``profile``, in order."""
    progress = get_job_progress(conn, job_id)
    checks: list[dict[str, Any]] = []
    if progress is None:
        return [_check("terminal", False, "no such job")]
    unfinished = progress.task_counts[TaskState.QUEUED] + progress.task_counts[TaskState.RUNNING]
    checks.append(
        _check(
            "terminal",
            unfinished == 0 and progress.status is expected_status,
            {"job_status": progress.status.value, "unfinished_tasks": unfinished},
        )
    )
    bad = _count(conn, _ONE_SUCCEEDED_ATTEMPT, job_id)
    checks.append(_check("one_succeeded_attempt", bad == 0, {"violations": bad}))
    double = _count(conn, _DOUBLE_RUNNING, job_id)
    checks.append(_check("no_double_running", double == 0, {"violations": double}))
    running = _count(conn, _RUNNING, job_id, job_id)
    checks.append(_check("nothing_running", running == 0, {"running": running}))
    failed = [str(row[0]) for row in conn.execute(_FAILED_KEYS, (job_id,))]
    checks.append(
        _check("failed_tasks", failed == sorted(expected_failed), {"failed": failed[:20]})
    )
    wanted = [0] * len(exit_codes) if expected_exit_codes is None else list(expected_exit_codes)
    checks.append(_check("worker_exits", list(exit_codes) == wanted, {"exit_codes": exit_codes}))
    if profile is Profile.INVOICE:
        assert expected is not None
        attempts = _count(
            conn,
            "SELECT count(*) FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE t.job_id = %s",
            job_id,
        )
        tasks = sum(progress.task_counts.values())
        clean = attempts == tasks and not failed
        checks.append(summary_check(conn, job_id, profile, expected))
        checks.append(usage_check(conn, job_id, profile, expected, clean=clean))
    else:
        wrong = _count(conn, _RESULT_MISMATCH[profile], job_id)
        checks.append(_check("results_exact", wrong == 0, {"mismatches": wrong}))
    return checks


# --- failures and usage figures ----------------------------------------------------------


def failure_figures(
    conn: metrics.Conn, job_id: uuid.UUID, exit_codes: Sequence[int]
) -> dict[str, Any]:
    attempts: dict[str, int] = {}
    for state, reason, count in conn.execute(
        "SELECT a.state, coalesce(a.revoked_reason, ''), count(*) FROM attempts a "
        "JOIN tasks t ON t.id = a.task_id WHERE t.job_id = %s GROUP BY 1, 2",
        (job_id,),
    ):
        attempts[f"{state}/{reason}" if reason else str(state)] = int(count)
    tasks = _count(conn, "SELECT count(*) FROM tasks WHERE job_id = %s", job_id)
    failed = _count(
        conn, "SELECT count(*) FROM tasks WHERE job_id = %s AND state = 'failed'", job_id
    )
    total = sum(attempts.values())
    return {
        "attempts_by_state": attempts,
        "extra_attempts_per_task": metrics.rate(total - tasks, tasks),
        "failed_tasks": failed,
        "worker_exit_codes": list(exit_codes),
    }


def usage_figures(conn: metrics.Conn, job_id: uuid.UUID, cap: int) -> dict[str, Any] | None:
    read = read_job_call_budget(conn, job_id, cap=cap)
    if read is None:
        return None
    report, budget = read
    return {
        "finality": report.finality.value,
        "calls": report.calls,
        "reported_totals": {
            unit: {"amount": str(total.amount), "calls": total.calls}
            for unit, total in report.reported_totals.items()
        },
        "by_attempt_state": {
            state.value: {
                "attempts": usage.attempts,
                "calls": usage.calls,
                "answered": usage.answered,
                "failed": usage.failed,
                "unresolved": usage.unresolved,
            }
            for state, usage in report.by_attempt_state.items()
        },
        "call_budget": {
            "cap": budget.cap,
            "cap_source": budget.cap_source,
            "bound": budget.bound,
            "verdict": budget.verdict.value,
        },
    }


# --- one cell ----------------------------------------------------------------------------


@dataclass(slots=True)
class Running:
    """What a hook sees while the cell waits for its job: called every poll."""

    conn: metrics.Conn
    job_id: uuid.UUID
    fleet: Fleet
    elapsed: float
    task_counts: Mapping[TaskState, int]


def cell_status(wait_ended: WaitEnded, gate: Sequence[Mapping[str, Any]]) -> Status:
    """A cell's status from how its wait ended and its gate.

    Only the time limit makes a cell ``incomplete``. A fleet that exited before
    its job ended leaves the job unfinished, so the gate's ``terminal`` check
    fails and the cell is ``failed_correctness``, not a timeout.
    """
    if wait_ended is WaitEnded.STOP_AFTER:
        return Status.INCOMPLETE
    passed = all(check["passed"] for check in gate)
    return Status.COMPLETE if passed else Status.FAILED_CORRECTNESS


def run_cell(
    url: str,
    cell: Cell,
    *,
    worker_settings: Mapping[str, str] = BENCH_WORKER_SETTINGS,
    raw_dir: Path,
    stop_after: float,
    run_id: str | None = None,
    ladder: str = "single",
    idle_window: float = 2.0,
    container: str | None = None,
    note: str = "",
    hook: Callable[[Running], None] | None = None,
) -> dict[str, Any]:
    """Run ``cell`` against the migrated schema ``url`` and return its result record.

    ``stop_after`` bounds the wait for the job in seconds; a job not finished by
    then makes the cell ``incomplete``. The wait also ends once every worker has
    exited; ``timing`` records which ended it and how long it lasted
    (``cell_status``). Raw files go to ``raw_dir``. ``hook``, if given, is called
    on every poll while the job runs (drills use it).
    """
    run_id = run_id or uuid.uuid4().hex[:12]
    raw_dir.mkdir(parents=True, exist_ok=True)
    workload = build_workload(cell)
    env = process_environment(url, worker_settings)
    fleet = Fleet(cell.profile, env, raw_dir)
    with (
        psycopg.connect(url, autocommit=True) as conn,
        psycopg.connect(url, autocommit=True) as sampling,
    ):
        record_environment = environment(conn, container=container, note=note)
        harness_start = metrics.harness_usage()
        own = [conn.info.backend_pid]  # the harness's polling backend, left out of contention
        contention: metrics.Sampler | None = None
        docker: metrics.Sampler | None = None
        try:
            for _ in range(cell.workers):
                fleet.start()
            fleet.wait_started()
            before = metrics.read_only(conn, metrics.database_counters)
            counted = time.monotonic()
            job_id, submission = submit(url, KIND_OF[cell.profile], workload.tasks, raw_dir)
            # The samplers cover the job alone: from the submission to the end of the wait.
            contention = metrics.Sampler(
                lambda: metrics.read_only(sampling, partial(metrics.contention_sample, exclude=own))
            ).start()
            if container:
                docker = metrics.Sampler(lambda: metrics.docker_stats_sample(container)).start()
            began = time.monotonic()
            before_job = began - counted
            wait_ended = WaitEnded.STOP_AFTER
            while time.monotonic() - began < stop_after:
                progress = metrics.read_only(conn, partial(get_job_progress, job_id=job_id))
                assert progress is not None
                counts = progress.task_counts
                if counts[TaskState.QUEUED] + counts[TaskState.RUNNING] == 0:
                    wait_ended = WaitEnded.FINISHED
                    break
                if hook is not None:
                    hook(Running(conn, job_id, fleet, time.monotonic() - began, counts))
                if fleet.alive() == 0:
                    wait_ended = WaitEnded.NO_WORKERS
                    break
                time.sleep(POLL)
            ended = time.monotonic()
            waited = ended - began
            contention_samples = contention.stop()
            contention_window = time.monotonic() - began
            # A docker stats reading takes about a second, so its window is its own.
            docker_samples = docker.stop() if docker else None
            docker_window = time.monotonic() - began
            time.sleep(STATS_SETTLE)
            after = metrics.read_only(conn, metrics.database_counters)
            after_job = time.monotonic() - ended
            idle_rate: dict[str, Any] | None = None
            if wait_ended is WaitEnded.FINISHED:  # otherwise work may still be running
                idle_began = time.monotonic()
                time.sleep(idle_window)
                idle_after = metrics.read_only(conn, metrics.database_counters)
                idle_seconds = time.monotonic() - idle_began
                idle_commits = (
                    idle_after["database"]["xact_commit"] - after["database"]["xact_commit"]
                )
                idle_rate = {
                    "commits": idle_commits,
                    "window_s": idle_seconds,
                    "per_s": metrics.rate(idle_commits, idle_seconds),
                    "formula": (
                        "pg_stat_database xact_commit delta over an idle window after the "
                        "finished job; the harness's reads roll back and never commit"
                    ),
                }
        finally:
            shutdown = _shutdown_seconds(worker_settings)
            reaped = fleet.stop(timeout=shutdown + 10.0)
            for sampler in (contention, docker):
                if sampler is not None:
                    sampler.stop()  # idempotent; only an exception reaches here running
        harness = metrics.harness_delta(harness_start, metrics.harness_usage())
        assert contention is not None  # the wait ran, so its sampler did
        contention_errors = contention.errors
        exit_codes = [worker["exit_code"] for worker in reaped]
        times = metrics.read_stored_times(conn, job_id)
        figures = metrics.timing_figures(times)
        gate = run_gate(conn, job_id, cell.profile, workload.expected, exit_codes)
        failures = failure_figures(conn, job_id, exit_codes)
        usage = (
            usage_figures(conn, job_id, DEFAULT_CAP) if cell.profile is Profile.INVOICE else None
        )
    status = cell_status(wait_ended, gate)
    database_stats = metrics.delta(before, after)
    tasks = len(workload.tasks)
    commits = metrics.job_commits(
        database_stats["database"]["xact_commit"],
        None if idle_rate is None else idle_rate["per_s"],
        before_job + after_job,
        None if idle_rate is None else idle_rate["window_s"],
        # Each idle worker claims once per poll interval: the rate the settings imply.
        cell.workers
        / parse_duration(worker_settings["ORQESTRA_WORKER_POLL_INTERVAL"]).total_seconds(),
    )
    database_stats["commits"] = commits
    database_stats["commits_per_task"] = (
        None if commits["job_commits"] is None else metrics.rate(commits["job_commits"], tasks)
    )
    makespan = figures["timing"]["makespan_s"]
    record: dict[str, Any] = {
        "format": 1,
        "run_id": run_id,
        "ladder": ladder,
        "status": status.value,
        "recorded_at": now(),
        "cell": {**asdict(cell), "profile": cell.profile.value},
        "environment": record_environment,
        "worker_settings": dict(worker_settings),
        "provider": (
            {"call_timeout": DEFAULT_CALL_TIMEOUT, "cap": DEFAULT_CAP, "route": "route-a"}
            if cell.profile is Profile.INVOICE
            else None
        ),
        "timing": {
            **figures["timing"],
            "submission": submission,
            "tasks": tasks,
            "items": workload.items,
            "stop_after_s": stop_after,
            "wait_ended": wait_ended.value,
            "waited_s": waited,
        },
        "throughput": figures["throughput"] if status is Status.COMPLETE else None,
        "latency": figures["latency"],
        "claim_gap": figures["claim_gap"],
        "idle_claim_rate": idle_rate,
        "resources": {
            "workers": [
                {
                    **worker,
                    "cpu_utilization": metrics.rate(worker["user_s"] + worker["sys_s"], makespan),
                }
                for worker in reaped
            ],
            "harness": harness,
            "database": (
                metrics.summarize_docker(docker_samples, docker_window)
                if docker_samples is not None
                else metrics.NOT_MEASURED
            ),
        },
        "database_stats": database_stats,
        "contention": {
            **metrics.summarize_contention(contention_samples, contention_window),
            "sample_errors": contention_errors,
        },
        "failures": failures,
        "usage": usage,
        "gate": gate,
        "drill": None,
    }
    # Whether a dirty tree's record may be written is the writer's decision; its shape is
    # checked here either way.
    validate_result({**record, "environment": {**record_environment, "dirty_allowed": True}})
    return record


def _shutdown_seconds(worker_settings: Mapping[str, str]) -> float:
    text = worker_settings.get("ORQESTRA_WORKER_SHUTDOWN_TIMEOUT", "PT5S")
    return parse_duration(text).total_seconds()


# --- the command -------------------------------------------------------------------------


def run_in_bench_database(
    cell: Cell,
    *,
    run_id: str,
    ladder: str,
    stop_after: float,
    out_dir: Path,
    keep_schema: bool = False,
    allow_dirty: bool = False,
    container: str | None = None,
    note: str = "",
) -> tuple[dict[str, Any], Path]:
    """Run ``cell`` in a fresh run schema of the benchmark database; write its result file."""
    url = require_bench_database_url(os.environ)
    with connect_verified(url) as admin:
        schema = run_schema_name(run_id, cell.name)
        create_run_schema(admin, schema)
        try:
            target = schema_url(admin, url, schema)
            migrate(target)
            record = run_cell(
                target,
                cell,
                raw_dir=RAW_DIR / run_id / cell.name,
                stop_after=stop_after,
                run_id=run_id,
                ladder=ladder,
                container=container,
                note=note,
            )
            path = write_result(record, out_dir, allow_dirty=allow_dirty)
        finally:
            if not keep_schema:
                drop_run_schema(admin, schema)
    return record, path


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--out-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--keep-schema", action="store_true")
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="run on a tree with uncommitted changes; the result is marked and never committed",
    )
    parser.add_argument("--db-container", help="the server's container, for docker stats")
    parser.add_argument("--note", default="", help="free text recorded with the environment")


def refuse_dirty(allow_dirty: bool) -> None:
    if code_state()["git_dirty"] and not allow_dirty:
        msg = "refusing to run: the working tree has uncommitted changes (see --allow-dirty)"
        raise DirtyTreeError(msg)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.run", description=__doc__)
    parser.add_argument("--profile", type=Profile, choices=list(Profile), required=True)
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--batch", type=int)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--repetition", type=int, default=1)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--stop-after-minutes", type=float, default=60.0)
    add_common_arguments(parser)
    args = parser.parse_args(argv)
    try:
        cell = Cell(args.profile, args.size, args.batch, args.workers, args.repetition, args.seed)
        refuse_dirty(args.allow_dirty)
        record, path = run_in_bench_database(
            cell,
            run_id=uuid.uuid4().hex[:12],
            ladder="single",
            stop_after=args.stop_after_minutes * 60,
            out_dir=args.out_dir,
            keep_schema=args.keep_schema,
            allow_dirty=args.allow_dirty,
            container=args.db_container,
            note=args.note,
        )
    except (ValueError, DirtyTreeError, UnsafeBenchDatabaseError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    except psycopg.OperationalError as exc:
        print(f"refused: {unreachable_message(exc)}", file=sys.stderr)
        return 1
    print(f"{record['status']} {path}")
    return 0 if record["status"] == Status.COMPLETE else 1


if __name__ == "__main__":
    sys.exit(main())
