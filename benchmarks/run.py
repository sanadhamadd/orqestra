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
``extract`` ``python -m benchmarks.worker --workload extract`` a batch of document texts
=========  ==============================================  ======================

The ``extract`` profile (``benchmarks.extract``) sends every generated document
(``benchmarks.documents``) to the model and needs ``--provider openrouter``. Its
workers get their own settings (``cell_worker_settings``): an execution timeout
that fits one call per document of a batch. Its gate adds ``extraction_complete``
and ``extraction_usage`` to the engine checks, and its record adds an
``extraction`` block (latency, time split, accuracy, tokens and cost, retries and
errors by code). A live run needs the model's price per million tokens
(``--price-in-per-mtok``, ``--price-out-per-mtok``); a dry run uses synthetic ones.

The job is submitted through ``orqestra job submit`` from a task file, every
task with ``max_attempts`` 3 (8 with OpenRouter, below). Workers start first and
are waited for until each has logged ``worker_started`` (its signal handlers are
installed by then), so a SIGTERM at the end always finds a worker that can stop
gracefully.

**Supervision.** A worker that exits while the job still has queued or running
tasks is recorded in ``failures.exits_while_running`` and replaced with the same
command, up to ``replacement_limit(workers)`` (4 x workers) replacements a cell, so
one death does not end the cell. A worker that stopped itself for a stalled
database call (exit 4) is replaced ``STALL_REPLACEMENT_DELAY`` (1 s) later, so its
replacement does not walk into the same slowdown; any other exit is replaced at
once. A long pause (the campaign saw a 58 s checkpoint) can still use up the bound,
because a replacement can stall again every few seconds: the engine riding out an
idle stall is what keeps a fleet whole, and this is the backstop that keeps the
cell going and records it. The work a dead worker held comes back through lease
expiry and recovery. Before the fleet is stopped, every running worker,
replacements included, is waited for until it has logged ``worker_started``.

**Checkpoints.** The harness reads the server's checkpoint counters while it waits
(``CheckpointWatch``) and records each checkpoint that ended during the cell in
``database_stats.checkpoints``, so a slowdown can be set beside them.

**The correctness gate** decides whether the cell counts. A cell
that fails any check is ``failed_correctness`` and reports no throughput:

1. ``terminal``: every task is terminal and the job has the expected status;
2. ``one_succeeded_attempt``, ``no_double_running``, ``nothing_running`` and
   ``failed_tasks``: the SQL invariants of the attempt history;
3. ``worker_exits``: every worker stopped at the end exits 0 after SIGTERM, and
   every worker that exited mid-cell exited 4 (``STALLED``: the watchdog's
   designed fail-fast for a database call past its bound, DEC-045); any other
   mid-cell exit fails the check, although the worker was replaced;
4. ``results_exact`` (``echo``, ``wait``): every stored result is the handler's
   exact answer, checked in SQL;
5. ``summary`` (``invoice``): ``summarize_invoice_job`` against the oracle;
6. ``usage`` (``invoice``): ``report_job_usage`` and the call budget check
   against the oracle.

The summary and usage reads raise ``WrongJobKindError`` for any other kind of
job, so their checks refuse a non-invoice profile before reading anything.

**The provider** (DEC-071) is the offline double unless ``--provider openrouter``
is given, which only this command and the crash drill ``benchmarks.drills d1``
accept (the other drills refuse it; ``benchmarks.ladder`` has no provider flag). It
needs ``--model``, ``--max-provider-calls`` and either ``--dry-run`` (canned
responses, no network, no cost) or ``--allow-live-calls`` (real, billable calls: they
need an explicit opt-in and a key with a dollar limit set on it). The cell's call
maximum is a counter file in the cell's raw directory, created before any worker
starts; ``ORQESTRA_OPENROUTER_API_KEY`` is passed to the workers only for a live
run. With OpenRouter the usage check compares the calls with the oracle, and the
credits only for a dry run, whose canned usage is known.

**OpenRouter cells** (live and dry run alike, so a dry run rehearses a live one)
run with ``LIVE_WORKER_SETTINGS``: database bounds with room for a hiccup of a few
seconds and a retry budget that outlasts a provider's throttling, with
``max_attempts`` ``LIVE_MAX_ATTEMPTS`` in the task file. An extract cell's
per-attempt call cap is ``extract_cap(batch)``, a little above the batch, so a
throttled document can be retried inside its attempt. ``--rate-limit-per-s``
(extract only) paces the cell's calls across its workers through one limiter file
in the cell's raw directory, which the workers create. The double keeps
``BENCH_WORKER_SETTINGS`` and ``max_attempts`` 3.
"""

import argparse
import itertools
import json
import math
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from decimal import Decimal
from enum import StrEnum
from functools import partial
from pathlib import Path
from typing import IO, Any

import psycopg

from benchmarks import extract, metrics, ratelimit
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
from benchmarks.documents import Document, document_texts, generate_documents
from benchmarks.generate import (
    DEFAULT_SEED,
    Expected,
    build_tasks,
    encode_tasks,
    expected_outcome,
    generate_invoices,
)
from benchmarks.live import (
    API_KEY_ENV,
    BUDGET_FILE_NAME,
    DOUBLE,
    DRY_RUN_LABEL,
    ProviderCallBudgetFileInvalid,
    ProviderChoice,
    ProviderName,
    canned_requests,
    create_budget_file,
    credential_from,
    expected_usage,
    provider_block,
    read_budget_file,
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
from benchmarks.sysload import SysloadSampler
from benchmarks.worker import DEFAULT_CALL_TIMEOUT, DEFAULT_CAP, parse_duration
from orqestra.aggregation.budget import BudgetVerdict, read_job_call_budget
from orqestra.aggregation.invoice import summarize_invoice_job
from orqestra.aggregation.usage import UsageFinality
from orqestra.domain.jobs import JobStatus, JsonValue
from orqestra.domain.states import AttemptState, TaskState
from orqestra.persistence.jobs import get_job_progress
from orqestra.persistence.migrate import migrate
from orqestra.worker.builtin import ECHO, WAIT
from orqestra.worker.fence import ExitCode
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.usage import check_provider_call_budget, usage_handoff_bound
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

# The measurement timing: test values recorded in every result, not
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


# OpenRouter cells: roomier database bounds and a longer retry budget, so a database
# call of a few seconds no longer stops a worker and a throttling episode of tens of
# seconds no longer exhausts a task. They pass WorkerSettings' relationships and
# DEC-070's: 6 s >= 2 x 3 s (and 20 s >= 3 x 3 s + 2 s); 6 + 3 + 2 = 11 s < 20 s;
# 1.5 s < 3 s. With 8 attempts the backoff after attempts 1 to 7 totals 91 to 182 s.
LIVE_WORKER_SETTINGS: Mapping[str, str] = {
    **BENCH_WORKER_SETTINGS,
    "ORQESTRA_WORKER_LEASE": "PT20S",
    "ORQESTRA_WORKER_HEARTBEAT_INTERVAL": "PT6S",
    "ORQESTRA_WORKER_DB_OPERATION_TIMEOUT": "PT3S",
    "ORQESTRA_WORKER_LOCK_TIMEOUT": "PT1.5S",
    "ORQESTRA_WORKER_RETRY_BASE_DELAY": "PT2S",
    "ORQESTRA_WORKER_RETRY_MAX_DELAY": "PT60S",
}
LIVE_MAX_ATTEMPTS = 8
RATE_LIMIT_FILE_NAME = "provider-rate-limit"


def extract_cap(batch: int) -> int:
    """An extract attempt's call cap: its batch plus room to retry throttled documents."""
    return batch + max(5, batch // 5)


def max_attempts_for(provider: ProviderChoice) -> int:
    return LIVE_MAX_ATTEMPTS if provider.openrouter else MAX_ATTEMPTS


def replacement_limit(workers: int) -> int:
    """How many dead workers a cell replaces before it lets its fleet run down."""
    return 4 * workers


# How long after a stalled worker's exit its replacement starts.
STALL_REPLACEMENT_DELAY = 1.0


class Profile(StrEnum):
    ECHO = "echo"
    WAIT = "wait"
    INVOICE = "invoice"
    EXTRACT = "extract"


KIND_OF = {
    Profile.ECHO: ECHO,
    Profile.WAIT: WAIT,
    Profile.INVOICE: JOB_KIND,
    Profile.EXTRACT: extract.JOB_KIND,
}
BATCHED = frozenset({Profile.INVOICE, Profile.EXTRACT})
# Each extraction call's timeout unless --call-timeout says otherwise.
DEFAULT_EXTRACT_CALL_TIMEOUT = "PT30S"


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
        if (self.profile in BATCHED) != (self.batch is not None):
            msg = "only the invoice and extract profiles have a batch size, and they need one"
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
    documents: list[Document] | None = None


def build_workload(cell: Cell, max_attempts: int = MAX_ATTEMPTS) -> Workload:
    if cell.profile is Profile.EXTRACT:
        assert cell.batch is not None
        documents = generate_documents(cell.size, cell.seed)
        tasks = build_tasks(document_texts(documents), cell.batch, max_attempts)
        return Workload(tasks, cell.size, None, documents)
    if cell.profile is Profile.INVOICE:
        assert cell.batch is not None
        invoices = generate_invoices(cell.size, cell.seed)
        tasks = build_tasks(invoices, cell.batch, max_attempts)
        expected = expected_outcome(invoices, cell.batch, DEFAULT_CAP)
        return Workload(tasks, cell.size, expected)
    payload: Callable[[int], JsonValue] = (
        (lambda index: {"n": index})
        if cell.profile is Profile.ECHO
        else (lambda _: {"seconds": WAIT_SECONDS})
    )
    tasks = [
        {"task_key": f"task-{index:07d}", "payload": payload(index), "max_attempts": max_attempts}
        for index in range(cell.size)
    ]
    return Workload(tasks, cell.size, None)


# --- worker processes --------------------------------------------------------------------


def worker_command(
    profile: Profile,
    provider: ProviderChoice = DOUBLE,
    budget_file: Path | None = None,
    call_timeout: str = DEFAULT_CALL_TIMEOUT,
    *,
    cap: int = DEFAULT_CAP,
    extra: Sequence[str] = (),
) -> list[str]:
    """A worker's argv. It never holds a credential: the key travels only in the environment.

    For ``extract``, ``cap`` is the batch size and ``extra`` the dry run's document flags.
    """
    if profile is Profile.EXTRACT:
        if not provider.openrouter:
            msg = "the extract profile needs --provider openrouter"
            raise ValueError(msg)
        return [
            sys.executable,
            "-m",
            "benchmarks.worker",
            "--workload",
            "extract",
            "--cap",
            str(cap),
            "--call-timeout",
            call_timeout,
            *provider.worker_arguments(budget_file),
            *extra,
        ]
    if profile is Profile.INVOICE:
        return [
            sys.executable,
            "-m",
            "benchmarks.worker",
            "--cap",
            str(DEFAULT_CAP),
            "--call-timeout",
            call_timeout,
            *provider.worker_arguments(budget_file),
        ]
    if provider.openrouter:
        msg = "--provider openrouter runs only the invoice profile"
        raise ValueError(msg)
    return [str(ORQESTRA), "worker", "run", "--kind", KIND_OF[profile]]


def process_environment(
    url: str, worker_settings: Mapping[str, str], provider: ProviderChoice = DOUBLE
) -> dict[str, str]:
    """This process's environment without any ``ORQESTRA_*`` value, plus the cell's own.

    For a live OpenRouter run only, the key is passed on from this process's
    environment (never from ``.env``); a dry run and the double never get it.
    """
    env = {key: value for key, value in os.environ.items() if not key.startswith("ORQESTRA_")}
    env = env | dict(worker_settings) | {"ORQESTRA_DATABASE_URL": url}
    if provider.live:
        credential = credential_from(os.environ)
        if credential is None:
            msg = f"{API_KEY_ENV} is not set"
            raise ValueError(msg)
        env[API_KEY_ENV] = credential.get_secret_value()
    return env


@dataclass(slots=True)
class WorkerHandle:
    process: subprocess.Popen[bytes]
    log_path: Path
    log: IO[bytes]
    number: int = 0  # 1-based, in start order: its log is worker-<number>.log
    exited: dict[str, Any] | None = None  # its accounting, once it has been reaped
    died: bool = False  # it exited while the cell's job still had work

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
    provider: ProviderChoice = DOUBLE
    budget_file: Path | None = None
    call_timeout: str = DEFAULT_CALL_TIMEOUT
    cap: int = DEFAULT_CAP
    extra: tuple[str, ...] = ()
    replaced: int = 0  # workers started in place of one that died
    scheduled: int = 0  # replacements granted, started or still due
    # Replacements not started yet: (due, in seconds of the wait, the dead worker's record).
    pending: list[tuple[float, dict[str, Any]]] = field(default_factory=list)

    def start(self) -> WorkerHandle:
        """Start one more worker. Every worker runs from ``ROOT``, where the harness lives."""
        self.started += 1
        path = self.log_dir / f"worker-{self.started}.log"
        log = path.open("wb")
        process = subprocess.Popen(  # a fixed command
            worker_command(
                self.profile,
                self.provider,
                self.budget_file,
                self.call_timeout,
                cap=self.cap,
                extra=self.extra,
            ),
            cwd=ROOT,
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
        )
        handle = WorkerHandle(process, path, log, self.started)
        self.workers.append(handle)
        return handle

    def supervise(
        self, elapsed: float, limit: int, delay: float = STALL_REPLACEMENT_DELAY
    ) -> list[dict[str, Any]]:
        """Record every worker that has exited since the last call, grant it a replacement
        while fewer than ``limit`` have been granted, and start every replacement that is
        due: ``delay`` after a ``STALLED`` exit, at once after any other. ``elapsed`` is the
        time into the wait; the new records, in start order."""
        found = []
        for handle in list(self.workers):
            if handle.died or handle.running():
                continue
            assert handle.exited is not None
            handle.died = True
            code = handle.exited["exit_code"]
            record: dict[str, Any] = {
                "worker": handle.number,
                "pid": handle.process.pid,
                "exit_code": code,
                "at_s": round(elapsed, 3),
                "replaced": False,
                "replacement_at_s": None,
            }
            if self.scheduled < limit:
                self.scheduled += 1
                wait = delay if code == ExitCode.STALLED else 0.0
                self.pending.append((elapsed + wait, record))
            found.append(record)
        self._start_due(elapsed)
        return found

    def _start_due(self, elapsed: float) -> None:
        still = []
        for due, record in self.pending:
            if due > elapsed:
                still.append((due, record))
                continue
            self.start()
            self.replaced += 1
            record["replaced"] = True
            record["replacement_at_s"] = round(elapsed, 3)
        self.pending = still

    def settle(self, timeout: float = STARTUP_TIMEOUT) -> None:
        """Wait, at most ``timeout``, until every worker still running has logged
        ``worker_started``, so a SIGTERM finds its handlers installed. Never raises."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pending = [
                handle
                for handle in self.workers
                if handle.running() and not _logged(handle.log_path, "worker_started")
            ]
            if not pending:
                return
            time.sleep(0.05)

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
    """Gate item 5: the job's invoice summary against the oracle."""
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
    provider: ProviderChoice = DOUBLE,
) -> dict[str, Any]:
    """Gate item 6: the usage report and call budget against the oracle.

    The succeeded attempts' calls and tokens are exact in every run. ``clean``
    means no task needed a second attempt, so the job totals must equal them
    too, with ``CLOSED`` finality and a ``within`` budget verdict. With
    OpenRouter the amounts are credits, compared only when known (a dry run).
    """
    _require_invoice(profile)
    read = read_job_call_budget(conn, job_id, cap=expected.cap)
    if read is None:
        return _check("usage", False, "no such job")
    report, budget = read
    succeeded = report.by_attempt_state[AttemptState.SUCCEEDED]
    if provider.openrouter:
        credits_passed, credits_detail = _credits_usage(
            report, budget, succeeded, expected, provider, clean
        )
        return _check("usage", credits_passed, credits_detail)
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
    return _amount(totals, TOKENS)


def _amount(totals: Mapping[str, Any], unit: str) -> Decimal:
    total = totals.get(unit)
    return Decimal(0) if total is None else Decimal(total.amount)


def _credits_usage(
    report: Any,
    budget: Any,
    succeeded: Any,
    expected: Expected,
    provider: ProviderChoice,
    clean: bool,
) -> tuple[bool, dict[str, Any]]:
    """The usage check for OpenRouter: calls against the oracle, credits when known."""
    unit, oracle_amount = expected_usage(provider, expected.calls)
    succeeded_amount = _amount(succeeded.reported_totals, unit)
    amount = _amount(report.reported_totals, unit)
    detail: dict[str, Any] = {
        "unit": unit,
        "succeeded_calls": succeeded.calls,
        "succeeded_amount": str(succeeded_amount),
        "oracle_calls": expected.calls,
        "oracle_amount": None if oracle_amount is None else str(oracle_amount),
        "calls": report.calls,
        "amount": str(amount),
        "finality": report.finality.value,
        "budget_verdict": budget.verdict.value,
        "budget_bound": budget.bound,
    }
    passed = succeeded.calls == expected.calls and (
        oracle_amount is None or succeeded_amount == oracle_amount
    )
    if clean:
        passed = (
            passed
            and report.calls == expected.calls
            and (oracle_amount is None or amount == oracle_amount)
            and report.finality is UsageFinality.CLOSED
            and budget.verdict is BudgetVerdict.WITHIN
        )
    return passed, detail


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
    provider: ProviderChoice = DOUBLE,
    documents: int | None = None,
    exits_while_running: Sequence[int] = (),
) -> list[dict[str, Any]]:
    """Every gate check that applies to ``profile``, in order.

    For ``extract``, ``documents`` is the cell's document count. ``exit_codes`` are the
    workers stopped at the end; ``exits_while_running`` the codes of the ones that
    exited mid-cell and were replaced, each of which must be ``STALLED``.
    """
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
    checks.append(worker_exits_check(exit_codes, expected_exit_codes, exits_while_running))
    if profile is Profile.EXTRACT:
        assert documents is not None
        attempts = _count(
            conn,
            "SELECT count(*) FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE t.job_id = %s",
            job_id,
        )
        clean = attempts == sum(progress.task_counts.values()) and not failed
        checks.extend(extract.extraction_gate(conn, job_id, documents, clean=clean))
    elif profile is Profile.INVOICE:
        assert expected is not None
        attempts = _count(
            conn,
            "SELECT count(*) FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE t.job_id = %s",
            job_id,
        )
        tasks = sum(progress.task_counts.values())
        clean = attempts == tasks and not failed
        checks.append(summary_check(conn, job_id, profile, expected))
        checks.append(usage_check(conn, job_id, profile, expected, clean=clean, provider=provider))
    else:
        wrong = _count(conn, _RESULT_MISMATCH[profile], job_id)
        checks.append(_check("results_exact", wrong == 0, {"mismatches": wrong}))
    return checks


def worker_exits_check(
    exit_codes: Sequence[int],
    expected_exit_codes: Sequence[int] | None,
    exits_while_running: Sequence[int],
) -> dict[str, Any]:
    """``worker_exits``: the stopped workers' codes are the expected ones (0 each unless
    given), and every worker that exited mid-cell stopped itself for a stalled database
    call (``STALLED``), the watchdog's designed fail-fast; any other code fails."""
    wanted = [0] * len(exit_codes) if expected_exit_codes is None else list(expected_exit_codes)
    stalled = all(code == ExitCode.STALLED for code in exits_while_running)
    detail: dict[str, Any] = {"exit_codes": list(exit_codes)}
    if exits_while_running:
        detail["exits_while_running"] = list(exits_while_running)
    return _check("worker_exits", list(exit_codes) == wanted and stalled, detail)


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


# --- checkpoints during a cell ------------------------------------------------------------

# The server's cumulative checkpoint counters: (view, timed, requested, write ms, sync ms).
# PostgreSQL 17 moved them from pg_stat_bgwriter to pg_stat_checkpointer.
_CHECKPOINT_SOURCES = (
    ("pg_stat_checkpointer", "num_timed", "num_requested", "write_time", "sync_time"),
    (
        "pg_stat_bgwriter",
        "checkpoints_timed",
        "checkpoints_req",
        "checkpoint_write_time",
        "checkpoint_sync_time",
    ),
)
CHECKPOINT_SAMPLE_INTERVAL = 1.0

# One reading: (seconds into the wait, timed, requested, write ms, sync ms).
CheckpointSample = tuple[float, float, float, float, float]


def checkpoint_source(conn: metrics.Conn) -> tuple[str, ...] | None:
    """The view this server keeps its checkpoint counters in, or ``None``. Asked with
    ``to_regclass``, so no query on a missing view is ever sent."""
    for source in _CHECKPOINT_SOURCES:
        row = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (f"pg_catalog.{source[0]}",))
        found = row.fetchone()
        if found is not None and found[0]:
            return source
    return None


class CheckpointWatch:
    """Reads the server's checkpoint counters while a cell waits, each reading in its own
    read-only transaction (so a fresh statistics snapshot, and never the job-progress
    read's). A reading that fails is counted and skipped: it never ends a cell."""

    def __init__(self, conn: metrics.Conn) -> None:
        self._conn = conn
        self.samples: list[CheckpointSample] = []
        self.lost = 0
        self.last: float | None = None
        try:
            self.source = metrics.read_only(conn, checkpoint_source)
        except Exception:  # an unreadable catalog: not measured, never a failed cell
            self.source = None

    def sample(self, at: float) -> None:
        self.last = at
        if self.source is None:
            return
        view, *columns = self.source
        query = f"SELECT {', '.join(f'{c}::float8' for c in columns)} FROM pg_catalog.{view}"
        try:
            row = metrics.read_only(self._conn, lambda conn: conn.execute(query).fetchone())
        except Exception:  # a lost reading, never a failed cell
            self.lost += 1
            return
        if row is not None:
            timed, requested, write, sync = (float(value) for value in row)
            self.samples.append((at, timed, requested, write, sync))

    def due(self, at: float) -> bool:
        return self.last is None or at - self.last >= CHECKPOINT_SAMPLE_INTERVAL

    def summary(self) -> dict[str, Any] | str:
        source = None if self.source is None else self.source[0]
        return summarize_checkpoints(source, self.samples, self.lost)


def summarize_checkpoints(
    source: str | None, samples: Sequence[CheckpointSample], lost: int = 0
) -> dict[str, Any] | str:
    """The checkpoints that ended between the first and the last reading.

    PostgreSQL publishes a checkpoint's counts and times when it ends, so a reading whose
    count rose marks the checkpoints that ended since the one before: an event, with the
    seconds into the wait at which it was first seen (``ended_by_s``) and their write and
    sync time. ``longest_ms`` is the largest write + sync of an event of exactly one
    checkpoint (two ending within one interval are counted together). A checkpoint still
    running at the last reading is not seen. A counter that falls (a statistics reset)
    starts a new baseline. ``not_measured`` without a view or two readings.
    """
    if source is None or len(samples) < 2:
        return metrics.NOT_MEASURED
    events: list[dict[str, Any]] = []
    timed = requested = 0
    write = sync = 0.0
    for previous, current in itertools.pairwise(samples):
        deltas = [now - before for now, before in zip(current[1:], previous[1:], strict=True)]
        if any(delta < 0 for delta in deltas):
            continue  # reset: this reading is the new baseline
        more_timed, more_requested, more_write, more_sync = deltas
        ended = int(more_timed + more_requested)
        if not ended:
            continue
        timed += int(more_timed)
        requested += int(more_requested)
        write += more_write
        sync += more_sync
        events.append(
            {
                "ended_by_s": round(current[0], 3),
                "checkpoints": ended,
                "write_ms": round(more_write, 3),
                "sync_ms": round(more_sync, 3),
            }
        )
    single = [event["write_ms"] + event["sync_ms"] for event in events if event["checkpoints"] == 1]
    return {
        "source": source,
        "count": timed + requested,
        "timed": timed,
        "requested": requested,
        "write_ms": round(write, 3),
        "sync_ms": round(sync, 3),
        "longest_ms": round(max(single), 3) if single else None,
        "events": events,
        "samples": len(samples),
        "lost_samples": lost,
    }


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
    provider: ProviderChoice = DOUBLE,
    call_timeout: str = DEFAULT_CALL_TIMEOUT,
    prices: extract.Prices | None = None,
    rate_limit_per_s: Decimal | None = None,
) -> dict[str, Any]:
    """Run ``cell`` against the migrated schema ``url`` and return its result record.

    ``stop_after`` bounds the wait for the job in seconds; a job not finished by
    then makes the cell ``incomplete``. The wait also ends once every worker has
    exited; ``timing`` records which ended it and how long it lasted
    (``cell_status``). Raw files go to ``raw_dir``. ``hook``, if given, is called
    on every poll while the job runs (drills use it). ``provider`` is the double
    unless ``benchmarks.run`` was given ``--provider openrouter`` (DEC-071); then
    the cell's call counter is created in ``raw_dir`` before any worker starts. An
    ``extract`` cell needs OpenRouter and ``prices`` (``CANNED_PRICES`` for a dry run);
    its per-attempt cap is ``extract_cap(batch)``. Its tasks get ``max_attempts_for``
    the provider. ``rate_limit_per_s`` (extract only) is passed to every worker with
    the cell's limiter file. A worker that exits mid-cell is replaced (module docstring).
    """
    run_id = run_id or uuid.uuid4().hex[:12]
    # Validated once, before any worker starts: every value read below comes from here.
    validated = bench_settings(worker_settings)
    refusal = provider.refusal(os.environ)
    if refusal is not None:
        raise ValueError(refusal)
    if provider.openrouter and cell.profile not in BATCHED:
        msg = "--provider openrouter runs only the invoice profile or the extract profile"
        raise ValueError(msg)
    extracting = cell.profile is Profile.EXTRACT
    if extracting and (not provider.openrouter or prices is None):
        msg = "the extract profile needs --provider openrouter and the model's prices"
        raise ValueError(msg)
    if rate_limit_per_s is not None and not extracting:
        msg = "--rate-limit-per-s applies only to the extract profile"
        raise ValueError(msg)
    raw_dir.mkdir(parents=True, exist_ok=True)
    max_attempts = max_attempts_for(provider)
    workload = build_workload(cell, max_attempts)
    env = process_environment(url, worker_settings, provider)
    budget_file = create_budget_file(raw_dir / BUDGET_FILE_NAME) if provider.openrouter else None
    cap = cell_cap(cell)
    extra = extract_worker_extra(cell, provider, raw_dir, rate_limit_per_s)
    fleet = Fleet(
        cell.profile,
        env,
        raw_dir,
        provider=provider,
        budget_file=budget_file,
        call_timeout=call_timeout,
        cap=cap,
        extra=extra,
    )
    with (
        psycopg.connect(url, autocommit=True) as conn,
        psycopg.connect(url, autocommit=True) as sampling,
    ):
        record_environment = environment(conn, container=container, note=note)
        harness_start = metrics.harness_usage()
        own = [conn.info.backend_pid]  # the harness's polling backend, left out of contention
        contention: metrics.Sampler | None = None
        docker: metrics.Sampler | None = None
        # Machine load from the submission to the end of the wait, on its own connection,
        # whose backend is left out of contention like the harness's own (``own``).
        sysload_backends: list[int] = []

        def sysload_connect() -> metrics.Conn:
            connection = psycopg.connect(url, autocommit=True)
            sysload_backends.append(connection.info.backend_pid)
            return connection

        sysload = SysloadSampler(
            lambda: [h.process.pid for h in fleet.workers if h.exited is None], sysload_connect
        )
        load: dict[str, Any] | None = None
        exits_while_running: list[dict[str, Any]] = []
        try:
            for _ in range(cell.workers):
                fleet.start()
            fleet.wait_started()
            before = metrics.read_only(conn, metrics.database_counters)
            checkpoints = CheckpointWatch(conn)
            counted = time.monotonic()
            job_id, submission = submit(url, KIND_OF[cell.profile], workload.tasks, raw_dir)
            # The samplers cover the job alone: from the submission to the end of the wait.
            contention = metrics.Sampler(
                lambda: metrics.read_only(
                    sampling,
                    partial(metrics.contention_sample, exclude=[*own, *sysload_backends]),
                )
            ).start()
            if container:
                docker = metrics.Sampler(lambda: metrics.docker_stats_sample(container)).start()
            sysload.start()
            began = time.monotonic()
            checkpoints.sample(0.0)
            before_job = began - counted
            wait_ended = WaitEnded.STOP_AFTER
            while time.monotonic() - began < stop_after:
                if checkpoints.due(time.monotonic() - began):
                    checkpoints.sample(time.monotonic() - began)
                progress = metrics.read_only(conn, partial(get_job_progress, job_id=job_id))
                assert progress is not None
                counts = progress.task_counts
                if counts[TaskState.QUEUED] + counts[TaskState.RUNNING] == 0:
                    wait_ended = WaitEnded.FINISHED
                    break
                if hook is not None:
                    hook(Running(conn, job_id, fleet, time.monotonic() - began, counts))
                exits_while_running += fleet.supervise(
                    time.monotonic() - began, replacement_limit(cell.workers)
                )
                if fleet.alive() == 0 and not fleet.pending:
                    wait_ended = WaitEnded.NO_WORKERS
                    break
                time.sleep(POLL)
            ended = time.monotonic()
            waited = ended - began
            load = sysload.stop()
            contention_samples = contention.stop()
            contention_window = time.monotonic() - began
            # A docker stats reading takes about a second, so its window is its own.
            docker_samples = docker.stop() if docker else None
            docker_window = time.monotonic() - began
            time.sleep(STATS_SETTLE)
            after = metrics.read_only(conn, metrics.database_counters)
            checkpoints.sample(time.monotonic() - began)
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
            if load is None:
                load = sysload.stop()  # idempotent; only an exception reaches here running
            shutdown = validated.shutdown_timeout.total_seconds()
            fleet.settle()  # a replacement still starting would die of the SIGTERM
            reaped = fleet.stop(timeout=shutdown + 10.0)
            for sampler in (contention, docker):
                if sampler is not None:
                    sampler.stop()  # idempotent; only an exception reaches here running
        harness = metrics.harness_delta(harness_start, metrics.harness_usage())
        assert contention is not None  # the wait ran, so its sampler did
        contention_errors = contention.errors
        exit_codes = [worker["exit_code"] for worker in reaped]
        stopped_codes = [
            handle.exited["exit_code"]
            for handle in fleet.workers
            if not handle.died and handle.exited is not None
        ]
        times = metrics.read_stored_times(conn, job_id)
        figures = metrics.timing_figures(times)
        gate = run_gate(
            conn,
            job_id,
            cell.profile,
            workload.expected,
            stopped_codes,
            provider=provider,
            documents=workload.items if extracting else None,
            exits_while_running=[died["exit_code"] for died in exits_while_running],
        )
        failures = failure_figures(conn, job_id, exit_codes)
        failures["exits_while_running"] = exits_while_running
        failures["replacements"] = fleet.replaced
        usage = (
            usage_figures(conn, job_id, DEFAULT_CAP) if cell.profile is Profile.INVOICE else None
        )
        extraction: dict[str, Any] | None = None
        if extracting:
            assert workload.documents is not None
            assert prices is not None
            extraction = extract.extraction_block(
                conn,
                job_id,
                [document.expected for document in workload.documents],
                [document.expected_status for document in workload.documents],
                prices,
            )
    status = cell_status(wait_ended, gate)
    database_stats = metrics.delta(before, after)
    tasks = len(workload.tasks)
    commits = metrics.job_commits(
        database_stats["database"]["xact_commit"],
        None if idle_rate is None else idle_rate["per_s"],
        before_job + after_job,
        None if idle_rate is None else idle_rate["window_s"],
        implied_idle_rate(cell.workers, validated),
    )
    database_stats["commits"] = commits
    database_stats["checkpoints"] = checkpoints.summary()
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
            provider_block(
                provider,
                call_timeout=call_timeout,
                cap=cap,
                route="route-a",
                calls_made=(
                    extraction["calls"]["total"]
                    if extraction is not None
                    else None
                    if usage is None
                    else usage["calls"]
                ),
            )
            | (
                {"max_attempts": max_attempts, "rate_limit_per_s": _text(rate_limit_per_s)}
                if provider.openrouter
                else {}  # the double's block is unchanged
            )
            if cell.profile in BATCHED
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
            "sysload": load,
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
    if extraction is not None:
        record["extraction"] = extraction
    # Whether a dirty tree's record may be written is the writer's decision; its shape is
    # checked here either way.
    validate_result({**record, "environment": {**record_environment, "dirty_allowed": True}})
    return record


def _text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def implied_idle_rate(workers: int, settings: WorkerSettings) -> float:
    """The idle commits a second the settings imply: each idle worker claims once per
    poll interval and runs one recovery sweep step per recovery interval, each its own
    transaction. An upper estimate, not a floor: each call takes time, so the real
    rate runs a little lower (metrics.job_commits uses the larger of the two)."""
    poll = settings.poll_interval.total_seconds()
    sweep = settings.recovery_interval.total_seconds()
    return workers * (1 / poll + 1 / sweep)


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
    provider: ProviderChoice = DOUBLE,
    call_timeout: str = DEFAULT_CALL_TIMEOUT,
    prices: extract.Prices | None = None,
    rate_limit_per_s: Decimal | None = None,
) -> tuple[dict[str, Any], Path]:
    """Run ``cell`` in a fresh run schema of the benchmark database; write its result file.

    An OpenRouter cell runs with ``cell_worker_settings``; the double with the bench's own.
    """
    worker_settings = cell_worker_settings(cell, provider, call_timeout, rate_limit_per_s)
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
                worker_settings=worker_settings,
                raw_dir=RAW_DIR / run_id / cell.name,
                stop_after=stop_after,
                run_id=run_id,
                ladder=ladder,
                container=container,
                note=note,
                provider=provider,
                call_timeout=call_timeout,
                prices=prices,
                rate_limit_per_s=rate_limit_per_s,
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


def add_provider_arguments(parser: argparse.ArgumentParser) -> None:
    """The provider flags; only this command has them (DEC-071)."""
    parser.add_argument(
        "--provider",
        type=ProviderName,
        choices=list(ProviderName),
        default=ProviderName.DOUBLE,
        help="what answers the invoice calls: the offline double (default) or openrouter",
    )
    parser.add_argument("--model", help="openrouter: the model slug")
    parser.add_argument(
        "--max-provider-calls", type=int, help="openrouter: the most calls the whole cell makes"
    )
    parser.add_argument(
        "--allow-live-calls",
        action="store_true",
        help="openrouter: real, billable calls (explicit opt-in; use a spend-limited key, DEC-071)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="openrouter: canned responses, no network, no cost"
    )
    parser.add_argument(
        "--call-timeout",
        help=(
            f"openrouter: each call's timeout, ISO 8601 (default {DEFAULT_CALL_TIMEOUT}; "
            f"{DEFAULT_EXTRACT_CALL_TIMEOUT} for the extract profile)"
        ),
    )
    parser.add_argument(
        "--price-in-per-mtok",
        type=Decimal,
        help="extract, live: the model's price per million input tokens, from its price list",
    )
    parser.add_argument(
        "--price-out-per-mtok",
        type=Decimal,
        help="extract, live: the model's price per million output tokens, from its price list",
    )
    parser.add_argument(
        "--rate-limit-per-s",
        type=Decimal,
        help="extract: pace the cell's model calls across its workers to this many a second",
    )


def provider_refusal(
    choice: ProviderChoice, cell: Cell, call_timeout: str | None, environ: Mapping[str, str]
) -> str | None:
    """Why this command may not run ``cell`` with ``choice``, or ``None``."""
    refusal = choice.refusal(environ)
    if refusal is not None:
        return refusal
    if cell.profile is Profile.EXTRACT and not choice.openrouter:
        return "the extract profile needs --provider openrouter: the double reads amounts only"
    if not choice.openrouter:
        return "--call-timeout applies only to --provider openrouter" if call_timeout else None
    if cell.profile not in BATCHED:
        return "--provider openrouter runs only the invoice profile or the extract profile"
    assert choice.max_provider_calls is not None
    if cell.workers > choice.max_provider_calls:
        return (
            "--workers is above --max-provider-calls: more workers than calls is almost "
            "certainly a typo"
        )
    if call_timeout is not None:
        try:
            parse_duration(call_timeout)
        except argparse.ArgumentTypeError as exc:
            return f"--call-timeout: {exc}"
    return None


def rate_limit_refusal(cell: Cell, rate: Decimal | None) -> str | None:
    """Why ``--rate-limit-per-s`` may not be used for ``cell``, or ``None``."""
    if rate is None:
        return None
    if cell.profile is not Profile.EXTRACT:
        return "--rate-limit-per-s applies only to the extract profile"
    try:
        ratelimit.check_rate(float(rate))  # the workers' own rule, so it is refused up front
    except ValueError as exc:
        return f"--rate-limit-per-s: {exc}"
    return None


def prices_of(
    cell: Cell,
    choice: ProviderChoice,
    price_in: Decimal | None,
    price_out: Decimal | None,
) -> extract.Prices | None:
    """The prices an extract cell is costed at; ``ValueError`` when they are wrong or missing.

    A live run must be given both, from the model's price list: nothing is assumed. A
    dry run uses ``CANNED_PRICES`` and refuses given ones, so a dry run's cost is never
    mistaken for a real one.
    """
    given = price_in is not None or price_out is not None
    if cell.profile is not Profile.EXTRACT:
        if given:
            msg = "--price-in-per-mtok and --price-out-per-mtok apply only to the extract profile"
            raise ValueError(msg)
        return None
    if choice.dry_run:
        if given:
            msg = "a dry run uses synthetic prices: leave out --price-in/out-per-mtok"
            raise ValueError(msg)
        return extract.CANNED_PRICES
    if price_in is None or price_out is None:
        msg = "a live extract run needs --price-in-per-mtok and --price-out-per-mtok"
        raise ValueError(msg)
    return extract.Prices(price_in, price_out)


def provider_worker_settings(
    worker_settings: Mapping[str, str],
    cap: int,
    call_timeout: str,
    *,
    pacing_wait: timedelta = timedelta(0),
) -> dict[str, str]:
    """``worker_settings`` with an execution timeout that fits ``cap`` calls: the one rule
    every OpenRouter cell and drill uses.

    The rule is ``check_provider_call_budget``'s: ``cap * (call_timeout + 2 * handoff
    bound) <= execution_timeout - fence_margin``, rounded up to whole seconds. Settings
    that already fit keep their own text. Only the execution timeout changes; the copy is
    validated as the workers will read it, and ``ValueError`` says why it cannot fit.
    """
    settings = dict(worker_settings)
    base = bench_settings(settings)
    # A paced call may first wait up to ``pacing_wait`` for the cell's limiter.
    per_call = parse_duration(call_timeout) + 2 * usage_handoff_bound(base) + pacing_wait
    needed = (cap * per_call + base.fence_margin).total_seconds()
    if needed > base.execution_timeout.total_seconds():  # else the bench's own text stays
        settings["ORQESTRA_WORKER_EXECUTION_TIMEOUT"] = f"PT{math.ceil(needed)}S"
    check_provider_call_budget(
        bench_settings(settings),
        max_provider_calls_per_attempt=cap,
        provider_call_timeout=parse_duration(call_timeout),
    )
    return settings


def extract_worker_extra(
    cell: Cell, provider: ProviderChoice, raw_dir: Path, rate_limit_per_s: Decimal | None
) -> tuple[str, ...]:
    """An extract worker's flags beyond the provider's: a dry run's documents, and the
    cell's pacing with its limiter file, which the workers create in ``raw_dir``."""
    if cell.profile is not Profile.EXTRACT:
        return ()
    extra: tuple[str, ...] = ()
    if provider.dry_run:
        extra = ("--documents", str(cell.size), "--seed", str(cell.seed))
    if rate_limit_per_s is not None:
        limiter = raw_dir / RATE_LIMIT_FILE_NAME
        extra += ("--rate-limit-per-s", str(rate_limit_per_s), "--rate-limit-file", str(limiter))
    return extra


def cell_cap(cell: Cell) -> int:
    """A cell's per-attempt call cap: ``extract_cap`` of its batch, or the invoice cap."""
    if cell.profile is Profile.EXTRACT and cell.batch:
        return extract_cap(cell.batch)
    return DEFAULT_CAP


def limiter_max_wait() -> timedelta:
    """The longest one call may wait for a pacing token: the workers' limiter's own bound,
    so this command never states a bound of its own."""
    return timedelta(seconds=ratelimit.LIMITER_MAX_WAIT)


def cell_worker_settings(
    cell: Cell,
    provider: ProviderChoice,
    call_timeout: str,
    rate_limit_per_s: Decimal | None = None,
    *,
    pacing_wait: timedelta | None = None,
) -> dict[str, str]:
    """The settings a cell's workers run with: the bench's own for the double, and for
    OpenRouter ``provider_worker_settings`` of ``LIVE_WORKER_SETTINGS`` at the cell's cap.

    A paced cell's execution timeout also gives every call of its cap the limiter's
    longest wait (``limiter_max_wait``, or ``pacing_wait`` when given), so a busy
    limiter delays an attempt rather than failing it.
    """
    if not provider.openrouter:
        return dict(BENCH_WORKER_SETTINGS)
    wait = timedelta(0)
    if rate_limit_per_s is not None and cell.profile is Profile.EXTRACT:
        wait = pacing_wait if pacing_wait is not None else limiter_max_wait()
    return provider_worker_settings(
        LIVE_WORKER_SETTINGS, cell_cap(cell), call_timeout, pacing_wait=wait
    )


def bench_settings(worker_settings: Mapping[str, str]) -> WorkerSettings:
    """``worker_settings`` as the worker will read them, without touching the environment."""
    fields = {
        key.removeprefix("ORQESTRA_WORKER_").lower(): value
        for key, value in worker_settings.items()
    }
    return WorkerSettings.model_validate(fields)  # no settings source is read


def dry_run_preview(
    cell: Cell, choice: ProviderChoice, call_timeout: str, rate_limit_per_s: Decimal | None = None
) -> list[str]:
    """What a dry run prints before its cell: the oracle's calls and the hard bounds."""
    if cell.profile is Profile.EXTRACT:
        return _extract_preview(cell, choice, call_timeout, rate_limit_per_s)
    attempts = max_attempts_for(choice)
    workload = build_workload(cell, attempts)
    assert workload.expected is not None
    assert choice.max_provider_calls is not None
    calls = workload.expected.calls
    bound = DEFAULT_CAP * attempts * len(workload.tasks)
    execution = "?"
    try:
        settings = cell_worker_settings(cell, choice, call_timeout)
        execution = settings["ORQESTRA_WORKER_EXECUTION_TIMEOUT"]
        verdict = "fits"
    except (TypeError, ValueError) as exc:
        verdict = f"refused: {exc}"
    lines = [
        f"expected provider calls (oracle): {calls}",
        f"cell call maximum (--max-provider-calls): {choice.max_provider_calls}",
        f"call budget bound (DEC-065, cap x sum of max_attempts): {bound}",
        f"per-attempt call budget (cap {DEFAULT_CAP}, call timeout {call_timeout}, "
        f"execution timeout {execution}): {verdict}",
    ]
    if calls > choice.max_provider_calls:
        lines.append("the call maximum would stop this run before every invoice is verified")
    return [f"{DRY_RUN_LABEL}: {line}" for line in lines]


def _extract_preview(
    cell: Cell, choice: ProviderChoice, call_timeout: str, rate_limit_per_s: Decimal | None
) -> list[str]:
    assert cell.batch is not None
    assert choice.max_provider_calls is not None
    tasks = math.ceil(cell.size / cell.batch)
    cap = cell_cap(cell)
    attempts = max_attempts_for(choice)
    settings = cell_worker_settings(cell, choice, call_timeout, rate_limit_per_s)
    lines = [
        f"expected provider calls (one per document): {cell.size}",
        f"cell call maximum (--max-provider-calls): {choice.max_provider_calls}",
        f"call budget bound (cap {cap} x max_attempts {attempts} x tasks): "
        f"{cap * attempts * tasks}",
        f"execution timeout for a cap of {cap} at {call_timeout} a call: "
        f"{settings['ORQESTRA_WORKER_EXECUTION_TIMEOUT']}",
    ]
    if cell.size > choice.max_provider_calls:
        lines.append("the call maximum would stop this run before every document is read")
    return [f"{DRY_RUN_LABEL}: {line}" for line in lines]


def _counted(path: Path) -> int | str:
    """The counter's count, or ``not measured`` when it cannot be read: never a crash."""
    try:
        return read_budget_file(path)
    except (ProviderCallBudgetFileInvalid, OSError):
        return metrics.NOT_MEASURED.replace("_", " ")


def dry_run_summary(record: Mapping[str, Any], raw_dir: Path) -> list[str]:
    """What a dry run prints after its cell: the requests sent and the counts kept."""
    lines = [
        f"requests answered by the canned transport: {canned_requests(raw_dir)}",
        f"calls in the usage ledger: {record['provider']['calls_made']}",
        f"calls counted against the maximum: {_counted(raw_dir / BUDGET_FILE_NAME)}",
    ]
    return [f"{DRY_RUN_LABEL}: {line}" for line in lines]


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
    add_provider_arguments(parser)
    args = parser.parse_args(argv)
    choice = ProviderChoice(
        name=args.provider,
        model=args.model,
        max_provider_calls=args.max_provider_calls,
        allow_live_calls=args.allow_live_calls,
        dry_run=args.dry_run,
    )
    default_timeout = (
        DEFAULT_EXTRACT_CALL_TIMEOUT if args.profile is Profile.EXTRACT else DEFAULT_CALL_TIMEOUT
    )
    call_timeout = args.call_timeout or default_timeout
    run_id = uuid.uuid4().hex[:12]
    try:
        cell = Cell(args.profile, args.size, args.batch, args.workers, args.repetition, args.seed)
        refusal = provider_refusal(
            choice, cell, args.call_timeout, os.environ
        ) or rate_limit_refusal(cell, args.rate_limit_per_s)
        if refusal is not None:
            raise ValueError(refusal)
        prices = prices_of(cell, choice, args.price_in_per_mtok, args.price_out_per_mtok)
        refuse_dirty(args.allow_dirty)
        if choice.dry_run:
            print("\n".join(dry_run_preview(cell, choice, call_timeout, args.rate_limit_per_s)))
        record, path = run_in_bench_database(
            cell,
            run_id=run_id,
            ladder="single",
            stop_after=args.stop_after_minutes * 60,
            out_dir=args.out_dir,
            keep_schema=args.keep_schema,
            allow_dirty=args.allow_dirty,
            container=args.db_container,
            note=args.note,
            provider=choice,
            call_timeout=call_timeout,
            prices=prices,
            rate_limit_per_s=args.rate_limit_per_s,
        )
    except (ValueError, DirtyTreeError, UnsafeBenchDatabaseError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    except psycopg.OperationalError as exc:
        print(f"refused: {unreachable_message(exc)}", file=sys.stderr)
        return 1
    if choice.dry_run:
        print("\n".join(dry_run_summary(record, RAW_DIR / run_id / cell.name)))
    print(f"{record['status']} {path}")
    return 0 if record["status"] == Status.COMPLETE else 1


if __name__ == "__main__":
    sys.exit(main())
