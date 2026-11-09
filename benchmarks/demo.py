"""The end-to-end demonstration: ``python -m benchmarks.demo``.

One run, on the guarded benchmark database (``benchmarks.database``), shows the
whole engine through its shipped commands:

1. a fresh run schema, migrated with ``orqestra migrate``;
2. seeded synthetic invoices, written as a task file, with the outcome the
   oracle expects (``benchmarks.generate``) and the provable call bound
   (``call_bound``, DEC-065) printed before anything is submitted;
3. ``orqestra job submit`` twice with one idempotency key: the second prints
   ``"created": false`` and the same job id;
4. four benchmark workers; once 30 % of the tasks have succeeded, one worker
   holding a running attempt is killed with SIGKILL between two provider calls
   (``KillDrill(at_call_boundary=True)``), and its revocation by another
   worker's sweep, the replacement attempt and a restarted worker are printed;
5. ``orqestra job status``, ``job summary`` and ``job usage`` checked against
   the oracle, and the correctness gate of ``benchmarks.run`` with the kill's
   own checks;
6. makespan, throughput and recovery delay, each labelled "this machine, this
   run": they are printed and recorded, never checked.

The workers run ``python -m benchmarks.worker``, because invoice verification
is not registered with ``orqestra worker run`` (DEC-062). Its provider is the
offline ``SeparatorReader`` double, so nothing here says anything about a real
provider. The result record goes under ``build/bench/demo/``, which git
ignores; the report never reads it. The exit code is 0 only when every check
passed. ``docs/demo.md`` explains each printed line.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from benchmarks import metrics
from benchmarks.database import (
    BENCH_DATABASE_NAME,
    UnsafeBenchDatabaseError,
    connect_verified,
    create_run_schema,
    drop_run_schema,
    require_bench_database_url,
    run_schema_name,
    schema_url,
    unreachable_message,
)
from benchmarks.drills import (
    INJECTION_POLL,
    DrillFleet,
    Harness,
    KillDrill,
    Poll,
    wait_over,
    worker_pid,
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
    DirtyTreeError,
    Status,
    WaitEnded,
    environment,
    now,
    write_result,
)
from benchmarks.run import (
    BENCH_WORKER_SETTINGS,
    MAX_ATTEMPTS,
    ORQESTRA,
    POLL,
    Cell,
    Profile,
    add_common_arguments,
    cell_status,
    failure_figures,
    process_environment,
    refuse_dirty,
    run_gate,
    usage_figures,
    worker_command,
)
from benchmarks.worker import DEFAULT_CALL_TIMEOUT, DEFAULT_CAP, parse_duration
from orqestra.aggregation.budget import call_bound
from orqestra.domain.states import AttemptState, TaskState
from orqestra.workloads.invoice.handler import JOB_KIND

DEMO_DIR = RAW_DIR / "demo"  # git-ignored; report.render reads only benchmarks/results/
DEFAULT_INVOICES = 5_000
DEFAULT_BATCH = 100
DEFAULT_WORKERS = 4
DEFAULT_STOP_AFTER = 1800.0  # a bound on a hung run, in seconds; never an expectation
CELL_NAME = "demo"  # the run schema is orq_bench_<run>_demo
TAGS = (
    "database",
    "schema",
    "migrate",
    "invoices",
    "expected",
    "bound",
    "submit",
    "resubmit",
    "workers",
    "kill",
    "revoked",
    "replaced",
    "restarted",
    "status",
    "summary",
    "usage",
    "makespan",
    "throughput",
    "recovery",
    "result",
    "wall time",
    "verdict",
)
LABEL = "this machine, this run"
TOKENS = "tokens"
Emit = Callable[[str, str], None]
Check = dict[str, Any]  # {"name", "passed", "detail"}, as run_gate's checks


def _check(name: str, passed: bool, detail: Any = None) -> Check:
    return {"name": name, "passed": passed, "detail": detail}


@dataclass(frozen=True, slots=True)
class DemoConfig:
    """What one demonstration runs; the defaults are the documented run."""

    invoices: int = DEFAULT_INVOICES
    seed: int = DEFAULT_SEED
    batch: int = DEFAULT_BATCH
    workers: int = DEFAULT_WORKERS
    stop_after: float = DEFAULT_STOP_AFTER

    def __post_init__(self) -> None:
        _ = self.cell  # Cell refuses what it cannot run
        if self.workers < 2:
            msg = "the demo needs at least 2 workers: another worker's sweep revokes the killed one"
            raise ValueError(msg)
        if self.batches <= self.workers:
            msg = "the demo needs more batches than workers, so a kill finds work left"
            raise ValueError(msg)
        if not self.stop_after > 0:
            msg = "stop_after must be positive"
            raise ValueError(msg)

    @property
    def idempotency_key(self) -> str:
        return f"demo-{self.seed}"

    @property
    def cell(self) -> Cell:
        return Cell(Profile.INVOICE, self.invoices, self.batch, self.workers, 1, self.seed)

    @property
    def batches(self) -> int:
        return -(-self.invoices // self.batch)


@dataclass(frozen=True, slots=True)
class CliResult:
    """One ``orqestra`` command: its exit code, its stdout JSON line and its stderr file."""

    returncode: int
    document: dict[str, Any]
    stderr_path: Path


@dataclass(frozen=True, slots=True)
class DemoOutcome:
    record: dict[str, Any]
    path: Path
    checks: list[Check]

    @property
    def passed(self) -> bool:
        return self.record["status"] == Status.COMPLETE and all(c["passed"] for c in self.checks)


class DemoAbortedError(RuntimeError):
    """A step the rest depends on failed (migrate, submit, worker start-up)."""


def print_line(tag: str, text: str) -> None:
    print(f"{tag:<10} {text}", flush=True)


def cli(
    url: str, args: Sequence[str], cwd: Path, name: str, *, timeout: float = 600.0
) -> CliResult:
    """Run the installed ``orqestra`` with ``args`` against ``url``; keep its output in ``cwd``.

    It runs in ``cwd``, the run's raw directory, with no ``ORQESTRA_*`` variable
    but the database URL, so a developer's ``.env`` or environment is never read.
    """
    completed = subprocess.run(  # a fixed command
        [str(ORQESTRA), *args],
        cwd=cwd,
        env=process_environment(url, {}),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    (cwd / f"{name}.json").write_text(completed.stdout)
    stderr_path = cwd / f"{name}.log"
    stderr_path.write_text(completed.stderr)
    document: dict[str, Any] = {}
    if completed.stdout.strip():
        try:
            parsed = json.loads(completed.stdout)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            document = parsed
    return CliResult(completed.returncode, document, stderr_path)


def expected_text(expected: Expected) -> str:
    return (
        f"verified {expected.verified}, rejected {expected.rejected}, "
        f"needs_review {expected.needs_review}, malformed {expected.malformed}; "
        f"provider calls {expected.calls}"
    )


def bound_of(tasks: Sequence[Mapping[str, Any]], cap: int = DEFAULT_CAP) -> int:
    """The job's provable call bound: ``cap`` times the sum of the tasks' retry limits."""
    return call_bound(sum(int(task["max_attempts"]) for task in tasks), cap)


def submission_checks(first: CliResult, second: CliResult) -> list[Check]:
    one, two = first.document, second.document
    return [
        _check(
            "submit_created",
            first.returncode == 0 and one.get("status") == "ok" and one.get("created") is True,
            {"exit": first.returncode, "created": one.get("created")},
        ),
        _check(
            "resubmit_same_job",
            second.returncode == 0
            and two.get("status") == "ok"
            and two.get("created") is False
            and two.get("job_id") is not None
            and two.get("job_id") == one.get("job_id"),
            {"exit": second.returncode, "created": two.get("created")},
        ),
    ]


def status_check(document: Mapping[str, Any], tasks: int) -> Check:
    counts = document.get("task_counts") or {}
    others = [counts.get(state.value) for state in TaskState if state is not TaskState.SUCCEEDED]
    passed = (
        document.get("job_status") == "succeeded"
        and counts.get(TaskState.SUCCEEDED.value) == tasks
        and all(count == 0 for count in others)
    )
    return _check("cli_status", passed, {"job_status": document.get("job_status"), **counts})


def summary_against_oracle(document: Mapping[str, Any], expected: Expected) -> Check:
    outcome = {
        name: document.get(name)
        for name in ("verified", "rejected", "needs_review", "malformed", "provider_assisted_items")
    }
    oracle = {
        "verified": expected.verified,
        "rejected": expected.rejected,
        "needs_review": expected.needs_review,
        "malformed": expected.malformed,
        "provider_assisted_items": expected.provider_assisted,
    }
    anomalies = document.get("anomalies") or {}
    passed = (
        outcome == oracle
        and document.get("expected_items") == expected.invoices
        and document.get("processed_items") == expected.invoices
        and document.get("complete") is True
        and document.get("integrity_ok") is True
        and isinstance(anomalies, Mapping)
        and all(count == 0 for count in anomalies.values())
    )
    return _check("cli_summary", passed, {"outcome": outcome, "oracle": oracle})


def usage_against_oracle(document: Mapping[str, Any], expected: Expected, bound: int) -> Check:
    budget = document.get("budget") or {}
    succeeded = (document.get("by_attempt_state") or {}).get(AttemptState.SUCCEEDED.value) or {}
    tokens = (succeeded.get("reported_totals") or {}).get(TOKENS) or {}
    try:
        amount = Decimal(str(tokens.get("amount")))
    except ArithmeticError:
        amount = None
    passed = (
        document.get("finality") == "closed"
        and budget.get("verdict") == "within"
        and budget.get("bound") == bound
        and succeeded.get("calls") == expected.calls
        and amount == Decimal(expected.tokens)
    )
    detail = {
        "finality": document.get("finality"),
        "verdict": budget.get("verdict"),
        "bound": budget.get("bound"),
        "succeeded_calls": succeeded.get("calls"),
        "succeeded_tokens": tokens.get("amount"),
    }
    return _check("cli_usage", passed, detail)


def _seconds(value: float | None, digits: int = 3) -> str:
    return "not measured" if value is None else f"{value:.{digits}f} s"


def figure_lines(figures: Mapping[str, Any], measured: Mapping[str, Any]) -> list[tuple[str, str]]:
    """The timing lines: printed and recorded, never checked."""
    makespan = figures["timing"]["makespan_s"]
    rate = figures["throughput"]["makespan_items_per_s"]
    delay = measured.get("recovery_delay_s", {}).get("max")
    return [
        ("makespan", f"{_seconds(makespan)}, from the job's creation ({LABEL})"),
        (
            "throughput",
            ("not measured" if rate is None else f"{rate:.1f} invoices/s")
            + f" over the makespan ({LABEL})",
        ),
        (
            "recovery",
            f"{_seconds(delay)} from the killed attempt's lease end to its replacement's "
            f"start ({LABEL})",
        ),
    ]


_VICTIM = (
    "SELECT a.state, a.revoked_reason, a.lease_expires_at, a.finished_at, a.task_id, "
    "a.attempt_number, t.task_key FROM attempts a JOIN tasks t ON t.id = a.task_id "
    "WHERE a.id = %s"
)
_SUCCESSOR = (
    "SELECT id, worker_id, started_at FROM attempts WHERE task_id = %s AND attempt_number = %s"
)


@dataclass(slots=True)
class VictimWatch:
    """Prints the kill, the victim's revocation and its replacement, each once."""

    killed: bool = False
    revoked: bool = False
    replaced: bool = False
    restarted: bool = False
    detail: dict[str, Any] = field(default_factory=dict)

    def after_step(self, drill: KillDrill, succeeded: int, tasks: int, emit: Emit) -> None:
        if self.killed or not drill.injected:
            return
        self.killed = True
        entry = drill.injections[0]
        self.detail["attempt_id"] = entry["attempt_id"]
        emit(
            "kill",
            f"SIGKILL worker pid {entry['pid']} holding attempt {entry['attempt_id']}, "
            f"at {succeeded} of {tasks} tasks succeeded, between two provider calls",
        )

    def observe(self, conn: Any, fleet: DrillFleet, emit: Emit) -> None:
        if not self.killed or self.replaced:
            return
        row = conn.execute(_VICTIM, (uuid.UUID(self.detail["attempt_id"]),)).fetchone()
        if row is None:
            return
        state, reason, lease_end, finished, task_id, number, task_key = row
        if not self.revoked and state == "revoked":
            self.revoked = True
            self.detail |= {"state": state, "revoked_reason": reason}
            emit(
                "revoked",
                f"attempt {number} of {task_key}: {reason}, lease ended {lease_end.isoformat()}, "
                f"revoked {finished.isoformat()} by another worker's recovery sweep",
            )
        if not self.revoked:
            return
        successor = conn.execute(_SUCCESSOR, (task_id, number + 1)).fetchone()
        if successor is None:
            return
        self.replaced = True
        _, worker_id, started = successor
        self.detail["successor_attempt"] = number + 1
        emit(
            "replaced",
            f"attempt {number + 1} of {task_key} started {started.isoformat()} "
            f"by worker pid {worker_pid(str(worker_id))}",
        )
        handle = fleet.start()
        self.restarted = True
        emit("restarted", f"a replacement worker, pid {handle.process.pid}")

    def checks(self) -> list[Check]:
        return [
            _check(
                "revocation_seen",
                self.revoked,
                {"revoked_reason": self.detail.get("revoked_reason")},
            ),
            _check(
                "replacement_seen", self.replaced, {"attempt": self.detail.get("successor_attempt")}
            ),
        ]


def _condensed(document: Mapping[str, Any], names: Sequence[str]) -> dict[str, Any]:
    return {name: document.get(name) for name in names}


def run_demo(
    url: str,
    config: DemoConfig,
    *,
    worker_settings: Mapping[str, str] = BENCH_WORKER_SETTINGS,
    raw_dir: Path,
    out_dir: Path,
    run_id: str | None = None,
    allow_dirty: bool = False,
    note: str = "",
    emit: Emit = print_line,
) -> DemoOutcome:
    """Run the demonstration against the empty schema ``url``; write and return its record."""
    run_id = run_id or uuid.uuid4().hex[:12]
    raw_dir.mkdir(parents=True, exist_ok=True)
    checks: list[Check] = []

    migrated = cli(url, ["migrate"], raw_dir, "migrate")
    ok = migrated.returncode == 0 and migrated.document.get("status") == "ok"
    checks.append(_check("migrated", ok, {"exit": migrated.returncode}))
    if not ok:
        msg = f"orqestra migrate failed (exit {migrated.returncode}); see {migrated.stderr_path}"
        raise DemoAbortedError(msg)
    emit("migrate", f"applied {', '.join(migrated.document.get('applied') or []) or 'nothing'}")

    invoices = generate_invoices(config.invoices, config.seed)
    tasks = build_tasks(invoices, config.batch, MAX_ATTEMPTS)
    expected = expected_outcome(invoices, config.batch, DEFAULT_CAP)
    bound = bound_of(tasks)
    task_file = raw_dir / "tasks.json"
    task_file.write_bytes(encode_tasks(tasks))
    emit(
        "invoices",
        f"{config.invoices} synthetic invoices, seed {config.seed}, batch {config.batch}: "
        f"{len(tasks)} tasks, max_attempts {MAX_ATTEMPTS}",
    )
    emit("expected", expected_text(expected))
    emit("bound", f"at most {bound} provider calls: cap {DEFAULT_CAP} x sum of max_attempts")

    submit_args = [
        "job",
        "submit",
        "--kind",
        JOB_KIND,
        "--idempotency-key",
        config.idempotency_key,
        "--max-attempts",
        str(MAX_ATTEMPTS),
        str(task_file),
    ]
    began_submit = time.monotonic()
    first = cli(url, submit_args, raw_dir, "submit")
    submit_seconds = time.monotonic() - began_submit
    emit("submit", json.dumps(first.document))
    if first.returncode != 0 or first.document.get("status") != "ok":
        msg = f"orqestra job submit failed (exit {first.returncode}); see {first.stderr_path}"
        raise DemoAbortedError(msg)
    second = cli(url, submit_args, raw_dir, "resubmit")
    same = second.document.get("job_id") == first.document.get("job_id")
    emit("resubmit", f"{json.dumps(second.document)} (same job id: {'yes' if same else 'no'})")
    checks += submission_checks(first, second)
    job_id = uuid.UUID(str(first.document["job_id"]))

    env = process_environment(url, worker_settings)
    fleet = DrillFleet(Profile.INVOICE, env, raw_dir, command=worker_command(Profile.INVOICE))
    harness = Harness(url)
    drill = KillDrill(1, at_call_boundary=True)
    watch = VictimWatch()
    operation = parse_duration(worker_settings["ORQESTRA_WORKER_DB_OPERATION_TIMEOUT"])
    quiet = 2 * operation.total_seconds() + 1.0  # as run_drill: a statement outlives its bound
    shutdown = parse_duration(worker_settings["ORQESTRA_WORKER_SHUTDOWN_TIMEOUT"])
    harness_start = metrics.harness_usage()
    wait_ended = WaitEnded.STOP_AFTER
    waited = 0.0
    restarted_ok = True
    try:
        record_environment = environment(harness.conn, note=note)
        try:
            for _ in range(config.workers):
                fleet.start()
            fleet.wait_started()
        except RuntimeError as exc:
            msg = f"the workers did not start: {exc}"
            raise DemoAbortedError(msg) from None
        emit(
            "workers",
            f"{config.workers} x python -m benchmarks.worker, pids "
            + ", ".join(str(h.process.pid) for h in fleet.workers),
        )
        began = time.monotonic()
        while time.monotonic() - began < config.stop_after:
            counts = harness.progress(job_id).task_counts
            finished = wait_over(counts, drill)
            if not finished:
                poll = Poll(harness, job_id, fleet, len(tasks), counts[TaskState.SUCCEEDED], quiet)
                drill.step(poll)
                watch.after_step(drill, counts[TaskState.SUCCEEDED], len(tasks), emit)
            watch.observe(harness.conn, fleet, emit)
            if finished:
                wait_ended = WaitEnded.FINISHED
                break
            if fleet.alive() == 0:
                wait_ended = WaitEnded.NO_WORKERS
                break
            time.sleep(POLL if drill.injected else INJECTION_POLL)
        waited = time.monotonic() - began
    except BaseException:
        harness.close()
        raise
    finally:
        try:
            try:
                fleet.wait_started()
            except RuntimeError:
                restarted_ok = False
            reaped = fleet.stop(timeout=shutdown.total_seconds() + 10.0)
        finally:
            drill.release()
            drill.close()
    try:
        conn = harness.conn
        status_cli = cli(url, ["job", "status", str(job_id)], raw_dir, "status")
        summary_cli = cli(url, ["job", "summary", str(job_id)], raw_dir, "summary")
        usage_cli = cli(
            url,
            ["job", "usage", str(job_id), "--max-provider-calls-per-attempt", str(DEFAULT_CAP)],
            raw_dir,
            "usage",
        )
        codes = [worker["exit_code"] for worker in reaped]
        failures = failure_figures(conn, job_id, codes)
        status_doc, summary_doc, usage_doc = (
            status_cli.document,
            summary_cli.document,
            usage_cli.document,
        )
        emit(
            "status",
            f"{status_doc.get('job_status')}, tasks {json.dumps(status_doc.get('task_counts'))}; "
            f"attempts {json.dumps(failures['attempts_by_state'], sort_keys=True)}",
        )
        emit(
            "summary",
            json.dumps(
                _condensed(
                    summary_doc,
                    (
                        "verified",
                        "rejected",
                        "needs_review",
                        "malformed",
                        "provider_assisted_items",
                        "complete",
                        "integrity_ok",
                    ),
                )
            ),
        )
        budget = usage_doc.get("budget") or {}
        emit(
            "usage",
            json.dumps(
                {
                    "finality": usage_doc.get("finality"),
                    "calls": usage_doc.get("calls"),
                    "verdict": budget.get("verdict"),
                    "bound": budget.get("bound"),
                }
            ),
        )
        wanted = [drill.expected_exit(w["pid"], w["exit_code"]) for w in reaped]
        gate = run_gate(conn, job_id, Profile.INVOICE, expected, codes, expected_exit_codes=wanted)
        gate += drill.checks(conn, job_id, fleet)
        gate += checks
        gate += watch.checks()
        gate += [
            _check(
                "worker_restarted",
                watch.restarted and restarted_ok,
                {"restarts": len(fleet.workers) - config.workers},
            ),
            status_check(status_doc, len(tasks)),
            summary_against_oracle(summary_doc, expected),
            usage_against_oracle(usage_doc, expected, bound),
        ]
        figures = metrics.timing_figures(metrics.read_stored_times(conn, job_id))
        measured = drill.measured(conn, job_id, fleet)
        usage = usage_figures(conn, job_id, DEFAULT_CAP)
    finally:
        harness.close()
    status = cell_status(wait_ended, gate)
    record: dict[str, Any] = {
        "format": 1,
        "run_id": run_id,
        "ladder": "demo",
        "status": status.value,
        "recorded_at": now(),
        "cell": {**asdict(config.cell), "profile": Profile.INVOICE.value},
        "environment": record_environment,
        "worker_settings": dict(worker_settings),
        "provider": {"call_timeout": DEFAULT_CALL_TIMEOUT, "cap": DEFAULT_CAP, "route": "route-a"},
        "timing": {
            **figures["timing"],
            "submission": {"seconds": submit_seconds, "tasks": len(tasks)},
            "tasks": len(tasks),
            "items": expected.invoices,
            "stop_after_s": config.stop_after,
            "wait_ended": wait_ended.value,
            "waited_s": waited,
        },
        "throughput": figures["throughput"] if status is Status.COMPLETE else None,
        "latency": figures["latency"],
        "claim_gap": figures["claim_gap"],
        "idle_claim_rate": None,
        "resources": {
            "workers": reaped,
            "harness": metrics.harness_delta(harness_start, metrics.harness_usage()),
        },
        "database_stats": None,
        "contention": None,
        "failures": failures,
        "usage": usage,
        "gate": gate,
        "drill": {
            "name": "demo",
            "injected": drill.injected,
            "injections": drill.injections,
            "restarts": len(fleet.workers) - config.workers,
            "measured": measured,
            "idempotency_key": config.idempotency_key,
            "call_bound": bound,
            "cli": {
                "status": _condensed(status_doc, ("job_status", "task_counts")),
                "summary": _condensed(
                    summary_doc,
                    (
                        "verified",
                        "rejected",
                        "needs_review",
                        "malformed",
                        "provider_assisted_items",
                        "complete",
                        "integrity_ok",
                    ),
                ),
                "usage": {"finality": usage_doc.get("finality"), "budget": budget},
            },
        },
    }
    path = write_result(record, out_dir, allow_dirty=allow_dirty)
    for tag, text in figure_lines(figures, measured):
        emit(tag, text)
    emit("result", f"{path} (raw files: {raw_dir})")
    return DemoOutcome(record, path, gate)


def run_in_bench_database(
    config: DemoConfig,
    url: str,
    *,
    out_dir: Path,
    keep_schema: bool,
    allow_dirty: bool,
    note: str,
    emit: Emit = print_line,
) -> DemoOutcome:
    """Run the demo in a fresh run schema of the benchmark database, then drop it."""
    with connect_verified(url) as admin:
        info = admin.info
        where = f"socket {info.host}" if info.host.startswith("/") else f"{info.host}:{info.port}"
        emit("database", f"{BENCH_DATABASE_NAME} on {where}: the benchmark database guard passed")
        run_id = uuid.uuid4().hex[:12]
        schema = run_schema_name(run_id, CELL_NAME)
        create_run_schema(admin, schema)
        emit("schema", f"{schema} created, empty")
        try:
            return run_demo(
                schema_url(admin, url, schema),
                config,
                raw_dir=RAW_DIR / run_id / CELL_NAME,
                out_dir=out_dir,
                run_id=run_id,
                allow_dirty=allow_dirty,
                note=note,
                emit=emit,
            )
        finally:
            if not keep_schema:
                drop_run_schema(admin, schema)
                emit("schema", f"{schema} dropped")


def _first_line(exc: BaseException) -> str:
    lines = str(exc).strip().splitlines()
    return lines[0] if lines else type(exc).__name__


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.demo", description=__doc__)
    parser.add_argument("--invoices", type=int, default=DEFAULT_INVOICES)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument(
        "--stop-after", type=float, default=DEFAULT_STOP_AFTER, help="a bound, in seconds"
    )
    add_common_arguments(parser)
    parser.set_defaults(out_dir=DEMO_DIR)
    args = parser.parse_args(argv)
    began = time.monotonic()
    try:
        config = DemoConfig(args.invoices, args.seed, args.batch, args.workers, args.stop_after)
        refuse_dirty(args.allow_dirty)
        url = require_bench_database_url(os.environ)
        outcome = run_in_bench_database(
            config,
            url,
            out_dir=args.out_dir,
            keep_schema=args.keep_schema,
            allow_dirty=args.allow_dirty,
            note=args.note,
        )
    except (ValueError, DirtyTreeError, UnsafeBenchDatabaseError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    except psycopg.OperationalError as exc:
        print(f"refused: {unreachable_message(exc)}", file=sys.stderr)
        return 1
    except DemoAbortedError as exc:
        print_line("verdict", f"FAIL: aborted: {_first_line(exc)}")
        return 1
    print_line("wall time", f"{time.monotonic() - began:.1f} s ({LABEL}; printed, never checked)")
    failed = [str(check["name"]) for check in outcome.checks if not check["passed"]]
    if outcome.passed:
        print_line("verdict", f"PASS: {len(outcome.checks)} checks passed")
        return 0
    status = outcome.record["status"]
    print_line("verdict", f"FAIL: {', '.join(failed) or status}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
