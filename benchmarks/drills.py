"""Failure drills (plan 2.9): ``python -m benchmarks.drills <drill>``.

Each drill runs the ``invoice`` profile (by default 2,000 invoices, batch 100,
4 workers, ``benchmarks.run.BENCH_WORKER_SETTINGS``) in a fresh run schema of
the benchmark database, injects one failure, and then applies the profile's
correctness gate (plan 2.6) with the drill-adjusted oracle, plus the drill's
own checks. A drill result is a format 1 record with ``ladder: "drill"`` and
no throughput; it is written under ``benchmarks/results/drills/``, which no
ladder or performance table reads.

The harness acts as the supervisor Orqestra does not have (DEC-054): a worker
that exits while the job runs is recorded (its exit code and accounting) and
replaced after ``RESTART_DELAY``.

========  =========================================================================
drill     injection, once the succeeded tasks reach 30 %
========  =========================================================================
``d1``    SIGKILL workers that each hold a running attempt (2 by default)
``d2``    SIGSTOP one such worker for the lease plus 2 s, then SIGCONT
``d3a``   the workers reach PostgreSQL through ``PgProxy``, which cuts every
          connection and refuses new ones for 5 s; the harness connects directly
``d3b``   ``docker compose -p orqestra-bench restart postgres``; refused unless
          ``--allow-db-restart`` is given, ``COMPOSE_PROJECT_NAME`` is
          ``orqestra-bench``, and that project's container is the server the
          URL reaches (the same ``system_identifier``); injected only if the
          server's ``pg_postmaster_start_time()`` moved forward; never run by tests
``d4``    two jobs on workers with ``benchmarks.faults``: a payload job with one
          extra non-batch task, and a retry job with an always-failing and a
          fail-once provider answer
========  =========================================================================

The job is submitted before any worker starts, and the harness polls every
10 ms until the injection, because a job of this size can pass 30 % within one
ordinary poll. A victim is chosen without racing its own completion: the
harness stops every live worker, waits until each shows as stopped and until
no client backend of the database is still running a statement (one a worker
sent before it stopped could otherwise still commit), and only then reads
which attempts are running and which PID (from ``worker_id``) owns each. Those
owners are the candidates; every worker not chosen is continued. Before the
workers are stopped at the end, the harness waits until every running worker,
replacements included, has logged ``worker_started``, so each one stops under
its own signal handlers.

Nothing here predicts a duration. Delays are measured from stored PostgreSQL
timestamps and recorded; the checks assert outcomes and the order of stored
timestamps only.
"""

import argparse
import contextlib
import copy
import itertools
import json
import math
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import psycopg

from benchmarks import metrics
from benchmarks.database import (
    BENCH_DATABASE_URL_ENV,
    UnsafeBenchDatabaseError,
    connect_verified,
    create_run_schema,
    drop_run_schema,
    require_bench_database_url,
    run_schema_name,
    schema_url,
)
from benchmarks.faults import Fault, create_fault_table, poison_total
from benchmarks.generate import (
    POISON_KEY_PREFIX,
    Expected,
    Kind,
    build_tasks,
    expected_outcome,
    generate_invoices,
    kind_of,
)
from benchmarks.result import (
    RAW_DIR,
    RESULTS_DIR,
    ROOT,
    DirtyTreeError,
    Status,
    environment,
    now,
    validate_result,
    write_result,
)
from benchmarks.run import (
    BENCH_WORKER_SETTINGS,
    MAX_ATTEMPTS,
    POLL,
    STARTUP_TIMEOUT,
    Cell,
    Fleet,
    Profile,
    WorkerHandle,
    add_common_arguments,
    failure_figures,
    process_environment,
    refuse_dirty,
    run_gate,
    submit,
    usage_figures,
    worker_command,
)
from benchmarks.worker import DEFAULT_CALL_TIMEOUT, DEFAULT_CAP, parse_duration
from orqestra.aggregation.usage import AttemptCapture, attempt_capture
from orqestra.domain.jobs import JobProgress, JobStatus, JsonValue
from orqestra.domain.retry import RetryPolicy, retry_delay
from orqestra.domain.states import TaskState
from orqestra.persistence.jobs import get_job_progress
from orqestra.persistence.migrate import migrate
from orqestra.persistence.snapshots import job_batch_snapshot
from orqestra.worker.fence import ExitCode
from orqestra.workloads.invoice.handler import BATCH_PAYLOAD_INVALID, JOB_KIND
from tests.integration.pg_proxy import PgProxy

DRILLS_DIR = RESULTS_DIR / "drills"
TRIGGER_SHARE = 0.3
INJECTION_POLL = 0.01  # until the injection: a job can pass 30 % within one ordinary poll
RESTART_DELAY = 5.0
OUTAGE_SECONDS = 5.0
FREEZE_EXTRA = 2.0
STOP_BOUND = 10.0  # how long a signalled process may take to show as stopped or exited
RECONNECT_BOUND = 300.0
BENCH_PROJECT = "orqestra-bench"
RESTART_COMMAND = ("docker", "compose", "-p", BENCH_PROJECT, "restart", "postgres")
IDENTITY_COMMAND = (
    *("docker", "compose", "-p", BENCH_PROJECT, "exec", "-T", "-u", "postgres", "postgres"),
    "pg_controldata",
)
FAULTS_COMMAND = (
    sys.executable,
    "-m",
    "benchmarks.faults",
    "--cap",
    str(DEFAULT_CAP),
    "--call-timeout",
    DEFAULT_CALL_TIMEOUT,
)

Conn = psycopg.Connection[Any]
Check = dict[str, Any]


def _check(name: str, passed: bool, detail: Any = None) -> Check:
    return {"name": name, "passed": passed, "detail": detail}


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _seconds(delta: timedelta | None) -> float | None:
    return None if delta is None else delta.total_seconds()


# --- the harness -------------------------------------------------------------------------


@dataclass(slots=True)
class DrillFleet(Fleet):
    """A ``Fleet`` whose workers run ``command``: the benchmark worker, or the faulting one."""

    command: list[str] = field(default_factory=list)

    def start(self) -> WorkerHandle:
        self.started += 1
        path = self.log_dir / f"worker-{self.started}.log"
        log = path.open("wb")
        process = subprocess.Popen(  # a fixed command
            self.command,
            cwd=ROOT,
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
        )
        handle = WorkerHandle(process, path, log)
        self.workers.append(handle)
        return handle

    def handle(self, pid: int) -> WorkerHandle | None:
        return next((h for h in self.workers if h.process.pid == pid), None)


class Harness:
    """The harness's own connection, straight to the run schema; it can reconnect."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.conn: Conn = psycopg.connect(url, autocommit=True)

    def reconnect(self, bound: float = RECONNECT_BOUND) -> float:
        """Connect again, retrying until ``bound`` seconds; the seconds it took."""
        began = time.monotonic()
        self.conn.close()
        while True:
            try:
                self.conn = psycopg.connect(self.url, autocommit=True, connect_timeout=5)
                return time.monotonic() - began
            except psycopg.OperationalError:
                if time.monotonic() - began >= bound:
                    raise
                time.sleep(POLL)

    def progress(self, job_id: uuid.UUID) -> JobProgress:
        try:
            progress = get_job_progress(self.conn, job_id)
        except psycopg.OperationalError:
            self.reconnect()
            progress = get_job_progress(self.conn, job_id)
        assert progress is not None
        return progress

    def clock(self) -> datetime:
        row = self.conn.execute("SELECT clock_timestamp()").fetchone()
        assert row is not None
        return cast(datetime, row[0])

    def server_started(self) -> datetime:
        """When the server this connection reached last started: it moves only on a restart."""
        row = self.conn.execute("SELECT pg_postmaster_start_time()").fetchone()
        assert row is not None
        return cast(datetime, row[0])

    def close(self) -> None:
        self.conn.close()


@dataclass(slots=True)
class Poll:
    """What a drill sees on each poll while the job runs."""

    harness: Harness
    job_id: uuid.UUID
    fleet: DrillFleet
    tasks: int
    succeeded: int
    quiet_bound: float  # how long a drill waits for in-flight statements to end

    @property
    def conn(self) -> Conn:
        return self.harness.conn

    def reached(self, share: float = TRIGGER_SHARE) -> bool:
        return self.succeeded >= math.ceil(share * self.tasks)


def worker_pid(worker_id: str) -> int | None:
    """The PID in a worker id ``host:pid:hex`` (``runtime.new_worker_id``)."""
    parts = worker_id.rsplit(":", 2)
    return int(parts[1]) if len(parts) == 3 and parts[1].isdigit() else None


def _process_state(pid: int) -> str:
    return subprocess.run(  # a fixed command
        ["ps", "-o", "stat=", "-p", str(pid)],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    ).stdout.strip()


def _wait_stopped(pid: int) -> bool:
    """Whether ``pid`` shows as stopped within ``STOP_BOUND`` seconds."""
    deadline = time.monotonic() + STOP_BOUND
    while time.monotonic() < deadline:
        if _process_state(pid).startswith("T"):
            return True
        time.sleep(0.01)
    return False


_RUNNING_ATTEMPTS = (
    "SELECT a.id, a.worker_id FROM attempts a JOIN tasks t ON t.id = a.task_id "
    "WHERE t.job_id = %s AND a.state = 'running' ORDER BY a.started_at, a.id"
)


@dataclass(frozen=True, slots=True)
class Victim:
    pid: int
    attempt_id: uuid.UUID


_ACTIVE_BACKENDS = (
    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
    "AND backend_type = 'client backend' AND state = 'active' AND pid <> pg_backend_pid()"
)


def _await_quiet(poll: Poll) -> bool:
    """Wait until no other client backend of this database runs a statement.

    A stopped worker cannot send anything new, but a statement it had already
    sent (a completion, a heartbeat) still runs on the server and may commit.
    Once no backend is active, nothing a stopped worker sent is still running.
    Bounded by ``poll.quiet_bound``; whether it became quiet is returned.
    """
    deadline = time.monotonic() + poll.quiet_bound
    while True:
        if _one(poll.conn, _ACTIVE_BACKENDS)[0] == 0:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)


def freeze_victims(poll: Poll, count: int, taken: set[int]) -> list[Victim]:
    """Stop up to ``count`` workers, each owning an attempt still running once it is stopped.

    Every live worker not in ``taken`` is stopped first, and the owners are read
    only once no statement a stopped worker sent is still running on the server
    (``_await_quiet``), so no attempt can end while they are read. The workers
    that are not chosen are always continued, also when reading fails.
    """
    live = [h for h in poll.fleet.workers if h.process.pid not in taken and h.running()]
    victims: list[Victim] = []
    try:
        for handle in live:
            os.kill(handle.process.pid, signal.SIGSTOP)
        stopped = {h.process.pid for h in live if _wait_stopped(h.process.pid)}
        if not _await_quiet(poll):
            return []  # tried again on a later poll
        for attempt_id, worker_id in poll.conn.execute(_RUNNING_ATTEMPTS, (poll.job_id,)):
            pid = worker_pid(worker_id)
            chosen = {victim.pid for victim in victims}
            if pid in stopped and pid not in chosen and len(victims) < count:
                victims.append(Victim(pid, attempt_id))
    except BaseException:
        victims = []
        raise
    finally:
        chosen = {victim.pid for victim in victims}
        for handle in live:
            if handle.process.pid not in chosen:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(handle.process.pid, signal.SIGCONT)
    return victims


# --- drills ------------------------------------------------------------------------------


class Drill:
    """No injection; the base of every drill."""

    name = "none"

    def __init__(self) -> None:
        self.injected = False
        self.injections: list[dict[str, Any]] = []
        self.killed: set[int] = set()  # expected to exit by SIGKILL
        self.recorded: set[int] = set()  # their exit codes are recorded, not predicted

    def worker_url(self, url: str) -> str:
        return url

    def step(self, poll: Poll) -> None:
        """Called on every poll while the job runs; never blocks for long."""

    def pending(self) -> bool:
        """Whether an injection is still in force that the drill itself must undo.

        While it is, the wait does not end on a finished job (``wait_over``).
        """
        return False

    def release(self) -> None:
        """Undo any injection still in force, before the workers are stopped."""

    def close(self) -> None:
        """Release what the drill holds, after the workers are stopped."""

    def expected_exit(self, pid: int, code: int) -> int:
        if pid in self.recorded:
            return code
        if pid in self.killed:
            return -signal.SIGKILL
        return 0

    def checks(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> list[Check]:
        return [_check("injected", self.injected, {"injections": len(self.injections)})]

    def measured(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> dict[str, Any]:
        return {}


_VICTIM_ATTEMPT = (
    "SELECT a.state, a.revoked_reason, a.lease_expires_at, a.finished_at, a.task_id, "
    "a.attempt_number, (SELECT count(*) FROM provider_calls c WHERE c.attempt_id = a.id) "
    "FROM attempts a WHERE a.id = %s"
)
_NEXT_START = "SELECT started_at FROM attempts WHERE task_id = %s AND attempt_number = %s"
_CLOSURE = "SELECT closed_by, capture_tier FROM attempt_usage_closures WHERE attempt_id = %s"
_UNRESOLVED = (
    "SELECT count(*) FROM provider_calls c WHERE c.attempt_id = %s AND NOT EXISTS "
    "(SELECT 1 FROM provider_call_results r "
    "WHERE r.attempt_id = c.attempt_id AND r.call_seq = c.call_seq)"
)
_LEDGER_ROWS = "SELECT count(*) FROM provider_calls WHERE attempt_id = %s"


def _captures(conn: Conn, job_id: uuid.UUID) -> dict[uuid.UUID, AttemptCapture]:
    """Each attempt's capture state, read as the usage report reads it."""
    with job_batch_snapshot(conn, job_id) as snapshot:
        assert snapshot is not None
        return {row.attempt_id: attempt_capture(row) for row in snapshot.attempt_usage()}


def _one(conn: Conn, query: str, *params: object) -> tuple[Any, ...]:
    row = conn.execute(query, params).fetchone()
    assert row is not None
    return tuple(row)


class KillDrill(Drill):
    """D1: SIGKILL ``victims`` workers, each holding a running attempt.

    With ``at_call_boundary``, a victim whose attempt has a provider call between
    its intent and its result is continued and chosen again on a later poll, so
    the kill never leaves an unanswered intent: recovery then closes the attempt
    ``complete`` and the usage report can read ``closed``. The count is read
    after ``freeze_victims`` has stopped every worker and waited for the server
    to go quiet, so it is final. Drill d1 keeps the default and kills mid-call too.
    """

    name = "d1"

    def __init__(self, victims: int, *, at_call_boundary: bool = False) -> None:
        super().__init__()
        if victims < 1:
            msg = "d1 needs at least one victim"
            raise ValueError(msg)
        self.victims = victims
        self.at_call_boundary = at_call_boundary

    def step(self, poll: Poll) -> None:
        if self.injected or not poll.reached():
            return
        taken = {entry["pid"] for entry in self.injections}
        for victim in freeze_victims(poll, self.victims - len(self.injections), taken):
            if self.at_call_boundary:
                try:
                    in_flight = _one(poll.conn, _UNRESOLVED, victim.attempt_id)[0]
                except BaseException:
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(victim.pid, signal.SIGCONT)
                    raise
                if in_flight:  # a call is between its intent and its result: try again later
                    os.kill(victim.pid, signal.SIGCONT)
                    continue
            os.kill(victim.pid, signal.SIGKILL)
            self.killed.add(victim.pid)
            handle = poll.fleet.handle(victim.pid)
            assert handle is not None
            deadline = time.monotonic() + STOP_BOUND
            while handle.running() and time.monotonic() < deadline:  # reaped: no write follows
                time.sleep(0.01)
            ledger_rows = _one(poll.conn, _LEDGER_ROWS, victim.attempt_id)[0]
            self.injections.append(
                {
                    "pid": victim.pid,
                    "attempt_id": str(victim.attempt_id),
                    "killed_at": _iso(poll.harness.clock()),
                    "ledger_rows_at_kill": ledger_rows,
                }
            )
        if len(self.injections) < self.victims:
            return  # the rest on a later poll
        self.injected = True

    def _victims(self, conn: Conn) -> list[dict[str, Any]]:
        found = []
        for entry in self.injections:
            attempt_id = uuid.UUID(entry["attempt_id"])
            state, reason, lease_end, finished, task_id, number, rows = _one(
                conn, _VICTIM_ATTEMPT, attempt_id
            )
            following = conn.execute(_NEXT_START, (task_id, number + 1)).fetchone()
            closure = conn.execute(_CLOSURE, (attempt_id,)).fetchone()
            unresolved = _one(conn, _UNRESOLVED, attempt_id)[0]
            found.append(
                {
                    **entry,
                    "state": state,
                    "revoked_reason": reason,
                    "lease_expires_at": lease_end,
                    "finished_at": finished,
                    "next_started_at": None if following is None else following[0],
                    "ledger_rows": rows,
                    "unresolved_calls": unresolved,
                    "closure": None if closure is None else tuple(closure),
                }
            )
        return found

    def checks(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> list[Check]:
        victims = self._victims(conn)
        captures = _captures(conn, job_id)
        revoked = [(v["state"], v["revoked_reason"]) for v in victims]
        closures = [v["closure"] for v in victims]
        read = [captures.get(uuid.UUID(v["attempt_id"])) for v in victims]
        wanted = [
            AttemptCapture.LOWER_BOUND if v["unresolved_calls"] else AttemptCapture.CLOSED
            for v in victims
        ]
        kept = [(v["ledger_rows_at_kill"], v["ledger_rows"]) for v in victims]
        return [
            *super().checks(conn, job_id, fleet),
            _check(
                "victims_revoked_lease_expired",
                bool(victims) and all(pair == ("revoked", "lease_expired") for pair in revoked),
                {"victims": revoked},
            ),
            _check(
                "victims_closed_by_recovery_t3",
                bool(victims) and all(c == ("recovery", "t3") for c in closures),
                {"closures": closures},
            ),
            _check(
                "victims_capture",
                read == wanted,
                {"read": [str(c) for c in read], "expected": [str(c) for c in wanted]},
            ),
            _check("victims_ledger_kept", all(a == b for a, b in kept), {"rows": kept}),
        ]

    def measured(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> dict[str, Any]:
        victims = self._victims(conn)
        revocation = [
            _seconds(v["finished_at"] - v["lease_expires_at"])
            for v in victims
            if v["state"] == "revoked"
        ]
        recovery = [
            _seconds(v["next_started_at"] - v["lease_expires_at"])
            for v in victims
            if v["next_started_at"] is not None
        ]
        return {
            "victims": [
                {
                    "attempt_id": v["attempt_id"],
                    "killed_at": v["killed_at"],
                    "lease_expires_at": _iso(v["lease_expires_at"]),
                    "revoked_at": _iso(v["finished_at"]),
                    "next_started_at": _iso(v["next_started_at"]),
                    "unresolved_calls": v["unresolved_calls"],
                }
                for v in victims
            ],
            "revocation_after_lease_s": metrics.percentiles(
                [s for s in revocation if s is not None]
            ),
            "recovery_delay_s": metrics.percentiles([s for s in recovery if s is not None]),
            "recovery_delay_formula": (
                "next attempt's started_at - revoked attempt's lease_expires_at"
            ),
        }


class FreezeDrill(Drill):
    """D2: SIGSTOP one busy worker for ``seconds``, then SIGCONT it."""

    name = "d2"

    def __init__(self, seconds: float) -> None:
        super().__init__()
        self.seconds = seconds
        self.victim: Victim | None = None
        self.frozen_at = 0.0
        self.thaw_at = 0.0
        self.thawed = False

    def step(self, poll: Poll) -> None:
        if self.victim is None:
            if poll.reached():
                victims = freeze_victims(poll, 1, set())
                self.victim = victims[0] if victims else None
                if self.victim is not None:
                    self.recorded.add(self.victim.pid)
                    self.frozen_at = time.monotonic()
                    self.thaw_at = self.frozen_at + self.seconds
                    self.injections.append(
                        {
                            "pid": self.victim.pid,
                            "attempt_id": str(self.victim.attempt_id),
                            "frozen_at": _iso(poll.harness.clock()),
                            "seconds": self.seconds,
                        }
                    )
        elif not self.thawed and time.monotonic() >= self.thaw_at:
            self._thaw("drill")
            self.injections[-1]["thawed_at"] = _iso(poll.harness.clock())
            self.injected = True  # frozen for the planned time, and thawed by the drill

    def pending(self) -> bool:
        return self.victim is not None and not self.thawed

    def release(self) -> None:
        """Continue a victim still frozen. A thaw here is cleanup: the drill is not injected."""
        if self.victim is not None and not self.thawed:
            self._thaw("cleanup")
            self.injections[-1]["thawed_at"] = None  # the database clock is not read here

    def _thaw(self, by: str) -> None:
        assert self.victim is not None
        os.kill(self.victim.pid, signal.SIGCONT)
        self.thawed = True
        self.injections[-1] |= {
            "frozen_s": time.monotonic() - self.frozen_at,
            "thawed_by": by,
        }

    def _attempt(self, conn: Conn) -> tuple[Any, ...] | None:
        if self.victim is None:
            return None
        return _one(conn, _VICTIM_ATTEMPT, self.victim.attempt_id)

    def checks(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> list[Check]:
        checks = super().checks(conn, job_id, fleet)
        attempt = self._attempt(conn)
        if attempt is None:
            return [*checks, _check("stale_completion_not_recorded", False, "no victim")]
        state, _, _, _, task_id, number, _ = attempt
        succeeded = conn.execute(
            "SELECT attempt_number FROM attempts WHERE task_id = %s AND state = 'succeeded'",
            (task_id,),
        ).fetchall()
        numbers = [row[0] for row in succeeded]
        passed = state == "succeeded" or (
            state == "revoked" and len(numbers) == 1 and numbers[0] > number
        )
        detail = {"state": state, "attempt_number": number, "succeeded_attempts": numbers}
        return [*checks, _check("stale_completion_not_recorded", passed, detail)]

    def measured(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> dict[str, Any]:
        attempt = self._attempt(conn)
        if attempt is None or self.victim is None:
            return {}
        state, reason, lease_end, finished, _, _, _ = attempt
        handle = fleet.handle(self.victim.pid)
        code = None if handle is None or handle.exited is None else handle.exited["exit_code"]
        known = {int(member) for member in ExitCode}
        return {
            "outcome": state,
            "revoked_reason": reason,
            "revocation_after_lease_s": (
                _seconds(finished - lease_end) if state == "revoked" else None
            ),
            "victim_exit_code": code,
            "victim_exit": ExitCode(code).name if code in known else None,
        }


class OutageDrill(Drill):
    """D3a: the workers' connections go through a proxy that refuses everything for a while."""

    name = "d3a"

    def __init__(self, url: str, seconds: float = OUTAGE_SECONDS) -> None:
        super().__init__()
        self.proxy = PgProxy(url)
        self.seconds = seconds
        self.until: float | None = None
        self.started_at: datetime | None = None
        self.ended_at: datetime | None = None

    def worker_url(self, url: str) -> str:
        return self.proxy.url(url)

    def step(self, poll: Poll) -> None:
        if self.until is None:
            if poll.reached():
                self.started_at = poll.harness.clock()
                self.proxy.refuse()
                self.until = time.monotonic() + self.seconds
        elif not self.injected and time.monotonic() >= self.until:
            self.proxy.accept()
            self.ended_at = poll.harness.clock()
            self.injected = True
            self.injections.append(
                {"started_at": _iso(self.started_at), "ended_at": _iso(self.ended_at)}
            )

    def release(self) -> None:
        self.proxy.accept()

    def close(self) -> None:
        self.proxy.close()

    def expected_exit(self, pid: int, code: int) -> int:
        return code  # recorded, not predicted

    def measured(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> dict[str, Any]:
        return {"next_completion_after_outage_s": _next_completion(conn, job_id, self.ended_at)}


def _next_completion(conn: Conn, job_id: uuid.UUID, after: datetime | None) -> float | None:
    if after is None:
        return None
    row = conn.execute(
        "SELECT min(a.finished_at) FROM attempts a JOIN tasks t ON t.id = a.task_id "
        "WHERE t.job_id = %s AND a.state = 'succeeded' AND a.finished_at >= %s",
        (job_id, after),
    ).fetchone()
    return None if row is None or row[0] is None else _seconds(row[0] - after)


class RestartRefusedError(RuntimeError):
    """D3b was not explicitly allowed on the dedicated project."""


def check_restart_allowed(allow: bool, environ: Mapping[str, str]) -> None:
    """Refuse D3b unless ``--allow-db-restart`` and ``COMPOSE_PROJECT_NAME=orqestra-bench``."""
    if not allow:
        msg = "d3b restarts the database server; it needs --allow-db-restart"
        raise RestartRefusedError(msg)
    if environ.get("COMPOSE_PROJECT_NAME") != BENCH_PROJECT:
        msg = (
            f"d3b runs only on the dedicated Compose project: COMPOSE_PROJECT_NAME={BENCH_PROJECT}"
        )
        raise RestartRefusedError(msg)


_IDENTITY_LINE = "Database system identifier:"


def check_restart_target(conn: Conn) -> None:
    """Refuse D3b unless the ``orqestra-bench`` container is the server ``conn`` reached.

    Both sides report the cluster's ``system_identifier``: the server through
    ``pg_control_system()``, the container through ``pg_controldata``. Called
    before any run schema is created, so nothing is restarted on a mismatch.
    """
    row = conn.execute("SELECT system_identifier FROM pg_control_system()").fetchone()
    reached = "" if row is None else str(row[0])
    try:
        shown = subprocess.run(  # a fixed command
            IDENTITY_COMMAND, cwd=ROOT, capture_output=True, text=True, check=True, timeout=60
        ).stdout
    except (OSError, subprocess.SubprocessError):
        msg = f"d3b could not read the identity of the {BENCH_PROJECT} container"
        raise RestartRefusedError(msg) from None
    container = next(
        (
            line.removeprefix(_IDENTITY_LINE).strip()
            for line in shown.splitlines()
            if line.startswith(_IDENTITY_LINE)
        ),
        "",
    )
    if not container or container != reached:
        msg = (
            f"d3b restarts only the {BENCH_PROJECT} container, which is not the server "
            f"{BENCH_DATABASE_URL_ENV} reaches"
        )
        raise RestartRefusedError(msg)


class RestartDrill(Drill):
    """D3b: restart the dedicated project's server once, if ``check_restart_allowed`` passed.

    The restart counts as injected only if the server's ``pg_postmaster_start_time()``
    moved forward across it; a command that restarted nothing, or restarted another
    server, leaves the drill uninjected, so its record is not complete.
    """

    name = "d3b"

    def __init__(self, command: Sequence[str] = RESTART_COMMAND) -> None:
        super().__init__()
        self.command = tuple(command)
        self.attempted = False
        self.ended_at: datetime | None = None

    def step(self, poll: Poll) -> None:
        if self.attempted or not poll.reached():
            return
        self.attempted = True
        started_at = poll.harness.clock()
        server_before = poll.harness.server_started()
        began = time.monotonic()
        subprocess.run(self.command, cwd=ROOT, check=True, timeout=300)  # fixed, or a test stub
        poll.harness.reconnect()
        downtime = time.monotonic() - began
        server_after = poll.harness.server_started()
        self.ended_at = poll.harness.clock()
        restarted = server_after > server_before
        self.injected = restarted
        self.injections.append(
            {
                "started_at": _iso(started_at),
                "ended_at": _iso(self.ended_at),
                "restart_to_reconnect_s": downtime,
                "server_started_before": _iso(server_before),
                "server_started_after": _iso(server_after),
                "restarted": restarted,
            }
        )

    def expected_exit(self, pid: int, code: int) -> int:
        return code  # recorded, not predicted

    def measured(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> dict[str, Any]:
        return {"next_completion_after_restart_s": _next_completion(conn, job_id, self.ended_at)}


_ATTEMPTS_OF = (
    "SELECT a.attempt_number, a.state, a.failure_kind, a.error_message, a.started_at, "
    "a.finished_at, a.id FROM attempts a JOIN tasks t ON t.id = a.task_id "
    "WHERE t.job_id = %s AND t.task_key = %s ORDER BY a.attempt_number"
)
_TASK_OF = "SELECT id, available_at FROM tasks WHERE job_id = %s AND task_key = %s"


def _attempts(conn: Conn, job_id: uuid.UUID, key: str) -> list[tuple[Any, ...]]:
    return [tuple(row) for row in conn.execute(_ATTEMPTS_OF, (job_id, key))]


class PayloadDrill(Drill):
    """D4, payload job: one extra task whose payload is not a batch."""

    name = "d4-payload"

    def __init__(self, expected: Expected) -> None:
        super().__init__()
        self.expected = expected
        self.injected = True  # the poison is in the submission itself

    def checks(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> list[Check]:
        attempts = _attempts(conn, job_id, f"{POISON_KEY_PREFIX}-0")
        seen = [(row[1], row[2], row[3]) for row in attempts]
        return [
            *super().checks(conn, job_id, fleet),
            _check(
                "poison_payload_fails_once",
                seen == [("failed", "permanent", BATCH_PAYLOAD_INVALID)],
                {"attempts": seen},
            ),
            _check("malformed_items_present", self.expected.malformed > 0, self.expected.malformed),
        ]


@dataclass(frozen=True, slots=True)
class Poison:
    always_key: str
    always_raw: str
    once_key: str
    once_raw: str
    once_calls: int  # the oracle's calls for the fail-once batch


class RetryDrill(Drill):
    """D4, retry job: an always-failing and a fail-once provider answer in two batches."""

    name = "d4-retry"

    def __init__(self, expected: Expected, poison: Poison, policy: RetryPolicy) -> None:
        super().__init__()
        self.expected = expected
        self.poison = poison
        self.policy = policy
        self.injected = True

    def _ordering(self, conn: Conn, job_id: uuid.UUID, key: str) -> dict[str, Any]:
        """Each retry's start against stored values: the earlier end plus its delay, and
        the task's stored ``available_at`` for the last attempt (a failure keeps it)."""
        task_id, available_at = _one(conn, _TASK_OF, job_id, key)
        attempts = _attempts(conn, job_id, key)
        violations = []
        for earlier, later in itertools.pairwise(attempts):
            floor = earlier[5] + retry_delay(self.policy, task_id, earlier[0])
            if later[4] < floor:
                violations.append({"attempt": later[0], "started_at": _iso(later[4])})
        last_ok = len(attempts) < 2 or attempts[-1][4] >= available_at
        return {
            "violations": violations,
            "last_after_available_at": last_ok,
            "starts_after_available_at_s": (
                _seconds(attempts[-1][4] - available_at) if len(attempts) >= 2 else None
            ),
        }

    def checks(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> list[Check]:
        poison = self.poison
        always = _attempts(conn, job_id, poison.always_key)
        once = _attempts(conn, job_id, poison.once_key)
        faults = conn.execute("SELECT raw FROM bench_provider_faults").fetchall()
        once_calls = (
            _one(conn, _LEDGER_ROWS, once[-1][6])[0]
            if once and once[-1][1] == "succeeded"
            else None
        )
        orders = {
            key: self._ordering(conn, job_id, key) for key in (poison.always_key, poison.once_key)
        }
        return [
            *super().checks(conn, job_id, fleet),
            _check(
                "always_fails_after_max_attempts",
                [(row[1], row[2]) for row in always] == [("failed", "transient")] * MAX_ATTEMPTS,
                {"attempts": [(row[1], row[2]) for row in always]},
            ),
            _check(
                "once_succeeds_on_second_attempt",
                [(row[1], row[2]) for row in once]
                == [("failed", "transient"), ("succeeded", None)],
                {"attempts": [(row[1], row[2]) for row in once]},
            ),
            _check(
                "once_faults_once_across_processes",
                [row[0] for row in faults] == [poison.once_raw],
                {"fault_rows": len(faults)},
            ),
            _check(
                "once_succeeded_attempt_calls",
                once_calls == poison.once_calls,
                {"calls": once_calls, "oracle": poison.once_calls},
            ),
            _check(
                "no_attempt_before_available_at",
                all(not o["violations"] and o["last_after_available_at"] for o in orders.values()),
                orders,
            ),
            _check("malformed_items_present", self.expected.malformed > 0, self.expected.malformed),
        ]

    def measured(self, conn: Conn, job_id: uuid.UUID, fleet: DrillFleet) -> dict[str, Any]:
        return {
            key: {
                "attempts": len(_attempts(conn, job_id, key)),
                **self._ordering(conn, job_id, key),
            }
            for key in (self.poison.always_key, self.poison.once_key)
        }


# --- workloads ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DrillWorkload:
    tasks: list[dict[str, JsonValue]]
    expected: Expected
    status: JobStatus = JobStatus.SUCCEEDED
    failed: tuple[str, ...] = ()


def standard_workload(invoices: Sequence[JsonValue], batch: int) -> DrillWorkload:
    return DrillWorkload(
        build_tasks(invoices, batch, MAX_ATTEMPTS), expected_outcome(invoices, batch, DEFAULT_CAP)
    )


def payload_workload(invoices: Sequence[JsonValue], batch: int) -> DrillWorkload:
    """The whole partition plus ``poison-0``, whose payload is not a batch."""
    tasks = build_tasks(invoices, batch, MAX_ATTEMPTS, invalid_payload_tasks=1)
    expected = expected_outcome(invoices, batch, DEFAULT_CAP, invalid_payload_tasks=1)
    return DrillWorkload(tasks, expected, JobStatus.FAILED, (f"{POISON_KEY_PREFIX}-0",))


def _first_ambiguous(batch_index: int, batch: int, count: int) -> int:
    for index in range(batch_index * batch, min((batch_index + 1) * batch, count)):
        if kind_of(index) is Kind.AMBIGUOUS:
            return index
    msg = f"batch {batch_index} has no ambiguous invoice; use a batch size of at least 20"
    raise ValueError(msg)


def _line_total(invoice: JsonValue) -> int:
    assert isinstance(invoice, dict)
    assert isinstance(invoice["lines"], list)
    total = 0
    for line in invoice["lines"]:
        assert isinstance(line, dict)
        total += cast(int, line["quantity"]) * cast(int, line["unit_amount"])
    return total


def retry_workload(invoices: Sequence[JsonValue], batch: int) -> tuple[DrillWorkload, Poison]:
    """Batch 1 holds an always-failing total and batch 2 a fail-once total (plan 2.9 b, c)."""
    if -(-len(invoices) // batch) < 3:
        msg = "the retry job needs at least three batches"
        raise ValueError(msg)
    poisoned = copy.deepcopy(list(invoices))
    raws = {}
    for batch_index, fault in ((1, Fault.ALWAYS), (2, Fault.ONCE)):
        index = _first_ambiguous(batch_index, batch, len(poisoned))
        invoice = poisoned[index]
        assert isinstance(invoice, dict)
        invoice["total"] = raws[fault] = poison_total(_line_total(invoice), fault)
    tasks = build_tasks(poisoned, batch, MAX_ATTEMPTS)
    expected = expected_outcome(poisoned, batch, DEFAULT_CAP, failed_batches=frozenset({1}))
    start = 2 * batch
    ambiguous = sum(
        kind_of(index) is Kind.AMBIGUOUS
        for index in range(start, min(start + batch, len(poisoned)))
    )
    poison = Poison(
        always_key=str(tasks[1]["task_key"]),
        always_raw=raws[Fault.ALWAYS],
        once_key=str(tasks[2]["task_key"]),
        once_raw=raws[Fault.ONCE],
        once_calls=min(ambiguous, DEFAULT_CAP),
    )
    workload = DrillWorkload(tasks, expected, JobStatus.FAILED, (poison.always_key,))
    return workload, poison


def retry_policy(worker_settings: Mapping[str, str]) -> RetryPolicy:
    return RetryPolicy(
        base_delay=parse_duration(worker_settings["ORQESTRA_WORKER_RETRY_BASE_DELAY"]),
        max_delay=parse_duration(worker_settings["ORQESTRA_WORKER_RETRY_MAX_DELAY"]),
    )


# --- one drill run -----------------------------------------------------------------------


def _started(handle: WorkerHandle) -> bool:
    for line in handle.log_path.read_bytes().splitlines():
        if line.startswith(b"{"):
            try:
                if json.loads(line).get("event") == "worker_started":
                    return True
            except ValueError:
                continue
    return False


def _await_started(fleet: DrillFleet, bound: float = STARTUP_TIMEOUT) -> None:
    """Wait, up to ``bound`` seconds, until every running worker has logged ``worker_started``.

    A worker stopped before it has installed its signal handlers dies by the
    signal's default action (exit -15), not by a controlled stop (exit 0); a
    replacement started near the end of a job would otherwise be stopped so.
    """
    deadline = time.monotonic() + bound
    while time.monotonic() < deadline:
        if all(_started(handle) for handle in fleet.workers if handle.running()):
            return
        time.sleep(0.05)


def _supervise(fleet: DrillFleet, seen: set[int], due: list[float], exits: list[Any]) -> None:
    """Record each worker that exited and replace it ``RESTART_DELAY`` seconds later."""
    for handle in list(fleet.workers):
        pid = handle.process.pid
        if pid not in seen and not handle.running():
            seen.add(pid)
            assert handle.exited is not None
            exits.append({"pid": pid, "exit_code": handle.exited["exit_code"]})
            due.append(time.monotonic() + RESTART_DELAY)
    current = time.monotonic()
    for when in [when for when in due if when <= current]:
        due.remove(when)
        fleet.start()


def wait_over(counts: Mapping[TaskState, int], drill: Drill) -> bool:
    """Whether the drill's wait ends: the job has finished and no injection is pending.

    A drill's own step must undo its injection (D2 thaws its victim), so a job that
    finishes first does not end the wait while the drill is still pending.
    """
    return counts[TaskState.QUEUED] + counts[TaskState.RUNNING] == 0 and not drill.pending()


def run_drill(
    url: str,
    cell: Cell,
    drill: Drill,
    workload: DrillWorkload,
    *,
    command: Sequence[str] | None = None,
    worker_settings: Mapping[str, str] = BENCH_WORKER_SETTINGS,
    raw_dir: Path,
    stop_after: float,
    run_id: str | None = None,
    note: str = "",
) -> dict[str, Any]:
    """Run ``drill`` against the migrated schema ``url``; its format 1 result record."""
    if cell.profile is not Profile.INVOICE:
        msg = "drills run the invoice profile"
        raise ValueError(msg)
    run_id = run_id or uuid.uuid4().hex[:12]
    raw_dir.mkdir(parents=True, exist_ok=True)
    env = process_environment(drill.worker_url(url), worker_settings)
    fleet = DrillFleet(
        cell.profile, env, raw_dir, command=list(command or worker_command(Profile.INVOICE))
    )
    harness = Harness(url)
    operation = parse_duration(worker_settings["ORQESTRA_WORKER_DB_OPERATION_TIMEOUT"])
    quiet = 2 * operation.total_seconds() + 1.0  # a statement outlives its bound by little
    seen: set[int] = set()
    due: list[float] = []
    exits: list[dict[str, Any]] = []
    finished = False
    harness_start = metrics.harness_usage()
    try:
        record_environment = environment(harness.conn, note=note)
        # Submitted before any worker starts, so polling begins before the job does.
        job_id, submission = submit(url, JOB_KIND, workload.tasks, raw_dir)
        for _ in range(cell.workers):
            fleet.start()
        began = time.monotonic()
        while time.monotonic() - began < stop_after:
            counts = harness.progress(job_id).task_counts
            if wait_over(counts, drill):
                finished = True
                break
            poll = Poll(
                harness, job_id, fleet, len(workload.tasks), counts[TaskState.SUCCEEDED], quiet
            )
            drill.step(poll)
            _supervise(fleet, seen, due, exits)
            time.sleep(POLL if drill.injected else INJECTION_POLL)
    except BaseException:
        harness.close()
        raise
    finally:
        # Each step runs even if an earlier one raised: a frozen worker is continued,
        # every worker is stopped and reaped, and the proxy is closed.
        try:
            drill.release()
        finally:
            try:
                _await_started(fleet)
                shutdown = parse_duration(worker_settings["ORQESTRA_WORKER_SHUTDOWN_TIMEOUT"])
                reaped = fleet.stop(timeout=shutdown.total_seconds() + 10.0)
            finally:
                drill.close()
    try:
        conn = harness.conn
        codes = [worker["exit_code"] for worker in reaped]
        wanted = [drill.expected_exit(w["pid"], w["exit_code"]) for w in reaped]
        gate = run_gate(
            conn,
            job_id,
            Profile.INVOICE,
            workload.expected,
            codes,
            expected_status=workload.status,
            expected_failed=workload.failed,
            expected_exit_codes=wanted,
        )
        gate += drill.checks(conn, job_id, fleet)
        figures = metrics.timing_figures(metrics.read_stored_times(conn, job_id))
        failures = failure_figures(conn, job_id, codes)
        usage = usage_figures(conn, job_id, DEFAULT_CAP)
        measured = drill.measured(conn, job_id, fleet)
    finally:
        harness.close()
    status = Status.INCOMPLETE
    if finished:
        passed = all(check["passed"] for check in gate)
        status = Status.COMPLETE if passed else Status.FAILED_CORRECTNESS
    record: dict[str, Any] = {
        "format": 1,
        "run_id": run_id,
        "ladder": "drill",
        "status": status.value,
        "recorded_at": now(),
        "cell": {**asdict(cell), "profile": cell.profile.value},
        "environment": record_environment,
        "worker_settings": dict(worker_settings),
        "provider": {"call_timeout": DEFAULT_CALL_TIMEOUT, "cap": DEFAULT_CAP, "route": "route-a"},
        "timing": {
            **figures["timing"],
            "submission": submission,
            "tasks": len(workload.tasks),
            "items": workload.expected.invoices,
            "stop_after_s": stop_after,
        },
        "throughput": None,  # a drill is not a performance cell
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
            "name": drill.name,
            "injected": drill.injected,
            "injections": drill.injections,
            "exits_while_running": exits,
            "restarts": len(fleet.workers) - cell.workers,
            "measured": measured,
        },
    }
    validate_result({**record, "environment": {**record_environment, "dirty_allowed": True}})
    return record


# --- the command -------------------------------------------------------------------------

DRILLS = ("d1", "d2", "d3a", "d3b", "d4")


def _jobs(
    name: str, args: argparse.Namespace, url_for_proxy: str
) -> list[tuple[Drill, DrillWorkload, Sequence[str] | None]]:
    invoices = generate_invoices(args.invoices)
    if name == "d4":
        retry, poison = retry_workload(invoices, args.batch)
        payload = payload_workload(invoices, args.batch)
        policy = retry_policy(BENCH_WORKER_SETTINGS)
        return [
            (PayloadDrill(payload.expected), payload, FAULTS_COMMAND),
            (RetryDrill(retry.expected, poison, policy), retry, FAULTS_COMMAND),
        ]
    workload = standard_workload(invoices, args.batch)
    drill: Drill
    if name == "d1":
        drill = KillDrill(args.victims)
    elif name == "d2":
        lease = parse_duration(BENCH_WORKER_SETTINGS["ORQESTRA_WORKER_LEASE"])
        drill = FreezeDrill(lease.total_seconds() + FREEZE_EXTRA)
    elif name == "d3a":
        drill = OutageDrill(url_for_proxy)
    else:
        drill = RestartDrill()
    return [(drill, workload, None)]


def run_named(name: str, args: argparse.Namespace, url: str) -> list[tuple[dict[str, Any], Path]]:
    """Run drill ``name`` in fresh run schemas of the benchmark database; write each result."""
    run_id = uuid.uuid4().hex[:12]
    cell = Cell(Profile.INVOICE, args.invoices, args.batch, args.workers)
    written = []
    if name == "d3b":
        with connect_verified(url) as admin:
            check_restart_target(admin)
    for number in range(2 if name == "d4" else 1):
        schema = run_schema_name(run_id, f"{name.replace('-', '')}j{number}")
        with connect_verified(url) as admin:
            create_run_schema(admin, schema)
            target = schema_url(admin, url, schema)
        try:
            migrate(target)
            drill, workload, command = _jobs(name, args, target)[number]
            if name == "d4":
                with psycopg.connect(target, autocommit=True) as conn:
                    create_fault_table(conn)
            record = run_drill(
                target,
                cell,
                drill,
                workload,
                command=command,
                raw_dir=RAW_DIR / run_id / drill.name,
                stop_after=args.stop_after,
                run_id=run_id,
                note=args.note,
            )
            written.append(
                (record, write_result(record, args.out_dir, allow_dirty=args.allow_dirty))
            )
        finally:
            if not args.keep_schema:
                with connect_verified(url) as admin:
                    drop_run_schema(admin, schema)
    return written


def _first_line(exc: BaseException) -> str:
    """The first non-empty line of ``exc``: a libpq error can span several lines."""
    return next(
        (line.strip() for line in str(exc).splitlines() if line.strip()), type(exc).__name__
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.drills", description=__doc__)
    parser.add_argument("drill", choices=DRILLS)
    parser.add_argument("--invoices", type=int, default=2_000)
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--victims", type=int, default=2, help="d1: workers killed")
    parser.add_argument("--stop-after", type=float, default=1800.0, help="a bound, in seconds")
    parser.add_argument(
        "--allow-db-restart",
        action="store_true",
        help="d3b only: allow restarting the orqestra-bench project's server",
    )
    add_common_arguments(parser)
    parser.set_defaults(out_dir=DRILLS_DIR)
    args = parser.parse_args(argv)
    try:
        if args.drill == "d3b":
            check_restart_allowed(args.allow_db_restart, os.environ)
        if args.drill == "d1" and args.victims > args.workers:
            msg = "--victims cannot exceed --workers"
            raise ValueError(msg)
        refuse_dirty(args.allow_dirty)
        url = require_bench_database_url(os.environ)
        written = run_named(args.drill, args, url)
    except (RestartRefusedError, ValueError, DirtyTreeError, UnsafeBenchDatabaseError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    except psycopg.OperationalError as exc:
        print(
            f"refused: could not reach the benchmark database: {_first_line(exc)}", file=sys.stderr
        )
        return 1
    for record, path in written:
        print(f"{record['status']} {record['drill']['name']} {path}")
    return 0 if all(record["status"] == Status.COMPLETE for record, _ in written) else 1


if __name__ == "__main__":
    sys.exit(main())
