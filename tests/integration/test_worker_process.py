"""The worker's process-level exits, in a real subprocess (DEC-045).

Each test starts ``python -m tests.integration.worker_process``, whose default
``terminate`` and watchdog end the process with ``os._exit``, and checks the
exit code, the event it logged last, and what PostgreSQL recorded. A handler
waits on an advisory lock the test holds; the process may reach the database
through a ``PgProxy`` the test can stall or cut. "Before its lease ended" is
checked against PostgreSQL's own clock and the stored ``lease_expires_at``.
Graceful shutdown (DEC-049) is driven with real SIGTERM and SIGINT, sent only
once the worker has logged that its handlers are installed, and timed from
the worker's own log timestamps. The worker writes the provider usage of its
``metered`` kind through a ``UsageSink`` (DEC-059); its provider double records
whether each call's intent row had committed before the call.
"""

import itertools
import json
import os
import secrets
import select
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.domain.jobs import JsonValue, Task
from orqestra.domain.leases import LeasePolicy
from orqestra.domain.retry import RetryPolicy, retry_delay
from orqestra.persistence.lifecycle import (
    request_cancellation,
    revoke_next_expired,
    start_attempt,
)
from orqestra.persistence.provider_usage import (
    AttemptCapture,
    AttemptUsage,
    CaptureState,
    CaptureTier,
    ClosedBy,
    capture_of,
    read_attempt_usage,
)
from orqestra.worker.fence import ExitCode
from tests.integration.pg_proxy import PgProxy
from tests.integration.test_worker_runtime import (
    WAIT,
    backend_alive,
    eventually,
    row_locked,
    rows,
    submit,
    task_states,
)
from tests.integration.worker_process import HANDLER_RUNS, PROVIDER_INVOCATIONS

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]
ROOT = Path(__file__).resolve().parents[2]

# Test values only (DEC-034): heartbeat 1 s + database bound 0.8 s + margin 1 s < lease 4 s.
LEASE = timedelta(seconds=4)
RETRY = RetryPolicy(timedelta(milliseconds=100), timedelta(milliseconds=500))  # as ENVIRONMENT
ENVIRONMENT = {
    "ORQESTRA_WORKER_LEASE": "PT4S",
    "ORQESTRA_WORKER_EXECUTION_TIMEOUT": "PT5M",
    "ORQESTRA_WORKER_HEARTBEAT_INTERVAL": "PT1S",
    "ORQESTRA_WORKER_DB_OPERATION_TIMEOUT": "PT0.8S",
    "ORQESTRA_WORKER_LOCK_TIMEOUT": "PT0.3S",
    "ORQESTRA_WORKER_FENCE_MARGIN": "PT1S",
    "ORQESTRA_WORKER_POLL_INTERVAL": "PT0.1S",
    "ORQESTRA_WORKER_RECOVERY_INTERVAL": "PT1S",
    "ORQESTRA_WORKER_RECOVERY_BATCH_LIMIT": "5",
    "ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": "PT1S",
    "ORQESTRA_WORKER_RETRY_BASE_DELAY": "PT0.1S",
    "ORQESTRA_WORKER_RETRY_MAX_DELAY": "PT0.5S",
}


class WorkerProcess:
    def __init__(
        self, worker_url: str, handler_url: str, overrides: dict[str, str] | None = None
    ) -> None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("ORQESTRA_WORKER_")}
        env |= (
            ENVIRONMENT
            | (overrides or {})
            | {
                "ORQESTRA_TEST_WORKER_URL": worker_url,
                "ORQESTRA_TEST_HANDLER_URL": handler_url,
            }
        )
        self.process = subprocess.Popen(
            [sys.executable, "-m", "tests.integration.worker_process"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._read = ""  # log text read before the process ended

    def wait_for(self, *events: str) -> dict[str, Any]:
        """Read the log (bounded) until a line with one of ``events`` appears; return it."""
        assert self.process.stderr is not None
        fd = self.process.stderr.fileno()  # read raw, so no text buffer hides a line
        deadline = time.monotonic() + WAIT
        while (left := deadline - time.monotonic()) > 0:
            *complete, _ = self._read.split("\n")
            for line in complete:
                if line.startswith("{") and json.loads(line).get("event") in events:
                    return dict(json.loads(line))
            if select.select([fd], [], [], left)[0]:
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                self._read += chunk.decode()
        pytest.fail(f"the worker logged none of {events}")

    def pump(self) -> None:
        """Read what the worker has logged so far without waiting, so its stderr pipe never fills.

        A worker making many calls logs every usage record; unread, the pipe fills and
        the worker blocks in a log write. The text is kept for ``exit``.
        """
        assert self.process.stderr is not None
        fd = self.process.stderr.fileno()
        for _ in range(64):  # bounded: a pipe holds far less than this many chunks
            if not select.select([fd], [], [], 0)[0]:
                return
            chunk = os.read(fd, 65536)
            if not chunk:
                return
            self._read += chunk.decode()

    def exit(self) -> tuple[int, list[dict[str, Any]]]:
        """Wait (bounded) for the process to end; its exit code and its JSON log lines."""
        _, stderr = self.process.communicate(timeout=WAIT)
        text = self._read + stderr
        lines = [json.loads(line) for line in text.splitlines() if line.startswith("{")]
        return self.process.returncode, lines

    def kill(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
            self.process.communicate(timeout=WAIT)


@pytest.fixture
def handler_db(db: Conn) -> Conn:
    db.execute(HANDLER_RUNS)
    return db


@pytest.fixture
def start(schema_url: str) -> Iterator[Any]:
    started: list[WorkerProcess] = []

    def launch(
        worker_url: str | None = None, overrides: dict[str, str] | None = None
    ) -> WorkerProcess:
        process = WorkerProcess(worker_url or schema_url, schema_url, overrides)
        started.append(process)
        return process

    yield launch
    for process in started:
        process.kill()


@dataclass
class Gate:
    """An advisory lock the test holds; ``gated`` handlers wait until it is released."""

    key: int
    holder: Conn

    def release(self) -> None:
        self.holder.execute("SELECT pg_advisory_unlock(%s)", (self.key,))


@pytest.fixture
def gate(schema_url: str) -> Iterator[Gate]:
    with psycopg.connect(schema_url, autocommit=True) as holder:
        key = secrets.randbits(62)
        holder.execute("SELECT pg_advisory_lock(%s)", (key,))
        yield Gate(key, holder)


def runs(db: Conn) -> int:
    return int(rows(db, "SELECT count(*) FROM handler_runs")[0][0])


def renewed(db: Conn, task: Task) -> Any:
    """The running attempt once its lease was renewed at least once, else None."""
    found = rows(
        db,
        "SELECT id, lease_expires_at FROM attempts WHERE task_id = %s AND state = 'running' "
        "AND lease_expires_at > started_at + %s",
        task.id,
        LEASE,
    )
    return found[0] if found else None


def before_lease_end(db: Conn, task: Task) -> bool:
    ((ended_in_time,),) = rows(
        db, "SELECT clock_timestamp() < lease_expires_at FROM attempts WHERE task_id = %s", task.id
    )
    return bool(ended_in_time)


def past(db: Conn, task: Task, delta: timedelta) -> bool:
    """Whether the database clock is more than ``delta`` past the task's cancellation request."""
    ((passed,),) = rows(
        db,
        "SELECT clock_timestamp() > cancel_requested_at + %s FROM tasks WHERE id = %s",
        delta,
        task.id,
    )
    return bool(passed)


def no_failure_recorded(db: Conn, task: Task) -> bool:
    return rows(
        db, "SELECT count(*) FROM attempts WHERE task_id = %s AND failure_kind IS NOT NULL", task.id
    ) == [(0,)]


def test_a_stalled_supervisor_call_ends_the_process_before_its_lease_ends(
    schema_url: str, handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key})
    with PgProxy(schema_url) as proxy:
        worker = start(proxy.url(schema_url))
        eventually(lambda: renewed(handler_db, task), "a renewal")
        proxy.stall()  # the next supervisor call never gets an answer
        code, lines = worker.exit()
    assert before_lease_end(handler_db, task)
    assert code == ExitCode.STALLED
    assert lines[-1]["event"] == "operation_deadline_passed"
    assert lines[-1]["operation"] in {"renew_lease", "revoke_next_expired"}  # schedule-dependent
    assert task_states(handler_db, [task]) == [("running", 1)]
    assert no_failure_recorded(handler_db, task)
    assert runs(handler_db) == 1


def test_refused_connections_fence_the_process_before_its_lease_ends(
    schema_url: str, handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key})
    with PgProxy(schema_url) as proxy:
        worker = start(proxy.url(schema_url))
        eventually(lambda: renewed(handler_db, task), "a renewal")
        proxy.refuse()  # every heartbeat now fails at once: none is confirmed
        code, lines = worker.exit()
    assert before_lease_end(handler_db, task)
    assert code == ExitCode.FENCED
    assert lines[-1]["event"] == "ownership_deadline_passed"
    assert any(line.get("event") == "heartbeat_unconfirmed" for line in lines)
    assert no_failure_recorded(handler_db, task)


def test_lost_ownership_ends_the_process_at_the_next_heartbeat(
    handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key})
    worker = start()
    eventually(lambda: runs(handler_db) == 1, "the handler")
    # A request with a 1 µs grace ends the lease at once: the next heartbeat is refused.
    request_cancellation(handler_db, task.id, grace=timedelta(microseconds=1))
    code, lines = worker.exit()
    assert code == ExitCode.FENCED
    assert lines[-1]["event"] == "ownership_lost"
    assert "LeaseExpiredError" in lines[-1]["message"]
    assert no_failure_recorded(handler_db, task)


def test_a_cancellation_request_fences_the_process_at_the_end_of_its_grace(
    handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key})
    worker = start()
    eventually(lambda: renewed(handler_db, task), "a renewal")
    request_cancellation(handler_db, task.id, grace=timedelta(seconds=3))
    code, lines = worker.exit()
    assert before_lease_end(handler_db, task)  # the lease now ends with the grace period
    assert code == ExitCode.FENCED
    assert lines[-1]["event"] == "ownership_deadline_passed"
    assert any(line.get("event") == "cancellation_requested" for line in lines)
    # Recovery then cancels the task (DEC-040).
    revocation = eventually(
        lambda: revoke_next_expired(handler_db, retry_policy=RETRY), "the grace to run out"
    )
    assert revocation.attempt.revoked_reason is not None
    assert revocation.attempt.revoked_reason.value == "cancellation_grace_expired"
    assert task_states(handler_db, [task]) == [("cancelled", 1)]


def test_a_grace_longer_than_the_lease_fences_the_process_at_the_grace_end_not_the_lease(
    handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key})  # ignores the signal
    worker = start()
    eventually(lambda: renewed(handler_db, task), "a renewal")
    request_cancellation(handler_db, task.id, grace=timedelta(seconds=12))
    # Any lease held at the request has ended by now; a fence restricted to it would
    # have ended the process (DEC-068).
    eventually(lambda: past(handler_db, task, LEASE + timedelta(milliseconds=500)), "the old lease")
    assert worker.process.poll() is None
    code, lines = worker.exit()
    assert code == ExitCode.FENCED
    assert lines[-1]["event"] == "ownership_deadline_passed"
    assert any(line.get("event") == "cancellation_requested" for line in lines)
    assert before_lease_end(handler_db, task)  # still before the grace's lease end
    revocation = eventually(
        lambda: revoke_next_expired(handler_db, retry_policy=RETRY), "the grace to run out"
    )
    assert revocation.attempt.revoked_reason is not None
    assert revocation.attempt.revoked_reason.value == "cancellation_grace_expired"
    assert task_states(handler_db, [task]) == [("cancelled", 1)]


def test_a_control_plane_signal_ends_the_process_without_recording_anything(
    handler_db: Conn, start: Any
) -> None:
    (task,) = submit(handler_db, "abort", {})
    worker = start()
    code, lines = worker.exit()
    assert code == ExitCode.HANDLER_ABORTED
    assert lines[-1]["event"] == "handler_aborted"
    assert "builtins.SystemExit" in lines[-1]["message"]
    assert task_states(handler_db, [task]) == [("running", 1)]  # left to recovery
    assert no_failure_recorded(handler_db, task)
    assert runs(handler_db) == 1


def test_a_lost_completion_answer_is_reconciled_and_the_process_carries_on(
    schema_url: str, handler_db: Conn, start: Any, gate: Gate
) -> None:
    first, second = submit(handler_db, "gated", {"gate": gate.key}, {"gate": gate.key})
    with PgProxy(schema_url) as proxy:
        worker = start(proxy.url(schema_url))
        eventually(lambda: runs(handler_db) == 1, "the first handler")
        proxy.drop_completion(committed=True)
        gate.release()  # the completion commits; its answer is lost
        eventually(
            lambda: task_states(handler_db, [first, second]) == [("succeeded", 1)] * 2,
            "both tasks",
        )
        assert worker.process.poll() is None  # reconciled: the process did not exit
    assert proxy.drop_count == 1
    assert runs(handler_db) == 2  # once per task, never again for the first


def test_an_unresolved_reconciliation_ends_the_process_at_the_deadline(
    schema_url: str, handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key})
    with PgProxy(schema_url) as proxy:
        # The execution deadline, which bounds reconciliation, equals the 4 s lease.
        worker = start(proxy.url(schema_url), {"ORQESTRA_WORKER_EXECUTION_TIMEOUT": "PT4S"})
        eventually(lambda: runs(handler_db) == 1, "the handler")
        proxy.drop_completion(committed=False, then_refuse=True)  # never reachable again
        gate.release()
        code, lines = worker.exit()
    assert code == ExitCode.AMBIGUOUS
    assert lines[-1]["event"] == "reconciliation_unresolved"
    assert any(line.get("event") == "completion_ambiguous" for line in lines)
    assert task_states(handler_db, [task]) == [
        ("running", 1)
    ]  # nothing committed; recovery decides
    assert no_failure_recorded(handler_db, task)
    assert runs(handler_db) == 1


def test_a_cooperative_handler_is_cancelled_and_the_process_carries_on(
    handler_db: Conn, start: Any
) -> None:
    (task,) = submit(handler_db, "cooperative", {})
    worker = start()
    eventually(lambda: runs(handler_db) == 1, "the handler")
    request_cancellation(handler_db, task.id, grace=timedelta(seconds=30))
    eventually(lambda: task_states(handler_db, [task]) == [("cancelled", 1)], "the cancellation")
    assert rows(
        handler_db, "SELECT state, revoked_reason FROM attempts WHERE task_id = %s", task.id
    ) == [("cancelled", None)]  # recorded by the worker, not revoked by recovery
    assert worker.process.poll() is None  # no exit: the worker carries on
    assert no_failure_recorded(handler_db, task)
    assert runs(handler_db) == 1


def test_a_cooperative_handler_winding_down_past_its_lease_is_not_fenced(
    handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "cooperative", {"gate": gate.key})  # winds down until released
    worker = start()
    eventually(lambda: runs(handler_db) == 1, "the handler")
    request_cancellation(handler_db, task.id, grace=timedelta(seconds=30))
    eventually(lambda: past(handler_db, task, LEASE + timedelta(milliseconds=500)), "the old lease")
    assert worker.process.poll() is None  # still winding down within its grace (DEC-068)
    gate.release()
    eventually(lambda: task_states(handler_db, [task]) == [("cancelled", 1)], "the cancellation")
    assert rows(
        handler_db, "SELECT state, revoked_reason FROM attempts WHERE task_id = %s", task.id
    ) == [("cancelled", None)]  # recorded by the worker, not revoked by recovery
    assert no_failure_recorded(handler_db, task)
    assert worker.process.poll() is None  # no exit: the worker carries on
    worker.pump()
    assert '"ownership_deadline_passed"' not in worker._read


def test_a_lost_connection_is_never_mistaken_for_a_cancellation(
    schema_url: str, handler_db: Conn, start: Any
) -> None:
    (task,) = submit(handler_db, "cooperative", {})
    with PgProxy(schema_url) as proxy:
        worker = start(proxy.url(schema_url))
        eventually(lambda: renewed(handler_db, task), "a renewal")
        proxy.refuse()  # heartbeats now fail unconfirmed; nothing was requested
        code, lines = worker.exit()
    assert code == ExitCode.FENCED
    assert lines[-1]["event"] == "ownership_deadline_passed"
    assert not any(line.get("event") == "cancellation_requested" for line in lines)
    assert rows(handler_db, "SELECT cancel_requested_at FROM tasks WHERE id = %s", task.id) == [
        (None,)
    ]
    assert task_states(handler_db, [task]) == [("running", 1)]  # the handler never ended it
    assert no_failure_recorded(handler_db, task)


# --- a handler that outlives its execution timeout ----------------------------------------

# Test values only (DEC-034): lease 4 s <= execution timeout 6 s, and the lease is capped at
# the deadline by the heartbeats at about 1 s and 2 s, so the ownership watchdog sits at
# about deadline minus the 1 s fence margin.
DEADLINE = timedelta(seconds=6)
MARGIN = timedelta(seconds=1)


def test_a_handler_outliving_the_execution_timeout_is_fenced_and_revoked_as_deadline_exceeded(
    handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key}, max_attempts=3)
    worker = start(overrides={"ORQESTRA_WORKER_EXECUTION_TIMEOUT": "PT6S"})
    eventually(lambda: runs(handler_db) == 1, "the handler")
    code, lines = worker.exit()
    events = [line.get("event") for line in lines]
    # Fenced by the deadline, not by a lost connection, a lost lease or a cancellation.
    assert code == ExitCode.FENCED
    assert events[-1] == "ownership_deadline_passed"
    for other in ("ownership_lost", "heartbeat_unconfirmed", "cancellation_requested"):
        assert other not in events
    started = next(line for line in lines if line.get("event") == "handler_started")
    fenced_after = (logged_at(lines[-1]) - logged_at(started)).total_seconds()
    assert fenced_after >= (DEADLINE - MARGIN).total_seconds() - 0.5
    # The handler outlived the deadline (the test still holds its gate) and never ended.
    assert before_lease_end(handler_db, task)
    assert task_states(handler_db, [task]) == [("running", 1)]
    assert no_failure_recorded(handler_db, task)
    # The lease reached its cap, which is what makes recovery's reason the deadline.
    ((lease_end, deadline_at, claimed_at),) = rows(
        handler_db,
        "SELECT lease_expires_at, deadline_at, started_at FROM attempts WHERE task_id = %s",
        task.id,
    )
    assert lease_end == deadline_at
    assert deadline_at - claimed_at == DEADLINE
    # Recovery then revokes the attempt for the deadline, and the task is retried (DEC-053).
    revocation = eventually(
        lambda: revoke_next_expired(handler_db, retry_policy=RETRY), "the deadline to pass"
    )
    assert revocation.attempt.revoked_reason is not None
    assert revocation.attempt.revoked_reason.value == "deadline_exceeded"
    assert task_states(handler_db, [task]) == [("queued", 1)]
    assert rows(
        handler_db, "SELECT state, revoked_reason FROM attempts WHERE task_id = %s", task.id
    ) == [("revoked", "deadline_exceeded")]
    assert runs(handler_db) == 1  # no worker is left to run it again


# --- graceful shutdown (DEC-049) ---------------------------------------------------------


def logged_at(line: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(line["ts"])


@pytest.mark.parametrize("timeout", ["PT1S", "PT0S"])
@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT], ids=["sigterm", "sigint"])
def test_a_signal_while_idle_stops_the_process_cleanly(
    handler_db: Conn, start: Any, sig: signal.Signals, timeout: str
) -> None:
    worker = start(overrides={"ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": timeout})
    worker.wait_for("worker_started")  # the handlers are installed before this
    worker.process.send_signal(sig)
    code, lines = worker.exit()
    assert code == 0
    assert [line["event"] for line in lines][-2:] == ["shutdown_requested", "worker_stopped"]


def test_a_task_settled_within_the_drain_ends_the_process_cleanly(
    handler_db: Conn, start: Any, gate: Gate
) -> None:
    first, second = submit(handler_db, "gated", {"gate": gate.key}, {"gate": gate.key})
    worker = start(overrides={"ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": "PT30S"})
    eventually(lambda: runs(handler_db) == 1, "the first handler")
    worker.process.send_signal(signal.SIGTERM)
    worker.wait_for("shutdown_requested")
    gate.release()
    code, lines = worker.exit()
    assert code == 0
    assert lines[-1]["event"] == "worker_stopped"
    # Which of the two is claimed first depends on its random id; the other is untouched.
    assert sorted(task_states(handler_db, [first, second])) == [("queued", 0), ("succeeded", 1)]
    assert runs(handler_db) == 1


@pytest.mark.parametrize(("max_attempts", "after"), [(3, ("queued", 1)), (1, ("failed", 1))])
def test_a_task_not_settled_in_time_ends_the_process_with_exit_9(
    handler_db: Conn, start: Any, gate: Gate, max_attempts: int, after: tuple[str, int]
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key}, max_attempts=max_attempts)
    worker = start(overrides={"ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": "PT2S"})
    eventually(lambda: runs(handler_db) == 1, "the handler")
    worker.process.send_signal(signal.SIGTERM)
    requested = worker.wait_for("shutdown_requested")
    code, lines = worker.exit()
    assert code == ExitCode.SHUTDOWN
    assert lines[-1]["event"] == "drain_deadline_passed"
    # Timed by the worker's own clock: the drain deadline, plus scheduling.
    drained = (logged_at(lines[-1]) - logged_at(requested)).total_seconds()
    assert 1.9 <= drained < 2.0 + 0.8
    assert task_states(handler_db, [task]) == [("running", 1)]
    assert no_failure_recorded(handler_db, task)
    assert rows(handler_db, "SELECT cancel_requested_at FROM tasks WHERE id = %s", task.id) == [
        (None,)
    ]
    # Recovery revokes it once its lease ends, using one attempt of its budget.
    revocation = eventually(
        lambda: revoke_next_expired(handler_db, retry_policy=RETRY), "the lease to end"
    )
    assert revocation.attempt.revoked_reason is not None
    assert revocation.attempt.revoked_reason.value == "lease_expired"
    assert task_states(handler_db, [task]) == [after]
    assert runs(handler_db) == 1


def test_a_second_signal_ends_a_draining_process_at_once(
    handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key})
    worker = start(overrides={"ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": "PT30S"})
    eventually(lambda: runs(handler_db) == 1, "the handler")
    worker.process.send_signal(signal.SIGTERM)
    requested = worker.wait_for("shutdown_requested")
    worker.process.send_signal(signal.SIGINT)
    code, lines = worker.exit()
    assert code == ExitCode.SHUTDOWN
    assert lines[-1]["event"] == "shutdown_forced"
    assert (logged_at(lines[-1]) - logged_at(requested)).total_seconds() < 5.0  # not 30 s
    assert task_states(handler_db, [task]) == [("running", 1)]
    assert no_failure_recorded(handler_db, task)


def test_a_zero_shutdown_timeout_ends_a_busy_process_at_once(
    handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key})
    worker = start(overrides={"ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": "PT0S"})
    eventually(lambda: runs(handler_db) == 1, "the handler")
    worker.process.send_signal(signal.SIGTERM)
    code, lines = worker.exit()
    assert code == ExitCode.SHUTDOWN
    assert lines[-1]["event"] == "drain_deadline_passed"
    assert task_states(handler_db, [task]) == [("running", 1)]
    assert no_failure_recorded(handler_db, task)


# --- a worker frozen inside a transaction (DEC-050) ---------------------------------------

# Test values only (DEC-034): a 2 s database bound leaves time to prove the frozen
# transaction holds its lock before PostgreSQL ends it; 3 + 2 + 1 < 8 and 0.5 < 2.
FROZEN = {
    "ORQESTRA_WORKER_LEASE": "PT8S",
    "ORQESTRA_WORKER_HEARTBEAT_INTERVAL": "PT3S",
    "ORQESTRA_WORKER_DB_OPERATION_TIMEOUT": "PT2S",
    "ORQESTRA_WORKER_LOCK_TIMEOUT": "PT0.5S",
    "ORQESTRA_WORKER_FENCE_MARGIN": "PT1S",
}
RENEWAL = b"UPDATE attempts SET lease_expires_at"  # the heartbeat's write, after its row lock


def test_a_worker_frozen_mid_heartbeat_is_recovered_and_cannot_overwrite(
    schema_url: str, handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key}, max_attempts=3)
    with PgProxy(schema_url) as proxy:
        frozen = start(proxy.url(schema_url), FROZEN)
        eventually(lambda: runs(handler_db) == 1, "the handler")
        ((attempt_id, lease_before),) = rows(
            handler_db, "SELECT id, lease_expires_at FROM attempts WHERE task_id = %s", task.id
        )
        proxy.hold_on(RENEWAL)
        assert proxy.held.wait(WAIT)  # the heartbeat holds the row lock, its write is in hand
        os.kill(frozen.process.pid, signal.SIGSTOP)  # frozen: no watchdog, no COMMIT
        proxy.resume()  # the write reaches PostgreSQL; nothing after it ever will
        # The vulnerable window was hit: an idle transaction holds the attempt's row.
        ((pid,),) = eventually(
            lambda: rows(
                handler_db,
                "SELECT pid FROM pg_stat_activity WHERE application_name = 'orqestra-worker' "
                "AND state = 'idle in transaction' AND query LIKE %s",
                RENEWAL.decode() + "%",
            ),
            "the frozen transaction",
        )
        assert row_locked(handler_db, "attempts", attempt_id)
        healthy = start(schema_url, FROZEN)  # straight to the database
        # PostgreSQL ends the abandoned session; its uncommitted renewal rolls back.
        eventually(lambda: not backend_alive(handler_db, pid), "the frozen session to end")
        assert not row_locked(handler_db, "attempts", attempt_id)
        assert rows(
            handler_db, "SELECT lease_expires_at FROM attempts WHERE id = %s", attempt_id
        ) == [(lease_before,)]
        # The healthy worker recovers it once the lease ends, and runs the next generation.
        eventually(lambda: runs(handler_db) == 2, "recovery and a rerun")
        gate.release()
        eventually(lambda: task_states(handler_db, [task]) == [("succeeded", 2)], "the task")
        assert healthy.process.poll() is None  # it never died
        settled = rows(
            handler_db,
            "SELECT attempt_number, state, revoked_reason, finished_at FROM attempts "
            "WHERE task_id = %s ORDER BY attempt_number",
            task.id,
        )
        result = rows(handler_db, "SELECT result FROM tasks WHERE id = %s", task.id)
        assert [row[:3] for row in settled] == [
            (1, "revoked", "lease_expired"),
            (2, "succeeded", None),
        ]
        # Resumed, it never counts. Its watchdog ends it at once (its frozen call's operation
        # deadline, exit 4, or, if the supervisor first meets the closed session, its
        # ownership deadline, exit 3); or, if its handler ends first, its write is refused.
        os.kill(frozen.process.pid, signal.SIGCONT)
        ended = {"operation_deadline_passed": ExitCode.STALLED}
        ended["ownership_deadline_passed"] = ExitCode.FENCED
        line = frozen.wait_for(*ended, "outcome_discarded")
        if line["event"] in ended:
            assert frozen.exit()[0] == ended[line["event"]]
    assert (
        rows(
            handler_db,
            "SELECT attempt_number, state, revoked_reason, finished_at FROM attempts "
            "WHERE task_id = %s ORDER BY attempt_number",
            task.id,
        )
        == settled
    )
    assert rows(handler_db, "SELECT result FROM tasks WHERE id = %s", task.id) == result
    assert rows(handler_db, "SELECT count(*) FROM attempts WHERE state = 'succeeded'") == [(1,)]
    healthy.process.send_signal(signal.SIGTERM)
    code, lines = healthy.exit()
    assert code == 0
    assert not any(line.get("event") == "operation_deadline_passed" for line in lines)


# --- retry scheduling (DEC-053) -----------------------------------------------------------

# Test values only (DEC-034): delays of 0.5 to 2 s, many poll intervals long.
RETRYING = {"ORQESTRA_WORKER_RETRY_BASE_DELAY": "PT1S", "ORQESTRA_WORKER_RETRY_MAX_DELAY": "PT2S"}
RETRYING_POLICY = RetryPolicy(timedelta(seconds=1), timedelta(seconds=2))


def attempt_history(db: Conn, task: Task) -> list[tuple[Any, ...]]:
    return rows(
        db,
        "SELECT attempt_number, started_at, finished_at, revoked_reason FROM attempts "
        "WHERE task_id = %s ORDER BY attempt_number",
        task.id,
    )


def available_at(db: Conn, task: Task) -> Any:
    ((available,),) = rows(db, "SELECT available_at FROM tasks WHERE id = %s", task.id)
    return available


def assert_spaced(db: Conn, task: Task, policy: RetryPolicy) -> None:
    """No attempt started before its predecessor's end plus that generation's delay."""
    for (n, _, finished, _), (_, started, _, _) in itertools.pairwise(attempt_history(db, task)):
        assert started >= finished + retry_delay(policy, task.id, n)


def stop(worker: WorkerProcess) -> list[dict[str, Any]]:
    worker.process.send_signal(signal.SIGTERM)
    code, lines = worker.exit()
    assert code == 0
    return lines


def test_a_transient_failure_is_retried_no_earlier_than_its_stored_time(
    handler_db: Conn, start: Any
) -> None:
    (task,) = submit(handler_db, "flaky", None, max_attempts=3)
    worker = start(overrides=RETRYING)
    eventually(lambda: task_states(handler_db, [task]) == [("failed", 3)], "every attempt")
    assert_spaced(handler_db, task, RETRYING_POLICY)
    assert runs(handler_db) == 3
    stop(worker)


def test_a_killed_workers_task_is_recovered_and_retried_no_earlier_than_its_stored_time(
    handler_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(handler_db, "gated", {"gate": gate.key})
    first = start(overrides=RETRYING)
    eventually(lambda: runs(handler_db) == 1, "the first handler")
    first.kill()  # SIGKILL: nothing is recorded, and the lease runs out
    assert task_states(handler_db, [task]) == [("running", 1)]
    second = start(overrides=RETRYING)  # recovers the attempt, then waits out the delay
    eventually(lambda: runs(handler_db) == 2, "the retry")
    (_, _, revoked_at, reason), (_, started, _, _) = attempt_history(handler_db, task)
    assert reason == "lease_expired"
    # Exactly the recovery's database time plus the first generation's delay.
    assert available_at(handler_db, task) == revoked_at + retry_delay(RETRYING_POLICY, task.id, 1)
    assert started >= available_at(handler_db, task)
    gate.release()
    eventually(lambda: task_states(handler_db, [task]) == [("succeeded", 2)], "the success")
    stop(second)


def test_a_signal_during_a_retry_delay_stops_the_process_and_leaves_the_task_waiting(
    handler_db: Conn, start: Any
) -> None:
    (task,) = submit(handler_db, "flaky", None, max_attempts=3)
    minute = {"ORQESTRA_WORKER_RETRY_BASE_DELAY": "PT1M", "ORQESTRA_WORKER_RETRY_MAX_DELAY": "PT1M"}
    worker = start(overrides=minute)
    eventually(lambda: task_states(handler_db, [task]) == [("queued", 1)], "the first failure")
    waiting = rows(handler_db, "SELECT * FROM tasks WHERE id = %s", task.id)
    attempts = rows(handler_db, "SELECT * FROM attempts WHERE task_id = %s", task.id)
    assert rows(handler_db, "SELECT available_at > clock_timestamp() FROM tasks") == [(True,)]
    lines = stop(worker)
    events = [line["event"] for line in lines if "event" in line]  # not the handler's own logs
    assert events[-2:] == ["shutdown_requested", "worker_stopped"]
    # Nothing fabricated: still queued for the same time, one attempt, one run.
    assert rows(handler_db, "SELECT * FROM tasks WHERE id = %s", task.id) == waiting
    assert rows(handler_db, "SELECT * FROM attempts WHERE task_id = %s", task.id) == attempts
    assert runs(handler_db) == 1


def test_two_workers_both_wait_for_the_stored_time(handler_db: Conn, start: Any) -> None:
    (task,) = submit(handler_db, "flaky", None, max_attempts=3)
    workers = [start(overrides=RETRYING) for _ in range(2)]
    eventually(lambda: task_states(handler_db, [task]) == [("failed", 3)], "every attempt")
    assert_spaced(handler_db, task, RETRYING_POLICY)
    assert runs(handler_db) == 3  # each generation ran once, by one of the two
    for worker in workers:
        stop(worker)


# --- provider usage through the worker's sink (DEC-059) ------------------------------------

# Test values only (DEC-034): a 0.4 s bound leaves 0.6 s of each 1 s heartbeat interval for
# usage writes; 1 + 0.4 + 1 < 4 and 0.2 < 0.4.
METERED = {
    "ORQESTRA_WORKER_DB_OPERATION_TIMEOUT": "PT0.4S",
    "ORQESTRA_WORKER_LOCK_TIMEOUT": "PT0.2S",
}
T3 = {"metered": CaptureTier.T3}
ENDLESS = 1_000_000  # calls in a first attempt: it is still calling when it is stopped
INTENT_WRITE = b"INSERT INTO provider_calls ("


@pytest.fixture
def usage_db(handler_db: Conn) -> Conn:
    handler_db.execute(PROVIDER_INVOCATIONS)
    return handler_db


def invocations(db: Conn, usage: AttemptUsage | None = None) -> int:
    """How many calls the provider double saw: in all, or of ``usage``'s attempt."""
    if usage is None:
        return int(rows(db, "SELECT count(*) FROM provider_invocations")[0][0])
    query = "SELECT count(*) FROM provider_invocations WHERE attempt_id = %s"
    return int(rows(db, query, usage.attempt_id)[0][0])


def no_call_without_its_row(db: Conn) -> bool:
    """Every call the provider double saw had its intent row committed before it."""
    return rows(db, "SELECT bool_and(intent_committed) FROM provider_invocations") in (
        [(True,)],
        [(None,)],  # no call at all
    )


def usage_of(db: Conn, task: Task, generation: int = 1) -> AttemptUsage:
    ((attempt_id,),) = rows(
        db,
        "SELECT id FROM attempts WHERE task_id = %s AND attempt_number = %s",
        task.id,
        generation,
    )
    usage = read_attempt_usage(db, attempt_id)
    assert usage is not None
    return usage


def assert_t3_recovery_closure(usage: AttemptUsage) -> None:
    """Closed by recovery at T3, and complete only if it counts every call, each answered."""
    assert usage.closure is not None
    closure = usage.closure.closure
    assert (closure.closed_by, closure.capture_tier) == (ClosedBy.RECOVERY, CaptureTier.T3)
    if closure.capture is CaptureState.COMPLETE:
        assert closure.call_count == len(usage.calls)
        assert all(call.result is not None for call in usage.calls)
        assert capture_of(usage) is AttemptCapture.CLOSED
    else:
        assert capture_of(usage) is AttemptCapture.LOWER_BOUND


def revoked_while_metered(db: Conn, task: Task) -> int:
    """How many other attempts were revoked after ``task``'s first attempt started."""
    query = (
        "SELECT count(*) FROM attempts o JOIN attempts m ON m.task_id = %s "
        "AND m.attempt_number = 1 WHERE o.task_id <> m.task_id AND o.state = 'revoked' "
        "AND o.finished_at > m.started_at"
    )
    return int(rows(db, query, task.id)[0][0])


def test_competing_work_and_a_stall_never_let_a_call_run_without_its_row(
    schema_url: str, usage_db: Conn, start: Any
) -> None:
    # The named disproof test: sweeps and heartbeats compete with waiting intents, then
    # the database stalls. A call made without its committed row, a renewal crowded out,
    # a failed attempt or a retry that does not number from 0 would each fail it.
    orphan_payloads: list[JsonValue] = [{"i": i} for i in range(8)]
    orphans = submit(usage_db, "orphaned", *orphan_payloads)
    for i, orphan in enumerate(orphans):
        # Attempts of a kind no worker runs, with leases staggered from 0.5 s to 7.5 s: they
        # keep expiring while the metered handler runs, so its sweeps really compete.
        expiring = LeasePolicy(
            lease=timedelta(milliseconds=500 + 1_000 * i), execution_timeout=timedelta(minutes=1)
        )
        start_attempt(usage_db, orphan.id, "dead-worker", policy=expiring)
    (task,) = submit(usage_db, "metered", {"calls": 5, "first_attempt_calls": ENDLESS})
    with PgProxy(schema_url) as proxy:
        first = start(proxy.url(schema_url), METERED)
        leases: list[Any] = []
        calls: list[int] = []

        def renewals() -> bool:
            first.pump()  # every intent and result is logged: keep the worker's pipe drained
            found = rows(
                usage_db,
                "SELECT lease_expires_at FROM attempts WHERE task_id = %s AND state = 'running'",
                task.id,
            )
            if found and (not leases or found[0][0] != leases[-1]):
                leases.append(found[0][0])
                calls.append(invocations(usage_db))
            return len(leases) >= 4 and revoked_while_metered(usage_db, task) > 0

        eventually(renewals, "three renewals and a revocation while calls are made")
        revoked_before_the_stall = revoked_while_metered(usage_db, task)
        proxy.stall()
        code, lines = first.exit()
    # Before the stall: renewals kept their cadence while calls went on between them, and
    # at least one orphan was revoked after the metered attempt started.
    gaps = [later - earlier for earlier, later in itertools.pairwise(leases)]
    assert max(gaps) < timedelta(seconds=2)  # heartbeat 1 s; a crowded-out one would show
    assert calls == sorted(calls)
    assert calls[-1] > calls[0]
    assert revoked_before_the_stall > 0
    assert rows(usage_db, "SELECT count(*) FROM attempts WHERE state = 'revoked'") != [(0,)]
    # The stall ends the process through the watchdog or the fence, never as a failure.
    assert (code, lines[-1]["event"]) in {
        (ExitCode.STALLED, "operation_deadline_passed"),
        (ExitCode.FENCED, "ownership_deadline_passed"),
    }
    assert no_failure_recorded(usage_db, task)
    assert no_call_without_its_row(usage_db)
    # Recovery by another worker closes the revoked attempt at T3; the retry numbers from 0.
    second = start(overrides=METERED)
    eventually(lambda: task_states(usage_db, [task]) == [("succeeded", 2)], "the retry")
    stop(second)
    first_usage = usage_of(usage_db, task, 1)
    assert first_usage.attempt_state.value == "revoked"
    assert_t3_recovery_closure(first_usage)
    made = invocations(usage_db, first_usage)
    assert made <= len(first_usage.calls) <= made + 1  # at most one intent without its call
    retry = usage_of(usage_db, task, 2)
    assert [call.call.intent.call_seq for call in retry.calls] == [0, 1, 2, 3, 4]
    assert retry.closure is not None
    assert (retry.closure.closure.capture, retry.closure.closure.call_count) == (
        CaptureState.COMPLETE,
        5,
    )
    assert no_call_without_its_row(usage_db)


def test_a_fence_during_a_stalled_call_leaves_it_unresolved_for_recovery(
    schema_url: str, usage_db: Conn, start: Any, gate: Gate
) -> None:
    (task,) = submit(usage_db, "metered", {"calls": 1, "gate": gate.key})
    with PgProxy(schema_url) as proxy:
        worker = start(proxy.url(schema_url), METERED)
        eventually(lambda: invocations(usage_db) == 1, "the call in flight")
        proxy.refuse()  # heartbeats are never confirmed again
        code, lines = worker.exit()
    assert code == ExitCode.FENCED
    assert lines[-1]["event"] == "ownership_deadline_passed"
    assert no_failure_recorded(usage_db, task)
    revocation = eventually(
        lambda: revoke_next_expired(usage_db, retry_policy=RETRY, usage_capture=T3),
        "the lease to end",
    )
    assert revocation.usage_closure is not None
    usage = usage_of(usage_db, task)
    assert [call.result for call in usage.calls] == [None]  # the call stays unresolved
    assert_t3_recovery_closure(usage)
    assert usage.closure is not None
    assert usage.closure.closure.capture is CaptureState.INCOMPLETE
    assert no_call_without_its_row(usage_db)


def test_a_stalled_intent_write_ends_the_process_before_any_call(
    schema_url: str, usage_db: Conn, start: Any
) -> None:
    (task,) = submit(usage_db, "metered", {"calls": 1})
    with PgProxy(schema_url) as proxy:
        proxy.hold_on(INTENT_WRITE)  # the intent's write never gets an answer
        worker = start(proxy.url(schema_url), METERED)
        code, lines = worker.exit()
    assert code == ExitCode.STALLED
    assert lines[-1]["event"] == "operation_deadline_passed"
    assert lines[-1]["operation"] == "record_call_intent"
    assert lines[-1]["call_seq"] == "0"
    assert invocations(usage_db) == 0  # no call
    assert usage_of(usage_db, task).calls == ()
    assert no_failure_recorded(usage_db, task)


def test_a_killed_worker_mid_batch_leaves_no_call_without_its_row(
    usage_db: Conn, start: Any
) -> None:
    (task,) = submit(usage_db, "metered", {"calls": 1, "first_attempt_calls": ENDLESS})
    worker = start(overrides=METERED)
    eventually(lambda: invocations(usage_db) >= 20, "calls under way")
    worker.kill()  # SIGKILL: no exit path runs
    revocation = eventually(
        lambda: revoke_next_expired(usage_db, retry_policy=RETRY, usage_capture=T3),
        "the lease to end",
    )
    assert revocation.usage_closure is not None
    usage = usage_of(usage_db, task)
    made = invocations(usage_db, usage)
    assert made <= len(usage.calls) <= made + 1
    assert [call.call.intent.call_seq for call in usage.calls] == list(range(len(usage.calls)))
    assert sum(call.result is None for call in usage.calls) <= 1
    assert_t3_recovery_closure(usage)
    assert no_call_without_its_row(usage_db)
    assert no_failure_recorded(usage_db, task)
