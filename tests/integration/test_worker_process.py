"""The worker's process-level exits, in a real subprocess (DEC-045).

Each test starts ``python -m tests.integration.worker_process``, whose default
``terminate`` and watchdog end the process with ``os._exit``, and checks the
exit code, the event it logged last, and what PostgreSQL recorded. A handler
waits on an advisory lock the test holds; the process may reach the database
through a ``PgProxy`` the test can stall or cut. "Before its lease ended" is
checked against PostgreSQL's own clock and the stored ``lease_expires_at``.
Graceful shutdown (DEC-049) is driven with real SIGTERM and SIGINT, sent only
once the worker has logged that its handlers are installed, and timed from
the worker's own log timestamps.
"""

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

from orqestra.domain.jobs import Task
from orqestra.persistence.lifecycle import request_cancellation, revoke_next_expired
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
from tests.integration.worker_process import HANDLER_RUNS

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]
ROOT = Path(__file__).resolve().parents[2]

# Test values only (DEC-034): heartbeat 1 s + database bound 0.8 s + margin 1 s < lease 4 s.
LEASE = timedelta(seconds=4)
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
    revocation = eventually(lambda: revoke_next_expired(handler_db), "the grace to run out")
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
    revocation = eventually(lambda: revoke_next_expired(handler_db), "the lease to end")
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
