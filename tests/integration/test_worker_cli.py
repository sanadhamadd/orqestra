"""``orqestra worker run`` as a real process: the installed console script (DEC-051).

Each test starts the ``orqestra`` script from this environment, exactly as an
operator would, against an isolated schema of the test database, and submits
work through the Python ``submit_job`` interface (there is no submission
command). It synchronises on the worker's own log lines and on PostgreSQL
states, never on fixed sleeps; ``diagnostic.wait`` keeps a worker busy for as
long as a test needs. Startup errors are JSON on stdout; the worker logs JSON
on stderr.
"""

import json
import os
import select
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.domain.jobs import JsonValue, Task
from orqestra.persistence.lifecycle import request_cancellation
from orqestra.persistence.migrate import load_migrations, migrate
from orqestra.worker.builtin import ECHO, WAIT
from orqestra.worker.fence import ExitCode
from tests.integration.pg_proxy import PgProxy
from tests.integration.test_worker_runtime import WAIT as BOUND
from tests.integration.test_worker_runtime import eventually, rows, submit, task_states

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]
ROOT = Path(__file__).resolve().parents[2]
ORQESTRA = Path(sys.executable).with_name("orqestra")  # the installed console script

# Test values only (DEC-034), never recommendations: heartbeat 1 s + database bound
# 0.8 s + margin 1 s < lease 4 s; lock wait 0.3 s < database bound (DEC-050).
ENVIRONMENT = {
    "ORQESTRA_ENVIRONMENT": "test",
    "ORQESTRA_LOG_LEVEL": "INFO",
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


class CliWorker:
    """One ``orqestra worker run`` process."""

    def __init__(self, url: str, kinds: list[str], overrides: dict[str, str]) -> None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("ORQESTRA_")}
        env |= ENVIRONMENT | {"ORQESTRA_DATABASE_URL": url} | overrides
        argv = [str(ORQESTRA), "worker", "run", *[a for kind in kinds for a in ("--kind", kind)]]
        self.process = subprocess.Popen(  # a fixed test command
            argv, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        self._read = ""  # stderr read before the process ended

    @property
    def pid(self) -> int:
        return self.process.pid

    def wait_for(self, *events: str) -> dict[str, Any]:
        """Read the log (bounded) until a line with one of ``events``; return that line."""
        assert self.process.stderr is not None
        fd = self.process.stderr.fileno()  # read raw, so no text buffer hides a line
        deadline = time.monotonic() + BOUND
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

    def exit(self) -> tuple[int, str, list[dict[str, Any]], str]:
        """Wait (bounded) for the end: exit code, stdout, JSON log lines and raw stderr."""
        out, err = self.process.communicate(timeout=BOUND)
        text = self._read + err
        lines = [json.loads(line) for line in text.splitlines() if line.startswith("{")]
        return self.process.returncode, out, lines, text

    def stop(self, sig: signal.Signals = signal.SIGTERM) -> tuple[int, list[dict[str, Any]]]:
        self.process.send_signal(sig)
        code, _, lines, _ = self.exit()
        return code, lines


@pytest.fixture
def start() -> Iterator[Any]:
    started: list[CliWorker] = []

    def launch(url: str, kinds: list[str], overrides: dict[str, str] | None = None) -> CliWorker:
        worker = CliWorker(url, kinds, overrides or {})
        started.append(worker)
        return worker

    yield launch
    for worker in started:
        if worker.process.poll() is None:
            os.kill(worker.pid, signal.SIGCONT)  # in case a test left it stopped
            worker.process.kill()
            worker.process.communicate(timeout=BOUND)


def attempts(db: Conn, task: Task) -> list[tuple[Any, ...]]:
    return rows(
        db,
        "SELECT attempt_number, state, revoked_reason, worker_id FROM attempts "
        "WHERE task_id = %s ORDER BY attempt_number",
        task.id,
    )


def held_by(db: Conn, task: Task, worker_id: str) -> bool:
    return rows(
        db,
        "SELECT count(*) FROM attempts WHERE task_id = %s AND state = 'running' AND worker_id = %s",
        task.id,
        worker_id,
    ) == [(1,)]


def result_of(db: Conn, task: Task) -> Any:
    return rows(db, "SELECT result FROM tasks WHERE id = %s", task.id)[0][0]


def test_the_console_script_is_installed() -> None:
    assert ORQESTRA.is_file()


# --- startup ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("case", "error"),
    [
        ("behind", "the database schema is not current: schema "),
        ("unreachable", "the database could not be reached or read (OperationalError)"),
        ("production", "diagnostic handlers are refused in production: ['diagnostic.echo']"),
    ],
)
def test_a_refused_start_exits_1_safely_and_claims_nothing(
    schema_url: str, start: Any, case: str, error: str
) -> None:
    migrate(schema_url, load_migrations()[:2] if case == "behind" else None)
    with psycopg.connect(schema_url, autocommit=True) as db:
        (task,) = submit(db, ECHO, {"n": 1})
        secret = "s3cret-not-the-real-one"
        url = f"postgresql://u:{secret}@127.0.0.1:1/orq" if case == "unreachable" else schema_url
        overrides = {"ORQESTRA_ENVIRONMENT": "production"} if case == "production" else {}
        code, out, lines, err = start(url, [ECHO], overrides).exit()
        assert code == 1
        assert json.loads(out)["status"] == "error"
        assert error in json.loads(out)["error"]
        assert secret not in out + err
        assert not any(line.get("event") == "worker_started" for line in lines)
        assert task_states(db, [task]) == [("queued", 0)]  # nothing was claimed


# --- two independent workers -----------------------------------------------------------------


def test_two_workers_share_the_work_and_every_task_settles_once(
    db: Conn, schema_url: str, start: Any
) -> None:
    (waiting,) = submit(db, WAIT, {"seconds": 10})
    first = start(schema_url, [ECHO, WAIT])
    first_id = first.wait_for("worker_started")["worker_id"]
    eventually(lambda: held_by(db, waiting, first_id), "the first worker to hold the wait")
    payloads: list[JsonValue] = [{"i": i} for i in range(9)]
    echoes = submit(db, ECHO, *payloads)
    (other,) = submit(db, "other.kind", {"x": 1})
    second = start(schema_url, [ECHO, WAIT])
    second_id = second.wait_for("worker_started")["worker_id"]
    eventually(lambda: task_states(db, echoes) == [("succeeded", 1)] * 9, "the echo tasks")
    # The first worker still holds the wait, so the second worker did all of these.
    assert held_by(db, waiting, first_id)
    eventually(lambda: task_states(db, [waiting]) == [("succeeded", 1)], "the wait", 30)
    assert first_id != second_id
    assert attempts(db, waiting) == [(1, "succeeded", None, first_id)]
    for i, task in enumerate(echoes):
        assert attempts(db, task) == [(1, "succeeded", None, second_id)]
        assert result_of(db, task) == {"i": i}
    assert result_of(db, waiting) == {"waited_seconds": 10}
    assert rows(db, "SELECT count(*) FROM attempts") == [(10,)]  # one attempt per task
    assert task_states(db, [other]) == [("queued", 0)]  # no worker runs that kind
    for worker in (first, second):
        code, lines = worker.stop()
        assert code == 0
        assert lines[-1]["event"] == "worker_stopped"


# --- crash recovery and fencing --------------------------------------------------------------


def test_a_killed_worker_is_recovered_by_another_and_one_result_stands(
    db: Conn, schema_url: str, start: Any
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 3}, max_attempts=3)
    first = start(schema_url, [WAIT])
    first_id = first.wait_for("worker_started")["worker_id"]
    eventually(lambda: held_by(db, task, first_id), "the first worker to hold it")
    first.process.kill()  # SIGKILL: no shutdown, no outcome
    assert first.exit()[0] == -signal.SIGKILL
    second = start(schema_url, [WAIT])
    second_id = second.wait_for("worker_started")["worker_id"]
    eventually(lambda: task_states(db, [task]) == [("succeeded", 2)], "recovery and a rerun", 30)
    assert attempts(db, task) == [
        (1, "revoked", "lease_expired", first_id),
        (2, "succeeded", None, second_id),
    ]
    assert result_of(db, task) == {"waited_seconds": 3}
    assert second.stop()[0] == 0


def test_a_stale_worker_cannot_replace_the_newer_result(
    db: Conn, schema_url: str, start: Any
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 2}, max_attempts=3)
    first = start(schema_url, [WAIT])
    first_id = first.wait_for("worker_started")["worker_id"]
    eventually(lambda: held_by(db, task, first_id), "the first worker to hold it")
    os.kill(first.pid, signal.SIGSTOP)  # frozen past its lease, handler and all
    second = start(schema_url, [WAIT])
    second_id = second.wait_for("worker_started")["worker_id"]
    eventually(lambda: task_states(db, [task]) == [("succeeded", 2)], "the newer generation", 30)
    settled = (attempts(db, task), result_of(db, task))
    os.kill(first.pid, signal.SIGCONT)
    # Either its fence ends it, or its late outcome is refused; it never counts.
    line = first.wait_for("ownership_deadline_passed", "ownership_lost", "outcome_discarded")
    assert line["event"] in {"ownership_deadline_passed", "ownership_lost", "outcome_discarded"}
    assert (attempts(db, task), result_of(db, task)) == settled
    assert settled[0] == [
        (1, "revoked", "lease_expired", first_id),
        (2, "succeeded", None, second_id),
    ]
    assert second.stop()[0] == 0


# --- cancellation, shutdown and exits ---------------------------------------------------------


def test_a_wait_is_cancelled_through_a_committed_request(
    db: Conn, schema_url: str, start: Any
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 120})
    worker = start(schema_url, [WAIT])
    worker_id = worker.wait_for("worker_started")["worker_id"]
    eventually(lambda: held_by(db, task, worker_id), "the worker to hold it")
    request_cancellation(db, task.id, grace=timedelta(seconds=30))
    eventually(lambda: task_states(db, [task]) == [("cancelled", 1)], "the cancellation")
    assert attempts(db, task) == [(1, "cancelled", None, worker_id)]
    assert worker.process.poll() is None  # it carries on
    assert worker.stop()[0] == 0


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT], ids=["sigterm", "sigint"])
def test_a_signal_while_idle_exits_0(
    db: Conn, schema_url: str, start: Any, sig: signal.Signals
) -> None:
    worker = start(schema_url, [ECHO])
    worker.wait_for("worker_started")  # its handlers are installed by now
    code, lines = worker.stop(sig)
    assert code == 0
    assert [line["event"] for line in lines][-2:] == ["shutdown_requested", "worker_stopped"]


def test_a_busy_worker_that_settles_within_the_drain_exits_0(
    db: Conn, schema_url: str, start: Any
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 2})
    worker = start(schema_url, [WAIT], {"ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": "PT30S"})
    worker_id = worker.wait_for("worker_started")["worker_id"]
    eventually(lambda: held_by(db, task, worker_id), "the worker to hold it")
    code, lines = worker.stop()
    assert code == 0
    assert lines[-1]["event"] == "worker_stopped"
    assert task_states(db, [task]) == [("succeeded", 1)]


def test_a_busy_worker_that_cannot_settle_in_time_exits_9(
    db: Conn, schema_url: str, start: Any
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 120})
    worker = start(schema_url, [WAIT])  # a 1 s drain
    worker_id = worker.wait_for("worker_started")["worker_id"]
    eventually(lambda: held_by(db, task, worker_id), "the worker to hold it")
    code, lines = worker.stop()
    assert code == ExitCode.SHUTDOWN
    assert lines[-1]["event"] == "drain_deadline_passed"
    assert task_states(db, [task]) == [("running", 1)]  # left to recovery, nothing recorded


def test_a_lost_database_while_busy_still_fences_the_process(
    db: Conn, schema_url: str, start: Any
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 120})
    with PgProxy(schema_url) as proxy:
        worker = start(proxy.url(schema_url), [WAIT])
        worker_id = worker.wait_for("worker_started")["worker_id"]
        eventually(lambda: held_by(db, task, worker_id), "the worker to hold it")
        proxy.refuse()  # no heartbeat can be confirmed from now on
        code, _, lines, _ = worker.exit()
    assert code == ExitCode.FENCED  # never 0
    assert lines[-1]["event"] == "ownership_deadline_passed"
    assert task_states(db, [task]) == [("running", 1)]
