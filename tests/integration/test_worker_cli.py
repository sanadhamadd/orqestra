"""``orqestra worker run`` as a real process: the installed console script (DEC-051).

Each test starts the ``orqestra`` script from this environment, exactly as an
operator would, against an isolated schema of the test database, and submits
work through the Python ``submit_job`` interface (there is no submission
command). It synchronises on the worker's own log lines and on PostgreSQL
states, never on fixed sleeps; ``diagnostic.wait`` keeps a worker busy for as
long as a test needs. Startup errors are JSON on stdout; the worker logs JSON
on stderr.

Both streams of every worker are read continuously, so a worker never blocks
on a full pipe, and every bounded wait that fails reports what was observed:
each worker's exit status and event sequence, its last non-JSON stderr lines
(credentials masked), the test's tasks and attempts, and the server's
connection counts. Log messages and payloads are never reported.
"""

import contextlib
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import IO, Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.domain.jobs import JsonValue, Task
from orqestra.persistence.lifecycle import StaleAttemptError, request_cancellation
from orqestra.persistence.migrate import load_migrations, migrate
from orqestra.worker.builtin import ECHO, WAIT
from orqestra.worker.fence import ExitCode, Guard
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
    "ORQESTRA_WORKER_RETRY_BASE_DELAY": "PT0.1S",
    "ORQESTRA_WORKER_RETRY_MAX_DELAY": "PT0.5S",
}

# --- observing a process -----------------------------------------------------------------------

SHOWN_FIELDS = ("event", "level", "exit_code", "task_id", "attempt_id", "operation")
STDERR_TAIL = 20
_CREDENTIALS = re.compile(r"\b([A-Za-z][A-Za-z0-9+.\-]*://)[^\s/@]*@")


def mask_credentials(text: str) -> str:
    """``scheme://user:password@`` becomes ``scheme://***@``."""
    return _CREDENTIALS.sub(r"\1***@", text)


@dataclass(frozen=True, slots=True)
class Line:
    """One line a process wrote; ``record`` is set for a complete JSON object line."""

    stream: str  # "stdout" or "stderr"
    text: str
    record: dict[str, Any] | None


def _parse(raw: bytes) -> dict[str, Any] | None:
    if not raw.endswith(b"\n"):
        return None  # incomplete: the process ended in the middle of it
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


class Observed:
    """A child process whose stdout and stderr are read continuously, line by line.

    Two daemon threads read the streams as the process writes them, so it never
    blocks on a full pipe, and append every line to ``lines``. ``wait_for``
    waits (bounded) for a JSON line with one of the given events; when it
    fails, it reports ``report()``, by default this process alone.
    """

    def __init__(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        stdin: bool = False,
        bound: float = BOUND,
        report: Callable[[], str] | None = None,
    ) -> None:
        self.bound = bound
        self.report = report or (lambda: describe([self], None, []))
        self.lines: list[Line] = []
        self._changed = threading.Condition()
        self._open = 2  # streams not yet at their end
        self.process = subprocess.Popen(  # a fixed test command
            list(argv),
            cwd=cwd,
            env=None if env is None else dict(env),
            stdin=subprocess.PIPE if stdin else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert self.process.stdout is not None
        assert self.process.stderr is not None
        self._readers = [
            threading.Thread(target=self._drain, args=(stream, name), daemon=True)
            for stream, name in ((self.process.stdout, "stdout"), (self.process.stderr, "stderr"))
        ]
        for reader in self._readers:
            reader.start()

    @property
    def pid(self) -> int:
        return self.process.pid

    def _drain(self, stream: IO[bytes], name: str) -> None:
        try:
            for raw in iter(stream.readline, b""):
                line = Line(name, raw.decode(errors="replace").rstrip("\n"), _parse(raw))
                with self._changed:
                    self.lines.append(line)
                    self._changed.notify_all()
        finally:
            with self._changed:
                self._open -= 1
                self._changed.notify_all()

    def snapshot(self) -> tuple[list[Line], bool]:
        """The lines read so far, and whether both streams have ended."""
        with self._changed:
            return list(self.lines), self._open == 0

    def records(self, stream: str = "stderr") -> list[dict[str, Any]]:
        lines, _ = self.snapshot()
        return [r for line in lines if (r := line.record) is not None and line.stream == stream]

    def wait_for(
        self, *events: str, where: Mapping[str, Any] | None = None, since: int = 0
    ) -> tuple[int, dict[str, Any]]:
        """Wait (bounded) for a JSON line with one of ``events``; return its index and the line.

        Only lines from index ``since`` on are considered, and only those whose
        fields equal every item of ``where``. It fails at once, with the
        report, if the process's output has ended without such a line.
        """
        wanted = dict(where or {})
        deadline = time.monotonic() + self.bound
        checked = since
        with self._changed:
            while True:
                for index in range(checked, len(self.lines)):
                    record = self.lines[index].record
                    if (
                        record is not None
                        and record.get("event") in events
                        and all(record.get(key) == value for key, value in wanted.items())
                    ):
                        return index, record
                checked = max(checked, len(self.lines))
                ended, left = self._open == 0, deadline - time.monotonic()
                if ended or left <= 0:
                    break
                self._changed.wait(left)
        if ended:
            # Its output ended: let the exit status be known before reporting it.
            with contextlib.suppress(subprocess.TimeoutExpired):
                self.process.wait(timeout=self.bound)
        why = "its output ended" if ended else f"within {self.bound}s"
        pytest.fail(
            f"pid {self.pid} logged none of {list(events)} where {wanted} from line {since} "
            f"({why})\n{self.report()}"
        )

    def exit(self) -> tuple[int, str, list[dict[str, Any]], str]:
        """Wait (bounded) for the end: exit code, stdout, stderr's JSON lines and raw stderr."""
        deadline = time.monotonic() + self.bound
        try:
            code = self.process.wait(timeout=self.bound)
        except subprocess.TimeoutExpired:
            pytest.fail(f"pid {self.pid} did not exit within {self.bound}s\n{self.report()}")
        for reader in self._readers:
            reader.join(max(0.0, deadline - time.monotonic()))
        lines, ended = self.snapshot()
        if not ended:
            pytest.fail(f"pid {self.pid}'s output did not end with it\n{self.report()}")
        self._close_streams()
        out = "".join(line.text + "\n" for line in lines if line.stream == "stdout")
        err = "".join(line.text + "\n" for line in lines if line.stream == "stderr")
        return code, out, self.records(), err

    def stop(self, sig: signal.Signals = signal.SIGTERM) -> tuple[int, list[dict[str, Any]]]:
        self.process.send_signal(sig)
        code, _, lines, _ = self.exit()
        return code, lines

    def close(self) -> None:
        """End the process if it still runs (bounded) and release its pipes."""
        if self.process.poll() is None:
            os.kill(self.pid, signal.SIGCONT)  # in case a test left it stopped
            self.process.kill()
        self.process.wait(timeout=self.bound)
        for reader in self._readers:
            reader.join(self.bound)
        if not any(reader.is_alive() for reader in self._readers):
            self._close_streams()

    def _close_streams(self) -> None:
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            if stream is not None:
                stream.close()

    def status(self) -> str:
        code = self.process.poll()
        if code is None:
            return "running"
        if code < 0:
            try:
                name = signal.Signals(-code).name
            except ValueError:
                name = "an unknown signal"
            return f"exit status {code} ({name})"
        return f"exit status {code}"

    def evidence(self, name: str) -> str:
        """This process's evidence; never a log message, a payload or a credential."""
        lines, _ = self.snapshot()
        out = [f"--- {name}: pid {self.pid}, {self.status()}", "events:"]
        for index, line in enumerate(lines):
            if (record := line.record) is not None:
                shown = [f"{key}={record[key]}" for key in SHOWN_FIELDS if key in record]
                fields = " ".join(shown) or "(no event fields)"
                out.append(f"  [{index}] {line.stream} {fields}")
        raw = [line.text for line in lines if line.record is None and line.stream == "stderr"]
        out.append(f"last {STDERR_TAIL} non-JSON stderr lines:")
        out.extend(f"  {mask_credentials(text)}" for text in raw[-STDERR_TAIL:])
        return "\n".join(out)


def _database(db: Conn, tasks: Sequence[Task]) -> list[str]:
    out = ["--- database", "tasks (task_key, state, attempt_count) and their attempts:"]
    try:
        for task in tasks:
            out.append(f"  {rows(db, _TASK, task.id)}")
            out.extend(f"    attempt {attempt}" for attempt in attempts(db, task))
        out.append("pg_stat_activity (state, application_name, count):")
        out.extend(f"  {row}" for row in rows(db, _ACTIVITY))
        out.append(f"max_connections: {rows(db, 'SHOW max_connections')[0][0]}")
    except psycopg.Error as exc:
        out.append(f"  unavailable ({type(exc).__name__})")
    return out


_TASK = "SELECT task_key, state, attempt_count FROM tasks WHERE id = %s"
_ACTIVITY = (
    "SELECT state, application_name, count(*) FROM pg_stat_activity "
    "GROUP BY state, application_name ORDER BY state NULLS FIRST, application_name"
)


def describe(workers: Sequence[Observed], db: Conn | None, tasks: Sequence[Task]) -> str:
    """The evidence of a failed wait: every process, then the test's tasks and the server."""
    parts = [worker.evidence(f"process {n}") for n, worker in enumerate(workers)]
    if db is not None:
        parts.extend(_database(db, tasks))
    return "\n".join(parts)


def settle[T](
    check: Callable[[], T | None],
    what: str,
    *,
    workers: Sequence[Observed],
    db: Conn | None,
    tasks: Sequence[Task],
    timeout: float = BOUND,
) -> T:
    """``eventually``, failing with the evidence report."""
    try:
        return eventually(check, what, timeout)
    except pytest.fail.Exception as exc:
        pytest.fail(f"{exc}\n{describe(workers, db, tasks)}")


# --- how a worker frozen with its handler running may end once resumed ----------------------------

OWNERSHIP_ENDED = frozenset({f"{Guard.OWNERSHIP.value}_deadline_passed", "ownership_lost"})
OPERATION_EXPIRED = f"{Guard.OPERATION.value}_deadline_passed"
# The supervisor's database calls while a handler runs: heartbeats, sweeps, its completion.
POST_START_OPERATIONS = frozenset({"renew_lease", "complete_attempt", "revoke_next_expired"})
DISCARDED = "outcome_discarded"
STALE_DISCARD = f"{DISCARDED}: {StaleAttemptError.__name__}"
# What the resumed worker is waited for: every accepted ending, and those that must be refused
# with the report rather than a timeout. Any other ending fails at once (its output ends).
RESUMED = (
    *sorted(OWNERSHIP_ENDED),
    OPERATION_EXPIRED,
    DISCARDED,
    "claimed_task_not_started",
    "attempt_completed",
    "completion_ambiguous",
)


def check_resumed(line: Mapping[str, Any], *, worker_id: str, attempt_id: str) -> ExitCode | None:
    """The exit code a resumed, stale worker must end with after ``line``; None if it carries on.

    Accepted only are the endings of a worker frozen after its handler started:

    - its fence: ``ownership_deadline_passed`` or ``ownership_lost`` for its attempt, exit 3;
    - a stalled post-start database call: ``operation_deadline_passed`` with
      ``renew_lease`` or ``complete_attempt`` for its attempt, or ``revoke_next_expired``
      (a sweep, which names no attempt), exit 4;
    - its late outcome refused because a newer generation exists: ``outcome_discarded``
      with ``StaleAttemptError`` for its attempt, and the worker carries on.

    Anything else fails, notably every pre-start ending.
    """
    event = line.get("event")
    assert line.get("worker_id") == worker_id, f"{event} is from another worker"
    assert line.get("attempt_id", attempt_id) == attempt_id, f"{event} is for another attempt"
    if event in OWNERSHIP_ENDED:
        assert line.get("exit_code") == ExitCode.FENCED, f"{event} with another exit code"
        assert "attempt_id" in line, f"{event} names no attempt"
        return ExitCode.FENCED
    if event == OPERATION_EXPIRED:
        operation = line.get("operation")
        post_start = operation in POST_START_OPERATIONS
        assert post_start, f"{event} during {operation!r}: not a call made while a handler runs"
        assert line.get("exit_code") == ExitCode.STALLED, f"{event} with another exit code"
        if operation == "revoke_next_expired":
            assert "attempt_id" not in line, f"{event} during a sweep names an attempt"
        else:
            assert "attempt_id" in line, f"{event} during {operation!r} names no attempt"
        return ExitCode.STALLED
    if event == DISCARDED:
        assert "exit_code" not in line, f"{event} ended the process"
        assert "attempt_id" in line, f"{event} names no attempt"
        assert line.get("message") == STALE_DISCARD, f"{event} for another reason"
        return None
    msg = f"{event!r} is not an ending of a worker resumed after its handler started"
    raise AssertionError(msg)


# --- the worker process --------------------------------------------------------------------------


class CliWorker(Observed):
    """One ``orqestra worker run`` process."""

    def __init__(
        self, url: str, kinds: list[str], overrides: dict[str, str], report: Callable[[], str]
    ) -> None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("ORQESTRA_")}
        env |= ENVIRONMENT | {"ORQESTRA_DATABASE_URL": url} | overrides
        argv = [str(ORQESTRA), "worker", "run", *[a for kind in kinds for a in ("--kind", kind)]]
        super().__init__(argv, env=env, cwd=ROOT, report=report)


class Launcher:
    """Starts a test's workers; a failed wait reports all of them and the watched tasks."""

    def __init__(self) -> None:
        self.workers: list[CliWorker] = []
        self.db: Conn | None = None
        self.tasks: list[Task] = []

    def __call__(
        self, url: str, kinds: list[str], overrides: dict[str, str] | None = None
    ) -> CliWorker:
        worker = CliWorker(url, kinds, overrides or {}, self.report)
        self.workers.append(worker)
        return worker

    def watch(self, db: Conn, *tasks: Task) -> None:
        self.db = db
        self.tasks.extend(tasks)

    def report(self) -> str:
        return describe(self.workers, self.db, self.tasks)

    def settle[T](self, check: Callable[[], T | None], what: str, timeout: float = BOUND) -> T:
        return settle(
            check, what, workers=self.workers, db=self.db, tasks=self.tasks, timeout=timeout
        )


@pytest.fixture
def start() -> Iterator[Launcher]:
    launcher = Launcher()
    yield launcher
    for worker in launcher.workers:
        worker.close()


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
    schema_url: str, start: Launcher, case: str, error: str
) -> None:
    migrate(schema_url, load_migrations()[:2] if case == "behind" else None)
    with psycopg.connect(schema_url, autocommit=True) as db:
        (task,) = submit(db, ECHO, {"n": 1})
        start.watch(db, task)
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
    db: Conn, schema_url: str, start: Launcher
) -> None:
    (waiting,) = submit(db, WAIT, {"seconds": 10})
    start.watch(db, waiting)
    first = start(schema_url, [ECHO, WAIT])
    first_id = first.wait_for("worker_started")[1]["worker_id"]
    start.settle(lambda: held_by(db, waiting, first_id), "the first worker to hold the wait")
    payloads: list[JsonValue] = [{"i": i} for i in range(9)]
    echoes = submit(db, ECHO, *payloads)
    (other,) = submit(db, "other.kind", {"x": 1})
    start.watch(db, *echoes, other)
    second = start(schema_url, [ECHO, WAIT])
    second_id = second.wait_for("worker_started")[1]["worker_id"]
    start.settle(lambda: task_states(db, echoes) == [("succeeded", 1)] * 9, "the echo tasks")
    # The first worker still holds the wait, so the second worker did all of these.
    assert held_by(db, waiting, first_id)
    start.settle(lambda: task_states(db, [waiting]) == [("succeeded", 1)], "the wait", 30)
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
    db: Conn, schema_url: str, start: Launcher
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 3}, max_attempts=3)
    start.watch(db, task)
    first = start(schema_url, [WAIT])
    first_id = first.wait_for("worker_started")[1]["worker_id"]
    start.settle(lambda: held_by(db, task, first_id), "the first worker to hold it")
    first.process.kill()  # SIGKILL: no shutdown, no outcome
    assert first.exit()[0] == -signal.SIGKILL
    second = start(schema_url, [WAIT])
    second_id = second.wait_for("worker_started")[1]["worker_id"]
    start.settle(lambda: task_states(db, [task]) == [("succeeded", 2)], "recovery and a rerun", 30)
    assert attempts(db, task) == [
        (1, "revoked", "lease_expired", first_id),
        (2, "succeeded", None, second_id),
    ]
    assert result_of(db, task) == {"waited_seconds": 3}
    assert second.stop()[0] == 0


def test_a_stale_worker_cannot_replace_the_newer_result(
    db: Conn, schema_url: str, start: Launcher
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 2}, max_attempts=3)
    start.watch(db, task)
    first = start(schema_url, [WAIT])
    first_id = first.wait_for("worker_started")[1]["worker_id"]
    # A claim commits before its handler starts: freeze only once the handler runs, and
    # only for the attempt PostgreSQL holds for this worker.
    started_at, started = first.wait_for("handler_started", where={"task_id": str(task.id)})
    attempt_id = started["attempt_id"]
    query = "SELECT id, attempt_number, state, worker_id FROM attempts WHERE task_id = %s"
    held = [(str(a), *rest) for a, *rest in rows(db, query, task.id)]
    assert held == [(attempt_id, 1, "running", first_id)], start.report()
    os.kill(first.pid, signal.SIGSTOP)  # frozen past its lease, handler and all
    # Frozen with its handler still running: nothing has been recorded for the attempt.
    early = "the handler had ended before the freeze"
    assert task_states(db, [task]) == [("running", 1)], f"{early}\n{start.report()}"
    assert attempts(db, task) == [(1, "running", None, first_id)], f"{early}\n{start.report()}"
    second = start(schema_url, [WAIT])
    second_id = second.wait_for("worker_started")[1]["worker_id"]
    start.settle(lambda: task_states(db, [task]) == [("succeeded", 2)], "the newer generation", 30)
    settled = (attempts(db, task), result_of(db, task))
    os.kill(first.pid, signal.SIGCONT)
    # Its fence ends it, a stalled post-start call ends it, or its late outcome is refused.
    _, line = first.wait_for(*RESUMED, where={"worker_id": first_id}, since=started_at + 1)
    try:
        expected = check_resumed(line, worker_id=first_id, attempt_id=attempt_id)
    except AssertionError as exc:
        pytest.fail(f"{exc}\n{start.report()}")
    if expected is None:
        assert first.process.poll() is None, start.report()  # it carries on
        assert first.stop()[0] == 0
    else:
        assert first.exit()[0] == expected, start.report()
    assert not any(r.get("event") == "attempt_completed" for r in first.records()), start.report()
    assert (attempts(db, task), result_of(db, task)) == settled
    assert settled[0] == [
        (1, "revoked", "lease_expired", first_id),
        (2, "succeeded", None, second_id),
    ]
    assert second.stop()[0] == 0


# --- cancellation, shutdown and exits ---------------------------------------------------------


def test_a_wait_is_cancelled_through_a_committed_request(
    db: Conn, schema_url: str, start: Launcher
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 120})
    start.watch(db, task)
    worker = start(schema_url, [WAIT])
    worker_id = worker.wait_for("worker_started")[1]["worker_id"]
    start.settle(lambda: held_by(db, task, worker_id), "the worker to hold it")
    request_cancellation(db, task.id, grace=timedelta(seconds=30))
    start.settle(lambda: task_states(db, [task]) == [("cancelled", 1)], "the cancellation")
    assert attempts(db, task) == [(1, "cancelled", None, worker_id)]
    assert worker.process.poll() is None  # it carries on
    assert worker.stop()[0] == 0


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT], ids=["sigterm", "sigint"])
def test_a_signal_while_idle_exits_0(
    db: Conn, schema_url: str, start: Launcher, sig: signal.Signals
) -> None:
    start.watch(db)
    worker = start(schema_url, [ECHO])
    worker.wait_for("worker_started")  # its handlers are installed by now
    code, lines = worker.stop(sig)
    assert code == 0
    assert [line["event"] for line in lines][-2:] == ["shutdown_requested", "worker_stopped"]


def test_a_busy_worker_that_settles_within_the_drain_exits_0(
    db: Conn, schema_url: str, start: Launcher
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 2})
    start.watch(db, task)
    worker = start(schema_url, [WAIT], {"ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": "PT30S"})
    worker_id = worker.wait_for("worker_started")[1]["worker_id"]
    start.settle(lambda: held_by(db, task, worker_id), "the worker to hold it")
    code, lines = worker.stop()
    assert code == 0
    assert lines[-1]["event"] == "worker_stopped"
    assert task_states(db, [task]) == [("succeeded", 1)]


def test_a_busy_worker_that_cannot_settle_in_time_exits_9(
    db: Conn, schema_url: str, start: Launcher
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 120})
    start.watch(db, task)
    worker = start(schema_url, [WAIT])  # a 1 s drain
    worker_id = worker.wait_for("worker_started")[1]["worker_id"]
    start.settle(lambda: held_by(db, task, worker_id), "the worker to hold it")
    code, lines = worker.stop()
    assert code == ExitCode.SHUTDOWN
    assert lines[-1]["event"] == "drain_deadline_passed"
    assert task_states(db, [task]) == [("running", 1)]  # left to recovery, nothing recorded


def test_a_lost_database_while_busy_still_fences_the_process(
    db: Conn, schema_url: str, start: Launcher
) -> None:
    (task,) = submit(db, WAIT, {"seconds": 120})
    start.watch(db, task)
    with PgProxy(schema_url) as proxy:
        worker = start(proxy.url(schema_url), [WAIT])
        worker_id = worker.wait_for("worker_started")[1]["worker_id"]
        start.settle(lambda: held_by(db, task, worker_id), "the worker to hold it")
        proxy.refuse()  # no heartbeat can be confirmed from now on
        code, _, lines, _ = worker.exit()
    assert code == ExitCode.FENCED  # never 0
    assert lines[-1]["event"] == "ownership_deadline_passed"
    assert task_states(db, [task]) == [("running", 1)]
