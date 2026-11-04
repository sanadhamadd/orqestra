"""How the worker CLI tests observe a process, and which endings they accept.

The observer of ``tests/integration/test_worker_cli.py`` is driven here by
real child processes (``sys.executable -I -c`` with a fixed script): it finds
events across partial writes, never lets a child block on a full pipe, and
reports what it saw without credentials or payloads.

The resumption classifier is checked row by row, and against the real
``Worker`` and ``Watchdog`` with a fake clock: a worker frozen before its
handler started must be refused, one frozen after it started accepted,
whichever guard or call the freeze caught.
"""

import json
import logging
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping
from typing import Any, NoReturn

import pytest

from orqestra.domain.jobs import JsonValue
from orqestra.logging import JsonFormatter
from orqestra.worker import fence, runtime
from orqestra.worker.fence import ExitCode, Expiry, Guard
from orqestra.worker.runtime import Step, next_step
from tests.integration.test_worker_cli import (
    BOUND,
    STALE_DISCARD,
    Observed,
    check_resumed,
    describe,
)
from tests.unit.test_worker_runtime import SETTINGS, Harness, attempt, running_of

pytestmark = pytest.mark.unit

# --- the observer, against real child processes ------------------------------------------------

Spawn = Callable[..., Observed]


@pytest.fixture
def spawn() -> Iterator[Spawn]:
    started: list[Observed] = []

    def launch(script: str, *, stdin: bool = False, bound: float = BOUND) -> Observed:
        child = Observed([sys.executable, "-I", "-c", script], stdin=stdin, bound=bound)
        started.append(child)
        return child

    yield launch
    for child in started:
        child.close()


# Half a JSON line, then (once the test answers on stdin) its other half; then a line the
# child never finishes.
SPLIT = r"""
import os, sys
os.write(2, b"not json\n")
os.write(2, b'{"event": "spl')
sys.stdin.buffer.readline()
os.write(2, b'it", "n": 1}\n')
os.write(2, b'{"event": "tail"}')
"""


def test_an_event_split_across_writes_is_found_and_other_lines_are_not(spawn: Spawn) -> None:
    child = spawn(SPLIT, stdin=True)
    assert child.process.stdin is not None
    child.process.stdin.write(b"\n")
    child.process.stdin.close()
    assert child.wait_for("split") == (1, {"event": "split", "n": 1})
    code, _, records, err = child.exit()
    assert code == 0
    assert records == [{"event": "split", "n": 1}]  # neither the noise nor the unfinished line
    assert err.splitlines() == ["not json", '{"event": "split", "n": 1}', '{"event": "tail"}']
    with pytest.raises(pytest.fail.Exception, match="its output ended"):
        child.wait_for("tail")


LINES = r"""
import os
for n in (1, 2, 1):
    os.write(2, b'{"event": "e", "n": %d}\n' % n)
"""


def test_where_and_since_select_the_line(spawn: Spawn) -> None:
    child = spawn(LINES)
    assert child.wait_for("e") == (0, {"event": "e", "n": 1})
    assert child.wait_for("e", where={"n": 2}) == (1, {"event": "e", "n": 2})
    assert child.wait_for("e", where={"n": 1}, since=1) == (2, {"event": "e", "n": 1})
    assert child.exit()[0] == 0
    with pytest.raises(pytest.fail.Exception, match="its output ended"):
        child.wait_for("e", where={"n": 2}, since=2)


FLOOD = r"""
import os
for _ in range(256):
    os.write(2, b"x" * 1023 + b"\n")
    os.write(1, b"y" * 1023 + b"\n")
"""


def test_a_child_writing_far_more_than_a_pipe_holds_is_never_blocked(spawn: Spawn) -> None:
    child = spawn(FLOOD)
    # Nothing here reads its output: only the observer's readers can let it finish.
    assert child.process.wait(timeout=BOUND) == 0
    code, out, _, err = child.exit()
    assert code == 0
    assert len(err) == len(out) == 256 * 1024


STALLED = r"""
import json, os
def log(**fields):
    os.write(2, (json.dumps(fields) + "\n").encode())
log(event="worker_started", level="INFO", worker_id="w", message="PLANTED-PAYLOAD-1")
os.write(2, b"connecting to postgresql://orq:PLANTED-PASSWORD@db.example/orq\n")
log(
    event="operation_deadline_passed", level="CRITICAL", exit_code=4, operation="renew_lease",
    worker_id="w", task_id="t", attempt_id="a", message="PLANTED-PAYLOAD-2",
    payload={"value": "PLANTED-PAYLOAD-3"},
)
os._exit(4)
"""


def test_a_failed_wait_reports_the_events_and_exit_without_secrets(spawn: Spawn) -> None:
    child = spawn(STALLED)
    with pytest.raises(pytest.fail.Exception, match="its output ended") as failed:
        child.wait_for("ownership_deadline_passed")
    report = str(failed.value)
    assert f"pid {child.pid}, exit status 4" in report
    first = report.index("[0] stderr event=worker_started level=INFO")
    last = report.index(
        "[2] stderr event=operation_deadline_passed level=CRITICAL exit_code=4 "
        "task_id=t attempt_id=a operation=renew_lease"
    )
    assert first < last
    assert "connecting to postgresql://***@db.example/orq" in report
    assert "PLANTED" not in report  # no password, message or payload
    assert child.exit()[0] == 4
    assert "PLANTED" not in describe([child], None, [])


def test_a_silent_live_child_is_reported_running(spawn: Spawn) -> None:
    child = spawn("import sys; sys.stdin.buffer.read()", stdin=True, bound=0.3)
    with pytest.raises(pytest.fail.Exception, match=r"within 0\.3s") as failed:
        child.wait_for("worker_started")
    assert f"pid {child.pid}, running" in str(failed.value)
    assert child.process.stdin is not None
    child.process.stdin.close()
    child.bound = BOUND
    assert child.exit()[0] == 0


# --- the classifier, row by row -----------------------------------------------------------------

WORKER, ATTEMPT = "host:1:0123abcd", "attempt-1"


def ending(event: str, **fields: Any) -> dict[str, Any]:
    return {"event": event, "worker_id": WORKER, "task_id": "task-1", **fields}


def expired(operation: str, **fields: Any) -> dict[str, Any]:
    return ending("operation_deadline_passed", exit_code=4, operation=operation, **fields)


SWEEP = {
    "event": "operation_deadline_passed",
    "exit_code": 4,
    "worker_id": WORKER,
    "operation": "revoke_next_expired",
}


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        pytest.param(
            ending("ownership_deadline_passed", exit_code=3, attempt_id=ATTEMPT),
            ExitCode.FENCED,
            id="fence",
        ),
        pytest.param(
            ending("ownership_lost", exit_code=3, attempt_id=ATTEMPT),
            ExitCode.FENCED,
            id="lost",
        ),
        pytest.param(expired("renew_lease", attempt_id=ATTEMPT), ExitCode.STALLED, id="renew"),
        pytest.param(
            expired("complete_attempt", attempt_id=ATTEMPT), ExitCode.STALLED, id="complete"
        ),
        pytest.param(SWEEP, ExitCode.STALLED, id="sweep"),
        pytest.param(
            ending("outcome_discarded", attempt_id=ATTEMPT, message=STALE_DISCARD),
            None,
            id="discarded",
        ),
    ],
)
def test_the_endings_after_the_handler_started_are_accepted(
    line: dict[str, Any], expected: ExitCode | None
) -> None:
    assert check_resumed(line, worker_id=WORKER, attempt_id=ATTEMPT) == expected


@pytest.mark.parametrize(
    ("line", "reason"),
    [
        pytest.param({**SWEEP, "operation": "claim"}, "'claim': not a call made", id="claim"),
        pytest.param(expired("read_task", attempt_id=ATTEMPT), "'read_task'", id="read_task"),
        pytest.param(expired("read_job", attempt_id=ATTEMPT), "'read_job'", id="read_job"),
        pytest.param(
            expired("read_completion_evidence", attempt_id=ATTEMPT),
            "'read_completion_evidence'",
            id="evidence",
        ),
        pytest.param(
            ending(
                "claimed_task_not_started",
                attempt_id=ATTEMPT,
                message="claimed_task_not_started: lease too short",
            ),
            "'claimed_task_not_started' is not an ending",
            id="not_started",
        ),
        pytest.param(
            ending("outcome_discarded", attempt_id=ATTEMPT, exit_code=3, message=STALE_DISCARD),
            "ended the process",
            id="discarded_exit",
        ),
        pytest.param(
            ending("outcome_discarded", attempt_id=ATTEMPT, message="outcome_discarded: revoked"),
            "another reason",
            id="discarded_revoked",
        ),
        pytest.param(
            {**SWEEP, "attempt_id": ATTEMPT}, "sweep names an attempt", id="sweep_attempt"
        ),
        pytest.param(expired("renew_lease"), "names no attempt", id="renew_no_attempt"),
        pytest.param(
            ending("ownership_deadline_passed", exit_code=3), "names no attempt", id="fence_bare"
        ),
        pytest.param(
            ending("ownership_deadline_passed", exit_code=3, attempt_id="attempt-2"),
            "another attempt",
            id="other_attempt",
        ),
        pytest.param(
            {**ending("ownership_lost", exit_code=3, attempt_id=ATTEMPT), "worker_id": "other"},
            "another worker",
            id="other_worker",
        ),
        pytest.param(
            ending("ownership_lost", exit_code=4, attempt_id=ATTEMPT),
            "another exit code",
            id="lost_exit_4",
        ),
        pytest.param(
            expired("renew_lease", attempt_id=ATTEMPT) | {"exit_code": 3},
            "another exit code",
            id="renew_exit_3",
        ),
        pytest.param(
            ending("attempt_completed", attempt_id=ATTEMPT),
            "'attempt_completed' is not an ending",
            id="completed",
        ),
    ],
)
def test_every_other_ending_is_refused(line: dict[str, Any], reason: str) -> None:
    with pytest.raises(AssertionError, match=reason):
        check_resumed(line, worker_id=WORKER, attempt_id=ATTEMPT)


# --- the classifier, against the real worker and watchdog ---------------------------------------


class Frozen(Exception):  # noqa: N818 - it names a state, not an error kind
    """The process was frozen inside this call; once resumed, the watchdog ends it."""


class FakeTask:
    payload: JsonValue = None
    job_id = uuid.UUID(int=3)


class FakeJob:
    kind = "k"


JUMP = SETTINGS.db_operation_timeout.total_seconds() + 0.1  # just past one call's bound
CLAIMED = str(attempt().id)


@pytest.fixture
def h(monkeypatch: pytest.MonkeyPatch) -> Iterator[Harness]:
    harness = Harness(monkeypatch)
    yield harness
    harness.gate.set()


def claimable(h: Harness) -> None:
    """The next claim returns ``attempt()``, whose task and job can be read."""
    h.monkeypatch.setattr(runtime, "claim_next_attempt", lambda *a, **k: attempt())
    h.monkeypatch.setattr(runtime, "get_task", lambda *a, **k: FakeTask)
    h.monkeypatch.setattr(runtime, "get_job", lambda *a, **k: FakeJob)


def frozen_for(h: Harness, seconds: float) -> Callable[..., NoReturn]:
    """A call during which the process is frozen for ``seconds``, then the watchdog checks."""

    def call(*args: Any, **kwargs: Any) -> NoReturn:
        h.clock.now += seconds
        h.watchdog.check()
        raise Frozen

    return call


def written(h: Harness, expiry: Expiry) -> dict[str, Any]:
    """The JSON line ``end_process`` writes for ``expiry``."""
    lines: list[dict[str, Any]] = []

    def capture(code: ExitCode, event: str, message: str, context: Mapping[str, str]) -> None:
        lines.append({"message": message, "event": event, "exit_code": int(code), **context})

    h.monkeypatch.setattr(fence, "end_process", capture)
    fence._end_for_expiry(expiry)
    (line,) = lines
    return line


def events(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [getattr(r, "event", "") for r in caplog.records if r.name == "orqestra.worker"]


def logged(caplog: pytest.LogCaptureFixture, event: str) -> dict[str, Any]:
    """The JSON line the worker logs for ``event``."""
    (record,) = [r for r in caplog.records if getattr(r, "event", None) == event]
    return dict(json.loads(JsonFormatter().format(record)))


def resumed(line: Mapping[str, Any]) -> ExitCode | None:
    return check_resumed(line, worker_id="worker-1", attempt_id=CLAIMED)


def started(h: Harness) -> None:
    claimable(h)
    h.worker._claim()
    running_of(h.worker)  # its handler runs, under the ownership guard


def test_a_freeze_while_reading_the_claimed_task_is_refused(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    claimable(h)
    h.monkeypatch.setattr(runtime, "get_task", frozen_for(h, JUMP))
    with pytest.raises(Frozen):
        h.worker._claim()
    (expiry,) = h.expiries
    assert expiry.guard is Guard.OPERATION
    assert "handler_started" not in events(caplog)
    line = written(h, expiry)
    assert (line["event"], line["operation"]) == ("operation_deadline_passed", "read_task")
    with pytest.raises(AssertionError, match="'read_task'"):
        resumed(line)


def test_a_freeze_across_the_lease_before_the_handler_starts_is_refused(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    claimable(h)

    def read_job_then_freeze(*args: Any, **kwargs: Any) -> type[FakeJob]:
        # Frozen just as the job was read: resumed past the lease, before the handler starts.
        h.clock.now = 100.0 + 30.0
        return FakeJob

    h.monkeypatch.setattr(runtime, "get_job", read_job_then_freeze)
    h.worker._claim()
    assert h.watchdog.check() is None  # nothing is armed: no handler, no call in flight
    assert h.expiries == []
    assert h.worker._schedule.running is None
    assert "handler_started" not in events(caplog)
    line = logged(caplog, "claimed_task_not_started")
    with pytest.raises(AssertionError, match="'claimed_task_not_started' is not an ending"):
        resumed(line)


def test_a_freeze_during_a_heartbeat_after_the_start_is_accepted(h: Harness) -> None:
    started(h)
    h.clock.now = 105.0  # the first heartbeat is due
    assert next_step(h.worker._timing())[0] is Step.HEARTBEAT
    h.renew(frozen_for(h, JUMP))
    with pytest.raises(Frozen):
        h.worker._heartbeat(running_of(h.worker))
    (expiry,) = h.expiries
    assert expiry.guard is Guard.OPERATION
    line = written(h, expiry)
    assert line["operation"] == "renew_lease"
    assert resumed(line) is ExitCode.STALLED


def test_a_freeze_while_waiting_after_the_start_is_accepted(h: Harness) -> None:
    started(h)
    h.monkeypatch.setattr(runtime, "revoke_next_expired", lambda *a, **k: None)
    h.worker._sweep()  # nothing to recover: the next sweep is an interval away
    assert next_step(h.worker._timing())[0] is Step.WAIT
    h.clock.now = 100.0 + 30.0 - 3.0 + JUMP  # past the fence: the lease, less the margin
    h.watchdog.check()
    (expiry,) = h.expiries
    assert expiry.guard is Guard.OWNERSHIP
    line = written(h, expiry)
    assert line["event"] == "ownership_deadline_passed"
    assert resumed(line) is ExitCode.FENCED


def test_a_freeze_during_a_sweep_after_the_start_is_accepted(h: Harness) -> None:
    started(h)
    # A sweep is due, and the next heartbeat is at least one bound away: it runs now.
    assert next_step(h.worker._timing())[0] is Step.SWEEP
    h.monkeypatch.setattr(runtime, "revoke_next_expired", frozen_for(h, JUMP))
    with pytest.raises(Frozen):
        h.worker._sweep()
    (expiry,) = h.expiries
    assert expiry.guard is Guard.OPERATION
    line = written(h, expiry)
    assert line["operation"] == "revoke_next_expired"
    assert "attempt_id" not in line  # a sweep names no attempt
    assert resumed(line) is ExitCode.STALLED
