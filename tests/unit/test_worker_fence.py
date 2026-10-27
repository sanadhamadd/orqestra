import json
import subprocess
import sys
import textwrap
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest

from orqestra.worker import fence
from orqestra.worker.fence import ExitCode, Expiry, Guard, Watchdog, end_process, local_deadline

pytestmark = pytest.mark.unit

CONTEXT = {"worker_id": "w", "task_id": "t", "attempt_id": "a"}


class FakeClock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class Recorder:
    """A terminate that records; with ``fired`` set for thread tests."""

    def __init__(self) -> None:
        self.expiries: list[Expiry] = []
        self.fired = threading.Event()

    def __call__(self, expiry: Expiry) -> None:
        self.expiries.append(expiry)
        self.fired.set()


def watchdog(clock: FakeClock | None = None) -> tuple[Watchdog, Recorder]:
    recorder = Recorder()
    return Watchdog(clock=clock or time.monotonic, terminate=recorder), recorder


# --- mapping the lease to the local clock ------------------------------------------


def test_a_database_instant_maps_to_the_local_clock_from_the_claim() -> None:
    started_at = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)
    lease_end = started_at + timedelta(seconds=30)
    # The claim was sent at local 100.0 and recorded by the database later, so the
    # mapped instant (130.0) is never later than the lease really ends.
    assert local_deadline(anchor=100.0, started_at=started_at, instant=lease_end) == 130.0
    assert local_deadline(anchor=100.0, started_at=started_at, instant=started_at) == 100.0


# --- arming, moving and disarming ----------------------------------------------------


def test_nothing_expires_before_its_deadline_and_an_expiry_fires_once() -> None:
    clock = FakeClock()
    dog, recorder = watchdog(clock)
    dog.arm(Guard.OWNERSHIP, 110.0, CONTEXT)
    clock.now = 109.999
    assert dog.check() is None
    clock.now = 110.0
    assert dog.check() == Expiry(Guard.OWNERSHIP, 110.0, 110.0, CONTEXT)
    assert recorder.expiries == [Expiry(Guard.OWNERSHIP, 110.0, 110.0, CONTEXT)]
    assert dog.deadline(Guard.OWNERSHIP) is None  # every guard is disarmed after an expiry
    clock.now = 200.0
    assert dog.check() is None
    assert len(recorder.expiries) == 1


def test_a_guard_cannot_be_armed_twice() -> None:
    dog, _ = watchdog(FakeClock())
    dog.arm(Guard.OPERATION, 1.0, CONTEXT)
    with pytest.raises(RuntimeError, match="already armed"):
        dog.arm(Guard.OPERATION, 2.0, CONTEXT)


def test_extend_only_moves_later_and_restrict_only_earlier() -> None:
    dog, _ = watchdog(FakeClock())
    token = dog.arm(Guard.OWNERSHIP, 110.0, CONTEXT)
    assert dog.extend(token, 105.0)  # never earlier
    assert dog.deadline(Guard.OWNERSHIP) == 110.0
    assert dog.extend(token, 120.0)
    assert dog.deadline(Guard.OWNERSHIP) == 120.0
    assert dog.restrict(token, 130.0)  # never later
    assert dog.deadline(Guard.OWNERSHIP) == 120.0
    assert dog.restrict(token, 115.0)
    assert dog.deadline(Guard.OWNERSHIP) == 115.0


def test_an_obsolete_token_cannot_touch_a_newer_arming() -> None:
    clock = FakeClock()
    dog, recorder = watchdog(clock)
    old = dog.arm(Guard.OWNERSHIP, 110.0, CONTEXT)
    assert dog.disarm(old)
    assert not dog.disarm(old)  # disarming twice changes nothing
    new = dog.arm(Guard.OWNERSHIP, 150.0, CONTEXT)
    assert new != old
    assert not dog.extend(old, 500.0)
    assert not dog.restrict(old, 101.0)
    assert not dog.disarm(old)
    assert dog.deadline(Guard.OWNERSHIP) == 150.0
    clock.now = 120.0  # past the old deadline: nothing fires
    assert dog.check() is None
    clock.now = 150.0
    expiry = dog.check()
    assert expiry is not None
    assert expiry.deadline == 150.0
    assert len(recorder.expiries) == 1


def test_a_disarmed_guard_never_fires() -> None:
    clock = FakeClock()
    dog, recorder = watchdog(clock)
    token = dog.arm(Guard.OWNERSHIP, 110.0, CONTEXT)
    assert dog.disarm(token)
    clock.now = 1_000.0
    assert dog.check() is None
    assert recorder.expiries == []


def test_the_earliest_of_two_guards_fires_and_both_are_disarmed() -> None:
    clock = FakeClock()
    dog, recorder = watchdog(clock)
    dog.arm(Guard.OWNERSHIP, 120.0, CONTEXT)
    dog.arm(Guard.OPERATION, 115.0, {**CONTEXT, "operation": "renew_lease"})
    clock.now = 125.0
    expiry = dog.check()
    assert expiry is not None
    assert (expiry.guard, expiry.deadline) == (Guard.OPERATION, 115.0)
    assert expiry.context["operation"] == "renew_lease"
    assert dog.deadline(Guard.OWNERSHIP) is None
    assert dog.deadline(Guard.OPERATION) is None
    assert len(recorder.expiries) == 1


# --- the thread ------------------------------------------------------------------------------


def test_the_thread_fires_while_another_thread_is_blocked() -> None:
    dog, recorder = watchdog()
    dog.start()
    try:
        blocked = threading.Event()  # stands in for a stalled database call: never set
        deadline = time.monotonic() + 0.2
        dog.arm(Guard.OWNERSHIP, deadline, CONTEXT)
        assert not blocked.wait(0)
        assert recorder.fired.wait(10)
        (expiry,) = recorder.expiries
        assert (expiry.guard, expiry.deadline) == (Guard.OWNERSHIP, deadline)
        assert expiry.now >= deadline
    finally:
        dog.stop(5)


def test_the_thread_honours_extension_and_never_fires_an_obsolete_deadline() -> None:
    dog, recorder = watchdog()
    dog.start()
    try:
        start = time.monotonic()
        first = dog.arm(Guard.OWNERSHIP, start + 0.2, CONTEXT)
        dog.disarm(first)  # the protected execution ended
        second = dog.arm(Guard.OWNERSHIP, start + 0.3, CONTEXT)
        assert dog.extend(second, start + 0.6)  # a confirmed renewal
        assert recorder.fired.wait(10)
        # Only the newest arming, at its extended deadline, ever fired.
        (expiry,) = recorder.expiries
        assert expiry.deadline == start + 0.6
        assert expiry.now >= start + 0.6
    finally:
        dog.stop(5)


def test_start_twice_is_refused_and_stop_joins_the_thread() -> None:
    dog, _ = watchdog()
    dog.start()
    with pytest.raises(RuntimeError, match="already started"):
        dog.start()
    dog.arm(Guard.OWNERSHIP, time.monotonic() + 3_600, CONTEXT)
    dog.stop(5)
    assert dog.deadline(Guard.OWNERSHIP) is None
    assert not any(t.name == "orqestra-watchdog" and t.is_alive() for t in threading.enumerate())


def test_stop_without_start_does_nothing() -> None:
    dog, _ = watchdog()
    dog.stop(1)


# --- ending the process ---------------------------------------------------------------


class Exited(Exception):  # noqa: N818
    pass


def test_end_process_writes_one_json_line_then_exits_with_the_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    written: list[tuple[int, bytes]] = []
    codes: list[int] = []

    class FakeOs:  # only the fence module's view of os is replaced
        @staticmethod
        def write(fd: int, data: bytes) -> int:
            written.append((fd, data))
            return len(data)

        @staticmethod
        def _exit(code: int) -> None:
            codes.append(code)
            raise Exited

    monkeypatch.setattr("orqestra.worker.fence.os", FakeOs)
    with pytest.raises(Exited):
        end_process(ExitCode.AMBIGUOUS, "completion_ambiguous", "lost", CONTEXT)
    assert codes == [6]
    ((fd, data),) = written
    assert fd == 2
    line = json.loads(data)
    assert line["event"] == "completion_ambiguous"
    assert line["exit_code"] == 6
    assert line["level"] == "CRITICAL"
    assert {key: line[key] for key in CONTEXT} == CONTEXT


@pytest.mark.parametrize(
    ("guard", "code", "event"),
    [
        (Guard.OWNERSHIP, ExitCode.FENCED, "ownership_deadline_passed"),
        (Guard.OPERATION, ExitCode.STALLED, "operation_deadline_passed"),
    ],
)
def test_the_default_terminate_ends_the_process_for_each_guard(
    monkeypatch: pytest.MonkeyPatch, guard: Guard, code: ExitCode, event: str
) -> None:
    calls: list[tuple[ExitCode, str]] = []

    def fake_end(code: ExitCode, event: str, message: str, context: object) -> None:
        calls.append((code, event))
        raise Exited

    monkeypatch.setattr(fence, "end_process", fake_end)
    clock = FakeClock()
    dog = Watchdog(clock=clock)  # the default terminate
    dog.arm(guard, 100.0, CONTEXT)
    with pytest.raises(Exited):
        dog.check()
    assert calls == [(code, event)]


@pytest.mark.parametrize(
    "failure",
    [OSError(9, "Bad file descriptor"), BrokenPipeError(32, "Broken pipe")],
    ids=["os-error", "broken-pipe"],
)
def test_end_process_exits_even_when_stderr_cannot_be_written(
    monkeypatch: pytest.MonkeyPatch, failure: OSError
) -> None:
    codes: list[int] = []

    class FakeOs:
        @staticmethod
        def write(fd: int, data: bytes) -> int:
            raise failure

        @staticmethod
        def _exit(code: int) -> None:
            codes.append(code)
            raise Exited

    monkeypatch.setattr("orqestra.worker.fence.os", FakeOs)
    with pytest.raises(Exited):  # the exit happened; the write's error did not escape
        end_process(ExitCode.FENCED, "ownership_deadline_passed", "fenced", CONTEXT)
    assert codes == [3]


# The production watchdog in a real process whose main thread is blocked, as
# in a stalled database call; stderr is made unusable first. Exit code 3 means
# the watchdog ended the process; 0 with "still running" means it did not.
_BLOCKED_PROCESS = textwrap.dedent(
    """
    import os, sys, threading, time
    from orqestra.worker.fence import Guard, Watchdog

    if sys.argv[1] == "closed":
        os.close(2)
    elif sys.argv[1] == "broken-pipe":
        read_end, write_end = os.pipe()
        os.dup2(write_end, 2)
        os.close(read_end)
    watchdog = Watchdog()
    watchdog.start()
    watchdog.arm(Guard.OWNERSHIP, time.monotonic() + 0.2, {"worker_id": "test"})
    threading.Event().wait(10)
    print("still running", flush=True)
    """
)


@pytest.mark.parametrize("stderr", ["writable", "closed", "broken-pipe"])
def test_the_watchdog_ends_a_real_process_whatever_state_stderr_is_in(stderr: str) -> None:
    started = time.monotonic()
    # subprocess.run kills the child if the outer timeout expires.
    result = subprocess.run(
        [sys.executable, "-c", _BLOCKED_PROCESS, stderr],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == ExitCode.FENCED
    assert "still running" not in result.stdout
    assert time.monotonic() - started < 10  # the 10 s block never ran out
    if stderr == "writable":
        line = json.loads(result.stderr.strip().splitlines()[-1])
        assert line["event"] == "ownership_deadline_passed"


def test_a_confirmed_renewal_after_the_old_deadline_but_before_the_check_does_not_expire() -> None:
    # The renewal succeeded, so the lease never lapsed; the watchdog thread just
    # had not checked yet when the deadline moved.
    clock = FakeClock()
    dog, recorder = watchdog(clock)
    token = dog.arm(Guard.OWNERSHIP, 110.0, CONTEXT)
    clock.now = 112.0  # past the old deadline, not yet checked
    assert dog.extend(token, 140.0)
    assert dog.check() is None
    assert recorder.expiries == []
    clock.now = 140.0
    expiry = dog.check()
    assert expiry is not None
    assert expiry.deadline == 140.0  # only the confirmed, later deadline ever fires


def test_exit_codes_are_stable() -> None:
    assert {code.name: int(code) for code in ExitCode} == {
        "FENCED": 3,
        "STALLED": 4,
        "HANDLER_ABORTED": 5,
        "AMBIGUOUS": 6,
        "DEFECT": 7,
        "UNRECORDED": 8,
    }
