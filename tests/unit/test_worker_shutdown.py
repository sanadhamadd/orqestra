"""Graceful shutdown (DEC-049): the latch, draining, the drain guard and the signal helper.

The worker runs here with a fake clock, a fake connection and scripted
database calls. Where the loop itself must run, it runs on a thread, and the
test synchronises with it through events and the watchdog's armed deadlines,
never through sleeps; waits are bounded so a regression fails instead of
hanging.
"""

import logging
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import timedelta
from pathlib import Path
from types import FrameType
from typing import Any

import psycopg
import pytest
from psycopg import errors

from orqestra.domain.jobs import AttemptSucceeded, JsonValue
from orqestra.persistence.lifecycle import CompletionEvidence
from orqestra.worker import runtime, shutdown
from orqestra.worker.fence import ExitCode, Expiry, Guard, Watchdog
from orqestra.worker.handlers import HandlerRegistry
from orqestra.worker.runtime import UncontrolledExitError, Worker
from orqestra.worker.shutdown import run_until_signalled, stop_on_signal
from tests.unit.test_worker_runtime import (
    SETTINGS,
    FakeConn,
    Harness,
    Terminated,
    attempt,
    completed,
    raising,
    terminate,
)

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
BOUND = 10.0  # the bound on every wait for another thread; never reached when correct


@pytest.fixture
def h(monkeypatch: pytest.MonkeyPatch) -> Iterator[Harness]:
    harness = Harness(monkeypatch)
    # Nothing to recover unless a test says so.
    monkeypatch.setattr(runtime, "revoke_next_expired", lambda conn: None)
    yield harness
    harness.gate.set()


class Task:
    payload: JsonValue = None
    job_id = uuid.UUID(int=3)


class Job:
    kind = "k"


def claims(h: Harness, *results: Callable[[], Any]) -> list[int]:
    """Script the claims: each one calls the next of ``results``; returns the call count."""
    calls: list[int] = []

    def claim(*args: Any, **kwargs: Any) -> Any:
        calls.append(1)
        assert len(calls) <= len(results), "claimed after the worker was stopped"
        return results[len(calls) - 1]()

    h.monkeypatch.setattr(runtime, "claim_next_attempt", claim)
    h.monkeypatch.setattr(runtime, "get_task", lambda *a, **k: Task)
    h.monkeypatch.setattr(runtime, "get_job", lambda *a, **k: Job)
    return calls


def with_settings(h: Harness, **changes: Any) -> None:
    h.worker._settings = SETTINGS.model_copy(update=changes)


def events(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [getattr(r, "event", "") for r in caplog.records if r.name == "orqestra.worker"]


def wait_until(check: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + BOUND
    while not check():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.005)


class Runner:
    """``worker.run()`` on a thread; ``result`` is what it raised, if anything."""

    def __init__(self, worker: Worker) -> None:
        self.result: BaseException | None = None
        self.thread = threading.Thread(target=self._run, args=(worker,), daemon=True)
        self.thread.start()

    def _run(self, worker: Worker) -> None:
        try:
            worker.run()
        except BaseException as exc:  # reported to the test
            self.result = exc

    def join(self) -> None:
        self.thread.join(BOUND)
        assert not self.thread.is_alive(), "run() did not return"


# --- the latch and stopping while idle -------------------------------------------------


def test_a_stop_requested_before_run_claims_nothing(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    calls = claims(h)  # any claim fails the test
    h.worker.request_stop()
    h.worker.run()
    assert calls == []
    assert h.worker.finished
    assert events(caplog) == ["worker_started", "shutdown_requested", "worker_stopped"]


def test_the_latch_is_permanent(h: Harness) -> None:
    assert not h.worker._observe_stop()
    h.worker.request_stop()
    assert h.worker._observe_stop()
    h.worker._stop_requested = False  # nothing can take a latched stop back
    assert h.worker._observe_stop()
    assert h.worker._timing().stopping


def test_a_stop_ends_an_idle_wait_at_once(h: Harness) -> None:
    with_settings(h, poll_interval=timedelta(minutes=10))  # an idle wait of 10 minutes
    claimed = threading.Event()
    claims(h, lambda: claimed.set())  # returns None: nothing to claim
    runner = Runner(h.worker)
    assert claimed.wait(BOUND)
    started = time.monotonic()
    h.worker.request_stop()
    runner.join()
    assert time.monotonic() - started < 2.0
    assert runner.result is None
    assert h.worker.finished


# --- draining a running task ------------------------------------------------------------


def test_a_busy_worker_latches_at_once_drains_and_then_stops(h: Harness) -> None:
    claims(h, attempt)
    complete = completed(h)
    h.monkeypatch.setattr(runtime, "complete_attempt", complete)
    renewals: list[float] = []

    def renew(*args: Any, **kwargs: Any) -> Any:
        renewals.append(h.clock.now)
        return attempt(lease_seconds=36.0)

    h.renew(renew)
    runner = Runner(h.worker)
    wait_until(lambda: len(h.contexts) == 1, "the handler")
    # The supervisor now waits up to 5 s for the heartbeat; a stop ends that wait.
    started = time.monotonic()
    h.worker.request_stop()
    wait_until(lambda: h.watchdog.deadline(Guard.DRAIN) is not None, "the drain guard")
    assert time.monotonic() - started < 2.0
    assert h.watchdog.deadline(Guard.DRAIN) == 100.0 + 30.0  # the latch + shutdown_timeout
    # Heartbeats go on while draining, and shutdown never tells the handler to cancel.
    h.clock.now = 105.0
    h.worker._wake.notify()
    wait_until(lambda: renewals == [105.0], "a heartbeat while draining")
    assert not h.contexts[0].cancellation.is_set()
    # The handler ends: the supervisor records its outcome and stops.
    started = time.monotonic()
    h.gate.set()
    runner.join()
    assert time.monotonic() - started < 2.0  # woken by the handler, not a timer
    assert runner.result is None
    assert complete.calls == [AttemptSucceeded({"ok": True})]  # type: ignore[attr-defined]
    assert h.worker.finished
    assert h.watchdog.deadline(Guard.DRAIN) is None


def test_the_drain_guard_ends_the_process_whatever_the_supervisor_does(h: Harness) -> None:
    with_settings(h, shutdown_timeout=timedelta(seconds=10))  # before the fence at 127.0
    running = h.start()
    h.worker.request_stop()
    assert h.worker._observe_stop()
    assert h.watchdog.deadline(Guard.DRAIN) == 110.0
    h.clock.now = 109.999
    assert h.watchdog.check() is None
    h.clock.now = 110.0  # the supervisor is not involved: say it is blocked in a call
    assert h.watchdog.check() == Expiry(Guard.DRAIN, 110.0, 110.0, running.context)
    assert not running.cancel.is_set()


def test_settling_while_draining_disarms_the_drain_guard_at_once(h: Harness) -> None:
    running = h.start()
    h.worker.request_stop()
    h.worker._observe_stop()
    h.finish_handler(running)
    h.monkeypatch.setattr(runtime, "complete_attempt", completed(h))
    h.worker._complete(running)
    # Settled: the stop may now exit 0, so the guard must not fire on the way out.
    assert h.watchdog.deadline(Guard.DRAIN) is None
    h.clock.now = 200.0
    assert h.watchdog.check() is None
    assert h.worker._timing().drain_due is None


def test_a_drain_deadline_that_passes_abandons_the_task(h: Harness) -> None:
    with_settings(h, shutdown_timeout=timedelta(seconds=10))
    h.start()
    h.worker.request_stop()
    h.worker._observe_stop()
    h.clock.now = 110.0
    with pytest.raises(Terminated) as info:
        h.worker.run()
    # The handler still runs, so the supervisor's own end then fences it.
    first = info.value.__context__
    assert isinstance(first, Terminated)
    assert (first.code, first.event) == (ExitCode.SHUTDOWN, "drain_deadline_passed")


def test_an_unrecorded_outcome_is_abandoned_at_the_drain_deadline(h: Harness) -> None:
    running = h.start()
    h.finish_handler(running)
    h.monkeypatch.setattr(runtime, "complete_attempt", raising(errors.DeadlockDetected()))
    h.worker.request_stop()
    h.worker._observe_stop()
    h.worker._complete(running)  # retried later, within the attempt's deadline
    h.clock.now = 130.0  # the drain deadline comes first
    with pytest.raises(Terminated) as info:
        h.worker.run()
    assert (info.value.code, info.value.event) == (ExitCode.SHUTDOWN, "drain_deadline_passed")


@pytest.mark.parametrize("busy", [False, True])
def test_a_zero_shutdown_timeout(h: Harness, busy: bool) -> None:
    with_settings(h, shutdown_timeout=timedelta(0))
    claims(h)
    if busy:
        h.start()
    h.worker.request_stop()
    if not busy:
        h.worker.run()  # nothing held: a clean stop
        assert h.worker.finished
        return
    with pytest.raises(Terminated) as info:
        h.worker.run()
    first = info.value.__context__
    assert isinstance(first, Terminated)
    assert (first.code, first.event) == (ExitCode.SHUTDOWN, "drain_deadline_passed")


# --- the stop/start race: the latch is the linearization point ------------------------------


def test_a_stop_during_a_claim_leaves_the_claimed_task_unstarted(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    def claim_then_stop() -> Any:
        h.worker.request_stop()  # arrives while the claim is in flight
        return attempt()

    calls = claims(h, claim_then_stop)
    h.worker.run()
    assert calls == [1]
    assert h.contexts == []  # the handler never started
    assert h.watchdog.deadline(Guard.OWNERSHIP) is None
    assert h.worker.finished
    assert "claimed_task_not_started" in events(caplog)
    (warning,) = [
        r for r in caplog.records if getattr(r, "event", "") == "stopped_with_unstarted_claim"
    ]
    assert warning.levelno == logging.WARNING
    assert (getattr(warning, "attempt_id", None), getattr(warning, "task_id", None)) == (
        str(uuid.UUID(int=1)),
        str(uuid.UUID(int=2)),
    )
    assert "lease_expires_at=2026-10-26T12:00:30+00:00" in warning.getMessage()
    assert "using one attempt" in warning.getMessage()


def test_a_stop_while_the_claimed_task_is_read_leaves_it_unstarted(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    claims(h, attempt)

    def read_job_then_stop(*args: Any, **kwargs: Any) -> Any:
        h.worker.request_stop()
        return Job

    h.monkeypatch.setattr(runtime, "get_job", read_job_then_stop)
    h.worker.run()
    assert h.contexts == []
    assert "stopped_with_unstarted_claim" in events(caplog)


CLAIM_CALLS = ["claim", "read_task", "read_job"]


@pytest.mark.parametrize("during", CLAIM_CALLS)
def test_a_stop_during_any_call_of_a_claim_is_latched_before_the_next_call(
    h: Harness, caplog: pytest.LogCaptureFixture, during: str
) -> None:
    caplog.set_level(logging.INFO)
    claims(h, attempt)
    calls: list[tuple[str, bool]] = []  # each supervisor call, and whether a stop was latched
    call = h.worker._call

    def recorded(operation: str, fn: Callable[[Any], Any], context: Any) -> Any:
        calls.append((operation, h.worker._stopping_at is not None))
        if operation == during:
            h.worker.request_stop()  # stored while this call is in flight
        return call(operation, fn, context)

    h.monkeypatch.setattr(h.worker, "_call", recorded)
    h.worker.run()
    # The call in flight finishes; no other database call follows it (DEC-049).
    expected = ["revoke_next_expired", *CLAIM_CALLS[: CLAIM_CALLS.index(during) + 1]]
    assert calls == [(operation, False) for operation in expected]
    assert h.contexts == []  # never started
    held = h.worker._held
    assert held is not None
    assert held.attempt is not None
    assert (held.attempt.id, held.until) == (uuid.UUID(int=1), 130.0)  # recovered after it
    assert events(caplog).count("claimed_task_not_started") == 1
    assert "stopped_with_unstarted_claim" in events(caplog)


def test_a_stop_during_the_evidence_read_is_latched_before_the_rewrite(h: Harness) -> None:
    running = h.start()
    h.finish_handler(running)
    running.uncertain = True  # an earlier answer was lost

    def evidence(*args: Any, **kwargs: Any) -> CompletionEvidence:
        h.worker.request_stop()  # stored while the read is in flight
        return CompletionEvidence.RUNNING

    seen: list[tuple[bool, float | None]] = []
    complete = completed(h)

    def rewrite(*args: Any, **kwargs: Any) -> Any:
        seen.append((h.worker._stopping_at is not None, h.watchdog.deadline(Guard.DRAIN)))
        return complete(*args, **kwargs)

    h.monkeypatch.setattr(runtime, "read_completion_evidence", evidence)
    h.monkeypatch.setattr(runtime, "complete_attempt", rewrite)
    h.worker._complete(running)
    assert seen == [(True, 130.0)]  # latched, and draining, before the second call
    assert h.worker._schedule.running is None  # then settled
    assert h.watchdog.deadline(Guard.DRAIN) is None


def test_a_stop_after_the_last_check_finds_the_handler_running_and_drains_it(
    h: Harness,
) -> None:
    calls = claims(h, attempt)
    complete = completed(h)
    h.monkeypatch.setattr(runtime, "complete_attempt", complete)
    arm = h.watchdog.arm

    def arm_after_a_stop(guard: Guard, deadline: float, context: Any) -> int:
        if guard is Guard.OWNERSHIP:
            # After the pre-start check, before thread.start(): the request is
            # stored, but the supervisor has not latched it yet.
            h.worker.request_stop()
        return arm(guard, deadline, context)

    h.monkeypatch.setattr(h.watchdog, "arm", arm_after_a_stop)
    runner = Runner(h.worker)
    wait_until(lambda: h.watchdog.deadline(Guard.DRAIN) is not None, "the drain guard")
    assert len(h.contexts) == 1  # it started; the latch came after it, so it is drained
    assert h.watchdog.deadline(Guard.DRAIN) == 130.0
    h.gate.set()
    runner.join()
    assert runner.result is None
    assert calls == [1]  # and nothing is claimed after it
    assert complete.calls == [AttemptSucceeded({"ok": True})]  # type: ignore[attr-defined]


def test_a_lost_claim_answer_then_a_stop_is_reported(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    def lost(*args: Any, **kwargs: Any) -> Any:
        h.conn.closed = True
        raise psycopg.OperationalError("lost")

    h.monkeypatch.setattr(runtime, "claim_next_attempt", lost)
    h.worker._claim()
    h.worker.request_stop()
    h.worker.run()
    assert "stopped_with_uncertain_claim" in events(caplog)


def test_a_claim_whose_lease_has_certainly_ended_is_not_reported(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    h.monkeypatch.setattr(runtime, "claim_next_attempt", lambda *a, **k: attempt())
    h.monkeypatch.setattr(runtime, "get_task", raising(psycopg.OperationalError("lost")))
    h.worker._claim()  # held until 130.0
    h.clock.now = 130.0
    h.worker.request_stop()
    h.worker.run()
    assert "stopped_with_unstarted_claim" not in events(caplog)


# --- no busy wait ----------------------------------------------------------------------------


def test_waiting_to_retry_a_completion_does_not_spin(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = Worker(
        registry=HandlerRegistry({"k": lambda ctx, payload: {"ok": True}}),
        settings=SETTINGS,
        connect=lambda: FakeConn(),  # type: ignore[arg-type,return-value]
        worker_id="worker-1",
        watchdog=Watchdog(terminate=lambda expiry: None),
        terminate=terminate,
    )
    worker._start(attempt(), time.monotonic(), {"worker_id": "worker-1"}, {}, "k")
    running = worker._schedule.running
    assert running is not None
    assert running.done.wait(BOUND)
    running.completion_due = time.monotonic() + 0.5  # as after a retryable failure
    monkeypatch.setattr(runtime, "complete_attempt", completed(None))  # type: ignore[arg-type]
    steps = 0
    timing = worker._timing

    def counted() -> runtime.Timing:
        nonlocal steps
        steps += 1
        return timing()

    monkeypatch.setattr(worker, "_timing", counted)
    worker.request_stop()
    started = time.monotonic()
    worker.run()
    assert time.monotonic() - started >= 0.45
    assert steps <= 5  # it waited; before DEC-049 this was hundreds of thousands


# --- never a false success ---------------------------------------------------------------------


@pytest.mark.parametrize("code", [0, None, False])
def test_a_success_exit_while_idle_is_never_a_controlled_stop(h: Harness, code: Any) -> None:
    h.monkeypatch.setattr(runtime, "revoke_next_expired", raising(SystemExit(code)))
    with pytest.raises(UncontrolledExitError) as info:
        h.worker.run()
    assert isinstance(info.value.__cause__, SystemExit)
    assert not h.worker.finished


def test_a_failure_exit_while_idle_passes_through(h: Harness) -> None:
    h.monkeypatch.setattr(runtime, "revoke_next_expired", raising(SystemExit(3)))
    with pytest.raises(SystemExit) as info:
        h.worker.run()
    assert info.value.code == 3


def test_a_success_exit_while_the_handler_runs_fences_the_process(h: Harness) -> None:
    h.start()
    h.clock.now = 106.0  # the heartbeat is due
    h.renew(raising(SystemExit(0)))
    with pytest.raises(Terminated) as info:
        h.worker.run()
    assert (info.value.code, info.value.event) == (ExitCode.FENCED, "supervisor_failed")


@pytest.mark.parametrize("stage", ["completing", "reconciling"])
@pytest.mark.parametrize("exc", [SystemExit(0), KeyboardInterrupt(), KeyError("bug")])
def test_an_exception_before_the_outcome_is_settled_is_never_a_success(
    h: Harness, stage: str, exc: BaseException
) -> None:
    running = h.start()
    h.finish_handler(running)
    if stage == "reconciling":
        running.uncertain = True
        h.monkeypatch.setattr(runtime, "read_completion_evidence", raising(exc))
    else:
        h.monkeypatch.setattr(runtime, "complete_attempt", raising(exc))
    expected = UncontrolledExitError if isinstance(exc, SystemExit) else type(exc)
    with pytest.raises(expected):
        h.worker.run()
    assert not h.worker.finished
    assert h.worker._schedule.running is running  # unsettled: left to recovery


_FALSE_SUCCESS = textwrap.dedent(
    """
    import sys
    from orqestra.worker import runtime
    from orqestra.worker.shutdown import run_until_signalled
    from tests.unit.test_worker_runtime import SETTINGS, FakeConn
    from orqestra.worker.handlers import HandlerRegistry

    def exits(conn):
        sys.exit(0)

    runtime.revoke_next_expired = exits
    worker = runtime.Worker(registry=HandlerRegistry({"k": lambda c, p: None}),
                            settings=SETTINGS, connect=FakeConn)
    run_until_signalled(worker)
    """
)


def test_a_process_whose_supervisor_calls_sys_exit_0_exits_nonzero() -> None:
    result = subprocess.run(  # a fixed test program
        [sys.executable, "-c", _FALSE_SUCCESS],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 1
    assert "UncontrolledExitError" in result.stderr


# --- the signal handler -------------------------------------------------------------------------


class Forced:
    def __init__(self) -> None:
        self.calls: list[tuple[ExitCode, str]] = []

    def __call__(self, code: ExitCode, event: str, message: str, context: Any) -> None:
        self.calls.append((code, event))


def test_the_first_signal_requests_a_stop_and_a_later_one_forces_the_exit(h: Harness) -> None:
    forced = Forced()
    handle = stop_on_signal(h.worker, forced)  # type: ignore[arg-type]
    handle(signal.SIGTERM, None)
    assert h.worker._stop_requested
    assert forced.calls == []
    handle(signal.SIGINT, None)
    assert forced.calls == [(ExitCode.SHUTDOWN, "shutdown_forced")]


def test_a_signal_nested_in_the_handler_still_counts_as_the_second(h: Harness) -> None:
    forced = Forced()
    handle = stop_on_signal(h.worker, forced)  # type: ignore[arg-type]
    request = h.worker.request_stop

    def request_then_nested_signal() -> None:
        handle(signal.SIGTERM, None)  # Python runs a handler between any two bytecodes
        request()

    h.monkeypatch.setattr(h.worker, "request_stop", request_then_nested_signal)
    handle(signal.SIGTERM, None)
    assert forced.calls == [(ExitCode.SHUTDOWN, "shutdown_forced")]
    assert h.worker._stop_requested


def test_a_signal_after_a_controlled_stop_forces_nothing(h: Harness) -> None:
    forced = Forced()
    handle = stop_on_signal(h.worker, forced)  # type: ignore[arg-type]
    handle(signal.SIGTERM, None)
    h.worker.run()
    handle(signal.SIGTERM, None)
    assert forced.calls == []


def test_the_handler_takes_no_lock_the_interrupted_thread_may_hold(h: Harness) -> None:
    # The main thread may be inside the watchdog, logging or the wake pipe when a
    # signal arrives. Holding all of these, the handler must still return.
    handle = stop_on_signal(h.worker, Forced())  # type: ignore[arg-type]
    while True:  # a full pipe: a wake is already pending
        try:
            os.write(h.worker.wakeup_fd, b"\0" * 4096)
        except BlockingIOError:
            break
    returned = threading.Event()

    def interrupt() -> None:
        handle(signal.SIGTERM, None)
        returned.set()

    with h.watchdog._condition, logging._lock:  # type: ignore[attr-defined]
        thread = threading.Thread(target=interrupt, daemon=True)
        thread.start()
        assert returned.wait(BOUND)
    assert h.worker._stop_requested


# --- installing and restoring handlers -----------------------------------------------------------


class Recorded:
    """A previous SIGTERM handler, so no stray signal can end the test process."""

    def __init__(self) -> None:
        self.signals: list[int] = []

    def __call__(self, signum: int, frame: FrameType | None) -> None:
        self.signals.append(signum)


@pytest.fixture
def previous() -> Iterator[Recorded]:
    recorded = Recorded()
    saved = {sig: signal.getsignal(sig) for sig in shutdown.SIGNALS}
    signal.signal(signal.SIGTERM, recorded)
    try:
        yield recorded
    finally:
        for sig, handler in saved.items():
            signal.signal(sig, handler)
        signal.set_wakeup_fd(-1)


def wakeup_fd() -> int:
    """The process's wakeup fd (reading it means setting it, so it is put back)."""
    fd = signal.set_wakeup_fd(-1)
    signal.set_wakeup_fd(fd)
    return fd


def test_handlers_are_installed_for_the_run_and_restored_after(
    h: Harness, previous: Recorded
) -> None:
    seen: dict[str, Any] = {}
    run = h.worker.run

    def observed_run() -> None:
        seen["term"] = signal.getsignal(signal.SIGTERM)
        seen["int"] = signal.getsignal(signal.SIGINT)
        seen["fd"] = wakeup_fd()
        run()

    h.monkeypatch.setattr(h.worker, "run", observed_run)
    sigint = signal.getsignal(signal.SIGINT)
    h.worker.request_stop()
    run_until_signalled(h.worker)
    assert seen["term"] is seen["int"]
    assert seen["term"] not in {previous, sigint}
    assert seen["fd"] == h.worker.wakeup_fd
    assert signal.getsignal(signal.SIGTERM) is previous
    assert signal.getsignal(signal.SIGINT) is sigint
    assert wakeup_fd() == -1


def test_a_signal_after_the_helper_returned_never_reaches_the_worker(
    h: Harness, previous: Recorded
) -> None:
    h.worker.request_stop()
    run_until_signalled(h.worker)
    h.worker._wake.drain()
    os.kill(os.getpid(), signal.SIGTERM)
    time.sleep(0)  # a bytecode boundary: the handler runs here
    assert previous.signals == [signal.SIGTERM]
    with pytest.raises(BlockingIOError):  # nothing was written to the worker's pipe
        os.read(h.worker._wake.read_fd, 1)


def test_a_signal_delivered_to_another_thread_still_wakes_the_supervisor(
    h: Harness, previous: Recorded
) -> None:
    with_settings(h, poll_interval=timedelta(seconds=30))
    claimed = threading.Event()
    claims(h, lambda: claimed.set())

    def signal_from_another_thread() -> None:
        assert claimed.wait(BOUND)  # the handlers are installed by now
        # Delivered to this thread, it cannot interrupt the main thread's wait;
        # only its byte in the wakeup fd can end that wait.
        signal.pthread_kill(threading.get_ident(), signal.SIGTERM)

    sender = threading.Thread(target=signal_from_another_thread)
    sender.start()
    started = time.monotonic()
    run_until_signalled(h.worker)
    sender.join(BOUND)
    assert time.monotonic() - started < 5.0  # not the 30-second idle wait
    assert h.worker.finished
    assert previous.signals == []  # our handler took it


def test_installation_off_the_main_thread_is_refused_and_changes_nothing(
    h: Harness, previous: Recorded
) -> None:
    raised: list[BaseException] = []

    def install() -> None:
        try:
            run_until_signalled(h.worker)
        except RuntimeError as exc:
            raised.append(exc)

    thread = threading.Thread(target=install)
    thread.start()
    thread.join(BOUND)
    assert len(raised) == 1
    assert signal.getsignal(signal.SIGTERM) is previous
    assert wakeup_fd() == -1


def test_a_handler_installed_outside_python_is_refused(h: Harness, previous: Recorded) -> None:
    getsignal = signal.getsignal
    h.monkeypatch.setattr(
        "orqestra.worker.shutdown.signal.getsignal",
        lambda sig: None if sig == signal.SIGINT else getsignal(sig),
    )
    with pytest.raises(RuntimeError, match="cannot be restored"):
        run_until_signalled(h.worker)
    assert getsignal(signal.SIGTERM) is previous
    assert wakeup_fd() == -1


def test_another_wakeup_fd_is_refused_and_left_in_place(h: Harness, previous: Recorded) -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    try:
        signal.set_wakeup_fd(write_fd)
        with pytest.raises(RuntimeError, match="another wakeup fd"):
            run_until_signalled(h.worker)
        assert wakeup_fd() == write_fd
        assert signal.getsignal(signal.SIGTERM) is previous
    finally:
        signal.set_wakeup_fd(-1)
        os.close(read_fd)
        os.close(write_fd)


def test_a_partial_installation_is_rolled_back(h: Harness, previous: Recorded) -> None:
    install = signal.signal
    sigint = signal.getsignal(signal.SIGINT)

    def failing(sig: int, handler: Any) -> Any:
        if sig == signal.SIGINT and handler not in {sigint}:
            raise OSError("refused")
        return install(sig, handler)

    h.monkeypatch.setattr("orqestra.worker.shutdown.signal.signal", failing)
    with pytest.raises(OSError, match="refused"):
        run_until_signalled(h.worker)
    assert signal.getsignal(signal.SIGTERM) is previous  # installed, then put back
    assert signal.getsignal(signal.SIGINT) is sigint
    assert wakeup_fd() == -1


def test_an_exception_from_run_restores_everything_and_propagates(
    h: Harness, previous: Recorded
) -> None:
    h.monkeypatch.setattr(runtime, "revoke_next_expired", raising(KeyError("bug")))
    with pytest.raises(KeyError):
        run_until_signalled(h.worker)
    assert signal.getsignal(signal.SIGTERM) is previous
    assert wakeup_fd() == -1
