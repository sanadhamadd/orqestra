import re
import threading
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn

import psycopg
import pytest
from psycopg import errors
from psycopg.conninfo import conninfo_to_dict

from orqestra.domain.errors import TransientError
from orqestra.domain.jobs import Attempt, AttemptFailed, AttemptSucceeded, JsonValue
from orqestra.domain.leases import RevocationReason
from orqestra.domain.policy import FailureKind
from orqestra.domain.states import AttemptState
from orqestra.persistence.lifecycle import (
    AttemptFinishedError,
    AttemptRevokedError,
    CancellationRequestedError,
    LeaseExpiredError,
    NotFoundError,
    StaleAttemptError,
)
from orqestra.worker import runtime
from orqestra.worker.fence import ExitCode, Expiry, Guard, Watchdog
from orqestra.worker.handlers import ControlPlaneSignal, HandlerRegistry, TaskContext
from orqestra.worker.runtime import (
    CompletionError,
    ConnectionUnavailable,
    Step,
    Timing,
    Worker,
    classify_completion_error,
    new_worker_id,
    next_step,
    supervisor_conninfo,
)
from orqestra.worker.settings import WorkerSettings

pytestmark = pytest.mark.unit

# Test values only (DEC-034): margin 3 s, database bound 2 s, heartbeat every 5 s, lease 30 s.
SETTINGS = WorkerSettings(
    _env_file=None,
    lease=timedelta(seconds=30),
    execution_timeout=timedelta(minutes=10),
    heartbeat_interval=timedelta(seconds=5),
    db_operation_timeout=timedelta(seconds=2),
    fence_margin=timedelta(seconds=3),
    poll_interval=timedelta(seconds=1),
    recovery_interval=timedelta(seconds=10),
    recovery_batch_limit=3,
    shutdown_timeout=timedelta(seconds=30),
)
T0 = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)


# --- scheduling ------------------------------------------------------------------------


def timing(**changes: Any) -> Timing:
    base: dict[str, Any] = {
        "now": 100.0,
        "running": False,
        "completing": False,
        "stop_requested": False,
        "heartbeat_due": None,
        "completion_due": 0.0,
        "sweep_due": 200.0,
        "claim_due": 200.0,
        "sweep_reserve": 2.0,
    }
    return Timing(**{**base, **changes})


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        # idle
        ({}, (Step.WAIT, 100.0)),
        ({"claim_due": 100.0}, (Step.CLAIM, 0.0)),
        ({"claim_due": 150.0}, (Step.WAIT, 50.0)),
        ({"sweep_due": 100.0, "claim_due": 100.0}, (Step.SWEEP, 0.0)),  # a sweep comes first
        ({"stop_requested": True, "claim_due": 0.0, "sweep_due": 0.0}, (Step.STOP, 0.0)),
        # running: never a claim, never a stop
        (
            {"running": True, "heartbeat_due": 105.0, "claim_due": 0.0, "stop_requested": True},
            (Step.WAIT, 5.0),
        ),
        ({"running": True, "heartbeat_due": 100.0, "sweep_due": 0.0}, (Step.HEARTBEAT, 0.0)),
        ({"running": True, "heartbeat_due": 102.0, "sweep_due": 100.0}, (Step.SWEEP, 0.0)),
        # a sweep that might not end before the heartbeat waits for it
        ({"running": True, "heartbeat_due": 101.9, "sweep_due": 100.0}, (Step.WAIT, 1.9)),
        ({"running": True, "heartbeat_due": 130.0, "sweep_due": 110.0}, (Step.WAIT, 10.0)),
        # no more renewals (after a cancellation request): sweeps are never held back
        ({"running": True, "heartbeat_due": None, "sweep_due": 100.0}, (Step.SWEEP, 0.0)),
        ({"running": True, "heartbeat_due": None, "sweep_due": 140.0}, (Step.WAIT, 40.0)),
        # completing: the outcome first, retries spaced
        ({"completing": True, "completion_due": 100.0, "sweep_due": 0.0}, (Step.COMPLETE, 0.0)),
        ({"completing": True, "completion_due": 101.5}, (Step.WAIT, 1.5)),
    ],
)
def test_next_step(changes: dict[str, Any], expected: tuple[Step, float]) -> None:
    step, seconds = next_step(timing(**changes))
    assert (step, seconds) == pytest.approx(expected)


def test_sweeps_never_delay_a_heartbeat_and_always_progress() -> None:
    # A busy worker, an endless backlog of expired attempts, and every database
    # call taking its full bound: each heartbeat still runs exactly when due,
    # and at least one revocation happens between consecutive heartbeats.
    interval, reserve = 5.0, 2.0
    now, heartbeat_due, sweep_due = 0.0, 5.0, 0.0
    lateness: list[float] = []
    sweeps_between: list[int] = []
    sweeps = 0
    while now < 1_000.0:
        step, seconds = next_step(
            timing(
                now=now,
                running=True,
                heartbeat_due=heartbeat_due,
                sweep_due=sweep_due,
                sweep_reserve=reserve,
            )
        )
        if step is Step.HEARTBEAT:
            lateness.append(now - heartbeat_due)
            sweeps_between.append(sweeps)
            sweeps = 0
            heartbeat_due = now + interval
            now += reserve
        elif step is Step.SWEEP:
            sweeps += 1
            now += reserve
            sweep_due = now  # the backlog never empties
        else:
            assert step is Step.WAIT
            assert 0.0 < seconds < float("inf")
            now += seconds
    assert len(lateness) > 100
    assert max(lateness) == 0.0
    assert min(sweeps_between[1:]) >= 1


# --- errors and connection settings ---------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "closed", "expected"),
    [
        (ConnectionUnavailable("refused"), True, CompletionError.RETRY),
        (psycopg.OperationalError("server closed the connection"), True, CompletionError.AMBIGUOUS),
        (psycopg.InterfaceError("the connection is closed"), False, CompletionError.AMBIGUOUS),
        (
            errors.DeadlockDetected(),
            True,
            CompletionError.AMBIGUOUS,
        ),  # an answer, but no connection
        (errors.DeadlockDetected(), False, CompletionError.RETRY),
        (errors.SerializationFailure(), False, CompletionError.RETRY),
        (errors.LockNotAvailable(), False, CompletionError.RETRY),
        (errors.QueryCanceled(), False, CompletionError.RETRY),
        (errors.CheckViolation(), False, CompletionError.REJECTED),
        (errors.InvalidTextRepresentation(), False, CompletionError.REJECTED),
        (errors.DataError(), False, CompletionError.AMBIGUOUS),  # no SQLSTATE: no answer
    ],
)
def test_completion_errors_are_classified_without_blaming_the_handler(
    exc: psycopg.Error | ConnectionUnavailable, closed: bool, expected: CompletionError
) -> None:
    assert classify_completion_error(exc, connection_closed=closed) is expected


def settings_with(db_timeout: timedelta) -> WorkerSettings:
    return WorkerSettings(
        _env_file=None,
        **{**SETTINGS.model_dump(), "db_operation_timeout": db_timeout},
    )


def test_the_supervisor_connection_keeps_options_and_adds_its_bounds() -> None:
    url = "postgresql://localhost/orqestra_test?options=-c%20search_path%3Dmy_schema"
    params = conninfo_to_dict(supervisor_conninfo(url, SETTINGS))
    assert params["options"] == "-c search_path=my_schema -c statement_timeout=2000"
    assert params["connect_timeout"] == "2"
    assert params["application_name"] == "orqestra-worker"
    assert params["dbname"] == "orqestra_test"


@pytest.mark.parametrize(
    ("db_timeout", "statement", "connect"),
    [
        (timedelta(microseconds=1_500), "2", "2"),  # rounded up to whole milliseconds
        (timedelta(milliseconds=500), "500", "2"),  # libpq's minimum is 2 s
        (timedelta(seconds=2.5), "2500", "3"),
    ],
)
def test_bounds_are_rounded_up(db_timeout: timedelta, statement: str, connect: str) -> None:
    settings = WorkerSettings(
        _env_file=None,
        **{
            **SETTINGS.model_dump(),
            "db_operation_timeout": db_timeout,
            "heartbeat_interval": timedelta(seconds=5),
        },
    )
    params = conninfo_to_dict(supervisor_conninfo("postgresql:///orqestra_test", settings))
    assert params["options"] == f"-c statement_timeout={statement}"
    assert params["connect_timeout"] == connect


def test_worker_ids_are_unique_per_start() -> None:
    first, second = new_worker_id(), new_worker_id()
    assert first != second
    assert re.fullmatch(r".+:\d+:[0-9a-f]{8}", first)


# --- the worker against injected lifecycle operations -------------------------------------------


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class Terminated(Exception):  # noqa: N818
    def __init__(self, code: ExitCode, event: str) -> None:
        super().__init__(event)
        self.code, self.event = code, event


def terminate(code: ExitCode, event: str, message: str, context: Any) -> NoReturn:
    raise Terminated(code, event)


class FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def attempt(lease_seconds: float = 30.0, worker: str = "worker-1") -> Attempt:
    return Attempt(
        uuid.UUID(int=1),
        uuid.UUID(int=2),
        1,
        worker,
        AttemptState.RUNNING,
        None,
        None,
        None,
        T0,
        None,
        T0 + timedelta(seconds=lease_seconds),
        T0 + timedelta(minutes=10),
        None,
    )


class Harness:
    """A worker whose handler waits on ``gate``, with a fake clock, connection and watchdog."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.clock = FakeClock()
        self.expiries: list[Expiry] = []
        self.watchdog = Watchdog(clock=self.clock, terminate=self.expiries.append)
        self.gate = threading.Event()
        self.conn = FakeConn()
        self.connects = 0
        self.monkeypatch = monkeypatch

        def handler(ctx: TaskContext, payload: JsonValue) -> JsonValue:
            assert self.gate.wait(10)
            return {"ok": True}

        def connect() -> Any:
            self.connects += 1
            return self.conn

        self.worker = Worker(
            registry=HandlerRegistry({"k": handler}),
            settings=SETTINGS,
            connect=connect,
            worker_id="worker-1",
            clock=self.clock,
            watchdog=self.watchdog,
            terminate=terminate,
        )

    def start(self) -> runtime._Running:
        # The claim was sent at local 100.0; its lease ends 30 s after the claim instant.
        self.worker._start(attempt(), 100.0, {"worker_id": "worker-1"}, {}, "k")
        running = self.worker._schedule.running
        assert running is not None
        return running

    def renew(self, result: Callable[..., Attempt]) -> None:
        self.monkeypatch.setattr(runtime, "renew_lease", result)

    def finish_handler(self, running: runtime._Running) -> None:
        self.gate.set()
        assert running.done.wait(10)
        running.thread.join(10)

    def deadline(self) -> float | None:
        return self.watchdog.deadline(Guard.OWNERSHIP)


def running_of(worker: Worker) -> runtime._Running:
    running = worker._schedule.running
    assert running is not None
    return running


def raising(exc: BaseException) -> Callable[..., Any]:
    def call(*args: Any, **kwargs: Any) -> Any:
        raise exc

    return call


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> Iterator[Harness]:
    h = Harness(monkeypatch)
    yield h
    h.gate.set()


def test_the_fence_is_armed_from_the_claim_and_moves_only_on_a_confirmed_renewal(
    harness: Harness,
) -> None:
    running = harness.start()
    assert harness.deadline() == 100.0 + 30.0 - 3.0  # lease from the claim, less the margin
    assert running.heartbeat_due == 105.0

    for failure in (
        psycopg.OperationalError("lost"),
        errors.DeadlockDetected(),
        errors.QueryCanceled(),
    ):
        harness.clock.now += 1.0
        harness.renew(raising(failure))
        harness.worker._heartbeat(running)
        assert harness.deadline() == 127.0  # an unconfirmed heartbeat never extends it
        assert running.heartbeat_due == harness.clock.now + 2.0  # retried after the bound

    harness.clock.now = 110.0
    harness.renew(lambda *a, **k: attempt(lease_seconds=40.0))  # confirmed: lease to T0 + 40 s
    harness.worker._heartbeat(running)
    assert harness.deadline() == 100.0 + 40.0 - 3.0
    assert running.heartbeat_due == 115.0
    assert harness.expiries == []


def test_a_heartbeat_confirmed_after_the_old_deadline_keeps_the_worker(harness: Harness) -> None:
    running = harness.start()
    harness.clock.now = 128.0  # past the old fence (127.0); the watchdog has not checked yet
    harness.renew(lambda *a, **k: attempt(lease_seconds=40.0))  # a valid, confirmed renewal
    harness.worker._heartbeat(running)
    assert harness.deadline() == 100.0 + 40.0 - 3.0
    assert harness.watchdog.check() is None  # not expired by the superseded deadline
    assert harness.expiries == []


def test_a_failed_connect_is_an_unconfirmed_heartbeat(harness: Harness) -> None:
    running = harness.start()

    def refuse() -> Any:
        raise psycopg.OperationalError("connection refused")

    harness.worker._connect = refuse
    harness.worker._conn = None
    harness.worker._heartbeat(running)
    assert harness.deadline() == 127.0
    assert running.heartbeat_due == 102.0


def test_the_handler_thread_disarms_and_a_later_renewal_cannot_rearm(harness: Harness) -> None:
    running = harness.start()
    harness.finish_handler(running)
    assert harness.deadline() is None  # disarmed by the handler thread itself
    harness.renew(lambda *a, **k: attempt(lease_seconds=60.0))
    harness.worker._heartbeat(running)
    assert harness.deadline() is None  # the obsolete token changes nothing
    harness.clock.now = 10_000.0
    assert harness.watchdog.check() is None
    assert harness.expiries == []


@pytest.mark.parametrize(
    "exc",
    [
        LeaseExpiredError(uuid.UUID(int=1), RevocationReason.LEASE_EXPIRED, T0, T0),
        AttemptRevokedError(uuid.UUID(int=1), RevocationReason.LEASE_EXPIRED),
        StaleAttemptError(uuid.UUID(int=1), 1, 2),
        AttemptFinishedError(uuid.UUID(int=1), AttemptState.SUCCEEDED),
        NotFoundError("attempt", uuid.UUID(int=1)),
    ],
    ids=["lease-expired", "revoked", "stale", "finished", "not-found"],
)
def test_lost_ownership_ends_the_process_while_the_handler_runs(
    harness: Harness, exc: Exception
) -> None:
    running = harness.start()
    harness.renew(raising(exc))
    with pytest.raises(Terminated) as info:
        harness.worker._heartbeat(running)
    assert (info.value.code, info.value.event) == (ExitCode.FENCED, "ownership_lost")


def test_lost_ownership_after_the_handler_ended_leaves_it_to_the_completion(
    harness: Harness,
) -> None:
    running = harness.start()
    harness.finish_handler(running)
    harness.renew(raising(AttemptRevokedError(uuid.UUID(int=1), RevocationReason.LEASE_EXPIRED)))
    harness.worker._heartbeat(running)  # no termination
    assert running.heartbeat_due is None


def test_a_cancellation_request_stops_renewals_and_fences_at_the_grace_end(
    harness: Harness,
) -> None:
    running = harness.start()
    grace_end = T0 + timedelta(seconds=12)
    harness.renew(raising(CancellationRequestedError(uuid.UUID(int=1), T0, grace_end)))
    harness.worker._heartbeat(running)
    assert running.heartbeat_due is None
    assert harness.deadline() == 100.0 + 12.0 - 3.0  # shortened, never lengthened
    harness.clock.now = 109.0
    expiry = harness.watchdog.check()
    assert expiry is not None
    assert expiry.guard is Guard.OWNERSHIP


def completed(
    harness: Harness, outcome_check: Callable[[Any], None] | None = None
) -> Callable[..., Any]:
    calls: list[Any] = []

    def complete(conn: Any, attempt_id: uuid.UUID, *, generation: int, outcome: Any) -> Any:
        calls.append(outcome)
        if outcome_check is not None:
            outcome_check(outcome)

        class Completion:
            class attempt:  # noqa: N801
                state = AttemptState.SUCCEEDED

            class task:  # noqa: N801
                state = AttemptState.SUCCEEDED

        return Completion

    complete.calls = calls  # type: ignore[attr-defined]
    return complete


def test_a_successful_completion_settles_the_task_and_claims_again(harness: Harness) -> None:
    running = harness.start()
    harness.finish_handler(running)
    complete = completed(harness)
    harness.monkeypatch.setattr(runtime, "complete_attempt", complete)
    harness.clock.now = 120.0
    harness.worker._complete(running)
    assert complete.calls == [AttemptSucceeded({"ok": True})]  # type: ignore[attr-defined]
    assert harness.worker._schedule.running is None
    assert harness.worker._schedule.claim_due == 120.0


@pytest.mark.parametrize(
    "exc",
    [
        AttemptRevokedError(uuid.UUID(int=1), RevocationReason.LEASE_EXPIRED),
        StaleAttemptError(uuid.UUID(int=1), 1, 2),
    ],
    ids=["revoked", "stale"],
)
def test_an_outcome_after_lost_ownership_is_discarded(harness: Harness, exc: Exception) -> None:
    running = harness.start()
    harness.finish_handler(running)
    harness.monkeypatch.setattr(runtime, "complete_attempt", raising(exc))
    harness.worker._complete(running)
    assert harness.worker._schedule.running is None


@pytest.mark.parametrize(
    ("exc", "closed", "code", "event"),
    [
        (psycopg.OperationalError("lost"), True, ExitCode.AMBIGUOUS, "completion_ambiguous"),
        (errors.CheckViolation(), False, ExitCode.DEFECT, "completion_rejected"),
        (
            AttemptFinishedError(uuid.UUID(int=1), AttemptState.SUCCEEDED),
            False,
            ExitCode.DEFECT,
            "completion_refused",
        ),
        (NotFoundError("attempt", uuid.UUID(int=1)), False, ExitCode.DEFECT, "completion_refused"),
        (ValueError("policy"), False, ExitCode.DEFECT, "completion_refused"),
    ],
    ids=["ambiguous", "rejected", "finished", "not-found", "policy"],
)
def test_completion_failures_end_the_process_without_a_handler_failure(
    harness: Harness, exc: Exception, closed: bool, code: ExitCode, event: str
) -> None:
    running = harness.start()
    harness.finish_handler(running)

    def fail(*args: Any, **kwargs: Any) -> Any:
        harness.conn.closed = closed
        raise exc

    harness.monkeypatch.setattr(runtime, "complete_attempt", fail)
    with pytest.raises(Terminated) as info:
        harness.worker._complete(running)
    assert (info.value.code, info.value.event) == (code, event)


def test_a_retryable_completion_is_retried_until_the_attempts_deadline(harness: Harness) -> None:
    running = harness.start()
    harness.finish_handler(running)
    harness.monkeypatch.setattr(runtime, "complete_attempt", raising(errors.DeadlockDetected()))
    harness.clock.now = 120.0
    harness.worker._complete(running)
    assert running.completion_due == 122.0  # retried after the database bound
    assert harness.worker._schedule.running is running
    harness.clock.now = 100.0 + 600.0  # the attempt's deadline, mapped to the local clock
    with pytest.raises(Terminated) as info:
        harness.worker._complete(running)
    assert info.value.code is ExitCode.UNRECORDED


def test_a_failed_connect_before_completion_is_retried(harness: Harness) -> None:
    running = harness.start()
    harness.finish_handler(running)

    def refuse() -> Any:
        raise psycopg.OperationalError("connection refused")

    harness.worker._connect = refuse
    harness.worker._conn = None
    harness.clock.now = 120.0
    harness.worker._complete(running)
    assert running.completion_due == 122.0


def test_a_control_plane_signal_is_never_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    harness = Harness(monkeypatch)
    running = harness.start()
    harness.finish_handler(running)
    running.outcome[0] = ControlPlaneSignal("builtins.SystemExit")
    complete = completed(harness)
    monkeypatch.setattr(runtime, "complete_attempt", complete)
    with pytest.raises(Terminated) as info:
        harness.worker._complete(running)
    assert info.value.code is ExitCode.HANDLER_ABORTED
    assert complete.calls == []  # type: ignore[attr-defined]


def test_a_handler_thread_without_an_outcome_is_a_defect(harness: Harness) -> None:
    running = harness.start()
    harness.finish_handler(running)
    running.outcome.clear()
    with pytest.raises(Terminated) as info:
        harness.worker._complete(running)
    assert info.value.code is ExitCode.DEFECT


def test_a_lease_too_short_to_run_is_never_started(harness: Harness) -> None:
    assert not harness.worker._start(attempt(lease_seconds=3.0), 100.0, {}, {}, "k")
    assert harness.worker._schedule.running is None
    assert harness.deadline() is None


def test_handler_failures_are_classified_as_before(monkeypatch: pytest.MonkeyPatch) -> None:
    harness = Harness(monkeypatch)
    running = harness.start()
    harness.finish_handler(running)
    assert running.outcome == [AttemptSucceeded({"ok": True})]

    def transient(ctx: TaskContext, payload: JsonValue) -> JsonValue:
        raise TransientError("later")

    worker = harness.worker
    worker._registry = HandlerRegistry({"k": transient})
    worker._schedule.running = None
    worker._start(attempt(), 100.0, {}, {}, "k")
    second = running_of(worker)
    assert second.done.wait(10)
    (failure,) = second.outcome
    assert isinstance(failure, AttemptFailed)
    assert failure.kind is FailureKind.TRANSIENT


# --- claims, sweeps and a failing supervisor --------------------------------------------


def test_a_failed_claim_is_retried_after_the_poll_interval(harness: Harness) -> None:
    harness.monkeypatch.setattr(runtime, "claim_next_attempt", raising(errors.QueryCanceled()))
    harness.worker._claim()
    assert harness.worker._schedule.running is None
    assert harness.worker._schedule.claim_due == 101.0


def test_an_ambiguous_claim_holds_further_claims_for_a_full_lease(harness: Harness) -> None:
    def lost(*args: Any, **kwargs: Any) -> Any:
        harness.conn.closed = True  # the connection broke: the claim may have committed
        raise psycopg.OperationalError("lost")

    harness.monkeypatch.setattr(runtime, "claim_next_attempt", lost)
    harness.worker._claim()
    assert harness.worker._schedule.running is None
    assert harness.worker._schedule.claim_due == 130.0  # now + the 30 s lease


def test_a_claim_whose_task_cannot_be_read_is_held_until_its_lease_ends(harness: Harness) -> None:
    harness.monkeypatch.setattr(runtime, "claim_next_attempt", lambda *a, **k: attempt())
    harness.monkeypatch.setattr(runtime, "get_task", raising(psycopg.OperationalError("lost")))
    harness.worker._claim()
    assert harness.worker._schedule.running is None
    assert harness.deadline() is None  # no handler, so no fence
    # No other claim until the unstarted attempt's lease has certainly ended.
    assert harness.worker._schedule.claim_due == 100.0 + 30.0


def test_a_claimed_lease_too_short_to_run_is_held_until_it_ends(harness: Harness) -> None:
    short = attempt(lease_seconds=3.0)  # a 3 s lease and a 3 s margin
    harness.monkeypatch.setattr(runtime, "claim_next_attempt", lambda *a, **k: short)

    class Task:
        payload: JsonValue = None
        job_id = uuid.UUID(int=3)

    class Job:
        kind = "k"

    harness.monkeypatch.setattr(runtime, "get_task", lambda *a, **k: Task)
    harness.monkeypatch.setattr(runtime, "get_job", lambda *a, **k: Job)
    harness.worker._claim()
    assert harness.worker._schedule.running is None
    assert harness.deadline() is None
    assert harness.worker._schedule.claim_due == 103.0


def test_a_claimed_task_without_its_job_is_a_defect(harness: Harness) -> None:
    harness.monkeypatch.setattr(runtime, "claim_next_attempt", lambda *a, **k: attempt())
    harness.monkeypatch.setattr(runtime, "get_task", lambda *a, **k: None)
    with pytest.raises(Terminated) as info:
        harness.worker._claim()
    assert (info.value.code, info.value.event) == (ExitCode.DEFECT, "claimed_task_unreadable")


def test_a_sweep_revokes_up_to_its_batch_limit_then_waits_for_the_interval(
    harness: Harness,
) -> None:
    revoked = attempt(worker="dead")

    class Revocation:
        attempt = revoked

    harness.monkeypatch.setattr(runtime, "revoke_next_expired", lambda conn: Revocation)
    sweep = harness.worker._schedule.sweep
    for left in (2, 1):  # the batch limit is 3
        harness.worker._sweep()
        assert (sweep.left, sweep.due) == (left, 100.0)  # continues at once
    harness.worker._sweep()
    assert (sweep.left, sweep.due) == (0, 110.0)  # the next sweep, one interval after this one


def test_a_failed_sweep_ends_until_the_next_interval(harness: Harness) -> None:
    harness.monkeypatch.setattr(runtime, "revoke_next_expired", raising(errors.QueryCanceled()))
    harness.worker._sweep()
    sweep = harness.worker._schedule.sweep
    assert (sweep.left, sweep.due) == (0, 110.0)


def test_a_supervisor_failing_while_a_handler_runs_ends_the_process(harness: Harness) -> None:
    harness.start()
    harness.clock.now = 106.0  # the first heartbeat is due
    harness.monkeypatch.setattr(runtime, "renew_lease", raising(KeyError("unexpected")))
    with pytest.raises(Terminated) as info:
        harness.worker.run(threading.Event())
    assert (info.value.code, info.value.event) == (ExitCode.FENCED, "supervisor_failed")
    assert isinstance(info.value.__context__, KeyError)
    assert harness.deadline() is None  # the watchdog was stopped on the way out


def test_a_supervisor_failing_while_idle_just_raises(harness: Harness) -> None:
    harness.monkeypatch.setattr(runtime, "revoke_next_expired", raising(KeyError("unexpected")))
    with pytest.raises(KeyError):
        harness.worker.run(threading.Event())
