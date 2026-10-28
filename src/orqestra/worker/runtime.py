"""The single-slot worker runtime: a supervisor and one handler thread at a time (DEC-045).

The supervisor runs on the calling thread and alone uses one autocommit
database connection. It claims a task of a supported kind, runs its handler on
a separate thread, renews the attempt's lease while the handler runs, sweeps
for expired attempts while idle or busy, and records the handler's outcome.
It never claims again until the previous task is settled.

Every supervisor database call is bounded by ``statement_timeout`` and, on the
client side, by the watchdog's operation guard; a running handler is bounded
by the ownership guard (``fence``). When the supervisor cannot establish that
it still owns a running task, it ends the process and Day 3A recovery decides.

When a completion's answer is lost, the supervisor keeps the handler's outcome
and reconciles it from PostgreSQL's evidence (DEC-047): the completion
committed, the attempt still runs and the same outcome is written again, or
recovery revoked it. The handler never runs again for it. If that cannot be
established before the attempt's deadline, the process ends.

Cancellation is cooperative (DEC-048): when a heartbeat shows that a
cancellation request has committed, the supervisor stops renewing, moves the
fence to the end of the grace period, and sets the attempt's cancellation
event, which the handler sees through its read-only ``CancellationSignal``.
Notice arrives only with a heartbeat, so it is not guaranteed before the
grace period ends.

Shutdown is graceful and bounded (DEC-049). ``request_stop`` only sets a flag
and wakes the supervisor; the supervisor latches the request at its next step,
and from then on sends no claim, starts no handler and starts no sweep. A task
it already holds is drained: heartbeats, completion and reconciliation go on,
and the watchdog's drain guard ends the process (exit 9) if the task is not
settled within ``shutdown_timeout`` of the latch. Shutdown never cancels or
fails a task and writes nothing; an abandoned attempt is left to recovery.
``shutdown.py`` turns SIGTERM and SIGINT into stop requests.

``orqestra worker run`` starts a worker from the command line (DEC-051);
there is no production service or workload handler yet.
"""

import contextlib
import logging
import math
import os
import select
import socket
import threading
import time
import uuid
import weakref
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, NoReturn

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from orqestra.domain.jobs import Attempt, AttemptOutcome, JsonValue
from orqestra.persistence.jobs import get_job, get_task
from orqestra.persistence.lifecycle import (
    AttemptFinishedError,
    AttemptRevokedError,
    CancellationRequestedError,
    CompletionEvidence,
    LeaseExpiredError,
    NotFoundError,
    StaleAttemptError,
    WorkerMismatchError,
    claim_next_attempt,
    complete_attempt,
    read_completion_evidence,
    renew_lease,
    revoke_next_expired,
)
from orqestra.worker.fence import ExitCode, Guard, Watchdog, end_process, local_deadline
from orqestra.worker.handlers import (
    CancellationSignal,
    ControlPlaneSignal,
    HandlerOutcome,
    HandlerRegistry,
    TaskContext,
    run_handler,
)
from orqestra.worker.settings import WorkerSettings, server_milliseconds

logger = logging.getLogger("orqestra.worker")

Conn = psycopg.Connection[Any]

# Server errors after which the transaction certainly rolled back and the same
# write may be tried again: serialization failure, deadlock, lock not
# available, statement timeout.
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01", "55P03", "57014"})

# Heartbeat errors after which the attempt can no longer be renewed: its
# ownership is lost or about to be, so a running handler must be stopped.
_OWNERSHIP_LOST = (
    LeaseExpiredError,
    AttemptRevokedError,
    StaleAttemptError,
    WorkerMismatchError,
    AttemptFinishedError,
    NotFoundError,
)


class ConnectionUnavailable(Exception):  # noqa: N818 - it names a state, not an error kind
    """The supervisor could not connect; nothing was sent to the database."""


class UncontrolledExitError(RuntimeError):
    """``SystemExit`` with a success code escaped the supervisor without a controlled stop.

    ``run`` raises this instead, so an unexpected exit can never end the
    process with status 0 (DEC-049).
    """


class Step(StrEnum):
    """What the supervisor does next."""

    HEARTBEAT = "heartbeat"
    COMPLETE = "complete"
    SWEEP = "sweep"
    CLAIM = "claim"
    STOP = "stop"
    ABANDON = "abandon"  # a stopping worker's drain deadline passed with its task unsettled
    WAIT = "wait"


@dataclass(frozen=True, slots=True)
class Timing:
    """The supervisor's schedule at one instant; all times are on the local clock."""

    now: float
    running: bool  # a handler thread is executing
    completing: bool  # a handler has finished and its outcome is not yet settled
    stopping: bool  # a stop request has been latched (DEC-049)
    drain_due: float | None  # while a stopping worker holds a task: its drain deadline
    heartbeat_due: float | None  # None: the lease is no longer renewed
    completion_due: float
    sweep_due: float  # a sweep in progress is due at once
    claim_due: float
    sweep_reserve: float  # the time one sweep step may take (db_operation_timeout)


def next_step(timing: Timing) -> tuple[Step, float]:
    """The supervisor's next step, and for ``WAIT`` how many seconds to wait.

    A drain deadline that has passed comes first: the task is abandoned.
    Then, in order: a finished handler's completion; a running handler's
    heartbeat; a recovery sweep step, which while a handler runs starts only
    if it can take its full ``sweep_reserve`` and still end before the next
    heartbeat is due, so sweeps never delay a heartbeat; then, only when idle,
    a stop or a claim. A stopping worker starts no sweep and no claim
    (DEC-049). Every wait is finite and positive, and the supervisor's wake
    pipe can end it sooner.
    """
    t = timing
    if t.drain_due is not None and t.now >= t.drain_due:
        return Step.ABANDON, 0.0
    if t.completing:
        if t.now >= t.completion_due:
            return Step.COMPLETE, 0.0
        return Step.WAIT, _until(t, t.completion_due)
    if t.running:
        heartbeat = t.heartbeat_due
        if heartbeat is not None and t.now >= heartbeat:
            return Step.HEARTBEAT, 0.0
        sweeping = not t.stopping
        if (
            sweeping
            and t.now >= t.sweep_due
            and (heartbeat is None or heartbeat - t.now >= t.sweep_reserve)
        ):
            return Step.SWEEP, 0.0
        # A sweep held back for the heartbeat waits for it; otherwise wake for either.
        wakes = [] if heartbeat is None else [heartbeat]
        if sweeping and t.sweep_due > t.now:
            wakes.append(t.sweep_due)
        return Step.WAIT, _until(t, *wakes)
    if t.stopping:
        return Step.STOP, 0.0
    if t.now >= t.sweep_due:
        return Step.SWEEP, 0.0
    if t.now >= t.claim_due:
        return Step.CLAIM, 0.0
    return Step.WAIT, min(t.sweep_due, t.claim_due) - t.now


def _until(t: Timing, *wakes: float) -> float:
    """Seconds until the earliest of ``wakes`` and the drain deadline, if any."""
    due = [*wakes] if t.drain_due is None else [*wakes, t.drain_due]
    if not due:  # pragma: no cover - a stopping worker that holds a task has a drain deadline
        return t.sweep_reserve
    return min(due) - t.now


class CompletionError(StrEnum):
    """How a database error raised while recording an outcome is treated."""

    AMBIGUOUS = "ambiguous"  # the connection broke: the write may or may not have committed
    RETRY = "retry"  # nothing committed, and the same write is worth trying again
    REJECTED = "rejected"  # rolled back, and the database refuses a valid outcome


def classify_completion_error(
    exc: psycopg.Error | ConnectionUnavailable, *, connection_closed: bool
) -> CompletionError:
    """Classify a database error from ``complete_attempt``; it is never a handler failure.

    A failed connect sent nothing. Without a working connection or a server
    error code, the client cannot know whether the commit happened. With one,
    the transaction rolled back.
    """
    if isinstance(exc, ConnectionUnavailable):
        return CompletionError.RETRY
    if connection_closed or exc.sqlstate is None:
        return CompletionError.AMBIGUOUS
    if exc.sqlstate in RETRYABLE_SQLSTATES:
        return CompletionError.RETRY
    return CompletionError.REJECTED


def supervisor_conninfo(database_url: str, settings: WorkerSettings) -> str:
    """``database_url`` with the supervisor's server-side bounds added.

    In whole milliseconds, rounded up (``server_milliseconds``):

    - ``statement_timeout`` is ``db_operation_timeout``;
    - ``lock_timeout`` is ``lock_timeout``, shorter, so that an ordinary lock
      wait fails with ``55P03``, which every supervisor path treats as
      unconfirmed and retries, rather than running into the watchdog's operation
      guard (DEC-050). This protects ordinary lock waits; it is not a timing
      guarantee: the guard bounds the whole call, so earlier statements of the
      same transaction, the network and scheduling can use up the difference,
      and the guard may still end the process first;
    - ``idle_in_transaction_session_timeout`` is ``db_operation_timeout``. Every
      supervisor transaction runs inside one guarded call, so a supervisor still
      running never sits idle in a transaction that long. A frozen, killed or cut
      off one may: PostgreSQL then ends its session, rolling the transaction
      back and releasing its locks, so recovery can proceed (DEC-050).

    ``connect_timeout`` is whole seconds, rounded up, and libpq treats anything
    under 2 as 2. Existing ``options``, such as a ``search_path``, are kept.
    These bound ordinary slow calls only: a stalled call is ended by the
    watchdog's operation guard.
    """
    timeout = settings.db_operation_timeout.total_seconds()
    statement = server_milliseconds(settings.db_operation_timeout)
    bounds = (
        f"-c statement_timeout={statement} "
        f"-c lock_timeout={server_milliseconds(settings.lock_timeout)} "
        f"-c idle_in_transaction_session_timeout={statement}"
    )
    existing = conninfo_to_dict(database_url).get("options")
    options = f"{existing} {bounds}" if existing else bounds
    return make_conninfo(
        database_url,
        options=options,
        connect_timeout=str(max(2, math.ceil(timeout))),
        application_name="orqestra-worker",
    )


def connect_supervisor(database_url: str, settings: WorkerSettings) -> Conn:
    """Open the supervisor's autocommit connection with its server-side bounds."""
    return psycopg.connect(supervisor_conninfo(database_url, settings), autocommit=True)


def new_worker_id() -> str:
    """A worker ID unique to this process start: host, process ID and a random suffix."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


type Terminate = Callable[[ExitCode, str, str, Mapping[str, str]], NoReturn]


def _lease(attempt: Attempt) -> tuple[datetime, datetime]:
    if attempt.lease_expires_at is None or attempt.deadline_at is None:  # pragma: no cover
        msg = f"attempt {attempt.id} has no lease"  # every claim since migration 0002 has one
        raise RuntimeError(msg)
    return attempt.lease_expires_at, attempt.deadline_at


@dataclass(slots=True)
class _Running:
    """The one task this worker holds, from its claim until its outcome is settled."""

    attempt: Attempt
    context: Mapping[str, str]
    anchor: float  # the local clock just before the claim was sent
    token: int  # the ownership guard's token
    thread: threading.Thread
    done: threading.Event
    outcome: list[HandlerOutcome]
    # Set once a heartbeat has confirmed a committed cancellation request; the
    # handler sees it only through its read-only CancellationSignal (DEC-048).
    cancel: threading.Event
    heartbeat_due: float | None
    completion_due: float = 0.0
    # An earlier write of this outcome may have committed: read the evidence
    # before writing again (DEC-047).
    uncertain: bool = False

    def local(self, instant: datetime) -> float:
        """A database instant of this attempt on the local clock, never later than it."""
        return local_deadline(
            anchor=self.anchor, started_at=self.attempt.started_at, instant=instant
        )


@dataclass(frozen=True, slots=True)
class _Held:
    """A claim that may hold an attempt whose handler never started (DEC-049)."""

    attempt: Attempt | None  # None: the claim's answer was lost
    until: float  # the local clock by which its lease has certainly ended


def _close_pipe(*fds: int) -> None:
    for fd in fds:
        os.close(fd)


class _Wake:
    """A nonblocking self-pipe that ends the supervisor's waits early (DEC-049).

    The handler thread when it ends, ``request_stop`` and, through
    ``signal.set_wakeup_fd``, the C-level signal handler of whichever thread
    receives a signal each write one byte. Writing takes no Python lock, so a
    signal handler may do it. The supervisor empties the pipe before it reads
    any state and then waits on it, so a wake that comes after the read ends
    the wait at once, and none is lost.

    The descriptors are closed only once this object is garbage. While
    anything can still write to them (the worker, its handler thread, a signal
    handler holding the worker), they stay open, so no write can ever reach a
    closed or reused descriptor.
    """

    def __init__(self) -> None:
        read_fd, write_fd = os.pipe()
        os.set_blocking(read_fd, False)
        os.set_blocking(write_fd, False)
        self.read_fd, self.write_fd = read_fd, write_fd
        self._poll = select.poll()
        self._poll.register(read_fd, select.POLLIN)
        weakref.finalize(self, _close_pipe, read_fd, write_fd)

    def notify(self) -> None:
        with contextlib.suppress(BlockingIOError):  # full: a wake is already pending
            os.write(self.write_fd, b"\0")

    def drain(self) -> None:
        with contextlib.suppress(BlockingIOError):  # empty
            while os.read(self.read_fd, 4096):
                pass

    def wait(self, seconds: float) -> None:
        """Wait up to ``seconds`` (rounded up to whole milliseconds), or until woken."""
        self._poll.poll(math.ceil(seconds * 1000))


@dataclass(slots=True)
class _Sweep:
    due: float
    started: float = 0.0
    left: int = 0  # revocations left in the sweep in progress; 0 when none is


@dataclass(slots=True)
class _Schedule:
    sweep: _Sweep
    claim_due: float
    running: _Running | None = field(default=None)


class Worker:
    """A single-slot worker: claims, runs and records one task at a time.

    ``connect`` opens a new supervisor connection in autocommit mode (see
    ``connect_supervisor``). ``clock``, ``watchdog`` and ``terminate`` are
    injectable for tests; by default ``terminate`` ends the process. ``run``
    works until ``request_stop`` is latched; a worker runs once.
    """

    def __init__(
        self,
        *,
        registry: HandlerRegistry,
        settings: WorkerSettings,
        connect: Callable[[], Conn],
        worker_id: str | None = None,
        clock: Callable[[], float] = time.monotonic,
        watchdog: Watchdog | None = None,
        terminate: Terminate = end_process,
    ) -> None:
        self.worker_id = worker_id or new_worker_id()
        self._registry = registry
        self._settings = settings
        self._policy = settings.lease_policy
        self._connect = connect
        self._clock = clock
        self._watchdog = watchdog or Watchdog(clock=clock)
        self._terminate = terminate
        self._conn: Conn | None = None
        now = clock()
        self._schedule = _Schedule(sweep=_Sweep(due=now), claim_due=now)
        self._wake = _Wake()
        self._stop_requested = False  # set by request_stop; read by the supervisor only
        self._stopping_at: float | None = None  # the latch (DEC-049)
        self._drain_due: float | None = None
        self._drain_token: int | None = None
        self._held: _Held | None = None
        self._finished = False

    def request_stop(self) -> None:
        """Ask the worker to stop; the supervisor latches the request at its next step.

        Safe from a signal handler and from any thread: it sets a flag and
        writes one byte to the wake pipe, and takes no lock. It never cancels
        or fails the task the worker holds (DEC-049).
        """
        self._stop_requested = True
        self._wake.notify()

    @property
    def finished(self) -> bool:
        """Whether ``run`` has returned through a controlled stop."""
        return self._finished

    @property
    def wakeup_fd(self) -> int:
        """The wake pipe's write end, for ``signal.set_wakeup_fd`` (``shutdown.py``)."""
        return self._wake.write_fd

    def run(self) -> None:
        """Work until a stop request is latched; a task already started is drained first.

        It returns only through a controlled stop: no handler is running, and
        each one this worker started has ended as PostgreSQL confirmed. A claim
        whose handler never started may still hold an attempt; that is logged
        (``stopped_with_unstarted_claim``) and left to recovery. Anything else
        ends the process or raises (DEC-049).
        """
        self._watchdog.start()
        self._event(logging.INFO, "worker_started", f"kinds={list(self._registry.kinds)}")
        try:
            while True:
                self._wake.drain()  # before any state is read, so no wake is lost
                self._observe_stop()
                step, seconds = next_step(self._timing())
                if step is Step.STOP:
                    self._stopped()
                    return
                if step is Step.WAIT:
                    self._wake.wait(seconds)
                elif step is Step.ABANDON:
                    self._terminate(
                        ExitCode.SHUTDOWN,
                        "drain_deadline_passed",
                        "the drain deadline passed before the task was settled; recovery decides",
                        self._require_running().context,
                    )
                elif step is Step.HEARTBEAT:
                    self._heartbeat(self._require_running())
                elif step is Step.COMPLETE:
                    self._complete(self._require_running())
                elif step is Step.SWEEP:
                    self._sweep()
                else:
                    self._claim()
        except BaseException as exc:
            running = self._schedule.running
            if running is not None and not running.done.is_set():
                # Nothing would supervise the handler any more: end the process.
                self._terminate(
                    ExitCode.FENCED,
                    "supervisor_failed",
                    "the supervisor stopped while a handler ran",
                    running.context,
                )
            if isinstance(exc, SystemExit) and exc.code in (None, 0):
                # Never a silent success: only a controlled stop returns.
                msg = "SystemExit with a success code ended the supervisor without a stop"
                raise UncontrolledExitError(msg) from exc
            raise
        finally:
            self._close()
            self._watchdog.stop(self._bound())

    # --- stopping -------------------------------------------------------------

    def _observe_stop(self) -> bool:
        """Latch a stop request, the shutdown's linearization point; True once latched.

        From the latch on, the supervisor sends no claim and starts no handler
        or sweep. A task it already holds, including one whose handler started
        after the request but before this read, is drained: the drain guard
        ends the process unless it is settled within ``shutdown_timeout``.
        """
        if self._stopping_at is None and self._stop_requested:
            self._stopping_at = now = self._clock()
            running = self._schedule.running
            if running is None:
                self._event(logging.INFO, "shutdown_requested", "idle")
            else:
                timeout = self._settings.shutdown_timeout.total_seconds()
                self._drain_due = now + timeout
                self._drain_token = self._watchdog.arm(
                    Guard.DRAIN, self._drain_due, running.context
                )
                self._event(
                    logging.INFO,
                    "shutdown_requested",
                    f"draining: the task has {timeout}s to settle",
                    running.context,
                )
        return self._stopping_at is not None

    def _stopped(self) -> None:
        held = self._held
        if held is not None and self._clock() < held.until:
            # Exit 0 does not mean every claim is settled: say which one is not.
            if held.attempt is None:
                self._event(
                    logging.WARNING,
                    "stopped_with_uncertain_claim",
                    "a claim's answer was lost; if it committed, recovery revokes that attempt "
                    "once its lease ends, using one attempt of its task's budget",
                )
            else:
                lease_expires_at, _ = _lease(held.attempt)
                self._event(
                    logging.WARNING,
                    "stopped_with_unstarted_claim",
                    f"generation={held.attempt.attempt_number} "
                    f"lease_expires_at={lease_expires_at.isoformat()}: its handler never "
                    "started; recovery revokes it once its lease ends, using one attempt of its "
                    "task's budget",
                    self._context(held.attempt),
                )
        self._finished = True
        self._event(logging.INFO, "worker_stopped")

    # --- scheduling -----------------------------------------------------------

    def _timing(self) -> Timing:
        running = self._schedule.running
        busy = running is not None and not running.done.is_set()
        return Timing(
            now=self._clock(),
            running=busy,
            completing=running is not None and not busy,
            stopping=self._stopping_at is not None,
            drain_due=self._drain_due,
            heartbeat_due=running.heartbeat_due if running is not None else None,
            completion_due=running.completion_due if running is not None else 0.0,
            sweep_due=self._schedule.sweep.due,
            claim_due=self._schedule.claim_due,
            sweep_reserve=self._bound(),
        )

    # --- claiming and running ---------------------------------------------------

    def _claim(self) -> None:
        anchor = self._clock()
        poll = self._settings.poll_interval.total_seconds()
        try:
            attempt = self._call(
                "claim",
                lambda conn: claim_next_attempt(
                    conn, self.worker_id, kinds=self._registry.kinds, policy=self._policy
                ),
                {},
            )
        except (psycopg.Error, ConnectionUnavailable) as exc:
            if classify_completion_error(exc, connection_closed=self._conn is None) is (
                CompletionError.AMBIGUOUS
            ):
                # The claim may have committed. Claim nothing else until any such
                # attempt's lease has certainly ended; recovery then takes it back.
                self._event(logging.WARNING, "claim_uncertain", type(exc).__name__)
                until = self._clock() + self._policy.lease.total_seconds()
                self._held = _Held(None, until)
                self._schedule.claim_due = until
            else:
                self._event(logging.WARNING, "claim_failed", type(exc).__name__)
                self._schedule.claim_due = self._clock() + poll
            return
        returned = self._clock()
        if attempt is None:
            self._schedule.claim_due = returned + poll
            return
        context = self._context(attempt)
        self._event(
            logging.INFO, "attempt_claimed", f"generation={attempt.attempt_number}", context
        )
        # A stop is looked for after each database call of this step, so it is
        # latched no more than one call after it was requested (DEC-049).
        if self._stopping_before_start(attempt, returned, context):
            return
        try:
            task = self._call("read_task", lambda conn: get_task(conn, attempt.task_id), context)
            if task is not None and self._stopping_before_start(attempt, returned, context):
                return
            job = (
                None
                if task is None
                else self._call("read_job", lambda conn: get_job(conn, task.job_id), context)
            )
        except (psycopg.Error, ConnectionUnavailable) as exc:
            # Without its payload the handler cannot run.
            self._event(logging.ERROR, "claimed_task_not_started", type(exc).__name__, context)
            self._hold(attempt, returned)
            return
        if task is None or job is None or job.kind not in self._registry:
            self._terminate(
                ExitCode.DEFECT,
                "claimed_task_unreadable",
                "a claimed task, its job or its handler is missing",
                context,
            )
        if not self._start(attempt, anchor, context, task.payload, job.kind):
            self._hold(attempt, returned)

    def _stopping_before_start(
        self, attempt: Attempt, returned: float, context: Mapping[str, str]
    ) -> bool:
        """True, holding ``attempt`` unstarted, if a stop is latched before its handler starts."""
        if not self._observe_stop():
            return False
        self._event(logging.WARNING, "claimed_task_not_started", "stopping", context)
        self._hold(attempt, returned)
        return True

    def _hold(self, attempt: Attempt, returned: float) -> None:
        """Claim nothing else until ``attempt``'s lease has certainly ended.

        The attempt was claimed but not started; it stays running in the
        database until recovery revokes it. ``returned`` is the local clock
        after the claim's answer arrived, later than the claim instant, so
        this waits at least until the real end of the lease.
        """
        lease_expires_at, _ = _lease(attempt)
        until = returned + (lease_expires_at - attempt.started_at).total_seconds()
        self._held = _Held(attempt, until)
        self._schedule.claim_due = until

    def _start(
        self,
        attempt: Attempt,
        anchor: float,
        context: Mapping[str, str],
        payload: JsonValue,
        kind: str,
    ) -> bool:
        """Start the handler under the ownership guard; False if it must not start.

        It must not start once a stop is latched (this is the last read before
        ``thread.start``), or if too little of the lease is left.
        """
        if self._observe_stop():
            self._event(logging.WARNING, "claimed_task_not_started", "stopping", context)
            return False
        lease_expires_at, _ = _lease(attempt)
        deadline = (
            local_deadline(anchor=anchor, started_at=attempt.started_at, instant=lease_expires_at)
            - self._margin()
        )
        if deadline <= self._clock():
            # Too little of the lease is left to run the handler safely.
            self._event(logging.ERROR, "claimed_task_not_started", "lease too short", context)
            return False
        token = self._watchdog.arm(Guard.OWNERSHIP, deadline, context)
        handler = self._registry[kind]
        cancel = threading.Event()  # this attempt's own, unset until a request is confirmed
        task_context = TaskContext(
            attempt.task_id,
            attempt.id,
            attempt.attempt_number,
            kind,
            CancellationSignal(cancel),
        )
        done = threading.Event()
        outcome: list[HandlerOutcome] = []
        wake = self._wake

        def execute() -> None:
            try:
                outcome.append(run_handler(handler, task_context, payload))
            finally:
                self._watchdog.disarm(token)  # the protected execution has ended
                done.set()
                wake.notify()  # the supervisor records the outcome at once

        thread = threading.Thread(
            target=execute, name=f"orqestra-handler-{attempt.id}", daemon=True
        )
        self._schedule.running = _Running(
            attempt=attempt,
            context=context,
            anchor=anchor,
            token=token,
            thread=thread,
            done=done,
            outcome=outcome,
            cancel=cancel,
            heartbeat_due=anchor + self._settings.heartbeat_interval.total_seconds(),
        )
        thread.start()
        self._event(logging.INFO, "handler_started", f"kind={kind}", context)
        return True

    # --- heartbeats -----------------------------------------------------------------

    def _heartbeat(self, running: _Running) -> None:
        attempt = running.attempt
        sent = self._clock()
        try:
            renewed = self._call(
                "renew_lease",
                lambda conn: renew_lease(
                    conn,
                    attempt.id,
                    generation=attempt.attempt_number,
                    worker_id=self.worker_id,
                    policy=self._policy,
                ),
                running.context,
            )
        except CancellationRequestedError as exc:
            # A committed request (renew_lease read it): the lease is not renewed
            # again and may now end sooner. Fence at the end of the grace period
            # first, so fencing holds whatever follows; then tell the handler.
            running.heartbeat_due = None
            self._watchdog.restrict(
                running.token, running.local(exc.lease_expires_at) - self._margin()
            )
            running.cancel.set()
            self._event(
                logging.WARNING,
                "cancellation_requested",
                "renewals stop; the handler is told",
                running.context,
            )
            return
        except _OWNERSHIP_LOST as exc:
            if running.done.is_set():
                running.heartbeat_due = None  # the handler has ended; the completion decides
                return
            self._terminate(
                ExitCode.FENCED,
                "ownership_lost",
                f"the attempt can no longer be renewed: {type(exc).__name__}",
                running.context,
            )
        except (psycopg.Error, ConnectionUnavailable) as exc:
            # Unconfirmed: the local deadline stays where the last confirmed
            # lease put it. Try again soon; the watchdog fences if this never succeeds.
            running.heartbeat_due = self._clock() + self._bound()
            self._event(
                logging.WARNING, "heartbeat_unconfirmed", type(exc).__name__, running.context
            )
            return
        running.attempt = renewed
        lease_expires_at, _ = _lease(renewed)
        self._watchdog.extend(running.token, running.local(lease_expires_at) - self._margin())
        running.heartbeat_due = sent + self._settings.heartbeat_interval.total_seconds()
        self._event(logging.DEBUG, "lease_renewed", "", running.context)

    # --- completion -------------------------------------------------------------------

    def _complete(self, running: _Running) -> None:
        running.thread.join(self._bound())
        if running.thread.is_alive() or not running.outcome:
            self._terminate(
                ExitCode.DEFECT,
                "handler_thread_unsettled",
                "the handler thread ended without an outcome",
                running.context,
            )
        outcome = running.outcome[0]
        if isinstance(outcome, ControlPlaneSignal):
            # Never classified and never recorded (DEC-009); recovery decides.
            self._terminate(
                ExitCode.HANDLER_ABORTED,
                "handler_aborted",
                f"the handler raised {outcome.signal_type}; nothing was recorded",
                running.context,
            )
        self._record(running, outcome)

    def _record(self, running: _Running, outcome: AttemptOutcome) -> None:
        """Record the outcome; after a lost answer, reconcile it from the evidence first."""
        if running.uncertain:
            evidence = self._read_evidence(running, outcome)
            if evidence is None:
                return  # the read failed; tried again later, within the deadline
            if evidence is CompletionEvidence.MATCHES:
                self._event(
                    logging.INFO, "completion_reconciled", "it had committed", running.context
                )
                self._settled()
                return
            if evidence is CompletionEvidence.REVOKED:
                self._event(logging.WARNING, "outcome_discarded", "revoked", running.context)
                self._settled()
                return
            if evidence is not CompletionEvidence.RUNNING:
                self._terminate(
                    ExitCode.DEFECT,
                    "completion_conflicts",
                    f"the recorded completion is {evidence.value}",
                    running.context,
                )
            # Still running: nothing committed, so write the same outcome again. A
            # stop requested during the read is latched before that write (DEC-049).
            self._observe_stop()
        attempt = running.attempt
        try:
            completion = self._call(
                "complete_attempt",
                lambda conn: complete_attempt(
                    conn, attempt.id, generation=attempt.attempt_number, outcome=outcome
                ),
                running.context,
            )
        except (AttemptRevokedError, StaleAttemptError, AttemptFinishedError, NotFoundError) as exc:
            if running.uncertain:
                # A rejection cannot tell an earlier write that committed from one
                # that did not (the generation is checked first): ask the evidence.
                running.completion_due = self._clock()
                return
            if isinstance(exc, AttemptRevokedError | StaleAttemptError):
                # Ownership ended before this completion: its outcome does not count.
                self._event(
                    logging.WARNING, "outcome_discarded", type(exc).__name__, running.context
                )
                self._settled()
                return
            self._terminate(
                ExitCode.DEFECT,
                "completion_refused",
                f"the completion was refused: {type(exc).__name__}",
                running.context,
            )
        except (ValueError, RuntimeError) as exc:
            self._terminate(
                ExitCode.DEFECT,
                "completion_refused",
                f"the completion was refused: {type(exc).__name__}",
                running.context,
            )
        except (psycopg.Error, ConnectionUnavailable) as exc:
            self._completion_failed(running, exc)
            return
        self._event(
            logging.INFO,
            "attempt_completed",
            f"attempt={completion.attempt.state.value} task={completion.task.state.value}",
            running.context,
        )
        self._settled()

    def _read_evidence(
        self, running: _Running, outcome: AttemptOutcome
    ) -> CompletionEvidence | None:
        """The completion evidence, or None after a failed read (retried later)."""
        attempt = running.attempt
        try:
            return self._call(
                "read_completion_evidence",
                lambda conn: read_completion_evidence(
                    conn, attempt.id, generation=attempt.attempt_number, outcome=outcome
                ),
                running.context,
            )
        except (psycopg.Error, ConnectionUnavailable) as exc:
            self._retry_later(running, "evidence_unread", type(exc).__name__)
            return None

    def _completion_failed(
        self, running: _Running, exc: psycopg.Error | ConnectionUnavailable
    ) -> None:
        kind = classify_completion_error(exc, connection_closed=self._conn is None)
        if kind is CompletionError.REJECTED:
            sqlstate = exc.sqlstate if isinstance(exc, psycopg.Error) else None
            self._terminate(
                ExitCode.DEFECT,
                "completion_rejected",
                f"the database rejected a valid outcome ({sqlstate})",
                running.context,
            )
        if kind is CompletionError.AMBIGUOUS:
            # The answer was lost: the write may have committed. Keep the outcome,
            # never rerun the handler, and reconcile from the evidence on a fresh
            # connection (DEC-047).
            first = not running.uncertain
            running.uncertain = True
            self._close()
            if first:  # always read the evidence once, at once, whatever the deadline
                running.completion_due = self._clock()
                self._event(logging.WARNING, "completion_ambiguous", "reconciling", running.context)
                return
            self._retry_later(running, "completion_ambiguous", "again")
            return
        self._retry_later(running, "completion_retry", type(exc).__name__)

    def _retry_later(self, running: _Running, event: str, detail: str) -> None:
        """Schedule the next completion step, or end the process once the deadline has passed.

        The bound is the attempt's execution deadline on the local clock. An
        outcome that may already have been recorded ends the process as
        ambiguous; one that certainly was not, as unrecorded.
        """
        _, deadline_at = _lease(running.attempt)
        if self._clock() >= running.local(deadline_at):
            if running.uncertain:
                self._terminate(
                    ExitCode.AMBIGUOUS,
                    "reconciliation_unresolved",
                    "whether the completion committed could not be established before the "
                    "attempt's deadline",
                    running.context,
                )
            self._terminate(
                ExitCode.UNRECORDED,
                "completion_unrecorded",
                "the outcome could not be recorded before the attempt's deadline",
                running.context,
            )
        running.completion_due = self._clock() + self._bound()
        self._event(logging.WARNING, event, detail, running.context)

    def _settled(self) -> None:
        self._schedule.running = None
        self._schedule.claim_due = self._clock()
        if self._drain_token is not None:
            # Settled while stopping: the drain guard has done its job (DEC-049).
            self._watchdog.disarm(self._drain_token)
            self._drain_token = self._drain_due = None

    # --- recovery -------------------------------------------------------------------------

    def _sweep(self) -> None:
        sweep = self._schedule.sweep
        if sweep.left == 0:
            sweep.started, sweep.left = self._clock(), self._settings.recovery_batch_limit
        try:
            revocation = self._call("revoke_next_expired", revoke_next_expired, {})
        except (psycopg.Error, ConnectionUnavailable) as exc:
            self._event(logging.WARNING, "sweep_failed", type(exc).__name__)
            revocation, sweep.left = None, 1  # end this sweep
        if revocation is not None:
            revoked = revocation.attempt
            self._event(
                logging.WARNING,
                "attempt_revoked",
                f"reason={revoked.revoked_reason} owner={revoked.worker_id}",
                self._context(revoked),
            )
        sweep.left -= 1
        if revocation is None or sweep.left == 0:
            sweep.left = 0
            sweep.due = sweep.started + self._settings.recovery_interval.total_seconds()
        else:
            sweep.due = self._clock()

    # --- database calls ---------------------------------------------------------------------

    def _call[T](self, operation: str, call: Callable[[Conn], T], context: Mapping[str, str]) -> T:
        """Run one supervisor database call under the watchdog's operation guard."""
        guard_context = {**self._base_context(), **context, "operation": operation}
        token = self._watchdog.arm(Guard.OPERATION, self._clock() + self._bound(), guard_context)
        try:
            if self._conn is None:
                try:
                    self._conn = self._connect()
                except psycopg.Error as exc:
                    raise ConnectionUnavailable(str(exc)) from exc
            return call(self._conn)
        finally:
            self._watchdog.disarm(token)
            if self._conn is not None and self._conn.closed:
                self._conn = None

    def _close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # --- helpers --------------------------------------------------------------------------------

    def _require_running(self) -> _Running:
        running = self._schedule.running
        if running is None:  # pragma: no cover - next_step schedules these only for a task
            msg = "no running task"
            raise RuntimeError(msg)
        return running

    def _bound(self) -> float:
        return self._settings.db_operation_timeout.total_seconds()

    def _margin(self) -> float:
        return self._settings.fence_margin.total_seconds()

    def _base_context(self) -> dict[str, str]:
        return {"worker_id": self.worker_id}

    def _context(self, attempt: Attempt) -> dict[str, str]:
        return {
            **self._base_context(),
            "task_id": str(attempt.task_id),
            "attempt_id": str(attempt.id),
        }

    def _event(
        self, level: int, event: str, detail: str = "", context: Mapping[str, str] | None = None
    ) -> None:
        extra = {**self._base_context(), **(context or {}), "event": event}
        logger.log(level, f"{event}: {detail}" if detail else event, extra=extra)
