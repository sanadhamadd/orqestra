"""The single-slot worker runtime: a supervisor and one handler thread at a time (DEC-045).

The supervisor runs on the calling thread and alone uses one autocommit
database connection. It claims a task of a supported kind, runs its handler on
a separate thread, renews the attempt's lease while the handler runs, sweeps
for expired attempts while idle or busy, and records the handler's outcome.
It never claims again until the previous task is settled.

Every supervisor database call is bounded by ``statement_timeout`` and, on the
client side, by the watchdog's operation guard; a running handler is bounded
by the ownership guard (``fence``). When the supervisor cannot establish that
it still owns a running task, or cannot tell what happened to a completion,
it ends the process: Day 3A recovery, or the completion that already
committed, then decides the outcome.

Not in this slice (Day 3B, later): telling the handler about a cancellation
request, reconciling an ambiguous completion (C4), graceful shutdown with
draining, and a command to run a worker.
"""

import logging
import math
import os
import socket
import threading
import time
import uuid
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
    LeaseExpiredError,
    NotFoundError,
    StaleAttemptError,
    WorkerMismatchError,
    claim_next_attempt,
    complete_attempt,
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
from orqestra.worker.settings import WorkerSettings

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


class Step(StrEnum):
    """What the supervisor does next."""

    HEARTBEAT = "heartbeat"
    COMPLETE = "complete"
    SWEEP = "sweep"
    CLAIM = "claim"
    STOP = "stop"
    WAIT = "wait"


@dataclass(frozen=True, slots=True)
class Timing:
    """The supervisor's schedule at one instant; all times are on the local clock."""

    now: float
    running: bool  # a handler thread is executing
    completing: bool  # a handler has finished and its outcome is not yet settled
    stop_requested: bool
    heartbeat_due: float | None  # None: the lease is no longer renewed
    completion_due: float
    sweep_due: float  # a sweep in progress is due at once
    claim_due: float
    sweep_reserve: float  # the time one sweep step may take (db_operation_timeout)


def next_step(timing: Timing) -> tuple[Step, float]:
    """The supervisor's next step, and for ``WAIT`` how many seconds to wait.

    In order: a finished handler's completion; a running handler's heartbeat;
    a recovery sweep step, which while a handler runs starts only if it can
    take its full ``sweep_reserve`` and still end before the next heartbeat is
    due, so sweeps never delay a heartbeat; then, only when idle, a stop or a
    claim. Every wait is finite.
    """
    t = timing
    if t.completing:
        if t.now >= t.completion_due:
            return Step.COMPLETE, 0.0
        return Step.WAIT, t.completion_due - t.now
    if t.running:
        heartbeat = t.heartbeat_due
        if heartbeat is not None and t.now >= heartbeat:
            return Step.HEARTBEAT, 0.0
        if t.now >= t.sweep_due and (heartbeat is None or heartbeat - t.now >= t.sweep_reserve):
            return Step.SWEEP, 0.0
        # A sweep held back for the heartbeat waits for it; otherwise wake for either.
        wakes = [] if heartbeat is None else [heartbeat]
        if t.sweep_due > t.now:
            wakes.append(t.sweep_due)
        return Step.WAIT, min(wakes) - t.now
    if t.stop_requested:
        return Step.STOP, 0.0
    if t.now >= t.sweep_due:
        return Step.SWEEP, 0.0
    if t.now >= t.claim_due:
        return Step.CLAIM, 0.0
    return Step.WAIT, min(t.sweep_due, t.claim_due) - t.now


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

    ``statement_timeout`` is ``db_operation_timeout`` in whole milliseconds,
    rounded up; ``connect_timeout`` is whole seconds, rounded up, and libpq
    treats anything under 2 as 2. Existing ``options``, such as a
    ``search_path``, are kept. These bound ordinary slow calls only: a stalled
    call is ended by the watchdog's operation guard.
    """
    timeout = settings.db_operation_timeout.total_seconds()
    existing = conninfo_to_dict(database_url).get("options")
    statement_timeout = f"-c statement_timeout={math.ceil(timeout * 1000)}"
    options = f"{existing} {statement_timeout}" if existing else statement_timeout
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
    heartbeat_due: float | None
    completion_due: float = 0.0

    def local(self, instant: datetime) -> float:
        """A database instant of this attempt on the local clock, never later than it."""
        return local_deadline(
            anchor=self.anchor, started_at=self.attempt.started_at, instant=instant
        )


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
    injectable for tests; by default ``terminate`` ends the process.
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

    def run(self, stop: threading.Event) -> None:
        """Work until ``stop`` is set while idle; a task already claimed is settled first."""
        self._watchdog.start()
        self._event(logging.INFO, "worker_started", f"kinds={list(self._registry.kinds)}")
        try:
            while True:
                step, seconds = next_step(self._timing(stop))
                if step is Step.STOP:
                    self._event(logging.INFO, "worker_stopped")
                    return
                if step is Step.WAIT:
                    running = self._schedule.running
                    (running.done if running is not None else stop).wait(seconds)
                elif step is Step.HEARTBEAT:
                    self._heartbeat(self._require_running())
                elif step is Step.COMPLETE:
                    self._complete(self._require_running())
                elif step is Step.SWEEP:
                    self._sweep()
                else:
                    self._claim()
        except BaseException:
            running = self._schedule.running
            if running is not None and not running.done.is_set():
                # Nothing would supervise the handler any more: end the process.
                self._terminate(
                    ExitCode.FENCED,
                    "supervisor_failed",
                    "the supervisor stopped while a handler ran",
                    running.context,
                )
            raise
        finally:
            self._close()
            self._watchdog.stop(self._bound())

    # --- scheduling -----------------------------------------------------------

    def _timing(self, stop: threading.Event) -> Timing:
        running = self._schedule.running
        busy = running is not None and not running.done.is_set()
        return Timing(
            now=self._clock(),
            running=busy,
            completing=running is not None and not busy,
            stop_requested=stop.is_set(),
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
                self._schedule.claim_due = self._clock() + self._policy.lease.total_seconds()
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
        try:
            task = self._call("read_task", lambda conn: get_task(conn, attempt.task_id), context)
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

    def _hold(self, attempt: Attempt, returned: float) -> None:
        """Claim nothing else until ``attempt``'s lease has certainly ended.

        The attempt was claimed but not started; it stays running in the
        database until recovery revokes it. ``returned`` is the local clock
        after the claim's answer arrived, later than the claim instant, so
        this waits at least until the real end of the lease.
        """
        lease_expires_at, _ = _lease(attempt)
        held = (lease_expires_at - attempt.started_at).total_seconds()
        self._schedule.claim_due = returned + held

    def _start(
        self,
        attempt: Attempt,
        anchor: float,
        context: Mapping[str, str],
        payload: JsonValue,
        kind: str,
    ) -> bool:
        """Start the handler under the ownership guard; False if too little lease is left."""
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
        task_context = TaskContext(
            attempt.task_id,
            attempt.id,
            attempt.attempt_number,
            kind,
            CancellationSignal(threading.Event()),  # never set in this slice
        )
        done = threading.Event()
        outcome: list[HandlerOutcome] = []

        def execute() -> None:
            try:
                outcome.append(run_handler(handler, task_context, payload))
            finally:
                self._watchdog.disarm(token)  # the protected execution has ended
                done.set()

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
            # The lease is not renewed again and may now end sooner: fence at
            # the end of the grace period. The handler is not told in this slice.
            running.heartbeat_due = None
            self._watchdog.restrict(
                running.token, running.local(exc.lease_expires_at) - self._margin()
            )
            self._event(logging.WARNING, "cancellation_requested", "renewals stop", running.context)
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
        attempt = running.attempt
        try:
            completion = self._call(
                "complete_attempt",
                lambda conn: complete_attempt(
                    conn, attempt.id, generation=attempt.attempt_number, outcome=outcome
                ),
                running.context,
            )
        except (AttemptRevokedError, StaleAttemptError) as exc:
            # Ownership ended before this completion: its outcome does not count.
            self._event(logging.WARNING, "outcome_discarded", type(exc).__name__, running.context)
            self._settled()
            return
        except (AttemptFinishedError, NotFoundError, ValueError, RuntimeError) as exc:
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

    def _completion_failed(
        self, running: _Running, exc: psycopg.Error | ConnectionUnavailable
    ) -> None:
        kind = classify_completion_error(exc, connection_closed=self._conn is None)
        if kind is CompletionError.AMBIGUOUS:
            # Slice 1B-B1 policy: no reconciliation, no rerun, no further claim.
            # Either the completion committed, or recovery decides.
            self._terminate(
                ExitCode.AMBIGUOUS,
                "completion_ambiguous",
                "the completion's acknowledgement was lost; its outcome is unknown here",
                running.context,
            )
        if kind is CompletionError.REJECTED:
            sqlstate = exc.sqlstate if isinstance(exc, psycopg.Error) else None
            self._terminate(
                ExitCode.DEFECT,
                "completion_rejected",
                f"the database rejected a valid outcome ({sqlstate})",
                running.context,
            )
        _, deadline_at = _lease(running.attempt)
        if self._clock() >= running.local(deadline_at):
            self._terminate(
                ExitCode.UNRECORDED,
                "completion_unrecorded",
                "the outcome could not be recorded before the attempt's deadline",
                running.context,
            )
        running.completion_due = self._clock() + self._bound()
        self._event(logging.WARNING, "completion_retry", type(exc).__name__, running.context)

    def _settled(self) -> None:
        self._schedule.running = None
        self._schedule.claim_due = self._clock()

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
