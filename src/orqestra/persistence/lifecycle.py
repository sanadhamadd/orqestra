"""Transactional attempt lifecycle: claim, start, renew, complete, revoke and cancel.

Each write is one short READ COMMITTED transaction that the operation owns
(DEC-028). It locks the task row first and then, when it needs it, the attempt
row; decides with the domain policy and transition tables; writes; and
commits. No transaction stays open while a worker executes: a claim commits
before the work begins, and each heartbeat and the completion are separate
transactions. ``renew_lease`` is the one exception to the lock order: it
locks only the attempt row, reads the task without a lock, and never waits
for a task lock while holding the attempt, so it cannot form a lock cycle
with the task-first operations (DEC-036).

Ownership (ADR-0003, DEC-029): attempt N holds generation N, the value of the
task's ``attempt_count`` when it started. ``complete_attempt`` and
``renew_lease`` take effect only for the attempt that is still the task's
current, running generation. Any other write is rejected with a typed error
and changes nothing. Rejections are expected concurrency outcomes, not task
failures (DEC-009).

Leases (DEC-034, DEC-035): every claim gives its attempt a lease and a
non-renewable execution deadline, both measured from one claim instant that
the claim's task UPDATE reads after the task lock is held, so a claim that
waited for that lock still gets its full lease. ``renew_lease`` extends the
lease up to the deadline. ``revoke_next_expired`` revokes one attempt whose
lease has expired and moves its task by the policy (DEC-037, DEC-038); from
that commit on, the old worker's completions and heartbeats are rejected. A
completion that commits before the revocation still counts. Every instant
that decides expiry comes from the database clock, read after the locks the
decision depends on.

Cancellation (DEC-020, DEC-040): ``request_cancellation`` takes a grace
period. For a running task it locks the task, then its running attempt, and
in that transaction records the request and sets the attempt's lease to end
one grace period later, capped at the deadline, unless the lease had already
expired. From that commit on, heartbeats are refused with
``CancellationRequestedError``; the worker may still complete the attempt
(as cancelled, or with its real result) until recovery revokes it, and
recovery then cancels the task whatever retry budget is left.

Reconciliation (DEC-047): when a completion's answer is lost,
``read_completion_evidence`` reads the attempt and its task in one statement
and says whether that completion committed, the attempt is still running, or
recovery revoked it, comparing a result with PostgreSQL's ``jsonb`` equality.

These operations are building blocks for at-least-once execution. The worker
runtime (``orqestra.worker.runtime``) runs them; revoking an attempt in the
database does not by itself stop its worker.
"""

import logging
import uuid
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from orqestra.domain.errors import OrqestraError
from orqestra.domain.jobs import (
    Attempt,
    AttemptFailed,
    AttemptOutcome,
    AttemptSucceeded,
    Task,
)
from orqestra.domain.kinds import check_job_kinds
from orqestra.domain.leases import (
    LeasePolicy,
    RevocationReason,
    cancellation_lease,
    check_duration,
    initial_lease,
    renewed_lease,
    revocation_reason,
)
from orqestra.domain.policy import FailureKind, next_task_state, state_after_cancel_request
from orqestra.domain.retry import RetryPolicy, retry_at
from orqestra.domain.states import (
    TERMINAL_TASK_STATES,
    AttemptState,
    TaskState,
    allowed_sources,
    can_transition,
    transition,
    transition_attempt,
)
from orqestra.persistence._shared import (
    ATTEMPT_COLUMNS,
    TASK_COLUMNS,
    Conn,
    Cursor,
    attempt_from_row,
    owned_transaction,
    task_from_row,
    tuple_cursor,
)

logger = logging.getLogger(__name__)

_LOCK_TASK = f"SELECT {TASK_COLUMNS} FROM tasks WHERE id = %s FOR UPDATE"
# attempts.task_id never changes, so reading it unlocked to find the task to
# lock first is safe; the attempt row is locked by the next statement.
_LOCK_TASK_OF_ATTEMPT = (
    f"SELECT {TASK_COLUMNS} FROM tasks "
    "WHERE id = (SELECT task_id FROM attempts WHERE id = %s) FOR UPDATE"
)
_LOCK_ATTEMPT = f"SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE id = %s FOR UPDATE"
_LOCK_CURRENT_ATTEMPT = (
    f"SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE task_id = %s AND attempt_number = %s FOR UPDATE"
)
# Discovery (DEC-033): lock the first claimable task of a supported job kind
# that no other transaction has locked. Rows locked elsewhere are skipped, not
# waited for, and a row that changed since the statement's snapshot is
# re-checked against the WHERE clause once locked, so a task claimed or
# cancelled meanwhile is never returned. Only the task row is locked: the job
# is read by the subquery, which takes no row lock and is not re-checked. That
# relies on a job's kind never changing; no code path updates it, but the
# database does not forbid it.
# A queued task may be claimed once the database clock reaches its available_at,
# inclusively (DEC-053). Both claim paths test it at the statement that decides.
_ELIGIBLE = "available_at <= {now}"
_LOCK_NEXT_CLAIMABLE_TASK = (
    f"SELECT {TASK_COLUMNS} FROM tasks WHERE state = ANY(%s) "
    f"AND {_ELIGIBLE.format(now='statement_timestamp()')} "
    "AND EXISTS (SELECT FROM jobs WHERE jobs.id = tasks.job_id AND jobs.kind = ANY(%s)) "
    "ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
)
_CLAIMABLE_STATES = [state.value for state in allowed_sources(TaskState.RUNNING)]

# The database clock at the start of the current statement. Operations read it
# after taking the locks a decision depends on, so it is never older than the
# rows it is compared with, however long they waited for those locks (DEC-035).
_NOW = "SELECT statement_timestamp()"
# The claim instant: this statement runs after the task row is locked, so its
# statement_timestamp() excludes any wait for that lock. The attempt's
# started_at, lease and deadline are all derived from the same instant.
# The same instant decides eligibility: an ineligible task updates no row.
_CLAIM_TASK = (
    "UPDATE tasks SET state = %s, attempt_count = attempt_count + 1, "
    "updated_at = statement_timestamp() "
    f"WHERE id = %s AND {_ELIGIBLE.format(now='statement_timestamp()')} "
    "RETURNING attempt_count, updated_at"
)
_AVAILABLE_AT = "SELECT available_at FROM tasks WHERE id = %s"
_INSERT_ATTEMPT = (
    "INSERT INTO attempts "
    "(id, task_id, attempt_number, worker_id, started_at, lease_expires_at, deadline_at) "
    f"VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING {ATTEMPT_COLUMNS}"
)
_FINISH_ATTEMPT = (
    "UPDATE attempts SET state = %s, failure_kind = %s, error_type = %s, error_message = %s, "
    f"finished_at = now() WHERE id = %s RETURNING {ATTEMPT_COLUMNS}"
)
# available_at changes only when the task is requeued (otherwise NULL keeps it).
_FINISH_TASK = (
    "UPDATE tasks SET state = %s, result = %s, updated_at = now(), "
    "finished_at = CASE WHEN %s THEN now() END, available_at = COALESCE(%s, available_at) "
    f"WHERE id = %s RETURNING {TASK_COLUMNS}"
)
# A request's instant is this statement's timestamp, taken after the task row
# (and, for a running task, its attempt row) is locked, like the claim instant.
_CANCEL_QUEUED_TASK = (
    "UPDATE tasks SET state = %s, cancel_requested_at = statement_timestamp(), "
    "finished_at = statement_timestamp(), updated_at = statement_timestamp() "
    f"WHERE id = %s RETURNING {TASK_COLUMNS}"
)
_RECORD_CANCEL_REQUEST = (
    "UPDATE tasks SET cancel_requested_at = statement_timestamp(), "
    f"updated_at = statement_timestamp() WHERE id = %s RETURNING {TASK_COLUMNS}"
)
# Read without a lock, only to describe a rejected heartbeat: renew_lease never
# locks the task (DEC-036).
_TASK_GENERATION = "SELECT attempt_count FROM tasks WHERE id = %s"
# The heartbeat's clock reading and the task's cancellation request, read
# together without locking the task, after the attempt row is locked. A request
# that committed before this statement is seen; one that has not committed yet
# is waiting for the attempt lock this heartbeat holds, and will reset the
# lease this heartbeat writes (DEC-036, DEC-040).
_NOW_AND_CANCELLATION = "SELECT statement_timestamp(), cancel_requested_at FROM tasks WHERE id = %s"
_SET_LEASE = f"UPDATE attempts SET lease_expires_at = %s WHERE id = %s RETURNING {ATTEMPT_COLUMNS}"
# Recovery (DEC-037): lock the task of the most overdue running attempt that no
# other transaction has locked; the attempt itself is locked and every
# condition re-checked by the next statements. The literal states let the
# planner use the partial index attempts_one_running_per_task.
_LOCK_NEXT_EXPIRED_TASK = (
    "SELECT {task_columns}, a.id FROM tasks t JOIN attempts a "
    "ON a.task_id = t.id AND a.attempt_number = t.attempt_count "
    "WHERE t.state = '{running_task}' AND a.state = '{running_attempt}' "
    "AND a.lease_expires_at <= statement_timestamp() "
    "ORDER BY a.lease_expires_at, a.id LIMIT 1 FOR UPDATE OF t SKIP LOCKED"
).format(
    task_columns=", ".join(f"t.{column}" for column in TASK_COLUMNS.split(", ")),
    running_task=TaskState.RUNNING.value,
    running_attempt=AttemptState.RUNNING.value,
)
_REVOKE_ATTEMPT = (
    "UPDATE attempts SET state = %s, revoked_reason = %s, finished_at = %s "
    f"WHERE id = %s RETURNING {ATTEMPT_COLUMNS}"
)
_RECOVER_TASK = (
    "UPDATE tasks SET state = %s, updated_at = %s, finished_at = %s, "
    "available_at = COALESCE(%s, available_at) "
    f"WHERE id = %s RETURNING {TASK_COLUMNS}"
)


class NotFoundError(OrqestraError, LookupError):
    """The task or attempt a write refers to does not exist."""

    def __init__(self, kind: str, record_id: uuid.UUID) -> None:
        super().__init__(f"{kind} {record_id} does not exist")
        self.kind = kind
        self.record_id = record_id


class TaskNotStartableError(OrqestraError):
    """The task is not queued with attempts left, so no attempt can start."""

    def __init__(self, task_id: uuid.UUID, state: TaskState) -> None:
        super().__init__(f"task {task_id} cannot start an attempt from state {state.value!r}")
        self.task_id = task_id
        self.state = state


class TaskNotEligibleError(TaskNotStartableError):
    """The task is queued, but its retry delay has not passed: no attempt can start yet.

    ``available_at`` is the database time from which it may start (DEC-053).
    """

    def __init__(self, task_id: uuid.UUID, available_at: datetime) -> None:
        OrqestraError.__init__(
            self, f"task {task_id} is queued but not eligible until {available_at.isoformat()}"
        )
        self.task_id = task_id
        self.state = TaskState.QUEUED
        self.available_at = available_at


class StaleAttemptError(OrqestraError):
    """The write does not hold the task's current generation; it was rejected."""

    def __init__(self, attempt_id: uuid.UUID, generation: int, current_generation: int) -> None:
        super().__init__(
            f"attempt {attempt_id} presented generation {generation}, but the task's "
            f"current generation is {current_generation}"
        )
        self.attempt_id = attempt_id
        self.generation = generation
        self.current_generation = current_generation


class AttemptFinishedError(OrqestraError):
    """The attempt already ended; its recorded outcome stands."""

    def __init__(self, attempt_id: uuid.UUID, state: AttemptState) -> None:
        super().__init__(f"attempt {attempt_id} already finished as {state.value!r}")
        self.attempt_id = attempt_id
        self.state = state


class AttemptRevokedError(AttemptFinishedError):
    """Recovery revoked the attempt: its worker lost ownership and nothing it reports counts.

    Unlike its base class, this does not mean the worker's own outcome was
    recorded: the attempt ended as ``REVOKED``, for ``reason``.
    """

    def __init__(self, attempt_id: uuid.UUID, reason: RevocationReason) -> None:
        OrqestraError.__init__(
            self, f"attempt {attempt_id} was revoked ({reason.value}); it no longer owns its task"
        )
        self.attempt_id = attempt_id
        self.state = AttemptState.REVOKED
        self.reason = reason


class LeaseExpiredError(OrqestraError):
    """The attempt's lease has expired, so it cannot be renewed; recovery may revoke it.

    The worker should stop: its attempt still holds the task until recovery
    commits a revocation, but it can no longer extend that.
    """

    def __init__(
        self,
        attempt_id: uuid.UUID,
        reason: RevocationReason,
        lease_expires_at: datetime,
        deadline_at: datetime,
    ) -> None:
        super().__init__(
            f"attempt {attempt_id}'s lease expired at {lease_expires_at.isoformat()} "
            f"({reason.value}); it cannot be renewed"
        )
        self.attempt_id = attempt_id
        self.reason = reason
        self.lease_expires_at = lease_expires_at
        self.deadline_at = deadline_at


class CancellationRequestedError(OrqestraError):
    """Cancellation of the attempt's task was requested, so its lease is no longer renewed.

    The worker should stop and complete the attempt as cancelled before
    ``lease_expires_at``, the end of its grace period: it still owns the task
    until recovery commits a revocation, but it can no longer extend that.
    """

    def __init__(
        self, attempt_id: uuid.UUID, cancel_requested_at: datetime, lease_expires_at: datetime
    ) -> None:
        super().__init__(
            f"cancellation of attempt {attempt_id}'s task was requested at "
            f"{cancel_requested_at.isoformat()}; its lease is no longer renewed and ends at "
            f"{lease_expires_at.isoformat()}"
        )
        self.attempt_id = attempt_id
        self.cancel_requested_at = cancel_requested_at
        self.lease_expires_at = lease_expires_at


class WorkerMismatchError(OrqestraError):
    """The attempt belongs to a different worker than the one that presented it."""

    def __init__(self, attempt_id: uuid.UUID, worker_id: str, owner: str) -> None:
        super().__init__(f"attempt {attempt_id} belongs to worker {owner!r}, not {worker_id!r}")
        self.attempt_id = attempt_id
        self.worker_id = worker_id
        self.owner = owner


@dataclass(frozen=True, slots=True)
class AttemptCompletion:
    """The finished attempt and its task as stored by ``complete_attempt``."""

    attempt: Attempt
    task: Task


@dataclass(frozen=True, slots=True)
class AttemptRevocation:
    """The revoked attempt and its task as stored by ``revoke_next_expired``."""

    attempt: Attempt
    task: Task


class CancellationOutcome(StrEnum):
    """What a cancellation request did to its task."""

    CANCELLED = "cancelled"  # the task was queued and is now cancelled
    REQUESTED = "requested"  # the task is running; the request is recorded
    ALREADY_REQUESTED = "already_requested"  # running, with an earlier request
    ALREADY_FINISHED = "already_finished"  # terminal; nothing changed


@dataclass(frozen=True, slots=True)
class CancellationResult:
    outcome: CancellationOutcome
    task: Task


def start_attempt(
    conn: Conn, task_id: uuid.UUID, worker_id: str, *, policy: LeasePolicy
) -> Attempt:
    """Start the next attempt of a queued task and return it.

    In one transaction: lock the task, then claim it (see ``_claim``) with a
    lease and deadline from ``policy``. Competing callers serialise on the
    task's row lock, so exactly one of them starts each generation; the others
    get ``TaskNotStartableError``. A queued task whose retry delay has not
    passed raises ``TaskNotEligibleError``, a subclass, with nothing changed.
    Raises ``NotFoundError`` for an unknown task, and ``ValueError`` if the
    deadline is not representable.
    """
    with owned_transaction(conn, "start_attempt") as cur:
        task = _lock_task(cur, _LOCK_TASK, task_id, "task")
        attempt = _claim(cur, task, worker_id, policy)
    _log_started(attempt)
    return attempt


def claim_next_attempt(
    conn: Conn, worker_id: str, *, kinds: Collection[str], policy: LeasePolicy
) -> Attempt | None:
    """Claim the next claimable task of a supported kind, start its attempt and return it.

    ``kinds`` names the job kinds the caller can run. It must be a non-empty
    collection of strings (a bare string is refused, since it would be read as
    its characters), each a kind a job could have: not blank in the sense of
    ``jobs_kind_not_blank`` (only spaces count, as for ``btrim``, so a tab or a
    newline is a kind), and without a NUL character, which PostgreSQL text
    cannot hold. Kinds match job kinds exactly. They are checked before
    anything is sent to the database.

    In one transaction: lock the first queued task of one of those kinds whose
    ``available_at`` has been reached, by ``created_at`` then ``id``, that no
    other transaction has locked, then
    claim it exactly as ``start_attempt`` does. Tasks of other kinds are never
    locked or claimed, so they keep their place and their retry budget, and
    never hold up supported ones. It never waits for a row lock: tasks being
    claimed or cancelled elsewhere are skipped. The order is therefore a
    preference, not strict FIFO, and ``None`` means "nothing of these kinds
    claimable right now", which includes queued tasks that are all locked by
    others.
    """
    supported = check_job_kinds(kinds)
    with owned_transaction(conn, "claim_next_attempt") as cur:
        row = cur.execute(_LOCK_NEXT_CLAIMABLE_TASK, (_CLAIMABLE_STATES, supported)).fetchone()
        if row is None:
            return None
        attempt = _claim(cur, task_from_row(row), worker_id, policy)
    _log_started(attempt)
    return attempt


def _claim(cur: Cursor, task: Task, worker_id: str, policy: LeasePolicy) -> Attempt:
    """Claim a task this transaction has locked, and insert its running attempt.

    Checks with the transition table that the task may move to ``running``
    (only a queued task may), advances ``attempt_count`` by one (the fencing
    trigger rejects any other change) and inserts the attempt whose number is
    the new generation, started at the claim instant with its lease and
    deadline. tasks_queued_has_attempts_left guarantees a queued task has an
    attempt left, so this cannot exceed ``max_attempts``. The claim itself
    requires ``available_at`` to have been reached at its own instant, so no
    caller can start a task early (DEC-053); a refused claim changes nothing.
    """
    if not can_transition(task.state, TaskState.RUNNING):
        raise TaskNotStartableError(task.id, task.state)
    claimed = cur.execute(_CLAIM_TASK, (TaskState.RUNNING.value, task.id)).fetchone()
    if claimed is None:
        (not_before,) = _one(cur.execute(_AVAILABLE_AT, (task.id,)))
        raise TaskNotEligibleError(task.id, not_before)
    generation, claimed_at = claimed
    lease_expires_at, deadline_at = initial_lease(policy, claimed_at)
    row = _one(
        cur.execute(
            _INSERT_ATTEMPT,
            (
                uuid.uuid4(),
                task.id,
                generation,
                worker_id,
                claimed_at,
                lease_expires_at,
                deadline_at,
            ),
        )
    )
    return attempt_from_row(row)


def _log_started(attempt: Attempt) -> None:
    logger.info(
        "attempt started",
        extra={
            "task_id": str(attempt.task_id),
            "attempt_id": str(attempt.id),
            "worker_id": attempt.worker_id,
        },
    )


def complete_attempt(
    conn: Conn,
    attempt_id: uuid.UUID,
    *,
    generation: int,
    outcome: AttemptOutcome,
    retry_policy: RetryPolicy,
) -> AttemptCompletion:
    """Record how an attempt ended and move its task accordingly.

    ``generation`` is the attempt's number as returned by ``start_attempt``.
    The write takes effect only if the attempt still holds the task's current
    generation and is running; then the attempt and the task are updated
    together, with the task's next state chosen by ``next_task_state``.
    A succeeded attempt's result becomes the task's result. An attempt whose
    lease expired still completes until recovery has committed its revocation.
    A task requeued by a transient failure becomes eligible again after
    ``retry_policy``'s delay, from the database's clock read after the locks
    (DEC-053); no other outcome changes its ``available_at``.

    Raises, without changing anything: ``StaleAttemptError`` when the
    generation does not match the attempt or a newer generation exists;
    ``AttemptRevokedError`` when recovery revoked the attempt;
    ``AttemptFinishedError`` when the attempt already ended otherwise (a
    duplicate completion); ``NotFoundError`` for an unknown attempt;
    ``ValueError`` from the policy for a cancelled outcome without a
    cancellation request.
    """
    with owned_transaction(conn, "complete_attempt") as cur:
        task = _lock_task(cur, _LOCK_TASK_OF_ATTEMPT, attempt_id, "attempt")
        current = attempt_from_row(_locked(cur, _LOCK_ATTEMPT, attempt_id, "attempt"))
        if generation != current.attempt_number or generation != task.attempt_count:
            raise StaleAttemptError(attempt_id, generation, task.attempt_count)
        if current.state is not AttemptState.RUNNING:
            raise _finished(current)
        if task.state is not TaskState.RUNNING:
            msg = (
                f"invariant violated: attempt {attempt_id} is running at the current "
                f"generation but task {task.id} is {task.state.value!r}"
            )
            raise RuntimeError(msg)

        failure = outcome if isinstance(outcome, AttemptFailed) else None
        next_state = transition(
            TaskState.RUNNING,
            next_task_state(
                outcome.state,
                None if failure is None else failure.kind,
                attempt_count=task.attempt_count,
                max_attempts=task.max_attempts,
                cancel_requested=task.cancel_requested_at is not None,
            ),
        )
        attempt_state = transition_attempt(AttemptState.RUNNING, outcome.state)
        not_before = (
            retry_at(retry_policy, task.id, task.attempt_count, _now(cur))
            if next_state is TaskState.QUEUED
            else None
        )
        attempt_row = _one(
            cur.execute(
                _FINISH_ATTEMPT,
                (
                    attempt_state.value,
                    None if failure is None else failure.kind.value,
                    None if failure is None else failure.error_type,
                    None if failure is None else failure.error_message,
                    attempt_id,
                ),
            )
        )
        # Jsonb(None) is JSON null, a result; SQL NULL means "no result" (DEC-030).
        result = Jsonb(outcome.result) if isinstance(outcome, AttemptSucceeded) else None
        task_row = _one(
            cur.execute(
                _FINISH_TASK,
                (
                    next_state.value,
                    result,
                    next_state in TERMINAL_TASK_STATES,
                    not_before,
                    task.id,
                ),
            )
        )
        completion = AttemptCompletion(attempt_from_row(attempt_row), task_from_row(task_row))
    logger.info(
        "attempt completed",
        extra={"task_id": str(task.id), "attempt_id": str(attempt_id)},
    )
    return completion


def renew_lease(
    conn: Conn,
    attempt_id: uuid.UUID,
    *,
    generation: int,
    worker_id: str,
    policy: LeasePolicy,
) -> Attempt:
    """Extend a running attempt's lease (a heartbeat) and return the attempt.

    The new lease is ``policy.lease`` from now, capped at the attempt's
    deadline, and never shorter than the current one; the deadline itself
    never moves. In one transaction that locks only the attempt row and reads
    the task without a lock (DEC-036).

    Raises, without changing anything: ``NotFoundError`` for an unknown
    attempt; ``StaleAttemptError`` when ``generation`` is not the attempt's;
    ``WorkerMismatchError`` when ``worker_id`` does not own it;
    ``AttemptRevokedError`` when recovery revoked it; ``AttemptFinishedError``
    when it already ended otherwise; ``LeaseExpiredError`` when its lease has
    expired, including at the deadline or at the end of a cancellation grace
    period; ``CancellationRequestedError`` when cancellation of its task was
    requested (DEC-040). A heartbeat therefore cannot revive an expired lease
    or a revoked attempt, nor extend a lease once a cancellation request has
    committed.
    """
    with owned_transaction(conn, "renew_lease") as cur:
        current = attempt_from_row(_locked(cur, _LOCK_ATTEMPT, attempt_id, "attempt"))
        if generation != current.attempt_number:
            (current_generation,) = _one(cur.execute(_TASK_GENERATION, (current.task_id,)))
            raise StaleAttemptError(attempt_id, generation, current_generation)
        if worker_id != current.worker_id:
            raise WorkerMismatchError(attempt_id, worker_id, current.worker_id)
        if current.state is not AttemptState.RUNNING:
            raise _finished(current)
        lease_expires_at, deadline_at = _lease_of(current)
        now, cancel_requested_at = _one(cur.execute(_NOW_AND_CANCELLATION, (current.task_id,)))
        reason = revocation_reason(
            lease_expires_at=lease_expires_at,
            deadline_at=deadline_at,
            cancel_requested_at=cancel_requested_at,
            now=now,
        )
        if reason is not None:
            raise LeaseExpiredError(attempt_id, reason, lease_expires_at, deadline_at)
        if cancel_requested_at is not None:
            raise CancellationRequestedError(attempt_id, cancel_requested_at, lease_expires_at)
        renewed = renewed_lease(
            policy, now=now, lease_expires_at=lease_expires_at, deadline_at=deadline_at
        )
        attempt = attempt_from_row(_one(cur.execute(_SET_LEASE, (renewed, attempt_id))))
    logger.debug(
        "lease renewed",
        extra={"task_id": str(attempt.task_id), "attempt_id": str(attempt_id)},
    )
    return attempt


def revoke_next_expired(conn: Conn, *, retry_policy: RetryPolicy) -> AttemptRevocation | None:
    """Revoke one running attempt whose lease has expired, and move its task.

    In one transaction: lock the task of the most overdue expired attempt that
    no other transaction has locked, then the attempt; re-check against the
    committed rows and the current database time that the attempt is still
    the task's running generation and that its lease has expired; then record
    the attempt as ``REVOKED`` with the reason its stored lease, deadline and
    cancellation request show, and move the task as ``next_task_state``
    decides for that reason (DEC-038, DEC-040). Ownership ends when this
    commits. A task requeued by a ``lease_expired`` or ``deadline_exceeded``
    revocation becomes eligible again after ``retry_policy``'s delay from that
    database time (DEC-053); a cancelled or failed one is not delayed.

    Returns the revoked attempt and its task, or ``None`` when this call
    revoked nothing: no expired attempt was unlocked, or the one found was
    renewed before it could be locked. It revokes at most one attempt;
    sweeping repeatedly is the caller's job.
    """
    with owned_transaction(conn, "revoke_next_expired") as cur:
        row = cur.execute(_LOCK_NEXT_EXPIRED_TASK).fetchone()
        if row is None:
            return None
        *task_columns, attempt_id = row
        task = task_from_row(tuple(task_columns))
        current = attempt_from_row(_one(cur.execute(_LOCK_ATTEMPT, (attempt_id,))))
        if (  # pragma: no cover - the task lock and its re-checked join exclude this
            current.state is not AttemptState.RUNNING
            or current.attempt_number != task.attempt_count
        ):
            msg = (
                f"invariant violated: task {task.id} is running at generation "
                f"{task.attempt_count} but attempt {current.id} is {current.state.value!r} "
                f"at generation {current.attempt_number}"
            )
            raise RuntimeError(msg)
        lease_expires_at, deadline_at = _lease_of(current)
        now = _now(cur)
        reason = revocation_reason(
            lease_expires_at=lease_expires_at,
            deadline_at=deadline_at,
            cancel_requested_at=task.cancel_requested_at,
            now=now,
        )
        if reason is None:
            return None  # a heartbeat renewed the lease after discovery saw it expired
        next_state = transition(
            TaskState.RUNNING,
            next_task_state(
                AttemptState.REVOKED,
                None,
                attempt_count=task.attempt_count,
                max_attempts=task.max_attempts,
                cancel_requested=task.cancel_requested_at is not None,
                revocation_reason=reason,
            ),
        )
        attempt_state = transition_attempt(AttemptState.RUNNING, AttemptState.REVOKED)
        attempt_row = _one(
            cur.execute(_REVOKE_ATTEMPT, (attempt_state.value, reason.value, now, current.id))
        )
        finished_at = now if next_state in TERMINAL_TASK_STATES else None
        not_before = (
            retry_at(retry_policy, task.id, task.attempt_count, now)
            if next_state is TaskState.QUEUED
            else None
        )
        task_row = _one(
            cur.execute(_RECOVER_TASK, (next_state.value, now, finished_at, not_before, task.id))
        )
        revocation = AttemptRevocation(attempt_from_row(attempt_row), task_from_row(task_row))
    logger.warning(
        "attempt revoked: %s",
        reason.value,
        extra={
            "task_id": str(task.id),
            "attempt_id": str(current.id),
            "worker_id": current.worker_id,
        },
    )
    return revocation


def request_cancellation(conn: Conn, task_id: uuid.UUID, *, grace: timedelta) -> CancellationResult:
    """Request cancellation of a task (cooperative, with a bounded grace period).

    ``grace`` must be a positive ``timedelta``; it is checked before anything
    is written, whatever the task's state. A queued task is cancelled at once.
    For a running task, in one transaction: lock the task, then its running
    attempt; record the request at one instant taken after those locks; and,
    if the attempt's lease still holds, set it to end ``grace`` after the
    request, capped at the deadline (DEC-040). The attempt's generation,
    worker, deadline and state do not change. Its worker may still complete it
    until recovery revokes it once that lease ends.

    Repeated requests and requests for finished tasks change nothing; a
    repeated request does not extend the grace period. Raises
    ``NotFoundError`` for an unknown task.
    """
    check_duration("grace", grace)
    with owned_transaction(conn, "request_cancellation") as cur:
        task = _lock_task(cur, _LOCK_TASK, task_id, "task")
        target = state_after_cancel_request(task.state)
        if target is not task.state:
            transition(task.state, target)
            row = _one(cur.execute(_CANCEL_QUEUED_TASK, (target.value, task_id)))
            outcome = CancellationOutcome.CANCELLED
        elif task.state in TERMINAL_TASK_STATES:
            row, outcome = None, CancellationOutcome.ALREADY_FINISHED
        elif task.cancel_requested_at is not None:
            row, outcome = None, CancellationOutcome.ALREADY_REQUESTED
        else:
            row = _record_running_request(cur, task, grace)
            outcome = CancellationOutcome.REQUESTED
        result = CancellationResult(outcome, task if row is None else task_from_row(row))
    logger.info("cancellation request: %s", outcome.value, extra={"task_id": str(task_id)})
    return result


def _record_running_request(cur: Cursor, task: Task, grace: timedelta) -> TupleRow:
    """Record a request for a running task this transaction has locked; return its row.

    Locks the task's current attempt, so that a heartbeat in progress commits
    first and the lease it renewed is the one reset here, and any later
    heartbeat sees the request.
    """
    row = cur.execute(_LOCK_CURRENT_ATTEMPT, (task.id, task.attempt_count)).fetchone()
    current = None if row is None else attempt_from_row(row)
    if current is None or current.state is not AttemptState.RUNNING:
        msg = (
            f"invariant violated: task {task.id} is running at generation "
            f"{task.attempt_count} without a running attempt"
        )
        raise RuntimeError(msg)
    lease_expires_at, deadline_at = _lease_of(current)
    task_row = _one(cur.execute(_RECORD_CANCEL_REQUEST, (task.id,)))
    requested_at = task_from_row(task_row).cancel_requested_at
    if requested_at is None:  # pragma: no cover - the statement above sets it
        msg = f"cancellation request for task {task.id} was not recorded"
        raise RuntimeError(msg)
    granted = cancellation_lease(
        requested_at=requested_at,
        grace=grace,
        lease_expires_at=lease_expires_at,
        deadline_at=deadline_at,
    )
    if granted != lease_expires_at:
        cur.execute(_SET_LEASE, (granted, current.id))
    return task_row


def get_attempt(conn: Conn, attempt_id: uuid.UUID) -> Attempt | None:
    """Return the attempt with ``attempt_id``, or ``None``."""
    with tuple_cursor(conn) as cur:
        row = cur.execute(
            f"SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE id = %s", (attempt_id,)
        ).fetchone()
    return None if row is None else attempt_from_row(row)


def list_attempts(conn: Conn, task_id: uuid.UUID) -> tuple[Attempt, ...]:
    """Return a task's attempts ordered by attempt number; empty for an unknown task."""
    with tuple_cursor(conn) as cur:
        rows = cur.execute(
            f"SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE task_id = %s ORDER BY attempt_number",
            (task_id,),
        ).fetchall()
    return tuple(attempt_from_row(row) for row in rows)


class CompletionEvidence(StrEnum):
    """What PostgreSQL shows about an attempt whose completion may or may not have committed."""

    RUNNING = "running"  # still running: no completion of it has committed
    REVOKED = "revoked"  # recovery ended it: no completion of it counts
    MATCHES = "matches"  # it ended with exactly the given outcome
    CONFLICTS = "conflicts"  # it ended, but not with the given outcome
    INCONSISTENT = "inconsistent"  # no such attempt, or its records contradict each other


# One statement, so the attempt and its task come from one snapshot. The result
# is compared by PostgreSQL's jsonb equality: Python's would call True equal to
# 1, and 1e308 unequal to the 309-digit number jsonb stores for it.
_COMPLETION_EVIDENCE = (
    "SELECT a.attempt_number, a.state, a.failure_kind, a.error_type, a.error_message, "
    "t.state, t.attempt_count, t.result = %s::jsonb "
    "FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE a.id = %s"
)

# A revocation or a transient failure at generation N leaves the task queued,
# cancelled or failed at N (next_task_state, DEC-038); only a later claim moves
# it on, to a higher generation. Running or succeeded at N contradicts it.
_AFTER_A_RETRYABLE_END = frozenset({TaskState.QUEUED, TaskState.CANCELLED, TaskState.FAILED})

# The task state a completion leaves behind, when it is final whatever came after.
_FINAL_TASK_STATE = {
    AttemptState.SUCCEEDED: TaskState.SUCCEEDED,
    AttemptState.CANCELLED: TaskState.CANCELLED,
    AttemptState.FAILED: TaskState.FAILED,  # permanent or unclassified only
}


def read_completion_evidence(
    conn: Conn, attempt_id: uuid.UUID, *, generation: int, outcome: AttemptOutcome
) -> CompletionEvidence:
    """Whether ``complete_attempt(attempt_id, generation=..., outcome=...)`` committed.

    Read-only: one ``SELECT`` of the attempt and its task. ``MATCHES`` needs
    every recorded detail to be the one this outcome writes:

    - success: the attempt succeeded with no failure details, and the task
      succeeded at this generation with a result equal, as ``jsonb``, to
      ``outcome.result`` (JSON ``null`` is a result, never SQL ``NULL``);
    - cancelled: the attempt and the task cancelled at this generation;
    - permanent or unclassified failure: the attempt failed with exactly this
      failure kind, error type and message, and the task failed at this
      generation;
    - transient failure: the attempt's failure details, and a task that is
      queued, cancelled or failed at this generation or has moved on to a
      later one (it may since have been requeued, claimed again, cancelled or
      failed).

    An attempt still running whose task is running at this generation is
    ``RUNNING``. A revoked attempt is ``REVOKED`` when its task is queued,
    cancelled or failed at this generation or has moved on to a later one.
    Any other finished state or detail is ``CONFLICTS``. A missing attempt, a
    different generation, or a task that contradicts its attempt (for a
    revoked or transient-failed attempt, a task still running or succeeded at
    its generation) is ``INCONSISTENT``. On a connection that
    is not in autocommit mode, the read opens a transaction the caller must end.
    """
    expected_result = Jsonb(outcome.result) if isinstance(outcome, AttemptSucceeded) else None
    with tuple_cursor(conn) as cur:
        row = cur.execute(_COMPLETION_EVIDENCE, (expected_result, attempt_id)).fetchone()
    if row is None:
        return CompletionEvidence.INCONSISTENT
    number, state, failure_kind, error_type, error_message, task_state, task_generation, same = row
    attempt_state, task_state = AttemptState(state), TaskState(task_state)
    if number != generation or task_generation < generation:
        return CompletionEvidence.INCONSISTENT
    if attempt_state is AttemptState.RUNNING:
        if task_state is TaskState.RUNNING and task_generation == generation:
            return CompletionEvidence.RUNNING
        return CompletionEvidence.INCONSISTENT
    retryable_end_consistent = task_generation > generation or task_state in _AFTER_A_RETRYABLE_END
    if attempt_state is AttemptState.REVOKED:
        if retryable_end_consistent:
            return CompletionEvidence.REVOKED
        return CompletionEvidence.INCONSISTENT
    failure = outcome if isinstance(outcome, AttemptFailed) else None
    expected = (
        (None, None, None)
        if failure is None
        else (failure.kind.value, failure.error_type, failure.error_message)
    )
    if attempt_state is not outcome.state or (failure_kind, error_type, error_message) != expected:
        return CompletionEvidence.CONFLICTS
    if failure is not None and failure.kind is FailureKind.TRANSIENT:
        # The task may have been requeued, cancelled, failed or claimed again since.
        if retryable_end_consistent:
            return CompletionEvidence.MATCHES
        return CompletionEvidence.INCONSISTENT
    if task_state is not _FINAL_TASK_STATE[attempt_state] or task_generation != generation:
        return CompletionEvidence.INCONSISTENT  # one transaction writes both rows
    if isinstance(outcome, AttemptSucceeded) and same is not True:
        return CompletionEvidence.CONFLICTS
    return CompletionEvidence.MATCHES


def _finished(attempt: Attempt) -> AttemptFinishedError:
    """The rejection for a write to an attempt that already ended."""
    if attempt.revoked_reason is not None:  # set exactly when the attempt was revoked
        return AttemptRevokedError(attempt.id, attempt.revoked_reason)
    return AttemptFinishedError(attempt.id, attempt.state)


def _lease_of(attempt: Attempt) -> tuple[datetime, datetime]:
    """A running attempt's ``(lease_expires_at, deadline_at)``."""
    if attempt.lease_expires_at is None or attempt.deadline_at is None:  # pragma: no cover
        msg = f"invariant violated: running attempt {attempt.id} has no lease"  # lease_required
        raise RuntimeError(msg)
    return attempt.lease_expires_at, attempt.deadline_at


def _now(cur: Cursor) -> datetime:
    (now,) = _one(cur.execute(_NOW))
    if not isinstance(now, datetime):  # pragma: no cover - statement_timestamp() is timestamptz
        msg = f"statement_timestamp() returned {now!r}"
        raise TypeError(msg)
    return now


def _one(cur: Cursor) -> TupleRow:
    """The single row a statement cannot fail to return.

    That is a write to a row this transaction has locked, a read of a row a
    locked row refers to, or a clock read.
    """
    row = cur.fetchone()
    if row is None:  # pragma: no cover - guaranteed by the locks and foreign keys
        msg = "a write on a locked row returned no row"
        raise RuntimeError(msg)
    return row


def _locked(cur: Cursor, query: str, record_id: uuid.UUID, kind: str) -> TupleRow:
    row = cur.execute(query, (record_id,)).fetchone()
    if row is None:
        raise NotFoundError(kind, record_id)
    return row


def _lock_task(cur: Cursor, query: str, record_id: uuid.UUID, kind: str) -> Task:
    return task_from_row(_locked(cur, query, record_id, kind))
