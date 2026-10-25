"""Transactional attempt lifecycle: start attempts, complete them, cancel tasks.

Each write is one short READ COMMITTED transaction that the operation owns
(DEC-028). It locks the task row first and then, when it needs it, the attempt
row; decides with the domain policy and transition tables; writes; and
commits. No transaction stays open while a worker executes: ``start_attempt``
commits before the work begins and ``complete_attempt`` runs afterwards.

Ownership (ADR-0003, DEC-029): attempt N holds generation N, the value of the
task's ``attempt_count`` when it started. ``complete_attempt`` takes effect
only for the attempt that is still the task's current, running generation.
Any other write is rejected with ``StaleAttemptError`` or
``AttemptFinishedError`` and changes nothing. Rejections are expected
concurrency outcomes, not task failures (DEC-009).

A task may run in several attempts over its lifetime, and generation fencing
ensures that only one attempt's result can become the task's result. These
operations are building blocks for at-least-once execution but do not provide
it: nothing delivers tasks automatically. Task discovery, leases, a worker
loop and crash recovery are not implemented, so a task whose worker stops is
not retried.
"""

import logging
import uuid
from dataclasses import dataclass
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
from orqestra.domain.policy import next_task_state, state_after_cancel_request
from orqestra.domain.states import (
    TERMINAL_TASK_STATES,
    AttemptState,
    TaskState,
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

_CLAIM_TASK = (
    "UPDATE tasks SET state = %s, attempt_count = attempt_count + 1, updated_at = now() "
    "WHERE id = %s RETURNING attempt_count"
)
_INSERT_ATTEMPT = (
    "INSERT INTO attempts (id, task_id, attempt_number, worker_id) VALUES (%s, %s, %s, %s) "
    f"RETURNING {ATTEMPT_COLUMNS}"
)
_FINISH_ATTEMPT = (
    "UPDATE attempts SET state = %s, failure_kind = %s, error_type = %s, error_message = %s, "
    f"finished_at = now() WHERE id = %s RETURNING {ATTEMPT_COLUMNS}"
)
_FINISH_TASK = (
    "UPDATE tasks SET state = %s, result = %s, updated_at = now(), "
    "finished_at = CASE WHEN %s THEN now() END "
    f"WHERE id = %s RETURNING {TASK_COLUMNS}"
)
_CANCEL_QUEUED_TASK = (
    "UPDATE tasks SET state = %s, cancel_requested_at = now(), finished_at = now(), "
    f"updated_at = now() WHERE id = %s RETURNING {TASK_COLUMNS}"
)
_RECORD_CANCEL_REQUEST = (
    "UPDATE tasks SET cancel_requested_at = now(), updated_at = now() "
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


@dataclass(frozen=True, slots=True)
class AttemptCompletion:
    """The finished attempt and its task as stored by ``complete_attempt``."""

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


def start_attempt(conn: Conn, task_id: uuid.UUID, worker_id: str) -> Attempt:
    """Start the next attempt of a queued task and return it.

    In one transaction: lock the task, check with the transition table that it
    may move to ``running`` (only a queued task may), advance ``attempt_count``
    by one (the fencing trigger rejects any other change) and insert the
    running attempt whose number is the new generation. Competing callers serialise on the task's
    row lock, so exactly one of them starts each generation; the others get
    ``TaskNotStartableError``. Raises ``NotFoundError`` for an unknown task.
    """
    with owned_transaction(conn, "start_attempt") as cur:
        task = _lock_task(cur, _LOCK_TASK, task_id, "task")
        # Only a queued task may start, and tasks_queued_has_attempts_left
        # guarantees a queued task has an attempt left, so this cannot exceed
        # max_attempts.
        if not can_transition(task.state, TaskState.RUNNING):
            raise TaskNotStartableError(task_id, task.state)
        (generation,) = _one(cur.execute(_CLAIM_TASK, (TaskState.RUNNING.value, task_id)))
        attempt = attempt_from_row(
            _one(cur.execute(_INSERT_ATTEMPT, (uuid.uuid4(), task_id, generation, worker_id)))
        )
    logger.info(
        "attempt started",
        extra={"task_id": str(task_id), "attempt_id": str(attempt.id), "worker_id": worker_id},
    )
    return attempt


def complete_attempt(
    conn: Conn, attempt_id: uuid.UUID, *, generation: int, outcome: AttemptOutcome
) -> AttemptCompletion:
    """Record how an attempt ended and move its task accordingly.

    ``generation`` is the attempt's number as returned by ``start_attempt``.
    The write takes effect only if the attempt still holds the task's current
    generation and is running; then the attempt and the task are updated
    together, with the task's next state chosen by ``next_task_state``.
    A succeeded attempt's result becomes the task's result.

    Raises, without changing anything: ``StaleAttemptError`` when the
    generation does not match the attempt or a newer generation exists;
    ``AttemptFinishedError`` when the attempt already ended (a duplicate
    completion); ``NotFoundError`` for an unknown attempt; ``ValueError`` from
    the policy for a cancelled outcome without a cancellation request.
    """
    with owned_transaction(conn, "complete_attempt") as cur:
        task = _lock_task(cur, _LOCK_TASK_OF_ATTEMPT, attempt_id, "attempt")
        current = attempt_from_row(_locked(cur, _LOCK_ATTEMPT, attempt_id, "attempt"))
        if generation != current.attempt_number or generation != task.attempt_count:
            raise StaleAttemptError(attempt_id, generation, task.attempt_count)
        if current.state is not AttemptState.RUNNING:
            raise AttemptFinishedError(attempt_id, current.state)
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
                (next_state.value, result, next_state in TERMINAL_TASK_STATES, task.id),
            )
        )
        completion = AttemptCompletion(attempt_from_row(attempt_row), task_from_row(task_row))
    logger.info(
        "attempt completed",
        extra={"task_id": str(task.id), "attempt_id": str(attempt_id)},
    )
    return completion


def request_cancellation(conn: Conn, task_id: uuid.UUID) -> CancellationResult:
    """Request cancellation of a task (cooperative; DEC-020).

    A queued task is cancelled at once. A running task records the request and
    keeps running; its attempt decides the outcome when it completes. Repeated
    requests and requests for finished tasks change nothing. Raises
    ``NotFoundError`` for an unknown task.
    """
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
            row = _one(cur.execute(_RECORD_CANCEL_REQUEST, (task_id,)))
            outcome = CancellationOutcome.REQUESTED
        result = CancellationResult(outcome, task if row is None else task_from_row(row))
    logger.info("cancellation request: %s", outcome.value, extra={"task_id": str(task_id)})
    return result


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


def _one(cur: Cursor) -> TupleRow:
    """The single row an UPDATE or INSERT ... RETURNING on a locked row produces."""
    row = cur.fetchone()
    if row is None:  # pragma: no cover - the row is locked by this transaction
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
