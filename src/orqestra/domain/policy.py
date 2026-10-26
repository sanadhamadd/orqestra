"""Lifecycle policy: how attempt outcomes and cancellation requests move a task.

Pure functions only. Persistence calls them while holding the task's row lock
and writes the state they return, so the retry and cancellation rules live in
one place instead of being restated in SQL.

Cancellation is cooperative. A request cancels a queued task immediately; a
running task keeps running and its current attempt may still finish. Once a
request exists, any outcome that would requeue the task cancels it instead;
outcomes that end the task on their own (success, permanent or unclassified
failure, exhausted retries) are recorded as they happened.

A revoked attempt is not a handler failure (DEC-009): recovery took
ownership back because the attempt's lease or deadline expired. For the task
it counts like a transient failure: it used one attempt of the budget
(DEC-019), requeues the task while attempts remain, cancels it instead when a
request is pending, and fails it once the budget is exhausted, whether or not
a request is pending. Revoking ownership after a bounded cancellation grace
period is a later contract and is not implemented here.
"""

from enum import StrEnum

from orqestra.domain.errors import PermanentError, is_retryable
from orqestra.domain.states import AttemptState, TaskState


class FailureKind(StrEnum):
    """Classification of a handler failure. Values are stable identifiers for storage."""

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    UNCLASSIFIED = "unclassified"


def classify_failure(exc: BaseException) -> FailureKind:
    """Classify an exception raised by a task handler.

    ``TRANSIENT`` exactly when ``is_retryable(exc)``; ``PERMANENT`` for other
    ``PermanentError``; anything else is ``UNCLASSIFIED`` and, per DEC-009, is
    not retried. Shutdown and cancellation signals (``BaseException`` outside
    ``Exception``) are control-plane events, not handler failures, and are
    rejected rather than classified.
    """
    if not isinstance(exc, Exception):
        msg = f"{type(exc).__name__} is not a handler failure and must not be classified"
        raise TypeError(msg)
    if is_retryable(exc):
        return FailureKind.TRANSIENT
    if isinstance(exc, PermanentError):
        return FailureKind.PERMANENT
    return FailureKind.UNCLASSIFIED


def next_task_state(
    outcome: AttemptState,
    failure_kind: FailureKind | None,
    *,
    attempt_count: int,
    max_attempts: int,
    cancel_requested: bool,
) -> TaskState:
    """Return the task state after its current attempt ends with ``outcome``.

    ``attempt_count`` is the task's generation, i.e. the number of the attempt
    that ended. ``failure_kind`` is required for a failed attempt and must be
    ``None`` otherwise, including for a revoked one. Raises ``ValueError`` for
    inputs no valid caller can produce.
    """
    if not 1 <= attempt_count <= max_attempts:
        msg = f"attempt_count must be in 1..{max_attempts}, got {attempt_count}"
        raise ValueError(msg)
    if (outcome is AttemptState.FAILED) != (failure_kind is not None):
        msg = "failure_kind is required for a failed attempt and only for a failed attempt"
        raise ValueError(msg)
    match outcome:
        case AttemptState.RUNNING:
            msg = "a running attempt has not ended"
            raise ValueError(msg)
        case AttemptState.SUCCEEDED:
            return TaskState.SUCCEEDED
        case AttemptState.CANCELLED:
            if not cancel_requested:
                msg = "an attempt can only be cancelled after a cancellation request"
                raise ValueError(msg)
            return TaskState.CANCELLED
        case AttemptState.FAILED if failure_kind is FailureKind.TRANSIENT:
            return _retry_unless_exhausted(attempt_count, max_attempts, cancel_requested)
        case AttemptState.FAILED:
            return TaskState.FAILED
        case AttemptState.REVOKED:
            return _retry_unless_exhausted(attempt_count, max_attempts, cancel_requested)


def _retry_unless_exhausted(
    attempt_count: int, max_attempts: int, cancel_requested: bool
) -> TaskState:
    """Requeue (or cancel, if requested) while attempts remain; otherwise fail."""
    if attempt_count < max_attempts:
        return TaskState.CANCELLED if cancel_requested else TaskState.QUEUED
    return TaskState.FAILED


def state_after_cancel_request(current: TaskState) -> TaskState:
    """Return the task state once a cancellation request is recorded.

    Queued tasks are cancelled at once; running tasks keep running until their
    attempt ends (see ``next_task_state``); terminal tasks are unchanged.
    """
    match current:
        case TaskState.QUEUED:
            return TaskState.CANCELLED
        case TaskState.RUNNING | TaskState.SUCCEEDED | TaskState.FAILED | TaskState.CANCELLED:
            return current
