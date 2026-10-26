"""Task and attempt lifecycle state machines.

The allowed transitions are explicit, immutable tables so the lifecycles are
documented, testable and cannot drift silently. They are the single source of
legal transitions. Persistence must additionally check the task's ownership
generation (``tasks.attempt_count``) on every authoritative write: a
state-only conditional write cannot tell a current worker from a stale one
(see ADR-0003).

Task lifecycle::

    queued ──> running ──> succeeded
      ^           │
      │           ├──────> failed        (permanent error or retries exhausted)
      │           │
      └───────────┘        running -> queued = retry / lease expiry / requeue
      queued, running ──> cancelled

An attempt is one execution of a task by one worker. It is created running
and ends exactly once::

    running ──> succeeded | failed | cancelled | revoked

``revoked`` is not reported by a worker: recovery records it when an
attempt's lease expired and it took ownership back (see ``leases``).
"""

from enum import StrEnum
from types import MappingProxyType

from orqestra.domain.errors import InvalidTransitionError


class TaskState(StrEnum):
    """States a task can be in. Values are stable identifiers for storage."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TASK_TRANSITIONS: MappingProxyType[TaskState, frozenset[TaskState]] = MappingProxyType(
    {
        TaskState.QUEUED: frozenset({TaskState.RUNNING, TaskState.CANCELLED}),
        TaskState.RUNNING: frozenset(
            {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.QUEUED, TaskState.CANCELLED}
        ),
        TaskState.SUCCEEDED: frozenset(),
        TaskState.FAILED: frozenset(),
        TaskState.CANCELLED: frozenset(),
    }
)

TERMINAL_TASK_STATES: frozenset[TaskState] = frozenset(
    state for state, targets in TASK_TRANSITIONS.items() if not targets
)


def can_transition(current: TaskState, new: TaskState) -> bool:
    """Return whether moving from ``current`` to ``new`` is allowed."""
    return new in TASK_TRANSITIONS[current]


def transition(current: TaskState, new: TaskState) -> TaskState:
    """Return ``new`` if the move is allowed, else raise ``InvalidTransitionError``."""
    if not can_transition(current, new):
        msg = f"task cannot move from {current.value!r} to {new.value!r}"
        raise InvalidTransitionError(msg)
    return new


def allowed_sources(target: TaskState) -> frozenset[TaskState]:
    """Return the states a task may move to ``target`` from.

    Conditional writes use this as their ``WHERE state = ANY(...)`` guard so
    the SQL is derived from the table instead of restating it.
    """
    return frozenset(state for state, targets in TASK_TRANSITIONS.items() if target in targets)


class AttemptState(StrEnum):
    """States an execution attempt can be in. Values are stable identifiers for storage."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REVOKED = "revoked"


ATTEMPT_TRANSITIONS: MappingProxyType[AttemptState, frozenset[AttemptState]] = MappingProxyType(
    {
        AttemptState.RUNNING: frozenset(
            {
                AttemptState.SUCCEEDED,
                AttemptState.FAILED,
                AttemptState.CANCELLED,
                AttemptState.REVOKED,
            }
        ),
        AttemptState.SUCCEEDED: frozenset(),
        AttemptState.FAILED: frozenset(),
        AttemptState.CANCELLED: frozenset(),
        AttemptState.REVOKED: frozenset(),
    }
)

TERMINAL_ATTEMPT_STATES: frozenset[AttemptState] = frozenset(
    state for state, targets in ATTEMPT_TRANSITIONS.items() if not targets
)


def can_transition_attempt(current: AttemptState, new: AttemptState) -> bool:
    """Return whether an attempt may move from ``current`` to ``new``."""
    return new in ATTEMPT_TRANSITIONS[current]


def transition_attempt(current: AttemptState, new: AttemptState) -> AttemptState:
    """Return ``new`` if the attempt move is allowed, else raise ``InvalidTransitionError``."""
    if not can_transition_attempt(current, new):
        msg = f"attempt cannot move from {current.value!r} to {new.value!r}"
        raise InvalidTransitionError(msg)
    return new
