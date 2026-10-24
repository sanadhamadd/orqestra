"""Task lifecycle state machine.

The allowed transitions are an explicit, immutable table so the lifecycle is
documented, testable and cannot drift silently. Persistence (Day 2) must
enforce the same rules with conditional writes; this module is the single
source of truth they are checked against.

Lifecycle::

    queued ──> running ──> succeeded
      ^           │
      │           ├──────> failed        (permanent error or retries exhausted)
      │           │
      └───────────┘        running -> queued = retry / lease expiry / requeue
      queued, running ──> cancelled
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
