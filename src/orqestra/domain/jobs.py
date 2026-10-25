"""Job and task records, and the request that submits them.

Plain immutable data: no I/O. ``JobSubmission`` checks the rules that only
make sense for a whole request; value rules (non-blank text, the retry-limit
range) are enforced by the database's named constraints, so they are not
restated here.

Frozen dataclasses do not freeze what they hold: a ``payload`` dict can still
be mutated in place, so callers should treat payloads as read-only.
"""

import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime

from orqestra.domain.states import TaskState

type JsonValue = bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None


@dataclass(frozen=True, slots=True)
class TaskSpec:
    """One task of a submission. ``task_key`` identifies it within its job."""

    task_key: str
    payload: JsonValue
    max_attempts: int


@dataclass(frozen=True, slots=True)
class JobSubmission:
    """A request to create a job and all of its tasks at once.

    Submissions that share an ``idempotency_key`` must be logically identical:
    the same ``kind`` and the same tasks by ``task_key``, each with an equal
    payload and retry limit. Task order is not part of a submission's identity.
    """

    kind: str
    tasks: tuple[TaskSpec, ...]
    idempotency_key: str | None = None

    def __post_init__(self) -> None:
        if not self.tasks:
            msg = "a job submission needs at least one task"
            raise ValueError(msg)
        duplicates = sorted(
            key for key, n in Counter(t.task_key for t in self.tasks).items() if n > 1
        )
        if duplicates:
            msg = f"task keys must be unique within a job; repeated: {duplicates}"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class Job:
    """A stored job. Its status is derived from its tasks, not stored (DEC-022)."""

    id: uuid.UUID
    kind: str
    idempotency_key: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class Task:
    """A stored task. ``attempt_count`` is its ownership generation (ADR-0003)."""

    id: uuid.UUID
    job_id: uuid.UUID
    task_key: str
    state: TaskState
    payload: JsonValue
    result: JsonValue
    max_attempts: int
    attempt_count: int
    cancel_requested_at: datetime | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None
