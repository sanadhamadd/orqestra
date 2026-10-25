"""Job, task and attempt records, the requests that change them, and job status.

Plain immutable data and pure rules: no I/O. ``JobSubmission`` checks the rules that only
make sense for a whole request; value rules (non-blank text, the retry-limit
range) are enforced by the database's named constraints, so they are not
restated here.

Frozen dataclasses do not freeze what they hold: a ``payload`` dict can still
be mutated in place, so callers should treat payloads as read-only.
"""

import uuid
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import ClassVar

from orqestra.domain.policy import FailureKind, classify_failure
from orqestra.domain.states import AttemptState, TaskState

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
    """A stored task. ``attempt_count`` is its ownership generation (ADR-0003).

    ``result`` is meaningful only when ``state`` is ``SUCCEEDED``. A succeeded
    task's result may itself be JSON ``null``, which reads as ``None``; every
    other task has no result and also reads as ``None`` (DEC-030).
    """

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


@dataclass(frozen=True, slots=True)
class Attempt:
    """One execution of a task by one worker.

    ``attempt_number`` is the generation the attempt holds: attempt N was
    started when the task's ``attempt_count`` became N, and only it may write
    the task's outcome while that is still the current generation (ADR-0003).
    """

    id: uuid.UUID
    task_id: uuid.UUID
    attempt_number: int
    worker_id: str
    state: AttemptState
    failure_kind: FailureKind | None
    error_type: str | None
    error_message: str | None
    started_at: datetime
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class AttemptSucceeded:
    """The attempt finished its work; ``result`` becomes the task's result."""

    state: ClassVar[AttemptState] = AttemptState.SUCCEEDED
    result: JsonValue


@dataclass(frozen=True, slots=True)
class AttemptFailed:
    """The task handler raised; ``kind`` decides whether the task may retry (DEC-009)."""

    state: ClassVar[AttemptState] = AttemptState.FAILED
    kind: FailureKind
    error_type: str
    error_message: str | None = None

    @classmethod
    def from_exception(cls, exc: BaseException) -> "AttemptFailed":
        """Describe a handler exception, classified by ``classify_failure``.

        Shutdown and cancellation signals are not handler failures and raise
        ``TypeError``; unrecognised exceptions are ``UNCLASSIFIED``, never
        ``PERMANENT``.
        """
        error_type = f"{type(exc).__module__}.{type(exc).__qualname__}"
        return cls(classify_failure(exc), error_type, str(exc) or None)


@dataclass(frozen=True, slots=True)
class AttemptCancelled:
    """The worker stopped because cancellation was requested (DEC-020)."""

    state: ClassVar[AttemptState] = AttemptState.CANCELLED


type AttemptOutcome = AttemptSucceeded | AttemptFailed | AttemptCancelled


class JobStatus(StrEnum):
    """A job's status, derived from its tasks' states (DEC-022, DEC-031)."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


def derive_job_status(task_counts: Mapping[TaskState, int]) -> JobStatus:
    """Roll the number of tasks in each state up into the job's status.

    - ``QUEUED``: every task is queued (including tasks requeued for a retry).
    - ``RUNNING``: some task is queued or running, and not every task is queued.
    - ``SUCCEEDED``: every task succeeded.
    - ``FAILED``: every task has finished and at least one failed.
    - ``CANCELLED``: every task has finished, none failed and at least one was cancelled.

    Missing states count as zero. Raises ``ValueError`` for a negative count or
    a job without tasks.
    """
    if any(n < 0 for n in task_counts.values()):
        msg = f"task counts must not be negative: {dict(task_counts)}"
        raise ValueError(msg)
    total = sum(task_counts.values())
    if total == 0:
        msg = "a job has at least one task"
        raise ValueError(msg)
    queued = task_counts.get(TaskState.QUEUED, 0)
    if queued == total:
        return JobStatus.QUEUED
    if queued or task_counts.get(TaskState.RUNNING, 0):
        return JobStatus.RUNNING
    if task_counts.get(TaskState.FAILED, 0):
        return JobStatus.FAILED
    if task_counts.get(TaskState.CANCELLED, 0):
        return JobStatus.CANCELLED
    return JobStatus.SUCCEEDED


@dataclass(frozen=True, slots=True)
class JobProgress:
    """A job's derived status and how many of its tasks are in each state."""

    job_id: uuid.UUID
    status: JobStatus
    task_counts: Mapping[TaskState, int]
