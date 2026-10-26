import dataclasses
import itertools
import uuid
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

import pytest

from orqestra.domain.errors import OrqestraError, PermanentError, TransientError, is_retryable
from orqestra.domain.jobs import (
    Attempt,
    AttemptCancelled,
    AttemptFailed,
    AttemptSucceeded,
    Job,
    JobProgress,
    JobStatus,
    JobSubmission,
    Task,
    TaskSpec,
    derive_job_status,
)
from orqestra.domain.leases import LeasePolicy, RevocationReason
from orqestra.domain.policy import FailureKind
from orqestra.domain.states import TERMINAL_TASK_STATES, AttemptState, TaskState
from orqestra.persistence.jobs import IdempotencyConflictError, SubmissionResult
from orqestra.persistence.lifecycle import (
    AttemptCompletion,
    AttemptFinishedError,
    AttemptRevocation,
    AttemptRevokedError,
    CancellationOutcome,
    CancellationResult,
    LeaseExpiredError,
    NotFoundError,
    StaleAttemptError,
    TaskNotStartableError,
    WorkerMismatchError,
)

pytestmark = pytest.mark.unit

NOW = datetime(2026, 10, 25, tzinfo=UTC)


def spec(key: str = "invoice-1") -> TaskSpec:
    return TaskSpec(task_key=key, payload={"invoice": key}, max_attempts=3)


def test_submission_defaults_to_no_idempotency_key() -> None:
    submission = JobSubmission(kind="invoice_verification", tasks=(spec("a"), spec("b")))
    assert submission.idempotency_key is None
    assert [task.task_key for task in submission.tasks] == ["a", "b"]


def test_submission_needs_at_least_one_task() -> None:
    with pytest.raises(ValueError, match="at least one task"):
        JobSubmission(kind="k", tasks=())


def test_submission_rejects_repeated_task_keys() -> None:
    tasks = (spec("b"), spec("a"), spec("b"), spec("c"), spec("a"))
    with pytest.raises(ValueError, match=r"repeated: \['a', 'b'\]"):
        JobSubmission(kind="k", tasks=tasks)


def test_records_are_frozen_and_slotted() -> None:
    task_id, job_id = uuid.uuid4(), uuid.uuid4()
    task = Task(task_id, job_id, "k", TaskState.QUEUED, {}, None, 3, 0, None, NOW, NOW, None)
    attempt = Attempt(
        uuid.uuid4(),
        task_id,
        1,
        "w",
        AttemptState.RUNNING,
        None,
        None,
        None,
        NOW,
        None,
        NOW + timedelta(minutes=5),
        NOW + timedelta(hours=1),
        None,
    )
    records: list[Any] = [
        spec(),
        JobSubmission(kind="k", tasks=(spec(),)),
        Job(job_id, "k", None, NOW),
        task,
        SubmissionResult(Job(job_id, "k", None, NOW), created=True),
        attempt,
        AttemptSucceeded(None),
        AttemptFailed(FailureKind.PERMANENT, "E"),
        AttemptCompletion(attempt, task),
        AttemptRevocation(attempt, task),
        LeasePolicy(timedelta(seconds=1), timedelta(seconds=2)),
        CancellationResult(CancellationOutcome.CANCELLED, task),
        JobProgress(job_id, JobStatus.QUEUED, MappingProxyType({TaskState.QUEUED: 1})),
    ]
    for record in records:
        assert not hasattr(record, "__dict__")
        field = dataclasses.fields(record)[0].name
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(record, field, None)


def test_idempotency_conflict_error_identifies_the_job_but_not_the_key() -> None:
    job_id = uuid.uuid4()
    exc = IdempotencyConflictError("client-batch-7", job_id)
    assert isinstance(exc, OrqestraError)
    assert not is_retryable(exc)
    assert (exc.idempotency_key, exc.job_id) == ("client-batch-7", job_id)
    assert str(job_id) in str(exc)
    assert "client-batch-7" not in str(exc)


# --- attempt outcomes --------------------------------------------------------


def test_each_outcome_names_its_attempt_state_without_storing_it() -> None:
    assert AttemptSucceeded.state is AttemptState.SUCCEEDED
    assert AttemptFailed.state is AttemptState.FAILED
    assert AttemptCancelled.state is AttemptState.CANCELLED
    assert [f.name for f in dataclasses.fields(AttemptSucceeded)] == ["result"]
    assert [f.name for f in dataclasses.fields(AttemptCancelled)] == []
    assert not hasattr(AttemptCancelled(), "__dict__")


class RateLimitedError(TransientError):
    pass


@pytest.mark.parametrize(
    ("exc", "kind", "error_type", "message"),
    [
        (RateLimitedError("429"), FailureKind.TRANSIENT, f"{__name__}.RateLimitedError", "429"),
        (
            PermanentError("no total"),
            FailureKind.PERMANENT,
            "orqestra.domain.errors.PermanentError",
            "no total",
        ),
        (KeyError("total"), FailureKind.UNCLASSIFIED, "builtins.KeyError", "'total'"),
        # Infrastructure errors raised by a handler are never permanent (DEC-009).
        (
            ConnectionResetError("reset"),
            FailureKind.UNCLASSIFIED,
            "builtins.ConnectionResetError",
            "reset",
        ),
        (TimeoutError(), FailureKind.UNCLASSIFIED, "builtins.TimeoutError", None),
    ],
)
def test_a_handler_exception_becomes_a_classified_failure(
    exc: Exception, kind: FailureKind, error_type: str, message: str | None
) -> None:
    assert AttemptFailed.from_exception(exc) == AttemptFailed(kind, error_type, message)


@pytest.mark.parametrize("signal", [KeyboardInterrupt(), SystemExit(1)])
def test_control_plane_signals_are_not_recorded_as_failures(signal: BaseException) -> None:
    with pytest.raises(TypeError, match="not a handler failure"):
        AttemptFailed.from_exception(signal)


# --- derived job status ------------------------------------------------------


def expected_status(counts: dict[TaskState, int]) -> JobStatus:
    """The rules restated independently, case by case."""
    total = sum(counts.values())
    if counts[TaskState.QUEUED] == total:
        return JobStatus.QUEUED
    if counts[TaskState.QUEUED] or counts[TaskState.RUNNING]:
        return JobStatus.RUNNING
    if counts[TaskState.FAILED]:
        return JobStatus.FAILED
    if counts[TaskState.CANCELLED]:
        return JobStatus.CANCELLED
    assert counts[TaskState.SUCCEEDED] == total
    return JobStatus.SUCCEEDED


ALL_COUNTS = [
    dict(zip(TaskState, combo, strict=True))
    for combo in itertools.product([0, 1, 2], repeat=len(TaskState))
    if sum(combo)
]


@pytest.mark.parametrize("counts", ALL_COUNTS, ids=lambda c: "-".join(str(n) for n in c.values()))
def test_job_status_decision_table(counts: dict[TaskState, int]) -> None:
    status = derive_job_status(counts)
    assert status is expected_status(counts)
    finished = all(counts[s] == 0 for s in (TaskState.QUEUED, TaskState.RUNNING))
    # A job is finished exactly when every task is terminal, and then its
    # status is a terminal task state.
    assert (status.value in {s.value for s in TERMINAL_TASK_STATES}) == finished


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ({TaskState.QUEUED: 3}, JobStatus.QUEUED),
        ({TaskState.QUEUED: 2, TaskState.SUCCEEDED: 1}, JobStatus.RUNNING),
        ({TaskState.RUNNING: 1}, JobStatus.RUNNING),
        ({TaskState.SUCCEEDED: 4}, JobStatus.SUCCEEDED),
        ({TaskState.SUCCEEDED: 3, TaskState.FAILED: 1}, JobStatus.FAILED),
        ({TaskState.FAILED: 1, TaskState.CANCELLED: 5}, JobStatus.FAILED),
        ({TaskState.SUCCEEDED: 3, TaskState.CANCELLED: 1}, JobStatus.CANCELLED),
    ],
)
def test_job_status_examples_with_missing_states_counted_as_zero(
    counts: dict[TaskState, int], expected: JobStatus
) -> None:
    assert derive_job_status(counts) is expected


@pytest.mark.parametrize(
    ("counts", "message"),
    [({}, "at least one task"), ({TaskState.QUEUED: 0}, "at least one task")],
)
def test_a_job_without_tasks_has_no_status(counts: dict[TaskState, int], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        derive_job_status(counts)


def test_negative_counts_are_rejected() -> None:
    with pytest.raises(ValueError, match="negative"):
        derive_job_status({TaskState.QUEUED: 2, TaskState.FAILED: -1})


def test_job_status_values_are_stable() -> None:
    assert [s.value for s in JobStatus] == [
        "queued",
        "running",
        "succeeded",
        "failed",
        "cancelled",
    ]


# --- lifecycle errors --------------------------------------------------------


def test_lifecycle_errors_carry_their_identifiers() -> None:
    record, attempt_id = uuid.uuid4(), uuid.uuid4()
    not_found = NotFoundError("task", record)
    assert isinstance(not_found, LookupError)
    assert (not_found.kind, not_found.record_id) == ("task", record)
    not_startable = TaskNotStartableError(record, TaskState.RUNNING)
    assert (not_startable.task_id, not_startable.state) == (record, TaskState.RUNNING)
    assert "'running'" in str(not_startable)
    stale = StaleAttemptError(attempt_id, 1, 2)
    assert (stale.attempt_id, stale.generation, stale.current_generation) == (attempt_id, 1, 2)
    finished = AttemptFinishedError(attempt_id, AttemptState.FAILED)
    assert (finished.attempt_id, finished.state) == (attempt_id, AttemptState.FAILED)
    revoked = AttemptRevokedError(attempt_id, RevocationReason.DEADLINE_EXCEEDED)
    assert isinstance(revoked, AttemptFinishedError)  # handled wherever "finished" is
    assert (revoked.attempt_id, revoked.state, revoked.reason) == (
        attempt_id,
        AttemptState.REVOKED,
        RevocationReason.DEADLINE_EXCEEDED,
    )
    assert "revoked (deadline_exceeded)" in str(revoked)
    assert "finished as" not in str(revoked)  # it does not claim an outcome was recorded
    expired = LeaseExpiredError(attempt_id, RevocationReason.LEASE_EXPIRED, NOW, NOW)
    assert (expired.attempt_id, expired.reason) == (attempt_id, RevocationReason.LEASE_EXPIRED)
    assert (expired.lease_expires_at, expired.deadline_at) == (NOW, NOW)
    assert NOW.isoformat() in str(expired)
    mismatch = WorkerMismatchError(attempt_id, "intruder", "owner")
    assert (mismatch.worker_id, mismatch.owner) == ("intruder", "owner")
    for exc in (not_found, not_startable, stale, finished, revoked, expired, mismatch):
        assert isinstance(exc, OrqestraError)
        assert not is_retryable(exc)
