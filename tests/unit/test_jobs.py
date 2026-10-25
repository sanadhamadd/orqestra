import dataclasses
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from orqestra.domain.errors import OrqestraError, is_retryable
from orqestra.domain.jobs import Job, JobSubmission, Task, TaskSpec
from orqestra.domain.states import TaskState
from orqestra.persistence.jobs import IdempotencyConflictError, SubmissionResult

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
    records: list[Any] = [
        spec(),
        JobSubmission(kind="k", tasks=(spec(),)),
        Job(job_id, "k", None, NOW),
        Task(task_id, job_id, "k", TaskState.QUEUED, {}, None, 3, 0, None, NOW, NOW, None),
        SubmissionResult(Job(job_id, "k", None, NOW), created=True),
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
