"""The invoice batch handler through the real task engine and PostgreSQL.

partition -> submit_job -> claim_next_attempt -> run_handler -> complete_attempt,
with the stored ``Task.result`` and the retry schedule read back from the database.
"""

import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, cast

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.ai.mock import MockProvider
from orqestra.ai.provider import ProviderTransientError, TransientCode
from orqestra.domain.jobs import AttemptFailed, AttemptSucceeded, JobStatus, JobSubmission
from orqestra.domain.partitioning import partition
from orqestra.domain.policy import FailureKind
from orqestra.domain.retry import RetryPolicy
from orqestra.domain.states import TaskState
from orqestra.persistence.jobs import get_job_progress, get_task, list_tasks, submit_job
from orqestra.persistence.lifecycle import AttemptCompletion, claim_next_attempt, complete_attempt
from orqestra.worker.handlers import (
    CancellationSignal,
    ControlPlaneSignal,
    Handler,
    HandlerOutcome,
    TaskContext,
    run_handler,
)
from orqestra.workloads.invoice.handler import JOB_KIND, invoice_verification_handler
from tests.integration.test_attempt_lifecycle import POLICY, assert_invariants
from tests.unit.test_invoice_verify import ROUTE, TIMEOUT, ambiguous_unit, known

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

# Test value only (DEC-034): a delay far longer than any test, so "later than now" is exact.
BACKOFF = RetryPolicy(base_delay=timedelta(hours=1), max_delay=timedelta(hours=8))


def db_clock(db: Conn) -> datetime:
    row = db.execute("SELECT clock_timestamp()").fetchone()
    assert row is not None
    return cast(datetime, row[0])


def available_at(db: Conn, task_id: uuid.UUID) -> datetime:
    row = db.execute("SELECT available_at FROM tasks WHERE id = %s", (task_id,)).fetchone()
    assert row is not None
    return cast(datetime, row[0])


def run_next(db: Conn, handler: Handler) -> tuple[HandlerOutcome, AttemptCompletion]:
    """Claim the next invoice task, run the handler on its payload and record the outcome."""
    attempt = claim_next_attempt(db, "worker-1", kinds=(JOB_KIND,), policy=POLICY)
    assert attempt is not None
    task = get_task(db, attempt.task_id)
    assert task is not None
    context = TaskContext(
        task.id, attempt.id, attempt.attempt_number, JOB_KIND, CancellationSignal(threading.Event())
    )
    outcome = run_handler(handler, context, task.payload)
    assert not isinstance(outcome, ControlPlaneSignal)
    completion = complete_attempt(
        db,
        attempt.id,
        generation=attempt.attempt_number,
        outcome=outcome,
        retry_policy=BACKOFF,
    )
    return outcome, completion


def test_a_partitioned_batch_is_stored_as_item_results_and_the_job_still_succeeds(
    db: Conn,
) -> None:
    rejected = known()
    rejected["total"] = 14
    items = [known(), rejected, ambiguous_unit()]
    specs = partition(items, batch_size=2, max_attempts=3, key_prefix="invoices")
    job = submit_job(db, JobSubmission(JOB_KIND, specs)).job
    mock = MockProvider(["5"])
    handler = invoice_verification_handler({"r1": mock}, [ROUTE], TIMEOUT)

    for _ in specs:
        outcome, completion = run_next(db, handler)
        assert isinstance(outcome, AttemptSucceeded)
        assert completion.task.state is TaskState.SUCCEEDED

    first, second = list_tasks(db, job.id)  # by task_key: batch order
    assert isinstance(first.result, dict)
    assert isinstance(second.result, dict)
    assert first.result["offset"] == 0
    assert second.result["offset"] == 2
    first_results: Any = first.result["results"]
    second_results: Any = second.result["results"]
    summary = [
        (r["index"], r["status"], r["provider_assisted"], r["routing"])
        for r in first_results + second_results
    ]
    assert summary == [
        (0, "verified", False, None),
        (1, "rejected", False, None),
        (2, "verified", True, {"route_id": "r1"}),
    ]
    assert len(mock.requests) == 1
    progress = get_job_progress(db, job.id)
    assert progress is not None
    # A rejected invoice is an item result, not a task failure (DEC-031).
    assert progress.status is JobStatus.SUCCEEDED
    assert_invariants(db)


def test_a_transient_provider_error_requeues_the_batch_after_a_durable_delay(db: Conn) -> None:
    specs = partition([ambiguous_unit()], batch_size=1, max_attempts=3, key_prefix="invoices")
    job = submit_job(db, JobSubmission(JOB_KIND, specs)).job
    mock = MockProvider([ProviderTransientError(TransientCode.RATE_LIMITED), "5"])
    handler = invoice_verification_handler({"r1": mock}, [ROUTE], TIMEOUT)

    outcome, completion = run_next(db, handler)

    assert isinstance(outcome, AttemptFailed)
    assert outcome.kind is FailureKind.TRANSIENT
    assert completion.attempt.error_message == "rate_limited"
    assert completion.task.state is TaskState.QUEUED
    (task,) = list_tasks(db, job.id)
    assert available_at(db, task.id) > db_clock(db)
    assert claim_next_attempt(db, "worker-1", kinds=(JOB_KIND,), policy=POLICY) is None
    assert_invariants(db)
