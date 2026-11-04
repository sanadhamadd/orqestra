"""The job usage report against real PostgreSQL.

Attempts go through the real lifecycle (start, complete, retry, lease expiry and
revocation) and ledger facts through the real recording functions; the invoice
cross-check runs the real handler with the scripted mock provider. Raw SQL is
used only to seed the scale test and to insert a job without tasks. The scale
test is marked ``performance``, so no default lane runs it: it records what it
observes (run with ``-s``) and asserts only correctness, never a threshold or an
environment value.
"""

import dataclasses
import json
import os
import platform
import random
import resource
import threading
import time
import tracemalloc
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from decimal import Context, Decimal, localcontext
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow

from orqestra.aggregation.invoice import WrongJobKindError, summarize_invoice_job
from orqestra.aggregation.usage import (
    AttemptCapture,
    JobUsageReport,
    ProvenanceUsage,
    UnitTotal,
    UsageAnomaly,
    UsageAnomalyCode,
    UsageFinality,
    UsageReportBuilder,
    report_job_usage,
)
from orqestra.domain.jobs import Attempt, AttemptOutcome, AttemptSucceeded, JobStatus, Task
from orqestra.domain.states import AttemptState
from orqestra.persistence import snapshots
from orqestra.persistence.jobs import list_tasks
from orqestra.persistence.lifecycle import (
    complete_attempt,
    list_attempts,
    request_cancellation,
    start_attempt,
)
from orqestra.persistence.provider_usage import (
    CallFailure,
    CallOutcome,
    CallResult,
    CaptureState,
    CaptureTier,
    ClosedBy,
    read_attempt_usage,
    record_call_intent,
    record_call_result,
    record_usage_closure,
)
from orqestra.persistence.snapshots import job_batch_snapshot
from orqestra.worker.handlers import Handler, run_handler
from tests.integration.test_attempt_lifecycle import (
    EXPIRING,
    GRACE,
    PERMANENT,
    POLICY,
    RETRY,
    SUCCESS,
    TRANSIENT,
    assert_invariants,
    complete,
    connect,
    new_tasks,
    revoke,
    snapshot,
    until_expired,
)
from tests.integration.test_invoice_summary_pg import (
    context_of,
    make_handler,
    observe,
    seed,
    submit_items,
)
from tests.integration.test_provider_usage_pg import (
    LEDGER_TABLES,
    MICRO_CREDITS,
    a_closure,
    an_answer,
    an_intent,
    incomplete,
    rows,
)
from tests.unit.test_invoice_verify import ambiguous_unit
from tests.unit.test_usage_report import assert_conserved

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

IDLE = TransactionStatus.IDLE
EPOCH = datetime(2000, 1, 1, tzinfo=UTC)
SET_READ_COMMITTED = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"
BIG = "99999999999999999999.999999999999999999"  # the largest amount the ledger accepts
TWICE_BIG = "199999999999999999999.999999999999999998"  # 39 significant digits


# --- helpers -----------------------------------------------------------------


def report_of(conn: Conn, job_id: uuid.UUID, **kwargs: Any) -> JobUsageReport:
    report = report_job_usage(conn, job_id, **kwargs)
    assert report is not None
    return report


def comparable(report: JobUsageReport) -> JobUsageReport:
    """The report without ``observed_at``, which differs between two reads."""
    return dataclasses.replace(report, observed_at=EPOCH)


def captures(report: JobUsageReport) -> dict[AttemptCapture, int]:
    return {capture: n for capture, n in report.attempts_by_capture.items() if n}


def raised(report: JobUsageReport) -> dict[UsageAnomalyCode, int]:
    return {code: n for code, n in report.anomalies.items() if n}


def answered(
    db: Conn,
    attempt_id: uuid.UUID,
    seq: int,
    amount: str | None = "0.25",
    unit: str = "credits",
    **intent: Any,
) -> None:
    """Record an intent and its answer, with ``amount`` in ``unit`` or no usage at all."""
    record_call_intent(db, an_intent(attempt_id, seq, **intent))
    usage: dict[str, Any] = (
        {"reported_amount": None, "reported_unit": None}
        if amount is None
        else {"reported_amount": Decimal(amount), "reported_unit": unit}
    )
    record_call_result(db, an_answer(attempt_id, seq, **usage))


def failed(
    db: Conn, attempt_id: uuid.UUID, seq: int, amount: str | None = None, unit: str = "credits"
) -> None:
    """Record an intent and a failure, which may report usage too."""
    record_call_intent(db, an_intent(attempt_id, seq))
    record_call_result(
        db,
        CallResult(
            attempt_id=attempt_id,
            call_seq=seq,
            outcome=CallOutcome.FAILED,
            failure_code=CallFailure.RATE_LIMITED,
            reported_amount=None if amount is None else Decimal(amount),
            reported_unit=None if amount is None else unit,
        ),
    )


def unresolved(db: Conn, attempt_id: uuid.UUID, seq: int, **intent: Any) -> None:
    """Record an intent whose outcome is unknown: it has no result."""
    record_call_intent(db, an_intent(attempt_id, seq, **intent))


def close(db: Conn, attempt_id: uuid.UUID, calls: int) -> None:
    record_usage_closure(db, a_closure(attempt_id, call_count=calls))


def one_attempt(db: Conn, key: str = "t-0") -> tuple[Task, Attempt]:
    (task,) = new_tasks(db, key)
    return task, start_attempt(db, task.id, "w", policy=POLICY)


def retried(db: Conn) -> tuple[Task, Attempt, Attempt, Attempt]:
    """A task revoked once, then failed transiently, then succeeded, each attempt with calls.

    1. Revoked after its lease expired: one answered call (0.25 credits) and one
       unresolved call estimated at 5 units, closed incomplete by recovery.
    2. Failed transiently: one answered call (1.50 credits), closed complete.
    3. Succeeded: one answered call (0.25 credits), closed complete.
    """
    (task,) = new_tasks(db, "t-0", max_attempts=3)
    first = start_attempt(db, task.id, "w1", policy=EXPIRING)
    answered(db, first.id, 0)
    unresolved(db, first.id, 1, estimated_cost_units=5)
    until_expired(db, first)
    assert revoke(db).attempt.id == first.id
    record_usage_closure(db, incomplete(first.id, closed_by=ClosedBy.RECOVERY))
    second = start_attempt(db, task.id, "w2", policy=POLICY)
    answered(db, second.id, 0, "1.50")
    close(db, second.id, 1)
    complete(db, second, TRANSIENT)
    third = start_attempt(db, task.id, "w3", policy=POLICY)
    answered(db, third.id, 0)
    close(db, third.id, 1)
    complete(db, third, SUCCESS)
    return task, first, second, third


def ledger(db: Conn) -> dict[str, list[tuple[Any, ...]]]:
    return {table: rows(db, table) for table in LEDGER_TABLES}


@pytest.fixture
def other(schema_url: str, db: Conn) -> Iterator[Conn]:
    """A second session on the same schema, for commits made while a snapshot is open."""
    conn = connect(schema_url)
    yield conn
    conn.close()


@pytest.fixture(params=[True, False], ids=["autocommit", "manual-commit"])
def conn(schema_url: str, db: Conn, request: pytest.FixtureRequest) -> Iterator[Conn]:
    """A connection in each mode a caller might use; it starts outside any transaction."""
    connection = psycopg.connect(schema_url, autocommit=request.param)
    assert connection.info.transaction_status is IDLE
    yield connection
    connection.close()


# --- retries, revocation and late results ------------------------------------


def test_usage_is_attributed_to_each_attempt_by_its_current_state(db: Conn) -> None:
    task, *_ = retried(db)

    report = report_of(db, task.job_id)

    assert report.job_status is JobStatus.SUCCEEDED
    assert (report.calls, report.answered, report.failed, report.unresolved) == (4, 3, 0, 1)
    assert dict(report.reported_totals) == {"credits": UnitTotal(Decimal("2.00"), 3)}
    revoked = report.by_attempt_state[AttemptState.REVOKED]
    assert (revoked.attempts, revoked.calls, revoked.answered, revoked.unresolved) == (1, 2, 1, 1)
    assert dict(revoked.reported_totals) == {"credits": UnitTotal(Decimal("0.25"), 1)}
    retry = report.by_attempt_state[AttemptState.FAILED]
    assert (retry.attempts, retry.calls, retry.answered, retry.unresolved) == (1, 1, 1, 0)
    assert dict(retry.reported_totals) == {"credits": UnitTotal(Decimal("1.50"), 1)}
    succeeded = report.by_attempt_state[AttemptState.SUCCEEDED]
    assert (succeeded.attempts, succeeded.calls, succeeded.answered) == (1, 1, 1)
    assert dict(succeeded.reported_totals) == {"credits": UnitTotal(Decimal("0.25"), 1)}
    assert (report.estimated_exposure_units, report.unresolved_without_estimate) == (5, 0)
    assert (report.late_intents, report.late_results) == (0, 0)
    assert captures(report) == {AttemptCapture.LOWER_BOUND: 1, AttemptCapture.CLOSED: 2}
    assert report.unclosed_attempts == 0
    assert report.finality is UsageFinality.LOWER_BOUND
    assert (report.assisted_checked_tasks, report.assisted_unchecked_tasks) == (1, 0)
    assert raised(report) == {}
    assert_conserved(report)
    assert_invariants(db)


def test_a_late_result_moves_a_call_from_unresolved_to_its_attempts_totals(db: Conn) -> None:
    task, first, _, _ = retried(db)
    late = record_call_result(db, an_answer(first.id, 1, reported_amount=Decimal("0.10")))
    assert late.record.late

    report = report_of(db, task.job_id)

    assert (report.calls, report.answered, report.unresolved) == (4, 4, 0)
    assert dict(report.reported_totals) == {"credits": UnitTotal(Decimal("2.10"), 4)}
    revoked = report.by_attempt_state[AttemptState.REVOKED]
    assert dict(revoked.reported_totals) == {"credits": UnitTotal(Decimal("0.35"), 2)}
    assert (report.late_intents, report.late_results) == (0, 1)
    assert report.estimated_exposure_units == 0
    # The revoked attempt's closure says incomplete, so the figures stay a lower bound.
    assert report.finality is UsageFinality.LOWER_BOUND
    assert_conserved(report)


# --- capture: missing, absent, zero, contradicted -----------------------------


def test_a_finished_attempt_that_recorded_nothing_is_not_a_zero(db: Conn) -> None:
    absent_task, absent = one_attempt(db, "absent")
    complete(db, absent, SUCCESS)
    zero_task, zero = one_attempt(db, "zero")
    close(db, zero.id, 0)
    complete(db, zero, SUCCESS)

    not_captured = report_of(db, absent_task.job_id)
    true_zero = report_of(db, zero_task.job_id)

    for report in (not_captured, true_zero):
        assert (report.calls, dict(report.reported_totals)) == (0, {})
        assert report.job_status is JobStatus.SUCCEEDED
    assert not_captured.finality is UsageFinality.LOWER_BOUND
    assert captures(not_captured) == {AttemptCapture.NOT_CAPTURED: 1}
    assert true_zero.finality is UsageFinality.CLOSED
    assert captures(true_zero) == {AttemptCapture.CLOSED: 1}


def test_a_finished_attempt_with_calls_and_no_closure_is_a_lower_bound(db: Conn) -> None:
    task, attempt = one_attempt(db)
    answered(db, attempt.id, 0)
    complete(db, attempt, SUCCESS)

    report = report_of(db, task.job_id)

    assert report.finality is UsageFinality.LOWER_BOUND
    assert captures(report) == {AttemptCapture.LOWER_BOUND: 1}
    assert report.unclosed_attempts == 1
    assert report.assisted_unchecked_tasks == 1


def test_an_intent_after_a_complete_closure_contradicts_it(db: Conn) -> None:
    task, attempt = one_attempt(db)
    answered(db, attempt.id, 0)
    close(db, attempt.id, 1)
    complete(db, attempt, SUCCESS)
    assert report_of(db, task.job_id).finality is UsageFinality.CLOSED
    unresolved(db, attempt.id, 1)  # accepted, and late

    report = report_of(db, task.job_id)

    assert report.finality is UsageFinality.LOWER_BOUND
    assert raised(report) == {UsageAnomalyCode.CLOSURE_CONTRADICTED: 1}
    assert report.anomaly_samples == (
        UsageAnomaly(UsageAnomalyCode.CLOSURE_CONTRADICTED, "t-0", 1),
    )
    assert (report.late_intents, report.unresolved, report.estimated_exposure_units) == (1, 1, 3)


def test_a_job_whose_tasks_were_cancelled_before_any_attempt_is_a_closed_zero(db: Conn) -> None:
    tasks = new_tasks(db, "a", "b")
    for task in tasks:
        request_cancellation(db, task.id, grace=GRACE)

    report = report_of(db, tasks[0].job_id)

    assert report.job_status is JobStatus.CANCELLED
    assert report.finality is UsageFinality.CLOSED
    assert sum(report.attempts_by_capture.values()) == 0
    assert report.calls == 0


# --- units and amounts --------------------------------------------------------


def test_units_are_kept_exactly_and_amounts_are_summed_without_rounding(db: Conn) -> None:
    task, attempt = one_attempt(db)
    answered(db, attempt.id, 0, "0.25", "credits")
    answered(db, attempt.id, 1, "1.50", "credits")
    answered(db, attempt.id, 2, "1", "Credits")
    failed(db, attempt.id, 3, "3", "Credits")  # a failed call that reported usage
    answered(db, attempt.id, 4, "2", MICRO_CREDITS)
    answered(db, attempt.id, 5, BIG, "big")
    answered(db, attempt.id, 6, BIG, "big")
    answered(db, attempt.id, 7, None)  # answered, without usage
    failed(db, attempt.id, 8)  # failed, without usage
    close(db, attempt.id, 9)
    complete(db, attempt, SUCCESS)

    report = report_of(db, task.job_id)

    totals = report.reported_totals
    assert set(totals) == {"credits", "Credits", MICRO_CREDITS, "big"}
    assert (str(totals["credits"].amount), totals["credits"].calls) == ("1.75", 2)
    assert totals["Credits"] == UnitTotal(Decimal(4), 2)
    assert totals[MICRO_CREDITS] == UnitTotal(Decimal(2), 1)
    assert (str(totals["big"].amount), totals["big"].calls) == (TWICE_BIG, 2)
    assert len(totals["big"].amount.as_tuple().digits) == 39
    # Summed in Python's default 28-digit context, the same two amounts would round.
    assert Context().add(Decimal(BIG), Decimal(BIG)) != Decimal(TWICE_BIG)
    assert (report.calls, report.answered, report.failed) == (9, 7, 2)
    assert report.settled_without_reported_usage == 2
    assert dict(report.by_attempt_state[AttemptState.SUCCEEDED].reported_totals) == dict(totals)
    assert report.finality is UsageFinality.CLOSED
    assert_conserved(report)


# --- open finality -------------------------------------------------------------


def test_a_running_attempt_makes_the_figures_open(db: Conn) -> None:
    task, attempt = one_attempt(db)
    answered(db, attempt.id, 0)

    report = report_of(db, task.job_id)

    assert report.job_status is JobStatus.RUNNING
    assert report.finality is UsageFinality.OPEN
    assert captures(report) == {AttemptCapture.OPEN: 1}
    assert report.unclosed_attempts == 0


def test_a_queued_task_makes_the_figures_open(db: Conn) -> None:
    done, _queued = new_tasks(db, "a", "b")
    attempt = start_attempt(db, done.id, "w", policy=POLICY)
    close(db, attempt.id, 0)
    complete(db, attempt, SUCCESS)

    report = report_of(db, done.job_id)

    assert report.job_status is JobStatus.RUNNING
    assert captures(report) == {AttemptCapture.CLOSED: 1}
    assert report.finality is UsageFinality.OPEN


# --- the cross-check with real handler results ----------------------------------


def run_recording(
    db: Conn, task: Task, handler: Handler, record: Callable[[uuid.UUID], None]
) -> None:
    """Run the real handler on the task's payload, record ledger facts, then complete."""
    attempt = start_attempt(db, task.id, "worker-1", policy=POLICY)
    outcome = run_handler(handler, context_of(task, attempt), task.payload)
    assert isinstance(outcome, AttemptSucceeded)
    record(attempt.id)
    complete_attempt(
        db, attempt.id, generation=attempt.attempt_number, outcome=outcome, retry_policy=RETRY
    )


def test_provider_assisted_items_are_cross_checked_against_answered_calls(db: Conn) -> None:
    job = submit_items(db, [ambiguous_unit(), ambiguous_unit(), ambiguous_unit()], batch_size=1)
    handler = make_handler()
    enough, too_few, unclosed = list_tasks(db, job.id)

    def one_answer_closed(attempt_id: uuid.UUID) -> None:
        answered(db, attempt_id, 0)
        close(db, attempt_id, 1)

    def no_call_closed(attempt_id: uuid.UUID) -> None:
        close(db, attempt_id, 0)

    def one_answer_unclosed(attempt_id: uuid.UUID) -> None:
        answered(db, attempt_id, 0)

    run_recording(db, enough, handler, one_answer_closed)
    run_recording(db, too_few, handler, no_call_closed)
    run_recording(db, unclosed, handler, one_answer_unclosed)

    report = report_of(db, job.id)
    summary = summarize_invoice_job(db, job.id)

    assert summary is not None
    assert summary.provider_assisted_items == 3
    assert report.assisted_items == summary.provider_assisted_items
    assert (report.assisted_checked_tasks, report.assisted_unchecked_tasks) == (2, 1)
    assert raised(report) == {UsageAnomalyCode.ASSISTED_EXCEEDS_ANSWERED: 1}
    assert report.anomaly_samples == (
        UsageAnomaly(UsageAnomalyCode.ASSISTED_EXCEEDS_ANSWERED, too_few.task_key, 1),
    )
    assert report.finality is UsageFinality.LOWER_BOUND  # the unclosed attempt
    assert_invariants(db)


# --- one snapshot ---------------------------------------------------------------


def gate(monkeypatch: pytest.MonkeyPatch, method_name: str, commit: Callable[[], object]) -> None:
    """Make ``JobBatchSnapshot.<method_name>`` run ``commit`` first, then the read.

    ``commit`` commits on another session. The reading session takes no row
    locks, so the commit never waits for it, and the order is fixed: the commit
    happens before the gated read starts, and after every read before it.
    """
    original: Callable[[snapshots.JobBatchSnapshot], object] = getattr(
        snapshots.JobBatchSnapshot, method_name
    )

    def gated(self: snapshots.JobBatchSnapshot) -> object:
        commit()
        return original(self)

    monkeypatch.setattr(snapshots.JobBatchSnapshot, method_name, gated)


def gated_report_of(
    db: Conn,
    job_id: uuid.UUID,
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    commit: Callable[[], object],
) -> JobUsageReport:
    """One report whose ``method_name`` read runs after ``commit``; later ones are not gated."""
    with monkeypatch.context() as patch:
        gate(patch, method_name, commit)
        return report_of(db, job_id)


def gated_report(
    db: Conn, other: Conn, monkeypatch: pytest.MonkeyPatch
) -> tuple[uuid.UUID, JobUsageReport]:
    """Read the job row, commit a late result and a late intent elsewhere, then read the ledger."""
    task, attempt = one_attempt(db)
    answered(db, attempt.id, 0)
    unresolved(db, attempt.id, 1)
    complete(db, attempt, SUCCESS)

    def commit_late_facts() -> None:
        assert record_call_result(other, an_answer(attempt.id, 1)).created
        assert record_call_intent(other, an_intent(attempt.id, 2)).created

    report = gated_report_of(db, task.job_id, monkeypatch, "usage_totals", commit_late_facts)
    return task.job_id, report


def test_ledger_facts_committed_after_the_job_read_are_not_seen(
    db: Conn, other: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id, inside = gated_report(db, other, monkeypatch)

    assert (inside.calls, inside.answered, inside.unresolved) == (2, 1, 1)
    assert dict(inside.reported_totals) == {"credits": UnitTotal(Decimal("0.25"), 1)}
    assert (inside.late_intents, inside.late_results) == (0, 0)
    assert_conserved(inside)

    fresh = report_of(db, job_id)
    assert (fresh.calls, fresh.answered, fresh.unresolved) == (3, 2, 1)
    assert dict(fresh.reported_totals) == {"credits": UnitTotal(Decimal("0.50"), 2)}
    assert (fresh.late_intents, fresh.late_results) == (1, 1)


def test_the_snapshot_test_fails_when_the_isolation_level_is_weakened(
    db: Conn, other: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(snapshots, "_SET_TRANSACTION", SET_READ_COMMITTED)

    _, inside = gated_report(db, other, monkeypatch)

    # The reads saw the later commits, so the assertions of the test above would fail.
    assert inside.calls == 3
    assert (inside.late_intents, inside.late_results) == (1, 1)
    assert dict(inside.reported_totals) == {"credits": UnitTotal(Decimal("0.50"), 2)}


def report_with_a_result_between_totals_and_attempts(
    db: Conn, other: Conn, monkeypatch: pytest.MonkeyPatch
) -> tuple[uuid.UUID, JobUsageReport]:
    """Read the totals, commit a late answer (0.25 credits) elsewhere, then read the attempts."""
    task, attempt = one_attempt(db)
    answered(db, attempt.id, 0)
    unresolved(db, attempt.id, 1)
    complete(db, attempt, SUCCESS)

    def commit_late_result() -> None:
        assert record_call_result(other, an_answer(attempt.id, 1)).created

    report = gated_report_of(db, task.job_id, monkeypatch, "attempt_usage", commit_late_result)
    return task.job_id, report


def test_the_totals_and_the_attempts_are_read_in_one_snapshot(
    db: Conn, other: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id, inside = report_with_a_result_between_totals_and_attempts(db, other, monkeypatch)

    credits = inside.reported_totals["credits"]
    assert credits == UnitTotal(Decimal("0.25"), 1)
    assert (inside.answered, inside.unresolved, inside.late_results) == (1, 1, 0)
    assert credits.calls == inside.answered
    assert_conserved(inside)

    fresh = report_of(db, job_id)
    assert (fresh.answered, fresh.unresolved, fresh.late_results) == (2, 0, 1)
    assert dict(fresh.reported_totals) == {"credits": UnitTotal(Decimal("0.50"), 2)}


def test_the_totals_and_attempts_test_fails_when_the_isolation_level_is_weakened(
    db: Conn, other: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(snapshots, "_SET_TRANSACTION", SET_READ_COMMITTED)

    _, inside = report_with_a_result_between_totals_and_attempts(db, other, monkeypatch)

    # The totals predate the commit and the attempts follow it, so they disagree.
    assert inside.reported_totals["credits"].calls == 1
    assert (inside.answered, inside.unresolved, inside.late_results) == (2, 0, 1)


def report_with_a_completion_between_attempts_and_tasks(
    db: Conn, other: Conn, monkeypatch: pytest.MonkeyPatch
) -> tuple[uuid.UUID, JobUsageReport]:
    """Read a running attempt, complete it elsewhere, then read its task."""
    task, attempt = one_attempt(db)
    answered(db, attempt.id, 0)

    def commit_completion() -> None:
        complete(other, attempt, SUCCESS)

    report = gated_report_of(db, task.job_id, monkeypatch, "tasks", commit_completion)
    return task.job_id, report


def test_an_attempt_and_its_task_are_read_in_one_snapshot(
    db: Conn, other: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id, inside = report_with_a_completion_between_attempts_and_tasks(db, other, monkeypatch)

    assert inside.job_status is JobStatus.RUNNING
    assert captures(inside) == {AttemptCapture.OPEN: 1}
    assert inside.finality is UsageFinality.OPEN
    assert (inside.assisted_checked_tasks, inside.assisted_unchecked_tasks) == (0, 0)

    fresh = report_of(db, job_id)
    assert fresh.job_status is JobStatus.SUCCEEDED
    assert captures(fresh) == {AttemptCapture.LOWER_BOUND: 1}
    assert fresh.finality is UsageFinality.LOWER_BOUND
    assert fresh.unclosed_attempts == 1
    assert fresh.assisted_unchecked_tasks == 1


def test_the_attempt_and_task_test_fails_when_the_isolation_level_is_weakened(
    db: Conn, other: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(snapshots, "_SET_TRANSACTION", SET_READ_COMMITTED)

    _, inside = report_with_a_completion_between_attempts_and_tasks(db, other, monkeypatch)

    # A succeeded task is reported next to the running attempt read before it.
    assert inside.job_status is JobStatus.SUCCEEDED
    assert captures(inside) == {AttemptCapture.OPEN: 1}
    assert inside.assisted_unchecked_tasks == 1


def test_reports_read_while_calls_are_recorded_each_describe_one_state(
    db: Conn, schema_url: str
) -> None:
    tasks = new_tasks(db, "a", "b", "c")
    attempts = [start_attempt(db, task.id, "w", policy=POLICY) for task in tasks]
    calls_each = 15
    failures: list[BaseException] = []

    def record() -> None:
        try:
            with connect(schema_url) as writer:
                for seq in range(calls_each):
                    for attempt in attempts:
                        record_call_intent(writer, an_intent(attempt.id, seq))
                        record_call_result(writer, an_answer(attempt.id, seq))
        except Exception as exc:  # returned to the test thread, which inspects it
            failures.append(exc)

    recording = threading.Thread(target=record)
    recording.start()
    reports: list[JobUsageReport] = []
    while recording.is_alive():
        reports.append(report_of(db, tasks[0].job_id))
    recording.join()
    reports.append(report_of(db, tasks[0].job_id))

    assert failures == []
    calls = [r.calls for r in reports]
    settled = [r.answered + r.failed for r in reports]
    observe("calls seen by each concurrent report: " + json.dumps(calls))
    assert calls == sorted(calls)  # the ledger only grows, and each read is one state
    assert settled == sorted(settled)
    for report in reports:
        assert_conserved(report)
        assert report.finality is UsageFinality.OPEN
        credits = report.reported_totals.get("credits")
        assert (0 if credits is None else credits.calls) == report.answered
    final = reports[-1]
    assert (final.calls, final.answered, final.unresolved) == (45, 45, 0)
    assert final.reported_totals["credits"] == UnitTotal(Decimal("11.25"), 45)


# --- conservation against an independent oracle ---------------------------------

UNITS = ["credits", "Credits", MICRO_CREDITS]
AMOUNTS = ["0.25", "1.50", "0.000000000000000001", BIG, "7"]
RESULT_KINDS = ["unresolved", "answered", "answered", "bare", "failed", "failed-with-usage"]


def random_result(rng: random.Random, attempt_id: uuid.UUID, seq: int, kind: str) -> CallResult:
    amount, unit = Decimal(rng.choice(AMOUNTS)), rng.choice(UNITS)
    if kind == "answered":
        return an_answer(attempt_id, seq, reported_amount=amount, reported_unit=unit)
    if kind == "bare":
        return an_answer(attempt_id, seq, reported_amount=None, reported_unit=None)
    usage = kind == "failed-with-usage"
    return CallResult(
        attempt_id=attempt_id,
        call_seq=seq,
        outcome=CallOutcome.FAILED,
        failure_code=CallFailure.TIMEOUT,
        reported_amount=amount if usage else None,
        reported_unit=unit if usage else None,
    )


def random_calls(db: Conn, rng: random.Random, attempt_id: uuid.UUID) -> tuple[int, list[int]]:
    """Record 0 to 4 calls; return how many, and the call numbers left without a result."""
    pending: list[int] = []
    count = rng.randrange(5)
    for seq in range(count):
        estimate = rng.choice([None, 0, 3, 11])
        record_call_intent(db, an_intent(attempt_id, seq, estimated_cost_units=estimate))
        kind = rng.choice(RESULT_KINDS)
        if kind == "unresolved":
            pending.append(seq)
        else:
            record_call_result(db, random_result(rng, attempt_id, seq, kind))
    return count, pending


OUTCOMES: dict[str, AttemptOutcome] = {"retry": TRANSIENT, "succeed": SUCCESS, "fail": PERMANENT}


def random_ledger(db: Conn, rng: random.Random, tasks: int) -> uuid.UUID:
    """A job whose tasks retry, succeed, fail or keep running, with random ledger facts."""
    job_tasks = new_tasks(db, *(f"t-{i:02d}" for i in range(tasks)), max_attempts=3)
    for task in job_tasks:
        for number in range(1, 4):
            attempt = start_attempt(db, task.id, "w", policy=POLICY)
            count, pending = random_calls(db, rng, attempt.id)
            closure = rng.choice(["none", "incomplete", "complete"])
            if closure == "complete" and not pending:
                close(db, attempt.id, count)
            elif closure == "incomplete":
                record_usage_closure(db, incomplete(attempt.id))
            endings = ["retry", "retry", "succeed", "fail", "run"]
            ending = rng.choice(endings if number < 3 else endings[2:])
            if ending == "run":
                break
            complete(db, attempt, OUTCOMES[ending])
            if rng.random() < 0.5:  # a late fact, after the attempt finished
                if pending:
                    record_call_result(db, an_answer(attempt.id, pending[0]))
                else:
                    record_call_intent(db, an_intent(attempt.id, count))
            if ending != "retry":
                break
    return job_tasks[0].job_id


@dataclasses.dataclass
class Tally:
    attempts: int = 0
    calls: int = 0
    answered: int = 0
    failed: int = 0
    unresolved: int = 0
    totals: dict[str, UnitTotal] = dataclasses.field(default_factory=dict)


def as_tally(usage: ProvenanceUsage) -> Tally:
    return Tally(
        attempts=usage.attempts,
        calls=usage.calls,
        answered=usage.answered,
        failed=usage.failed,
        unresolved=usage.unresolved,
        totals=dict(usage.reported_totals),
    )


def add_amount(totals: dict[str, UnitTotal], unit: str, amount: Decimal) -> None:
    old = totals.get(unit, UnitTotal(Decimal(0), 0))
    totals[unit] = UnitTotal(old.amount + amount, old.calls + 1)


def oracle(
    db: Conn, job_id: uuid.UUID
) -> tuple[dict[str, int], dict[AttemptState, Tally], dict[str, UnitTotal]]:
    """The job's figures from ``read_attempt_usage``, attempt by attempt, summed exactly."""
    counts = dict.fromkeys(
        ["settled_without", "late_intents", "late_results", "exposure", "without_estimate"], 0
    )
    states = {state: Tally() for state in AttemptState}
    job_totals: dict[str, UnitTotal] = {}
    with localcontext(Context(prec=200)):
        for task in list_tasks(db, job_id):
            for attempt in list_attempts(db, task.id):
                usage = read_attempt_usage(db, attempt.id)
                assert usage is not None
                tally = states[usage.attempt_state]
                tally.attempts += 1
                for call in usage.calls:
                    tally.calls += 1
                    counts["late_intents"] += call.call.late
                    if call.result is None:
                        tally.unresolved += 1
                        estimate = call.call.intent.estimated_cost_units
                        if estimate is None:
                            counts["without_estimate"] += 1
                        else:
                            counts["exposure"] += estimate
                        continue
                    counts["late_results"] += call.result.late
                    result = call.result.result
                    if result.outcome is CallOutcome.ANSWERED:
                        tally.answered += 1
                    else:
                        tally.failed += 1
                    if result.reported_amount is None or result.reported_unit is None:
                        counts["settled_without"] += 1
                        continue
                    add_amount(tally.totals, result.reported_unit, result.reported_amount)
                    add_amount(job_totals, result.reported_unit, result.reported_amount)
    return counts, states, job_totals


@pytest.mark.parametrize("seed_value", [7, 11, 20261104])
def test_a_random_ledger_matches_an_independent_oracle(db: Conn, seed_value: int) -> None:
    job_id = random_ledger(db, random.Random(seed_value), tasks=8)

    report = report_of(db, job_id)
    counts, states, job_totals = oracle(db, job_id)

    assert report.settled_without_reported_usage == counts["settled_without"]
    assert (report.late_intents, report.late_results) == (
        counts["late_intents"],
        counts["late_results"],
    )
    assert report.estimated_exposure_units == counts["exposure"]
    assert report.unresolved_without_estimate == counts["without_estimate"]
    assert {state: as_tally(usage) for state, usage in report.by_attempt_state.items()} == states
    assert dict(report.reported_totals) == job_totals
    assert report.calls == sum(t.calls for t in states.values())
    assert_conserved(report)
    assert_invariants(db)


# --- the snapshot contract --------------------------------------------------------


def test_the_ledger_reads_run_inside_the_read_only_snapshot(db: Conn) -> None:
    task, first, second, third = retried(db)

    with job_batch_snapshot(db, task.job_id) as snap:
        assert snap is not None
        assert db.execute("SHOW transaction_isolation").fetchone() == ("repeatable read",)
        assert db.execute("SHOW transaction_read_only").fetchone() == ("on",)
        totals = snap.usage_totals()
        stream = snap.attempt_usage()
        cursors = db.execute("SELECT name FROM pg_cursors").fetchall()
        assert [name.startswith("orqestra_attempt_usage_") for (name,) in cursors] == [True]
        attempts = list(stream)

    assert db.info.transaction_status is IDLE
    assert db.execute("SELECT count(*) FROM pg_cursors").fetchone() == (0,)
    assert {(t.attempt_state, t.unit, t.calls, t.amount) for t in totals} == {
        (None, "credits", 3, Decimal("2.00")),
        (AttemptState.REVOKED, "credits", 1, Decimal("0.25")),
        (AttemptState.FAILED, "credits", 1, Decimal("1.50")),
        (AttemptState.SUCCEEDED, "credits", 1, Decimal("0.25")),
    }
    assert [(a.attempt_id, a.attempt_number, a.state) for a in attempts] == [
        (first.id, 1, AttemptState.REVOKED),
        (second.id, 2, AttemptState.FAILED),
        (third.id, 3, AttemptState.SUCCEEDED),
    ]
    revoked, _, done = attempts
    assert (revoked.task_id, revoked.task_key) == (task.id, "t-0")
    assert (revoked.intents, revoked.answered, revoked.failed, revoked.unresolved) == (2, 1, 0, 1)
    assert (revoked.unresolved_estimate_units, revoked.unresolved_without_estimate) == (5, 0)
    assert (revoked.closure_capture, revoked.closure_call_count) == (CaptureState.INCOMPLETE, None)
    assert (revoked.closure_closed_by, revoked.closure_tier) == (ClosedBy.RECOVERY, CaptureTier.T1)
    assert (done.closure_capture, done.closure_call_count) == (CaptureState.COMPLETE, 1)
    assert (done.closure_closed_by, done.closure_tier) == (ClosedBy.WORKER, CaptureTier.T1)


def test_reading_a_report_changes_no_table(db: Conn) -> None:
    task, *_ = retried(db)
    before = (snapshot(db), ledger(db))

    report_of(db, task.job_id)

    assert (snapshot(db), ledger(db)) == before


def test_the_connection_is_outside_a_transaction_after_a_report(conn: Conn, db: Conn) -> None:
    task, *_ = retried(db)

    report_of(conn, task.job_id)

    assert conn.info.transaction_status is IDLE


def test_the_connection_is_outside_a_transaction_after_an_error(
    conn: Conn, db: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    task, *_ = retried(db)

    def fail(self: UsageReportBuilder, row: object) -> None:
        msg = "boom"
        raise RuntimeError(msg)

    monkeypatch.setattr(UsageReportBuilder, "add_attempt", fail)

    with pytest.raises(RuntimeError, match="boom"):
        report_job_usage(conn, task.job_id, fetch_size=1)

    assert conn.info.transaction_status is IDLE
    conn.execute("SELECT 1")  # the connection is usable
    conn.commit()


def abandon_the_attempt_stream(conn: Conn, job_id: uuid.UUID) -> None:
    with job_batch_snapshot(conn, job_id, fetch_size=1) as snap:
        assert snap is not None
        stream = snap.attempt_usage()
        next(stream)  # one row of three, then the block ends


def test_the_connection_is_outside_a_transaction_after_an_abandoned_stream(
    conn: Conn, db: Conn
) -> None:
    task, *_ = retried(db)

    abandon_the_attempt_stream(conn, task.job_id)

    assert conn.info.transaction_status is IDLE
    assert conn.execute("SELECT count(*) FROM pg_cursors").fetchone() == (0,)
    conn.commit()


def read_after_the_block(conn: Conn, job_id: uuid.UUID, read: str) -> None:
    with job_batch_snapshot(conn, job_id) as snap:
        assert snap is not None
        ended = snap
    getattr(ended, read)()


def test_the_ledger_reads_run_only_while_the_snapshot_is_open(conn: Conn, db: Conn) -> None:
    task, *_ = retried(db)

    with job_batch_snapshot(conn, task.job_id) as snap:
        assert snap is not None
        snap.attempt_usage()
        snap.tasks()  # a stream of its own, alongside
        with pytest.raises(RuntimeError, match="already opened"):
            snap.attempt_usage()
        assert snap.usage_totals() == snap.usage_totals()  # a plain query, repeatable
    for read in ("attempt_usage", "usage_totals"):
        with pytest.raises(RuntimeError, match="has ended"):
            read_after_the_block(conn, task.job_id, read)

    assert conn.info.transaction_status is IDLE


def test_a_caller_open_transaction_is_refused(conn: Conn, db: Conn) -> None:
    task, *_ = retried(db)

    with conn.transaction():
        with pytest.raises(RuntimeError, match="own transaction"):
            report_job_usage(conn, task.job_id)
        assert conn.info.transaction_status is TransactionStatus.INTRANS

    assert conn.info.transaction_status is IDLE


def test_a_missing_job_is_none(conn: Conn) -> None:
    assert report_job_usage(conn, uuid.uuid4()) is None
    assert conn.info.transaction_status is IDLE


def test_another_kind_of_job_is_refused_before_any_ledger_row_is_read(
    conn: Conn, db: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    (task,) = new_tasks(db, "x", kind="diagnostic.echo")

    def never_read(self: object) -> None:
        pytest.fail("the ledger or the tasks were read for a job of another kind")

    for read in ("usage_totals", "attempt_usage", "tasks"):
        monkeypatch.setattr(snapshots.JobBatchSnapshot, read, never_read)

    with pytest.raises(WrongJobKindError, match=r"diagnostic\.echo") as excinfo:
        report_job_usage(conn, task.job_id)

    assert excinfo.value.job_id == task.job_id
    assert conn.info.transaction_status is IDLE


def test_a_job_without_tasks_is_a_value_error(conn: Conn, db: Conn) -> None:
    job_id = uuid.uuid4()
    db.execute("INSERT INTO jobs (id, kind) VALUES (%s, %s)", (job_id, "invoice_verification"))

    with pytest.raises(ValueError, match="at least one task"):
        report_job_usage(conn, job_id)

    assert conn.info.transaction_status is IDLE


def test_the_report_is_the_same_whatever_the_fetch_size(db: Conn) -> None:
    job_id = random_ledger(db, random.Random(5), tasks=6)

    reference = comparable(report_of(db, job_id, fetch_size=1000))

    assert sum(reference.attempts_by_capture.values()) >= 6
    for fetch_size in (1, 2, 1000):
        assert comparable(report_of(db, job_id, fetch_size=fetch_size)) == reference


# --- scale (outside the default lanes; observations, not thresholds) ---------------

_CLAIM_TASKS = """
UPDATE tasks SET state = 'running', attempt_count = attempt_count + 1, updated_at = now()
WHERE job_id = %(job)s AND state = 'queued'
  AND (NOT %(every_tenth)s OR mod(right(task_key, 7)::integer, 10) = 0)
"""

_INSERT_REVOKED_ATTEMPTS = """
INSERT INTO attempts (id, task_id, attempt_number, worker_id, state, started_at, finished_at,
    lease_expires_at, deadline_at, revoked_reason)
SELECT gen_random_uuid(), t.id, t.attempt_count, 'seed-worker', 'revoked',
    now() - interval '2 hours', now() - interval '100 minutes',
    now() - interval '110 minutes', now() - interval '1 hour', 'lease_expired'
FROM tasks t
WHERE t.job_id = %(job)s AND t.state = 'running'
"""

_REQUEUE_TASKS = """
UPDATE tasks SET state = 'queued', updated_at = now()
WHERE job_id = %(job)s AND state = 'running'
"""

_INSERT_SUCCEEDED_ATTEMPTS = """
INSERT INTO attempts (id, task_id, attempt_number, worker_id, state, started_at, finished_at,
    lease_expires_at, deadline_at)
SELECT gen_random_uuid(), t.id, t.attempt_count, 'seed-worker', 'succeeded',
    now() - interval '30 minutes', now(),
    now() - interval '25 minutes', now() + interval '30 minutes'
FROM tasks t
WHERE t.job_id = %(job)s AND t.state = 'running'
"""

# Two calls per revoked attempt, and one per invoice in each succeeded attempt.
_INSERT_INTENTS = """
INSERT INTO provider_calls (attempt_id, call_seq, job_id, task_id, provider, route_id,
    capability, field_kind, item_index, estimated_cost_units, attempt_state_at_record)
SELECT a.id, s, t.job_id, t.id, 'mock', 'r1', 'interpret_amount', NULL, NULL, 3, 'running'
FROM tasks t
JOIN attempts a ON a.task_id = t.id AND a.state = 'revoked'
CROSS JOIN generate_series(0, 1) AS s
WHERE t.job_id = %(job)s
UNION ALL
SELECT a.id, i - (t.payload ->> 'offset')::integer, t.job_id, t.id, 'mock', 'r1',
    'interpret_amount', 'unit_amount', i, 3, 'running'
FROM tasks t
JOIN attempts a ON a.task_id = t.id AND a.state = 'succeeded'
CROSS JOIN LATERAL generate_series(
    (t.payload ->> 'offset')::integer,
    (t.payload ->> 'offset')::integer + jsonb_array_length(t.payload -> 'items') - 1
) AS i
WHERE t.job_id = %(job)s
"""

# Every call is answered with 0.25 credits, except the revoked attempts' last call.
_INSERT_RESULTS = """
INSERT INTO provider_call_results (attempt_id, call_seq, outcome, reported_amount,
    reported_unit, attempt_state_at_record)
SELECT c.attempt_id, c.call_seq, 'answered', 0.25, 'credits', 'running'
FROM provider_calls c
JOIN attempts a ON a.id = c.attempt_id
WHERE c.job_id = %(job)s AND (a.state = 'succeeded' OR c.call_seq = 0)
"""

_INSERT_CLOSURES = """
INSERT INTO attempt_usage_closures (attempt_id, capture, call_count, closed_by, capture_tier)
SELECT a.id,
    CASE WHEN a.state = 'succeeded' THEN 'complete' ELSE 'incomplete' END,
    CASE WHEN a.state = 'succeeded' THEN jsonb_array_length(t.payload -> 'items') END,
    CASE WHEN a.state = 'succeeded' THEN 'worker' ELSE 'recovery' END,
    't1'
FROM tasks t
JOIN attempts a ON a.task_id = t.id
WHERE t.job_id = %(job)s
"""

# Every item is verified with a provider's help, as the handler would store it.
_COMPLETE_RUNNING_TASKS = """
UPDATE tasks t
SET state = 'succeeded', finished_at = now(), updated_at = now(),
    result = jsonb_build_object(
        'offset', t.payload -> 'offset',
        'results', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'index', i, 'status', 'verified', 'provider_assisted', true,
                    'findings', '[]'::jsonb, 'issues', '[]'::jsonb,
                    'reason', NULL::text, 'routing', jsonb_build_object('route_id', 'r1')
                ) ORDER BY i)
            FROM generate_series(
                (t.payload ->> 'offset')::bigint,
                (t.payload ->> 'offset')::bigint + jsonb_array_length(t.payload -> 'items') - 1
            ) AS i))
WHERE t.job_id = %(job)s AND t.state = 'running'
"""

_ROW_COUNTS = """
SELECT
    (SELECT count(*) FROM tasks WHERE job_id = %(job)s),
    (SELECT count(*) FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE t.job_id = %(job)s),
    (SELECT count(*) FROM provider_calls WHERE job_id = %(job)s),
    (SELECT count(*) FROM provider_call_results r
        JOIN provider_calls c ON c.attempt_id = r.attempt_id AND c.call_seq = r.call_seq
        WHERE c.job_id = %(job)s),
    (SELECT count(*) FROM attempt_usage_closures u
        JOIN attempts a ON a.id = u.attempt_id JOIN tasks t ON t.id = a.task_id
        WHERE t.job_id = %(job)s)
"""

OBSERVED_SETTINGS = [
    "shared_buffers",
    "work_mem",
    "max_wal_size",
    "min_wal_size",
    "checkpoint_timeout",
    "max_worker_processes",
    "max_parallel_workers",
    "max_parallel_workers_per_gather",
    "jit",
    "jit_above_cost",
]


def seed_ledger(db: Conn, invoices: int, batch_size: int) -> uuid.UUID:
    """Seed with set-based SQL that goes through the fencing guard, as claims would.

    Every tenth task is first claimed and revoked with one answered and one
    unresolved call, closed incomplete by recovery, and requeued. Then every
    task is claimed and succeeds with one answered call per invoice, closed
    complete.
    """
    job_id = seed(db, invoices, batch_size)
    params = {"job": job_id}
    db.execute(_CLAIM_TASKS, {**params, "every_tenth": True})
    db.execute(_INSERT_REVOKED_ATTEMPTS, params)
    db.execute(_REQUEUE_TASKS, params)
    db.execute(_CLAIM_TASKS, {**params, "every_tenth": False})
    db.execute(_INSERT_SUCCEEDED_ATTEMPTS, params)
    for statement in (_INSERT_INTENTS, _INSERT_RESULTS, _INSERT_CLOSURES, _COMPLETE_RUNNING_TASKS):
        db.execute(statement, params)
    db.execute(f"ANALYZE jobs, tasks, attempts, {', '.join(LEDGER_TABLES)}")
    return job_id


CGROUP_CPU_MAX = Path("/sys/fs/cgroup/cpu.max")


def client_cpu_affinity() -> int | None:
    """How many CPUs the test process may be scheduled on, where the platform says."""
    affinity: Callable[[int], set[int]] | None = getattr(os, "sched_getaffinity", None)
    return None if affinity is None else len(affinity(0))


def client_cgroup_cpu_max() -> str | None:
    """The test process's cgroup v2 CPU quota and period, if readable.

    A CPU limit set as a quota does not change the affinity count, so both are kept.
    """
    try:
        return CGROUP_CPU_MAX.read_text().strip()
    except OSError:
        return None


def environment(db: Conn) -> dict[str, Any]:
    version = db.execute("SHOW server_version").fetchone()
    assert version is not None
    settings = db.execute(
        "SELECT name, setting, unit FROM pg_settings WHERE name = ANY(%s) ORDER BY name",
        (OBSERVED_SETTINGS,),
    ).fetchall()
    return {
        "server_version": version[0],
        "settings": {name: f"{setting}{unit or ''}" for name, setting, unit in settings},
        "python": platform.python_version(),
        "psycopg": psycopg.__version__,
        "cpu_count": os.cpu_count(),
        # Both describe the test process, not the database server.
        "client_cpu_affinity": client_cpu_affinity(),
        "client_cgroup_cpu_max": client_cgroup_cpu_max(),
    }


def explain(db: Conn, query: str, params: tuple[uuid.UUID, ...]) -> dict[str, Any]:
    """A summary of ``EXPLAIN (ANALYZE, BUFFERS)`` for one ledger query."""
    with psycopg.ClientCursor(db) as cur:
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}", params)
        row = cur.fetchone()
    assert row is not None
    (plan,) = row[0]
    top = plan["Plan"]
    keys = ("Shared Hit Blocks", "Shared Read Blocks", "Temp Read Blocks", "Temp Written Blocks")
    return {
        "planning_ms": plan.get("Planning Time"),
        "execution_ms": plan.get("Execution Time"),
        "top_node": top.get("Node Type"),
        **{key.lower().replace(" ", "_"): top.get(key) for key in keys},
    }


def measure(call: Callable[[], JobUsageReport]) -> tuple[JobUsageReport, dict[str, float]]:
    """Wall time, then a second run under tracemalloc.

    ``ru_maxrss`` is the whole process's high-water mark, in the platform's unit.
    """
    started = time.perf_counter()
    call()
    wall = time.perf_counter() - started
    tracemalloc.start()
    try:
        report = call()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return report, {"wall_s": wall, "tracemalloc_peak_bytes": peak, "max_rss_raw": peak_rss}


@pytest.mark.performance
@pytest.mark.parametrize("batch_size", [1, 100, 1000])
def test_observe_a_usage_report_at_fifty_thousand_invoices(
    db: Conn, batch_size: int, record_property: Callable[[str, object], None]
) -> None:
    invoices = 50_000
    tasks = invoices // batch_size
    revoked = (tasks + 9) // 10
    job_id = seed_ledger(db, invoices, batch_size)
    counts = db.execute(_ROW_COUNTS, {"job": job_id}).fetchone()
    assert counts is not None

    report, observed = measure(lambda: report_of(db, job_id))

    plans = {
        "usage_totals": explain(db, snapshots._USAGE_TOTALS, (job_id,)),
        "attempt_usage": explain(db, snapshots._STREAM_ATTEMPT_USAGE, (job_id, job_id)),
    }
    names = ["tasks", "attempts", "calls", "results", "closures"]
    record = {
        "invoices": invoices,
        "batch_size": batch_size,
        "rows": dict(zip(names, counts, strict=True)),
        **observed,
        "plans": plans,
        "environment": environment(db),
    }
    observe(json.dumps(record, default=str))
    record_property("usage_report_observation", json.dumps(record, default=str))

    assert report.job_status is JobStatus.SUCCEEDED
    assert report.finality is UsageFinality.LOWER_BOUND
    assert (report.calls, report.answered) == (invoices + 2 * revoked, invoices + revoked)
    assert (report.failed, report.unresolved) == (0, revoked)
    expected = UnitTotal(Decimal("0.25") * (invoices + revoked), invoices + revoked)
    assert dict(report.reported_totals) == {"credits": expected}
    assert report.estimated_exposure_units == 3 * revoked
    assert captures(report) == {AttemptCapture.CLOSED: tasks, AttemptCapture.LOWER_BOUND: revoked}
    assert report.by_attempt_state[AttemptState.REVOKED].attempts == revoked
    assert report.assisted_items == invoices
    assert (report.assisted_checked_tasks, report.assisted_unchecked_tasks) == (tasks, 0)
    assert raised(report) == {}
    assert_conserved(report)
