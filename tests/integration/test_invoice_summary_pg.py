"""The invoice-job summary against real PostgreSQL.

Happy paths go through the real lifecycle (partition, submit, start, handler,
complete). Raw SQL is used only to make data the engine would never write: a
result that does not match its payload, a payload that is not a batch, a job
without tasks. The scale tests at the end are marked ``performance``, so no
default lane runs them: they record what they observe (run with ``-s``) and assert
only correctness, never a threshold.
"""

import dataclasses
import json
import resource
import threading
import time
import tracemalloc
import uuid
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from typing import Any, cast

import psycopg
import pytest
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from orqestra.aggregation.invoice import WrongJobKindError, summarize_invoice_job
from orqestra.calls.mock import MockProvider
from orqestra.domain.errors import PermanentError
from orqestra.domain.jobs import (
    Attempt,
    AttemptFailed,
    AttemptSucceeded,
    Job,
    JobStatus,
    JobSubmission,
    JsonValue,
    Task,
    TaskSpec,
)
from orqestra.domain.partitioning import partition
from orqestra.domain.policy import FailureKind
from orqestra.domain.states import TaskState
from orqestra.persistence import snapshots
from orqestra.persistence.jobs import get_job_progress, list_tasks, submit_job
from orqestra.persistence.lifecycle import complete_attempt, request_cancellation, start_attempt
from orqestra.persistence.snapshots import BatchTaskRow, job_batch_snapshot
from orqestra.worker.handlers import CancellationSignal, Handler, TaskContext, run_handler
from orqestra.workloads.invoice.handler import JOB_KIND, _batch, invoice_verification_handler
from orqestra.workloads.invoice.summary import (
    AnomalyCode,
    InvoiceJobSummary,
    SummaryBuilder,
)
from tests.integration.test_attempt_lifecycle import (
    GRACE,
    POLICY,
    RETRY,
    assert_invariants,
    connect,
    snapshot,
)
from tests.unit.test_invoice_summary import (
    assert_partition,
    entry,
    envelope,
    good,
    payload_of,
    project,
    raised,
)
from tests.unit.test_invoice_verify import ROUTE, TIMEOUT, ambiguous_unit, known

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

IDLE = TransactionStatus.IDLE
FINISHED = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}
EPOCH = datetime(2000, 1, 1, tzinfo=UTC)
SET_READ_COMMITTED = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"


# --- helpers -----------------------------------------------------------------


def make_handler() -> Handler:
    return invoice_verification_handler(
        {"r1": MockProvider(["5"] * 4)},
        [ROUTE],
        TIMEOUT,
        max_provider_calls_per_attempt=64,
        non_billable_routes=frozenset({"r1"}),
    )


def submit_items(db: Conn, items: Sequence[JsonValue], batch_size: int) -> Job:
    specs = partition(items, batch_size=batch_size, max_attempts=3, key_prefix="invoices")
    return submit_job(db, JobSubmission(JOB_KIND, specs)).job


def explicit_job(db: Conn, payloads: Sequence[JsonValue], kind: str = JOB_KIND) -> Job:
    specs = tuple(TaskSpec(f"t-{i:03d}", payload, 3) for i, payload in enumerate(payloads))
    return submit_job(db, JobSubmission(kind, specs)).job


def context_of(task: Task, attempt: Attempt) -> TaskContext:
    signal = CancellationSignal(threading.Event())
    return TaskContext(task.id, attempt.id, attempt.attempt_number, JOB_KIND, signal)


def run_task(conn: Conn, task: Task, handler: Handler) -> None:
    """Start the task's attempt, run the real handler on its payload and record the result."""
    attempt = start_attempt(conn, task.id, "worker-1", policy=POLICY)
    outcome = run_handler(handler, context_of(task, attempt), task.payload)
    assert isinstance(outcome, AttemptSucceeded)
    complete_attempt(
        conn,
        attempt.id,
        generation=attempt.attempt_number,
        outcome=outcome,
        retry_policy=RETRY,
    )


def put_in_state(db: Conn, task: Task, state: str) -> None:
    """Move a queued task through the real lifecycle to ``state`` (never to succeeded)."""
    if state == "queued":
        return
    if state == "cancelled":
        request_cancellation(db, task.id, grace=GRACE)
        return
    attempt = start_attempt(db, task.id, "worker-1", policy=POLICY)  # running
    if state == "failed":
        outcome = AttemptFailed(FailureKind.PERMANENT, "tests.Boom")
        complete_attempt(
            db,
            attempt.id,
            generation=attempt.attempt_number,
            outcome=outcome,
            retry_policy=RETRY,
        )


def force_result(db: Conn, task: Task, result: JsonValue) -> None:
    """Raw SQL: mark a task succeeded with exactly this result, as the engine never would."""
    db.execute(
        "UPDATE tasks SET state = 'succeeded', result = %s, finished_at = now(), "
        "updated_at = now() WHERE id = %s",
        (Jsonb(result), task.id),
    )


def summary_of(conn: Conn, job_id: uuid.UUID, **kwargs: Any) -> InvoiceJobSummary:
    summary = summarize_invoice_job(conn, job_id, **kwargs)
    assert summary is not None
    return summary


def comparable(summary: InvoiceJobSummary) -> InvoiceJobSummary:
    """The summary without ``observed_at``, which differs between two reads."""
    return dataclasses.replace(summary, observed_at=EPOCH)


def clock(db: Conn) -> datetime:
    row = db.execute("SELECT clock_timestamp()").fetchone()
    assert row is not None
    return cast(datetime, row[0])


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


# --- the real lifecycle ------------------------------------------------------


def test_a_real_job_is_summarized_from_its_stored_results(db: Conn) -> None:
    rejected = known()
    rejected["total"] = 14
    job = submit_items(db, [known(), rejected, ambiguous_unit()], batch_size=2)
    handler = make_handler()
    for task in list_tasks(db, job.id):
        run_task(db, task, handler)

    before = clock(db)
    summary = summary_of(db, job.id)
    after = clock(db)

    assert summary.job == job
    assert before <= summary.observed_at <= after
    assert summary.expected_items == 3
    assert (summary.verified, summary.rejected) == (2, 1)
    assert (summary.needs_review, summary.malformed) == (0, 0)
    assert summary.provider_assisted_items == 1
    assert summary.integrity_ok
    assert summary.complete
    progress = get_job_progress(db, job.id)
    assert progress is not None
    assert summary.job_status is progress.status is JobStatus.SUCCEEDED
    assert dict(summary.task_counts) == dict(progress.task_counts)
    assert_invariants(db)


@pytest.mark.parametrize(
    ("second", "third"),
    [
        pytest.param("failed", "failed", id="failed"),
        pytest.param("cancelled", "cancelled", id="cancelled"),
        pytest.param("queued", "queued", id="queued"),
        pytest.param("running", "running", id="running"),
        pytest.param("failed", "running", id="mixed"),
        pytest.param("cancelled", "queued", id="cancelled-and-queued"),
    ],
)
def test_a_job_with_unfinished_tasks_counts_their_items_as_unprocessed(
    db: Conn, second: str, third: str
) -> None:
    job = submit_items(db, [known(), known(), known()], batch_size=1)
    first_task, second_task, third_task = list_tasks(db, job.id)
    run_task(db, first_task, make_handler())
    put_in_state(db, second_task, second)
    put_in_state(db, third_task, third)

    summary = summary_of(db, job.id)

    assert summary.processed_items == 1
    assert summary.unprocessed_items == 2
    assert summary.expected_items == 3
    assert summary.integrity_ok
    assert not summary.complete
    progress = get_job_progress(db, job.id)
    assert progress is not None
    assert summary.job_status is progress.status
    assert dict(summary.task_counts) == dict(progress.task_counts)
    assert summary.finished is (progress.status in FINISHED)
    assert_partition(summary)


# --- payloads that are not batches (the guarded projection) ------------------

NON_BATCH_PAYLOADS: list[JsonValue] = [
    5,
    "x",
    None,
    [1],
    {"invoice_id": "INV-1"},
    {"offset": 1.5, "items": []},
    {"offset": 0, "items": "x"},
]

PROJECTED_PAYLOADS: list[JsonValue] = [
    *NON_BATCH_PAYLOADS,
    True,
    [],
    {},
    {"offset": 0, "items": []},
    {"offset": 7, "items": [1, 2]},
    {"offset": 10**30, "items": [{}]},
    {"offset": True, "items": []},
    {"offset": -1, "items": []},
    {"offset": 1.0, "items": []},
    {"offset": "0", "items": []},
    {"offset": None, "items": None},
    {"offset": 0, "items": [], "extra": {"deep": [1]}},
    {"offset": 0},
    {"items": []},
]


def test_the_unguarded_projection_fails_on_payloads_that_are_not_batches(db: Conn) -> None:
    job = explicit_job(db, NON_BATCH_PAYLOADS)

    with pytest.raises(psycopg.DatabaseError):
        db.execute("SELECT payload - 'items' FROM tasks WHERE job_id = %s", (job.id,)).fetchall()
    with pytest.raises(psycopg.DatabaseError):
        db.execute(
            "SELECT jsonb_array_length(payload -> 'items') FROM tasks WHERE job_id = %s",
            (job.id,),
        ).fetchall()
    with pytest.raises(psycopg.DatabaseError):
        db.execute(
            "SELECT count(*) FROM tasks t, jsonb_object_keys(t.payload) WHERE t.job_id = %s",
            (job.id,),
        ).fetchall()


def test_payloads_that_are_not_batches_are_reported_not_raised(db: Conn) -> None:
    job = explicit_job(db, NON_BATCH_PAYLOADS)
    string_task = list_tasks(db, job.id)[1]
    force_result(db, string_task, good(0, 1))  # a perfect-looking result is still ignored

    summary = summary_of(db, job.id)

    assert raised(summary) == {AnomalyCode.PAYLOAD_INVALID: len(NON_BATCH_PAYLOADS)}
    assert [a.task_key for a in summary.anomaly_samples] == [f"t-{i:03d}" for i in range(7)]
    assert summary.expected_items is None
    assert summary.processed_items == 0
    assert not summary.integrity_ok
    assert not summary.complete
    assert db.info.transaction_status is IDLE


def test_the_projection_describes_each_payload_without_returning_it(db: Conn) -> None:
    job = explicit_job(db, PROJECTED_PAYLOADS)

    with job_batch_snapshot(db, job.id) as snap:
        assert snap is not None
        rows = list(snap.tasks())

    assert len(rows) == len(PROJECTED_PAYLOADS)
    for row, payload in zip(rows, PROJECTED_PAYLOADS, strict=True):
        shape = {name: getattr(row, name) for name in project(payload)}
        assert shape == project(payload), payload
        assert row.state is TaskState.QUEUED
        assert row.result is None
    assert {f.name for f in dataclasses.fields(BatchTaskRow)} == {
        "task_id",
        "task_key",
        "state",
        "payload_type",
        "key_count",
        "has_offset",
        "has_items",
        "offset_text",
        "items_length",
        "result",
    }


@pytest.mark.parametrize("payload", PROJECTED_PAYLOADS, ids=repr)
def test_payload_validity_read_back_from_the_database_is_the_handlers_rule(
    db: Conn, payload: JsonValue
) -> None:
    job = explicit_job(db, [payload])
    (task,) = list_tasks(db, job.id)
    try:
        _batch(task.payload)
        handler_accepts = True
    except PermanentError:
        handler_accepts = False

    summary = summary_of(db, job.id)

    assert (summary.anomalies[AnomalyCode.PAYLOAD_INVALID] == 0) is handler_accepts


# --- results that do not match their payloads --------------------------------

# A second batch of two items at offset 2 (indices 2 and 3) after a valid first batch:
# its stored result, the anomalies raised, and its processed, invalid and missing items.
CORRUPT_RESULTS = [
    pytest.param(good(2, 2), {}, 2, 0, 0, id="valid-control"),
    pytest.param(
        envelope(2, [entry(2), entry(2, "rejected")]),
        {AnomalyCode.INDEX_DUPLICATE: 1},
        0,
        1,
        1,
        id="duplicate",
    ),
    pytest.param(
        envelope(2, [entry(2), "x"]),
        {AnomalyCode.ITEM_INVALID: 1},
        1,
        0,
        1,
        id="missing-and-unexpected",
    ),
    pytest.param(
        envelope(2, [entry(2), entry(9)]),
        {AnomalyCode.INDEX_OUT_OF_RANGE: 1},
        1,
        0,
        1,
        id="out-of-range",
    ),
    pytest.param(
        envelope(2, [entry(0), entry(1)]),
        {AnomalyCode.INDEX_OUT_OF_RANGE: 2},
        0,
        0,
        2,
        id="relative-index",
    ),
    pytest.param(None, {AnomalyCode.RESULT_INVALID: 1}, 0, 2, 0, id="null"),
    pytest.param("x", {AnomalyCode.RESULT_INVALID: 1}, 0, 2, 0, id="non-object"),
    pytest.param(
        {"offset": 0, "results": [entry(2), entry(3)]},
        {AnomalyCode.RESULT_OFFSET_MISMATCH: 1},
        0,
        2,
        0,
        id="wrong-offset-envelope",
    ),
    pytest.param(
        envelope(2, [entry(2)]),
        {AnomalyCode.RESULT_LENGTH_MISMATCH: 1},
        0,
        2,
        0,
        id="wrong-length",
    ),
    pytest.param(
        envelope(2, []),
        {AnomalyCode.RESULT_LENGTH_MISMATCH: 1},
        0,
        2,
        0,
        id="empty-results",
    ),
]


@pytest.mark.parametrize(
    ("result", "anomalies", "processed", "invalid", "missing"), CORRUPT_RESULTS
)
def test_a_stored_result_that_does_not_match_its_payload_is_reported_not_raised(
    db: Conn,
    result: JsonValue,
    anomalies: dict[AnomalyCode, int],
    processed: int,
    invalid: int,
    missing: int,
) -> None:
    job = explicit_job(db, [payload_of(0, 2), payload_of(2, 2)])
    first, second = list_tasks(db, job.id)
    force_result(db, first, good(0, 2))
    force_result(db, second, result)

    summary = summary_of(db, job.id)

    assert raised(summary) == anomalies
    assert summary.processed_items == 2 + processed
    assert summary.invalid_items == invalid
    assert summary.missing_items == missing
    assert summary.expected_items == 4
    assert summary.job_status is JobStatus.SUCCEEDED
    assert summary.complete is (not anomalies)
    assert_partition(summary)
    assert db.info.transaction_status is IDLE


def test_a_batch_without_items_is_valid_and_never_complete(db: Conn) -> None:
    job = explicit_job(db, [{"offset": 0, "items": []}])
    (task,) = list_tasks(db, job.id)
    force_result(db, task, envelope(0, []))

    summary = summary_of(db, job.id)

    assert summary.expected_items == 0
    assert raised(summary) == {}
    assert summary.job_status is JobStatus.SUCCEEDED
    assert not summary.complete


def test_ranges_that_leave_a_gap_are_reported(db: Conn) -> None:
    job = explicit_job(db, [payload_of(0, 2), payload_of(3, 2)])
    first, second = list_tasks(db, job.id)
    force_result(db, first, good(0, 2))
    force_result(db, second, good(3, 2))

    summary = summary_of(db, job.id)

    assert raised(summary) == {AnomalyCode.PARTITION_NOT_CONTIGUOUS: 1}
    assert summary.processed_items == 4
    assert not summary.complete


# --- structural cases ---------------------------------------------------------


def test_a_missing_job_is_none(conn: Conn) -> None:
    assert summarize_invoice_job(conn, uuid.uuid4()) is None
    assert conn.info.transaction_status is IDLE


def test_another_kind_of_job_is_refused_before_any_task_is_read(
    conn: Conn, db: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = explicit_job(db, [{"anything": 1}], kind="diagnostic.echo")

    def never_opened(self: object) -> None:
        pytest.fail("the task stream was opened for a job of another kind")

    monkeypatch.setattr(snapshots.JobBatchSnapshot, "tasks", never_opened)

    with pytest.raises(WrongJobKindError, match=r"diagnostic\.echo") as excinfo:
        summarize_invoice_job(conn, job.id)

    assert excinfo.value.job_id == job.id
    assert excinfo.value.kind == "diagnostic.echo"
    assert conn.info.transaction_status is IDLE


def test_a_job_without_tasks_is_a_value_error(conn: Conn, db: Conn) -> None:
    job_id = uuid.uuid4()
    db.execute("INSERT INTO jobs (id, kind) VALUES (%s, %s)", (job_id, JOB_KIND))

    with pytest.raises(ValueError, match="at least one task"):
        summarize_invoice_job(conn, job_id)

    assert conn.info.transaction_status is IDLE


# --- the snapshot: consistency, read-only, cleanup --------------------------


def gated_read(db: Conn, other: Conn) -> tuple[uuid.UUID, InvoiceJobSummary]:
    """Read the job row, commit the last completion from another session, then open the stream."""
    job = submit_items(db, [known(), known()], batch_size=1)
    handler = make_handler()
    first, second = list_tasks(db, job.id)
    run_task(db, first, handler)
    attempt = start_attempt(db, second.id, "worker-2", policy=POLICY)
    outcome = run_handler(handler, context_of(second, attempt), second.payload)
    assert isinstance(outcome, AttemptSucceeded)

    builder = SummaryBuilder()
    with job_batch_snapshot(db, job.id) as snap:
        assert snap is not None
        complete_attempt(
            other,
            attempt.id,
            generation=attempt.attempt_number,
            outcome=outcome,
            retry_policy=RETRY,
        )
        for row in snap.tasks():
            builder.add(row)
        read_job, observed_at = snap.job, snap.observed_at
    return job.id, builder.finish(read_job, observed_at)


def test_a_completion_committed_after_the_job_read_is_not_seen_by_the_stream(
    db: Conn, other: Conn
) -> None:
    job_id, inside = gated_read(db, other)

    assert inside.job_status is JobStatus.RUNNING
    assert inside.task_counts[TaskState.SUCCEEDED] == 1
    assert inside.task_counts[TaskState.RUNNING] == 1
    assert inside.processed_items == 1
    assert inside.unprocessed_items == 1
    assert not inside.complete

    fresh = summary_of(db, job_id)
    assert fresh.job_status is JobStatus.SUCCEEDED
    assert fresh.processed_items == 2
    assert fresh.complete


def test_the_consistency_test_fails_when_the_isolation_level_is_weakened(
    db: Conn, other: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(snapshots, "_SET_TRANSACTION", SET_READ_COMMITTED)

    _, inside = gated_read(db, other)

    # The stream saw the later commit, so the assertions of the test above would fail.
    assert inside.job_status is JobStatus.SUCCEEDED
    assert inside.complete


def write_inside_a_snapshot(db: Conn, job_id: uuid.UUID) -> None:
    with job_batch_snapshot(db, job_id):
        db.execute("UPDATE tasks SET updated_at = now()")


def test_the_snapshot_is_repeatable_read_and_read_only(db: Conn) -> None:
    job = submit_items(db, [known()], batch_size=1)

    with job_batch_snapshot(db, job.id) as snap:
        assert snap is not None
        assert db.execute("SHOW transaction_isolation").fetchone() == ("repeatable read",)
        assert db.execute("SHOW transaction_read_only").fetchone() == ("on",)
        stream = snap.tasks()
        cursors = db.execute("SELECT name FROM pg_cursors").fetchall()
        assert [name.startswith("orqestra_tasks_") for (name,) in cursors] == [True]
        assert [row.task_key for row in stream] == [t.task_key for t in list_tasks(db, job.id)]

    assert db.info.transaction_status is IDLE
    assert db.execute("SELECT count(*) FROM pg_cursors").fetchone() == (0,)


def test_a_write_inside_the_snapshot_fails_and_changes_nothing(db: Conn) -> None:
    job = submit_items(db, [known(), known()], batch_size=1)
    before = snapshot(db)

    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        write_inside_a_snapshot(db, job.id)

    assert snapshot(db) == before
    assert db.info.transaction_status is IDLE


def test_reading_a_summary_changes_no_table(db: Conn) -> None:
    job = submit_items(db, [known(), known(), known()], batch_size=2)
    run_task(db, list_tasks(db, job.id)[0], make_handler())
    before = snapshot(db)

    summary_of(db, job.id)

    assert snapshot(db) == before


def test_the_connection_is_outside_a_transaction_after_a_summary(conn: Conn, db: Conn) -> None:
    job = submit_items(db, [known()], batch_size=1)

    summary_of(conn, job.id)

    assert conn.info.transaction_status is IDLE


def test_the_connection_is_outside_a_transaction_after_an_error(
    conn: Conn, db: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = submit_items(db, [known(), known()], batch_size=1)

    def fail(self: SummaryBuilder, row: object) -> None:
        msg = "boom"
        raise RuntimeError(msg)

    monkeypatch.setattr(SummaryBuilder, "add", fail)

    with pytest.raises(RuntimeError, match="boom"):
        summarize_invoice_job(conn, job.id, fetch_size=1)

    assert conn.info.transaction_status is IDLE
    conn.execute("SELECT 1")  # the connection is usable
    conn.commit()


def abandon_the_stream(conn: Conn, job_id: uuid.UUID) -> None:
    with job_batch_snapshot(conn, job_id, fetch_size=1) as snap:
        assert snap is not None
        stream = snap.tasks()
        next(stream)  # one row of two, then the block ends


def test_the_connection_is_outside_a_transaction_after_an_abandoned_stream(
    conn: Conn, db: Conn
) -> None:
    job = submit_items(db, [known(), known()], batch_size=1)

    abandon_the_stream(conn, job.id)

    assert conn.info.transaction_status is IDLE
    assert conn.execute("SELECT count(*) FROM pg_cursors").fetchone() == (0,)
    conn.commit()


def stream_after_the_block(conn: Conn, job_id: uuid.UUID) -> None:
    with job_batch_snapshot(conn, job_id) as snap:
        assert snap is not None
        ended = snap
    ended.tasks()


def test_the_stream_opens_once_and_only_while_the_snapshot_is_open(conn: Conn, db: Conn) -> None:
    job = submit_items(db, [known()], batch_size=1)

    with job_batch_snapshot(conn, job.id) as snap:
        assert snap is not None
        snap.tasks()
        with pytest.raises(RuntimeError, match="already opened"):
            snap.tasks()
    with pytest.raises(RuntimeError, match="has ended"):
        stream_after_the_block(conn, job.id)

    assert conn.info.transaction_status is IDLE


def test_a_caller_open_transaction_is_refused(conn: Conn, db: Conn) -> None:
    job = submit_items(db, [known()], batch_size=1)

    with conn.transaction():
        with pytest.raises(RuntimeError, match="own transaction"):
            summarize_invoice_job(conn, job.id)
        assert conn.info.transaction_status is TransactionStatus.INTRANS

    assert conn.info.transaction_status is IDLE


@pytest.mark.parametrize("fetch_size", [0, -1, True])
def test_an_invalid_fetch_size_is_refused_before_anything_is_sent(
    conn: Conn, db: Conn, fetch_size: int
) -> None:
    job = submit_items(db, [known()], batch_size=1)

    with pytest.raises(ValueError, match="fetch_size"):
        summarize_invoice_job(conn, job.id, fetch_size=fetch_size)

    assert conn.info.transaction_status is IDLE


def test_the_summary_is_the_same_whatever_the_fetch_size(db: Conn) -> None:
    payloads: list[JsonValue] = [
        payload_of(0, 2),
        payload_of(2, 2),
        payload_of(4, 2),
        "x",
        5,
        payload_of(6, 1),
        payload_of(7, 1),
    ]
    job = explicit_job(db, payloads)
    tasks = list_tasks(db, job.id)
    force_result(db, tasks[0], good(0, 2))
    force_result(db, tasks[1], envelope(2, [entry(2), entry(2)]))
    force_result(db, tasks[3], good(0, 1))  # its payload is not a batch: ignored
    force_result(db, tasks[5], None)

    reference = comparable(summary_of(db, job.id, fetch_size=1000))

    assert reference.anomalies[AnomalyCode.PAYLOAD_INVALID] == 2
    assert reference.anomalies[AnomalyCode.INDEX_DUPLICATE] == 1
    assert reference.anomalies[AnomalyCode.RESULT_INVALID] == 1
    for fetch_size in (1, 2, 1000):
        assert comparable(summary_of(db, job.id, fetch_size=fetch_size)) == reference


# --- scale and memory (outside the default lanes; observations, not thresholds) ---------

_SEED_JOB = "INSERT INTO jobs (id, kind) VALUES (%s, %s)"

_SEED_TASKS = """
INSERT INTO tasks (id, job_id, task_key, payload, max_attempts)
SELECT gen_random_uuid(), %(job)s, 'batch-' || lpad(b::text, 7, '0'),
       jsonb_build_object(
           'offset', b * %(size)s,
           'items', (SELECT jsonb_agg(jsonb_build_object('invoice_id', 'INV-' || i))
                     FROM generate_series(b * %(size)s, b * %(size)s + %(size)s - 1) AS i)),
       3
FROM generate_series(0, %(batches)s - 1) AS b
"""

# Complete queued tasks (all of the job's, or only the given ids) with a realistic
# result per item, as the handler would store it.
_COMPLETE_TASKS = """
UPDATE tasks t
SET state = 'succeeded', finished_at = now(), updated_at = now(),
    result = jsonb_build_object(
        'offset', t.payload -> 'offset',
        'results', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'index', i, 'status', 'verified', 'provider_assisted', false,
                    'findings', '[]'::jsonb, 'issues', '[]'::jsonb,
                    'reason', NULL::text, 'routing', NULL::jsonb
                ) ORDER BY i)
            FROM generate_series(
                (t.payload ->> 'offset')::bigint,
                (t.payload ->> 'offset')::bigint + jsonb_array_length(t.payload -> 'items') - 1
            ) AS i))
WHERE t.job_id = %(job)s AND t.state = 'queued'
  AND (%(ids)s::uuid[] IS NULL OR t.id = ANY(%(ids)s))
"""


def seed(db: Conn, invoices: int, batch_size: int) -> uuid.UUID:
    job_id = uuid.uuid4()
    db.execute(_SEED_JOB, (job_id, JOB_KIND))
    db.execute(
        _SEED_TASKS,
        {"job": job_id, "size": batch_size, "batches": invoices // batch_size},
    )
    return job_id


def complete_tasks(conn: Conn, job_id: uuid.UUID, ids: list[uuid.UUID] | None = None) -> None:
    conn.execute(_COMPLETE_TASKS, {"job": job_id, "ids": ids})


def observe(text: str) -> None:
    """Record an observation on standard output (run with ``-s``)."""
    print(f"OBSERVATION {text}")


def timed(label: str, call: Callable[[], Any]) -> Any:
    started = time.perf_counter()
    value = call()
    observe(f"{label}: wall {time.perf_counter() - started:.3f}s")
    return value


def measure(call: Callable[[], InvoiceJobSummary]) -> tuple[InvoiceJobSummary, dict[str, float]]:
    """Wall time, then a second run under tracemalloc.

    ``ru_maxrss`` is the whole process's high-water mark, in the platform's unit.
    """
    started = time.perf_counter()
    call()
    wall = time.perf_counter() - started
    tracemalloc.start()
    try:
        summary = call()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return summary, {"wall_s": wall, "tracemalloc_peak_bytes": peak, "max_rss_raw": peak_rss}


@pytest.mark.performance
@pytest.mark.parametrize("batch_size", [1, 10, 100, 1000])
def test_observe_time_and_memory_at_fifty_thousand_invoices(db: Conn, batch_size: int) -> None:
    invoices = 50_000
    job_id = seed(db, invoices, batch_size)
    complete_tasks(db, job_id)

    summary, observed = measure(lambda: summary_of(db, job_id))

    record = {"invoices": invoices, "batch_size": batch_size, **observed}
    observe(json.dumps(record))
    assert summary.complete
    assert summary.verified == invoices
    assert summary.expected_items == invoices


@pytest.mark.performance
def test_observe_the_cost_of_the_ordering_and_of_the_fetch_size(db: Conn) -> None:
    job_id = seed(db, 50_000, 10)
    complete_tasks(db, job_id)
    prefix = "SELECT count(*) FROM (SELECT task_key FROM tasks WHERE job_id = %s ORDER BY task_key"

    def count_ordered(collation: str) -> Any:
        return db.execute(f"{prefix}{collation}) AS s", (job_id,)).fetchone()

    timed('order by task_key COLLATE "C"', lambda: count_ordered(' COLLATE "C"'))
    timed("order by task_key (default collation)", lambda: count_ordered(""))
    reference = comparable(summary_of(db, job_id))
    for fetch_size in (10, 100, 500, 5000):
        label = f"summary at fetch size {fetch_size}"

        def summarize_at_size(size: int = fetch_size) -> InvoiceJobSummary:
            return summary_of(db, job_id, fetch_size=size)

        result = timed(label, summarize_at_size)
        assert comparable(result) == reference


@pytest.mark.performance
def test_observe_reads_while_tasks_are_completed_concurrently(db: Conn, schema_url: str) -> None:
    job_id = seed(db, 5_000, 10)
    ids = [row[0] for row in db.execute("SELECT id FROM tasks WHERE job_id = %s", (job_id,))]
    failures: list[BaseException] = []

    def complete_in_chunks() -> None:
        try:
            with connect(schema_url) as writer:
                for start in range(0, len(ids), 10):
                    complete_tasks(writer, job_id, ids[start : start + 10])
                    time.sleep(0.002)
        except Exception as exc:
            failures.append(exc)

    worker = threading.Thread(target=complete_in_chunks)
    worker.start()
    reads: list[InvoiceJobSummary] = []
    while worker.is_alive():
        reads.append(summary_of(db, job_id))
    worker.join()
    reads.append(summary_of(db, job_id))

    processed = [s.processed_items for s in reads]
    observe("items processed by each concurrent read: " + json.dumps(processed))
    assert failures == []
    assert processed == sorted(processed)  # completions only add, and each read is one state
    for summary in reads:
        assert summary.integrity_ok
        assert_partition(summary)
        assert summary.processed_items % 10 == 0  # a task is seen whole or not at all
    assert reads[-1].complete
