"""Attempt lifecycle against real PostgreSQL: start, discover, complete, retry, cancel,
renew leases, recover expired attempts and enforce cancellation grace periods.

Every test ends by checking the cross-table invariants over the whole schema,
and the race tests check them after every round. Concurrency tests use one
connection per thread, as separate worker processes would. Deterministic
tests hold one transaction at a known point with a test-only gate trigger and
observe the competitor waiting in ``pg_locks`` before letting either proceed.
"""

import dataclasses
import random
import secrets
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from typing import Any

import psycopg
import pytest
from psycopg import errors, sql
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow, dict_row

from orqestra.domain.errors import OrqestraError, PermanentError, TransientError
from orqestra.domain.jobs import (
    Attempt,
    AttemptCancelled,
    AttemptFailed,
    AttemptOutcome,
    AttemptSucceeded,
    JobStatus,
    JobSubmission,
    Task,
    TaskSpec,
)
from orqestra.domain.leases import LeasePolicy, RevocationReason
from orqestra.domain.policy import FailureKind
from orqestra.domain.states import AttemptState, TaskState
from orqestra.persistence.jobs import get_job_progress, get_task, list_tasks, submit_job
from orqestra.persistence.lifecycle import (
    AttemptCompletion,
    AttemptFinishedError,
    AttemptRevocation,
    AttemptRevokedError,
    CancellationOutcome,
    CancellationRequestedError,
    LeaseExpiredError,
    NotFoundError,
    StaleAttemptError,
    TaskNotStartableError,
    WorkerMismatchError,
    claim_next_attempt,
    complete_attempt,
    get_attempt,
    list_attempts,
    renew_lease,
    request_cancellation,
    revoke_next_expired,
    start_attempt,
)

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]
Snapshot = dict[str, list[tuple[Any, ...]]]

SUCCESS = AttemptSucceeded({"verified": True})
TRANSIENT = AttemptFailed(FailureKind.TRANSIENT, "test.RateLimited", "429")
PERMANENT = AttemptFailed(FailureKind.PERMANENT, "test.InvalidInvoice", "no total")

# Test values only: Orqestra has no default lease policy (DEC-034). POLICY's
# lease outlasts any test, so its attempts expire only when a test makes them.
POLICY = LeasePolicy(lease=timedelta(minutes=5), execution_timeout=timedelta(hours=1))
# Attempts claimed with these expire one microsecond after their claim
# instant; ``until_expired`` waits for the database clock to pass that.
EXPIRING = LeasePolicy(lease=timedelta(microseconds=1), execution_timeout=timedelta(hours=1))
DEADLINE_EXPIRING = LeasePolicy(
    lease=timedelta(microseconds=1), execution_timeout=timedelta(microseconds=1)
)
# Test value only: there is no default cancellation grace period (DEC-040). It
# is shorter than POLICY's lease, so a request shortens a fresh lease.
GRACE = timedelta(minutes=1)


# --- helpers -----------------------------------------------------------------


def new_tasks(db: Conn, *keys: str, max_attempts: int = 3) -> list[Task]:
    specs = tuple(TaskSpec(key, {"invoice": key}, max_attempts) for key in keys)
    job = submit_job(db, JobSubmission("invoice_verification", specs)).job
    return list(list_tasks(db, job.id))


def new_task(db: Conn, *, max_attempts: int = 3) -> Task:
    (task,) = new_tasks(db, "t", max_attempts=max_attempts)
    return task


def task_now(db: Conn, task_id: uuid.UUID) -> Task:
    task = get_task(db, task_id)
    assert task is not None
    return task


def complete(db: Conn, attempt: Attempt, outcome: AttemptOutcome) -> AttemptCompletion:
    return complete_attempt(db, attempt.id, generation=attempt.attempt_number, outcome=outcome)


def renew(db: Conn, attempt: Attempt, policy: LeasePolicy = POLICY) -> Attempt:
    return renew_lease(
        db,
        attempt.id,
        generation=attempt.attempt_number,
        worker_id=attempt.worker_id,
        policy=policy,
    )


def until_expired(db: Conn, *attempts: Attempt) -> None:
    """Wait until the database clock has passed every one of the attempts' leases."""
    leases = [a.lease_expires_at for a in attempts]
    assert all(lease is not None for lease in leases)
    latest = max(lease for lease in leases if lease is not None)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        row = db.execute("SELECT statement_timestamp() >= %s", (latest,)).fetchone()
        if row is not None and row[0]:
            return
        time.sleep(0.005)
    pytest.fail(f"the database clock never reached {latest}")


def revoke(db: Conn) -> AttemptRevocation:
    """Revoke the next expired attempt, which the test knows exists."""
    revocation = revoke_next_expired(db)
    assert revocation is not None
    return revocation


def snapshot(db: Conn) -> Snapshot:
    return {
        table: db.execute(
            sql.SQL("SELECT * FROM {} ORDER BY id").format(sql.Identifier(table))
        ).fetchall()
        for table in ("jobs", "tasks", "attempts")
    }


# Each query returns (rule, task_id) for every task that breaks the rule. The
# history rules hold for the operations implemented so far, where a task is only
# requeued after a transient failure or a revocation for its lease or deadline.
INVARIANTS = """
SELECT 'running task has exactly one running attempt at its generation', t.id
FROM tasks t WHERE t.state = 'running' AND (
    SELECT count(*) FROM attempts a
    WHERE a.task_id = t.id AND a.state = 'running' AND a.attempt_number = t.attempt_count
) <> 1
UNION ALL
SELECT 'running attempts belong to a running task at its generation', a.task_id
FROM attempts a JOIN tasks t ON t.id = a.task_id
WHERE a.state = 'running' AND (t.state <> 'running' OR a.attempt_number <> t.attempt_count)
UNION ALL
SELECT 'succeeded task has its succeeded attempt at the current generation', t.id
FROM tasks t WHERE t.state = 'succeeded' AND NOT EXISTS (
    SELECT FROM attempts a
    WHERE a.task_id = t.id AND a.state = 'succeeded' AND a.attempt_number = t.attempt_count
)
UNION ALL
SELECT 'only a succeeded task has a succeeded attempt', a.task_id
FROM attempts a JOIN tasks t ON t.id = a.task_id
WHERE a.state = 'succeeded' AND t.state <> 'succeeded'
UNION ALL
SELECT 'attempt numbers are exactly 1..attempt_count', t.id
FROM tasks t WHERE (SELECT count(*) FROM attempts a WHERE a.task_id = t.id) <> t.attempt_count
    OR EXISTS (SELECT FROM attempts a WHERE a.task_id = t.id AND a.attempt_number > t.attempt_count)
UNION ALL
SELECT 'every earlier attempt failed transiently or was revoked', a.task_id
FROM attempts a JOIN tasks t ON t.id = a.task_id
WHERE a.attempt_number < t.attempt_count
    AND NOT (a.state = 'revoked' OR (a.state = 'failed' AND a.failure_kind = 'transient'))
UNION ALL
SELECT 'a queued task''s last attempt failed transiently or lost its lease or deadline', t.id
FROM tasks t JOIN attempts a ON a.task_id = t.id AND a.attempt_number = t.attempt_count
WHERE t.state = 'queued' AND NOT (
    (a.state = 'revoked' AND a.revoked_reason <> 'cancellation_grace_expired')
    OR (a.state = 'failed' AND a.failure_kind = 'transient')
)
UNION ALL
SELECT 'a failed task''s last attempt failed for good or used the last attempt', t.id
FROM tasks t LEFT JOIN attempts a ON a.task_id = t.id AND a.attempt_number = t.attempt_count
WHERE t.state = 'failed' AND NOT (
    (a.state = 'failed' AND (a.failure_kind <> 'transient' OR t.attempt_count = t.max_attempts))
    OR (a.state = 'revoked' AND a.revoked_reason <> 'cancellation_grace_expired'
        AND t.attempt_count = t.max_attempts)
)
UNION ALL
SELECT 'a cancelled task''s last attempt was cancelled, or would have been retried', t.id
FROM tasks t JOIN attempts a ON a.task_id = t.id AND a.attempt_number = t.attempt_count
WHERE t.state = 'cancelled' AND NOT (
    a.state = 'cancelled'
    OR a.revoked_reason = 'cancellation_grace_expired'
    OR ((a.state = 'revoked' OR (a.state = 'failed' AND a.failure_kind = 'transient'))
        AND t.attempt_count < t.max_attempts)
)
UNION ALL
SELECT 'a cancellation grace ran out after a request made while the lease held', a.task_id
FROM attempts a JOIN tasks t ON t.id = a.task_id
WHERE a.revoked_reason = 'cancellation_grace_expired' AND NOT (
    t.state = 'cancelled' AND t.cancel_requested_at < a.lease_expires_at
)
UNION ALL
SELECT 'a revoked attempt''s lease had expired when it was revoked', a.task_id
FROM attempts a WHERE a.state = 'revoked' AND a.finished_at < a.lease_expires_at
UNION ALL
SELECT 'a running attempt holds a lease within its deadline', a.task_id
FROM attempts a WHERE a.state = 'running' AND NOT (
    a.lease_expires_at > a.started_at AND a.lease_expires_at <= a.deadline_at
)
"""


def assert_invariants(db: Conn) -> None:
    assert db.execute(INVARIANTS).fetchall() == []


@pytest.fixture
def checked(db: Conn) -> Iterator[Conn]:
    """The migrated test connection; the invariants must hold when the test ends."""
    yield db
    assert_invariants(db)


@pytest.fixture
def unrelated(checked: Conn) -> Snapshot:
    """Other jobs, including a running and a succeeded task; returns their rows."""
    running, done, _queued = new_tasks(checked, "other-1", "other-2", "other-3")
    start_attempt(checked, running.id, "other-worker", policy=POLICY)
    complete(checked, start_attempt(checked, done.id, "other-worker", policy=POLICY), SUCCESS)
    return snapshot(checked)


def assert_untouched(db: Conn, before: Snapshot) -> None:
    after = snapshot(db)
    for table, rows in before.items():
        assert [row for row in rows if row not in after[table]] == [], table


def connect(url: str) -> Conn:
    conn = psycopg.connect(url, autocommit=True)
    conn.execute("SET statement_timeout = '20s'")  # a hang fails the test instead
    return conn


@pytest.fixture
def worker(schema_url: str) -> Iterator[Callable[[], Conn]]:
    """Open extra connections, each a separate worker session; closed at teardown."""
    opened: list[Conn] = []

    def open_connection() -> Conn:
        conn = connect(schema_url)
        opened.append(conn)
        return conn

    yield open_connection
    for conn in opened:
        conn.close()


def race(url: str, calls: list[Callable[[Conn], Any]]) -> list[Any]:
    """Run every call at the same instant, each on its own connection.

    Returns each call's result, or the ``OrqestraError`` it raised.
    """
    barrier = threading.Barrier(len(calls))

    def run(call: Callable[[Conn], Any]) -> Any:
        with connect(url) as conn:
            barrier.wait()
            try:
                return call(conn)
            except OrqestraError as exc:
                return exc

    with ThreadPoolExecutor(len(calls)) as pool:
        return list(pool.map(run, calls))


@pytest.fixture
def gate(db: Conn) -> Iterator[Callable[[str, str], Callable[[], None]]]:
    """``gate(event, table)`` holds every ``event`` on ``table`` until released.

    A test-only statement trigger takes a shared advisory lock that this
    fixture holds exclusively, so a transaction stops at that statement while
    keeping every lock it already holds. Returns the function that releases it.
    """
    held: list[int] = []

    def close(event: str, table: str) -> Callable[[], None]:
        key = secrets.randbits(62)
        name = sql.Identifier(f"gate_{len(held)}_{event.lower()}_{table}")
        db.execute(
            sql.SQL("""
                CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_advisory_xact_lock_shared({key}); RETURN NULL; END $$;
                CREATE TRIGGER {name} BEFORE {event} ON {table}
                    FOR EACH STATEMENT EXECUTE FUNCTION {name}();
            """).format(
                name=name, key=sql.Literal(key), event=sql.SQL(event), table=sql.Identifier(table)
            )
        )
        db.execute("SELECT pg_advisory_lock(%s)", (key,))
        held.append(key)

        def release() -> None:
            if key in held:
                db.execute("SELECT pg_advisory_unlock(%s)", (key,))
                held.remove(key)

        return release

    yield close
    for key in list(held):
        db.execute("SELECT pg_advisory_unlock(%s)", (key,))


def wait_for_lock(monitor: Conn, waiter: Conn, locktype: str) -> None:
    """Wait until ``waiter``'s backend is blocked waiting for a lock of ``locktype``."""
    pid = waiter.info.backend_pid
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        row = monitor.execute(
            "SELECT EXISTS (SELECT FROM pg_locks WHERE pid = %s AND NOT granted AND locktype = %s)",
            (pid, locktype),
        ).fetchone()
        if row is not None and row[0]:
            return
        time.sleep(0.01)
    pytest.fail(f"backend {pid} never waited for a {locktype} lock")


# --- 1. start and complete ---------------------------------------------------


def test_start_and_complete_successfully(checked: Conn, unrelated: Snapshot) -> None:
    db = checked
    task = new_task(db)

    attempt = start_attempt(db, task.id, "worker-1", policy=POLICY)
    assert (attempt.task_id, attempt.attempt_number, attempt.worker_id) == (task.id, 1, "worker-1")
    assert attempt.state is AttemptState.RUNNING
    assert (attempt.failure_kind, attempt.error_type, attempt.finished_at) == (None, None, None)
    running = task_now(db, task.id)
    assert (running.state, running.attempt_count) == (TaskState.RUNNING, 1)
    assert running.updated_at == attempt.started_at
    assert get_attempt(db, attempt.id) == attempt

    done = complete(db, attempt, AttemptSucceeded({"verified": True, "total": "10.00"}))
    assert done.attempt == get_attempt(db, attempt.id)
    assert done.task == task_now(db, task.id)
    assert done.attempt.state is AttemptState.SUCCEEDED
    assert done.attempt.finished_at is not None
    assert done.task.state is TaskState.SUCCEEDED
    assert done.task.result == {"verified": True, "total": "10.00"}
    assert done.task.finished_at == done.task.updated_at == done.attempt.finished_at
    assert (done.task.attempt_count, done.task.cancel_requested_at) == (1, None)
    assert list_attempts(db, task.id) == (done.attempt,)
    assert_untouched(db, unrelated)


def test_unknown_records_are_reported_as_not_found(checked: Conn) -> None:
    missing = uuid.uuid4()
    with pytest.raises(NotFoundError, match=r"task .* does not exist") as info:
        start_attempt(checked, missing, "w", policy=POLICY)
    assert (info.value.kind, info.value.record_id) == ("task", missing)
    with pytest.raises(NotFoundError, match="attempt"):
        complete_attempt(checked, missing, generation=1, outcome=SUCCESS)
    with pytest.raises(NotFoundError, match="task"):
        request_cancellation(checked, missing, grace=GRACE)
    assert get_attempt(checked, missing) is None
    assert list_attempts(checked, missing) == ()


@pytest.mark.parametrize(
    "operation",
    [
        lambda conn, task: start_attempt(conn, task.id, "w", policy=POLICY),
        lambda conn, task: request_cancellation(conn, task.id, grace=GRACE),
        lambda conn, task: complete_attempt(conn, uuid.uuid4(), generation=1, outcome=SUCCESS),
        lambda conn, task: renew_lease(
            conn, uuid.uuid4(), generation=1, worker_id="w", policy=POLICY
        ),
        lambda conn, task: revoke_next_expired(conn),
    ],
    ids=["start", "cancel", "complete", "renew", "revoke"],
)
def test_writes_refuse_a_caller_transaction(
    checked: Conn, operation: Callable[[Conn, Task], object]
) -> None:
    task = new_task(checked)
    before = snapshot(checked)
    with checked.transaction(), pytest.raises(RuntimeError, match="outside any open transaction"):
        operation(checked, task)
    assert snapshot(checked) == before


def test_operations_work_on_a_connection_with_any_row_factory(
    schema_url: str, checked: Conn
) -> None:
    first, second = new_tasks(checked, "a", "b")
    with psycopg.connect(schema_url, autocommit=True, row_factory=dict_row) as conn:
        attempt = start_attempt(conn, first.id, "w", policy=POLICY)
        assert attempt == get_attempt(checked, attempt.id)
        done = complete_attempt(conn, attempt.id, generation=1, outcome=SUCCESS)
        assert done.task == task_now(checked, first.id)
        assert list_attempts(conn, first.id) == (done.attempt,)
        assert request_cancellation(conn, second.id, grace=GRACE).task == task_now(
            checked, second.id
        )


# --- 2. competing starts -----------------------------------------------------


def test_concurrent_starts_of_one_task_have_exactly_one_winner(
    schema_url: str, checked: Conn
) -> None:
    rounds, starters = 20, 8
    for n in range(rounds):
        task = new_task(checked)
        results = race(
            schema_url,
            [
                partial(start_attempt, task_id=task.id, worker_id=f"w{n}-{i}", policy=POLICY)
                for i in range(starters)
            ],
        )
        winners = [r for r in results if isinstance(r, Attempt)]
        losers = [r for r in results if isinstance(r, TaskNotStartableError)]
        assert len(winners) == 1
        assert len(losers) == starters - 1
        assert all(loser.state is TaskState.RUNNING for loser in losers)
        assert list_attempts(checked, task.id) == (winners[0],)
        assert task_now(checked, task.id).attempt_count == 1
        assert_invariants(checked)


def test_a_waiting_starter_sees_the_task_already_running(
    checked: Conn, gate: Callable[[str, str], Callable[[], None]], worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    release = gate("INSERT", "attempts")
    first_conn, second_conn = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(start_attempt, first_conn, task.id, "first", policy=POLICY)
        wait_for_lock(checked, first_conn, "advisory")  # task claimed, attempt not inserted
        second = pool.submit(start_attempt, second_conn, task.id, "second", policy=POLICY)
        wait_for_lock(checked, second_conn, "transactionid")  # blocked on the task row
        assert task_now(checked, task.id).state is TaskState.QUEUED  # nothing committed yet
        release()
        winner = first.result(timeout=20)
        with pytest.raises(TaskNotStartableError) as info:
            second.result(timeout=20)
    assert info.value.state is TaskState.RUNNING
    assert (winner.worker_id, winner.attempt_number) == ("first", 1)
    assert list_attempts(checked, task.id) == (winner,)


# --- 3. atomic start ---------------------------------------------------------


def test_a_failed_attempt_insert_rolls_back_the_claim(checked: Conn, unrelated: Snapshot) -> None:
    task = new_task(checked)
    with pytest.raises(errors.CheckViolation) as info:
        start_attempt(checked, task.id, "   ", policy=POLICY)
    assert info.value.diag.constraint_name == "attempts_worker_id_not_blank"
    assert task_now(checked, task.id) == task  # queued, generation 0, untouched
    assert list_attempts(checked, task.id) == ()
    # The generation was not consumed: the next start is attempt 1.
    assert start_attempt(checked, task.id, "w", policy=POLICY).attempt_number == 1
    assert_untouched(checked, unrelated)


def test_the_claim_and_attempt_insert_are_one_transaction(checked: Conn) -> None:
    # A test-only trigger fails the attempt insert and reports what the
    # transaction had written by then, proving there was a claim to undo.
    checked.execute("""
        CREATE FUNCTION poison() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'poisoned with task % at generation %',
                (SELECT state FROM tasks WHERE id = NEW.task_id),
                (SELECT attempt_count FROM tasks WHERE id = NEW.task_id);
        END $$;
        CREATE TRIGGER poison BEFORE INSERT ON attempts
            FOR EACH ROW WHEN (NEW.worker_id = 'poison') EXECUTE FUNCTION poison();
    """)
    task = new_task(checked)
    with pytest.raises(errors.RaiseException, match="task running at generation 1"):
        start_attempt(checked, task.id, "poison", policy=POLICY)
    assert task_now(checked, task.id) == task
    assert list_attempts(checked, task.id) == ()


# --- 4. authoritative success and the result contract ------------------------


@pytest.mark.parametrize(
    ("result", "jsonb_type"),
    [
        ({"total": "10.00", "lines": [1, 2]}, "object"),
        ([1, "two"], "array"),
        ("ok", "string"),
        (0, "number"),
        (False, "boolean"),
        (None, "null"),
    ],
    ids=["object", "array", "string", "zero", "false", "json-null"],
)
def test_a_success_stores_its_result_and_json_null_is_a_result(
    checked: Conn, result: Any, jsonb_type: str
) -> None:
    task = new_task(checked)
    done = complete(
        checked, start_attempt(checked, task.id, "w", policy=POLICY), AttemptSucceeded(result)
    )
    assert done.task.state is TaskState.SUCCEEDED
    assert done.task.result == result
    # SQL NULL means "no result" and only non-succeeded tasks have it; JSON
    # null is stored as the jsonb value 'null'.
    row = checked.execute(
        "SELECT result IS NULL, jsonb_typeof(result) FROM tasks WHERE id = %s", (task.id,)
    ).fetchone()
    assert row == (False, jsonb_type)


def test_unfinished_tasks_have_no_result(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    complete(checked, attempt, TRANSIENT)
    row = checked.execute("SELECT result IS NULL FROM tasks WHERE id = %s", (task.id,)).fetchone()
    assert row == (True,)


# --- 5. duplicate completion -------------------------------------------------


@pytest.mark.parametrize(
    "duplicate",
    [SUCCESS, AttemptSucceeded({"verified": False}), PERMANENT, TRANSIENT, AttemptCancelled()],
    ids=["same", "different-result", "permanent", "transient", "cancelled"],
)
def test_a_duplicate_completion_changes_nothing(checked: Conn, duplicate: AttemptOutcome) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    complete(checked, attempt, SUCCESS)
    before = snapshot(checked)
    with pytest.raises(AttemptFinishedError) as info:
        complete(checked, attempt, duplicate)
    assert info.value.state is AttemptState.SUCCEEDED
    assert snapshot(checked) == before
    assert task_now(checked, task.id).result == {"verified": True}


def test_a_duplicate_completion_of_a_requeued_attempt_changes_nothing(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    complete(checked, attempt, TRANSIENT)  # requeued; generation 1 is still current
    before = snapshot(checked)
    with pytest.raises(AttemptFinishedError) as info:
        complete(checked, attempt, SUCCESS)
    assert info.value.state is AttemptState.FAILED
    assert snapshot(checked) == before


def test_concurrent_completions_of_one_attempt_record_exactly_one(
    schema_url: str, checked: Conn
) -> None:
    rounds, completers = 20, 4
    for n in range(rounds):
        task = new_task(checked)
        attempt = start_attempt(checked, task.id, "w", policy=POLICY)
        outcomes = [AttemptSucceeded({"round": n, "by": i}) for i in range(completers)]
        results = race(
            schema_url,
            [partial(complete, attempt=attempt, outcome=outcome) for outcome in outcomes],
        )
        winners = [r for r in results if isinstance(r, AttemptCompletion)]
        assert len(winners) == 1
        assert all(
            isinstance(r, AttemptFinishedError) and r.state is AttemptState.SUCCEEDED
            for r in results
            if r is not winners[0]
        )
        assert task_now(checked, task.id).result == winners[0].task.result
        assert_invariants(checked)


# --- 6-8. retry policy -------------------------------------------------------


def test_a_transient_failure_requeues_and_a_later_attempt_succeeds(
    checked: Conn, unrelated: Snapshot
) -> None:
    task = new_task(checked, max_attempts=3)
    first = start_attempt(checked, task.id, "w1", policy=POLICY)
    failed = complete(checked, first, TRANSIENT)
    assert failed.task.state is TaskState.QUEUED
    assert (failed.task.attempt_count, failed.task.finished_at) == (1, None)
    assert failed.attempt.state is AttemptState.FAILED
    assert failed.attempt.failure_kind is FailureKind.TRANSIENT
    assert (failed.attempt.error_type, failed.attempt.error_message) == ("test.RateLimited", "429")

    second = start_attempt(checked, task.id, "w2", policy=POLICY)
    assert second.attempt_number == 2
    done = complete(checked, second, SUCCESS)
    assert (done.task.state, done.task.attempt_count) == (TaskState.SUCCEEDED, 2)
    history = list_attempts(checked, task.id)
    assert [(a.attempt_number, a.state, a.worker_id) for a in history] == [
        (1, AttemptState.FAILED, "w1"),
        (2, AttemptState.SUCCEEDED, "w2"),
    ]
    assert history[0] == failed.attempt  # history is kept as it was
    assert_untouched(checked, unrelated)


@pytest.mark.parametrize(
    ("failure", "kind"),
    [
        (PERMANENT, FailureKind.PERMANENT),
        (AttemptFailed(FailureKind.UNCLASSIFIED, "builtins.KeyError", "'total'"), None),
        (AttemptFailed.from_exception(PermanentError("bad invoice")), FailureKind.PERMANENT),
        # A handler's infrastructure error is unclassified, never permanent (DEC-009).
        (AttemptFailed.from_exception(ConnectionResetError("peer reset")), None),
        (AttemptFailed.from_exception(psycopg.OperationalError("db down")), None),
    ],
    ids=["permanent", "unclassified", "permanent-exception", "os-error", "database-error"],
)
def test_permanent_and_unclassified_failures_end_the_task(
    checked: Conn, failure: AttemptFailed, kind: FailureKind | None
) -> None:
    task = new_task(checked, max_attempts=5)
    done = complete(checked, start_attempt(checked, task.id, "w", policy=POLICY), failure)
    assert done.task.state is TaskState.FAILED
    assert done.task.finished_at is not None
    assert done.task.result is None
    assert done.attempt.failure_kind is (kind or FailureKind.UNCLASSIFIED)
    assert (done.attempt.error_type, done.attempt.error_message) == (
        failure.error_type,
        failure.error_message,
    )
    with pytest.raises(TaskNotStartableError):
        start_attempt(checked, task.id, "w", policy=POLICY)


def test_a_transient_exception_is_classified_for_retry(checked: Conn) -> None:
    task = new_task(checked)
    failure = AttemptFailed.from_exception(TransientError("timeout"))
    assert failure.error_type == "orqestra.domain.errors.TransientError"
    done = complete(checked, start_attempt(checked, task.id, "w", policy=POLICY), failure)
    assert done.task.state is TaskState.QUEUED


def test_retries_stop_at_max_attempts(checked: Conn) -> None:
    task = new_task(checked, max_attempts=3)
    for number in (1, 2, 3):
        attempt = start_attempt(checked, task.id, f"w{number}", policy=POLICY)
        assert attempt.attempt_number == number
        done = complete(checked, attempt, TRANSIENT)
    assert done.task.state is TaskState.FAILED  # exhausted, although transient
    assert done.task.attempt_count == 3
    with pytest.raises(TaskNotStartableError) as info:
        start_attempt(checked, task.id, "w4", policy=POLICY)
    assert info.value.state is TaskState.FAILED
    assert [a.failure_kind for a in list_attempts(checked, task.id)] == [FailureKind.TRANSIENT] * 3


def test_competing_workers_never_start_more_than_max_attempts(
    schema_url: str, checked: Conn
) -> None:
    max_attempts, workers = 5, 6
    for _ in range(5):
        task = new_task(checked, max_attempts=max_attempts)

        def work(conn: Conn, task_id: uuid.UUID = task.id) -> int:
            started = 0
            while True:
                try:
                    attempt = start_attempt(
                        conn, task_id, f"pid-{conn.info.backend_pid}", policy=POLICY
                    )
                except TaskNotStartableError as exc:
                    if exc.state is TaskState.FAILED:
                        return started
                    continue  # another worker holds it; it will be requeued or fail
                started += 1
                complete_attempt(
                    conn, attempt.id, generation=attempt.attempt_number, outcome=TRANSIENT
                )

        per_worker = race(schema_url, [work] * workers)
        assert sum(per_worker) == max_attempts
        final = task_now(checked, task.id)
        assert (final.state, final.attempt_count) == (TaskState.FAILED, max_attempts)
        numbers = [a.attempt_number for a in list_attempts(checked, task.id)]
        assert numbers == list(range(1, max_attempts + 1))
        assert_invariants(checked)


# --- 9. stale generations ----------------------------------------------------


def test_a_stale_generation_cannot_complete_after_a_newer_one_starts(
    checked: Conn, unrelated: Snapshot
) -> None:
    task = new_task(checked)
    old = start_attempt(checked, task.id, "w1", policy=POLICY)
    complete(checked, old, TRANSIENT)
    current = start_attempt(checked, task.id, "w2", policy=POLICY)
    before = snapshot(checked)

    for outcome in (SUCCESS, PERMANENT, TRANSIENT):
        with pytest.raises(StaleAttemptError) as info:
            complete(checked, old, outcome)
        assert (info.value.generation, info.value.current_generation) == (1, 2)
    assert snapshot(checked) == before

    # The current generation is unaffected and its result is authoritative.
    done = complete(checked, current, AttemptSucceeded({"by": "w2"}))
    with pytest.raises(StaleAttemptError):
        complete(checked, old, AttemptSucceeded({"by": "w1"}))
    assert task_now(checked, task.id).result == {"by": "w2"} == done.task.result
    assert_untouched(checked, unrelated)


def test_a_worker_that_lost_ownership_cannot_overwrite_the_new_result(checked: Conn) -> None:
    task = new_task(checked)
    slow = start_attempt(checked, task.id, "slow-worker", policy=EXPIRING)
    until_expired(checked, slow)
    assert revoke(checked).attempt.id == slow.id  # recovery takes ownership back
    fast = start_attempt(checked, task.id, "fast-worker", policy=POLICY)
    complete(checked, fast, AttemptSucceeded({"by": "fast"}))
    before = snapshot(checked)

    with pytest.raises(StaleAttemptError):
        complete(checked, slow, AttemptSucceeded({"by": "slow"}))
    assert snapshot(checked) == before
    assert task_now(checked, task.id).result == {"by": "fast"}


@pytest.mark.parametrize("presented", [0, 1, 3], ids=["zero", "older", "future"])
def test_a_generation_that_does_not_match_the_attempt_is_rejected(
    checked: Conn, presented: int
) -> None:
    task = new_task(checked)
    complete(checked, start_attempt(checked, task.id, "w1", policy=POLICY), TRANSIENT)
    current = start_attempt(checked, task.id, "w2", policy=POLICY)  # generation 2
    before = snapshot(checked)
    with pytest.raises(StaleAttemptError) as info:
        complete_attempt(checked, current.id, generation=presented, outcome=SUCCESS)
    assert (info.value.generation, info.value.current_generation) == (presented, 2)
    assert snapshot(checked) == before


@pytest.mark.parametrize("presented", [0, 2], ids=["older", "future"])
def test_a_finished_attempt_with_a_mismatched_generation_is_stale(
    checked: Conn, unrelated: Snapshot, presented: int
) -> None:
    # The generation is checked before whether the attempt finished (DEC-029),
    # so a mismatched generation is stale even for an attempt that already ended.
    task = new_task(checked)
    recorded = complete(
        checked, start_attempt(checked, task.id, "w", policy=POLICY), SUCCESS
    )  # generation 1
    before = snapshot(checked)

    with pytest.raises(StaleAttemptError) as info:
        complete_attempt(
            checked,
            recorded.attempt.id,
            generation=presented,
            outcome=AttemptSucceeded({"verified": False}),
        )
    assert (info.value.attempt_id, info.value.generation) == (recorded.attempt.id, presented)
    assert info.value.current_generation == 1
    assert snapshot(checked) == before
    # The recorded outcome stays authoritative.
    assert get_attempt(checked, recorded.attempt.id) == recorded.attempt
    assert task_now(checked, task.id) == recorded.task
    assert recorded.task.result == SUCCESS.result
    # With its own generation, the same duplicate is reported as finished instead.
    with pytest.raises(AttemptFinishedError):
        complete(checked, recorded.attempt, AttemptSucceeded({"verified": False}))
    assert_untouched(checked, unrelated)


def test_completion_refuses_a_corrupted_running_attempt(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    # Corrupt the cross-table invariant behind the operations' back.
    checked.execute(
        "UPDATE tasks SET state = 'failed', finished_at = now() WHERE id = %s", (task.id,)
    )
    before = snapshot(checked)
    with pytest.raises(RuntimeError, match="invariant violated"):
        complete(checked, attempt, SUCCESS)
    assert snapshot(checked) == before
    # Restore the invariant so the fixture's final check describes the operations.
    checked.execute(
        "UPDATE attempts SET state = 'failed', failure_kind = 'permanent', error_type = 'x', "
        "finished_at = now() WHERE id = %s",
        (attempt.id,),
    )


# --- 10. cancellation before start ------------------------------------------


def test_cancelling_a_queued_task_cancels_it_at_once(checked: Conn, unrelated: Snapshot) -> None:
    task = new_task(checked)
    result = request_cancellation(checked, task.id, grace=GRACE)
    assert result.outcome is CancellationOutcome.CANCELLED
    assert result.task == task_now(checked, task.id)
    assert result.task.state is TaskState.CANCELLED
    assert result.task.cancel_requested_at is not None
    assert result.task.finished_at == result.task.cancel_requested_at == result.task.updated_at
    with pytest.raises(TaskNotStartableError) as info:
        start_attempt(checked, task.id, "w", policy=POLICY)
    assert info.value.state is TaskState.CANCELLED
    assert list_attempts(checked, task.id) == ()
    assert_untouched(checked, unrelated)


def test_cancelling_a_requeued_task_keeps_its_attempt_history(checked: Conn) -> None:
    task = new_task(checked)
    failed = complete(
        checked, start_attempt(checked, task.id, "w", policy=POLICY), TRANSIENT
    ).attempt
    result = request_cancellation(checked, task.id, grace=GRACE)
    assert result.outcome is CancellationOutcome.CANCELLED
    assert list_attempts(checked, task.id) == (failed,)


# --- 11. cancellation racing completion ---------------------------------------


# Both commit orders of a completion and a cancellation request, forced
# deterministically: the first transaction is held at its task UPDATE while
# holding the task row lock, and the second is observed waiting for that lock.
# (Unforced races below almost always commit the shorter cancellation first.)
@pytest.mark.parametrize(
    ("first", "outcome", "cancellation", "task_state", "requested"),
    [
        ("cancel", SUCCESS, CancellationOutcome.REQUESTED, TaskState.SUCCEEDED, True),
        ("cancel", TRANSIENT, CancellationOutcome.REQUESTED, TaskState.CANCELLED, True),
        ("complete", SUCCESS, CancellationOutcome.ALREADY_FINISHED, TaskState.SUCCEEDED, False),
        # Requeued by the failure, then cancelled at once by the request.
        ("complete", TRANSIENT, CancellationOutcome.CANCELLED, TaskState.CANCELLED, True),
    ],
    ids=[
        "request-then-success",
        "request-then-transient",
        "success-then-request",
        "transient-then-request",
    ],
)
def test_cancellation_and_completion_in_either_order(
    checked: Conn,
    gate: Callable[[str, str], Callable[[], None]],
    worker: Callable[[], Conn],
    first: str,
    outcome: AttemptOutcome,
    cancellation: CancellationOutcome,
    task_state: TaskState,
    requested: bool,
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    release = gate("UPDATE", "tasks")
    cancel_conn, worker_conn = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        if first == "cancel":
            cancel = pool.submit(request_cancellation, cancel_conn, task.id, grace=GRACE)
            wait_for_lock(checked, cancel_conn, "advisory")  # holds the task row lock
            finish = pool.submit(complete, worker_conn, attempt, outcome)
            wait_for_lock(checked, worker_conn, "transactionid")  # waits for the task row
        else:
            finish = pool.submit(complete, worker_conn, attempt, outcome)
            wait_for_lock(checked, worker_conn, "advisory")  # attempt updated, task row locked
            cancel = pool.submit(request_cancellation, cancel_conn, task.id, grace=GRACE)
            wait_for_lock(checked, cancel_conn, "transactionid")
        release()
        done = finish.result(timeout=20)
        result = cancel.result(timeout=20)

    final = task_now(checked, task.id)
    assert result.outcome is cancellation
    assert final.state is task_state
    assert (final.cancel_requested_at is not None) is requested
    assert final.finished_at is not None
    assert done.attempt.state is outcome.state  # the attempt's own outcome is kept
    if task_state is TaskState.SUCCEEDED:
        assert final.result == SUCCESS.result


def test_cancellation_racing_success_never_loses_the_result(schema_url: str, checked: Conn) -> None:
    for _ in range(30):
        task = new_task(checked)
        attempt = start_attempt(checked, task.id, "w", policy=POLICY)
        finished, cancelled = race(
            schema_url,
            [
                partial(complete, attempt=attempt, outcome=SUCCESS),
                partial(request_cancellation, task_id=task.id, grace=GRACE),
            ],
        )
        final = task_now(checked, task.id)
        assert isinstance(finished, AttemptCompletion)
        assert (final.state, final.result) == (TaskState.SUCCEEDED, SUCCESS.result)
        if cancelled.outcome is CancellationOutcome.REQUESTED:
            assert final.cancel_requested_at is not None
        else:
            assert cancelled.outcome is CancellationOutcome.ALREADY_FINISHED
            assert final.cancel_requested_at is None
        assert_invariants(checked)


def test_cancellation_racing_a_transient_failure_always_cancels(
    schema_url: str, checked: Conn
) -> None:
    for _ in range(30):
        task = new_task(checked)
        attempt = start_attempt(checked, task.id, "w", policy=POLICY)
        finished, cancelled = race(
            schema_url,
            [
                partial(complete, attempt=attempt, outcome=TRANSIENT),
                partial(request_cancellation, task_id=task.id, grace=GRACE),
            ],
        )
        # Request first: the failure cancels instead of requeueing. Failure
        # first: the task is requeued and the request cancels it at once.
        assert isinstance(finished, AttemptCompletion)
        assert cancelled.outcome in (CancellationOutcome.REQUESTED, CancellationOutcome.CANCELLED)
        final = task_now(checked, task.id)
        assert final.state is TaskState.CANCELLED
        assert final.attempt_count == 1
        assert_invariants(checked)


# --- 12. cancellation pending at completion ----------------------------------


@pytest.mark.parametrize(
    ("outcome", "max_attempts", "task_state"),
    [
        (TRANSIENT, 3, TaskState.CANCELLED),  # would requeue: cancels instead
        (TRANSIENT, 1, TaskState.FAILED),  # exhausted: recorded as it happened
        (PERMANENT, 3, TaskState.FAILED),
        (AttemptCancelled(), 3, TaskState.CANCELLED),
        (SUCCESS, 3, TaskState.SUCCEEDED),
    ],
    ids=["transient", "transient-exhausted", "permanent", "acknowledged", "success"],
)
def test_a_pending_request_decides_how_the_attempt_ends(
    checked: Conn, outcome: AttemptOutcome, max_attempts: int, task_state: TaskState
) -> None:
    task = new_task(checked, max_attempts=max_attempts)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    requested = request_cancellation(checked, task.id, grace=GRACE)
    assert requested.outcome is CancellationOutcome.REQUESTED
    assert requested.task.state is TaskState.RUNNING  # cooperative: still running

    done = complete(checked, attempt, outcome)
    assert done.task.state is task_state
    assert done.task.finished_at is not None
    assert done.task.cancel_requested_at == requested.task.cancel_requested_at
    assert done.attempt.state is outcome.state  # the attempt's truth is kept
    with pytest.raises(TaskNotStartableError):
        start_attempt(checked, task.id, "w", policy=POLICY)


def test_repeated_requests_keep_the_first(checked: Conn) -> None:
    task = new_task(checked)
    start_attempt(checked, task.id, "w", policy=POLICY)
    first = request_cancellation(checked, task.id, grace=GRACE)
    before = snapshot(checked)
    again = request_cancellation(checked, task.id, grace=GRACE)
    assert again.outcome is CancellationOutcome.ALREADY_REQUESTED
    assert again.task == first.task
    assert snapshot(checked) == before


def test_an_unrequested_cancellation_is_refused(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    before = snapshot(checked)
    with pytest.raises(ValueError, match="cancellation request"):
        complete(checked, attempt, AttemptCancelled())
    assert snapshot(checked) == before


# --- 13. terminal immutability -----------------------------------------------


def finish_as(db: Conn, how: str) -> tuple[Task, Attempt | None]:
    task = new_task(db, max_attempts=3)
    if how == "cancelled-while-queued":
        request_cancellation(db, task.id, grace=GRACE)
        return task_now(db, task.id), None
    attempt = start_attempt(db, task.id, "w", policy=POLICY)
    if how == "cancelled-while-running":
        request_cancellation(db, task.id, grace=GRACE)
    outcomes: dict[str, AttemptOutcome] = {
        "succeeded": SUCCESS,
        "failed": PERMANENT,
        "cancelled-while-running": AttemptCancelled(),
    }
    complete(db, attempt, outcomes[how])
    return task_now(db, task.id), attempt


@pytest.mark.parametrize(
    "how", ["succeeded", "failed", "cancelled-while-queued", "cancelled-while-running"]
)
def test_finished_tasks_cannot_change(checked: Conn, how: str) -> None:
    task, attempt = finish_as(checked, how)
    assert task.finished_at is not None
    before = snapshot(checked)

    with pytest.raises(TaskNotStartableError) as info:
        start_attempt(checked, task.id, "w", policy=POLICY)
    assert info.value.state is task.state
    assert (
        request_cancellation(checked, task.id, grace=GRACE).outcome
        is CancellationOutcome.ALREADY_FINISHED
    )
    if attempt is not None:
        for outcome in (SUCCESS, PERMANENT, TRANSIENT, AttemptCancelled()):
            with pytest.raises(AttemptFinishedError):
                complete(checked, attempt, outcome)
    assert snapshot(checked) == before


# --- 14. derived job status --------------------------------------------------


def test_job_status_follows_its_tasks(checked: Conn) -> None:
    a, b, c, d = new_tasks(checked, "a", "b", "c", "d")
    job_id = a.job_id

    def status() -> tuple[JobStatus, dict[str, int]]:
        progress = get_job_progress(checked, job_id)
        assert progress is not None
        assert progress.job_id == job_id
        assert set(progress.task_counts) == set(TaskState)
        return progress.status, {s.value: n for s, n in progress.task_counts.items() if n}

    assert status() == (JobStatus.QUEUED, {"queued": 4})
    attempt_a = start_attempt(checked, a.id, "w", policy=POLICY)
    assert status() == (JobStatus.RUNNING, {"queued": 3, "running": 1})
    complete(checked, attempt_a, TRANSIENT)  # requeued: every task is queued again
    assert status() == (JobStatus.QUEUED, {"queued": 4})
    complete(checked, start_attempt(checked, a.id, "w", policy=POLICY), SUCCESS)
    assert status() == (JobStatus.RUNNING, {"queued": 3, "succeeded": 1})
    complete(checked, start_attempt(checked, b.id, "w", policy=POLICY), SUCCESS)
    request_cancellation(checked, c.id, grace=GRACE)
    assert status() == (JobStatus.RUNNING, {"queued": 1, "succeeded": 2, "cancelled": 1})
    complete(checked, start_attempt(checked, d.id, "w", policy=POLICY), PERMANENT)
    assert status() == (JobStatus.FAILED, {"succeeded": 2, "cancelled": 1, "failed": 1})

    assert get_job_progress(checked, uuid.uuid4()) is None


def test_a_job_without_tasks_has_no_status(checked: Conn) -> None:
    # submit_job never creates one; a row inserted directly is reported, not guessed.
    job_id = uuid.uuid4()
    checked.execute("INSERT INTO jobs (id, kind) VALUES (%s, 'empty')", (job_id,))
    with pytest.raises(ValueError, match="at least one task"):
        get_job_progress(checked, job_id)


@pytest.mark.parametrize(
    ("outcomes", "expected"),
    [
        (["success", "success"], JobStatus.SUCCEEDED),
        (["success", "cancel"], JobStatus.CANCELLED),
        (["cancel", "cancel"], JobStatus.CANCELLED),
        (["cancel", "fail"], JobStatus.FAILED),
    ],
)
def test_a_finished_job_status_reflects_its_outcomes(
    checked: Conn, outcomes: list[str], expected: JobStatus
) -> None:
    tasks = new_tasks(checked, *[f"t{i}" for i in range(len(outcomes))])
    for task, how in zip(tasks, outcomes, strict=True):
        if how == "cancel":
            request_cancellation(checked, task.id, grace=GRACE)
        else:
            complete(
                checked,
                start_attempt(checked, task.id, "w", policy=POLICY),
                SUCCESS if how == "success" else PERMANENT,
            )
    progress = get_job_progress(checked, tasks[0].job_id)
    assert progress is not None
    assert progress.status is expected


# --- 15. no partial state after failed transactions ---------------------------


@pytest.mark.parametrize(
    ("outcome", "failure"),
    [
        # Fails at the task update, after the attempt row was already updated.
        (AttemptSucceeded({"total": float("nan")}), errors.InvalidTextRepresentation),
        # Fails at the attempt update.
        (AttemptFailed(FailureKind.PERMANENT, "test.Bad", "nul\u0000"), psycopg.DataError),
    ],
    ids=["invalid-result", "invalid-error-text"],
)
def test_a_failed_completion_leaves_the_attempt_running(
    checked: Conn, unrelated: Snapshot, outcome: AttemptOutcome, failure: type[Exception]
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    before = snapshot(checked)
    with pytest.raises(failure):
        complete(checked, attempt, outcome)
    assert snapshot(checked) == before
    assert checked.info.transaction_status is TransactionStatus.IDLE
    # Not recorded as a handler failure: the attempt can still complete.
    assert complete(checked, attempt, SUCCESS).task.state is TaskState.SUCCEEDED
    assert_untouched(checked, unrelated)


def test_a_connection_lost_mid_completion_records_nothing(
    checked: Conn, gate: Callable[[str, str], Callable[[], None]], worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    release = gate("UPDATE", "tasks")
    conn = worker()
    with ThreadPoolExecutor(1) as pool:
        finishing: Future[AttemptCompletion] = pool.submit(complete, conn, attempt, PERMANENT)
        wait_for_lock(checked, conn, "advisory")  # attempt row updated, task not yet
        checked.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
        with pytest.raises(psycopg.OperationalError):
            finishing.result(timeout=20)
    release()

    # The infrastructure failure was not recorded as a failed attempt.
    assert get_attempt(checked, attempt.id) == attempt
    assert task_now(checked, task.id).state is TaskState.RUNNING
    assert_invariants(checked)
    # The worker can report the real outcome again on a new connection.
    assert complete(checked, attempt, SUCCESS).task.state is TaskState.SUCCEEDED


def test_attempts_are_listed_by_number(checked: Conn) -> None:
    task = new_task(checked, max_attempts=4)
    for _ in range(3):
        complete(checked, start_attempt(checked, task.id, "w", policy=POLICY), TRANSIENT)
    last = start_attempt(checked, task.id, "w", policy=POLICY)
    listed = list_attempts(checked, task.id)
    assert [a.attempt_number for a in listed] == [1, 2, 3, 4]
    assert listed[-1] == last
    other = new_task(checked)
    assert list_attempts(checked, other.id) == ()


# --- 16. discovery: claim_next_attempt (DEC-033) ------------------------------
#
# Claimants that must not wait run with a short lock_timeout, so any wait for a
# row lock fails them with LockNotAvailable instead of passing by luck.

Gate = Callable[[str, str], Callable[[], None]]


@pytest.fixture
def claim_gate(checked: Conn) -> Iterator[Callable[[str], Callable[[], None]]]:
    """``claim_gate(worker_id)`` holds that worker's attempt insert until released.

    Unlike the statement-level ``gate``, only the named worker is held, so other
    claimants run freely while it keeps its task row locked mid-claim.
    """
    held: list[int] = []

    def close(worker_id: str) -> Callable[[], None]:
        key = secrets.randbits(62)
        name = sql.Identifier(f"claim_gate_{len(held)}")
        checked.execute(
            sql.SQL("""
                CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_advisory_xact_lock_shared({key}); RETURN NEW; END $$;
                CREATE TRIGGER {name} BEFORE INSERT ON attempts FOR EACH ROW
                    WHEN (NEW.worker_id = {worker}) EXECUTE FUNCTION {name}();
            """).format(name=name, key=sql.Literal(key), worker=sql.Literal(worker_id))
        )
        checked.execute("SELECT pg_advisory_lock(%s)", (key,))
        held.append(key)

        def release() -> None:
            if key in held:
                checked.execute("SELECT pg_advisory_unlock(%s)", (key,))
                held.remove(key)

        return release

    yield close
    for key in list(held):
        checked.execute("SELECT pg_advisory_unlock(%s)", (key,))


def no_wait(conn: Conn) -> Conn:
    """Make any wait for a lock on ``conn`` fail fast instead of blocking."""
    conn.execute("SET lock_timeout = '200ms'")
    return conn


def claim_all(db: Conn, worker_id: str = "w") -> list[Attempt]:
    claimed = []
    while (attempt := claim_next_attempt(db, worker_id, policy=POLICY)) is not None:
        claimed.append(attempt)
    return claimed


# --- discovery: finding and claiming -----------------------------------------


def test_a_queued_task_is_discovered_and_claimed(checked: Conn) -> None:
    # Discovery is global, so this test owns every queued task in its schema.
    task = new_task(checked)
    attempt = claim_next_attempt(checked, "worker-1", policy=POLICY)

    assert attempt is not None
    assert (attempt.task_id, attempt.attempt_number) == (task.id, 1)
    assert (attempt.worker_id, attempt.state) == ("worker-1", AttemptState.RUNNING)
    claimed = task_now(checked, task.id)
    assert (claimed.state, claimed.attempt_count) == (TaskState.RUNNING, 1)
    assert claimed.updated_at == attempt.started_at
    assert list_attempts(checked, task.id) == (attempt,)
    # The claimed attempt completes through the existing lifecycle.
    assert complete(checked, attempt, SUCCESS).task.state is TaskState.SUCCEEDED


def test_an_empty_queue_returns_none_and_changes_nothing(checked: Conn) -> None:
    before = snapshot(checked)
    assert claim_next_attempt(checked, "w", policy=POLICY) is None
    assert snapshot(checked) == before


def test_running_and_finished_tasks_are_never_claimed(checked: Conn) -> None:
    running, succeeded, failed, cancelled, exhausted = new_tasks(
        checked, "running", "succeeded", "failed", "cancelled", "exhausted", max_attempts=1
    )
    start_attempt(checked, running.id, "w", policy=POLICY)
    complete(checked, start_attempt(checked, succeeded.id, "w", policy=POLICY), SUCCESS)
    complete(checked, start_attempt(checked, failed.id, "w", policy=POLICY), PERMANENT)
    request_cancellation(checked, cancelled.id, grace=GRACE)
    complete(
        checked, start_attempt(checked, exhausted.id, "w", policy=POLICY), TRANSIENT
    )  # 1 of 1 used
    assert task_now(checked, exhausted.id).state is TaskState.FAILED
    before = snapshot(checked)

    assert claim_next_attempt(checked, "w", policy=POLICY) is None
    assert snapshot(checked) == before

    queued = new_task(checked)
    attempt = claim_next_attempt(checked, "w", policy=POLICY)
    assert attempt is not None
    assert attempt.task_id == queued.id


def test_each_task_is_claimed_once_per_generation(checked: Conn) -> None:
    tasks = new_tasks(checked, *[f"t{i}" for i in range(5)])
    claimed = claim_all(checked)
    assert sorted(a.task_id for a in claimed) == sorted(t.id for t in tasks)
    assert {a.attempt_number for a in claimed} == {1}
    for task in tasks:
        assert task_now(checked, task.id).attempt_count == 1
        assert len(list_attempts(checked, task.id)) == 1


def test_claims_prefer_older_tasks(checked: Conn) -> None:
    older = new_tasks(checked, "a", "b", "c")
    newer = new_tasks(checked, "d", "e")  # a later transaction: a later created_at
    claimed = [a.task_id for a in claim_all(checked)]
    expected = [t.id for t in sorted(older, key=lambda t: t.id)]
    expected += [t.id for t in sorted(newer, key=lambda t: t.id)]
    assert claimed == expected  # created_at first, then id; uncontended, so exact


def test_a_requeued_task_keeps_its_place(checked: Conn) -> None:
    # A requeued task keeps its created_at, so without contention it is claimed
    # again before newer tasks: there is no backoff in this slice.
    first = new_task(checked)
    later = new_task(checked)
    attempt = claim_next_attempt(checked, "w", policy=POLICY)
    assert attempt is not None
    assert attempt.task_id == first.id
    complete(checked, attempt, TRANSIENT)  # requeued
    again = claim_next_attempt(checked, "w", policy=POLICY)
    assert again is not None
    assert (again.task_id, again.attempt_number) == (first.id, 2)
    assert task_now(checked, later.id).state is TaskState.QUEUED


# --- discovery: concurrent claimants -----------------------------------------


def test_concurrent_claimants_claim_distinct_tasks(schema_url: str, checked: Conn) -> None:
    rounds, claimants = 20, 8
    for n in range(rounds):
        tasks = new_tasks(checked, *[f"r{n}-{i}" for i in range(claimants)])
        results = race(
            schema_url,
            [
                partial(claim_next_attempt, worker_id=f"w{i}", policy=POLICY)
                for i in range(claimants)
            ],
        )
        claimed = [r for r in results if isinstance(r, Attempt)]
        assert len(claimed) == claimants
        assert sorted(a.task_id for a in claimed) == sorted(t.id for t in tasks)
        assert {a.attempt_number for a in claimed} == {1}
        assert_invariants(checked)


def test_more_claimants_than_tasks_start_exactly_the_available_attempts(
    schema_url: str, checked: Conn
) -> None:
    rounds, claimants, available = 20, 8, 3
    for n in range(rounds):
        tasks = new_tasks(checked, *[f"r{n}-{i}" for i in range(available)])
        results = race(
            schema_url,
            [
                partial(claim_next_attempt, worker_id=f"w{i}", policy=POLICY)
                for i in range(claimants)
            ],
        )
        claimed = [r for r in results if isinstance(r, Attempt)]
        assert len(claimed) == available
        assert results.count(None) == claimants - available
        assert sorted(a.task_id for a in claimed) == sorted(t.id for t in tasks)
        assert_invariants(checked)


# --- discovery: skipping locked rows without waiting -------------------------


def test_a_task_being_claimed_is_skipped_without_waiting(
    checked: Conn, claim_gate: Callable[[str], Callable[[], None]], worker: Callable[[], Conn]
) -> None:
    first_task, second_task = sorted(new_tasks(checked, "a", "b"), key=lambda t: t.id)
    release = claim_gate("holder")
    holder, skipper = worker(), no_wait(worker())
    with ThreadPoolExecutor(1) as pool:
        held = pool.submit(claim_next_attempt, holder, "holder", policy=POLICY)
        wait_for_lock(checked, holder, "advisory")  # first task locked and claimed, not committed
        attempt = claim_next_attempt(
            skipper, "skipper", policy=POLICY
        )  # would raise LockNotAvailable on a wait
        assert attempt is not None
        assert attempt.task_id == second_task.id
        release()
        holder_attempt = held.result(timeout=20)
    assert holder_attempt is not None
    assert holder_attempt.task_id == first_task.id


def test_returns_none_when_every_queued_task_is_locked(
    checked: Conn, claim_gate: Callable[[str], Callable[[], None]], worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    release = claim_gate("holder")
    holder, skipper = worker(), no_wait(worker())
    with ThreadPoolExecutor(1) as pool:
        held = pool.submit(claim_next_attempt, holder, "holder", policy=POLICY)
        wait_for_lock(checked, holder, "advisory")
        assert (
            claim_next_attempt(skipper, "skipper", policy=POLICY) is None
        )  # queued, but locked: no wait
        release()
        holder_attempt = held.result(timeout=20)
    assert holder_attempt is not None
    assert holder_attempt.task_id == task.id
    # Once committed, the task is running and is not claimable either.
    assert claim_next_attempt(skipper, "skipper", policy=POLICY) is None


# --- discovery: cancellation racing discovery --------------------------------


def test_a_task_being_cancelled_is_skipped_and_never_claimed(
    checked: Conn, gate: Gate, worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    release = gate("UPDATE", "tasks")
    canceller, claimant = worker(), no_wait(worker())
    with ThreadPoolExecutor(1) as pool:
        cancelling = pool.submit(request_cancellation, canceller, task.id, grace=GRACE)
        wait_for_lock(checked, canceller, "advisory")  # task row locked by the cancellation
        assert claim_next_attempt(claimant, "w", policy=POLICY) is None
        release()
        assert cancelling.result(timeout=20).outcome is CancellationOutcome.CANCELLED
    assert claim_next_attempt(claimant, "w", policy=POLICY) is None  # cancelled: never claimable
    assert list_attempts(checked, task.id) == ()


def test_a_cancellation_after_a_claim_is_recorded_as_a_request(
    checked: Conn, claim_gate: Callable[[str], Callable[[], None]], worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    release = claim_gate("w")
    claimant, canceller = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        claiming = pool.submit(claim_next_attempt, claimant, "w", policy=POLICY)
        wait_for_lock(checked, claimant, "advisory")  # claimed, not committed
        cancelling = pool.submit(request_cancellation, canceller, task.id, grace=GRACE)
        wait_for_lock(checked, canceller, "transactionid")  # waits for the claim
        release()
        attempt = claiming.result(timeout=20)
        result = cancelling.result(timeout=20)
    assert attempt is not None
    assert result.outcome is CancellationOutcome.REQUESTED
    assert result.task.state is TaskState.RUNNING
    # The claimed attempt can acknowledge the cancellation through the lifecycle.
    done = complete(checked, attempt, AttemptCancelled())
    assert done.task.state is TaskState.CANCELLED


def test_cancellation_racing_discovery_never_yields_an_invalid_claim(
    schema_url: str, checked: Conn
) -> None:
    for _ in range(30):
        task = new_task(checked)
        claimed, cancelled = race(
            schema_url,
            [
                partial(claim_next_attempt, worker_id="w", policy=POLICY),
                partial(request_cancellation, task_id=task.id, grace=GRACE),
            ],
        )
        final = task_now(checked, task.id)
        if claimed is None:  # the cancellation held or ended the task first
            assert cancelled.outcome is CancellationOutcome.CANCELLED
            assert final.state is TaskState.CANCELLED
            assert list_attempts(checked, task.id) == ()
        else:  # the claim committed first; the request waits for the attempt
            assert claimed.task_id == task.id
            assert cancelled.outcome is CancellationOutcome.REQUESTED
            assert (final.state, final.attempt_count) == (TaskState.RUNNING, 1)
        assert_invariants(checked)


# --- discovery: atomic claim -------------------------------------------------


def test_a_failed_insert_rolls_back_a_discovered_claim(checked: Conn) -> None:
    task = new_task(checked)
    before = snapshot(checked)
    with pytest.raises(errors.CheckViolation) as info:
        claim_next_attempt(checked, "   ", policy=POLICY)
    assert info.value.diag.constraint_name == "attempts_worker_id_not_blank"
    assert snapshot(checked) == before  # nothing at all changed: still queued at generation 0
    attempt = claim_next_attempt(checked, "w", policy=POLICY)  # the generation was not consumed
    assert attempt is not None
    assert (attempt.task_id, attempt.attempt_number) == (task.id, 1)


def test_the_discovered_claim_and_attempt_insert_are_one_transaction(checked: Conn) -> None:
    # A test-only trigger fails the attempt insert and reports what the
    # transaction had written by then, proving there was a claim to undo.
    checked.execute("""
        CREATE FUNCTION poison() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'poisoned with task % at generation %',
                (SELECT state FROM tasks WHERE id = NEW.task_id),
                (SELECT attempt_count FROM tasks WHERE id = NEW.task_id);
        END $$;
        CREATE TRIGGER poison BEFORE INSERT ON attempts
            FOR EACH ROW WHEN (NEW.worker_id = 'poison') EXECUTE FUNCTION poison();
    """)
    task = new_task(checked)
    with pytest.raises(errors.RaiseException, match="task running at generation 1"):
        claim_next_attempt(checked, "poison", policy=POLICY)
    assert task_now(checked, task.id) == task
    assert list_attempts(checked, task.id) == ()


# --- discovery: retry budget -------------------------------------------------


def test_discovery_stops_at_the_retry_budget(checked: Conn) -> None:
    task = new_task(checked, max_attempts=3)
    for number in (1, 2, 3):
        attempt = claim_next_attempt(checked, "w", policy=POLICY)
        assert attempt is not None
        assert (attempt.task_id, attempt.attempt_number) == (task.id, number)
        complete(checked, attempt, TRANSIENT)
    assert task_now(checked, task.id).state is TaskState.FAILED  # exhausted
    assert claim_next_attempt(checked, "w", policy=POLICY) is None
    assert len(list_attempts(checked, task.id)) == 3


def test_competing_claimants_never_exceed_any_retry_budget(schema_url: str, checked: Conn) -> None:
    max_attempts, claimants = 3, 6
    tasks = new_tasks(checked, *[f"t{i}" for i in range(10)], max_attempts=max_attempts)
    task_ids = [t.id for t in tasks]

    def work(conn: Conn) -> int:
        started = 0
        while True:
            attempt = claim_next_attempt(conn, f"pid-{conn.info.backend_pid}", policy=POLICY)
            if attempt is None:
                states = conn.execute(
                    "SELECT count(*) FROM tasks "
                    "WHERE id = ANY(%s) AND state IN ('queued', 'running')",
                    (task_ids,),
                ).fetchone()
                if states is not None and states[0] == 0:
                    return started
                continue  # others hold the remaining tasks; they will requeue or fail
            started += 1
            complete_attempt(conn, attempt.id, generation=attempt.attempt_number, outcome=TRANSIENT)

    per_worker = race(schema_url, [work] * claimants)
    assert sum(per_worker) == max_attempts * len(tasks)
    for task in tasks:
        final = task_now(checked, task.id)
        assert (final.state, final.attempt_count) == (TaskState.FAILED, max_attempts)
        assert [a.attempt_number for a in list_attempts(checked, task.id)] == [1, 2, 3]
    assert_invariants(checked)


# --- discovery: start_attempt alongside discovery ----------------------------


def test_start_attempt_and_discovery_share_one_claim(checked: Conn) -> None:
    targeted, other = sorted(new_tasks(checked, "a", "b"), key=lambda t: t.id)
    started = start_attempt(checked, targeted.id, "explicit", policy=POLICY)
    discovered = claim_next_attempt(checked, "discovery", policy=POLICY)
    assert discovered is not None
    assert discovered.task_id == other.id  # the started task is running, not claimable
    with pytest.raises(TaskNotStartableError):
        start_attempt(checked, other.id, "explicit", policy=POLICY)  # discovered first
    for attempt in (started, discovered):
        assert attempt.attempt_number == 1
        assert attempt.state is AttemptState.RUNNING


def test_discovery_refuses_a_caller_transaction(checked: Conn) -> None:
    new_task(checked)
    before = snapshot(checked)
    with checked.transaction(), pytest.raises(RuntimeError, match="outside any open transaction"):
        claim_next_attempt(checked, "w", policy=POLICY)
    assert snapshot(checked) == before


def test_discovery_ignores_tasks_it_cannot_see_yet(
    schema_url: str, checked: Conn, worker: Callable[[], Conn]
) -> None:
    # A submission that has not committed is invisible to discovery.
    submitter = worker()
    with submitter.transaction():
        job_id = uuid.uuid4()
        submitter.execute("INSERT INTO jobs (id, kind) VALUES (%s, 'k')", (job_id,))
        submitter.execute(
            "INSERT INTO tasks (id, job_id, task_key, payload, max_attempts) "
            "VALUES (%s, %s, 't', '{}', 3)",
            (uuid.uuid4(), job_id),
        )
        assert claim_next_attempt(checked, "w", policy=POLICY) is None
    attempt = claim_next_attempt(checked, "w", policy=POLICY)
    assert attempt is not None


# --- 17. leases at claim (DEC-034, DEC-035) -----------------------------------


def claim_by(how: str, db: Conn, task: Task, policy: LeasePolicy) -> Attempt:
    """Claim ``task`` by starting it explicitly or by discovery (the only queued task)."""
    if how == "start":
        return start_attempt(db, task.id, "w", policy=policy)
    attempt = claim_next_attempt(db, "w", policy=policy)
    assert attempt is not None
    assert attempt.task_id == task.id
    return attempt


@pytest.mark.parametrize("how", ["start", "discover"])
@pytest.mark.parametrize(
    "policy",
    [
        POLICY,
        LeasePolicy(lease=timedelta(minutes=20), execution_timeout=timedelta(minutes=20)),
    ],
    ids=["lease-shorter-than-timeout", "lease-equals-timeout"],
)
def test_a_claim_records_its_lease_and_deadline_from_one_instant(
    checked: Conn, how: str, policy: LeasePolicy
) -> None:
    task = new_task(checked)
    attempt = claim_by(how, checked, task, policy)
    claimed = task_now(checked, task.id)
    # One claim instant: the task's update, the attempt's start, lease and deadline.
    assert attempt.started_at == claimed.updated_at
    assert attempt.deadline_at == attempt.started_at + policy.execution_timeout
    assert attempt.lease_expires_at == attempt.started_at + min(
        policy.lease, policy.execution_timeout
    )
    assert attempt.revoked_reason is None
    assert get_attempt(checked, attempt.id) == attempt


# The claim instant is read after the task lock is held, so time spent waiting
# for that lock does not shorten the lease. A holder keeps the claimant waiting,
# reads the clock, then lets go: a lease measured from the claim transaction's
# start (now()) or from the statement that waited would start before that.
@pytest.mark.parametrize("prepared", [False, True], ids=["unprepared", "prepared"])
@pytest.mark.parametrize("how", ["start", "discover"])
def test_a_claim_that_waited_for_the_task_lock_still_gets_its_full_lease(
    checked: Conn, worker: Callable[[], Conn], how: str, prepared: bool
) -> None:
    task = new_task(checked)
    holder, claimant = worker(), worker()
    if prepared:
        claimant.prepare_threshold = 0  # server-side prepared statements from the first call
    holder.execute("BEGIN")
    if how == "start":
        holder.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task.id,))
        lock = "transactionid"
    else:
        holder.execute("LOCK TABLE tasks IN EXCLUSIVE MODE")  # discovery skips rows, not this
        lock = "relation"
    with ThreadPoolExecutor(1) as pool:
        claiming = pool.submit(claim_by, how, claimant, task, POLICY)
        wait_for_lock(checked, claimant, lock)
        row = holder.execute("SELECT clock_timestamp()").fetchone()
        assert row is not None
        released_at: datetime = row[0]
        holder.execute("COMMIT")
        attempt = claiming.result(timeout=20)
    assert attempt.started_at > released_at
    assert attempt.lease_expires_at == attempt.started_at + POLICY.lease
    assert attempt.deadline_at == attempt.started_at + POLICY.execution_timeout
    assert task_now(checked, task.id).updated_at == attempt.started_at


@pytest.mark.parametrize("how", ["start", "discover"])
def test_an_unrepresentable_deadline_rolls_back_the_claim(checked: Conn, how: str) -> None:
    task = new_task(checked)
    # Longer than the years left before datetime's maximum, but a valid duration.
    distant = LeasePolicy(lease=timedelta(days=1), execution_timeout=timedelta(days=365 * 8000))
    before = snapshot(checked)
    with pytest.raises(ValueError, match="not representable"):
        claim_by(how, checked, task, distant)
    assert snapshot(checked) == before  # still queued at generation 0, no attempt
    assert claim_by(how, checked, task, POLICY).attempt_number == 1


# --- 18. heartbeats: renew_lease (DEC-036) ------------------------------------


def test_a_heartbeat_extends_the_lease_but_never_the_deadline(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    renewed = renew(checked, attempt)
    assert renewed.lease_expires_at is not None
    assert attempt.lease_expires_at is not None
    assert (
        attempt.lease_expires_at
        < renewed.lease_expires_at
        < attempt.started_at + timedelta(minutes=6)
    )
    # Only the lease changed; the task is untouched.
    assert renewed == dataclasses.replace(attempt, lease_expires_at=renewed.lease_expires_at)
    assert get_attempt(checked, attempt.id) == renewed
    assert task_now(checked, task.id).updated_at == attempt.started_at


def test_a_heartbeat_is_capped_at_the_deadline(checked: Conn) -> None:
    task = new_task(checked)
    short = LeasePolicy(lease=timedelta(minutes=30), execution_timeout=timedelta(hours=1))
    attempt = start_attempt(checked, task.id, "w", policy=short)
    long = LeasePolicy(lease=timedelta(hours=2), execution_timeout=timedelta(hours=2))
    capped = renew(checked, attempt, long)
    assert capped.lease_expires_at == capped.deadline_at == attempt.deadline_at
    again = renew(checked, capped, long)  # at the cap, but not expired: still renewable
    assert again == capped


def test_a_heartbeat_never_shortens_the_lease(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    brief = LeasePolicy(lease=timedelta(seconds=1), execution_timeout=timedelta(hours=1))
    assert renew(checked, attempt, brief) == attempt


@pytest.mark.parametrize(
    ("policy", "reason"),
    [
        (EXPIRING, RevocationReason.LEASE_EXPIRED),
        (DEADLINE_EXPIRING, RevocationReason.DEADLINE_EXCEEDED),
    ],
    ids=["lease", "deadline"],
)
def test_a_heartbeat_after_expiry_is_refused(
    checked: Conn, policy: LeasePolicy, reason: RevocationReason
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=policy)
    until_expired(checked, attempt)
    before = snapshot(checked)
    with pytest.raises(LeaseExpiredError) as info:
        renew(checked, attempt)
    assert info.value.reason is reason
    assert (info.value.lease_expires_at, info.value.deadline_at) == (
        attempt.lease_expires_at,
        attempt.deadline_at,
    )
    assert snapshot(checked) == before  # not revived, and not revoked either
    assert task_now(checked, task.id).state is TaskState.RUNNING


def test_heartbeat_rejections_change_nothing(checked: Conn) -> None:
    done_task, revoked_task, live_task = new_tasks(checked, "done", "revoked", "live")
    done = start_attempt(checked, done_task.id, "w", policy=POLICY)
    complete(checked, done, SUCCESS)
    lost = start_attempt(checked, revoked_task.id, "w", policy=EXPIRING)
    until_expired(checked, lost)
    revoke(checked)
    live = start_attempt(checked, live_task.id, "w", policy=POLICY)
    before = snapshot(checked)

    with pytest.raises(NotFoundError, match="attempt"):
        renew_lease(checked, uuid.uuid4(), generation=1, worker_id="w", policy=POLICY)
    with pytest.raises(StaleAttemptError) as stale:
        renew_lease(checked, live.id, generation=2, worker_id="w", policy=POLICY)
    assert (stale.value.generation, stale.value.current_generation) == (2, 1)
    with pytest.raises(WorkerMismatchError) as mismatch:
        renew_lease(checked, live.id, generation=1, worker_id="other", policy=POLICY)
    assert (mismatch.value.worker_id, mismatch.value.owner) == ("other", "w")
    with pytest.raises(AttemptFinishedError) as finished:
        renew(checked, done)
    assert not isinstance(finished.value, AttemptRevokedError)
    assert finished.value.state is AttemptState.SUCCEEDED
    with pytest.raises(AttemptRevokedError) as revoked:
        renew(checked, lost)
    assert revoked.value.reason is RevocationReason.LEASE_EXPIRED
    assert snapshot(checked) == before


def test_a_heartbeat_does_not_lock_the_task(checked: Conn, worker: Callable[[], Conn]) -> None:
    # Locking only the attempt is what keeps heartbeats out of lock cycles with
    # the task-first operations (DEC-036), including through the foreign key.
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    holder, beater, completer = worker(), no_wait(worker()), no_wait(worker())
    with holder.transaction():
        holder.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task.id,))
        renewed = renew(beater, attempt)  # would raise LockNotAvailable on a wait
        with pytest.raises(errors.LockNotAvailable):
            complete(completer, attempt, SUCCESS)  # control: task-first operations do wait
    assert get_attempt(checked, attempt.id) == renewed


# --- 19. recovery: revoke_next_expired (DEC-037, DEC-038) ----------------------


def test_recovery_without_expired_attempts_changes_nothing(checked: Conn) -> None:
    assert revoke_next_expired(checked) is None  # empty
    task = new_task(checked)
    start_attempt(checked, task.id, "w", policy=POLICY)  # running, lease current
    new_task(checked)  # queued
    before = snapshot(checked)
    assert revoke_next_expired(checked) is None
    assert snapshot(checked) == before


def test_an_expired_lease_is_revoked_and_the_task_requeued(
    checked: Conn, unrelated: Snapshot
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    until_expired(checked, attempt)
    revocation = revoke(checked)

    revoked, requeued = revocation.attempt, revocation.task
    assert revoked == get_attempt(checked, attempt.id)
    assert requeued == task_now(checked, task.id)
    assert (revoked.state, revoked.revoked_reason) == (
        AttemptState.REVOKED,
        RevocationReason.LEASE_EXPIRED,
    )
    # Truthful: not a handler failure, and the lease facts it was decided on are kept.
    assert (revoked.failure_kind, revoked.error_type, revoked.error_message) == (None, None, None)
    assert (revoked.lease_expires_at, revoked.deadline_at) == (
        attempt.lease_expires_at,
        attempt.deadline_at,
    )
    assert revoked.finished_at is not None
    assert attempt.lease_expires_at is not None
    assert revoked.finished_at >= attempt.lease_expires_at
    assert (requeued.state, requeued.attempt_count, requeued.finished_at) == (
        TaskState.QUEUED,
        1,
        None,
    )
    assert requeued.updated_at == revoked.finished_at
    assert revoke_next_expired(checked) is None  # revoked exactly once
    again = start_attempt(checked, task.id, "w2", policy=POLICY)
    assert again.attempt_number == 2  # the revoked attempt used one of the budget
    assert_untouched(checked, unrelated)


def test_an_exceeded_deadline_is_revoked_as_such(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=DEADLINE_EXPIRING)
    assert attempt.lease_expires_at == attempt.deadline_at
    until_expired(checked, attempt)
    assert revoke(checked).attempt.revoked_reason is RevocationReason.DEADLINE_EXCEEDED


def test_a_lease_kept_alive_until_its_deadline_is_revoked_for_the_deadline(
    checked: Conn,
) -> None:
    # The stuck-but-alive case: heartbeats keep succeeding, but they cannot move
    # the lease past the deadline, so recovery still takes the task back.
    task = new_task(checked)
    policy = LeasePolicy(lease=timedelta(seconds=1), execution_timeout=timedelta(seconds=2))
    attempt = start_attempt(checked, task.id, "w", policy=policy)
    capped = renew(
        checked,
        attempt,
        LeasePolicy(lease=timedelta(hours=1), execution_timeout=timedelta(hours=1)),
    )
    assert capped.lease_expires_at == capped.deadline_at
    until_expired(checked, capped)
    with pytest.raises(LeaseExpiredError) as info:
        renew(checked, capped)
    assert info.value.reason is RevocationReason.DEADLINE_EXCEEDED
    assert revoke(checked).attempt.revoked_reason is RevocationReason.DEADLINE_EXCEEDED


# (max_attempts, attempts before the revoked one, cancellation pending) -> task state
@pytest.mark.parametrize(
    ("max_attempts", "earlier", "cancel", "expected"),
    [
        (3, 0, False, TaskState.QUEUED),
        (3, 0, True, TaskState.CANCELLED),
        (1, 0, False, TaskState.FAILED),
        (1, 0, True, TaskState.FAILED),  # exhausted: recorded as it happened (DEC-038)
        (2, 1, False, TaskState.FAILED),
        (2, 1, True, TaskState.FAILED),
    ],
    ids=[
        "left",
        "left-cancel-pending",
        "single-attempt",
        "single-attempt-cancel-pending",
        "last-attempt",
        "last-attempt-cancel-pending",
    ],
)
def test_a_revocation_moves_the_task_by_the_retry_and_cancellation_policy(
    checked: Conn, max_attempts: int, earlier: int, cancel: bool, expected: TaskState
) -> None:
    task = new_task(checked, max_attempts=max_attempts)
    for _ in range(earlier):
        complete(checked, start_attempt(checked, task.id, "w", policy=POLICY), TRANSIENT)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    if cancel:
        requested = request_cancellation(checked, task.id, grace=GRACE)
        assert requested.outcome is CancellationOutcome.REQUESTED
    until_expired(checked, attempt)
    revocation = revoke(checked)

    assert revocation.attempt.state is AttemptState.REVOKED
    final = revocation.task
    assert final.state is expected
    assert final.attempt_count == earlier + 1
    assert (final.cancel_requested_at is not None) is cancel
    assert (final.finished_at is not None) is (expected is not TaskState.QUEUED)
    progress = get_job_progress(checked, task.job_id)
    assert progress is not None
    if expected is TaskState.QUEUED:
        assert claim_next_attempt(checked, "w", policy=POLICY) is not None
    else:
        with pytest.raises(TaskNotStartableError):
            start_attempt(checked, task.id, "w", policy=POLICY)
        assert progress.status.value == expected.value  # the job's only task decides it


def test_a_revoked_worker_is_fenced_off_before_and_after_a_new_claim(checked: Conn) -> None:
    task = new_task(checked)
    old = start_attempt(checked, task.id, "old", policy=EXPIRING)
    until_expired(checked, old)
    revoke(checked)
    before = snapshot(checked)
    # Requeued, so the old generation is still the task's latest: it is told
    # it was revoked, not that its result was recorded.
    with pytest.raises(AttemptRevokedError) as revoked:
        complete(checked, old, SUCCESS)
    assert revoked.value.reason is RevocationReason.LEASE_EXPIRED
    with pytest.raises(AttemptRevokedError):
        renew(checked, old)
    assert snapshot(checked) == before

    new = start_attempt(checked, task.id, "new", policy=POLICY)
    with pytest.raises(StaleAttemptError):
        complete(checked, old, SUCCESS)
    with pytest.raises(AttemptRevokedError):
        renew(checked, old)
    done = complete(checked, new, AttemptSucceeded({"by": "new"}))
    assert done.task.result == {"by": "new"}


def test_a_completion_before_revocation_still_counts(checked: Conn) -> None:
    # Expiry makes an attempt revocable; only a committed revocation ends ownership.
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    until_expired(checked, attempt)
    with pytest.raises(LeaseExpiredError):
        renew(checked, attempt)  # the worker can no longer extend its lease
    done = complete(checked, attempt, SUCCESS)  # but its result is still accepted
    assert (done.task.state, done.task.result) == (TaskState.SUCCEEDED, SUCCESS.result)
    assert revoke_next_expired(checked) is None


def test_concurrent_recoverers_revoke_each_expired_attempt_exactly_once(
    schema_url: str, checked: Conn
) -> None:
    rounds, expired, recoverers = 10, 4, 6
    for n in range(rounds):
        tasks = new_tasks(checked, *[f"r{n}-{i}" for i in range(expired)])
        attempts = [start_attempt(checked, t.id, "w", policy=EXPIRING) for t in tasks]
        until_expired(checked, *attempts)
        results = race(schema_url, [revoke_next_expired] * recoverers)
        revoked = [r for r in results if isinstance(r, AttemptRevocation)]
        assert results.count(None) == recoverers - expired
        assert sorted(r.attempt.id for r in revoked) == sorted(a.id for a in attempts)
        assert revoke_next_expired(checked) is None
        assert_invariants(checked)


def test_recovery_skips_a_task_another_transaction_holds(
    checked: Conn, worker: Callable[[], Conn]
) -> None:
    held_task, free_task = new_tasks(checked, "held", "free")
    held = start_attempt(checked, held_task.id, "w", policy=EXPIRING)
    free = start_attempt(checked, free_task.id, "w", policy=EXPIRING)
    until_expired(checked, held, free)
    holder, recoverer = worker(), no_wait(worker())
    with holder.transaction():
        holder.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (held_task.id,))
        assert revoke(recoverer).attempt.id == free.id  # no wait for the held task
        assert revoke_next_expired(recoverer) is None  # only the held one is left
    assert revoke(recoverer).attempt.id == held.id


def test_a_failed_revocation_rolls_back_both_records(checked: Conn) -> None:
    # A test-only trigger fails the task update, after the attempt row was
    # already updated in the same transaction.
    checked.execute("""
        CREATE FUNCTION poison() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'poisoned requeue'; END $$;
        CREATE TRIGGER poison BEFORE UPDATE ON tasks
            FOR EACH ROW WHEN (NEW.state = 'queued') EXECUTE FUNCTION poison();
    """)
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    until_expired(checked, attempt)
    before = snapshot(checked)
    with pytest.raises(errors.RaiseException, match="poisoned requeue"):
        revoke_next_expired(checked)
    assert snapshot(checked) == before
    assert checked.info.transaction_status is TransactionStatus.IDLE
    checked.execute("DROP TRIGGER poison ON tasks")
    assert revoke(checked).attempt.id == attempt.id  # recovery can simply try again


def test_a_connection_lost_mid_recovery_records_nothing(
    checked: Conn, gate: Gate, worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    until_expired(checked, attempt)
    before = snapshot(checked)
    release = gate("UPDATE", "tasks")
    conn = worker()
    with ThreadPoolExecutor(1) as pool:
        recovering = pool.submit(revoke_next_expired, conn)
        wait_for_lock(checked, conn, "advisory")  # attempt revoked in its transaction, task not yet
        checked.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
        with pytest.raises(psycopg.OperationalError):
            recovering.result(timeout=20)
    release()
    assert snapshot(checked) == before
    assert revoke(checked).attempt.id == attempt.id


# --- 20. races between heartbeats, completion and recovery -----------------------
#
# Each race is forced in both orders: the first transaction is held at a gate
# while holding its row locks, and the second is observed waiting for them.


def test_recovery_first_then_a_heartbeat_cannot_revive_the_attempt(
    checked: Conn, gate: Gate, worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    until_expired(checked, attempt)
    release = gate("UPDATE", "attempts")
    recoverer, beater = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        recovering = pool.submit(revoke_next_expired, recoverer)
        wait_for_lock(checked, recoverer, "advisory")  # task and attempt locked, decided
        beating = pool.submit(renew, beater, attempt)
        wait_for_lock(checked, beater, "transactionid")  # waits for the attempt row
        release()
        revocation = recovering.result(timeout=20)
        with pytest.raises(AttemptRevokedError):
            beating.result(timeout=20)
    assert revocation is not None
    assert get_attempt(checked, attempt.id) == revocation.attempt  # no resurrection
    assert revocation.attempt.lease_expires_at == attempt.lease_expires_at


def test_a_heartbeat_first_then_recovery_finds_the_lease_renewed(
    checked: Conn, gate: Gate, worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    # The heartbeat must pass its expiry check before this lease runs out; it
    # then holds the attempt until the lease has expired by the clock, so
    # recovery discovers it as expired and must re-check after the heartbeat.
    short = LeasePolicy(lease=timedelta(seconds=2), execution_timeout=timedelta(hours=1))
    attempt = start_attempt(checked, task.id, "w", policy=short)
    release = gate("UPDATE", "attempts")
    beater, recoverer = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        beating = pool.submit(renew, beater, attempt)
        wait_for_lock(checked, beater, "advisory")  # attempt locked, renewal decided
        until_expired(checked, attempt)
        recovering = pool.submit(revoke_next_expired, recoverer)
        wait_for_lock(checked, recoverer, "transactionid")  # task locked, waits for the attempt
        release()
        renewed = beating.result(timeout=20)
        assert recovering.result(timeout=20) is None
    assert renewed.lease_expires_at is not None
    assert attempt.lease_expires_at is not None
    assert renewed.lease_expires_at > attempt.lease_expires_at
    assert get_attempt(checked, attempt.id) == renewed
    assert task_now(checked, task.id).state is TaskState.RUNNING


@pytest.mark.parametrize("first", ["recover", "complete"])
def test_completion_and_recovery_in_either_order(
    checked: Conn, gate: Gate, worker: Callable[[], Conn], first: str
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    until_expired(checked, attempt)
    release = gate("UPDATE", "tasks")
    recoverer, completer = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        if first == "recover":
            recovering = pool.submit(revoke_next_expired, recoverer)
            wait_for_lock(checked, recoverer, "advisory")  # attempt revoked, task not yet
            completing = pool.submit(complete, completer, attempt, SUCCESS)
            wait_for_lock(checked, completer, "transactionid")
            release()
            assert recovering.result(timeout=20) is not None
            with pytest.raises(AttemptRevokedError):
                completing.result(timeout=20)
            assert task_now(checked, task.id).state is TaskState.QUEUED
        else:
            completing = pool.submit(complete, completer, attempt, SUCCESS)
            wait_for_lock(checked, completer, "advisory")  # attempt finished, task not yet
            assert revoke_next_expired(no_wait(recoverer)) is None  # skips the locked task
            release()
            assert completing.result(timeout=20).task.state is TaskState.SUCCEEDED
            assert revoke_next_expired(recoverer) is None
    assert_invariants(checked)


def jittered(
    call: Callable[[Conn], Any], seed: int, *, earliest: float = 0.0, latest: float = 0.003
) -> Callable[[Conn], Any]:
    """``call``, started a seeded ``earliest``..``latest`` seconds late.

    It varies unforced interleavings and synchronises nothing: correctness
    never depends on the delays, only how often each interleaving occurs.
    """
    delay = random.Random(seed).uniform(earliest, latest)

    def run(conn: Conn) -> Any:
        time.sleep(delay)
        return call(conn)

    return run


def test_heartbeats_completion_recovery_and_cancellation_never_deadlock(
    schema_url: str, checked: Conn
) -> None:
    # Unforced races on one task, rotating through sets of participants with
    # seeded start jitter so that both completion and recovery win some rounds.
    # Whatever the interleaving, nothing deadlocks (a DeadlockDetected would
    # escape race()), exactly one of completion and recovery ends the attempt,
    # and the invariants hold.
    winners: set[str] = set()
    for n in range(40):
        task = new_task(checked, max_attempts=2)
        expired = n % 4 != 0
        attempt = start_attempt(checked, task.id, "w", policy=EXPIRING if expired else POLICY)
        if expired:
            until_expired(checked, attempt)
        participants: dict[str, Callable[[Conn], Any]] = {
            "renew": partial(renew, attempt=attempt),
            "complete": partial(complete, attempt=attempt, outcome=SUCCESS if n % 3 else TRANSIENT),
            "revoke": revoke_next_expired,
            "cancel": partial(request_cancellation, task_id=task.id, grace=GRACE),
        }
        if n % 4 == 2:
            del participants["cancel"]
        elif n % 4 == 3:
            del participants["renew"]
        results = dict(
            zip(
                participants,
                race(
                    schema_url,
                    [
                        jittered(call, seed=n * 10 + i)
                        for i, call in enumerate(participants.values())
                    ],
                ),
                strict=True,
            )
        )
        completed, revoked = results["complete"], results["revoke"]
        assert isinstance(completed, AttemptCompletion) != isinstance(revoked, AttemptRevocation)
        if isinstance(revoked, AttemptRevocation):
            winners.add("revoke")
            assert isinstance(completed, AttemptRevokedError)
            assert not isinstance(results.get("renew"), Attempt)
        else:
            winners.add("complete")
        assert_invariants(checked)
    assert winners == {"complete", "revoke"}  # both orders were actually exercised


# --- 21. cancellation grace periods (DEC-040) ----------------------------------

# A grace period that has run out by the time any later statement runs; see
# ``until_expired``.
BRIEF_GRACE = timedelta(microseconds=1)


def request(db: Conn, task: Task, grace: timedelta = GRACE) -> CancellationOutcome:
    return request_cancellation(db, task.id, grace=grace).outcome


def attempt_now(db: Conn, attempt_id: uuid.UUID) -> Attempt:
    attempt = get_attempt(db, attempt_id)
    assert attempt is not None
    return attempt


def test_a_request_shortens_a_long_lease_and_changes_nothing_else(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)  # five-minute lease
    result = request_cancellation(checked, task.id, grace=GRACE)

    assert result.outcome is CancellationOutcome.REQUESTED
    requested_at = result.task.cancel_requested_at
    assert requested_at is not None
    assert result.task == task_now(checked, task.id)
    assert (result.task.state, result.task.attempt_count) == (TaskState.RUNNING, 1)
    assert result.task.updated_at == requested_at
    after = attempt_now(checked, attempt.id)
    assert after.lease_expires_at == requested_at + GRACE  # one grace period, not five minutes
    # Only the lease changed: same generation, worker, deadline and state.
    assert after == dataclasses.replace(attempt, lease_expires_at=after.lease_expires_at)


def test_a_request_lengthens_a_short_lease_that_still_holds(checked: Conn) -> None:
    task = new_task(checked)
    short = LeasePolicy(lease=timedelta(seconds=10), execution_timeout=timedelta(hours=2))
    attempt = start_attempt(checked, task.id, "w", policy=short)
    result = request_cancellation(checked, task.id, grace=timedelta(hours=1))
    requested_at = result.task.cancel_requested_at
    assert requested_at is not None
    after = attempt_now(checked, attempt.id)
    assert attempt.lease_expires_at is not None
    assert after.lease_expires_at == requested_at + timedelta(hours=1)
    assert after.lease_expires_at > attempt.lease_expires_at  # the worker gets its full grace


def test_a_request_never_extends_past_the_deadline(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)  # one-hour deadline
    request_cancellation(checked, task.id, grace=timedelta(days=1))
    after = attempt_now(checked, attempt.id)
    assert after.lease_expires_at == after.deadline_at == attempt.deadline_at


def test_a_request_never_revives_an_expired_lease(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    until_expired(checked, attempt)
    assert request(checked, task, timedelta(hours=1)) is CancellationOutcome.REQUESTED
    assert attempt_now(checked, attempt.id).lease_expires_at == attempt.lease_expires_at
    with pytest.raises(LeaseExpiredError) as info:
        renew(checked, attempt)
    assert info.value.reason is RevocationReason.LEASE_EXPIRED  # not turned into a grace expiry


def test_a_repeated_request_cannot_extend_the_grace(checked: Conn) -> None:
    task = new_task(checked)
    start_attempt(checked, task.id, "w", policy=POLICY)
    request(checked, task)
    before = snapshot(checked)
    assert request(checked, task, timedelta(hours=1)) is CancellationOutcome.ALREADY_REQUESTED
    assert snapshot(checked) == before


@pytest.mark.parametrize(
    ("grace", "error"),
    [
        (timedelta(0), ValueError),
        (timedelta(seconds=-1), ValueError),
        (timedelta.max, ValueError),
        (30, TypeError),
        (None, TypeError),
    ],
    ids=["zero", "negative", "unrepresentable", "int", "none"],
)
@pytest.mark.parametrize("state", ["queued", "running", "finished"])
def test_an_invalid_grace_is_refused_before_anything_is_written(
    checked: Conn, grace: Any, error: type[Exception], state: str
) -> None:
    task = new_task(checked)
    if state != "queued":
        attempt = start_attempt(checked, task.id, "w", policy=POLICY)
        if state == "finished":
            complete(checked, attempt, SUCCESS)
    before = snapshot(checked)
    with pytest.raises(error, match="grace"):
        request_cancellation(checked, task.id, grace=grace)
    assert snapshot(checked) == before


@pytest.mark.parametrize("state", ["queued", "running"])
def test_a_request_that_waited_for_the_task_lock_is_timed_after_it(
    checked: Conn, worker: Callable[[], Conn], state: str
) -> None:
    task = new_task(checked)
    if state == "running":
        start_attempt(checked, task.id, "w", policy=POLICY)
    holder, requester = worker(), worker()
    holder.execute("BEGIN")
    holder.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task.id,))
    with ThreadPoolExecutor(1) as pool:
        requesting = pool.submit(request_cancellation, requester, task.id, grace=GRACE)
        wait_for_lock(checked, requester, "transactionid")
        row = holder.execute("SELECT clock_timestamp()").fetchone()
        assert row is not None
        released_at: datetime = row[0]
        holder.execute("COMMIT")
        result = requesting.result(timeout=20)
    requested_at = result.task.cancel_requested_at
    assert requested_at is not None
    assert requested_at > released_at
    assert result.task.updated_at == requested_at
    if state == "queued":
        assert result.task.finished_at == requested_at
    else:
        attempt = list_attempts(checked, task.id)[0]
        assert attempt.lease_expires_at == requested_at + GRACE


def test_a_heartbeat_after_a_request_is_refused(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    result = request_cancellation(checked, task.id, grace=GRACE)
    before = snapshot(checked)
    with pytest.raises(CancellationRequestedError) as info:
        renew(checked, attempt)
    assert info.value.attempt_id == attempt.id
    assert info.value.cancel_requested_at == result.task.cancel_requested_at
    assert info.value.lease_expires_at == attempt_now(checked, attempt.id).lease_expires_at
    assert snapshot(checked) == before
    # Older protections come first: a wrong generation or worker is still that.
    with pytest.raises(StaleAttemptError):
        renew_lease(checked, attempt.id, generation=2, worker_id="w", policy=POLICY)
    with pytest.raises(WorkerMismatchError):
        renew_lease(checked, attempt.id, generation=1, worker_id="other", policy=POLICY)


def test_an_acknowledged_cancellation_ends_the_attempt_as_cancelled(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    request(checked, task)
    done = complete(checked, attempt, AttemptCancelled())
    assert (done.attempt.state, done.task.state) == (AttemptState.CANCELLED, TaskState.CANCELLED)
    assert revoke_next_expired(checked) is None


@pytest.mark.parametrize("max_attempts", [1, 3])
def test_an_ignored_cancellation_is_revoked_when_its_grace_ends(
    checked: Conn, max_attempts: int
) -> None:
    # The reviewed case: a lease as long as the execution timeout. The grace
    # period, not the deadline, bounds how long the request is ignored, and
    # the task is cancelled whatever budget is left.
    task = new_task(checked, max_attempts=max_attempts)
    whole = LeasePolicy(lease=timedelta(hours=1), execution_timeout=timedelta(hours=1))
    attempt = start_attempt(checked, task.id, "w", policy=whole)
    assert attempt.lease_expires_at == attempt.deadline_at
    request(checked, task, BRIEF_GRACE)
    shortened = attempt_now(checked, attempt.id)
    until_expired(checked, shortened)
    with pytest.raises(LeaseExpiredError) as info:
        renew(checked, attempt)
    assert info.value.reason is RevocationReason.CANCELLATION_GRACE_EXPIRED

    revocation = revoke(checked)
    assert revocation.attempt.revoked_reason is RevocationReason.CANCELLATION_GRACE_EXPIRED
    assert (revocation.attempt.failure_kind, revocation.attempt.error_type) == (None, None)
    final = revocation.task
    assert (final.state, final.attempt_count) == (TaskState.CANCELLED, 1)
    assert final.finished_at == revocation.attempt.finished_at
    assert claim_next_attempt(checked, "w", policy=POLICY) is None
    progress = get_job_progress(checked, task.job_id)
    assert progress is not None
    assert progress.status is JobStatus.CANCELLED


def test_a_deadline_reached_during_the_grace_still_counts_as_the_deadline(
    checked: Conn,
) -> None:
    task = new_task(checked, max_attempts=1)
    policy = LeasePolicy(lease=timedelta(seconds=1), execution_timeout=timedelta(seconds=2))
    attempt = start_attempt(checked, task.id, "w", policy=policy)
    request(checked, task, timedelta(hours=1))  # the grace would outlast the deadline
    capped = attempt_now(checked, attempt.id)
    assert capped.lease_expires_at == capped.deadline_at
    until_expired(checked, capped)
    revocation = revoke(checked)
    assert revocation.attempt.revoked_reason is RevocationReason.DEADLINE_EXCEEDED
    assert revocation.task.state is TaskState.FAILED  # exhausted: DEC-038, not cancelled


@pytest.mark.parametrize(
    ("max_attempts", "expected"), [(1, TaskState.FAILED), (3, TaskState.CANCELLED)]
)
def test_a_request_after_the_lease_expired_is_not_a_grace_expiry(
    checked: Conn, max_attempts: int, expected: TaskState
) -> None:
    task = new_task(checked, max_attempts=max_attempts)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    until_expired(checked, attempt)
    request(checked, task)  # the worker was lost before anyone asked to cancel
    revocation = revoke(checked)
    assert revocation.attempt.revoked_reason is RevocationReason.LEASE_EXPIRED
    assert revocation.task.state is expected


def test_a_completion_within_the_grace_counts_and_after_revocation_does_not(
    checked: Conn,
) -> None:
    first, second = new_tasks(checked, "within", "after")
    within = start_attempt(checked, first.id, "w", policy=POLICY)
    after = start_attempt(checked, second.id, "w", policy=POLICY)
    request(checked, first, BRIEF_GRACE)
    request(checked, second, BRIEF_GRACE)
    until_expired(checked, attempt_now(checked, within.id), attempt_now(checked, after.id))

    # Expired but not yet revoked: a legitimate late success still stands.
    done = complete(checked, within, SUCCESS)
    assert (done.task.state, done.task.result) == (TaskState.SUCCEEDED, SUCCESS.result)
    assert done.task.cancel_requested_at is not None

    assert revoke(checked).attempt.id == after.id
    before = snapshot(checked)
    for outcome in (SUCCESS, AttemptCancelled(), TRANSIENT):
        with pytest.raises(AttemptRevokedError) as info:
            complete(checked, after, outcome)
        assert info.value.reason is RevocationReason.CANCELLATION_GRACE_EXPIRED
    with pytest.raises(AttemptRevokedError):
        renew(checked, after)
    assert snapshot(checked) == before
    assert revoke_next_expired(checked) is None


# Forced orders, as in section 20: the first transaction is held at a gate
# while holding its row locks, and the second is observed waiting for them.


def test_a_heartbeat_first_then_the_request_resets_the_renewed_lease(
    checked: Conn, gate: Gate, worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    release = gate("UPDATE", "attempts")
    beater, requester = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        beating = pool.submit(renew, beater, attempt)
        wait_for_lock(checked, beater, "advisory")  # attempt locked, no request seen
        requesting = pool.submit(request_cancellation, requester, task.id, grace=GRACE)
        wait_for_lock(checked, requester, "transactionid")  # task locked, waits for the attempt
        release()
        renewed = beating.result(timeout=20)
        result = requesting.result(timeout=20)
    requested_at = result.task.cancel_requested_at
    assert requested_at is not None
    final = attempt_now(checked, attempt.id)
    assert final.lease_expires_at == requested_at + GRACE  # the request read the fresh lease
    assert renewed.lease_expires_at is not None
    assert final.lease_expires_at < renewed.lease_expires_at  # the renewal did not survive
    with pytest.raises(CancellationRequestedError):
        renew(checked, attempt)


def test_a_request_first_then_the_heartbeat_is_refused(
    checked: Conn, gate: Gate, worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    release = gate("UPDATE", "tasks")
    requester, beater = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        requesting = pool.submit(request_cancellation, requester, task.id, grace=GRACE)
        wait_for_lock(checked, requester, "advisory")  # task and attempt locked
        beating = pool.submit(renew, beater, attempt)
        wait_for_lock(checked, beater, "transactionid")  # waits for the attempt row
        release()
        result = requesting.result(timeout=20)
        with pytest.raises(CancellationRequestedError):
            beating.result(timeout=20)
    requested_at = result.task.cancel_requested_at
    assert requested_at is not None
    assert attempt_now(checked, attempt.id).lease_expires_at == requested_at + GRACE


@pytest.mark.parametrize("first", ["recover", "request"])
def test_a_request_and_recovery_in_either_order(
    checked: Conn, gate: Gate, worker: Callable[[], Conn], first: str
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    until_expired(checked, attempt)
    release = gate("UPDATE", "tasks")
    recoverer, requester = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        if first == "recover":
            recovering = pool.submit(revoke_next_expired, recoverer)
            wait_for_lock(checked, recoverer, "advisory")  # attempt revoked, task not yet
            requesting = pool.submit(request_cancellation, requester, task.id, grace=GRACE)
            wait_for_lock(checked, requester, "transactionid")
            release()
            assert recovering.result(timeout=20) is not None
            # Requeued by the recovery, then cancelled at once by the request.
            assert requesting.result(timeout=20).outcome is CancellationOutcome.CANCELLED
        else:
            requesting = pool.submit(request_cancellation, requester, task.id, grace=GRACE)
            wait_for_lock(checked, requester, "advisory")  # task and attempt locked
            assert revoke_next_expired(no_wait(recoverer)) is None  # skips the locked task
            release()
            assert requesting.result(timeout=20).outcome is CancellationOutcome.REQUESTED
            assert revoke(recoverer).task.state is TaskState.CANCELLED
    revoked = attempt_now(checked, attempt.id)
    assert revoked.revoked_reason is RevocationReason.LEASE_EXPIRED  # expired before the request
    assert task_now(checked, task.id).state is TaskState.CANCELLED


def test_a_failed_request_rolls_back_the_request_and_the_lease(checked: Conn) -> None:
    # A test-only trigger fails the lease update, after the task row was
    # already updated in the same transaction.
    checked.execute("""
        CREATE FUNCTION poison() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'poisoned lease'; END $$;
        CREATE TRIGGER poison BEFORE UPDATE ON attempts FOR EACH ROW
            WHEN (NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at)
            EXECUTE FUNCTION poison();
    """)
    task = new_task(checked)
    start_attempt(checked, task.id, "w", policy=POLICY)
    before = snapshot(checked)
    with pytest.raises(errors.RaiseException, match="poisoned lease"):
        request(checked, task)
    assert snapshot(checked) == before
    assert task_now(checked, task.id).cancel_requested_at is None
    checked.execute("DROP TRIGGER poison ON attempts")
    assert request(checked, task) is CancellationOutcome.REQUESTED


def test_a_failed_grace_revocation_rolls_back_both_records(checked: Conn) -> None:
    checked.execute("""
        CREATE FUNCTION poison() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'poisoned cancellation'; END $$;
        CREATE TRIGGER poison BEFORE UPDATE ON tasks
            FOR EACH ROW WHEN (NEW.state = 'cancelled') EXECUTE FUNCTION poison();
    """)
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    request(checked, task, BRIEF_GRACE)
    until_expired(checked, attempt_now(checked, attempt.id))
    before = snapshot(checked)
    with pytest.raises(errors.RaiseException, match="poisoned cancellation"):
        revoke_next_expired(checked)
    assert snapshot(checked) == before
    checked.execute("DROP TRIGGER poison ON tasks")
    assert revoke(checked).task.state is TaskState.CANCELLED


def test_a_connection_lost_mid_request_records_nothing(
    checked: Conn, gate: Gate, worker: Callable[[], Conn]
) -> None:
    task = new_task(checked)
    start_attempt(checked, task.id, "w", policy=POLICY)
    before = snapshot(checked)
    release = gate("UPDATE", "attempts")
    conn = worker()
    with ThreadPoolExecutor(1) as pool:
        requesting = pool.submit(request_cancellation, conn, task.id, grace=GRACE)
        wait_for_lock(
            checked, conn, "advisory"
        )  # request recorded in its transaction, lease not yet
        checked.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
        with pytest.raises(psycopg.OperationalError):
            requesting.result(timeout=20)
    release()
    assert snapshot(checked) == before
    assert request(checked, task) is CancellationOutcome.REQUESTED


def test_a_request_refuses_a_running_task_without_its_running_attempt(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    # Corrupt the cross-table invariant behind the operations' back.
    checked.execute(
        "UPDATE attempts SET state = 'failed', failure_kind = 'permanent', error_type = 'x', "
        "finished_at = now() WHERE id = %s",
        (attempt.id,),
    )
    before = snapshot(checked)
    with pytest.raises(RuntimeError, match="invariant violated"):
        request(checked, task)
    assert snapshot(checked) == before
    # Restore the invariant so the fixture's final check describes the operations.
    checked.execute(
        "UPDATE tasks SET state = 'failed', finished_at = now() WHERE id = %s", (task.id,)
    )


def test_cancellation_heartbeats_completion_and_recovery_never_deadlock(
    schema_url: str, checked: Conn
) -> None:
    # Unforced races on one task with a valid lease: a request with a brief
    # grace, a heartbeat, a completion and two recoverers, with seeded start
    # jitter. Whatever the interleaving, nothing deadlocks (a DeadlockDetected
    # would escape race()), exactly one of completion and recovery ends the
    # attempt, a completion's success stands, a grace revocation always cancels
    # the task, and the invariants hold.
    #
    # Which of them wins is left to scheduling and not asserted. Each outcome
    # is forced deterministically elsewhere: completion first and recovery
    # first in test_completion_and_recovery_in_either_order, and for a grace
    # period in test_a_completion_within_the_grace_counts_and_after_revocation_does_not
    # and test_an_ignored_cancellation_is_revoked_when_its_grace_ends. The start
    # windows below, in seconds, only vary which interleavings occur.
    windows = {
        "request": (0.0, 0.001),
        "renew": (0.0, 0.004),
        "complete": (0.0, 0.006),
        "revoke": (0.001, 0.004),
        "revoke-again": (0.002, 0.006),
    }
    for n in range(40):
        task = new_task(checked, max_attempts=1 if n % 2 else 3)
        attempt = start_attempt(checked, task.id, "w", policy=POLICY)
        participants: dict[str, Callable[[Conn], Any]] = {
            "request": partial(request_cancellation, task_id=task.id, grace=BRIEF_GRACE),
            "renew": partial(renew, attempt=attempt),
            "complete": partial(complete, attempt=attempt, outcome=SUCCESS),
            "revoke": revoke_next_expired,
            "revoke-again": revoke_next_expired,
        }
        calls = [
            jittered(call, seed=n * 10 + i, earliest=windows[name][0], latest=windows[name][1])
            for i, (name, call) in enumerate(participants.items())
        ]
        results = dict(zip(participants, race(schema_url, calls), strict=True))
        revocations = [
            r
            for key in ("revoke", "revoke-again")
            if isinstance(r := results[key], AttemptRevocation)
        ]
        completion = results["complete"]
        completed = isinstance(completion, AttemptCompletion)
        assert len(revocations) + completed == 1
        if revocations:
            (revocation,) = revocations
            assert revocation.attempt.revoked_reason is RevocationReason.CANCELLATION_GRACE_EXPIRED
            assert revocation.task.state is TaskState.CANCELLED
            assert isinstance(completion, AttemptRevokedError)
        else:
            assert isinstance(completion, AttemptCompletion)
            assert completion.task.state is TaskState.SUCCEEDED  # a success stands (DEC-020)
            # The attempt is finished, so recovery finds nothing to revoke.
            assert revoke_next_expired(checked) is None
        assert_invariants(checked)
