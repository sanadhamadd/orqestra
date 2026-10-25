"""Attempt lifecycle against real PostgreSQL: start, complete, retry, cancel, job status.

Every test ends by checking the cross-table invariants over the whole schema,
and the race tests check them after every round. Concurrency tests use one
connection per thread, as separate worker processes would. Deterministic
tests hold one transaction at a known point with a test-only gate trigger and
observe the competitor waiting in ``pg_locks`` before letting either proceed.
"""

import secrets
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
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
from orqestra.domain.policy import FailureKind
from orqestra.domain.states import AttemptState, TaskState
from orqestra.persistence.jobs import get_job_progress, get_task, list_tasks, submit_job
from orqestra.persistence.lifecycle import (
    AttemptCompletion,
    AttemptFinishedError,
    CancellationOutcome,
    NotFoundError,
    StaleAttemptError,
    TaskNotStartableError,
    complete_attempt,
    get_attempt,
    list_attempts,
    request_cancellation,
    start_attempt,
)

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]
Snapshot = dict[str, list[tuple[Any, ...]]]

SUCCESS = AttemptSucceeded({"verified": True})
TRANSIENT = AttemptFailed(FailureKind.TRANSIENT, "test.RateLimited", "429")
PERMANENT = AttemptFailed(FailureKind.PERMANENT, "test.InvalidInvoice", "no total")


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


def snapshot(db: Conn) -> Snapshot:
    return {
        table: db.execute(
            sql.SQL("SELECT * FROM {} ORDER BY id").format(sql.Identifier(table))
        ).fetchall()
        for table in ("jobs", "tasks", "attempts")
    }


# Each query returns (rule, task_id) for every task that breaks the rule. The
# history rules hold for the operations implemented so far, where a task is only
# requeued after a transient failure.
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
SELECT 'every earlier attempt failed transiently', a.task_id
FROM attempts a JOIN tasks t ON t.id = a.task_id
WHERE a.attempt_number < t.attempt_count
    AND NOT (a.state = 'failed' AND a.failure_kind = 'transient')
UNION ALL
SELECT 'a queued task''s last attempt failed transiently', t.id
FROM tasks t JOIN attempts a ON a.task_id = t.id AND a.attempt_number = t.attempt_count
WHERE t.state = 'queued' AND NOT (a.state = 'failed' AND a.failure_kind = 'transient')
UNION ALL
SELECT 'a failed task''s last attempt failed for good', t.id
FROM tasks t LEFT JOIN attempts a ON a.task_id = t.id AND a.attempt_number = t.attempt_count
WHERE t.state = 'failed' AND NOT (
    a.state = 'failed' AND (a.failure_kind <> 'transient' OR t.attempt_count = t.max_attempts)
)
UNION ALL
SELECT 'a cancelled task''s last attempt was cancelled or failed transiently', t.id
FROM tasks t JOIN attempts a ON a.task_id = t.id AND a.attempt_number = t.attempt_count
WHERE t.state = 'cancelled'
    AND NOT (a.state = 'cancelled' OR (a.state = 'failed' AND a.failure_kind = 'transient'))
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
    start_attempt(checked, running.id, "other-worker")
    complete(checked, start_attempt(checked, done.id, "other-worker"), SUCCESS)
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

    attempt = start_attempt(db, task.id, "worker-1")
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
        start_attempt(checked, missing, "w")
    assert (info.value.kind, info.value.record_id) == ("task", missing)
    with pytest.raises(NotFoundError, match="attempt"):
        complete_attempt(checked, missing, generation=1, outcome=SUCCESS)
    with pytest.raises(NotFoundError, match="task"):
        request_cancellation(checked, missing)
    assert get_attempt(checked, missing) is None
    assert list_attempts(checked, missing) == ()


@pytest.mark.parametrize(
    "operation",
    [
        lambda conn, task: start_attempt(conn, task.id, "w"),
        lambda conn, task: request_cancellation(conn, task.id),
        lambda conn, task: complete_attempt(conn, uuid.uuid4(), generation=1, outcome=SUCCESS),
    ],
    ids=["start", "cancel", "complete"],
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
        attempt = start_attempt(conn, first.id, "w")
        assert attempt == get_attempt(checked, attempt.id)
        done = complete_attempt(conn, attempt.id, generation=1, outcome=SUCCESS)
        assert done.task == task_now(checked, first.id)
        assert list_attempts(conn, first.id) == (done.attempt,)
        assert request_cancellation(conn, second.id).task == task_now(checked, second.id)


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
                partial(start_attempt, task_id=task.id, worker_id=f"w{n}-{i}")
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
        first = pool.submit(start_attempt, first_conn, task.id, "first")
        wait_for_lock(checked, first_conn, "advisory")  # task claimed, attempt not inserted
        second = pool.submit(start_attempt, second_conn, task.id, "second")
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
        start_attempt(checked, task.id, "   ")
    assert info.value.diag.constraint_name == "attempts_worker_id_not_blank"
    assert task_now(checked, task.id) == task  # queued, generation 0, untouched
    assert list_attempts(checked, task.id) == ()
    # The generation was not consumed: the next start is attempt 1.
    assert start_attempt(checked, task.id, "w").attempt_number == 1
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
        start_attempt(checked, task.id, "poison")
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
    done = complete(checked, start_attempt(checked, task.id, "w"), AttemptSucceeded(result))
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
    attempt = start_attempt(checked, task.id, "w")
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
    attempt = start_attempt(checked, task.id, "w")
    complete(checked, attempt, SUCCESS)
    before = snapshot(checked)
    with pytest.raises(AttemptFinishedError) as info:
        complete(checked, attempt, duplicate)
    assert info.value.state is AttemptState.SUCCEEDED
    assert snapshot(checked) == before
    assert task_now(checked, task.id).result == {"verified": True}


def test_a_duplicate_completion_of_a_requeued_attempt_changes_nothing(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w")
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
        attempt = start_attempt(checked, task.id, "w")
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
    first = start_attempt(checked, task.id, "w1")
    failed = complete(checked, first, TRANSIENT)
    assert failed.task.state is TaskState.QUEUED
    assert (failed.task.attempt_count, failed.task.finished_at) == (1, None)
    assert failed.attempt.state is AttemptState.FAILED
    assert failed.attempt.failure_kind is FailureKind.TRANSIENT
    assert (failed.attempt.error_type, failed.attempt.error_message) == ("test.RateLimited", "429")

    second = start_attempt(checked, task.id, "w2")
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
    done = complete(checked, start_attempt(checked, task.id, "w"), failure)
    assert done.task.state is TaskState.FAILED
    assert done.task.finished_at is not None
    assert done.task.result is None
    assert done.attempt.failure_kind is (kind or FailureKind.UNCLASSIFIED)
    assert (done.attempt.error_type, done.attempt.error_message) == (
        failure.error_type,
        failure.error_message,
    )
    with pytest.raises(TaskNotStartableError):
        start_attempt(checked, task.id, "w")


def test_a_transient_exception_is_classified_for_retry(checked: Conn) -> None:
    task = new_task(checked)
    failure = AttemptFailed.from_exception(TransientError("timeout"))
    assert failure.error_type == "orqestra.domain.errors.TransientError"
    done = complete(checked, start_attempt(checked, task.id, "w"), failure)
    assert done.task.state is TaskState.QUEUED


def test_retries_stop_at_max_attempts(checked: Conn) -> None:
    task = new_task(checked, max_attempts=3)
    for number in (1, 2, 3):
        attempt = start_attempt(checked, task.id, f"w{number}")
        assert attempt.attempt_number == number
        done = complete(checked, attempt, TRANSIENT)
    assert done.task.state is TaskState.FAILED  # exhausted, although transient
    assert done.task.attempt_count == 3
    with pytest.raises(TaskNotStartableError) as info:
        start_attempt(checked, task.id, "w4")
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
                    attempt = start_attempt(conn, task_id, f"pid-{conn.info.backend_pid}")
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
    old = start_attempt(checked, task.id, "w1")
    complete(checked, old, TRANSIENT)
    current = start_attempt(checked, task.id, "w2")
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


def revoke_ownership(db: Conn, attempt: Attempt) -> None:
    """Test stand-in for future lease recovery: end a running attempt and requeue its task."""
    with db.transaction():
        db.execute(
            "UPDATE attempts SET state = 'failed', failure_kind = 'transient', "
            "error_type = 'test.OwnershipRevoked', finished_at = now() WHERE id = %s",
            (attempt.id,),
        )
        db.execute("UPDATE tasks SET state = 'queued' WHERE id = %s", (attempt.task_id,))


def test_a_worker_that_lost_ownership_cannot_overwrite_the_new_result(checked: Conn) -> None:
    task = new_task(checked)
    slow = start_attempt(checked, task.id, "slow-worker")
    revoke_ownership(checked, slow)
    fast = start_attempt(checked, task.id, "fast-worker")
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
    complete(checked, start_attempt(checked, task.id, "w1"), TRANSIENT)
    current = start_attempt(checked, task.id, "w2")  # generation 2
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
    recorded = complete(checked, start_attempt(checked, task.id, "w"), SUCCESS)  # generation 1
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
    attempt = start_attempt(checked, task.id, "w")
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
    result = request_cancellation(checked, task.id)
    assert result.outcome is CancellationOutcome.CANCELLED
    assert result.task == task_now(checked, task.id)
    assert result.task.state is TaskState.CANCELLED
    assert result.task.cancel_requested_at is not None
    assert result.task.finished_at == result.task.cancel_requested_at == result.task.updated_at
    with pytest.raises(TaskNotStartableError) as info:
        start_attempt(checked, task.id, "w")
    assert info.value.state is TaskState.CANCELLED
    assert list_attempts(checked, task.id) == ()
    assert_untouched(checked, unrelated)


def test_cancelling_a_requeued_task_keeps_its_attempt_history(checked: Conn) -> None:
    task = new_task(checked)
    failed = complete(checked, start_attempt(checked, task.id, "w"), TRANSIENT).attempt
    result = request_cancellation(checked, task.id)
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
    attempt = start_attempt(checked, task.id, "w")
    release = gate("UPDATE", "tasks")
    cancel_conn, worker_conn = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        if first == "cancel":
            cancel = pool.submit(request_cancellation, cancel_conn, task.id)
            wait_for_lock(checked, cancel_conn, "advisory")  # holds the task row lock
            finish = pool.submit(complete, worker_conn, attempt, outcome)
            wait_for_lock(checked, worker_conn, "transactionid")  # waits for the task row
        else:
            finish = pool.submit(complete, worker_conn, attempt, outcome)
            wait_for_lock(checked, worker_conn, "advisory")  # attempt updated, task row locked
            cancel = pool.submit(request_cancellation, cancel_conn, task.id)
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
        attempt = start_attempt(checked, task.id, "w")
        finished, cancelled = race(
            schema_url,
            [
                partial(complete, attempt=attempt, outcome=SUCCESS),
                partial(request_cancellation, task_id=task.id),
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
        attempt = start_attempt(checked, task.id, "w")
        finished, cancelled = race(
            schema_url,
            [
                partial(complete, attempt=attempt, outcome=TRANSIENT),
                partial(request_cancellation, task_id=task.id),
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
    attempt = start_attempt(checked, task.id, "w")
    requested = request_cancellation(checked, task.id)
    assert requested.outcome is CancellationOutcome.REQUESTED
    assert requested.task.state is TaskState.RUNNING  # cooperative: still running

    done = complete(checked, attempt, outcome)
    assert done.task.state is task_state
    assert done.task.finished_at is not None
    assert done.task.cancel_requested_at == requested.task.cancel_requested_at
    assert done.attempt.state is outcome.state  # the attempt's truth is kept
    with pytest.raises(TaskNotStartableError):
        start_attempt(checked, task.id, "w")


def test_repeated_requests_keep_the_first(checked: Conn) -> None:
    task = new_task(checked)
    start_attempt(checked, task.id, "w")
    first = request_cancellation(checked, task.id)
    before = snapshot(checked)
    again = request_cancellation(checked, task.id)
    assert again.outcome is CancellationOutcome.ALREADY_REQUESTED
    assert again.task == first.task
    assert snapshot(checked) == before


def test_an_unrequested_cancellation_is_refused(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w")
    before = snapshot(checked)
    with pytest.raises(ValueError, match="cancellation request"):
        complete(checked, attempt, AttemptCancelled())
    assert snapshot(checked) == before


# --- 13. terminal immutability -----------------------------------------------


def finish_as(db: Conn, how: str) -> tuple[Task, Attempt | None]:
    task = new_task(db, max_attempts=3)
    if how == "cancelled-while-queued":
        request_cancellation(db, task.id)
        return task_now(db, task.id), None
    attempt = start_attempt(db, task.id, "w")
    if how == "cancelled-while-running":
        request_cancellation(db, task.id)
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
        start_attempt(checked, task.id, "w")
    assert info.value.state is task.state
    assert request_cancellation(checked, task.id).outcome is CancellationOutcome.ALREADY_FINISHED
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
    attempt_a = start_attempt(checked, a.id, "w")
    assert status() == (JobStatus.RUNNING, {"queued": 3, "running": 1})
    complete(checked, attempt_a, TRANSIENT)  # requeued: every task is queued again
    assert status() == (JobStatus.QUEUED, {"queued": 4})
    complete(checked, start_attempt(checked, a.id, "w"), SUCCESS)
    assert status() == (JobStatus.RUNNING, {"queued": 3, "succeeded": 1})
    complete(checked, start_attempt(checked, b.id, "w"), SUCCESS)
    request_cancellation(checked, c.id)
    assert status() == (JobStatus.RUNNING, {"queued": 1, "succeeded": 2, "cancelled": 1})
    complete(checked, start_attempt(checked, d.id, "w"), PERMANENT)
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
            request_cancellation(checked, task.id)
        else:
            complete(
                checked,
                start_attempt(checked, task.id, "w"),
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
    attempt = start_attempt(checked, task.id, "w")
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
    attempt = start_attempt(checked, task.id, "w")
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
        complete(checked, start_attempt(checked, task.id, "w"), TRANSIENT)
    last = start_attempt(checked, task.id, "w")
    listed = list_attempts(checked, task.id)
    assert [a.attempt_number for a in listed] == [1, 2, 3, 4]
    assert listed[-1] == last
    other = new_task(checked)
    assert list_attempts(checked, other.id) == ()
