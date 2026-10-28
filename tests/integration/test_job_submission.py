"""Job submission and job/task reads against real PostgreSQL.

Covers atomicity (a failure anywhere leaves nothing behind), idempotent
replay and conflict detection, concurrent submissions, retrieval, and that
unrelated rows are never touched. Concurrency tests use one connection per
thread, as separate processes would.
"""

import contextlib
import secrets
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

import psycopg
import pytest
from psycopg import IsolationLevel, errors, sql
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow, dict_row

from orqestra.domain.jobs import JobSubmission, JsonValue, TaskSpec
from orqestra.domain.partitioning import partition
from orqestra.domain.states import TaskState
from orqestra.persistence.jobs import (
    IdempotencyConflictError,
    SubmissionResult,
    get_job,
    get_task,
    list_tasks,
    submit_job,
)

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

KIND = "invoice_verification"


def tasks(*keys: str, max_attempts: int = 3) -> tuple[TaskSpec, ...]:
    return tuple(TaskSpec(key, {"invoice": key, "lines": [1, 2]}, max_attempts) for key in keys)


def submission(*keys: str, key: str | None = None, kind: str = KIND) -> JobSubmission:
    return JobSubmission(kind=kind, tasks=tasks(*keys), idempotency_key=key)


def snapshot(db: Conn) -> dict[str, list[tuple[Any, ...]]]:
    """Every row of every table, in a stable order."""
    return {
        table: db.execute(
            sql.SQL("SELECT * FROM {} ORDER BY id").format(sql.Identifier(table))
        ).fetchall()
        for table in ("jobs", "tasks", "attempts")
    }


def counts(db: Conn) -> tuple[int, int]:
    row = db.execute("SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM tasks)").fetchone()
    assert row is not None
    return int(row[0]), int(row[1])


@pytest.fixture
def unrelated(db: Conn) -> dict[str, list[tuple[Any, ...]]]:
    """Seed other jobs, including a running task with an attempt; return the snapshot."""
    submit_job(db, submission("other-1", "other-2", key="other-batch"))
    other = submit_job(db, submission("other-3")).job
    (task,) = list_tasks(db, other.id)
    db.execute("UPDATE tasks SET state = 'running', attempt_count = 1 WHERE id = %s", (task.id,))
    db.execute(
        "INSERT INTO attempts (id, task_id, attempt_number, worker_id, lease_expires_at, "
        "deadline_at) VALUES (%s, %s, 1, 'w1', now() + interval '5 minutes', "
        "now() + interval '1 hour')",
        (uuid.uuid4(), task.id),
    )
    return snapshot(db)


def assert_unchanged(db: Conn, before: dict[str, list[tuple[Any, ...]]]) -> None:
    after = snapshot(db)
    for table, rows in before.items():
        missing = [row for row in rows if row not in after[table]]
        assert not missing, f"{table}: existing rows changed or vanished: {missing}"


def assert_only_added(
    db: Conn, before: dict[str, list[tuple[Any, ...]]], *, jobs: int, tasks: int
) -> None:
    assert_unchanged(db, before)
    after = snapshot(db)
    added = {table: len(after[table]) - len(rows) for table, rows in before.items()}
    assert added == {"jobs": jobs, "tasks": tasks, "attempts": 0}


# --- submission and retrieval ------------------------------------------------


def test_submission_stores_the_job_and_all_its_tasks(db: Conn, unrelated: Any) -> None:
    request = JobSubmission(
        kind=KIND,
        tasks=(
            TaskSpec("invoice-2", {"total": "10.00"}, 3),
            TaskSpec("invoice-1", {"total": "5.50", "lines": [{"sku": "A", "qty": 2}]}, 1),
            TaskSpec("invoice-3", None, 100),
        ),
        idempotency_key="batch-1",
    )
    result = submit_job(db, request)

    assert result.created
    job = result.job
    assert (job.kind, job.idempotency_key) == (KIND, "batch-1")
    assert get_job(db, job.id) == job

    stored = list_tasks(db, job.id)
    assert [t.task_key for t in stored] == ["invoice-1", "invoice-2", "invoice-3"]
    by_key = {spec.task_key: spec for spec in request.tasks}
    for task in stored:
        spec = by_key[task.task_key]
        assert (task.payload, task.max_attempts) == (spec.payload, spec.max_attempts)
        assert task.job_id == job.id
        assert task.state is TaskState.QUEUED
        assert (task.attempt_count, task.result) == (0, None)
        assert (task.cancel_requested_at, task.finished_at) == (None, None)
        # One transaction: the database clock gives every row the same time.
        assert task.created_at == task.updated_at == job.created_at
    assert len({t.id for t in stored}) == 3
    assert_only_added(db, unrelated, jobs=1, tasks=3)


def test_reads_return_the_requested_records_only(db: Conn, unrelated: Any) -> None:
    first = submit_job(db, submission("a", "b")).job
    second = submit_job(db, submission("a", "c")).job

    first_tasks, second_tasks = list_tasks(db, first.id), list_tasks(db, second.id)
    assert {t.job_id for t in first_tasks} == {first.id}
    assert [t.task_key for t in second_tasks] == ["a", "c"]
    for task in first_tasks + second_tasks:
        assert get_task(db, task.id) == task
    assert get_job(db, second.id) == second

    assert get_job(db, uuid.uuid4()) is None
    assert get_task(db, uuid.uuid4()) is None
    assert get_task(db, first.id) is None  # a job ID is not a task ID
    assert list_tasks(db, uuid.uuid4()) == ()
    assert_unchanged(db, unrelated)


def test_tasks_are_listed_in_code_point_order_whatever_the_locale(db: Conn) -> None:
    keys = ["b", "B", "a", "_x", "10", "9", "é", "Z", "a-2", "a-10"]
    job = submit_job(db, submission(*keys)).job
    assert [t.task_key for t in list_tasks(db, job.id)] == sorted(keys)


def test_reads_reflect_later_task_state(db: Conn) -> None:
    job = submit_job(db, submission("a")).job
    (task,) = list_tasks(db, job.id)
    db.execute(
        "UPDATE tasks SET state = 'running', attempt_count = 1, updated_at = now() WHERE id = %s",
        (task.id,),
    )
    current = get_task(db, task.id)
    assert current is not None
    assert (current.state, current.attempt_count) == (TaskState.RUNNING, 1)
    assert list_tasks(db, job.id) == (current,)


@pytest.mark.parametrize(
    "payload",
    [
        {"nested": {"list": [1, 2.5, None, True, "x"]}, "unicode": "Grüße ✓"},
        [1, "two", {"three": 3}],
        "a string",
        42,
        None,
    ],
)
def test_payloads_round_trip(db: Conn, payload: Any) -> None:
    job = submit_job(db, JobSubmission(KIND, (TaskSpec("t", payload, 2),))).job
    (task,) = list_tasks(db, job.id)
    assert task.payload == payload


def test_submissions_without_a_key_are_never_deduplicated(db: Conn) -> None:
    first = submit_job(db, submission("a"))
    second = submit_job(db, submission("a"))
    assert first.created
    assert second.created
    assert first.job.id != second.job.id
    assert counts(db) == (2, 2)


def test_commits_on_a_connection_that_is_not_in_autocommit(schema_url: str, db: Conn) -> None:
    with psycopg.connect(schema_url) as conn:
        result = submit_job(conn, submission("a", "b"))
        assert conn.info.transaction_status is TransactionStatus.IDLE
    assert get_job(db, result.job.id) == result.job
    assert len(list_tasks(db, result.job.id)) == 2


def test_refuses_to_join_a_transaction_the_caller_opened(schema_url: str, db: Conn) -> None:
    with db.transaction(), pytest.raises(RuntimeError, match="outside any open transaction"):
        submit_job(db, submission("a"))
    assert counts(db) == (0, 0)


@pytest.mark.parametrize(
    ("opening_statement", "status"),
    [
        ("SELECT 1", TransactionStatus.INTRANS),
        ("SELECT 1 / 0", TransactionStatus.INERROR),
    ],
    ids=["implicit", "failed"],
)
def test_leaves_a_refused_caller_transaction_as_it_was(
    schema_url: str, db: Conn, opening_statement: str, status: TransactionStatus
) -> None:
    with psycopg.connect(schema_url) as conn:
        with contextlib.suppress(errors.DivisionByZero):
            conn.execute(opening_statement)  # psycopg opens an implicit transaction
        with pytest.raises(RuntimeError, match=f"outside any open transaction.*{status.name}"):
            submit_job(conn, submission("a"))
        # The caller still owns its transaction, unchanged; ending it frees the connection.
        assert conn.info.transaction_status is status
        conn.rollback()
        assert submit_job(conn, submission("a")).created
    assert counts(db) == (1, 1)


def test_reads_on_a_non_autocommit_connection_leave_the_transaction_to_the_caller(
    schema_url: str, db: Conn
) -> None:
    with psycopg.connect(schema_url) as conn:
        job = submit_job(conn, submission("a")).job
        assert get_job(conn, job.id) == job
        # psycopg opened an implicit transaction for the read; the caller ends it.
        assert conn.info.transaction_status is TransactionStatus.INTRANS
        with pytest.raises(RuntimeError, match="outside any open transaction"):
            submit_job(conn, submission("b"))
        conn.commit()
        assert submit_job(conn, submission("b")).created


@pytest.mark.parametrize("lose", ["closed", "terminated"])
def test_a_closed_or_lost_connection_is_reported_as_a_connection_error(
    schema_url: str, db: Conn, lose: str
) -> None:
    conn = psycopg.connect(schema_url, autocommit=True)
    if lose == "closed":
        conn.close()
    else:
        db.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
        with pytest.raises(psycopg.OperationalError):
            conn.execute("SELECT 1")  # psycopg notices the loss
    assert conn.info.transaction_status is TransactionStatus.UNKNOWN
    with pytest.raises(psycopg.OperationalError):
        submit_job(conn, submission("a"))
    conn.close()
    assert counts(db) == (0, 0)


@pytest.mark.parametrize("autocommit", [True, False], ids=["autocommit", "transactional"])
def test_a_connection_stays_usable_after_every_outcome(
    schema_url: str, db: Conn, autocommit: bool
) -> None:
    with psycopg.connect(schema_url, autocommit=autocommit) as conn:
        original = submit_job(conn, submission("a", key="k"))
        assert conn.info.transaction_status is TransactionStatus.IDLE

        with pytest.raises(errors.CheckViolation):
            submit_job(conn, JobSubmission(KIND, (TaskSpec("a", {}, 0),), "other"))
        assert conn.info.transaction_status is TransactionStatus.IDLE

        with pytest.raises(IdempotencyConflictError):
            submit_job(conn, submission("b", key="k"))
        assert conn.info.transaction_status is TransactionStatus.IDLE

        assert submit_job(conn, submission("a", key="k")) == SubmissionResult(
            original.job, created=False
        )
        assert conn.info.transaction_status is TransactionStatus.IDLE

        assert submit_job(conn, submission("c", key="other")).created
        assert conn.info.transaction_status is TransactionStatus.IDLE
    assert counts(db) == (2, 2)


def test_works_on_a_connection_with_any_row_factory(schema_url: str, db: Conn) -> None:
    with psycopg.connect(schema_url, autocommit=True, row_factory=dict_row) as conn:
        original = submit_job(conn, submission("b", "a", key="k"))
        replay = submit_job(conn, submission("a", "b", key="k"))
        with pytest.raises(IdempotencyConflictError):
            submit_job(conn, submission("a", key="k"))
        assert replay == SubmissionResult(original.job, created=False)
        assert get_job(conn, original.job.id) == get_job(db, original.job.id) == original.job
        listed = list_tasks(conn, original.job.id)
        assert listed == list_tasks(db, original.job.id)
        assert [t.task_key for t in listed] == ["a", "b"]
        assert get_task(conn, listed[0].id) == listed[0]


# --- atomicity ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("request_", "failure", "constraint"),
    [
        (
            JobSubmission(KIND, (*tasks("a", "b"), TaskSpec("c", {}, 0)), "batch-x"),
            errors.CheckViolation,
            "tasks_max_attempts_range",
        ),
        (
            JobSubmission(KIND, (*tasks("a"), TaskSpec(" ", {}, 3), *tasks("c")), "batch-x"),
            errors.CheckViolation,
            "tasks_task_key_not_blank",
        ),
        (
            JobSubmission(KIND, (*tasks("a"), TaskSpec("b", {"text": "nul\u0000"}, 3))),
            errors.UntranslatableCharacter,
            None,
        ),
        (submission("a", "b", kind="  "), errors.CheckViolation, "jobs_kind_not_blank"),
        (submission("a", key=""), errors.CheckViolation, "jobs_idempotency_key_not_blank"),
        (
            JobSubmission(KIND, (*tasks("a"), TaskSpec("b", {"total": float("nan")}, 3))),
            errors.InvalidTextRepresentation,
            None,
        ),
    ],
    ids=[
        "retry-limit",
        "blank-task-key",
        "invalid-json-text",
        "blank-kind",
        "blank-key",
        "nan-is-not-json",
    ],
)
def test_a_rejected_submission_leaves_nothing_behind(
    db: Conn,
    unrelated: Any,
    request_: JobSubmission,
    failure: type[psycopg.Error],
    constraint: str | None,
) -> None:
    with pytest.raises(failure) as info:
        submit_job(db, request_)
    assert info.value.diag.constraint_name == constraint
    assert db.info.transaction_status is TransactionStatus.IDLE
    assert_only_added(db, unrelated, jobs=0, tasks=0)


def test_a_task_failure_rolls_back_the_job_and_the_tasks_before_it(
    db: Conn, unrelated: Any
) -> None:
    # A test-only trigger fails the third task's insert and reports what the
    # transaction had written by then, proving there was something to undo.
    db.execute("""
        CREATE FUNCTION poison() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'poisoned after % job(s) and % task(s)',
                (SELECT count(*) FROM jobs WHERE id = NEW.job_id),
                (SELECT count(*) FROM tasks WHERE job_id = NEW.job_id);
        END $$;
        CREATE TRIGGER poison BEFORE INSERT ON tasks
            FOR EACH ROW WHEN (NEW.task_key = 'poison') EXECUTE FUNCTION poison();
    """)
    request = JobSubmission(KIND, (*tasks("a", "b"), *tasks("poison")), "batch-p")

    with pytest.raises(errors.RaiseException, match=r"after 1 job\(s\) and 2 task\(s\)"):
        submit_job(db, request)
    assert_only_added(db, unrelated, jobs=0, tasks=0)

    # The key was not consumed: a corrected submission with it succeeds.
    fixed = submit_job(db, submission("a", "b", "c", key="batch-p"))
    assert fixed.created
    assert_only_added(db, unrelated, jobs=1, tasks=3)


# --- idempotency -------------------------------------------------------------


def test_an_identical_resubmission_returns_the_original_job(db: Conn, unrelated: Any) -> None:
    original = submit_job(db, submission("a", "b", "c", key="batch-1"))
    stored = snapshot(db)

    replay = submit_job(db, submission("a", "b", "c", key="batch-1"))
    assert replay == SubmissionResult(original.job, created=False)
    assert snapshot(db) == stored
    assert_only_added(db, unrelated, jobs=1, tasks=3)


def test_equivalence_ignores_task_order_and_json_formatting(db: Conn) -> None:
    payloads: dict[str, Any] = {
        "a": {"x": 1, "y": [1, 2]},
        "b": {"total": 1, "big": 10**30, "zero": 0},
    }
    original = submit_job(
        db, JobSubmission(KIND, tuple(TaskSpec(k, p, 3) for k, p in payloads.items()), "k1")
    )
    # Same tasks in another order and object keys reordered. 1.0, 1e+30 and -0.0 are
    # the same JSON numbers as 1, 10**30 and 0: jsonb compares numbers by value.
    replay = submit_job(
        db,
        JobSubmission(
            KIND,
            (
                TaskSpec("b", {"zero": -0.0, "big": 1e30, "total": 1.0}, 3),
                TaskSpec("a", {"y": [1, 2], "x": 1}, 3),
            ),
            "k1",
        ),
    )
    assert replay == SubmissionResult(original.job, created=False)
    assert counts(db) == (1, 2)


def test_idempotency_keys_match_exactly(db: Conn) -> None:
    results = [submit_job(db, submission("a", key=k)) for k in ("batch-1", "Batch-1", "batch-1 ")]
    assert all(r.created for r in results)
    assert len({r.job.id for r in results}) == 3


def test_a_replay_does_not_touch_tasks_that_have_progressed(db: Conn) -> None:
    original = submit_job(db, submission("a", "b", key="batch-1")).job
    db.execute(
        "UPDATE tasks SET state = 'running', attempt_count = 1 WHERE job_id = %s "
        "AND task_key = 'a'",
        (original.id,),
    )
    stored = snapshot(db)
    replay = submit_job(db, submission("a", "b", key="batch-1"))
    assert replay == SubmissionResult(original, created=False)
    assert snapshot(db) == stored


@pytest.mark.parametrize(
    "changed",
    [
        submission("a", "b", key="batch-1", kind="other_kind"),
        submission("a", "b", "c", key="batch-1"),
        submission("a", key="batch-1"),
        submission("a", "renamed", key="batch-1"),
        JobSubmission(KIND, (*tasks("a"), *tasks("b", max_attempts=4)), "batch-1"),
        JobSubmission(KIND, (*tasks("a"), TaskSpec("b", {"invoice": "B"}, 3)), "batch-1"),
        JobSubmission(KIND, (*tasks("a"), TaskSpec("b", {"invoice": "b"}, 3)), "batch-1"),
    ],
    ids=[
        "kind",
        "extra-task",
        "missing-task",
        "renamed-task",
        "retry-limit",
        "payload-value",
        "payload-missing-field",
    ],
)
def test_a_different_submission_with_the_same_key_is_rejected(
    db: Conn, unrelated: Any, changed: JobSubmission
) -> None:
    original = submit_job(db, submission("a", "b", key="batch-1")).job
    stored = snapshot(db)

    with pytest.raises(IdempotencyConflictError) as info:
        submit_job(db, changed)
    assert (info.value.idempotency_key, info.value.job_id) == ("batch-1", original.id)
    assert snapshot(db) == stored
    assert db.info.transaction_status is TransactionStatus.IDLE
    assert_only_added(db, unrelated, jobs=1, tasks=2)


def test_a_replay_that_the_database_rejects_is_not_reported_as_a_conflict(
    db: Conn,
) -> None:
    submit_job(db, submission("a", key="k"))
    stored = snapshot(db)
    invalid = JobSubmission(KIND, (TaskSpec("a", {"text": "nul\u0000"}, 3),), "k")
    with pytest.raises(errors.UntranslatableCharacter):
        submit_job(db, invalid)
    assert snapshot(db) == stored


@pytest.mark.parametrize(
    ("first", "second"),
    [
        (True, 1),
        (1, "1"),
        (None, {}),
        ([1, 2], [2, 1]),
        ({"a": None}, {}),
        (0.1 + 0.2, 0.3),
        ("é", "é"),
        ("k", "K"),
    ],
    ids=[
        "bool-vs-number",
        "number-vs-string",
        "null-vs-object",
        "list-order",
        "null-field",
        "float-rounding",
        "unicode-normalisation",
        "string-case",
    ],
)
def test_json_values_that_differ_in_type_or_order_conflict(
    db: Conn, first: Any, second: Any
) -> None:
    submit_job(db, JobSubmission(KIND, (TaskSpec("t", first, 3),), "k1"))
    with pytest.raises(IdempotencyConflictError):
        submit_job(db, JobSubmission(KIND, (TaskSpec("t", second, 3),), "k1"))
    assert counts(db) == (1, 1)


# --- concurrency -------------------------------------------------------------


def connect(url: str) -> Conn:
    conn = psycopg.connect(url, autocommit=True)
    conn.execute("SET statement_timeout = '20s'")  # a hang fails the test instead
    return conn


def race(url: str, requests: list[JobSubmission]) -> list[SubmissionResult | Exception]:
    """Submit every request at the same instant, each on its own connection."""
    barrier = threading.Barrier(len(requests))

    def run(request: JobSubmission) -> SubmissionResult | Exception:
        with connect(url) as conn:
            barrier.wait()
            try:
                return submit_job(conn, request)
            except IdempotencyConflictError as exc:
                return exc

    with ThreadPoolExecutor(len(requests)) as pool:
        return list(pool.map(run, requests))


def test_concurrent_identical_submissions_create_one_job(
    schema_url: str, db: Conn, unrelated: Any
) -> None:
    rounds, submitters = 10, 8
    for n in range(rounds):
        key = f"race-{n}"
        results = race(schema_url, [submission("a", "b", "c", key=key)] * submitters)
        assert all(isinstance(r, SubmissionResult) for r in results)
        outcomes = [r for r in results if isinstance(r, SubmissionResult)]
        assert sum(r.created for r in outcomes) == 1
        assert len({r.job.id for r in outcomes}) == 1
    assert_only_added(db, unrelated, jobs=rounds, tasks=3 * rounds)


def test_concurrent_conflicting_submissions_store_exactly_one(
    schema_url: str, db: Conn, unrelated: Any
) -> None:
    rounds, submitters = 10, 8
    for n in range(rounds):
        key = f"race-{n}"
        requests = [submission("a", key=key), submission("b", key=key)] * (submitters // 2)
        results = race(schema_url, requests)
        winners = [r for r in results if isinstance(r, SubmissionResult) and r.created]
        assert len(winners) == 1
        winner = winners[0].job
        (winning_task,) = list_tasks(db, winner.id)
        for request, result in zip(requests, results, strict=True):
            if request.tasks[0].task_key == winning_task.task_key:
                assert isinstance(result, SubmissionResult)
                assert result.job == winner
            else:
                assert isinstance(result, IdempotencyConflictError)
                assert result.job_id == winner.id
    assert_only_added(db, unrelated, jobs=rounds, tasks=rounds)


@pytest.fixture
def gate(db: Conn) -> Iterator[Callable[[], None]]:
    """Hold every task insert in this schema until the returned ``release`` is called.

    A test-only statement trigger takes a shared advisory lock that this
    fixture holds exclusively, so a submission stops after inserting its job
    row and before committing, while still holding its idempotency key.
    """
    lock_key = secrets.randbits(62)
    db.execute(
        sql.SQL("""
            CREATE FUNCTION gate() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN PERFORM pg_advisory_xact_lock_shared({}); RETURN NULL; END $$;
            CREATE TRIGGER gate BEFORE INSERT ON tasks
                FOR EACH STATEMENT EXECUTE FUNCTION gate();
        """).format(sql.Literal(lock_key))
    )
    db.execute("SELECT pg_advisory_lock(%s)", (lock_key,))
    released = False

    def release() -> None:
        nonlocal released
        if not released:
            db.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
            released = True

    yield release
    release()


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


@pytest.fixture
def submitters(schema_url: str) -> Iterator[Callable[..., tuple[Conn, Future[SubmissionResult]]]]:
    """Start submissions in the background, each on its own connection.

    ``configure`` prepares the connection first. Connections stay open after
    the submission so a test can reuse them, and are closed at teardown.
    """
    opened: list[Conn] = []

    def start(
        pool: ThreadPoolExecutor,
        request: JobSubmission,
        configure: Callable[[Conn], object] = lambda conn: None,
    ) -> tuple[Conn, Future[SubmissionResult]]:
        conn = connect(schema_url)
        opened.append(conn)
        configure(conn)
        return conn, pool.submit(submit_job, conn, request)

    yield start
    for conn in opened:
        conn.close()


def session_default(level: str) -> Callable[[Conn], object]:
    return lambda conn: conn.execute(
        sql.SQL("SET default_transaction_isolation = {}").format(sql.Literal(level))
    )


def psycopg_isolation(level: IsolationLevel) -> Callable[[Conn], object]:
    return lambda conn: setattr(conn, "isolation_level", level)


# The waiting submission's isolation settings must not matter: submit_job sets
# READ COMMITTED itself. Under REPEATABLE READ or SERIALIZABLE, an ON CONFLICT
# that meets a row committed after its snapshot fails with a serialization error.
# A session default and psycopg's own setting (sent with BEGIN) are both covered.
ISOLATIONS = pytest.mark.parametrize(
    "isolation",
    [
        session_default("read committed"),
        session_default("serializable"),
        psycopg_isolation(IsolationLevel.SERIALIZABLE),
    ],
    ids=["session-read-committed", "session-serializable", "psycopg-serializable"],
)

Start = Callable[..., tuple[Conn, Future[SubmissionResult]]]


@ISOLATIONS
def test_a_waiting_duplicate_replays_once_the_first_commits(
    db: Conn, gate: Callable[[], None], submitters: Start, isolation: Callable[[Conn], object]
) -> None:
    request = submission("a", "b", key="batch-1")
    with ThreadPoolExecutor(2) as pool:
        first_conn, first = submitters(pool, request)
        wait_for_lock(db, first_conn, "advisory")  # job row inserted, not committed
        second_conn, second = submitters(pool, request, isolation)
        wait_for_lock(db, second_conn, "transactionid")  # blocked on the first's key

        assert counts(db) == (0, 0)  # nothing is visible before the commit
        gate()
        created, replayed = first.result(timeout=20), second.result(timeout=20)

    assert created.created
    assert replayed == SubmissionResult(created.job, created=False)
    assert counts(db) == (1, 2)


@ISOLATIONS
def test_a_waiting_conflict_is_rejected_once_the_first_commits(
    db: Conn, gate: Callable[[], None], submitters: Start, isolation: Callable[[Conn], object]
) -> None:
    with ThreadPoolExecutor(2) as pool:
        first_conn, first = submitters(pool, submission("a", key="k"))
        wait_for_lock(db, first_conn, "advisory")
        second_conn, second = submitters(pool, submission("b", key="k"), isolation)
        wait_for_lock(db, second_conn, "transactionid")
        gate()
        created = first.result(timeout=20)
        with pytest.raises(IdempotencyConflictError) as info:
            second.result(timeout=20)

    assert info.value.job_id == created.job.id
    assert [t.task_key for t in list_tasks(db, created.job.id)] == ["a"]
    assert counts(db) == (1, 1)


@ISOLATIONS
def test_a_waiting_submission_takes_the_key_if_the_first_rolls_back(
    db: Conn, gate: Callable[[], None], submitters: Start, isolation: Callable[[Conn], object]
) -> None:
    doomed = JobSubmission(KIND, (*tasks("a"), TaskSpec("b", {}, 0)), "k")
    with ThreadPoolExecutor(2) as pool:
        first_conn, first = submitters(pool, doomed)
        wait_for_lock(db, first_conn, "advisory")
        second_conn, second = submitters(pool, submission("c", key="k"), isolation)
        wait_for_lock(db, second_conn, "transactionid")
        gate()
        with pytest.raises(errors.CheckViolation):
            first.result(timeout=20)
        survivor = second.result(timeout=20)

    assert survivor.created
    assert [t.task_key for t in list_tasks(db, survivor.job.id)] == ["c"]
    assert counts(db) == (1, 1)


def test_a_lock_timeout_while_waiting_is_reported_as_such(
    db: Conn, gate: Callable[[], None], submitters: Start
) -> None:
    request = submission("a", key="k")
    with ThreadPoolExecutor(2) as pool:
        first_conn, first = submitters(pool, request)
        wait_for_lock(db, first_conn, "advisory")
        second_conn, second = submitters(
            pool, request, lambda conn: conn.execute("SET lock_timeout = '200ms'")
        )
        # Neither a conflict nor a replay: the database's own timeout, intact.
        with pytest.raises(errors.LockNotAvailable):
            second.result(timeout=20)
        assert second_conn.info.transaction_status is TransactionStatus.IDLE
        gate()
        created = first.result(timeout=20)

    # The connection that timed out is still usable and now sees the job.
    assert submit_job(second_conn, request) == SubmissionResult(created.job, created=False)
    assert counts(db) == (1, 1)


def test_a_connection_lost_mid_submission_stores_nothing(
    db: Conn, gate: Callable[[], None], submitters: Start
) -> None:
    with ThreadPoolExecutor(1) as pool:
        conn, doomed = submitters(pool, submission("a", "b", key="k"))
        wait_for_lock(db, conn, "advisory")  # job row inserted, not committed
        db.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
        with pytest.raises(psycopg.OperationalError):
            doomed.result(timeout=20)
    gate()

    assert counts(db) == (0, 0)
    assert submit_job(db, submission("c", key="k")).created  # the key was never taken


# --- partitioned submissions (DEC-052) --------------------------------------------------------


def stored_batches(db: Conn, job_id: uuid.UUID) -> list[tuple[str, int, list[Any]]]:
    """Each stored task's key, offset and items, as ``list_tasks`` orders them."""
    found = []
    for task in list_tasks(db, job_id):
        assert isinstance(task.payload, dict)
        offset, items = task.payload["offset"], task.payload["items"]
        assert isinstance(offset, int)
        assert isinstance(items, list)
        found.append((task.task_key, offset, items))
    return found


def test_a_partitioned_job_stores_its_batches_in_order(db: Conn) -> None:
    items: list[JsonValue] = [i % 4 for i in range(23)]  # repeats kept
    result = submit_job(db, JobSubmission(KIND, partition(items, batch_size=2, max_attempts=3)))
    batches = stored_batches(db, result.job.id)
    assert len(batches) == 12  # ceil(23 / 2)
    assert [key for key, _, _ in batches] == [f"batch-{i:02d}" for i in range(12)]
    assert [offset for _, offset, _ in batches] == list(range(0, 23, 2))
    assert [x for _, _, chunk in batches for x in chunk] == items  # in order, as given
    assert {task.max_attempts for task in list_tasks(db, result.job.id)} == {3}


def test_a_partitioned_submission_replays_by_its_tasks_not_its_settings(db: Conn) -> None:
    items: list[JsonValue] = [{"id": i} for i in range(5)]
    key = f"partitioned-{secrets.token_hex(4)}"

    def submit(batch_size: int) -> SubmissionResult:
        tasks = partition(items, batch_size=batch_size, max_attempts=3)
        return submit_job(db, JobSubmission(KIND, tasks, idempotency_key=key))

    first = submit(5)  # one batch of all five items
    assert first.created
    before = counts(db)
    for same_tasks in (5, 6, 100):  # every batch size of 5 or more makes the same one task
        replay = submit(same_tasks)
        assert (replay.created, replay.job.id) == (False, first.job.id)
    with pytest.raises(IdempotencyConflictError):
        submit(2)  # different tasks under the same key
    assert counts(db) == before  # nothing was added or changed
    assert stored_batches(db, first.job.id) == [("batch-0", 0, items)]
