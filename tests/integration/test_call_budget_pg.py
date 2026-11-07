"""The call budget (DEC-065) against real PostgreSQL.

Attempts go through the real lifecycle and ledger facts through the real
recording functions, with the usage report tests' helpers. Raw SQL is used only
to commit a task row that ``submit_job`` would never add to an existing job, so
the snapshot test can show it stays unseen.
"""

import uuid
from collections.abc import Iterator

import psycopg
import pytest
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from orqestra.aggregation.budget import BudgetVerdict, CallBudgetCheck, read_job_call_budget
from orqestra.aggregation.invoice import WrongJobKindError
from orqestra.aggregation.usage import JobUsageReport, UsageFinality, read_job_usage
from orqestra.persistence import snapshots
from orqestra.persistence.lifecycle import start_attempt
from orqestra.persistence.provider_usage import ClosedBy, record_usage_closure
from orqestra.persistence.snapshots import job_batch_snapshot
from tests.integration.test_attempt_lifecycle import (
    EXPIRING,
    POLICY,
    SUCCESS,
    complete,
    connect,
    new_tasks,
    revoke,
    until_expired,
)
from tests.integration.test_job_usage_report_pg import answered, close, unresolved
from tests.integration.test_provider_usage_pg import incomplete

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

IDLE = TransactionStatus.IDLE


@pytest.fixture
def other(schema_url: str, db: Conn) -> Iterator[Conn]:
    """A second session on the same schema, for work done while a snapshot is open."""
    conn = connect(schema_url)
    yield conn
    conn.close()


def budget_of(conn: Conn, job_id: uuid.UUID, cap: int) -> tuple[JobUsageReport, CallBudgetCheck]:
    read = read_job_call_budget(conn, job_id, cap=cap)
    assert read is not None
    assert conn.info.transaction_status is IDLE
    return read


def closed_with_calls(db: Conn, calls: int, *, max_attempts: int) -> uuid.UUID:
    """A job of one task whose only attempt made ``calls`` calls, closed them and succeeded."""
    (task,) = new_tasks(db, "t-0", max_attempts=max_attempts)
    attempt = start_attempt(db, task.id, "w", policy=POLICY)
    for seq in range(calls):
        answered(db, attempt.id, seq)
    close(db, attempt.id, calls)
    complete(db, attempt, SUCCESS)
    return task.job_id


# --- one snapshot, no locks --------------------------------------------------------


def test_the_retry_limits_and_the_usage_come_from_one_snapshot_that_takes_no_lock(
    db: Conn, other: Conn
) -> None:
    first, _ = new_tasks(db, "a", "b", max_attempts=2)
    attempt = start_attempt(db, first.id, "w", policy=POLICY)
    answered(db, attempt.id, 0)
    job_id = first.job_id

    with job_batch_snapshot(db, job_id) as snapshot:
        assert snapshot is not None
        # Committed after the job read: a task submit_job would never add, and a new call.
        other.execute(
            "INSERT INTO tasks (id, job_id, task_key, payload, max_attempts) "
            "VALUES (%s, %s, 'late', %s, 5)",
            (uuid.uuid4(), job_id, Jsonb({"late": True})),
        )
        answered(other, attempt.id, 1)
        # The snapshot holds no row lock that a worker could wait on.
        with other.transaction():
            other.execute(
                "SELECT id FROM tasks WHERE job_id = %s FOR UPDATE NOWAIT", (job_id,)
            ).fetchall()
            other.execute(
                "SELECT a.id FROM attempts a JOIN tasks t ON t.id = a.task_id "
                "WHERE t.job_id = %s FOR UPDATE OF a NOWAIT",
                (job_id,),
            ).fetchall()
        builder = read_job_usage(snapshot)
        inside_total = snapshot.max_attempts_total()
    inside = builder.finish()

    assert (inside_total, inside.calls) == (4, 1)

    report, check = budget_of(db, job_id, cap=1)
    assert (check.max_attempts_total, report.calls) == (9, 2)


def test_the_snapshot_reads_nothing_more_once_it_has_ended(db: Conn) -> None:
    (task,) = new_tasks(db, "a")
    with job_batch_snapshot(db, task.job_id) as snapshot:
        assert snapshot is not None
        assert snapshot.max_attempts_total() == 3
    with pytest.raises(RuntimeError, match="has ended"):
        snapshot.max_attempts_total()


# --- verdicts from a real ledger ---------------------------------------------------


def test_a_closed_job_is_within_at_its_cap_and_exceeded_below_it(db: Conn) -> None:
    job_id = closed_with_calls(db, 3, max_attempts=1)

    report, within = budget_of(db, job_id, cap=3)
    assert report.finality is UsageFinality.CLOSED
    assert within == CallBudgetCheck(
        cap=3,
        max_attempts_total=1,
        bound=3,
        calls=3,
        finality=UsageFinality.CLOSED,
        verdict=BudgetVerdict.WITHIN,
        unknown_reason=None,
    )
    assert within.cap_source == "operator"

    _, exceeded = budget_of(db, job_id, cap=2)
    assert (exceeded.max_attempts_total, exceeded.bound, exceeded.calls) == (1, 2, 3)
    assert exceeded.verdict is BudgetVerdict.EXCEEDED


def test_the_bound_counts_every_attempt_a_task_may_start(db: Conn) -> None:
    job_id = closed_with_calls(db, 3, max_attempts=3)

    _, check = budget_of(db, job_id, cap=1)

    assert (check.max_attempts_total, check.bound, check.calls) == (3, 3, 3)
    assert check.verdict is BudgetVerdict.WITHIN


def test_a_job_with_no_attempt_yet_is_unknown_and_open(db: Conn) -> None:
    first, _ = new_tasks(db, "a", "b", max_attempts=2)

    report, check = budget_of(db, first.job_id, cap=4)

    assert (report.calls, check.max_attempts_total, check.bound) == (0, 4, 16)
    assert (check.verdict, check.unknown_reason) == (BudgetVerdict.UNKNOWN, UsageFinality.OPEN)


def test_a_running_attempt_is_unknown_and_open(db: Conn) -> None:
    (task,) = new_tasks(db, "a", max_attempts=2)
    attempt = start_attempt(db, task.id, "w", policy=POLICY)
    answered(db, attempt.id, 0)

    _, check = budget_of(db, task.job_id, cap=4)

    assert (check.calls, check.max_attempts_total, check.bound) == (1, 2, 8)
    assert (check.verdict, check.unknown_reason) == (BudgetVerdict.UNKNOWN, UsageFinality.OPEN)


def test_a_revoked_attempt_with_an_unresolved_call_is_unknown_and_a_lower_bound(
    db: Conn,
) -> None:
    (task,) = new_tasks(db, "a", max_attempts=1)
    attempt = start_attempt(db, task.id, "w", policy=EXPIRING)
    answered(db, attempt.id, 0)
    unresolved(db, attempt.id, 1)
    until_expired(db, attempt)
    assert revoke(db).attempt.id == attempt.id
    record_usage_closure(db, incomplete(attempt.id, closed_by=ClosedBy.RECOVERY))

    report, check = budget_of(db, task.job_id, cap=5)

    assert report.finality is UsageFinality.LOWER_BOUND
    assert (check.calls, check.max_attempts_total, check.bound) == (2, 1, 5)
    assert (check.verdict, check.unknown_reason) == (
        BudgetVerdict.UNKNOWN,
        UsageFinality.LOWER_BOUND,
    )


# --- refusals ----------------------------------------------------------------------


def test_a_missing_job_is_none(db: Conn) -> None:
    assert read_job_call_budget(db, uuid.uuid4(), cap=1) is None
    assert db.info.transaction_status is IDLE


def test_another_kind_of_job_is_refused_before_any_ledger_row_or_task_is_read(
    db: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    (task,) = new_tasks(db, "x", kind="diagnostic.echo")

    def never_read(self: object) -> None:
        pytest.fail("the ledger or the tasks were read for a job of another kind")

    for read in ("usage_totals", "attempt_usage", "tasks", "max_attempts_total"):
        monkeypatch.setattr(snapshots.JobBatchSnapshot, read, never_read)

    with pytest.raises(WrongJobKindError, match=r"diagnostic\.echo"):
        read_job_call_budget(db, task.job_id, cap=1)

    assert db.info.transaction_status is IDLE


def test_a_caller_open_transaction_is_refused(db: Conn) -> None:
    (task,) = new_tasks(db, "a")

    with db.transaction():
        with pytest.raises(RuntimeError, match="own transaction"):
            read_job_call_budget(db, task.job_id, cap=1)
        assert db.info.transaction_status is TransactionStatus.INTRANS

    assert db.info.transaction_status is IDLE


def test_an_invalid_cap_is_refused_before_anything_is_read(
    db: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    (task,) = new_tasks(db, "a")

    def never_opened(*args: object, **kwargs: object) -> None:
        pytest.fail("a snapshot was opened for an invalid cap")

    monkeypatch.setattr("orqestra.aggregation.budget.job_batch_snapshot", never_opened)

    with pytest.raises(ValueError, match="cap"):
        read_job_call_budget(db, task.job_id, cap=-1)
