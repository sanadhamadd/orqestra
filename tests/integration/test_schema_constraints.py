"""Direct-SQL tests of the schema's constraints and the attempt_count fencing guard.

These prove what the database itself refuses to store. They do not prove the
transactional task operations, which are not implemented yet.
"""

import re
import uuid
from typing import Any

import psycopg
import pytest
from psycopg import errors
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from orqestra.domain.leases import RevocationReason
from orqestra.domain.policy import FailureKind
from orqestra.domain.states import AttemptState, TaskState

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]


def new_job(db: Conn, *, kind: str = "invoice_verification", key: str | None = None) -> uuid.UUID:
    job_id = uuid.uuid4()
    db.execute(
        "INSERT INTO jobs (id, kind, idempotency_key) VALUES (%s, %s, %s)", (job_id, kind, key)
    )
    return job_id


def new_task(
    db: Conn, job_id: uuid.UUID, *, task_key: str = "invoice-1", max_attempts: int = 3
) -> uuid.UUID:
    task_id = uuid.uuid4()
    db.execute(
        "INSERT INTO tasks (id, job_id, task_key, payload, max_attempts) "
        "VALUES (%s, %s, %s, %s, %s)",
        (task_id, job_id, task_key, Jsonb({"invoice": task_key}), max_attempts),
    )
    return task_id


def claim(db: Conn, task_id: uuid.UUID) -> int:
    """A legal claim: queued -> running, advancing the generation by one."""
    row = db.execute(
        "UPDATE tasks SET state = 'running', attempt_count = attempt_count + 1 "
        "WHERE id = %s AND state = 'queued' RETURNING attempt_count",
        (task_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def requeue(db: Conn, task_id: uuid.UUID) -> None:
    db.execute("UPDATE tasks SET state = 'queued' WHERE id = %s AND state = 'running'", (task_id,))


def task_row(db: Conn, task_id: uuid.UUID) -> tuple[Any, ...]:
    row = db.execute("SELECT state, attempt_count FROM tasks WHERE id = %s", (task_id,)).fetchone()
    assert row is not None
    return row


# A valid lease for a row inserted now: a running or revoked attempt needs one.
LEASE = {
    "lease_expires_at": "now() + interval '5 minutes'",
    "deadline_at": "now() + interval '1 hour'",
}
LEASE_COLUMNS = ", ".join(LEASE)
LEASE_VALUES = ", ".join(LEASE.values())


def new_attempt(db: Conn, task_id: uuid.UUID, number: int, **columns: Any) -> uuid.UUID:
    """Insert an attempt; it gets a valid lease unless ``columns`` set lease columns."""
    attempt_id = uuid.uuid4()
    values = {"id": attempt_id, "task_id": task_id, "attempt_number": number, "worker_id": "w1"}
    values.update(columns)
    names = list(values)
    placeholders = ["%s"] * len(values)
    if not set(LEASE) & set(values):
        names += LEASE
        placeholders += LEASE.values()
    db.execute(
        f"INSERT INTO attempts ({', '.join(names)}) VALUES ({', '.join(placeholders)})",
        tuple(values.values()),
    )
    return attempt_id


def rejected(
    db: Conn, query: str, params: tuple[Any, ...], *, constraint: str
) -> errors.IntegrityError:
    with pytest.raises(errors.IntegrityError) as info:
        db.execute(query, params)
    assert info.value.diag.constraint_name == constraint
    return info.value


def check_values(db: Conn, constraint: str) -> set[str]:
    row = db.execute(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = %s "
        "AND connamespace = current_schema()::regnamespace",
        (constraint,),
    ).fetchone()
    assert row is not None
    return set(re.findall(r"'([a-z_]+)'::text", str(row[0])))


# --- drift between the domain enums and the stored values -------------------


def test_stored_state_values_match_the_domain(db: Conn) -> None:
    assert check_values(db, "tasks_state_valid") == {state.value for state in TaskState}
    assert check_values(db, "attempts_state_valid") == {state.value for state in AttemptState}
    assert check_values(db, "attempts_failure_kind_valid") == {kind.value for kind in FailureKind}
    assert check_values(db, "attempts_revoked_reason_valid") == {
        reason.value for reason in RevocationReason
    }


# --- jobs --------------------------------------------------------------------


def test_job_idempotency_key_is_unique_but_optional(db: Conn) -> None:
    new_job(db, key=None)
    new_job(db, key=None)
    new_job(db, key="client-batch-7")
    rejected(
        db,
        "INSERT INTO jobs (id, kind, idempotency_key) VALUES (%s, 'k', 'client-batch-7')",
        (uuid.uuid4(),),
        constraint="jobs_idempotency_key_unique",
    )


@pytest.mark.parametrize(
    ("column", "constraint"),
    [("kind", "jobs_kind_not_blank"), ("idempotency_key", "jobs_idempotency_key_not_blank")],
)
def test_job_text_fields_must_not_be_blank(db: Conn, column: str, constraint: str) -> None:
    values = {"kind": "k", "idempotency_key": None, column: "  "}
    rejected(
        db,
        "INSERT INTO jobs (id, kind, idempotency_key) VALUES (%s, %s, %s)",
        (uuid.uuid4(), values["kind"], values["idempotency_key"]),
        constraint=constraint,
    )


# --- tasks -------------------------------------------------------------------


def test_new_task_defaults(db: Conn) -> None:
    task_id = new_task(db, new_job(db))
    row = db.execute(
        "SELECT state, attempt_count, result, cancel_requested_at, finished_at, "
        "created_at IS NOT NULL AND updated_at IS NOT NULL FROM tasks WHERE id = %s",
        (task_id,),
    ).fetchone()
    assert row == ("queued", 0, None, None, None, True)


def test_task_key_is_unique_within_a_job(db: Conn) -> None:
    job_id = new_job(db)
    new_task(db, job_id, task_key="invoice-1")
    new_task(db, new_job(db), task_key="invoice-1")  # same key, another job: fine
    rejected(
        db,
        "INSERT INTO tasks (id, job_id, task_key, payload, max_attempts) "
        "VALUES (%s, %s, 'invoice-1', '{}', 3)",
        (uuid.uuid4(), job_id),
        constraint="tasks_job_task_key_unique",
    )


def test_task_id_is_unique(db: Conn) -> None:
    job_id = new_job(db)
    task_id = new_task(db, job_id)
    rejected(
        db,
        "INSERT INTO tasks (id, job_id, task_key, payload, max_attempts) "
        "VALUES (%s, %s, 'other', '{}', 3)",
        (task_id, job_id),
        constraint="tasks_pkey",
    )


def test_task_requires_an_existing_job(db: Conn) -> None:
    rejected(
        db,
        "INSERT INTO tasks (id, job_id, task_key, payload, max_attempts) "
        "VALUES (%s, %s, 'k', '{}', 3)",
        (uuid.uuid4(), uuid.uuid4()),
        constraint="tasks_job_fk",
    )


def test_job_with_tasks_cannot_be_deleted(db: Conn) -> None:
    job_id = new_job(db)
    new_task(db, job_id)
    rejected(db, "DELETE FROM jobs WHERE id = %s", (job_id,), constraint="tasks_job_fk")


@pytest.mark.parametrize(
    ("assignment", "constraint"),
    [
        ("state = 'paused'", "tasks_state_valid"),
        ("max_attempts = 0", "tasks_max_attempts_range"),
        ("max_attempts = 101", "tasks_max_attempts_range"),
        ("task_key = ' '", "tasks_task_key_not_blank"),
        ("result = '{}'", "tasks_result_iff_succeeded"),
        ("finished_at = now()", "tasks_finished_iff_terminal"),
        ("state = 'failed'", "tasks_finished_iff_terminal"),
        ("state = 'running'", "tasks_attempt_count_fencing"),  # a claim must advance
        ("cancel_requested_at = now()", "tasks_cancel_request_not_queued"),
        ("state = 'cancelled', finished_at = now()", "tasks_cancelled_was_requested"),
        ("state = 'succeeded', finished_at = now()", "tasks_result_iff_succeeded"),
    ],
)
def test_queued_task_rejects_inconsistent_updates(
    db: Conn, assignment: str, constraint: str
) -> None:
    task_id = new_task(db, new_job(db))
    rejected(db, f"UPDATE tasks SET {assignment} WHERE id = %s", (task_id,), constraint=constraint)
    assert task_row(db, task_id) == ("queued", 0)


def test_terminal_task_states_with_their_required_fields_are_accepted(db: Conn) -> None:
    job_id = new_job(db)
    succeeded = new_task(db, job_id, task_key="a")
    claim(db, succeeded)
    db.execute(
        "UPDATE tasks SET state = 'succeeded', result = '{\"ok\": true}', finished_at = now() "
        "WHERE id = %s",
        (succeeded,),
    )
    cancelled = new_task(db, job_id, task_key="b")
    db.execute(
        "UPDATE tasks SET state = 'cancelled', cancel_requested_at = now(), finished_at = now() "
        "WHERE id = %s",
        (cancelled,),
    )
    assert task_row(db, succeeded) == ("succeeded", 1)
    assert task_row(db, cancelled) == ("cancelled", 0)


def test_running_task_may_carry_a_cancellation_request(db: Conn) -> None:
    task_id = new_task(db, new_job(db))
    claim(db, task_id)
    db.execute("UPDATE tasks SET cancel_requested_at = now() WHERE id = %s", (task_id,))
    rejected(
        db,
        "UPDATE tasks SET state = 'queued' WHERE id = %s",
        (task_id,),
        constraint="tasks_cancel_request_not_queued",
    )


def test_running_task_requires_a_started_attempt(db: Conn) -> None:
    rejected(
        db,
        "INSERT INTO tasks (id, job_id, task_key, payload, max_attempts, state) "
        "VALUES (%s, %s, 'k', '{}', 3, 'running')",
        (uuid.uuid4(), new_job(db)),
        constraint="tasks_running_has_attempt",
    )


# --- fencing: tasks.attempt_count -------------------------------------------


def test_new_task_must_start_at_generation_zero(db: Conn) -> None:
    error = rejected(
        db,
        "INSERT INTO tasks (id, job_id, task_key, payload, max_attempts, attempt_count) "
        "VALUES (%s, %s, 'k', '{}', 3, 1)",
        (uuid.uuid4(), new_job(db)),
        constraint="tasks_attempt_count_fencing",
    )
    assert isinstance(error, errors.CheckViolation)


def test_legal_claims_advance_the_generation_by_one(db: Conn) -> None:
    task_id = new_task(db, new_job(db), max_attempts=3)
    assert claim(db, task_id) == 1
    requeue(db, task_id)  # retry: generation is kept while queued
    assert task_row(db, task_id) == ("queued", 1)
    assert claim(db, task_id) == 2
    requeue(db, task_id)
    assert claim(db, task_id) == 3
    assert task_row(db, task_id) == ("running", 3)


@pytest.mark.parametrize(
    ("assignment", "message"),
    [
        ("attempt_count = attempt_count - 1", "only when a task is claimed"),
        ("attempt_count = 0", "only when a task is claimed"),
        ("attempt_count = attempt_count + 1", "only when a task is claimed"),
        ("state = 'queued', attempt_count = attempt_count + 1", "only when a task is claimed"),
    ],
    ids=["decrease", "reset", "increment-while-running", "increment-on-requeue"],
)
def test_running_task_generation_cannot_change(db: Conn, assignment: str, message: str) -> None:
    task_id = new_task(db, new_job(db), max_attempts=5)
    claim(db, task_id)
    error = rejected(
        db,
        f"UPDATE tasks SET {assignment} WHERE id = %s",
        (task_id,),
        constraint="tasks_attempt_count_fencing",
    )
    assert message in str(error)
    assert task_row(db, task_id) == ("running", 1)


@pytest.mark.parametrize(
    ("assignment", "message"),
    [
        ("state = 'running', attempt_count = attempt_count + 2", "exactly 1"),
        ("state = 'running'", "exactly 1"),
        ("attempt_count = attempt_count + 1", "only when a task is claimed"),
        ("attempt_count = attempt_count - 1", "only when a task is claimed"),
    ],
    ids=["skipped-generation", "claim-without-advance", "increment-while-queued", "decrease"],
)
def test_requeued_task_generation_changes_only_through_a_claim(
    db: Conn, assignment: str, message: str
) -> None:
    task_id = new_task(db, new_job(db), max_attempts=5)
    claim(db, task_id)
    requeue(db, task_id)
    error = rejected(
        db,
        f"UPDATE tasks SET {assignment} WHERE id = %s",
        (task_id,),
        constraint="tasks_attempt_count_fencing",
    )
    assert message in str(error)
    assert task_row(db, task_id) == ("queued", 1)


def test_terminal_task_generation_is_frozen(db: Conn) -> None:
    task_id = new_task(db, new_job(db))
    claim(db, task_id)
    db.execute("UPDATE tasks SET state = 'failed', finished_at = now() WHERE id = %s", (task_id,))
    rejected(
        db,
        "UPDATE tasks SET attempt_count = attempt_count + 1 WHERE id = %s",
        (task_id,),
        constraint="tasks_attempt_count_fencing",
    )


def test_exhausted_task_cannot_be_requeued_and_must_fail(db: Conn) -> None:
    task_id = new_task(db, new_job(db), max_attempts=2)
    claim(db, task_id)
    requeue(db, task_id)
    claim(db, task_id)
    rejected(
        db,
        "UPDATE tasks SET state = 'queued' WHERE id = %s",
        (task_id,),
        constraint="tasks_queued_has_attempts_left",
    )
    db.execute("UPDATE tasks SET state = 'failed', finished_at = now() WHERE id = %s", (task_id,))
    assert task_row(db, task_id) == ("failed", 2)


def test_retry_limit_cannot_be_lowered_below_attempts_started(db: Conn) -> None:
    task_id = new_task(db, new_job(db), max_attempts=3)
    claim(db, task_id)
    requeue(db, task_id)
    claim(db, task_id)
    rejected(
        db,
        "UPDATE tasks SET max_attempts = 1 WHERE id = %s",
        (task_id,),
        constraint="tasks_attempt_count_range",
    )
    requeue(db, task_id)
    rejected(
        db,
        "UPDATE tasks SET max_attempts = 2 WHERE id = %s",
        (task_id,),
        constraint="tasks_queued_has_attempts_left",
    )


# --- attempts ----------------------------------------------------------------


@pytest.fixture
def running_task(db: Conn) -> uuid.UUID:
    task_id = new_task(db, new_job(db), max_attempts=5)
    claim(db, task_id)
    return task_id


def test_running_attempt_defaults(db: Conn, running_task: uuid.UUID) -> None:
    attempt_id = new_attempt(db, running_task, 1)
    row = db.execute(
        "SELECT state, failure_kind, finished_at, started_at IS NOT NULL FROM attempts "
        "WHERE id = %s",
        (attempt_id,),
    ).fetchone()
    assert row == ("running", None, None, True)


def test_at_most_one_running_attempt_per_task(db: Conn, running_task: uuid.UUID) -> None:
    new_attempt(db, running_task, 1)
    rejected(
        db,
        f"INSERT INTO attempts (id, task_id, attempt_number, worker_id, {LEASE_COLUMNS}) "
        f"VALUES (%s, %s, 2, 'w2', {LEASE_VALUES})",
        (uuid.uuid4(), running_task),
        constraint="attempts_one_running_per_task",
    )


def test_at_most_one_succeeded_attempt_per_task(db: Conn, running_task: uuid.UUID) -> None:
    new_attempt(db, running_task, 1, state="succeeded", finished_at="now")
    rejected(
        db,
        "INSERT INTO attempts (id, task_id, attempt_number, worker_id, state, finished_at) "
        "VALUES (%s, %s, 2, 'w2', 'succeeded', now())",
        (uuid.uuid4(), running_task),
        constraint="attempts_one_succeeded_per_task",
    )


def test_attempt_history_may_hold_many_finished_attempts(db: Conn, running_task: uuid.UUID) -> None:
    new_attempt(
        db,
        running_task,
        1,
        state="failed",
        failure_kind="transient",
        error_type="TimeoutError",
        finished_at="now",
    )
    new_attempt(db, running_task, 2, state="cancelled", finished_at="now")
    new_attempt(db, running_task, 3, state="succeeded", finished_at="now")
    new_attempt(db, running_task, 4)
    row = db.execute("SELECT count(*) FROM attempts WHERE task_id = %s", (running_task,)).fetchone()
    assert row == (4,)


def test_attempt_number_is_unique_per_task(db: Conn, running_task: uuid.UUID) -> None:
    new_attempt(db, running_task, 1, state="cancelled", finished_at="now")
    rejected(
        db,
        f"INSERT INTO attempts (id, task_id, attempt_number, worker_id, {LEASE_COLUMNS}) "
        f"VALUES (%s, %s, 1, 'w', {LEASE_VALUES})",
        (uuid.uuid4(), running_task),
        constraint="attempts_task_number_unique",
    )


def test_attempt_requires_an_existing_task(db: Conn) -> None:
    rejected(
        db,
        f"INSERT INTO attempts (id, task_id, attempt_number, worker_id, {LEASE_COLUMNS}) "
        f"VALUES (%s, %s, 1, 'w', {LEASE_VALUES})",
        (uuid.uuid4(), uuid.uuid4()),
        constraint="attempts_task_fk",
    )


def test_task_with_attempts_cannot_be_deleted(db: Conn, running_task: uuid.UUID) -> None:
    new_attempt(db, running_task, 1)
    rejected(db, "DELETE FROM tasks WHERE id = %s", (running_task,), constraint="attempts_task_fk")


@pytest.mark.parametrize(
    ("columns", "constraint"),
    [
        ("attempt_number = 0", "attempts_number_range"),
        ("attempt_number = 101", "attempts_number_range"),
        ("worker_id = ''", "attempts_worker_id_not_blank"),
        ("state = 'expired', finished_at = now()", "attempts_state_valid"),
        ("finished_at = now()", "attempts_finished_iff_terminal"),
        ("state = 'succeeded'", "attempts_finished_iff_terminal"),
        (
            "state = 'failed', error_type = 'E', finished_at = now()",
            "attempts_failure_kind_iff_failed",
        ),
        ("failure_kind = 'transient'", "attempts_failure_kind_iff_failed"),
        (
            "state = 'failed', failure_kind = 'transient', finished_at = now()",
            "attempts_failed_has_error_type",
        ),
        (
            "state = 'failed', failure_kind = 'flaky', error_type = 'E', finished_at = now()",
            "attempts_failure_kind_valid",
        ),
        (
            "state = 'succeeded', error_message = 'x', finished_at = now()",
            "attempts_error_details_only_when_failed",
        ),
    ],
)
def test_attempt_rejects_inconsistent_records(
    db: Conn, running_task: uuid.UUID, columns: str, constraint: str
) -> None:
    attempt_id = new_attempt(db, running_task, 1)
    rejected(
        db, f"UPDATE attempts SET {columns} WHERE id = %s", (attempt_id,), constraint=constraint
    )


@pytest.mark.parametrize("kind", [kind.value for kind in FailureKind])
def test_failed_attempt_records_its_classification(
    db: Conn, running_task: uuid.UUID, kind: str
) -> None:
    attempt_id = new_attempt(db, running_task, 1)
    db.execute(
        "UPDATE attempts SET state = 'failed', failure_kind = %s, error_type = 'RuntimeError', "
        "error_message = 'boom', finished_at = now() WHERE id = %s",
        (kind, attempt_id),
    )
    row = db.execute("SELECT state, failure_kind FROM attempts WHERE id = %s", (attempt_id,))
    assert row.fetchone() == ("failed", kind)


# --- attempt leases and revocation (migration 0002) ---------------------------


def test_finished_attempts_need_no_lease_but_running_ones_do(
    db: Conn, running_task: uuid.UUID
) -> None:
    # History from before 0002 has no lease; every running attempt must have one.
    new_attempt(
        db,
        running_task,
        1,
        state="succeeded",
        finished_at="now",
        lease_expires_at=None,
        deadline_at=None,
    )
    rejected(
        db,
        "INSERT INTO attempts (id, task_id, attempt_number, worker_id) VALUES (%s, %s, 2, 'w')",
        (uuid.uuid4(), running_task),
        constraint="attempts_lease_required",
    )


@pytest.mark.parametrize(
    ("lease", "deadline", "constraint"),
    [
        ("now() + interval '5 minutes'", "NULL", "attempts_lease_iff_deadline"),
        ("NULL", "now() + interval '1 hour'", "attempts_lease_iff_deadline"),
        ("now() - interval '1 second'", "now() + interval '1 hour'", "attempts_lease_after_start"),
        ("now()", "now() + interval '1 hour'", "attempts_lease_after_start"),
        (
            "now() + interval '2 hours'",
            "now() + interval '1 hour'",
            "attempts_lease_within_deadline",
        ),
    ],
    ids=["no-deadline", "no-lease", "lease-before-start", "lease-at-start", "lease-past-deadline"],
)
def test_a_lease_must_lie_between_start_and_deadline(
    db: Conn, running_task: uuid.UUID, lease: str, deadline: str, constraint: str
) -> None:
    rejected(
        db,
        "INSERT INTO attempts (id, task_id, attempt_number, worker_id, lease_expires_at, "
        f"deadline_at) VALUES (%s, %s, 1, 'w', {lease}, {deadline})",
        (uuid.uuid4(), running_task),
        constraint=constraint,
    )


def test_a_lease_may_reach_but_not_pass_its_deadline(db: Conn, running_task: uuid.UUID) -> None:
    attempt_id = new_attempt(db, running_task, 1)
    db.execute("UPDATE attempts SET lease_expires_at = deadline_at WHERE id = %s", (attempt_id,))
    rejected(
        db,
        "UPDATE attempts SET lease_expires_at = deadline_at + interval '1 microsecond' "
        "WHERE id = %s",
        (attempt_id,),
        constraint="attempts_lease_within_deadline",
    )


@pytest.mark.parametrize(
    "assignment",
    [
        "deadline_at = deadline_at + interval '1 second'",
        "deadline_at = deadline_at - interval '1 second'",
        "deadline_at = NULL, lease_expires_at = NULL, state = 'succeeded', finished_at = now()",
    ],
    ids=["extend", "shorten", "erase"],
)
def test_an_attempt_deadline_never_changes(
    db: Conn, running_task: uuid.UUID, assignment: str
) -> None:
    attempt_id = new_attempt(db, running_task, 1)
    error = rejected(
        db,
        f"UPDATE attempts SET {assignment} WHERE id = %s",
        (attempt_id,),
        constraint="attempts_deadline_immutable",
    )
    assert "deadline_at cannot change" in str(error)
    # Updates that leave the deadline alone are not affected by the guard.
    db.execute(
        "UPDATE attempts SET deadline_at = deadline_at, lease_expires_at = deadline_at "
        "WHERE id = %s",
        (attempt_id,),
    )


@pytest.mark.parametrize("reason", [reason.value for reason in RevocationReason])
def test_a_revoked_attempt_records_a_reason_matching_its_lease(
    db: Conn, running_task: uuid.UUID, reason: str
) -> None:
    attempt_id = new_attempt(db, running_task, 1)
    if reason == RevocationReason.DEADLINE_EXCEEDED:
        db.execute(
            "UPDATE attempts SET lease_expires_at = deadline_at WHERE id = %s", (attempt_id,)
        )
    db.execute(
        "UPDATE attempts SET state = 'revoked', revoked_reason = %s, finished_at = now() "
        "WHERE id = %s",
        (reason, attempt_id),
    )
    row = db.execute(
        "SELECT state, revoked_reason, failure_kind, error_type FROM attempts WHERE id = %s",
        (attempt_id,),
    ).fetchone()
    assert row == ("revoked", reason, None, None)


@pytest.mark.parametrize(
    ("columns", "constraint"),
    [
        (
            "state = 'revoked', revoked_reason = 'worker_crashed', finished_at = now()",
            "attempts_revoked_reason_valid",
        ),
        ("state = 'revoked', finished_at = now()", "attempts_revoked_reason_iff_revoked"),
        ("revoked_reason = 'lease_expired'", "attempts_revoked_reason_iff_revoked"),
        (
            "state = 'revoked', revoked_reason = 'deadline_exceeded', finished_at = now()",
            "attempts_revoked_reason_matches_lease",
        ),
        (
            "state = 'revoked', revoked_reason = 'lease_expired', finished_at = now(), "
            "lease_expires_at = deadline_at",
            "attempts_revoked_reason_matches_lease",
        ),
        (
            "state = 'revoked', revoked_reason = 'lease_expired', finished_at = now(), "
            "failure_kind = 'transient'",
            "attempts_failure_kind_iff_failed",
        ),
        ("state = 'revoked', revoked_reason = 'lease_expired'", "attempts_finished_iff_terminal"),
    ],
    ids=[
        "unknown-reason",
        "revoked-without-reason",
        "reason-without-revocation",
        "deadline-reason-before-deadline",
        "lease-reason-at-deadline",
        "revoked-as-failure",
        "revoked-without-finish",
    ],
)
def test_a_revoked_attempt_rejects_inconsistent_records(
    db: Conn, running_task: uuid.UUID, columns: str, constraint: str
) -> None:
    attempt_id = new_attempt(db, running_task, 1)
    rejected(
        db, f"UPDATE attempts SET {columns} WHERE id = %s", (attempt_id,), constraint=constraint
    )


def test_a_revoked_attempt_must_keep_its_lease(db: Conn, running_task: uuid.UUID) -> None:
    rejected(
        db,
        "INSERT INTO attempts (id, task_id, attempt_number, worker_id, state, revoked_reason, "
        "finished_at) VALUES (%s, %s, 1, 'w', 'revoked', 'lease_expired', now())",
        (uuid.uuid4(), running_task),
        constraint="attempts_lease_required",
    )
