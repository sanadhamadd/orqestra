"""Lifecycle writes as code before migration 0004 made them, for the 0002 and 0003 migration tests.

Test-only. Since 0004 (DEC-053) the claim, completion and recovery read or write
``tasks.available_at``, so the current code cannot run against an older
schema. These helpers reproduce what those three writes did there, with the
same statements in the same lock order (task before attempt), so the
historical upgrade tests keep exercising genuine 0002 and 0003 schemas.
Operations 0004 did not change (submission, listing, cancellation requests,
heartbeats, reads) are still the current code. The decisions themselves come
from the production pure functions; only the SQL is reproduced here.
"""

import uuid
from collections.abc import Collection

import psycopg
from psycopg.rows import TupleRow

from orqestra.domain.jobs import Attempt
from orqestra.domain.leases import LeasePolicy, RevocationReason, initial_lease, revocation_reason
from orqestra.domain.policy import next_task_state
from orqestra.domain.states import TERMINAL_TASK_STATES, AttemptState, TaskState
from orqestra.persistence._shared import (
    ATTEMPT_COLUMNS,
    TASK_COLUMNS,
    attempt_from_row,
    task_from_row,
)

Conn = psycopg.Connection[TupleRow]

_LOCK_TASK = f"SELECT {TASK_COLUMNS} FROM tasks WHERE id = %s FOR UPDATE"
_LOCK_NEXT_QUEUED = (
    f"SELECT {TASK_COLUMNS} FROM tasks WHERE state = 'queued' "
    "AND EXISTS (SELECT FROM jobs WHERE jobs.id = tasks.job_id AND jobs.kind = ANY(%s)) "
    "ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
)
_CLAIM_TASK = (
    "UPDATE tasks SET state = 'running', attempt_count = attempt_count + 1, "
    "updated_at = statement_timestamp() WHERE id = %s RETURNING attempt_count, updated_at"
)
_INSERT_ATTEMPT = (
    "INSERT INTO attempts "
    "(id, task_id, attempt_number, worker_id, started_at, lease_expires_at, deadline_at) "
    f"VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING {ATTEMPT_COLUMNS}"
)
_LOCK_NEXT_EXPIRED = (
    f"SELECT {', '.join(f't.{c}' for c in TASK_COLUMNS.split(', '))}, a.id "
    "FROM tasks t JOIN attempts a ON a.task_id = t.id AND a.attempt_number = t.attempt_count "
    "WHERE t.state = 'running' AND a.state = 'running' "
    "AND a.lease_expires_at <= statement_timestamp() "
    "ORDER BY a.lease_expires_at, a.id LIMIT 1 FOR UPDATE OF t SKIP LOCKED"
)
_LOCK_ATTEMPT = f"SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE id = %s FOR UPDATE"


def _start_locked(conn: Conn, task_id: uuid.UUID, worker_id: str, policy: LeasePolicy) -> Attempt:
    row = conn.execute(_CLAIM_TASK, (task_id,)).fetchone()
    assert row is not None
    generation, claimed_at = row
    lease_expires_at, deadline_at = initial_lease(policy, claimed_at)
    attempt = conn.execute(
        _INSERT_ATTEMPT,
        (uuid.uuid4(), task_id, generation, worker_id, claimed_at, lease_expires_at, deadline_at),
    ).fetchone()
    assert attempt is not None
    return attempt_from_row(attempt)


def start(conn: Conn, task_id: uuid.UUID, worker_id: str, *, policy: LeasePolicy) -> Attempt:
    """``start_attempt`` before 0004: lock the queued task, then claim it."""
    with conn.transaction():
        row = conn.execute(_LOCK_TASK, (task_id,)).fetchone()
        assert row is not None
        assert task_from_row(row).state is TaskState.QUEUED
        return _start_locked(conn, task_id, worker_id, policy)


def claim(
    conn: Conn, worker_id: str, *, policy: LeasePolicy, kinds: Collection[str]
) -> Attempt | None:
    """``claim_next_attempt`` before 0004: the first queued task of these kinds, not locked."""
    with conn.transaction():
        row = conn.execute(_LOCK_NEXT_QUEUED, (list(kinds),)).fetchone()
        if row is None:
            return None
        return _start_locked(conn, task_from_row(row).id, worker_id, policy)


def succeed(conn: Conn, attempt: Attempt) -> TaskState:
    """``complete_attempt`` with a success before 0004: lock the task, then the attempt."""
    with conn.transaction():
        row = conn.execute(
            f"SELECT {TASK_COLUMNS} FROM tasks WHERE id = %s FOR UPDATE", (attempt.task_id,)
        ).fetchone()
        assert row is not None
        conn.execute(_LOCK_ATTEMPT, (attempt.id,))
        conn.execute(
            "UPDATE attempts SET state = 'succeeded', finished_at = now() WHERE id = %s",
            (attempt.id,),
        )
        conn.execute(
            "UPDATE tasks SET state = 'succeeded', result = '{}'::jsonb, updated_at = now(), "
            "finished_at = now() WHERE id = %s",
            (attempt.task_id,),
        )
    return TaskState.SUCCEEDED


def revoke(conn: Conn) -> tuple[RevocationReason, TaskState] | None:
    """``revoke_next_expired`` before 0004: the reason, and the state its task moved to."""
    with conn.transaction():
        row = conn.execute(_LOCK_NEXT_EXPIRED).fetchone()
        if row is None:
            return None
        *task_columns, attempt_id = row
        task = task_from_row(tuple(task_columns))
        locked = conn.execute(_LOCK_ATTEMPT, (attempt_id,)).fetchone()
        assert locked is not None
        attempt = attempt_from_row(locked)
        now_row = conn.execute("SELECT statement_timestamp()").fetchone()
        assert now_row is not None
        (now,) = now_row
        assert attempt.lease_expires_at is not None
        assert attempt.deadline_at is not None
        reason = revocation_reason(
            lease_expires_at=attempt.lease_expires_at,
            deadline_at=attempt.deadline_at,
            cancel_requested_at=task.cancel_requested_at,
            now=now,
        )
        assert reason is not None
        next_state = next_task_state(
            AttemptState.REVOKED,
            None,
            attempt_count=task.attempt_count,
            max_attempts=task.max_attempts,
            cancel_requested=task.cancel_requested_at is not None,
            revocation_reason=reason,
        )
        conn.execute(
            "UPDATE attempts SET state = 'revoked', revoked_reason = %s, finished_at = %s "
            "WHERE id = %s",
            (reason.value, now, attempt_id),
        )
        conn.execute(
            "UPDATE tasks SET state = %s, updated_at = %s, finished_at = %s WHERE id = %s",
            (next_state.value, now, now if next_state in TERMINAL_TASK_STATES else None, task.id),
        )
    return reason, next_state
