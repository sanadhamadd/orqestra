"""Transaction ownership and row mapping shared by the persistence operations.

Every write operation runs one short READ COMMITTED transaction that it owns
(DEC-024, DEC-028) and reads rows through its own tuple-row cursor, so the
caller's isolation settings and row factory do not matter.

``NotFoundError`` lives here so that ``lifecycle`` and ``provider_usage`` can
both raise it without importing each other; ``lifecycle`` re-exports it.
"""

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow, tuple_row

from orqestra.domain.errors import OrqestraError
from orqestra.domain.jobs import Attempt, Task
from orqestra.domain.leases import RevocationReason
from orqestra.domain.policy import FailureKind
from orqestra.domain.states import AttemptState, TaskState

Conn = psycopg.Connection[Any]
Cursor = psycopg.Cursor[TupleRow]

# Statuses in which the caller owns an open transaction (or a command is still
# running). UNKNOWN means the connection is closed or broken; psycopg reports that.
_CALLER_TRANSACTION = frozenset(
    {TransactionStatus.ACTIVE, TransactionStatus.INTRANS, TransactionStatus.INERROR}
)

TASK_COLUMNS = (
    "id, job_id, task_key, state, payload, result, max_attempts, attempt_count, "
    "cancel_requested_at, created_at, updated_at, finished_at"
)
ATTEMPT_COLUMNS = (
    "id, task_id, attempt_number, worker_id, state, failure_kind, error_type, error_message, "
    "started_at, finished_at, lease_expires_at, deadline_at, revoked_reason"
)


class NotFoundError(OrqestraError, LookupError):
    """The task or attempt a write refers to does not exist."""

    def __init__(self, kind: str, record_id: uuid.UUID) -> None:
        super().__init__(f"{kind} {record_id} does not exist")
        self.kind = kind
        self.record_id = record_id


@contextmanager
def owned_transaction(conn: Conn, operation: str) -> Iterator[Cursor]:
    """Run one READ COMMITTED transaction that commits on exit and rolls back on error.

    Raises ``RuntimeError`` if the caller already has a transaction open on
    ``conn``: the operation's atomicity depends on committing on its own.
    """
    status = conn.info.transaction_status
    if status in _CALLER_TRANSACTION:
        msg = (
            f"{operation} runs its own transaction; call it outside any open transaction "
            f"(connection status: {status.name})"
        )
        raise RuntimeError(msg)
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        yield cur


def tuple_cursor(conn: Conn) -> Cursor:
    """A cursor that returns tuples whatever the connection's row factory."""
    return conn.cursor(row_factory=tuple_row)


def task_from_row(row: TupleRow) -> Task:
    task_id, job_id, task_key, state, *rest = row
    return Task(task_id, job_id, task_key, TaskState(state), *rest)


def attempt_from_row(row: TupleRow) -> Attempt:
    (
        attempt_id,
        task_id,
        number,
        worker_id,
        state,
        failure_kind,
        error_type,
        error_message,
        started_at,
        finished_at,
        lease_expires_at,
        deadline_at,
        revoked_reason,
    ) = row
    return Attempt(
        attempt_id,
        task_id,
        number,
        worker_id,
        AttemptState(state),
        None if failure_kind is None else FailureKind(failure_kind),
        error_type,
        error_message,
        started_at,
        finished_at,
        lease_expires_at,
        deadline_at,
        None if revoked_reason is None else RevocationReason(revoked_reason),
    )
