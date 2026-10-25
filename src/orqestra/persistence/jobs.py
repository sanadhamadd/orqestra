"""Transactional job submission and job and task reads.

``submit_job`` inserts a job and all of its tasks in one short READ COMMITTED
transaction that it owns: either everything is stored or nothing is. It
refuses to run inside a transaction the caller already opened, because its
atomicity and idempotency depend on committing on its own. A closed or broken
connection is left to psycopg, which reports it as a connection error.

Idempotency (DEC-025): ``jobs.idempotency_key`` is unique. The job insert uses
``ON CONFLICT ... DO NOTHING``, so a concurrent submission with the same key
waits for the first to commit or roll back instead of creating a second job.
When the key is taken, the stored job and tasks are compared with the request
in SQL, using PostgreSQL ``jsonb`` equality for payloads: an identical request
returns the existing job unchanged, a different one raises
``IdempotencyConflictError``. Submission columns are never updated, so the
stored rows are the original request.

Reads are single statements. On a connection that is not in autocommit mode,
psycopg opens an implicit transaction for them that the caller must end; until
then ``submit_job`` refuses that connection.

Every function reads rows through its own tuple-row cursor, so the
connection's row factory does not matter.
"""

import logging
import uuid
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow, tuple_row
from psycopg.types.json import Jsonb

from orqestra.domain.errors import OrqestraError
from orqestra.domain.jobs import Job, JobSubmission, Task
from orqestra.domain.states import TaskState

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

# Statuses in which the caller owns an open transaction (or a command is still
# running). UNKNOWN means the connection is closed or broken.
_CALLER_TRANSACTION = frozenset(
    {TransactionStatus.ACTIVE, TransactionStatus.INTRANS, TransactionStatus.INERROR}
)

_INSERT_JOB = """
INSERT INTO jobs (id, kind, idempotency_key) VALUES (%s, %s, %s)
ON CONFLICT ON CONSTRAINT jobs_idempotency_key_unique DO NOTHING
RETURNING created_at
"""

_INSERT_TASKS = """
INSERT INTO tasks (id, job_id, task_key, payload, max_attempts)
SELECT t.id, %s, t.task_key, t.payload, t.max_attempts
FROM unnest(%s::uuid[], %s::text[], %s::jsonb[], %s::integer[])
    AS t (id, task_key, payload, max_attempts)
"""

# The existing job for a key, and whether its stored content equals the
# request. Task keys are unique on both sides, so equal counts plus a match for
# every requested key make the two task sets identical.
_FIND_BY_KEY = """
SELECT j.id, j.kind, j.idempotency_key, j.created_at,
       j.kind = %(kind)s
       AND (SELECT count(*) FROM tasks WHERE job_id = j.id) = %(task_count)s
       AND NOT EXISTS (
           SELECT FROM unnest(%(task_keys)s::text[], %(payloads)s::jsonb[],
                              %(max_attempts)s::integer[]) AS r (task_key, payload, max_attempts)
           LEFT JOIN tasks t ON t.job_id = j.id AND t.task_key = r.task_key
           WHERE t.id IS NULL OR t.payload <> r.payload OR t.max_attempts <> r.max_attempts
       )
FROM jobs j
WHERE j.idempotency_key = %(key)s
"""

_JOB_COLUMNS = "id, kind, idempotency_key, created_at"
_TASK_COLUMNS = (
    "id, job_id, task_key, state, payload, result, max_attempts, attempt_count, "
    "cancel_requested_at, created_at, updated_at, finished_at"
)


class IdempotencyConflictError(OrqestraError):
    """An idempotency key was reused for a submission with different content."""

    def __init__(self, idempotency_key: str, job_id: uuid.UUID) -> None:
        super().__init__(f"idempotency key is already used by job {job_id} with different content")
        self.idempotency_key = idempotency_key
        self.job_id = job_id


@dataclass(frozen=True, slots=True)
class SubmissionResult:
    """The submitted job; ``created`` is false when an earlier identical one was returned."""

    job: Job
    created: bool


def submit_job(conn: Conn, submission: JobSubmission) -> SubmissionResult:
    """Store a job and all of its tasks atomically, or return the identical earlier job.

    Raises ``IdempotencyConflictError`` when the key belongs to a job with
    different content, and ``RuntimeError`` when ``conn`` is already inside a
    transaction. Database and connection errors, including the schema's named constraint
    violations, propagate after the whole submission has been rolled back.
    """
    status = conn.info.transaction_status
    if status in _CALLER_TRANSACTION:
        msg = (
            "submit_job runs its own transaction; call it outside any open transaction "
            f"(connection status: {status.name})"
        )
        raise RuntimeError(msg)
    job_id = uuid.uuid4()
    key = submission.idempotency_key
    task_keys = [task.task_key for task in submission.tasks]
    payloads = [Jsonb(task.payload) for task in submission.tasks]
    max_attempts = [task.max_attempts for task in submission.tasks]
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        inserted = cur.execute(_INSERT_JOB, (job_id, submission.kind, key)).fetchone()
        if inserted is not None:
            task_ids = [uuid.uuid4() for _ in task_keys]
            cur.execute(_INSERT_TASKS, (job_id, task_ids, task_keys, payloads, max_attempts))
            result = SubmissionResult(Job(job_id, submission.kind, key, inserted[0]), True)
        else:
            row = cur.execute(
                _FIND_BY_KEY,
                {
                    "key": key,
                    "kind": submission.kind,
                    "task_count": len(task_keys),
                    "task_keys": task_keys,
                    "payloads": payloads,
                    "max_attempts": max_attempts,
                },
            ).fetchone()
            # A conflict means a committed job holds the key, and jobs with
            # tasks cannot be deleted, so READ COMMITTED always sees it here.
            if row is None or key is None:  # pragma: no cover - see above
                msg = "idempotency key conflict reported, but no job holds the key"
                raise RuntimeError(msg)
            *columns, same_content = row
            existing = Job(*columns)
            if not same_content:
                raise IdempotencyConflictError(key, existing.id)
            result = SubmissionResult(existing, False)
    logger.info(
        "job submitted" if result.created else "job submission replayed",
        extra={"job_id": str(result.job.id)},
    )
    return result


def get_job(conn: Conn, job_id: uuid.UUID) -> Job | None:
    """Return the job with ``job_id``, or ``None``."""
    with conn.cursor(row_factory=tuple_row) as cur:
        row = cur.execute(f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s", (job_id,)).fetchone()
    return None if row is None else Job(*row)


def get_task(conn: Conn, task_id: uuid.UUID) -> Task | None:
    """Return the task with ``task_id``, or ``None``."""
    with conn.cursor(row_factory=tuple_row) as cur:
        row = cur.execute(f"SELECT {_TASK_COLUMNS} FROM tasks WHERE id = %s", (task_id,)).fetchone()
    return None if row is None else _task(row)


def list_tasks(conn: Conn, job_id: uuid.UUID) -> tuple[Task, ...]:
    """Return a job's tasks ordered by ``task_key``; empty if the job does not exist.

    Keys are compared by code point (``COLLATE "C"``), the same order as
    Python's ``sorted``, so the order does not depend on the database locale.
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        rows = cur.execute(
            f'SELECT {_TASK_COLUMNS} FROM tasks WHERE job_id = %s ORDER BY task_key COLLATE "C"',
            (job_id,),
        ).fetchall()
    return tuple(_task(row) for row in rows)


def _task(row: TupleRow) -> Task:
    task_id, job_id, task_key, state, *rest = row
    return Task(task_id, job_id, task_key, TaskState(state), *rest)
