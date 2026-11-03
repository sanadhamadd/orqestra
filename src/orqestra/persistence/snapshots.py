"""A consistent, bounded read of one job's batch tasks.

``job_batch_snapshot`` owns one ``REPEATABLE READ, READ ONLY`` transaction. Its
first statement is ``SET TRANSACTION``; the job row is read next, and the tasks
are streamed on request through a named server-side cursor. Every statement in
the block sees the database as of the job-row read, so a completion committed
meanwhile is not visible, and the block takes no row locks and does not block
workers. Like ``owned_transaction`` it refuses a connection that is already in a
transaction, and it always ends the transaction: it commits when the block exits
normally and rolls back when it raises, whether or not the stream was read to its
end.

The projection is bounded and cannot raise a database error whatever a payload
holds (a task payload is any JSON value). Every payload expression sits behind a
``jsonb_typeof`` check and nothing is cast. It returns the JSON type of the
payload and, for an object only, its key count, whether ``offset`` and ``items``
are present, the text of a numeric ``offset`` and the length of an array
``items``. It never returns the payload, its items or any other key. ``result`` is
returned only for a succeeded task, the only state in which one exists
(``tasks_result_iff_succeeded``).

Rows are ordered by ``task_key`` by code point, so the order does not depend on
the database locale. The order only makes a streamed read repeatable; callers
must not rely on it for correctness. The stream is the first named cursor in
this package: it holds the snapshot, and with it the vacuum horizon, for as long
as the block is open.

``observed_at`` is the ``statement_timestamp()`` of the job-row read, which is
the snapshot's first query: approximate, not the snapshot instant.
"""

import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime

import psycopg
from psycopg.rows import TupleRow, tuple_row

from orqestra.domain.jobs import Job, JsonValue
from orqestra.domain.states import TaskState
from orqestra.persistence._shared import _CALLER_TRANSACTION, Conn, tuple_cursor

DEFAULT_FETCH_SIZE = 500

_SET_TRANSACTION = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"

_READ_JOB = """
SELECT id, kind, idempotency_key, created_at, statement_timestamp()
FROM jobs
WHERE id = %s
"""

_STREAM_TASKS = """
SELECT
    t.id,
    t.task_key,
    t.state,
    jsonb_typeof(t.payload),
    CASE WHEN jsonb_typeof(t.payload) = 'object' THEN
        (SELECT count(*) FROM jsonb_object_keys(t.payload))
    END,
    CASE WHEN jsonb_typeof(t.payload) = 'object' THEN t.payload ? 'offset' END,
    CASE WHEN jsonb_typeof(t.payload) = 'object' THEN t.payload ? 'items' END,
    CASE WHEN jsonb_typeof(t.payload) = 'object' THEN
        CASE WHEN jsonb_typeof(t.payload -> 'offset') = 'number'
            THEN (t.payload -> 'offset') #>> '{}'
        END
    END,
    CASE WHEN jsonb_typeof(t.payload) = 'object' THEN
        CASE WHEN jsonb_typeof(t.payload -> 'items') = 'array'
            THEN jsonb_array_length(t.payload -> 'items')
        END
    END,
    CASE WHEN t.state = 'succeeded' THEN t.result END
FROM tasks t
WHERE t.job_id = %s
ORDER BY t.task_key COLLATE "C"
"""


@dataclass(frozen=True, slots=True)
class BatchTaskRow:
    """The bounded projection of one task; see the module docstring for what is set when."""

    task_id: uuid.UUID
    task_key: str
    state: TaskState
    payload_type: str
    key_count: int | None
    has_offset: bool | None
    has_items: bool | None
    offset_text: str | None
    items_length: int | None
    result: JsonValue


class JobBatchSnapshot:
    """The job row read at the start of the snapshot and, on request, its task stream."""

    def __init__(self, conn: Conn, job: Job, observed_at: datetime, fetch_size: int) -> None:
        self.job = job
        self.observed_at = observed_at
        self._conn = conn
        self._fetch_size = fetch_size
        self._cursor: psycopg.ServerCursor[TupleRow] | None = None
        self._closed = False

    def tasks(self) -> Iterator[BatchTaskRow]:
        """Open the job's task stream now and return its rows, fetched in chunks.

        It can be opened once, and only while the snapshot block is open. The rows
        describe the snapshot, whatever has been committed since.
        """
        if self._closed:
            msg = "the snapshot has ended"
            raise RuntimeError(msg)
        if self._cursor is not None:
            msg = "the task stream was already opened"
            raise RuntimeError(msg)
        name = f"orqestra_tasks_{uuid.uuid4().hex}"
        cursor = self._conn.cursor(name=name, row_factory=tuple_row)
        self._cursor = cursor
        cursor.itersize = self._fetch_size
        cursor.execute(_STREAM_TASKS, (self.job.id,))
        return _rows(cursor)

    def _close(self) -> None:
        """End the stream before its transaction does, without masking the error that ended it."""
        self._closed = True
        if self._cursor is not None:
            with suppress(psycopg.Error):
                self._cursor.close()


@contextmanager
def job_batch_snapshot(
    conn: Conn, job_id: uuid.UUID, *, fetch_size: int = DEFAULT_FETCH_SIZE
) -> Iterator[JobBatchSnapshot | None]:
    """Run one read-only snapshot of a job; yield ``None`` if the job does not exist.

    ``fetch_size`` is the number of rows fetched from the server at a time, a
    positive integer; it never changes what the rows say. Raises ``RuntimeError``
    if the caller already has a transaction open on ``conn``. Database and
    connection errors propagate after the transaction has been ended.
    """
    if isinstance(fetch_size, bool) or fetch_size < 1:
        msg = f"fetch_size must be a positive integer, got {fetch_size!r}"
        raise ValueError(msg)
    status = conn.info.transaction_status
    if status in _CALLER_TRANSACTION:
        msg = (
            "job_batch_snapshot runs its own transaction; call it outside any open "
            f"transaction (connection status: {status.name})"
        )
        raise RuntimeError(msg)
    with conn.transaction(), tuple_cursor(conn) as cur:
        cur.execute(_SET_TRANSACTION)
        row = cur.execute(_READ_JOB, (job_id,)).fetchone()
        if row is None:
            yield None
            return
        *columns, observed_at = row
        snapshot = JobBatchSnapshot(conn, Job(*columns), observed_at, fetch_size)
        try:
            yield snapshot
        finally:
            snapshot._close()


def _rows(cursor: psycopg.ServerCursor[TupleRow]) -> Iterator[BatchTaskRow]:
    for row in cursor:
        task_id, task_key, state, payload_type, *rest = row
        yield BatchTaskRow(task_id, task_key, TaskState(state), payload_type, *rest)
