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
must not rely on it for correctness. Each stream (``tasks`` and
``attempt_usage``) is a named server-side cursor; while the block is open they
hold the snapshot, and with it the vacuum horizon.

``observed_at`` is the ``statement_timestamp()`` of the job-row read, which is
the snapshot's first query: approximate, not the snapshot instant.

The same snapshot also reads the job's provider usage ledger (migration 0005),
for a usage report:

- ``usage_totals`` returns the reported amounts summed per unit, for the whole
  job and per current attempt state. The sums are computed by PostgreSQL in
  ``numeric``, which is exact: Python never adds amounts, so its default
  28-digit ``Decimal`` context cannot round them. A unit is grouped by its
  exact stored text (``COLLATE "C"``), never normalized or converted;
- ``attempt_usage`` streams one row per attempt of the job, through a second
  named cursor, with that attempt's call counts and its closure. Rows are
  ordered by ``task_key`` by code point, then ``attempt_number``.

``max_attempts_total`` sums the job's task retry limits, for a call budget
(``orqestra.aggregation.budget``).

None of them returns a provider's text, a model name or any other ledger content.
The ledger is filtered on ``provider_calls.job_id``, copied from the attempt
when each intent was recorded.
"""

import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import psycopg
from psycopg.rows import TupleRow, tuple_row

from orqestra.domain.jobs import Job, JsonValue
from orqestra.domain.states import AttemptState, TaskState
from orqestra.persistence._shared import _CALLER_TRANSACTION, Conn, tuple_cursor
from orqestra.persistence.provider_usage import CaptureState, CaptureTier, ClosedBy

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

# Every call that reported an amount, summed exactly in numeric: per (unit,
# current attempt state), and per unit for the whole job (grouping flag 1).
_USAGE_TOTALS = """
SELECT GROUPING(u.state), u.state, u.unit, count(*), sum(u.amount)
FROM (
    SELECT a.state, r.reported_unit COLLATE "C" AS unit, r.reported_amount AS amount
    FROM provider_calls c
    JOIN provider_call_results r ON r.attempt_id = c.attempt_id AND r.call_seq = c.call_seq
    JOIN attempts a ON a.id = c.attempt_id
    WHERE c.job_id = %s AND r.reported_amount IS NOT NULL
) AS u
GROUP BY GROUPING SETS ((u.unit, u.state), (u.unit))
ORDER BY 1, 2, 3
"""

_MAX_ATTEMPTS_TOTAL = "SELECT coalesce(sum(max_attempts), 0) FROM tasks WHERE job_id = %s"

# One row per attempt of the job, with its calls counted and its closure, if any.
# A call is unresolved while it has no result.
_STREAM_ATTEMPT_USAGE = """
SELECT
    a.id,
    t.id,
    t.task_key,
    a.attempt_number,
    a.state,
    coalesce(u.intents, 0),
    coalesce(u.answered, 0),
    coalesce(u.failed, 0),
    coalesce(u.unresolved, 0),
    coalesce(u.settled_without_amount, 0),
    coalesce(u.late_intents, 0),
    coalesce(u.late_results, 0),
    coalesce(u.unresolved_estimate, 0),
    coalesce(u.unresolved_without_estimate, 0),
    cl.capture,
    cl.call_count,
    cl.closed_by,
    cl.capture_tier
FROM tasks t
JOIN attempts a ON a.task_id = t.id
LEFT JOIN (
    SELECT
        c.attempt_id,
        count(*) AS intents,
        count(*) FILTER (WHERE r.outcome = 'answered') AS answered,
        count(*) FILTER (WHERE r.outcome = 'failed') AS failed,
        count(*) FILTER (WHERE r.attempt_id IS NULL) AS unresolved,
        count(*) FILTER (
            WHERE r.attempt_id IS NOT NULL AND r.reported_amount IS NULL
        ) AS settled_without_amount,
        count(*) FILTER (WHERE c.attempt_state_at_record <> 'running') AS late_intents,
        count(*) FILTER (WHERE r.attempt_state_at_record <> 'running') AS late_results,
        sum(c.estimated_cost_units) FILTER (WHERE r.attempt_id IS NULL) AS unresolved_estimate,
        count(*) FILTER (
            WHERE r.attempt_id IS NULL AND c.estimated_cost_units IS NULL
        ) AS unresolved_without_estimate
    FROM provider_calls c
    LEFT JOIN provider_call_results r ON r.attempt_id = c.attempt_id AND r.call_seq = c.call_seq
    WHERE c.job_id = %s
    GROUP BY c.attempt_id
) AS u ON u.attempt_id = a.id
LEFT JOIN attempt_usage_closures cl ON cl.attempt_id = a.id
WHERE t.job_id = %s
ORDER BY t.task_key COLLATE "C", a.attempt_number
"""


@dataclass(frozen=True, slots=True)
class UsageTotalRow:
    """The exact sum of the amounts reported in one unit.

    ``attempt_state`` is ``None`` for the whole job's total of the unit, and
    otherwise the current state of the attempts whose calls it sums. ``calls``
    counts the calls that reported an amount in this unit; ``amount`` is their
    sum, computed by PostgreSQL in ``numeric``.
    """

    attempt_state: AttemptState | None
    unit: str
    calls: int
    amount: Decimal


@dataclass(frozen=True, slots=True)
class AttemptUsageRow:
    """One attempt of the job and what its ledger says, as counts.

    ``state`` is the attempt's current state. Every call has an intent; it is
    ``answered`` or ``failed`` once its result is recorded, and ``unresolved``
    until then, so ``intents == answered + failed + unresolved``.
    ``settled_without_amount`` counts results that reported no amount.
    ``late_intents`` and ``late_results`` count facts recorded after the attempt
    stopped running (advisory). ``unresolved_estimate_units`` sums the route
    estimates of the unresolved calls, and ``unresolved_without_estimate`` counts
    unresolved calls that had none. The ``closure_*`` fields are the attempt's
    closure, all ``None`` when it has none.
    """

    attempt_id: uuid.UUID
    task_id: uuid.UUID
    task_key: str
    attempt_number: int
    state: AttemptState
    intents: int
    answered: int
    failed: int
    unresolved: int
    settled_without_amount: int
    late_intents: int
    late_results: int
    unresolved_estimate_units: int
    unresolved_without_estimate: int
    closure_capture: CaptureState | None
    closure_call_count: int | None
    closure_closed_by: ClosedBy | None
    closure_tier: CaptureTier | None


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
    """The job row read at the start of the snapshot and, on request, its other reads.

    ``tasks`` and ``attempt_usage`` are streams; each can be opened once.
    ``usage_totals`` and ``max_attempts_total`` are single bounded queries. All
    of them run only while the snapshot block is open, and describe the
    snapshot, whatever has been committed since.
    """

    def __init__(self, conn: Conn, job: Job, observed_at: datetime, fetch_size: int) -> None:
        self.job = job
        self.observed_at = observed_at
        self._conn = conn
        self._fetch_size = fetch_size
        self._cursors: dict[str, psycopg.ServerCursor[TupleRow]] = {}
        self._closed = False

    def tasks(self) -> Iterator[BatchTaskRow]:
        """Open the job's task stream now and return its rows, fetched in chunks."""
        cursor = self._open_stream("tasks", "task stream", _STREAM_TASKS, (self.job.id,))
        return _rows(cursor)

    def attempt_usage(self) -> Iterator[AttemptUsageRow]:
        """Open the job's attempt usage stream now and return its rows, fetched in chunks."""
        params = (self.job.id, self.job.id)
        cursor = self._open_stream(
            "attempt_usage", "attempt usage stream", _STREAM_ATTEMPT_USAGE, params
        )
        return _attempt_rows(cursor)

    def usage_totals(self) -> list[UsageTotalRow]:
        """The job's exact reported totals per unit, whole-job and per attempt state.

        Its size is bounded by the number of distinct units times the number of
        attempt states, so it is fetched at once.
        """
        self._check_open()
        with tuple_cursor(self._conn) as cur:
            rows = cur.execute(_USAGE_TOTALS, (self.job.id,)).fetchall()
        return [
            UsageTotalRow(None if job_level else AttemptState(state), unit, calls, amount)
            for job_level, state, unit, calls, amount in rows
        ]

    def max_attempts_total(self) -> int:
        """The sum of the job's task retry limits: the most attempts its tasks may start."""
        self._check_open()
        with tuple_cursor(self._conn) as cur:
            row = cur.execute(_MAX_ATTEMPTS_TOTAL, (self.job.id,)).fetchone()
        return 0 if row is None else int(row[0])

    def _check_open(self) -> None:
        if self._closed:
            msg = "the snapshot has ended"
            raise RuntimeError(msg)

    def _open_stream(
        self, prefix: str, label: str, query: str, params: tuple[uuid.UUID, ...]
    ) -> psycopg.ServerCursor[TupleRow]:
        self._check_open()
        if prefix in self._cursors:
            msg = f"the {label} was already opened"
            raise RuntimeError(msg)
        name = f"orqestra_{prefix}_{uuid.uuid4().hex}"
        cursor = self._conn.cursor(name=name, row_factory=tuple_row)
        self._cursors[prefix] = cursor
        cursor.itersize = self._fetch_size
        cursor.execute(query, params)
        return cursor

    def _close(self) -> None:
        """End the streams before the transaction does, without masking the error that ended it."""
        self._closed = True
        for cursor in self._cursors.values():
            with suppress(psycopg.Error):
                cursor.close()


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


def _attempt_rows(cursor: psycopg.ServerCursor[TupleRow]) -> Iterator[AttemptUsageRow]:
    for row in cursor:
        (
            attempt_id,
            task_id,
            task_key,
            attempt_number,
            state,
            intents,
            answered,
            failed,
            unresolved,
            settled_without_amount,
            late_intents,
            late_results,
            unresolved_estimate,
            unresolved_without_estimate,
            capture,
            call_count,
            closed_by,
            tier,
        ) = row
        yield AttemptUsageRow(
            attempt_id=attempt_id,
            task_id=task_id,
            task_key=task_key,
            attempt_number=attempt_number,
            state=AttemptState(state),
            intents=intents,
            answered=answered,
            failed=failed,
            unresolved=unresolved,
            settled_without_amount=settled_without_amount,
            late_intents=late_intents,
            late_results=late_results,
            # sum(bigint) is numeric; the estimates are integers, so this is exact.
            unresolved_estimate_units=int(unresolved_estimate),
            unresolved_without_estimate=unresolved_without_estimate,
            closure_capture=None if capture is None else CaptureState(capture),
            closure_call_count=call_count,
            closure_closed_by=None if closed_by is None else ClosedBy(closed_by),
            closure_tier=None if tier is None else CaptureTier(tier),
        )
