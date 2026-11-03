"""Migration 0004 (durable retry eligibility) against real PostgreSQL (DEC-053).

Each test starts from a genuine schema at migration 0003, populated as the code
of that time populated it (``historical``), with no ``available_at`` anywhere.
The upgrade must keep every stored row as it was, make every task eligible at
once, leave running attempts alone, fail at once rather than wait while a
session uses ``tasks``, roll back completely when it fails, and not lock
``attempts``.
"""

import threading
import time
from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.domain.jobs import JobSubmission, Task, TaskSpec
from orqestra.domain.leases import LeasePolicy
from orqestra.persistence._shared import TASK_COLUMNS
from orqestra.persistence.jobs import list_tasks, submit_job
from orqestra.persistence.lifecycle import (
    claim_next_attempt,
    renew_lease,
    request_cancellation,
)
from orqestra.persistence.migrate import MigrationError, load_migrations, migrate, verify_current
from tests.integration import historical
from tests.integration.test_attempt_lifecycle import GRACE, POLICY, assert_invariants, until_expired

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

INITIAL, LEASES, GRACE_REASON, AVAILABILITY, *_ = load_migrations()
UP_TO_0003 = [INITIAL, LEASES, GRACE_REASON]
UP_TO_0004 = [*UP_TO_0003, AVAILABILITY]  # never a later migration: it has its own tests
KINDS = ("availability",)
EXPIRING = LeasePolicy(lease=timedelta(microseconds=1), execution_timeout=timedelta(hours=1))


def connect(url: str, *, autocommit: bool = True) -> Conn:
    conn = psycopg.connect(url, autocommit=autocommit)
    conn.execute("SET statement_timeout = '20s'")  # a hang fails the test instead
    if not autocommit:
        conn.commit()
    return conn


@pytest.fixture
def at_0003(schema_url: str) -> Iterator[Callable[[], Conn]]:
    """Bring ``schema_url`` to migration 0003; return a factory of connections to it."""
    migrate(schema_url, UP_TO_0003)
    opened: list[Conn] = []

    def open_connection() -> Conn:
        conn = connect(schema_url)
        opened.append(conn)
        return conn

    yield open_connection
    for conn in opened:
        conn.close()


def new_tasks(conn: Conn, count: int) -> list[Task]:
    specs = tuple(TaskSpec(f"t{n}", {}, 3) for n in range(count))
    job = submit_job(conn, JobSubmission(KINDS[0], specs)).job
    return list(list_tasks(conn, job.id))


def versions(conn: Conn) -> list[int]:
    return [int(v) for (v,) in conn.execute("SELECT version FROM schema_migrations ORDER BY 1")]


def has_available_at(conn: Conn) -> bool:
    row = conn.execute(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() "
        "AND table_name = 'tasks' AND column_name = 'available_at'"
    ).fetchone()
    return row == (1,)


def snapshot(conn: Conn) -> dict[str, list[tuple[Any, ...]]]:
    """Every row as 0003 knew it: the task columns before 0004, and every attempt column."""
    return {
        "tasks": conn.execute(f"SELECT {TASK_COLUMNS} FROM tasks ORDER BY id").fetchall(),
        "attempts": conn.execute("SELECT * FROM attempts ORDER BY id").fetchall(),
    }


def clock(conn: Conn) -> Any:
    row = conn.execute("SELECT clock_timestamp()").fetchone()
    assert row is not None
    return row[0]


def upgrade_refused(url: str) -> MigrationError:
    """Run the upgrade, which must refuse promptly; a blocked upgrade fails the test."""
    outcome: list[object] = []

    def run() -> None:
        try:
            outcome.append(migrate(url, UP_TO_0004))
        except Exception as exc:  # returned to the test thread, which inspects it
            outcome.append(exc)

    upgrading = threading.Thread(target=run, daemon=True)
    upgrading.start()
    upgrading.join(timeout=10)
    if upgrading.is_alive():
        pytest.fail("the upgrade waited for a lock instead of refusing at once")
    (result,) = outcome
    assert isinstance(result, MigrationError), f"expected a refusal, got {result!r}"
    return result


def populate(conn: Conn) -> dict[str, Any]:
    """A 0003 history: queued, requeued after revocation, running, cancelled and succeeded."""
    queued, requeued, running, cancelled, succeeded = new_tasks(conn, 5)
    expired = historical.start(conn, requeued.id, "dead-worker", policy=EXPIRING)
    until_expired(conn, expired)
    assert historical.revoke(conn) is not None  # requeued with budget left, as 0003 did
    live = historical.start(conn, running.id, "w", policy=POLICY)
    request_cancellation(conn, cancelled.id, grace=GRACE)  # queued: cancelled at once
    done = historical.start(conn, succeeded.id, "w", policy=POLICY)
    historical.succeed(conn, done)
    return {"queued": queued, "requeued": requeued, "live": live}


# --- tests ------------------------------------------------------------------------


def test_upgrades_a_populated_0003_schema_keeping_rows_and_making_tasks_eligible(
    schema_url: str, at_0003: Callable[[], Conn]
) -> None:
    conn = at_0003()
    history = populate(conn)
    assert not has_available_at(conn)
    before = snapshot(conn)
    upgrade_started = clock(conn)

    assert [m.label for m in migrate(schema_url, UP_TO_0004)] == [AVAILABILITY.label]

    upgrade_ended = clock(conn)
    assert versions(conn) == [1, 2, 3, 4]
    assert snapshot(conn) == before  # every task and attempt as it was
    stamps = {row[0] for row in conn.execute("SELECT DISTINCT available_at FROM tasks")}
    assert len(stamps) == 1  # one default, evaluated once by the upgrade
    (stamp,) = stamps
    assert upgrade_started <= stamp <= upgrade_ended
    # Current code: both queued tasks are claimable at once; the running attempt carries on.
    claimed = {
        a.task_id
        for a in (claim_next_attempt(conn, "w", policy=POLICY, kinds=KINDS) for _ in range(3))
        if a is not None
    }
    assert claimed == {history["queued"].id, history["requeued"].id}
    live = history["live"]
    renewed = renew_lease(conn, live.id, generation=1, worker_id="w", policy=POLICY)
    assert renewed.lease_expires_at is not None
    assert_invariants(conn)


def test_a_repeated_upgrade_changes_nothing_and_the_history_verifies(
    schema_url: str, at_0003: Callable[[], Conn]
) -> None:
    conn = at_0003()
    populate(conn)
    assert [m.label for m in migrate(schema_url, UP_TO_0004)] == [AVAILABILITY.label]
    after = conn.execute("SELECT * FROM tasks ORDER BY id").fetchall()
    assert migrate(schema_url, UP_TO_0004) == ()
    assert conn.execute("SELECT * FROM tasks ORDER BY id").fetchall() == after
    verify_current(conn, UP_TO_0004)  # the exact, read-only history check a worker runs (DEC-051)


def test_fails_at_once_while_a_reader_holds_tasks_and_rolls_back(
    schema_url: str, at_0003: Callable[[], Conn]
) -> None:
    conn = at_0003()
    populate(conn)
    before = snapshot(conn)
    with connect(schema_url, autocommit=False) as reader:
        reader.execute("SELECT count(*) FROM tasks")  # left idle in its transaction
        refused = upgrade_refused(schema_url)
        assert refused.sqlstate == "55P03"
        assert "could not lock tasks" in str(refused)
        assert versions(conn) == [1, 2, 3]  # rolled back: no history row
        assert not has_available_at(conn)  # and no column
        assert snapshot(conn) == before
        reader.rollback()
    assert [m.label for m in migrate(schema_url, UP_TO_0004)] == [AVAILABILITY.label]  # the retry


def test_fails_at_once_while_a_lifecycle_write_holds_a_task(
    schema_url: str, at_0003: Callable[[], Conn]
) -> None:
    conn, holder = at_0003(), at_0003()
    (task,) = new_tasks(conn, 1)
    holder.execute("BEGIN")  # as a claim or completion of that time holds its task
    holder.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task.id,))
    assert upgrade_refused(schema_url).sqlstate == "55P03"
    assert versions(conn) == [1, 2, 3]
    assert not has_available_at(conn)
    holder.execute("ROLLBACK")
    assert [m.label for m in migrate(schema_url, UP_TO_0004)] == [AVAILABILITY.label]


def test_does_not_lock_the_attempts_table(schema_url: str, at_0003: Callable[[], Conn]) -> None:
    # A transaction holding an attempt row (and nothing on tasks) does not stop
    # the upgrade: it changes only tasks.
    conn, holder = at_0003(), at_0003()
    (task,) = new_tasks(conn, 1)
    attempt = historical.start(conn, task.id, "w", policy=POLICY)
    holder.execute("BEGIN")
    holder.execute("SELECT id FROM attempts WHERE id = %s FOR UPDATE", (attempt.id,))
    started = time.monotonic()
    assert [m.label for m in migrate(schema_url, UP_TO_0004)] == [AVAILABILITY.label]
    assert time.monotonic() - started < 5
    holder.execute("ROLLBACK")


def test_adds_a_not_null_timestamptz_column_and_no_index(
    schema_url: str, at_0003: Callable[[], Conn]
) -> None:
    conn = at_0003()
    indexes = "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema() "
    indexes += "AND tablename = 'tasks' ORDER BY indexname"
    before = conn.execute(indexes).fetchall()
    assert [m.label for m in migrate(schema_url, UP_TO_0004)] == [AVAILABILITY.label]
    assert conn.execute(indexes).fetchall() == before  # claim order is unchanged: no new index
    column = conn.execute(
        "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = 'tasks' "
        "AND column_name = 'available_at'"
    ).fetchone()
    assert column is not None
    data_type, nullable, default = column
    assert (data_type, nullable) == ("timestamp with time zone", "NO")
    assert "now()" in str(default)
